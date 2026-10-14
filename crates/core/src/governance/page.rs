//! The governed-page typestate (RFC 0014, #895).
//!
//! A [`GovernedPage`] is a page of records that went through every per-page
//! governance pass — masking, quality, contract, schema drift. Only this
//! module tree can mint one (from the output of those passes), and every
//! write path inside the engine takes one, so `faucet-core` cannot hand a
//! sink a page that skipped governance: the mistake does not compile.
//!
//! Derivations keep the property: a [`slice`](GovernedPage::slice) or a
//! [`select`](GovernedPage::select)ion of a governed page is governed, and a
//! DLQ envelope built from a governed record joins a [`GovernedEnvelopes`].

use serde_json::Value;
use std::borrow::Cow;
use std::ops::Range;

/// A page of records that went through every per-page governance pass.
///
/// Nothing outside [`governance`](crate::governance) can construct one: there
/// is no public constructor and the field is private. Get one by running the
/// passes — [`Governance::govern`](crate::governance::Governance::govern), or
/// the pipeline itself — and write it with
/// [`Sink::write_governed`](crate::Sink::write_governed).
///
/// A raw page cannot be passed where a governed one is expected:
///
/// ```compile_fail,E0308
/// use faucet_core::{Sink, json};
///
/// async fn write(sink: &dyn Sink) {
///     let records = vec![json!({"id": 1})];
///     // error: expected `&GovernedPage<'_>`, found `&Vec<Value>`
///     let _ = sink.write_governed(&records).await;
/// }
/// ```
///
/// and a governed page cannot be built by hand:
///
/// ```compile_fail,E0451
/// use faucet_core::governance::GovernedPage;
///
/// // error: `GovernedPage` has no public constructor and a private field
/// let page = GovernedPage { records: vec![].into() };
/// ```
///
/// Running the passes is the only way in:
///
/// ```
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// use faucet_core::governance::Governance;
/// use faucet_core::{FaucetError, Sink, Value, async_trait, json};
///
/// struct Rows;
///
/// #[async_trait]
/// impl Sink for Rows {
///     async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
///         Ok(records.len())
///     }
/// }
///
/// let governed = Governance::new().govern(vec![json!({"id": 1})], &Rows).await?;
/// let report = Rows.write_governed(&governed.page).await?;
/// assert_eq!(report.accepted, 1);
/// # Ok::<(), FaucetError>(())
/// # }).unwrap();
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct GovernedPage<'a> {
    records: Cow<'a, [Value]>,
}

impl GovernedPage<'static> {
    /// The output of the governance passes. Only this module tree calls it.
    pub(in crate::governance) fn sealed(records: Vec<Value>) -> Self {
        Self {
            records: Cow::Owned(records),
        }
    }
}

impl<'a> GovernedPage<'a> {
    /// The governed records.
    pub fn records(&self) -> &[Value] {
        &self.records
    }

    /// How many records the page holds.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the page holds no record.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The records `range` of this page, still governed. Panics when `range`
    /// is out of bounds, like slice indexing.
    pub fn slice(&self, range: Range<usize>) -> GovernedPage<'_> {
        GovernedPage {
            records: Cow::Borrowed(&self.records[range]),
        }
    }

    /// The records at `indices`, in that order, still governed. Panics when
    /// an index is out of bounds.
    pub fn select(&self, indices: &[usize]) -> GovernedPage<'static> {
        GovernedPage {
            records: Cow::Owned(indices.iter().map(|&i| self.records[i].clone()).collect()),
        }
    }

    /// The page with `records` in place of these: for a governance layer
    /// (the metadata columns) that rewrites the governed records it was
    /// handed. Crate-internal: only governance layers derive pages this way.
    pub(crate) fn restamped(&self, records: Vec<Value>) -> GovernedPage<'static> {
        GovernedPage {
            records: Cow::Owned(records),
        }
    }

    /// Leave the typestate: the records, owned.
    pub fn into_records(self) -> Vec<Value> {
        self.records.into_owned()
    }
}

/// Dead-letter envelopes built from governed records — the only thing the
/// engine's DLQ writer accepts.
///
/// The governance passes produce the quarantine envelopes; a sink failure adds
/// one per failed row with [`push_failure`](Self::push_failure), which takes
/// the governed page the row came from.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GovernedEnvelopes {
    envelopes: Vec<Value>,
}

impl GovernedEnvelopes {
    /// No envelope.
    pub fn new() -> Self {
        Self::default()
    }

    pub(in crate::governance) fn sealed(envelopes: Vec<Value>) -> Self {
        Self { envelopes }
    }

    /// Add the envelope `build` makes from record `index` of `page`. Panics
    /// when `index` is out of bounds.
    pub fn push_failure(
        &mut self,
        page: &GovernedPage<'_>,
        index: usize,
        build: impl FnOnce(&Value) -> Value,
    ) {
        self.envelopes.push(build(&page.records()[index]));
    }

    /// Put `earlier`'s envelopes ahead of these, keeping both orders.
    pub fn prepend(&mut self, earlier: GovernedEnvelopes) {
        self.envelopes.splice(0..0, earlier.envelopes);
    }

    /// The envelopes.
    pub fn envelopes(&self) -> &[Value] {
        &self.envelopes
    }

    /// How many envelopes there are.
    pub fn len(&self) -> usize {
        self.envelopes.len()
    }

    /// Whether there is no envelope.
    pub fn is_empty(&self) -> bool {
        self.envelopes.is_empty()
    }
}

/// Proof that no record pass is configured for a run, minted by
/// [`GovernanceSpecs::clearance`](super::apply::GovernanceSpecs::clearance):
/// with nothing to apply, a columnar batch or a native byte batch is governed
/// as it is.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Clearance(pub(in crate::governance) ());

/// A columnar batch the governance passes produced, or one passed through
/// under a [`Clearance`]. The columnar write path takes only this.
#[cfg(feature = "arrow")]
#[derive(Debug, Clone)]
pub(crate) struct GovernedBatch {
    batch: arrow::array::RecordBatch,
}

#[cfg(feature = "arrow")]
impl GovernedBatch {
    /// A batch no record pass applies to: one under a [`Clearance`], or an
    /// empty one (nothing to govern). Anything else is refused.
    pub(crate) fn passthrough(
        batch: arrow::array::RecordBatch,
        clearance: Option<&Clearance>,
    ) -> Result<Self, crate::FaucetError> {
        if clearance.is_none() && batch.num_rows() > 0 {
            return Err(crate::FaucetError::Config(
                "a columnar batch with a record governance pass configured must be governed \
                 before it is written"
                    .into(),
            ));
        }
        Ok(Self { batch })
    }

    /// Re-encode a governed page against `schema`.
    pub(crate) fn encode(
        page: &GovernedPage<'_>,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, crate::FaucetError> {
        Ok(Self {
            batch: crate::columnar::values_to_record_batch(page.records(), schema)?,
        })
    }

    pub(crate) fn batch(&self) -> &arrow::array::RecordBatch {
        &self.batch
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// A native byte batch passed through under a [`Clearance`]. The native load
/// path takes only this.
#[derive(Debug)]
pub(crate) struct GovernedNative {
    batch: crate::native::NativeBatch,
}

impl GovernedNative {
    /// A native batch no record pass applies to.
    pub(crate) fn passthrough(batch: crate::native::NativeBatch, _: &Clearance) -> Self {
        Self { batch }
    }

    pub(crate) fn into_inner(self) -> crate::native::NativeBatch {
        self.batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn page() -> GovernedPage<'static> {
        GovernedPage::sealed(vec![json!(0), json!(1), json!(2), json!(3)])
    }

    #[test]
    fn derivations_stay_governed_and_keep_order() {
        let p = page();
        assert_eq!(p.len(), 4);
        assert!(!p.is_empty());
        let s = p.slice(1..3);
        assert_eq!(s.records(), &[json!(1), json!(2)]);
        let picked = p.select(&[3, 0]);
        assert_eq!(picked.into_records(), vec![json!(3), json!(0)]);
        assert!(GovernedPage::sealed(vec![]).is_empty());
        assert_eq!(p.clone(), p);
    }

    #[test]
    fn envelopes_are_built_from_governed_rows_and_prepend_in_order() {
        let p = page();
        let mut env = GovernedEnvelopes::new();
        assert!(env.is_empty());
        env.push_failure(&p, 2, |r| json!({ "payload": r }));
        let mut all = GovernedEnvelopes::sealed(vec![json!("q")]);
        all.prepend(GovernedEnvelopes::sealed(vec![json!("first")]));
        all.envelopes.extend(env.envelopes().iter().cloned());
        assert_eq!(all.len(), 3);
        assert_eq!(
            all.envelopes(),
            &[json!("first"), json!("q"), json!({"payload": 2})]
        );
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn a_governed_page_re_encodes_into_a_governed_batch() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema =
            std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let p = GovernedPage::sealed(vec![json!({"id": 1}), json!({"id": 2})]);
        let b = GovernedBatch::encode(&p, schema.clone()).unwrap();
        assert_eq!(b.num_rows(), 2);
        let again = GovernedBatch::passthrough(b.batch().clone(), Some(&Clearance(()))).unwrap();
        assert_eq!(again.batch().schema(), schema);
        let refused = GovernedBatch::passthrough(b.batch().clone(), None).unwrap_err();
        assert!(
            refused.to_string().contains("must be governed"),
            "{refused}"
        );
        let empty = arrow::array::RecordBatch::new_empty(schema);
        assert_eq!(
            GovernedBatch::passthrough(empty, None).unwrap().num_rows(),
            0
        );
    }

    #[test]
    fn a_native_batch_passes_through_unchanged() {
        let n = crate::native::NativeBatch {
            format: crate::native::NativeFormat::NdJson,
            payload: crate::native::NativePayload::Bytes(b"{}\n".to_vec()),
            csv: Default::default(),
            records: Some(1),
            bookmark: None,
        };
        let g = GovernedNative::passthrough(n, &Clearance(()));
        assert_eq!(g.into_inner().records, Some(1));
    }
}
