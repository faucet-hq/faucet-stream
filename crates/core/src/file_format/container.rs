//! Multi-file decoding of the self-describing container formats — Avro and
//! ORC (#719).
//!
//! A connector reading a prefix or a directory decodes many files into one
//! stream, so their shapes must agree. [`ContainerDecoder`] holds that rule in
//! one place, keyed on the first file it decodes:
//!
//! - **Avro** — the configured reader schema, or else the first file's writer
//!   schema, is the reader schema for every file. A later file written with an
//!   evolved schema is resolved against it (fields it lacks take their
//!   defaults); a file that cannot be resolved fails with an error naming both
//!   files. Without a configured schema, a later file that adds top-level
//!   fields fails too, naming them, rather than having them dropped for the
//!   whole run.
//! - **ORC** — the first file's (projected) Arrow schema is the reference; a
//!   file whose schema differs fails with an error naming both files.
//!
//! Decoding is synchronous. From async code use the `*_offloaded` methods,
//! which run it on a blocking thread; both [`FileInput`] variants stream
//! rather than materialize the decoded rows.

#![cfg_attr(
    not(any(feature = "file-format-avro", feature = "file-format-orc")),
    allow(unreachable_code, unused_variables, dead_code)
)]

use super::{FileFormat, FormatOptions};
use crate::error::FaucetError;
use serde_json::Value;

/// Where one container file's bytes come from.
#[derive(Debug)]
pub enum FileInput {
    /// A local file — decoded incrementally (Avro block by block, ORC stripe
    /// by stripe), so memory is bounded by the chunk, not the file.
    File(std::fs::File),
    /// A whole object already in memory (object stores).
    Bytes(Vec<u8>),
}

#[derive(Debug)]
enum Anchor {
    #[cfg(feature = "file-format-avro")]
    Avro(apache_avro::Schema),
    #[cfg(feature = "file-format-orc")]
    Orc(arrow::datatypes::SchemaRef),
}

/// Decodes a sequence of Avro or ORC files against one shape.
#[derive(Debug)]
pub struct ContainerDecoder {
    format: FileFormat,
    #[cfg(feature = "file-format-orc")]
    opts: FormatOptions,
    #[cfg(feature = "file-format-avro")]
    configured: bool,
    anchor: Option<(String, Anchor)>,
}

impl ContainerDecoder {
    /// A decoder for `format`, which must be [`FileFormat::Avro`] or
    /// [`FileFormat::Orc`] in a build with the matching feature.
    pub fn new(format: FileFormat, opts: &FormatOptions) -> Result<Self, FaucetError> {
        let anchor: Option<(String, Anchor)> = match format {
            #[cfg(feature = "file-format-avro")]
            FileFormat::Avro => opts
                .avro
                .parsed_schema()?
                .map(|s| ("avro.schema".to_string(), Anchor::Avro(s))),
            #[cfg(feature = "file-format-orc")]
            FileFormat::Orc => None,
            #[cfg(not(feature = "file-format-avro"))]
            FileFormat::Avro => return Err(super::missing_feature(format, "file-format-avro")),
            #[cfg(not(feature = "file-format-orc"))]
            FileFormat::Orc => return Err(super::missing_feature(format, "file-format-orc")),
            other => {
                return Err(FaucetError::Config(format!(
                    "ContainerDecoder handles avro and orc, not `{}`",
                    other.as_str()
                )));
            }
        };
        Ok(Self {
            format,
            #[cfg(feature = "file-format-orc")]
            opts: opts.clone(),
            #[cfg(feature = "file-format-avro")]
            configured: anchor.is_some(),
            anchor,
        })
    }

    /// The format this decoder reads.
    pub fn format(&self) -> FileFormat {
        self.format
    }

    /// Decode one whole file into its records.
    pub fn decode_all(&mut self, name: &str, input: FileInput) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::new();
        self.records(name, input, 0, &mut |c| {
            out.extend(c);
            Ok(())
        })?;
        Ok(out)
    }

    /// [`decode_all`](Self::decode_all) on a blocking thread, so a large
    /// object does not stall an async worker (CORE-34). Takes and returns the
    /// decoder because the decode runs on another thread.
    pub async fn decode_all_offloaded(
        mut self,
        name: String,
        input: FileInput,
    ) -> Result<(Self, Vec<Value>), FaucetError> {
        tokio::task::spawn_blocking(move || {
            let rows = self.decode_all(&name, input)?;
            Ok((self, rows))
        })
        .await
        .map_err(|e| FaucetError::Source(format!("container decode task failed: {e}")))?
    }

    /// [`decode_batches`](Self::decode_batches) on a blocking thread.
    #[cfg(feature = "arrow")]
    #[allow(clippy::type_complexity)]
    pub async fn decode_batches_offloaded(
        mut self,
        name: String,
        input: FileInput,
        batch_size: usize,
    ) -> Result<(Self, Vec<arrow::array::RecordBatch>), FaucetError> {
        tokio::task::spawn_blocking(move || {
            let (_, batches) = self.decode_batches(&name, input, batch_size)?;
            Ok((self, batches))
        })
        .await
        .map_err(|e| FaucetError::Source(format!("container decode task failed: {e}")))?
    }

    /// Decode one whole file into Arrow batches of at most `batch_size` rows.
    #[cfg(feature = "arrow")]
    pub fn decode_batches(
        &mut self,
        name: &str,
        input: FileInput,
        batch_size: usize,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), FaucetError> {
        let mut out = Vec::new();
        let schema = self.batches(name, input, batch_size, &mut |b| {
            out.push(b);
            Ok(())
        })?;
        Ok((schema, out))
    }

    /// Decode one file's records in chunks of `chunk` (`0` = the whole file
    /// as one chunk), calling `f` per chunk.
    pub fn records(
        &mut self,
        name: &str,
        input: FileInput,
        chunk: usize,
        f: &mut dyn FnMut(Vec<Value>) -> Result<(), FaucetError>,
    ) -> Result<(), FaucetError> {
        let chunk = if chunk == 0 { usize::MAX } else { chunk };
        match self.format {
            #[cfg(feature = "file-format-avro")]
            FileFormat::Avro => {
                let reader_schema = self.avro_reader_schema();
                let check = self.avro_writer_check();
                let result = with_reader(input, |r| {
                    super::avro::read_records_checked(
                        r,
                        reader_schema.as_ref(),
                        check.as_deref(),
                        chunk,
                        f,
                    )
                });
                self.finish_avro(name, reader_schema, result)
            }
            #[cfg(feature = "file-format-orc")]
            FileFormat::Orc => {
                let batch = if chunk == usize::MAX { 0 } else { chunk };
                let mut pending: Vec<Value> = Vec::new();
                self.orc_read(name, input, batch, &mut |b| {
                    let rows = crate::columnar::record_batch_to_values(&b)?;
                    if chunk == usize::MAX {
                        pending.extend(rows);
                        Ok(())
                    } else {
                        f(rows)
                    }
                })?;
                if !pending.is_empty() {
                    f(pending)?;
                }
                Ok(())
            }
            _ => unreachable!("rejected in new()"),
        }
    }

    /// Decode one file as Arrow batches of at most `batch_size` rows (`0` =
    /// the reader's natural size), calling `f` per batch. Returns the file's
    /// Arrow schema, identical for every file this decoder accepts.
    #[cfg(feature = "arrow")]
    pub fn batches(
        &mut self,
        name: &str,
        input: FileInput,
        batch_size: usize,
        f: &mut dyn FnMut(arrow::array::RecordBatch) -> Result<(), FaucetError>,
    ) -> Result<arrow::datatypes::SchemaRef, FaucetError> {
        match self.format {
            #[cfg(feature = "file-format-avro")]
            FileFormat::Avro => {
                let reader_schema = self.avro_reader_schema();
                let check = self.avro_writer_check();
                let result = with_reader(input, |r| {
                    super::avro::read_batches_checked(
                        r,
                        reader_schema.as_ref(),
                        check.as_deref(),
                        batch_size,
                        f,
                    )
                });
                let arrow = result.as_ref().ok().map(|(_, a)| a.clone());
                self.finish_avro(name, reader_schema, result.map(|(s, _)| s))?;
                Ok(arrow.expect("finish_avro succeeded only on Ok"))
            }
            #[cfg(feature = "file-format-orc")]
            FileFormat::Orc => self.orc_read(name, input, batch_size, f),
            _ => unreachable!("rejected in new()"),
        }
    }

    #[cfg(feature = "file-format-avro")]
    fn avro_reader_schema(&self) -> Option<apache_avro::Schema> {
        match &self.anchor {
            Some((_, Anchor::Avro(s))) => Some(s.clone()),
            _ => None,
        }
    }

    /// When the reader schema is the first file's writer schema (no
    /// configured `avro.schema`), refuse a later file whose writer schema has
    /// top-level fields the anchor lacks: Avro resolution would silently drop
    /// them for the whole run (CORE-38).
    #[cfg(feature = "file-format-avro")]
    #[allow(clippy::type_complexity)]
    fn avro_writer_check(&self) -> Option<Box<super::avro::WriterCheck<'static>>> {
        if self.configured {
            return None;
        }
        let Some((_, Anchor::Avro(anchor))) = &self.anchor else {
            return None;
        };
        let known: std::collections::HashSet<String> =
            record_field_names(anchor).into_iter().collect();
        Some(Box::new(move |writer: &apache_avro::Schema| {
            let extra: Vec<String> = record_field_names(writer)
                .into_iter()
                .filter(|n| !known.contains(n))
                .collect();
            if extra.is_empty() {
                Ok(())
            } else {
                Err(FaucetError::Source(format!(
                    "it has field(s) {} the reader schema lacks, which would be dropped — \
                     set `avro.schema` to a reader schema that includes them",
                    extra
                        .iter()
                        .map(|n| format!("`{n}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )))
            }
        }))
    }

    #[cfg(feature = "file-format-avro")]
    fn finish_avro(
        &mut self,
        name: &str,
        reader_schema: Option<apache_avro::Schema>,
        result: Result<apache_avro::Schema, FaucetError>,
    ) -> Result<(), FaucetError> {
        match (result, &self.anchor) {
            (Ok(schema), None) => {
                self.anchor = Some((name.to_string(), Anchor::Avro(schema)));
                Ok(())
            }
            (Ok(_), Some(_)) => Ok(()),
            (Err(e), Some((first, _))) if reader_schema.is_some() => {
                let against = if self.configured {
                    "the configured `avro.schema`".to_string()
                } else {
                    format!("'{first}' (the first file's schema)")
                };
                Err(FaucetError::Source(format!(
                    "avro schema of '{name}' cannot be resolved against {against}: {e}"
                )))
            }
            (Err(e), _) => Err(FaucetError::Source(format!("'{name}': {e}"))),
        }
    }

    #[cfg(feature = "file-format-orc")]
    fn orc_read(
        &mut self,
        name: &str,
        input: FileInput,
        batch_size: usize,
        f: &mut dyn FnMut(arrow::array::RecordBatch) -> Result<(), FaucetError>,
    ) -> Result<arrow::datatypes::SchemaRef, FaucetError> {
        let input = match input {
            FileInput::File(file) => super::orc::OrcInput::File(file),
            FileInput::Bytes(b) => super::orc::OrcInput::Bytes(bytes::Bytes::from(b)),
        };
        let reference = match &self.anchor {
            Some((first, Anchor::Orc(s))) => Some((first.clone(), s.clone())),
            _ => None,
        };
        let mut check = |schema: &arrow::datatypes::SchemaRef| -> Result<(), FaucetError> {
            match &reference {
                Some((first, reference)) if !same_shape(reference, schema) => {
                    Err(schema_conflict(name, first, reference, schema))
                }
                _ => Ok(()),
            }
        };
        let schema =
            super::orc::read_batches_checked(input, &self.opts.orc, batch_size, &mut check, f)
                .map_err(|e| prefix(name, e))?;
        if self.anchor.is_none() {
            self.anchor = Some((name.to_string(), Anchor::Orc(schema.clone())));
        }
        Ok(schema)
    }
}

/// The columnar page stream for a list of container objects: fetch each with
/// an ordered `concurrency`-wide look-ahead, decode in listing order, and
/// yield one [`ColumnarPage`](crate::columnar::ColumnarPage) per non-empty
/// batch. The shared body of every object-store source's `stream_batches`
/// for Avro and ORC.
#[cfg(feature = "arrow")]
pub fn columnar_pages<'a, F, Fut>(
    names: Vec<String>,
    concurrency: usize,
    mut decoder: ContainerDecoder,
    batch_size: usize,
    fetch: F,
) -> std::pin::Pin<
    Box<dyn futures::Stream<Item = Result<crate::columnar::ColumnarPage, FaucetError>> + Send + 'a>,
>
where
    F: Fn(String) -> Fut + Send + Sync + 'a,
    Fut: std::future::Future<Output = Result<Vec<u8>, FaucetError>> + Send + 'a,
{
    use futures::StreamExt as _;
    Box::pin(async_stream::try_stream! {
        let fetch = &fetch;
        let mut fetched = futures::stream::iter(names)
            .map(|name| async move {
                let body = fetch(name.clone()).await;
                (name, body)
            })
            .buffered(concurrency.max(1));
        while let Some((name, body)) = fetched.next().await {
            let (d, batches) = decoder
                .decode_batches_offloaded(name, FileInput::Bytes(body?), batch_size)
                .await?;
            decoder = d;
            for batch in batches {
                yield crate::columnar::ColumnarPage::new(batch, None);
            }
        }
    })
}

#[cfg(feature = "file-format-avro")]
fn record_field_names(schema: &apache_avro::Schema) -> Vec<String> {
    match schema {
        apache_avro::Schema::Record(r) => r.fields.iter().map(|f| f.name.clone()).collect(),
        _ => Vec::new(),
    }
}

#[cfg(feature = "file-format-avro")]
fn with_reader<T>(
    input: FileInput,
    f: impl FnOnce(&mut dyn std::io::Read) -> Result<T, FaucetError>,
) -> Result<T, FaucetError> {
    match input {
        FileInput::File(file) => f(&mut std::io::BufReader::new(file)),
        FileInput::Bytes(b) => f(&mut &b[..]),
    }
}

#[cfg(feature = "file-format-orc")]
fn prefix(name: &str, e: FaucetError) -> FaucetError {
    match e {
        FaucetError::Source(m) if !m.starts_with('\'') => {
            FaucetError::Source(format!("'{name}': {m}"))
        }
        other => other,
    }
}

/// Whether two schemas carry the same columns: names and types in order.
/// Nullability and metadata are not shape — a writer marking a column
/// nullable in one file and not in the next still produces the same rows.
#[cfg(feature = "arrow")]
pub fn same_shape(a: &arrow::datatypes::Schema, b: &arrow::datatypes::Schema) -> bool {
    a.fields().len() == b.fields().len()
        && a.fields()
            .iter()
            .zip(b.fields().iter())
            .all(|(x, y)| x.name() == y.name() && x.data_type() == y.data_type())
}

/// The error for two files whose shapes disagree, naming both and the first
/// field that differs.
#[cfg(feature = "arrow")]
pub fn schema_conflict(
    file: &str,
    first: &str,
    reference: &arrow::datatypes::Schema,
    schema: &arrow::datatypes::Schema,
) -> FaucetError {
    let detail = reference
        .fields()
        .iter()
        .zip(schema.fields().iter())
        .find(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
        .map(|(a, b)| {
            format!(
                "field `{}` ({}) vs `{}` ({})",
                a.name(),
                a.data_type(),
                b.name(),
                b.data_type()
            )
        })
        .unwrap_or_else(|| {
            format!(
                "{} vs {} fields",
                reference.fields().len(),
                schema.fields().len()
            )
        });
    FaucetError::Source(format!(
        "schema of '{file}' conflicts with '{first}' (the first file's schema): {detail}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "file-format-avro")]
    use serde_json::json;

    #[cfg(feature = "file-format-avro")]
    fn avro(records: &[Value]) -> Vec<u8> {
        super::super::avro::encode(records, &Default::default()).expect("encode")
    }

    #[cfg(all(feature = "file-format-avro", feature = "arrow"))]
    #[tokio::test]
    async fn columnar_pages_decode_in_listing_order() {
        use futures::StreamExt as _;
        let bodies: std::collections::HashMap<String, Vec<u8>> = [
            ("a".to_string(), avro(&[json!({"id": 1}), json!({"id": 2})])),
            ("b".to_string(), avro(&[json!({"id": 3})])),
        ]
        .into_iter()
        .collect();
        let decoder = ContainerDecoder::new(FileFormat::Avro, &FormatOptions::default()).unwrap();
        let pages: Vec<_> = columnar_pages(
            vec!["a".into(), "b".into(), "missing".into()],
            2,
            decoder,
            1,
            |n| {
                let body = bodies.get(&n).cloned();
                async move { body.ok_or_else(|| FaucetError::Source(format!("no {n}"))) }
            },
        )
        .collect()
        .await;
        assert_eq!(pages.len(), 4);
        assert_eq!(
            pages[..3]
                .iter()
                .map(|p| p.as_ref().unwrap().num_rows())
                .sum::<usize>(),
            3
        );
        assert!(pages[3].as_ref().is_err());
    }

    #[cfg(all(feature = "file-format-avro", feature = "arrow"))]
    #[tokio::test]
    async fn offloaded_decodes_keep_the_anchor() {
        let d = ContainerDecoder::new(FileFormat::Avro, &FormatOptions::default()).unwrap();
        let (d, rows) = d
            .decode_all_offloaded("a".into(), FileInput::Bytes(avro(&[json!({"id": 1})])))
            .await
            .unwrap();
        assert_eq!(rows, vec![json!({"id": 1})]);
        let (d, batches) = d
            .decode_batches_offloaded("b".into(), FileInput::Bytes(avro(&[json!({"id": 2})])), 0)
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        let err = d
            .decode_all_offloaded(
                "c".into(),
                FileInput::Bytes(avro(&[json!({"id": 1, "z": 1})])),
            )
            .await
            .expect_err("anchored on a");
        assert!(err.to_string().contains("`z`"), "{err}");
    }

    #[test]
    fn only_container_formats_are_accepted() {
        let err =
            ContainerDecoder::new(FileFormat::Csv, &FormatOptions::default()).expect_err("csv");
        assert!(err.to_string().contains("avro and orc"), "{err}");
    }

    #[cfg(feature = "file-format-avro")]
    #[test]
    fn later_avro_files_resolve_against_the_first() {
        let mut d = ContainerDecoder::new(FileFormat::Avro, &FormatOptions::default()).unwrap();
        assert_eq!(d.format(), FileFormat::Avro);
        let mut rows = Vec::new();
        d.records(
            "a.avro",
            FileInput::Bytes(avro(&[json!({"id": 1, "x": "a"})])),
            0,
            &mut |c| {
                rows.extend(c);
                Ok(())
            },
        )
        .unwrap();
        // A later file that adds a field would lose it: refused, naming it,
        // before any of its rows are emitted (CORE-38).
        let wider = avro(&[json!({"id": 2, "x": "b", "extra": true})]);
        let err = d
            .records("b.avro", FileInput::Bytes(wider), 1, &mut |c| {
                rows.extend(c);
                Ok(())
            })
            .expect_err("added field");
        let msg = err.to_string();
        assert!(msg.contains("b.avro") && msg.contains("`extra`"), "{msg}");
        // A file with a subset of the fields still resolves.
        d.records(
            "b2.avro",
            FileInput::Bytes(avro(&[json!({"id": 2, "x": "b"})])),
            1,
            &mut |c| {
                rows.extend(c);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            rows,
            vec![json!({"id": 1, "x": "a"}), json!({"id": 2, "x": "b"})]
        );
        let err = d
            .records(
                "c.avro",
                FileInput::Bytes(avro(&[json!({"id": "text"})])),
                0,
                &mut |_| Ok(()),
            )
            .expect_err("conflict");
        let msg = err.to_string();
        assert!(msg.contains("c.avro") && msg.contains("a.avro"), "{msg}");
    }

    #[cfg(feature = "file-format-avro")]
    #[test]
    fn a_configured_reader_schema_is_named_in_conflicts() {
        let opts = FormatOptions {
            avro: super::super::AvroOptions {
                schema: Some(
                    json!({"type": "record", "name": "faucet_record", "fields": [
                        {"name": "id", "type": "long"}
                    ]}),
                ),
                ..Default::default()
            },
            ..Default::default()
        };
        let mut d = ContainerDecoder::new(FileFormat::Avro, &opts).unwrap();
        let err = d
            .records(
                "x.avro",
                FileInput::Bytes(avro(&[json!({"other": 1})])),
                0,
                &mut |_| Ok(()),
            )
            .expect_err("unresolvable");
        assert!(err.to_string().contains("configured"), "{err}");
        let err = d
            .records("y.avro", FileInput::Bytes(b"junk".to_vec()), 0, &mut |_| {
                Ok(())
            })
            .expect_err("junk");
        assert!(err.to_string().contains("y.avro"), "{err}");
    }

    #[cfg(all(feature = "file-format-avro", feature = "arrow"))]
    #[test]
    fn avro_batches_and_local_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.avro");
        std::fs::write(&path, avro(&[json!({"id": 1}), json!({"id": 2})])).unwrap();
        let mut d = ContainerDecoder::new(FileFormat::Avro, &FormatOptions::default()).unwrap();
        let mut n = 0;
        let schema = d
            .batches(
                "a.avro",
                FileInput::File(std::fs::File::open(&path).unwrap()),
                1,
                &mut |b| {
                    n += b.num_rows();
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(schema.field(0).name(), "id");
    }

    #[cfg(feature = "file-format-orc")]
    #[test]
    fn orc_files_must_share_a_schema() {
        const FIXTURE: &[u8] = include_bytes!("../../tests/fixtures/orc/people.orc");
        let mut d = ContainerDecoder::new(FileFormat::Orc, &FormatOptions::default()).unwrap();
        let mut rows = Vec::new();
        d.records("a.orc", FileInput::Bytes(FIXTURE.to_vec()), 0, &mut |c| {
            rows.extend(c);
            Ok(())
        })
        .unwrap();
        assert_eq!(rows.len(), 3);
        let mut chunks = 0;
        d.records("b.orc", FileInput::Bytes(FIXTURE.to_vec()), 2, &mut |_| {
            chunks += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(chunks, 2);
        let mut projected = ContainerDecoder::new(
            FileFormat::Orc,
            &FormatOptions {
                orc: super::super::OrcOptions {
                    columns: Some(vec!["id".into()]),
                },
                ..Default::default()
            },
        )
        .unwrap();
        let s = projected
            .batches("p.orc", FileInput::Bytes(FIXTURE.to_vec()), 0, &mut |_| {
                Ok(())
            })
            .unwrap();
        // A second decoder anchored on the full schema refuses the projected one.
        let full = d.anchor.as_ref().map(|(_, a)| match a {
            Anchor::Orc(s) => s.clone(),
            #[allow(unreachable_patterns)]
            _ => unreachable!(),
        });
        let err = schema_conflict("p.orc", "a.orc", &full.unwrap(), &s);
        assert!(err.to_string().contains("p.orc") && err.to_string().contains("a.orc"));
        d.anchor = Some(("a.orc".into(), Anchor::Orc(s)));
        let err = d
            .records("c.orc", FileInput::Bytes(FIXTURE.to_vec()), 0, &mut |_| {
                Ok(())
            })
            .expect_err("conflict");
        assert!(err.to_string().contains("c.orc"), "{err}");
        let err = d
            .records("d.orc", FileInput::Bytes(b"junk".to_vec()), 0, &mut |_| {
                Ok(())
            })
            .expect_err("junk");
        assert!(err.to_string().contains("d.orc"), "{err}");
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn schema_conflict_reports_a_field_count_difference() {
        use arrow::datatypes::{DataType, Field, Schema};
        let a = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        let b = Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]);
        let msg = schema_conflict("b", "a", &a, &b).to_string();
        assert!(msg.contains("1 vs 2"), "{msg}");
    }
}
