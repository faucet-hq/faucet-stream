//! Apache ORC (#719) — **read-only**.
//!
//! Decoded with `orc-rust`, which reads straight into Arrow `RecordBatch`es,
//! stripe by stripe, at most the configured batch size per batch.
//! `orc.columns` projects top-level columns before any stripe is decoded.
//!
//! Memory is not bounded by the batch size alone: `orc-rust` decompresses the
//! footer and each stream chunk whole, without a limit on the decompressed
//! size, so a small file can decompress to far more than it holds. Bounding
//! that needs `orc-rust` to cap every codec's output (faucet-stream#1062).
//! On [`ContainerDecoder`](super::ContainerDecoder)'s record path the
//! converted records are charged against `max_decoded_bytes` (see
//! [`DecodeBudget`](super::DecodeBudget)).
//!
//! There is no ORC writer here by design. `orc-rust`'s writer is experimental:
//! it covers primitive columns only, panics on nested and temporal types, and
//! writes no compression — not something to put behind a sink a pipeline
//! trusts. A sink asked for `format: orc` refuses at encode time; Parquet is
//! the columnar format to write.
//!
//! `orc-rust` 0.8 panics on a stripe whose writer time zone it does not know
//! (faucet-stream#1119). Until a release returns an error instead
//! (datafusion-contrib/orc-rust#95), every stripe footer is checked before
//! any stripe is decoded, and the decoder runs behind a panic boundary: a
//! file it cannot read fails with an error and emits no rows. Delete
//! `check_writer_zones` when the reader reports the zone itself.
//!
//! On the record path the batches go through
//! [`record_batch_to_values`](crate::columnar::record_batch_to_values), so an
//! ORC file and a Parquet file with the same content produce the same records.

use super::OrcOptions;
use crate::error::FaucetError;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use orc_rust::projection::ProjectionMask;
use orc_rust::reader::ChunkReader;
use orc_rust::reader::metadata::FileMetadata;
use orc_rust::stripe::Stripe;
use serde_json::Value;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Where an ORC file's bytes come from.
///
/// A closed set rather than a generic reader so `orc-rust`'s trait never
/// appears in this crate's public API.
#[derive(Debug)]
pub enum OrcInput {
    /// A local file, read stripe by stripe.
    File(std::fs::File),
    /// A whole object already in memory.
    Bytes(bytes::Bytes),
}

/// Decode a whole ORC object into records.
pub fn decode(bytes: &[u8], opts: &OrcOptions) -> Result<Vec<Value>, FaucetError> {
    decode_input(OrcInput::Bytes(bytes::Bytes::copy_from_slice(bytes)), opts)
}

/// [`decode`] taking ownership of the body, so it is read in place rather
/// than copied (CORE-73).
pub fn decode_owned(bytes: Vec<u8>, opts: &OrcOptions) -> Result<Vec<Value>, FaucetError> {
    decode_input(OrcInput::Bytes(bytes::Bytes::from(bytes)), opts)
}

fn decode_input(input: OrcInput, opts: &OrcOptions) -> Result<Vec<Value>, FaucetError> {
    let mut out = Vec::new();
    read_batches(input, opts, 0, &mut |b| {
        out.extend(crate::columnar::record_batch_to_values(&b)?);
        Ok(())
    })?;
    Ok(out)
}

/// Stream an ORC file's batches (at most `batch_size` rows each; `0` means the
/// reader's own stripe-sized batches). Returns the (projected) Arrow schema,
/// known even for a file with no rows.
pub fn read_batches(
    input: OrcInput,
    opts: &OrcOptions,
    batch_size: usize,
    f: &mut dyn FnMut(RecordBatch) -> Result<(), FaucetError>,
) -> Result<SchemaRef, FaucetError> {
    read_batches_checked(input, opts, batch_size, &mut |_| Ok(()), f)
}

/// [`read_batches`], calling `check` with the file's (projected) schema
/// before any stripe is decoded — so a caller can refuse a file whose shape
/// it cannot accept without having emitted any of its rows.
pub fn read_batches_checked(
    input: OrcInput,
    opts: &OrcOptions,
    batch_size: usize,
    check: &mut dyn FnMut(&SchemaRef) -> Result<(), FaucetError>,
    f: &mut dyn FnMut(RecordBatch) -> Result<(), FaucetError>,
) -> Result<SchemaRef, FaucetError> {
    // The zone check's handle; it is done before the decode starts.
    match input {
        OrcInput::File(file) => {
            let second = file.try_clone().map_err(|e| {
                FaucetError::Source(format!("orc: could not open the file a second time: {e}"))
            })?;
            read_from(file, second, opts, batch_size, check, f)
        }
        OrcInput::Bytes(b) => read_from(b.clone(), b, opts, batch_size, check, f),
    }
}

/// What a caught decoder panic carried.
fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "no message".to_string())
}

/// Run a step of the ORC decoder; a panic inside it is an error.
fn guarded<T>(what: &str, step: impl FnOnce() -> T) -> Result<T, FaucetError> {
    catch_unwind(AssertUnwindSafe(step)).map_err(|payload| {
        FaucetError::Source(format!(
            "orc: the decoder panicked while {what} ({}); the file is damaged or uses something \
             the ORC reader does not support",
            panic_text(payload)
        ))
    })
}

/// Whether `name` parses as the zone type the decoder parses writer zones
/// into. The type is taken from the decoder's own signature, so the check and
/// the decoder cannot disagree about which names exist.
fn zone_is_known<T: std::str::FromStr>(_decoded_as: fn(&Stripe) -> Option<T>, name: &str) -> bool {
    name.parse::<T>().is_ok()
}

/// Refuse a file with a stripe written in a time zone the decoder does not
/// know, before any stripe is decoded: the decoder would panic on it, after
/// it had emitted the rows of the stripes before.
fn check_writer_zones<R: ChunkReader>(
    reader: &R,
    metadata: &FileMetadata,
) -> Result<(), FaucetError> {
    use prost::Message as _;
    use std::io::Read as _;
    let len = reader.len();
    for (n, stripe) in metadata.stripe_metadatas().iter().enumerate() {
        let (offset, length) = (stripe.footer_offset(), stripe.footer_length());
        if offset.checked_add(length).is_none_or(|end| end > len) {
            return Err(FaucetError::Source(format!(
                "orc: decode error: the footer of stripe {n} lies outside the file \
                 (offset {offset}, length {length}, file length {len})"
            )));
        }
        let footer = guarded("reading a stripe footer", || {
            let bytes = reader
                .get_bytes(offset, length)
                .map_err(|e| format!("could not read the footer of stripe {n}: {e}"))?;
            let mut plain = Vec::new();
            orc_rust::compression::Decompressor::new(bytes, metadata.compression(), Vec::new())
                .read_to_end(&mut plain)
                .map_err(|e| format!("could not decompress the footer of stripe {n}: {e}"))?;
            orc_rust::proto::StripeFooter::decode(plain.as_slice())
                .map_err(|e| format!("the footer of stripe {n} is not a stripe footer: {e}"))
        })?
        .map_err(|e: String| FaucetError::Source(format!("orc: decode error: {e}")))?;
        if let Some(zone) = footer.writer_timezone.as_deref()
            && !zone_is_known(Stripe::writer_tz, zone)
        {
            return Err(FaucetError::Source(format!(
                "orc: stripe {n} was written in time zone {zone:?}, which is not a zone of this \
                 build's IANA time-zone database (a Java writer stores its JVM default, which \
                 can be a legacy id such as PST or an offset such as GMT+08:00). The file cannot \
                 be read, with or without timestamp columns, until the ORC reader accepts such \
                 a zone; nothing of it was read"
            )));
        }
    }
    Ok(())
}

fn read_from<R: ChunkReader>(
    reader: R,
    second: R,
    opts: &OrcOptions,
    batch_size: usize,
    check: &mut dyn FnMut(&SchemaRef) -> Result<(), FaucetError>,
    f: &mut dyn FnMut(RecordBatch) -> Result<(), FaucetError>,
) -> Result<SchemaRef, FaucetError> {
    let mut builder = guarded("reading the file footer", || {
        orc_rust::ArrowReaderBuilder::try_new(reader)
    })?
    .map_err(|e| FaucetError::Source(format!("orc: not a readable ORC file: {e}")))?;
    if let Some(cols) = &opts.columns {
        let root = builder.file_metadata().root_data_type();
        let known: Vec<&str> = root.children().iter().map(|c| c.name()).collect();
        if let Some(missing) = cols.iter().find(|c| !known.contains(&c.as_str())) {
            return Err(FaucetError::Source(format!(
                "orc: column {missing:?} is not in the file (columns: {})",
                known.join(", ")
            )));
        }
        let mask = ProjectionMask::named_roots(root, cols);
        builder = builder.with_projection(mask);
    }
    if batch_size > 0 {
        builder = builder.with_batch_size(batch_size);
    }
    let schema = builder.schema();
    check(&schema)?;
    check_writer_zones(&second, builder.file_metadata())?;
    drop(second);
    let mut batches = guarded("opening the first stripe", || builder.build())?;
    while let Some(batch) = guarded("decoding a stripe", || batches.next())? {
        let batch = batch.map_err(|e| FaucetError::Source(format!("orc: decode error: {e}")))?;
        if batch.num_rows() > 0 {
            f(batch)?;
        }
    }
    Ok(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &[u8] = include_bytes!("../../tests/fixtures/orc/people.orc");

    #[test]
    fn reads_every_column_and_row() {
        let rows = decode(FIXTURE, &OrcOptions::default()).expect("decode");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["id"], serde_json::json!(1));
        assert_eq!(rows[0]["name"], serde_json::json!("ada"));
        assert!(rows[0].get("score").is_some());
    }

    #[test]
    fn projection_keeps_only_the_named_columns() {
        let opts = OrcOptions {
            columns: Some(vec!["name".into()]),
        };
        let mut schemas = Vec::new();
        let mut rows = Vec::new();
        let schema = read_batches(
            OrcInput::Bytes(bytes::Bytes::from_static(FIXTURE)),
            &opts,
            2,
            &mut |b| {
                schemas.push(b.schema());
                rows.extend(crate::columnar::record_batch_to_values(&b)?);
                Ok(())
            },
        )
        .expect("read");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schemas.len(), 2, "batch size 2 over 3 rows");
        assert_eq!(rows[2], serde_json::json!({"name": "grace"}));
    }

    #[test]
    fn unknown_columns_and_garbage_are_errors() {
        let opts = OrcOptions {
            columns: Some(vec!["nope".into()]),
        };
        let err = decode(FIXTURE, &opts).expect_err("unknown column");
        assert!(err.to_string().contains("nope"), "{err}");
        let err = decode(b"definitely not orc", &OrcOptions::default()).expect_err("garbage");
        assert!(err.to_string().contains("orc"), "{err}");
    }

    /// Two uncompressed stripes of 1,024 rows, writer zone `GMT` in each
    /// stripe footer (`generate.py`).
    const ZONES: &[u8] = include_bytes!("../../tests/fixtures/orc/zones.orc");
    /// `StripeFooter.writer_timezone` (field 3, a 3-byte string) holding `GMT`.
    const GMT_FIELD: &[u8] = &[0x1a, 0x03, b'G', b'M', b'T'];

    /// `ZONES` with the writer zone of the listed stripes replaced by `zone`
    /// (three bytes, so every length in the file stays right).
    fn zones_with(zone: &[u8; 3], stripes: &[usize]) -> Vec<u8> {
        let mut bytes = ZONES.to_vec();
        let sites: Vec<usize> = bytes
            .windows(GMT_FIELD.len())
            .enumerate()
            .filter(|(_, w)| *w == GMT_FIELD)
            .map(|(at, _)| at)
            .collect();
        assert_eq!(sites.len(), 2, "one writer zone per stripe footer");
        for stripe in stripes {
            bytes[sites[*stripe] + 2..sites[*stripe] + 5].copy_from_slice(zone);
        }
        bytes
    }

    fn count_rows(input: OrcInput) -> Result<usize, FaucetError> {
        let mut n = 0;
        read_batches(input, &OrcOptions::default(), 0, &mut |b| {
            n += b.num_rows();
            Ok(())
        })?;
        Ok(n)
    }

    #[test]
    fn a_file_with_known_writer_zones_reads_every_stripe() {
        let metadata = orc_rust::ArrowReaderBuilder::try_new(bytes::Bytes::from_static(ZONES))
            .unwrap()
            .file_metadata()
            .stripe_metadatas()
            .len();
        assert_eq!(metadata, 2, "the fixture has two stripes");
        assert_eq!(count_rows(OrcInput::Bytes(ZONES.into())).unwrap(), 2048);
        // Another name the database knows reads the same.
        let utc = zones_with(b"UTC", &[0, 1]);
        assert_eq!(count_rows(OrcInput::Bytes(utc.into())).unwrap(), 2048);
    }

    #[test]
    fn an_unknown_writer_zone_is_an_error_naming_the_stripe_and_the_zone() {
        for (stripes, stripe) in [(&[0usize, 1][..], 0), (&[0][..], 0), (&[1][..], 1)] {
            let bad = zones_with(b"Q0Q", stripes);
            let err = decode(&bad, &OrcOptions::default())
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(&format!("orc: stripe {stripe} was written")),
                "{err}"
            );
            assert!(
                err.contains(r#""Q0Q""#) && err.contains("nothing of it was read"),
                "{err}"
            );
        }
    }

    #[test]
    fn a_bad_later_stripe_emits_no_rows_of_the_earlier_one() {
        let bad = zones_with(b"Q0Q", &[1]);
        let mut seen = 0;
        let err = read_batches(
            OrcInput::Bytes(bad.clone().into()),
            &OrcOptions::default(),
            100,
            &mut |b| {
                seen += b.num_rows();
                Ok(())
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("stripe 1"), "{err}");
        assert_eq!(
            seen, 0,
            "the first stripe is fine but nothing may be emitted"
        );

        // The same through a local file.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.orc");
        std::fs::write(&path, &bad).unwrap();
        let err = count_rows(OrcInput::File(std::fs::File::open(&path).unwrap())).unwrap_err();
        assert!(err.to_string().contains(r#""Q0Q""#), "{err}");
        std::fs::write(&path, ZONES).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        assert_eq!(count_rows(OrcInput::File(file)).unwrap(), 2048);
    }

    #[test]
    fn the_schema_check_still_runs_before_the_zone_check() {
        let bad = zones_with(b"Q0Q", &[0]);
        let err = read_batches_checked(
            OrcInput::Bytes(bad.into()),
            &OrcOptions::default(),
            0,
            &mut |_| Err(FaucetError::Source("shape refused".into())),
            &mut |_| Ok(()),
        )
        .unwrap_err();
        assert!(err.to_string().ends_with("shape refused"), "{err}");
    }

    /// A file cut off at the front still ends in a postscript, whose lengths
    /// then point before its start: the footer read must fail, not panic.
    #[test]
    fn a_file_whose_footer_lengths_exceed_it_is_an_error() {
        for keep in [40, 60, 120, 200] {
            let tail = ZONES[ZONES.len() - keep..].to_vec();
            let err = decode(&tail, &OrcOptions::default())
                .unwrap_err()
                .to_string();
            assert!(err.contains("orc:"), "{keep}: {err}");
        }
    }

    #[test]
    fn a_decoder_panic_is_an_error() {
        let err = guarded("decoding a stripe", || -> u8 { panic!("boom {}", 7) }).unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("panicked while decoding a stripe (boom 7)"),
            "{text}"
        );
        let err = guarded("x", || -> u8 { std::panic::panic_any(5u8) }).unwrap_err();
        assert!(err.to_string().contains("no message"), "{err}");
        let err = guarded("x", || -> u8 { panic!("{}", String::from("owned")) }).unwrap_err();
        assert!(err.to_string().contains("owned"), "{err}");
    }

    /// A stripe footer the file's own lengths put past its end is refused
    /// before it is read (the in-memory reader would panic on the slice).
    #[test]
    fn a_stripe_footer_outside_the_file_is_an_error() {
        struct Short(bytes::Bytes);
        impl ChunkReader for Short {
            type T = <bytes::Bytes as ChunkReader>::T;
            fn len(&self) -> u64 {
                10
            }
            fn get_read(&self, offset: u64) -> std::io::Result<Self::T> {
                self.0.get_read(offset)
            }
        }
        let whole = bytes::Bytes::from_static(ZONES);
        let builder = orc_rust::ArrowReaderBuilder::try_new(whole.clone()).unwrap();
        let err = check_writer_zones(&Short(whole.clone()), builder.file_metadata()).unwrap_err();
        assert!(err.to_string().contains("lies outside the file"), "{err}");
        check_writer_zones(&whole, builder.file_metadata()).unwrap();
    }

    /// The decoder builds the ORC epoch in the writer zone with an `unwrap`,
    /// which would panic in a zone with a gap or a repeated hour at that
    /// instant. No zone of the bundled database has one.
    #[test]
    fn no_bundled_zone_has_a_gap_or_fold_at_the_orc_epoch() {
        use chrono::TimeZone as _;
        for zone in chrono_tz::TZ_VARIANTS {
            assert!(
                zone.with_ymd_and_hms(2015, 1, 1, 0, 0, 0)
                    .single()
                    .is_some(),
                "{zone}"
            );
        }
    }

    #[test]
    fn reads_from_a_local_file() {
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("p.orc");
        std::fs::write(&path, FIXTURE).expect("write");
        let mut n = 0;
        read_batches(
            OrcInput::File(std::fs::File::open(&path).expect("open")),
            &OrcOptions::default(),
            0,
            &mut |b| {
                n += b.num_rows();
                Ok(())
            },
        )
        .expect("read");
        assert_eq!(n, 3);
    }
}
