#![no_main]

use std::sync::LazyLock;

use faucet_core::compression::{Compression, wrap_async_reader};
use faucet_core::file_format::{FileFormat, FormatOptions, decode, read_to_end_capped};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;

const MAX_OBJECT_BYTES: u64 = 64 << 20;

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime")
});

// First byte: codec (bits 0-1) and format (bits 2-4); second byte: CSV
// delimiter. The rest is the file body.
fuzz_target!(|data: &[u8]| {
    let [selector, delimiter, body @ ..] = data else {
        return;
    };
    let codec = match selector & 3 {
        0 => Compression::None,
        1 => Compression::Gzip,
        _ => Compression::Zstd,
    };
    let format = match (selector >> 2) & 7 {
        0 | 1 => FileFormat::Csv,
        2 | 3 => FileFormat::Xml,
        4 => FileFormat::JsonLines,
        5 => FileFormat::JsonArray,
        _ => FileFormat::RawText,
    };
    let mut opts = FormatOptions::default();
    if delimiter.is_ascii() {
        opts.csv.delimiter = char::from(*delimiter).to_string();
    }
    opts.csv.has_headers = selector & 0x20 == 0;
    opts.csv.flexible = Some(selector & 0x40 != 0);
    RT.block_on(async {
        let reader = wrap_async_reader(body, codec);
        let Ok(bytes) = read_to_end_capped(reader, MAX_OBJECT_BYTES, "fuzz").await else {
            return;
        };
        let _ = decode(&bytes, format, &opts).await;
    });
});
