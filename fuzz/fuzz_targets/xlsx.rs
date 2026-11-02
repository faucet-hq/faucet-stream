#![no_main]

use std::sync::LazyLock;

use faucet_core::file_format::{FileFormat, FormatOptions, decode};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime")
});

// First byte: header row (bits 0-1) and sheet selector (bits 2-3); the rest is
// the workbook.
fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else {
        return;
    };
    let header_row = usize::from(selector & 3);
    let sheet = match (selector >> 2) & 3 {
        0 => None,
        1 => Some("Sheet1"),
        2 => Some("0"),
        _ => Some("missing"),
    };
    let _ = faucet_source_rest::format::parse_excel(body, sheet, header_row);
    let mut opts = FormatOptions::default();
    opts.excel.sheet = sheet.map(str::to_owned);
    opts.excel.header_row = header_row;
    let _ = RT.block_on(decode(body, FileFormat::Xlsx, &opts));
});
