//! CRC32C checksums for uploads (#803).
//!
//! google-cloud-storage (>= 1.19) validates every upload's CRC32C. Left to
//! itself, a single-shot upload sends the checksum as a third multipart part,
//! which a server that reads only metadata + media (the GCS emulator) stores
//! as object content, so the upload then fails its own validation. Declaring
//! the checksum up front (`with_known_crc32c`) puts it in the metadata part,
//! where every server checks it against the bytes it stored.

use std::path::Path;

/// The CRC32C of an in-memory upload body.
pub fn crc32c_of_bytes(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Open a local file for upload, with its CRC32C (read in chunks; the file is
/// rewound to its start).
pub async fn open_with_crc32c(path: &Path) -> std::io::Result<(tokio::fs::File, u32)> {
    use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
    let mut file = tokio::fs::File::open(path).await?;
    let mut buf = vec![0_u8; 256 * 1024];
    let mut crc = 0_u32;
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        crc = crc32c::crc32c_append(crc, &buf[..n]);
    }
    file.rewind().await?;
    Ok((file, crc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_use_the_crc32c_check_value() {
        assert_eq!(crc32c_of_bytes(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c_of_bytes(b""), 0);
    }

    #[tokio::test]
    async fn the_crc32c_covers_the_whole_file_across_chunks() {
        use tokio::io::AsyncReadExt as _;
        let dir = std::env::temp_dir().join(format!("faucet-crc32c-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let crc = |name: &str, body: &[u8]| {
            let path = dir.join(name);
            std::fs::write(&path, body).unwrap();
            async move {
                let (mut file, crc) = open_with_crc32c(&path).await.unwrap();
                let mut read = Vec::new();
                file.read_to_end(&mut read).await.unwrap();
                assert_eq!(read.len(), std::fs::metadata(&path).unwrap().len() as usize);
                crc
            }
        };
        assert_eq!(crc("small", b"123456789").await, 0xE306_9283);
        let body: Vec<u8> = (0..700_000_u32).map(|i| (i % 251) as u8).collect();
        assert_eq!(crc("big", &body).await, crc32c_of_bytes(&body));
        assert_eq!(crc("empty", b"").await, 0);
        assert!(open_with_crc32c(&dir.join("missing")).await.is_err());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
