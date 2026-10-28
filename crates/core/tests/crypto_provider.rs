use std::sync::Arc;

use rustls::crypto::{CryptoProvider, aws_lc_rs};

#[test]
fn ensure_crypto_provider_keeps_an_already_installed_provider() {
    let mut mine = aws_lc_rs::default_provider();
    mine.cipher_suites.truncate(1);
    mine.install_default()
        .expect("first install in this process");

    faucet_core::ensure_crypto_provider();
    faucet_core::tls::ensure_crypto_provider();

    let installed: Arc<CryptoProvider> = CryptoProvider::get_default().unwrap().clone();
    assert_eq!(installed.cipher_suites.len(), 1);
}
