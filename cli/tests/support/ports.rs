//! A loopback port for a test server. The OS hands out a free ephemeral port
//! and the listener is dropped before the server binds it, so two tests of one
//! binary could otherwise be handed the same port in that window; a port is
//! never handed out twice per process.
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Mutex;

static HANDED_OUT: Mutex<Option<HashSet<u16>>> = Mutex::new(None);

/// A free loopback port no other caller in this process has been given.
pub fn free_port() -> u16 {
    let mut seen = HANDED_OUT.lock().unwrap_or_else(|e| e.into_inner());
    let seen = seen.get_or_insert_with(HashSet::new);
    loop {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("bind an ephemeral loopback port")
            .local_addr()
            .expect("a bound address")
            .port();
        if seen.insert(port) {
            return port;
        }
    }
}
