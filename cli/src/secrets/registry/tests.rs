use super::*;
use serial_test::serial;

const PEM: &str = "-----BEGIN KEY-----\nMIIBVgIBADANBgkqhkiG9w0BAQEF\nshort\n-----END KEY-----";

/// Drop only the values these tests register: the registry is global,
/// and wiping it would remove a secret a concurrent test elsewhere just
/// registered.
fn clear() {
    for s in [
        "supersecrettoken",
        "pa\"ss\\word-1",
        PEM,
        "abcd",
        "abcdXYZW",
        "supersecretvalue",
        "rotated-away-token",
        "still-live-token",
        "alpha-secret",
        "hunter2pass",
    ] {
        unregister(s);
    }
}

#[test]
#[serial]
fn redacts_registered_value() {
    clear();
    register("supersecrettoken");
    assert_eq!(
        redact("Authorization: supersecrettoken"),
        "Authorization: ***"
    );
}

#[test]
#[serial]
fn escaped_and_per_line_forms_are_redacted() {
    clear();
    register("pa\"ss\\word-1");
    let json = serde_json::json!({ "msg": "pw pa\"ss\\word-1" }).to_string();
    assert!(!redact(&json).contains("word-1"), "{}", redact(&json));
    let dbg = format!("{:?}", "pa\"ss\\word-1");
    assert_eq!(redact(&dbg), "\"***\"");
    let pem = PEM;
    register(pem);
    assert!(!redact("line: MIIBVgIBADANBgkqhkiG9w0BAQEF").contains("MIIBVg"));
    let escaped = serde_json::to_string(pem).unwrap();
    assert!(!redact(&escaped).contains("MIIBVg"), "{}", redact(&escaped));
    assert_eq!(redact("short"), "short");
    clear();
}

#[test]
#[serial]
fn leaves_unregistered_text_untouched() {
    clear();
    register("supersecrettoken");
    assert_eq!(redact("nothing to see"), "nothing to see");
}

#[test]
#[serial]
fn does_not_register_short_values() {
    clear();
    register("abc"); // < MIN_REDACT_LEN
    assert_eq!(redact("abc def"), "abc def");
}

#[test]
#[serial]
fn redact_handles_overlapping_secrets_longest_first() {
    clear();
    // A shorter secret that is a prefix of a longer one. Redacting the
    // shorter first (unordered iteration) leaves the longer secret's tail
    // ("XYZW") exposed; longest-first redaction must mask the whole thing.
    register("abcd");
    register("abcdXYZW");
    let out = redact("value=abcdXYZW end");
    assert!(
        !out.contains("XYZW"),
        "longer secret partially leaked: {out}"
    );
    assert_eq!(out, "value=*** end");
}

#[test]
#[serial]
fn overlapping_secrets_are_masked_as_one_run() {
    clear();
    register("xxab");
    register("abcdXYZW");
    let out = redact("v=xxabcdXYZW end");
    assert_eq!(out, "v=*** end");
    // A secret ending where a longer one ends (reported first) still
    // masks the whole longer one.
    register("cdXYZW-tail");
    register("abcdXYZW-tail");
    assert_eq!(redact("k=abcdXYZW-tail."), "k=***.");
    unregister("cdXYZW-tail");
    unregister("abcdXYZW-tail");
    let tokens = redact_with("v=xxabcdXYZW and abcdXYZW", |s| format!("<{}>", s.len()));
    assert_eq!(tokens, "v=<8> and <8>");
    unregister("xxab");
}

#[test]
#[serial]
fn redaction_is_one_pass_over_many_forms() {
    clear();
    for i in 0..5000 {
        register(&format!("secret-value-{i:05}"));
    }
    let line = "a b secret-value-04999 c secret-value-00001 d".repeat(4);
    let started = std::time::Instant::now();
    for _ in 0..200 {
        let out = redact(&line);
        assert!(!out.contains("secret-value"), "{out}");
    }
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
    for i in 0..5000 {
        unregister(&format!("secret-value-{i:05}"));
    }
}

#[test]
#[serial]
fn writer_scrubs_secret_split_across_writes() {
    clear();
    register("supersecretvalue");
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = RedactingWriter::new(&mut buf);
        // The secret straddles two separate write() calls.
        w.write_all(b"token=supersec").unwrap();
        w.write_all(b"retvalue done").unwrap();
        w.flush().unwrap();
    }
    let out = String::from_utf8(buf).unwrap();
    assert!(
        !out.contains("supersecretvalue"),
        "secret leaked across write boundary: {out}"
    );
    assert_eq!(out, "token=*** done");
}

#[test]
#[serial]
fn unregister_drops_every_form_of_a_rotated_secret() {
    clear();
    register("rotated-away-token");
    register("still-live-token");
    assert_eq!(redact("rotated-away-token"), "***");
    unregister("rotated-away-token");
    unregister("never-registered");
    assert_eq!(redact("rotated-away-token"), "rotated-away-token");
    assert_eq!(redact("still-live-token"), "***");
    clear();
}

#[test]
fn the_registry_is_bounded_and_evicts_the_oldest() {
    let mut reg = Registry {
        max_forms: 4,
        ..Registry::default()
    };
    reg.insert(forms_of("the-very-first-secret"));
    for i in 0..6 {
        reg.insert(forms_of(&format!("bounded-secret-{i}")));
    }
    assert!(reg.forms.len() <= 4, "{}", reg.forms.len());
    assert!(!reg.forms.contains_key("the-very-first-secret"));
    assert!(reg.forms.contains_key("bounded-secret-5"));
    let compiled = reg.compile();
    assert_eq!(compiled.max_len, "bounded-secret-5".len());
    assert!(Registry::default().compile().patterns.is_empty());
}

#[test]
fn pinned_secrets_survive_eviction_and_are_still_redacted() {
    let mut reg = Registry {
        max_forms: 4,
        ..Registry::default()
    };
    reg.pin(forms_of("server-bearer-token"));
    for i in 0..8 {
        reg.insert(forms_of(&format!("tenant-secret-{i}")));
    }
    assert!(reg.forms.len() <= 4);
    assert!(reg.pinned.contains("server-bearer-token"));
    assert!(
        reg.compile()
            .patterns
            .iter()
            .any(|p| p == "server-bearer-token")
    );
}

#[test]
#[serial]
fn register_pinned_redacts_and_ignores_short_values() {
    register_pinned("pinned-cred-value");
    register_pinned("abc");
    assert_eq!(redact("token=pinned-cred-value abc"), "token=*** abc");
}

#[test]
#[serial]
fn redact_with_hands_the_token_the_matched_secret() {
    clear();
    assert_eq!(redact_with("plain", |_| unreachable!()), "plain");
    register("alpha-secret");
    let out = redact_with("a alpha-secret b alpha-secret", |s| {
        format!("<{}>", s.len())
    });
    assert_eq!(out, "a <12> b <12>");
    clear();
}

#[test]
#[serial]
fn writer_scrubs_secret_on_write() {
    clear();
    let secret = "hunter2pass";
    register(secret);
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = RedactingWriter::new(&mut buf);
        write!(w, "token={secret} done").unwrap();
        w.flush().unwrap();
    }
    assert_eq!(String::from_utf8(buf).unwrap(), "token=*** done");
}

#[test]
#[serial]
fn redact_message_masks_secrets_and_url_credentials() {
    register("s3cr3t-874-value");
    let msg = "open 'postgres://app:pw@db:5432/x' failed (s3cr3t-874-value); \
                   retry https://h/api?api_key=k1&x=1 and mysql://u:p2@m/y";
    let out = redact_message(msg);
    assert!(!out.contains("s3cr3t-874-value"), "{out}");
    assert!(!out.contains("app:pw"), "{out}");
    assert!(!out.contains("k1"), "{out}");
    assert!(!out.contains("u:p2"), "{out}");
    assert!(out.contains("postgres://db:5432/x"), "{out}");
    assert!(out.contains("mysql://m/y"), "{out}");
    assert!(!redact_message("Password=hunter;Server=x").contains("hunter"));
    assert_eq!(redact_message("plain"), "plain");
    assert_eq!(redact_message("at end url=s3://b/k"), "at end url=s3://b/k");
    unregister("s3cr3t-874-value");
}

#[test]
fn caller_values_never_displace_a_server_secret() {
    let mut reg = Registry {
        max_forms: 4,
        max_caller_forms: 4,
        ..Registry::default()
    };
    reg.insert(forms_of("server-resolved-secret"));
    for i in 0..10 {
        reg.insert_caller(forms_of(&format!("caller-flood-{i}")));
    }
    assert!(reg.forms.contains_key("server-resolved-secret"));
    assert!(reg.caller.len() <= 4, "{}", reg.caller.len());
    assert!(reg.caller.contains_key("caller-flood-9"));
    reg.insert_caller(forms_of("server-resolved-secret"));
    assert!(!reg.caller.contains_key("server-resolved-secret"));
    let patterns = reg.compile().patterns;
    assert!(patterns.iter().any(|p| p == "caller-flood-9"));
    assert_eq!(
        patterns
            .iter()
            .filter(|p| *p == "server-resolved-secret")
            .count(),
        1
    );
}

#[test]
#[serial]
fn a_caller_value_is_redacted_until_unregistered() {
    register_caller("caller-param-value");
    register_caller("abc");
    assert_eq!(redact("v=caller-param-value abc"), "v=*** abc");
    unregister("caller-param-value");
    assert_eq!(redact("caller-param-value"), "caller-param-value");
}

#[tokio::test]
#[serial]
async fn caller_secrets_cannot_evict_server_secrets() {
    register("server-flood-guard-secret");
    as_caller(async {
        for i in 0..MAX_CALLER_FORMS + 10 {
            register(&format!("caller-key-line-{i:06}"));
        }
    })
    .await;
    let caller_policy = std::sync::Arc::new(
        faucet_core::resolve::ReferencePolicy::deny_all().with_path_roots(["/srv/caller"]),
    );
    faucet_core::http::with_reference_policy(caller_policy, async {
        register("policy-scoped-caller-value");
    })
    .await;
    {
        let reg = registry().read().unwrap();
        assert!(reg.forms.contains_key("server-flood-guard-secret"));
        assert!(!reg.forms.keys().any(|f| f.starts_with("caller-key-line-")));
        assert!(reg.caller.len() <= MAX_CALLER_FORMS);
        assert!(reg.caller.contains_key("policy-scoped-caller-value"));
    }
    assert_eq!(redact("t=server-flood-guard-secret"), "t=***");
    // A trusted run's policy is open: its values stay with the server's.
    faucet_core::http::with_reference_policy(
        std::sync::Arc::new(faucet_core::resolve::ReferencePolicy::open()),
        async { register("open-policy-server-value") },
    )
    .await;
    assert!(
        registry()
            .read()
            .unwrap()
            .forms
            .contains_key("open-policy-server-value")
    );
    for i in 0..MAX_CALLER_FORMS + 10 {
        unregister(&format!("caller-key-line-{i:06}"));
    }
    for s in [
        "server-flood-guard-secret",
        "policy-scoped-caller-value",
        "open-policy-server-value",
    ] {
        unregister(s);
    }
}

#[test]
fn protected_secrets_survive_every_flood_until_rotated_away() {
    let mut reg = Registry {
        max_forms: 4,
        max_caller_forms: 4,
        ..Registry::default()
    };
    reg.insert(forms_of("tenant-connection-secret"));
    reg.protect(forms_of("tenant-connection-secret"));
    assert!(!reg.forms.contains_key("tenant-connection-secret"));
    for i in 0..10 {
        reg.insert(forms_of(&format!("server-churn-{i}")));
        reg.insert_caller(forms_of(&format!("caller-churn-{i}")));
    }
    reg.insert(forms_of("tenant-connection-secret"));
    reg.insert_caller(forms_of("tenant-connection-secret"));
    assert!(reg.protected.contains("tenant-connection-secret"));
    assert!(!reg.forms.contains_key("tenant-connection-secret"));
    assert!(!reg.caller.contains_key("tenant-connection-secret"));
    assert!(
        reg.compile()
            .patterns
            .iter()
            .any(|p| p == "tenant-connection-secret")
    );
    reg.pin(forms_of("server-pinned-token"));
    reg.protect(forms_of("server-pinned-token"));
    assert!(!reg.protected.contains("server-pinned-token"));
}

#[test]
#[serial]
fn a_rotated_protected_secret_is_dropped() {
    register_protected("rotating-refresh-token-1");
    register_protected("abc");
    assert_eq!(redact("rt=rotating-refresh-token-1"), "rt=***");
    register_protected("rotating-refresh-token-2");
    unregister("rotating-refresh-token-1");
    assert_eq!(
        redact("rotating-refresh-token-1 rotating-refresh-token-2"),
        "rotating-refresh-token-1 ***"
    );
    unregister("rotating-refresh-token-2");
}

#[tokio::test]
#[serial]
async fn values_the_server_resolves_stay_with_the_servers_inside_a_caller_scope() {
    as_caller(async {
        register_server("server-resolved-in-caller-load");
        register("caller-supplied-in-caller-load");
        for i in 0..MAX_CALLER_FORMS + 10 {
            register(&format!("caller-flood-after-{i:06}"));
        }
    })
    .await;
    let reg = registry().read().unwrap();
    assert!(reg.forms.contains_key("server-resolved-in-caller-load"));
    assert!(!reg.caller.contains_key("server-resolved-in-caller-load"));
    assert!(!reg.forms.contains_key("caller-supplied-in-caller-load"));
    drop(reg);
    assert_eq!(redact("v=server-resolved-in-caller-load"), "v=***");
}

#[tokio::test]
#[serial]
#[allow(unsafe_code)] // sets a variable no other test reads
async fn an_env_directive_is_the_servers_unless_the_caller_supplied_it() {
    unsafe { std::env::set_var("FAUCET_TEST_SERVER_ENV_SECRET", "server-env-resolved-value") };
    let overlay = crate::interpolate::EnvOverlay::from([(
        "FAUCET_TEST_CALLER_ENV".to_string(),
        "caller-overlay-env-value".to_string(),
    )]);
    as_caller(async {
        crate::interpolate::interpolate_with_env(
            "${env:FAUCET_TEST_SERVER_ENV_SECRET} ${env:FAUCET_TEST_CALLER_ENV}",
            &overlay,
        )
        .unwrap();
    })
    .await;
    unsafe { std::env::remove_var("FAUCET_TEST_SERVER_ENV_SECRET") };
    let reg = registry().read().unwrap();
    assert!(reg.forms.contains_key("server-env-resolved-value"));
    assert!(reg.caller.contains_key("caller-overlay-env-value"));
    assert!(!reg.forms.contains_key("caller-overlay-env-value"));
}
