//! `faucet_serve_template_tests_total`, `faucet_serve_template_label_refusals_total`
//! and `faucet_serve_template_revalidations_total` (#856). Every label is a
//! closed set.

use metrics::{counter, describe_counter};

pub fn describe() {
    describe_counter!(
        "faucet_serve_template_tests_total",
        "Template test-bundle runs recorded, by outcome (pass / fail)"
    );
    describe_counter!(
        "faucet_serve_template_label_refusals_total",
        "Label moves (launch, rollback, channel) refused, by reason (failing / untested / \
         no_tests / stale_evidence)"
    );
    describe_counter!(
        "faucet_serve_template_revalidations_total",
        "Label-holding template versions re-tested by a revalidation, by outcome (pass / fail / \
         error)"
    );
}

pub fn record_test(passed: bool) {
    counter!(
        "faucet_serve_template_tests_total",
        "outcome" => if passed { "pass" } else { "fail" }
    )
    .increment(1);
}

pub fn record_refusal(reason: &'static str) {
    counter!("faucet_serve_template_label_refusals_total", "reason" => reason).increment(1);
}

pub fn record_revalidation(outcome: &'static str) {
    counter!("faucet_serve_template_revalidations_total", "outcome" => outcome).increment(1);
}
