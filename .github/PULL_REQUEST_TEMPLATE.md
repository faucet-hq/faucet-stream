<!-- Thanks for contributing! Keep PRs focused — one logical change per PR. -->

## Summary

<!-- What does this change and why? -->

## Related issue

<!-- Put the closing keyword in the body so GitHub links it: -->
Closes #

## Type of change

- [ ] Bug fix
- [ ] New feature / connector
- [ ] Enhancement to existing capability
- [ ] Docs only
- [ ] Refactor / internal

## Checklist

- [ ] `cargo fmt --all --check` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes
- [ ] `cargo test --workspace --all-features` passes (new behavior has tests)
- [ ] Changed connector / serve / executor I/O runs in an integration test (`tests/`, testcontainer or wiremock) — or the description carries `no-integration-test: <reason>`
- [ ] `cargo doc --workspace --all-features --no-deps` is warning-free
- [ ] Updated the relevant crate `README.md` / root `README.md` / docs site if config, defaults, or behavior changed
- [ ] If adding/removing a connector: updated the umbrella + CLI features and the `feature-check` matrix in `.github/workflows/ci.yml`

## Notes for reviewers

<!-- Anything that needs a closer look, tradeoffs, follow-ups. -->
