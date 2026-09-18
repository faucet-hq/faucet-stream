# Golden state-format fixtures (#651 Category G1)

These files are **on-disk state written by a released version of faucet** — a
resumable pipeline's bookmark, an exactly-once envelope, a commit token. They
are frozen inputs, not expected outputs: `compat_state_format.rs` reads them and
asserts the *current* code still understands them.

## Why they exist

A running pipeline's bookmark lives in a state store between runs, so an upgrade
crosses the format boundary in the worst possible place: if release N+1 cannot
read what N wrote, the pipeline silently restarts from the beginning
(re-delivering everything) or from nothing (losing everything). Neither fails
loudly, and a unit test that round-trips through the *current* serializer cannot
catch it, because both sides change together.

## Rules

- **Never edit a file in this directory to make a test pass.** A test failing
  here means the current code stopped reading state a release already wrote —
  the fix belongs in the reader, or in an explicit, documented migration.
- **Add** a new file when a new state shape ships. Name it
  `<shape>-v<version-that-first-wrote-it>.json`.
- Keep each file byte-exact as the writer emitted it, including key order.
