# Database decoder fuzz targets

These Cargo Fuzz targets exercise the canonical `encoding/v2` decoders through the
feature-gated `db::fuzzing` byte-slice boundary. They never construct alternate
persisted DTOs or emit database rows.

Run each target from the repository root with nightly Rust:

```text
cargo +nightly fuzz run --fuzz-dir crates/db/fuzz current_secondary_records
cargo +nightly fuzz run --fuzz-dir crates/db/fuzz current_search_records
cargo +nightly fuzz run --fuzz-dir crates/db/fuzz current_index_v2_keys
cargo +nightly fuzz run --fuzz-dir crates/db/fuzz current_index_v2_records
cargo +nightly fuzz run --fuzz-dir crates/db/fuzz current_index_v2_work
```

The checked-in corpus contains valid deployed secondary range, text
manifest/live-state/version, vector, V2 scoped/global key, canonical record,
outbox records. V2 corpus fixtures use a
reviewable `hex:` envelope that the harness converts to the exact persisted
bytes before calling the production decoder. Arbitrary libFuzzer inputs remain
raw. Any input that previously caused a panic or invalid successful decode
belongs in the matching corpus directory as a permanent regression seed.
