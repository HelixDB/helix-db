# Index driver failure validation

PR #1078 is integrated with `main` at `1fc01e8b8896354447e8f0c701aae2dedfec6ef2`.
The original PR head is `56c0bc79f019cd81b42bdff35bd8729360e86058`.

## Failure contract

```text
driver error
  discard failed preparation and staged writes
  closed/fenced storage -> return the error
  known permanent error -> recheck exact claim -> persist invariant blocker
                                               -> remove queue pointer
  temporary/unknown error -> recheck exact claim -> queue with backoff
```

Permanent object-store errors include missing immutable data, invalid paths,
unsupported operations, invalid configuration, denied access, and task panics.
Typed causes remain recognizable through error wrappers. Conditional-write
races, cancelled tasks, and temporary I/O retain retries. Opaque provider errors
remain retryable because the object-store API does not expose every wrapped HTTP
status publicly; the classifier does not parse error messages.

The existing operation state and encodings are reused. Explicit retry recreates
the queue pointer. Tests cover build and abort operations, failed staging rollback,
stale claims, closed/fenced writers, and real configuration failures.

Missing manifest pages report `page_sequence`; changed ranges invalidate the
prepared diagnosis. All eight vector-adoption interruption points inject typed
temporary storage errors, including legacy-definition retirement. Production
configuration errors remain permanent.

## Local validation

Use the pinned Rust toolchain with `CARGO_INCREMENTAL=0`,
`CARGO_PROFILE_DEV_DEBUG=0`, and `CARGO_PROFILE_TEST_DEBUG=0`.

- `cargo test --locked --workspace --all-targets`
- `cargo test --locked --workspace --doc`
- `cargo clippy --locked --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`
- `python3 scripts/validate-cargo-target-references.py`
- `cargo test --locked -p db --features production-coverage,migration-parity,index-lifecycle-testing --test production_internal_contracts index_driver_failure_classification_preserves_retry_boundaries -- --exact`
- `cargo test --locked -p db --features production-coverage,migration-parity,index-lifecycle-testing --test production_migration_contracts -- --test-threads=1`
- `cargo llvm-cov --locked -p db --lib --json --output-path coverage.json -- index_lifecycle::`
- `docker build --platform linux/arm64 -t helixdb:index-failure-fixes-local .`
- `docker-image/test.sh --platform linux/arm64 --image helixdb:index-failure-fixes-local`

Workspace validation passes 3,769 tests (13 existing ignored tests) and 258
doctests. Strict all-target workspace Clippy and formatting pass. The external
classifier contract and the eight-boundary adoption recovery regression pass.
Cargo references and managed-index codec boundaries also pass validation.
All 27 production migration contracts pass, including the 64-case vector
materialization and retirement recovery matrix.

Focused LLVM coverage passes 275 lifecycle tests. The new production classifier
covers all 24 executable lines and all 28 regions. The same classifier contract
also passes through the external production-coverage test entry point.

The ARM64 release image suite passes packaging, memory mode, disk persistence and reopen,
query contracts, graceful shutdown, and MinIO-backed persistence and idle refresh.
The all-target Clippy check also required replacing an existing test-only `vec!`
with an array in the pull-value identity test.

Hosted checks and the Linux production-coverage fingerprint must be refreshed
after an authorized push. The focused local coverage report does not replace
that gate. No coverage threshold or baseline was changed.
