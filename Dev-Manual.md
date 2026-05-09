# Developer Manual

This manual documents the day-to-day developer workflow for the HelixDB
repository, with a focus on the harness engineering you need to know to
build, test, and ship changes confidently. New contributors should read it
end-to-end the first time and then use it as a reference.

---

## 1. Repository layout

HelixDB is a Cargo workspace. Members are declared in the top-level
`Cargo.toml`:

| Path | Crate name | Purpose |
|---|---|---|
| `helix-db/` | `helix-db` | The core library: storage engine, gateway, query compiler, **memst integration** |
| `helix-container/` | `helix-container` | Daemon / process boundary that ships HelixDB as a service |
| `helix-cli/` | `helix-cli` | Command-line entry point |
| `helix-macros/` | `helix-macros` | Procedural macros consumed by `helix-db` |
| `metrics/` | `helix-metrics` | Lightweight metrics library |
| `hql-tests/` | `hql-tests` | Black-box query-language tests |

Everything new for memst lives under `helix-db/src/memst/`. See
[`README-MEMST.md`](README-MEMST.md) for module-level details.

---

## 2. Toolchain prerequisites

| Tool | Required version | Notes |
|---|---|---|
| Rust toolchain | latest stable | The crate uses `edition = "2024"`, so you need a recent rustc (>= 1.85). `rustup update stable` is the usual fix. |
| `cargo` | bundled with Rust | |
| C toolchain | system default | LMDB (`heed3`), `mimalloc`, and `blake3` ship a C implementation; on Linux install `build-essential`, on macOS `xcode-select --install`, on Windows MSVC build tools. |
| `git` | any recent | |
| `pkg-config` | recommended on Linux | Some transitive deps probe with `pkg-config`. |

Optional but useful:

- `cargo-nextest` - faster test runner, drop-in replacement for `cargo test`.
- `cargo-watch` - re-run a command on file changes.
- `cargo-deny` / `cargo-audit` - dependency scanning.

Verify your toolchain:

```bash
rustc --version          # should print 1.85.x or newer
cargo --version
```

---

## 3. First-time setup

```bash
git clone https://github.com/yfyang86/helix-db.git
cd helix-db

# Fetch dependencies and confirm the workspace builds
cargo check --workspace

# (Optional) build everything, debug profile
cargo build --workspace
```

Cold builds take a few minutes - `polars`, `axum`, `heed3`, and `mimalloc`
are heavy. Subsequent builds are fast thanks to incremental compilation.

> **Tip.** If you're on a constrained machine (CI runner, laptop on
> battery), build only the crate you're touching:
> `cargo check -p helix-db`.

---

## 4. Cargo features

`helix-db/Cargo.toml` exposes a feature matrix. The relevant ones:

| Feature | Default? | Pulls in |
|---|---|---|
| `compiler` | via `server` | HQL parser (`pest`, `pest_derive`, `ariadne`) |
| `vectors` | via `server` | Cosine similarity + `url` |
| `server` | **yes** | `compiler` + `vectors` + `reqwest` (full daemon) |
| `dev` | no | `debug-output`, `server`, `bench` |
| `bench` | no | `polars` for benchmarking |
| `production` | no | `api-key` + `server` |
| `debug-output` | no | Verbose `helix-macros` output |
| `dev-instance` | no | Local-developer instance flag |

Common build invocations:

```bash
# default profile (server enabled)
cargo build -p helix-db

# everything (compiler + vectors + server + bench)
cargo build -p helix-db --features dev

# minimal: storage core only, no compiler / no HTTP layer
cargo build -p helix-db --no-default-features

# release-tuned binary
cargo build -p helix-db --release
```

The `[profile.release]` section of the workspace Cargo.toml uses
`opt-level=2`, `lto=true`, `codegen-units=1`, `panic=abort`. That gives
small fast binaries but slow builds - keep iteration on the dev profile.

---

## 5. Running tests

### 5.1 Test taxonomy

There are four kinds of tests in this repo and they are run differently:

1. **Inline unit tests** - `#[cfg(test)] mod tests` inside source files.
   Run by default with `cargo test`.
2. **Integration tests** - files under `helix-db/tests/`. Each file is
   compiled into its own binary, so they cannot share state but they can
   reach the public crate API.
3. **HQL tests** - under `hql-tests/`, exercise the query language end-to-end.
4. **Doc tests** - anything inside ```rust``` blocks in module docs.

The memst integration adds three integration test files:

- `helix-db/tests/memst_unit.rs` - fine-grained unit tests
- `helix-db/tests/memst_uat.rs` - Given/When/Then UATs
- `helix-db/tests/memst_uat_multisession.rs` - multi-session UAT

### 5.2 Environment variables

Set these before invoking `cargo test`. None are required, but each unlocks
something useful when you need it:

| Variable | Effect |
|---|---|
| `RUST_BACKTRACE=1` | Prints a stack trace on panic. Set this first when debugging a failing test. |
| `RUST_BACKTRACE=full` | Verbose backtrace including symbols. |
| `RUST_LOG=debug` | Enables `tracing` logs (HelixDB uses the `tracing` crate). Filter by crate, e.g. `RUST_LOG=helix_db::memst=debug`. |
| `RUST_TEST_THREADS=1` | Force serial execution. Use this when you suspect a race in test fixtures. |
| `CARGO_TARGET_DIR=/tmp/helix-target` | Move the build cache off slow disks (CI / NFS / Docker bind mounts). Speeds builds dramatically. |
| `RUSTFLAGS="-D warnings"` | Treat warnings as errors. Mirrors what CI does - recommended on PRs. |
| `RUST_MIN_STACK=8388608` | Bump stack size if a test recurses deeply (rare). |

Recommended baseline for local development:

```bash
export RUST_BACKTRACE=1
export RUST_LOG=warn
export CARGO_TARGET_DIR="$HOME/.cache/helix-target"
```

Add those to your shell rc once and forget about them.

### 5.3 Common test commands

```bash
# Run every test in every workspace member (this is what CI does)
cargo test --workspace

# Run only helix-db tests
cargo test -p helix-db

# Run only the memst tests
cargo test -p helix-db --test memst_unit
cargo test -p helix-db --test memst_uat
cargo test -p helix-db --test memst_uat_multisession

# Run a single test by name
cargo test -p helix-db --test memst_uat uat_18_session_persistence_across_reopen

# Run only the inline unit tests inside the crate (lib tests, not integration)
cargo test -p helix-db --lib

# Run with output captured to stdout (useful for println! debugging)
cargo test -p helix-db -- --nocapture

# Run serially - fixes spurious failures in shared-state tests
cargo test -p helix-db -- --test-threads=1
```

### 5.4 LMDB / `serial_test` gotcha

HelixDB's storage engine sits on top of `heed3` (LMDB). Several stress
tests are annotated with `#[serial]` from the `serial_test` crate to
prevent two LMDB environments from racing on the same temp path. If you
add a new test that touches `helix_engine::storage_core`, follow the
existing pattern:

```rust
use serial_test::serial;

#[test]
#[serial]
fn my_lmdb_test() { /* ... */ }
```

If you forget the attribute and the test passes locally but flakes on
CI, this is almost always the cause.

### 5.5 memst-specific harness notes

The memst integration uses two filesystem features that need attention:

1. **Exclusive file lock (`fs2`).** `SessionStore::init` and
   `SessionStore::open` take an `flock`-style exclusive lock on
   `<base>/store.lock`. Two `SessionStore` instances on the **same** path
   will deadlock - always drop the first one before re-opening. The
   provided integration tests do this with a scoped block:

   ```rust
   let (alice_id, bob_id) = {
       let store = SessionStore::init(dir.path())?;
       // ... write data ...
       (alice_id, bob_id)
   };
   let store = SessionStore::open(dir.path())?;  // lock released, reopen
   ```

2. **TempDir cleanup.** All memst tests use `tempfile::TempDir`, which
   removes the directory when the value goes out of scope. Do **not**
   `mem::forget` a TempDir - the lock file plus the `objects/` tree will
   linger. If you see leftover directories under `$TMPDIR`, look for a
   panicking test that bypassed Drop.

### 5.6 Doc tests

Doc tests are slow and easy to break. Run them only when you've changed
docs:

```bash
cargo test -p helix-db --doc
```

If you add a runnable example to a doc comment, gate it with `no_run` or
`ignore` if it relies on filesystem fixtures.

### 5.7 Faster local cycles with nextest

```bash
cargo install cargo-nextest --locked
cargo nextest run -p helix-db
```

Nextest fans out test binaries across cores aggressively and isolates
panics. The downsides: it doesn't run doc tests, and `--test-threads=1`
must become `--test-threads 1` (no equals sign).

---

## 6. Running the example demo

The memst integration ships a runnable demo simulating three users:

```bash
cargo run -p helix-db --example memst_multi_session
```

It writes to a `TempDir`, prints a per-session summary, demonstrates
budget-driven compaction, and verifies persistence by re-opening the
store. Use it as a sanity check after touching `memst/store.rs` or
`memst/memory.rs`.

---

## 7. Linting and formatting

A pre-flight script lives at the repo root:

```bash
./clippy_check.sh
```

It runs `cargo clippy` with the project's chosen lint level. Run it
before opening a PR - CI runs the same script.

Format with rustfmt:

```bash
cargo fmt --all
cargo fmt --all --check     # CI mode: fail if any file is unformatted
```

Recommended workflow per change:

```bash
cargo fmt --all
./clippy_check.sh
cargo test -p helix-db
```

If you treat warnings as errors locally, set this once:

```bash
export RUSTFLAGS="-D warnings"
```

---

## 8. Tracing and runtime logs

HelixDB uses the `tracing` crate. Enable logs with `RUST_LOG`:

```bash
# Top-level info
RUST_LOG=info cargo run -p helix-db --example memst_multi_session

# Drill into a specific module
RUST_LOG=helix_db::memst::store=debug cargo test -p helix-db --test memst_uat -- --nocapture

# Wide spread, useful when reproducing a CI failure
RUST_LOG=helix_db=trace,heed3=info cargo test -p helix-db
```

`tracing-subscriber` is set up by `helix-container`; library tests do not
configure a subscriber by default, so `info!()` calls in `helix-db` are
silent unless your test installs one. The simplest pattern:

```rust
let _ = tracing_subscriber::fmt::try_init();
```

at the top of a test gives you formatted log output.

---

## 9. Working on the memst module

`helix-db/src/memst/` is organized so each file owns one concern:

```
mod.rs       - re-exports (the `pub use` block is the public API contract)
error.rs     - Error / Result
types.rs     - Message, MemoryItem, MemoryTier, ...
objects.rs   - ObjectId, Blob/Tree/Commit/Tag, ObjectStore (BLAKE3-backed)
memory.rs    - MemoryLifecycle, checkpoints, token counters
store.rs     - SessionStore (file/session management)
```

When adding a new public type:

1. Define it in the most specific module (e.g. a new lifecycle trigger
   goes in `memory.rs`).
2. Add it to the `pub use` block in `mod.rs` so downstream users can
   reach it via `helix_db::memst::NewType`.
3. Write at least:
   - one inline unit test next to the type (`#[cfg(test)] mod tests`),
   - one fine-grained edge-case test in `tests/memst_unit.rs`,
   - and if the type changes a user-visible behaviour, one BDD-style
     scenario in `tests/memst_uat.rs`.

If your change affects the on-disk layout, **bump the schema version**:

```rust
// in store.rs
const SCHEMA_VERSION: &str = "1.1.0";  // was 1.0.0
```

...and write a migration path (or, at minimum, document in
`README-MEMST.md` that an old store will refuse to open).

---

## 10. Branching, commits, and PRs

- Develop on a feature branch named `claude/<short-topic>-<token>` (the
  Claude Code harness uses this convention; manual branches can be
  anything sensible).
- Keep commits small and self-contained. Conventional commits are
  preferred but not enforced.
- Before pushing, the local checklist:

  ```bash
  cargo fmt --all
  ./clippy_check.sh
  cargo test -p helix-db
  ```

- Push with `git push -u origin <branch>`. The Claude Code harness
  retries up to four times with exponential backoff on transient network
  errors (2s / 4s / 8s / 16s); the same approach is fine for humans.
- Open a PR via the GitHub UI or `gh pr create`. The PR for the memst
  branch is **#1**.
- Reviewers expect: changed-file scope <= ~500 LOC of net change; tests
  covering new behaviour; CI green.

---

## 11. Continuous Integration

The repo's CI (under `.github/workflows/`) runs the same commands listed
above:

1. `cargo fmt --all --check`
2. `./clippy_check.sh`
3. `cargo test --workspace`
4. (Optional, on release tags) `cargo build --release`

Common CI flake patterns and how to handle them:

| Symptom | Likely cause | Fix |
|---|---|---|
| LMDB test passes locally, flakes in CI | Missing `#[serial]` | Add `serial_test::serial` |
| `error[E0308]` on stable but not nightly | Edition-2024-only construct | Stick to stable Rust 1.85+ syntax |
| `linker not found` | C toolchain missing | Install build essentials in the runner |
| Out-of-memory on the bench job | `polars` is heavy with all features | Drop `bench` from the failing job |

---

## 12. Releases (high-level)

The crate version lives in `helix-db/Cargo.toml`'s `[package].version`.
For a release:

1. Update the version: `1.3.3` -> `1.3.4` (or `1.4.0` if the public API
   changed).
2. Update `CONTRIBUTORS.md` and any user-facing docs.
3. Tag: `git tag v1.3.4 && git push --tags`.
4. CI builds release binaries; manually trigger `cargo publish` only if
   the crate is being published.

> **Don't** run `cargo publish` from a developer machine without
> explicit approval - once published, a version cannot be replaced, only
> yanked.

---

## 13. Troubleshooting cookbook

**Build fails with `failed to load source for dependency 'heed3'`.**
Network or proxy. Try `cargo fetch --locked` and check `~/.cargo/config`
for stale `[source.crates-io]` entries.

**`cargo test` hangs forever.** Almost always two `SessionStore`s on the
same directory deadlocking on the lock file. Add `RUST_LOG=trace`,
re-run, and look for the test that initialized but didn't drop the
store before re-opening. The fix is the scoped-block pattern in 5.5.

**Test panics with `Failed to acquire lock: ...`.** Same root cause as
above, but the second instance gave up immediately because some other
process held the lock. Check for orphaned test processes:
`pgrep -af helix-db`.

**Linker error `undefined symbol: rust_eh_personality`.** Mismatch
between profile `panic = "abort"` and a dependency built with `unwind`.
`cargo clean -p <dep>` and rebuild.

**`stack overflow` in a memst test.** A `MemoryItem` content blob got
too large. The bincode-serialized message is read back as a single
allocation; cap test inputs at a few MB.

**`error: linking with cc failed: exit code: 1` on macOS.** Xcode CLI
tools missing. `xcode-select --install`.

**`bincode::Error: io error: unexpected end of file`.** A `messages.bin`
or `*.bin` tier file is truncated. The tier reader handles this as
end-of-stream, but if you see it inside a deserialize call, the file
was corrupted by an unfinished write - investigate process kills, disk
full, or buggy custom code that bypassed `OpenOptions::append(true)`.

**Compile error referring to `serde_json` while building helix-db.** You
removed the dep from `helix-db/Cargo.toml`. The memst module depends on
`serde_json::Value` (operation payloads), so it must remain in the
`[dependencies]` block.

---

## 14. Quick reference

```bash
# Setup once
rustup update stable
cargo check --workspace
export RUST_BACKTRACE=1

# Daily loop
cargo fmt --all
cargo test -p helix-db
./clippy_check.sh

# Memst-specific
cargo test -p helix-db --test memst_unit
cargo test -p helix-db --test memst_uat
cargo test -p helix-db --test memst_uat_multisession
cargo run  -p helix-db --example memst_multi_session

# Drill into one failing test with logs
RUST_LOG=helix_db::memst=debug \
RUST_BACKTRACE=full \
cargo test -p helix-db --test memst_uat <test_name> -- --nocapture --test-threads=1
```

If something here is wrong or out of date, please open a PR updating
this file alongside the change.
