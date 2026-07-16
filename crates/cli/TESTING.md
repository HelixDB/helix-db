# CLI Testing

The CLI suite is layered so fast pull-request jobs cover behavior without live services, while scheduled Ubuntu jobs exercise the real container runtime.

## Test layers

| Layer | Location | Contract |
| --- | --- | --- |
| Unit | `src/**` | Parsing, typed state transitions, config validation, rendering, path safety, and command argument construction. |
| Mocked service | `tests/api_contracts.rs`, `tests/enterprise_api.rs` | HTTP and SSE methods, paths, headers, bodies, decoding, happy paths, and service errors through local `wiremock` servers. |
| Binary integration | `tests/*_commands.rs`, `tests/command_surface.rs`, `tests/e2e_cli.rs`, `tests/sync_reconciliation.rs`, `tests/typescript_runtime.rs` | The compiled `helix` executable, every command/subcommand help surface, flags, aliases, state files, subprocess arguments, and non-interactive errors. |
| Runtime integration | `tests/e2e_runtime.rs` | Real Docker lifecycle, query write/read behavior, restart, logs, stop/prune, and disk persistence. These tests are ignored by default. |

All tests isolate `HOME`, `HELIX_HOME`, the CLI cache, credentials, metrics, runtime logs, and host actions. Network tests use loopback mock servers. They must not call Helix Cloud, GitHub, the metrics service, npm, or a browser.

## Local commands

Run the normal suite:

```bash
cargo test --locked -p helix-cli --all-targets
cargo test --locked -p helix-cli --doc
cargo clippy --locked -p helix-cli --all-targets -- -D warnings
cargo fmt -p helix-cli -- --check
```

Run the same line-coverage gate used by CI:

```bash
cargo llvm-cov --locked -p helix-cli --all-targets \
  --ignore-filename-regex '(crates/cli/tests/|crates/cli/src/prompts.rs|crates/cli/src/commands/update.rs)' \
  --fail-under-lines 80
```

The gate excludes two host-bound adapters:

- `prompts.rs` requires a real TTY and is covered through non-interactive caller behavior plus manual terminal checks.
- `commands/update.rs` replaces the running executable. Tests use the typed host-action update contract instead of mutating the developer or CI binary.

Their callers, state handling, update checks, caches, output, and failure paths remain in the coverage calculation. The current threshold is a floor and must not be lowered to accommodate new code.

Run the real Docker tests sequentially:

```bash
cargo test --locked -p helix-cli --test e2e_runtime -- --ignored --test-threads=1
```

## Test contracts

The integration fixture exposes process-scoped environment contracts:

| Variable | Purpose |
| --- | --- |
| `HELIX_TEST_HTTP_BASE_URL` | Routes Cloud, release, skills, and metrics requests to one loopback server. |
| `HELIX_TEST_TOOL_DIR` | Resolves typed external tools to fixture scripts on Unix and Windows. |
| `HELIX_TEST_CONTAINER_RUNTIME_BIN` | Resolves Docker/Podman operations to the fixture runtime. |
| `HELIX_TEST_HOST_ACTION_LOG` | Records browser and updater actions as JSON lines. |
| `HELIX_TEST_CHEF_PERMISSION_MODE` | Selects a typed chef permission result for headless agent tests. |

Keep overrides on the spawned `helix` process. Do not mutate process-global environment variables inside parallel Rust tests.

## Adding coverage

For each new command or flag:

1. Add parser/unit coverage for valid and conflicting combinations.
2. Add an actual-binary happy path and a user-visible error path.
3. Mock every external request and assert its method, path, auth, body, and response decoding.
4. Cover both Unix executable scripts and Windows `.cmd` argument forwarding through the shared fixture.
5. Add a real-runtime test only when behavior depends on Docker state, persistence, or the gateway process.
6. Keep doc examples runnable with `cargo test -p helix-cli --doc`.
