# SDK Query Coverage

The parity suite combines coverage tools with explicit contract assertions:

- Rust's fixture visitor classifies every authoritative AST enum variant. A new
  unclassified variant fails compilation, and the JSON-only corpus must contain
  every classified variant.
- All four language SDKs independently construct 233 executable requests and 17
  serialization-only requests. Both Python clients serialize the same request
  types.
- All 233 executable requests run through Rust, TypeScript, Go, synchronous
  Python, and asynchronous Python embedded clients in memory and disk modes.
  Disk Python runs include seeded read-only reopen checkpoints.
- The current-source server executes the same corpus in disk mode for both
  Python clients, with fresh storage per client and a restart checkpoint.
- `run_server_parity.py --image` repeats both Python modes against the published
  image on isolated ports and volumes and records its inspected digest.
- Async embedded and server acceptance checks overlap read and write batches
  with `asyncio.gather(...)`.
- Unit tests cover headers, options, decoding, network failures, timeouts,
  cancellation, deterministic response cleanup, repeated close, post-close
  rejection, and client reuse. Language coverage tools guard their current
  line, branch, function, or statement baselines.

Run the coverage checks from the repository root:

```sh
cd sdks/typescript && npm run test:coverage
PYTHONPATH=sdks/python/src python -m coverage run --branch --source=helixdb -m unittest discover -s sdks/python/tests
cd sdks/go && go test . -coverprofile=coverage.out
CARGO_TARGET_DIR=/tmp/helix-sdk-rust-coverage cargo llvm-cov --manifest-path sdks/rust/Cargo.toml --features embedded --lib
```

`npm run test:parity` remains the end-to-end query and embedded runtime contract.

Run published-image parity from the repository root after generating the Rust
server baseline:

```sh
python sdks/python/scripts/run_server_parity.py \
  --image ghcr.io/helixdb/helixdb:v0.0.3 \
  --baseline-results sdks/tests/parity/results/rust
```
