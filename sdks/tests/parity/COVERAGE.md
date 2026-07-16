# SDK Query Coverage

The parity suite combines coverage tools with explicit contract assertions:

- Rust's fixture visitor classifies every authoritative AST enum variant. A new
  unclassified variant fails compilation, and the JSON-only corpus must contain
  every classified variant.
- All four SDKs independently construct 224 executable requests and 15
  serialization-only requests.
- All 224 executable requests run through every public embedded client and must
  produce structurally equal results.
- Unit tests cover server and embedded success/error states. Language coverage
  tools guard their current line, branch, function, or statement baselines.

Run the coverage checks from the repository root:

```sh
cd sdks/typescript && npm run test:coverage
PYTHONPATH=sdks/python/src python -m coverage run --branch --source=helixdb -m unittest discover -s sdks/python/tests
cd sdks/go && go test . -coverprofile=coverage.out
CARGO_TARGET_DIR=/tmp/helix-sdk-rust-coverage cargo llvm-cov --manifest-path sdks/rust/Cargo.toml --features embedded --lib
```

`npm run test:parity` remains the end-to-end query and embedded runtime contract.
