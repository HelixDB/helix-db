# Cross-SDK DSL Parity

This suite proves that every SDK emits the same query JSON, then executes every
runtime fixture through the public embedded Rust, TypeScript, Go, and Python
clients against separate fresh memory and disk databases.

Run from `sdks/typescript/`:

```sh
npm run test:parity
```

The suite does three things:

- `parity:generate` independently writes Rust, TypeScript, Go, and Python
  requests under `tests/parity/generated`.
- `parity:compare-json` structurally compares all 250 requests from all four
  SDKs. This includes integers outside JavaScript's safe range.
- `parity:embedded` generates Python, Node, and Go UniFFI bindings in a fresh
  temporary directory, runs all 233 runtime fixtures through each SDK in memory
  and disk modes, reopens disk readers and writers, compares all results, and
  removes the generated bindings.
- `parity:server-disk` runs the runtime corpus through the real server HTTP API,
  restarts the server against the same disk directory, and verifies persisted
  text search before completing item and whole-index deletion.

Generated bindings and runtime results are test artifacts only. They are never
written into an SDK source tree or committed.

The Rust generator contains an exhaustive visitor whose matches fail to compile
when a new authoritative `helix-ast` variant is not classified. Its coverage
assertions also require every classified variant to appear in the fixture corpus.

The `json-only` bucket covers DSL shapes that must serialize identically but are
not safe or useful to execute. The ordered `runtime` bucket is replayed
sequentially by all four SDKs.
