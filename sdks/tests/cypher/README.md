# Cypher SDK runtime contract

`runtime.json` contains ten ordered cases, with independently authored expected
results. Each SDK executes the same query text and parameters through its public
Cypher method. Drivers record actual results and structured errors; they never
retry mutations or substitute an expected response.

The cases cover nested lossless integers, nonfinite floats, escaped maps,
Unicode, node/relationship/path values, creation, rollback after a failed plain
node deletion, map/property updates, persistence, and detach deletion. A read after the disk-reopen boundary exercises
DISTINCT followed by an optional named-path MATCH and a top-k return, including
duplicated keys, unmatched keys and nulls. Graph IDs
are compared using fixture identities only after checking canonical unsigned
decimal encoding, consistent repeated identities, distinct element identities,
relationship endpoints, and path direction. Node and relationship ID namespaces
are independent. Other values, column order, row order, and duplicates are
compared exactly. Error category, phase, and detail must all match; message text
alone cannot satisfy a negative case.

Rust, TypeScript, Go, synchronous Python and asynchronous Python each run the
corpus against in-memory embedded storage, disk embedded storage and the local
HTTP server. The `persistent-read` boundary closes the disk writer and opens a
reader for embedded clients. The HTTP parent stops and restarts the disk server
at the same boundary. Missing cases, changed case order, a missing restart
boundary, missing/extra result files, malformed results and individual mismatches
fail the gate. Changes to expectations require explicit review; drivers never
update the corpus.

Run the existing SDK parity command from `sdks/typescript`:

```sh
HELIX_TELEMETRY_LEVEL=off HELIX_NO_UPDATE_CHECK=1 npm run test:parity
```

The command retains the separate 248-request and 233-runtime DSL denominators.
Set `HELIX_PYTHON` to a supported Python executable when the system default is
older than the SDK requires. Native build tools and language dependencies must
already be available. All servers bind to loopback; temporary databases and
bindings are removed on completion or failure. No reports are published.

Set `HELIX_PARITY_PROGRESS=1` for live command and TypeScript fixture/reopen/close
diagnostics on stderr. These messages stay separate from result JSON and do not
change the fixture set or subprocess timeouts.

The common comparator is `sdks/typescript/scripts/parity/cypher-results.ts`. Its
independent boundary tests run in the TypeScript unit suite. The openCypher TCK
is a separate production-service conformance gate; SDK parity does not replace
its full denominator or either conformance manifest.
