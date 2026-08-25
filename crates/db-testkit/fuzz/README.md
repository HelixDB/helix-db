# Planner and stateful workload fuzzing

These Cargo Fuzz targets exercise separate semantic boundaries:

- `query_json`: public `QueryRequest` JSON parsing and round-trip stability.
- `planner_context_ast`: arbitrary serialized AST/context pairs plus the finite
  normalized planner domain, including bitmap sets, verified ranges and
  windows, unique/null equality, and late-bound equality cardinality.
- `planner_interpreter`: public planner-to-interpreter execution against one
  process-local empty database across access, filter, set, ordering, window,
  distinct, range, and count cursor boundaries.
- `stateful_action_trace`: replay-trace parsing, lifecycle validation, and
  serialization stability.

Run a target with, for example:

```bash
cargo fuzz run --fuzz-dir crates/db-testkit/fuzz query_json
```

Replay the checked-in cardinality corpora with bounded runs before publishing
planner/executor contract changes. The runner copies the seeds to an isolated
temporary corpus so libFuzzer cannot add generated inputs to the source tree:

```bash
scripts/cardinality-fuzz-corpus.sh
```

Set `FUZZ_RUNS` to override the default 128 bounded executions per target.

Persist every minimized failure beneath the matching `corpus/` directory and
keep it as a deterministic regression seed.
