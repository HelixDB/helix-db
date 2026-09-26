# Reviewing query-plan regressions

`cargo test -p helix-cypher --test plans` checks 810 fixed cases through the
production planner. The suite covers both frontends: 50 Cypher query families
across small/large populations, index availability and three storage profiles;
five skewed graph families with normal/exhausted optimizer budgets; and 12
native planner fixture families at three scales under each storage profile.
Another 72 native cases exercise unique/nonunique membership and shared
conjunctions around disjunctions at 1, 2, 64 and 65 values, crossing batch and
union-planning boundaries.

The Cypher cases cover scans, equality/unique/range access, intersections,
disjunctions, chains, stars, cycles, selective endpoints, repeated bindings,
correlated lookups, hash joins, products, optional matches, paths, grouping,
distinct, ordering, top-k, windows, scope changes and mutations. Native cases
also cover root reuse, branching, nested execution, search, index DDL and wide
Boolean predicates. Existing semantic/model tests remain separate correctness
gates; these signatures do not establish query-result correctness.

Every case signs its complete input and selected plan, including expressions,
properties, trace decisions, blocking operators, windows, cost estimates and
deterministic optimizer work. The compact checked-in manifest also exposes root
costs and work counters. Only measured optimizer duration is removed at the
known planner-metrics boundaries. User data with the same field name and the
configured optimizer time budget remain intact. New fields require review.

The capture also includes derived runtime contracts that production plan
serialization intentionally omits: execution stages and concurrency policies,
pull-region members and terminals, absorbed producers, per-operator demand
capabilities, and executable return shapes. Nested branches, repeats and foreach
bodies carry their own schedules. Cypher captures include compact cell width,
remapped operators, returns, windows, consumer proofs and graph bindings. These
checks expose extra buffering or lost cell reuse even when serialized DAG steps
stay unchanged. This is test metadata; it does not change the public plan or
storage format.

A mismatch fails and writes the full candidate under `target/planner-regression`.
There is no automatic baseline acceptance, including for apparently cheaper
plans. Removed cases, changed queries/catalogs, changed rejection details and
missing resources cannot silently become passes.

## Capture and compare

Keep complete captures locally before changing planner code. The example refuses
to overwrite existing files. It records queries, parameters, catalog statistics,
storage settings and optimizer limits so the candidate can replay identical
inputs even if its defaults change.

```sh
cargo run -p helix-cypher --example planner_snapshots -- capture target/plans-before.json
cargo run -p helix-cypher --example planner_snapshots -- replay target/plans-before.json target/plans-after.json
python3 scripts/compare-planner-plans.py target/plans-before.json target/plans-after.json target/plan-diffs
```

Run capture on the baseline revision and replay on the candidate revision.
An unchanged replay must compare cleanly before trusting a comparison across
revisions. The comparator writes a complete unified diff per changed case and
separate cost/work deltas. Any change exits unsuccessfully until reviewed.
Captures and reports contain no authored session timestamps.

After generating the full SDK parity corpus, include all request fixtures:

```sh
cargo run -p helix-cypher --example planner_snapshots -- capture target/plans-with-sdk.json sdks/tests/parity/generated/rust 248
```

This adds 1,488 cases, for 2,298 total: each request runs with the search
indexes from SDK setup fixture 024 both absent and present. Missing-index
rejections therefore remain tested alongside executable vector/text plans. The explicit expected fixture count is a
denominator check. Review corpus changes before changing that argument. Rejected
native fixtures stay in the capture with their exact planner error.

## Storage latency and runtime verification

The default profile preserves production defaults. The two cold profiles charge
50 ms and 200 ms for point reads, batch setup, range seeks and each authoritative
property verification. These are sensitivity scenarios, not measurements or
predictions of cache behavior. They deliberately expose plans that rely on many
serial verification reads. The profile retains per-key CPU costs and batch-width
limits. A lower estimated cost alone does not establish better performance.

For each changed access path or execution schedule, inspect selected indexes,
direction, join order, estimated rows, batching, blocking state, predicate and
optional boundaries, and limit placement. Check actual results and storage work
against the previous implementation. Existing local-disk runtime measurements
are available with:

```sh
cargo run --release -p db --example cypher_scaling > target/cypher-scaling.json
```

That executable asserts result/resource contracts and records logical reads,
foreground object-store reads, query admission peaks, planner work and elapsed
time separately. Keep the same cases and sizes in before/after comparisons.
Logical KV reads are not necessarily remote requests, and admission peaks do not
include all native buffers or allocator overhead. Timing requires repeated runs
under comparable cache and storage conditions; do not infer a latency improvement
from a single noisy local run.

Only after complete diff review, semantic tests and runtime verification should
the reviewed manifest be edited. A candidate manifest can be exported locally:

```sh
cargo run -p helix-cypher --example planner_snapshots -- manifest target/plans-after.json target/plans-manifest.json
```

The fixed gate's manifest must contain exactly its 810 cases; SDK captures are a
separate full-corpus review. Changes to the cost model can increase estimates
without increasing runtime work. Document that distinction and the measured
evidence instead of weakening the gate or accepting every increase together.
