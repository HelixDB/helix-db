# DB test-target inventory

This inventory is the discovery and retirement evidence for HEL-715 and
`docs/vector_search_review.md`. It distinguishes tests Cargo executes from
legacy sources that were removed only after their contracts had named current
replacements.

## Cargo-discovered targets

The authoritative command is:

```bash
cargo metadata --no-deps --format-version 1 \
  | jq -r '.packages[] | select(.name == "db") | .targets[] | [.name, (.kind | join(",")), .src_path] | @tsv'
```

The final inventory reports exactly:

| Target | Kind | Source | Production-only coverage role |
|---|---|---|---|
| `db` | `lib` | `crates/db/src/lib.rs` | Unit-test behavior only. Its coverage report includes inline `#[cfg(test)]` code and is never used for production-only thresholds. |
| `embedded_write_latency` | `bench` | `crates/db/benches/embedded_write_latency.rs` | Measures acknowledged-write latency for in-memory and on-disk embedded storage. |
| `encoding_only` | `test` | `crates/db/tests/encoding_only.rs` | Unit-style encoding target. It includes `src/encoding` by path under a test crate and bridges production DTO modules rather than copying them, so it is deliberately excluded from production-only coverage. |
| `fts_prefilter` | `bench` | `crates/db/benches/fts_prefilter.rs` | Measures exact collector-only FTS prefilter latency, allocations, and object-store reads across release fixtures. |
| `index_lifecycle_contracts` | `test` | `crates/db/tests/index_lifecycle_contracts.rs` | Requires `index-lifecycle-testing` and runs deterministic family-shape, backfill-mutation, concurrent-create, and repeated-recoverable-fault acceptance contracts through the installed production drivers. |
| `production_contracts` | `test` | `crates/db/tests/production_contracts.rs` | Imports the compiled `db` library without `cfg(test)` and covers the public query/interpreter boundary, including graph behavior, source-fed node/edge mutations, all typed KV read modes, explicit comparison/membership shapes, limited and empty access, branch partition and mixed-row invariants, nested branch-context restoration, unbound conditional variables, scalar and operator-specific predicate-error propagation, repeat-predicate and shortest-path missing/ambiguous/revisited/isolated endpoint shapes, nested and edge-endpoint property paths, typed/transport ID parameters and variable ID-source shapes, label mutation/no-op/cascade plus empty/unlabeled deletion invariants, malformed requests, dynamic and typed AST `FOREACH`, complete literal/runtime property-value conversion, stable parallel-stage execution on a public reader, scalar/malformed multi-dependency composition, ordinary/folded/scalar stream-operation shapes, scalar aggregate and aggregate-after-fold shape contracts, typed and rejected dynamic bounds, empty intersection, fail-closed index-lifecycle value consumption, shared-runtime vector/text search and DDL availability, chained injection, sparse/selected/binding projections, vector-distance metadata and search-label projections, typed vector-array conversions through the live planner context, missing-property ordering, mixed/empty aggregates, expression value/error edges, managed secondary/range/search read-your-writes, range-index-delivered order preservation, and same-transaction labeled/unfiltered out/in/both topology reads across parallel differently labeled edges. |
| `projection_materialization` | `bench` | `crates/db/benches/projection_materialization.rs` | Deterministic 512-node/512-edge Divan projection baseline with allocation profiling; setup and planning are excluded from measurement. |
| `production_index_lifecycle_contracts` | `test` | `crates/db/tests/production_index_lifecycle_contracts.rs` | Requires `production-coverage` and runs the V2 outbox transition failpoint matrix, real secondary lifecycle state model, multi-scope discovery, compact typed model/resource boundaries, fail-closed Active text serving reads, and state-only Active text retirement against the compiled production library. |
| `production_index_lifecycle_scale` | `test` | `crates/db/tests/production_index_lifecycle_scale.rs` | Requires `production-scale`; non-ignored fixed 100k production-entry builds for non-unique/unique secondary, 128D f32 vector, paged text, and a 100k workload distributed across 16 tenant scopes, followed by public search oracles and lifecycle cleanup. A fixed 8k vector create/search/drop/residue contract runs independently in bounded nightly CI. Separate maximum-batch contracts cover typed limit blocking/reopen/retry/abort for every family. |
| `production_internal_contracts` | `test` | `crates/db/tests/production_internal_contracts.rs` | Requires `production-coverage` and invokes feature-gated production module contracts without compiling inline unit-test code into the measured library, including vector storage/search/lifecycle boundaries, native and forced-scalar vector magnitude safety, ambiguous Active-text graph-commit recovery, and exact request-read-view fail-closed guards. |
| `production_migration_contracts` | `test` | `crates/db/tests/production_migration_contracts.rs` | Requires `production-coverage` and covers legacy definition convergence, populated-vector adoption, malformed and ineligible vector lanes, ownership conflicts, invalid V2 bootstrap tuples, failure-preserving resume, and active/conflicting definition handling through production migration boundaries. |
| `production_row_mode_contract` | `test` | `crates/db/tests/production_row_mode_contract.rs` | Launches process-isolated public queries with valid and invalid `HELIX_ROW_MODE_MAX_ROWS` values, covering production environment parsing, cached cap activation, exact overflow errors, and malformed-setting rejection without mutating a multithreaded test process's environment. |
| `production_text_correctness_regressions` | `test` | `crates/db/tests/production_text_correctness_regressions.rs` | Runs bounded text correctness regressions through the compiled production library. |
| `production_text_lifecycle` | `test` | `crates/db/tests/production_text_lifecycle.rs` | Requires `production-coverage` and runs public text create/backfill, insert/update/delete, search, reopen, drop/recreate, typed blocked retry, abort, and tenant-partition isolation against an independent model. Its internal observer cross-checks manifest roots/pages, builder evidence, entity state, and metadata-only terminal cleanup; public tenant contracts cover literal/expression selection, fail-closed tenant-shape errors, and atomic partition moves/removals/reinsertions. |
| `production_vector_planner` | `test` | `crates/db/tests/production_vector_planner.rs` | Exercises managed DDL, planner publication, node/edge mutation maintenance, reopen, every active f32 metric, tenant-partition isolation, and exact magnitude rejection/rollback through public executable plans. Its closed lifecycle state machine adds brute-force search comparison, drop/recreate, query-planned status/retry/abort control over a typed blocked build and cleanup, runtime literal/expression tenant selection, fail-closed tenant-shape errors, and atomic partition moves/removals/reinsertions. |
| `secondary_lifecycle_public_step_contract` | `test` | `crates/db/tests/secondary_lifecycle_public_step_contract.rs` | Checks the public bounded-step lifecycle boundary. |
| `text_correctness_support` | `test` | `crates/db/tests/text_correctness_support.rs` | Checks shared text correctness support independently. |
| `writer_fence_contract` | `test` | `crates/db/tests/writer_fence_contract.rs` | Proves a newer SlateDB writer claims its epoch before open returns and an already-open transaction from the old writer is rejected as fenced. |
| `embedded_write_latency` | `bench` | `crates/db/benches/embedded_write_latency.rs` | Measures fixed in-memory and disk embedded write latency through the public client boundary. |
| `fts_prefilter` | `bench` | `crates/db/benches/fts_prefilter.rs` | Measures exact traversal-scoped full-text prefiltering against the production text lifecycle. |
| `secondary_equality_hot_path` | `bench` | `crates/db/benches/secondary_equality_hot_path.rs` | Measures 50-index V4 equality write and read throughput, latency, and allocations. |
| `secondary_equality_read_scale` | `bench` | `crates/db/benches/secondary_equality_read_scale.rs` | Measures equality lookup cost over the 10,000-node shared-value fixture. |
| `text_transaction_batching` | `bench` | `crates/db/benches/text_transaction_batching.rs` | Measures text transaction batching. |
| `vector_batch_insert` | `bench` | `crates/db/benches/vector_batch_insert.rs` | Measures vector batch insertion. |
| `write_path_mutations` | `bench` | `crates/db/benches/write_path_mutations.rs` | Measures graph mutation write paths. |

The vector library also owns the ignored, release-only diagnostic
`vector_search_scale_gate_reports_recall_and_median_throughput` contract in
`search/vector/scale_contracts.rs`. It is not a separate Cargo target and is
excluded from production-only coverage. Because it constructs a raw
`VectorIndex` and writes physical rows directly, it is retained only as a
search-kernel regression and does not satisfy a V2 production lifecycle or
primary scale contract. It measures deterministic 10k and 100k current-f32
fixtures, computes exact recall@10, and can enforce supplied same-host baseline
medians as a 95% throughput floor.

The standalone `crates/db/fuzz` workspace adds six non-test Cargo Fuzz
targets: `current_secondary_records`, `current_search_records`,
`current_index_v2_keys`, `current_index_v2_records`, and
`current_index_v2_work`, and `current_index_v2_bitmap`. The V2 targets cover scoped/global physical framing,
canonical catalog/operation/control values and outbox work values. They are
deliberately outside Cargo's test target inventory and call
only the feature-gated byte-slice decoder boundary.

Run `scripts/db-production-coverage.sh` from any directory to discover and run
every `db` integration target whose name starts with `production_`, except the
separate `production-scale` release gate. The naming and feature contracts keep
path-included unit suites and multi-hour scale fixtures out of instrumentation
while all bounded production-linked targets join the baseline automatically.
The runner excludes `tests/`, `benches/`, and `examples/` source files from the
report, prints stable whole-DB and `search/vector` production counts, and deletes
its owned temporary directory on exit.

### Clean Phase 0 coverage baselines

The 2026-07-17 Phase 0 run started from `testing` parent `40c9e6b9`, repaired
the stale benchmark manifest entry, and deleted all prior LLVM coverage data
with `cargo llvm-cov clean --workspace`. The clean workspace command was:

```bash
cargo llvm-cov --workspace --all-targets --json \
  --output-path /private/tmp/helix-proper-testing-phase0-all-targets.json \
  --ignore-filename-regex '(^|/)(tests|benches|examples)/|/(registry|rustc)/'
```

Its production-source LLVM metrics were:

| Scope | Functions | Lines | Regions |
|---|---:|---:|---:|
| Planner | 3,034 / 3,129 (96.9639%) | 27,406 / 28,371 (96.5986%) | 33,156 / 34,684 (95.5945%) |
| DB | 7,460 / 7,981 (93.4720%) | 109,812 / 116,152 (94.5416%) | 144,400 / 154,623 (93.3884%) |
| Interpreter | 1,151 / 1,184 (97.2128%) | 13,376 / 13,931 (96.0161%) | 17,024 / 18,212 (93.4768%) |
| Index V2 | 2,512 / 2,760 (91.0145%) | 53,933 / 57,726 (93.4293%) | 68,039 / 73,320 (92.7973%) |
| Search | 2,154 / 2,345 (91.8550%) | 24,033 / 25,620 (93.8056%) | 34,486 / 36,998 (93.2104%) |

The separate clean `scripts/db-production-coverage.sh` run discovered
`production_contracts`, `production_index_lifecycle_contracts`,
`production_internal_contracts`, `production_text_lifecycle`, and
`production_vector_planner`. It produced the following production-linked line
baselines; zero coverage is recorded rather than hidden where the bounded
integration corpus does not yet enter a component.

| Scope | Covered / instrumented lines | Coverage |
|---|---:|---:|
| Whole DB | 32,118 / 55,616 | 57.7496% |
| Interpreter | 2,822 / 7,622 | 37.0244% |
| Query service | 0 / 238 | 0.0000% |
| Runtime dependencies | 72 / 151 | 47.6821% |
| Index V2 | 14,341 / 23,133 | 61.9937% |
| Secondary lifecycle | 774 / 1,446 | 53.5270% |
| Vector lifecycle | 1,044 / 1,774 | 58.8501% |
| Text lifecycle | 7,525 / 13,039 | 57.7115% |
| Text search | 1,377 / 3,685 | 37.3677% |

The vector gate additionally merged instantiated LLVM lines into unique source
lines, applied 51 justified unreachable exclusions, classified all 104
remaining uncovered lines, and passed its ratchet: 844 / 861 functions
(98.0256%), 5,597 / 5,701 source lines (98.1758%), and 9,503 / 10,000 regions
(95.0300%). The former 2026-07-10 three-test production baseline is historical
and must not be used for regression comparisons.

### Phase 7 production-linked ratchets

The final launch gate retains the same Cargo-discovered bounded DB corpus and
adds explicit covered-line and percentage floors for every critical subsystem.
The current clean 2026-07-20 report after the vector cleanup batching and
captured-write application corrections, query-planned lifecycle controls,
public value conversion,
parallel reader stages, multi-dependency composition, public row-metadata and
aggregate/expression value contracts, managed index/search/topology
read-your-writes, typed `FOREACH`, branch partitions, nested property paths,
typed ID/variable sources, label/mutation tail invariants, explicit
predicate/access shapes, deterministic typed vector cleanup, empty or
unlabeled edge-deletion contracts, and process-isolated row-mode environment
plus barrier/merge dispatch contracts established:

| Scope | Covered / instrumented lines | Coverage |
|---|---:|---:|
| Whole DB | 38,617 / 55,713 | 69.3142% |
| Interpreter | 7,078 / 7,594 | 93.2052% |
| Runtime dependencies | 88 / 171 | 51.4620% |
| Index V2 | 15,036 / 23,134 | 64.9952% |
| Secondary lifecycle | 977 / 1,446 | 67.5657% |
| Vector lifecycle | 1,459 / 2,193 | 66.5299% |
| Text lifecycle | 7,653 / 13,039 | 58.6932% |
| Text search | 1,902 / 3,685 | 51.6147% |

The cleanup correction removed an always-covered three-line branch that had
incorrectly applied the decoded-source-entity limit to physical delete tokens.
The whole-DB, Index V2, and vector absolute covered-line floors moved down by
those same three removed lines; no uncovered production line was added. The
captured-write application correction later removed the second deployed HNSW
execution path: Index V2 and vector lifecycle each lost 18 instrumented lines
and 19 covered lines tied to that duplicate execution, while the replacement
write boundary raised whole-DB coverage by six lines and raised instantiated
`search/vector` coverage by 25 lines. The source-topology-aware Index V2/vector
floors were therefore re-established at the current report rather than holding
an impossible absolute line count from deleted code. The
subsequent query-planned status/retry/abort contract covered all 12 deployed
lines in the previously unexecuted retry and abort interpreter arms. Public
dynamic `FOREACH` contracts then covered every runtime `QueryValue` conversion,
nested array/object storage and projection, non-object item rejection, and
empty-key rejection. A literal property round trip covered every AST property
variant, leaving `stream/values/conversion.rs` at 42 / 42 production lines. In
total these value contracts added 28 covered interpreter lines and 43 covered
whole-DB lines. A validated public-reader DAG then proved two independent
accesses execute as a two-wide parallel stage and publish their shared binding
in stable stage order. It added 79 covered interpreter lines and 149 covered
whole-DB lines, removing 112 unique source lines from the backlog across
scheduling, dependency inputs, stable read views, access dispatch, cache
configuration, reader open/bootstrap, and the Index V2 repository. The
multi-dependency contract then concatenated count, boolean, and scalar outputs
and rejected mixed stream/scalar and folded-stream shapes at the ordinary step
input boundary. It added 24 covered interpreter lines and 28 covered whole-DB
lines, moving `dependencies.rs` from 39 / 73 to 62 / 73 production lines. The
subsequent value-shape tail proved ordinary-stream `unfold`, folded membership
sets, scalar-ID filtering, and exact scalar rejection for membership and
`unfold`. It added 19 covered interpreter and whole-DB lines, exercised two
additional production functions, and removed 13 unique source lines. The
executable-tail contract then defined empty intersection, resolved a typed
AST-bound limit, and passed an already-active `IfNotExists` receipt into
project, aggregate, distinct, all three window operators, and a multi-input
dependency root. It added 25 interpreter, eight Index V2, and 33 whole-DB
covered lines while removing 17 unique source lines. The stream/projection tail
then appended stored streams through chained injection,
rejected folded and lifecycle inputs through the generic stream boundary, and
omitted missing binding, property, and coalesce projections. It added 17
interpreter and whole-DB covered lines while removing 14 unique source lines.
The row-metadata contract then preserved vector-search `$distance` through both
current-row and named-binding projections and defined missing-property
ascending, descending, and tie ordering, including the reverse comparator
direction. It added nine interpreter and whole-DB covered lines and removed
nine unique source lines. The aggregate/expression value contract then covered
mixed numeric coercion, ignored nonnumeric and missing values, all empty
aggregate results, time and null-case projections, and exact numeric/unbound
parameter errors. It added 25 interpreter and 27 whole-DB covered lines,
removed 18 unique source lines, and left numeric expression coercion fully
production-covered. At that point the reviewed non-vector uncovered set was
13,221 lines. The subsequent same-transaction index contract proved
read-your-writes through
managed node equality/range and edge equality/range access. It added 33
interpreter and whole-DB covered lines, moved `access/indexes.rs` from 226 / 328
to 239 / 328 and `access/range.rs` from 201 / 287 to 221 / 287, and removed 23
unique source lines. At that point the reviewed non-vector uncovered set was
13,198 lines. The search read-your-writes contract then proved that text and
vector mutations are immediately visible through their managed generation and
storage paths in the same write transaction. It added 27 interpreter and
whole-DB covered lines,
moved `access/search/generation.rs` from 142 / 225 to 158 / 225 and
`access/search/storage.rs` from 186 / 256 to 197 / 256, and removed 19 unique
source lines. At that point the reviewed non-vector uncovered set was 13,179
lines. The topology read-your-writes contract then created and read nodes and
an edge inside one write transaction through labeled vertex/edge expansion, a
labeled edge scan, endpoint projection, and full node/edge scans. It added 35
interpreter and whole-DB covered lines, moved `access/indexes.rs` from 239 / 328
to 272 / 328 and `storage.rs` from 75 / 118 to 77 / 118, and removed 20 unique
source lines. At that point the reviewed non-vector uncovered set was 13,159
lines. The same contract now also distinguishes parallel differently labeled
edges across labeled and unfiltered out/in/both vertex and edge expansion in
that write transaction. This strengthens the scenario oracle without moving
the combined or production line counters because those operator regions were
already instrumented by less adversarial topology shapes. The typed `FOREACH`
contract then reused one public planner output across typed AST
scalar/item/key failures, verified their
rollback, and executed a valid typed object frame. It added 35 interpreter and
36 whole-DB covered lines, moved `control/foreach.rs` from 68 / 107 to 103 / 107,
and removed 24 unique source lines. At that point the reviewed non-vector
uncovered set was 13,135 lines. The branch-partition contract then exercised
empty, all-passing, and split conditional partitions; optional success/fallback;
exhausted coalescing; and bound/unbound union element invariants. It added 43
interpreter and whole-DB covered lines, moved `control/branch.rs` from 107 / 145
to 145 / 145, and removed 39 unique source lines. The nested property-path
contract added another 30 interpreter and whole-DB covered lines, moved
`stream/eval/property.rs` from 134 / 171 to 164 / 171, and removed 26 unique
source lines. The typed ID-parameter contract covered scalar, typed-array,
generic-array, missing, invalid-member, negative, and invalid-shape AST inputs
plus equivalent malformed query-transport arrays. It added 23 interpreter and
whole-DB covered lines, moved `access/params.rs` from 94 / 140 to 117 / 140,
and removed 11 unique source lines. At that point the reviewed non-vector
uncovered set was 13,059 lines. The variable ID-source contract then covered
ordinary, folded, missing, wrong-element, and scalar-error node/edge sources
through public queries and a validated executable plan. It added 27 interpreter
and whole-DB covered lines, moved `access/params.rs` from 117 / 140 to 140 / 140,
and removed 18 unique source lines. At that point the reviewed non-vector
uncovered set was 13,041 lines. The label-mutation contract then rejected
reserved node/edge creation assignments and removals, rejected edge and invalid
node relabeling exactly, proved rollback, and atomically updated a valid node
label. It added 38 interpreter and whole-DB covered lines, moved
`mutation/node.rs` from 314 / 369 to 349 / 369 and `mutation/contracts.rs` from
74 / 112 to 77 / 112, and removed 27 unique source lines. The reviewed
non-vector uncovered set was then 13,014 lines. The mutation-tail contract
proved same-label and missing-property no-ops before cascading deletion across
incoming, outgoing, and self-loop edges. It added 20 interpreter and 34
whole-DB covered lines, moved `mutation/node.rs` from 349 / 369 to 360 / 369 and
`stream/projection/rows.rs` from 145 / 156 to 153 / 156, and removed 27 unique
source lines. The reviewed non-vector uncovered set is now 12,987 lines; its
digest contains no newly uncovered source line. Public managed vector and text
tenant contracts then proved partition isolation for literal and runtime
expression values while rejecting missing, unscoped, wrong-property, and
tenant-on-unscoped shapes. They added 56 interpreter and 325 whole-DB covered
lines, including 239 Index V2, 125 vector-lifecycle, and 35 text-lifecycle
lines, and removed another 225 unique source lines. The reviewed non-vector
uncovered set was then 12,762 lines. Active tenant moves, removals, and
reinsertions added 76 whole-DB and 75 Index V2 covered lines, including 27 in
the vector lifecycle root and 39 in text lifecycle, and removed 52 unique
source lines. The reviewed non-vector uncovered set was then 12,710 lines. The
vector-lifecycle scope now includes both `index_lifecycle/vector.rs` and its module
directory instead of omitting the root module from its denominator. Public
source-fed node creation and complete edge-output direction contracts then
added 70 covered interpreter lines and 76 whole-DB lines. Direct writer
equality/range lookup parity reduced the combined unit/integration interpreter
gap by another 25 lines. Typed point, ordered multiget, bounded/unbounded range,
and prefix reads then covered 29 of the 30 remaining deployed `access/kv.rs`
lines. Source-fed edge fan-out, per-source assignments, empty-source no-op, and
empty-target rejection covered another 21 deployed interpreter lines. Together
these contracts added 50 interpreter and whole-DB lines and removed 42 unique
source lines from the reviewed backlog. The reviewed non-vector uncovered set
was then 12,610 lines, with no newly uncovered source line. The combined
unit/integration interpreter result remained 14,524 / 14,704 because those
lines already had unit-path evidence; the gain closed missing compiled-
production-boundary evidence. Explicit comparison operators,
generic/string/scalar membership, stored-null and wrong-type string behavior,
plus limited node/edge and empty-edge access then added 42 interpreter and
whole-DB covered lines. They moved `stream/eval/predicate.rs` from 93 / 135 to
117 / 135 production lines and `access/dispatch.rs` from 117 / 132 to 124 / 132,
removing 31 unique source lines and moving the combined result to 14,525 /
14,704 (98.7826%). The reviewed non-vector uncovered set was then 12,579 lines.
An exact 13-row typed cleanup scan made upper-neighbor and upper-vector key
identity coverage deterministic across two identical replays. It added eight
whole-DB lines without changing interpreter coverage and reduced the reviewed
set to 12,571 lines. Public unlabeled edge deletion and missing/empty mutation
contracts then added nine interpreter lines plus one search cleanup line. The
reviewed non-vector uncovered set was then 12,561 lines, with no newly uncovered
source line. These production-boundary paths already had unit evidence, so the
combined all-target result remained 14,525 / 14,704 (98.7826%).

Process-isolated public row-mode contracts then covered cap activation,
successive-operation cache reuse, exact overflow, zero, malformed integers, and
non-Unicode environment values. Public barrier pass-through and canonical
merge operation naming complete the bounded dispatch evidence. This chunk
added 35 production interpreter and whole-DB lines, removed 26 unique source
lines from the reviewed backlog, and moved that backlog to 12,535 lines with
digest `7f98f29c42552480d5bae34d2687ab83c76cbd1ff439a6196dfbda328426c5ed`.
The clean combined result is now 14,530 / 14,704 (98.8166%), leaving 174
instrumented interpreter lines. `row_mode.rs` itself is 139 / 139 production
lines, 16 / 16 functions, and 166 / 166 regions; its combined compilation is
253 / 253 lines.

Public branch plans now reject scalar union, coalesce, optional, and both
`choose_else` partition outputs and propagate unbound branch predicates.
Public repeat emit/until predicates and both shortest-path endpoint positions
also propagate exact unbound-parameter errors. These contracts removed 12
additional unique production source lines from the non-vector backlog, moving
it to 12,523 lines with digest
`526b5f790a4d134d7b744612b6695d46add719605adaf907ceafaf97c507d8a7`.
The instantiated production interpreter metric remains 6,622 / 7,622 because
LLVM's instantiated-line and unique-source-line metrics aggregate segments
differently. Combined tests
additionally distinguish hidden and visible empty rows, exercise key/value
reads through both stable request-view variants, and preserve backend commit
error classification for non-transaction errors and transaction conflicts with
current active generations. The subsequent corruption and nested-error
contracts cover nine previously untouched source lines across shortest path,
dynamic search limits, text maintenance/application, modulo and case
evaluation, and predicate evaluation. The current clean combined result is
14,701 / 14,870 (98.8635%), leaving 169 instrumented interpreter lines; inline
test code expanded the covered denominator while the source-segment backlog
fell from 257 to 248 with no newly uncovered source line. The public dynamic
limit error removes one more deployed source line from the reviewed non-vector
backlog, leaving 12,522 with digest
`b2cdfdbec9b9cd6672b8fd04b6eaa6dd9b9237c515f312b2d7c9572512f2918b`.

The following evaluator, projection, and access error-propagation slice keeps
the clean combined result at 14,701 / 14,870 (98.8635%) while reducing the
finer untouched source-position backlog by another 26 lines, from 248 to 222.
It covers every comparison/string predicate's nested expression failure,
corrupt property reads through null predicates, endpoint paths, expressions,
binding projections, and all property materializers, missing canonical vector
generations for every distance metric, missing canonical text generation,
corrupt managed text roots, and corrupt node/edge equality bitmaps. The
aggregate line counter does not move because these newly reached subregions
share lines LLVM already counted as covered. The remaining 169 instrumented
lines and 222 untouched source positions mean combined interpreter coverage is
still not complete.

The next bounded access-tail slice reduces the untouched source-position
backlog by another ten lines, from 222 to 212. It covers corrupt `$label`
bitmaps; corrupt canonical node equality, node range, vector, and text records;
oversized node and edge range identities; the empty result for an absent
managed text-tenant partition; corrupt text-manifest pages; and typed root/page
split-count disagreement. Six positions also advance LLVM's coarse line
counter, producing 14,707 / 14,870 (98.9038%), with 163 instrumented lines
remaining. This slice changes no production source or production-boundary
target, so the production ratchet remains 6,622 / 7,622 (86.8801%). Combined
interpreter coverage remains incomplete.

The following public production contract executes an unbounded active managed
range access followed by `OrderPlan::RangeIndex` and proves that the
index-delivered node order is preserved without re-sorting. It covers the
deployed all-range conversion, managed unbounded scan, range-order pass-through,
and secondary unbounded-bounds lines. The production report advances by three
interpreter lines and four whole-DB lines to 6,625 / 7,622 (86.9194%) and
37,432 / 55,734 (67.1619%), while the reviewed non-vector backlog falls to
12,518 with digest
`b52af770dbcf597b6d55ff127238d8f10aba2668168b385116d0ad9fd9b25e46`.

The next public typed-`FOREACH` contract rejects an entirely missing parameter
binding before the previously covered invalid scalar, item, and key shapes. It
closes both deployed missing-binding source lines, which LLVM instantiates
twice, advancing the production interpreter and whole-DB reports by four lines
to 6,629 / 7,622 (86.9719%) and 37,436 / 55,734 (67.1691%). The reviewed
non-vector backlog falls to 12,516 with digest
`6309401c9a31cad3a341a82c08a0c5eca0a4a427fbfa3533f3d6027fcb8ebe06`.

The following control-state contract nests an executable branch whose inner
optional path leaves `$context` on the last of two rows, then proves a later
source injection observes the restored outer `Node(0)` row. A separate
conditional step rejects an unbound variable with the exact query error. These
contracts close five unique interpreter source lines and seven instantiated
interpreter/whole-DB lines, advancing the production reports to
6,636 / 7,622 (87.0638%) and 37,443 / 55,734 (67.1816%). The reviewed
non-vector backlog falls to 12,511 with digest
`9d7afcd3f761d8a66a41479ef19d385addec4ee064f35455a108671bf4d18922`.

The next public vector contract builds a read plan from the DB's live planner
context and executes it with typed f32, f64, i64, and heterogeneous numeric
array parameters. Every representation selects the same nearest node through
the active managed index. It closes four unique vector-input conversion lines,
six instantiated interpreter lines, and 14 additional public planner-context
and runtime-catalog source lines. The production reports advance to
6,642 / 7,622 (87.1425%) interpreter lines and 37,468 / 55,734 (67.2265%)
whole-DB lines; the reviewed non-vector backlog falls to 12,493 with digest
`f0143d9935b2956e791c75f40bedc1788b50d790d6d7495e73bf5ef42423554b`.

The same vector runtime-boundary slice adds the missing tenant-shape row for a
wrong scoped property without a tenant value. It is distinct from both the
matching-property missing-value error and the already covered wrong-property
valued plan. This closes one more production interpreter/whole-DB line, moving
the ratchets to 6,643 / 7,622 (87.1556%) and 37,469 / 55,734 (67.2283%). The
reviewed non-vector backlog is 12,492 with digest
`0f1bfe0960e34480ec9eda2dce0c55bf0e3f090205853e2775aec6785f2f5eb9`.

Finally, the active vector request asks the index for three results while the
outer stream supplies a pushed limit of one, and asserts that only the nearest
node is published. This closes the remaining vector-result truncation source
line, represented by two LLVM instantiations, advancing the reports to
6,645 / 7,622 (87.1818%) interpreter lines and 37,471 / 55,734 (67.2319%)
whole-DB lines. The reviewed non-vector backlog is 12,491 with digest
`ac4d78738608a4a338cec422de3cbc838b00b71e962e6a29e8ba30707bc08d18`.

The following public executable aggregate contract counts one scalar terminal
item, rejects grouping that scalar input, and rejects aggregation of a folded
stream until it is explicitly unfolded. The exact runtime errors close nine
unique deployed lines in `stream/aggregate.rs`, represented by 17 LLVM
instantiations. Production coverage advances to 6,662 / 7,622 (87.4049%)
interpreter lines and 37,488 / 55,734 (67.2624%) whole-DB lines. The reviewed
non-vector backlog is 12,482 with digest
`78b9fe7d255c209e5cbd3179d96da2966d7bcf234fdc0dc10593e8b5b9b9287c`.

The next public projection and bound slice omits an absent property from
`values`, preserves `$id` in an explicit selected value map, filters node rows
at the edge-properties terminal, recovers a vector-search row's stored label,
and rejects both a non-integer dynamic bound and an unsupported property-bound
expression. It closes four unique deployed source lines and five LLVM
instantiations, advancing production coverage to 6,667 / 7,622 (87.4705%)
interpreter lines and 37,493 / 55,734 (67.2713%) whole-DB lines. The reviewed
non-vector backlog is 12,478 with digest
`d2d0e26db7c6e4170ef8b32e41f3ddbb4f0a80e592a86f2bbc93be8adaf8e726`.

A public writer opened over a caller-provided shared object store then proves
that an unavailable optional text runtime rejects text searches with the stable
blob-publication error code before catalog or storage access, while graph,
equality, and vector operations remain available. The contract closes 42 unique deployed source lines,
adds eight instantiated interpreter lines, 16 runtime-dependency lines, and 62
whole-DB lines. Production coverage advances to 6,675 / 7,622 (87.5754%)
interpreter, 88 / 171 (51.4620%) runtime-dependency, and 37,555 / 55,734
(67.3826%) whole-DB lines. The reviewed non-vector backlog is 12,436 with digest
`112628313509c11b817857de4fa5fcc030dd31dc4b222a2e7bee95a13c928137`.

The following public shortest-path tail covers nonexistent source and target
nodes, rejects all-node and two-node endpoint references, proves a
bidirectional search suppresses revisits, and returns no path from an existing
isolated node with no adjacency row. It closes six unique deployed source lines
and 11 LLVM instantiations, moving production coverage to 6,686 / 7,622
(87.7198%) interpreter lines and 37,566 / 55,734 (67.4023%) whole-DB lines. The
reviewed non-vector backlog is 12,430 with digest
`cae57ace6d3b6e49c2d52811e2665575393afa4c4db35b68a9693fdf5555592d`.

The same unavailable shared runtime then submits secondary, vector, and text
`CREATE INDEX IF NOT EXISTS` requests and asserts the stable family-specific
lifecycle codes. It closes `interpreter/ddl.rs:50` and two public capability
lines, adds two whole-DB LLVM lines, and moves whole-DB production coverage to
37,568 / 55,734 (67.4059%). The reviewed non-vector backlog is 12,427 with
digest `378fe5cd2bdb4e63a4b387bbdd45cfbc8bbce6eab5c853215a7cf91912bb0e73`.

A compact public predicate table then injects one missing runtime expression
through equality, inequality, every ordering arm, explicit equality and
inequality comparisons, both operands of the string-prefix/suffix paths, the
string-contains path, and a nested conjunction. The exact public error closes
15 unique deployed predicate source lines and one LLVM interpreter/whole-DB
line. Production coverage advances to 6,687 / 7,622 (87.7329%) interpreter and
37,569 / 55,734 (67.4077%) whole-DB lines. The reviewed non-vector backlog is
12,412 with digest
`ef11f3c86b7da8e6f1b46758c515f37cf383a1beba93390bd9631298e1925ad5`.

The expression error table then propagates a missing parameter from both
modulo operands and from a `case` predicate. This closes the remaining three
reachable recursive-expression source lines without changing LLVM's
already-counted line summary. The reviewed non-vector backlog is 12,409 with
digest `a44f39eb4b80478df72da757d8726f4744f09206f8abad9f1c23fe84f21df58d`.

The original launch-pull production baseline at `41200d0d` covered only 2,822
interpreter lines (37.0244%). Public request-boundary tests added since that
baseline cover mutation/projection/aggregate/range behavior; graph expansion,
branching, paths, and repeat modes; dynamic bounds and predicates; shortest
paths; real secondary/vector/text DDL and mutation maintenance; and invalid
request rollback. A clean bounded DB all-target shard after the source-fed
mutation, direct writer view-parity, explicit predicate/access additions, and
final error-propagation, access-tail, and recursive-expression contracts covers
14,773 / 14,931 interpreter lines (98.9418%), with 158 uncovered instrumented
lines, so interpreter coverage remains incomplete. The final
cache/commit-sequence contract had added 31 covered lines while adding 24
test-compiled lines, reducing the uncovered count by seven. Newer
production-boundary contracts exercise paths already represented somewhere in
the combined unit/integration corpus, while the missing-blob and virtual-label
tail added one covered line and expressed four impossible test-helper success
branches outside local instrumentation. The latest unit slice filters stale
typed vector hits for both element kinds; proves raw multigets select direct
writer, direct reader, and active-transaction views; proves transaction-local
prefix visibility; and covers empty plus oversized-ID value maps. It closes
eight source positions, reducing the finer untouched backlog from 212 to 204,
while leaving the production-only 6,687 / 7,622 ratchet unchanged.

The next production-compiled internal contract executes configured node and
edge text create/update/move/delete maintenance, including its unchanged-value
and non-text rejection boundaries. A companion stages an Active text mutation,
wins a competing serializable graph commit, and proves the interpreter aborts
without overwriting the graph row while retaining durable reclaim authority.
Together they add 355 covered interpreter lines and 1,006 whole-DB lines,
raising production interpreter coverage to 7,042 / 7,622 (92.3904%), Index V2
to 15,036 / 23,134 (64.9952%), text lifecycle to 7,653 / 13,039 (58.6932%), and
text search to 1,902 / 3,685 (51.6147%). The reviewed non-vector backlog falls
to 11,639 with digest
`2a178694f62ec76b1c33f1212c01372d296f81650cdef0bae321c1cdea4b81ab`.

The following request-isolation correction routes global edge-label,
labeled-neighbor, edge-pair, and endpoint reads through the request's stable
view instead of the live storage handle. A concurrent edge commit is excluded
from every old-snapshot lookup, and the production build returns exact
fail-closed invariant errors for twelve raw/index paths invoked without a
request view. Removing the obsolete direct-storage fallbacks reduces the
production topology by 28 lines while adding 36 covered interpreter lines.
Production coverage advances to 7,078 / 7,594 (93.2052%) interpreter and
38,617 / 55,713 (69.3142%) whole-DB lines. The reviewed non-vector backlog is
11,601 with digest
`f25fe7e6866a1e2c6a37115515fc06baf5f7cc360ea1c0a432455aa7f8511803`.

The transport corpus is measured separately so `db::query_service` is compiled
as an ordinary dependency without DB `cfg(test)` internals. It establishes
208 / 264 query-service lines (78.7879%) and 483 / 600 server lines (80.5000%).

All uncovered vector source lines retain their line-specific named-test,
architecture, invariant, or platform disposition. Every other uncovered DB
production source line is conservatively classified as test-required. The DB
and server scripts record exact counts and SHA-256 digests for those sets, so a
new, removed, or newly covered line requires an intentional reviewed ratchet
update rather than silently changing the denominator.

### Retired persisted-format compatibility harness

Phase 1 deletes `src/persistence_compatibility_tests.rs` with the development
sidecar/adoption implementation and legacy catalog serializers. Its exact
catalog, secondary-job, vector, and text source-format fixtures are frozen in
`docs/INDEX_V2_AUDIT_CLOSURE.md` solely for the separate migrations follow-up;
runtime code no longer constructs or decodes those catalog rows.

### Retired direct-physical benchmark baseline

`test_phase0_public_result_and_io_baseline` uses scripted layers and a fixed
search seed. For one four-vector cosine fixture it freezes public `(node_id,
distance)` results as `(1, 0)`, `(2, 0.5)`, `(4, 0.5)`, `(3, 1)`. It also
freezes 12 logical reads split equally across neighbors, SimHash key derivation,
and vector fetches, plus three multi-get calls split two for SimHash key
derivation and one for vectors. The test asserts both exact counters and their
accounting identities; timing fields are deliberately excluded.

The former secondary DDL suite is intentionally retired in Phase 1 because its
contracts depended on the deleted job/catalog publication sequence. Phase 5
replaces it with V2 generation-qualified build, mutation, uniqueness, tenant,
drop, and reopen/resume evidence recorded in the audit closure ledger.

The former `vector_search_baseline` Criterion target was removed when raw
metadata DTOs and the descriptorless physical `VectorIndex` facade became
crate-private. Keeping the target would require reopening the lifecycle bypass
that this plan closes. The historical measurements below remain useful as
context only; Phase 11 must replace them with a benchmark that enters through
production DDL enqueue, activation, loaded-catalog resolution, descriptor
validation, and search.

On 2026-07-10, production source `8edca8f`, Rust 1.96.1/LLVM 22.1.2,
`aarch64-apple-darwin`, and macOS 26.5, two consecutive runs of the
512-entity/dimension-32/`ef=64` fixture reported `[425.77, 429.85] us` and
`[418.79, 423.57] us`. These machine-qualified observations are not universal
thresholds. Performance workstreams must run this unchanged fixture before and
after on the same host and report the paired median comparison; correctness
continues to use the deterministic result/I/O test.

Run the projection materialization fixture with:

```bash
cargo bench -p db --bench projection_materialization -- \
  --sample-count 10 --min-time 1
```

The node case projects the label, external identity, four attributes, and inline
ID. The edge case projects the label, Graphify key, weight, four attributes,
both endpoint IDs, and inline edge ID. HEL-726's deterministic unit contracts
freeze the logical read counts; this benchmark records paired timing and
allocation observations on the same host.

The pre-cache baseline was captured on 2026-07-17 against production source
`8f700d03`, Rust 1.96.1/LLVM 22.1.2, `aarch64-apple-darwin`, and macOS 26.5:

| Projection | Median run 1 | Median run 2 | Allocations | Allocated bytes |
|---|---:|---:|---:|---:|
| 512 nodes | 5.300 ms | 5.019 ms | 199,778 | 9.161 MB |
| 512 edges | 7.640 ms | 6.954 ms | 275,554 | 12.900 MB |

The corresponding current logical amplification is 3,072 property gets and
decodes for nodes, plus 3,584 property gets and decodes and 1,024 endpoint gets
for edges. Post-cache observations must use the same command and host.

The HEL-726 row-local cache, measured with the same command and host, produced:

| Projection | Median run 1 | Median run 2 | Allocations | Allocated bytes |
|---|---:|---:|---:|---:|
| 512 nodes | 1.837 ms | 1.805 ms | 74,850 | 4.347 MB |
| 512 edges | 2.574 ms | 2.612 ms | 102,498 | 5.833 MB |

Paired medians improved by 64.0-65.3% for nodes and 62.4-66.3% for edges.
Allocation counts fell by 62.5% and 62.8%, while allocated bytes fell by 52.5%
and 54.8%, respectively. Deterministic contracts reduce the combined property
gets and decodes from 6,656 to 1,024 and endpoint gets from 1,024 to 512.

## Excluded secondary-worker suite
On 2026-07-13, the final performance gate compared baseline `a0e9a36a` with
implementation `010e60fa` used Rust 1.96.1/LLVM 22.1.2 on the same
`aarch64-apple-darwin` macOS 26.5 host and the 60-sample command above. The
immediately preceding baseline interval was `[416.29, 416.92, 417.66] us`.
Two consecutive final-code intervals were `[431.45, 432.20, 432.99] us` and
`[422.57, 423.22, 423.97] us`, giving median changes of +3.66% and +1.51%.
Both remain below the phase gate's 5% same-run regression ceiling.

Run the aggregate 10k/100k scale gate in release mode. First run the reviewed
baseline without the environment variables, then supply its reported medians
to the implementation run:

```bash
HELIX_VECTOR_SCALE_BASELINE_NS_10000=1725208 \
HELIX_VECTOR_SCALE_BASELINE_NS_100000=2526531 \
CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/private/tmp/helix-proper-scale-target \
cargo test --release -p db \
  vector_search_scale_gate_reports_recall_and_median_throughput -- \
  --ignored --nocapture
```

On 2026-07-13, baseline `a0e9a36a` and implementation `42f7cb1c` ran the
identical current-f32 fixture with Rust 1.96.1/LLVM 22.1.2 on the same
`aarch64-apple-darwin` macOS 26.5 host. The baseline used test-only identity
adapters for its former `index_id` and `node_id` field names; production code
and persisted rows were unchanged. Baseline medians were 1,725,208 ns at 10k
and 2,526,531 ns at 100k, both with 100% recall@10. The baseline-aware
implementation run reported 1,661,661 ns and 2,381,045 ns, also with 100%
recall@10, for throughput ratios of 1.038243 and 1.061102. Recall therefore
dropped 0.0 percentage points and throughput remained 103.82%/106.11% of
baseline, passing the 0.5-point and 95% aggregate gates. These medians are
machine-qualified evidence, not portable absolute thresholds.

## Retired secondary-worker suite

The deleted `crates/db/src/execution/interpreter/ddl/tests/secondary.rs` used
pre-lifecycle tuning and status façades and produced 79 compile errors when
temporarily declared. Its twelve contracts were replaced as follows before the
undiscovered source was removed:

| Retired contract | Current named evidence |
|---|---|
| `node_secondary_ddl_enqueues_pending_backfill_and_maintains_new_writes` | `builder_and_active_mutations_cover_insert_update_delete_and_label_move` |
| `node_secondary_backfill_processor_batches_and_finalizes_catalog_visibility` | `source_scan_commits_no_more_than_the_configured_entity_batch`; `every_build_and_drop_stage_resumes_after_database_reopen` |
| `node_secondary_range_backfill_processor_batches_and_finalizes_catalog_visibility` | `every_node_and_edge_equality_and_range_shape_builds_its_exact_lane` |
| `pending_secondary_drop_cleans_partially_backfilled_physical_entries` | `abort_and_drop_publish_non_visible_state_before_exact_generation_cleanup` |
| `secondary_backfill_preserves_writes_added_after_partial_scan` | `building_mutation_coalesces_delta_and_catch_up_rereads_authoritative_state` |
| `secondary_backfill_processor_records_failure_for_unindexable_values` | `cleanup_blocks_instead_of_skipping_a_row_larger_than_one_transaction`; `unique_build_and_active_mutation_report_exact_conflicting_entity_ids` |
| `edge_secondary_ddl_enqueues_pending_backfill_and_maintains_new_writes` | `builder_and_active_mutations_cover_insert_update_delete_and_label_move` |
| `edge_secondary_backfill_processor_batches_and_finalizes_catalog_visibility` | `every_node_and_edge_equality_and_range_shape_builds_its_exact_lane`; `every_build_and_drop_stage_resumes_after_database_reopen` |
| `edge_secondary_range_backfill_processor_batches_and_finalizes_catalog_visibility` | `every_node_and_edge_equality_and_range_shape_builds_its_exact_lane` |
| `secondary_backfill_background_worker_drains_pending_jobs` | `index_lifecycle_secondary_state_machine_matches_reference_model` |
| `secondary_backfill_ddl_wakes_idle_background_worker` | `every_build_and_drop_stage_resumes_after_database_reopen` |
| `unique_node_equality_ddl_rejects_existing_duplicate_values_atomically` | `unique_build_and_active_mutation_report_exact_conflicting_entity_ids` |

## Retired stale integration suite

The deleted `crates/db/tests/lib/mod.rs` contained 76 tests but was not a Cargo
target. A temporary root harness exposed 275 compile errors against removed
planner and IR APIs, private fields, and integration-crate path assumptions.
It was never counted as current coverage.

Every deleted test remains listed below as the case-by-case replacement record.
The current module suites and the three `production_*` targets named by each
family are Cargo-discovered and passing.

### Runtime, reader, and planner facade contracts

Disposition: **Replaced** by current `lib.rs`, `query_service`, scoped-runtime,
planner-context tests, and `production_vector_planner` public plans.

- `open_writer_builds_planner_catalog_from_runtime_index_config`
- `open_with_object_store_opens_writer`
- `open_with_runtime_index_config_builds_planner_catalog`
- `object_storage_source_builds_object_store_without_network`
- `query_executes_request_directly`
- `query_json_executes_request_directly`
- `query_json_rejects_invalid_json`
- `reader_query_json_rejects_write_requests`
- `reader_rejects_write_physical_plan_before_interpreting_entries`
- `reader_executes_flushed_read_plans_without_writer_access`
- `reader_executes_flushed_expand_paths_without_writer_access`

### General access, stream, projection, and control contracts

Disposition: **Replaced** by the current interpreter access/control/stream
unit suites and public executable-plan production contracts.

- `execution_value_len_and_empty_cover_all_result_shapes`
- `literal_bounds_window_direct_all_scans`
- `literal_bounds_window_direct_access_sources`
- `literal_bounds_short_circuit_scan_then_filter_sources`
- `descending_range_indexes_preserve_semantic_bounds`
- `initial_run_condition_false_skips_execution`
- `interpreter_executes_simple_point_id_stream_steps`
- `interpreter_executes_edge_point_id_stream_steps`
- `interpreter_executes_node_access_filter_bounds_aggregate_and_projection_arms`
- `residual_filter_covers_predicate_branches_and_short_circuit_identities`
- `residual_filter_reports_malformed_expression_and_predicate_inputs`
- `project_projection_covers_expression_and_case_predicate_branches`
- `stream_bound_exprs_accept_dynamic_numeric_bounds_and_reject_row_context`
- `interpreter_rejects_malformed_bitmap_index_rows`
- `equality_index_access_accepts_dynamic_params_inline`
- `range_index_access_accepts_dynamic_params_inline`
- `interpreter_executes_edge_access_and_expand_arms`
- `interpreter_executes_every_expand_direction_output_label_combination`
- `expand_edges_skips_missing_and_malformed_endpoint_rows`
- `interpreter_executes_projection_variable_and_aggregate_arms`
- `interpreter_executes_branch_repeat_and_reserved_arms`
- `interpreter_executes_every_repeat_stop_emit_combination`
- `interpreter_executes_param_var_order_range_and_distinct_arms`
- `from_var_access_filters_mixed_node_and_edge_streams`
- `from_param_access_accepts_scalar_and_array_parameter_shapes`
- `from_param_access_rejects_missing_mixed_and_negative_id_shapes`
- `interpreter_rejects_invalid_access_bound_and_variable_inputs`
- `helixdb_executes_planner_ir_from_ast_write_batch`
- `helixdb_executes_planner_ir_from_ast_read_batch_with_indexes`
- `helixdb_executes_shortest_path_from_ast_read_batch`
- `interpreter_returns_requested_batch_variable`
- `interpreter_executes_initial_foreach_and_restores_original_param`
- `interpreter_executes_followup_foreach_and_restores_original_param`
- `interpreter_executes_initial_foreach_static_param_shapes`
- `interpreter_executes_dynamic_initial_and_followup_foreach_shapes`
- `interpreter_executes_dynamic_foreach_scalar_shapes_and_restores_original_param`
- `interpreter_executes_typed_initial_and_followup_foreach_shapes`
- `interpreter_reports_missing_foreach_params_and_skips_false_followup_conditions`
- `interpreter_executes_initial_and_followup_run_condition_variants`
- `interpreter_executes_node_access_union_and_intersect_sources`

### Graph mutation contracts

Disposition: **Replaced** by current mutation lifecycle/property/index suites
and request-owned vector transaction contracts.

- `mutations_create_implicit_system_timestamps_for_nodes_and_edges`
- `system_timestamp_properties_are_db_owned`
- `property_mutations_refresh_updated_at_and_move_timestamp_indexes`
- `mutation_arms_keep_raw_slate_rows_and_indexes_consistent`
- `mutation_property_expr_neg_is_evaluated_for_add_and_set_paths`
- `add_node_property_expr_covers_dynamic_params_and_malformed_inputs`
- `add_edge_property_expr_reports_malformed_inputs`
- `set_property_expr_covers_dynamic_object_and_malformed_inputs`

### Dynamic DDL and catalog contracts

Disposition: **Replaced** by shared lifecycle transition, duplicate-create,
runtime publication, and reopen reconciliation contracts. The legacy
non-atomic catalog expectations were intentionally not preserved.

- `ddl_updates_same_handle_planner_catalog`
- `open_loads_dynamic_indexes_from_metadata_into_runtime_index_config`
- `ddl_create_mode_controls_duplicate_dynamic_indexes`
- `ddl_drop_removes_all_dynamic_index_kinds_from_same_handle_planner_catalog`

### Text-index contracts

Disposition: **Replaced** by current public query builders, text lifecycle
build/drop/reopen contracts, direct-publication failure tests, and
unchanged-manifest goldens.

- `index_ddl_create_text_backfills_existing_node_documents`
- `node_text_search_uses_tenant_value_query_expr_and_limit_expr`
- `edge_text_search_backfills_and_accepts_query_expr_and_limit_expr`
- `search_access_reports_malformed_vector_and_text_inputs`

### Vector-index contracts

Disposition: **Replaced** by production-only integration tests rather than
private-field legacy tests.

Current production-linked replacements:

- `public_vector_index_lifecycle_is_transactional` covers the compiled public
  create/error/empty-search/insert/upsert/reopen/delete/drop transaction path.
- `public_vector_index_supports_every_active_f32_metric` replaces
  `node_vector_write_paths_cover_non_cosine_metrics` at the public vector
  façade for cosine, Euclidean, and Manhattan. Binary/f16 remain outside the
  active descriptor contract.
- `public_vector_parameter_types_reject_invalid_states` and
  `public_vector_dimension_types_bind_exact_lengths` cover every exported
  numeric/dimension constructor family through the production library.
- `public_vector_memory_store_hydrates_and_evicts_typed_rows` covers typed
  SimHash dimension rejection, compatible/bounded/shutdown hydration, and
  public typed row eviction without using `cfg(test)` cache constructors.
- `public_vector_search_parameters_normalize_all_overrides` covers every public
  query-time SimHash mode and override normalization path.
- `public_vector_graph_mutations_cover_dense_f32_workload` exercises populated
  graph insertion, all three active SimHash search modes, replacement,
  reciprocal-link pruning, deletion, and post-delete search through production
  transactions.
- `public_vector_codecs_round_trip_current_f32_state` freezes the public
  metadata/item/neighbor/entry helper behavior through the production library.
- `public_vector_configuration_rejects_every_invalid_field_family` covers
  current configuration validation and bounded layer selection without relying
  on unit-only constructors.
- `public_dynamic_vector_ddl_backfills_existing_nodes` covers dynamic vector
  DDL backfill through the current production executable-plan boundary.
- `public_node_mutations_keep_vector_generation_synchronized` covers node add,
  set-property, remove-property, and drop maintenance through committed planner
  requests.
- `public_edge_mutations_keep_vector_generation_synchronized` covers edge add,
  set-property, remove-property, and drop-by-id maintenance through committed
  planner requests.

## Completion rule

This inventory is closed: every retired row above has named current-test
evidence, the stale undiscovered sources are deleted, all intended current
targets are Cargo-discovered, and the enforced production-only coverage gate
passes.
