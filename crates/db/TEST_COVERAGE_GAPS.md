# db/src test coverage discovery

Status: current-baseline discovery complete. Pure/unit-test sections are being
implemented incrementally from this backlog. The authoritative discovered and
excluded target inventory is `tests/TEST_TARGET_INVENTORY.md`. Use
`../../scripts/db-production-coverage.sh` for production-only coverage; the
historical all-targets figures below include inline test code and are not release
threshold evidence.

The initial production-linked baseline at `8ac67e9` was reproduced twice:

- whole `db`: 752 / 27,799 lines (2.7051%);
- `search/vector`: 45 / 4,765 lines (0.9444%).

See `tests/TEST_TARGET_INVENTORY.md` for exact function/line/region counts and
the excluded-suite disposition.

This file is the running discovery log for pushing `crates/db` toward near-100%
coverage. It intentionally contains only test planning notes and does not
change runtime code.

## Historical all-targets coverage baseline

Command run from `/Users/xav/GitHub/helix-proper/crates`:

```bash
CARGO_TARGET_DIR=/private/tmp/helix-proper-db-coverage-target \
  cargo llvm-cov -p db --all-targets --json \
  --output-path /private/tmp/helix-proper-db-coverage.json \
  --ignore-filename-regex '/(registry|rustc)/'
```

Observed test targets:

```bash
cargo test -p db --all-targets --no-run \
  --target-dir /private/tmp/helix-proper-db-test-target \
  --message-format=short
```

Cargo currently builds only:

- `src/lib.rs` unit tests
- `tests/encoding_only.rs`
- `tests/production_contracts.rs`

Important harness hole: `db/tests/lib/mod.rs` is 11,030 lines and contains many
`#[test]` / `#[tokio::test]` cases, but Cargo does not discover nested
`tests/lib/mod.rs` as an integration target by itself. Either add a root
`db/tests/lib.rs` that declares `mod lib;`, or move the file to a discovered
integration-test target before relying on those tests for coverage.

Implementation note: a discovered `tests/integration.rs` harness was attempted
against the current worktree. It exposed the file as a stale pre-refactor suite:
`cargo test -p db --test integration` failed with 275 compile errors from old
planner/IR types (`PhysicalPlan`, `PhysicalBatchEntry`, `AtLeastTwo`,
`IndexRange::lower`/`between`, `IndexBound::inclusive`/`exclusive`), direct
private-field access, and assumptions that `crate::...` refers to the db
library root from an integration test. This is a major test-suite migration,
not a small coverage fix. Leave it disabled until the suite is ported to the
current planner builders and public crate imports.

Original `db/src` line coverage from the JSON report:

- Covered: 29,350 / 37,003 executable lines
- Line coverage: 79.32%
- Files in `db/src`: 147

Latest all-target snapshot after completed coverage sections:

```bash
CARGO_TARGET_DIR=/private/tmp/helix-proper-db-coverage-target-6 \
  cargo llvm-cov -p db --all-targets --json \
  --output-path /private/tmp/helix-proper-db-coverage-vector-search-modes.json \
  --ignore-filename-regex '/(registry|rustc)/'
```

- Covered: 44,176 / 45,791 executable lines
- Line coverage: 96.47%
- Files in `db/src`: 147

Repeated full runs at the vector memory-store section covered 35,725-35,777
lines. Randomized vector-index and concurrent test paths make the aggregate
fluctuate between otherwise passing runs; file-level deterministic section
results are stable.
Consecutive text-directory runs likewise moved `search/vector` coverage between
6,490 and 6,603 covered lines without vector code changes.

One row-mode coverage run also exposed this instability as a test failure:
`node_vector_index_tracks_executable_mutations` returned node 0 instead of node
1. The exact test rerun and the subsequent full coverage rerun passed. A broader
deterministic approximate-search fix is outside this test-only coverage work.
The index-access full run reproduced the same failure after 736 tests passed;
the coverage report was generated from that otherwise complete run. The next
full runtime-tail run passed all 739 unit tests and all 130 encoding tests; the
latest index-maintenance run passed all 746 unit tests and all 130 encoding
tests. The mutation-complete run passed all 751 unit tests and all 130 encoding
tests. The text-apply run passed all 754 unit tests and all 130 encoding tests.
The text-directory run passed all 760 unit tests and all 130 encoding tests. The
secondary-backfill run passed all 762 unit tests and all 130 encoding tests.
The hot-directory run passed all 766 unit tests and all 130 encoding tests.
The split-bundle run passed all 769 unit tests and all 130 encoding tests.
The text-compaction run passed all 776 unit tests and all 130 encoding tests.
The secondary-DDL run passed all 779 unit tests and all 130 encoding tests.
The search-contract run passed all 782 unit tests and all 130 encoding tests
after the known randomized vector-mutation test passed on focused and full reruns.
The final vector-search-mode run passed all 798 unit tests, all 130 encoding
tests, and all 3 production-contract integration tests.

Completed sections:

- `db/src/config/tests.rs`: config builder, parser, cache projection, and SlateDB
  option contract coverage.
- `db/src/config/tests.rs` and `db/src/error.rs`: additional config contract
  coverage for explicit typed wrappers and error display/classification coverage.
- `db/src/secondary_backfill.rs`: pure backfill state, serde, create/drop spec,
  scan-bound, status, and write-maintenance contract coverage.
- `db/src/execution/interpreter/ddl/secondary.rs`: pure secondary-DDL helper
  coverage for scoped keys, nested value rejection, unique node equality values,
  and edge endpoint decoding errors.
- `db/src/search/mod.rs`: pure search helper coverage for deterministic names,
  metadata keys, index value formatting, indexable-value gating, unique identity,
  and malformed roaring bitmap decode.
- `db/src/search/mod.rs`: node secondary-index helper coverage for equality,
  range, descending range, tenant isolation, malformed stored bitmaps, property
  index orchestration, wrong labels, removals, and nested-value rejection.
- `db/src/search/mod.rs`: edge secondary-index helper coverage for equality,
  pair, range, global range, endpoint/property storage, tenant isolation,
  update/replacement/removal orchestration, and nested-value rejection.
- `db/src/search/text/union_directory.rs`: union split directory coverage for
  synthetic meta, split-file routing, missing files, schema mismatch, debug,
  watch, and lock behavior.
- `db/src/search/text/cache.rs`: cache state, disk namespace, metadata/reference,
  warm short-circuit, opened-generation budget, and disk cleanup contract
  coverage.
- `db/src/search/text/cache.rs`: end-to-end remote, memory, and disk generation
  loading; corrupt local-artifact fallback; in-flight load coalescing; missing
  blob error accounting; multi-split handling; and configured startup warming.
- `db/src/search/text/mod.rs`: scoped manifest loading, legacy manifest
  migration, live-state version/dead filtering, split deduplication, blob copy
  and collection, typed node/edge document collection, value/tenant validation,
  and legacy schemas without logical-version fields.
- `db/src/lib.rs`: writer/reader facade modes, object-store and disk source
  paths, cache snapshots and warming, scoped planner/runtime state, catalog
  persistence rejection, vector-memory discovery/admission/shutdown behavior,
  malformed metadata, and public query delegation.
- `db/src/search/vector/index.rs`: public create/search/stats/delete/drop
  lifecycle, metadata and dimension errors, cache attachment modes, tenant key
  isolation, and layer-0 search in Off, Always, and Adaptive SimHash modes.
- `db/src/search/text/{bundle_storage,storage_directory,storage_with_cache,overlay_directory,debug_proxy_directory,debounced_storage,caching_directory,hot_directory}.rs`:
  storage adapter range mapping, async read, cache reuse, overlay/debug
  delegation, debounced retries, hot-cache fallback, watch, and lock behavior
  coverage.
- `db/src/search/text/byte_range_cache.rs`: complete byte-range cache coverage
  for empty ranges, ignored invalid inserts, path isolation, non-contiguous
  misses, prefix/suffix preserving merges, middle replacement, and covered
  subrange inserts.
- `db/src/search/text/warmup.rs`: term range projection, merge, simplify, field
  norm, automaton, fast-field, and pruning contract coverage; only two
  coverage-tool-marked retain closure lines remain uncovered.
- `db/src/encoding/v1/keys/tenant.rs`: malformed ULID, Crockford alias,
  tenant prefix, legacy scope, and strip-key negative contract coverage.
- `db/src/search/vector/simhash.rs`: statistics formatting, neutral rate,
  shared/custom hasher reuse, tenant-scoped SimHash key contracts, transactional
  miss/read/compute/delete behavior, cached fallback behavior, and malformed
  persisted-value rejection. The only remaining production misses are the
  `SimHash::from_bytes` error-mapping closure, which cannot execute after the
  preceding exact eight-byte length check.
- `db/src/search/vector/memory_store.rs`: dirty-row tracking, pending-row
  reference counts, publish generation/locking, whole-store eviction, identity
  accessors, and clear behavior. The 12 remaining misses are watch-channel
  scheduling branches and an out-of-scope prefix-scan invariant that cannot be
  produced by SlateDB's prefix iterator.
- `db/src/query_service.rs`: complete service-wrapper, execute/warm/scoped
  delegation, read/write batch normalization, reader-mode rejection, response
  serialization, scalar/folded-stream conversion, conflict classification, and
  service-to-database error conversion coverage.
- `db/src/id_allocator.rs`: complete initialized batch-extension, typed
  node/edge wrapper, lease accounting, persistence, concurrent allocation, and
  deterministic extension-waiter coverage, including the test-only counting
  allocator paths.
- `db/src/execution/interpreter/shortest_path.rs`: missing source/target,
  endpoint cardinality and empty-name rejection, missing adjacency, and labeled
  incoming-only lookup coverage. The four remaining lines are queue-length and
  predecessor-chain invariants guaranteed by the local breadth-first traversal.
- `db/src/execution/interpreter/stream/aggregate.rs`: complete folded/scalar
  dispatch, empty element-row rejection for every aggregate family, scalar
  count, and numeric/string/non-numeric conversion coverage.
- Stream and row-state tail coverage: folded-stream rejection, empty-row
  distinct identity, virtual-property binding snapshots, direct folded-stream
  emptiness, sack clearing, missing sack properties, and both non-numeric sack
  operands. `stream.rs`, `stream/sets/variables.rs`, and `reserved/sack.rs` are
  at 100%; `types.rs` and `stream/sets/distinct.rs` have no missing source lines
  and retain only coverage-region summary artifacts.
- `db/src/execution/interpreter/stream/eval/property.rs`: complete missing edge
  endpoint handling for direct and nested endpoint properties plus malformed
  and non-object nested property traversal coverage.
- `db/src/execution/interpreter/stream/order.rs`: range-index plans now directly
  prove that interpreter ordering preserves access-path order. Every production
  source line under `execution/interpreter/stream/` is covered; the one-line
  subtree deficit is a compiler coverage-region artifact in distinct-row keying.
- Control-flow branch/repeat/support coverage: choose and choose-else zero/all
  match shortcuts, every repeat emission mode, filtered repeat emissions, and
  restoration of a pre-existing context variable. `branch.rs`, `repeat.rs`,
  and `support.rs` are all at 100%.
- `db/src/execution/interpreter/control/foreach.rs`: complete static and dynamic
  malformed-input coverage for outer array shape, object item shape, and empty
  field names. The control subtree has no missing production source lines; its
  sole deficit is the intentional success-arm panic in a test error helper.
- Search-access tails: missing text-definition errors, expression-backed tenant
  values, mismatched and internally inconsistent tenant shapes, and Manhattan
  vector metric dispatch. `search/{definitions,tenant,dispatch}.rs` are all at
  100%.
- `db/src/execution/interpreter/access/expand.rs`: complete unlabeled node
  expansion, edge-current direction/path/self-loop behavior, missing endpoint
  and adjacency rows, absent labels, and non-node edge-output inputs. All
  production lines are covered; four test-only panic arms remain unexecuted.
- `db/src/execution/interpreter/access/range.rs`: complete reader and writer
  dispatch coverage for node and edge all/bounded scans, including inclusive
  and exclusive lower bounds and inclusive upper bounds.
- `db/src/execution/interpreter/access/indexes.rs`: complete reader, writer,
  and active-transaction dispatch coverage for node/edge equality, global edge
  label/equality, directional neighbors, edge-pair indexes, and endpoint rows.
- `db/src/execution/interpreter/access/kv.rs`: prefix-scan execution with
  pushed limits, edge-keyspace ID scans, and typed metadata-key rejection. The
  report has no missing source lines; its two-line summary deficit is a coverage
  region mapping artifact.
- `db/src/execution/interpreter/access/search/storage.rs`: complete reader and
  writer dispatch coverage for vector search, text manifest loading, and text
  search, including suppressed and propagated missing-index failures.
- `db/src/execution/interpreter/access/search/input.rs`: complete literal and
  expression input coverage for vectors, text, and limits, including non-string
  and empty text plus non-integer and non-positive runtime limits.
- `db/src/search/vector/spaces/simple.rs`: binary-quantized equal and mismatched
  byte-length paths. The six remaining lines are x86_64-inapplicable scalar/NEON
  detection and a trailing-byte loop blocked by the `BinaryQuantized` eight-byte
  alignment invariant.
- `db/src/execution/interpreter/access/dispatch.rs`: complete node equality and
  edge empty/variable/all/label source routing plus ID and vector search-result
  limit truncation coverage.
- `db/src/execution/interpreter/row_mode.rs`: cached setting resolution,
  at/below-cap behavior, and previously uncovered operation-name variants. The
  five remaining lines require process-global environment mutation for enabled
  and non-Unicode values and are intentionally not exercised by parallel tests.
- `db/src/execution/interpreter/ddl/vector.rs`: Manhattan physical create/drop
  dispatch and malformed typed metadata rejection for tenant partition drops.
  The eight remaining lines are short-key, wrong-keyspace, and out-of-scope
  branches blocked by typed prefix scans.
- `db/src/execution/interpreter/ddl/text.rs`: tenant-scoped node and edge
  partition collection now covers rows without indexed properties. The 16
  remaining lines are typed prefix-scan invariants, validated serialization or
  postcondition failures, low-level transaction failures, and one coverage
  mapping artifact.
- `db/src/search/text/split.rs`: footer decode rejection, trailer validation,
  footer-cache metadata validation, synthetic split directory read, missing
  file, watch, and lock contract coverage.
- `db/src/config/{cache,db,indexes,runtime_catalog,utils}.rs`: cache/db builder
  edges, path/error wrappers, secondary-index serde rejection, dynamic catalog
  key/entry identity, dynamic insert/remove projections, and planner
  create/drop DDL conversion coverage including drop-spec existence and
  uniqueness checks.
- `db/src/encoding/v1/indexes/range.rs`: range, edge-range, and global
  edge-range key prefix contracts plus global edge range malformed parser
  coverage.
- `db/src/encoding/v1/keys/tenant.rs`: complete Crockford ULID alphabet and
  tenant-prefix malformed-input coverage.
- `db/src/encoding/v1/indexes/mod.rs`: additional index prefix dispatch and
  generic `IndexKey` global edge variant coverage.
- `db/src/encoding/v1/indexes/{equality,label}.rs`: key prefix contracts,
  wrong-prefix rejection, trailing-byte rejection, and global/neighbor parser
  edges.
- `db/src/encoding/v1/keys/vectors.rs`: accessor and encoded-length contracts
  for all 15 typed vector key shapes, plus exhaustive concrete-parser short,
  trailing, wrong-prefix, and wrong-kind boundaries.
- `db/src/encoding/v1/indexes/scan_prefixes.rs`: complete exclusive and
  inclusive range-bound byte-layout coverage for node, edge, and global-edge
  prefixes.
- `db/src/encoding/v1/keys/mod.rs`: generic vector/index dispatch, fixed-key
  short boundaries, keyspace/prefix contracts, and physical tenant-scope parser
  coverage.
- `db/src/encoding/v1/indexes/mod.rs`: complete generic key-prefix and
  index-prefix contracts across all eight typed index-key variants, including
  malformed equality dispatch.
- `db/src/encoding/v1/values/edges.rs`: complete empty typed edge-value
  constructor coverage, including equivalence with the public static bytes and
  the normal encoder.
- `db/src/encoding/v1/property/property_view.rs`: empty, valid, and invalid
  construction coverage for both reusable `&[u8]` and `Bytes` input forms.
- Encoding tail audit: all 23 remaining lines are unreachable typed-invariant
  branches, assertion-only `let ... else` panics, 32-bit-only overflow paths,
  or coverage-mapped postcondition lines. No reachable encoding behavior remains
  without direct tests.
- `db/src/config/{cache,db}.rs`: complete default-constructor and legacy/canonical
  edge encoding and update-policy byte conversion coverage.
- `db/src/config/runtime_catalog.rs`: missing dynamic text and edge-vector drop
  lookup coverage; all remaining conversion errors are blocked by validated
  planner/catalog input types.
- `db/src/config/indexes.rs`: exhaustive public runtime catalog builders,
  scoped lookup predicates, asc/desc iterators, planner projections, serde
  variants, and invalid vector/text builder contracts.
- `db/src/search/vector/mod.rs`: metric/config repair, all query parameter
  builders, malformed helper inputs, non-finite layer selection, diversity
  fallback, and test distance-codec contracts.
- Runtime tail coverage: active-write serial scheduling fallback, empty
  dependency concatenation, filter operation dispatch, SlateDB transaction
  error classification, runtime expression property inputs, and unsupported
  text-document values. `error.rs`, `dependencies.rs`, `dispatch.rs`, and
  `mutation/properties.rs` are at 100%; scheduler retains two intentional test
  panic arms, and text document extraction retains one impossible `Ok(None)`
  arm after text normalization.
- Index-maintenance tails: vector document name mapping for unscoped and tenant
  partitions, inconsistent tenant-definition rejection, every numeric array
  conversion, non-numeric rejection, bidirectional adjacency updates, partial
  adjacency removal, missing-edge mutation idempotence, absent edge-property
  removal, and parameter/variable edge targets. Adjacency is at 100%; edge
  mutation retains one closing-brace mapping artifact, and vector document
  extraction has no missing source lines despite three region-summary misses.
- Mutation contracts: edge equality/ascending-range/descending-range removal,
  unlabeled no-op removal, node label type validation, same-label and cross-label
  index maintenance, absent property and missing node idempotence, incoming-edge
  cleanup, all-node targets, reader/transaction existence checks, and direct
  edge-label mutation rejection. Every production source line in the mutation
  subtree is covered; its three reported misses are coverage-region artifacts.
- Text index apply state: no-op changes, missing and legacy version counters,
  malformed counter bytes, overflow, both legacy policies, reader/missing/bad
  manifest compaction skips, forced merge failures, and invalid old/new document
  propagation. `search_index/text.rs` is at 100%; apply's remaining source lines
  require impossible serde failures, transaction write failure injection, or an
  empty manifest from a non-empty document batch.
- Text directory adapters: invalid/duplicate/settings-mismatched union splits,
  payload reconciliation, split routing, explicit read-only operations, overlay
  error conversion, missing object-store blobs, precovered and short cache reads,
  and storage/debug wrapper contracts. Caching, storage, and debug wrappers have
  no missing production source lines; remaining adapter misses are platform,
  validated-loop, cache-race, or injected-directory error tails.
- Secondary backfill transactions: batched node and edge scans, exclusive resume
  progression, completion, equality and ascending/descending range writes,
  wrong-family no-ops, missing endpoints/properties, wrong labels, and nested
  value rejection. The remaining source misses are a typed dynamic-index
  constructor failure, an inconsistent batch state excluded by synchronized
  counters, infallible serialization of the validated definition enum, and two
  intentional test panic arms.
- Hot directory and cache bundle: complete/partial/disjoint/empty byte ranges,
  malformed directory metadata and slice indexes, each output writer failure,
  underlying sync/async fallback, read-only operations, and a real Tantivy index
  hotcache roundtrip. Remaining production misses require impossible bincode
  serialization failures or a custom directory that injects reader, metadata,
  file-open, and slice-read failures during hotcache construction.
- Text split bundles: real-index build/file/byte roundtrips, invalid source
  directories and files, every persisted-reference mismatch, missing metadata,
  malformed footer/trailer lengths and identities, cached-footer file reads,
  local range bounds, and read-only directory methods. Remaining production
  misses require 32-bit length overflow or injected file open/stat/range-read
  failures after earlier operations on the same file have succeeded.
- Text compaction: end-to-end live-version merge persistence, stale-only
  manifest deletion, byte/count selection, live/dead and malformed state rows,
  stale-document schema/fast-field/value invariants, duplicate/mismatched split
  metadata, and output-file pruning. Remaining production misses require
  transaction, filesystem, or Tantivy failure injection, infallible manifest
  serialization, or synthetic-meta payload/error tails.
- Secondary DDL physical and pending paths: all node/edge equality and range
  create/drop variants, descending storage direction, wrong labels, unique
  conflicts and rollback, typed malformed rows, search-index no-op dispatch,
  existing catalog entries, duplicate pending jobs, and missing pending drops.
  Remaining production misses are typed job-key propagations and short rows
  excluded by typed prefix scans; three more misses are test panic arms.
- Search index contracts: stale/missing/self unique-candidate validation and
  real conflicts, node/edge ascending and descending replacement, every node,
  edge, and global range-bound shape, directional bulk cleanup, global label
  removal/clear, and public batch wrappers. `search/mod.rs` is at 99.12%; its
  remaining lines are malformed typed-scan skips, no-op wrappers, and coverage
  mapping tails.

## Module-level coverage

| Module | Lines | Covered | Missing | Files |
|---|---:|---:|---:|---:|
| `search` | 94.18% | 20206/21454 | 1248 | 41 |
| `execution` | 99.11% | 12270/12380 | 110 | 77 |
| `config` | 98.46% | 1858/1887 | 29 | 6 |
| `encoding` | 99.63% | 6188/6211 | 23 | 18 |
| `lib.rs` | 88.01% | 1409/1601 | 192 | 1 |
| `secondary_backfill.rs` | 98.72% | 1006/1019 | 13 | 1 |
| `query_service.rs` | 100.00% | 561/561 | 0 | 1 |
| `id_allocator.rs` | 100.00% | 632/632 | 0 | 1 |

Notable submodules:

| Submodule | Lines | Covered | Missing | Files |
|---|---:|---:|---:|---:|
| `search/text` | 91.87% | 8084/8799 | 715 | 15 |
| `search/vector` | 93.52% | 7055/7544 | 489 | 24 |
| `execution/interpreter/ddl` | 97.81% | 1608/1644 | 36 | 3 |
| `execution/interpreter/access` | 99.51% | 2658/2671 | 13 | 13 |
| `execution/interpreter/mutation` | 99.81% | 1607/1610 | 3 | 7 |
| `execution/interpreter/search_index` | 98.92% | 1554/1571 | 17 | 10 |
| `execution/interpreter/stream` | 99.92% | 1323/1324 | 1 | 22 |
| `execution/interpreter/control` | 99.86% | 713/714 | 1 | 4 |
| `search/vector/unaligned_vector` | 98.55% | 1220/1238 | 18 | 9 |
| `search/vector/distance` | 99.66% | 289/290 | 1 | 8 |

## Highest priority gaps

1. `search/vector/index.rs` - 89.85%, 441 uncovered all-target lines.
   Public lifecycle and all three SimHash search modes now have direct tests.
   Remaining production clusters are deeper relink/pruning combinations,
   mutation-cache pressure and injected missing/corrupt graph rows. Broadening
   these safely requires deterministic graph fixtures rather than more
   randomized ANN result assertions.

2. `search/text/*` - 91.87%, 715 uncovered all-target lines.
   Manifest, query, cache-tier, compaction, split, storage, and hot-directory
   behavior are covered. Remaining lines are dominated by filesystem,
   object-store, transaction, Tantivy, and task-join failure injection plus
   typed or serialization invariants.

3. `lib.rs` - 88.01%, 192 uncovered all-target lines.
   Writer/reader, scoped runtime state, FTS warm, vector-memory refresh, disk,
   in-memory, and object-storage construction are covered. Remaining branches
   are hybrid-cache device failures, task panics, poisoned locks, serialization
   failures, and broader public integration contracts tracked in
   `tests/TEST_TARGET_INVENTORY.md`.

4. Search index tail audit.
   `search/mod.rs` is at 99.12% with all reachable secondary-index lifecycle,
   unique-validation, range-bound, storage, and cleanup contracts covered. The
   44 remaining lines are malformed typed-scan skips, no-op wrappers, and
   coverage-mapped tails.

5. Secondary DDL tail audit.
   `execution/interpreter/ddl/secondary.rs` is at 99.23% with every reachable
   pending-job and physical node/edge equality/range contract covered. The five
   remaining production misses are typed job-key error propagations and short
   rows excluded by typed prefix scans; three additional misses are intentional
   test panic arms.

6. Config tail audit.
   Config is at 98.46% with all reachable contracts covered. The 29 remaining
   lines are 16 planner/catalog constructor propagations and 13 invalid internal
   index states excluded by typed definitions; cache, DB, and utility config
   files are at 100%.

7. Secondary backfill tail audit.
   `secondary_backfill.rs` is at 98.75% with every reachable job lifecycle,
   status, scan, and physical node/edge write contract covered. Its remaining
   production lines are typed-constructor, synchronized-state, and validated
   serialization failures; two additional misses are intentional test panic
   arms.

## Module-by-module notes

### `search`

This is the dominant coverage debt. Prioritize behavior tests over isolated
line coverage because the missed paths are mostly index lifecycle, tenant
scope, storage, and cache behavior. The highest-risk areas are:

- `search/mod.rs`: secondary index add/remove/search helpers, tenant-scoped
  key formation, equality/range helper rejection paths, update/delete
  idempotence, unique-value restrictions, and edge-vs-node symmetry.
- `search/text`: split bundle validation, object-store range reads, cache
  hydrate/open/evict flows, compaction stale-document paths, analyzer-specific
  query behavior, and unioned split directory behavior.
- `search/vector`: HNSW scenario tests around stale entry points, deleted
  vectors, dirty-row caches, snapshot lookup, missing upper-layer rows, and
  adaptive traversal boundaries.

Suggested `search/mod.rs` test group split:

- naming and metadata keys: `vector_index_name`, `text_index_name`,
  multitenant name/prefix helpers, manifest/live-state/version/guard keys, and
  node-vs-edge hash prefix stability
- property values: secondary-indexable value gating, `property_value_to_index_string`
  for every variant, unique node equality identity for supported and rejected
  variants, and invalid roaring bitmap decode
- node equality/range indexes: add/remove/lookup, empty bitmap deletion,
  prefix scans with `limit == 0`, filtered scans, tenant scope isolation, and
  malformed stored bitmap errors
- bounded range scans: inclusive/exclusive/unbounded ranges, ascending vs
  descending direction, invalid encoded key/value skip behavior, and limit
  truncation
- node property orchestration: `update_indexes_for_property`,
  `remove_indexes_for_property`, batched property updates/removals, wrong label
  no-ops, missing `$label`, nested array/object rejection, and unique-index
  conflicts against stale/missing stored node rows
- edge label indexes: out/in/both label lookup, self-loop handling, global label
  index add/remove/clear, tenant scope isolation, and empty-result behavior
- edge equality/range/pair indexes: add/remove/lookup in both directions,
  global edge equality/range scans, bounded range scans with source/target
  filters, endpoint storage/delete, property-by-id storage/delete, and
  full-edge index removal idempotence

### `execution`

Most stream/projection/value submodules are already near complete. The
remaining work should focus on high-value interpreter boundaries:

- `execution/interpreter/ddl/secondary.rs`: completed all reachable pending-job
  orchestration and physical node/edge equality/range create/drop behavior.
- `execution/interpreter/access`: indexed access, range bounds, expansion
  direction/label branches, and storage error/miss propagation.
- `execution/interpreter/mutation`: node/edge mutation contract edge cases,
  especially label/property shape rejection and index-maintenance side effects.

### `config`

The config layer is a good place for exhaustive constructor and serde tests.
Tests should prove invalid states stay unrepresentable at boundaries:

- blank labels/properties/tenant properties and internal separator rejection
- vector dimension and parameter normalization, including non-finite floats
- text analyzer/positions serde round-trips
- dynamic index key conversion and planner snapshot projection
- cache budget/warm-mode combinations and invalid raw values

### `encoding`

Encoding is at 99.63%, with all reachable parser and malformed-input contracts
covered. The 23 reported misses are classified rather than actionable:

- 7 typed vector-key defensive assertion or impossible propagation lines
- 7 range-key assertion or post-length-check read lines
- 2 equality-key post-length-check/assertion lines
- 2 vector-value overflow propagations reachable only on 32-bit targets
- 5 guarded or coverage-mapped tails across generic keys/indexes, edge labels,
  property decoding, and property views

### Facades and single-file services

- `lib.rs`: open/init variants, startup catalog loading, vector-memory refresh,
  tenant scope behavior, storage acquisition errors, and runtime facade methods.
- `query_service.rs`: warm-mode validation, parameter validation, error
  serialization, stream/scalar response shape, and query execution facade paths.
- `secondary_backfill.rs`: persisted job lifecycle, status transitions, scan
  bounds, resume keys, JSON compatibility, and transaction-backed write/delete.
- `id_allocator.rs`: already high coverage; leave for the tail pass unless
  coverage shows a real allocator invariant gap after the integration suite is
  wired in.

## Function-level clusters from coverage

These are the largest uncovered executable clusters after grouping uncovered
lines by the nearest preceding function or impl boundary. Treat the grouping as
an approximation for prioritization, then verify against annotated coverage
before implementing tests.

| Cluster | Missing | Plan |
|---|---:|---|
| `search/vector/index.rs::search_layer0_with_simhash` | 154 | Add scenario tests with and without simhash cache hits, missing simhash rows, sampled neighbor deferral, exact fallback, empty frontier, and adaptive threshold transitions. |
| `search/mod.rs::edge_range_scan_bounds_with_direction` | 126 | Add pure/unit tests for out/in directions, asc/desc physical direction, inclusive/exclusive lower and upper bounds, unbounded bounds, source/target filters, and prefix-end caps. |
| `search/vector/index.rs::relink_neighbor` | 96 | Add delete/relink scenarios where replacement candidates are present, missing, stale, duplicated, or self-referential, including upper-layer and layer-0 variants. |
| `search/vector/index.rs::add_bidirectional_link` | 49 | Add insertion tests that force neighbor pruning, mutual reverse updates, duplicate links, and saturated neighbor sets. |
| `search/text/cache.rs::open_remote_split_generation` | 49 | Add object-store-backed cache tests for valid remote split open, bad footer, missing blob, wrong digest/size, multiple splits, and analyzer/field lookup failure. |
| `search/text/cache.rs::get_or_load_generation` | 48 | Cover memory hit, in-flight dedupe, disk hit, remote fallback, load-error stats, and concurrent callers. |
| `search/text/mod.rs::migrate_legacy_manifest_to_split_set_scoped` | 47 | Add manifest migration tests for legacy v1, already-v2 no-op, tenant scope, persisted replacement, and malformed persisted manifest. |
| `config/runtime_catalog.rs::dynamic_index_definition_from_drop_spec` | 16 | Reachable create/drop variants, uniqueness mismatches, exact semantic-definition recovery, and missing indexes are covered; remaining lines propagate constructor errors blocked by typed planner/catalog inputs. |
| `execution/interpreter/mutation/node.rs::set_node_property` | 37 | Add mutation tests for setting `$label`, replacing indexed properties, missing node rows, empty property sets, and index maintenance failures. |
| `search/vector/index.rs::search_with_stats` | 35 | Cover `k == 0`, missing metadata, empty index, exact search fallback, stats fields, and read-only memory-store interactions. |
| `lib.rs::open_reader_inner` | 30 | Add reader-open tests for configured storage, cache setup, runtime catalog loading, warm modes, and writer-only error surfaces. |
| `encoding/v1/keys/tenant.rs::decode_crockford_base32` | 28 | Add malformed ULID/base32 tests for invalid characters, lowercase handling if intended, overflow, length mismatch, and boundary values. |
| `search/text/cache.rs::after_successful_search` | 22 | Cover no disk cache, multi-split skip, local artifact metadata update, existing artifact metadata update, and background hydration path. |
| `lib.rs::refresh_vector_memory_stores` | 14 | Cover shutdown-before-load, budget admission, empty stores, reader vs writer storage, and fallback runtime index discovery. |

## Test placement plan

Keep tests near the contract they exercise unless an integration path is needed
to build realistic storage state.

| Area | Existing placement | Add tests in |
|---|---|---|
| Orphaned integration suite | `db/tests/lib/mod.rs` is not discovered | First add a discovered `db/tests/lib.rs` harness or move the file; then rerun coverage before adding more broad integration tests. |
| Search naming, property values, node/edge indexes | Inline `db/src/search/mod.rs` tests currently cover only a few helpers | Extend `db/src/search/mod.rs` tests for pure helpers and storage helpers using in-memory SlateDB transactions. |
| Text union directory | No tests; file is 0% covered | Add inline tests to `db/src/search/text/union_directory.rs`. |
| Text cache | Existing tests in `db/src/search/text/cache.rs` cover generation key, validation, eviction, warm dedupe, and cleanup | Extend the same module for cache state, remote/disk load, open generation, after-search metadata, and error stats. |
| Text split/storage/compaction | Existing tests in `split.rs`, `storage_directory.rs`, `byte_range_cache.rs`, `compaction.rs`, `warmup.rs` are narrow | Extend each owning module; use object-store memory fixtures for range and footer errors. |
| Vector index | Large inline test module in `db/src/search/vector/index.rs` | Extend inline tests; prefer scenario tests that force private helper paths rather than testing helpers through artificial visibility. |
| Config | `db/src/config/tests.rs` centralizes config tests | Extend this file for constructors, serde, runtime catalog projection, and dynamic catalog drop keys. |
| Secondary DDL integration | `db/src/execution/interpreter/ddl/tests/secondary.rs` and `secondary_scoped.rs` | Extend integration-style tests for planner/interpreter-visible behavior; add inline tests in `ddl/secondary.rs` only if private helper branches cannot be reached cleanly. |
| Secondary backfill contracts | Inline tests in `db/src/secondary_backfill.rs` | Extend inline tests for persisted job JSON, status transitions, scan bounds, and batch row helpers. |
| Query service | Inline tests in `db/src/query_service.rs` | Extend inline tests for folded streams, count/bool/scalar variants, read/write coercion, writer-mode rejection, scoped service wrappers, and error conversions. |
| DB facade/runtime | Inline tests in `db/src/lib.rs`; broad orphaned integration suite may already cover much of this | After wiring `db/tests/lib.rs`, add only focused inline tests for cache/open/runtime catalog branches still uncovered. |
| Encoding tail | Existing inline tests plus `tests/encoding_only.rs` | Add only targeted parser/malformed-input tests in the owning encoding modules; avoid broad duplicate round-trip tests. |

## Detailed subsystem backlog

This section expands the per-file table into implementation-ready backlog
items. The intent is to keep future test additions systematic after the
integration harness is fixed and coverage is rerun.

### Config backlog

- `config/cache.rs`: completed; constructors, projections, defaults, warm modes,
  and memory/disk option combinations have direct coverage.
- `config/db.rs`: completed; typed and raw edge settings, defaults, validation,
  attribution, and builder contracts have direct coverage.
- `config/indexes.rs`: completed all reachable serde, dynamic catalog,
  insert/remove, planner snapshot, scoped split/lookup, iterator, and public
  builder contracts. The 13 misses require an impossible zero `m0`, descending
  equality definitions, or invalid planner keys from validated definitions.
- `config/runtime_catalog.rs`: create/drop conversion, scope handling, wrong
  uniqueness on drop, missing index errors for each drop family, and
  vector/text definition extraction are covered. The 16 remaining `?` error
  tails are blocked by validated planner/catalog inputs unless a future
  malformed-fixture boundary is introduced intentionally.
- `config/utils.rs`: fully covered by display/source, path wrapper, and
  conversion/accessor tests.

### Encoding backlog

- `encoding/v1/indexes/equality.rs`: prefix/accessor contracts and malformed
  global equality parser paths are now covered. The two misses are a read after
  exact-length validation and an assertion-only `let ... else` panic.
- `encoding/v1/indexes/label.rs`: prefix/accessor contracts, wrong-prefix
  cases, neighbor direction rejection, and trailing-byte rejection are now
  covered. The sole miss is a read after exact-length validation.
- `encoding/v1/indexes/mod.rs`: completed all reachable generic key-prefix,
  index-prefix, and parser dispatch contracts. The sole reported miss is an
  `unreachable!` arm guarded by the immediately enclosing `0x03 | 0x06` match.
- `encoding/v1/indexes/range.rs`: range key prefix/accessor contracts and
  global edge range malformed buffers are now covered. The seven misses are six
  assertion-only `let ... else` panics and one read after exact-length
  validation.
- `encoding/v1/indexes/scan_prefixes.rs`: completed; all node, edge, and
  global-edge range prefix variants and exclusive/inclusive end-bound wrappers
  have exact byte-layout coverage.
- `encoding/v1/keys/mod.rs`: completed all reachable generic key dispatch,
  fixed-key prefix, and physical tenant-scope parser contracts. The sole
  reported miss is a propagated `MetadataKey` error that cannot occur after
  generic prefix classification has already accepted `0xff`.
- `encoding/v1/keys/tenant.rs`: completed; Crockford ULID alphabet, aliases,
  malformed characters, overflow, length checks, and tenant prefix isolation are
  covered.
- `encoding/v1/keys/vectors.rs`: completed accessor/length coverage and the
  concrete parser matrix for all typed vector-key shapes. The seven reported
  misses are defensive assertion failures or a propagated error that cannot be
  reached after the generic parser has validated the exact prefix shape.
- `encoding/v1/values/edges.rs`: completed; the const empty-value constructor,
  public typed bytes, normal encoder, compatibility decoders, and malformed
  length boundaries all have direct coverage.
- `encoding/v1/values/vectors.rs`: completed for this 64-bit target. The two
  misses propagate count overflow from a wire `u32`; that multiplication cannot
  overflow `usize` on 64-bit systems.
- `encoding/v1/property/mod.rs`: all reachable encode/decode paths are covered;
  the remaining deserialize error follows successful rkyv validation.
- `encoding/v1/property/property_view.rs`: both reusable input forms cover
  empty, valid, and invalid data. The sole line miss is the closing brace after
  successful validation as mapped by LLVM coverage.

### Execution backlog

- `execution/interpreter/access/dispatch.rs`: cover node and edge access
  dispatch variants that currently miss branches: label access, ids from
  variables/params, index access, search access, count/scalar conversions, and
  search result truncation.
- `execution/interpreter/access/expand.rs`: production coverage complete; the
  four report misses are intentional panic arms in test result extractors.
- `execution/interpreter/access/indexes.rs`: completed reader, writer, and
  active-transaction adapter coverage for every index lookup contract.
- `execution/interpreter/access/kv.rs`: add tests for each `KvReadPlan`
  variant, invalid element id parsing, element prefix end for max prefix bytes,
  missing rows, edge reads, and count/limit behavior.
- `execution/interpreter/access/range.rs`: complete across bound shapes,
  direction, limits, and reader/writer storage dispatch.
- `execution/interpreter/access/search/input.rs`: cover text query extraction
  from dynamic/static values, missing values, wrong value shapes, zero limits,
  parameterized limits, and vector component coercion errors.
- `execution/interpreter/access/search/storage.rs`: cover missing text
  manifests, manifest load errors, vector index construction, tenant-scoped
  storage, and search storage fallback branches.
- `execution/interpreter/ddl/secondary.rs`: completed physical node/edge
  equality/range create/drop, descending range indexes, malformed endpoint and
  property rows, unsupported nested values, wrong labels, existing catalog
  short-circuits, duplicate enqueue/wakeup, and missing-job idempotence. Revisit
  only if typed key construction or raw prefix scans change.
- `execution/interpreter/ddl/text.rs`: reachable tenant partition collection
  branches are covered. Remaining misses require malformed typed prefix-scan
  rows, validated serialization/postcondition failures, or transaction-layer
  write failures.
- `execution/interpreter/ddl/vector.rs`: cover tenant partition collection for
  node and edge vectors, drop partition discovery, absent tenant properties,
  invalid vector shapes, and cleanup of multiple physical vector partitions.
- `execution/interpreter/mutation/contracts.rs`: completed edge property index
  removal for equality and both range directions, plus unlabeled no-op behavior.
- `execution/interpreter/mutation/adjacency.rs`: completed bidirectional add,
  remove, empty-delete, and nonempty-rewrite behavior.
- `execution/interpreter/mutation/node.rs`: add tests for `set_node_property`
  label changes, absent-property removal, missing-node deletion, incoming-edge
  cleanup, all-node targeting, and reader/transaction existence checks are
  covered. The sole reported miss is a closing-brace mapping artifact.
- `execution/interpreter/mutation/edge.rs`: cover the remaining delete-edge and
  property mutation branches for missing endpoint rows, already-deleted edges,
  and absent properties are covered. The sole reported miss is a closing-brace
  mapping artifact.
- `execution/interpreter/search_index/text/apply.rs`: cover append/dead-mark
  version-state errors, malformed counters, and compaction skip/failure behavior
  are covered. Remaining lines are impossible serialization failures, low-level
  transaction write failures, and an empty manifest from a non-empty batch.
- `execution/interpreter/search_index/vector/document.rs`: add document
  extraction is complete for partition naming, tenant gating, non-numeric
  components, dimension mismatch, integer/float coercions, and missing values.
  The report has no missing source lines and retains three region artifacts.
- `execution/interpreter/test_support.rs`: leave for tail unless these helpers
  are intentionally public to tests; otherwise coverage here can be satisfied
  naturally by adding behavioral tests that use currently unused helper paths.

### Search text backlog

- `search/text/bundle_storage.rs`: add object-store bundle storage tests for
  missing footer entries, range beyond file length, empty ranges, object-store
  read failures, `get_all`, `file_num_bytes`, and `Debug` are covered. Remaining
  source lines are `u64`-to-`usize` overflow paths on 32-bit targets.
- `search/text/byte_range_cache.rs`: completed; keep future additions limited to
  regressions for new behavior.
- `search/text/cache.rs`: add tests for `search_manifest`, candidate search,
  snapshot with and without disk root, `get_or_load_generation` hit/miss/error
  counters, `open_remote_split_generation`, disk artifact hydration,
  generation insertion/eviction, disk cleanup join failures where practical,
  after-search metadata writes, and direct-directory open across multiple
  splits.
- `search/text/caching_directory.rs`: file-handle reads, precovered and partial
  cache paths, fallback reads, `open_read`, `atomic_read`, `exists`, missing
  files, `watch`, `Debug`, and read-only operations are covered. No production
  source lines remain missing.
- `search/text/compaction.rs`: completed merge-only live and stale-only
  orchestration, stale-document collection/deletion, split selection, synthetic
  metadata validation, output pruning, manifest persistence/deletion, and
  multi-split merge ordering. Remaining misses require low-level transaction,
  filesystem, or Tantivy fault injection plus infallible serialization tails.
- `search/text/debounced_storage.rs`: cover successful shared in-flight
  request, error propagation to waiters, `get_all`, `file_num_bytes`,
  `Debug`, and non-overlapping concurrent requests.
- `search/text/debug_proxy_directory.rs`: sync/async read recording, missing
  paths, `exists`, `watch`, lock, `Debug`, and explicit read-only methods are
  covered. No production source lines remain missing.
- `search/text/hot_directory.rs`: completed hotcache write/read/flush/open paths,
  cache-file corruption, static slice cache bounds, missing entries, writer
  failures, read-only methods, and real-index roundtrip. Remaining misses require
  impossible serialization failures or a purpose-built faulting Tantivy
  directory; defer unless reusable directory failure injection is introduced.
- `search/text/mod.rs`: add tests for legacy manifest migration, text document
  collection from node and edge readers, definition validation for missing
  labels/properties/tenant values, live-state persistence, blob copying and
  deletion, analyzer registration, warm-up term dictionary fields, and blob
  hash parsing.
- `search/text/overlay_directory.rs`: read precedence, delete error conversion,
  missing deletes, `exists`, `watch`, lock, and write routing are covered. The
  two remaining source lines require a custom directory that fails `exists`.
- `search/text/split.rs`: completed valid and invalid bundle construction,
  footer metadata and cache decoding, hotcache/footer bounds, all persisted
  reference validation, file/byte opens, local range bounds, and malformed
  bundle contracts. Remaining misses are 32-bit overflow and injected file I/O
  failures after prior operations on the same file succeed.
- `search/text/storage_directory.rs`: async reads, sync rejection, `atomic_read`,
  `exists`, missing/error files, `get_all`, `len`, `watch`, lock, `Debug`, and
  explicit read-only operations are covered. No production source lines remain
  missing.
- `search/text/union_directory.rs`: schema/settings/segment validation,
  conflicting payloads, split routing, missing files, and read-only operations
  are covered. Remaining lines are metadata-load/serialization failures and a
  synthetic-meta absence blocked by the validated non-empty directory loop.
- `search/text/warmup.rs`: direct contracts completed; only retain-closure
  lines in `simplify` remain marked uncovered by coverage.

### Search vector backlog

- `search/vector/index.rs`: add scenario tests for layer-0 simhash search,
  greedy/beam upper-layer search, live entry candidate lookup, HNSW insertion
  with saturated neighbors, bidirectional link repair, deletion/relink paths,
  cached-neighbor flush/eviction, stale metadata entry points, pending dirty
  rows, read-only memory-store behavior, `k == 0`, missing metadata, and stats
  fields.
- `search/vector/memory_store.rs`: cover dirty-row tracking accessors, clear,
  lock publish/acquire-all, scope/index-id accessors, load from reader with
  unsupported rows, budget admission, shutdown during scan, and remove-node
  paths that leave no rows behind.
- `search/vector/mod.rs`: completed all reachable config, query-parameter,
  decoding, layer-selection, diversity fallback, and local codec contracts.
  The five remaining lines are assertion-only `let ... else` panics in typed
  key round-trip tests.
- `search/vector/simhash.rs`: cover counted lookup stats, cache
  `get_or_compute`, explicit get/delete behavior, custom hasher construction,
  display formatting, and merge/reset of filter stats.
- `search/vector/spaces/simple.rs`: cover scalar fallback for binary-quantized
  dot product, SIMD detection branches where target features allow, empty
  vectors, and dimension mismatch behavior.

### Facade, service, and allocator backlog

- `lib.rs`: after wiring the orphaned integration tests, rerun coverage before
  adding focused tests for reader open, disk/object-storage source splitting,
  Slate hybrid cache construction errors, runtime catalog persistence restore
  after commit failure, dynamic vector-memory store create/drop, FTS warm modes,
  vector-memory refresh shutdown/budget branches, catalog discovery from
  storage, and dynamic catalog key encoding.
- `query_service.rs`: cover all public wrapper methods, scoped execution,
  read/write batch coercion, writer-mode rejection on read-only DBs, folded
  stream/count/bool/scalar/object serialization, `From<QueryServiceError>`,
  transaction-conflict detection, and JSON serialization error paths where
  constructible.
- `secondary_backfill.rs`: completed transaction-backed scan batches and
  node/edge equality/range writes, plus wrong labels, missing properties,
  nested values, sort keys, write-maintenance config, failed jobs, strict JSON,
  scan-prefix boundaries, and scoped metadata isolation. Revisit only if typed
  constructors or transaction failure injection make the classified tails
  reachable.
- `id_allocator.rs`: defer until the main gaps close; remaining misses appear
  to be rare allocator branches such as persisted-watermark load errors,
  `remaining_in_lease`, small batch boundaries, and shutdown/concurrency edge
  cases.
- `search/contracts.rs`: add constructor/accessor tests for node and edge
  update/removal request structs, empty catalogs, and catalog combinations
  used by mutation/index maintenance.
- `error.rs` and facade modules with 0%: cover display/source/from conversions
  if they encode real behavior; otherwise consider excluding pure re-export
  facades from coverage after documenting the rationale.

## Saturated and tail files

The coverage report shows 85 `db/src` files at 100% line coverage.
Do not spend first-pass implementation time here unless a behavior audit finds
a missing edge case that line coverage cannot see:

```text
config/cache.rs
config/db.rs
config/mod.rs
config/utils.rs
encoding/v1/indexes/scan_prefixes.rs
encoding/v1/keys/keys.rs
encoding/v1/keys/metadata.rs
encoding/v1/keys/tenant.rs
encoding/v1/mod.rs
encoding/v1/property/property.rs
encoding/v1/property/property_value.rs
encoding/v1/values/edges.rs
encoding/v1/values/mod.rs
execution/interpreter/access/dispatch.rs
execution/interpreter/access/indexes.rs
execution/interpreter/access/range.rs
execution/interpreter/access/search/definitions.rs
execution/interpreter/access/search/dispatch.rs
execution/interpreter/access/search/input.rs
execution/interpreter/access/search/limits.rs
execution/interpreter/access/search/storage.rs
execution/interpreter/access/search/tenant.rs
execution/interpreter/control/branch.rs
execution/interpreter/control/repeat.rs
execution/interpreter/control/support.rs
execution/interpreter/dependencies.rs
execution/interpreter/dispatch.rs
execution/interpreter/mutation/adjacency.rs
execution/interpreter/mutation/contracts.rs
execution/interpreter/mutation/properties.rs
execution/interpreter/mutation/tx.rs
execution/interpreter/reserved.rs
execution/interpreter/reserved/fold.rs
execution/interpreter/reserved/path.rs
execution/interpreter/reserved/sack.rs
execution/interpreter/runtime_context.rs
execution/interpreter/search_index/properties.rs
execution/interpreter/search_index/text/change.rs
execution/interpreter/search_index/text.rs
execution/interpreter/search_index/text/outcome.rs
execution/interpreter/search_index/vector.rs
execution/interpreter/search_index/vector/change.rs
execution/interpreter/state.rs
execution/interpreter/stream.rs
execution/interpreter/stream/aggregate.rs
execution/interpreter/stream/bounds/dispatch.rs
execution/interpreter/stream/bounds/eval.rs
execution/interpreter/stream/bounds/rows.rs
execution/interpreter/stream/eval/expr.rs
execution/interpreter/stream/eval/numeric.rs
execution/interpreter/stream/eval/params.rs
execution/interpreter/stream/eval/property.rs
execution/interpreter/stream/eval/predicate.rs
execution/interpreter/stream/eval/sets.rs
execution/interpreter/stream/order.rs
execution/interpreter/stream/projection/bindings.rs
execution/interpreter/stream/projection/dispatch.rs
execution/interpreter/stream/projection/helpers.rs
execution/interpreter/stream/projection/rows.rs
execution/interpreter/stream/projection/scalar.rs
execution/interpreter/stream/sets/merge.rs
execution/interpreter/stream/sets/variables.rs
execution/interpreter/stream/values/conversion.rs
execution/interpreter/stream/values/params.rs
execution/interpreter/stream/values/scalars.rs
execution/interpreter/subplan.rs
id_allocator.rs
query_service.rs
error.rs
search/contracts.rs
search/text/byte_range_cache.rs
search/vector/distance/binary_quantized_euclidean.rs
search/vector/distance/binary_quantized_manhattan.rs
search/vector/distance/cosine.rs
search/vector/distance/euclidean.rs
search/vector/distance/hamming.rs
search/vector/distance/manhattan.rs
search/vector/distance/mod.rs
search/vector/item.rs
search/vector/unaligned_vector/binary_quantized_test.rs
search/vector/unaligned_vector/binary_test.rs
search/vector/unaligned_vector/codec_test.rs
search/vector/unaligned_vector/f32.rs
search/vector/unaligned_vector/mod.rs
search/vector/unaligned_vector/simhash_test.rs
```

Tail-pass files with three or fewer missed executable lines:

- 1 missed line: `encoding/v1/property/mod.rs`, `execution/interpreter/ddl.rs`,
  `execution/interpreter/search_index/text/document.rs`,
  `search/vector/distance/binary_quantized_cosine.rs`
- 2 missed lines: `encoding/v1/values/vectors.rs`,
  `execution/interpreter/mod.rs`, `execution/interpreter/scheduler.rs`
- 3 missed lines: `search/vector/spaces/simple_neon.rs`

Handle these after the large gaps. Some may be re-export/facade lines or
platform-specific branches; verify with annotated coverage before writing
tests solely to hit a line.

## Per-file uncovered-line inventory

Files with 0 uncovered lines are omitted from this table.
This table is summarized from the latest JSON report at
`/private/tmp/helix-proper-db-coverage-search-contracts-closed-3.json`; lower-tail rows may move
slightly between focused coverage passes.

| File | Lines | Covered | Missing | Notes |
|---|---:|---:|---:|---|
| `search/vector/index.rs` | 83.67% | 3439/4110 | 671 | high-value vector scenario gaps |
| `search/text/mod.rs` | 64.82% | 1124/1734 | 610 | text indexing/search orchestration |
| `search/text/cache.rs` | 63.46% | 1056/1664 | 608 | FTS cache warm/disk/remote branches |
| `lib.rs` | 67.64% | 880/1301 | 421 | facade/open/runtime branches |
| `search/mod.rs` | 99.12% | 4928/4972 | 44 | typed scan skips and wrapper tails |
| `execution/interpreter/ddl/secondary.rs` | 99.23% | 1033/1041 | 8 | typed invariants and test panic arms |
| `search/text/compaction.rs` | 88.69% | 863/973 | 110 | storage/Tantivy injection and invariant tails |
| `secondary_backfill.rs` | 98.75% | 1023/1036 | 13 | typed invariants and two test panic arms |
| `search/text/split.rs` | 93.30% | 863/925 | 62 | overflow and injected file I/O tails |
| `search/text/hot_directory.rs` | 92.56% | 597/645 | 48 | serialization and injected directory failures |
| `execution/interpreter/search_index/text/apply.rs` | 97.37% | 445/457 | 12 | serialization/storage-injection invariant tails |
| `search/text/union_directory.rs` | 94.69% | 321/339 | 18 | validated-loop and metadata error tails |
| `execution/interpreter/search_index/vector/document.rs` | 98.86% | 260/263 | 3 | no missing source lines; region artifacts only |
| `execution/interpreter/test_support.rs` | 91.48% | 279/305 | 26 | test helper branches |
| `execution/interpreter/ddl/text.rs` | 94.82% | 366/386 | 20 | typed scan and storage failure tails; 16 source lines reported missing |
| `config/runtime_catalog.rs` | 95.02% | 305/321 | 16 | typed constructor error propagations |
| `search/text/bundle_storage.rs` | 94.44% | 153/162 | 9 | 32-bit platform overflow tails |
| `config/indexes.rs` | 98.72% | 999/1012 | 13 | invalid internal typed-definition states |
| `search/text/caching_directory.rs` | 98.78% | 242/245 | 3 | test-helper region artifacts; production complete |
| `search/text/storage_with_cache.rs` | 95.74% | 90/94 | 4 | cache-race invariant tail |
| `search/text/overlay_directory.rs` | 97.54% | 119/122 | 3 | injected `exists` errors and region artifact |
| `search/text/storage_directory.rs` | 98.18% | 162/165 | 3 | region artifacts; production complete |
| `search/text/debug_proxy_directory.rs` | 98.86% | 173/175 | 2 | region artifacts; production complete |
| `search/vector/unaligned_vector/simhash.rs` | 94.12% | 160/170 | 10 | simhash vector leftovers |
| `execution/interpreter/mutation/node.rs` | 99.75% | 403/404 | 1 | closing-brace region artifact |
| `execution/interpreter/mutation/edge.rs` | 99.74% | 386/387 | 1 | closing-brace region artifact |
| `execution/interpreter/mutation/ops.rs` | 99.69% | 322/323 | 1 | no missing source lines; region artifact only |
| `execution/interpreter/search_index/vector/apply.rs` | 98.98% | 97/98 | 1 | storage create-error propagation |
| `execution/interpreter/storage.rs` | 97.92% | 376/384 | 8 | storage facade rare errors |
| `encoding/v1/keys/vectors.rs` | 99.51% | 1428/1435 | 7 | defensive assertion and unreachable parser-error tails |
| `encoding/v1/indexes/range.rs` | 99.05% | 729/736 | 7 | exact-length invariant and assertion-only tails |
| `execution/interpreter/access/rows.rs` | 95.31% | 122/128 | 6 | row materialization leftovers |
| `search/vector/mod.rs` | 99.17% | 600/605 | 5 | assertion-only typed key test tails |
| `execution/interpreter/access/params.rs` | 98.77% | 321/325 | 4 | param rejection leftovers |
| `search/vector/unaligned_vector/binary.rs` | 97.39% | 149/153 | 4 | binary vector leftovers |
| `search/vector/unaligned_vector/binary_quantized.rs` | 97.40% | 150/154 | 4 | binary-quantized leftovers |
| `encoding/v1/indexes/equality.rs` | 99.44% | 357/359 | 2 | exact-length invariant and assertion-only tails |
| `encoding/v1/indexes/label.rs` | 99.52% | 209/210 | 1 | exact-length read invariant |
| `execution/interpreter/scheduler.rs` | 99.31% | 288/290 | 2 | intentional test panic arms |
| `search/vector/spaces/simple_neon.rs` | 96.55% | 84/87 | 3 | neon simple-space branch |
| `encoding/v1/values/vectors.rs` | 99.17% | 240/242 | 2 | 32-bit-only wire-count overflow paths |
| `execution/interpreter/mod.rs` | 94.12% | 32/34 | 2 | interpreter facade branch |
| `search/text/warmup.rs` | 99.30% | 284/286 | 2 | warmup query simplification |
| `encoding/v1/property/mod.rs` | 98.85% | 86/87 | validated deserialize error tail |
| `execution/interpreter/ddl.rs` | 98.21% | 55/56 | 1 | DDL dispatcher leftover |
| `execution/interpreter/search_index/text/document.rs` | 99.35% | 153/154 | 1 | impossible `Ok(None)` after text normalization |
| `search/text/debounced_storage.rs` | 99.17% | 120/121 | 1 | concurrent error/debounce branches |
| `search/vector/distance/binary_quantized_cosine.rs` | 96.15% | 25/26 | 1 | BQ cosine leftover |
| `encoding/v1/keys/mod.rs` | 99.83% | 601/602 | 1 | unreachable metadata parser propagation |
| `encoding/v1/indexes/mod.rs` | 99.72% | 354/355 | 1 | guarded unreachable prefix arm |
| `encoding/v1/property/property_view.rs` | 99.48% | 190/191 | 1 | coverage-mapped post-validation brace |

## Suggested implementation order

1. Defer the orphaned integration-suite migration.
   A discovery harness exposed 275 compile errors against retired planner and
   private DB APIs. Porting it is a major migration, so keep it disabled and
   track it separately rather than folding it into incremental coverage work.

2. Expand focused text storage and cache tests.
   `search/text/cache.rs`, `search/text/mod.rs`, and
   `search/text/compaction.rs` are now the largest text gaps. Target
   remote/disk cache misses, malformed footer files, object-store range
   failures, cleanup budget exhaustion, warm concurrency errors, and split
   compaction delete/stale-document behavior.

3. Treat config as complete for reachable behavior.
   Revisit only if planner/catalog types gain malformed-fixture constructors or
   config ownership changes.

4. Treat secondary DDL/backfill as complete for reachable behavior.
   Revisit only if typed job-key construction, raw prefix-scan ownership, or
   transaction failure injection changes.

5. Fill the remaining text directory tails.
   Union, overlay, hot, caching, and bundle/storage adapters now have direct
   coverage; the remaining lines are mostly less common error and fallback
   branches.

6. Expand vector index scenario tests.
   Prefer scenario tests around real insert/search/delete flows where possible,
   then fill remaining pure-function gaps in adaptive traversal and simhash
   helpers.

7. Clean up the small tail.
   Once the large modules are covered, use the per-file table to add direct
   tests for the remaining one-line to twenty-line gaps. Some facade modules
   may be better excluded from coverage if they are only re-export surfaces,
   but make that explicit rather than silently accepting 0% files.

## Verification commands for future implementation

Use temp target directories so coverage/build artifacts stay out of the repo:

```bash
CARGO_TARGET_DIR=/private/tmp/helix-proper-db-target cargo test -p db --all-targets
CARGO_TARGET_DIR=/private/tmp/helix-proper-db-coverage-target cargo llvm-cov -p db --all-targets --json --output-path /private/tmp/helix-proper-db-coverage.json --ignore-filename-regex '/(registry|rustc)/'
CARGO_TARGET_DIR=/private/tmp/helix-proper-db-clippy-target cargo clippy --workspace -- -D warnings
rustfmt --edition 2024 --check
```
