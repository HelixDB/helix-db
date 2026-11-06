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

Cargo discovers these targets:

| Target | Kind | Source | Role |
|---|---|---|---|
| `db` | `lib` | `crates/db/src/lib.rs` | Unit-test behavior, including contracts over private internals whose bodies live in `crates/db/tests/unit/` and are path-included by their owning modules. Unit tests that need `index-lifecycle-testing` live in `lifecycle_testing_tests` modules. Tests that arm process-global failpoints, environment variables, or locks rerun themselves alone in a child process of the test binary. |
| `bench_internals_contracts` | `test` | `crates/db/tests/bench_internals_contracts.rs` | Requires `bench-internals`. Checks that the vector batch and FTS prefilter benchmark fixtures reject invalid cases, run every metric, workload, layout and strategy, and match the FTS fixture's exact oracle. |
| `cypher_scaling` | `example` | `crates/db/examples/cypher_scaling.rs` | Runs local release graph workloads with deterministic result, planning-work, storage-read, and memory guards; latency measurements are reported separately. |
| `concurrent_request_throughput` | `bench` | `crates/db/benches/concurrent_request_throughput.rs` | Measures request throughput and latency percentiles for concurrent indexed reads, writes, and a 90/10 mix in one scope; under `cargo test` it runs short smoke rounds that fail on any request error. |
| `embedded_write_latency` | `bench` | `crates/db/benches/embedded_write_latency.rs` | Measures acknowledged-write latency for in-memory and on-disk embedded storage. |
| `edge_stream_drop` | `test` | `crates/db/tests/edge_stream_drop.rs` | Exercises edge-stream deletion, empty results, traversal selection, transaction rollback, replacement batches, and node cascades through the public query boundary. |
| `fts_prefilter` | `bench` | `crates/db/benches/fts_prefilter.rs` | Requires `bench-internals`. Measures exact collector-only FTS prefilter latency, allocations, and object-store reads across release fixtures. |
| `hybrid_cache_reuse` | `test` | `crates/db/tests/hybrid_cache_reuse.rs` | Reopens and closes a hybrid-cache reader repeatedly and asserts every reopen reads through the process's one Foyer device, so exactly one set of partition files stays open across reopens, and that no other descriptor outlives a closed reader. Counts `/proc/self/fd` and skips without procfs; it is its own target so no concurrent test perturbs the count. |
| `index_lifecycle_contracts` | `test` | `crates/db/tests/index_lifecycle_contracts.rs` | Requires `index-lifecycle-testing` and runs deterministic family-shape, backfill-mutation, concurrent-create, and repeated-recoverable-fault acceptance contracts through the installed production drivers. Late vector/text writes at every build stage enqueue complete operations through the real producer and ledger; the contracts require that they never send a build back to catch-up, survive reopen, are visible to strong searches before publication, and match after explicit publication. Text builds whose scanned entities are deleted, re-texted, moved, or un-indexed before partition construction activate and match an independent build's hits and scores. |
| `mutation_count_visibility` | `test` | `crates/db/tests/mutation_count_visibility.rs` | Covers counts of a write's own rows through the public query boundary: they return their rows without flushing deferred index maintenance, later index-backed counts and expansions in the same batch still see every staged write, and a unique conflict between counted writes fails the commit and writes nothing. |
| `production_contracts` | `test` | `crates/db/tests/production_contracts.rs` | Imports the compiled `db` library without `cfg(test)` and covers the public query/interpreter boundary, including graph behavior, source-fed node/edge mutations, all typed KV read modes, explicit comparison/membership shapes, limited and empty access, branch partition and mixed-row invariants, nested branch-context restoration, unbound conditional variables, scalar and operator-specific predicate-error propagation, repeat-predicate and shortest-path missing/ambiguous/revisited/isolated endpoint shapes, nested and edge-endpoint property paths, typed/transport ID parameters and variable ID-source shapes, label mutation/no-op/cascade plus empty/unlabeled deletion invariants, malformed requests, dynamic and typed AST `FOREACH`, complete literal/runtime property-value conversion, stable parallel-stage execution on a public reader, scalar/malformed multi-dependency composition, ordinary/folded/scalar stream-operation shapes, scalar aggregate and aggregate-after-fold shape contracts, typed and rejected dynamic bounds, empty intersection, fail-closed index-lifecycle value consumption, shared-runtime vector/text search and DDL availability, chained injection, sparse/selected/binding projections, vector-distance metadata and search-label projections, typed vector-array conversions through the live planner context, missing-property ordering, mixed/empty aggregates, expression value/error edges, managed secondary/range/search read-your-writes, range-index-delivered order preservation, same-transaction labeled/unfiltered out/in/both topology reads across parallel differently labeled edges, and index-served source filters and counts (null, runtime, range-only and unique equality, long IN lists, and point, runtime, label and text-search count sets) checked against an unindexed database. |
| `projection_materialization` | `bench` | `crates/db/benches/projection_materialization.rs` | Deterministic 512-node/512-edge Divan projection baseline with allocation profiling; setup and planning are excluded from measurement. |
| `property_row_reads` | `bench` | `crates/db/benches/property_row_reads.rs` | Measures filtered counts, one-field projections and full-row reads over 2,000 wide nodes (text body and embedding) with allocation profiling; setup and planning are excluded from measurement. |
| `production_cypher_contracts` | `test` | `crates/db/tests/production_cypher_contracts.rs` | Imports the compiled library and checks public Cypher query, mutation, transaction, index, resource, and lossless-result contracts. Its submodules under `crates/db/tests/cypher/` remain in this shared test executable. |
| `production_index_delete_regressions` | `test` | `crates/db/tests/production_index_delete_regressions.rs` | Covers retroactive equality, range, vector, and text index deletion for entities without the indexed property, including tenant partitions without physical mappings and incident-edge cascade deletion. |
| `production_index_operation_queue` | `test` | `crates/db/tests/production_index_operation_queue.rs` | Requires `index-lifecycle-testing`. With explicit scheduling it covers queue tuning and lag histogram contracts, restart rediscovery, retired-generation discard, hidden-build deferral, blocked-build admission without a blocking entity, and the strong vector search bound (retryable `pending_vector_bytes` backpressure in read and write requests until publication, eventual searches and a write's own changes exempt) through the public API. The ledger, reconciliation, codec, and fail-closed open contracts are `index_lifecycle::queue` unit tests. |
| `production_index_lifecycle_scale` | `test` | `crates/db/tests/production_index_lifecycle_scale.rs` | Requires `production-scale`; non-ignored fixed 100k production-entry builds for non-unique/unique secondary, 128D f32 vector, paged text, and a 100k workload distributed across 16 tenant scopes, followed by public search oracles and lifecycle cleanup. A fixed 8k vector create/search/drop/residue contract runs independently in bounded nightly CI. Separate maximum-batch contracts cover typed limit blocking/reopen/retry/abort for every family. |
| `production_vector_migration_scale` | `test` | `crates/db/tests/production_vector_migration_scale.rs` | Requires `production-scale`; runs a process-isolated fixed 100k vector migration resource contract plus ignored 1M and 10M release soaks. The target separation prevents unrelated lifecycle scale allocations from contaminating process-wide RSS measurements. |
| `production_row_mode_contract` | `test` | `crates/db/tests/production_row_mode_contract.rs` | Launches process-isolated public queries with valid and invalid `HELIX_ROW_MODE_MAX_ROWS` values, covering production environment parsing, cached cap activation, exact overflow errors, and malformed-setting rejection without mutating a multithreaded test process's environment. |
| `production_text_correctness_regressions` | `test` | `crates/db/tests/production_text_correctness_regressions.rs` | Requires `index-lifecycle-testing` and runs bounded text correctness regressions through the compiled library, observing durable state through the `migration_parity` diagnostics. |
| `production_text_queue_contracts` | `test` | `crates/db/tests/production_text_queue_contracts.rs` | Requires `index-lifecycle-testing`. Holds publication with explicit scheduling to prove that restricted, termless, null-tenant, eventual, and write-local overlays return the latest committed state. Restarts with automatic scheduling to prove Active compaction merges live splits, retires all-stale pages, and discards pointers left by dropped or rebuilt generations. Sweeps batch input and operation budgets, and lowers them mid-build, to prove text builds activate or block with typed limits, that an input budget below one manifest root blocks partition scans and abort cleanup until it is restored, and pauses builds at every validation lane to prove damaged pages, roots, corpus, entity states, markers, pre-queue build deltas, and split objects block validation, still abort, and recover by retry once an object is restored; a root that counts every addressable page blocks manifest preparation with a typed limit. |
| `production_vector_planner` | `test` | `crates/db/tests/production_vector_planner.rs` | Exercises managed DDL, planner publication, node/edge mutation maintenance, reopen, every active f32 metric, tenant-partition isolation, and exact magnitude rejection/rollback through public executable plans. Its closed lifecycle state machine adds brute-force search comparison, drop/recreate, query-planned status/retry/abort control over a typed blocked build and cleanup, runtime literal/expression tenant selection, fail-closed tenant-shape errors, and atomic partition moves/removals/reinsertions. |
| `public_encoding_compatibility` | `test` | `crates/db/tests/public_encoding_compatibility.rs` | Imports the compiled library with default features and verifies retained root, V1, and V2 property codec paths plus the public text live-state encoder and decoder. |
| `scoped_search_gate` | `test` | `crates/db/tests/scoped_search_gate.rs` | Requires `production-scale`; one ignored release-only gate on the synthetic `Group <- Item -> Attribute` fixture shared with the `scoped_search_bench` example. Every traversal-scoped vector shape must return only in-scope IDs, and its mean recall@k against a client-side exact scan must reach the floor of the restricted strategy it ran: 1.0 for the exact scan and 0.90 for the filtered graph walk. Recall is tie-aware: a returned ID within the f32 rounding bound of the k-th exact neighbour counts as a hit. The default fixture sends the kind-B scopes through the walk and the group scopes through the exact scan, and the gate fails unless both strategies ran. A post-expansion stored-property filter must count exactly the rows a client-side filter keeps. The fixture load waits for the async index queue to drain, and the gate asserts no pending index operation before recall, so no search overlays unpublished work. A non-ignored embedded contract loads 12 one-item batches against a pending-member limit of one item, so consecutive batches are backpressured for real; the load must retry at least once, complete, and return with no pending index operation. |
| `scoped_search_bench` | `example` | `crates/db/examples/scoped_search_bench/main.rs` | Loads the synthetic `Group <- Item -> Attribute` graph shared with `scoped_search_gate` and reports global and traversal-scoped vector and BM25 search latency, recall@k and embedded object-store GETs against a server, local-filesystem or S3 backend. It is a manual benchmark. |
| `storage_size` | `example` | `crates/db/examples/storage_size.rs` | Writes one deterministic graph or vector dataset once per SST block codec, to an embedded disk database or an S3-compatible bucket under the server default configuration, and reports stored bytes, one cold read, and optionally the size after garbage collection. It is a manual measurement. |
| `queue_acceptance_verify` | `example` | `crates/db/examples/queue_acceptance_verify.rs` | Existing-database acceptance runner for the index-operation queue, run manually against a copy of a preserved pre-queue fixture. |
| `unique_index_enforcement` | `test` | `crates/db/tests/unique_index_enforcement.rs` | Checks through the public query boundary that a node unique equality index admits at most one live node per value on every write path (separate requests, one batch, `for_each`, `set_property`, drop and reclaim, concurrent inserts and updates, reopen, Cypher), while the index builds (duplicates of indexed values rejected at write time, claims still enforced while a build is blocked), and that builds finish under steady writes and never block on a value that only moved between nodes. |
| `writer_fence_contract` | `test` | `crates/db/tests/writer_fence_contract.rs` | Proves a newer SlateDB writer claims its epoch before open returns and an already-open transaction from the old writer is rejected as fenced. |
| `secondary_equality_hot_path` | `bench` | `crates/db/benches/secondary_equality_hot_path.rs` | Requires `bench-internals`. Measures 50-index V4 equality write and read throughput, latency, and allocations. |
| `secondary_equality_read_scale` | `bench` | `crates/db/benches/secondary_equality_read_scale.rs` | Requires `bench-internals`. Measures equality lookup cost over the 10,000-node shared-value fixture. |
| `text_transaction_batching` | `bench` | `crates/db/benches/text_transaction_batching.rs` | Requires `bench-internals` and `index-lifecycle-testing`. Measures text transaction batching. |
| `vector_batch_insert` | `bench` | `crates/db/benches/vector_batch_insert.rs` | Requires `bench-internals`. Measures vector batch insertion. |
| `vector_memory_store` | `bench` | `crates/db/benches/vector_memory_store.rs` | Requires `bench-internals`. Measures resident vector-memory hydration from on-disk SSTs: retained heap against charged bytes, hydration time and allocations, and warm lookup latency. |
| `write_path_mutations` | `bench` | `crates/db/benches/write_path_mutations.rs` | Measures graph mutation write paths. |

`cargo test -p db --all-targets` skips targets that require
`index-lifecycle-testing`, `bench-internals`, or `production-scale`. The `DB
feature-gated contracts` job in `.github/workflows/rust-ci.yml` runs
each `index-lifecycle-testing` test target by name, plus the library's
`lifecycle_testing_tests` modules, so a new feature-gated target must be added
there. `bench-internals` exposes `db::bench_internals`, the internal fixtures
the benches drive, and nothing else; `production-scale` exposes
`db::production_scale` for the release soaks.

The `queue_acceptance_verify` example is an existing-database acceptance
runner: it opens a fixture written by the
pre-queue release through the normal open path and checks graph data,
secondary lookups, and strong/eventual vector and text search against exact
oracles before and after new queued writes, publication, and restart, plus
interrupted pre-queue builds (finish, or block and recover through abort and
recreate). It is run manually against a copy of a preserved fixture.

The vector library also owns the ignored, release-only diagnostic
`vector_search_scale_gate_reports_recall_and_median_throughput` contract in
`search/vector/hnsw/scale_contracts.rs`. It is not a separate Cargo target. Because it constructs a raw
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
targets are Cargo-discovered and run in CI.

## Asynchronous index-operation queue

Queue behavior is covered by `cfg(test)` modules of the `db` library rather
than separate Cargo targets:

| Module | Coverage |
|---|---|
| `encoding/v2/values/indexes/operation_queue/tests.rs` | Queue codec round trips, malformed values, a reference-model proptest of the merge algebra over every grouping, the per-value size bound that caps the retained-byte ceiling, and latest-state decodes that select each entity at its latest operation within a budget in both layouts, comparing entities by their raw bytes and decoding neither superseded nor unselected bodies. |
| `index_lifecycle/queue/codec_storage_tests.rs` | Real SlateDB merge behavior: conflicts, zero-read producer staging, flush, reopen, snapshots, reader replay, multi-level compaction measured through fresh readers, a replaying reader dropping an enqueue its new manifest cancelled, retained queues sharing one writer-wide budget, latest-state row reads, and the known limitation that a read below a pending operand merges the whole queue whatever its budget. |
| `index_lifecycle/queue/backlog/tests.rs` | Ledger reservation, exact limits, uncertain outcomes, reconciliation, and admission stamps that change when an operation is charged and again when its enqueue commit returns. |
| `index_lifecycle/queue/tests.rs` | Producer atomicity, collapse, ordering, limits, operand cap, restart reconstruction, and concurrent writers. |
| `index_lifecycle/queue/publication_tests.rs` | Vector publication: exact search, ordered convergence, prefix acknowledgement, concurrent updates, failure atomicity, payload-only publication, uncertain commits, restart recovery, tenant partition moves, acknowledgements bounded by the operand bound and by half of each output budget (row layout included), trimming at the crossing entity while reusing the queue the first attempt read, a single operation that cannot fit blocking without a trim in either layout, capped backoff for charged work that reads empty, exact charge release after a post-commit failure, and in-flight worker timers. |
| `index_lifecycle/queue/text_publication_tests.rs` | Batched text publication against an independent build's BM25 scores, one split per epoch, tenant moves, failure atomicity, acknowledgements bounded by the operand bound and by half of each output budget (row layout included), admission that keeps every document publishable, halving a batch that cannot fit, a single operation that cannot fit blocking without a trim in either layout, drains that alternate trimmed and published epochs from one queue read, draining from the queue the last commit or conflict retained until an attempt fails or commits uncertainly, held-back entities and their repairs (including a draining entity's retained state), and stalled generations that reread a retained queue before waiting and wake when a racing enqueue commit returns. |
| `index_lifecycle/queue/overlay_tests.rs` | Strong/eventual search overlays, eventual budgets (entities selected at their latest state or not at all, a decode bound, plus the known limitation that a read with pending operands resolves the whole queue), write-batch visibility, pending-only partitions, and eventual rejection for writes. |
| `index_lifecycle/queue/lifecycle_tests.rs` | Builds with writes on both sides of the scan cursor, restart during a build and after activation, index drop with discard across restart, build abort, discards bounded by the operand bound and filling the row-layout output budget, deferral, reconciliation, and discards that never wait for a step's generation ownership, retrying a generation retired while awaiting ownership, blocked builds that accept source repairs, and eventual searches under every budget cut after a build that never show a state older than its rows. |
