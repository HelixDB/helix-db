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
| `encoding_only` | `test` | `crates/db/tests/encoding_only.rs` | Unit-style encoding target. It includes `src/encoding` by path under a test crate and bridges production DTO modules rather than copying them, so it is deliberately excluded from production-only coverage. |
| `production_contracts` | `test` | `crates/db/tests/production_contracts.rs` | Imports the compiled `db` library without `cfg(test)` and seeds the production-only compatibility baseline. |
| `production_index_v2_contracts` | `test` | `crates/db/tests/production_index_v2_contracts.rs` | Requires `production-coverage` and runs the V2 outbox transition failpoint matrix plus the real secondary lifecycle state model against the compiled production library. |
| `production_index_v2_scale` | `test` | `crates/db/tests/production_index_v2_scale.rs` | Requires `production-scale`; non-ignored fixed 100k production-entry builds for non-unique/unique secondary, 128D f32 vector, paged text, and a 100k workload distributed across 16 tenant scopes, followed by public search oracles and lifecycle cleanup. |
| `production_internal_contracts` | `test` | `crates/db/tests/production_internal_contracts.rs` | Requires `production-coverage` and invokes feature-gated production module contracts without compiling inline unit-test code into the measured library. |
| `production_vector_planner` | `test` | `crates/db/tests/production_vector_planner.rs` | Exercises managed DDL, planner publication, node/edge mutation maintenance, reopen, and every active f32 metric through public executable plans. |
| `writer_fence_contract` | `test` | `crates/db/tests/writer_fence_contract.rs` | Proves a newer SlateDB writer claims its epoch before open returns and an already-open transaction from the old writer is rejected as fenced. |

The vector library also owns the ignored, release-only diagnostic
`vector_search_scale_gate_reports_recall_and_median_throughput` contract in
`search/vector/scale_contracts.rs`. It is not a separate Cargo target and is
excluded from production-only coverage. Because it constructs a raw
`VectorIndex` and writes physical rows directly, it is retained only as a
search-kernel regression and does not satisfy a V2 production lifecycle or
primary scale contract. It measures deterministic 10k and 100k current-f32
fixtures, computes exact recall@10, and can enforce supplied same-host baseline
medians as a 95% throughput floor.

The standalone `crates/db/fuzz` workspace adds five non-test Cargo Fuzz
targets: `current_secondary_records`, `current_search_records`,
`current_index_v2_keys`, `current_index_v2_records`, and
`current_index_v2_work`. The V2 targets cover scoped/global physical framing,
canonical catalog/operation/control values, outbox work, upload intent,
active-mutation proof, global reachability reference, and blob-GC pass/attempt
values. They are deliberately outside Cargo's test target inventory and call
only the feature-gated byte-slice decoder boundary.

Run `scripts/db-production-coverage.sh` from any directory to discover and run
every `db` integration target whose name starts with `production_`, except the
separate `production-scale` release gate. The naming and feature contracts keep
path-included unit suites and multi-hour scale fixtures out of instrumentation
while all bounded production-linked targets join the baseline automatically.
The runner excludes `tests/`, `benches/`, and `examples/` source files from the
report, prints stable whole-DB and `search/vector` production counts, and deletes
its owned temporary directory on exit.

### Reproducible production baseline

Two consecutive runs on 2026-07-10 against production source at `9369b9a`
produced identical counts. The unrelated dirty files were `AGENTS.md` plus
Phase 0 benchmark, documentation, and `#[cfg(test)]` changes, so they did not
enter the production-linked library or these totals.

| Scope | Functions | Lines | Regions |
|---|---:|---:|---:|
| Whole `db` production | 128 / 3,248 (3.9409%) | 752 / 27,821 (2.7030%) | 913 / 37,390 (2.4418%) |
| `search/vector` production | 4 / 485 (0.8247%) | 45 / 4,787 (0.9400%) | 51 / 7,076 (0.7207%) |

Both runs discovered only `production_contracts`, ran its three tests, and
passed. These deliberately low numbers are the honest initial integration
baseline. The historical 93.50% all-targets line figure is useful for locating
unit-tested code but cannot satisfy a production-only gate.

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
| `secondary_backfill_background_worker_drains_pending_jobs` | `index_v2_secondary_state_machine_matches_reference_model` |
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
build/drop/reopen contracts, blob-GC handoff tests, and unchanged-manifest
goldens.

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
