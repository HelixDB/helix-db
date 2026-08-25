//! Integration entry points for production-only internal contract coverage.
//!
//! The `production-coverage` feature exposes narrowly scoped runners whose
//! orchestration remains outside the measured production source tree. These
//! tests invoke those runners through the crate boundary, proving the contracts
//! compile as an external consumer would while still executing real private
//! implementations and production codecs. Default builds expose none of this
//! support surface.

/// Verifies graph mutation no-op detection preserves exact V1 representation.
#[test]
fn graph_mutation_representation_is_recursive_and_bit_exact() {
    db::production_coverage::graph_mutation_representation_contracts();
}

/// Exercises the feature-gated vector batch harness across every metric and
/// both fresh and replacement transaction shapes.
#[tokio::test]
async fn vector_batch_benchmark_exercises_all_metrics_and_workloads() {
    use db::production_coverage::{
        VectorBatchBenchmarkCacheLimits, VectorBatchBenchmarkCase, VectorBatchBenchmarkFixture,
        VectorBatchBenchmarkMetric, VectorBatchBenchmarkWorkload,
    };

    assert!(VectorBatchBenchmarkCase::try_new(
        0,
        4,
        VectorBatchBenchmarkMetric::Cosine,
        VectorBatchBenchmarkWorkload::Fresh,
    )
    .is_err());
    assert!(VectorBatchBenchmarkCase::try_new(
        1,
        0,
        VectorBatchBenchmarkMetric::Cosine,
        VectorBatchBenchmarkWorkload::Fresh,
    )
    .is_err());
    assert!(VectorBatchBenchmarkCacheLimits::try_new(0, 1, 1, 1).is_err());
    assert!(VectorBatchBenchmarkCase::try_new_with_initial_count(
        usize::MAX,
        1,
        4,
        VectorBatchBenchmarkMetric::Cosine,
        VectorBatchBenchmarkWorkload::Fresh,
    )
    .is_err());

    for metric in [
        VectorBatchBenchmarkMetric::Cosine,
        VectorBatchBenchmarkMetric::Euclidean,
        VectorBatchBenchmarkMetric::Manhattan,
    ] {
        for workload in [
            VectorBatchBenchmarkWorkload::Fresh,
            VectorBatchBenchmarkWorkload::Replacement,
        ] {
            // The benchmark's scripted selector assigns ordinal 11 to layer 1.
            // Keep one case large enough to make upper-neighbor staging deterministic.
            let batch_size = match (metric, workload) {
                (VectorBatchBenchmarkMetric::Cosine, VectorBatchBenchmarkWorkload::Fresh) => 12,
                _ => 2,
            };
            let initial_count =
                usize::from(workload == VectorBatchBenchmarkWorkload::Replacement) * 2;
            let case = VectorBatchBenchmarkCase::try_new_with_initial_count(
                batch_size,
                initial_count,
                4,
                metric,
                workload,
            )
            .expect("small vector benchmark case validates");
            let cache_limits = if workload == VectorBatchBenchmarkWorkload::Replacement {
                VectorBatchBenchmarkCacheLimits::try_new(256, 1, 1, 1)
                    .expect("pressure benchmark limits validate")
            } else {
                VectorBatchBenchmarkCacheLimits::default()
            };
            let fixture = if workload == VectorBatchBenchmarkWorkload::Replacement {
                VectorBatchBenchmarkFixture::prepare_with_cache_limits(case, cache_limits).await
            } else {
                VectorBatchBenchmarkFixture::prepare(case).await
            }
            .expect("small vector benchmark fixture prepares");
            let sample = fixture
                .run_sample()
                .await
                .expect("small vector benchmark sample completes")
                .with_allocations(3, 128);
            assert_eq!(sample.allocated_calls, 3);
            assert_eq!(sample.allocated_bytes, 128);
            assert!(sample.unique_final_rows > 0);
            assert!(sample.unique_final_bytes > 0);
            assert!(sample.telemetry.staged_write_bytes > 0);
            assert!((0.0..=1.0).contains(&sample.recall));
            fixture
                .close()
                .await
                .expect("small vector benchmark fixture closes");
        }
    }
}

/// Verifies every descriptor-bound memory-registry lifecycle transition.
#[tokio::test]
async fn vector_memory_registry_exercises_every_lifecycle_transition() {
    db::production_coverage::vector_memory_registry_contracts().await;
}

/// Verifies vector-memory cache capabilities, fencing, and bounded hydration.
#[tokio::test]
async fn vector_memory_store_exercises_cache_and_hydration_boundaries() {
    db::production_coverage::vector_memory_store_contracts().await;
}

/// Verifies descriptor-bound startup hydration and budget partitioning.
#[tokio::test]
async fn vector_hydration_exercises_active_generation_boundaries() {
    db::production_coverage::vector_hydration_contracts().await;
}

/// Verifies vector generation capabilities retain exact lifecycle authority.
#[test]
fn vector_generation_exercises_descriptor_bound_authority() {
    db::production_coverage::vector_generation_contracts();
}

/// Verifies every active layer-zero filtering and sampling policy branch.
#[test]
fn vector_policy_exercises_metric_and_frontier_boundaries() {
    db::production_coverage::vector_policy_contracts();
}

/// Verifies bounded deterministic SimHasher registry admission and reuse.
#[test]
fn vector_simhash_registry_exercises_limits_lru_and_single_flight() {
    db::production_coverage::vector_simhash_registry_contracts();
}

/// Verifies transactional SimHash rows preserve scope, codec, and failure semantics.
#[tokio::test]
async fn vector_simhash_exercises_transactional_row_contracts() {
    db::production_coverage::vector_simhash_contracts().await;
}

/// Verifies measured vector writes preserve transaction and checkpoint semantics.
#[tokio::test]
async fn vector_write_transaction_exercises_replacement_and_failure_boundaries() {
    db::production_coverage::vector_write_transaction_contracts().await;
}

/// Verifies typed vector rows own scoped keys, codecs, tokens, and cleanup.
#[tokio::test]
async fn vector_storage_exercises_all_current_row_families() {
    db::production_coverage::vector_storage_contracts().await;
}

/// Keeps missing tenant mapping validation fail-closed for real vector rows.
#[tokio::test]
async fn vector_missing_partition_mapping_delete_fails_closed() {
    db::production_coverage::vector_missing_partition_mapping_delete_contract().await;
}

/// Characterizes safe finite-score limits independently of production validation.
#[test]
fn vector_magnitude_oracle_and_kernels_cover_numeric_boundaries() {
    db::production_coverage::vector_magnitude_oracle_and_kernel_contracts();
}

/// Freezes accepted persisted bytes, existing scores, and cosine behavior.
#[tokio::test]
async fn vector_magnitude_golden_inputs_preserve_existing_semantics() {
    db::production_coverage::vector_magnitude_golden_and_cosine_contracts().await;
}

/// Exposes current-row decoding of finite values outside the safe metric domain.
#[test]
fn vector_magnitude_current_rows_fail_closed() {
    db::production_coverage::vector_magnitude_current_row_decode_contracts();
}

/// Exposes insert/upsert writes and cache effects before magnitude rejection.
#[tokio::test]
async fn vector_magnitude_mutations_reject_before_state_changes() {
    db::production_coverage::vector_magnitude_mutation_contracts().await;
}

/// Exposes unrestricted finite-query values that can reach invalid scoring.
#[tokio::test]
async fn vector_magnitude_queries_reject_before_hnsw_scoring() {
    db::production_coverage::vector_magnitude_search_contracts().await;
}

/// Exposes restricted queries that bypass the intended numeric domain.
#[tokio::test]
async fn vector_magnitude_restricted_queries_match_unrestricted_validation() {
    db::production_coverage::vector_magnitude_restricted_search_contracts().await;
}

/// Exposes legacy physical rows that adoption currently accepts.
#[tokio::test]
async fn vector_magnitude_legacy_rows_block_adoption_validation() {
    db::production_coverage::vector_magnitude_legacy_validation_contracts().await;
}

/// Verifies the single search session owns validation, traversal, and observation.
#[tokio::test]
async fn vector_search_exercises_session_and_policy_boundaries() {
    db::production_coverage::vector_search_contracts().await;
}

/// Verifies restricted search admission, exact scans, filtered traversal, and row safety.
#[tokio::test]
async fn vector_restricted_search_exercises_bounded_graph_and_directory_paths() {
    db::production_coverage::vector_restricted_search_contracts().await;
}

/// Verifies mutation-cache transitions, stale-root repair, and typed graph writes.
#[tokio::test]
async fn vector_mutation_cache_exercises_closed_state_transitions() {
    db::production_coverage::vector_mutation_cache_contracts().await;
}

/// Verifies active f32 views and typed runtime primitives reject invalid states.
#[test]
fn vector_primitives_exercise_active_codec_and_runtime_boundaries() {
    db::production_coverage::vector_primitive_contracts();
}

/// Verifies active distance semantics and bounded canonical neighbor states.
#[test]
fn vector_distance_neighbors_exercise_metric_and_graph_invariants() {
    db::production_coverage::vector_distance_neighbor_contracts();
}

/// Verifies request read ownership and generation-bound cache visibility.
#[tokio::test]
async fn vector_read_boundaries_exercise_snapshot_and_generation_ownership() {
    db::production_coverage::vector_read_boundary_contracts().await;
}

/// Verifies vector facade identity, DDL, cache, corruption, and cleanup boundaries.
#[tokio::test]
async fn vector_index_exercises_facade_and_row_safety_contracts() {
    db::production_coverage::vector_index_contracts().await;
}

/// Proves a serializable graph conflict leaves only an unattached immutable blob.
#[tokio::test]
async fn interpreter_active_text_graph_conflict_resolves_fail_closed() {
    db::production_coverage::interpreter_active_text_graph_conflict_contracts().await;
}

/// Proves one split per destination/epoch, delete-only behavior, and shared versions.
#[cfg(feature = "index-lifecycle-testing")]
#[tokio::test]
async fn interpreter_active_text_transactions_batch_by_destination() {
    db::production_coverage::interpreter_active_text_transaction_batching_contracts().await;
}

/// Proves internal production reads fail closed without their request snapshot.
#[tokio::test]
async fn interpreter_request_read_view_guards_fail_closed() {
    db::production_coverage::interpreter_request_read_view_guard_contracts().await;
}

/// Exercises every exact cardinality primitive through a production-linked binary.
#[test]
fn interpreter_cardinality_programs_cover_exact_contracts() {
    std::thread::Builder::new()
        .name("exact-cardinality-production-contracts".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cardinality production runtime builds")
                .block_on(db::production_coverage::interpreter_cardinality_program_contracts());
        })
        .expect("cardinality production thread starts")
        .join()
        .expect("cardinality production contracts complete");
}

/// Exercises both production FTS layouts and both candidate-selection primitives.
#[tokio::test]
async fn text_prefilter_benchmark_covers_exact_layout_and_strategy_matrix() {
    use db::production_coverage::{
        FtsPrefilterBenchmarkCase, FtsPrefilterBenchmarkFixture, FtsPrefilterBenchmarkLayout,
        FtsPrefilterBenchmarkStrategy,
    };

    assert!(FtsPrefilterBenchmarkFixture::try_new(0, 1).await.is_err());
    assert!(FtsPrefilterBenchmarkFixture::try_new(4, 0).await.is_err());
    assert!(FtsPrefilterBenchmarkFixture::try_new(3, 4).await.is_err());

    let fixture = FtsPrefilterBenchmarkFixture::try_new(24, 3)
        .await
        .expect("small production FTS fixture prepares");
    assert_eq!(fixture.document_count(), 24);
    assert!(FtsPrefilterBenchmarkCase::try_new(
        FtsPrefilterBenchmarkLayout::MultiSplit,
        FtsPrefilterBenchmarkStrategy::Collector,
        0,
        "rareterm",
        3,
        fixture.document_count(),
    )
    .is_err());
    assert!(FtsPrefilterBenchmarkCase::try_new(
        FtsPrefilterBenchmarkLayout::MultiSplit,
        FtsPrefilterBenchmarkStrategy::Collector,
        fixture.document_count() + 1,
        "rareterm",
        3,
        fixture.document_count(),
    )
    .is_err());
    assert!(FtsPrefilterBenchmarkCase::try_new(
        FtsPrefilterBenchmarkLayout::MultiSplit,
        FtsPrefilterBenchmarkStrategy::Collector,
        8,
        "unsupported",
        3,
        fixture.document_count(),
    )
    .is_err());
    assert!(FtsPrefilterBenchmarkCase::try_new(
        FtsPrefilterBenchmarkLayout::MultiSplit,
        FtsPrefilterBenchmarkStrategy::Collector,
        8,
        "rareterm",
        0,
        fixture.document_count(),
    )
    .is_err());

    for layout in [
        FtsPrefilterBenchmarkLayout::MultiSplit,
        FtsPrefilterBenchmarkLayout::Compacted,
    ] {
        for strategy in [
            FtsPrefilterBenchmarkStrategy::Collector,
            FtsPrefilterBenchmarkStrategy::Unrestricted,
        ] {
            assert!(!layout.as_str().is_empty());
            assert!(!strategy.as_str().is_empty());
            for query in ["rareterm", "mediumterm", "commonterm"] {
                let case = FtsPrefilterBenchmarkCase::try_new(
                    layout,
                    strategy,
                    8,
                    query,
                    5,
                    fixture.document_count(),
                )
                .expect("benchmark matrix case validates");
                let sample = fixture
                    .run_case(case)
                    .await
                    .expect("benchmark matrix case matches its exact oracle");
                assert_eq!(sample.candidate_count, 8);
                assert_eq!(
                    sample.split_count,
                    if layout.as_str() == "multi_split" {
                        3
                    } else {
                        1
                    }
                );
                assert!(sample.result_count <= 5);
                assert_eq!(sample.result_digest.len(), 64);
            }
        }
    }
}

/// Exercises literal secondary storage primitives without executor-side dispatch.
#[tokio::test]
async fn secondary_exact_storage_obeys_encoded_primitives() {
    db::production_coverage::secondary_exact_storage_contracts().await;
}

/// Proves catalog authority excludes lifecycle publication through write-open.
#[tokio::test]
async fn interpreter_catalog_authority_transfers_through_write_open() {
    db::production_coverage::interpreter_catalog_write_open_authority_contracts().await;
}

/// Proves request-mode composition and operation-owned mutation commits.
#[tokio::test]
async fn interpreter_request_modes_preserve_isolated_mutation_ownership() {
    db::production_coverage::interpreter_request_mode_and_isolated_mutation_contracts().await;
}

/// Proves coalescing order, multigraph topology, rollback, and conflict semantics.
#[tokio::test]
async fn interpreter_topology_mutations_preserve_transactional_semantics() {
    db::production_coverage::interpreter_topology_mutation_contracts().await;
}

/// Proves parallel dependency transfer and row projections execute their encoded shapes.
#[tokio::test]
async fn interpreter_scheduler_and_projection_paths_are_production_linked() {
    db::production_coverage::interpreter_scheduler_and_projection_contracts().await;
}

/// Verifies process-local writer identity and readiness through the public boundary.
#[test]
fn process_local_runtime_dependencies_preserve_identity_and_readiness() {
    assert!(db::IndexRuntimeReadiness::Ready.is_ready());
    assert_eq!(db::IndexRuntimeReadiness::Ready.code(), "ready");
    assert!(db::ProcessLocalDatabaseToken::new("").is_err());

    let token = db::ProcessLocalDatabaseToken::new("production-runtime-dependencies")
        .expect("a non-empty process-local database path validates");
    let cloned = token.clone();
    assert_eq!(token.database_id(), cloned.database_id());
    let debug = format!("{token:?}");
    assert!(debug.contains("ProcessLocalDatabaseToken"));
    assert!(debug.contains("production-runtime-dependencies"));
    assert!(!debug.contains("object_store"));
}
