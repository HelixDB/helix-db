//! Integration entry points for production-only internal contract coverage.
//!
//! The `production-coverage` feature exposes narrowly scoped runners whose
//! orchestration remains outside the measured production source tree. These
//! tests invoke those runners through the crate boundary, proving the contracts
//! compile as an external consumer would while still executing real private
//! implementations and production codecs. Default builds expose none of this
//! support surface.

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

/// Verifies the single search session owns validation, traversal, and observation.
#[tokio::test]
async fn vector_search_exercises_session_and_policy_boundaries() {
    db::production_coverage::vector_search_contracts().await;
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
