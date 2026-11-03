//! Entry points for release-scale contracts (`production-scale`).
//!
//! These soaks seed authoritative rows through current typed codecs and then
//! drive public DDL, the supervised worker, and public search at release
//! shapes, so they need internal access in an optimized non-test build. The
//! feature compiles this module into the database crate; nothing else uses it.

pub use crate::search::vector::RestrictedSearchStrategy;

mod index_lifecycle;

/// Runs the non-ignored 100k production-entry lifecycle scale contract.
///
/// This runner seeds authoritative graph rows through current typed codecs,
/// then routes every index through the public DDL interpreter, supervised
/// worker, refreshed catalog, and public search path. It is kept behind the
/// explicit `production-scale` feature because it is a release gate rather
/// than a unit-test workload.
pub async fn index_lifecycle_secondary_text_scale_contracts() {
    index_lifecycle::run_secondary_text_tenant().await;
}

/// Reproduces text CREATE/search/DROP without the full release-scale fixture.
pub async fn index_lifecycle_text_drop_smoke() {
    index_lifecycle::run_text_drop_smoke().await;
}

/// Reproduces text CREATE/search/DROP after multi-split compaction.
pub async fn index_lifecycle_text_drop_multi_split_smoke() {
    index_lifecycle::run_text_drop_multi_split_smoke().await;
}

/// Runs the non-ignored 100k 128D f32 vector lifecycle scale contract.
pub async fn index_lifecycle_vector_scale_contracts() {
    index_lifecycle::run_vector().await;
}

/// Runs the fixed 8k public vector lifecycle contract used by bounded CI.
pub async fn index_lifecycle_vector_ci_contracts() {
    index_lifecycle::run_vector_ci().await;
}

/// Runs the 50k 1536D indexed-prefilter, one-hop, restricted-vector benchmark.
pub async fn traversal_vector_prefilter_scale_contract() {
    index_lifecycle::run_traversal_vector_prefilter().await;
}

/// Runs the disk-backed 1M 1536D traversal benchmark at four selectivities.
pub async fn traversal_vector_prefilter_1m_scale_contract() {
    index_lifecycle::run_traversal_vector_prefilter_1m().await;
}

/// Runs one full configured-batch resource blocker for every V2 index family.
///
/// Each family proves exact-checkpoint retry after a higher-limit reopen,
/// successful activation and drop, then a second blocked build's abort and
/// complete cleanup.
pub async fn index_lifecycle_blocked_limit_scale_contracts() {
    index_lifecycle::run_blocked_limits().await;
}

/// Runs `query` and returns the strategy of the restricted vector search it ran.
///
/// `None` means `query` ran no traversal-scoped vector search; when it ran
/// several, the last one is reported. Release gates use this to prove which
/// restricted execution a scoped shape's recall measures, so an exact scan
/// cannot stand in for the filtered graph walk.
pub async fn observe_restricted_vector_strategy<F>(
    query: F,
) -> (F::Output, Option<RestrictedSearchStrategy>)
where
    F: std::future::Future,
{
    let (output, stats) = crate::search::vector::observe_restricted_search(query).await;
    (output, stats.and_then(|stats| stats.strategy))
}

/// Runs vector property materialization and physical retirement for 100k rows.
pub async fn vector_migration_scale_100k() {
    crate::migrations::run_vector_migration_scale_contract(100_000).await;
}

/// Runs vector property materialization and physical retirement for one million rows.
pub async fn vector_migration_scale_1m() {
    crate::migrations::run_vector_migration_scale_contract(1_000_000).await;
}

/// Runs the opt-in ten-million-row vector migration release soak.
pub async fn vector_migration_scale_10m() {
    crate::migrations::run_vector_migration_scale_contract(10_000_000).await;
}
