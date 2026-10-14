//! Process-isolated vector migration resource contracts.
//!
//! These tests measure process-wide resident memory. They intentionally live
//! outside the other lifecycle scale contracts so unrelated concurrent test
//! allocations cannot contaminate their working-set observations.

/// Measures and bounds vector migration work at 100,000 legacy entities.
#[tokio::test(flavor = "multi_thread")]
async fn vector_migration_scale_100k() {
    db::production_coverage::vector_migration_scale_100k().await;
}

/// Measures the one-million-row release shape when explicitly selected.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "opt-in one-million-row migration release soak"]
async fn vector_migration_scale_1m() {
    db::production_coverage::vector_migration_scale_1m().await;
}

/// Measures the ten-million-row release shape when explicitly selected.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "opt-in ten-million-row migration release soak"]
async fn vector_migration_scale_10m() {
    db::production_coverage::vector_migration_scale_10m().await;
}
