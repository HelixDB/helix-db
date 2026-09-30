//! Production-coverage contracts for asynchronous vector and text indexing.
//!
//! These tests call feature-gated runners whose orchestration lives under
//! `tests/production_support`, so the coverage report measures the vector
//! lifecycle driver and the queue publisher they drive rather than the
//! harness. Default builds expose none of this support surface.

/// Verifies vector build, cleanup, adoption, and planning steps fail closed.
#[tokio::test]
async fn vector_lifecycle_driver_step_boundaries_fail_closed() {
    db::production_coverage::vector_lifecycle_driver_contracts().await;
}

/// Verifies queue publication trims, blocks, reconciles, retires, and retries.
#[tokio::test]
async fn queue_publication_trims_blocks_reconciles_and_fails_closed() {
    db::production_coverage::queue_publication_contracts().await;
}
