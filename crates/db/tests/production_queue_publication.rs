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

/// Verifies WAL-only queue work survives fencing and failed WAL uploads.
#[test]
fn wal_only_queue_work_survives_fencing_and_failed_commits() {
    // Overlaid searches over a replayed writer exceed default debug-build
    // thread stacks; production sizing is unchanged.
    const STACK_BYTES: usize = 16 * 1024 * 1024;
    std::thread::Builder::new()
        .stack_size(STACK_BYTES)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("contract runtime builds")
                .block_on(db::production_coverage::queue_publication_wal_only_contracts());
        })
        .expect("contract thread starts")
        .join()
        .expect("contract completes");
}
