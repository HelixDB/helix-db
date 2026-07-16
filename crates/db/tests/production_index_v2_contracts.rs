//! Production-linked index V2 lifecycle acceptance contracts.
//!
//! This target imports the compiled `db` crate without `cfg(test)` and invokes
//! feature-gated harness code that drives the real canonical, outbox, upload,
//! and reachability repositories.

/// Runs every stable operation/upload crash boundary twice from clean storage.
#[tokio::test]
async fn index_v2_outbox_failpoints_leave_only_legal_recovery_states() {
    db::production_coverage::index_v2_outbox_failpoint_contracts().await;
}

/// Compares lifecycle, mutations, and indexed reads with one reference model.
#[tokio::test]
async fn index_v2_secondary_state_machine_matches_reference_model() {
    db::production_coverage::index_v2_secondary_state_machine_contracts().await;
}

/// Proves global operation/upload queues retain exact tenant ownership.
#[tokio::test]
async fn index_v2_global_outbox_discovers_sixteen_isolated_scopes() {
    db::production_coverage::index_v2_multi_scope_discovery_contracts().await;
}
