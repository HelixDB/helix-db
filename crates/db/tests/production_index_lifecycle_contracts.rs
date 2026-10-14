//! Production-linked index V2 lifecycle acceptance contracts.
//!
//! This target imports the compiled `db` crate without `cfg(test)` and invokes
//! feature-gated harness code that drives the real canonical and outbox
//! repositories.

use std::process::Command;

/// Proves valid incomplete schemas are promotable only through writer open.
#[tokio::test]
async fn index_lifecycle_writer_migration_requirements_are_typed() {
    db::production_coverage::writer_migration_requirement_contracts().await;
}

/// Runs every stable operation/upload crash boundary twice from clean storage.
#[tokio::test]
async fn index_lifecycle_outbox_failpoints_leave_only_legal_recovery_states() {
    db::production_coverage::index_lifecycle_outbox_failpoint_contracts().await;
}

/// Proves the explicit crash action terminates at its configured boundary.
#[test]
fn index_lifecycle_failpoint_abort_action_terminates_process() {
    const CHILD_ENV: &str = "HELIX_INDEX_OUTBOX_ABORT_PROBE_CHILD";
    if std::env::var_os(CHILD_ENV).is_some() {
        db::production_coverage::index_lifecycle_failpoint_abort_probe();
    }

    let status = Command::new(std::env::current_exe().expect("test executable path resolves"))
        .args([
            "--exact",
            "index_lifecycle_failpoint_abort_action_terminates_process",
            "--nocapture",
        ])
        .env(CHILD_ENV, "1")
        .env("HELIX_INDEX_OUTBOX_FAILPOINT", "commit_before")
        .env("HELIX_INDEX_OUTBOX_FAIL_ACTION", "abort")
        .status()
        .expect("abort probe child starts");
    assert!(
        !status.success(),
        "abort probe child must not exit normally"
    );
}

/// Compares lifecycle, mutations, and indexed reads with one reference model.
#[tokio::test]
async fn index_lifecycle_secondary_state_machine_matches_reference_model() {
    db::production_coverage::index_lifecycle_secondary_state_machine_contracts().await;
}

/// Proves the global operation queue retains exact tenant ownership.
#[tokio::test]
async fn index_lifecycle_global_outbox_discovers_sixteen_isolated_scopes() {
    db::production_coverage::index_lifecycle_multi_scope_discovery_contracts().await;
}

/// Proves compact V2 model and resource gates retain their typed boundaries.
#[test]
fn index_lifecycle_typed_boundaries_fail_closed() {
    db::production_coverage::index_lifecycle_typed_boundary_contracts();
}

/// Proves Active text serving reads reject every cross-owned durable row.
#[tokio::test]
async fn index_lifecycle_text_serving_reads_fail_closed() {
    db::production_coverage::index_lifecycle_text_serving_contracts().await;
}

/// Proves state-only Active text retirement validates before atomic staging.
#[tokio::test]
async fn index_lifecycle_active_text_retirement_fails_closed() {
    db::production_coverage::index_lifecycle_active_text_retirement_contracts().await;
}
