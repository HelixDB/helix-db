//! Contracts for the stable V2 failpoint control boundary.
//!
//! They cover stable name parsing, environment-triggered errors and aborts,
//! one-shot reset, and poisoned synchronization. The environment and failpoint
//! slot are process-wide, so each contract runs in its own process.

use std::process::Command;
use std::thread;

use super::*;

/// Proves the explicit crash action terminates at its configured boundary.
#[test]
fn abort_action_terminates_process() {
    const CHILD_ENV: &str = "HELIX_INDEX_OUTBOX_ABORT_PROBE_CHILD";
    let name = concat!(module_path!(), "::abort_action_terminates_process")
        .strip_prefix("db::")
        .expect("test paths start at the crate root");
    if std::env::var_os(CHILD_ENV).is_some() {
        let _ = trip(IndexOutboxFailpoint::CommitBefore);
        panic!("abort failpoint unexpectedly returned");
    }

    let status = Command::new(std::env::current_exe().expect("test executable path resolves"))
        .args(["--exact", name, "--nocapture"])
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

/// Proves failpoint parsing, environment, reset, and poisoned-lock contracts.
#[test]
fn controls_parse_trip_reset_and_fail_closed_when_poisoned() {
    crate::test_isolation::in_own_process(failpoint_control_contracts);
}

fn failpoint_control_contracts() {
    for failpoint in IndexOutboxFailpoint::ALL {
        assert_eq!(
            IndexOutboxFailpoint::parse(failpoint.as_str()),
            Some(failpoint)
        );
    }
    assert_eq!(IndexOutboxFailpoint::parse("unknown"), None);

    inject_once(IndexOutboxFailpoint::ClaimBefore).expect("one-shot failpoint installs");
    assert!(trip(IndexOutboxFailpoint::ClaimBefore).is_err());
    assert!(was_triggered());
    assert!(trip(IndexOutboxFailpoint::ClaimBefore).is_ok());

    // SAFETY: this contract runs alone in its own process, and both variables
    // are removed before this function returns.
    unsafe {
        std::env::set_var("HELIX_INDEX_OUTBOX_FAILPOINT", "commit_before");
        std::env::remove_var("HELIX_INDEX_OUTBOX_FAIL_ACTION");
    }
    assert!(matches!(
        trip(IndexOutboxFailpoint::CommitBefore),
        Err(HelixDbError::InvariantViolation(reason))
            if reason.contains("commit_before")
    ));
    // SAFETY: this restores the isolated process environment immediately
    // after the single production call that observes it.
    unsafe {
        std::env::remove_var("HELIX_INDEX_OUTBOX_FAILPOINT");
    }

    let poisoner = thread::spawn(|| {
        let _guard = INJECTED_FAILPOINT
            .lock()
            .expect("failpoint mutex starts healthy");
        panic!("poison failpoint mutex for the fail-closed contract");
    });
    assert!(poisoner.join().is_err());
    assert!(matches!(
        inject_once(IndexOutboxFailpoint::ClaimAfter),
        Err(HelixDbError::InvariantViolation(reason))
            if reason.contains("mutex was poisoned")
    ));
    assert!(matches!(
        trip(IndexOutboxFailpoint::ClaimAfter),
        Err(HelixDbError::InvariantViolation(reason))
            if reason.contains("mutex was poisoned")
    ));
    INJECTED_FAILPOINT.clear_poison();
    *INJECTED_FAILPOINT
        .lock()
        .expect("cleared failpoint mutex locks") = None;
}
