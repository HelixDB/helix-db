//! Process isolation for unit tests that change process-global state.
//!
//! Failpoint slots, environment variables, and deliberately poisoned locks are
//! shared by every test in one process. Under `cargo test` all unit tests share
//! the library's test binary, so a test that changes such state reruns itself
//! alone in a child process of that binary. Under nextest, which already runs
//! each test in its own process, the child is merely redundant.

use std::process::Command;

/// Names the only test a child process runs.
const CHILD: &str = "HELIX_DB_ISOLATED_TEST";

/// Runs `contract` alone in a child process of the current test binary.
///
/// Call it first thing in a `#[test]` function: libtest runs each test on a
/// thread named after the test's path, which selects the child's only test.
/// In that child, `contract` runs directly. The parent fails unless the child
/// ran exactly that test and it passed.
pub(crate) fn in_own_process(contract: impl FnOnce()) {
    let current = std::thread::current();
    let name = current
        .name()
        .filter(|name| name.contains("::"))
        .expect("isolated tests run on libtest's per-test thread");
    if std::env::var(CHILD).as_deref() == Ok(name) {
        contract();
        return;
    }
    let output = Command::new(std::env::current_exe().expect("test binary path resolves"))
        .args([name, "--exact", "--test-threads=1", "--nocapture"])
        .env(CHILD, name)
        .output()
        .expect("isolated test process starts");
    let stdout = String::from_utf8_lossy(&output.stdout);
    print!("{stdout}");
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(
        output.status.success(),
        "isolated test {name} failed: {}",
        output.status
    );
    // A child that selects nothing, after a rename or `#[ignore]`, still exits
    // successfully.
    assert!(
        stdout.contains("test result: ok. 1 passed;"),
        "isolated test {name} ran no test in its child process"
    );
}
