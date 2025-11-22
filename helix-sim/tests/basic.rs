use helix_sim::{AsyncRuntime, DeterministicRuntime, TokioRuntime};
use std::time::Duration;

#[tokio::test]
async fn tokio_sleep_test() {
    let rt = TokioRuntime;
    let sleep = rt.sleep(Duration::from_millis(100));
    let _ = sleep.await;
}

#[test]
fn deterministic_runtime_now() {
    let rt = DeterministicRuntime::new();

    let t1 = rt.now();
    assert_eq!(t1, 0);

    rt.advance_to(1000);
    let t2 = rt.now();
    assert_eq!(t2, 1000);
}

#[test]
fn deterministic_sleep_future() {
    let rt = DeterministicRuntime::new();

    let sleep = rt.sleep(Duration::from_nanos(500));
    let _ = std::hint::black_box(sleep);

    rt.advance_to(1000);
    let now = rt.now();
    assert_eq!(now, 1000);
}
