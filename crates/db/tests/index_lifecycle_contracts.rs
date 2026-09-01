//! Feature-gated deterministic Index V2 lifecycle acceptance target.
//!
//! The runner uses explicit scheduling and the installed production drivers;
//! it does not introduce another lifecycle implementation or persistence path.

#![recursion_limit = "256"]

static CONTRACT_SUITE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// Runs every small DDL family shape and canonical CREATE/DROP state contract.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_deterministic_lifecycle_matrix() {
    let _permit = CONTRACT_SUITE
        .acquire()
        .await
        .expect("lifecycle contract semaphore remains open");
    db::index_lifecycle_testing::run_deterministic_lifecycle_contracts().await;
}

/// Proves mutations converge during backfill and unique validation can recover.
#[test]
fn index_lifecycle_backfill_mutations_and_unique_retry_converge() {
    // The composed build-session and SimHash mutation future exceeds Tokio's
    // and libtest's default Linux stacks in debug builds. Keep the larger
    // calling and worker stacks local to this acceptance contract; production
    // runtime sizing is unchanged.
    std::thread::Builder::new()
        .name("index-lifecycle-lifecycle-mutation".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(16 * 1024 * 1024)
                .build()
                .expect("lifecycle mutation test runtime should build")
                .block_on(async {
                    let _permit = CONTRACT_SUITE
                        .acquire()
                        .await
                        .expect("lifecycle contract semaphore remains open");
                    db::index_lifecycle_testing::run_deterministic_lifecycle_mutation_contracts()
                        .await;
                });
        })
        .expect("lifecycle mutation test thread should spawn")
        .join()
        .expect("lifecycle mutation test thread should not panic");
}

/// Proves every managed index shape catches up after late validation mutations.
#[test]
fn index_lifecycle_all_index_shapes_reenter_catch_up_before_activation() {
    std::thread::Builder::new()
        .name("index-lifecycle-all-index-validation".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(16 * 1024 * 1024)
                .build()
                .expect("all-index validation test runtime should build")
                .block_on(async {
                    let _permit = CONTRACT_SUITE
                        .acquire()
                        .await
                        .expect("lifecycle contract semaphore remains open");
                    db::index_lifecycle_testing::run_deterministic_all_index_validation_contracts()
                        .await;
                });
        })
        .expect("all-index validation test thread should spawn")
        .join()
        .expect("all-index validation test thread should not panic");
}

/// Proves simultaneous CREATE requests converge after retryable serialization.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_concurrent_create_converges_for_every_family() {
    let _permit = CONTRACT_SUITE
        .acquire()
        .await
        .expect("lifecycle contract semaphore remains open");
    db::index_lifecycle_testing::run_deterministic_lifecycle_race_contracts().await;
}

/// Proves every family resumes after two failures at the same durable boundary.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_repeated_recoverable_errors_resume_exactly_once() {
    let _permit = CONTRACT_SUITE
        .acquire()
        .await
        .expect("lifecycle contract semaphore remains open");
    db::index_lifecycle_testing::run_deterministic_lifecycle_fault_contracts().await;
}

/// Proves tenant builds, drops, and aborts resume from every durable checkpoint.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_tenant_reopens_resume_every_family_exactly_once() {
    let _permit = CONTRACT_SUITE
        .acquire()
        .await
        .expect("lifecycle contract semaphore remains open");
    db::index_lifecycle_testing::run_deterministic_lifecycle_reopen_contracts().await;
}
