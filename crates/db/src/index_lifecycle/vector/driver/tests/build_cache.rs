//! Persisted-graph equivalence and throughput contracts for vector source backfill.
//!
//! The golden digest pins every physical vector row produced by a deterministic
//! Euclidean build. Its components are small integers, so every squared
//! distance is exact in `f32` and the contract does not depend on SIMD
//! summation order. Equivalence contracts compare complete row sets built under
//! different step boundaries and cache policies inside one process, and the
//! retained-cache contracts prove that failed, stale, blocked, and aborted
//! steps never let planning state outlive the rows it mirrors.

use std::time::{Duration, Instant};

use super::*;

/// Source entities in [`euclidean_golden_fixture`].
const GOLDEN_ENTITIES: u64 = 320;
/// Vector dimension of [`euclidean_golden_fixture`].
const GOLDEN_DIMENSION: usize = 8;

/// SHA-256 over every physical row of [`euclidean_golden_fixture`].
const EUCLIDEAN_GOLDEN_DIGEST: &str =
    "805ae4c6120ed75d9ddb0027da2191f6968f5abd65a991c88ad067d6c54ad76c";
/// Physical row count of [`euclidean_golden_fixture`].
const EUCLIDEAN_GOLDEN_ROWS: usize = 11_686;

/// Deterministic 64-bit mixer used to derive fixture components.
fn splitmix64(state: u64) -> u64 {
    let mut mixed = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

/// Returns one fixture component seed for an entity and dimension.
fn component_seed(entity_id: u64, component: usize) -> u64 {
    splitmix64(
        entity_id
            .wrapping_mul(1_000_003)
            .wrapping_add(component as u64),
    )
}

/// Returns small integer components whose squared distances are exact in `f32`.
fn integral_vector(entity_id: u64, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|component| (component_seed(entity_id, component) % 16) as f32)
        .collect()
}

/// Returns non-zero components in `[-1, 1)` for cosine fixtures.
fn unit_range_vector(entity_id: u64, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|component| {
            let bits = component_seed(entity_id, component) >> 40;
            bits as f32 / (1_u64 << 23) as f32 - 1.0
        })
        .collect()
}

fn vector_definition(
    dimension: usize,
    metric: VectorDistanceMetric,
) -> ValidatedDynamicIndexDefinition {
    let runtime = VectorIndexDefinition::new_node("Document", "embedding", dimension, metric)
        .expect("fixture vector definition validates");
    ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
            .expect("fixture V2 vector definition validates"),
    )
}

/// Writes `count` source nodes in bounded transactions.
async fn seed_sources(db: &Db, scope: DataScope, count: u64, vector: impl Fn(u64) -> Vec<f32>) {
    for chunk_start in (0..count).step_by(1_024) {
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("fixture source transaction opens");
        for entity_id in chunk_start..count.min(chunk_start + 1_024) {
            transaction
                .put(
                    source_key(scope, entity_id),
                    encode_properties(&[
                        Property::new("$label", PropertyValue::String("Document".to_string())),
                        Property::new("embedding", PropertyValue::F32Array(vector(entity_id))),
                    ]),
                )
                .expect("fixture source stages");
        }
        transaction
            .commit()
            .await
            .expect("fixture source chunk commits");
    }
}

/// Wall-clock evidence for one complete build.
struct BuildReport {
    physical_index_id: VectorPhysicalIndexId,
    steps: u64,
    elapsed: Duration,
    /// Elapsed time when source scanning finished.
    scan_elapsed: Duration,
    /// Elapsed time when each quarter of the source entities was admitted.
    quarter_elapsed: [Duration; 4],
}

/// Returns cumulative admitted source entities, or `u64::MAX` after the scan.
async fn admitted_entities(db: &Db, scope: DataScope, operation_id: IndexOperationId) -> u64 {
    let operation = read_operation(db, scope, operation_id).await;
    let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
        VectorBuildStage::Scan(progress),
    )) = operation.progress()
    else {
        return u64::MAX;
    };
    progress.counters.entities
}

/// Drives one build to activation with `driver`, returning timing evidence.
async fn build_to_active(
    db: &Db,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    count: u64,
    driver: &VectorIndexDriver,
    limits: SearchIndexBatchLimits,
) -> BuildReport {
    let (build_id, _, _) = create_build(db, scope, definition, count - 1).await;
    let mut claim_sequence = 1;
    let mut steps = 0_u64;
    let mut quarter_elapsed = [Duration::ZERO; 4];
    let mut next_quarter = 0_usize;
    let mut scan_elapsed = None;
    let started = Instant::now();
    loop {
        steps += 1;
        let step = drive_one(db, driver, build_id, &mut claim_sequence, limits).await;
        let admitted = admitted_entities(db, scope, build_id).await;
        while next_quarter < quarter_elapsed.len()
            && admitted >= count * (next_quarter as u64 + 1) / 4
        {
            quarter_elapsed[next_quarter] = started.elapsed();
            next_quarter += 1;
        }
        if admitted == u64::MAX && scan_elapsed.is_none() {
            scan_elapsed = Some(started.elapsed());
        }
        match step {
            CommittedOperationStep::Progressed => {}
            CommittedOperationStep::Completed => break,
            CommittedOperationStep::Blocked | CommittedOperationStep::TransientFailure => {
                panic!("vector fixture build stopped at {step:?}")
            }
        }
    }
    let elapsed = started.elapsed();
    let active = read_index(db, scope, definition).await;
    let IndexStateV2::Active {
        physical:
            PhysicalGeneration::Vector {
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                ..
            },
        ..
    } = active.state()
    else {
        panic!("completed fixture build is active and unpartitioned");
    };
    BuildReport {
        physical_index_id: *physical_index_id,
        steps,
        elapsed,
        scan_elapsed: scan_elapsed.expect("completed build left its source scan"),
        quarter_elapsed,
    }
}

/// Returns the physical row count and SHA-256 over every sorted key/value pair.
async fn physical_digest(
    db: &Db,
    scope: DataScope,
    physical_index_id: VectorPhysicalIndexId,
) -> (usize, String) {
    let rows = physical_vector_rows(db, scope, physical_index_id).await;
    let mut digest = Sha256::new();
    for (key, value) in &rows {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    let bytes: [u8; 32] = digest.finalize().into();
    (
        rows.len(),
        bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
    )
}

/// Builds the deterministic golden fixture and returns its complete digest.
async fn euclidean_golden_fixture(
    name: &str,
    driver: &VectorIndexDriver,
    limits: SearchIndexBatchLimits,
) -> (usize, String, u64) {
    let db = test_db(name).await;
    let scope = DataScope::LegacyUnscoped;
    let definition = vector_definition(GOLDEN_DIMENSION, VectorDistanceMetric::Euclidean);
    seed_sources(&db, scope, GOLDEN_ENTITIES, |entity_id| {
        integral_vector(entity_id, GOLDEN_DIMENSION)
    })
    .await;
    let report = build_to_active(&db, scope, &definition, GOLDEN_ENTITIES, driver, limits).await;
    let (rows, digest) = physical_digest(&db, scope, report.physical_index_id).await;
    db.close().await.expect("golden fixture database closes");
    (rows, digest, report.steps)
}

fn limits_with_output_operations(operations: u64) -> SearchIndexBatchLimits {
    let defaults = SearchIndexBackfillLimits::default().batch();
    SearchIndexBatchLimits::try_new(
        defaults.max_entities(),
        defaults.max_input_bytes(),
        NonZeroU64::new(operations).expect("fixture operation limit is positive"),
        defaults.max_output_bytes(),
        defaults.max_single_vector_output_bytes(),
    )
    .expect("fixture limits validate")
}

#[tokio::test]
async fn source_backfill_matches_the_pinned_physical_graph_golden() {
    let (rows, digest, steps) = euclidean_golden_fixture(
        "vector-build-golden-bounded-steps",
        &driver(),
        limits_with_output_operations(2_048),
    )
    .await;
    assert!(steps > 8, "fixture must span many bounded steps");
    assert_eq!(
        (rows, digest.as_str()),
        (EUCLIDEAN_GOLDEN_ROWS, EUCLIDEAN_GOLDEN_DIGEST)
    );
}

#[tokio::test]
async fn source_backfill_graph_is_independent_of_step_boundaries() {
    let defaults = SearchIndexBackfillLimits::default().batch();
    let thirteen_entities = SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(13).expect("fixture entity limit is positive"),
        defaults.max_input_bytes(),
        defaults.max_output_operations(),
        defaults.max_output_bytes(),
        defaults.max_single_vector_output_bytes(),
    )
    .expect("thirteen-entity limits validate");
    for (name, limits) in [
        ("vector-build-golden-default-steps", defaults),
        (
            "vector-build-golden-thirteen-entity-steps",
            thirteen_entities,
        ),
    ] {
        let (rows, digest, _) = euclidean_golden_fixture(name, &driver(), limits).await;
        assert_eq!(
            (rows, digest.as_str()),
            (EUCLIDEAN_GOLDEN_ROWS, EUCLIDEAN_GOLDEN_DIGEST),
            "{name}"
        );
    }
}

/// The golden fixture driven one explicit step at a time.
struct GoldenBuild {
    db: Db,
    scope: DataScope,
    definition: ValidatedDynamicIndexDefinition,
    operation_id: IndexOperationId,
    claim_sequence: u64,
}

impl GoldenBuild {
    /// Seeds the golden sources and enqueues their build.
    async fn start(name: &str) -> Self {
        let db = test_db(name).await;
        let scope = DataScope::LegacyUnscoped;
        let definition = vector_definition(GOLDEN_DIMENSION, VectorDistanceMetric::Euclidean);
        seed_sources(&db, scope, GOLDEN_ENTITIES, |entity_id| {
            integral_vector(entity_id, GOLDEN_DIMENSION)
        })
        .await;
        let (operation_id, _, _) = create_build(&db, scope, &definition, GOLDEN_ENTITIES - 1).await;
        Self {
            db,
            scope,
            definition,
            operation_id,
            claim_sequence: 1,
        }
    }

    /// Claims and commits one bounded step through the outbox.
    async fn step(&mut self, driver: &VectorIndexDriver) -> CommittedOperationStep {
        drive_one(
            &self.db,
            driver,
            self.operation_id,
            &mut self.claim_sequence,
            limits_with_output_operations(2_048),
        )
        .await
    }

    /// Returns the durable operation record the next step starts from.
    async fn operation(&self) -> IndexOperationRecord {
        read_operation(&self.db, self.scope, self.operation_id).await
    }

    /// Completes the build and returns its physical row digest.
    async fn finish(mut self, driver: &VectorIndexDriver) -> (usize, String) {
        loop {
            match self.step(driver).await {
                CommittedOperationStep::Progressed => {}
                CommittedOperationStep::Completed => break,
                stopped @ (CommittedOperationStep::Blocked
                | CommittedOperationStep::TransientFailure) => {
                    panic!("golden build stopped at {stopped:?}")
                }
            }
        }
        let active = read_index(&self.db, self.scope, &self.definition).await;
        let IndexStateV2::Active {
            physical:
                PhysicalGeneration::Vector {
                    layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                    ..
                },
            ..
        } = active.state()
        else {
            panic!("completed golden build is active and unpartitioned");
        };
        let digest = physical_digest(&self.db, self.scope, *physical_index_id).await;
        self.db.close().await.expect("golden build database closes");
        digest
    }
}

/// Returns the checkpoint of the session a driver most recently retained.
fn retained_checkpoint(driver: &VectorIndexDriver) -> Option<VectorBuildCheckpoint> {
    driver
        .build_cache
        .retained
        .try_lock()
        .expect("no commit is trimming the retained sessions")
        .last()
        .map(|retained| retained.checkpoint.clone())
}

/// Returns the checkpoint of the session a driver retains for one operation.
fn retained_checkpoint_for(
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
) -> Option<VectorBuildCheckpoint> {
    driver
        .build_cache
        .retained
        .try_lock()
        .expect("no commit is trimming the retained sessions")
        .iter()
        .find(|retained| retained.checkpoint.operation_id == operation_id)
        .map(|retained| retained.checkpoint.clone())
}

/// Returns the budget-charged bytes of the most recently retained Euclidean session.
fn retained_euclidean_bytes(driver: &VectorIndexDriver) -> Option<usize> {
    driver
        .build_cache
        .retained
        .try_lock()
        .expect("no commit is trimming the retained sessions")
        .last()
        .map(|retained| {
            let session: &dyn Any = retained.session.as_ref();
            assert!(
                session.is::<VectorBuildSession<vector::distance::Euclidean>>(),
                "golden fixture retains a Euclidean session"
            );
            retained.session.retained_bytes()
        })
}

fn assert_golden(digest: (usize, String), context: &str) {
    assert_eq!(
        (digest.0, digest.1.as_str()),
        (EUCLIDEAN_GOLDEN_ROWS, EUCLIDEAN_GOLDEN_DIGEST),
        "{context}"
    );
}

#[tokio::test]
async fn committed_steps_retain_a_session_bounded_by_its_budget() {
    let budget = NonZeroU64::new(24 * 1024).expect("fixture budget is positive");
    let driver = driver().with_build_cache_bytes(budget);
    let mut build = GoldenBuild::start("vector-build-cache-budget").await;
    for _ in 0..4 {
        assert_eq!(
            build.step(&driver).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            retained_checkpoint(&driver).map(|checkpoint| checkpoint.progress),
            Some(build.operation().await.progress().clone()),
            "a committed step retains the checkpoint the next step starts from"
        );
        let retained = retained_euclidean_bytes(&driver).expect("session is retained");
        assert!(retained > 0);
        assert!(retained <= usize::try_from(budget.get()).expect("budget fits usize"));
    }
    assert_golden(
        build.finish(&driver).await,
        "eviction-bounded retained session",
    );
}

#[tokio::test]
async fn uncommitted_step_drops_its_session_and_the_retry_matches_golden() {
    let driver = driver();
    let mut build = GoldenBuild::start("vector-build-cache-uncommitted").await;
    for _ in 0..3 {
        assert_eq!(
            build.step(&driver).await,
            CommittedOperationStep::Progressed
        );
    }
    let operation = build.operation().await;
    assert!(retained_checkpoint(&driver).is_some());

    // Stage one step exactly as the outbox does, then abandon it the way a
    // failed, conflicted, or crashed commit does.
    let transaction = build
        .db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("abandoned step transaction opens");
    let execution = driver
        .step(
            &build.db,
            &transaction,
            build.scope,
            &operation,
            limits_with_output_operations(2_048),
        )
        .await
        .expect("abandoned step stages");
    assert!(matches!(
        execution.committed_state(),
        Some(CommittedStepState::VectorBuild(_))
    ));
    drop(execution);
    transaction.rollback();
    assert!(
        retained_checkpoint(&driver).is_none(),
        "planning state of an uncommitted step must never be retained"
    );
    assert_eq!(build.operation().await.progress(), operation.progress());

    assert_eq!(
        build.step(&driver).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(
        retained_checkpoint(&driver).map(|checkpoint| checkpoint.progress),
        Some(build.operation().await.progress().clone())
    );
    assert_golden(
        build.finish(&driver).await,
        "retry after an uncommitted step",
    );
}

#[tokio::test]
async fn session_is_not_reused_after_another_driver_commits() {
    let first = driver();
    let second = driver();
    let mut build = GoldenBuild::start("vector-build-cache-stale-checkpoint").await;
    for _ in 0..3 {
        assert_eq!(build.step(&first).await, CommittedOperationStep::Progressed);
    }
    let stale = retained_checkpoint(&first).expect("first driver retains its session");

    // Another driver (a failover writer or a concurrent task) commits a step
    // the first driver's cache never observed.
    assert_eq!(
        build.step(&second).await,
        CommittedOperationStep::Progressed
    );
    assert_ne!(&stale.progress, build.operation().await.progress());
    assert_eq!(retained_checkpoint(&first), Some(stale));

    // Reusing the stale rows would relink against a graph missing the second
    // driver's step and diverge from the golden graph.
    assert_eq!(build.step(&first).await, CommittedOperationStep::Progressed);
    assert_eq!(
        retained_checkpoint(&first).map(|checkpoint| checkpoint.progress),
        Some(build.operation().await.progress().clone())
    );
    assert_golden(
        build.finish(&first).await,
        "stale checkpoint after another commit",
    );
}

#[tokio::test]
async fn blocked_and_aborted_builds_release_their_retained_session() {
    let driver = driver();
    let mut build = GoldenBuild::start("vector-build-cache-blocked").await;
    assert_eq!(
        build.step(&driver).await,
        CommittedOperationStep::Progressed
    );
    assert!(retained_checkpoint(&driver).is_some());
    let tiny_output = SearchIndexBatchLimits::try_new(
        NonZeroUsize::MIN,
        NonZeroU64::new(1024 * 1024).expect("input limit is positive"),
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
    )
    .expect("tiny output policy validates");
    assert_eq!(
        drive_one(
            &build.db,
            &driver,
            build.operation_id,
            &mut build.claim_sequence,
            tiny_output,
        )
        .await,
        CommittedOperationStep::Blocked
    );
    assert!(retained_checkpoint(&driver).is_none());
    build
        .db
        .close()
        .await
        .expect("blocked build database closes");

    let mut build = GoldenBuild::start("vector-build-cache-aborted").await;
    assert_eq!(
        build.step(&driver).await,
        CommittedOperationStep::Progressed
    );
    assert!(retained_checkpoint(&driver).is_some());
    let receipt = drop_index_operation(&build.db, build.scope, &build.definition)
        .await
        .expect("building vector converts to abort cleanup");
    assert!(matches!(
        receipt,
        IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build.operation_id
    ));
    assert_eq!(
        build.step(&driver).await,
        CommittedOperationStep::Progressed
    );
    assert!(
        retained_checkpoint(&driver).is_none(),
        "a committed cleanup step forgets its build session"
    );
    assert_eq!(
        drive_to_terminal(
            &build.db,
            &driver,
            build.operation_id,
            &mut build.claim_sequence
        )
        .await,
        CommittedOperationStep::Completed
    );
    assert!(retained_checkpoint(&driver).is_none());
    build
        .db
        .close()
        .await
        .expect("aborted build database closes");
}

#[tokio::test]
async fn build_cache_reuses_only_the_exact_committed_checkpoint() {
    type Euclidean = vector::distance::Euclidean;
    let db = test_db("vector-build-cache-checkpoints").await;
    let scope = DataScope::LegacyUnscoped;
    seed_sources(&db, scope, 2, |entity_id| integral_vector(entity_id, 3)).await;
    let first_definition = vector_definition(3, VectorDistanceMetric::Euclidean);
    let second_runtime = VectorIndexDefinition::new_node(
        "Document",
        "other_embedding",
        3,
        VectorDistanceMetric::Euclidean,
    )
    .expect("second vector definition validates");
    let second_definition = ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(&second_runtime)
            .expect("second V2 vector definition validates"),
    );
    let (first_id, _, _) = create_build(&db, scope, &first_definition, 1).await;
    let (second_id, _, _) = create_build(&db, scope, &second_definition, 1).await;
    let first = read_operation(&db, scope, first_id).await;
    let second = read_operation(&db, scope, second_id).await;
    let first_record = read_index(&db, scope, &first_definition).await;
    let second_record = read_index(&db, scope, &second_definition).await;
    let checkpoint = VectorBuildCheckpoint::new(&first, &first_record, first.progress().clone());
    let other_operation =
        VectorBuildCheckpoint::new(&second, &second_record, second.progress().clone());
    let mut advanced = checkpoint.clone();
    advanced.progress = IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
        VectorBuildStage::CatchUp(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        }),
    ));

    const BUDGET: u64 = 1 << 20;
    const MARKED: usize = 3;
    let cache = VectorBuildCache::new(NonZeroU64::new(BUDGET).expect("budget is positive"));
    let marked = |checkpoint: &VectorBuildCheckpoint| {
        Some(CommittedStepState::VectorBuild(Box::new(
            RetainedVectorBuild {
                checkpoint: checkpoint.clone(),
                session: Box::new(VectorBuildSession::<Euclidean>::with_test_simhashes(
                    NonZeroU64::new(BUDGET).expect("marker budget is positive"),
                    u64::try_from(MARKED).expect("marker count fits u64"),
                )),
            },
        )))
    };

    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &checkpoint).await,
        0,
        "an empty cache yields a fresh session"
    );
    for committed in [
        CommittedOperationStep::Blocked,
        CommittedOperationStep::Completed,
        CommittedOperationStep::TransientFailure,
    ] {
        cache
            .after_commit(first_id, committed, marked(&checkpoint))
            .await;
        assert!(
            retained_checkpoints(&cache).is_empty(),
            "{committed:?} retains nothing"
        );
    }

    cache
        .after_commit(
            first_id,
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &checkpoint).await,
        MARKED,
        "the exact checkpoint is reused"
    );
    assert!(
        retained_checkpoints(&cache).is_empty(),
        "checkout moves the session out"
    );

    cache
        .after_commit(
            first_id,
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    assert_eq!(
        checked_out_simhashes::<vector::distance::Cosine>(&cache, &checkpoint).await,
        0,
        "a session of another metric is never reused"
    );

    cache
        .after_commit(
            first_id,
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &other_operation).await,
        0
    );
    assert_eq!(
        retained_checkpoints(&cache),
        vec![checkpoint.clone()],
        "another operation's step leaves the session in place"
    );
    cache
        .after_commit(second_id, CommittedOperationStep::Progressed, None)
        .await;
    assert_eq!(retained_checkpoints(&cache), vec![checkpoint.clone()]);

    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &advanced).await,
        0,
        "a stale checkpoint is dropped"
    );
    assert!(retained_checkpoints(&cache).is_empty());

    cache
        .after_commit(
            first_id,
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    cache
        .after_commit(first_id, CommittedOperationStep::Progressed, None)
        .await;
    assert!(
        retained_checkpoints(&cache).is_empty(),
        "a committed step of the same operation without state forgets it"
    );

    cache
        .after_commit(
            first_id,
            CommittedOperationStep::Progressed,
            marked(&checkpoint),
        )
        .await;
    cache
        .after_commit(
            second_id,
            CommittedOperationStep::Progressed,
            marked(&other_operation),
        )
        .await;
    assert_eq!(
        retained_checkpoints(&cache),
        vec![checkpoint.clone(), other_operation.clone()],
        "another operation's commit keeps this operation's session"
    );
    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &checkpoint).await,
        MARKED
    );
    assert_eq!(
        checked_out_simhashes::<Euclidean>(&cache, &other_operation).await,
        MARKED
    );
    db.close().await.expect("checkpoint database closes");
}

/// Checks out `checkpoint`'s session and returns the SimHashes it retains.
async fn checked_out_simhashes<D: Distance>(
    cache: &VectorBuildCache,
    checkpoint: &VectorBuildCheckpoint,
) -> usize {
    cache.checkout::<D>(checkpoint).await.simhash_count()
}

/// Returns the checkpoints a cache retains, least recently committed first.
fn retained_checkpoints(cache: &VectorBuildCache) -> Vec<VectorBuildCheckpoint> {
    cache
        .retained
        .try_lock()
        .expect("no commit is trimming the retained sessions")
        .iter()
        .map(|retained| retained.checkpoint.clone())
        .collect()
}

/// Wraps `session` as the committed state of a progressed step at `checkpoint`.
fn committed_session(
    checkpoint: &VectorBuildCheckpoint,
    session: VectorBuildSession<vector::distance::Euclidean>,
) -> Option<CommittedStepState> {
    Some(CommittedStepState::VectorBuild(Box::new(
        RetainedVectorBuild {
            checkpoint: checkpoint.clone(),
            session: Box::new(session),
        },
    )))
}

/// Returns a checkpoint of `checkpoint`'s shape for a new operation.
fn another_operation(checkpoint: &VectorBuildCheckpoint) -> VectorBuildCheckpoint {
    VectorBuildCheckpoint {
        operation_id: IndexOperationId::new_v4(),
        ..checkpoint.clone()
    }
}

#[tokio::test]
async fn retained_sessions_share_the_budget_max_min_fairly() {
    type Euclidean = vector::distance::Euclidean;
    let build = GoldenBuild::start("vector-build-cache-shared-budget").await;
    let record = read_index(&build.db, build.scope, &build.definition).await;
    let operation = build.operation().await;
    let first = VectorBuildCheckpoint::new(&operation, &record, operation.progress().clone());
    let second = another_operation(&first);
    let third = another_operation(&first);
    let per_simhash = 96 + core::mem::size_of::<u64>();
    let namespace = 4_096;
    let session = |simhashes| {
        VectorBuildSession::<Euclidean>::with_test_simhashes(
            NonZeroU64::new(1 << 20).expect("session budget is positive"),
            simhashes,
        )
    };
    assert_eq!(
        RetainedBuildSession::retained_bytes(&session(400)),
        namespace + 400 * per_simhash
    );
    const BUDGET: usize = 64 * 1024;
    let cache = VectorBuildCache::new(
        NonZeroU64::new(u64::try_from(BUDGET).expect("budget fits u64")).expect("positive"),
    );
    let sizes = || {
        cache
            .retained
            .try_lock()
            .expect("no commit is trimming the retained sessions")
            .iter()
            .map(|retained| retained.session.retained_bytes())
            .collect::<Vec<_>>()
    };

    // One session within the budget is kept whole.
    cache
        .after_commit(
            first.operation_id,
            CommittedOperationStep::Progressed,
            committed_session(&first, session(400)),
        )
        .await;
    assert_eq!(sizes(), vec![namespace + 400 * per_simhash]);

    // Two large sessions shrink to equal halves instead of one evicting the other.
    cache
        .after_commit(
            second.operation_id,
            CommittedOperationStep::Progressed,
            committed_session(&second, session(400)),
        )
        .await;
    for size in sizes() {
        assert!(
            (BUDGET / 2 - per_simhash..=BUDGET / 2).contains(&size),
            "{size}"
        );
    }

    // A session under its fair share keeps every entry; the others split the rest.
    let small = namespace + 10 * per_simhash;
    cache
        .after_commit(
            third.operation_id,
            CommittedOperationStep::Progressed,
            committed_session(&third, session(10)),
        )
        .await;
    let cap = (BUDGET - small) / 2;
    let shared = sizes();
    assert_eq!(shared[2], small);
    for size in &shared[..2] {
        assert!((cap - per_simhash..=cap).contains(size), "{size}");
    }
    assert!(shared.iter().sum::<usize>() <= BUDGET);
    assert_eq!(
        retained_checkpoints(&cache),
        vec![first.clone(), second.clone(), third.clone()]
    );
    build
        .db
        .close()
        .await
        .expect("shared budget database closes");
}

#[tokio::test]
async fn retained_sessions_are_bounded_per_operation_and_in_count() {
    type Euclidean = vector::distance::Euclidean;
    let build = GoldenBuild::start("vector-build-cache-count-bound").await;
    let record = read_index(&build.db, build.scope, &build.definition).await;
    let operation = build.operation().await;
    let first = VectorBuildCheckpoint::new(&operation, &record, operation.progress().clone());
    let cache = VectorBuildCache::new(NonZeroU64::new(1 << 20).expect("budget is positive"));
    let empty = || VectorBuildSession::<Euclidean>::new(NonZeroU64::MIN);
    let checkpoints = core::iter::once(first.clone())
        .chain((0..MAX_RETAINED_VECTOR_BUILDS).map(|_| another_operation(&first)))
        .collect::<Vec<_>>();

    // A newer commit of the same operation replaces its session in place.
    for _ in 0..2 {
        cache
            .after_commit(
                first.operation_id,
                CommittedOperationStep::Progressed,
                committed_session(&first, empty()),
            )
            .await;
    }
    assert_eq!(retained_checkpoints(&cache), vec![first.clone()]);

    for checkpoint in &checkpoints[1..] {
        cache
            .after_commit(
                checkpoint.operation_id,
                CommittedOperationStep::Progressed,
                committed_session(checkpoint, empty()),
            )
            .await;
    }
    assert_eq!(
        retained_checkpoints(&cache),
        checkpoints[1..].to_vec(),
        "the least recently committed session is evicted beyond the count bound"
    );
    build.db.close().await.expect("count bound database closes");
}

#[tokio::test]
async fn interleaved_builds_each_keep_their_own_retained_session() {
    let driver = driver();
    let mut first = GoldenBuild::start("vector-build-cache-interleaved-first").await;
    let mut second = GoldenBuild::start("vector-build-cache-interleaved-second").await;
    for _ in 0..3 {
        for build in [&mut first, &mut second] {
            assert_eq!(
                build.step(&driver).await,
                CommittedOperationStep::Progressed
            );
        }
        for build in [&first, &second] {
            assert_eq!(
                retained_checkpoint_for(&driver, build.operation_id)
                    .map(|checkpoint| checkpoint.progress),
                Some(build.operation().await.progress().clone()),
                "each build retains the checkpoint its next step starts from"
            );
        }
    }
    assert_golden(first.finish(&driver).await, "first interleaved build");
    assert_golden(second.finish(&driver).await, "second interleaved build");
    assert!(driver
        .build_cache
        .retained
        .try_lock()
        .expect("no commit is trimming the retained sessions")
        .is_empty());
}

/// Summarizes each retained golden session, least recently committed first.
///
/// Yields the operation, the bytes the session charges, the budget it was
/// last checked out with, and the entries it evicted since that checkout.
fn retained_summaries(driver: &VectorIndexDriver) -> Vec<(IndexOperationId, usize, usize, u64)> {
    driver
        .build_cache
        .retained
        .try_lock()
        .expect("no commit is trimming the retained sessions")
        .iter()
        .map(|retained| {
            let session: &dyn Any = retained.session.as_ref();
            let session = session
                .downcast_ref::<VectorBuildSession<vector::distance::Euclidean>>()
                .expect("golden builds retain Euclidean sessions");
            let stats = session.stats();
            (
                retained.checkpoint.operation_id,
                RetainedBuildSession::retained_bytes(session),
                session.max_payload_bytes(),
                stats.item_evictions() + stats.neighbor_evictions() + stats.simhash_evictions(),
            )
        })
        .collect()
}

#[tokio::test]
async fn interleaved_builds_step_within_their_max_min_shares() {
    const BUDGET: usize = 24 * 1024;
    // A trimmed session can end up to one evicted entry under its cap.
    const ENTRY_SLACK: usize = 1024;
    let driver = driver().with_build_cache_bytes(
        NonZeroU64::new(u64::try_from(BUDGET).expect("budget fits u64")).expect("positive"),
    );
    let mut first = GoldenBuild::start("vector-build-cache-shares-first").await;
    let mut second = GoldenBuild::start("vector-build-cache-shares-second").await;
    let mut evicted = false;
    for _ in 0..3 {
        for build in [&mut first, &mut second] {
            let operation_id = build.operation_id;
            let others = |summaries: &[(IndexOperationId, usize, usize, u64)]| {
                summaries
                    .iter()
                    .filter(|(other, ..)| *other != operation_id)
                    .map(|(_, bytes, ..)| *bytes)
                    .collect::<Vec<_>>()
            };
            let before = retained_summaries(&driver);
            let own_before = before
                .iter()
                .find(|(other, ..)| *other == operation_id)
                .map_or(0, |(_, bytes, ..)| *bytes);
            let others_before = others(&before);
            let share = max_min_cap(
                BUDGET,
                others_before
                    .iter()
                    .copied()
                    .chain(core::iter::once(usize::MAX)),
            )
            .expect("an unbounded demand exceeds a bounded budget");

            assert_eq!(
                build.step(&driver).await,
                CommittedOperationStep::Progressed
            );
            let after = retained_summaries(&driver);
            let &(_, own_after, own_budget, own_evictions) = after
                .iter()
                .find(|(other, ..)| *other == operation_id)
                .expect("a progressed scan step retains its session");
            evicted |= own_evictions > 0;
            assert_eq!(
                own_budget, share,
                "a checked-out session is bound to its max-min share"
            );
            assert!(
                own_after <= share,
                "per-entity eviction keeps the step within its share"
            );
            let trimmed = others_before
                .iter()
                .sum::<usize>()
                .checked_sub(others(&after).iter().sum::<usize>())
                .expect("other sessions never grow while one build steps");
            assert!(
                trimmed <= own_after.saturating_sub(own_before) + ENTRY_SLACK,
                "a commit trims only what its step added beyond the budget: {trimmed}"
            );
            assert!(after.iter().map(|(_, bytes, ..)| bytes).sum::<usize>() <= BUDGET);
        }
    }
    assert!(evicted, "the budget binds, so the shares matter");
    assert_golden(first.finish(&driver).await, "first shared-budget build");
    assert_golden(second.finish(&driver).await, "second shared-budget build");
}

/// Budget bytes that admit one [`ShrinkProbe`] entry.
const PROBE_BYTES_PER_ENTRY: usize = 100;

/// Retained session of a fixed charged size that records the threads shrinking it.
///
/// Like a real session, it caps its one entry class at one entry per
/// [`PROBE_BYTES_PER_ENTRY`] bytes of its budget, so rebinding it to a smaller
/// share can leave it over its class cap while its bytes still fit.
struct ShrinkProbe {
    bytes: usize,
    entries: usize,
    max_entries: usize,
    shrunk_on: Arc<parking_lot::Mutex<Vec<std::thread::ThreadId>>>,
}

impl RetainedBuildSession for ShrinkProbe {
    fn retained_bytes(&self) -> usize {
        self.bytes
    }

    fn shrink_to(&mut self, max_bytes: usize) -> Result<()> {
        self.shrunk_on.lock().push(std::thread::current().id());
        self.bytes = self.bytes.min(max_bytes);
        self.entries = self.entries.min(self.max_entries);
        Ok(())
    }

    fn set_max_retained_bytes(&mut self, max_bytes: NonZeroU64) {
        self.max_entries = usize::try_from(max_bytes.get()).expect("probe budget fits usize")
            / PROBE_BYTES_PER_ENTRY;
    }

    fn exceeds_limits(&self) -> bool {
        self.entries > self.max_entries
    }
}

/// Wraps a [`ShrinkProbe`] bound to `budget` as a progressed step's committed state.
fn committed_probe(
    checkpoint: &VectorBuildCheckpoint,
    budget: usize,
    bytes: usize,
    entries: usize,
    shrunk_on: &Arc<parking_lot::Mutex<Vec<std::thread::ThreadId>>>,
) -> Option<CommittedStepState> {
    Some(CommittedStepState::VectorBuild(Box::new(
        RetainedVectorBuild {
            checkpoint: checkpoint.clone(),
            session: Box::new(ShrinkProbe {
                bytes,
                entries,
                max_entries: budget / PROBE_BYTES_PER_ENTRY,
                shrunk_on: Arc::clone(shrunk_on),
            }),
        },
    )))
}

#[tokio::test]
async fn commits_trim_retained_sessions_off_the_executor_thread() {
    type Euclidean = vector::distance::Euclidean;
    let build = GoldenBuild::start("vector-build-cache-trim-thread").await;
    let record = read_index(&build.db, build.scope, &build.definition).await;
    let operation = build.operation().await;
    let first = VectorBuildCheckpoint::new(&operation, &record, operation.progress().clone());
    let second = another_operation(&first);
    let cache = VectorBuildCache::new(NonZeroU64::new(1_000).expect("budget is positive"));
    let shrunk_on = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let probe = |checkpoint: &VectorBuildCheckpoint, bytes| {
        committed_probe(checkpoint, 1_000, bytes, 0, &shrunk_on)
    };

    cache
        .after_commit(
            first.operation_id,
            CommittedOperationStep::Progressed,
            probe(&first, 800),
        )
        .await;
    assert!(
        shrunk_on.lock().is_empty(),
        "sessions that fit the budget are not trimmed"
    );
    cache
        .after_commit(
            second.operation_id,
            CommittedOperationStep::Progressed,
            probe(&second, 800),
        )
        .await;
    let executor = std::thread::current().id();
    let threads = shrunk_on.lock().clone();
    assert_eq!(threads.len(), 2, "both sessions over their share shrink");
    assert!(
        threads.iter().all(|thread| *thread != executor),
        "trimming never runs on the executor thread"
    );
    assert_eq!(
        cache
            .retained
            .try_lock()
            .expect("the trim released the retained sessions")
            .iter()
            .map(|retained| retained.session.retained_bytes())
            .collect::<Vec<_>>(),
        vec![500, 500]
    );

    // Each build is checked out at its max-min share beside the others.
    assert_eq!(
        cache
            .checkout::<Euclidean>(&another_operation(&first))
            .await
            .max_payload_bytes(),
        333,
        "a third build splits the budget three ways"
    );
    let reused = cache.checkout::<Euclidean>(&first).await;
    assert_eq!(
        (reused.simhash_count(), reused.max_payload_bytes()),
        (0, 500),
        "a session of another type is never reused"
    );
    assert_eq!(
        cache
            .checkout::<Euclidean>(&second)
            .await
            .max_payload_bytes(),
        1_000,
        "the only build is bound to the whole budget"
    );
    build.db.close().await.expect("trim thread database closes");
}

#[tokio::test]
async fn checkouts_evict_class_cap_excess_off_the_executor_thread() {
    type Euclidean = vector::distance::Euclidean;
    const BUDGET: usize = 1_000;
    let build = GoldenBuild::start("vector-build-cache-checkout-thread").await;
    let record = read_index(&build.db, build.scope, &build.definition).await;
    let operation = build.operation().await;
    let first = VectorBuildCheckpoint::new(&operation, &record, operation.progress().clone());
    let second = another_operation(&first);
    let cache = VectorBuildCache::new(
        NonZeroU64::new(u64::try_from(BUDGET).expect("budget fits u64")).expect("positive"),
    );
    let shrunk_on = Arc::new(parking_lot::Mutex::new(Vec::new()));

    // `first` holds 8 of the 10 entries the whole budget allows, in 300 bytes,
    // and `second` 600 bytes: both fit, so no commit trims.
    for (checkpoint, bytes, entries) in [(&first, 300, 8), (&second, 600, 0)] {
        cache
            .after_commit(
                checkpoint.operation_id,
                CommittedOperationStep::Progressed,
                committed_probe(checkpoint, BUDGET, bytes, entries, &shrunk_on),
            )
            .await;
    }
    assert!(shrunk_on.lock().is_empty());

    // Its share halves beside `second`, capping it at 5 entries: its bytes
    // still fit, its entries do not, and checkout evicts them off the executor.
    assert_eq!(
        cache
            .checkout::<Euclidean>(&first)
            .await
            .max_payload_bytes(),
        BUDGET / 2
    );
    let threads = shrunk_on.lock().clone();
    assert_eq!(threads.len(), 1, "checkout evicts the class-cap excess");
    assert_ne!(
        threads[0],
        std::thread::current().id(),
        "checkout never evicts on the executor thread"
    );

    // A reused session within its rebound limits is returned untouched.
    cache
        .after_commit(
            first.operation_id,
            CommittedOperationStep::Progressed,
            committed_probe(&first, BUDGET, 300, 5, &shrunk_on),
        )
        .await;
    cache.checkout::<Euclidean>(&first).await;
    assert_eq!(shrunk_on.lock().len(), 1);
    build
        .db
        .close()
        .await
        .expect("checkout thread database closes");
}

#[tokio::test]
async fn reused_sessions_are_checked_out_within_their_rebound_class_caps() {
    type Euclidean = vector::distance::Euclidean;
    const BUDGET: usize = 64 * 1024;
    let build = GoldenBuild::start("vector-build-cache-rebound-class-caps").await;
    let record = read_index(&build.db, build.scope, &build.definition).await;
    let operation = build.operation().await;
    let first = VectorBuildCheckpoint::new(&operation, &record, operation.progress().clone());
    let second = another_operation(&first);
    let cache = VectorBuildCache::new(
        NonZeroU64::new(u64::try_from(BUDGET).expect("budget fits u64")).expect("positive"),
    );
    let simhashes = |budget: usize, simhashes| {
        VectorBuildSession::<Euclidean>::with_test_simhashes(
            NonZeroU64::new(u64::try_from(budget).expect("budget fits u64")).expect("positive"),
            simhashes,
        )
    };

    // Under the whole budget `first` holds its cap of 64 SimHashes in about a
    // sixth of the bytes, and `second` retains most of the rest. Both fit, so
    // the commits trim nothing.
    let dense = simhashes(BUDGET, 64);
    assert!(!dense.exceeds_limits());
    let dense_bytes = RetainedBuildSession::retained_bytes(&dense);
    let large = simhashes(1 << 40, 450);
    let large_bytes = RetainedBuildSession::retained_bytes(&large);
    assert!(dense_bytes + large_bytes <= BUDGET);
    for (checkpoint, session) in [(&first, dense), (&second, large)] {
        cache
            .after_commit(
                checkpoint.operation_id,
                CommittedOperationStep::Progressed,
                committed_session(checkpoint, session),
            )
            .await;
    }

    // Beside `second`, `first` is rebound to half the budget: its bytes fit,
    // but its SimHash cap halves to 32.
    let mut reused = cache.checkout::<Euclidean>(&first).await;
    assert_eq!(reused.max_payload_bytes(), BUDGET / 2);
    assert!(dense_bytes <= BUDGET / 2);
    assert_eq!(
        reused.simhash_count(),
        32,
        "checkout evicts the class-cap excess"
    );
    assert!(!reused.exceeds_limits());

    // So the step's first per-entity eviction pass evicts nothing.
    reused
        .shrink_to(usize::MAX)
        .expect("a reused session is clean");
    assert_eq!(reused.stats().simhash_evictions(), 0);
    assert_eq!(reused.simhash_count(), 32);
    build
        .db
        .close()
        .await
        .expect("rebound class cap database closes");
}

/// Checks `operation_id`'s retained session out as its next step would, then retains it again.
///
/// Asserts that the checked-out session is within every limit of its max-min
/// share, so the step's first per-entity eviction pass evicts nothing, and
/// returns whether checkout evicted entries to get it there. Returns `false`
/// when the operation retains no session.
async fn check_out_within_limits(
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    budget: usize,
) -> bool {
    let Some((checkpoint, held, bytes, others)) = ({
        let retained = driver
            .build_cache
            .retained
            .try_lock()
            .expect("no commit is trimming the retained sessions");
        retained
            .iter()
            .find(|retained| retained.checkpoint.operation_id == operation_id)
            .map(|own| {
                let session: &dyn Any = own.session.as_ref();
                let session = session
                    .downcast_ref::<VectorBuildSession<vector::distance::Euclidean>>()
                    .expect("golden builds retain Euclidean sessions");
                (
                    own.checkpoint.clone(),
                    (
                        session.item_count(),
                        session.neighbor_count(),
                        session.simhash_count(),
                    ),
                    own.session.retained_bytes(),
                    retained
                        .iter()
                        .filter(|other| other.checkpoint.operation_id != operation_id)
                        .map(|other| other.session.retained_bytes())
                        .collect::<Vec<_>>(),
                )
            })
    }) else {
        return false;
    };
    let share = max_min_cap(
        budget,
        others.into_iter().chain(core::iter::once(usize::MAX)),
    )
    .expect("an unbounded demand exceeds a bounded budget");
    assert!(
        bytes <= share,
        "a reused session's bytes already fit its share"
    );

    let mut session = driver
        .build_cache
        .checkout::<vector::distance::Euclidean>(&checkpoint)
        .await;
    assert_eq!(session.max_payload_bytes(), share);
    assert!(
        !session.exceeds_limits(),
        "a reused session is checked out within its rebound limits"
    );
    let kept = (
        session.item_count(),
        session.neighbor_count(),
        session.simhash_count(),
    );
    assert!(kept.0 <= held.0 && kept.1 <= held.1 && kept.2 <= held.2);
    session
        .shrink_to(usize::MAX)
        .expect("a reused session is clean");
    let stats = session.stats();
    assert_eq!(
        stats.item_evictions() + stats.neighbor_evictions() + stats.simhash_evictions(),
        0,
        "the step's first per-entity eviction pass evicts nothing"
    );
    driver
        .build_cache
        .after_commit(
            operation_id,
            CommittedOperationStep::Progressed,
            committed_session(&checkpoint, session),
        )
        .await;
    kept != held
}

#[tokio::test]
async fn joining_builds_check_out_class_capped_sessions_within_their_caps() {
    // Golden sessions reach every class cap well under this budget, so the
    // caps bind before the bytes do.
    const BUDGET: usize = 128 * 1024;
    let driver = driver().with_build_cache_bytes(
        NonZeroU64::new(u64::try_from(BUDGET).expect("budget fits u64")).expect("positive"),
    );
    let mut first = GoldenBuild::start("vector-build-cache-class-caps-first").await;
    let mut second = GoldenBuild::start("vector-build-cache-class-caps-second").await;

    // `first` steps alone under the whole budget, then `second` joins.
    for _ in 0..2 {
        assert!(!check_out_within_limits(&driver, first.operation_id, BUDGET).await);
        assert_eq!(
            first.step(&driver).await,
            CommittedOperationStep::Progressed
        );
    }
    let mut evicted = false;
    for _ in 0..2 {
        for build in [&mut second, &mut first] {
            evicted |= check_out_within_limits(&driver, build.operation_id, BUDGET).await;
            assert_eq!(
                build.step(&driver).await,
                CommittedOperationStep::Progressed
            );
        }
    }
    assert!(
        evicted,
        "a joining build lowered a reused session's class caps below what it held"
    );
    assert_golden(first.finish(&driver).await, "first class-capped build");
    assert_golden(second.finish(&driver).await, "second class-capped build");
}

#[test]
fn max_min_cap_splits_the_budget_fairly() {
    for (sizes, cap) in [
        (vec![], None),
        (vec![10, 20], None),
        (vec![50, 50], None),
        (vec![10, 30, 60], None),
        (vec![60, 60], Some(50)),
        (vec![60, 10, 60], Some(45)),
        (vec![10, 40, 60], Some(50)),
        (vec![usize::MAX], Some(100)),
        (vec![10, usize::MAX], Some(90)),
        (vec![60, usize::MAX], Some(50)),
        (vec![60, 60, usize::MAX], Some(33)),
    ] {
        assert_eq!(max_min_cap(100, sizes.clone()), cap, "{sizes:?}");
    }
    assert_eq!(
        max_min_cap(usize::MAX, [0, usize::MAX]),
        None,
        "only an unbounded budget fits an unbounded demand"
    );
}

#[tokio::test]
async fn only_scan_and_catch_up_checkpoints_retain_a_session() {
    let driver = driver();
    let mut build = GoldenBuild::start("vector-build-cache-stage-scope").await;
    loop {
        assert_eq!(
            build.step(&driver).await,
            CommittedOperationStep::Progressed
        );
        let progress = build.operation().await.progress().clone();
        let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::Scan(_) | VectorBuildStage::CatchUp(_),
        )) = &progress
        else {
            assert_eq!(
                retained_checkpoint(&driver),
                None,
                "no step checks out a session at {progress:?}"
            );
            break;
        };
        assert_eq!(
            retained_checkpoint(&driver).map(|checkpoint| checkpoint.progress),
            Some(progress)
        );
    }
    assert_golden(build.finish(&driver).await, "stage-scoped retention");
}

/// Source mutations applied while a build is still scanning.
enum SourceChange {
    /// Replaces the vector and moves the entity to `tenant`.
    Upsert { entity_id: u64, tenant: i64 },
    /// Removes the indexed property.
    Remove { entity_id: u64 },
}

/// Chooses which driver runs each step of [`mutated_build_digests`].
enum StepDrivers {
    /// One driver retains its planning cache across every step.
    Shared(Box<VectorIndexDriver>),
    /// A new driver per step reproduces per-step planning sessions.
    FreshPerStep,
}

/// Drives one step with the shared driver or a new one.
async fn drive_with(
    db: &Db,
    drivers: &StepDrivers,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> CommittedOperationStep {
    match drivers {
        StepDrivers::Shared(shared) => {
            drive_one(db, shared, operation_id, claim_sequence, limits).await
        }
        StepDrivers::FreshPerStep => {
            drive_one(db, &driver(), operation_id, claim_sequence, limits).await
        }
    }
}

/// Builds a fixture whose sources change mid-build and digests every namespace.
///
/// Scanned and unscanned entities are replaced, removed, and (when
/// partitioned) moved between tenants, so catch-up exercises deletes,
/// replacements, partition moves, and new mappings on top of retained state.
async fn mutated_build_digests(
    name: &str,
    partitioned: bool,
    drivers: StepDrivers,
) -> Vec<(usize, String)> {
    const ENTITIES: u64 = 180;
    const DIMENSION: usize = 8;
    let db = test_db(name).await;
    let scope = DataScope::LegacyUnscoped;
    let runtime = VectorIndexDefinition::new_node(
        "Document",
        "embedding",
        DIMENSION,
        VectorDistanceMetric::Euclidean,
    )
    .expect("mutated fixture definition validates");
    let runtime = if partitioned {
        runtime
            .with_tenant_property("account_id")
            .expect("tenant property validates")
    } else {
        runtime
    };
    let definition = ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
            .expect("mutated fixture V2 definition validates"),
    );
    let source = |seed: u64, tenant: i64| {
        let mut properties = vec![
            Property::new("$label", PropertyValue::String("Document".to_string())),
            Property::new(
                "embedding",
                PropertyValue::F32Array(integral_vector(seed, DIMENSION)),
            ),
        ];
        if partitioned {
            properties.push(Property::new("account_id", PropertyValue::I64(tenant)));
        }
        properties
    };
    let mut current = (0..ENTITIES)
        .map(|entity_id| {
            let tenant = 10 + 10 * i64::try_from(entity_id % 3).expect("tenant index fits i64");
            source(entity_id, tenant)
        })
        .collect::<Vec<_>>();
    for (entity_id, properties) in (0..ENTITIES).zip(&current) {
        put_source(&db, scope, entity_id, properties).await;
    }
    let (operation_id, index_id, generation) =
        create_build(&db, scope, &definition, ENTITIES - 1).await;
    let limits = limits_with_output_operations(1_024);
    let mut claim_sequence = 1;
    let rounds = [
        (
            3,
            vec![
                SourceChange::Upsert {
                    entity_id: 5,
                    tenant: 20,
                },
                SourceChange::Remove { entity_id: 7 },
                SourceChange::Upsert {
                    entity_id: 11,
                    tenant: 40,
                },
                SourceChange::Upsert {
                    entity_id: 170,
                    tenant: 10,
                },
            ],
        ),
        (
            2,
            vec![
                SourceChange::Upsert {
                    entity_id: 5,
                    tenant: 30,
                },
                SourceChange::Remove { entity_id: 9 },
                SourceChange::Upsert {
                    entity_id: 7,
                    tenant: 40,
                },
            ],
        ),
    ];
    let mut seed = 1_000;
    for (steps, changes) in rounds {
        for _ in 0..steps {
            assert_eq!(
                drive_with(&db, &drivers, operation_id, &mut claim_sequence, limits).await,
                CommittedOperationStep::Progressed
            );
        }
        for change in changes {
            let (entity_id, after) = match change {
                SourceChange::Upsert { entity_id, tenant } => {
                    seed += 1;
                    (entity_id, source(seed, tenant))
                }
                SourceChange::Remove { entity_id } => (
                    entity_id,
                    vec![Property::new(
                        "$label",
                        PropertyValue::String("Document".to_string()),
                    )],
                ),
            };
            let index = usize::try_from(entity_id).expect("fixture entity fits usize");
            mutate_building_source(&db, scope, entity_id, &current[index], &after).await;
            current[index] = after;
        }
    }
    loop {
        match drive_with(&db, &drivers, operation_id, &mut claim_sequence, limits).await {
            CommittedOperationStep::Progressed => {}
            CommittedOperationStep::Completed => break,
            stopped @ (CommittedOperationStep::Blocked
            | CommittedOperationStep::TransientFailure) => {
                panic!("mutated build stopped at {stopped:?}")
            }
        }
    }
    let active = read_index(&db, scope, &definition).await;
    let IndexStateV2::Active {
        physical: PhysicalGeneration::Vector { layout, .. },
        ..
    } = active.state()
    else {
        panic!("completed mutated build is active");
    };
    let mut physical_index_ids = match layout {
        VectorPhysicalLayout::Unpartitioned { physical_index_id } => vec![*physical_index_id],
        VectorPhysicalLayout::Partitioned => mapping_values(&db, scope, index_id, generation)
            .await
            .into_iter()
            .map(|mapping| mapping.physical_index_id)
            .collect(),
    };
    physical_index_ids.sort_unstable();
    let mut digests = Vec::with_capacity(physical_index_ids.len());
    for physical_index_id in physical_index_ids {
        digests.push(physical_digest(&db, scope, physical_index_id).await);
    }
    db.close().await.expect("mutated build database closes");
    digests
}

#[tokio::test]
async fn catch_up_on_retained_sessions_matches_per_step_sessions() {
    for partitioned in [false, true] {
        let reference = mutated_build_digests(
            &format!("vector-build-mutated-fresh-{partitioned}"),
            partitioned,
            StepDrivers::FreshPerStep,
        )
        .await;
        assert!(!reference.is_empty());
        if partitioned {
            assert!(reference.len() >= 3, "every tenant owns a namespace");
        }
        for (label, driver) in [
            ("shared", driver()),
            (
                "evicting",
                driver()
                    .with_build_cache_bytes(NonZeroU64::new(8 * 1024).expect("budget is positive")),
            ),
        ] {
            let digests = mutated_build_digests(
                &format!("vector-build-mutated-{label}-{partitioned}"),
                partitioned,
                StepDrivers::Shared(Box::new(driver)),
            )
            .await;
            assert_eq!(digests, reference, "{label} partitioned={partitioned}");
        }
    }
}

/// Returns user plus system CPU seconds consumed by this process.
fn process_cpu_seconds() -> f64 {
    let mut usage = core::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fully initializes the provided `rusage` on success.
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(status, 0, "getrusage succeeds for the current process");
    // SAFETY: the successful call above initialized every field.
    let usage = unsafe { usage.assume_init() };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

/// Reports source-backfill throughput for 20k 128-dimensional cosine vectors.
///
/// Run with `cargo test -p db --lib --release -- --ignored --nocapture
/// vector_source_backfill_throughput_report`. Process CPU seconds are
/// reported beside wall time because shared build hosts distort wall time.
#[tokio::test]
#[ignore = "manual throughput report; run in release mode"]
async fn vector_source_backfill_throughput_report() {
    const ENTITIES: u64 = 20_000;
    const DIMENSION: usize = 128;
    let db = test_db("vector-build-throughput-report").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = vector_definition(DIMENSION, VectorDistanceMetric::Cosine);
    seed_sources(&db, scope, ENTITIES, |entity_id| {
        unit_range_vector(entity_id, DIMENSION)
    })
    .await;
    let cpu_before = process_cpu_seconds();
    let report = build_to_active(
        &db,
        scope,
        &definition,
        ENTITIES,
        &driver(),
        SearchIndexBackfillLimits::default().batch(),
    )
    .await;
    let cpu_seconds = process_cpu_seconds() - cpu_before;
    let quarter = ENTITIES as f64 / 4.0;
    let mut previous = Duration::ZERO;
    for (index, elapsed) in report.quarter_elapsed.iter().enumerate() {
        println!(
            "quarter {} vectors/s={:.1}",
            index + 1,
            quarter / (*elapsed - previous).as_secs_f64()
        );
        previous = *elapsed;
    }
    println!(
        "vectors={ENTITIES} dimension={DIMENSION} steps={} scan={:.2}s scan vectors/s={:.1} total={:.2}s cpu={:.2}s vectors/cpu-s={:.1}",
        report.steps,
        report.scan_elapsed.as_secs_f64(),
        ENTITIES as f64 / report.scan_elapsed.as_secs_f64(),
        report.elapsed.as_secs_f64(),
        cpu_seconds,
        ENTITIES as f64 / cpu_seconds,
    );
    db.close().await.expect("throughput database closes");
}
