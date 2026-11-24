//! Contracts of the bulk vector build's lifecycle steps.
//!
//! This child of the bulk build drives whole builds through the production
//! outbox claim and step entry points against raw SlateDB databases, with the
//! fixtures of the vector driver contracts. A fresh driver stands for a
//! restarted process: it holds no graph, only what the database stores. Source
//! rows are written directly, so no operation is queued for them; what the
//! queue does with a changed entity after activation is the queue's contract.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use super::super::driver_contracts::{
    create_build, definition, drive_one, driver, properties, put_source, read_index,
    read_operation, source_cursor, source_key, test_db, NOW_MILLIS,
};
use super::super::*;
use super::*;
use crate::config::{SearchIndexBackfillLimits, VectorIndexDefinition};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v2::values::property::encode_properties;
use crate::index_lifecycle::lifecycle::drop_index_operation;
use crate::index_lifecycle::outbox::{
    claim_operation, execute_claimed_step, observe_operation_pointer, retry_operation,
    ClaimPermission, OperationPointerObservation, WriteDuringStepDriver,
};
use crate::index_lifecycle::worker::IndexWorkerWakeHandle;
use crate::index_lifecycle::{
    ClaimSequence, IndexDdlReceipt, IndexOperationExecutionState, IndexStateV2, WriterEpoch,
};
use crate::search::vector::distance::Euclidean;
use crate::search::vector::{SearchParams, SimHashMode};

const SCOPE: DataScope = DataScope::LegacyUnscoped;
/// Entity ids of the standard source are `0..SOURCE`.
const SOURCE: u64 = 240;
/// SlateDB's default `max_unflushed_bytes`, which the fixture drivers take
/// as their write buffer.
const WRITE_BUFFER: u64 = 1 << 30;
/// A budget that holds several builds of the standard source with the whole
/// write buffer each, retried ones included.
const BUDGET: u64 = 8 * (FIXED_ALLOWANCE_BYTES + 2 * WRITE_BUFFER);
/// What a build of the standard source reserves when it writes two windows
/// ahead, less its few kilobytes of vectors.
const WINDOWED_BUILD: u64 = FIXED_ALLOWANCE_BYTES + 2 * WRITE_BUFFER_FOOTPRINT * WRITE_WINDOW_BYTES;
/// A budget that holds one windowed build of the standard source and no
/// second one, nor a build with the whole write buffer.
const ONE_WINDOWED_BUILD: u64 = WINDOWED_BUILD + WINDOWED_BUILD / 4;

/// Source vectors by entity id.
type Expected = BTreeMap<u64, [f32; 3]>;

/// A distinct point of the unit cube for every entity id below 2^62.
fn vector_of(entity_id: u64) -> [f32; 3] {
    let mixed = (entity_id + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    [0, 21, 42].map(|shift| ((mixed >> shift) & 0x1F_FFFF) as f32 / 1_048_576.0 - 1.0)
}

/// A point so close to `entity_id`'s that each is the other's nearest
/// neighbor, so the graph links them.
fn beside(entity_id: u64) -> [f32; 3] {
    let [x, y, z] = vector_of(entity_id);
    [x + 1.0e-4, y, z]
}

/// A driver whose bulk builds share exactly `memory_budget_bytes`, or are off
/// at zero.
///
/// Its steps unlink without a bound in time: [`UNLINK_STEP_TIME`] is a bound
/// on the wall clock, which a slow or busy machine reaches after any number
/// of unlinks, and the contracts here assert what a step unlinked. So a
/// step of this driver unlinks by its counts alone, on every machine, and
/// the contract of the time bound sets one itself.
fn bulk_driver(threads: usize, memory_budget_bytes: u64, min_vectors: u64) -> VectorIndexDriver {
    bulk_driver_waking(
        threads,
        memory_budget_bytes,
        min_vectors,
        IndexWorkerWakeHandle::default(),
    )
}

/// [`bulk_driver`] with the handle its builds wake the worker through.
fn bulk_driver_waking(
    threads: usize,
    memory_budget_bytes: u64,
    min_vectors: u64,
    worker: IndexWorkerWakeHandle,
) -> VectorIndexDriver {
    let memory = NonZeroU64::new(memory_budget_bytes)
        .map_or(VectorBulkBuildMemory::Off, VectorBulkBuildMemory::Bytes);
    let mut driver = driver().with_bulk_builds(
        VectorBulkBuildTuning::try_new(threads, memory, min_vectors)
            .expect("bulk build tuning validates"),
        0,
        WRITE_BUFFER,
        worker,
    );
    driver.bulk.unlink.step_time = std::time::Duration::MAX;
    driver
}

/// The memory of builds on a host with `ceiling`, whose resident anonymous
/// memory the returned counter sets, with a write buffer of the default
/// size.
fn host_memory(
    policy: VectorBulkBuildMemory,
    configured_bytes: u64,
    ceiling: Option<u64>,
    worker: IndexWorkerWakeHandle,
) -> (Arc<BuildMemory>, Arc<AtomicU64>) {
    let resident = Arc::new(AtomicU64::new(0));
    let memory = Arc::new(BuildMemory {
        policy,
        configured_bytes,
        write_buffer_bytes: WRITE_BUFFER,
        host: HostMemory::Fixed {
            ceiling,
            resident: Arc::clone(&resident),
        },
        ledger: parking_lot::Mutex::new(Ledger::default()),
        parking: Arc::new(Parking::new(worker)),
    });
    (memory, resident)
}

/// What a build of a label of `members` members of the fixture's three
/// dimensions reserves when it paces its writes.
fn paced_build_bytes(members: u64) -> u64 {
    let parameters =
        BulkParameters::try_from_config(&VectorIndexConfig::new("paced", "embedding", 3))
            .expect("the default configuration links in bulk");
    build_bytes(
        parameters.bytes_per_vector::<Euclidean>(3) + GROWTH_SLACK_BYTES,
        members,
        2 * WRITE_BUFFER_FOOTPRINT * WRITE_WINDOW_BYTES,
    )
}

/// The fixture's index over the embeddings of another label.
fn labelled_definition(label: &str) -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(
            &VectorIndexDefinition::new_node(
                label,
                "embedding",
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("vector definition validates"),
        )
        .expect("V2 vector definition validates"),
    )
}

/// The properties of a node of `label` with `vector` as its embedding.
fn labelled(label: &str, vector: [f32; 3]) -> Vec<Property> {
    vec![
        Property::new("$label", PropertyValue::String(label.to_string())),
        Property::new("embedding", PropertyValue::F32Array(vector.to_vec())),
    ]
}

/// Default limits with `max_entities` source entities per step.
fn entity_limits(max_entities: usize) -> SearchIndexBatchLimits {
    let defaults = SearchIndexBackfillLimits::default().batch();
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(max_entities).expect("entity limit is positive"),
        defaults.max_input_bytes(),
        defaults.max_output_operations(),
        defaults.max_output_bytes(),
        defaults.max_single_vector_output_bytes(),
    )
    .expect("entity limits validate")
}

/// Default limits with `max_operations` output operations per step.
fn operation_limits(max_operations: u64) -> SearchIndexBatchLimits {
    let defaults = SearchIndexBackfillLimits::default().batch();
    SearchIndexBatchLimits::try_new(
        defaults.max_entities(),
        defaults.max_input_bytes(),
        NonZeroU64::new(max_operations).expect("operation limit is positive"),
        defaults.max_output_bytes(),
        defaults.max_single_vector_output_bytes(),
    )
    .expect("operation limits validate")
}

async fn seed(db: &Db, entities: impl IntoIterator<Item = (u64, [f32; 3])>) -> Expected {
    let expected = entities.into_iter().collect::<Expected>();
    put_sources(
        db,
        expected
            .iter()
            .map(|(entity_id, vector)| (*entity_id, properties(*vector, None))),
    )
    .await;
    expected
}

/// Writes the source rows and their label memberships in one transaction: a
/// durable write per row would wait for one WAL flush each.
async fn put_sources(db: &Db, rows: impl IntoIterator<Item = (u64, Vec<Property>)>) {
    let transaction = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("source write begins");
    for (entity_id, properties) in rows {
        crate::index_lifecycle::label_source::testing::stage_labels(
            &transaction,
            SCOPE,
            IndexElementKind::Node,
            entity_id,
            &[],
            &properties,
        )
        .await;
        transaction
            .put(source_key(SCOPE, entity_id), encode_properties(&properties))
            .expect("source row stages");
    }
    transaction
        .commit()
        .await
        .expect("vector sources are written");
}

async fn seed_standard(db: &Db) -> Expected {
    seed(
        db,
        (0..SOURCE).map(|entity_id| (entity_id, vector_of(entity_id))),
    )
    .await
}

/// Key and value bytes of every source row, which one read of the source
/// counts as its input.
async fn source_bytes(db: &Db) -> u64 {
    let mut rows = db
        .scan(source_key(SCOPE, 0)..=source_key(SCOPE, u64::MAX))
        .await
        .expect("the source is readable");
    let mut bytes = 0;
    while let Some(row) = rows.next().await.expect("a source row is readable") {
        bytes += (row.key.len() + row.value.len()) as u64;
    }
    bytes
}

/// The scan stage `operation_id` stores, or `None` once it left both.
async fn stored_scan(db: &Db, operation_id: IndexOperationId) -> Option<StoredScan> {
    let operation = read_operation(db, SCOPE, operation_id).await;
    let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(stage)) =
        operation.progress()
    else {
        return None;
    };
    match stage {
        VectorBuildStage::Scan(progress) => Some(StoredScan::Serial(progress.clone())),
        VectorBuildStage::BulkBuild(progress) => Some(StoredScan::Bulk(progress.clone())),
        VectorBuildStage::AdoptLegacy(_)
        | VectorBuildStage::ValidateAdoptedDirectory(_)
        | VectorBuildStage::CatchUp(_)
        | VectorBuildStage::ValidateDescriptor(_)
        | VectorBuildStage::Activate(_) => None,
    }
}

/// The stored scan progress under either scan stage, or `None` once the
/// operation left both.
async fn scan_progress(db: &Db, operation_id: IndexOperationId) -> Option<SourceScanProgress> {
    stored_scan(db, operation_id)
        .await
        .map(|stored| stored.progress().clone())
}

/// Bulk attempts the stored bulk stage counts, or `None` under any other
/// stage.
async fn attempts(db: &Db, operation_id: IndexOperationId) -> Option<u8> {
    stored_scan(db, operation_id)
        .await
        .and_then(|stored| stored.attempts())
        .map(BulkBuildAttempts::get)
}

/// The bulk stage of attempt `attempts` with nothing read: what the step
/// that counts the attempt stores.
async fn counted(db: &Db, operation_id: IndexOperationId, attempts: u8) -> BulkBuildProgress {
    BulkBuildProgress {
        scan: scan_from_start(&scan_progress(db, operation_id).await.expect("scan stage")),
        attempts: BulkBuildAttempts::new(attempts).expect("attempts are counted from one"),
    }
}

/// Stores `stage` as the progress of `operation_id`, as a process that died
/// right after committing it would leave the record.
async fn store_stage(db: &Db, operation_id: IndexOperationId, stage: VectorBuildStage) {
    let stored = read_operation(db, SCOPE, operation_id).await;
    let forged = IndexOperationRecord::try_new(
        stored.operation_id(),
        stored.index_id(),
        stored.identity().clone(),
        stored.generation(),
        stored.index_record_revision(),
        stored.operation_revision(),
        stored.kind(),
        stored.family(),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(stage)),
        stored.attempt(),
        stored.execution_state().clone(),
    )
    .expect("the forged record validates");
    db.put(
        crate::index_lifecycle::outbox::scoped_operation_key(SCOPE, operation_id),
        crate::encoding::v2::values::encode_operation_record(&forged),
    )
    .await
    .expect("the forged record is stored");
}

/// Steps a new attempt takes before its first write step: the one that
/// stores its count. The next step reads, links and writes.
const COUNT: usize = 1;

/// Drives `driver` through the step that counts a new attempt of its own,
/// deleting first whatever an earlier attempt left, and returns the stage
/// that step stored. The attempt holds its turn and its memory and has read
/// nothing.
async fn start_attempt(
    db: &Db,
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
) -> BulkBuildProgress {
    for _ in 0..1_000 {
        drive_steps(
            db,
            driver,
            operation_id,
            claim_sequence,
            entity_limits(16),
            1,
        )
        .await;
        let slot = Arc::clone(&driver.bulk.builds.lock()[&operation_id]);
        let starting = matches!(&*slot.lock().await, BuildState::Starting(_));
        drop(slot);
        if starting {
            let Some(StoredScan::Bulk(attempt)) = stored_scan(db, operation_id).await else {
                panic!("the step stored the attempt's count");
            };
            return attempt;
        }
    }
    panic!("no attempt started within the bounded test steps")
}

/// Starts an attempt of `driver` and puts `prepared` in place of its passes
/// 1 and 2, which wake the operation when they end as the real ones do.
/// Returns the flag the build raises when it drops them.
async fn prepare_with(
    db: &Db,
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    prepared: tokio::task::JoinHandle<Result<Prepared>>,
) -> Arc<AtomicBool> {
    let attempt = start_attempt(db, driver, operation_id, claim_sequence).await;
    let cancelled = Arc::new(AtomicBool::new(false));
    let (sender, done) = tokio::sync::oneshot::channel();
    let wake = WakeOnDrop {
        parking: Arc::clone(&driver.bulk.parking),
        operation_id,
    };
    let task = tokio::spawn(async move {
        let _wake = wake;
        let sender = sender;
        let _ = sender.send(prepared.await.expect("the stand-in passes join"));
    });
    // Replacing the started attempt frees its turn and its memory.
    driver.bulk.builds.lock().insert(
        operation_id,
        Arc::new(tokio::sync::Mutex::new(BuildState::Preparing(
            Preparation {
                task,
                done,
                cancelled: Arc::clone(&cancelled),
                attempt,
                progress: Arc::default(),
            },
        ))),
    );
    cancelled
}

/// Where a process dies in a bulk attempt it started.
#[derive(Debug, Clone, Copy)]
enum Death {
    /// After it stored the attempt's count, before it read a row.
    Counted,
    /// While its read pass runs.
    Reading,
    /// After this many steps of its write pass.
    Writing(usize),
}

/// A new process that starts a bulk attempt of `operation_id` and dies in
/// it. Returns the count the attempt was stored with.
async fn die(
    db: &Db,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    death: Death,
) -> u8 {
    let dying = bulk_driver(1, BUDGET, 16);
    let limits = entity_limits(16);
    match death {
        Death::Counted => {
            start_attempt(db, &dying, operation_id, claim_sequence).await;
        }
        Death::Reading => {
            prepare_with(
                db,
                &dying,
                operation_id,
                claim_sequence,
                tokio::spawn(std::future::pending::<Result<Prepared>>()),
            )
            .await;
            // The step finds the passes running and parks.
            assert_eq!(
                drive_one(db, &dying, operation_id, claim_sequence, limits).await,
                CommittedOperationStep::Progressed
            );
        }
        Death::Writing(steps) => {
            start_attempt(db, &dying, operation_id, claim_sequence).await;
            drive_steps(db, &dying, operation_id, claim_sequence, limits, steps).await;
            assert_eq!(
                dying.build_reports().path(operation_id),
                Some(VectorBuildPath::Bulk)
            );
            assert!(scan_progress(db, operation_id)
                .await
                .unwrap()
                .cursor
                .is_some());
        }
    }
    attempts(db, operation_id)
        .await
        .expect("the bulk stage is stored")
}

/// Entities the finished or running operation reports.
async fn reported_entities(db: &Db, operation_id: IndexOperationId) -> u64 {
    crate::index_lifecycle::IndexOperationStatus::from_record(
        &read_operation(db, SCOPE, operation_id).await,
    )
    .common()
    .progress
    .entities
}

/// One step of `operation_id` that its build did not wait in, as the worker
/// gives it: a step that parks the operation commits nothing new, and the
/// worker offers the next one once the driver wakes it. Those steps and the
/// waits between them are not counted. Whether a step parked is the
/// driver's own count, because what the build waited for may have woken it
/// before this looks.
async fn step(
    db: &Db,
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> CommittedOperationStep {
    for _ in 0..1_000 {
        let parked = driver.bulk.parked_steps.load(Ordering::Relaxed);
        let outcome = drive_one(db, driver, operation_id, claim_sequence, limits).await;
        if driver.bulk.parked_steps.load(Ordering::Relaxed) == parked {
            return outcome;
        }
        assert_eq!(outcome, CommittedOperationStep::Progressed);
        unparked(db, driver, operation_id).await;
    }
    panic!("the build parked without end")
}

/// How long a contract waits for a background task to end, and [`unparked`]
/// for its wake, before it gives the build up for never woken. A bound on
/// liveness and no more: the wait ends when the background task does, which
/// no contract here times, so a slow or busy machine only waits longer. A
/// source of a few thousand rows has taken more than the park limit of
/// thirty seconds to read and link on two cores shared by sixty-four test
/// processes.
const WOKEN_WITHIN: std::time::Duration = std::time::Duration::from_secs(600);

/// Waits until `driver` no longer parks `operation_id`: what its build
/// waited for in the background, its read and link passes or a flush, has
/// ended and woken it. Its next step then finds that done, however slowly
/// the background task ran on this machine.
async fn unparked(db: &Db, driver: &dyn IndexOperationDriver, operation_id: IndexOperationId) {
    let record = read_operation(db, SCOPE, operation_id).await;
    let waited = std::time::Instant::now();
    while driver.parked_until(&record).is_some() {
        assert!(
            waited.elapsed() < WOKEN_WITHIN,
            "nothing woke the parked build"
        );
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

/// Drives `operation_id` until a step does not progress, and returns that
/// step with the count of progressed steps before it.
async fn drive(
    db: &Db,
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> (CommittedOperationStep, usize) {
    for progressed in 0..4_000 {
        let step = step(db, driver, operation_id, claim_sequence, limits).await;
        if step != CommittedOperationStep::Progressed {
            return (step, progressed);
        }
    }
    panic!("vector operation exceeded bounded test checkpoints")
}

/// `count` steps of a build that waits: each commits the stage as it is
/// stored and leaves the operation parked.
async fn parked_steps(
    db: &Db,
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
    count: usize,
) {
    for _ in 0..count {
        let stored = stored_scan(db, operation_id).await;
        assert_eq!(
            drive_one(db, driver, operation_id, claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        assert!(driver
            .parked_until(&read_operation(db, SCOPE, operation_id).await)
            .is_some());
        assert_eq!(stored_scan(db, operation_id).await, stored);
    }
}

/// Deletes source rows as a graph delete does: with their memberships of
/// the fixture's label.
async fn delete_sources(db: &Db, entity_ids: impl IntoIterator<Item = u64>) {
    let transaction = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("source delete begins");
    for entity_id in entity_ids {
        crate::index_lifecycle::label_source::testing::stage_departure(
            &transaction,
            SCOPE,
            IndexElementKind::Node,
            "Document",
            entity_id,
        )
        .await;
        transaction
            .delete(source_key(SCOPE, entity_id))
            .expect("source delete stages");
    }
    transaction
        .commit()
        .await
        .expect("vector sources are deleted");
}

/// Drives `count` steps that must all progress.
async fn drive_steps(
    db: &Db,
    driver: &VectorIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
    count: usize,
) {
    for _ in 0..count {
        assert_eq!(
            step(db, driver, operation_id, claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
    }
}

/// Runs the step the outbox requeued after a commit conflict, once its delay
/// has passed and its build is no longer parked.
///
/// The step whose commit was lost may have started the build's passes and
/// parked the operation for them. The worker offers the retry only once they
/// have woken it, and so does this: offered while they still run, the step
/// would park again and commit nothing of what the caller expects of it.
async fn retry_conflicted_step(
    db: &Db,
    driver: &dyn IndexOperationDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> CommittedOperationStep {
    unparked(db, driver, operation_id).await;
    let writer_epoch = WriterEpoch::from_bytes([0x6B; 16]).expect("writer epoch is non-nil");
    let OperationPointerObservation::Delayed { delay_millis } =
        observe_operation_pointer(db, operation_id, writer_epoch, NOW_MILLIS)
            .await
            .expect("vector operation pointer is readable")
    else {
        panic!("the conflicting step is requeued after a delay");
    };
    let retry_at = NOW_MILLIS + delay_millis;
    let OperationPointerObservation::Eligible(eligible) =
        observe_operation_pointer(db, operation_id, writer_epoch, retry_at)
            .await
            .expect("vector operation pointer is readable")
    else {
        panic!("the requeued step is eligible after its delay");
    };
    let claimed = claim_operation(
        db,
        &eligible,
        writer_epoch,
        ClaimSequence::new(*claim_sequence).expect("claim sequence is non-zero"),
        retry_at,
        ClaimPermission::Normal,
    )
    .await
    .expect("vector retry claim succeeds")
    .expect("vector revision is claimable");
    *claim_sequence += 1;
    execute_claimed_step(db, &claimed, driver, limits, retry_at)
        .await
        .expect("the repeated step commits")
}

/// A step of `driver` whose commit a concurrent write to the index record
/// fails.
async fn lose_commit(
    db: &Db,
    driver: &VectorIndexDriver,
    definition: &ValidatedDynamicIndexDefinition,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) {
    let index_key = scoped_index_key(SCOPE, ScopedKey::index_record(definition.identity()));
    let index_value = db.get(&index_key).await.unwrap().unwrap();
    let racing = WriteDuringStepDriver {
        inner: driver,
        writes: vec![(index_key, index_value)],
    };
    assert_eq!(
        drive_one(db, &racing, operation_id, claim_sequence, limits).await,
        CommittedOperationStep::TransientFailure
    );
}

/// The rows an uninterrupted bulk build of the standard source without the
/// `absent` entities stores. One construction thread links the same graph
/// every time.
async fn uninterrupted_rows(name: &str, absent: &[u64]) -> BTreeMap<Bytes, Bytes> {
    let db = test_db(name).await;
    let definition = definition(None);
    seed(
        &db,
        (0..SOURCE)
            .filter(|entity_id| !absent.contains(entity_id))
            .map(|entity_id| (entity_id, vector_of(entity_id))),
    )
    .await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    assert_eq!(
        drive(
            &db,
            &bulk_driver(1, BUDGET, 16),
            build_id,
            &mut 1,
            entity_limits(16)
        )
        .await
        .0,
        CommittedOperationStep::Completed
    );
    let rows = active_index(&db, &definition)
        .await
        .stored_rows(&db)
        .await
        .unwrap();
    db.close().await.expect("vector test database closes");
    rows
}

/// The hidden generation `operation_id` builds.
async fn building_index(
    db: &Db,
    definition: &ValidatedDynamicIndexDefinition,
    operation_id: IndexOperationId,
) -> VectorIndex<Euclidean> {
    let record = read_index(db, SCOPE, definition).await;
    let IndexStateVectorPhysical { layout } =
        IndexStateVectorPhysical::from_record(&record).expect("building record is vector");
    let VectorPhysicalLayout::Unpartitioned { physical_index_id } = layout else {
        panic!("fixture build is unpartitioned");
    };
    let handle = ValidatedVectorBuildGenerationHandle::try_from_building::<Euclidean>(
        SCOPE,
        &record,
        operation_id,
        physical_index_id,
    )
    .expect("building generation validates");
    VectorIndex::from_generation(handle.generation())
}

async fn active_index(
    db: &Db,
    definition: &ValidatedDynamicIndexDefinition,
) -> VectorIndex<Euclidean> {
    let active = read_index(db, SCOPE, definition).await;
    let IndexStateV2::Active {
        physical:
            PhysicalGeneration::Vector {
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                ..
            },
        ..
    } = active.state()
    else {
        panic!("completed vector build is active and unpartitioned");
    };
    let handle = ActiveIndexHandle::try_from_record(SCOPE, &active)
        .expect("active vector record projects a handle");
    VectorIndex::from_generation(
        &ValidatedVectorGenerationHandle::try_from_active::<Euclidean>(&handle, *physical_index_id)
            .expect("active physical generation validates"),
    )
}

async fn applied_rows(db: &Db, index_id: IndexId, generation: IndexGenerationId) -> usize {
    let prefix = generation_prefix(SCOPE, RecordKind::AppliedState, index_id, generation);
    let mut rows = db
        .scan_prefix(prefix, ..)
        .await
        .expect("applied-state prefix is readable");
    let mut count = 0;
    while rows
        .next()
        .await
        .expect("applied-state row is readable")
        .is_some()
    {
        count += 1;
    }
    count
}

/// Asserts `index` is a complete graph of exactly `expected`: every entity
/// holds its vector and is found by an exhaustive search, every stored link
/// is symmetric with a reverse locator and ends at an indexed entity, and no
/// row of an `absent` entity remains.
async fn assert_index_holds(
    db: &Db,
    index: &VectorIndex<Euclidean>,
    expected: &Expected,
    absent: &[u64],
) {
    let metadata = index
        .get_metadata(db)
        .await
        .expect("metadata is readable")
        .expect("a stored graph has metadata");
    assert_eq!(metadata.count, expected.len() as u64);
    let params = SearchParams::new(1)
        .unwrap()
        .with_ef(expected.len())
        .unwrap()
        .with_simhash_mode(SimHashMode::Off)
        .with_pre_simhash_sampling_ratio(1.0)
        .unwrap();
    for (entity_id, vector) in expected {
        let item = index
            .get_item(db, *entity_id)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("entity {entity_id} is indexed"));
        assert_eq!(item.vector.to_vec(), vector, "entity {entity_id}");
        let (links, sources) = index.stored_layer0_links(db, *entity_id).await.unwrap();
        assert_eq!(links, sources, "entity {entity_id} links are symmetric");
        assert!(
            !links.is_empty() || expected.len() == 1,
            "entity {entity_id} is linked"
        );
        assert!(
            links.iter().all(|link| expected.contains_key(link)),
            "entity {entity_id} links only indexed entities: {links:?}"
        );
        let found = index.search(db, vector, &params).await.unwrap();
        assert_eq!(
            found.first().map(|result| result.entity_id()),
            Some(*entity_id),
            "entity {entity_id} is reachable"
        );
    }
    for entity_id in absent {
        assert!(
            index.get_item(db, *entity_id).await.unwrap().is_none(),
            "entity {entity_id} is not indexed"
        );
        assert_eq!(
            index.stored_layer0_links(db, *entity_id).await.unwrap(),
            (Vec::new(), Vec::new()),
            "entity {entity_id} has no link rows"
        );
    }
}

/// Asserts `operation_id` is blocked under the serial stage with nothing
/// read, the record a build that left the bulk stage blocks with: one a
/// release from before bulk builds decodes, and that no queue entry names.
async fn assert_blocked_at_the_serial_stage(db: &Db, operation_id: IndexOperationId) {
    let operation = read_operation(db, SCOPE, operation_id).await;
    assert!(matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Blocked(_)
    ));
    let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
        VectorBuildStage::Scan(progress),
    )) = operation.progress()
    else {
        panic!("a blocked build is at the serial stage: {operation:?}");
    };
    assert_eq!(
        (&progress.cursor, progress.counters),
        (&None, OperationCounters::default())
    );
    assert!(db
        .get(crate::index_lifecycle::outbox::global_operation_key(
            operation_id
        ))
        .await
        .unwrap()
        .is_none());
}

fn nothing_held(driver: &VectorIndexDriver) -> bool {
    driver.bulk.memory.reserved() == 0 && driver.bulk.builds.lock().is_empty()
}

/// The bulk path stores its stage and its first attempt before it reads a
/// row, stores the whole source through bounded steps that keep the operation
/// at that stage, writes the metadata row last, and leaves an index any
/// process can validate and activate. The scan counters are the write
/// pass's: every source entity once.
#[tokio::test(flavor = "multi_thread")]
async fn a_bulk_build_stores_its_source_in_bounded_steps_and_activates() {
    let db = test_db("vector-bulk-build").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(4, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    assert_eq!(driver.build_reports().path(build_id), None);
    let created = stored_scan(&db, build_id).await.expect("scan stage");
    assert!(matches!(created, StoredScan::Serial(_)));
    let building = building_index(&db, &definition, build_id).await;

    // The first step stores the bulk stage and the attempt's count, with
    // the attempt's memory and turn held and nothing read.
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, COUNT).await;
    assert_eq!(
        stored_scan(&db, build_id).await,
        Some(StoredScan::Bulk(BulkBuildProgress {
            scan: created.progress().clone(),
            attempts: BulkBuildAttempts::FIRST,
        }))
    );
    assert!(driver.bulk.memory.reserved() > 0);
    assert_eq!(driver.bulk.preparing.available_permits(), 0);
    let slot = Arc::clone(&driver.bulk.builds.lock()[&build_id]);
    assert!(matches!(&*slot.lock().await, BuildState::Starting(_)));
    // The slot would keep the finished build's reservation alive.
    drop(slot);
    assert!(building.stored_rows(&db).await.unwrap().is_empty());
    assert_eq!(driver.build_reports().path(build_id), None);

    // Passes 1 and 2 finish within the second step's wait, so it also
    // writes.
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert_eq!(attempts(&db, build_id).await, Some(1));
    let progress = scan_progress(&db, build_id).await.expect("scan stage");
    assert_eq!(progress.cursor, Some(source_cursor(SCOPE, 15)));
    // The counters are the write pass's: the read pass stored nothing.
    assert_eq!(progress.counters.entities, 16);
    let source_bytes = source_bytes(&db).await;
    assert_eq!(progress.counters.input_bytes, source_bytes * 16 / SOURCE);
    assert!(building.get_metadata(&db).await.unwrap().is_none());
    assert!(!building.stored_rows(&db).await.unwrap().is_empty());
    assert_eq!(applied_rows(&db, index_id, generation).await, 16);
    // The vectors, the fixed allowance, and the whole write buffer's.
    let reserved = driver.bulk.memory.reserved() - (FIXED_ALLOWANCE_BYTES + 2 * WRITE_BUFFER);
    assert!(
        (SOURCE * 100..SOURCE * 1_000).contains(&reserved),
        "{reserved}"
    );

    let mut scan_steps = 1;
    while let Some(stored) = stored_scan(&db, build_id).await {
        // The bulk stage is stored until the graph is.
        assert_eq!(stored.attempts().map(BulkBuildAttempts::get), Some(1));
        assert_eq!(
            stored.progress().counters.entities,
            16 * scan_steps,
            "every step counts the rows it stored"
        );
        assert!(building.get_metadata(&db).await.unwrap().is_none());
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
        scan_steps += 1;
    }
    assert!(scan_steps >= SOURCE / 16, "{scan_steps} scan steps");
    assert_eq!(
        building.get_metadata(&db).await.unwrap().unwrap().count,
        SOURCE
    );
    assert_eq!(applied_rows(&db, index_id, generation).await, 240);
    assert!(nothing_held(&driver));
    // The scan ends at what a serial scan of the source counts.
    let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
        VectorBuildStage::ValidateDescriptor(stored),
    )) = read_operation(&db, SCOPE, build_id)
        .await
        .progress()
        .clone()
    else {
        panic!("the stored graph is validated next");
    };
    assert_eq!(stored.counters.entities, SOURCE);
    assert_eq!(stored.counters.input_bytes, source_bytes);

    // Validation and activation need no graph.
    let restarted = super::super::driver_contracts::driver();
    assert_eq!(
        drive(&db, &restarted, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// Validation deletes the applied state [`VALIDATION_STEP_ROWS`] rows a step,
/// whatever the entity ceiling: a step fences writers, so it is bounded, but
/// not to the few hundred rows a scan step may decode.
#[tokio::test(flavor = "multi_thread")]
async fn validation_deletes_applied_state_in_bounded_steps() {
    const REST: u64 = 104;
    const ROWS: u64 = VALIDATION_STEP_ROWS as u64 + REST;
    let db = test_db("vector-bulk-validation-steps").await;
    let definition = definition(None);
    seed(
        &db,
        (0..ROWS).map(|entity_id| (entity_id, vector_of(entity_id))),
    )
    .await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, ROWS - 1).await;
    let driver = bulk_driver(2, BUDGET, 16);
    let mut claim_sequence = 1;
    while scan_progress(&db, build_id).await.is_some() {
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            SearchIndexBackfillLimits::default().batch(),
            1,
        )
        .await;
    }
    assert_eq!(applied_rows(&db, index_id, generation).await as u64, ROWS);

    let limits = entity_limits(16);
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(applied_rows(&db, index_id, generation).await as u64, REST);
    assert!(matches!(
        read_operation(&db, SCOPE, build_id).await.progress(),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
                cursor: Some(_),
                ..
            })
        ))
    ));
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(applied_rows(&db, index_id, generation).await, 0);
    assert_eq!(
        active_index(&db, &definition)
            .await
            .get_metadata(&db)
            .await
            .unwrap()
            .unwrap()
            .count,
        ROWS
    );
    db.close().await.expect("vector test database closes");
}

/// Every build the bulk path cannot take is built by the serial scan, which
/// the recorded path names with its reason, and holds no budget afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn builds_the_bulk_path_cannot_take_scan_serially() {
    const COUNT: u64 = 60;
    for (name, driver, tenant, path) in [
        (
            "disabled",
            driver(),
            None,
            VectorBuildPath::Serial(VectorSerialBuildReason::BulkBuildsDisabled),
        ),
        (
            "partitioned",
            bulk_driver(2, BUDGET, 16),
            Some(7),
            VectorBuildPath::Serial(VectorSerialBuildReason::TenantPartitioned),
        ),
        (
            "below-fixed-allowance",
            bulk_driver(2, FIXED_ALLOWANCE_BYTES - 1, 16),
            None,
            VectorBuildPath::Serial(VectorSerialBuildReason::OverMemoryBudget),
        ),
        (
            "below-source",
            bulk_driver(2, FIXED_ALLOWANCE_BYTES + 1_024, 16),
            None,
            VectorBuildPath::Serial(VectorSerialBuildReason::OverMemoryBudget),
        ),
        (
            "too-few",
            bulk_driver(2, BUDGET, COUNT + 1),
            None,
            VectorBuildPath::Serial(VectorSerialBuildReason::TooFewVectors),
        ),
        (
            "just-enough",
            bulk_driver(2, BUDGET, COUNT),
            None,
            VectorBuildPath::Bulk,
        ),
    ] {
        let db = test_db(&format!("vector-bulk-fallback-{name}")).await;
        let definition = definition(tenant.map(|_| "account_id"));
        let expected = (0..COUNT)
            .map(|entity_id| (entity_id, vector_of(entity_id)))
            .collect::<Expected>();
        put_sources(
            &db,
            expected
                .iter()
                .map(|(entity_id, vector)| (*entity_id, properties(*vector, tenant))),
        )
        .await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, COUNT - 1).await;
        let mut claim_sequence = 1;
        assert_eq!(
            drive(
                &db,
                &driver,
                build_id,
                &mut claim_sequence,
                entity_limits(16)
            )
            .await
            .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(driver.build_reports().path(build_id), Some(path), "{name}");
        assert!(nothing_held(&driver), "{name}");
        if tenant.is_none() {
            assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        }
        db.close().await.expect("vector test database closes");
    }
}

/// A source without a vector has no graph to link. An empty source is not
/// read for a bulk build. One whose entities hold no vector is read, and the
/// step that collects the read pass finishes the scan without a serial scan
/// of the same rows. Both activate an index that holds nothing, and the
/// second counts its entities once.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_without_vectors_activates_an_empty_index() {
    let no_vector = [Property::new(
        "$label",
        PropertyValue::String("Document".to_string()),
    )];
    for (name, rows, path) in [
        (
            "empty",
            0_u64,
            VectorBuildPath::Serial(VectorSerialBuildReason::TooFewVectors),
        ),
        ("vectorless", 40, VectorBuildPath::Bulk),
    ] {
        let db = test_db(&format!("vector-bulk-no-vectors-{name}")).await;
        let definition = definition(None);
        if rows > 0 {
            put_sources(
                &db,
                (0..rows).map(|entity_id| (entity_id, no_vector.to_vec())),
            )
            .await;
        }
        let source_bytes = source_bytes(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, rows.saturating_sub(1)).await;
        let driver = bulk_driver(2, BUDGET, 0);
        let mut claim_sequence = 1;
        // The empty source is scanned serially in one step; the other is
        // counted as an attempt and then read.
        let steps = match path {
            VectorBuildPath::Serial(_) => 1,
            VectorBuildPath::Bulk => COUNT + 1,
        };
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            entity_limits(16),
            steps,
        )
        .await;
        assert_eq!(driver.build_reports().path(build_id), Some(path), "{name}");
        let IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::ValidateDescriptor(scanned),
        )) = read_operation(&db, SCOPE, build_id)
            .await
            .progress()
            .clone()
        else {
            panic!("{name}: the step that collects the read finishes the scan");
        };
        assert_eq!(
            (scanned.counters.entities, scanned.counters.input_bytes),
            (rows, source_bytes),
            "{name}"
        );
        assert_eq!(
            drive(
                &db,
                &driver,
                build_id,
                &mut claim_sequence,
                entity_limits(16)
            )
            .await
            .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert!(nothing_held(&driver), "{name}");
        let metadata = active_index(&db, &definition)
            .await
            .get_metadata(&db)
            .await
            .unwrap()
            .expect("validation creates the empty namespace");
        assert_eq!((metadata.count, metadata.entry_point), (0, None), "{name}");
        assert_eq!(applied_rows(&db, index_id, generation).await, 0, "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// A source with fewer vectors than a bulk build is started for is built
/// serially. A small one is found out from its first rows, before the build
/// takes a turn, reserves memory or reads the source for a graph; one too
/// large for that is read by the read pass, which counts its vectors.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_with_too_few_vectors_is_built_serially() {
    const MIN_VECTORS: u64 = 16;
    let no_vector = [Property::new(
        "$label",
        PropertyValue::String("Document".to_string()),
    )];
    for (name, rows, probed) in [
        // The probe reads eight rows a vector and meets the source's end.
        ("probed", MIN_VECTORS * PROBE_ROWS_PER_VECTOR - 1, true),
        ("read", MIN_VECTORS * PROBE_ROWS_PER_VECTOR + 72, false),
    ] {
        let db = test_db(&format!("vector-bulk-few-vectors-{name}")).await;
        let definition = definition(None);
        put_sources(
            &db,
            (0..rows).map(|entity_id| (entity_id, no_vector.to_vec())),
        )
        .await;
        // Fifteen vectors, the last on the last row.
        let expected = seed(
            &db,
            (0..MIN_VECTORS - 1).map(|vector| {
                let entity_id = rows - 1 - vector * 2;
                (entity_id, vector_of(entity_id))
            }),
        )
        .await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, rows - 1).await;
        let driver = bulk_driver(2, BUDGET, MIN_VECTORS);
        // Another build holds the turn: only a source that needs a read
        // pass waits for it.
        let other_build = Arc::clone(&driver.bulk.preparing)
            .try_acquire_owned()
            .expect("no build is preparing");
        let mut claim_sequence = 1;
        let limits = entity_limits(16);
        assert_eq!(
            drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        assert_eq!(
            driver
                .parked_until(&read_operation(&db, SCOPE, build_id).await)
                .is_some(),
            !probed,
            "{name}: only a source that needs a read pass waits"
        );
        let path = driver.build_reports().path(build_id);
        let progress = scan_progress(&db, build_id).await.unwrap();
        if probed {
            assert_eq!(
                path,
                Some(VectorBuildPath::Serial(
                    VectorSerialBuildReason::TooFewVectors
                )),
                "{name}"
            );
            assert_eq!(progress.cursor, Some(source_cursor(SCOPE, 15)), "{name}");
            assert_eq!(progress.counters.entities, 16, "{name}");
        } else {
            assert_eq!(path, None, "{name}: the build waits for its turn");
            assert_eq!(progress.cursor, None, "{name}");
        }
        drop(other_build);
        if !probed {
            // The attempt is counted, its read pass finds too few vectors,
            // and the step that collects it scans serially: the serial
            // scan's first commit takes the build off the bulk stage.
            drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, COUNT).await;
            assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
            drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
            let Some(StoredScan::Serial(serial)) = stored_scan(&db, build_id).await else {
                panic!("{name}: the serial stage is stored");
            };
            assert_eq!(serial.cursor, Some(source_cursor(SCOPE, 15)), "{name}");
            assert_eq!(serial.counters.entities, 16, "{name}");
        }
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            driver.build_reports().path(build_id),
            Some(VectorBuildPath::Serial(
                VectorSerialBuildReason::TooFewVectors
            )),
            "{name}"
        );
        assert!(nothing_held(&driver), "{name}");
        assert_eq!(driver.bulk.preparing.available_permits(), 1, "{name}");
        let absent = (0..rows)
            .filter(|entity_id| !expected.contains_key(entity_id))
            .collect::<Vec<u64>>();
        assert_index_holds(
            &db,
            &active_index(&db, &definition).await,
            &expected,
            &absent,
        )
        .await;
        db.close().await.expect("vector test database closes");
    }
}

/// A source row the serial scan would block on blocks the bulk build at the
/// same entity, from the read pass, without a serial scan of the rows before
/// it. The blocked build holds nothing and stays at its bulk stage, and once
/// the row is repaired its retry builds in bulk, as a second attempt.
#[tokio::test(flavor = "multi_thread")]
async fn a_row_the_read_pass_cannot_take_blocks_the_build_at_its_entity() {
    let defaults = SearchIndexBackfillLimits::default().batch();
    let small_input = SearchIndexBatchLimits::try_new(
        defaults.max_entities(),
        NonZeroU64::new(512).expect("input limit is positive"),
        defaults.max_output_operations(),
        defaults.max_output_bytes(),
        defaults.max_single_vector_output_bytes(),
    )
    .expect("small input limits validate");
    let mut oversized = properties(vector_of(7), None);
    oversized.push(Property::new(
        "padding",
        PropertyValue::String("x".repeat(2_048)),
    ));
    for (name, entity_id, value, limits) in [
        ("invalid", 5, Bytes::from_static(b"malformed"), defaults),
        ("oversized", 7, encode_properties(&oversized), small_input),
    ] {
        let db = test_db(&format!("vector-bulk-unreadable-{name}")).await;
        let definition = definition(None);
        let expected = seed(
            &db,
            (0..60).map(|entity_id| (entity_id, vector_of(entity_id))),
        )
        .await;
        db.put(source_key(SCOPE, entity_id), value)
            .await
            .expect("unreadable source is written");
        let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, 59).await;
        let driver = bulk_driver(2, BUDGET, 16);
        let mut claim_sequence = 1;
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Blocked,
            "{name}"
        );
        assert_eq!(driver.build_reports().path(build_id), None, "{name}");
        let operation = read_operation(&db, SCOPE, build_id).await;
        let IndexOperationExecutionState::Blocked(blocker) = operation.execution_state() else {
            panic!("{name}: the build is blocked");
        };
        let (IndexOperationBlocker::InvalidSourceData {
            entity_id: blocked, ..
        }
        | IndexOperationBlocker::OversizedEntity {
            entity_id: blocked, ..
        }) = blocker
        else {
            panic!("{name}: unexpected blocker {blocker:?}");
        };
        assert_eq!(*blocked, IndexEntityId::new(entity_id), "{name}");
        assert_eq!(
            matches!(blocker, IndexOperationBlocker::OversizedEntity { .. }),
            name == "oversized"
        );
        // No entity before the row was inserted serially, and the build
        // left the bulk stage before it blocked.
        assert_eq!(applied_rows(&db, index_id, generation).await, 0, "{name}");
        assert_blocked_at_the_serial_stage(&db, build_id).await;
        assert!(nothing_held(&driver), "{name}");

        put_source(
            &db,
            SCOPE,
            entity_id,
            &properties(vector_of(entity_id), None),
        )
        .await;
        retry_operation(&db, SCOPE, build_id)
            .await
            .expect("the blocked build is retried");
        // The retry starts from the serial stage, with a first attempt.
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, COUNT).await;
        assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            driver.build_reports().path(build_id),
            Some(VectorBuildPath::Bulk),
            "{name}"
        );
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// The step that collects a read pass stopped at a row reads the row through
/// its own transaction: it blocks on a row that is still unreadable, and
/// reads the source again when the row was repaired or deleted meanwhile.
#[tokio::test(flavor = "multi_thread")]
async fn a_row_repaired_before_its_blocker_is_stored_does_not_block() {
    let limits = entity_limits(16);
    for (name, row, blocked) in [
        ("malformed", Some(Bytes::from_static(b"malformed")), true),
        (
            "repaired",
            Some(encode_properties(&properties(vector_of(5), None))),
            false,
        ),
        ("deleted", None, false),
    ] {
        let db = test_db(&format!("vector-bulk-blocker-reread-{name}")).await;
        let definition = definition(None);
        let mut expected = seed(
            &db,
            (0..60).map(|entity_id| (entity_id, vector_of(entity_id))),
        )
        .await;
        match row {
            Some(value) => {
                db.put(source_key(SCOPE, 5), value).await.unwrap();
            }
            None => {
                db.delete(source_key(SCOPE, 5)).await.unwrap();
                expected.remove(&5);
            }
        }
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, 59).await;
        let driver = bulk_driver(2, BUDGET, 16);
        let mut claim_sequence = 1;
        // A read pass that found the row unreadable in its snapshot.
        prepare_with(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            tokio::spawn(async {
                Ok(Prepared::Blocked {
                    key: source_key(SCOPE, 5),
                    entity_id: IndexEntityId::new(5),
                })
            }),
        )
        .await;
        let (step, _) = drive(&db, &driver, build_id, &mut claim_sequence, limits).await;
        if blocked {
            assert_eq!(step, CommittedOperationStep::Blocked, "{name}");
            assert!(matches!(
                read_operation(&db, SCOPE, build_id).await.execution_state(),
                IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
                    entity_id,
                    ..
                }) if *entity_id == IndexEntityId::new(5)
            ));
        } else {
            assert_eq!(step, CommittedOperationStep::Completed, "{name}");
            assert_eq!(
                driver.build_reports().path(build_id),
                Some(VectorBuildPath::Bulk),
                "{name}"
            );
            assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        }
        assert!(nothing_held(&driver), "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// Entities updated, deleted, stripped of their vector or added after pass 1
/// read the source are never stored, and stored neighbors that linked to
/// them are restaged without the link. An entity changed behind the scan
/// keeps the vector the build stored; a rewrite of the same bytes changes
/// nothing.
#[tokio::test(flavor = "multi_thread")]
async fn source_changes_after_the_read_pass_stay_out_of_the_graph() {
    let db = test_db("vector-bulk-source-changes").await;
    let definition = definition(None);
    // 40, 41, 42 and 200 are the nearest neighbors of 0, 1, 2 and 3, which
    // the first step stores. 50 is absent.
    let partners = [(0, 40), (1, 41), (2, 42), (3, 200)];
    let mut expected = seed(
        &db,
        (0..SOURCE)
            .filter(|entity_id| *entity_id != 50)
            .map(|entity_id| {
                let vector = partners
                    .iter()
                    .find(|(_, partner)| *partner == entity_id)
                    .map_or(vector_of(entity_id), |(stored, _)| beside(*stored));
                (entity_id, vector)
            }),
    )
    .await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(4, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;
    assert_eq!(
        scan_progress(&db, build_id).await.unwrap().cursor,
        Some(source_cursor(SCOPE, 15))
    );
    let building = building_index(&db, &definition, build_id).await;
    for (stored, partner) in partners {
        let (links, _) = building.stored_layer0_links(&db, stored).await.unwrap();
        assert!(links.contains(&partner), "{stored} links {partner}");
    }

    put_source(&db, SCOPE, 40, &properties(vector_of(1_040), None)).await;
    db.delete(source_key(SCOPE, 41)).await.unwrap();
    let no_vector = [Property::new(
        "$label",
        PropertyValue::String("Document".to_string()),
    )];
    put_source(&db, SCOPE, 42, &no_vector).await;
    put_source(&db, SCOPE, 50, &properties(vector_of(50), None)).await;
    // The last source row: the scan ends before its node.
    db.delete(source_key(SCOPE, SOURCE - 1)).await.unwrap();
    // Behind the scan, and unchanged bytes ahead of it.
    put_source(&db, SCOPE, 5, &properties(vector_of(1_005), None)).await;
    put_source(&db, SCOPE, 60, &properties(vector_of(60), None)).await;

    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 3).await;
    for (stored, partner) in &partners[..3] {
        let (links, sources) = building.stored_layer0_links(&db, *stored).await.unwrap();
        assert!(!links.contains(partner), "{stored} lost {partner}");
        // A reverse locator is written with its source's row.
        let stored_links = links.iter().copied().filter(|link| *link < 64);
        assert_eq!(sources, stored_links.collect::<Vec<_>>());
    }
    put_source(&db, SCOPE, 200, &properties(vector_of(1_200), None)).await;

    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    let absent = [40, 41, 42, 50, 200, SOURCE - 1];
    expected.retain(|entity_id, _| !absent.contains(entity_id));
    assert_index_holds(
        &db,
        &active_index(&db, &definition).await,
        &expected,
        &absent,
    )
    .await;
    assert!(nothing_held(&driver));
    db.close().await.expect("vector test database closes");
}

/// A step whose commit is lost leaves the stored rows where the last
/// committed step left them while the in-memory graph has already dropped a
/// changed entity. The repeated step stages the neighbor's row again.
#[tokio::test(flavor = "multi_thread")]
async fn a_step_that_loses_its_commit_is_repeated_from_the_stored_rows() {
    let db = test_db("vector-bulk-lost-commit").await;
    let definition = definition(None);
    let mut expected = seed(
        &db,
        (0..SOURCE).map(|entity_id| {
            let vector = match entity_id {
                20 => beside(0),
                entity_id => vector_of(entity_id),
            };
            (entity_id, vector)
        }),
    )
    .await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(4, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;
    let building = building_index(&db, &definition, build_id).await;
    let (links, _) = building.stored_layer0_links(&db, 0).await.unwrap();
    assert!(links.contains(&20));
    put_source(&db, SCOPE, 20, &properties(vector_of(1_020), None)).await;

    let stored = building.stored_rows(&db).await.unwrap();
    let progress = scan_progress(&db, build_id).await;
    lose_commit(
        &db,
        &driver,
        &definition,
        build_id,
        &mut claim_sequence,
        limits,
    )
    .await;
    assert_eq!(scan_progress(&db, build_id).await, progress);
    assert!(building.stored_rows(&db).await.unwrap() == stored);

    assert_eq!(
        retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(
        scan_progress(&db, build_id).await.unwrap().cursor,
        Some(source_cursor(SCOPE, 31))
    );
    // A commit that did not land ended no attempt.
    assert_eq!(attempts(&db, build_id).await, Some(1));
    let (links, sources) = building.stored_layer0_links(&db, 0).await.unwrap();
    assert!(!links.contains(&20));
    // A reverse locator is written with its source's row.
    let stored_links = links.iter().copied().filter(|link| *link < 32);
    assert_eq!(sources, stored_links.collect::<Vec<_>>());

    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    expected.remove(&20);
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[20]).await;
    db.close().await.expect("vector test database closes");
}

/// A step whose first source row is invalid reports it only when it has
/// nothing to commit: the rows a lost commit left to stage again are
/// committed first, with the scan where it was. The next step finds the row
/// with nothing staged, and the build leaves the bulk stage and blocks.
#[tokio::test(flavor = "multi_thread")]
async fn a_step_commits_the_rows_it_stages_again_before_it_blocks() {
    let db = test_db("vector-bulk-restage-then-block").await;
    let definition = definition(None);
    seed(
        &db,
        (0..SOURCE).map(|entity_id| {
            let vector = match entity_id {
                20 => beside(0),
                entity_id => vector_of(entity_id),
            };
            (entity_id, vector)
        }),
    )
    .await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(4, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;
    let building = building_index(&db, &definition, build_id).await;
    put_source(&db, SCOPE, 20, &properties(vector_of(1_020), None)).await;
    // The step unlinks 20 in memory and its commit is lost.
    lose_commit(
        &db,
        &driver,
        &definition,
        build_id,
        &mut claim_sequence,
        limits,
    )
    .await;
    db.put(source_key(SCOPE, 16), Bytes::from_static(b"malformed"))
        .await
        .expect("invalid vector source is written");

    assert_eq!(
        retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(
        scan_progress(&db, build_id).await.unwrap().cursor,
        Some(source_cursor(SCOPE, 15))
    );
    let (links, _) = building.stored_layer0_links(&db, 0).await.unwrap();
    assert!(!links.contains(&20));
    assert_eq!(attempts(&db, build_id).await, Some(1));

    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Blocked
    );
    let operation = read_operation(&db, SCOPE, build_id).await;
    assert!(matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
            entity_id,
            ..
        }) if *entity_id == IndexEntityId::new(16)
    ));
    assert_blocked_at_the_serial_stage(&db, build_id).await;
    assert!(building.stored_rows(&db).await.unwrap().is_empty());
    assert!(nothing_held(&driver));
    db.close().await.expect("vector test database closes");
}

/// A step unlinks at most as many nodes as it may read entities: a run of
/// deleted entities longer than that is unlinked over several steps, and
/// none of them is stored. Entities deleted with their label memberships are
/// no members to take, so those steps leave the scan where it was; rows gone
/// from under memberships that stayed are taken and passed, sixteen ids a
/// step, as the serial scan passes them.
#[tokio::test(flavor = "multi_thread")]
async fn a_long_run_of_deleted_entities_is_unlinked_over_several_steps() {
    for (name, memberships_stay, steps) in [
        // 16..32 and 32..48 are unlinked, then 48..56 before sixteen rows.
        ("deleted", false, [(15, 16), (15, 16), (71, 32)]),
        // The ids 16..32 and 32..48 are passed, then 48..56 before eight rows.
        ("rows-gone", true, [(31, 16), (47, 16), (63, 24)]),
    ] {
        let db = test_db(&format!("vector-bulk-deleted-run-{name}")).await;
        let definition = definition(None);
        let mut expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let driver = bulk_driver(2, BUDGET, 16);
        let limits = entity_limits(16);
        let mut claim_sequence = 1;
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            limits,
            COUNT + 1,
        )
        .await;

        let deleted = (16..56).collect::<Vec<u64>>();
        if memberships_stay {
            let mut batch = slatedb::WriteBatch::new();
            for entity_id in &deleted {
                batch.delete(source_key(SCOPE, *entity_id));
            }
            db.write(batch).await.expect("vector sources are deleted");
        } else {
            delete_sources(&db, deleted.iter().copied()).await;
        }
        expected.retain(|entity_id, _| !deleted.contains(entity_id));

        // Sixteen unlinks a step, and deleted entities are no rows to count.
        for (cursor, entities) in steps {
            drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
            let progress = scan_progress(&db, build_id).await.unwrap();
            assert_eq!(
                progress.cursor,
                Some(source_cursor(SCOPE, cursor)),
                "{name}"
            );
            assert_eq!(progress.counters.entities, entities, "{name}");
        }

        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            reported_entities(&db, build_id).await,
            SOURCE - deleted.len() as u64,
            "{name}"
        );
        assert_index_holds(
            &db,
            &active_index(&db, &definition).await,
            &expected,
            &deleted,
        )
        .await;
        assert!(nothing_held(&driver), "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// A source emptied after the graph was linked and before any of it was
/// stored leaves nothing to write: every node is unlinked over bounded steps,
/// no vector, link or metadata row is staged for them, and validation
/// activates the empty index a serial build of an empty source leaves.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_emptied_before_the_first_stored_row_activates_an_empty_index() {
    let db = test_db("vector-bulk-emptied-source").await;
    let definition = definition(None);
    seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(2, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, COUNT).await;
    // The read pass takes its snapshot in this step, whose commit is lost:
    // whatever it staged, nothing is stored.
    lose_commit(
        &db,
        &driver,
        &definition,
        build_id,
        &mut claim_sequence,
        limits,
    )
    .await;
    let building = building_index(&db, &definition, build_id).await;
    assert!(building.stored_rows(&db).await.unwrap().is_empty());

    let mut batch = slatedb::WriteBatch::new();
    for entity_id in 0..SOURCE {
        batch.delete(source_key(SCOPE, entity_id));
    }
    db.write(batch).await.expect("vector sources are deleted");

    assert_eq!(
        retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    let active = active_index(&db, &definition).await;
    let metadata = active
        .get_metadata(&db)
        .await
        .expect("metadata is readable")
        .expect("validation creates the metadata of an empty index");
    assert_eq!((metadata.count, metadata.entry_point), (0, None));
    // The metadata row is all the namespace holds.
    assert_eq!(active.stored_rows(&db).await.unwrap().len(), 1);
    assert_eq!(applied_rows(&db, index_id, generation).await, 0);
    assert!(nothing_held(&driver));
    db.close().await.expect("vector test database closes");
}

/// A process that finds a partially written graph deletes its applied-state
/// and physical rows in bounded steps, under the bulk stage. The first of
/// them restarts the stored scan, a process restarted at any later step goes
/// on deleting, and a step that loses its commit is repeated. No step in
/// between stores a metadata row, and the attempt that follows stores the
/// index an uninterrupted build stores and counts its entities once.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_in_the_write_pass_deletes_the_rows_and_builds_the_same_index() {
    let limits = entity_limits(16);
    let uninterrupted = uninterrupted_rows("vector-bulk-uninterrupted", &[]).await;
    let db = test_db("vector-bulk-crash-in-write").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &bulk_driver(1, BUDGET, 16),
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 3,
    )
    .await;
    let crashed = scan_progress(&db, build_id).await.unwrap();
    assert_eq!(crashed.cursor, Some(source_cursor(SCOPE, 47)));
    let building = building_index(&db, &definition, build_id).await;
    let partial = building.stored_rows(&db).await.unwrap().len() + 48;
    assert_eq!(applied_rows(&db, index_id, generation).await, 48);

    // Sixty-four deletes a step, with a restart before every fifth step.
    let few_operations = operation_limits(64);
    let mut restarted = bulk_driver(1, BUDGET, 16);
    let mut left = partial;
    let mut discard_steps = 0;
    while left > 0 {
        assert!(discard_steps < 100, "{left} rows are left");
        if discard_steps % 5 == 4 {
            restarted = bulk_driver(1, BUDGET, 16);
        }
        if discard_steps == 2 {
            lose_commit(
                &db,
                &restarted,
                &definition,
                build_id,
                &mut claim_sequence,
                few_operations,
            )
            .await;
            assert_eq!(
                building.stored_rows(&db).await.unwrap().len()
                    + applied_rows(&db, index_id, generation).await,
                left
            );
            assert_eq!(
                retry_conflicted_step(
                    &db,
                    &restarted,
                    build_id,
                    &mut claim_sequence,
                    few_operations
                )
                .await,
                CommittedOperationStep::Progressed
            );
        } else {
            drive_steps(
                &db,
                &restarted,
                build_id,
                &mut claim_sequence,
                few_operations,
                1,
            )
            .await;
        }
        discard_steps += 1;
        assert_eq!(
            stored_scan(&db, build_id).await,
            Some(StoredScan::Bulk(BulkBuildProgress {
                scan: scan_from_start(&crashed),
                attempts: BulkBuildAttempts::FIRST,
            })),
            "a deleting step restarts the scan and keeps the stage and its count"
        );
        assert!(building.get_metadata(&db).await.unwrap().is_none());
        let now = building.stored_rows(&db).await.unwrap().len()
            + applied_rows(&db, index_id, generation).await;
        assert!(
            (left.saturating_sub(64)..=left).contains(&now),
            "a step left {now} of {left} rows"
        );
        left = now;
    }
    assert!(
        discard_steps > partial / 64,
        "{discard_steps} steps deleted {partial} rows"
    );

    // A process that finds the bulk stage and no row starts another attempt.
    let finishing = bulk_driver(1, BUDGET, 16);
    drive_steps(
        &db,
        &finishing,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT,
    )
    .await;
    assert_eq!(attempts(&db, build_id).await, Some(2));
    assert_eq!(
        drive(&db, &finishing, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    // Nothing of the attempt that died is left in the counters.
    assert_eq!(reported_entities(&db, build_id).await, SOURCE);
    assert_eq!(
        finishing.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    let index = active_index(&db, &definition).await;
    assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
    assert_index_holds(&db, &index, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A bounded batch of applied-state deletes hands on the last key it
/// deleted. A scan resumed there deletes the rows after it and cannot report
/// the rows exhausted; a scan from the start can, and a key of no row of the
/// generation is no position.
#[tokio::test(flavor = "multi_thread")]
async fn applied_state_deletes_resume_behind_the_last_deleted_key() {
    let db = test_db("vector-bulk-applied-resume").await;
    let definition = definition(None);
    seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    drive_steps(
        &db,
        &bulk_driver(1, BUDGET, 16),
        build_id,
        &mut 1,
        entity_limits(16),
        COUNT + 3,
    )
    .await;
    let operation = read_operation(&db, SCOPE, build_id).await;
    let defaults = SearchIndexBackfillLimits::default().batch();
    let foreign = Bytes::from_static(b"\xFFno row of the generation");
    let mut deleted = Vec::new();
    for (resume, limits) in [
        (None, operation_limits(8)),
        (None, defaults),
        (Some(&foreign), defaults),
    ] {
        // Dropped uncommitted: every batch reads all 48 rows.
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let CleanupWorkOutcome::Progress {
            counters,
            exhausted,
            resume: next,
        } = delete_delta_and_applied_rows(
            &transaction,
            SCOPE,
            &operation,
            &[RecordKind::AppliedState],
            resume,
            OperationCounters::default(),
            limits,
        )
        .await
        .unwrap()
        else {
            panic!("applied-state deletes fit the limits");
        };
        deleted.push((counters.output_operations, exhausted, next));
    }
    let (_, _, Some(eighth)) = &deleted[0] else {
        panic!("a batch that stops early hands on its last key");
    };
    assert_eq!((deleted[0].0, deleted[0].1), (8, false));
    assert!(deleted[1..]
        .iter()
        .all(|batch| matches!(batch, (48, true, None))));

    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let CleanupWorkOutcome::Progress {
        counters,
        exhausted: false,
        resume: None,
    } = delete_delta_and_applied_rows(
        &transaction,
        SCOPE,
        &operation,
        &[RecordKind::AppliedState],
        Some(eighth),
        OperationCounters::default(),
        defaults,
    )
    .await
    .unwrap()
    else {
        panic!("a resumed scan that reaches the end proves nothing exhausted");
    };
    assert_eq!(counters.output_operations, 40);
    db.close().await.expect("vector test database closes");
}

/// A deleting step resumes behind the last row it deleted, so only a scan
/// from the start ends the deletion: a row stored behind the resume position
/// is deleted before pass 1 starts.
#[tokio::test(flavor = "multi_thread")]
async fn a_row_behind_the_resume_position_is_deleted_before_the_build_starts() {
    let limits = entity_limits(16);
    // Entity 7 is stored by the crashed build and gone before the next one.
    let uninterrupted = uninterrupted_rows("vector-bulk-uninterrupted-without-7", &[7]).await;
    let db = test_db("vector-bulk-row-behind-resume").await;
    let definition = definition(None);
    let mut expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &bulk_driver(1, BUDGET, 16),
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 3,
    )
    .await;
    let building = building_index(&db, &definition, build_id).await;
    let partial = building.stored_rows(&db).await.unwrap();
    db.delete(source_key(SCOPE, 7)).await.unwrap();
    expected.remove(&7);

    // A row only the crashed build stores, deleted while rows are left.
    let driver = bulk_driver(1, BUDGET, 16);
    let (key, value) = loop {
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            operation_limits(64),
            1,
        )
        .await;
        let left = building.stored_rows(&db).await.unwrap();
        assert!(!left.is_empty(), "no row of entity 7 went before the last");
        let Some((key, value)) = partial
            .iter()
            .find(|(key, _)| !left.contains_key(*key) && !uninterrupted.contains_key(*key))
        else {
            continue;
        };
        break (key.clone(), value.clone());
    };
    let state = driver.bulk.builds.lock().get(&build_id).cloned().unwrap();
    assert!(matches!(
        &*state.lock().await,
        BuildState::Discarding {
            resume: Some(DiscardResume::Physical(_)),
            ..
        }
    ));
    db.put(key, value)
        .await
        .expect("the deleted row is restored");

    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    let index = active_index(&db, &definition).await;
    assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
    assert_index_holds(&db, &index, &expected, &[7]).await;
    db.close().await.expect("vector test database closes");
}

/// A step that deletes the last stored rows restarts the stored scan like
/// every deleting step: only a step that deleted nothing found nothing
/// stored. Physical rows under a cursor without applied state are a shape no
/// step stores; they are deleted like any other.
#[tokio::test(flavor = "multi_thread")]
async fn a_step_that_deletes_the_last_rows_restarts_the_scan() {
    let limits = entity_limits(16);
    let uninterrupted = uninterrupted_rows("vector-bulk-uninterrupted-last-rows", &[]).await;
    let db = test_db("vector-bulk-last-rows").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &bulk_driver(1, BUDGET, 16),
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 3,
    )
    .await;
    let crashed = scan_progress(&db, build_id).await.unwrap();
    assert_eq!(crashed.cursor, Some(source_cursor(SCOPE, 47)));

    let mut applied = db
        .scan_prefix(
            generation_prefix(SCOPE, RecordKind::AppliedState, index_id, generation),
            ..,
        )
        .await
        .expect("applied-state prefix is readable");
    let mut batch = slatedb::WriteBatch::new();
    loop {
        let Some(row) = applied.next().await.expect("applied-state row is readable") else {
            break;
        };
        batch.delete(row.key);
    }
    db.write(batch)
        .await
        .expect("applied-state rows are deleted");
    assert_eq!(applied_rows(&db, index_id, generation).await, 0);
    let building = building_index(&db, &definition, build_id).await;
    assert!(!building.stored_rows(&db).await.unwrap().is_empty());

    // The default limits delete every physical row in one step.
    let restarted = bulk_driver(1, BUDGET, 16);
    drive_steps(&db, &restarted, build_id, &mut claim_sequence, limits, 1).await;
    assert!(building.stored_rows(&db).await.unwrap().is_empty());
    assert_eq!(
        stored_scan(&db, build_id).await,
        Some(StoredScan::Bulk(BulkBuildProgress {
            scan: scan_from_start(&crashed),
            attempts: BulkBuildAttempts::FIRST,
        }))
    );

    assert_eq!(
        drive(&db, &restarted, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        restarted.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    let index = active_index(&db, &definition).await;
    assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
    assert_index_holds(&db, &index, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A step that deletes the last stored rows starts no build: its deletes are
/// not committed yet, so a build begun beside them would write over the rows
/// still stored once the step lost its commit.
#[tokio::test(flavor = "multi_thread")]
async fn a_step_that_deletes_the_last_rows_starts_no_build() {
    let limits = entity_limits(16);
    let uninterrupted = uninterrupted_rows("vector-bulk-uninterrupted-lost-delete", &[]).await;
    let db = test_db("vector-bulk-lost-delete").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &bulk_driver(1, BUDGET, 16),
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 3,
    )
    .await;

    // Sixty-four entities a step delete the applied state in one step and
    // every physical row in the next.
    let limits = entity_limits(64);
    let restarted = bulk_driver(1, BUDGET, 16);
    drive_steps(&db, &restarted, build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(applied_rows(&db, index_id, generation).await, 0);
    assert_eq!(scan_progress(&db, build_id).await.unwrap().cursor, None);
    let building = building_index(&db, &definition, build_id).await;
    let partial = building.stored_rows(&db).await.unwrap();
    assert!(!partial.is_empty());

    lose_commit(
        &db,
        &restarted,
        &definition,
        build_id,
        &mut claim_sequence,
        limits,
    )
    .await;
    assert!(building.stored_rows(&db).await.unwrap() == partial);
    let state = restarted
        .bulk
        .builds
        .lock()
        .get(&build_id)
        .cloned()
        .unwrap();
    assert!(matches!(
        &*state.lock().await,
        BuildState::Discarding { resume: None, .. }
    ));
    assert_eq!(restarted.bulk.memory.reserved(), 0);

    assert_eq!(
        retry_conflicted_step(&db, &restarted, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert!(building.stored_rows(&db).await.unwrap().is_empty());
    assert_eq!(
        drive(&db, &restarted, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    let index = active_index(&db, &definition).await;
    assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
    assert_index_holds(&db, &index, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A scan with a cursor and nothing stored has met no vector yet. A process
/// that finds it scans on serially from the cursor instead of starting over.
#[tokio::test(flavor = "multi_thread")]
async fn a_scan_that_stored_no_vector_is_continued_serially() {
    let db = test_db("vector-bulk-empty-serial-scan").await;
    let definition = definition(None);
    let no_vector = || {
        vec![Property::new(
            "$label",
            PropertyValue::String("Document".to_string()),
        )]
    };
    put_sources(&db, (0..16).map(|entity_id| (entity_id, no_vector()))).await;
    let expected = seed(
        &db,
        (16..SOURCE).map(|entity_id| (entity_id, vector_of(entity_id))),
    )
    .await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(&db, &driver(), build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(
        scan_progress(&db, build_id).await.unwrap().cursor,
        Some(source_cursor(SCOPE, 15))
    );
    let building = building_index(&db, &definition, build_id).await;
    assert!(building.stored_rows(&db).await.unwrap().is_empty());

    let restarted = bulk_driver(2, BUDGET, 16);
    drive_steps(&db, &restarted, build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(
        scan_progress(&db, build_id).await.unwrap().cursor,
        Some(source_cursor(SCOPE, 31))
    );
    assert_eq!(
        restarted.build_reports().path(build_id),
        Some(VectorBuildPath::Serial(
            VectorSerialBuildReason::ResumedSerialScan
        ))
    );
    assert_eq!(
        drive(&db, &restarted, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    let absent = (0..16).collect::<Vec<u64>>();
    assert_index_holds(
        &db,
        &active_index(&db, &definition).await,
        &expected,
        &absent,
    )
    .await;
    db.close().await.expect("vector test database closes");
}

/// A process that dies in pass 1 or 2 has stored nothing but its attempt:
/// its steps committed only the stage they found. The next process counts a
/// second attempt, links the source from the start and stores the rows an
/// uninterrupted build stores.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_before_the_write_pass_starts_the_build_again() {
    let limits = entity_limits(16);
    let uninterrupted = uninterrupted_rows("vector-bulk-crash-preparing-reference", &[]).await;
    let db = test_db("vector-bulk-crash-preparing").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let mut claim_sequence = 1;

    // Passes 1 and 2 of the first process never finish.
    let crashed = bulk_driver(1, BUDGET, 16);
    let cancelled = prepare_with(
        &db,
        &crashed,
        build_id,
        &mut claim_sequence,
        tokio::spawn(std::future::pending::<Result<Prepared>>()),
    )
    .await;
    let first_attempt = counted(&db, build_id, 1).await;
    parked_steps(&db, &crashed, build_id, &mut claim_sequence, limits, 2).await;
    assert_eq!(
        stored_scan(&db, build_id).await,
        Some(StoredScan::Bulk(first_attempt)),
        "steps that wait for the passes store nothing of them"
    );
    assert!(building_index(&db, &definition, build_id)
        .await
        .stored_rows(&db)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(applied_rows(&db, index_id, generation).await, 0);
    drop(crashed);
    assert!(cancelled.load(Ordering::Relaxed));

    let restarted = bulk_driver(1, BUDGET, 16);
    drive_steps(
        &db,
        &restarted,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT,
    )
    .await;
    assert_eq!(attempts(&db, build_id).await, Some(2));
    assert_eq!(
        drive(&db, &restarted, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        restarted.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    let index = active_index(&db, &definition).await;
    assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
    assert_index_holds(&db, &index, &expected, &[]).await;
    assert!(nothing_held(&restarted));
    db.close().await.expect("vector test database closes");
}

/// A build that waited its turn while a second process stored rows and a
/// third began to delete them finds the scan without a cursor, as it left
/// it, over rows it must not write on. The stage moved, so it looks at what
/// is stored again and deletes it.
#[tokio::test(flavor = "multi_thread")]
async fn a_waiting_build_deletes_rows_stored_while_it_waited() {
    let limits = entity_limits(16);
    // Entity 7 is stored by the second process and gone before pass 1.
    let uninterrupted = uninterrupted_rows("vector-bulk-waited-without-7", &[7]).await;
    let db = test_db("vector-bulk-moved-while-waiting").await;
    let definition = definition(None);
    let mut expected = seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let mut claim_sequence = 1;
    let first = bulk_driver(1, BUDGET, 16);
    let other_build = Arc::clone(&first.bulk.preparing)
        .try_acquire_owned()
        .expect("no build is preparing");
    parked_steps(&db, &first, build_id, &mut claim_sequence, limits, 1).await;

    drive_steps(
        &db,
        &bulk_driver(1, BUDGET, 16),
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 2,
    )
    .await;
    drive_steps(
        &db,
        &bulk_driver(1, BUDGET, 16),
        build_id,
        &mut claim_sequence,
        operation_limits(8),
        1,
    )
    .await;
    assert_eq!(scan_progress(&db, build_id).await.unwrap().cursor, None);
    assert_eq!(applied_rows(&db, index_id, generation).await, 24);
    delete_sources(&db, [7]).await;
    expected.remove(&7);
    drop(other_build);

    // Sixteen entities a step.
    drive_steps(&db, &first, build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(first.build_reports().path(build_id), None);
    assert_eq!(applied_rows(&db, index_id, generation).await, 8);
    assert_eq!(
        drive(&db, &first, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    let index = active_index(&db, &definition).await;
    assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
    assert_index_holds(&db, &index, &expected, &[7]).await;
    db.close().await.expect("vector test database closes");
}

/// A build in passes 1 and 2 whose scan another process moved gives up what
/// it reads and links, and deletes what that process stored.
#[tokio::test(flavor = "multi_thread")]
async fn a_preparing_build_gives_up_when_the_scan_moves() {
    let db = test_db("vector-bulk-moved-while-preparing").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    let first = bulk_driver(2, BUDGET, 16);
    let cancelled = prepare_with(
        &db,
        &first,
        build_id,
        &mut claim_sequence,
        tokio::spawn(std::future::pending::<Result<Prepared>>()),
    )
    .await;
    parked_steps(&db, &first, build_id, &mut claim_sequence, limits, 1).await;
    assert!(!cancelled.load(Ordering::Relaxed));

    // A second process counts its own attempt and stores a first step.
    drive_steps(
        &db,
        &bulk_driver(2, BUDGET, 16),
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;
    assert_eq!(attempts(&db, build_id).await, Some(2));
    assert_eq!(
        scan_progress(&db, build_id).await.unwrap().cursor,
        Some(source_cursor(SCOPE, 15))
    );
    drive_steps(&db, &first, build_id, &mut claim_sequence, limits, 1).await;
    assert!(cancelled.load(Ordering::Relaxed));
    assert_eq!(scan_progress(&db, build_id).await.unwrap().cursor, None);

    assert_eq!(
        drive(&db, &first, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    assert!(nothing_held(&first));
    db.close().await.expect("vector test database closes");
}

/// A graph whose stored scan another process moved is dropped: the process
/// that holds it deletes the stored rows and links again.
#[tokio::test(flavor = "multi_thread")]
async fn a_graph_the_stored_scan_left_behind_is_dropped() {
    let db = test_db("vector-bulk-stale-graph").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    let first = bulk_driver(2, BUDGET, 16);
    drive_steps(
        &db,
        &first,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;

    // A second process discards the first one's rows and writes two batches.
    let second = bulk_driver(2, BUDGET, 16);
    let mut discarded = false;
    while scan_progress(&db, build_id).await.unwrap().cursor != Some(source_cursor(SCOPE, 31)) {
        drive_steps(&db, &second, build_id, &mut claim_sequence, limits, 1).await;
        discarded |= scan_progress(&db, build_id).await.unwrap().cursor.is_none();
    }
    assert!(discarded);

    // The first process holds a graph for a scan at entity 15: its step
    // stores nothing of it and begins to delete what the second stored.
    let building = building_index(&db, &definition, build_id).await;
    let stored = building.stored_rows(&db).await.unwrap().len();
    drive_steps(&db, &first, build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(scan_progress(&db, build_id).await.unwrap().cursor, None);
    assert!(building.stored_rows(&db).await.unwrap().len() <= stored);
    assert_eq!(
        drive(&db, &first, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    assert!(nothing_held(&first));
    db.close().await.expect("vector test database closes");
}

/// A step takes the graph out of its build for as long as it runs, and hands
/// it back whole when it ends. A step that never ends, because its task was
/// dropped part-way through an unlink, takes the graph along, so no later
/// step can write from a graph an unlink left torn: the next step deletes
/// the stored rows and the build starts over, with another attempt.
#[tokio::test(flavor = "multi_thread")]
async fn a_graph_an_unlink_left_torn_is_dropped() {
    let db = test_db("vector-bulk-torn-graph").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    let driver = bulk_driver(2, BUDGET, 16);
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 2,
    )
    .await;
    assert_eq!(
        scan_progress(&db, build_id).await.unwrap().cursor,
        Some(source_cursor(SCOPE, 31))
    );

    let slot = Arc::clone(&driver.bulk.builds.lock()[&build_id]);
    let mut state = slot.lock().await;
    let BuildState::Writing(writing) = &mut *state else {
        panic!("the build is in its write pass");
    };
    // What a step does with the graph.
    let GraphSlot::Whole(LinkedGraph::Euclidean(mut graph)) =
        std::mem::replace(&mut writing.graph, GraphSlot::Taken)
    else {
        panic!("the fixture graph is whole and Euclidean");
    };
    let nodes = graph.len() as u32;
    assert!(graph.unlink(nodes - 1, &mut writing.journal).is_ok());
    assert!(!writing.journal.is_empty());
    // No node has this position.
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = graph.unlink(nodes, &mut writing.journal);
    }))
    .is_err());
    drop(graph);
    assert!(matches!(writing.graph, GraphSlot::Taken));
    drop(state);
    // The slot would keep the finished build's reservation alive.
    drop(slot);
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(scan_progress(&db, build_id).await.unwrap().cursor, None);
    // The attempt this process ended itself is counted like any other.
    assert_eq!(attempts(&db, build_id).await, Some(1));
    while scan_progress(&db, build_id).await.unwrap().cursor.is_none() {
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
    }
    assert_eq!(attempts(&db, build_id).await, Some(2));
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    assert!(nothing_held(&driver));
    db.close().await.expect("vector test database closes");
}

/// A build that finds another build in passes 1 and 2 commits the unchanged
/// serial stage once and parks: it stores nothing, reserves nothing, counts
/// no attempt and says what it waits for. The worker gives it no step until
/// the turn is released, which wakes it; then it takes its turn.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_waits_its_turn_without_moving_the_scan() {
    let db = test_db("vector-bulk-waiting").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let worker = IndexWorkerWakeHandle::default();
    let driver = bulk_driver_waking(2, BUDGET, 16, worker.clone());
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    let created = stored_scan(&db, build_id).await.unwrap();
    assert!(matches!(&created, StoredScan::Serial(progress) if progress.cursor.is_none()));

    let other_build = Turn {
        _permit: Arc::clone(&driver.bulk.preparing)
            .try_acquire_owned()
            .expect("no build is preparing"),
        parking: Arc::clone(&driver.bulk.parking),
    };
    let before = Instant::now();
    assert_eq!(
        drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    let record = read_operation(&db, SCOPE, build_id).await;
    let until = driver
        .parked_until(&record)
        .expect("the waiting build is parked");
    assert!(until >= before + PARK_LIMIT && until <= Instant::now() + PARK_LIMIT);
    assert!(!worker.take_wake(), "nothing the build waits for has moved");
    assert_eq!(stored_scan(&db, build_id).await.unwrap(), created);
    assert_eq!(driver.build_reports().path(build_id), None);
    assert_eq!(
        driver.build_reports().phase(build_id),
        Some(VectorBulkBuildPhase::Waiting {
            waiting_for: VectorBulkBuildWait::Turn
        })
    );
    assert_eq!(driver.bulk.memory.reserved(), 0);
    assert!(building_index(&db, &definition, build_id)
        .await
        .stored_rows(&db)
        .await
        .unwrap()
        .is_empty());
    // A step the worker gives it at the park limit finds it waiting still.
    assert_eq!(
        drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert!(driver
        .parked_until(&read_operation(&db, SCOPE, build_id).await)
        .is_some());
    assert_eq!(stored_scan(&db, build_id).await.unwrap(), created);

    drop(other_build);
    assert!(worker.take_wake(), "the released turn wakes the worker");
    assert_eq!(
        driver.parked_until(&read_operation(&db, SCOPE, build_id).await),
        None
    );
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert_eq!(driver.build_reports().phase(build_id), None);
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// Forgetting a build in passes 1 and 2 raises its cancellation flag and
/// frees its turn and its memory once the task is dropped, which wakes
/// whatever waited for either.
#[tokio::test(flavor = "multi_thread")]
async fn forgetting_a_preparing_build_frees_its_turn_and_memory() {
    let db = test_db("vector-bulk-forget-preparing").await;
    let definition = definition(None);
    put_source(&db, SCOPE, 0, &properties(vector_of(0), None)).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, 0).await;
    let worker = IndexWorkerWakeHandle::default();
    let builds = bulk_driver_waking(2, BUDGET, 0, worker.clone()).bulk;
    let turn = Turn {
        _permit: Arc::clone(&builds.preparing)
            .try_acquire_owned()
            .expect("no build is preparing"),
        parking: Arc::clone(&builds.parking),
    };
    let Admission::Reserved(reservation) = MemoryReservation::admit(&builds.memory, 1 << 20) else {
        panic!("the budget holds the reservation");
    };
    let cancelled = Arc::new(AtomicBool::new(false));
    let (sender, done) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _held = (turn, reservation, sender);
        std::future::pending::<()>().await
    });
    let attempt = counted(&db, build_id, 1).await;
    builds.builds.lock().insert(
        build_id,
        Arc::new(tokio::sync::Mutex::new(BuildState::Preparing(
            Preparation {
                task,
                done,
                cancelled: Arc::clone(&cancelled),
                attempt,
                progress: Arc::default(),
            },
        ))),
    );
    assert_eq!(builds.memory.reserved(), 1 << 20);
    // Another build waits for the turn or the memory.
    let waiting = IndexOperationId::from_bytes([0x77; 16]).unwrap();
    builds.parking.park(waiting);

    builds.forget(build_id);
    assert!(cancelled.load(Ordering::Relaxed));
    tokio::time::timeout(WOKEN_WITHIN, async {
        while builds.memory.reserved() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the aborted task drops its reservation");
    assert_eq!(builds.preparing.available_permits(), 1);
    assert_eq!(builds.parking.until(waiting), None);
    assert!(worker.take_wake());
    db.close().await.expect("vector test database closes");
}

/// An abort during pass 3 takes the operation off the bulk stage at once,
/// drops the graph with its reservation and removes every row the build
/// stored.
#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_build_drops_its_graph_and_rows() {
    let db = test_db("vector-bulk-abort").await;
    let definition = definition(None);
    seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(2, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 2,
    )
    .await;
    assert_eq!(attempts(&db, build_id).await, Some(1));
    let building = building_index(&db, &definition, build_id).await;
    assert!(!building.stored_rows(&db).await.unwrap().is_empty());
    assert!(!nothing_held(&driver));

    let receipt = drop_index_operation(&db, SCOPE, &definition)
        .await
        .expect("building vector converts to abort cleanup");
    assert!(matches!(
        receipt,
        IndexDdlReceipt::ExistingOperation { operation_id } if operation_id == build_id
    ));
    // The abort itself stores cleanup progress, which every binary reads.
    assert!(matches!(
        read_operation(&db, SCOPE, build_id).await.progress(),
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(
            VectorCleanupProgress::RetireCache(_)
        ))
    ));
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert!(nothing_held(&driver));
    assert!(matches!(
        read_index(&db, SCOPE, &definition).await.state(),
        IndexStateV2::Dropped { .. }
    ));
    assert!(building.stored_rows(&db).await.unwrap().is_empty());
    assert_eq!(applied_rows(&db, index_id, generation).await, 0);
    db.close().await.expect("vector test database closes");
}

/// A node whose rows no step can hold blocks a build whose step staged
/// nothing: the graph is dropped and the build leaves the bulk stage before
/// it blocks. A retried build starts over.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_no_step_can_hold_blocks_the_write_pass() {
    let db = test_db("vector-bulk-oversized-node").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(2, BUDGET, 16);
    let defaults = SearchIndexBackfillLimits::default().batch();
    let tiny_vector = SearchIndexBatchLimits::try_new(
        defaults.max_entities(),
        defaults.max_input_bytes(),
        defaults.max_output_operations(),
        defaults.max_output_bytes(),
        NonZeroU64::new(64).expect("vector output limit is positive"),
    )
    .expect("tiny vector limits validate");
    let mut claim_sequence = 1;
    // The step that counts the attempt, and the one that stores the serial
    // stage again once the write pass found the node.
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, tiny_vector).await,
        (CommittedOperationStep::Blocked, COUNT + 1)
    );
    assert_blocked_at_the_serial_stage(&db, build_id).await;
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    let operation = read_operation(&db, SCOPE, build_id).await;
    assert!(matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Blocked(IndexOperationBlocker::OversizedEntity {
            entity_id,
            limit: 64,
            ..
        }) if *entity_id == IndexEntityId::new(0)
    ));
    assert!(nothing_held(&driver));
    assert!(building_index(&db, &definition, build_id)
        .await
        .stored_rows(&db)
        .await
        .unwrap()
        .is_empty());

    retry_operation(&db, SCOPE, build_id)
        .await
        .expect("the blocked build is retried");
    assert_eq!(
        drive(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            entity_limits(16)
        )
        .await
        .0,
        CommittedOperationStep::Completed
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A source row that turns invalid after pass 1 blocks the build at its
/// entity once the write pass reaches it: the rows stored before it are
/// deleted and the build blocks at the serial stage. The retried build
/// reads the repaired row.
#[tokio::test(flavor = "multi_thread")]
async fn a_row_invalid_after_the_read_pass_blocks_at_its_entity() {
    let db = test_db("vector-bulk-invalid-after-read").await;
    let definition = definition(None);
    let mut expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(2, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;
    db.put(source_key(SCOPE, 100), Bytes::from_static(b"malformed"))
        .await
        .expect("invalid vector source is written");

    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Blocked
    );
    let operation = read_operation(&db, SCOPE, build_id).await;
    assert!(matches!(
        operation.execution_state(),
        IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
            entity_id,
            ..
        }) if *entity_id == IndexEntityId::new(100)
    ));
    assert_blocked_at_the_serial_stage(&db, build_id).await;
    assert!(building_index(&db, &definition, build_id)
        .await
        .stored_rows(&db)
        .await
        .unwrap()
        .is_empty());
    assert!(nothing_held(&driver));

    put_source(&db, SCOPE, 100, &properties(vector_of(1_100), None)).await;
    expected.insert(100, vector_of(1_100));
    retry_operation(&db, SCOPE, build_id)
        .await
        .expect("the blocked build is retried");
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A step with enough nodes plans their rows on several threads and admits
/// the plans in source order: the stored rows are the ones that steps too
/// small for a second thread store.
#[tokio::test(flavor = "multi_thread")]
async fn steps_planned_on_several_threads_store_the_rows_one_thread_plans() {
    assert!(SOURCE as usize >= STAGING_THREADS * NODES_PER_STAGING_THREAD);
    let definition = definition(None);
    let mut builds = Vec::new();
    for (name, max_entities) in [
        ("vector-bulk-one-planner", NODES_PER_STAGING_THREAD - 1),
        ("vector-bulk-planners", SOURCE as usize),
    ] {
        let db = test_db(name).await;
        let expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        // One construction thread links the same graph every time.
        let (step, steps) = drive(
            &db,
            &bulk_driver(1, BUDGET, 16),
            build_id,
            &mut 1,
            entity_limits(max_entities),
        )
        .await;
        assert_eq!(step, CommittedOperationStep::Completed);
        let index = active_index(&db, &definition).await;
        assert_index_holds(&db, &index, &expected, &[]).await;
        builds.push((steps, index.stored_rows(&db).await.unwrap()));
        db.close().await.expect("vector test database closes");
    }
    assert!(builds[0].0 > builds[1].0, "smaller steps are more steps");
    assert!(builds[0].1 == builds[1].1);
}

/// A step reads further than its output limits admit. An entity it finds
/// changed past the last admitted row is unlinked in that step, which
/// restages the stored neighbor's row, and the rows it read and did not
/// admit are read again: every entity is stored once.
#[tokio::test(flavor = "multi_thread")]
async fn rows_read_past_the_output_limit_are_read_again() {
    let db = test_db("vector-bulk-output-limit").await;
    let definition = definition(None);
    let mut expected = seed(
        &db,
        (0..SOURCE).map(|entity_id| {
            let vector = match entity_id {
                100 => beside(0),
                entity_id => vector_of(entity_id),
            };
            (entity_id, vector)
        }),
    )
    .await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(4, BUDGET, 16);
    let defaults = SearchIndexBackfillLimits::default().batch();
    let few_operations = SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(128).expect("entity limit is positive"),
        defaults.max_input_bytes(),
        NonZeroU64::new(600).expect("operation limit is positive"),
        defaults.max_output_bytes(),
        defaults.max_single_vector_output_bytes(),
    )
    .expect("few-operation limits validate");
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        few_operations,
        COUNT + 1,
    )
    .await;
    let first = scan_progress(&db, build_id).await.expect("scan stage");
    let stored = applied_rows(&db, index_id, generation).await as u64;
    assert!(
        (1..50).contains(&stored),
        "{stored} entities fit the first step"
    );
    assert_eq!(first.cursor, Some(source_cursor(SCOPE, stored - 1)));
    assert_eq!(first.counters.entities, stored);
    let building = building_index(&db, &definition, build_id).await;
    let (links, _) = building.stored_layer0_links(&db, 0).await.unwrap();
    assert!(links.contains(&100));

    // Among the 128 rows the next step reads, past the ones it admits.
    put_source(&db, SCOPE, 100, &properties(vector_of(1_100), None)).await;
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        few_operations,
        1,
    )
    .await;
    let second = scan_progress(&db, build_id).await.expect("scan stage");
    let stored_twice = applied_rows(&db, index_id, generation).await as u64;
    assert!((stored + 1..100).contains(&stored_twice));
    assert_eq!(second.cursor, Some(source_cursor(SCOPE, stored_twice - 1)));
    // The rows read past the last admitted one are not counted.
    assert_eq!(second.counters.entities, stored_twice);
    let (links, _) = building.stored_layer0_links(&db, 0).await.unwrap();
    assert!(
        !links.contains(&100),
        "0 lost 100 before the scan reached it"
    );

    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, few_operations)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    expected.remove(&100);
    assert_index_holds(
        &db,
        &active_index(&db, &definition).await,
        &expected,
        &[100],
    )
    .await;
    assert!(nothing_held(&driver));
    db.close().await.expect("vector test database closes");
}

/// A second index on the standard source: its `summary` vectors.
fn summary_definition() -> ValidatedDynamicIndexDefinition {
    ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(
            &VectorIndexDefinition::new_node(
                "Document",
                "summary",
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("vector definition validates"),
        )
        .expect("V2 vector definition validates"),
    )
}

/// Seeds the standard source with a `summary` vector beside each embedding.
async fn seed_two_vectors(db: &Db) {
    put_sources(
        db,
        (0..SOURCE).map(|entity_id| {
            let mut both = properties(vector_of(entity_id), None);
            both.push(Property::new(
                "summary",
                PropertyValue::F32Array(vector_of(entity_id + 1_000).to_vec()),
            ));
            (entity_id, both)
        }),
    )
    .await;
}

/// Builds share one budget. A build that fits it, but not beside a running
/// build, waits: it holds the turn and no memory, stores nothing, takes one
/// step and parks, and builds in bulk once the running build has stored its
/// graph, which wakes it.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_waits_for_the_memory_of_a_running_build() {
    let db = test_db("vector-bulk-shared-budget").await;
    let (first, second) = (definition(None), summary_definition());
    seed_two_vectors(&db).await;
    let (first_id, _, _) = create_build(&db, SCOPE, &first, SOURCE - 1).await;
    let (second_id, _, _) = create_build(&db, SCOPE, &second, SOURCE - 1).await;
    let worker = IndexWorkerWakeHandle::default();
    let driver = bulk_driver_waking(2, ONE_WINDOWED_BUILD, 16, worker.clone());
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &driver,
        first_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;
    assert_eq!(
        driver.build_reports().path(first_id),
        Some(VectorBuildPath::Bulk)
    );
    let held = driver.bulk.memory.reserved();
    assert!((WINDOWED_BUILD..ONE_WINDOWED_BUILD).contains(&held));
    worker.take_wake();

    let created = stored_scan(&db, second_id).await.unwrap();
    assert!(matches!(created, StoredScan::Serial(_)));
    for _ in 0..2 {
        assert_eq!(
            drive_one(&db, &driver, second_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed
        );
        assert!(driver
            .parked_until(&read_operation(&db, SCOPE, second_id).await)
            .is_some());
    }
    // A build that waits has started no attempt.
    assert_eq!(stored_scan(&db, second_id).await.unwrap(), created);
    assert_eq!(driver.build_reports().path(second_id), None);
    assert_eq!(
        driver.build_reports().phase(second_id),
        Some(VectorBulkBuildPhase::Waiting {
            waiting_for: VectorBulkBuildWait::Memory
        })
    );
    assert_eq!(driver.bulk.memory.reserved(), held);
    assert_eq!(driver.bulk.preparing.available_permits(), 0);
    // The running build's steps do not wake the waiting one.
    drive_steps(&db, &driver, first_id, &mut claim_sequence, limits, 2).await;
    assert!(!worker.take_wake());
    assert!(driver
        .parked_until(&read_operation(&db, SCOPE, second_id).await)
        .is_some());

    assert_eq!(
        drive(&db, &driver, first_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(driver.bulk.memory.reserved(), 0);
    assert!(worker.take_wake(), "the released memory wakes the worker");
    assert_eq!(
        driver.parked_until(&read_operation(&db, SCOPE, second_id).await),
        None
    );
    assert_eq!(
        drive(&db, &driver, second_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.build_reports().path(second_id),
        Some(VectorBuildPath::Bulk)
    );
    assert!(nothing_held(&driver));
    assert_eq!(driver.bulk.preparing.available_permits(), 1);
    db.close().await.expect("vector test database closes");
}

/// A build no budget would hold does not wait for a running build's memory:
/// it scans serially at once, without the turn and without reading the
/// source for a bulk build.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_that_can_never_fit_scans_serially_without_waiting() {
    let db = test_db("vector-bulk-never-fits").await;
    let first = definition(None);
    let second = labelled_definition("Note");
    seed_standard(&db).await;
    // The second build's label holds one member more than the first's.
    put_sources(
        &db,
        (SOURCE..=2 * SOURCE).map(|entity_id| (entity_id, labelled("Note", vector_of(entity_id)))),
    )
    .await;
    let (first_id, _, _) = create_build(&db, SCOPE, &first, 2 * SOURCE).await;
    let (second_id, _, _) = create_build(&db, SCOPE, &second, 2 * SOURCE).await;
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    // The budget holds a paced build of the first label exactly.
    let driver = bulk_driver(2, paced_build_bytes(SOURCE), 16);
    drive_steps(
        &db,
        &driver,
        first_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;
    assert!(!nothing_held(&driver));

    drive_steps(&db, &driver, second_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(
        driver.build_reports().path(second_id),
        Some(VectorBuildPath::Serial(
            VectorSerialBuildReason::OverMemoryBudget
        ))
    );
    let progress = scan_progress(&db, second_id).await.unwrap();
    assert_eq!(progress.cursor, Some(source_cursor(SCOPE, SOURCE + 15)));
    assert_eq!(progress.counters.entities, 16);
    assert_eq!(driver.bulk.preparing.available_permits(), 1);
    for build_id in [first_id, second_id] {
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed
        );
    }
    assert_eq!(
        driver.build_reports().path(first_id),
        Some(VectorBuildPath::Bulk)
    );
    assert!(nothing_held(&driver));
    db.close().await.expect("vector test database closes");
}

/// An attempt reads nothing before its count is durable: the step after the
/// one that counts it waits for the WAL upload, which a progressed step does
/// not. A process that dies before the upload has started no attempt, and
/// the next one finds the serial stage.
#[tokio::test(flavor = "multi_thread")]
async fn an_attempt_starts_only_once_its_count_is_durable() {
    use crate::search::vector::gated_wal::{GatedWalStore, WalUploads};
    const NAME: &str = "vector-bulk-durable-count";
    let limits = entity_limits(16);
    for crash in [false, true] {
        let store = Arc::new(GatedWalStore::new());
        let db = Db::builder(
            NAME,
            Arc::clone(&store) as Arc<dyn slatedb::object_store::ObjectStore>,
        )
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("vector test database opens");
        crate::migrations::startup::bootstrap_writer(&db)
            .await
            .expect("vector test database bootstraps");
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        db.flush().await.expect("the created build is durable");
        let driver = bulk_driver(2, BUDGET, 16);
        let mut claim_sequence = 1;

        store.uploads.send_replace(WalUploads::Held);
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, COUNT).await;
        assert_eq!(attempts(&db, build_id).await, Some(1));
        {
            let mut reading = std::pin::pin!(drive_one(
                &db,
                &driver,
                build_id,
                &mut claim_sequence,
                limits
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(1_500), &mut reading)
                    .await
                    .is_err(),
                "the attempt read its source before its count was uploaded"
            );
            if !crash {
                store.uploads.send_replace(WalUploads::Open);
                assert_eq!(reading.await, CommittedOperationStep::Progressed);
            }
        }
        if crash {
            // The count never uploads: the old writer is gone with it.
            drop(driver);
            drop(db);
            let db = Db::builder(
                NAME,
                Arc::new(store.restarted()) as Arc<dyn slatedb::object_store::ObjectStore>,
            )
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("a new writer opens over the crashed one");
            assert!(matches!(
                stored_scan(&db, build_id).await,
                Some(StoredScan::Serial(progress)) if progress.cursor.is_none()
            ));
            db.close().await.expect("vector test database closes");
            continue;
        }
        // The step started the passes; the first one that finds them done
        // writes.
        if scan_progress(&db, build_id).await.unwrap().cursor.is_none() {
            drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
        }
        assert_eq!(
            scan_progress(&db, build_id).await.unwrap().cursor,
            Some(source_cursor(SCOPE, 15))
        );
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed
        );
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// The step that counts an attempt holds the attempt's turn and memory. When
/// its commit does not land no attempt has started: the turn and the memory
/// are given back, and the repeated step counts the same attempt.
#[tokio::test(flavor = "multi_thread")]
async fn a_count_that_did_not_land_is_no_attempt() {
    let db = test_db("vector-bulk-lost-count").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(2, ONE_WINDOWED_BUILD, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    lose_commit(
        &db,
        &driver,
        &definition,
        build_id,
        &mut claim_sequence,
        limits,
    )
    .await;
    assert!(matches!(
        stored_scan(&db, build_id).await,
        Some(StoredScan::Serial(_))
    ));
    let held = driver.bulk.memory.reserved();
    assert!(held > 0);

    // The budget holds one build: the repeated step reserves again only
    // because the lost one's reservation was dropped first.
    assert_eq!(
        retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(attempts(&db, build_id).await, Some(1));
    assert_eq!(driver.bulk.memory.reserved(), held);
    assert_eq!(driver.bulk.preparing.available_permits(), 0);
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert!(nothing_held(&driver));
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A build gets [`BulkBuildAttempts::LIMIT`] bulk attempts, wherever they die. Every
/// process that starts one counts it first; the process after the last
/// deletes what it left, starts none, and scans serially. The serial scan's
/// first step stores the serial stage with its cursor, which every later
/// process continues: the build cannot loop. The index is the source's and
/// its entities are counted once.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_whose_attempts_all_die_scans_serially() {
    let limits = entity_limits(16);
    for (name, death) in [
        ("counted", Death::Counted),
        ("reading", Death::Reading),
        ("writing", Death::Writing(3)),
    ] {
        let db = test_db(&format!("vector-bulk-attempt-limit-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut claim_sequence = 1;
        for attempt in 1..=BulkBuildAttempts::LIMIT {
            assert_eq!(
                die(&db, build_id, &mut claim_sequence, death).await,
                attempt,
                "{name}"
            );
        }

        let last = bulk_driver(1, BUDGET, 16);
        let serial = loop {
            drive_steps(&db, &last, build_id, &mut claim_sequence, limits, 1).await;
            match stored_scan(&db, build_id).await.expect("scan stage") {
                // Deleting steps keep the stage and its count.
                StoredScan::Bulk(progress) => {
                    assert_eq!(progress.attempts.get(), BulkBuildAttempts::LIMIT, "{name}");
                    assert_eq!(progress.scan.cursor, None, "{name}");
                }
                StoredScan::Serial(progress) => break progress,
            }
        };
        assert_eq!(serial.cursor, Some(source_cursor(SCOPE, 15)), "{name}");
        assert_eq!(serial.counters.entities, 16, "{name}");
        assert_eq!(applied_rows(&db, index_id, generation).await, 16, "{name}");
        assert_eq!(
            last.build_reports().path(build_id),
            Some(VectorBuildPath::Serial(
                VectorSerialBuildReason::EarlierAttemptsFailed
            )),
            "{name}"
        );
        assert_eq!(last.bulk.memory.reserved(), 0);
        assert_eq!(last.bulk.preparing.available_permits(), 1, "{name}");

        // No later process takes the build for a new one.
        let later = bulk_driver(1, BUDGET, 16);
        assert_eq!(
            drive(&db, &later, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            later.build_reports().path(build_id),
            Some(VectorBuildPath::Serial(
                VectorSerialBuildReason::ResumedSerialScan
            )),
            "{name}"
        );
        assert_eq!(reported_entities(&db, build_id).await, SOURCE, "{name}");
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// Fewer deaths than the limit leave the build on the bulk path: the process
/// after them stores the index an uninterrupted build stores, counted once.
#[tokio::test(flavor = "multi_thread")]
async fn attempts_below_the_limit_keep_the_bulk_path() {
    let limits = entity_limits(16);
    let uninterrupted = uninterrupted_rows("vector-bulk-below-limit-reference", &[]).await;
    for deaths in [1, BulkBuildAttempts::LIMIT - 1] {
        let db = test_db(&format!("vector-bulk-below-limit-{deaths}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut claim_sequence = 1;
        for death in [Death::Writing(3), Death::Reading]
            .into_iter()
            .take(deaths.into())
        {
            die(&db, build_id, &mut claim_sequence, death).await;
        }
        let last = bulk_driver(1, BUDGET, 16);
        assert_eq!(
            start_attempt(&db, &last, build_id, &mut claim_sequence)
                .await
                .attempts
                .get(),
            deaths + 1
        );
        assert_eq!(
            drive(&db, &last, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{deaths} deaths"
        );
        assert_eq!(
            last.build_reports().path(build_id),
            Some(VectorBuildPath::Bulk),
            "{deaths} deaths"
        );
        assert_eq!(reported_entities(&db, build_id).await, SOURCE);
        let index = active_index(&db, &definition).await;
        assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
        assert_index_holds(&db, &index, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// The count a process finds stored decides alone: below the limit the next
/// attempt is counted one higher, and at the limit, past it, or where the
/// stored byte can count no further, none is started.
#[tokio::test(flavor = "multi_thread")]
async fn the_stored_count_decides_whether_another_attempt_starts() {
    assert_eq!(BulkBuildAttempts::LIMIT, 3);
    let limits = entity_limits(16);
    for (stored, next) in [
        (1, Some(2)),
        (BulkBuildAttempts::LIMIT - 1, Some(BulkBuildAttempts::LIMIT)),
        (BulkBuildAttempts::LIMIT, None),
        (BulkBuildAttempts::LIMIT + 1, None),
        (u8::MAX, None),
    ] {
        let db = test_db(&format!("vector-bulk-stored-count-{stored}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        store_stage(
            &db,
            build_id,
            VectorBuildStage::BulkBuild(counted(&db, build_id, stored).await),
        )
        .await;
        let driver = bulk_driver(1, BUDGET, 16);
        let mut claim_sequence = 1;
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
        match next {
            Some(next) => {
                assert_eq!(
                    stored_scan(&db, build_id).await,
                    Some(StoredScan::Bulk(counted(&db, build_id, next).await)),
                    "{stored}"
                );
                assert_eq!(driver.build_reports().path(build_id), None, "{stored}");
            }
            None => {
                assert!(
                    matches!(
                        stored_scan(&db, build_id).await,
                        Some(StoredScan::Serial(progress))
                            if progress.cursor == Some(source_cursor(SCOPE, 15))
                    ),
                    "{stored}"
                );
                assert_eq!(
                    driver.build_reports().path(build_id),
                    Some(VectorBuildPath::Serial(
                        VectorSerialBuildReason::EarlierAttemptsFailed
                    )),
                    "{stored}"
                );
                assert_eq!(driver.bulk.memory.reserved(), 0);
            }
        }
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{stored}"
        );
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// A build that blocks leaves the bulk stage and its count with it, so its
/// retry is a first attempt however often it blocked: only attempts that
/// ended without the build leaving the stage are counted. Blocked four times
/// in its write pass, each time behind stored rows, the build deletes them,
/// blocks under the serial stage, and is built in bulk once the rows are
/// repaired. A process that dies while it deletes is one such attempt: the
/// next process counts a second, meets the row in its read pass, and blocks
/// at the serial stage too.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_that_blocks_leaves_the_bulk_stage_and_its_count() {
    for (name, dies_deleting) in [("same-process", false), ("dies-deleting", true)] {
        let db = test_db(&format!("vector-bulk-blocked-attempts-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let limits = entity_limits(16);
        let mut claim_sequence = 1;
        let mut running = bulk_driver(1, BUDGET, 16);
        let building = building_index(&db, &definition, build_id).await;
        for row in [100, 150, 200, 220] {
            // The row turns invalid once a new attempt's read pass is past
            // it and its write pass has stored a first step.
            while scan_progress(&db, build_id).await.unwrap().cursor
                != Some(source_cursor(SCOPE, 15))
            {
                drive_steps(&db, &running, build_id, &mut claim_sequence, limits, 1).await;
            }
            assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
            db.put(source_key(SCOPE, row), Bytes::from_static(b"malformed"))
                .await
                .unwrap();
            if dies_deleting {
                // The write pass reaches the row and the process deletes one
                // batch of what it stored, then dies.
                while applied_rows(&db, index_id, generation).await < row as usize {
                    drive_steps(&db, &running, build_id, &mut claim_sequence, limits, 1).await;
                }
                drive_steps(
                    &db,
                    &running,
                    build_id,
                    &mut claim_sequence,
                    operation_limits(8),
                    1,
                )
                .await;
                assert_eq!(
                    running.build_reports().phase(build_id),
                    Some(VectorBulkBuildPhase::Discarding { rows_deleted: 8 }),
                    "{name}"
                );
                assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
                running = bulk_driver(1, BUDGET, 16);
            }
            assert_eq!(
                drive(&db, &running, build_id, &mut claim_sequence, limits)
                    .await
                    .0,
                CommittedOperationStep::Blocked,
                "{name}"
            );
            assert!(
                matches!(
                    read_operation(&db, SCOPE, build_id).await.execution_state(),
                    IndexOperationExecutionState::Blocked(
                        IndexOperationBlocker::InvalidSourceData { entity_id, .. }
                    ) if *entity_id == IndexEntityId::new(row)
                ),
                "{name}"
            );
            // A blocked build keeps neither its stage nor its rows.
            assert_blocked_at_the_serial_stage(&db, build_id).await;
            assert!(building.stored_rows(&db).await.unwrap().is_empty());
            assert_eq!(applied_rows(&db, index_id, generation).await, 0, "{name}");
            assert!(nothing_held(&running), "{name}");
            assert_eq!(running.build_reports().phase(build_id), None, "{name}");
            put_source(&db, SCOPE, row, &properties(vector_of(row), None)).await;
            retry_operation(&db, SCOPE, build_id)
                .await
                .expect("the blocked build is retried");
        }
        assert_eq!(
            drive(&db, &running, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            running.build_reports().path(build_id),
            Some(VectorBuildPath::Bulk),
            "{name}"
        );
        assert!(nothing_held(&running), "{name}");
        assert_eq!(reported_entities(&db, build_id).await, SOURCE, "{name}");
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// A failure of a bulk attempt itself, which the outbox would record as an
/// invariant violation, never blocks the build: the step that fails reports
/// a transient failure, the next ones delete what the attempt stored, and
/// the serial scan builds the index at once, in the same process and without
/// another attempt, whether the read that sizes the build failed before an
/// attempt was stored (nothing is deleted then), the attempt's count could
/// not be uploaded, a run of its read failed (such a failure is not read
/// again in place), its passes failed, or its write pass failed or
/// panicked. A panic left to unwind would end the worker's cycle and cost
/// the build an attempt each time it repeated. An operator's retry of a
/// blocked build would have repeated the failure from attempt one.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_bulk_attempt_is_discarded_and_built_serially() {
    #[derive(Debug, Clone, Copy)]
    enum Failing {
        /// The read of the source's first rows, under the serial stage.
        Probe,
        /// The upload of the attempt's count.
        Upload,
        /// A run of pass 1.
        Read,
        /// Passes 1 and 2.
        Passes,
        /// The write pass, after this many of its steps.
        Writing(usize),
        /// A write step that panics, after this many steps of the pass.
        Panic(usize),
    }
    let limits = entity_limits(16);
    for failing in [
        Failing::Probe,
        Failing::Upload,
        Failing::Read,
        Failing::Passes,
        Failing::Writing(2),
        Failing::Panic(2),
    ] {
        let name = format!("{failing:?}");
        let db = test_db(&format!("vector-bulk-failed-{}", name.to_lowercase())).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let driver = bulk_driver(1, BUDGET, 16);
        let mut claim_sequence = 1;
        match failing {
            Failing::Probe => driver.bulk.faults.read_blocked.store(1, Ordering::Relaxed),
            Failing::Upload => {
                start_attempt(&db, &driver, build_id, &mut claim_sequence).await;
                driver.bulk.faults.flush.store(1, Ordering::Relaxed);
            }
            Failing::Read => {
                start_attempt(&db, &driver, build_id, &mut claim_sequence).await;
                driver.bulk.faults.read_blocked.store(1, Ordering::Relaxed);
            }
            Failing::Passes => {
                prepare_with(
                    &db,
                    &driver,
                    build_id,
                    &mut claim_sequence,
                    tokio::spawn(async {
                        Err(HelixDbError::InvariantViolation(
                            "the stand-in passes failed".to_string(),
                        ))
                    }),
                )
                .await;
            }
            // A write pass that stored rows finds its position past the
            // next source entity, which no step may store behind.
            Failing::Writing(steps) => {
                drive_steps(
                    &db,
                    &driver,
                    build_id,
                    &mut claim_sequence,
                    limits,
                    COUNT + steps,
                )
                .await;
                assert_eq!(applied_rows(&db, index_id, generation).await, 32, "{name}");
                let slot = Arc::clone(&driver.bulk.builds.lock()[&build_id]);
                let BuildState::Writing(writing) = &mut *slot.lock().await else {
                    panic!("{name}: the build is in its write pass");
                };
                writing.position = SOURCE as u32 - 1;
            }
            Failing::Panic(steps) => {
                drive_steps(
                    &db,
                    &driver,
                    build_id,
                    &mut claim_sequence,
                    limits,
                    COUNT + steps,
                )
                .await;
                assert_eq!(applied_rows(&db, index_id, generation).await, 32, "{name}");
                driver.bulk.faults.write_panics.store(1, Ordering::Relaxed);
            }
        }
        let (failed, _) = drive(&db, &driver, build_id, &mut claim_sequence, limits).await;
        assert_eq!(failed, CommittedOperationStep::TransientFailure, "{name}");
        assert_eq!(
            driver.bulk.faults.read_blocked.load(Ordering::Relaxed),
            0,
            "{name}"
        );
        assert_eq!(
            attempts(&db, build_id).await,
            match failing {
                Failing::Probe => None,
                Failing::Upload
                | Failing::Read
                | Failing::Passes
                | Failing::Writing(_)
                | Failing::Panic(_) => Some(1),
            },
            "{name}"
        );
        assert!(
            matches!(
                read_operation(&db, SCOPE, build_id).await.execution_state(),
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: Some(_)
                }
            ),
            "{name}: the failing step is retried, not blocked"
        );
        assert_eq!(
            retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed,
            "{name}"
        );
        // The serial scan finishes the build: it is never blocked, and no
        // other attempt starts.
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            driver.build_reports().path(build_id),
            Some(VectorBuildPath::Serial(
                VectorSerialBuildReason::BulkAttemptFailed
            )),
            "{name}"
        );
        assert!(nothing_held(&driver), "{name}");
        assert_eq!(reported_entities(&db, build_id).await, SOURCE, "{name}");
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// A step that fails while it deletes what an attempt stored, with an error
/// the outbox retries, leaves the build deleting, with what it does once the
/// rows are gone: the step's retry goes on, then the build scans serially,
/// or blocks, as it had decided. Starting over from the stored stage would
/// forget the decision and spend another bulk attempt first.
#[tokio::test(flavor = "multi_thread")]
async fn a_step_that_fails_while_it_deletes_goes_on_deleting() {
    let limits = entity_limits(16);
    for (name, blocks) in [("serial", false), ("block", true)] {
        let db = test_db(&format!("vector-bulk-failed-discard-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let driver = bulk_driver(1, BUDGET, 16);
        let mut claim_sequence = 1;
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            limits,
            COUNT + 2,
        )
        .await;
        assert_eq!(applied_rows(&db, index_id, generation).await, 32, "{name}");
        match blocks {
            // The write pass meets a row no step can read.
            true => {
                db.put(source_key(SCOPE, 100), Bytes::from_static(b"malformed"))
                    .await
                    .expect("invalid vector source is written");
            }
            // The write pass fails where a serial scan would not.
            false => {
                let slot = Arc::clone(&driver.bulk.builds.lock()[&build_id]);
                let BuildState::Writing(writing) = &mut *slot.lock().await else {
                    panic!("{name}: the build is in its write pass");
                };
                writing.position = SOURCE as u32 - 1;
            }
        }
        driver.bulk.faults.discard.store(1, Ordering::Relaxed);
        // Step by step to the end: no step starts another attempt. A step
        // that failed is offered again once its delay has passed.
        let (mut delayed, mut failures) = (false, 0);
        let ended = loop {
            let stepped = match delayed {
                true => {
                    retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await
                }
                false => step(&db, &driver, build_id, &mut claim_sequence, limits).await,
            };
            assert_ne!(attempts(&db, build_id).await, Some(2), "{name}");
            delayed = stepped == CommittedOperationStep::TransientFailure;
            failures += usize::from(delayed);
            match stepped {
                CommittedOperationStep::Progressed | CommittedOperationStep::TransientFailure => {}
                ended @ (CommittedOperationStep::Blocked | CommittedOperationStep::Completed) => {
                    break ended;
                }
            }
        };
        assert_eq!(
            driver.bulk.faults.discard.load(Ordering::Relaxed),
            0,
            "{name}"
        );
        // The step that fell back, where one did, and the one that failed
        // while it deleted.
        assert_eq!(failures, if blocks { 1 } else { 2 }, "{name}");
        match blocks {
            true => {
                assert_eq!(ended, CommittedOperationStep::Blocked, "{name}");
                assert_blocked_at_the_serial_stage(&db, build_id).await;
            }
            false => {
                assert_eq!(ended, CommittedOperationStep::Completed, "{name}");
                assert_eq!(
                    driver.build_reports().path(build_id),
                    Some(VectorBuildPath::Serial(
                        VectorSerialBuildReason::BulkAttemptFailed
                    )),
                    "{name}"
                );
                assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[])
                    .await;
            }
        }
        assert!(nothing_held(&driver), "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// A step's unlinks are bounded whatever they are for. Once they have taken
/// the step's unlink time the step ends, after its first unlink at the
/// least, and the next step goes on behind it; and a step makes no more of
/// them than the entities it may read, whether their entities are gone or
/// hold other vectors: a step that has unlinked that many nodes of deleted
/// entities unlinks none of a changed row.
#[tokio::test(flavor = "multi_thread")]
async fn a_steps_unlinks_end_with_their_time_and_their_count() {
    let limits = entity_limits(16);
    for (name, step_time, deleted, changed, after_one_step) in [
        // No time at all: one unlink a step, of the first changed row.
        ("time", Some(std::time::Duration::ZERO), 0..0, 16..32, 16),
        // Less time than one unlink takes: the first one spends it.
        (
            "time-spent",
            Some(std::time::Duration::from_nanos(1)),
            0..0,
            16..32,
            16,
        ),
        // Sixteen nodes of deleted entities fill the step's count: the
        // changed rows behind them wait for the next step. No time bounds
        // the step's unlinks, as in every other contract of these drivers.
        ("count", None, 16..32, 32..48, 15),
    ] {
        let db = test_db(&format!("vector-bulk-unlink-bound-{name}")).await;
        let definition = definition(None);
        let mut expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut driver = bulk_driver(2, BUDGET, 16);
        driver.bulk.unlink.step_time = step_time.unwrap_or(driver.bulk.unlink.step_time);
        let mut claim_sequence = 1;
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            limits,
            COUNT + 1,
        )
        .await;
        assert_eq!(
            scan_progress(&db, build_id).await.unwrap().cursor,
            Some(source_cursor(SCOPE, 15)),
            "{name}"
        );
        delete_sources(&db, deleted.clone()).await;
        for entity_id in changed.clone() {
            put_source(
                &db,
                SCOPE,
                entity_id,
                &properties(vector_of(1_000 + entity_id), None),
            )
            .await;
        }
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
        assert_eq!(
            scan_progress(&db, build_id).await.unwrap().cursor,
            Some(source_cursor(SCOPE, after_one_step)),
            "{name}"
        );
        {
            let slot = Arc::clone(&driver.bulk.builds.lock()[&build_id]);
            let BuildState::Writing(writing) = &*slot.lock().await else {
                panic!("{name}: the build is in its write pass");
            };
            assert_eq!(
                writing.unlinks.nodes,
                if deleted.is_empty() { 1 } else { 16 },
                "{name}"
            );
        }
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            driver.build_reports().path(build_id),
            Some(VectorBuildPath::Bulk),
            "{name}"
        );
        let absent = deleted.chain(changed).collect::<Vec<u64>>();
        expected.retain(|entity_id, _| !absent.contains(entity_id));
        assert_index_holds(
            &db,
            &active_index(&db, &definition).await,
            &expected,
            &absent,
        )
        .await;
        assert!(nothing_held(&driver), "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// A write pass gives its attempt up once it has made enough unlinks to
/// judge and the nodes it has not passed would hold, at the share it
/// unlinked so far, more unlinks than an eighth of the graph: not at an
/// eighth exactly, not before it has made that many unlinks, and never with
/// less than an eighth of its nodes left, however many it unlinked.
#[test]
fn a_write_pass_gives_up_for_the_unlinks_still_to_come() {
    // 143 of the first 1,000 of 8,000 nodes project 1,001 unlinks in the
    // 7,000 left, one more than an eighth of the graph; 142 project 994.
    assert!(gives_up(8_000, 1_000, 143, 1));
    assert!(!gives_up(8_000, 1_000, 142, 1));
    // An eighth exactly is not more than an eighth.
    assert!(!gives_up(8, 4, 1, 1));
    assert!(gives_up(8, 4, 2, 1));
    // Not before the pass has made the unlinks it judges by.
    assert!(!gives_up(8_000, 1_000, 1_000, 1_024));
    assert!(gives_up(8_000, 1_024, 1_024, 1_024));
    // A pass that unlinked every node it passed finishes once fewer than an
    // eighth are left.
    assert!(gives_up(8_000, 6_999, 6_999, 1));
    assert!(!gives_up(8_000, 7_001, 7_001, 1));
    assert!(!gives_up(8_000, 8_000, 8_000, 1));
    // The largest graph does not overflow the judgement.
    assert!(gives_up(u32::MAX, u32::MAX / 2, u32::MAX / 2, 1));
}

/// A source of which too many rows changed after the read pass is not
/// finished in bulk. Once the write pass has made the unlinks it judges by,
/// and at that share the rest of the graph would cost more unlinks than an
/// eighth of it, the build deletes what the attempt stored and scans
/// serially at once, with a reason of its own, no failure and no other
/// attempt: the index holds every row as it is now. A source that changed
/// at a share under an eighth is finished in bulk, without the changed rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_that_changed_too_much_is_built_serially() {
    let limits = entity_limits(16);
    for (name, every, serial) in [("over", 2, true), ("under", 9, false)] {
        let db = test_db(&format!("vector-bulk-changed-{name}")).await;
        let definition = definition(None);
        let mut expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut driver = bulk_driver(2, BUDGET, 16);
        driver.bulk.unlink.judged_after = 4;
        let mut claim_sequence = 1;
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            limits,
            COUNT + 1,
        )
        .await;
        let changed = (16..SOURCE)
            .filter(|entity_id| entity_id % every == 0)
            .collect::<Vec<u64>>();
        for entity_id in &changed {
            put_source(
                &db,
                SCOPE,
                *entity_id,
                &properties(vector_of(1_000 + entity_id), None),
            )
            .await;
        }
        // No step fails on the way: giving up is no failure.
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(reported_entities(&db, build_id).await, SOURCE, "{name}");
        assert!(nothing_held(&driver), "{name}");
        let index = active_index(&db, &definition).await;
        match serial {
            true => {
                assert_eq!(
                    driver.build_reports().path(build_id),
                    Some(VectorBuildPath::Serial(
                        VectorSerialBuildReason::TooManyRowsChanged
                    )),
                    "{name}"
                );
                // The serial scan reads the rows as they are now.
                changed.iter().for_each(|entity_id| {
                    expected.insert(*entity_id, vector_of(1_000 + entity_id));
                });
                assert_index_holds(&db, &index, &expected, &[]).await;
            }
            false => {
                assert_eq!(
                    driver.build_reports().path(build_id),
                    Some(VectorBuildPath::Bulk),
                    "{name}"
                );
                // The changed rows are left to their queued operations.
                expected.retain(|entity_id, _| !changed.contains(entity_id));
                assert_index_holds(&db, &index, &expected, &changed).await;
            }
        }
        db.close().await.expect("vector test database closes");
    }
}

/// The read that sizes a build before any attempt runs on a lifecycle step,
/// which holds the operation's claim and its lane: a run of it that fails
/// with an error the outbox retries is not read again in place, with a wait,
/// as a run of pass 1 is. The step fails, and the next one reads again.
#[tokio::test(flavor = "multi_thread")]
async fn the_read_that_sizes_a_build_is_not_read_again_in_place() {
    let limits = entity_limits(16);
    let db = test_db("vector-bulk-probe-fault").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(1, BUDGET, 16);
    let mut claim_sequence = 1;
    // As many failures as one run of pass 1 would read through.
    driver
        .bulk
        .faults
        .read
        .store(READ_RUN_RETRIES as usize, Ordering::Relaxed);
    for left in (0..READ_RUN_RETRIES as usize).rev() {
        let failed = match left + 1 == READ_RUN_RETRIES as usize {
            true => {
                drive(&db, &driver, build_id, &mut claim_sequence, limits)
                    .await
                    .0
            }
            false => {
                retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await
            }
        };
        assert_eq!(failed, CommittedOperationStep::TransientFailure);
        assert_eq!(driver.bulk.faults.read.load(Ordering::Relaxed), left);
        assert_eq!(attempts(&db, build_id).await, None);
    }
    assert_eq!(
        retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A row that blocks a build is read again before the block is stored, by
/// the step that collects the read pass and by the step that blocks. A read
/// that fails for now, with an error the outbox retries, leaves the block
/// pending: the build goes on to block, on the step's retry where the
/// blocking step's own read failed, and reads the source for no other
/// attempt.
#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_row_that_cannot_be_read_for_now_still_blocks() {
    let limits = SearchIndexBackfillLimits::default().batch();
    for (name, faults) in [("collecting", 1), ("blocking", 2)] {
        let db = test_db(&format!("vector-bulk-blocker-read-{name}")).await;
        let definition = definition(None);
        seed(
            &db,
            (0..60).map(|entity_id| (entity_id, vector_of(entity_id))),
        )
        .await;
        db.put(source_key(SCOPE, 5), Bytes::from_static(b"malformed"))
            .await
            .expect("unreadable source is written");
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, 59).await;
        let driver = bulk_driver(2, BUDGET, 16);
        let mut claim_sequence = 1;
        driver.bulk.faults.row_read.store(faults, Ordering::Relaxed);
        let ended = drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0;
        let blocked = match faults {
            // The step that collected the pass could not read the row, and
            // the blocking step did.
            1 => ended,
            // The blocking step could not either: its retry does.
            _ => {
                assert_eq!(ended, CommittedOperationStep::TransientFailure, "{name}");
                assert_eq!(attempts(&db, build_id).await, None, "{name}");
                retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await
            }
        };
        assert_eq!(blocked, CommittedOperationStep::Blocked, "{name}");
        assert_eq!(
            driver.bulk.faults.row_read.load(Ordering::Relaxed),
            0,
            "{name}"
        );
        let operation = read_operation(&db, SCOPE, build_id).await;
        assert!(
            matches!(
                operation.execution_state(),
                IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
                    entity_id,
                    ..
                }) if *entity_id == IndexEntityId::new(5)
            ),
            "{name}"
        );
        assert_blocked_at_the_serial_stage(&db, build_id).await;
        assert!(nothing_held(&driver), "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// An upload of an attempt's count that fails with an error the outbox
/// retries leaves the attempt as it is, with its turn and its memory: the
/// step is offered again and the attempt starts then, counted once.
#[tokio::test(flavor = "multi_thread")]
async fn a_retried_upload_of_the_count_keeps_the_attempt() {
    let limits = entity_limits(16);
    let db = test_db("vector-bulk-retried-count-upload").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(1, BUDGET, 16);
    let mut claim_sequence = 1;
    start_attempt(&db, &driver, build_id, &mut claim_sequence).await;
    let held = driver.bulk.memory.reserved();
    assert!(held > 0);
    driver.bulk.faults.flush_retried.store(1, Ordering::Relaxed);
    let (failed, _) = drive(&db, &driver, build_id, &mut claim_sequence, limits).await;
    assert_eq!(failed, CommittedOperationStep::TransientFailure);
    {
        let slot = Arc::clone(&driver.bulk.builds.lock()[&build_id]);
        assert!(matches!(&*slot.lock().await, BuildState::Starting(_)));
    }
    assert_eq!(driver.bulk.memory.reserved(), held);
    assert_eq!(driver.bulk.preparing.available_permits(), 0);
    assert_eq!(
        retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    assert_eq!(attempts(&db, build_id).await, Some(1));
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// Pass 1 reads its runs at different moments, each under a snapshot of its
/// own, and holds none between them. Rows inserted, updated and deleted
/// between two runs leave the index as the serial scan's guarantee has it: a
/// node is stored only for a row that still holds the vector pass 1 read,
/// and every other change is left to the operation the write queued. A row
/// updated before its run is read as it is then, which a snapshot held from
/// the first run on would not show.
#[tokio::test(flavor = "multi_thread")]
async fn rows_changed_between_two_read_runs_are_stored_only_as_read() {
    const RUN: u64 = 60;
    let limits = entity_limits(16);
    let db = test_db("vector-bulk-changes-between-runs").await;
    let definition = definition(None);
    // Even ids, so that rows can be inserted among them.
    let mut expected = seed(
        &db,
        (0..SOURCE).map(|member| (member * 2, vector_of(member * 2))),
    )
    .await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, (SOURCE - 1) * 2).await;
    let mut driver = bulk_driver(1, BUDGET, 16);
    driver.bulk.read_run = ReadRun {
        ids: RUN as usize,
        bytes: READ_RUN_BYTES,
        retry_delay: READ_RUN_RETRY_DELAY,
    };
    let mut claim_sequence = 1;
    start_attempt(&db, &driver, build_id, &mut claim_sequence).await;
    // One run is let through: the ids below `2 * RUN`.
    let gate = Arc::new(tokio::sync::Semaphore::new(1));
    *driver.bulk.faults.read_gate.lock() = Some(Arc::clone(&gate));
    assert_eq!(
        drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
        CommittedOperationStep::Progressed
    );
    tokio::time::timeout(WOKEN_WITHIN, async {
        while driver.build_reports().phase(build_id)
            != Some(VectorBulkBuildPhase::Reading {
                members_read: RUN,
                members: SOURCE,
            })
        {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the first run is read");

    // In the run already read: an update, a delete and an insert.
    let (read, unread) = (0, 2 * RUN);
    put_sources(
        &db,
        [
            (read, properties(vector_of(10_000), None)),
            (read + 3, properties(vector_of(10_003), None)),
            // In the runs still to read: the same, and the update is what
            // the pass reads.
            (unread, properties(vector_of(20_000), None)),
            (unread + 3, properties(vector_of(20_003), None)),
        ],
    )
    .await;
    delete_sources(&db, [read + 2, unread + 2]).await;
    let absent = [read, read + 2, read + 3, unread + 2, unread + 3];
    absent.iter().for_each(|entity_id| {
        expected.remove(entity_id);
    });
    expected.insert(unread, vector_of(20_000));

    gate.add_permits(1_000);
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert_index_holds(
        &db,
        &active_index(&db, &definition).await,
        &expected,
        &absent,
    )
    .await;
    db.close().await.expect("vector test database closes");
}

/// A run of pass 1 that fails with an error the outbox would retry is read
/// again in place: failures up to the retries of one run cost no attempt,
/// and one more ends the attempt, which the next one repeats.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_read_run_is_read_again_before_it_costs_an_attempt() {
    let limits = entity_limits(16);
    for (name, faults, attempts_started) in [
        ("retried", READ_RUN_RETRIES as usize, 1),
        ("exhausted", READ_RUN_RETRIES as usize + 1, 2),
    ] {
        let db = test_db(&format!("vector-bulk-read-faults-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut driver = bulk_driver(1, BUDGET, 16);
        driver.bulk.read_run.retry_delay = std::time::Duration::from_millis(1);
        let mut claim_sequence = 1;
        start_attempt(&db, &driver, build_id, &mut claim_sequence).await;
        driver.bulk.faults.read.store(faults, Ordering::Relaxed);
        let (ended, _) = drive(&db, &driver, build_id, &mut claim_sequence, limits).await;
        assert_eq!(driver.bulk.faults.read.load(Ordering::Relaxed), 0, "{name}");
        match attempts_started {
            1 => assert_eq!(ended, CommittedOperationStep::Completed, "{name}"),
            _ => {
                // The run failed once more than it is retried: the attempt
                // is over, and the next one reads the source again.
                assert_eq!(ended, CommittedOperationStep::TransientFailure, "{name}");
                assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
                assert_eq!(driver.bulk.memory.reserved(), 0, "{name}");
                assert_eq!(driver.bulk.preparing.available_permits(), 1, "{name}");
                assert_eq!(
                    retry_conflicted_step(&db, &driver, build_id, &mut claim_sequence, limits)
                        .await,
                    CommittedOperationStep::Progressed,
                    "{name}"
                );
                assert_eq!(attempts(&db, build_id).await, Some(2), "{name}");
                assert_eq!(
                    drive(&db, &driver, build_id, &mut claim_sequence, limits)
                        .await
                        .0,
                    CommittedOperationStep::Completed,
                    "{name}"
                );
            }
        }
        assert_eq!(
            driver.build_reports().path(build_id),
            Some(VectorBuildPath::Bulk),
            "{name}"
        );
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// A build leaves the bulk stage for the serial scan by the serial scan's
/// own first commit. When that commit is lost the bulk stage is still
/// stored, with nothing under it, and the same process or the next takes the
/// build off it again. The serial scan starts at the source's first row and
/// counts from zero, whatever cursor and counters the last attempt left.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_first_serial_step_leaves_the_bulk_stage_stored() {
    let limits = entity_limits(16);
    for (name, restart) in [("same-process", false), ("restarted", true)] {
        let db = test_db(&format!("vector-bulk-lost-fallback-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        // The last attempt's write pass passed rows and stored nothing.
        let left = BulkBuildProgress {
            scan: SourceScanProgress {
                cursor: Some(source_cursor(SCOPE, 47)),
                counters: OperationCounters {
                    entities: 48,
                    input_bytes: 4_800,
                    ..OperationCounters::default()
                },
                ..scan_progress(&db, build_id).await.unwrap()
            },
            attempts: BulkBuildAttempts::new(BulkBuildAttempts::LIMIT).unwrap(),
        };
        store_stage(&db, build_id, VectorBuildStage::BulkBuild(left.clone())).await;
        let mut claim_sequence = 1;
        let mut running = bulk_driver(1, BUDGET, 16);
        lose_commit(
            &db,
            &running,
            &definition,
            build_id,
            &mut claim_sequence,
            limits,
        )
        .await;
        assert_eq!(
            stored_scan(&db, build_id).await,
            Some(StoredScan::Bulk(left)),
            "{name}"
        );
        assert_eq!(applied_rows(&db, index_id, generation).await, 0, "{name}");
        assert!(building_index(&db, &definition, build_id)
            .await
            .stored_rows(&db)
            .await
            .unwrap()
            .is_empty());
        if restart {
            running = bulk_driver(1, BUDGET, 16);
        }
        assert_eq!(
            retry_conflicted_step(&db, &running, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed,
            "{name}"
        );
        let Some(StoredScan::Serial(serial)) = stored_scan(&db, build_id).await else {
            panic!("{name}: the serial scan's first commit stores the serial stage");
        };
        assert_eq!(serial.cursor, Some(source_cursor(SCOPE, 15)), "{name}");
        assert_eq!(serial.counters.entities, 16, "{name}");
        assert_eq!(
            running.build_reports().path(build_id),
            Some(VectorBuildPath::Serial(
                VectorSerialBuildReason::EarlierAttemptsFailed
            )),
            "{name}"
        );
        assert_eq!(
            drive(&db, &running, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(reported_entities(&db, build_id).await, SOURCE, "{name}");
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// A build that leaves the bulk path for the serial scan stores the serial
/// stage with that scan's first rows. Where the scan's first step blocks
/// instead, on its first row, the build stores the serial stage without a
/// cursor and blocks in the step after, under the stage every binary reads.
/// Its retry starts from there: with a first bulk attempt.
#[tokio::test(flavor = "multi_thread")]
async fn a_fallback_blocked_on_its_first_row_leaves_the_bulk_stage() {
    let db = test_db("vector-bulk-blocked-fallback").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    db.put(source_key(SCOPE, 0), Bytes::from_static(b"malformed"))
        .await
        .unwrap();
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let exhausted = counted(&db, build_id, BulkBuildAttempts::LIMIT).await;
    store_stage(
        &db,
        build_id,
        VectorBuildStage::BulkBuild(exhausted.clone()),
    )
    .await;
    let driver = bulk_driver(1, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits).await,
        (CommittedOperationStep::Blocked, 1)
    );
    assert_blocked_at_the_serial_stage(&db, build_id).await;
    assert!(matches!(
        read_operation(&db, SCOPE, build_id).await.execution_state(),
        IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvalidSourceData {
            entity_id,
            ..
        }) if *entity_id == IndexEntityId::new(0)
    ));
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Serial(
            VectorSerialBuildReason::EarlierAttemptsFailed
        ))
    );

    put_source(&db, SCOPE, 0, &properties(vector_of(0), None)).await;
    retry_operation(&db, SCOPE, build_id)
        .await
        .expect("the blocked build is retried");
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, COUNT).await;
    assert_eq!(attempts(&db, build_id).await, Some(1));
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// The way back for an operator. A process with bulk builds off that finds a
/// build at the bulk stage deletes what is stored under that stage, then
/// stores the serial stage without a cursor before it scans a row, whatever
/// the attempt was doing and however many were counted. A step that loses
/// that commit repeats it. The build then goes on serially.
#[tokio::test(flavor = "multi_thread")]
async fn turning_bulk_builds_off_returns_a_build_to_its_serial_stage() {
    let limits = entity_limits(16);
    for (name, deaths) in [
        ("counted", vec![Death::Counted]),
        ("reading", vec![Death::Reading]),
        ("writing", vec![Death::Writing(3)]),
        (
            "exhausted",
            vec![Death::Writing(1), Death::Reading, Death::Writing(3)],
        ),
    ] {
        let db = test_db(&format!("vector-bulk-turned-off-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let created = scan_progress(&db, build_id).await.unwrap();
        let mut claim_sequence = 1;
        let mut counted = 0;
        for death in deaths {
            counted = die(&db, build_id, &mut claim_sequence, death).await;
        }
        let building = building_index(&db, &definition, build_id).await;

        let off = driver();
        // Deleting steps keep the bulk stage for as long as a row is left.
        while !building.stored_rows(&db).await.unwrap().is_empty()
            || applied_rows(&db, index_id, generation).await > 0
        {
            assert_eq!(attempts(&db, build_id).await, Some(counted), "{name}");
            drive_steps(&db, &off, build_id, &mut claim_sequence, limits, 1).await;
        }
        assert_eq!(attempts(&db, build_id).await, Some(counted), "{name}");
        lose_commit(
            &db,
            &off,
            &definition,
            build_id,
            &mut claim_sequence,
            limits,
        )
        .await;
        assert_eq!(attempts(&db, build_id).await, Some(counted), "{name}");
        assert_eq!(
            retry_conflicted_step(&db, &off, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed,
            "{name}"
        );
        // The serial stage, as a new build stores it, and no row yet.
        assert_eq!(
            stored_scan(&db, build_id).await,
            Some(StoredScan::Serial(created.clone())),
            "{name}"
        );
        assert!(building.stored_rows(&db).await.unwrap().is_empty());
        assert_eq!(applied_rows(&db, index_id, generation).await, 0, "{name}");

        drive_steps(&db, &off, build_id, &mut claim_sequence, limits, 1).await;
        assert_eq!(
            off.build_reports().path(build_id),
            Some(VectorBuildPath::Serial(
                VectorSerialBuildReason::BulkBuildsDisabled
            )),
            "{name}"
        );
        assert_eq!(
            scan_progress(&db, build_id).await.unwrap().cursor,
            Some(source_cursor(SCOPE, 15)),
            "{name}"
        );
        assert_eq!(
            drive(&db, &off, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(reported_entities(&db, build_id).await, SOURCE, "{name}");
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// A write pass that passed source rows without a vector leaves a cursor and
/// counters over nothing stored. Whatever follows starts at the source's
/// first row and counts from zero: the next attempt, and with bulk builds
/// off the serial stage.
#[tokio::test(flavor = "multi_thread")]
async fn a_cursor_left_over_nothing_stored_is_dropped() {
    let limits = entity_limits(16);
    for (name, driver, bulk) in [
        ("next-attempt", bulk_driver(1, BUDGET, 16), true),
        ("turned-off", driver(), false),
    ] {
        let db = test_db(&format!("vector-bulk-leftover-cursor-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let created = scan_progress(&db, build_id).await.unwrap();
        store_stage(
            &db,
            build_id,
            VectorBuildStage::BulkBuild(BulkBuildProgress {
                scan: SourceScanProgress {
                    cursor: Some(source_cursor(SCOPE, 47)),
                    counters: OperationCounters {
                        entities: 48,
                        input_bytes: 4_800,
                        ..OperationCounters::default()
                    },
                    ..created.clone()
                },
                attempts: BulkBuildAttempts::FIRST,
            }),
        )
        .await;
        let mut claim_sequence = 1;
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
        assert_eq!(
            stored_scan(&db, build_id).await,
            Some(if bulk {
                StoredScan::Bulk(BulkBuildProgress {
                    scan: created,
                    attempts: BulkBuildAttempts::new(2).unwrap(),
                })
            } else {
                StoredScan::Serial(created)
            }),
            "{name}"
        );
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(reported_entities(&db, build_id).await, SOURCE, "{name}");
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// A build returned to its serial stage with bulk builds off is a build no
/// process has chosen a path for: a process with bulk builds on again counts
/// a first attempt, also for a build that had used every attempt.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_returned_to_its_serial_stage_may_be_built_in_bulk_again() {
    let limits = entity_limits(16);
    let uninterrupted = uninterrupted_rows("vector-bulk-turned-on-again-reference", &[]).await;
    let db = test_db("vector-bulk-turned-on-again").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let mut claim_sequence = 1;
    for _ in 0..BulkBuildAttempts::LIMIT {
        die(&db, build_id, &mut claim_sequence, Death::Writing(2)).await;
    }
    let off = driver();
    while attempts(&db, build_id).await.is_some() {
        drive_steps(&db, &off, build_id, &mut claim_sequence, limits, 1).await;
    }
    drop(off);

    let on = bulk_driver(1, BUDGET, 16);
    assert_eq!(
        start_attempt(&db, &on, build_id, &mut claim_sequence)
            .await
            .attempts,
        BulkBuildAttempts::FIRST
    );
    assert_eq!(
        drive(&db, &on, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        on.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    let index = active_index(&db, &definition).await;
    assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
    assert_index_holds(&db, &index, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A record blocked at the bulk stage, as a build whose stored rows could
/// not be deleted leaves it, runs no step. Once it is retried a process with
/// bulk builds off deletes its partly written graph and stores the serial
/// stage, and the serial scan blocks at the row that is still invalid under
/// the stage every binary reads.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_blocked_at_the_bulk_stage_returns_to_its_serial_stage_once_retried() {
    let db = test_db("vector-bulk-turned-off-blocked").await;
    let definition = definition(None);
    seed_standard(&db).await;
    let (build_id, index_id, generation) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    let on = bulk_driver(1, BUDGET, 16);
    drive_steps(&db, &on, build_id, &mut claim_sequence, limits, COUNT + 2).await;
    drop(on);
    // The record as the blocked step of such a build commits it.
    let stored = read_operation(&db, SCOPE, build_id).await;
    let blocked = IndexOperationRecord::try_new(
        stored.operation_id(),
        stored.index_id(),
        stored.identity().clone(),
        stored.generation(),
        stored.index_record_revision(),
        stored.operation_revision(),
        stored.kind(),
        stored.family(),
        stored.progress().clone(),
        stored.attempt(),
        IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvariantViolation),
    )
    .expect("the blocked record validates");
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            crate::index_lifecycle::outbox::scoped_operation_key(SCOPE, build_id),
            crate::encoding::v2::values::encode_operation_record(&blocked),
        )
        .unwrap();
    transaction
        .delete(crate::index_lifecycle::outbox::global_operation_key(
            build_id,
        ))
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(attempts(&db, build_id).await, Some(1));
    assert_eq!(applied_rows(&db, index_id, generation).await, 32);
    db.put(source_key(SCOPE, 100), Bytes::from_static(b"malformed"))
        .await
        .unwrap();

    retry_operation(&db, SCOPE, build_id)
        .await
        .expect("the blocked build is retried");
    let off = driver();
    assert_eq!(
        drive(&db, &off, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Blocked
    );
    assert!(matches!(
        stored_scan(&db, build_id).await,
        Some(StoredScan::Serial(progress)) if progress.cursor == Some(source_cursor(SCOPE, 99))
    ));
    db.close().await.expect("vector test database closes");
}

/// An abort takes the operation off the bulk stage in the commit that
/// accepts it, wherever the attempt is, and cleanup removes whatever the
/// stage had stored and whatever the process held.
#[tokio::test(flavor = "multi_thread")]
async fn an_abort_leaves_no_bulk_stage_wherever_the_attempt_is() {
    let limits = entity_limits(16);
    for (name, death, same_process) in [
        ("counted", Death::Counted, true),
        ("reading", Death::Reading, true),
        ("writing-then-restarted", Death::Writing(3), false),
        ("exhausted", Death::Writing(2), false),
    ] {
        let db = test_db(&format!("vector-bulk-abort-{name}")).await;
        let definition = definition(None);
        seed_standard(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut claim_sequence = 1;
        let driver = bulk_driver(1, BUDGET, 16);
        match (death, same_process) {
            (Death::Counted, _) => {
                start_attempt(&db, &driver, build_id, &mut claim_sequence).await;
            }
            (Death::Reading, _) => {
                prepare_with(
                    &db,
                    &driver,
                    build_id,
                    &mut claim_sequence,
                    tokio::spawn(std::future::pending::<Result<Prepared>>()),
                )
                .await;
            }
            (Death::Writing(steps), _) => {
                let dying = if name == "exhausted" {
                    BulkBuildAttempts::LIMIT
                } else {
                    1
                };
                for _ in 0..dying {
                    die(&db, build_id, &mut claim_sequence, Death::Writing(steps)).await;
                }
                // The next process is part-way through deleting the rows, or
                // has decided to scan serially and not committed a step.
                drive_steps(
                    &db,
                    &driver,
                    build_id,
                    &mut claim_sequence,
                    operation_limits(8),
                    1,
                )
                .await;
            }
        }
        assert!(attempts(&db, build_id).await.is_some(), "{name}");
        let building = building_index(&db, &definition, build_id).await;

        drop_index_operation(&db, SCOPE, &definition)
            .await
            .expect("building vector converts to abort cleanup");
        assert!(
            matches!(
                read_operation(&db, SCOPE, build_id).await.progress(),
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
            ),
            "{name}"
        );
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert!(
            matches!(
                read_operation(&db, SCOPE, build_id).await.progress(),
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Aborting(_))
            ),
            "{name}"
        );
        assert!(nothing_held(&driver), "{name}");
        assert_eq!(driver.bulk.preparing.available_permits(), 1, "{name}");
        assert!(building.stored_rows(&db).await.unwrap().is_empty());
        assert_eq!(applied_rows(&db, index_id, generation).await, 0, "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// The last step of the write pass leaves the bulk stage in the commit that
/// stores the metadata row. When that commit is lost the bulk stage is
/// still stored over a graph without metadata: the process that holds the
/// graph repeats the step, and another deletes the rows and starts again.
#[tokio::test(flavor = "multi_thread")]
async fn a_lost_last_step_leaves_the_bulk_stage_stored() {
    let limits = entity_limits(16);
    let uninterrupted = uninterrupted_rows("vector-bulk-lost-last-step-reference", &[]).await;
    for (name, restart) in [("same-process", false), ("restarted", true)] {
        let db = test_db(&format!("vector-bulk-lost-last-step-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut claim_sequence = 1;
        let mut running = bulk_driver(1, BUDGET, 16);
        // All but the last sixteen rows are stored; the next step stores
        // those and the metadata.
        while scan_progress(&db, build_id).await.unwrap().cursor
            != Some(source_cursor(SCOPE, SOURCE - 17))
        {
            drive_steps(&db, &running, build_id, &mut claim_sequence, limits, 1).await;
        }
        let building = building_index(&db, &definition, build_id).await;
        let stored = stored_scan(&db, build_id).await;
        lose_commit(
            &db,
            &running,
            &definition,
            build_id,
            &mut claim_sequence,
            limits,
        )
        .await;
        assert_eq!(stored_scan(&db, build_id).await, stored, "{name}");
        assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
        assert!(building.get_metadata(&db).await.unwrap().is_none());

        if restart {
            running = bulk_driver(1, BUDGET, 16);
        }
        assert_eq!(
            retry_conflicted_step(&db, &running, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed,
            "{name}"
        );
        if restart {
            // The new process holds no graph: it began to delete the rows.
            assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
            assert_eq!(scan_progress(&db, build_id).await.unwrap().cursor, None);
            assert!(building.get_metadata(&db).await.unwrap().is_none());
        } else {
            assert_eq!(stored_scan(&db, build_id).await, None, "{name}");
            assert_eq!(
                building.get_metadata(&db).await.unwrap().unwrap().count,
                SOURCE,
                "{name}"
            );
        }
        assert_eq!(
            drive(&db, &running, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            running.build_reports().path(build_id),
            Some(VectorBuildPath::Bulk),
            "{name}"
        );
        let index = active_index(&db, &definition).await;
        assert!(index.stored_rows(&db).await.unwrap() == uninterrupted);
        assert_index_holds(&db, &index, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// The bulk stage is stored only for an unpartitioned generation. Stored for
/// a partitioned one, the record is not what any process wrote: the step
/// stores nothing under it and the operation blocks as it is.
#[tokio::test(flavor = "multi_thread")]
async fn the_bulk_stage_of_a_partitioned_generation_is_refused() {
    let db = test_db("vector-bulk-partitioned-stage").await;
    let definition = definition(Some("account_id"));
    put_sources(
        &db,
        (0..60).map(|entity_id| (entity_id, properties(vector_of(entity_id), Some(7)))),
    )
    .await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, 59).await;
    let forged = counted(&db, build_id, 1).await;
    store_stage(&db, build_id, VectorBuildStage::BulkBuild(forged.clone())).await;
    let driver = bulk_driver(1, BUDGET, 16);
    assert_eq!(
        drive_one(&db, &driver, build_id, &mut 1, entity_limits(16)).await,
        CommittedOperationStep::Blocked
    );
    assert!(matches!(
        read_operation(&db, SCOPE, build_id).await.execution_state(),
        IndexOperationExecutionState::Blocked(IndexOperationBlocker::InvariantViolation)
    ));
    assert_eq!(
        stored_scan(&db, build_id).await,
        Some(StoredScan::Bulk(forged))
    );
    assert_eq!(driver.bulk.memory.reserved(), 0);
    assert_eq!(driver.bulk.preparing.available_permits(), 1);
    assert_eq!(driver.build_reports().path(build_id), None);
    db.close().await.expect("vector test database closes");
}

/// A source is sized by its label's members before a row of it is read,
/// however far apart their ids are. One whose members do not fit the budget
/// is scanned serially without a read pass, by a single byte; one with fewer
/// members than a bulk build is started for is too.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_is_sized_by_its_labels_members() {
    const MEMBERS: u64 = 60;
    for (name, budget, min_vectors, path) in [
        (
            "fits",
            paced_build_bytes(MEMBERS),
            16,
            VectorBuildPath::Bulk,
        ),
        (
            "over",
            paced_build_bytes(MEMBERS) - 1,
            16,
            VectorBuildPath::Serial(VectorSerialBuildReason::OverMemoryBudget),
        ),
        (
            "few",
            BUDGET,
            MEMBERS + 1,
            VectorBuildPath::Serial(VectorSerialBuildReason::TooFewVectors),
        ),
        // As many members as a bulk build is started for are enough.
        ("enough", BUDGET, MEMBERS, VectorBuildPath::Bulk),
    ] {
        let db = test_db(&format!("vector-bulk-bound-{name}")).await;
        let definition = definition(None);
        // The last member's id is a trillion past the others'.
        let last_id = 1_u64 << 40;
        let expected = seed(
            &db,
            (0..MEMBERS - 1)
                .chain([last_id])
                .map(|entity_id| (entity_id, vector_of(entity_id))),
        )
        .await;
        // Rows of another label between them are no members.
        put_sources(
            &db,
            (MEMBERS..4 * MEMBERS)
                .map(|entity_id| (entity_id, labelled("Note", vector_of(entity_id)))),
        )
        .await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, last_id).await;
        let driver = bulk_driver(2, budget, min_vectors);
        let mut claim_sequence = 1;
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            entity_limits(16),
            1,
        )
        .await;
        match path {
            VectorBuildPath::Bulk => {
                // The step counted the attempt, for exactly its members.
                assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
                assert_eq!(
                    driver.bulk.memory.reserved() - paced_build_bytes(0),
                    paced_build_bytes(MEMBERS) - paced_build_bytes(0)
                        + if budget == BUDGET {
                            2 * WRITE_BUFFER - 2 * WRITE_BUFFER_FOOTPRINT * WRITE_WINDOW_BYTES
                        } else {
                            0
                        },
                    "{name}"
                );
            }
            VectorBuildPath::Serial(_) => {
                assert_eq!(driver.build_reports().path(build_id), Some(path), "{name}");
                // Only the serial step's sixteen rows were read.
                let progress = scan_progress(&db, build_id).await.unwrap();
                assert_eq!(progress.cursor, Some(source_cursor(SCOPE, 15)), "{name}");
                assert_eq!(progress.counters.entities, 16, "{name}");
                assert_eq!(driver.bulk.memory.reserved(), 0, "{name}");
                assert_eq!(driver.bulk.preparing.available_permits(), 1, "{name}");
            }
        }
        assert_eq!(
            drive(
                &db,
                &driver,
                build_id,
                &mut claim_sequence,
                entity_limits(16)
            )
            .await
            .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(driver.build_reports().path(build_id), Some(path), "{name}");
        assert_eq!(reported_entities(&db, build_id).await, MEMBERS, "{name}");
        assert_index_holds(
            &db,
            &active_index(&db, &definition).await,
            &expected,
            &(MEMBERS..4 * MEMBERS).collect::<Vec<_>>(),
        )
        .await;
        db.close().await.expect("vector test database closes");
    }
}

/// A label most of whose members hold no vector reserves for every member
/// until the read pass has counted its vectors, and for those from then on.
#[tokio::test(flavor = "multi_thread")]
async fn a_reservation_is_trimmed_to_the_vectors_read() {
    const VECTORS: u64 = 60;
    const MEMBERS: u64 = 560;
    let db = test_db("vector-bulk-trimmed-reservation").await;
    let definition = definition(None);
    let expected = seed(
        &db,
        (0..VECTORS).map(|entity_id| (entity_id, vector_of(entity_id))),
    )
    .await;
    put_sources(
        &db,
        (VECTORS..MEMBERS).map(|entity_id| {
            (
                entity_id,
                vec![Property::new(
                    "$label",
                    PropertyValue::String("Document".to_string()),
                )],
            )
        }),
    )
    .await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, MEMBERS - 1).await;
    let driver = bulk_driver(2, BUDGET, 16);
    let mut claim_sequence = 1;
    let fixed = FIXED_ALLOWANCE_BYTES + 2 * WRITE_BUFFER;
    let per_member = (paced_build_bytes(MEMBERS) - paced_build_bytes(0)) / MEMBERS;
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        entity_limits(16),
        COUNT,
    )
    .await;
    assert_eq!(driver.bulk.memory.reserved() - fixed, MEMBERS * per_member);
    drive_steps(
        &db,
        &driver,
        build_id,
        &mut claim_sequence,
        entity_limits(16),
        1,
    )
    .await;
    assert_eq!(
        driver.build_reports().path(build_id),
        Some(VectorBuildPath::Bulk)
    );
    assert_eq!(driver.bulk.memory.reserved() - fixed, VECTORS * per_member);
    assert_eq!(
        drive(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            entity_limits(16)
        )
        .await
        .0,
        CommittedOperationStep::Completed
    );
    // The scan counts every member's row, as a serial scan does.
    assert_eq!(reported_entities(&db, build_id).await, MEMBERS);
    assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
    db.close().await.expect("vector test database closes");
}

/// A build that could reserve only two windows of the write buffer writes
/// only while the memtables hold less than two windows, whoever filled them:
/// rows another writer buffers make its next step write nothing, say so, and
/// start a flush, which wakes it. A build that reserved the whole buffer
/// writes regardless. Either stores the rows an undisturbed build stores.
#[tokio::test(flavor = "multi_thread")]
async fn a_paced_write_pass_writes_only_below_two_windows_of_memtables() {
    const WINDOW: u64 = 256 << 10;
    let limits = entity_limits(16);
    let uninterrupted = uninterrupted_rows("vector-bulk-windowed-reference", &[]).await;
    for (name, budget, write_ahead) in [
        ("unbounded", BUDGET, WriteAhead::Unbounded),
        ("windowed", ONE_WINDOWED_BUILD, WriteAhead::Windowed),
    ] {
        let db = test_db(&format!("vector-bulk-write-ahead-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, index_id, generation) =
            create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut driver = bulk_driver(1, budget, 16);
        driver.bulk.write_window_bytes = WINDOW;
        let mut claim_sequence = 1;
        // The fixture's rows are far below a window: nothing paces yet.
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            limits,
            COUNT + 2,
        )
        .await;
        assert_eq!(applied_rows(&db, index_id, generation).await, 32, "{name}");
        let writing = |paused| {
            Some(VectorBulkBuildPhase::Writing {
                nodes_written: 32,
                nodes: SOURCE,
                paused_for_write_buffer: paused,
            })
        };
        assert_eq!(
            driver.build_reports().phase(build_id),
            writing(false),
            "{name}"
        );
        let slot = Arc::clone(&driver.bulk.builds.lock()[&build_id]);
        {
            let state = slot.lock().await;
            let BuildState::Writing(writing) = &*state else {
                panic!("{name}: the build is in its write pass");
            };
            assert_eq!(writing.write_ahead, write_ahead, "{name}");
            assert!(writing.flushing.is_none(), "{name}");
        }
        drop(slot);

        // Another writer buffers four windows of rows.
        db.put(
            b"unrelated-ingest".as_slice(),
            vec![0x5A; 4 * WINDOW as usize],
        )
        .await
        .expect("the concurrent write is buffered");
        let stored = scan_progress(&db, build_id).await.unwrap();
        assert_eq!(
            drive_one(&db, &driver, build_id, &mut claim_sequence, limits).await,
            CommittedOperationStep::Progressed,
            "{name}"
        );
        match write_ahead {
            WriteAhead::Unbounded => {
                assert_eq!(applied_rows(&db, index_id, generation).await, 48, "{name}");
            }
            WriteAhead::Windowed => {
                // The step wrote nothing and says why.
                assert_eq!(
                    scan_progress(&db, build_id).await.unwrap(),
                    stored,
                    "{name}"
                );
                assert_eq!(applied_rows(&db, index_id, generation).await, 32, "{name}");
                assert_eq!(
                    driver.build_reports().phase(build_id),
                    writing(true),
                    "{name}"
                );
                // The flush it started wakes it, and the next step writes.
                drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
                assert_eq!(applied_rows(&db, index_id, generation).await, 48, "{name}");
                let unflushed = db.unflushed_bytes().unwrap();
                assert!(
                    ((unflushed.active_memtable_bytes + unflushed.immutable_memtable_bytes) as u64)
                        < WINDOW,
                    "{name}: the other writer's rows were flushed"
                );
            }
        }
        assert_eq!(
            driver.build_reports().phase(build_id),
            Some(VectorBulkBuildPhase::Writing {
                nodes_written: 48,
                nodes: SOURCE,
                paused_for_write_buffer: false,
            }),
            "{name}"
        );
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        let index = active_index(&db, &definition).await;
        assert!(
            index.stored_rows(&db).await.unwrap() == uninterrupted,
            "{name}"
        );
        assert_index_holds(&db, &index, &expected, &[]).await;
        assert!(nothing_held(&driver), "{name}");
        db.close().await.expect("vector test database closes");
    }
}

/// A scan one path started is finished correctly by a process configured for
/// the other: a serial scan resumes serially, and a partially written bulk
/// graph is deleted and its build returned to the serial stage where bulk
/// builds are off.
#[tokio::test(flavor = "multi_thread")]
async fn a_scan_another_configuration_started_is_finished() {
    let limits = entity_limits(16);
    for (name, started_by, steps, finished_by, path) in [
        (
            "serial-then-bulk",
            driver(),
            2,
            bulk_driver(2, BUDGET, 16),
            VectorBuildPath::Serial(VectorSerialBuildReason::ResumedSerialScan),
        ),
        (
            "bulk-then-serial",
            bulk_driver(2, BUDGET, 16),
            COUNT + 2,
            driver(),
            VectorBuildPath::Serial(VectorSerialBuildReason::BulkBuildsDisabled),
        ),
    ] {
        let db = test_db(&format!("vector-bulk-handover-{name}")).await;
        let definition = definition(None);
        let expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let mut claim_sequence = 1;
        drive_steps(
            &db,
            &started_by,
            build_id,
            &mut claim_sequence,
            limits,
            steps,
        )
        .await;
        assert_eq!(
            scan_progress(&db, build_id).await.unwrap().cursor,
            Some(source_cursor(SCOPE, 31)),
            "{name}"
        );
        assert_eq!(
            drive(&db, &finished_by, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            finished_by.build_reports().path(build_id),
            Some(path),
            "{name}"
        );
        assert_index_holds(&db, &active_index(&db, &definition).await, &expected, &[]).await;
        db.close().await.expect("vector test database closes");
    }
}

/// The remembered paths hold the latest path of the most recent operations.
#[test]
fn build_paths_remember_the_latest_operations() {
    let operations = (1..=REMEMBERED_BUILD_PATHS as u128 + 1)
        .map(|ordinal| {
            IndexOperationId::new(uuid::Uuid::from_u128(ordinal)).expect("operation id is non-nil")
        })
        .collect::<Vec<_>>();
    let reports = VectorBuildReports::default();
    let serial = VectorBuildPath::Serial(VectorSerialBuildReason::TooFewVectors);
    for operation_id in &operations[..REMEMBERED_BUILD_PATHS] {
        reports.record(*operation_id, serial);
    }
    assert_eq!(reports.path(operations[0]), Some(serial));
    assert_eq!(reports.path(operations[REMEMBERED_BUILD_PATHS]), None);

    // A new path for a remembered operation replaces the old one in place of
    // nothing else.
    reports.record(operations[0], VectorBuildPath::Bulk);
    assert_eq!(reports.path(operations[0]), Some(VectorBuildPath::Bulk));
    assert_eq!(reports.path(operations[1]), Some(serial));
    assert_eq!(reports.recent.lock().len(), REMEMBERED_BUILD_PATHS);

    // One more operation forgets the least recently recorded one.
    reports.record(operations[REMEMBERED_BUILD_PATHS], serial);
    assert_eq!(reports.path(operations[1]), None);
    assert_eq!(reports.path(operations[0]), Some(VectorBuildPath::Bulk));
    assert_eq!(
        reports.path(operations[REMEMBERED_BUILD_PATHS]),
        Some(serial)
    );
    assert_eq!(reports.recent.lock().len(), REMEMBERED_BUILD_PATHS);
}

/// What an attempt is doing is reported from its own counters, never past
/// their totals, until the process holds no attempt.
#[test]
fn an_attempts_phase_is_reported_from_its_counters() {
    let operation_id = IndexOperationId::from_bytes([0x31; 16]).unwrap();
    let reports = VectorBuildReports::default();
    assert_eq!(reports.phase(operation_id), None);
    reports.publish(
        operation_id,
        Some(Activity::Waiting(VectorBulkBuildWait::Turn)),
    );
    assert_eq!(
        reports.phase(operation_id),
        Some(VectorBulkBuildPhase::Waiting {
            waiting_for: VectorBulkBuildWait::Turn
        })
    );
    let progress = Arc::new(PreparationProgress {
        members: 10,
        ..PreparationProgress::default()
    });
    reports.publish(
        operation_id,
        Some(Activity::Preparing(Arc::clone(&progress))),
    );
    let reading = |members_read| {
        Some(VectorBulkBuildPhase::Reading {
            members_read,
            members: 10,
        })
    };
    assert_eq!(reports.phase(operation_id), reading(0));
    progress.members_read.store(7, Ordering::Relaxed);
    assert_eq!(reports.phase(operation_id), reading(7));
    progress.members_read.store(11, Ordering::Relaxed);
    assert_eq!(reports.phase(operation_id), reading(10));
    // The read ended with eight vectors: the attempt links them.
    progress.vectors.set(8).unwrap();
    let linking = |vectors_linked| {
        Some(VectorBulkBuildPhase::Linking {
            vectors_linked,
            vectors: 8,
        })
    };
    assert_eq!(reports.phase(operation_id), linking(0));
    progress.vectors_linked.store(5, Ordering::Relaxed);
    assert_eq!(reports.phase(operation_id), linking(5));
    progress.vectors_linked.store(9, Ordering::Relaxed);
    assert_eq!(reports.phase(operation_id), linking(8));
    reports.publish(
        operation_id,
        Some(Activity::Writing {
            nodes_written: 3,
            nodes: 8,
            paused_for_write_buffer: true,
        }),
    );
    assert_eq!(
        reports.phase(operation_id),
        Some(VectorBulkBuildPhase::Writing {
            nodes_written: 3,
            nodes: 8,
            paused_for_write_buffer: true,
        })
    );
    reports.publish(
        operation_id,
        Some(Activity::Discarding { rows_deleted: 40 }),
    );
    assert_eq!(
        reports.phase(operation_id),
        Some(VectorBulkBuildPhase::Discarding { rows_deleted: 40 })
    );
    reports.publish(operation_id, None);
    assert_eq!(reports.phase(operation_id), None);
}

/// A reservation is all of its bytes or none. A build that does not fit the
/// budget is told to wait only while other builds hold memory and it would
/// fit the most the budget can be; it is refused when nothing is reserved,
/// or when it exceeds that most. Released memory wakes the builds that
/// wait.
#[test]
fn a_build_waits_only_for_memory_other_builds_will_release() {
    let worker = IndexWorkerWakeHandle::default();
    let (memory, _) = host_memory(
        VectorBulkBuildMemory::Bytes(NonZeroU64::new(100).unwrap()),
        0,
        None,
        worker.clone(),
    );
    let admit = |bytes| MemoryReservation::admit(&memory, bytes);
    let refused = |admission| {
        matches!(
            admission,
            Admission::Refused {
                budget_bytes: 100,
                capacity_bytes: 100
            }
        )
    };

    // Nothing is reserved: the build fits the budget or is refused.
    assert!(refused(admit(101)));
    assert_eq!(memory.reserved(), 0);
    // The whole capacity is a build that fits.
    let Admission::Reserved(whole) = admit(100) else {
        panic!("a hundred of a hundred bytes are free");
    };
    assert_eq!(memory.reserved(), 100);
    drop(whole);
    assert_eq!(memory.reserved(), 0);
    let Admission::Reserved(mut first) = admit(60) else {
        panic!("sixty of a hundred bytes are free");
    };
    assert_eq!((first.bytes, memory.reserved()), (60, 60));

    // Beside the first build: what is left, exactly.
    assert!(matches!(admit(41), Admission::Busy));
    // No release makes room for more than the capacity.
    assert!(refused(admit(101)));
    assert!(refused(admit(u64::MAX)));
    assert_eq!(memory.reserved(), 60);
    let Admission::Reserved(second) = admit(40) else {
        panic!("forty bytes are left");
    };
    assert_eq!(memory.reserved(), 100);
    assert!(matches!(admit(1), Admission::Busy));
    assert!(matches!(admit(0), Admission::Reserved(_)));

    // A trimmed reservation releases the difference, which wakes waiting
    // builds, and never grows.
    let waiting = IndexOperationId::from_bytes([0x41; 16]).unwrap();
    memory.parking.park(waiting);
    assert!(!worker.take_wake());
    first.shrink_to(25);
    assert_eq!((first.bytes, memory.reserved()), (25, 65));
    assert_eq!(memory.parking.until(waiting), None);
    assert!(worker.take_wake());
    memory.parking.park(waiting);
    first.shrink_to(30);
    assert_eq!((first.bytes, memory.reserved()), (25, 65));
    assert!(
        memory.parking.until(waiting).is_some() && !worker.take_wake(),
        "nothing was released"
    );
    drop(second);
    assert_eq!(memory.reserved(), 25);
    assert_eq!(memory.parking.until(waiting), None);
    assert!(worker.take_wake());
    drop(first);
    assert_eq!(memory.reserved(), 0);
}

/// An automatic budget is taken against the rest of the process, and what
/// the builds leave resident is theirs: a build that fitted before another
/// ran fits after it, where the resident memory alone would refuse it for
/// good. What the rest of the process grows by while no build runs counts.
#[test]
fn memory_the_builds_left_resident_does_not_count_against_the_next_build() {
    const GIB: u64 = 1 << 30;
    // 16 GiB less the free eighth and 3 GiB of configured memory.
    let (memory, resident) = host_memory(
        VectorBulkBuildMemory::Automatic,
        3 * GIB,
        Some(16 * GIB),
        IndexWorkerWakeHandle::default(),
    );
    let admit = |bytes| MemoryReservation::admit(&memory, bytes);
    assert_eq!(memory.capacity(), 11 * GIB);
    resident.store(2 * GIB, Ordering::Relaxed);
    let Admission::Reserved(first) = admit(8 * GIB) else {
        panic!("the first build fits the capacity");
    };
    // While it runs its memory is resident too, and what is resident within
    // its reservation tells nothing of the rest: a second build waits.
    for held in [2 * GIB, 9 * GIB, 10 * GIB] {
        resident.store(held, Ordering::Relaxed);
        assert!(matches!(admit(8 * GIB), Admission::Busy), "{held}");
    }
    // It ends and leaves 3.5 GiB above the configured memory resident. The
    // resident memory alone would leave 7.5 GiB, and refuse the second.
    resident.store(6 * GIB + GIB / 2, Ordering::Relaxed);
    drop(first);
    assert_eq!(memory.ledger.lock().left, 3 * GIB + GIB / 2);
    let Admission::Reserved(second) = admit(8 * GIB) else {
        panic!("the second build fits as the first did");
    };
    drop(second);
    // The rest of the process grows by 4 GiB while no build runs: it holds
    // 7 GiB beside what the builds left, and the budget is 7 GiB.
    resident.store(10 * GIB + GIB / 2, Ordering::Relaxed);
    assert!(matches!(
        admit(7 * GIB + 1),
        Admission::Refused {
            budget_bytes,
            capacity_bytes,
        } if budget_bytes == 7 * GIB && capacity_bytes == 11 * GIB
    ));
    let Admission::Reserved(third) = admit(7 * GIB) else {
        panic!("seven GiB are free");
    };
    // The build started from a process that held 7 GiB: what is resident
    // above that when it ends is what it left.
    resident.store(12 * GIB, Ordering::Relaxed);
    drop(third);
    assert_eq!(memory.ledger.lock().left, 5 * GIB);
    assert!(matches!(admit(7 * GIB), Admission::Reserved(_)));
}

/// The builds are taken to have left no more than they reserved at once,
/// and nothing where the process holds no more than it did before them.
#[test]
fn builds_leave_no_more_resident_than_they_reserved() {
    const GIB: u64 = 1 << 30;
    let (memory, resident) = host_memory(
        VectorBulkBuildMemory::Automatic,
        3 * GIB,
        Some(16 * GIB),
        IndexWorkerWakeHandle::default(),
    );
    let admit = |bytes| MemoryReservation::admit(&memory, bytes);
    resident.store(GIB, Ordering::Relaxed);
    let Admission::Reserved(mut small) = admit(2 * GIB) else {
        panic!("two GiB fit");
    };
    // While a build runs the rest of the process is what it was when the
    // build started, as long as no more is resident than the builds
    // reserved beside it: the build's own memory is resident too.
    resident.store(3 * GIB, Ordering::Relaxed);
    let Admission::Reserved(beside) = admit(GIB) else {
        panic!("a third GiB fits beside them");
    };
    // The peak stays what the two held together, though one shrinks.
    small.shrink_to(GIB);
    drop(beside);
    // The rest of the process grew by 5 GiB meanwhile: 3 are the builds'.
    resident.store(8 * GIB, Ordering::Relaxed);
    drop(small);
    assert_eq!(memory.ledger.lock().left, 3 * GIB);
    // The process holds 5 GiB beside them, so 9 of the 14 GiB are free.
    assert!(matches!(admit(9 * GIB + 1), Admission::Refused { .. }));
    let Admission::Reserved(next) = admit(9 * GIB) else {
        panic!("nine GiB are free");
    };
    // It ends in a process that holds less than before it: it left nothing.
    resident.store(4 * GIB, Ordering::Relaxed);
    drop(next);
    assert_eq!(memory.ledger.lock().left, 0);
    assert!(matches!(admit(10 * GIB), Admission::Reserved(_)));
}

/// Memory the builds left resident may go back to the OS while no build
/// runs. The rest of the process is then not taken to have shrunk by it: the
/// next build gets the budget the last one was admitted under, not one
/// larger by what the allocator returned. A rest that holds less than
/// before the builds is believed once no more than it is resident.
#[test]
fn memory_the_allocator_returned_does_not_grow_the_budget() {
    const GIB: u64 = 1 << 30;
    // 16 GiB less the free eighth: 14 GiB beside the rest of the process.
    let (memory, resident) = host_memory(
        VectorBulkBuildMemory::Automatic,
        3 * GIB,
        Some(16 * GIB),
        IndexWorkerWakeHandle::default(),
    );
    let admit = |bytes| MemoryReservation::admit(&memory, bytes);
    // The rest of the process holds 4 GiB, so the budget is 10 GiB.
    resident.store(4 * GIB, Ordering::Relaxed);
    let Admission::Reserved(first) = admit(8 * GIB) else {
        panic!("eight GiB fit ten");
    };
    resident.store(6 * GIB, Ordering::Relaxed);
    drop(first);
    assert_eq!(memory.ledger.lock().left, 2 * GIB);
    // The allocator returns the 2 GiB: the rest still holds its 4 GiB.
    resident.store(4 * GIB, Ordering::Relaxed);
    assert!(matches!(
        admit(10 * GIB + 1),
        Admission::Refused { budget_bytes, .. } if budget_bytes == 10 * GIB
    ));
    // Half of it returned is the same.
    resident.store(5 * GIB, Ordering::Relaxed);
    assert!(matches!(
        admit(10 * GIB + 1),
        Admission::Refused { budget_bytes, .. } if budget_bytes == 10 * GIB
    ));
    // Less is resident than the rest held before the builds: it did shrink,
    // to no more than that, and the configured memory is what bounds it.
    resident.store(2 * GIB, Ordering::Relaxed);
    let Admission::Reserved(second) = admit(11 * GIB) else {
        panic!("eleven GiB fit beside the configured three");
    };
    // That build was admitted beside a rest of 2 GiB and left nothing above
    // the configured memory, so the next is measured against what is
    // resident.
    resident.store(3 * GIB, Ordering::Relaxed);
    drop(second);
    assert_eq!(memory.ledger.lock().left, 0);
    resident.store(5 * GIB, Ordering::Relaxed);
    assert!(matches!(
        admit(9 * GIB + 1),
        Admission::Refused { budget_bytes, .. } if budget_bytes == 9 * GIB
    ));
    assert!(matches!(admit(9 * GIB), Admission::Reserved(_)));
}

/// The rest of the process may grow while builds run. What is resident
/// beyond everything the builds reserved at once is the rest's, whoever
/// looks: it is not taken for memory the builds left, and the builds that
/// follow get a budget smaller by it. Growth that hides inside what the
/// builds reserved and do not use is taken for theirs, once: the next
/// build's own run shows it.
#[test]
fn growth_of_the_rest_seen_beside_the_builds_is_not_left_by_them() {
    const GIB: u64 = 1 << 30;
    // 16 GiB less the free eighth: 14 GiB beside the rest of the process.
    let (memory, resident) = host_memory(
        VectorBulkBuildMemory::Automatic,
        3 * GIB,
        Some(16 * GIB),
        IndexWorkerWakeHandle::default(),
    );
    let admit = |bytes| MemoryReservation::admit(&memory, bytes);
    // The rest of the process holds 4 GiB, so the budget is 10 GiB.
    resident.store(4 * GIB, Ordering::Relaxed);
    let Admission::Reserved(first) = admit(8 * GIB) else {
        panic!("eight GiB fit ten");
    };
    assert_eq!(first.budget_bytes, 10 * GIB);
    assert_eq!(first.rest_bytes, Some(4 * GIB));
    // The rest grows by 2 GiB while the build uses half of what it
    // reserved: 10 GiB are resident, within the 12 the model allows. The
    // look finds 2 GiB that cannot be the build's, fewer than the rest is
    // taken to hold already, so it tells nothing.
    resident.store(10 * GIB, Ordering::Relaxed);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, 2 * GIB);
    // The build ends with 8 GiB resident: 6 of the rest and 2 it left. The
    // 4 GiB above the rest it was admitted beside are taken for the
    // build's, and the next budget is the same 10 GiB, 2 GiB too many.
    resident.store(8 * GIB, Ordering::Relaxed);
    drop(first);
    assert_eq!(memory.ledger.lock().left, 4 * GIB);
    assert_eq!(memory.ledger.lock().floor, 0);
    let Admission::Reserved(second) = admit(8 * GIB) else {
        panic!("the second build is admitted under the first one's budget");
    };
    assert_eq!(second.budget_bytes, 10 * GIB);
    // The second build uses all it reserved, beside a rest that grew by
    // another GiB: 15 GiB are resident, 7 more than it reserved. A step of
    // it has the ledger look.
    resident.store(15 * GIB, Ordering::Relaxed);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, 7 * GIB);
    // No other build is admitted beside a rest of 7 GiB: 7 are free, and 8
    // are reserved.
    assert!(matches!(admit(GIB), Admission::Busy));
    // It ends with 9 GiB resident: the rest's 7 and 2 it left, no more.
    resident.store(9 * GIB, Ordering::Relaxed);
    drop(second);
    assert_eq!(memory.ledger.lock().left, 2 * GIB);
    assert!(matches!(
        admit(7 * GIB + 1),
        Admission::Refused { budget_bytes, .. } if budget_bytes == 7 * GIB
    ));
    let Admission::Reserved(third) = admit(7 * GIB) else {
        panic!("seven GiB fit beside a rest of seven");
    };
    assert_eq!(third.rest_bytes, Some(7 * GIB));
    // The allocator returns what the builds left: the rest is still taken
    // to hold what it was seen to hold.
    resident.store(14 * GIB, Ordering::Relaxed);
    drop(third);
    resident.store(7 * GIB, Ordering::Relaxed);
    assert!(matches!(
        admit(7 * GIB + 1),
        Admission::Refused { budget_bytes, .. } if budget_bytes == 7 * GIB
    ));
    // A build that no step looked at ends with more resident than the rest
    // it was admitted beside and all it reserved: the release looks too, and
    // the 2 GiB beyond are the rest's when the allocator returns the others.
    let Admission::Reserved(fourth) = admit(5 * GIB) else {
        panic!("five GiB fit beside a rest of seven");
    };
    resident.store(14 * GIB, Ordering::Relaxed);
    drop(fourth);
    assert_eq!(memory.ledger.lock().rest, Some(9 * GIB));
    assert_eq!(memory.ledger.lock().left, 5 * GIB);
    resident.store(9 * GIB, Ordering::Relaxed);
    assert!(matches!(
        admit(5 * GIB + 1),
        Admission::Refused { budget_bytes, .. } if budget_bytes == 5 * GIB
    ));
}

/// What a large build left is reused by a small one that follows it, not
/// added to: the small build is not taken to have left only what it
/// reserved, with the remainder turned into the rest of the process, and a
/// look during it does not take the large build's memory for the rest's. A
/// large build that follows fits again.
#[test]
fn a_small_build_keeps_what_a_larger_one_left() {
    const GIB: u64 = 1 << 30;
    // 16 GiB less the free eighth: 14 GiB beside the rest of the process.
    let (memory, resident) = host_memory(
        VectorBulkBuildMemory::Automatic,
        GIB,
        Some(16 * GIB),
        IndexWorkerWakeHandle::default(),
    );
    let admit = |bytes| MemoryReservation::admit(&memory, bytes);
    resident.store(2 * GIB, Ordering::Relaxed);
    let Admission::Reserved(large) = admit(10 * GIB) else {
        panic!("ten GiB fit twelve");
    };
    resident.store(12 * GIB, Ordering::Relaxed);
    drop(large);
    assert_eq!(memory.ledger.lock().left, 10 * GIB);
    // The small build runs in what the large one left: nothing more is
    // resident, and none of it is the rest's.
    let Admission::Reserved(small) = admit(GIB) else {
        panic!("one GiB fits");
    };
    assert_eq!(small.rest_bytes, Some(2 * GIB));
    assert_eq!(memory.ledger.lock().peak, 10 * GIB);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, 2 * GIB);
    drop(small);
    assert_eq!(memory.ledger.lock().rest, Some(2 * GIB));
    assert_eq!(memory.ledger.lock().left, 10 * GIB);
    let Admission::Reserved(large) = admit(12 * GIB) else {
        panic!("twelve GiB fit beside a rest of two");
    };
    assert_eq!(large.budget_bytes, 12 * GIB);
    drop(large);
    // The allocator returned half of what the builds left before the next
    // small build: what is left of it is all that build is taken to reuse.
    resident.store(7 * GIB, Ordering::Relaxed);
    let Admission::Reserved(small) = admit(GIB) else {
        panic!("one GiB fits");
    };
    assert_eq!(small.rest_bytes, Some(2 * GIB));
    assert_eq!(memory.ledger.lock().peak, 5 * GIB);
    // The rest grows by 3 GiB during it, and a look sees that.
    resident.store(10 * GIB, Ordering::Relaxed);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, 5 * GIB);
    drop(small);
    assert_eq!(memory.ledger.lock().rest, Some(5 * GIB));
    assert_eq!(memory.ledger.lock().left, 5 * GIB);
}

/// What a build's steps make the ledger look at is what is resident beyond
/// the most the builds reserved at once, not beyond what they still hold:
/// memory a build released, or one that ended left, is not the rest's. A
/// look while no build runs changes nothing.
#[test]
fn a_look_measures_the_rest_beyond_the_builds_peak() {
    const GIB: u64 = 1 << 30;
    let (memory, resident) = host_memory(
        VectorBulkBuildMemory::Automatic,
        3 * GIB,
        Some(32 * GIB),
        IndexWorkerWakeHandle::default(),
    );
    let admit = |bytes| MemoryReservation::admit(&memory, bytes);
    resident.store(4 * GIB, Ordering::Relaxed);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, 0);
    let Admission::Reserved(mut first) = admit(8 * GIB) else {
        panic!("eight GiB fit");
    };
    let Admission::Reserved(second) = admit(4 * GIB) else {
        panic!("four more fit");
    };
    // The first trims its reservation and the second ends: 3 GiB are
    // reserved, of a peak of 12.
    first.shrink_to(3 * GIB);
    drop(second);
    assert_eq!(memory.reserved(), 3 * GIB);
    resident.store(13 * GIB, Ordering::Relaxed);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, GIB);
    resident.store(18 * GIB, Ordering::Relaxed);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, 6 * GIB);
    // A lower look does not lower what was seen.
    resident.store(5 * GIB, Ordering::Relaxed);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, 6 * GIB);
    drop(first);
    assert_eq!(memory.ledger.lock().rest, Some(6 * GIB));
    // No build runs: a look has nothing to measure against.
    resident.store(30 * GIB, Ordering::Relaxed);
    memory.observe();
    assert_eq!(memory.ledger.lock().floor, 0);
}

/// The budget of an explicit policy is its bytes whatever the process holds,
/// and a build's write pass reserves the write buffer's memory beyond its
/// encoded size, or two windows of it.
#[test]
fn an_explicit_budget_is_taken_as_given() {
    let (memory, resident) = host_memory(
        VectorBulkBuildMemory::Bytes(NonZeroU64::new(1_000).unwrap()),
        u64::MAX,
        Some(1),
        IndexWorkerWakeHandle::default(),
    );
    resident.store(u64::MAX, Ordering::Relaxed);
    assert_eq!(memory.capacity(), 1_000);
    assert!(matches!(
        MemoryReservation::admit(&memory, 1_000),
        Admission::Reserved(_)
    ));
    assert!(matches!(
        MemoryReservation::admit(&memory, 1_001),
        Admission::Refused {
            budget_bytes: 1_000,
            capacity_bytes: 1_000
        }
    ));
    let buffer = |write_buffer_bytes| BuildMemory {
        policy: memory.policy,
        configured_bytes: 0,
        write_buffer_bytes,
        host: HostMemory::Process,
        ledger: parking_lot::Mutex::new(Ledger::default()),
        parking: Arc::clone(&memory.parking),
    };
    // A write buffer smaller than two windows is all a build can fill.
    assert_eq!(buffer(300).write_allowance(WriteAhead::Unbounded), 600);
    assert_eq!(buffer(300).write_allowance(WriteAhead::Windowed), 600);
    assert_eq!(
        buffer(WRITE_BUFFER).write_allowance(WriteAhead::Unbounded),
        2 * WRITE_BUFFER
    );
    assert_eq!(
        buffer(WRITE_BUFFER).write_allowance(WriteAhead::Windowed),
        6 * WRITE_WINDOW_BYTES
    );
    let (off, _) = host_memory(
        VectorBulkBuildMemory::Off,
        0,
        Some(u64::MAX),
        IndexWorkerWakeHandle::default(),
    );
    assert_eq!(off.capacity(), 0);
    assert!(matches!(
        MemoryReservation::admit(&off, 1),
        Admission::Refused {
            budget_bytes: 0,
            capacity_bytes: 0
        }
    ));
}

/// A build reserves its vectors, the fixed allowance and its write
/// allowance, and a sum past `u64` is the largest request there is.
#[test]
fn a_build_reserves_its_vectors_and_allowances() {
    assert_eq!(build_bytes(700, 0, 0), FIXED_ALLOWANCE_BYTES);
    assert_eq!(build_bytes(700, 10, 5), FIXED_ALLOWANCE_BYTES + 7_005);
    assert_eq!(build_bytes(700, u64::MAX, 5), u64::MAX);
    assert_eq!(build_bytes(700, 10, u64::MAX), u64::MAX);
    assert_eq!(build_bytes(u64::MAX, 2, 0), u64::MAX);
}

/// Stage `scan` with path `bulk` is reported exactly while the bulk stage is
/// stored, to every process: one that knows nothing of the build, one that
/// has since decided to scan serially, and one that built in bulk. Under the
/// serial stage a process reports only why it scans serially, and past the
/// scan whatever path it remembers.
#[tokio::test(flavor = "multi_thread")]
async fn the_stored_bulk_stage_is_reported_as_the_bulk_path() {
    use crate::index_lifecycle::{IndexOperationStage, IndexOperationStatus};
    let db = test_db("vector-bulk-reported-path").await;
    let definition = definition(None);
    seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let serial = VectorBuildPath::Serial(VectorSerialBuildReason::EarlierAttemptsFailed);
    let reported = async |known: Option<VectorBuildPath>| {
        let status = IndexOperationStatus::from_record(&read_operation(&db, SCOPE, build_id).await)
            .with_vector_build_report(known, None);
        (status.common().stage, status.common().vector_build_path)
    };

    let scan = IndexOperationStage::Scan;
    assert_eq!(reported(None).await, (scan, None));
    assert_eq!(reported(Some(serial)).await, (scan, Some(serial)));
    assert_eq!(reported(Some(VectorBuildPath::Bulk)).await, (scan, None));

    let attempt = counted(&db, build_id, 2).await;
    store_stage(&db, build_id, VectorBuildStage::BulkBuild(attempt)).await;
    for known in [None, Some(serial), Some(VectorBuildPath::Bulk)] {
        assert_eq!(
            reported(known).await,
            (scan, Some(VectorBuildPath::Bulk)),
            "{known:?}"
        );
    }
    let status = serde_json::to_value(IndexOperationStatus::from_record(
        &read_operation(&db, SCOPE, build_id).await,
    ))
    .unwrap();
    assert_eq!(status["stage"], "scan");
    assert_eq!(
        status["vector_build_path"],
        serde_json::json!({"path": "bulk"})
    );

    store_stage(
        &db,
        build_id,
        VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
            cursor: None,
            counters: OperationCounters::default(),
        }),
    )
    .await;
    let validating = IndexOperationStage::ValidateDescriptor;
    assert_eq!(reported(None).await, (validating, None));
    assert_eq!(reported(Some(serial)).await, (validating, Some(serial)));
    assert_eq!(
        reported(Some(VectorBuildPath::Bulk)).await,
        (validating, Some(VectorBuildPath::Bulk))
    );
    db.close().await.expect("vector test database closes");
}

/// The stored stage answers for its scan, its count and itself, and a
/// restarted scan keeps only the source's bound.
#[test]
fn a_stored_scan_stage_names_its_progress_and_its_attempts() {
    let scan = SourceScanProgress {
        inclusive_upper_bound: source_cursor(SCOPE, 9),
        cursor: Some(source_cursor(SCOPE, 4)),
        counters: OperationCounters {
            entities: 5,
            input_bytes: 6,
            output_operations: 7,
            output_bytes: 8,
        },
    };
    let attempt = BulkBuildProgress {
        scan: scan.clone(),
        attempts: BulkBuildAttempts::new(2).unwrap(),
    };
    let serial = StoredScan::Serial(scan.clone());
    let bulk = StoredScan::Bulk(attempt.clone());
    assert_eq!((serial.progress(), bulk.progress()), (&scan, &scan));
    assert_eq!(
        (serial.attempts(), bulk.attempts()),
        (None, Some(attempt.attempts))
    );
    assert_eq!(serial.stage(), VectorBuildStage::Scan(scan.clone()));
    assert_eq!(bulk.stage(), VectorBuildStage::BulkBuild(attempt.clone()));

    // Exactly the stored attempt, in its stage, count and scan.
    assert!(bulk.is_bulk(&attempt));
    assert!(!serial.is_bulk(&attempt));
    assert!(!bulk.is_bulk(&BulkBuildProgress {
        attempts: BulkBuildAttempts::new(3).unwrap(),
        ..attempt.clone()
    }));
    assert!(!bulk.is_bulk(&BulkBuildProgress {
        scan: scan_from_start(&scan),
        ..attempt
    }));

    assert_eq!(
        scan_from_start(&scan),
        SourceScanProgress {
            inclusive_upper_bound: source_cursor(SCOPE, 9),
            cursor: None,
            counters: OperationCounters::default(),
        }
    );
}

/// The fetches of a pass-1 scan hold at most [`READ_AHEAD_BYTES`] across the
/// SSTs it merges, however many those are, down to one least fetch for each.
#[test]
fn a_read_scan_holds_a_bounded_read_ahead_whatever_the_tree_holds() {
    let held = |ssts: usize| {
        let tuning = read_tuning(ssts);
        (
            tuning.read_ahead_bytes().get(),
            tuning.max_fetch_tasks().get(),
        )
    };
    // A tree of a few SSTs gets the most a scan reads ahead of each.
    for ssts in [0, 1, 4] {
        assert_eq!(
            held(ssts),
            (SOURCE_READ_AHEAD_BYTES, SOURCE_FETCH_TASKS),
            "{ssts}"
        );
    }
    assert_eq!(held(16), (1 << 20, SOURCE_FETCH_TASKS));
    assert_eq!(held(64), (SOURCE_MIN_READ_AHEAD_BYTES, SOURCE_FETCH_TASKS));
    assert_eq!(held(128), (SOURCE_MIN_READ_AHEAD_BYTES, 8));
    assert_eq!(held(1_024), (SOURCE_MIN_READ_AHEAD_BYTES, 1));
    assert_eq!(held(usize::MAX), (SOURCE_MIN_READ_AHEAD_BYTES, 1));
    for ssts in (1..=2_000).chain([1 << 20, usize::MAX]) {
        let (read_ahead, tasks) = held(ssts);
        assert!(
            (SOURCE_MIN_READ_AHEAD_BYTES..=SOURCE_READ_AHEAD_BYTES).contains(&read_ahead)
                && (1..=SOURCE_FETCH_TASKS).contains(&tasks),
            "{ssts}"
        );
        let per_sst = read_ahead * tasks;
        assert!(
            per_sst.saturating_mul(ssts) <= READ_AHEAD_BYTES
                || per_sst == SOURCE_MIN_READ_AHEAD_BYTES,
            "{ssts} SSTs hold {per_sst} each"
        );
    }
}

/// A label is admitted for its own members, though its rows lie among as
/// many rows of other labels: a budget that holds exactly its members builds
/// it in bulk, where a bound over the ids its rows span would have taken
/// twice as much. The graph holds its label's entities alone, whether the
/// passes read the label's rows by a scan of their range (every other id) or
/// by point reads (every fifth).
#[tokio::test(flavor = "multi_thread")]
async fn a_label_among_other_labels_is_admitted_for_its_own_members() {
    const MEMBERS: u64 = 120;
    for (name, stride) in [("dense", 2), ("sparse", 5)] {
        let db = test_db(&format!("vector-bulk-multi-label-{name}")).await;
        let definition = definition(None);
        let last_id = MEMBERS * stride - 1;
        let expected = seed(
            &db,
            (0..MEMBERS).map(|member| (member * stride, vector_of(member * stride))),
        )
        .await;
        let others = (0..=last_id)
            .filter(|entity_id| entity_id % stride != 0)
            .collect::<Vec<_>>();
        put_sources(
            &db,
            others
                .iter()
                .map(|entity_id| (*entity_id, labelled("Note", vector_of(*entity_id)))),
        )
        .await;
        // Members of the label past the build's bound are no part of its
        // source: they arrive from the queue.
        let beyond = (1..=40)
            .map(|member| last_id + member * stride)
            .collect::<Vec<_>>();
        put_sources(
            &db,
            beyond
                .iter()
                .map(|entity_id| (*entity_id, properties(vector_of(*entity_id), None))),
        )
        .await;
        let others = others.into_iter().chain(beyond).collect::<Vec<_>>();
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, last_id).await;
        let limits = entity_limits(16);
        let mut claim_sequence = 1;
        // One byte less holds no build of the label.
        let short = bulk_driver(2, paced_build_bytes(MEMBERS) - 1, 16);
        let driver = bulk_driver(2, paced_build_bytes(MEMBERS), 16);
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, COUNT).await;
        assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
        assert_eq!(
            driver.bulk.memory.reserved(),
            paced_build_bytes(MEMBERS),
            "{name}: the reservation is the label's, not its id span's"
        );
        assert_eq!(
            driver.build_reports().phase(build_id),
            Some(VectorBulkBuildPhase::Reading {
                members_read: 0,
                members: MEMBERS
            }),
            "{name}"
        );
        // The first write step stores the label's first sixteen members.
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
        let progress = scan_progress(&db, build_id).await.unwrap();
        assert_eq!(
            progress.cursor,
            Some(source_cursor(SCOPE, 15 * stride)),
            "{name}"
        );
        assert_eq!(progress.counters.entities, 16, "{name}");
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            driver.build_reports().path(build_id),
            Some(VectorBuildPath::Bulk),
            "{name}"
        );
        assert_eq!(reported_entities(&db, build_id).await, MEMBERS, "{name}");
        assert_index_holds(
            &db,
            &active_index(&db, &definition).await,
            &expected,
            &others,
        )
        .await;
        assert!(nothing_held(&driver), "{name}");

        // The same label under the smaller budget is built serially.
        let again = test_db(&format!("vector-bulk-multi-label-{name}-short")).await;
        seed(
            &again,
            (0..MEMBERS).map(|member| (member * stride, vector_of(member * stride))),
        )
        .await;
        let (short_id, _, _) = create_build(&again, SCOPE, &definition, last_id).await;
        assert_eq!(
            drive(&again, &short, short_id, &mut 1, limits).await.0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            short.build_reports().path(short_id),
            Some(VectorBuildPath::Serial(
                VectorSerialBuildReason::OverMemoryBudget
            )),
            "{name}"
        );
        again.close().await.expect("vector test database closes");
        db.close().await.expect("vector test database closes");
    }
}

/// Bulk builds are off unless the configuration turns them on: a driver
/// with the default policy builds serially, says why, and never stores the
/// bulk stage, which a release from before bulk builds could not read.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_is_serial_unless_bulk_builds_are_turned_on() {
    let db = test_db("vector-bulk-default-off").await;
    let definition = definition(None);
    let expected = seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    assert_eq!(
        VectorBulkBuildTuning::default().memory(),
        VectorBulkBuildMemory::Off
    );
    assert_eq!(
        crate::config::DbConfig::new()
            .index_lifecycle_throughput()
            .vector_bulk_build()
            .memory(),
        VectorBulkBuildMemory::Off
    );
    for driver in [
        driver(),
        driver().with_bulk_builds(
            VectorBulkBuildTuning::default(),
            0,
            WRITE_BUFFER,
            IndexWorkerWakeHandle::default(),
        ),
    ] {
        let db = if driver.bulk.tuning == VectorBulkBuildTuning::disabled() {
            &db
        } else {
            &test_db("vector-bulk-default-policy").await
        };
        let build_id = if driver.bulk.tuning == VectorBulkBuildTuning::disabled() {
            build_id
        } else {
            seed_standard(db).await;
            create_build(db, SCOPE, &definition, SOURCE - 1).await.0
        };
        let limits = entity_limits(16);
        let mut claim_sequence = 1;
        let mut scan_steps = 0;
        while let Some(stored) = stored_scan(db, build_id).await {
            assert!(
                matches!(stored, StoredScan::Serial(_)),
                "no step of a build stores the bulk stage while bulk builds are off"
            );
            assert_eq!(
                drive_one(db, &driver, build_id, &mut claim_sequence, limits).await,
                CommittedOperationStep::Progressed
            );
            scan_steps += 1;
        }
        assert_eq!(scan_steps, SOURCE / 16);
        assert_eq!(
            driver.build_reports().path(build_id),
            Some(VectorBuildPath::Serial(
                VectorSerialBuildReason::BulkBuildsDisabled
            ))
        );
        assert_eq!(driver.build_reports().phase(build_id), None);
        assert_eq!(
            drive(db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed
        );
        assert_index_holds(db, &active_index(db, &definition).await, &expected, &[]).await;
    }
    db.close().await.expect("vector test database closes");
}

/// With an automatic budget, a second build that waited for the first is
/// built in bulk after it, though the first leaves the process holding more
/// than it did: what the builds leave resident is theirs. The same resident
/// memory in a process that ran no build refuses the build.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_build_is_built_in_bulk_after_the_first() {
    const GIB: u64 = 1 << 30;
    let db = test_db("vector-bulk-second-build").await;
    let (first, second) = (definition(None), summary_definition());
    seed_two_vectors(&db).await;
    let (first_id, _, _) = create_build(&db, SCOPE, &first, SOURCE - 1).await;
    let (second_id, _, _) = create_build(&db, SCOPE, &second, SOURCE - 1).await;
    // A ceiling that leaves one paced build and a quarter beside 3 GiB of
    // configured memory, once its free eighth is taken.
    let needed = paced_build_bytes(SOURCE);
    let ceiling = (3 * GIB + needed + needed / 4) / 7 * 8;
    let automatic = |resident: u64| {
        let worker = IndexWorkerWakeHandle::default();
        let mut driver = bulk_driver_waking(2, BUDGET, 16, worker.clone());
        let (memory, counter) = host_memory(
            VectorBulkBuildMemory::Automatic,
            3 * GIB,
            Some(ceiling),
            worker,
        );
        counter.store(resident, Ordering::Relaxed);
        driver.bulk.memory = Arc::new(BuildMemory {
            policy: memory.policy,
            configured_bytes: memory.configured_bytes,
            write_buffer_bytes: memory.write_buffer_bytes,
            host: memory.host.clone(),
            ledger: parking_lot::Mutex::new(Ledger::default()),
            parking: Arc::clone(&driver.bulk.parking),
        });
        (driver, counter)
    };
    let (driver, resident) = automatic(2 * GIB);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    drive_steps(
        &db,
        &driver,
        first_id,
        &mut claim_sequence,
        limits,
        COUNT + 1,
    )
    .await;
    assert_eq!(
        driver.build_reports().path(first_id),
        Some(VectorBuildPath::Bulk)
    );
    // The second waits for the first's memory.
    parked_steps(&db, &driver, second_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(
        driver.build_reports().phase(second_id),
        Some(VectorBulkBuildPhase::Waiting {
            waiting_for: VectorBulkBuildWait::Memory
        })
    );
    // The first ends with the process holding 1.5 GiB above its configured
    // memory: by the resident memory alone the second would not fit.
    let left = 3 * GIB + GIB + GIB / 2;
    resident.store(left, Ordering::Relaxed);
    assert!(ceiling - ceiling / 8 - left < needed);
    assert_eq!(
        drive(&db, &driver, first_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.parked_until(&read_operation(&db, SCOPE, second_id).await),
        None,
        "the first build's memory wakes the second"
    );
    assert_eq!(
        drive(&db, &driver, second_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert_eq!(
        driver.build_reports().path(second_id),
        Some(VectorBuildPath::Bulk),
        "the second build is built in bulk after the first"
    );
    assert!(nothing_held(&driver));

    // A process that holds as much without having run a build refuses it.
    let other = test_db("vector-bulk-second-build-refused").await;
    seed_standard(&other).await;
    let (refused_id, _, _) = create_build(&other, SCOPE, &first, SOURCE - 1).await;
    let (fresh, _) = automatic(left);
    drive_steps(&other, &fresh, refused_id, &mut 1, limits, 1).await;
    assert_eq!(
        fresh.build_reports().path(refused_id),
        Some(VectorBuildPath::Serial(
            VectorSerialBuildReason::OverMemoryBudget
        ))
    );
    other.close().await.expect("vector test database closes");
    db.close().await.expect("vector test database closes");
}

/// The status of a build never says the bulk stage is gone before it is
/// gone from what a restart would find. A step that leaves the stage is
/// applied before its WAL upload, and until the upload the status is the
/// durable record's: stage `scan` on the bulk path. A process killed in
/// that window leaves exactly that record, which a restart reads; once the
/// step is uploaded the status moves, and a restart finds what it says.
#[tokio::test(flavor = "multi_thread")]
async fn the_status_never_leaves_the_bulk_stage_before_the_stored_record_does() {
    use crate::index_lifecycle::outbox::read_durable_operation_record;
    use crate::index_lifecycle::{status_record, IndexOperationStage, IndexOperationStatus};
    use crate::search::vector::gated_wal::{GatedWalStore, WalUploads};
    let limits = entity_limits(16);
    for (name, off) in [("turned-off", true), ("finished", false)] {
        let path = format!("vector-bulk-durable-status-{name}");
        let store = Arc::new(GatedWalStore::new());
        let open = async |store: Arc<dyn slatedb::object_store::ObjectStore>| {
            Db::builder(path.as_str(), store)
                .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
                .build()
                .await
                .expect("vector test database opens")
        };
        let db = open(Arc::clone(&store) as Arc<dyn slatedb::object_store::ObjectStore>).await;
        crate::migrations::startup::bootstrap_writer(&db)
            .await
            .expect("vector test database bootstraps");
        let definition = definition(None);
        seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        // What `get_index_operation` reports for a writer.
        let reported = async |db: &Db| {
            let applied = read_operation(db, SCOPE, build_id).await;
            let durable = read_durable_operation_record(db, SCOPE, build_id)
                .await
                .unwrap();
            let status =
                IndexOperationStatus::from_record(status_record(&applied, durable.as_ref()));
            (status.common().stage, status.common().vector_build_path)
        };
        let bulk = (IndexOperationStage::Scan, Some(VectorBuildPath::Bulk));
        let mut claim_sequence = 1;
        let on = bulk_driver(1, BUDGET, 16);
        let leaving = if off {
            // A partly written graph, and a process with bulk builds off
            // that deletes it and stores the serial stage.
            drive_steps(&db, &on, build_id, &mut claim_sequence, limits, COUNT + 2).await;
            driver()
        } else {
            // A graph whose last step stores its metadata and leaves for
            // validation.
            while scan_progress(&db, build_id).await.unwrap().cursor
                != Some(source_cursor(SCOPE, SOURCE - 17))
            {
                drive_steps(&db, &on, build_id, &mut claim_sequence, limits, 1).await;
            }
            on
        };
        db.flush().await.expect("the bulk stage is durable");
        assert_eq!(reported(&db).await, bulk, "{name}");

        store.uploads.send_replace(WalUploads::Held);
        // The steps that leave the stage are applied, and none uploaded.
        while matches!(stored_scan(&db, build_id).await, Some(StoredScan::Bulk(_))) {
            drive_steps(&db, &leaving, build_id, &mut claim_sequence, limits, 1).await;
        }
        let applied =
            IndexOperationStatus::from_record(&read_operation(&db, SCOPE, build_id).await);
        assert_eq!(
            (applied.common().stage, applied.common().vector_build_path),
            (
                if off {
                    IndexOperationStage::Scan
                } else {
                    IndexOperationStage::ValidateDescriptor
                },
                None
            ),
            "{name}: the applied record has left the bulk stage"
        );
        assert_eq!(
            reported(&db).await,
            bulk,
            "{name}: the status is the durable record's until the step is uploaded"
        );

        // Killed here, the process leaves the bulk stage stored: what the
        // status said.
        let killed = open(Arc::new(store.restarted())).await;
        assert!(
            matches!(
                stored_scan(&killed, build_id).await,
                Some(StoredScan::Bulk(_))
            ),
            "{name}: a restart finds the stage the status reported"
        );
        killed.close().await.expect("the restarted database closes");

        // The upload goes through: the status moves with what is stored.
        // The first writer was fenced by the restart, so a new process
        // repeats the steps.
        drop(db);
        let db = open(Arc::new(store.restarted())).await;
        let leaving = if off {
            driver()
        } else {
            bulk_driver(1, BUDGET, 16)
        };
        let mut claim_sequence = 10_000;
        while reported(&db).await == bulk {
            drive_steps(&db, &leaving, build_id, &mut claim_sequence, limits, 1).await;
            db.flush().await.expect("the step is durable");
        }
        let after = reported(&db).await;
        let reopened = open(Arc::new(store.restarted())).await;
        let stored =
            IndexOperationStatus::from_record(&read_operation(&reopened, SCOPE, build_id).await);
        assert_eq!(
            (stored.common().stage, stored.common().vector_build_path),
            after,
            "{name}: a restart finds what the status said"
        );
        assert_ne!(after, bulk, "{name}");
        reopened
            .close()
            .await
            .expect("the reopened database closes");
    }
}

/// The record a writer's status is projected from is the applied one, but
/// for a durable record still at the bulk stage that the applied one left.
#[test]
fn a_status_is_the_durable_records_while_only_that_is_at_the_bulk_stage() {
    use crate::index_lifecycle::status_record;
    let scan = SourceScanProgress {
        inclusive_upper_bound: source_cursor(SCOPE, 9),
        cursor: None,
        counters: OperationCounters::default(),
    };
    let record = |stage: VectorBuildStage| {
        let definition = definition(None);
        IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([0x51; 16]).unwrap(),
            IndexId::initial(),
            definition.identity().clone(),
            IndexGenerationId::initial(),
            crate::index_lifecycle::IndexRevision::initial(),
            crate::index_lifecycle::IndexOperationRevision::initial(),
            crate::index_lifecycle::IndexOperationKind::Build,
            crate::index_lifecycle::IndexOperationFamily::Vector,
            IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(stage)),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .expect("the record validates")
    };
    let bulk = record(VectorBuildStage::BulkBuild(BulkBuildProgress {
        scan: scan.clone(),
        attempts: BulkBuildAttempts::FIRST,
    }));
    let serial = record(VectorBuildStage::Scan(scan));
    let validating = record(VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    }));
    // The applied record, wherever both stand, but for that one case.
    for applied in [&bulk, &serial, &validating] {
        assert_eq!(status_record(applied, None), applied);
        assert_eq!(status_record(applied, Some(applied)), applied);
        assert_eq!(status_record(applied, Some(&serial)), applied);
        assert_eq!(status_record(applied, Some(&validating)), applied);
    }
    assert_eq!(status_record(&bulk, Some(&bulk)), &bulk);
    assert_eq!(status_record(&serial, Some(&bulk)), &bulk);
    assert_eq!(status_record(&validating, Some(&bulk)), &bulk);
}

/// Only an operation this process stepped at the bulk stage can have a
/// durable record still at that stage behind an applied one that left it, so
/// only such an operation is remembered for a status to check, each step
/// that finds it there counting. It is forgotten when a status that saw every
/// such step found the two records equal off the stage, and not by one that
/// began before the last of them.
#[tokio::test(flavor = "multi_thread")]
async fn only_a_build_stepped_at_the_bulk_stage_is_remembered_for_its_durable_record() {
    let limits = entity_limits(16);
    let db = test_db("vector-bulk-stage-sightings").await;
    let definition = definition(None);
    seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(1, BUDGET, 16);
    let reports = driver.build_reports();
    let mut claim_sequence = 1;
    // The step that stores the bulk stage found the serial one.
    start_attempt(&db, &driver, build_id, &mut claim_sequence).await;
    assert_eq!(reports.bulk_stage_sightings(build_id), None);
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
    let first = reports
        .bulk_stage_sightings(build_id)
        .expect("a step found the build at the bulk stage");
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
    let second = reports.bulk_stage_sightings(build_id).unwrap();
    assert!(second > first);
    // A status that began before the last step forgets nothing.
    reports.durably_left_bulk_stage(build_id, first);
    assert_eq!(reports.bulk_stage_sightings(build_id), Some(second));
    reports.durably_left_bulk_stage(build_id, second);
    assert_eq!(reports.bulk_stage_sightings(build_id), None);
    // The next step at the bulk stage is remembered again.
    drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
    assert_eq!(reports.bulk_stage_sightings(build_id), Some(1));

    // A build that scans serially is never remembered.
    let other = test_db("vector-bulk-stage-sightings-serial").await;
    seed_standard(&other).await;
    let (serial_id, _, _) = create_build(&other, SCOPE, &definition, SOURCE - 1).await;
    let off = bulk_driver(1, 0, 16);
    assert_eq!(
        drive(&other, &off, serial_id, &mut 1, limits).await.0,
        CommittedOperationStep::Completed
    );
    assert_eq!(off.build_reports().bulk_stage_sightings(serial_id), None);
    other.close().await.expect("vector test database closes");
    db.close().await.expect("vector test database closes");
}

/// A status carries the attempt its record counts to every process, and the
/// phase of the attempt where the process runs it. A build that waits to
/// start after an attempt was abandoned reports the attempt it will start.
/// Of a record at the serial scan only a build that waits to start is
/// reported, as its first attempt; whatever else the process remembers there
/// is over.
#[test]
fn a_status_reports_the_attempt_and_its_phase() {
    use crate::index_lifecycle::{
        IndexOperationStage, IndexOperationStatus, VectorBulkBuildStatus,
    };
    let scan = SourceScanProgress {
        inclusive_upper_bound: source_cursor(SCOPE, 9),
        cursor: None,
        counters: OperationCounters::default(),
    };
    let status = |stage: VectorBuildStage| {
        let definition = definition(None);
        IndexOperationStatus::from_record(
            &IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x52; 16]).unwrap(),
                IndexId::initial(),
                definition.identity().clone(),
                IndexGenerationId::initial(),
                crate::index_lifecycle::IndexRevision::initial(),
                crate::index_lifecycle::IndexOperationRevision::initial(),
                crate::index_lifecycle::IndexOperationKind::Build,
                crate::index_lifecycle::IndexOperationFamily::Vector,
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(stage)),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .expect("the record validates"),
        )
    };
    let reading = VectorBulkBuildPhase::Reading {
        members_read: 4,
        members: 10,
    };
    let waiting = VectorBulkBuildPhase::Waiting {
        waiting_for: VectorBulkBuildWait::Turn,
    };
    let second = BulkBuildAttempts::new(2).unwrap();
    let bulk = || {
        status(VectorBuildStage::BulkBuild(BulkBuildProgress {
            scan: scan.clone(),
            attempts: second,
        }))
    };
    let attempt = |attempt, phase| {
        Some(VectorBulkBuildStatus {
            bulk_attempt: attempt,
            bulk_attempt_limit: BulkBuildAttempts::LIMIT,
            phase,
        })
    };
    // A process that does not run the attempt reports its count alone.
    assert_eq!(bulk().common().vector_bulk_build, attempt(2, None));
    assert_eq!(
        bulk()
            .with_vector_build_report(None, None)
            .common()
            .vector_bulk_build,
        attempt(2, None)
    );
    let running = bulk().with_vector_build_report(None, Some(reading));
    assert_eq!(
        running.common().vector_bulk_build,
        attempt(2, Some(reading))
    );
    // The second attempt is over once the build waits again: for its third.
    assert_eq!(
        bulk()
            .with_vector_build_report(None, Some(waiting))
            .common()
            .vector_bulk_build,
        attempt(3, Some(waiting))
    );
    let discarding = VectorBulkBuildPhase::Discarding { rows_deleted: 7 };
    assert_eq!(
        bulk()
            .with_vector_build_report(None, Some(discarding))
            .common()
            .vector_bulk_build,
        attempt(2, Some(discarding))
    );
    let json = serde_json::to_value(&running).unwrap();
    assert_eq!(json["stage"], "scan");
    assert_eq!(
        json["vector_build_path"],
        serde_json::json!({"path": "bulk"})
    );
    assert_eq!(
        json["vector_bulk_build"],
        serde_json::json!({
            "bulk_attempt": 2,
            "bulk_attempt_limit": 3,
            "phase": "reading",
            "members_read": "4",
            "members": "10",
        })
    );

    // At the serial scan: a waiting build is its first attempt to be.
    let serial = || status(VectorBuildStage::Scan(scan.clone()));
    assert_eq!(serial().common().vector_bulk_build, None);
    assert_eq!(
        serial()
            .with_vector_build_report(None, Some(waiting))
            .common()
            .vector_bulk_build,
        attempt(1, Some(waiting))
    );
    assert_eq!(
        serial()
            .with_vector_build_report(None, Some(reading))
            .common()
            .vector_bulk_build,
        None
    );
    let json = serde_json::to_value(serial().with_vector_build_report(None, None)).unwrap();
    assert!(json.get("vector_bulk_build").is_none());
    assert!(json.get("vector_build_path").is_none());

    // Past the scan nothing of an attempt is reported.
    let validating = status(VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
        cursor: None,
        counters: OperationCounters::default(),
    }))
    .with_vector_build_report(Some(VectorBuildPath::Bulk), Some(waiting));
    assert_eq!(
        validating.common().stage,
        IndexOperationStage::ValidateDescriptor
    );
    assert_eq!(validating.common().vector_bulk_build, None);
    assert_eq!(
        validating.common().vector_build_path,
        Some(VectorBuildPath::Bulk)
    );
}

/// Pass 1 reads a label in runs, each of a bounded number of ids and bytes,
/// and every run starts behind the last id the one before read or passed:
/// runs of seven ids, or of a few rows' bytes, link the graph one run links,
/// from a label whose ids are dense or sparse, with rows gone from under
/// their memberships.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_pass_in_many_runs_links_the_graph_one_run_links() {
    let limits = entity_limits(16);
    for (name, stride, run) in [
        (
            "dense-ids",
            1,
            ReadRun {
                ids: 7,
                bytes: READ_RUN_BYTES,
                retry_delay: READ_RUN_RETRY_DELAY,
            },
        ),
        (
            "dense-bytes",
            1,
            ReadRun {
                ids: READ_RUN_IDS,
                bytes: 300,
                retry_delay: READ_RUN_RETRY_DELAY,
            },
        ),
        (
            "sparse-ids",
            5,
            ReadRun {
                ids: 7,
                bytes: READ_RUN_BYTES,
                retry_delay: READ_RUN_RETRY_DELAY,
            },
        ),
        (
            "sparse-bytes",
            5,
            ReadRun {
                ids: READ_RUN_IDS,
                bytes: 300,
                retry_delay: READ_RUN_RETRY_DELAY,
            },
        ),
    ] {
        // The graph of the same label read in one run.
        let mut rows = Vec::new();
        for read_run in [
            ReadRun {
                ids: READ_RUN_IDS,
                bytes: READ_RUN_BYTES,
                retry_delay: READ_RUN_RETRY_DELAY,
            },
            run,
        ] {
            let db = test_db(&format!("vector-bulk-read-runs-{name}-{}", read_run.ids)).await;
            let definition = definition(None);
            let mut expected = seed(
                &db,
                (0..SOURCE).map(|member| (member * stride, vector_of(member * stride))),
            )
            .await;
            // Rows gone from under memberships that stayed, at the start, in
            // the middle and at the end of the label.
            let gone = [0, 3, 4, 100, 101, 102, SOURCE - 1].map(|member| member * stride);
            let mut batch = slatedb::WriteBatch::new();
            for entity_id in gone {
                batch.delete(source_key(SCOPE, entity_id));
                expected.remove(&entity_id);
            }
            db.write(batch).await.expect("vector sources are deleted");
            let last_id = (SOURCE - 1) * stride;
            let (build_id, _, _) = create_build(&db, SCOPE, &definition, last_id).await;
            let mut driver = bulk_driver(1, BUDGET, 16);
            driver.bulk.read_run = read_run;
            assert_eq!(
                drive(&db, &driver, build_id, &mut 1, limits).await.0,
                CommittedOperationStep::Completed,
                "{name}"
            );
            assert_eq!(
                driver.build_reports().path(build_id),
                Some(VectorBuildPath::Bulk),
                "{name}"
            );
            assert_eq!(
                reported_entities(&db, build_id).await,
                SOURCE - gone.len() as u64,
                "{name}"
            );
            let index = active_index(&db, &definition).await;
            assert_index_holds(&db, &index, &expected, &gone).await;
            rows.push(index.stored_rows(&db).await.unwrap());
            db.close().await.expect("vector test database closes");
        }
        assert!(rows[0] == rows[1], "{name}: the runs change no row");
    }
}

/// A run of pass 1 takes no more ids than its bound, whatever the label
/// holds beyond them: once its first row is handed out, the ids left are
/// short of exactly one run's, and the next run starts behind them.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_run_takes_no_more_ids_than_its_bound() {
    const RUN: u64 = 7;
    let db = test_db("vector-bulk-read-run-bound").await;
    seed_standard(&db).await;
    let snapshot = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("the snapshot begins");
    let layout = label_source::label_layout(&snapshot)
        .await
        .expect("the label layout reads");
    let members =
        label_source::label_members(&snapshot, layout, IndexElementKind::Node, "Document", SCOPE)
            .await
            .expect("the label's members read");
    drop(snapshot);
    assert_eq!(members.len(), SOURCE);
    let source = Source {
        db: db.clone(),
        scope: SCOPE,
        kind: IndexElementKind::Node,
        operation_id: IndexOperationId::from_bytes([0x61; 16]).unwrap(),
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        definition: ValidatedVectorIndexDefinition::try_from_runtime(
            &VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("vector definition validates"),
        )
        .expect("V2 vector definition validates"),
        members,
        max_input_bytes: u64::MAX,
        read_run: ReadRun {
            ids: RUN as usize,
            bytes: READ_RUN_BYTES,
            retry_delay: READ_RUN_RETRY_DELAY,
        },
        faults: Arc::default(),
    };
    let mut rows = SourceRows::new(&source, source.members.clone(), usize::MAX, 0);
    // The rows of the first run: the second is not read before they are
    // handed out.
    for entity_id in 0..RUN {
        let row = rows
            .next()
            .await
            .expect("the row reads")
            .expect("the label holds more rows");
        assert_eq!(row.entity_id.get(), entity_id);
        assert_eq!(rows.left.len(), SOURCE - RUN);
        assert_eq!(rows.left.min(), Some(RUN));
    }
    // The first row of the second run, which starts behind the first.
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.entity_id.get(), RUN);
    assert_eq!(rows.left.len(), SOURCE - 2 * RUN);
    drop(rows);
    drop(source);
    db.close().await.expect("vector test database closes");
}

/// A parked build is passed over only while its record is at a scan stage:
/// an abort moves the record to cleanup, which the worker runs at once
/// though the build it replaces still waits.
#[tokio::test(flavor = "multi_thread")]
async fn an_abort_of_a_waiting_build_is_not_parked() {
    let db = test_db("vector-bulk-abort-waiting").await;
    let definition = definition(None);
    seed_standard(&db).await;
    let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
    let driver = bulk_driver(2, BUDGET, 16);
    let limits = entity_limits(16);
    let mut claim_sequence = 1;
    let other_build = Arc::clone(&driver.bulk.preparing)
        .try_acquire_owned()
        .expect("no build is preparing");
    parked_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;

    drop_index_operation(&db, SCOPE, &definition)
        .await
        .expect("building vector converts to abort cleanup");
    let aborting = read_operation(&db, SCOPE, build_id).await;
    assert!(driver.bulk.parked_until(build_id).is_some());
    assert_eq!(driver.parked_until(&aborting), None);
    assert_eq!(
        drive(&db, &driver, build_id, &mut claim_sequence, limits)
            .await
            .0,
        CommittedOperationStep::Completed
    );
    assert!(nothing_held(&driver));
    assert_eq!(driver.bulk.parked_until(build_id), None);
    drop(other_build);
    db.close().await.expect("vector test database closes");
}

/// The step that blocks reads the row again through its own transaction. A
/// row repaired, or deleted, while the build deleted what it had stored and
/// returned to the serial stage blocks nothing: the build starts over, with
/// a first attempt, and is built in bulk.
#[tokio::test(flavor = "multi_thread")]
async fn a_row_repaired_while_the_build_leaves_blocks_nothing() {
    let limits = entity_limits(16);
    for (name, repaired) in [("repaired", true), ("deleted", false)] {
        let db = test_db(&format!("vector-bulk-repaired-leaving-{name}")).await;
        let definition = definition(None);
        let mut expected = seed_standard(&db).await;
        let (build_id, _, _) = create_build(&db, SCOPE, &definition, SOURCE - 1).await;
        let driver = bulk_driver(1, BUDGET, 16);
        let mut claim_sequence = 1;
        drive_steps(
            &db,
            &driver,
            build_id,
            &mut claim_sequence,
            limits,
            COUNT + 1,
        )
        .await;
        db.put(source_key(SCOPE, 100), Bytes::from_static(b"malformed"))
            .await
            .unwrap();
        // The write pass meets the row, the stored rows are deleted and the
        // serial stage is stored: the next step would block.
        while !matches!(
            stored_scan(&db, build_id).await,
            Some(StoredScan::Serial(_))
        ) {
            drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, 1).await;
        }
        assert!(stored_scan(&db, build_id).await.unwrap().is_unstarted());
        let slot = Arc::clone(&driver.bulk.builds.lock()[&build_id]);
        assert!(matches!(&*slot.lock().await, BuildState::Blocking(_)));
        drop(slot);
        if repaired {
            put_source(&db, SCOPE, 100, &properties(vector_of(1_100), None)).await;
            expected.insert(100, vector_of(1_100));
        } else {
            delete_sources(&db, [100]).await;
            expected.remove(&100);
        }
        drive_steps(&db, &driver, build_id, &mut claim_sequence, limits, COUNT).await;
        assert_eq!(attempts(&db, build_id).await, Some(1), "{name}");
        assert_eq!(
            drive(&db, &driver, build_id, &mut claim_sequence, limits)
                .await
                .0,
            CommittedOperationStep::Completed,
            "{name}"
        );
        assert_eq!(
            driver.build_reports().path(build_id),
            Some(VectorBuildPath::Bulk),
            "{name}"
        );
        let absent = if repaired { vec![] } else { vec![100] };
        assert_index_holds(
            &db,
            &active_index(&db, &definition).await,
            &expected,
            &absent,
        )
        .await;
        db.close().await.expect("vector test database closes");
    }
}
