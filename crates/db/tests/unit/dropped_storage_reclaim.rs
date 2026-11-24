//! End-to-end contracts of dropped index storage, through the database
//! handle and its real compactor: what compactions discard and leave row for
//! row, when an entry retires and when it must wait, and what a requested
//! reclaim adds.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use futures::stream::BoxStream;
use helix_ast::batch::{read_batch, write_batch};
use helix_ast::error_code::QueryErrorCode;
use helix_ast::expr::Predicate;
use helix_ast::index::{IndexSpec, VectorDistanceMetric};
use helix_ast::query::QueryRequest;
use helix_ast::traversal::{g, Terminal, Traversal, WriteEnabled};
use helix_ast::value::PropertyInput;
use slatedb::compactor::{Compaction, CompactionSpec, CompactionStatus, SourceId};
use slatedb::config::{
    FlushOptions, FlushType, GarbageCollectorDirectoryOptions, GarbageCollectorOptions,
};
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use slatedb::{CompactionJobContext, DbReadOps, IsolationLevel};

use super::*;
use crate::config::{
    IndexLifecycleThroughputTuning, SearchIndexBackfillLimits, SearchIndexBatchLimits,
    VectorBulkBuildMemory, VectorBulkBuildTuning,
};
use crate::encoding::v2::keys::indexes::vector::VectorStorageLane;
use crate::encoding::v2::keys::scope::TenantId;
use crate::encoding::v2::keys::{DataKey, ManagedIndexKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::encode_metadata_value;
use crate::index_lifecycle::dropped::ownership::OwnerKey;
use crate::index_lifecycle::dropped::reclaim::{
    DroppedStorageReclaim, LeftInPlace, ReclaimObstacle, Reclaimer, Worth,
};
use crate::index_lifecycle::dropped::{self, Armed, QuarantinedDroppedGeneration};
use crate::index_lifecycle::outbox::step_pause::{StepMatcher, StepPause};
use crate::index_lifecycle::{
    DroppedGenerationValue, IndexGenerationId, IndexId, IndexOperationProgress,
    IndexV2MetadataValue, VectorBuildProgress, VectorBuildStage, VectorPhysicalIndexId,
};
use crate::{HelixDB, HelixDbSource, HelixStorage, ProcessLocalDatabaseToken};

const PROPERTY: &str = "embedding";
const DIMENSION: usize = 32;
/// Longest wait for an operation, a compaction or a manifest poll.
const WAIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(50);
/// Two scans this far apart that read the same rows have settled.
const SETTLE: Duration = Duration::from_millis(500);
/// Compactions the default compactor runs at a time.
const SLOTS: usize = 4;

/// Failpoints are process-global, so the contracts that arm one run in their
/// own process.
fn isolated<F: Future<Output = ()>>(contract: impl FnOnce() -> F) {
    crate::test_isolation::in_own_process(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("contract runtime builds")
            .block_on(contract());
    });
}

async fn open(database: &str) -> HelixDB {
    HelixDB::open(HelixDbSource::InMemory {
        database: database.to_string(),
    })
    .await
    .expect("the writer opens")
}

/// Opens a writer whose compaction scheduler never proposes a compaction, so
/// the only compactions are the ones a contract submits.
async fn open_quiet(token: ProcessLocalDatabaseToken) -> HelixDB {
    let never = ["min_compaction_sources", "max_compaction_sources"]
        .map(|option| (option.to_string(), "1000000".to_string()));
    HelixDB::open_with_config(
        HelixDbSource::InMemoryToken { token },
        crate::DbConfig::new().with_slate_settings(slatedb::Settings {
            compactor_options: Some(slatedb::config::CompactorOptions {
                scheduler_options: HashMap::from(never),
                ..slatedb::config::CompactorOptions::default()
            }),
            ..slatedb::Settings::default()
        }),
    )
    .await
    .expect("the writer opens")
}

/// The tables in the store's manifest.
async fn stored_tables(db: &HelixDB) -> HashSet<TableId> {
    admin(db)
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest")
        .all_sst_views()
        .map(|view| TableId(view.sst.id.value().0))
        .collect()
}

/// The instant [`clockless`] puts in place of every clock.
static CLOCKLESS: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

/// `storage` without its clocks, which no contract can predict.
fn clockless(mut storage: DroppedIndexStorage) -> DroppedIndexStorage {
    storage.waiting_since = None;
    storage.stalled_since = None;
    storage.tracking_since = None;
    storage.last_pass = None;
    for quarantined in &mut storage.quarantined {
        quarantined.since = *CLOCKLESS;
    }
    storage
}

fn writer(db: &HelixDB) -> &HelixWriter {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("the contract opens a writer");
    };
    writer
}

/// A vector no other seed's vector is parallel to.
fn vector(seed: usize) -> Vec<f32> {
    (0..DIMENSION)
        .map(|axis| ((seed + 1) as f32 * (axis + 1) as f32 * 0.37).sin())
        .collect()
}

fn spec(label: &str) -> IndexSpec {
    IndexSpec::node_vector(
        label,
        PROPERTY,
        NonZeroUsize::new(DIMENSION).expect("the dimension is nonzero"),
        VectorDistanceMetric::Cosine,
        None::<&str>,
    )
}

/// Writes one node of `label` that carries `vector(seed)`. Returns its id.
async fn add(db: &HelixDB, label: &str, seed: usize) -> u64 {
    let response = db
        .query(QueryRequest::write(
            write_batch()
                .var_as(
                    "node",
                    g().add_n(label, vec![(PROPERTY, PropertyInput::from(vector(seed)))])
                        .id(),
                )
                .returning(["node"]),
        ))
        .await
        .expect("the node is written");
    response["node"][0]
        .as_u64()
        .expect("the write returns its node id")
}

/// The node ids `ids` selects in `scope`, or why the query failed.
async fn selected(db: &HelixDB, scope: DataScope, ids: Traversal<Terminal>) -> Result<Vec<u64>> {
    let response = db
        .query_scoped(
            QueryRequest::read(read_batch().var_as("ids", ids).returning(["ids"])),
            scope,
        )
        .await?;
    Ok(response["ids"]
        .as_array()
        .expect("the query returns ids")
        .iter()
        .map(|id| id.as_u64().expect("ids are numeric"))
        .collect())
}

/// The ids of the `k` nodes of `label` nearest `vector(seed)`, or why the
/// search failed.
async fn searched(db: &HelixDB, label: &str, seed: usize, k: usize) -> Result<Vec<u64>> {
    selected(
        db,
        DataScope::LegacyUnscoped,
        g().vector_search_nodes_with(
            label,
            PROPERTY,
            PropertyInput::from(vector(seed)),
            k,
            None::<PropertyInput>,
        )
        .id(),
    )
    .await
}

/// The ids of the `k` nodes of `label` nearest `vector(seed)`.
async fn nearest(db: &HelixDB, label: &str, seed: usize, k: usize) -> Vec<u64> {
    searched(db, label, seed, k).await.expect("the search runs")
}

/// One index of every family over `label`: a vector index partitioned by
/// `tenant`, an equality index, a range index and a text index.
fn family(label: &str) -> [IndexSpec; 4] {
    [
        IndexSpec::node_vector(
            label,
            PROPERTY,
            NonZeroUsize::new(DIMENSION).expect("the dimension is nonzero"),
            VectorDistanceMetric::Cosine,
            Some("tenant"),
        ),
        IndexSpec::node_equality(label, "category"),
        IndexSpec::node_range(label, "rank"),
        IndexSpec::node_text(label, "body", None::<&str>),
    ]
}

/// Writes one node of `label` in `scope` that every index of [`family`]
/// covers.
async fn add_document(db: &HelixDB, scope: DataScope, label: &str, seed: usize) {
    db.query_scoped(
        QueryRequest::write(
            write_batch()
                .var_as(
                    "node",
                    g().add_n(
                        label,
                        vec![
                            (PROPERTY, PropertyInput::from(vector(seed))),
                            (
                                "tenant",
                                PropertyInput::from(format!("tenant-{}", seed % 3)),
                            ),
                            (
                                "category",
                                PropertyInput::from(format!("category-{}", seed % 4)),
                            ),
                            (
                                "rank",
                                PropertyInput::from(i64::try_from(seed).expect("seeds are small")),
                            ),
                            (
                                "body",
                                PropertyInput::from(format!("common word{seed} group{}", seed % 5)),
                            ),
                        ],
                    )
                    .id(),
                )
                .returning(["node"]),
        ),
        scope,
    )
    .await
    .expect("the node is written");
}

/// What the indexes of [`family`] over `label` answer in `scope`: the nearest
/// nodes of each vector partition, then the nodes of one category, of one
/// rank range and of one text term, those three sorted by id.
async fn family_answers(db: &HelixDB, scope: DataScope, label: &str) -> Vec<Vec<u64>> {
    let mut answers = Vec::new();
    for tenant in 0..3_usize {
        answers.push(
            selected(
                db,
                scope,
                g().vector_search_nodes_with(
                    label,
                    PROPERTY,
                    PropertyInput::from(vector(tenant)),
                    5_usize,
                    Some(PropertyInput::from(format!("tenant-{tenant}"))),
                )
                .id(),
            )
            .await
            .expect("the partition search runs"),
        );
    }
    for unordered in [
        g().n_with_label_where(label, Predicate::eq("category", "category-1"))
            .id(),
        g().n_with_label_where(label, Predicate::between("rank", 5_i64, 15_i64))
            .id(),
        g().text_search_nodes_with(
            label,
            "body",
            PropertyInput::from("group2"),
            100_usize,
            None::<PropertyInput>,
        )
        .id(),
    ] {
        let mut ids = selected(db, scope, unordered)
            .await
            .expect("the lookup runs");
        ids.sort_unstable();
        answers.push(ids);
    }
    answers
}

/// Runs one DDL statement in `scope` and waits for the operation it started
/// to succeed.
async fn applied(db: &HelixDB, scope: DataScope, statement: Traversal<Terminal, WriteEnabled>) {
    let receipt = db
        .query_scoped(
            QueryRequest::write(
                write_batch()
                    .var_as("operation", statement)
                    .returning(["operation"]),
            ),
            scope,
        )
        .await
        .expect("the statement is accepted");
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .unwrap_or_else(|| panic!("the statement starts an operation: {receipt}"));
    tokio::time::timeout(WAIT, async {
        loop {
            let status = db
                .query_scoped(
                    QueryRequest::read(
                        read_batch()
                            .var_as("status", g().get_index_operation(operation_id))
                            .returning(["status"]),
                    ),
                    scope,
                )
                .await
                .expect("the operation status reads");
            match status["status"]["status"].as_str() {
                Some("succeeded") => break,
                Some("queued" | "running") => tokio::time::sleep(POLL).await,
                state => panic!("the operation reached {state:?}"),
            }
        }
    })
    .await
    .expect("the operation succeeds in time");
}

async fn create_index(db: &HelixDB, label: &str) {
    applied(
        db,
        DataScope::LegacyUnscoped,
        g().create_index_if_not_exists(spec(label)),
    )
    .await;
}

async fn drop_index(db: &HelixDB, label: &str) {
    applied(db, DataScope::LegacyUnscoped, g().drop_index(spec(label))).await;
}

/// Every row `read` sees.
async fn rows(read: &(impl DbReadOps + Sync)) -> BTreeMap<Bytes, Bytes> {
    let mut scan = read
        .scan_prefix(Bytes::new(), ..)
        .await
        .expect("the scan opens");
    let mut rows = BTreeMap::new();
    while let Some(row) = scan.next().await.expect("the scan reads") {
        rows.insert(row.key, row.value);
    }
    rows
}

/// The rows the writer reads once background index work has stopped changing
/// them.
async fn settled_rows(db: &HelixDB) -> BTreeMap<Bytes, Bytes> {
    tokio::time::timeout(WAIT, async {
        let mut last = rows(writer(db).db()).await;
        loop {
            tokio::time::sleep(SETTLE).await;
            let next = rows(writer(db).db()).await;
            if next == last {
                break next;
            }
            last = next;
        }
    })
    .await
    .expect("background index work settles in time")
}

async fn stored_entries(db: &HelixDB) -> Vec<DroppedGeneration> {
    dropped::load_entries(writer(db).db())
        .await
        .expect("entries load")
        .into_iter()
        .map(|(entry, _)| entry)
        .collect()
}

/// Arms the entries the drops so far left, as a background pass would.
/// Returns the key prefixes of every row the armed entries give up.
async fn armed_prefixes(db: &HelixDB) -> Vec<Bytes> {
    let writer = writer(db);
    writer
        .dropped()
        .refresh(writer.db())
        .await
        .expect("the stored entries arm");
    writer
        .dropped()
        .armed()
        .iter()
        .flat_map(|(_, armed)| armed.owners.iter())
        .flat_map(|owner| owner.prefixes())
        .collect()
}

/// The writer's retirer.
async fn retirer(db: &HelixDB) -> Arc<Retirer> {
    db.inner
        .dropped_retire
        .lock()
        .await
        .as_ref()
        .expect("an embedded writer retires")
        .retirer()
}

/// Runs one retirement pass, as the background task does every tick.
async fn pass(db: &HelixDB) -> DroppedIndexStorage {
    retirer(db).await.pass().await.expect("the pass runs")
}

/// What the writer's registry holds of its one armed entry.
fn armed_entry(db: &HelixDB) -> Armed {
    let armed = writer(db).dropped().armed();
    let [(_, armed)] = &armed[..] else {
        panic!("one entry is armed: {armed:?}");
    };
    armed.clone()
}

/// Reclaims on request, again while a call's compaction of the sorted runs
/// is lost to one the scheduler picked.
async fn reclaim(db: &HelixDB) -> DroppedStorageReclaim {
    tokio::time::timeout(WAIT, async {
        loop {
            match db
                .reclaim_dropped_index_storage(WAIT)
                .await
                .expect("the reclaim runs")
            {
                DroppedStorageReclaim::Incomplete {
                    obstacle: ReclaimObstacle::Busy | ReclaimObstacle::Rejected,
                    ..
                } => tokio::time::sleep(POLL).await,
                outcome @ (DroppedStorageReclaim::Idle
                | DroppedStorageReclaim::Retained { .. }
                | DroppedStorageReclaim::Incomplete {
                    obstacle: ReclaimObstacle::TimedOut | ReclaimObstacle::Closed,
                    ..
                }
                | DroppedStorageReclaim::Declined { .. }
                | DroppedStorageReclaim::Compacted { .. }) => return outcome,
            }
        }
    })
    .await
    .expect("a reclaim sees its compaction complete in time")
}

/// Reclaims on request until every entry has retired. A call whose
/// compactions leave an entry waiting, for a compaction the scheduler had in
/// flight, is followed by another.
async fn reclaim_every_entry(db: &HelixDB) {
    tokio::time::timeout(WAIT, async {
        loop {
            let outcome = reclaim(db).await;
            assert!(
                matches!(
                    outcome,
                    DroppedStorageReclaim::Idle | DroppedStorageReclaim::Compacted { .. }
                ),
                "{outcome:?}"
            );
            if stored_entries(db).await.is_empty() {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("every entry retires in time");
}

/// Submits a compaction of every table into the oldest run, after a memtable
/// flush, and returns its record without waiting for it.
async fn submit_compaction_of_every_table(db: &HelixDB) -> Compaction {
    let admin = admin(db);
    writer(db)
        .db()
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("the memtable flushes");
    let manifest = admin
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest");
    let sources = manifest
        .l0()
        .iter()
        .map(|view| SourceId::SstView(view.id))
        .chain(
            manifest
                .compacted()
                .iter()
                .map(|run| SourceId::SortedRun(run.id)),
        )
        .collect::<Vec<_>>();
    let destination = manifest
        .compacted()
        .iter()
        .map(|run| run.id)
        .min()
        .unwrap_or(0);
    admin
        .submit_compaction(CompactionSpec::new(sources, destination))
        .await
        .expect("the compaction is submitted")
}

fn admin(db: &HelixDB) -> Admin {
    Admin::builder(
        db.inner.storage.path(),
        Arc::clone(db.inner.storage.object_store()),
    )
    .build()
}

/// Waits for a submitted compaction to finish. Returns whether it completed:
/// one that lost to a compaction the scheduler picked did not.
async fn finished(db: &HelixDB, compaction: &Compaction) -> bool {
    let admin = admin(db);
    loop {
        match admin
            .read_compaction(compaction.id(), None)
            .await
            .expect("the compaction status reads")
            .map(|compaction| compaction.status())
        {
            Some(CompactionStatus::Completed) => break true,
            Some(CompactionStatus::Failed) | None => break false,
            Some(
                CompactionStatus::Submitted
                | CompactionStatus::Scheduled
                | CompactionStatus::Running
                | CompactionStatus::Compacted,
            ) => tokio::time::sleep(POLL).await,
        }
    }
}

/// Compacts every table into the oldest run, as the scheduler might: no
/// retention check, and nothing told of the result. The filter alone decides
/// what the job discards.
async fn compact_every_table(db: &HelixDB) {
    tokio::time::timeout(WAIT, async {
        loop {
            let compaction = submit_compaction_of_every_table(db).await;
            if finished(db, &compaction).await {
                break;
            }
        }
    })
    .await
    .expect("a compaction of every table completes in time");
}

/// Waits until the writer reads exactly `expected`. A writer applies the
/// result of its compactor's job at its next manifest poll, so the rows a job
/// discarded stay readable for a moment after the job completed.
async fn assert_rows(db: &HelixDB, expected: &BTreeMap<Bytes, Bytes>) {
    let deadline = Instant::now() + WAIT;
    let mut actual = rows(writer(db).db()).await;
    while actual != *expected && Instant::now() < deadline {
        tokio::time::sleep(POLL).await;
        actual = rows(writer(db).db()).await;
    }
    let lost = expected
        .keys()
        .filter(|key| !actual.contains_key(*key))
        .collect::<Vec<_>>();
    let added = actual
        .keys()
        .filter(|key| !expected.contains_key(*key))
        .collect::<Vec<_>>();
    let changed = expected
        .iter()
        .filter(|(key, value)| actual.get(*key).is_some_and(|actual| actual != *value))
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    assert!(
        lost.is_empty() && added.is_empty() && changed.is_empty(),
        "lost {lost:?}, added {added:?}, changed {changed:?}"
    );
}

/// Waits until the writer reads no row under `prefixes`.
async fn assert_discarded(db: &HelixDB, prefixes: &[Bytes]) {
    let deadline = Instant::now() + WAIT;
    loop {
        let left = rows(writer(db).db())
            .await
            .into_keys()
            .filter(|key| prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .count();
        if left == 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{left} rows of the dropped generation are still readable"
        );
        tokio::time::sleep(POLL).await;
    }
}

/// Proves a reclaim discards exactly the rows of the dropped generation and
/// its entry. The source rows, the catalog, a second index and a generation
/// rebuilt under the dropped definition while the entry was armed are
/// identical row for row, and both indexes answer as before.
#[tokio::test]
async fn a_reclaim_discards_exactly_the_dropped_generation() {
    let db = open("dropped-reclaim-exact").await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Kept", seed).await;
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Kept").await;
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    create_index(&db, "Gone").await;

    let prefixes = armed_prefixes(&db).await;
    let entries = stored_entries(&db).await;
    let [entry] = entries[..] else {
        panic!("one drop leaves one entry: {entries:?}");
    };
    let before = rows(writer(&db).db()).await;
    let owned = |key: &Bytes| prefixes.iter().any(|prefix| key.starts_with(prefix));
    assert!(
        before.keys().any(owned),
        "the drop left rows of its generation to compactions"
    );
    async fn answers(db: &HelixDB) -> Vec<(Vec<u64>, Vec<u64>)> {
        let mut answers = Vec::new();
        for seed in (0..48).step_by(5) {
            answers.push((
                nearest(db, "Kept", seed, 5).await,
                nearest(db, "Gone", seed, 5).await,
            ));
        }
        answers
    }
    let answered = answers(&db).await;
    assert!(answered
        .iter()
        .all(|(kept, gone)| kept.len() == 5 && gone.len() == 5));

    reclaim_every_entry(&db).await;
    let expected = before
        .into_iter()
        .filter(|(key, _)| *key != entry.key() && !owned(key))
        .collect::<BTreeMap<_, _>>();
    assert_rows(&db, &expected).await;
    assert_eq!(answers(&db).await, answered);
    assert_eq!(
        db.reclaim_dropped_index_storage(WAIT).await.unwrap(),
        DroppedStorageReclaim::Idle
    );
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
    db.close().await.expect("the writer closes");
}

/// Proves no compaction discards a dropped generation's rows while a snapshot
/// or a transaction opened before the drop can read them, and that the
/// reclaim waits for both.
#[tokio::test]
async fn compactions_retain_dropped_rows_an_older_reader_can_see() {
    let db = open("dropped-reclaim-older-readers").await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    let slate = writer(&db).db();
    let snapshot = slate.snapshot().await.expect("the snapshot opens");
    let snapshot_rows = rows(snapshot.as_ref()).await;
    let transaction = slate
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("the transaction begins");
    let transaction_rows = rows(&transaction).await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    assert!(snapshot_rows
        .keys()
        .any(|key| prefixes.iter().any(|prefix| key.starts_with(prefix))));

    // SlateDB publishes the oldest open reader when it flushes a memtable, so
    // each round writes a row for the flush to carry.
    add(&db, "Other", 0).await;
    compact_every_table(&db).await;
    assert_eq!(
        reclaim(&db).await,
        DroppedStorageReclaim::Retained { entries: 1 }
    );
    assert_eq!(rows(snapshot.as_ref()).await, snapshot_rows);
    assert_eq!(rows(&transaction).await, transaction_rows);

    drop(snapshot);
    add(&db, "Other", 1).await;
    compact_every_table(&db).await;
    assert_eq!(
        reclaim(&db).await,
        DroppedStorageReclaim::Retained { entries: 1 }
    );
    assert_eq!(rows(&transaction).await, transaction_rows);

    // Every compaction so far kept the rows, and each one counted.
    assert_eq!(db.dropped_index_storage().retained, 1);
    assert!(armed_entry(&db).kept >= 2);

    drop(transaction);
    add(&db, "Other", 2).await;
    reclaim_every_entry(&db).await;
    assert_discarded(&db, &prefixes).await;
    assert_eq!(stored_entries(&db).await, vec![]);
    db.close().await.expect("the writer closes");
}

/// Proves an entry retires through compactions nobody ran on its behalf, and
/// not before they have discarded its rows: a pass that finds the rows stored
/// tracks the tables that hold them and deletes nothing, and the pass after a
/// compaction of those tables deletes exactly the entry. The source rows,
/// the catalog, a second index and the generation rebuilt under the dropped
/// definition right after the drop are identical row for row.
#[tokio::test]
async fn ordinary_compactions_retire_an_entry_without_a_reclaim() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-retire-ordinary").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Kept", seed).await;
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Kept").await;
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    create_index(&db, "Gone").await;

    let prefixes = armed_prefixes(&db).await;
    let entries = stored_entries(&db).await;
    let [entry] = entries[..] else {
        panic!("one drop leaves one entry: {entries:?}");
    };
    let before = settled_rows(&db).await;
    let owned = |key: &Bytes| prefixes.iter().any(|prefix| key.starts_with(prefix));
    assert!(before.keys().any(owned));
    let answered = (
        nearest(&db, "Kept", 3, 5).await,
        nearest(&db, "Gone", 3, 5).await,
    );

    // Twice: a pass over tables it has already looked into changes nothing.
    let mut suspects = Vec::new();
    for _ in 0..2 {
        let storage = pass(&db).await;
        assert_eq!((storage.tracking, storage.entries()), (1, 1), "{storage:?}");
        let Proof::Tracking { tables } = armed_entry(&db).proof else {
            panic!("the entry's rows are stored, so its proof tracks their tables");
        };
        assert_eq!(storage.tracked_tables, tables.tables().len());
        assert_eq!(storage.tracked_bytes, tables.tables().values().sum::<u64>());
        suspects.push(tables);
    }
    assert_eq!(suspects[0], suspects[1]);
    assert_eq!(stored_entries(&db).await, entries);
    assert_eq!(rows(writer(&db).db()).await, before);

    compact_every_table(&db).await;
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    let expected = before
        .into_iter()
        .filter(|(key, _)| *key != entry.key() && !owned(key))
        .collect::<BTreeMap<_, _>>();
    assert_rows(&db, &expected).await;
    assert_eq!(
        (
            nearest(&db, "Kept", 3, 5).await,
            nearest(&db, "Gone", 3, 5).await
        ),
        answered
    );
    db.close().await.expect("the writer closes");
}

/// Proves an entry whose rows compactions discarded before anything retired
/// it, as after a crash between the two, retires at the first pass of the
/// next writer, with no compaction on its behalf: the pass looks into the
/// tables it finds instead of waiting for them to be rewritten.
#[tokio::test]
async fn an_entry_whose_rows_are_gone_retires_at_the_first_pass_after_a_reopen() {
    let token = ProcessLocalDatabaseToken::new("dropped-retire-rows-gone").unwrap();
    let db = open_quiet(token.clone()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Kept", seed).await;
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Kept").await;
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    compact_every_table(&db).await;
    assert_discarded(&db, &prefixes).await;
    assert_eq!(stored_entries(&db).await.len(), 1);
    db.close().await.expect("the writer closes");

    let db = open_quiet(token).await;
    // The open read no entry: the first pass arms it, and retires it.
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
    let tables = stored_tables(&db).await;
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_eq!(stored_entries(&db).await, vec![]);
    assert_eq!(stored_tables(&db).await, tables, "nothing was compacted");
    assert_eq!(nearest(&db, "Kept", 3, 5).await.len(), 5);
    db.close().await.expect("the writer closes");
}

/// Proves the background task reports a pass that fails and tries again by
/// itself: the handle shows the failure with what it failed with, nothing is
/// armed meanwhile, and the next pass, which nothing but the task's own
/// timer starts, arms the entry and is reported as having read the store.
#[test]
fn a_background_pass_that_fails_is_reported_and_tried_again() {
    isolated(|| async {
        let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-pass-failure").unwrap()).await;
        db.wait_for_background_migrations().await;
        for seed in 0..24 {
            add(&db, "Gone", seed).await;
        }
        create_index(&db, "Gone").await;
        drop_index(&db, "Gone").await;
        assert_eq!(stored_entries(&db).await.len(), 1);
        assert_eq!(db.dropped_index_storage().last_pass, None);

        failpoints::inject_once(IndexOutboxFailpoint::DroppedEntryArmBefore).unwrap();
        // Idle for longer than the contract runs: the task never reclaims.
        retirer(&db)
            .await
            .run_in_background(idle_pace(Duration::from_secs(3_600)));
        let (at, since, error) = tokio::time::timeout(WAIT, async {
            loop {
                match db.dropped_index_storage().last_pass {
                    Some(crate::DroppedStoragePass::Failed { at, since, error }) => {
                        break (at, since, error);
                    }
                    Some(crate::DroppedStoragePass::Completed { .. }) | None => {
                        tokio::time::sleep(POLL).await;
                    }
                }
            }
        })
        .await
        .expect("the task's first pass fails");
        assert!(failpoints::was_triggered());
        assert_eq!(at, since, "the first failure of a row");
        assert!(!error.is_empty());
        assert_eq!(writer(&db).dropped().armed(), vec![]);
        assert_eq!(stored_entries(&db).await.len(), 1);

        // No operation commits and no table changes: the task's retry timer
        // alone starts the pass that works.
        tokio::time::timeout(WAIT, async {
            while !matches!(
                db.dropped_index_storage().last_pass,
                Some(crate::DroppedStoragePass::Completed { .. })
            ) {
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .expect("the task tries again by itself");
        assert_eq!(writer(&db).dropped().armed().len(), 1);
        db.close().await.expect("the writer closes");
    });
}

/// Proves a pass that fails at each point around arming and deleting leaves
/// a store the next pass, of this writer or of the next, finishes: no entry
/// is deleted before its rows are gone, and none is left once they are.
#[test]
fn a_pass_that_fails_at_any_point_is_finished_by_the_next() {
    isolated(|| async {
        // Each failpoint, with whether the pass has armed the entry and
        // how many of the three partition mappings it has left by then.
        for (failpoint, armed, mappings_left) in [
            (IndexOutboxFailpoint::DroppedEntryArmBefore, false, 3),
            (IndexOutboxFailpoint::DroppedEntryDeleteBefore, true, 3),
            (
                IndexOutboxFailpoint::DroppedEntryDeleteAfterMappings,
                true,
                0,
            ),
        ] {
            for reopens in [false, true] {
                let token = ProcessLocalDatabaseToken::new(format!(
                    "dropped-retire-crash-{}-{reopens}",
                    failpoint.as_str()
                ))
                .unwrap();
                let db = open_quiet(token.clone()).await;
                db.wait_for_background_migrations().await;
                let scope = DataScope::LegacyUnscoped;
                for seed in 0..24 {
                    add_document(&db, scope, "Gone", seed).await;
                }
                // The partitioned vector index of the family: its entry
                // retires with its partition mappings.
                let [vector, ..] = family("Gone");
                applied(&db, scope, g().create_index_if_not_exists(vector.clone())).await;
                applied(&db, scope, g().drop_index(vector)).await;
                let entries = stored_entries(&db).await;
                let [entry] = entries[..] else {
                    panic!("one drop leaves one entry: {entries:?}");
                };
                let mappings = dropped::partition_mappings_prefix(scope, entry);
                let mapped = |rows: &BTreeMap<Bytes, Bytes>| {
                    rows.keys().filter(|key| key.starts_with(&mappings)).count()
                };
                assert_eq!(mapped(&rows(writer(&db).db()).await), 3);

                // An armed entry's rows are discarded first, so that the
                // pass gets as far as its deletes.
                let prefixes = if armed {
                    let prefixes = armed_prefixes(&db).await;
                    compact_every_table(&db).await;
                    assert_discarded(&db, &prefixes).await;
                    prefixes
                } else {
                    Vec::new()
                };
                failpoints::inject_once(failpoint).unwrap();
                retirer(&db).await.pass().await.expect_err("the pass fails");
                assert!(failpoints::was_triggered());
                if !armed {
                    assert_eq!(writer(&db).dropped().armed(), vec![]);
                }
                // The mappings go first, and only once nothing reads them:
                // the rows they list are gone.
                assert_eq!(mapped(&rows(writer(&db).db()).await), mappings_left);
                assert_eq!(stored_entries(&db).await, entries);

                let db = if reopens {
                    db.close().await.expect("the writer closes");
                    open_quiet(token).await
                } else {
                    db
                };
                let prefixes = [prefixes, armed_prefixes(&db).await].concat();
                compact_every_table(&db).await;
                assert_eq!(pass(&db).await, DroppedIndexStorage::default());
                assert_eq!(stored_entries(&db).await, vec![]);
                assert_discarded(&db, &prefixes).await;
                assert_eq!(mapped(&rows(writer(&db).db()).await), 0);
                db.close().await.expect("the writer closes");
            }
        }
    });
}

/// An object store through which every read makes a compaction filter keep
/// the rows of every armed entry of `writer`, as a job whose retention
/// sequence is below their gates does when its filter is created.
struct KeepingStore {
    inner: Arc<dyn ObjectStore>,
    writer: Arc<HelixWriter>,
}

impl std::fmt::Debug for KeepingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeepingStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl KeepingStore {
    fn keep(&self) {
        let discardable = self.writer.dropped().discardable(&CompactionJobContext {
            destination: 0,
            is_dest_last_run: true,
            compaction_clock_tick: 0,
            retention_min_seq: None,
        });
        assert!(discardable.is_empty());
    }
}

impl std::fmt::Display for KeepingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "keeping({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for KeepingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.keep();
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.keep();
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.keep();
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Proves a pass deletes nothing for an entry a filter kept after the pass
/// read the entry's proof, though every table it then reads is free of the
/// entry's rows: the stored entry and its partition mappings stay, the proof
/// starts again, and the next undisturbed pass retires the entry.
#[tokio::test]
async fn a_filter_that_keeps_an_entry_while_a_pass_reads_the_store_voids_its_retirement() {
    let db =
        open_quiet(ProcessLocalDatabaseToken::new("dropped-retire-kept-mid-pass").unwrap()).await;
    db.wait_for_background_migrations().await;
    let scope = DataScope::LegacyUnscoped;
    for seed in 0..24 {
        add_document(&db, scope, "Gone", seed).await;
    }
    // The partitioned vector index of the family: a retirement would delete
    // its partition mappings before its entry.
    let [vector, ..] = family("Gone");
    applied(&db, scope, g().create_index_if_not_exists(vector.clone())).await;
    applied(&db, scope, g().drop_index(vector)).await;
    let entries = stored_entries(&db).await;
    let [entry] = entries[..] else {
        panic!("one drop leaves one entry: {entries:?}");
    };
    let mappings = dropped::partition_mappings_prefix(scope, entry);
    let mapped =
        |rows: BTreeMap<Bytes, Bytes>| rows.keys().filter(|key| key.starts_with(&mappings)).count();
    let prefixes = armed_prefixes(&db).await;
    compact_every_table(&db).await;
    assert_discarded(&db, &prefixes).await;
    assert_eq!(armed_entry(&db).kept, 0);

    // The pass reads the entry's proof, then the store: no table holds a row
    // of the entry, and a filter has kept the entry by then.
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    let disturbed = Retirer::new(
        Arc::clone(shared),
        db.inner.storage.path(),
        Arc::new(KeepingStore {
            inner: Arc::clone(db.inner.storage.object_store()),
            writer: Arc::clone(shared),
        }),
        SLOTS,
    );
    let storage = disturbed.pass().await.expect("the pass runs");
    assert_eq!(
        clockless(storage),
        DroppedIndexStorage {
            unproven: 1,
            retained: 1,
            ..DroppedIndexStorage::default()
        }
    );
    assert!(armed_entry(&db).kept > 0);
    assert_eq!(armed_entry(&db).proof, Proof::Unproven);
    assert_eq!(stored_entries(&db).await, entries);
    assert_eq!(mapped(rows(writer(&db).db()).await), 3);

    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_eq!(stored_entries(&db).await, vec![]);
    assert_eq!(mapped(rows(writer(&db).db()).await), 0);
    db.close().await.expect("the writer closes");
}

/// Proves a table is found to hold an entry's row that starts a block whose
/// index key is shorter than the entry's prefix: SlateDB indexes a block by a
/// shortened key, which then sorts below the prefix, so only reading the
/// block tells. An entry whose prefix falls in the same block without a row
/// is not held.
#[tokio::test]
async fn a_row_that_starts_a_block_under_a_shortened_index_key_is_found() {
    let db = open("dropped-retire-block-start").await;
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    // A store of its own, so that the rows below are the whole table.
    let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
    let path = "dropped-retire-block-start-rows";
    let rows = slatedb::Db::builder(path, Arc::clone(&store))
        .build()
        .await
        .expect("the row store opens");
    let owner = |physical_index_id| OwnerKey::PhysicalIndex {
        scope: DataScope::LegacyUnscoped,
        physical_index_id,
    };
    let [held_prefix, ..] = &owner(7).prefixes()[..] else {
        panic!("a physical index owns a prefix");
    };
    let [empty_prefix, ..] = &owner(8).prefixes()[..] else {
        panic!("a physical index owns a prefix");
    };
    let held_row = Bytes::from([&held_prefix[..], b"row"].concat());
    // The first row fills its block, so the second starts the next one.
    rows.put(b"a", vec![0u8; 6000])
        .await
        .expect("the row writes");
    rows.put(&held_row, b"dropped")
        .await
        .expect("the row writes");
    rows.put([0xFFu8], b"last").await.expect("the row writes");
    rows.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("the rows flush");
    let manifest = Admin::builder(path, Arc::clone(&store))
        .build()
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open store has a manifest");
    let tables = manifest
        .all_sst_views()
        .map(|view| view.sst.clone())
        .collect::<Vec<_>>();
    let [table] = &tables[..] else {
        panic!("one flush leaves one table: {}", tables.len());
    };
    assert_eq!(table.info.first_entry.as_deref(), Some(&b"a"[..]));
    assert_eq!(table.info.last_entry.as_deref(), Some(&[0xFFu8][..]));

    let retirer = Retirer::new(Arc::clone(shared), path, store, SLOTS);
    let index = retirer
        .tables
        .open_with_handle(table.clone())
        .expect("the table opens")
        .index()
        .await
        .expect("the block index reads");
    assert_eq!(index.len(), 2);
    let (second_at, second) = index.get(1).expect("the table has a second block");
    assert!(
        second.len() < held_prefix.len() && held_prefix.starts_with(second),
        "the second block is indexed by a key shorter than the prefix: {second:?}"
    );
    // No block is indexed under the prefix, so the estimate of its rows is
    // the one block that can hold them: the second, to the end of the data.
    assert_eq!(table.info.filter_len, 0, "three rows get no filter");
    let second_block = table.info.index_offset - second_at;
    assert!(second_block > 0);

    let prefixes = BTreeMap::from([
        (held_prefix.clone(), generation_entry(1)),
        (empty_prefix.clone(), generation_entry(2)),
    ]);
    assert_eq!(
        retirer
            .held(table, &prefixes)
            .await
            .expect("the table is looked into"),
        HashMap::from([(generation_entry(1), second_block)])
    );
    rows.close().await.expect("the row store closes");
    db.close().await.expect("the writer closes");
}

/// Proves what a table's key range tells of the bytes: a table that lies
/// wholly under one prefix counts in full, with nothing read of it, and a
/// table that only starts under the prefix counts the blocks its rows are
/// in, which is less than the table.
#[tokio::test]
async fn a_table_wholly_under_a_prefix_counts_in_full_and_one_that_starts_under_it_does_not() {
    let db = open("dropped-retire-whole-table").await;
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    let [prefix, ..] = &OwnerKey::PhysicalIndex {
        scope: DataScope::LegacyUnscoped,
        physical_index_id: 7,
    }
    .prefixes()[..] else {
        panic!("a physical index owns a prefix");
    };
    let prefixes = BTreeMap::from([(prefix.clone(), generation_entry(1))]);
    for (name, last_row, whole) in [
        ("whole", [&prefix[..], b"z"].concat(), true),
        ("starts", vec![0xFFu8], false),
    ] {
        // A store of its own, so that the rows below are the whole table.
        let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = format!("dropped-retire-whole-table-{name}");
        let rows = slatedb::Db::builder(path.clone(), Arc::clone(&store))
            .build()
            .await
            .expect("the row store opens");
        rows.put([&prefix[..], b"a"].concat(), vec![7u8; 6000])
            .await
            .expect("the row writes");
        rows.put(&last_row, vec![7u8; 6000])
            .await
            .expect("the row writes");
        rows.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("the rows flush");
        let manifest = Admin::builder(path.clone(), Arc::clone(&store))
            .build()
            .read_manifest(None)
            .await
            .expect("the manifest reads")
            .expect("an open store has a manifest");
        let tables = manifest
            .all_sst_views()
            .map(|view| view.sst.clone())
            .collect::<Vec<_>>();
        let [table] = &tables[..] else {
            panic!("one flush leaves one table: {}", tables.len());
        };
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let table_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let retirer = Retirer::new(
            Arc::clone(shared),
            &path,
            Arc::new(CountingStore {
                inner: store,
                requests: Arc::clone(&requests),
                table_reads: Arc::clone(&table_reads),
            }),
            SLOTS,
        );
        let held = retirer
            .held(table, &prefixes)
            .await
            .expect("the table is looked into");
        let [(entry, bytes)] = held.into_iter().collect::<Vec<_>>()[..] else {
            panic!("the table holds rows of the one entry");
        };
        assert_eq!(entry, generation_entry(1));
        if whole {
            assert_eq!(bytes, table.estimate_size(), "{name}");
            assert_eq!(table_reads.load(Ordering::SeqCst), 0, "{name}");
        } else {
            // The first block of two: the second starts above the prefix.
            assert!(
                0 < bytes && bytes < table.estimate_size() / 2 + 512,
                "{name} {bytes}"
            );
            assert!(table_reads.load(Ordering::SeqCst) > 0, "{name}");
        }
        rows.close().await.expect("the row store closes");
    }
    db.close().await.expect("the writer closes");
}

/// The bytes [`Retirer::held`] counts for the one entry that owns every
/// prefix in `owned`, in a table of one flush that holds `rows` rows under
/// each given prefix and nothing else. Every row fills a block of its own,
/// of [`BLOCK_ROW`] bytes and a little framing.
async fn held_bytes_of_one_table(name: &str, rows: &[(&Bytes, usize)], owned: &[&Bytes]) -> u64 {
    let db = open(name).await;
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    let store: Arc<dyn ObjectStore> = Arc::new(slatedb::object_store::memory::InMemory::new());
    let path = format!("{name}-rows");
    let raw = slatedb::Db::builder(path.clone(), Arc::clone(&store))
        .build()
        .await
        .expect("the row store opens");
    for (prefix, count) in rows {
        for row in 0..*count {
            raw.put(
                [&prefix[..], format!("{row:04}").as_bytes()].concat(),
                vec![7u8; BLOCK_ROW as usize],
            )
            .await
            .expect("the row writes");
        }
    }
    raw.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("the rows flush");
    let manifest = Admin::builder(path.clone(), Arc::clone(&store))
        .build()
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open store has a manifest");
    let tables = manifest
        .all_sst_views()
        .map(|view| view.sst.clone())
        .collect::<Vec<_>>();
    let [table] = &tables[..] else {
        panic!("one flush leaves one table: {}", tables.len());
    };
    let written = rows.iter().map(|(_, count)| *count as u64).sum::<u64>();
    assert!(table.estimate_size() > written * BLOCK_ROW);
    let prefixes = owned
        .iter()
        .map(|prefix| ((*prefix).clone(), generation_entry(1)))
        .collect::<BTreeMap<_, _>>();
    let held = Retirer::new(Arc::clone(shared), &path, store, SLOTS)
        .held(table, &prefixes)
        .await
        .expect("the table is looked into");
    let [(entry, bytes)] = held.into_iter().collect::<Vec<_>>()[..] else {
        panic!("the table holds rows of the one entry");
    };
    assert_eq!(entry, generation_entry(1));
    raw.close().await.expect("the row store closes");
    db.close().await.expect("the writer closes");
    bytes
}

/// A row larger than a block, so that it is stored in a block of its own.
const BLOCK_ROW: u64 = 6000;

/// The prefix of generation `generation` of one index under one record kind:
/// those of consecutive generations sort next to each other.
fn sibling_prefix(generation: u64) -> Bytes {
    OwnerKey::Generation {
        scope: DataScope::LegacyUnscoped,
        index_id: 7,
        generation,
    }
    .prefixes()
    .swap_remove(4)
}

/// Proves a prefix of a held entry that has no row in a table adds nothing
/// to the entry's bytes in it, whether or not the table starts and ends
/// under the entry: the block before where its rows would be belongs to
/// someone else.
#[tokio::test]
async fn a_prefix_without_rows_adds_no_block_to_an_entrys_bytes() {
    let [g2, g3, g4, g5, g6, g7, g8] = [2, 3, 4, 5, 6, 7, 8].map(sibling_prefix);
    // The entry has rows under one prefix, strictly inside the table, and
    // none under two others that sort between rows of other owners.
    let inside = held_bytes_of_one_table(
        "dropped-retire-empty-prefixes-inside",
        &[(&g2, 3), (&g3, 3), (&g5, 1), (&g6, 3), (&g8, 3)],
        &[&g4, &g5, &g7],
    )
    .await;
    assert!(
        (BLOCK_ROW..2 * BLOCK_ROW).contains(&inside),
        "one block: {inside}"
    );
    // The table starts and ends under the entry, which owns a prefix
    // without rows between the rows of two other owners.
    let at_the_ends = held_bytes_of_one_table(
        "dropped-retire-empty-prefixes-ends",
        &[(&g2, 3), (&g3, 3), (&g6, 3), (&g8, 3)],
        &[&g2, &g5, &g8],
    )
    .await;
    assert!(
        (6 * BLOCK_ROW..7 * BLOCK_ROW).contains(&at_the_ends),
        "six blocks: {at_the_ends}"
    );
}

/// Proves every prefix of an entry counts where the table holds its rows,
/// those strictly inside a table that starts and ends under two others of
/// the same entry included, as the physical indexes of a partitioned vector
/// generation lie: the entry is counted every block of its rows, not only
/// those at the table's two ends, and no block of the rows between them
/// that belong to someone else.
#[tokio::test]
async fn a_table_that_starts_and_ends_under_an_entry_counts_its_prefixes_between() {
    let [g2, g3, g4, g5, g6] = [2, 3, 4, 5, 6].map(sibling_prefix);
    let bytes = held_bytes_of_one_table(
        "dropped-retire-prefixes-between",
        &[(&g2, 4), (&g3, 4), (&g4, 4), (&g5, 4), (&g6, 4)],
        &[&g2, &g3, &g5, &g6],
    )
    .await;
    assert!(
        (16 * BLOCK_ROW..17 * BLOCK_ROW).contains(&bytes),
        "sixteen blocks: {bytes}"
    );
}

/// Proves a caller's wait bounds the whole reclaim, the wait for a pass or
/// another reclaim under way included: while one holds the pass lock, a
/// reclaim returns timed out once its wait is over, having looked at
/// nothing and submitted nothing, and so does the removal of a quarantined
/// entry, which answers busy. A reclaim whose wait is already over when it
/// has planned submits nothing either. With the lock free and time to wait
/// it does its work.
#[tokio::test]
async fn a_reclaims_wait_bounds_the_wait_for_a_pass_under_way() {
    let (db, prefixes) = two_runs_and_a_drop("dropped-reclaim-lock-wait").await;
    let retirer = retirer(&db).await;
    let submitted = || async {
        admin(&db)
            .read_compactions(None)
            .await
            .expect("the compactions read")
            .iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .map(|compaction| compaction.id())
            .collect::<HashSet<_>>()
    };
    let before = submitted().await;
    let wait = Duration::from_millis(300);
    let under_way = retirer.pass.lock().await;
    let started = Instant::now();
    let outcome = tokio::time::timeout(
        WAIT,
        retirer.reclaim(tokio::time::Instant::now() + wait, Reclaimer::Caller),
    )
    .await
    .expect("the wait bounds the reclaim though a pass is under way")
    .expect("the reclaim runs");
    assert_eq!(
        outcome,
        DroppedStorageReclaim::Incomplete {
            entries: 1,
            obstacle: ReclaimObstacle::TimedOut,
        }
    );
    assert!(started.elapsed() >= wait, "{:?}", started.elapsed());
    assert_eq!(submitted().await, before);
    let started = Instant::now();
    assert_eq!(
        tokio::time::timeout(
            WAIT,
            db.remove_quarantined_dropped_index_entry(9_000, 1, wait)
        )
        .await
        .expect("the wait bounds the removal though a pass is under way")
        .expect("the removal runs"),
        crate::QuarantinedEntryRemoval::Busy
    );
    assert!(started.elapsed() >= wait, "{:?}", started.elapsed());
    drop(under_way);
    // The lock is free and the plan has a run to rewrite, but the wait is
    // over by the time it would be submitted.
    let outcome = retirer
        .reclaim(tokio::time::Instant::now(), Reclaimer::Caller)
        .await
        .expect("the reclaim runs");
    assert_eq!(
        outcome,
        DroppedStorageReclaim::Incomplete {
            entries: 1,
            obstacle: ReclaimObstacle::TimedOut,
        }
    );
    assert_eq!(submitted().await, before);
    let outcome = retirer
        .reclaim(tokio::time::Instant::now() + WAIT, Reclaimer::Caller)
        .await
        .expect("the reclaim runs");
    assert!(
        matches!(
            outcome,
            DroppedStorageReclaim::Compacted {
                retired: 1,
                entries: 0,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a reclaim does not hold the pass lock while its compaction runs:
/// with a caller's reclaim waiting on a compaction nothing runs, a pass
/// runs at once and arms and counts an index dropped meanwhile, a removal
/// answers, and the reclaim then ends timed out with both entries left.
#[tokio::test]
async fn a_reclaim_gives_the_pass_lock_back_while_its_compaction_runs() {
    let token = ProcessLocalDatabaseToken::new("dropped-reclaim-lock-released").unwrap();
    // A scheduler that proposes nothing and a worker that claims nothing: a
    // submitted compaction stays in flight.
    let db = HelixDB::open_with_object_store_and_config(
        token.database().to_string(),
        token.object_store(),
        crate::DbConfig::new().with_slate_settings(slatedb::Settings {
            compactor_options: Some(slatedb::config::CompactorOptions {
                poll_interval: Duration::from_millis(50),
                scheduler_options: HashMap::from(
                    ["min_compaction_sources", "max_compaction_sources"]
                        .map(|option| (option.to_string(), "1000000".to_string())),
                ),
                worker: Some(slatedb::config::CompactionWorkerOptions {
                    compactions_poll_interval: Duration::from_secs(3_600),
                    ..slatedb::config::CompactionWorkerOptions::default()
                }),
                ..slatedb::config::CompactorOptions::default()
            }),
            ..slatedb::Settings::default()
        }),
    )
    .await
    .expect("the writer opens");
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
        add(&db, "Later", seed).await;
    }
    create_index(&db, "Gone").await;
    create_index(&db, "Later").await;
    drop_index(&db, "Gone").await;
    let retirer = retirer(&db).await;
    assert_eq!(retirer.pass().await.expect("the pass runs").tracking, 1);

    // Far longer than a pass takes, far shorter than the lock would be held.
    let prompt = Duration::from_secs(5);
    let wait = 3 * prompt;
    let reclaim = tokio::spawn({
        let retirer = Arc::clone(&retirer);
        async move {
            retirer
                .reclaim(tokio::time::Instant::now() + wait, Reclaimer::Caller)
                .await
        }
    });
    tokio::time::timeout(WAIT, async {
        while !admin(&db)
            .read_compactions(None)
            .await
            .expect("the compactions read")
            .iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .any(|compaction| compaction.active())
        {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("the reclaim submits its compaction");

    drop_index(&db, "Later").await;
    let storage = tokio::time::timeout(prompt, retirer.pass())
        .await
        .expect("a pass runs while the reclaim's compaction is in flight")
        .expect("the pass runs");
    assert_eq!(
        storage.unproven + storage.draining + storage.tracking,
        2,
        "{storage:?}"
    );
    assert_eq!(writer(&db).dropped().armed().len(), 2);
    assert_eq!(
        tokio::time::timeout(
            prompt,
            db.remove_quarantined_dropped_index_entry(9_000, 1, prompt)
        )
        .await
        .expect("a removal answers while the reclaim's compaction is in flight")
        .expect("the removal runs"),
        crate::QuarantinedEntryRemoval::Absent
    );
    assert!(
        !reclaim.is_finished(),
        "the reclaim still waits for its compaction"
    );
    assert_eq!(
        reclaim
            .await
            .expect("the reclaim task joins")
            .expect("the reclaim runs"),
        DroppedStorageReclaim::Incomplete {
            entries: 2,
            obstacle: ReclaimObstacle::TimedOut,
        }
    );
    db.close().await.expect("the writer closes");
}

/// Proves SlateDB refuses the rewrite of a sorted run that a compaction in
/// flight writes, which a reclaim can come to submit: it reads the store
/// and plans to rewrite a run into itself, the scheduler then schedules a
/// merge into that run, and the reclaim submits what it planned.
///
/// Were both to run, the one that commits second would win. If that is the
/// rewrite, it replaces the merged run with the rows it read before the
/// merge: every row the merge brought in is lost. The reclaim's own look at
/// the compactions in flight cannot close this, since the merge is
/// scheduled after it.
///
/// The refusal is in the SlateDB revision this crate pins from here on:
/// before it this contract fails, with the rewrite scheduled beside the
/// merge.
#[tokio::test]
async fn a_rewrite_planned_before_a_merge_into_its_run_was_scheduled_is_refused() {
    let token = ProcessLocalDatabaseToken::new("dropped-reclaim-overlap-refused").unwrap();
    // One sorted run that holds the rows of a dropped index.
    let first = open_quiet(token.clone()).await;
    first.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&first, "Gone", seed).await;
    }
    create_index(&first, "Gone").await;
    compact_every_table(&first).await;
    drop_index(&first, "Gone").await;
    first.close().await.expect("the first writer closes");

    // A scheduler that proposes nothing and a worker that claims nothing:
    // what is submitted is validated, and then stays in flight.
    let db = HelixDB::open_with_object_store_and_config(
        token.database().to_string(),
        token.object_store(),
        crate::DbConfig::new().with_slate_settings(slatedb::Settings {
            compactor_options: Some(slatedb::config::CompactorOptions {
                poll_interval: Duration::from_millis(50),
                scheduler_options: HashMap::from(
                    ["min_compaction_sources", "max_compaction_sources"]
                        .map(|option| (option.to_string(), "1000000".to_string())),
                ),
                worker: Some(slatedb::config::CompactionWorkerOptions {
                    compactions_poll_interval: Duration::from_secs(3_600),
                    ..slatedb::config::CompactionWorkerOptions::default()
                }),
                ..slatedb::config::CompactorOptions::default()
            }),
            ..slatedb::Settings::default()
        }),
    )
    .await
    .expect("the second writer opens");
    db.wait_for_background_migrations().await;
    assert_eq!(pass(&db).await.tracking, 1);
    // Newer rows, in L0 tables the scheduler will merge into the run.
    add(&db, "Other", 1_000).await;
    flush(&db).await;

    // The reclaim reads the store, and plans from what it read.
    let admin = admin(&db);
    let view = admin
        .read_compactor_state_view()
        .await
        .expect("the store reads");
    let planned = dropped::reclaim::Plan::of(
        &dropped::reclaim::Layout::of(view.manifest()),
        &writer(&db).dropped().discardable_tracked(u64::MAX),
        Reclaimer::Caller,
    );
    let [dropped::reclaim::Rewrite::Runs { ids, into, .. }] = &planned.rewrites[..] else {
        panic!("the plan rewrites the one run into itself: {planned:?}");
    };
    assert_eq!(ids[..], [*into]);

    // The scheduler schedules a merge of the L0 tables into that run.
    // What a submitted compaction is once the coordinator has looked at it.
    let validated = |compaction: &Compaction| {
        let (admin, id) = (&admin, compaction.id());
        async move {
            tokio::time::timeout(WAIT, async {
                loop {
                    let status = admin
                        .read_compaction(id, None)
                        .await
                        .expect("the compaction status reads")
                        .map(|compaction| compaction.status());
                    match status {
                        Some(CompactionStatus::Submitted) => tokio::time::sleep(POLL).await,
                        Some(
                            CompactionStatus::Scheduled
                            | CompactionStatus::Running
                            | CompactionStatus::Compacted
                            | CompactionStatus::Completed
                            | CompactionStatus::Failed,
                        )
                        | None => break status,
                    }
                }
            })
            .await
            .expect("the coordinator validates what is submitted")
        }
    };
    let merge = admin
        .submit_compaction(CompactionSpec::new(
            view.manifest()
                .l0()
                .iter()
                .map(|table| SourceId::SstView(table.id))
                .chain([SourceId::SortedRun(*into)])
                .collect(),
            *into,
        ))
        .await
        .expect("the merge is submitted");
    assert_eq!(validated(&merge).await, Some(CompactionStatus::Scheduled));

    // The reclaim submits the rewrite it planned before the merge existed.
    let rewrite = admin
        .submit_compaction(CompactionSpec::new(
            ids.iter().copied().map(SourceId::SortedRun).collect(),
            *into,
        ))
        .await
        .expect("the rewrite is submitted");
    assert_eq!(
        validated(&rewrite).await,
        Some(CompactionStatus::Failed),
        "a rewrite of a run that a merge in flight writes must be refused"
    );
    assert_eq!(validated(&merge).await, Some(CompactionStatus::Scheduled));
    db.close().await.expect("the second writer closes");
}

fn generation_entry(index_id: u64) -> DroppedGeneration {
    DroppedGeneration {
        index_id: IndexId::new(index_id).unwrap(),
        generation: IndexGenerationId::initial(),
    }
}

/// Proves a writer that opens trusts no table it finds: an entry another
/// writer was already tracking starts unproven again, is tracked again
/// through the tables that hold its rows now, and retires only once those
/// are compacted.
#[tokio::test]
async fn a_reopened_writer_starts_every_proof_again() {
    let token = ProcessLocalDatabaseToken::new("dropped-retire-reopen").unwrap();
    let db = open_quiet(token.clone()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    let entries = stored_entries(&db).await;
    assert_eq!(pass(&db).await.tracking, 1);
    db.close().await.expect("the writer closes");

    let db = open_quiet(token).await;
    // The open read no entry; arming one starts its proof from nothing.
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
    assert_eq!(
        armed_prefixes(&db)
            .await
            .into_iter()
            .collect::<HashSet<_>>(),
        prefixes.iter().cloned().collect::<HashSet<_>>()
    );
    assert_eq!(armed_entry(&db).proof, Proof::Unproven);
    let owned = |rows: BTreeMap<Bytes, Bytes>| {
        rows.into_keys()
            .filter(|key| prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .count()
    };
    let held = owned(rows(writer(&db).db()).await);
    assert!(held > 0);
    assert_eq!(pass(&db).await.tracking, 1);
    assert_eq!(stored_entries(&db).await, entries);
    assert_eq!(owned(rows(writer(&db).db()).await), held);

    compact_every_table(&db).await;
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a writer opens without reading a dropped-generation entry, and
/// that this is safe: after a reopen onto a stored entry its registry is
/// empty, a compaction of every table that runs before the first pass
/// discards no row of the entry, and the store reads as it did. The first
/// pass arms the entry and finds its rows, and the compaction after that
/// discards them.
#[tokio::test]
async fn a_compaction_that_runs_before_a_writers_first_pass_discards_nothing() {
    let token = ProcessLocalDatabaseToken::new("dropped-open-unarmed").unwrap();
    let db = open_quiet(token.clone()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Kept", seed).await;
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Kept").await;
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let entries = stored_entries(&db).await;
    assert_eq!(entries.len(), 1);
    db.close().await.expect("the writer closes");

    let db = open_quiet(token).await;
    // The open armed nothing, and reports nothing.
    assert_eq!(writer(&db).dropped().armed(), vec![]);
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
    assert_eq!(stored_entries(&db).await, entries);
    let before = settled_rows(&db).await;
    let held = owned_rows(&db).await;
    assert!(held > 0);

    // Its filters are created with the registry empty: every row stays,
    // those of the dropped generation included.
    let tables = stored_tables(&db).await;
    compact_every_table(&db).await;
    assert!(tables.is_disjoint(&stored_tables(&db).await));
    assert_rows(&db, &before).await;
    assert_eq!(owned_rows(&db).await, held);
    assert_eq!(writer(&db).dropped().armed(), vec![]);
    assert_eq!(stored_entries(&db).await, entries);

    // The first pass arms the entry and finds the rows where that
    // compaction put them.
    assert_eq!(pass(&db).await.tracking, 1);
    let prefixes = armed_prefixes(&db).await;
    compact_every_table(&db).await;
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_discarded(&db, &prefixes).await;
    assert_eq!(nearest(&db, "Kept", 3, 5).await.len(), 5);
    db.close().await.expect("the writer closes");
}

/// Proves a writer opens a store whose entry the catalog contradicts, and
/// quarantines that entry: no compaction discards a row for it, the handle
/// reports it, and every other entry retires as usual. Here the wrong entry
/// names the live generation of an equality index. Once that generation is
/// really dropped the entry is right, and it is armed and retired.
#[tokio::test]
async fn a_store_with_an_entry_the_catalog_contradicts_opens_and_keeps_its_rows() {
    let token = ProcessLocalDatabaseToken::new("dropped-retire-quarantine").unwrap();
    let db = open_quiet(token.clone()).await;
    db.wait_for_background_migrations().await;
    let scope = DataScope::LegacyUnscoped;
    for seed in 0..24 {
        add_document(&db, scope, "Live", seed).await;
    }
    let [_, equality, ..] = family("Live");
    applied(&db, scope, g().create_index_if_not_exists(equality.clone())).await;
    applied(&db, scope, g().drop_index(equality.clone())).await;
    applied(&db, scope, g().create_index_if_not_exists(equality.clone())).await;
    let entries = stored_entries(&db).await;
    let [dropped_entry] = entries[..] else {
        panic!("one drop leaves one entry: {entries:?}");
    };
    let live = DroppedGeneration {
        generation: dropped_entry.generation.checked_next().unwrap(),
        ..dropped_entry
    };
    let value = DroppedGenerationValue {
        scope,
        rows: DroppedGenerationRows::Secondary,
    };
    writer(&db)
        .db()
        .put(
            live.key(),
            encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(value)),
        )
        .await
        .expect("the fixture entry is written");
    let live_rows = |rows: &BTreeMap<Bytes, Bytes>| {
        let prefixes = OwnerKey::Generation {
            scope,
            index_id: live.index_id.get(),
            generation: live.generation.get(),
        }
        .prefixes();
        rows.iter()
            .filter(|(key, _)| prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    let before = live_rows(&settled_rows(&db).await);
    assert!(
        !before.is_empty(),
        "the live generation has rows of its own"
    );
    async fn answer(db: &HelixDB) -> Vec<u64> {
        let mut ids = selected(
            db,
            DataScope::LegacyUnscoped,
            g().n_with_label_where("Live", Predicate::eq("category", "category-1"))
                .id(),
        )
        .await
        .expect("the lookup runs");
        ids.sort_unstable();
        ids
    }
    let answered = answer(&db).await;
    assert!(!answered.is_empty());
    db.close().await.expect("the writer closes");

    let db = open_quiet(token).await;
    let quarantined = vec![QuarantinedDroppedGeneration {
        index_id: live.index_id.get(),
        generation: live.generation.get(),
        scope,
        rows: dropped::DroppedEntryRows::Secondary,
        contradiction: dropped::DroppedEntryContradiction::GenerationStillOwned,
        since: *CLOCKLESS,
    }];
    // The open read neither entry. The cross-check a pass begins with arms
    // the one that is right and quarantines the other.
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
    armed_prefixes(&db).await;
    assert_eq!(
        clockless(db.dropped_index_storage()),
        DroppedIndexStorage {
            unproven: 1,
            quarantined: quarantined.clone(),
            ..DroppedIndexStorage::default()
        }
    );
    // Every table is rewritten, twice, with the wrong entry stored.
    for _ in 0..2 {
        compact_every_table(&db).await;
        pass(&db).await;
    }
    assert_eq!(
        clockless(db.dropped_index_storage()),
        DroppedIndexStorage {
            quarantined,
            ..DroppedIndexStorage::default()
        }
    );
    assert_eq!(stored_entries(&db).await, vec![live]);
    assert_eq!(live_rows(&settled_rows(&db).await), before);
    assert_eq!(answer(&db).await, answered);

    applied(&db, scope, g().drop_index(equality)).await;
    assert_eq!(pass(&db).await.quarantined, vec![]);
    compact_every_table(&db).await;
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_eq!(live_rows(&settled_rows(&db).await), BTreeMap::new());
    db.close().await.expect("the writer closes");
}

/// Proves writes made while a reclaim rewrites every table all land: every
/// vector written to a live index during the compaction is found afterwards,
/// and so is every vector written before it.
#[tokio::test]
async fn writes_made_during_a_reclaim_all_land() {
    let db = open("dropped-reclaim-concurrent-writes").await;
    db.wait_for_background_migrations().await;
    let mut kept = Vec::new();
    for seed in 0..48 {
        kept.push(add(&db, "Kept", seed).await);
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Kept").await;
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;

    let (outcome, written) = tokio::join!(reclaim(&db), async {
        let mut written = Vec::new();
        for seed in 48..148 {
            written.push(add(&db, "Kept", seed).await);
        }
        written
    });
    assert!(
        matches!(outcome, DroppedStorageReclaim::Compacted { .. }),
        "{outcome:?}"
    );
    reclaim_every_entry(&db).await;
    assert_discarded(&db, &prefixes).await;
    kept.extend(written);
    for (seed, id) in kept.into_iter().enumerate() {
        assert_eq!(nearest(&db, "Kept", seed, 1).await, vec![id], "seed {seed}");
    }
    db.close().await.expect("the writer closes");
}

/// Proves a read-only handle reclaims nothing.
#[tokio::test]
async fn a_reader_reclaims_nothing() {
    let token = ProcessLocalDatabaseToken::new("dropped-reclaim-reader").unwrap();
    HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("the writer opens")
    .close()
    .await
    .expect("the writer closes");
    let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
        .await
        .expect("the reader opens");
    assert!(matches!(
        reader.reclaim_dropped_index_storage(WAIT).await,
        Err(HelixDbError::WriterModeRequired { .. })
    ));
    assert_eq!(
        reader.dropped_index_storage(),
        DroppedIndexStorage::default()
    );
    reader.close().await.expect("the reader closes");
}

/// Builds the indexes of [`family`] over `Kept` and `Gone` in each of
/// `scopes`, drops the four over `Gone` in `dropped_scope` and rebuilds them
/// while their entries wait. Proves a reclaim discards exactly the four
/// dropped generations, the dropped vector generation's partition mappings
/// and the four entries: every other row of every scope, the rebuilt
/// generations included, is identical, and every index answers as before.
async fn a_reclaim_discards_exactly_the_dropped_families(
    database: &str,
    scopes: &[DataScope],
    dropped_scope: DataScope,
) {
    let db = open(database).await;
    db.wait_for_background_migrations().await;
    for scope in scopes.iter().copied() {
        for label in ["Kept", "Gone"] {
            for seed in 0..24 {
                add_document(&db, scope, label, seed).await;
            }
            for spec in family(label) {
                applied(&db, scope, g().create_index_if_not_exists(spec)).await;
            }
        }
    }
    // No compaction discards a row a reader older than the drops can see, so
    // until this snapshot closes every dropped row and every entry stays.
    let older = writer(&db)
        .db()
        .snapshot()
        .await
        .expect("the snapshot opens");
    for spec in family("Gone") {
        applied(&db, dropped_scope, g().drop_index(spec)).await;
    }
    for spec in family("Gone") {
        applied(&db, dropped_scope, g().create_index_if_not_exists(spec)).await;
    }

    let prefixes = armed_prefixes(&db).await;
    let armed = writer(&db).dropped().armed();
    let mut layouts = armed
        .iter()
        .map(|(_, armed)| {
            assert_eq!(armed.value.scope, dropped_scope);
            format!("{:?}", armed.value.rows)
        })
        .collect::<Vec<_>>();
    layouts.sort_unstable();
    assert_eq!(
        layouts,
        ["PartitionedVector", "Secondary", "Secondary", "Text"]
    );
    let mappings = armed
        .iter()
        .filter(|(_, armed)| armed.value.rows == DroppedGenerationRows::PartitionedVector)
        .map(|(entry, _)| dropped::partition_mappings_prefix(dropped_scope, *entry))
        .collect::<Vec<_>>();
    let held = settled_rows(&db).await;
    for (entry, armed) in &armed {
        let owned = armed
            .owners
            .iter()
            .flat_map(|owner| owner.prefixes())
            .collect::<Vec<_>>();
        assert!(
            held.keys()
                .any(|key| owned.iter().any(|prefix| key.starts_with(prefix))),
            "the drop of {entry:?} left its rows to compactions"
        );
    }
    assert_eq!(
        held.keys()
            .filter(|key| mappings.iter().any(|prefix| key.starts_with(prefix)))
            .count(),
        3,
        "the dropped vector generation keeps one mapping per tenant value"
    );
    async fn answers(db: &HelixDB, scopes: &[DataScope]) -> Vec<Vec<Vec<u64>>> {
        let mut answers = Vec::new();
        for scope in scopes.iter().copied() {
            for label in ["Kept", "Gone"] {
                answers.push(family_answers(db, scope, label).await);
            }
        }
        answers
    }
    let answered = answers(&db, scopes).await;
    assert!(answered
        .iter()
        .all(|family| family.iter().map(Vec::len).eq([5, 5, 5, 6, 11, 5])));

    drop(older);
    // SlateDB publishes the oldest open reader when it flushes a memtable, so
    // the flush needs a row to carry.
    add(&db, "Other", 0).await;
    let before = rows(writer(&db).db()).await;
    assert_eq!(stored_entries(&db).await.len(), 4);
    reclaim_every_entry(&db).await;
    let expected = before
        .into_iter()
        .filter(|(key, _)| {
            armed.iter().all(|(entry, _)| *key != entry.key())
                && !prefixes
                    .iter()
                    .chain(&mappings)
                    .any(|prefix| key.starts_with(prefix))
        })
        .collect::<BTreeMap<_, _>>();
    assert_rows(&db, &expected).await;
    assert_eq!(answers(&db, scopes).await, answered);
    assert_eq!(
        db.reclaim_dropped_index_storage(WAIT).await.unwrap(),
        DroppedStorageReclaim::Idle
    );
    db.close().await.expect("the writer closes");
}

#[tokio::test]
async fn a_reclaim_discards_exactly_the_dropped_generations_of_every_family() {
    a_reclaim_discards_exactly_the_dropped_families(
        "dropped-reclaim-families",
        &[DataScope::LegacyUnscoped],
        DataScope::LegacyUnscoped,
    )
    .await;
}

/// The dropped indexes are one tenant's; its neighbours on either side of it
/// in key order hold the same labels, properties and indexes.
#[tokio::test]
async fn a_reclaim_discards_exactly_one_tenant_among_many() {
    let [before, dropped, after] =
        [1_u128, 2, 3].map(|tenant| DataScope::Tenant(TenantId::from_u128(tenant)));
    a_reclaim_discards_exactly_the_dropped_families(
        "dropped-reclaim-tenants",
        &[before, dropped, after],
        dropped,
    )
    .await;
}

/// Proves a read replica never answers from a partly discarded index. Across
/// a drop, a reclaim and a collection of the tables the reclaim replaced,
/// every search a replica answers returns what it returned before the drop,
/// every other search fails with index-not-found, and a snapshot the replica
/// opened before the drop reads the same rows throughout. One replica polls
/// the manifest at the default ten seconds, the other every ten milliseconds.
#[tokio::test]
async fn a_replica_answers_a_dropped_index_in_full_or_not_at_all() {
    const SEEDS: [usize; 4] = [0, 13, 29, 47];
    /// Searches `replica` until the index is gone from it and `collected`
    /// is set. Returns how many searches it answered.
    async fn probed(replica: &HelixDB, full: &[Vec<u64>], collected: &AtomicBool) -> usize {
        let mut answered = 0_usize;
        let mut missing = false;
        while !(missing && collected.load(Ordering::Relaxed)) {
            for (seed, full) in SEEDS.into_iter().zip(full) {
                match searched(replica, "Gone", seed, 5).await {
                    Ok(ids) => {
                        assert!(!missing, "a replica found the dropped index again");
                        assert_eq!(&ids, full, "a replica answered from a discarded index");
                        answered += 1;
                    }
                    Err(error) => {
                        assert_eq!(error.error_code(), QueryErrorCode::IndexNotFound, "{error}");
                        missing = true;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        answered
    }

    let token = ProcessLocalDatabaseToken::new("dropped-reclaim-replica").unwrap();
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("the writer opens");
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    writer(&db)
        .db()
        .flush()
        .await
        .expect("the index becomes replica-visible");
    let mut full = Vec::new();
    for seed in SEEDS {
        full.push(nearest(&db, "Gone", seed, 5).await);
    }
    let lagging = HelixDB::open_reader(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("the lagging replica opens");
    let prompt = HelixDB::open_reader_with_object_store_for_tests(
        token.database().to_string(),
        token.object_store(),
    )
    .await
    .expect("the prompt replica opens");
    for replica in [&lagging, &prompt] {
        tokio::time::timeout(WAIT, async {
            while searched(replica, "Gone", SEEDS[0], 5).await.ok().as_ref() != Some(&full[0]) {
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .expect("a replica sees the built index in time");
    }
    let HelixStorage::Reader(lagging_reader) = lagging.storage() else {
        panic!("the contract opens a replica");
    };
    let snapshot = lagging_reader
        .snapshot()
        .await
        .expect("the replica snapshot opens");
    let snapshot_rows = rows(snapshot.as_ref()).await;

    let collected = AtomicBool::new(false);
    let (lagging_answered, prompt_answered, ()) = tokio::time::timeout(WAIT, async {
        tokio::join!(
            probed(&lagging, &full, &collected),
            probed(&prompt, &full, &collected),
            async {
                tokio::time::sleep(POLL).await;
                drop_index(&db, "Gone").await;
                let prefixes = armed_prefixes(&db).await;
                add(&db, "Other", 0).await;
                reclaim_every_entry(&db).await;
                assert_discarded(&db, &prefixes).await;
                let every_file = Some(GarbageCollectorDirectoryOptions {
                    interval: None,
                    min_age: Duration::ZERO,
                    dry_run: false,
                });
                admin(&db)
                    .run_gc_once(GarbageCollectorOptions {
                        manifest_options: every_file,
                        wal_options: every_file,
                        compacted_options: every_file,
                        ..GarbageCollectorOptions::default()
                    })
                    .await
                    .expect("the collection runs");
                collected.store(true, Ordering::Relaxed);
            }
        )
    })
    .await
    .expect("both replicas see the drop in time");
    assert!(lagging_answered > 0 && prompt_answered > 0);
    assert_eq!(rows(snapshot.as_ref()).await, snapshot_rows);
    drop(snapshot);
    lagging.close().await.expect("the lagging replica closes");
    prompt.close().await.expect("the prompt replica closes");
    db.close().await.expect("the writer closes");
}

/// Proves node writes and the text compactions they start, racing a text
/// drop, leave nothing of the dropped generation behind: once its entry is
/// reclaimed no row of the generation and no compaction pointer to it is
/// written again.
#[tokio::test]
async fn a_text_drop_racing_text_compactions_leaves_nothing_behind() {
    let db = open("dropped-reclaim-text-race").await;
    db.wait_for_background_migrations().await;
    let scope = DataScope::LegacyUnscoped;
    let text = || IndexSpec::node_text("Gone", "body", None::<&str>);
    for seed in 0..64 {
        add_document(&db, scope, "Gone", seed).await;
    }
    applied(&db, scope, g().create_index_if_not_exists(text())).await;
    tokio::join!(
        async {
            for seed in 64..320 {
                add_document(&db, scope, "Gone", seed).await;
            }
        },
        async {
            tokio::time::sleep(2 * POLL).await;
            applied(&db, scope, g().drop_index(text())).await;
        }
    );

    let prefixes = armed_prefixes(&db).await;
    assert_eq!(stored_entries(&db).await.len(), 1);
    reclaim_every_entry(&db).await;
    for seed in 320..352 {
        add_document(&db, scope, "Gone", seed).await;
    }
    compact_every_table(&db).await;
    let pointers = crate::encoding::v2::keys::GlobalKey::logical_prefix(
        crate::encoding::v2::keys::GlobalKind::TextCompactionPointer,
    );
    let left = settled_rows(&db)
        .await
        .into_keys()
        .filter(|key| {
            key.starts_with(&pointers) || prefixes.iter().any(|prefix| key.starts_with(prefix))
        })
        .collect::<Vec<_>>();
    assert_eq!(left, Vec::<Bytes>::new());
    db.close().await.expect("the writer closes");
}

/// Proves a compaction that kept an entry's rows for a reader older than the
/// drop proves nothing, though it replaces every table the proof was
/// tracking: the entry stays, its proof starts again from the tables that
/// hold the rows now, and it retires only after a compaction that discards
/// them.
#[tokio::test]
async fn a_compaction_that_kept_the_rows_restarts_the_proof() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-retire-kept").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    let older = writer(&db)
        .db()
        .snapshot()
        .await
        .expect("the snapshot opens");
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    let entries = stored_entries(&db).await;
    let owned = |rows: BTreeMap<Bytes, Bytes>| {
        rows.into_keys()
            .filter(|key| prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .count()
    };
    let held = owned(rows(writer(&db).db()).await);
    assert!(held > 0);

    assert_eq!(pass(&db).await.tracking, 1);
    let Proof::Tracking { tables: tracked } = armed_entry(&db).proof else {
        panic!("the entry's rows are stored, so its proof tracks their tables");
    };
    // SlateDB publishes the oldest open reader when it flushes a memtable, so
    // the flush needs a row to carry.
    add(&db, "Other", 0).await;
    compact_every_table(&db).await;
    let stored = stored_tables(&db).await;
    assert!(tracked.tables().keys().all(|table| !stored.contains(table)));
    assert_eq!(
        armed_entry(&db),
        Armed {
            kept: armed_entry(&db).kept,
            proof: Proof::Unproven,
            ..armed_entry(&db)
        }
    );
    assert!(armed_entry(&db).kept > 0);
    assert_eq!(db.dropped_index_storage().retained, 1);

    assert_eq!(pass(&db).await.tracking, 1);
    let Proof::Tracking { tables: kept } = armed_entry(&db).proof else {
        panic!("the compaction kept the rows, so its output is tracked");
    };
    assert!(kept
        .tables()
        .keys()
        .all(|table| !tracked.tables().contains_key(table)));
    assert_eq!(stored_entries(&db).await, entries);
    assert_eq!(owned(rows(writer(&db).db()).await), held);
    assert_eq!(owned(rows(older.as_ref()).await), held);

    drop(older);
    add(&db, "Other", 1).await;
    compact_every_table(&db).await;
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a compaction whose filters were created before an entry was armed
/// proves nothing, though it commits after the proof has started and
/// replaces every table that held the entry's rows then: the proof waits for
/// it, then tracks its output, which still holds the rows.
#[tokio::test]
async fn a_compaction_in_flight_when_an_entry_is_armed_is_waited_for() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-retire-in-flight").unwrap()).await;
    db.wait_for_background_migrations().await;
    let admin = admin(&db);
    // A compaction commits within about a second of writing its output, so
    // a round can miss the window in between; the next round tries again.
    let mut observed = false;
    for round in 0..8 {
        let label = format!("Gone{round}");
        for seed in 0..48 {
            add(&db, &label, seed).await;
        }
        create_index(&db, &label).await;
        drop_index(&db, &label).await;
        let entries = stored_entries(&db).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(writer(&db).dropped().armed(), vec![]);

        // Its filters are created, and its output written, with the entry
        // not armed: it keeps the rows.
        let compaction = submit_compaction_of_every_table(&db).await;
        let status = || async {
            admin
                .read_compaction(compaction.id(), None)
                .await
                .expect("the compaction status reads")
                .map(|compaction| compaction.status())
        };
        while matches!(
            status().await,
            Some(
                CompactionStatus::Submitted
                    | CompactionStatus::Scheduled
                    | CompactionStatus::Running
            )
        ) {
            tokio::task::yield_now().await;
        }
        let before = stored_tables(&db).await;
        let prefixes = armed_prefixes(&db).await;
        pass(&db).await;
        let proof = armed_entry(&db).proof;
        let in_flight = status().await == Some(CompactionStatus::Compacted);
        assert!(finished(&db, &compaction).await);
        let owned = rows(writer(&db).db())
            .await
            .into_keys()
            .filter(|key| prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .count();
        assert!(owned > 0, "the compaction kept the rows");
        if in_flight {
            observed = true;
            assert_eq!(
                proof,
                Proof::Draining {
                    compactions: dropped::Waiting::new(HashSet::from([CompactionId(
                        compaction.id().0
                    )]))
                    .expect("one compaction is waited for")
                }
            );
            // Every table the store had when the proof started is gone.
            assert!(before.is_disjoint(&stored_tables(&db).await));
            assert_eq!(pass(&db).await.tracking, 1);
            assert_eq!(stored_entries(&db).await, entries);
        }

        compact_every_table(&db).await;
        assert_eq!(pass(&db).await, DroppedIndexStorage::default());
        assert_discarded(&db, &prefixes).await;
        if observed {
            break;
        }
    }
    assert!(
        observed,
        "no round armed its entry while a compaction was in flight"
    );
    db.close().await.expect("the writer closes");
}

/// Proves a reclaim returns when its caller's wait runs out, and that the
/// compaction it submitted goes on: the entry retires through it.
#[tokio::test]
async fn a_reclaim_that_outlives_its_wait_returns_and_its_compaction_goes_on() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-reclaim-timeout").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    let submitted = || async {
        admin(&db)
            .read_compactions(None)
            .await
            .expect("the compactions read")
            .iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .count()
    };
    let before = submitted().await;
    // Time to plan and submit, and far too little for the compaction, which
    // is validated, claimed and committed at polls a second and more apart.
    // A reclaim so slow that its wait is over before it submits submits
    // nothing, and is asked again.
    let wait = Duration::from_millis(300);
    tokio::time::timeout(WAIT, async {
        while submitted().await == before {
            assert_eq!(
                db.reclaim_dropped_index_storage(wait).await.unwrap(),
                DroppedStorageReclaim::Incomplete {
                    entries: 1,
                    obstacle: ReclaimObstacle::TimedOut,
                }
            );
        }
    })
    .await
    .expect("a reclaim submits its compaction within its wait");
    assert_eq!(stored_entries(&db).await.len(), 1);
    tokio::time::timeout(WAIT, async {
        while pass(&db).await != DroppedIndexStorage::default() {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("the submitted compaction retires the entry in time");
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a writer that runs no compaction worker of its own does nothing
/// with dropped generations but leave their entries: it runs no retirement
/// task, so it arms none, counts none, and refuses a reclaim and a removal;
/// its handle says that its compactor is the reason.
#[tokio::test]
async fn a_writer_without_a_compaction_worker_arms_and_retires_nothing() {
    let db = HelixDB::open_with_config(
        HelixDbSource::InMemory {
            database: "dropped-retire-no-worker".to_string(),
        },
        crate::DbConfig::new().with_slate_settings(slatedb::Settings {
            compactor_options: Some(slatedb::config::CompactorOptions {
                worker: None,
                ..slatedb::config::CompactorOptions::default()
            }),
            ..slatedb::Settings::default()
        }),
    )
    .await
    .expect("the writer opens");
    db.wait_for_background_migrations().await;
    for seed in 0..8 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    assert!(db.inner.dropped_retire.lock().await.is_none());
    assert!(matches!(
        db.reclaim_dropped_index_storage(WAIT).await,
        Err(HelixDbError::Config(_))
    ));
    assert!(matches!(
        db.remove_quarantined_dropped_index_entry(1, 1, WAIT).await,
        Err(HelixDbError::Config(_))
    ));
    // Long enough for a first pass, had there been a task to run one.
    tokio::time::sleep(QUIET).await;
    assert_eq!(writer(&db).dropped().armed(), vec![]);
    assert_eq!(
        db.dropped_index_storage(),
        DroppedIndexStorage {
            compactor: crate::DroppedStorageCompactor::None,
            ..DroppedIndexStorage::default()
        }
    );
    assert_eq!(stored_entries(&db).await.len(), 1);
    db.close().await.expect("the writer closes");
}

/// Proves a retired partitioned generation loses every partition mapping,
/// however many writes that takes, and no mapping of another generation.
#[tokio::test]
async fn a_retirement_deletes_every_partition_mapping_of_its_generation_only() {
    let db = open("dropped-reclaim-mappings").await;
    db.wait_for_background_migrations().await;
    let scope = DataScope::LegacyUnscoped;
    let generation = |generation| DroppedGeneration {
        index_id: IndexId::new(900).unwrap(),
        generation: IndexGenerationId::new(generation).unwrap(),
    };
    let mappings = |entry| {
        let prefix = dropped::partition_mappings_prefix(scope, entry);
        (0..2 * MAPPING_DELETE_BATCH as u32 + 3)
            .map(|partition: u32| Bytes::from([&prefix[..], &partition.to_be_bytes()].concat()))
            .collect::<Vec<_>>()
    };
    let storage = writer(&db).db();
    let mut written = WriteBatch::new();
    for key in mappings(generation(1))
        .into_iter()
        .chain(mappings(generation(2)))
    {
        written.put(key, b"mapping");
    }
    storage.write(written).await.expect("the mappings write");
    let before = rows(storage).await;

    retirer(&db)
        .await
        .delete_partition_mappings(generation(1), scope)
        .await
        .expect("the mappings delete");
    let deleted = mappings(generation(1));
    let expected = before
        .into_iter()
        .filter(|(key, _)| !deleted.contains(key))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows(storage).await, expected);
    assert_eq!(
        expected
            .keys()
            .filter(|key| mappings(generation(2)).contains(key))
            .count(),
        2 * MAPPING_DELETE_BATCH + 3
    );
    db.close().await.expect("the writer closes");
}

/// Proves a supervisor dropped without `stop` ends its task: nothing keeps
/// the retirer, and the writer it holds, alive behind a handle that was
/// never closed.
#[tokio::test]
async fn a_dropped_supervisor_releases_its_retirer() {
    let db = open("dropped-retire-supervisor-drop").await;
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    let supervisor = RetireSupervisor::start(Retirer::new(
        Arc::clone(shared),
        db.inner.storage.path(),
        Arc::clone(db.inner.storage.object_store()),
        SLOTS,
    ));
    let retirer = Arc::downgrade(&supervisor.retirer());
    assert_eq!(retirer.strong_count(), 2, "the supervisor and its task");
    drop(supervisor);
    tokio::time::timeout(WAIT, async {
        while retirer.strong_count() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the task ends and releases the retirer");
    db.close().await.expect("the writer closes");
}

/// An object store that counts the requests read through it, and those of
/// them that read a table.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    requests: Arc<std::sync::atomic::AtomicUsize>,
    table_reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingStore {
    fn count(&self, location: Option<&Path>) {
        self.requests.fetch_add(1, Ordering::SeqCst);
        if location.is_some_and(|location| location.as_ref().ends_with(".sst")) {
            self.table_reads.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "counting({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.count(None);
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.count(None);
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.count(Some(location));
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.count(None);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.count(None);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.count(None);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.count(None);
        self.inner.copy_opts(from, to, options).await
    }
}

/// A retirer over `db` that reads object storage through a counter. Returns
/// it with the count of its requests and of those that read a table.
fn counted_retirer(
    db: &HelixDB,
) -> (
    Retirer,
    Arc<std::sync::atomic::AtomicUsize>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let table_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retirer = Retirer::new(
        Arc::clone(shared),
        db.inner.storage.path(),
        Arc::new(CountingStore {
            inner: Arc::clone(db.inner.storage.object_store()),
            requests: Arc::clone(&requests),
            table_reads: Arc::clone(&table_reads),
        }),
        SLOTS,
    );
    (retirer, requests, table_reads)
}

/// Shortest time between two passes of a contract's background task.
const GAP: Duration = Duration::from_millis(10);
/// Long enough for a background task that had anything to do to have done
/// it many times over.
const QUIET: Duration = Duration::from_millis(600);

/// Waits until `db` stores no dropped-generation entry.
/// A background pace that reclaims a store idle for `idle_after`, and one
/// that is not idle never.
fn idle_pace(idle_after: Duration) -> Pace {
    Pace {
        look_gap: GAP,
        idle_after,
    }
}

async fn retired(db: &HelixDB) {
    tokio::time::timeout(WAIT, async {
        while !stored_entries(db).await.is_empty() {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("every entry retires in time");
}

/// The rows `db` stores that some index generation or vector physical index
/// owns, live or dropped.
async fn owned_rows(db: &HelixDB) -> usize {
    rows(writer(db).db())
        .await
        .keys()
        .filter(|key| dropped::ownership::owned_row(key).is_some())
        .count()
}

/// Proves a drop on a store that then takes no write gives its storage back
/// with no one asking: the background task is told of the drop by the index
/// worker, arms the entry, finds its rows in tables nothing is going to
/// compact, and once the store has been idle reclaims and retires it.
#[tokio::test]
async fn an_idle_store_reclaims_a_drop_by_itself() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-idle-reclaim").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    assert!(owned_rows(&db).await > 0);
    retirer(&db)
        .await
        .run_in_background(idle_pace(Duration::from_millis(200)));
    // The task has nothing to look at yet.
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        clockless(db.dropped_index_storage()),
        DroppedIndexStorage::default()
    );
    // Its first pass ran all the same, and read the store.
    assert!(matches!(
        db.dropped_index_storage().last_pass,
        Some(crate::DroppedStoragePass::Completed { .. })
    ));

    drop_index(&db, "Gone").await;
    retired(&db).await;
    assert_eq!(
        clockless(db.dropped_index_storage()),
        DroppedIndexStorage::default()
    );
    assert_eq!(owned_rows(&db).await, 0);
    assert!(searched(&db, "Gone", 3, 1).await.is_err());
    db.close().await.expect("the writer closes");
}

/// Proves a writer that opens onto a stored entry needs no one to tell it:
/// the open arms the entry, the background task looks at the store at once,
/// and, the store staying idle, reclaims and retires it.
#[tokio::test]
async fn a_writer_that_opens_onto_an_entry_retires_it_by_itself() {
    let token = ProcessLocalDatabaseToken::new("dropped-background-reopen").unwrap();
    let db = open_quiet(token.clone()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let entries = stored_entries(&db).await;
    assert_eq!(entries.len(), 1);
    db.close().await.expect("the writer closes");

    let db = open_quiet(token).await;
    // The open read no entry.
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
    assert!(owned_rows(&db).await > 0);
    // Nothing happens in this process that the task could be told of: its
    // first pass, due as it starts, arms the entry.
    retirer(&db)
        .await
        .run_in_background(idle_pace(Duration::from_millis(200)));
    retired(&db).await;
    assert_eq!(
        clockless(db.dropped_index_storage()),
        DroppedIndexStorage::default()
    );
    assert_eq!(owned_rows(&db).await, 0);
    db.close().await.expect("the writer closes");
}

/// Proves the background task reads object storage only when it is told of
/// a change: with an entry whose proof tracks tables, or one that is
/// quarantined, a store where nothing happens costs no request, and the
/// task does not reclaim before the store has been idle long enough. A
/// table that leaves the manifest wakes it, and it retires the entry.
#[tokio::test]
async fn a_background_task_reads_nothing_while_nothing_changes() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-background-quiet").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    // An entry no refresh will arm, beside the armed one.
    let [gone] = stored_entries(&db).await[..] else {
        panic!("one drop leaves one entry");
    };
    let unreached = DroppedGeneration {
        generation: gone.generation.checked_next().unwrap(),
        ..gone
    };
    writer(&db)
        .db()
        .put(
            unreached.key(),
            encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(
                DroppedGenerationValue {
                    scope: DataScope::LegacyUnscoped,
                    rows: DroppedGenerationRows::Secondary,
                },
            )),
        )
        .await
        .expect("the fixture entry is written");

    let (retirer, requests, _) = counted_retirer(&db);
    // Idle for longer than the contract runs: the task never reclaims.
    retirer.run_in_background(idle_pace(Duration::from_secs(3_600)));
    let supervisor = RetireSupervisor::start(retirer);
    // The first pass is due at once, for the entry the open armed.
    tokio::time::timeout(WAIT, async {
        while db.dropped_index_storage().tracking != 1 {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("the task finds the tables that hold the entry's rows");
    let storage = db.dropped_index_storage();
    assert_eq!(storage.quarantined.len(), 1, "{storage:?}");
    let settled = requests.load(Ordering::SeqCst);
    assert!(settled > 0, "the pass read the store");
    tokio::time::sleep(QUIET).await;
    assert_eq!(requests.load(Ordering::SeqCst), settled);
    assert_eq!(db.dropped_index_storage(), storage);
    assert_eq!(stored_entries(&db).await, vec![gone, unreached]);

    // A write that flushes changes the manifest, but no tracked table has
    // left it: still nothing to read.
    add(&db, "Other", 0).await;
    writer(&db)
        .db()
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("the memtable flushes");
    tokio::time::sleep(QUIET).await;
    assert_eq!(requests.load(Ordering::SeqCst), settled);

    // The tables the proof tracks are compacted away: the task is told by
    // the writer's manifest, looks, and retires the entry.
    compact_every_table(&db).await;
    tokio::time::timeout(WAIT, async {
        while stored_entries(&db).await != vec![unreached] {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("the armed entry retires by itself");
    assert_discarded(&db, &prefixes).await;
    assert!(requests.load(Ordering::SeqCst) > settled);

    // Only the quarantined entry is left: nothing waits, nothing is read.
    tokio::time::sleep(QUIET).await;
    let settled = requests.load(Ordering::SeqCst);
    compact_every_table(&db).await;
    add(&db, "Other", 1).await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(requests.load(Ordering::SeqCst), settled);
    assert_eq!(db.dropped_index_storage().quarantined.len(), 1);
    supervisor.stop().await;
    db.close().await.expect("the writer closes");
}

/// Proves a store whose compactor has work is not idle, and that a store
/// that is not idle is not reclaimed however long its rows wait: the
/// background task submits nothing while a compaction job of this process
/// is running, nor while jobs keep starting and ending, for many times the
/// idle time, and it reclaims once they have stopped for the idle time.
#[tokio::test]
async fn a_store_whose_compactor_has_work_is_not_reclaimed() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-idle-not-yet").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    assert_eq!(pass(&db).await.tracking, 1);
    let held = owned_rows(&db).await;
    assert!(held > 0);
    // A compaction job that discards the entry's rows starts, and runs on.
    let registry = writer(&db).dropped();
    let job = CompactionJobContext {
        destination: 0,
        is_dest_last_run: true,
        compaction_clock_tick: 0,
        retention_min_seq: Some(u64::MAX),
    };
    assert!(!registry.discardable(&job).is_empty());
    // A reclaim shows in `.compactions` the moment it submits a compaction,
    // seconds before a worker claims it and a row goes.
    let submitted = || async {
        admin(&db)
            .read_compactions(None)
            .await
            .expect("the compactions read")
            .map_or(0, |compactions| compactions.recent_compactions().count())
    };
    let before = submitted().await;
    let idle_after = Duration::from_millis(300);
    retirer(&db).await.run_in_background(idle_pace(idle_after));
    tokio::time::sleep(idle_after * 5).await;
    assert_eq!(submitted().await, before);
    assert_eq!(stored_entries(&db).await.len(), 1);
    assert_eq!(owned_rows(&db).await, held);

    // It ends, and short jobs follow one another more often than the idle
    // time: none is running whenever the task looks, yet it is not idle.
    registry.filter_dropped();
    let busy = Instant::now();
    while busy.elapsed() < idle_after * 5 {
        registry.discardable(&job);
        registry.filter_dropped();
        tokio::time::sleep(idle_after / 4).await;
    }
    assert_eq!(submitted().await, before);
    assert_eq!(stored_entries(&db).await.len(), 1);
    assert_eq!(db.dropped_index_storage().tracking, 1);
    assert_eq!(owned_rows(&db).await, held);

    // The compactor rests: the store is reclaimed.
    retired(&db).await;
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a reclaim that cannot make progress is not repeated on a store
/// that stays still: while a reader older than the drop is open the idle
/// reclaim retains the rows, and the background task then reads nothing
/// until the store changes. Once the reader is gone and the store has
/// changed and been idle again, it reclaims and the entry retires.
#[tokio::test]
async fn an_idle_reclaim_that_retains_the_rows_is_not_repeated() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-idle-retained").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    let older = writer(&db)
        .db()
        .snapshot()
        .await
        .expect("the snapshot opens");
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    let held = owned_rows(&db).await;

    let (retirer, requests, _) = counted_retirer(&db);
    retirer.run_in_background(idle_pace(Duration::from_millis(200)));
    let supervisor = RetireSupervisor::start(retirer);
    // Long enough for the first reclaim and, were it repeated, many more.
    tokio::time::sleep(QUIET * 3).await;
    let settled = requests.load(Ordering::SeqCst);
    assert!(settled > 0);
    tokio::time::sleep(QUIET * 2).await;
    assert_eq!(requests.load(Ordering::SeqCst), settled);
    assert_eq!(stored_entries(&db).await.len(), 1);
    assert_eq!(owned_rows(&db).await, held);

    drop(older);
    // SlateDB publishes the oldest open reader when it flushes a memtable
    // that holds rows; the flush is also the change that ends the wait.
    add(&db, "Other", 0).await;
    writer(&db)
        .db()
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("the memtable flushes");
    retired(&db).await;
    assert_discarded(&db, &prefixes).await;
    supervisor.stop().await;
    db.close().await.expect("the writer closes");
}

/// Proves what a pass reads of a table is read once: a proof that starts
/// again looks into the same tables without reading any of them again.
#[tokio::test]
async fn a_proof_that_starts_again_reads_no_table_twice() {
    let db =
        open_quiet(ProcessLocalDatabaseToken::new("dropped-retire-table-cache").unwrap()).await;
    db.wait_for_background_migrations().await;
    let scope = DataScope::LegacyUnscoped;
    // Several generations of several families in one table, so the owner
    // prefixes of each entry fall inside the table and inside its blocks.
    for seed in 0..64 {
        add_document(&db, scope, "Gone", seed).await;
        add_document(&db, scope, "Kept", seed).await;
    }
    for spec in family("Kept") {
        applied(&db, scope, g().create_index_if_not_exists(spec)).await;
    }
    for spec in family("Gone") {
        applied(&db, scope, g().create_index_if_not_exists(spec.clone())).await;
        applied(&db, scope, g().drop_index(spec)).await;
    }
    compact_every_table(&db).await;
    let (retirer, _, table_reads) = counted_retirer(&db);
    let first = retirer.pass().await.expect("the pass runs");
    assert_eq!((first.tracking, first.entries()), (4, 4), "{first:?}");
    let read = table_reads.load(Ordering::SeqCst);
    assert!(read > 0, "the proof looked into a table");

    // Every proof starts again, as after a compaction that kept the rows.
    let registry = writer(&db).dropped();
    for (entry, armed) in registry.armed() {
        assert!(registry.advance(entry, armed.kept, Proof::Unproven));
    }
    let again = retirer.pass().await.expect("the pass runs");
    assert_eq!(clockless(again), clockless(first));
    assert_eq!(table_reads.load(Ordering::SeqCst), read);
    db.close().await.expect("the writer closes");
}

/// An object store that holds every compaction a retirer submits until the
/// contract lets it through, so the contract can write to the store at the
/// moment a reclaim starts a compaction.
struct SubmissionGate {
    inner: Arc<dyn ObjectStore>,
    submissions: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>,
}

impl std::fmt::Debug for SubmissionGate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubmissionGate")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for SubmissionGate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "gated({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SubmissionGate {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        // A retirer writes only to submit a compaction.
        let (through, let_through) = tokio::sync::oneshot::channel();
        self.submissions
            .send(through)
            .expect("the contract is waiting for submissions");
        let_through.await.expect("the contract lets it through");
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// The table ids and the stored bytes of each sorted run, by run id.
async fn sorted_runs(db: &HelixDB) -> BTreeMap<u32, (Vec<TableId>, u64)> {
    admin(db)
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest")
        .compacted()
        .iter()
        .map(|run| {
            (
                run.id,
                (
                    run.sst_views()
                        .iter()
                        .map(|view| TableId(view.sst.id.value().0))
                        .collect(),
                    run.estimate_size(),
                ),
            )
        })
        .collect()
}

/// The ids of the L0 tables, newest first.
async fn l0_tables(db: &HelixDB) -> Vec<TableId> {
    admin(db)
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest")
        .l0()
        .iter()
        .map(|view| TableId(view.sst.id.value().0))
        .collect()
}

async fn flush(db: &HelixDB) {
    writer(db)
        .db()
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("the memtable flushes");
}

/// Compacts the L0 tables, after a memtable flush, into a new sorted run
/// above every other, and waits for it.
async fn compact_l0(db: &HelixDB) {
    flush(db).await;
    let admin = admin(db);
    let manifest = admin
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest");
    let compaction = admin
        .submit_compaction(CompactionSpec::new(
            manifest
                .l0()
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect(),
            manifest
                .compacted()
                .iter()
                .map(|run| run.id)
                .max()
                .map_or(0, |id| id + 1),
        ))
        .await
        .expect("the compaction is submitted");
    assert!(tokio::time::timeout(WAIT, finished(db, &compaction))
        .await
        .expect("the compaction of the L0 tables ends in time"));
}

/// A quiet store with two sorted runs and a dropped index: the older run
/// holds the rows of the dropped `Gone` index among others, the newer run
/// holds none of them. Returns the store and the prefixes of the dropped
/// rows, with the entry armed and its proof tracking the older run's tables.
async fn two_runs_and_a_drop(database: &str) -> (HelixDB, Vec<Bytes>) {
    let db = open_quiet(ProcessLocalDatabaseToken::new(database).unwrap()).await;
    db.wait_for_background_migrations().await;
    // Enough rows that the nodes, which stay, and the index rows, which go,
    // each fill many blocks: the estimate of the dropped bytes is good to a
    // block or two.
    for seed in 0..300 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    compact_every_table(&db).await;
    for seed in 0..96 {
        add(&db, "Other", seed).await;
    }
    compact_l0(&db).await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    assert_eq!(pass(&db).await.tracking, 1);
    assert_eq!(sorted_runs(&db).await.len(), 2);
    (db, prefixes)
}

/// Proves a reclaim rewrites the sorted run that holds the dropped rows and
/// no other: the run that holds none of them keeps its tables, the L0 tables
/// that hold none are left in L0, one compaction is submitted, and the
/// outcome counts the bytes of the one run.
#[tokio::test]
async fn a_reclaim_rewrites_only_the_sorted_run_that_holds_the_dropped_rows() {
    let (db, prefixes) = two_runs_and_a_drop("dropped-reclaim-scope").await;
    let before = sorted_runs(&db).await;
    let (holding, apart) = (
        *before.keys().next().expect("the older run"),
        *before.keys().next_back().expect("the newer run"),
    );
    let Proof::Tracking { tables: tracked } = armed_entry(&db).proof else {
        panic!("the entry's rows are stored, so its proof tracks their tables");
    };
    assert!(tracked
        .tables()
        .keys()
        .all(|table| before[&holding].0.contains(table)));
    let tracked_bytes = tracked.tables().values().sum::<u64>();
    assert!(tracked_bytes > 0);
    // The drop left L0 tables: they hold its entry and its record, and no
    // row of the dropped generation.
    let l0 = l0_tables(&db).await;
    assert!(!l0.is_empty());
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    let (submissions, mut submitted) = tokio::sync::mpsc::unbounded_channel();
    let counted = Retirer::new(
        Arc::clone(shared),
        db.inner.storage.path(),
        Arc::new(SubmissionGate {
            inner: Arc::clone(db.inner.storage.object_store()),
            submissions,
        }),
        SLOTS,
    );
    let reclaim = counted.reclaim(tokio::time::Instant::now() + WAIT, Reclaimer::Caller);
    tokio::pin!(reclaim);
    let mut written = 0;
    let outcome = loop {
        tokio::select! {
            outcome = &mut reclaim => break outcome.expect("the reclaim runs"),
            through = submitted.recv() => {
                written += 1;
                through
                    .expect("the gate is open")
                    .send(())
                    .expect("the submission is waiting");
            }
        }
    };

    assert_eq!(
        outcome,
        DroppedStorageReclaim::Compacted {
            retired: 1,
            entries: 0,
            rewritten_bytes: before[&holding].1,
            dropped_bytes: tracked_bytes,
            left: None,
        }
    );
    assert_eq!(written, 1, "one compaction, of the one run");
    let after = sorted_runs(&db).await;
    assert_eq!(after[&apart], before[&apart]);
    assert!(before[&holding]
        .0
        .iter()
        .all(|table| !after[&holding].0.contains(table)));
    assert!(after[&holding].1 < before[&holding].1);
    assert_eq!(after.len(), 2);
    assert_eq!(l0_tables(&db).await, l0);
    assert_eq!(stored_entries(&db).await, vec![]);
    assert_discarded(&db, &prefixes).await;
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
    db.close().await.expect("the writer closes");
}

/// Proves a reclaim the writer starts leaves a sorted run that is not worth
/// rewriting, says what it left, and submits nothing; that the storage
/// report counts the dropped bytes that wait; and that a caller's reclaim
/// rewrites the same run.
#[tokio::test]
async fn the_writer_leaves_dropped_rows_in_a_run_not_worth_rewriting_and_a_caller_does_not() {
    let (db, prefixes) = two_runs_and_a_drop("dropped-reclaim-declined").await;
    let before = sorted_runs(&db).await;
    let holding = *before.keys().next().expect("the older run");
    let storage = db.dropped_index_storage();
    assert!(storage.tracked_bytes > 0);
    assert!(storage.tracked_bytes < before[&holding].1);
    // The run is small, so by the writer's own rule nothing is left.
    assert_eq!(storage.left_in_place, None);
    let retirer = retirer(&db).await;
    // No run is small, and a run is worth its rewrite only if every byte of
    // it is dropped: this one also holds the nodes.
    let never = Worth {
        small_run_bytes: 0,
        rewritten_per_dropped_byte: 1,
    };
    let left = LeftInPlace {
        dropped_bytes: storage.tracked_bytes,
        run_bytes: before[&holding].1,
        runs: 1,
    };
    // Twice: a reclaim that declines changes nothing, so the next declines
    // the same.
    for _ in 0..2 {
        assert_eq!(
            retirer
                .reclaim(
                    tokio::time::Instant::now() + WAIT,
                    Reclaimer::IdleStore(never)
                )
                .await
                .expect("the reclaim runs"),
            DroppedStorageReclaim::Declined { entries: 1, left }
        );
        assert_eq!(sorted_runs(&db).await, before);
        assert_eq!(stored_entries(&db).await.len(), 1);
        assert_eq!(
            clockless(db.dropped_index_storage()),
            clockless(storage.clone())
        );
        // The storage report names what a writer that follows this rule
        // leaves, and counts everything else as the writer's own does.
        assert_eq!(
            clockless(dropped::reclaim::storage(writer(&db), never)),
            clockless(DroppedIndexStorage {
                left_in_place: Some(left),
                ..storage.clone()
            })
        );
    }
    // An eighth of the run or more is dropped rows: a rule that asks for
    // less than that rewrites it.
    let outcome = retirer
        .reclaim(
            tokio::time::Instant::now() + WAIT,
            Reclaimer::IdleStore(Worth {
                small_run_bytes: 0,
                rewritten_per_dropped_byte: before[&holding].1 / storage.tracked_bytes + 1,
            }),
        )
        .await
        .expect("the reclaim runs");
    assert_eq!(
        outcome,
        DroppedStorageReclaim::Compacted {
            retired: 1,
            entries: 0,
            rewritten_bytes: before[&holding].1,
            dropped_bytes: storage.tracked_bytes,
            left: None,
        }
    );
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a caller is never refused: where the writer's rule would leave
/// the run, a requested reclaim rewrites it.
#[tokio::test]
async fn a_caller_reclaims_a_run_the_writer_would_leave() {
    let (db, prefixes) = two_runs_and_a_drop("dropped-reclaim-caller").await;
    let before = sorted_runs(&db).await;
    let holding = *before.keys().next().expect("the older run");
    assert!(matches!(
        retirer(&db)
            .await
            .reclaim(
                tokio::time::Instant::now() + WAIT,
                Reclaimer::IdleStore(Worth {
                    small_run_bytes: 0,
                    rewritten_per_dropped_byte: 1,
                }),
            )
            .await
            .expect("the reclaim runs"),
        DroppedStorageReclaim::Declined { .. }
    ));
    let outcome = db
        .reclaim_dropped_index_storage(WAIT)
        .await
        .expect("the reclaim runs");
    assert!(
        matches!(
            outcome,
            DroppedStorageReclaim::Compacted {
                retired: 1,
                entries: 0,
                left: None,
                ..
            }
        ),
        "{outcome:?}"
    );
    let after = sorted_runs(&db).await;
    assert!(before[&holding]
        .0
        .iter()
        .all(|table| !after[&holding].0.contains(table)));
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a reclaim compacts the newest L0 table that holds dropped rows
/// and every older one, into a new run, and leaves a newer L0 table that
/// holds none where it is.
#[tokio::test]
async fn a_reclaim_compacts_the_l0_tables_from_the_newest_that_holds_dropped_rows() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-reclaim-l0").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    flush(&db).await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    assert_eq!(pass(&db).await.tracking, 1);
    let Proof::Tracking { tables: tracked } = armed_entry(&db).proof else {
        panic!("the entry's rows are stored, so its proof tracks their tables");
    };
    // A table flushed since holds no dropped row.
    add(&db, "Other", 0).await;
    flush(&db).await;
    let l0 = l0_tables(&db).await;
    let newest_tracked = l0
        .iter()
        .position(|table| tracked.tables().contains_key(table))
        .expect("the dropped rows are in L0");
    assert!(newest_tracked > 0, "a newer table holds none: {l0:?}");
    assert_eq!(sorted_runs(&db).await.len(), 0);

    let outcome = db
        .reclaim_dropped_index_storage(WAIT)
        .await
        .expect("the reclaim runs");

    assert!(
        matches!(
            outcome,
            DroppedStorageReclaim::Compacted {
                retired: 1,
                entries: 0,
                left: None,
                ..
            }
        ),
        "{outcome:?}"
    );
    // The newer tables are still in L0, in their order; the rest is one run.
    assert_eq!(l0_tables(&db).await, l0[..newest_tracked]);
    assert_eq!(sorted_runs(&db).await.len(), 1);
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a reclaim submits nothing that shares a table with a compaction
/// in flight, and that one the writer starts submits nothing beside any
/// compaction: with the run that holds the dropped rows in a compaction, a
/// caller's reclaim says the store is busy; with only the L0 tables in one,
/// the writer's reclaim does and a caller's goes on.
#[tokio::test]
async fn a_reclaim_starts_nothing_that_a_compaction_in_flight_is_rewriting() {
    let (db, prefixes) = two_runs_and_a_drop("dropped-reclaim-busy").await;
    let before = sorted_runs(&db).await;
    let always = Reclaimer::IdleStore(Worth {
        small_run_bytes: u64::MAX,
        rewritten_per_dropped_byte: 1,
    });
    let retirer = retirer(&db).await;
    let busy = DroppedStorageReclaim::Incomplete {
        entries: 1,
        obstacle: ReclaimObstacle::Busy,
    };

    // A compaction of the L0 tables alone: it reads no sorted run.
    add(&db, "Other", 1000).await;
    flush(&db).await;
    let manifest = admin(&db)
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest");
    let of_l0 = admin(&db)
        .submit_compaction(CompactionSpec::new(
            manifest
                .l0()
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect(),
            before.keys().next_back().expect("the newer run") + 1,
        ))
        .await
        .expect("the compaction is submitted");
    assert_eq!(
        retirer
            .reclaim(tokio::time::Instant::now() + WAIT, always)
            .await
            .expect("the reclaim runs"),
        busy
    );
    let in_flight = admin(&db)
        .read_compaction(of_l0.id(), None)
        .await
        .expect("the compaction status reads")
        .is_some_and(|compaction| compaction.active());
    assert!(
        in_flight,
        "the reclaim ended while the compaction was in flight"
    );
    assert_eq!(sorted_runs(&db).await, before);
    assert!(finished(&db, &of_l0).await);

    // A compaction of every table: it reads the run that holds the rows.
    let before = sorted_runs(&db).await;
    let of_all = submit_compaction_of_every_table(&db).await;
    assert_eq!(
        retirer
            .reclaim(tokio::time::Instant::now() + WAIT, Reclaimer::Caller)
            .await
            .expect("the reclaim runs"),
        busy
    );
    let in_flight = admin(&db)
        .read_compaction(of_all.id(), None)
        .await
        .expect("the compaction status reads")
        .is_some_and(|compaction| compaction.active());
    assert!(
        in_flight,
        "the reclaim ended while the compaction was in flight"
    );
    assert_eq!(sorted_runs(&db).await, before);
    // That compaction discards the rows itself.
    assert!(finished(&db, &of_all).await);
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a caller's reclaim goes on beside a compaction that reads none of
/// its tables, where that leaves a slot free: with three slots, one taken by
/// a compaction of the L0 tables, it takes the second.
#[tokio::test]
async fn a_callers_reclaim_runs_beside_a_compaction_of_other_tables() {
    let (db, prefixes) = two_runs_and_a_drop("dropped-reclaim-beside").await;
    add(&db, "Other", 1000).await;
    flush(&db).await;
    let manifest = admin(&db)
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest");
    admin(&db)
        .submit_compaction(CompactionSpec::new(
            manifest
                .l0()
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect(),
            manifest
                .compacted()
                .iter()
                .map(|run| run.id)
                .max()
                .expect("two runs")
                + 1,
        ))
        .await
        .expect("the compaction is submitted");
    // Three slots: the compaction of the L0 tables has one, the reclaim's
    // takes another, and one is left for the scheduler.
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    let outcome = Retirer::new(
        Arc::clone(shared),
        db.inner.storage.path(),
        Arc::clone(db.inner.storage.object_store()),
        3,
    )
    .reclaim(tokio::time::Instant::now() + WAIT, Reclaimer::Caller)
    .await
    .expect("the reclaim runs");
    assert!(
        matches!(
            outcome,
            DroppedStorageReclaim::Compacted {
                retired: 1,
                entries: 0,
                ..
            }
        ),
        "{outcome:?}"
    );
    // The other compaction ran too: `.compactions` keeps only the last to
    // finish, so the manifest tells.
    tokio::time::timeout(WAIT, async {
        while !l0_tables(&db).await.is_empty() {
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .expect("the compaction of the L0 tables completes in time");
    assert_eq!(sorted_runs(&db).await.len(), 3);
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a reclaim never takes the compactor's last free slot: on a
/// compactor with one slot it submits nothing, a caller's or the writer's;
/// on one with two it submits nothing beside a compaction in flight, though
/// that one reads none of its tables; the runs and the entry are as they
/// were each time; and once the other compaction has ended the same reclaim
/// finishes the job.
#[tokio::test]
async fn a_reclaim_never_takes_the_last_free_slot() {
    let (db, prefixes) = two_runs_and_a_drop("dropped-reclaim-last-slot").await;
    let before = sorted_runs(&db).await;
    let HelixStorage::Writer(shared) = db.storage() else {
        panic!("the contract opens a writer");
    };
    let with_slots = |slots| {
        Retirer::new(
            Arc::clone(shared),
            db.inner.storage.path(),
            Arc::clone(db.inner.storage.object_store()),
            slots,
        )
    };
    let submitted = || async {
        admin(&db)
            .read_compactions(None)
            .await
            .expect("the compactions read")
            .iter()
            .flat_map(|compactions| compactions.recent_compactions())
            .map(|compaction| compaction.id())
            .collect::<HashSet<_>>()
    };
    let busy = DroppedStorageReclaim::Incomplete {
        entries: 1,
        obstacle: ReclaimObstacle::Busy,
    };
    let always = Reclaimer::IdleStore(Worth {
        small_run_bytes: u64::MAX,
        rewritten_per_dropped_byte: 1,
    });

    // One slot, and nothing in flight: its compaction would take the only
    // one, so none is submitted.
    let earlier = submitted().await;
    let one_slot = with_slots(1);
    for reclaimer in [Reclaimer::Caller, always] {
        assert_eq!(
            one_slot
                .reclaim(tokio::time::Instant::now() + WAIT, reclaimer)
                .await
                .expect("the reclaim runs"),
            busy,
            "{reclaimer:?}"
        );
    }
    assert_eq!(submitted().await, earlier);
    assert_eq!(sorted_runs(&db).await, before);
    assert_eq!(pass(&db).await.tracking, 1);

    // Two slots, one taken by a compaction of the L0 tables alone, which
    // reads none of the reclaim's: the other is the last free one.
    add(&db, "Other", 1000).await;
    flush(&db).await;
    let manifest = admin(&db)
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest");
    let of_l0 = admin(&db)
        .submit_compaction(CompactionSpec::new(
            manifest
                .l0()
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect(),
            before.keys().next_back().expect("the newer run") + 1,
        ))
        .await
        .expect("the compaction is submitted");
    let two_slots = with_slots(2);
    let listed = submitted().await;
    assert_eq!(
        two_slots
            .reclaim(tokio::time::Instant::now() + WAIT, Reclaimer::Caller)
            .await
            .expect("the reclaim runs"),
        busy
    );
    assert_eq!(submitted().await, listed);
    let in_flight = admin(&db)
        .read_compaction(of_l0.id(), None)
        .await
        .expect("the compaction status reads")
        .is_some_and(|compaction| compaction.active());
    assert!(
        in_flight,
        "the reclaim ended while the compaction was in flight"
    );
    assert_eq!(sorted_runs(&db).await, before);
    assert_eq!(stored_entries(&db).await.len(), 1);

    // That compaction ends, and a slot stays free beside the reclaim's.
    assert!(finished(&db, &of_l0).await);
    let outcome = two_slots
        .reclaim(tokio::time::Instant::now() + WAIT, Reclaimer::Caller)
        .await
        .expect("the reclaim runs");
    assert!(
        matches!(
            outcome,
            DroppedStorageReclaim::Compacted {
                retired: 1,
                entries: 0,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves a reclaim submits no compaction of L0 tables beside another:
/// with the dropped rows in L0 and a compaction of the L0 tables in flight,
/// it says the store is busy, and that compaction discards the rows.
#[tokio::test]
async fn a_reclaim_compacts_no_l0_table_beside_a_compaction_of_l0_tables() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-reclaim-l0-busy").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    flush(&db).await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    assert_eq!(pass(&db).await.tracking, 1);
    let l0 = l0_tables(&db).await;
    let manifest = admin(&db)
        .read_manifest(None)
        .await
        .expect("the manifest reads")
        .expect("an open database has a manifest");
    let of_l0 = admin(&db)
        .submit_compaction(CompactionSpec::new(
            manifest
                .l0()
                .iter()
                .map(|view| SourceId::SstView(view.id))
                .collect(),
            0,
        ))
        .await
        .expect("the compaction is submitted");

    assert_eq!(
        retirer(&db)
            .await
            .reclaim(tokio::time::Instant::now() + WAIT, Reclaimer::Caller)
            .await
            .expect("the reclaim runs"),
        DroppedStorageReclaim::Incomplete {
            entries: 1,
            obstacle: ReclaimObstacle::Busy,
        }
    );
    let in_flight = admin(&db)
        .read_compaction(of_l0.id(), None)
        .await
        .expect("the compaction status reads")
        .is_some_and(|compaction| compaction.active());
    assert!(
        in_flight,
        "the reclaim ended while the compaction was in flight"
    );
    assert_eq!(l0_tables(&db).await, l0);

    assert!(finished(&db, &of_l0).await);
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves the bytes a proof tracks are about the bytes of the dropped rows:
/// no more than the tables that hold them, no less than half of what the
/// rows take unencoded, and what the run shrinks by when it is rewritten,
/// to within a block at each end of each owner's rows.
#[tokio::test]
async fn the_bytes_a_proof_tracks_are_those_of_the_dropped_rows() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-tracked-bytes").unwrap()).await;
    db.wait_for_background_migrations().await;
    for seed in 0..400 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    compact_every_table(&db).await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    let unencoded = rows(writer(&db).db())
        .await
        .into_iter()
        .filter(|(key, _)| prefixes.iter().any(|prefix| key.starts_with(prefix)))
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum::<u64>();
    let storage = pass(&db).await;
    assert_eq!(storage.tracking, 1);
    let before = sorted_runs(&db).await;
    let [(tables, stored)] = &before.values().cloned().collect::<Vec<_>>()[..] else {
        panic!("one run holds every row: {before:?}");
    };
    assert_eq!(storage.tracked_tables, tables.len());
    assert!(
        unencoded / 2 <= storage.tracked_bytes && storage.tracked_bytes <= *stored,
        "{unencoded} {storage:?} {stored}"
    );

    reclaim_every_entry(&db).await;

    let after = sorted_runs(&db).await;
    let rewritten = after.values().map(|(_, bytes)| bytes).sum::<u64>();
    // Two owners, a block of at most 4 KiB at each end of each, and the
    // filter and index the smaller table no longer carries.
    let shrunk = stored - rewritten;
    assert!(
        shrunk.abs_diff(storage.tracked_bytes) <= shrunk / 4 + 4 * 4096,
        "{shrunk} {storage:?}"
    );
    assert_discarded(&db, &prefixes).await;
    db.close().await.expect("the writer closes");
}

/// Proves an entry the catalog contradicts fails no index creation and can
/// be removed on request, and what the removal leaves: an entry that names
/// the next index id is skipped by the index created next; removing it
/// deletes that entry and nothing else, so the rows stored under the id it
/// named survive every later compaction; an armed entry is refused, and
/// retires by itself with its rows discarded.
#[tokio::test]
async fn a_quarantined_entry_fails_no_creation_and_is_removed_only_on_request() {
    let token = ProcessLocalDatabaseToken::new("dropped-quarantine-removal").unwrap();
    let db = open_quiet(token.clone()).await;
    db.wait_for_background_migrations().await;
    let scope = DataScope::LegacyUnscoped;
    for seed in 0..24 {
        add(&db, "Gone", seed).await;
        add(&db, "New", seed).await;
    }
    create_index(&db, "Gone").await;
    drop_index(&db, "Gone").await;
    let [gone] = stored_entries(&db).await[..] else {
        panic!("one drop leaves one entry");
    };
    let gone_prefixes = armed_prefixes(&db).await;

    // An entry that names the id the next index would take, with rows
    // stored under the generation it names.
    let next = crate::index_lifecycle::repository::load_logical_index_watermark(writer(&db).db())
        .await
        .expect("the watermark reads")
        .next_id;
    let unallocated = DroppedGeneration {
        index_id: next,
        generation: IndexGenerationId::initial(),
    };
    let stray_prefixes = OwnerKey::Generation {
        scope,
        index_id: unallocated.index_id.get(),
        generation: unallocated.generation.get(),
    }
    .prefixes();
    let slate = writer(&db).db();
    slate
        .put(
            unallocated.key(),
            encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(
                DroppedGenerationValue {
                    scope,
                    rows: DroppedGenerationRows::Secondary,
                },
            )),
        )
        .await
        .expect("the fixture entry is written");
    for (row, prefix) in stray_prefixes.iter().enumerate() {
        slate
            .put([&prefix[..], &[row as u8]].concat(), b"stray")
            .await
            .expect("the fixture row is written");
    }
    let stray = |rows: BTreeMap<Bytes, Bytes>| {
        rows.into_keys()
            .filter(|key| stray_prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .count()
    };
    assert_eq!(stray(rows(slate).await), stray_prefixes.len());

    // The creation skips the id the entry names.
    create_index(&db, "New").await;
    assert_eq!(nearest(&db, "New", 3, 1).await.len(), 1);
    assert_eq!(
        crate::index_lifecycle::repository::load_logical_index_watermark(slate)
            .await
            .expect("the watermark reads")
            .next_id
            .get(),
        next.get() + 2
    );
    let storage = pass(&db).await;
    let [quarantined] = &storage.quarantined[..] else {
        panic!("the fixture entry is quarantined: {storage:?}");
    };
    assert_eq!(
        (
            quarantined.index_id,
            quarantined.generation,
            quarantined.scope,
            quarantined.rows,
            quarantined.contradiction
        ),
        (
            next.get(),
            1,
            scope,
            dropped::DroppedEntryRows::Secondary,
            dropped::DroppedEntryContradiction::IndexWithoutRecord
        )
    );
    // The clock of an entry that stays quarantined runs on across passes.
    let since = quarantined.since;
    assert_eq!(pass(&db).await.quarantined[0].since, since);

    // An armed entry is not removed, nor is one that is not stored.
    use crate::QuarantinedEntryRemoval::{Absent, Armed as StillArmed, Removed};
    let handle = &db;
    let remove = |index_id: u64, generation: u64| async move {
        handle
            .remove_quarantined_dropped_index_entry(index_id, generation, WAIT)
            .await
            .expect("the removal runs")
    };
    assert_eq!(
        remove(gone.index_id.get(), gone.generation.get()).await,
        StillArmed
    );
    assert_eq!(remove(next.get(), 2).await, Absent);
    assert_eq!(remove(next.get() + 7, 1).await, Absent);
    assert_eq!(remove(0, 1).await, Absent);
    assert_eq!(remove(1, 0).await, Absent);
    assert_eq!(stored_entries(&db).await, vec![gone, unallocated]);

    // The quarantined one is, and only its entry row goes.
    let before = settled_rows(&db).await;
    assert_eq!(remove(next.get(), 1).await, Removed);
    assert_eq!(stored_entries(&db).await, vec![gone]);
    let mut expected = before;
    assert!(expected.remove(&unallocated.key()).is_some());
    assert_rows(&db, &expected).await;
    assert_eq!(db.dropped_index_storage().quarantined, vec![]);
    assert_eq!(remove(next.get(), 1).await, Absent);

    // An entry no pass has read yet is judged by the removal itself.
    let unread = DroppedGeneration {
        index_id: IndexId::new(next.get() + 9).unwrap(),
        generation: IndexGenerationId::initial(),
    };
    slate
        .put(
            unread.key(),
            encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(
                DroppedGenerationValue {
                    scope,
                    rows: DroppedGenerationRows::Text,
                },
            )),
        )
        .await
        .expect("the fixture entry is written");
    assert_eq!(db.dropped_index_storage().quarantined, vec![]);
    assert_eq!(remove(unread.index_id.get(), 1).await, Removed);
    assert_eq!(stored_entries(&db).await, vec![gone]);

    // Its rows are kept by every compaction from then on; the armed entry's
    // are discarded, and it retires.
    compact_every_table(&db).await;
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_discarded(&db, &gone_prefixes).await;
    assert_eq!(stray(rows(slate).await), stray_prefixes.len());
    db.close().await.expect("the writer closes");

    // A reopened writer finds no entry, and the rows still stored.
    let db = open_quiet(token.clone()).await;
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
    compact_every_table(&db).await;
    assert_eq!(stray(rows(writer(&db).db()).await), stray_prefixes.len());
    assert_eq!(nearest(&db, "New", 3, 1).await.len(), 1);
    db.close().await.expect("the writer closes");

    let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
        .await
        .expect("the reader opens");
    assert!(matches!(
        reader
            .remove_quarantined_dropped_index_entry(1, 1, WAIT)
            .await,
        Err(HelixDbError::WriterModeRequired { .. })
    ));
    reader.close().await.expect("the reader closes");
}

/// Proves a removal on request never deletes an entry it did not find
/// contradicted. A forged entry names a live generation, so it is
/// quarantined. The removal reads it, finds it contradicted and stages its
/// delete; before that commits, the drop of the index completes and stores
/// the generation's own entry under the same key. The removal's transaction
/// conflicts, looks again, finds an entry the catalog agrees with and leaves
/// it: the entry is armed, and a reclaim discards the generation's rows.
/// Deleted unconditionally, the entry would be gone and the rows stored for
/// good, with nothing to name them.
#[tokio::test]
async fn a_removal_overtaken_by_the_drop_of_its_generation_leaves_the_drops_entry() {
    let db = open_quiet(ProcessLocalDatabaseToken::new("dropped-removal-raced").unwrap()).await;
    db.wait_for_background_migrations().await;
    let scope = DataScope::LegacyUnscoped;
    for seed in 0..48 {
        add(&db, "Gone", seed).await;
    }
    create_index(&db, "Gone").await;
    // The first index of a store is index 1 at generation 1, which the
    // contradiction below confirms; its rows name its physical id.
    let entry = generation_entry(1);
    let owners = settled_rows(&db)
        .await
        .keys()
        .filter_map(|key| dropped::ownership::owned_row(key))
        .map(|row| row.owner)
        .collect::<HashSet<_>>();
    let [OwnerKey::PhysicalIndex {
        physical_index_id, ..
    }] = owners.iter().collect::<Vec<_>>()[..]
    else {
        panic!("the active index stores rows of one physical id: {owners:?}");
    };
    writer(&db)
        .db()
        .put(
            entry.key(),
            encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(
                DroppedGenerationValue {
                    scope,
                    rows: DroppedGenerationRows::UnpartitionedVector {
                        physical_index_id: VectorPhysicalIndexId::new(*physical_index_id).unwrap(),
                    },
                },
            )),
        )
        .await
        .expect("the fixture entry is written");
    let quarantined = pass(&db).await.quarantined;
    let [quarantined] = &quarantined[..] else {
        panic!("the forged entry is quarantined: {quarantined:?}");
    };
    assert_eq!(
        quarantined.contradiction,
        dropped::DroppedEntryContradiction::GenerationStillOwned
    );

    let retirer = retirer(&db).await;
    retirer.removal.arm();
    let removal = db.remove_quarantined_dropped_index_entry(
        entry.index_id.get(),
        entry.generation.get(),
        WAIT,
    );
    tokio::pin!(removal);
    tokio::select! {
        ended = &mut removal => panic!("the removal is held before its commit: {ended:?}"),
        () = retirer.removal.staged() => {}
    }
    // The drop completes while the removal's delete is staged.
    drop_index(&db, "Gone").await;
    assert_eq!(stored_entries(&db).await, vec![entry]);
    retirer.removal.release();
    assert_eq!(
        tokio::time::timeout(WAIT, removal)
            .await
            .expect("the removal ends in time")
            .expect("the removal runs"),
        crate::QuarantinedEntryRemoval::Armed
    );

    assert_eq!(stored_entries(&db).await, vec![entry]);
    assert_eq!(pass(&db).await.quarantined, vec![]);
    let prefixes = armed_prefixes(&db).await;
    assert!(!prefixes.is_empty());
    reclaim_every_entry(&db).await;
    assert_discarded(&db, &prefixes).await;
    assert_eq!(owned_rows(&db).await, 0);
    db.close().await.expect("the writer closes");
}

/// Source vectors of a contract's bulk build: more than the fewest
/// [`open_bulk`] builds in bulk, and several write steps' worth.
const BULK_SOURCE: usize = 96;

/// Opens a writer over `object_store` that builds a vector index of at
/// least 16 vectors in bulk, eight source rows a write step, and whose
/// compaction scheduler never proposes a compaction.
async fn open_bulk(object_store: Arc<dyn ObjectStore>, database: &str) -> HelixDB {
    let never = ["min_compaction_sources", "max_compaction_sources"]
        .map(|option| (option.to_string(), "1000000".to_string()));
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let eight_rows = SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(8).expect("eight is positive"),
        batch.max_input_bytes(),
        batch.max_output_operations(),
        batch.max_output_bytes(),
        batch.max_single_vector_output_bytes(),
    )
    .expect("the step limits validate");
    HelixDB::open_with_object_store_and_config(
        database.to_string(),
        object_store,
        crate::DbConfig::new()
            .with_slate_settings(slatedb::Settings {
                compactor_options: Some(slatedb::config::CompactorOptions {
                    scheduler_options: HashMap::from(never),
                    ..slatedb::config::CompactorOptions::default()
                }),
                ..slatedb::Settings::default()
            })
            .with_index_lifecycle_throughput_tuning(
                IndexLifecycleThroughputTuning::default().with_vector_bulk_build(
                    VectorBulkBuildTuning::try_new(
                        1,
                        // Accounted, not allocated: room for the whole write
                        // buffer of several builds.
                        VectorBulkBuildMemory::Bytes(
                            NonZeroU64::new(64 << 30).expect("the budget is positive"),
                        ),
                        16,
                    )
                    .expect("the bulk tuning validates"),
                ),
            )
            .with_search_index_backfill_limits(
                SearchIndexBackfillLimits::try_new(
                    eight_rows,
                    defaults.edge_property_read_batch(),
                    defaults.text_artifacts(),
                    defaults.text_compaction(),
                )
                .expect("the backfill limits validate"),
            ),
    )
    .await
    .expect("the writer opens")
}

/// A step of a bulk build's write pass that follows one whose rows are
/// stored.
fn writing(progress: &IndexOperationProgress) -> bool {
    matches!(
        progress,
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::BulkBuild(bulk)
        )) if bulk.scan.cursor.is_some()
    )
}

/// A step at the bulk stage outside the write pass: the attempt waits,
/// reads or links, or the rows of an abandoned one are being deleted.
fn not_writing(progress: &IndexOperationProgress) -> bool {
    matches!(
        progress,
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::BulkBuild(bulk)
        )) if bulk.scan.cursor.is_none()
    )
}

/// Runs one DDL statement and returns the operation it started or met,
/// without waiting for it.
async fn started(db: &HelixDB, statement: Traversal<Terminal, WriteEnabled>) -> String {
    let receipt = db
        .query_scoped(
            QueryRequest::write(
                write_batch()
                    .var_as("operation", statement)
                    .returning(["operation"]),
            ),
            DataScope::LegacyUnscoped,
        )
        .await
        .expect("the statement is accepted");
    receipt["operation"]["operation_id"]
        .as_str()
        .unwrap_or_else(|| panic!("the statement names an operation: {receipt}"))
        .to_string()
}

/// Waits for `operation_id` to end as `ended`.
async fn ended(db: &HelixDB, operation_id: &str, ended: &str) {
    tokio::time::timeout(WAIT, async {
        loop {
            let status = db
                .query_scoped(
                    QueryRequest::read(
                        read_batch()
                            .var_as("status", g().get_index_operation(operation_id))
                            .returning(["status"]),
                    ),
                    DataScope::LegacyUnscoped,
                )
                .await
                .expect("the operation status reads");
            match status["status"]["status"].as_str() {
                Some(state) if state == ended => break,
                Some("queued" | "running") => tokio::time::sleep(POLL).await,
                state => panic!("the operation reached {state:?}, not {ended}"),
            }
        }
    })
    .await
    .expect("the operation ends in time");
}

/// Builds the index of `label` and proves its write pass ran: the build
/// stored the graph it linked in memory.
async fn build_in_bulk(db: &HelixDB, label: &str) {
    let pause = StepPause::arm(db.inner_db().as_ref(), writing);
    let build = started(db, g().create_index_if_not_exists(spec(label))).await;
    tokio::time::timeout(WAIT, pause.reached())
        .await
        .expect("the build reaches its write pass");
    pause.release();
    ended(db, &build, "succeeded").await;
}

/// Every vector row of the legacy scope, of any physical index or
/// generation: the three storage lanes, and the build deltas and applied
/// state.
async fn vector_rows(db: &HelixDB) -> BTreeMap<Bytes, Bytes> {
    let prefixes = VectorStorageLane::ALL
        .map(|lane| DataKey::data_prefix(DataScope::LegacyUnscoped, lane.scan_prefix()))
        .into_iter()
        .chain(
            [RecordKind::BuildDelta, RecordKind::AppliedState].map(|kind| {
                ManagedIndexKey::data_prefix(
                    DataScope::LegacyUnscoped,
                    ScopedKey::logical_prefix(kind),
                )
            }),
        )
        .collect::<Vec<_>>();
    rows(writer(db).db())
        .await
        .into_iter()
        .filter(|(key, _)| prefixes.iter().any(|prefix| key.starts_with(prefix)))
        .collect()
}

/// Reclaims until the one entry stored has retired, and proves the store
/// then holds exactly the vector rows it held `before` the generation was
/// created: no row of it is left that neither its cleanup nor a compaction
/// deleted, and no other index lost one.
async fn assert_reclaimed_to(db: &HelixDB, before: &BTreeMap<Bytes, Bytes>) {
    let entries = stored_entries(db).await;
    assert_eq!(entries.len(), 1, "one generation was given up: {entries:?}");
    reclaim_every_entry(db).await;
    let deadline = Instant::now() + WAIT;
    let mut now = vector_rows(db).await;
    while now != *before && Instant::now() < deadline {
        tokio::time::sleep(POLL).await;
        now = vector_rows(db).await;
    }
    let orphans = now
        .keys()
        .filter(|key| !before.contains_key(*key))
        .collect::<Vec<_>>();
    let lost = before
        .iter()
        .filter(|(key, value)| now.get(*key) != Some(*value))
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    assert!(
        orphans.is_empty() && lost.is_empty(),
        "{} orphan rows, such as {:?}; lost or changed {lost:?}",
        orphans.len(),
        orphans.first()
    );
    assert_eq!(db.dropped_index_storage(), DroppedIndexStorage::default());
}

/// A bulk writer with a bulk-built `Kept` index and the sources of `Gone`.
/// Returns it with the vector rows it holds.
async fn bulk_store(
    object_store: Arc<dyn ObjectStore>,
    database: &str,
) -> (HelixDB, BTreeMap<Bytes, Bytes>) {
    let db = open_bulk(object_store, database).await;
    db.wait_for_background_migrations().await;
    for seed in 0..BULK_SOURCE {
        add(&db, "Kept", seed).await;
        add(&db, "Gone", seed).await;
    }
    build_in_bulk(&db, "Kept").await;
    let before = vector_rows(&db).await;
    assert!(!before.is_empty());
    (db, before)
}

/// Proves an index that was built in bulk is dropped like any other: its
/// drop leaves an entry, and a reclaim leaves no row of it, while the
/// bulk-built index beside it keeps every row and answers.
#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_bulk_built_index_leaves_no_row_after_a_reclaim() {
    let token = ProcessLocalDatabaseToken::new("dropped-bulk-built").unwrap();
    let (db, before) = bulk_store(token.object_store(), token.database()).await;
    let kept = nearest(&db, "Kept", 3, 5).await;
    build_in_bulk(&db, "Gone").await;
    assert_eq!(nearest(&db, "Gone", 3, 5).await.len(), 5);
    assert_ne!(vector_rows(&db).await, before);

    drop_index(&db, "Gone").await;
    assert_reclaimed_to(&db, &before).await;
    assert_eq!(nearest(&db, "Kept", 3, 5).await, kept);
    db.close().await.expect("the writer closes");
}

/// Aborts the bulk build of `Gone` while a step that `held` matches is
/// staged and not committed, and proves a reclaim then leaves no row of the
/// generation. `stores_rows` says whether the build had stored any.
async fn abort_meets_a_bulk_build(database: &str, held: StepMatcher, stores_rows: bool) {
    let token = ProcessLocalDatabaseToken::new(database).unwrap();
    let (db, before) = bulk_store(token.object_store(), token.database()).await;
    let pause = StepPause::arm(db.inner_db().as_ref(), held);
    let build = started(&db, g().create_index_if_not_exists(spec("Gone"))).await;
    tokio::time::timeout(WAIT, pause.reached())
        .await
        .expect("the build reaches the step");
    assert_eq!(vector_rows(&db).await != before, stores_rows);

    assert_eq!(started(&db, g().drop_index(spec("Gone"))).await, build);
    // The held step finds its claim superseded and commits nothing.
    pause.release();
    ended(&db, &build, "aborted").await;
    assert_reclaimed_to(&db, &before).await;
    assert_eq!(nearest(&db, "Kept", 3, 5).await.len(), 5);
    db.close().await.expect("the writer closes");
}

/// Proves an abort that meets a bulk build before its write pass, while the
/// attempt is counted and waits, reads or links, ends in an entry that
/// retires with no row left.
#[tokio::test(flavor = "multi_thread")]
async fn an_abort_before_a_bulk_builds_write_pass_leaves_no_row_after_a_reclaim() {
    abort_meets_a_bulk_build("dropped-bulk-abort-before-write", not_writing, false).await;
}

/// Proves an abort that meets a bulk build in its write pass, with part of
/// the graph stored and no metadata row, ends in an entry whose reclaim
/// leaves no row of the partly written graph.
#[tokio::test(flavor = "multi_thread")]
async fn an_abort_in_a_bulk_builds_write_pass_leaves_no_row_after_a_reclaim() {
    abort_meets_a_bulk_build("dropped-bulk-abort-in-write", writing, true).await;
}

/// Proves an abort that arrives while a writer deletes, row by row, the
/// rows of a bulk attempt another process abandoned leaves no row behind:
/// what the deletion had not reached is the entry's, and a reclaim discards
/// it.
///
/// The second writer takes the store while the first holds a step of its
/// write pass, as a process that replaces a killed one does. Its first step
/// deletes the attempt's applied state; the abort is accepted while its
/// second, which deletes physical rows, is staged and not committed.
#[test]
fn an_abort_while_an_abandoned_bulk_attempt_is_deleted_leaves_no_row_after_a_reclaim() {
    isolated(|| async {
        let token = ProcessLocalDatabaseToken::new("dropped-bulk-abort-in-discard").unwrap();
        let (abandoning, before) = bulk_store(token.object_store(), token.database()).await;
        let write = StepPause::arm(abandoning.inner_db().as_ref(), writing);
        let build = started(&abandoning, g().create_index_if_not_exists(spec("Gone"))).await;
        tokio::time::timeout(WAIT, write.reached())
            .await
            .expect("the build reaches its write pass");
        let applied_state = ManagedIndexKey::data_prefix(
            DataScope::LegacyUnscoped,
            ScopedKey::logical_prefix(RecordKind::AppliedState),
        );
        let applied = |rows: &BTreeMap<Bytes, Bytes>| {
            rows.keys()
                .filter(|key| key.starts_with(&applied_state))
                .count()
        };
        let written = vector_rows(&abandoning).await;
        assert!(applied(&written) > applied(&before));
        // A step that progresses does not wait for its upload: what the
        // attempt stored is durable before another writer opens over it.
        writer(&abandoning)
            .db()
            .flush()
            .await
            .expect("the attempt's rows are uploaded");

        // The step of the second writer that follows its first deleting one.
        let delete = StepPause::arm_in_process(not_writing);
        let db = open_bulk(token.object_store(), token.database()).await;
        tokio::time::timeout(WAIT, delete.reached())
            .await
            .expect("the second writer deletes the abandoned attempt's rows");
        let left = vector_rows(&db).await;
        assert_eq!(applied(&left), applied(&before));
        assert_eq!(
            left.len() - before.len(),
            written.len() - before.len() - (applied(&written) - applied(&before)),
            "the physical rows of the abandoned attempt are still stored"
        );

        assert_eq!(started(&db, g().drop_index(spec("Gone"))).await, build);
        delete.release();
        ended(&db, &build, "aborted").await;
        // The first writer is fenced: its held step commits nothing.
        drop(write);
        drop(abandoning.close().await);
        assert_reclaimed_to(&db, &before).await;
        assert_eq!(nearest(&db, "Kept", 3, 5).await.len(), 5);
        db.close().await.expect("the writer closes");
    });
}

/// Proves no compaction discards a row of a generation at the bulk stage,
/// whatever entry names it. The cross-check reads the index record, not the
/// operation's stage, and the record of a build at the bulk stage is a
/// building one as at the serial stage: an entry that names the generation
/// and the physical id its attempt writes under is quarantined while the
/// attempt is part-way through its write pass, two compactions of every
/// table keep every row of the partly written graph, and the build then
/// stores the rest and answers.
#[tokio::test(flavor = "multi_thread")]
async fn no_compaction_discards_a_row_of_a_generation_at_the_bulk_stage() {
    let token = ProcessLocalDatabaseToken::new("dropped-bulk-stage-quarantine").unwrap();
    let (db, before) = bulk_store(token.object_store(), token.database()).await;
    let scope = DataScope::LegacyUnscoped;
    let pause = StepPause::arm(db.inner_db().as_ref(), writing);
    let build = started(&db, g().create_index_if_not_exists(spec("Gone"))).await;
    tokio::time::timeout(WAIT, pause.reached())
        .await
        .expect("the build reaches its write pass");
    let building = |rows: &BTreeMap<Bytes, Bytes>| {
        rows.iter()
            .filter(|(key, _)| !before.contains_key(*key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>()
    };
    let written = building(&vector_rows(&db).await);
    // The owners the stored rows of the attempt name: its generation, in
    // the applied state, and its physical id, in the filtered lanes.
    let owners = written
        .keys()
        .filter_map(|key| dropped::ownership::owned_row(key))
        .map(|row| row.owner)
        .collect::<HashSet<_>>();
    let (entry, physical_index_id) = match owners.iter().collect::<Vec<_>>()[..] {
        [OwnerKey::Generation {
            index_id,
            generation,
            ..
        }, OwnerKey::PhysicalIndex {
            physical_index_id, ..
        }]
        | [OwnerKey::PhysicalIndex {
            physical_index_id, ..
        }, OwnerKey::Generation {
            index_id,
            generation,
            ..
        }] => (
            DroppedGeneration {
                index_id: IndexId::new(*index_id).unwrap(),
                generation: IndexGenerationId::new(*generation).unwrap(),
            },
            *physical_index_id,
        ),
        _ => panic!("the attempt writes under one generation and one physical id: {owners:?}"),
    };
    writer(&db)
        .db()
        .put(
            entry.key(),
            encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(
                DroppedGenerationValue {
                    scope,
                    rows: DroppedGenerationRows::UnpartitionedVector {
                        physical_index_id: VectorPhysicalIndexId::new(physical_index_id).unwrap(),
                    },
                },
            )),
        )
        .await
        .expect("the fixture entry is written");

    let quarantined = vec![QuarantinedDroppedGeneration {
        index_id: entry.index_id.get(),
        generation: entry.generation.get(),
        scope,
        rows: dropped::DroppedEntryRows::UnpartitionedVector { physical_index_id },
        contradiction: dropped::DroppedEntryContradiction::GenerationStillOwned,
        since: *CLOCKLESS,
    }];
    // Every table is rewritten, twice, with the entry stored and the build
    // at the bulk stage.
    for _ in 0..2 {
        assert_eq!(
            clockless(pass(&db).await),
            DroppedIndexStorage {
                quarantined: quarantined.clone(),
                ..DroppedIndexStorage::default()
            }
        );
        assert_eq!(writer(&db).dropped().armed(), vec![]);
        compact_every_table(&db).await;
    }
    assert_eq!(building(&vector_rows(&db).await), written);

    // Without the entry the build goes on from the rows it stored.
    assert_eq!(
        db.remove_quarantined_dropped_index_entry(
            entry.index_id.get(),
            entry.generation.get(),
            WAIT
        )
        .await
        .expect("the removal runs"),
        crate::QuarantinedEntryRemoval::Removed
    );
    pause.release();
    ended(&db, &build, "succeeded").await;
    assert_eq!(nearest(&db, "Gone", 3, 5).await.len(), 5);
    assert_eq!(stored_entries(&db).await, vec![]);
    db.close().await.expect("the writer closes");
}

/// What [`supersede`] found.
#[derive(Debug, PartialEq, Eq)]
struct Superseded {
    /// Whether the compaction the new writer scheduled completed while only
    /// the superseded writer's worker was polling for work.
    ran_under_the_superseded_writer: bool,
    /// Rows of the dropped generation left once a writer with a worker of
    /// its own had compacted every table and no entry was stored.
    rows_left_behind: usize,
}

/// Opens a writer over `object_store` that does not notice it has been
/// superseded, a second that supersedes it and drops an index, and lets the
/// first writer's compaction worker at a compaction the second schedules.
///
/// The first writer's coordinator and manifest poll rest for an hour, as
/// those of a process stalled on a request would, while its worker polls for
/// work. The second writer's worker does not poll in time, so only the first
/// can run the compaction, and is given `chance` to.
async fn supersede(
    object_store: Arc<dyn ObjectStore>,
    database: &str,
    chance: Duration,
) -> Superseded {
    let hour = Duration::from_secs(3_600);
    let never = || {
        HashMap::from(
            ["min_compaction_sources", "max_compaction_sources"]
                .map(|option| (option.to_string(), "1000000".to_string())),
        )
    };
    let open = |settings: slatedb::Settings| {
        HelixDB::open_with_object_store_and_config(
            database.to_string(),
            Arc::clone(&object_store),
            crate::DbConfig::new().with_slate_settings(settings),
        )
    };
    let stalled = open(slatedb::Settings {
        manifest_poll_interval: hour,
        compactor_options: Some(slatedb::config::CompactorOptions {
            poll_interval: hour,
            commit_compacted_interval: hour,
            scheduler_options: never(),
            worker: Some(slatedb::config::CompactionWorkerOptions {
                compactions_poll_interval: Duration::from_millis(20),
                ..slatedb::config::CompactionWorkerOptions::default()
            }),
            ..slatedb::config::CompactorOptions::default()
        }),
        ..slatedb::Settings::default()
    })
    .await
    .expect("the first writer opens");
    stalled.wait_for_background_migrations().await;
    for seed in 0..48 {
        add(&stalled, "Gone", seed).await;
    }
    create_index(&stalled, "Gone").await;

    let db = open(slatedb::Settings {
        compactor_options: Some(slatedb::config::CompactorOptions {
            poll_interval: Duration::from_millis(50),
            commit_compacted_interval: Duration::from_millis(50),
            scheduler_options: never(),
            worker: Some(slatedb::config::CompactionWorkerOptions {
                compactions_poll_interval: hour,
                ..slatedb::config::CompactionWorkerOptions::default()
            }),
            ..slatedb::config::CompactorOptions::default()
        }),
        ..slatedb::Settings::default()
    })
    .await
    .expect("the second writer opens");
    db.wait_for_background_migrations().await;
    drop_index(&db, "Gone").await;
    let prefixes = armed_prefixes(&db).await;
    assert_eq!(pass(&db).await.tracking, 1);
    // The first writer never read the entry: its filters keep the rows.
    assert_eq!(writer(&stalled).dropped().armed(), vec![]);

    let compaction = submit_compaction_of_every_table(&db).await;
    let ran_under_the_superseded_writer =
        tokio::time::timeout(chance, finished(&db, &compaction)).await == Ok(true);
    // The second writer retires the entry if every table it tracked is gone.
    pass(&db).await;
    db.close().await.expect("the second writer closes");
    // Fenced by now, one way or the other.
    drop(stalled.close().await);

    // A writer with a worker of its own runs whatever is still scheduled,
    // then compacts every table once more.
    let db = open(slatedb::Settings {
        compactor_options: Some(slatedb::config::CompactorOptions {
            scheduler_options: never(),
            worker: Some(slatedb::config::CompactionWorkerOptions {
                compactions_poll_interval: Duration::from_millis(20),
                ..slatedb::config::CompactionWorkerOptions::default()
            }),
            ..slatedb::config::CompactorOptions::default()
        }),
        ..slatedb::Settings::default()
    })
    .await
    .expect("a writer opens");
    // A writer opens with an empty registry: a pass arms whatever entry is
    // left before the compaction that is to discard its rows.
    pass(&db).await;
    compact_every_table(&db).await;
    assert_eq!(clockless(pass(&db).await), DroppedIndexStorage::default());
    assert_eq!(stored_entries(&db).await, vec![]);
    let owned = || async {
        rows(writer(&db).db())
            .await
            .into_keys()
            .filter(|key| prefixes.iter().any(|prefix| key.starts_with(prefix)))
            .count()
    };
    // A writer applies the result of its compactor's job at its next
    // manifest poll, so rows a job discarded stay readable for a moment:
    // rows are left behind only if they are still there well after it.
    let mut rows_left_behind = owned().await;
    let read = Instant::now();
    while rows_left_behind > 0 && read.elapsed() < WAIT / 4 {
        tokio::time::sleep(SETTLE).await;
        rows_left_behind = owned().await;
    }
    db.close().await.expect("the writer closes");
    Superseded {
        ran_under_the_superseded_writer,
        rows_left_behind,
    }
}

/// Proves the compaction worker of a superseded writer cannot run a
/// compaction the writer that superseded it scheduled: with a registry that
/// never read the new writer's entries, its filters would keep their rows,
/// the new writer would see the tables it tracked replaced and retire the
/// entries, and the rows would stay stored for good.
///
/// SlateDB fences the worker, in the revision this crate pins from here on:
/// before it this contract fails with rows left behind.
#[tokio::test]
async fn a_superseded_writers_compaction_worker_leaves_no_rows_behind() {
    let token = ProcessLocalDatabaseToken::new("dropped-superseded-worker").unwrap();
    assert_eq!(
        supersede(
            token.object_store(),
            token.database(),
            Duration::from_secs(5)
        )
        .await,
        Superseded {
            ran_under_the_superseded_writer: false,
            rows_left_behind: 0
        }
    );
}
