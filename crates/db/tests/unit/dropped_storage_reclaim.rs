//! End-to-end contracts of dropped index storage, through the database
//! handle and its real compactor: what compactions discard and leave row for
//! row, when an entry retires and when it must wait, and what a requested
//! reclaim adds.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
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
use crate::encoding::v2::keys::scope::TenantId;
use crate::encoding::v2::values::encode_metadata_value;
use crate::index_lifecycle::dropped::ownership::OwnerKey;
use crate::index_lifecycle::dropped::reclaim::{DroppedStorageReclaim, ReclaimObstacle};
use crate::index_lifecycle::dropped::{self, Armed, QuarantinedDroppedGeneration};
use crate::index_lifecycle::{
    DroppedGenerationValue, IndexGenerationId, IndexId, IndexV2MetadataValue,
};
use crate::{HelixDB, HelixDbSource, HelixStorage, ProcessLocalDatabaseToken};

const PROPERTY: &str = "embedding";
const DIMENSION: usize = 32;
/// Longest wait for an operation, a compaction or a manifest poll.
const WAIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(50);
/// Two scans this far apart that read the same rows have settled.
const SETTLE: Duration = Duration::from_millis(500);

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
                    obstacle:
                        ReclaimObstacle::Busy | ReclaimObstacle::Rejected | ReclaimObstacle::Unobserved,
                    ..
                } => tokio::time::sleep(POLL).await,
                outcome @ (DroppedStorageReclaim::Idle
                | DroppedStorageReclaim::Retained { .. }
                | DroppedStorageReclaim::Incomplete {
                    obstacle: ReclaimObstacle::TimedOut | ReclaimObstacle::Closed,
                    ..
                }
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
        assert_eq!(storage.tracked_tables, tables.len());
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
    assert_eq!(armed_entry(&db).proof, Proof::Unproven);
    let tables = stored_tables(&db).await;
    assert_eq!(pass(&db).await, DroppedIndexStorage::default());
    assert_eq!(stored_entries(&db).await, vec![]);
    assert_eq!(stored_tables(&db).await, tables, "nothing was compacted");
    assert_eq!(nearest(&db, "Kept", 3, 5).await.len(), 5);
    db.close().await.expect("the writer closes");
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
    );
    let storage = disturbed.pass().await.expect("the pass runs");
    assert_eq!(
        storage,
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

    let retirer = Retirer::new(Arc::clone(shared), path, store);
    let index = retirer
        .tables
        .open_with_handle(table.clone())
        .expect("the table opens")
        .index()
        .await
        .expect("the block index reads");
    assert_eq!(index.len(), 2);
    let (_, second) = index.get(1).expect("the table has a second block");
    assert!(
        second.len() < held_prefix.len() && held_prefix.starts_with(second),
        "the second block is indexed by a key shorter than the prefix: {second:?}"
    );

    let prefixes = BTreeMap::from([
        (held_prefix.clone(), generation_entry(1)),
        (empty_prefix.clone(), generation_entry(2)),
    ]);
    assert_eq!(
        retirer
            .held(table, &prefixes)
            .await
            .expect("the table is looked into"),
        HashSet::from([generation_entry(1)])
    );
    rows.close().await.expect("the row store closes");
    db.close().await.expect("the writer closes");
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
        contradiction: "names a generation its index record still owns",
    }];
    assert_eq!(
        db.dropped_index_storage(),
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
        db.dropped_index_storage(),
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
    assert!(tracked.is_disjoint(&stored_tables(&db).await));
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
    assert!(kept.is_disjoint(&tracked));
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
                    compactions: HashSet::from([CompactionId(compaction.id().0)])
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
    assert_eq!(
        db.reclaim_dropped_index_storage(Duration::ZERO)
            .await
            .unwrap(),
        DroppedStorageReclaim::Incomplete {
            entries: 1,
            obstacle: ReclaimObstacle::TimedOut,
        }
    );
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

/// Proves a writer that runs no compaction worker of its own retires and
/// reclaims nothing: its drops still leave entries, and it still arms them.
#[tokio::test]
async fn a_writer_without_a_compaction_worker_retires_nothing() {
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
    armed_prefixes(&db).await;
    assert_eq!(
        db.dropped_index_storage(),
        DroppedIndexStorage {
            unproven: 1,
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
