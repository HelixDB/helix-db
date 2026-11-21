//! End-to-end contracts of dropped index storage reclaim, through the
//! database handle and its real compactor: what a reclaim discards, what it
//! leaves row for row, and when it must wait.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::error_code::QueryErrorCode;
use helix_ast::expr::Predicate;
use helix_ast::index::{IndexSpec, VectorDistanceMetric};
use helix_ast::query::QueryRequest;
use helix_ast::traversal::{g, Terminal, Traversal, WriteEnabled};
use helix_ast::value::PropertyInput;
use slatedb::config::{GarbageCollectorDirectoryOptions, GarbageCollectorOptions};
use slatedb::{DbReadOps, IsolationLevel};

use super::*;
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::values::encode_metadata_value;
use crate::index_lifecycle::{
    dropped, DroppedGenerationValue, IndexGenerationId, IndexId, IndexV2MetadataValue,
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

/// Reclaims on request, again while a pass loses its compaction to one the
/// scheduler picked.
async fn reclaim(db: &HelixDB) -> DroppedStorageReclaim {
    tokio::time::timeout(WAIT, async {
        loop {
            match db
                .reclaim_dropped_index_storage()
                .await
                .expect("the pass runs")
            {
                DroppedStorageReclaim::Unproven { .. } => tokio::time::sleep(POLL).await,
                outcome @ (DroppedStorageReclaim::Idle
                | DroppedStorageReclaim::Retained { .. }
                | DroppedStorageReclaim::BelowShare { .. }
                | DroppedStorageReclaim::Reclaimed { .. }) => return outcome,
            }
        }
    })
    .await
    .expect("a pass proves its compaction in time")
}

/// Compacts every table into the oldest run, as a reclaim does but without
/// its retention check: the filter alone decides what the job discards.
async fn compact_every_table(db: &HelixDB) {
    let admin = Admin::builder(
        db.inner.storage.path(),
        Arc::clone(db.inner.storage.object_store()),
    )
    .build();
    tokio::time::timeout(WAIT, async {
        loop {
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
            let id = admin
                .submit_compaction(CompactionSpec::new(sources, destination))
                .await
                .expect("the compaction is submitted")
                .id();
            let completed = loop {
                match admin
                    .read_compaction(id, None)
                    .await
                    .expect("the compaction status reads")
                    .map(|compaction| compaction.status())
                {
                    Some(CompactionStatus::Completed) => break true,
                    // Lost to a compaction the scheduler picked.
                    Some(CompactionStatus::Failed) | None => break false,
                    Some(
                        CompactionStatus::Submitted
                        | CompactionStatus::Scheduled
                        | CompactionStatus::Running
                        | CompactionStatus::Compacted,
                    ) => tokio::time::sleep(POLL).await,
                }
            };
            if completed {
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

    assert!(matches!(
        reclaim(&db).await,
        DroppedStorageReclaim::Reclaimed { entries: 1, .. }
    ));
    let expected = before
        .into_iter()
        .filter(|(key, _)| *key != entry.key() && !owned(key))
        .collect::<BTreeMap<_, _>>();
    assert_rows(&db, &expected).await;
    assert_eq!(answers(&db).await, answered);
    assert_eq!(
        db.reclaim_dropped_index_storage().await.unwrap(),
        DroppedStorageReclaim::Idle
    );
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

    drop(transaction);
    add(&db, "Other", 2).await;
    assert!(matches!(
        reclaim(&db).await,
        DroppedStorageReclaim::Reclaimed { entries: 1, .. }
    ));
    assert_discarded(&db, &prefixes).await;
    assert_eq!(stored_entries(&db).await, vec![]);
    db.close().await.expect("the writer closes");
}

/// Proves the background pass leaves a dropped generation that is a small
/// share of the stored bytes in place, entry and rows, and reclaims once the
/// dropped rows reach [`DROPPED_PERCENT_OF_LIVE`] percent of the live bytes.
#[tokio::test]
async fn the_background_pass_reclaims_only_at_the_documented_share() {
    let db = open("dropped-reclaim-share").await;
    db.wait_for_background_migrations().await;
    for seed in 0..400 {
        add(&db, "Large", seed).await;
    }
    for seed in 0..4 {
        add(&db, "Small", seed).await;
    }
    create_index(&db, "Small").await;
    drop_index(&db, "Small").await;
    let reclaimer = db
        .inner
        .dropped_reclaim
        .lock()
        .await
        .as_ref()
        .expect("an embedded writer reclaims")
        .reclaimer();
    let small = armed_prefixes(&db).await;

    let DroppedStorageReclaim::BelowShare {
        entries: 1,
        dropped_bytes,
        live_bytes,
    } = reclaimer.pass(Trigger::Share).await.expect("the pass runs")
    else {
        panic!("four dropped vectors are a small share of four hundred live ones");
    };
    assert!(dropped_bytes * 100 < live_bytes * DROPPED_PERCENT_OF_LIVE);
    assert_eq!(stored_entries(&db).await.len(), 1);
    assert!(rows(writer(&db).db())
        .await
        .keys()
        .any(|key| small.iter().any(|prefix| key.starts_with(prefix))));

    create_index(&db, "Large").await;
    drop_index(&db, "Large").await;
    let both = armed_prefixes(&db).await;
    let outcome = tokio::time::timeout(WAIT, async {
        loop {
            match reclaimer.pass(Trigger::Share).await.expect("the pass runs") {
                DroppedStorageReclaim::Unproven { .. } => tokio::time::sleep(POLL).await,
                outcome @ (DroppedStorageReclaim::Idle
                | DroppedStorageReclaim::Retained { .. }
                | DroppedStorageReclaim::BelowShare { .. }
                | DroppedStorageReclaim::Reclaimed { .. }) => break outcome,
            }
        }
    })
    .await
    .expect("a pass proves its compaction in time");
    assert!(
        matches!(outcome, DroppedStorageReclaim::Reclaimed { entries: 2, .. }),
        "an index over every large row is a large share: {outcome:?}"
    );
    assert_discarded(&db, &both).await;
    assert_eq!(stored_entries(&db).await, vec![]);
    db.close().await.expect("the writer closes");
}

/// Proves a reopened writer arms the entries it finds, and that a reclaim
/// which fails after its compaction but before it deletes its entries leaves
/// them for the next writer to reclaim again.
#[test]
fn a_reclaim_that_fails_before_its_entry_delete_repeats_after_a_reopen() {
    isolated(|| async {
        let token = ProcessLocalDatabaseToken::new("dropped-reclaim-reopen").unwrap();
        let reopened = || {
            let token = token.clone();
            async move {
                HelixDB::open(HelixDbSource::InMemoryToken { token })
                    .await
                    .expect("the writer opens")
            }
        };
        let armed_entries = |db: &HelixDB| {
            writer(db)
                .dropped()
                .armed()
                .into_iter()
                .map(|(entry, _)| entry)
                .collect::<Vec<_>>()
        };

        let db = reopened().await;
        for seed in 0..48 {
            add(&db, "Gone", seed).await;
        }
        create_index(&db, "Gone").await;
        drop_index(&db, "Gone").await;
        let entries = stored_entries(&db).await;
        assert_eq!(entries.len(), 1);
        db.close().await.expect("the writer closes");

        let db = reopened().await;
        assert_eq!(armed_entries(&db), entries);
        let prefixes = armed_prefixes(&db).await;
        failpoints::inject_once(IndexOutboxFailpoint::DroppedEntryDeleteBefore).unwrap();
        tokio::time::timeout(WAIT, async {
            // A pass that loses its compaction never reaches the failpoint.
            while db.reclaim_dropped_index_storage().await.is_ok() {
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .expect("a pass reaches its entry delete in time");
        assert!(failpoints::was_triggered());
        assert_eq!(stored_entries(&db).await, entries);
        assert_discarded(&db, &prefixes).await;
        db.close().await.expect("the writer closes");

        let db = reopened().await;
        assert_eq!(armed_entries(&db), entries);
        assert!(matches!(
            reclaim(&db).await,
            DroppedStorageReclaim::Reclaimed { entries: 1, .. }
        ));
        assert_eq!(stored_entries(&db).await, vec![]);
        assert_discarded(&db, &prefixes).await;
        db.close().await.expect("the writer closes");
    });
}

/// Proves a writer refuses to open a store whose entry the catalog
/// contradicts, instead of running compactions that could discard live rows.
#[tokio::test]
async fn a_store_with_an_entry_the_catalog_contradicts_does_not_open() {
    let token = ProcessLocalDatabaseToken::new("dropped-reclaim-contradicted").unwrap();
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("the writer opens");
    let entry = DroppedGeneration {
        index_id: IndexId::new(1 << 40).unwrap(),
        generation: IndexGenerationId::initial(),
    };
    writer(&db)
        .db()
        .put(
            entry.key(),
            encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(
                DroppedGenerationValue {
                    scope: DataScope::LegacyUnscoped,
                    rows: DroppedGenerationRows::Secondary,
                },
            )),
        )
        .await
        .expect("the fixture entry is written");
    db.close().await.expect("the writer closes");

    let error = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .err()
        .expect("the open fails");
    assert!(
        matches!(
            &error,
            HelixDbError::IndexCatalogCorruption(message)
                if message.contains("names an index id above the watermark")
        ),
        "{error}"
    );
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
    assert!(matches!(
        outcome,
        DroppedStorageReclaim::Reclaimed { entries: 1, .. }
    ));
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
        reader.reclaim_dropped_index_storage().await,
        Err(HelixDbError::WriterModeRequired { .. })
    ));
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
    let reclaimed = reclaim(&db).await;
    assert!(
        matches!(
            reclaimed,
            DroppedStorageReclaim::Reclaimed { entries: 4, .. }
        ),
        "{reclaimed:?}"
    );
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
        db.reclaim_dropped_index_storage().await.unwrap(),
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
                assert!(matches!(
                    reclaim(&db).await,
                    DroppedStorageReclaim::Reclaimed { entries: 1, .. }
                ));
                assert_discarded(&db, &prefixes).await;
                let every_file = Some(GarbageCollectorDirectoryOptions {
                    interval: None,
                    min_age: Duration::ZERO,
                    dry_run: false,
                });
                Admin::builder(
                    db.inner.storage.path(),
                    Arc::clone(db.inner.storage.object_store()),
                )
                .build()
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
    assert!(matches!(
        reclaim(&db).await,
        DroppedStorageReclaim::Reclaimed { entries: 1, .. }
    ));
    assert_eq!(stored_entries(&db).await, vec![]);
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
