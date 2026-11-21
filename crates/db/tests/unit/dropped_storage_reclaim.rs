//! End-to-end contracts of dropped index storage reclaim, through the
//! database handle and its real compactor: what a reclaim discards, what it
//! leaves row for row, and when it must wait.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::time::Instant;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::index::{IndexSpec, VectorDistanceMetric};
use helix_ast::query::QueryRequest;
use helix_ast::traversal::g;
use helix_ast::value::PropertyInput;
use slatedb::{DbReadOps, IsolationLevel};

use super::*;
use crate::encoding::v2::keys::scope::DataScope;
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

/// The ids of the `k` nodes of `label` nearest `vector(seed)`.
async fn nearest(db: &HelixDB, label: &str, seed: usize, k: usize) -> Vec<u64> {
    let response = db
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "ids",
                    g().vector_search_nodes_with(
                        label,
                        PROPERTY,
                        PropertyInput::from(vector(seed)),
                        k,
                        None::<PropertyInput>,
                    )
                    .id(),
                )
                .returning(["ids"]),
        ))
        .await
        .expect("the search runs");
    response["ids"]
        .as_array()
        .expect("the search returns ids")
        .iter()
        .map(|id| id.as_u64().expect("ids are numeric"))
        .collect()
}

/// Waits for the operation a DDL statement started to succeed.
async fn succeeded(db: &HelixDB, receipt: &serde_json::Value) {
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .unwrap_or_else(|| panic!("the statement starts an operation: {receipt}"));
    tokio::time::timeout(WAIT, async {
        loop {
            let status = db
                .query(QueryRequest::read(
                    read_batch()
                        .var_as("status", g().get_index_operation(operation_id))
                        .returning(["status"]),
                ))
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
    let receipt = db
        .query(QueryRequest::write(
            write_batch()
                .var_as("operation", g().create_index_if_not_exists(spec(label)))
                .returning(["operation"]),
        ))
        .await
        .expect("the index build is accepted");
    succeeded(db, &receipt).await;
}

async fn drop_index(db: &HelixDB, label: &str) {
    let receipt = db
        .query(QueryRequest::write(
            write_batch()
                .var_as("operation", g().drop_index(spec(label)))
                .returning(["operation"]),
        ))
        .await
        .expect("the index drop is accepted");
    succeeded(db, &receipt).await;
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
