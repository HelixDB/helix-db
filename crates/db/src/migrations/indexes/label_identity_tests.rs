use std::num::NonZeroU64;
use std::sync::Arc;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::graph::NodeRef;
use helix_ast::query::QueryRequest;
use helix_ast::traversal::g;
use helix_ast::value::PropertyInput;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::DbReadOps;
use tokio::sync::Notify;

use super::*;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey};
use crate::encoding::v2::values::encode_metadata_value;
use crate::encoding::v2::values::indexes::SecondaryEqualityBitmapValue;
use crate::index_lifecycle::repository::{LabelRowWrites, ReaderStorageCompatibility};
use crate::index_lifecycle::IndexV2MetadataValue;

type Rows = BTreeMap<Bytes, RoaringTreemap>;

const DB_PATH: &str = "label-identity";

/// Holds a copy step between its reads and its commit: it signals `reached`,
/// then waits for `resume`.
pub(super) struct CommitPause {
    reached: Arc<Notify>,
    resume: Arc<Notify>,
}

pub(super) async fn pause(pause: Option<CommitPause>) {
    let Some(pause) = pause else {
        return;
    };
    pause.reached.notify_one();
    pause.resume.notified().await;
}

fn scopes() -> [DataScope; 3] {
    [
        DataScope::LegacyUnscoped,
        DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000A").unwrap()),
        DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000B").unwrap()),
    ]
}

fn marker_key() -> Bytes {
    ManagedIndexKey::Global {
        kind: GlobalKey::StorageVersion,
    }
    .to_bytes()
}

fn metadata(name: &[u8]) -> Bytes {
    super::super::super::scoped_metadata_key(DataScope::LegacyUnscoped, name)
}

/// Two people who know each other and both work at one company.
async fn write_graph(db: &crate::HelixDB, scope: DataScope) {
    let since = || vec![("since", PropertyInput::from(2020i64))];
    db.query_scoped(
        QueryRequest::write(
            write_batch()
                .var_as(
                    "ada",
                    g().add_n("Person", vec![("name", PropertyInput::from("ada"))]),
                )
                .var_as(
                    "bob",
                    g().add_n("Person", vec![("name", PropertyInput::from("bob"))]),
                )
                .var_as(
                    "acme",
                    g().add_n("Company", vec![("name", PropertyInput::from("acme"))]),
                )
                .var_as(
                    "knows",
                    g().n(NodeRef::var("ada"))
                        .add_e("KNOWS", NodeRef::var("bob"), since()),
                )
                .var_as(
                    "ada_works",
                    g().n(NodeRef::var("ada"))
                        .add_e("WORKS_AT", NodeRef::var("acme"), since()),
                )
                .var_as(
                    "bob_works",
                    g().n(NodeRef::var("bob"))
                        .add_e("WORKS_AT", NodeRef::var("acme"), since()),
                )
                .returning(Vec::<String>::new()),
        ),
        scope,
    )
    .await
    .expect("fixture graph writes");
}

/// Every label-served read shape: node and edge label scans and labeled
/// expansion in both directions.
async fn label_reads(db: &crate::HelixDB, scope: DataScope) -> serde_json::Value {
    db.query_scoped(
        QueryRequest::read(
            read_batch()
                .var_as("people", g().n_with_label("Person").count())
                .var_as("companies", g().n_with_label("Company").count())
                .var_as("knows", g().e_with_label("KNOWS").count())
                .var_as("works_at", g().e_with_label("WORKS_AT").count())
                .var_as(
                    "employers",
                    g().n_with_label("Person").out(Some("WORKS_AT")).count(),
                )
                .var_as(
                    "employees",
                    g().n_with_label("Company").in_(Some("WORKS_AT")).count(),
                )
                .returning([
                    "people",
                    "companies",
                    "knows",
                    "works_at",
                    "employers",
                    "employees",
                ]),
        ),
        scope,
    )
    .await
    .expect("label reads execute")
}

async fn all_label_reads(db: &crate::HelixDB) -> Vec<serde_json::Value> {
    let mut reads = Vec::new();
    for scope in scopes() {
        reads.push(label_reads(db, scope).await);
    }
    reads
}

/// Exact and legacy label rows of every scope.
async fn label_rows(reader: &(impl DbReadOps + Sync)) -> (Rows, Rows) {
    let mut exact = Rows::new();
    let mut legacy = Rows::new();
    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(reader)
        .await
        .unwrap()
    {
        let prefixes = label::LabelScanPrefix::ALL
            .map(|prefix| (true, prefix.to_bytes()))
            .into_iter()
            .chain(label_hash::LegacyLabelScanPrefix::ALL.map(|prefix| (false, prefix.to_bytes())));
        for (is_exact, prefix) in prefixes {
            let rows = if is_exact { &mut exact } else { &mut legacy };
            let mut scan = reader
                .scan_prefix(DataKey::data_prefix(scope, prefix), ..)
                .await
                .unwrap();
            while let Some(row) = scan.next().await.unwrap() {
                rows.insert(
                    row.key,
                    SecondaryEqualityValue::decode(&row.value)
                        .unwrap()
                        .into_ids(),
                );
            }
        }
    }
    (exact, legacy)
}

/// `rows` without empty bitmaps, which a removal may leave in either layout.
fn non_empty(rows: Rows) -> Rows {
    rows.into_iter()
        .filter(|(_, members)| !members.is_empty())
        .collect()
}

/// Each exact row under the hash key of its label, as a store before label
/// identity files it.
fn hash_keyed(exact: &Rows) -> Rows {
    exact
        .iter()
        .map(|(key, members)| {
            let (scope, logical) = DataScope::split_physical_key(key).unwrap();
            let legacy = match PropertyIndexKey::parse_from_slice(logical).unwrap() {
                PropertyIndexKey::NodeLabel(key) => DataKey::Data {
                    scope,
                    kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(
                        label_hash::LegacyNodeLabelKey::for_label(key.identity().label()),
                    )),
                }
                .to_bytes(),
                PropertyIndexKey::EdgeLabel(key) => DataKey::data_prefix(
                    scope,
                    label_hash::LegacyEdgeLabelKey::for_label(key.identity().label()).to_bytes(),
                ),
                PropertyIndexKey::EdgeLabelNeighbor(key) => DataKey::data_prefix(
                    scope,
                    label_hash::LegacyEdgeLabelNeighborKey::for_label(
                        key.direction(),
                        key.node_id(),
                        key.identity().label(),
                    )
                    .to_bytes(),
                ),
                other @ (PropertyIndexKey::Equality(_)
                | PropertyIndexKey::EdgeEquality(_)
                | PropertyIndexKey::GlobalEdgeEquality(_)
                | PropertyIndexKey::Range(_)
                | PropertyIndexKey::EdgeRange(_)
                | PropertyIndexKey::GlobalEdgeRange(_)) => {
                    panic!("label prefixes hold only label rows, got {other:?}")
                }
            };
            (legacy, members.clone())
        })
        .collect()
}

fn node_row(label: &str) -> Bytes {
    DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(
            label_hash::LegacyNodeLabelKey::for_label(label),
        )),
    }
    .to_bytes()
}

fn edge_row(label: &str) -> Bytes {
    label_hash::LegacyEdgeLabelKey::for_label(label).to_bytes()
}

fn neighbor_row(direction: EdgeDirection, node: NodeId, label: &str) -> Bytes {
    label_hash::LegacyEdgeLabelNeighborKey::for_label(direction, node, label).to_bytes()
}

fn exact_node_row(label: &str) -> Bytes {
    ExactRows::Node.key(
        DataScope::LegacyUnscoped,
        label::LabelIdentity::new(label).unwrap(),
    )
}

async fn put_legacy_row(db: &Db, key: &Bytes, ids: &[u64]) {
    crate::transaction::put_unbatched(
        db,
        key,
        &SecondaryEqualityBitmapValue::new(RoaringTreemap::from_iter(ids.iter().copied())).encode(),
    )
    .await
    .unwrap();
}

/// Stores a node record of `label` under `id` in the unscoped scope.
async fn put_node(db: &Db, id: u64, label: &str) {
    crate::transaction::put_unbatched(
        db,
        &DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(id)),
        }
        .to_bytes(),
        &encode_properties(&[Property::string("$label", label)]),
    )
    .await
    .unwrap();
}

/// A writer without the background migration worker, so a test takes every
/// copy and cleanup step itself.
async fn open_writer(store: &Arc<dyn ObjectStore>) -> crate::HelixDB {
    crate::HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(store),
        crate::config::DbConfig::new().with_migration_tuning(
            MigrationTuning::default()
                .with_worker_mode(crate::config::MigrationWorkerMode::Disabled),
        ),
    )
    .await
    .expect("writer opens")
}

/// A bare SlateDB writer of the store, which runs no startup migration.
async fn open_raw(store: &Arc<dyn ObjectStore>) -> Db {
    Db::builder(DB_PATH, Arc::clone(store))
        .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
        .build()
        .await
        .expect("raw writer opens")
}

async fn open_reader(store: &Arc<dyn ObjectStore>) -> crate::HelixDB {
    crate::HelixDB::open_reader_with_object_store_for_tests(DB_PATH, Arc::clone(store))
        .await
        .expect("a store before or during label identity is reader-compatible")
}

/// A managed failover writer of the store at `writer_epoch`, with the default
/// background migration worker.
async fn open_failover(store: &Arc<dyn ObjectStore>, writer_epoch: u64) -> Result<crate::HelixDB> {
    crate::HelixDB::open_writer_inner_with_index_scheduling(
        DB_PATH.to_string(),
        Arc::clone(store),
        crate::GcBoundaryFiles::AsConfigured,
        crate::config::DbConfig::new(),
        crate::WriterOpenMode::Managed {
            writer_epoch: NonZeroU64::new(writer_epoch).unwrap(),
            intent: crate::ManagedWriterOpenIntent::Failover,
        },
        crate::IndexLifecycleScheduling::Configured,
    )
    .await
}

fn writer_of(db: &crate::HelixDB) -> Arc<crate::HelixWriter> {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("the handle is a writer");
    };
    Arc::clone(writer)
}

/// The unscoped fixture graph's node IDs, from its legacy label rows.
fn fixture_nodes(legacy: &Rows) -> (u64, u64, u64) {
    let [ada, bob] = legacy[&node_row("Person")].iter().collect::<Vec<_>>()[..] else {
        panic!("the fixture has two people");
    };
    let acme = legacy[&node_row("Company")]
        .min()
        .expect("the fixture has a company");
    (ada, bob, acme)
}

/// A closed store with the fixture graph in every scope, filed under hash
/// keys as a binary before label identity left it, with the label reads and
/// the exact rows a current store holds for it.
async fn legacy_store() -> (Arc<dyn ObjectStore>, Vec<serde_json::Value>, Rows) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store).await;
    assert_eq!(
        storage_version(db.inner_db().as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    for scope in scopes() {
        write_graph(&db, scope).await;
    }
    let reads = all_label_reads(&db).await;
    for scope_reads in &reads {
        for (read, expected) in [
            ("people", 2),
            ("companies", 1),
            ("knows", 1),
            ("works_at", 2),
            ("employers", 2),
            ("employees", 2),
        ] {
            assert_eq!(scope_reads[read], serde_json::json!(expected), "{read}");
        }
    }
    let (exact, legacy) = label_rows(db.inner_db().as_ref()).await;
    assert!(legacy.is_empty());
    // Two node labels, two edge labels, and five neighbor rows per scope.
    assert_eq!(exact.len(), 9 * scopes().len());
    make_legacy_label_fixture(&db.inner_db()).await.unwrap();
    db.close().await.unwrap();
    (store, reads, exact)
}

/// A writer of the legacy store, which its open moved to version 6 without
/// copying a row.
async fn copying_store() -> (
    Arc<dyn ObjectStore>,
    crate::HelixDB,
    Vec<serde_json::Value>,
    Rows,
) {
    let (store, reads, exact) = legacy_store().await;
    let db = open_writer(&store).await;
    let inner = db.inner_db();
    assert_eq!(
        storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_COPY
    );
    let (copied, legacy) = label_rows(inner.as_ref()).await;
    assert!(copied.is_empty());
    assert_eq!(legacy, hash_keyed(&exact));
    (store, db, reads, exact)
}

async fn present(db: &Db, name: &[u8]) -> bool {
    db.get(metadata(name)).await.unwrap().is_some()
}

async fn copy_progress(db: &Db) -> Option<label_migration::LabelCopyProgressValue> {
    db.get(metadata(LABEL_IDENTITY_COPY_PROGRESS))
        .await
        .unwrap()
        .map(|value| label_migration::LabelCopyProgressValue::decode(&value).unwrap())
}

/// Runs one copy step, holding it between its reads and its commit while
/// `write` runs.
async fn step_racing(
    writer: &Arc<crate::HelixWriter>,
    mut session: LabelCopySession,
    write: impl std::future::Future<Output = ()>,
) -> (LabelCopyStep, LabelCopySession) {
    let reached = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    session.before_commit = Some(CommitPause {
        reached: Arc::clone(&reached),
        resume: Arc::clone(&resume),
    });
    let writer = Arc::clone(writer);
    let step = tokio::spawn(async move {
        let step = copy_hash_label_rows_once(&writer, &mut session)
            .await
            .unwrap();
        (step, session)
    });
    reached.notified().await;
    write.await;
    resume.notify_one();
    step.await.unwrap()
}

/// Takes copy steps until none is left, returning each step's outcome.
async fn copy_all(
    writer: &crate::HelixWriter,
    session: &mut LabelCopySession,
) -> Vec<LabelCopyStep> {
    let mut steps = Vec::new();
    loop {
        let step = copy_hash_label_rows_once(writer, session).await.unwrap();
        steps.push(step);
        if !step.advanced() {
            return steps;
        }
    }
}

/// Takes cleanup steps of `batch_rows` rows until the cleanup is done,
/// returning each step's outcome.
async fn clean_up(db: &Db, batch_rows: usize) -> Vec<LabelHashCleanup> {
    let tuning = MigrationTuning::default()
        .with_batch_rows(crate::config::MigrationBatchRows::new(batch_rows).unwrap());
    let mut steps = Vec::new();
    loop {
        let step = delete_hash_label_rows_once(db, tuning).await.unwrap();
        steps.push(step);
        if !step.advanced() {
            return steps;
        }
    }
}

async fn wait_for(db: &Db, name: &[u8]) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !present(db, name).await {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the background worker finishes the migration");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_starts_the_copy_and_reads_stay_exact_at_every_step() {
    let (store, db, reads, exact_before) = copying_store().await;
    let inner = db.inner_db();
    let writer = writer_of(&db);
    // The open copied nothing, and the writer reads the complete hash rows.
    assert_eq!(writer.known_label_row_writes(), None);
    assert_eq!(all_label_reads(&db).await, reads);
    let reader = open_reader(&store).await;
    assert_eq!(
        reader.reader_storage_compatibility_for_tests(),
        ReaderStorageCompatibility::LegacyLabelHash
    );
    assert_eq!(all_label_reads(&reader).await, reads);

    // One step copies the unscoped node and edge rows; every hash row stays.
    let mut session = LabelCopySession::with_limits(4, BATCH_MEMBERS);
    assert_eq!(
        copy_hash_label_rows_once(&writer, &mut session)
            .await
            .unwrap(),
        LabelCopyStep::Copied
    );
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact.len(), 4);
    assert_eq!(legacy.len(), exact_before.len());
    let Some(label_migration::LabelCopyProgressValue::Rows { row }) = copy_progress(&inner).await
    else {
        panic!("the step recorded the last row it copied");
    };
    assert_eq!(&row, legacy.keys().nth(3).unwrap());
    assert_eq!(all_label_reads(&db).await, reads);
    assert_eq!(all_label_reads(&reader).await, reads);

    let steps = copy_all(&writer, &mut session).await;
    assert_eq!(steps.last(), Some(&LabelCopyStep::Published));
    assert!(steps[..steps.len() - 1]
        .iter()
        .all(|step| *step == LabelCopyStep::Copied));
    assert_eq!(
        session.totals(),
        LabelIdentityMigration {
            copied: exact_before.len() as u64,
            ..LabelIdentityMigration::default()
        }
    );
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact, exact_before);
    // Published, but the hash rows stay until the cleanup.
    assert_eq!(legacy.len(), exact_before.len());
    assert!(!present(&inner, LABEL_IDENTITY_COPY_PROGRESS).await);
    assert_eq!(
        storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    assert_eq!(writer.known_label_row_writes(), Some(LabelRowWrites::Exact));
    assert_eq!(
        copy_hash_label_rows_once(&writer, &mut session)
            .await
            .unwrap(),
        LabelCopyStep::AlreadyPublished
    );
    assert_eq!(all_label_reads(&db).await, reads);

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            assert_eq!(all_label_reads(&reader).await, reads);
            if reader.reader_storage_compatibility_for_tests()
                == ReaderStorageCompatibility::Current
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the reader switches to exact label rows after publication");

    assert_eq!(
        clean_up(&inner, 1_024).await,
        [LabelHashCleanup::Done {
            deleted: exact_before.len() as u64
        }]
    );
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact, exact_before);
    assert!(legacy.is_empty());
    assert!(cleanup_ready(&inner).await.unwrap());
    assert_eq!(all_label_reads(&reader).await, reads);

    reader.close().await.unwrap();
    db.close().await.unwrap();
}

/// Graph writes file label rows under the keys their snapshot reads: both
/// while the copy runs, exact only once it is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_writes_file_label_rows_under_the_keys_of_their_snapshot() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let add_node = |label: &'static str| {
        let db = &db;
        async move {
            db.query(QueryRequest::write(
                write_batch()
                    .var_as(
                        "node",
                        g().add_n(label, Vec::<(&str, PropertyInput)>::new()),
                    )
                    .returning(Vec::<String>::new()),
            ))
            .await
            .unwrap();
        }
    };
    add_node("Ghost").await;
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact[&exact_node_row("Ghost")].len(), 1);
    assert_eq!(legacy[&node_row("Ghost")], exact[&exact_node_row("Ghost")]);

    copy_all(&writer_of(&db), &mut LabelCopySession::new()).await;
    add_node("Spirit").await;
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact[&exact_node_row("Spirit")].len(), 1);
    assert!(!legacy.contains_key(&node_row("Spirit")));
    db.close().await.unwrap();
}

/// Writes between steps reach the rows the copy passed and the rows it has
/// yet to read alike, so the published exact rows equal the hash rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_writes_during_the_copy_keep_both_layouts_exact() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let writer = writer_of(&db);
    let mut session = LabelCopySession::with_limits(4, BATCH_MEMBERS);
    let (ada, bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    for scope in scopes() {
        assert_eq!(
            copy_hash_label_rows_once(&writer, &mut session)
                .await
                .unwrap(),
            LabelCopyStep::Copied
        );
        // A new person who knows nobody yet, and another scope's copy of the
        // fixture growing a second company.
        db.query_scoped(
            QueryRequest::write(
                write_batch()
                    .var_as(
                        "cy",
                        g().add_n("Person", vec![("name", PropertyInput::from("cy"))]),
                    )
                    .var_as(
                        "initech",
                        g().add_n("Company", vec![("name", PropertyInput::from("initech"))]),
                    )
                    .var_as(
                        "cy_works",
                        g().n(NodeRef::var("cy")).add_e(
                            "WORKS_AT",
                            NodeRef::var("initech"),
                            Vec::<(&str, PropertyInput)>::new(),
                        ),
                    )
                    .var_as(
                        "cy_mentors",
                        g().n(NodeRef::var("cy")).add_e(
                            "MENTORS",
                            NodeRef::var("cy"),
                            Vec::<(&str, PropertyInput)>::new(),
                        ),
                    )
                    .returning(Vec::<String>::new()),
            ),
            scope,
        )
        .await
        .unwrap();
    }
    // Bob leaves in the unscoped scope: rows the copy passed and rows it has
    // not read lose him alike.
    db.query(QueryRequest::write(
        write_batch()
            .var_as("bob", g().n(NodeRef::id(bob)).drop())
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    let reads = all_label_reads(&db).await;
    for (scope, scope_reads) in scopes().into_iter().zip(&reads) {
        let unscoped = scope == DataScope::LegacyUnscoped;
        assert_eq!(
            scope_reads["people"],
            serde_json::json!(if unscoped { 2 } else { 3 })
        );
        assert_eq!(scope_reads["companies"], serde_json::json!(2));
        assert_eq!(
            scope_reads["works_at"],
            serde_json::json!(if unscoped { 2 } else { 3 })
        );
    }

    assert_eq!(
        copy_all(&writer, &mut session).await.last(),
        Some(&LabelCopyStep::Published)
    );
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(hash_keyed(&non_empty(exact.clone())), non_empty(legacy));
    assert!(!exact[&exact_node_row("Person")].contains(bob));
    assert!(exact[&exact_node_row("Person")].contains(ada));
    assert!(exact[&exact_node_row("Company")].contains(acme));
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

/// A graph write that removes a member a batch copies, committed after the
/// batch began, makes the batch conflict; one that only adds a member does
/// not. The conflicted batch is taken again and copies what is there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_that_races_a_batch_conflicts_and_the_batch_is_taken_again() {
    let (_store, db, _reads, exact_before) = copying_store().await;
    let db = Arc::new(db);
    let inner = db.inner_db();
    let (ada, bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    let writer = writer_of(&db);
    let mut session = LabelCopySession::with_limits(4, BATCH_MEMBERS);

    // The first batch copies the unscoped node and edge rows. A new person
    // adds a member no row of the batch holds yet.
    let (step, next) = step_racing(&writer, session, async {
        db.query(QueryRequest::write(
            write_batch()
                .var_as(
                    "cy",
                    g().add_n("Person", vec![("name", PropertyInput::from("cy"))]),
                )
                .returning(Vec::<String>::new()),
        ))
        .await
        .unwrap();
    })
    .await;
    session = next;
    assert_eq!(step, LabelCopyStep::Copied);
    let people = label_rows(inner.as_ref()).await.0[&exact_node_row("Person")].clone();
    assert_eq!(people.len(), 3);
    assert!(people.contains(ada) && people.contains(bob));

    // The next batch copies the unscoped neighbor rows, which hold Bob.
    let before = copy_progress(&inner).await;
    let (step, next) = step_racing(&writer, session, async {
        db.query(QueryRequest::write(
            write_batch()
                .var_as("bob", g().n(NodeRef::id(bob)).drop())
                .returning(Vec::<String>::new()),
        ))
        .await
        .unwrap();
    })
    .await;
    session = next;
    assert_eq!(step, LabelCopyStep::Conflicted);
    assert_eq!(session.totals().conflicts, 1);
    assert_eq!(copy_progress(&inner).await, before);

    assert_eq!(
        copy_all(&writer, &mut session).await.last(),
        Some(&LabelCopyStep::Published)
    );
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(hash_keyed(&non_empty(exact.clone())), non_empty(legacy));
    let in_acme = ExactRows::Neighbor(EdgeDirection::In, acme).key(
        DataScope::LegacyUnscoped,
        label::LabelIdentity::new("WORKS_AT").unwrap(),
    );
    assert_eq!(exact[&in_acme], RoaringTreemap::from_iter([ada]));
    // Only the rows Bob's edges alone filled are gone, from both layouts.
    assert_eq!(
        exact_before
            .keys()
            .filter(|key| !exact.contains_key(*key))
            .count(),
        4
    );
    db.close().await.unwrap();
}

/// A row with more members than a batch copies is copied a range of
/// members at a time, each step recording the last member it copied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_larger_than_a_batch_is_copied_a_range_of_members_at_a_time() {
    let (_store, db, reads, exact_before) = copying_store().await;
    let inner = db.inner_db();
    let writer = writer_of(&db);
    let mut session = LabelCopySession::with_limits(BATCH_ROWS, 1);
    let (ada, bob, _acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    let person = node_row("Person");
    let mut partial = 0;
    loop {
        let step = copy_hash_label_rows_once(&writer, &mut session)
            .await
            .unwrap();
        if !step.advanced() {
            assert_eq!(step, LabelCopyStep::Published);
            break;
        }
        let Some(label_migration::LabelCopyProgressValue::Members { row, through }) =
            copy_progress(&inner).await
        else {
            continue;
        };
        partial += 1;
        if row != person {
            continue;
        }
        assert_eq!(through, ada.min(bob));
        assert_eq!(
            label_rows(inner.as_ref()).await.0[&exact_node_row("Person")],
            RoaringTreemap::from_iter([ada.min(bob)])
        );
    }
    // In each scope the person row, the works-at row, and Acme's incoming
    // works-at row hold two members each.
    assert_eq!(partial, 3 * scopes().len());
    assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

/// Progress is the only state a resumed copy needs, so a later open picks up
/// after the last batch an earlier one committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_open_resumes_the_copy_after_its_last_batch() {
    let (store, db, reads, exact_before) = copying_store().await;
    let mut session = LabelCopySession::with_limits(4, BATCH_MEMBERS);
    for _ in 0..3 {
        assert_eq!(
            copy_hash_label_rows_once(&writer_of(&db), &mut session)
                .await
                .unwrap(),
            LabelCopyStep::Copied
        );
    }
    db.close().await.unwrap();

    let db = open_writer(&store).await;
    let inner = db.inner_db();
    assert_eq!(label_rows(inner.as_ref()).await.0.len(), 12);
    assert_eq!(all_label_reads(&db).await, reads);
    let mut session = LabelCopySession::new();
    assert_eq!(
        copy_all(&writer_of(&db), &mut session).await,
        [LabelCopyStep::Published]
    );
    assert_eq!(session.totals().copied as usize, exact_before.len() - 12);
    assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

/// A failover writer runs no other migration, but it serves a store whose
/// copy is running and finishes both the copy and the cleanup.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failover_writer_serves_and_finishes_the_copy() {
    let (store, db, reads, exact_before) = copying_store().await;
    assert_eq!(
        copy_hash_label_rows_once(
            &writer_of(&db),
            &mut LabelCopySession::with_limits(4, BATCH_MEMBERS)
        )
        .await
        .unwrap(),
        LabelCopyStep::Copied
    );
    let epoch = db.inner_db().manifest().writer_epoch();
    db.close().await.unwrap();

    let db = open_failover(&store, epoch + 1)
        .await
        .expect("failover opens a store whose copy is running");
    let inner = db.inner_db();
    wait_for(&inner, LABEL_HASH_CLEANUP_READY).await;
    assert_eq!(
        storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact, exact_before);
    assert!(legacy.is_empty());
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_open_starts_the_copy_and_the_background_worker_finishes_it() {
    let (store, reads, exact_before) = legacy_store().await;

    // The default configuration runs the background migration worker.
    let db = crate::HelixDB::open_with_object_store_for_tests(DB_PATH, Arc::clone(&store))
        .await
        .expect("writer opens");
    let inner = db.inner_db();
    assert_eq!(all_label_reads(&db).await, reads);
    wait_for(&inner, LABEL_HASH_CLEANUP_READY).await;
    assert_eq!(
        storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact, exact_before);
    assert!(legacy.is_empty());
    assert!(!present(&inner, LABEL_HASH_CLEANUP_PROGRESS).await);
    assert!(!present(&inner, LABEL_IDENTITY_COPY_PROGRESS).await);
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

/// A write transaction that read version 6 cannot commit after publication:
/// it would file hash rows the cleanup may already have passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_that_read_the_copy_running_conflicts_with_publication() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let writer = writer_of(&db);
    let transaction = crate::transaction::Owned::begin(&inner, None, None)
        .await
        .unwrap();
    assert_eq!(transaction.label_rows(), LabelRowWrites::HashAndExact);
    Mutation::put(
        &transaction,
        metadata(b"label_identity_test"),
        Bytes::from_static(b"1"),
    )
    .unwrap();
    copy_all(&writer, &mut LabelCopySession::new()).await;
    let Err(error) = transaction
        .commit(crate::transaction::CommitDurability::Applied)
        .await
    else {
        panic!("the transaction read the marker publication changed");
    };
    assert_eq!(error.kind(), slatedb::ErrorKind::Transaction);
    let transaction = crate::transaction::Owned::begin(&inner, None, None)
        .await
        .unwrap();
    assert_eq!(transaction.label_rows(), LabelRowWrites::Exact);
    db.close().await.unwrap();
}

/// A reader before publication reads hash rows, where two types whose labels
/// share a hash share one row: a relationship scan of both reads that row
/// twice but binds each edge once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relationship_scan_binds_a_shared_hash_row_once() {
    let (store, _reads, _exact) = legacy_store().await;
    let raw = open_raw(&store).await;
    let knows = label_rows(&raw).await.1[&edge_row("KNOWS")].clone();
    // As if "KNOWS" and "MET" shared a hash.
    put_legacy_row(&raw, &edge_row("MET"), &knows.iter().collect::<Vec<_>>()).await;
    raw.close().await.unwrap();

    let reader = open_reader(&store).await;
    assert_eq!(
        reader.reader_storage_compatibility_for_tests(),
        ReaderStorageCompatibility::LegacyLabelHash
    );
    for (query, count) in [
        ("MATCH ()-[r:KNOWS]->() RETURN count(r)", 1),
        ("MATCH ()-[r:KNOWS|MET]->() RETURN count(r)", 1),
        ("MATCH ()-[r:KNOWS|WORKS_AT|MET]->() RETURN count(r)", 3),
    ] {
        let result = reader
            .cypher(crate::cypher::Request::new(query))
            .await
            .unwrap();
        assert_eq!(result.rows, vec![vec![serde_json::json!(count)]], "{query}");
    }
    reader.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_graph_rewrite_of_a_published_store_stages_no_hash_row() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = crate::HelixDB::open_writer_inner_with_index_scheduling(
        DB_PATH.to_string(),
        Arc::clone(&store),
        crate::GcBoundaryFiles::AsConfigured,
        crate::config::DbConfig::new().with_migration_tuning(
            MigrationTuning::default()
                .with_worker_mode(crate::config::MigrationWorkerMode::Disabled),
        ),
        crate::WriterOpenMode::Managed {
            writer_epoch: NonZeroU64::MIN,
            intent: crate::ManagedWriterOpenIntent::Bootstrap,
        },
        crate::IndexLifecycleScheduling::Configured,
    )
    .await
    .expect("a managed bootstrap opens an empty store");
    write_graph(&db, DataScope::LegacyUnscoped).await;
    let reads = label_reads(&db, DataScope::LegacyUnscoped).await;
    assert_eq!(
        clean_up(&db.inner_db(), 1_024).await,
        [LabelHashCleanup::Done { deleted: 0 }]
    );
    let (exact, legacy) = label_rows(db.inner_db().as_ref()).await;
    assert!(legacy.is_empty());
    db.close().await.unwrap();

    let db = open_writer(&store).await;
    let inner = db.inner_db();
    assert_eq!(label_rows(inner.as_ref()).await, (exact, Rows::new()));
    assert_eq!(label_reads(&db, DataScope::LegacyUnscoped).await, reads);
    assert_eq!(
        clean_up(&inner, 1_024).await,
        [LabelHashCleanup::AlreadyDone]
    );
    db.close().await.unwrap();
}

/// The public label helpers serve a published store: they read and write
/// exact rows whatever the stored marker says, and every equality helper
/// serves `$label` from the node label index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_label_helpers_read_and_write_exact_rows() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let (ada, bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    let scope = DataScope::LegacyUnscoped;
    let lookups = || async {
        let read = inner.begin(IsolationLevel::Snapshot).await.unwrap();
        (
            crate::search::lookup_node_label_index_scoped(&read, "Person", scope)
                .await
                .unwrap(),
            crate::search::lookup_equality_index_set_scoped(&read, "$label", "Person", scope)
                .await
                .unwrap(),
            crate::search::lookup_global_edge_label_index_scoped(&read, "WORKS_AT", scope)
                .await
                .unwrap()
                .len(),
            crate::search::lookup_in_neighbors_by_label_scoped(&read, acme, "WORKS_AT", scope)
                .await
                .unwrap(),
        )
    };
    let people = RoaringTreemap::from_iter([ada, bob]);
    let expected = (people.clone(), people.clone(), 2, people.clone());
    // Before the copy only the hash rows hold the graph.
    assert_eq!(
        lookups().await,
        (
            RoaringTreemap::new(),
            RoaringTreemap::new(),
            0,
            RoaringTreemap::new()
        )
    );
    let write = inner
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    crate::search::add_to_equality_index_scoped(&write, "$label", "Ghost", 99, scope).unwrap();
    crate::search::add_to_global_edge_label_index_scoped(&write, "HAUNTS", 98, scope).unwrap();
    write.commit().await.unwrap();
    let read = inner.begin(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(
        crate::search::lookup_equality_index_set_scoped(&read, "$label", "Ghost", scope)
            .await
            .unwrap(),
        RoaringTreemap::from_iter([99])
    );
    assert_eq!(
        crate::search::lookup_global_edge_label_index_scoped(&read, "HAUNTS", scope)
            .await
            .unwrap(),
        RoaringTreemap::from_iter([98])
    );
    drop(read);
    let write = inner
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    crate::search::remove_from_equality_index_scoped(&write, "$label", "Ghost", 99, scope).unwrap();
    crate::search::remove_from_global_edge_label_index_scoped(&write, "HAUNTS", 98, scope).unwrap();
    write.commit().await.unwrap();

    copy_all(&writer_of(&db), &mut LabelCopySession::new()).await;
    assert_eq!(lookups().await, expected);
    clean_up(&inner, 1_024).await;
    assert_eq!(lookups().await, expected);

    // `$label` equality writes maintain the exact node label row.
    let write = inner
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    crate::search::add_to_equality_index_scoped(&write, "$label", "Person", 77, scope).unwrap();
    crate::search::remove_from_equality_index_scoped(&write, "$label", "Person", bob, scope)
        .unwrap();
    // Removing a label too long for any key removes nothing, as for graph
    // writes.
    let long = "L".repeat(label::MAX_LABEL_LEN + 1);
    crate::search::remove_from_equality_index_scoped(&write, "$label", &long, ada, scope).unwrap();
    crate::search::remove_from_edge_label_index_scoped(&write, ada, acme, &long, scope).unwrap();
    crate::search::remove_from_global_edge_label_index_scoped(&write, &long, 1, scope).unwrap();
    write.commit().await.unwrap();
    let people = RoaringTreemap::from_iter([ada, 77]);
    let (by_label, by_equality, _, _) = lookups().await;
    assert_eq!((by_label, by_equality), (people.clone(), people));

    // A `$label` prefix scan reads every node label row, and deleting
    // `$label`'s rows deletes them.
    let read = inner.begin(IsolationLevel::Snapshot).await.unwrap();
    assert_eq!(
        crate::search::scan_equality_index_property_prefix_limited_filtered_scoped(
            &read, "$label", 10, None, scope,
        )
        .await
        .unwrap(),
        RoaringTreemap::from_iter([ada, 77, acme])
    );
    drop(read);
    let write = inner
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    crate::search::delete_equality_index_entries_for_property(&write, "$label")
        .await
        .unwrap();
    write.commit().await.unwrap();
    let (exact, _legacy) = label_rows(inner.as_ref()).await;
    assert!(!exact.contains_key(&exact_node_row("Person")));
    assert!(!exact.contains_key(&exact_node_row("Company")));
    assert!(exact
        .contains_key(&ExactRows::Edge.key(scope, label::LabelIdentity::new("WORKS_AT").unwrap())));
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rows_without_a_live_member_are_not_copied_and_are_cleaned_up() {
    let (_store, db, _reads, exact_before) = copying_store().await;
    let inner = db.inner_db();
    put_legacy_row(&inner, &node_row("Ghost"), &[9_999]).await;
    put_legacy_row(&inner, &edge_row("HAUNTS"), &[9_999]).await;

    let mut session = LabelCopySession::new();
    copy_all(&writer_of(&db), &mut session).await;
    assert_eq!(session.totals().copied, exact_before.len() as u64);
    assert_eq!(session.totals().skipped, 2);
    assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
    assert_eq!(
        clean_up(&inner, 1_024).await,
        [LabelHashCleanup::Done {
            deleted: exact_before.len() as u64 + 2
        }]
    );
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact, exact_before);
    assert!(legacy.is_empty());
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_members_are_left_out_of_the_row_they_no_longer_belong_to() {
    let (_store, db, reads, exact_before) = copying_store().await;
    let inner = db.inner_db();
    let (ada, bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    // Ada and Bob, both Persons, stayed in the Company row after a label
    // change, below and above Acme, its one real member.
    assert!(ada < acme && bob < acme);
    put_node(&inner, 9_999, "Person").await;
    put_legacy_row(&inner, &node_row("Company"), &[ada, bob, acme, 9_999]).await;

    let mut session = LabelCopySession::new();
    copy_all(&writer_of(&db), &mut session).await;
    assert_eq!(session.totals().stale_members, 3);
    assert_eq!(session.totals().copied, exact_before.len() as u64);
    let (exact, _legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact, exact_before);
    assert_eq!(
        exact[&exact_node_row("Company")],
        RoaringTreemap::from_iter([acme])
    );
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_labels_naming_one_row_fail_closed() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let (ada, bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    let record = |id| DataKeyKind::NodeProperty(NodePropertyKey::new(id));
    let members = RoaringTreemap::from_iter([ada, bob, acme, 9_999]);

    // As if "Person" and "Company" shared a hash: the lowest and highest
    // live members disagree.
    assert!(matches!(
        row_label(&inner, DataScope::LegacyUnscoped, &members, record, |_| {
            true
        })
        .await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    // A dead highest member is passed over for the next live one.
    let RowLabel { label, stale } = row_label(
        &inner,
        DataScope::LegacyUnscoped,
        &RoaringTreemap::from_iter([ada, bob, 9_999]),
        record,
        |_| true,
    )
    .await
    .unwrap()
    .expect("the people name the row");
    assert_eq!(label.identity().label(), "Person");
    assert!(stale.is_empty());
    // No live member whose label hashes to the row names nothing.
    assert!(
        row_label(&inner, DataScope::LegacyUnscoped, &members, record, |_| {
            false
        })
        .await
        .unwrap()
        .is_none()
    );

    // Two edge labels one scope files under one hash fail closed too.
    let mut labels = BTreeMap::new();
    let shared = |text: &str| label::SharedLabel::new(label::LabelIdentity::new(text).unwrap());
    let hash = hash_property_value("KNOWS");
    record_edge_label(
        &mut labels,
        DataScope::LegacyUnscoped,
        hash,
        shared("KNOWS"),
    )
    .unwrap();
    record_edge_label(
        &mut labels,
        DataScope::LegacyUnscoped,
        hash,
        shared("KNOWS"),
    )
    .unwrap();
    assert!(matches!(
        record_edge_label(&mut labels, DataScope::LegacyUnscoped, hash, shared("MET")),
        Err(HelixDbError::MigrationRequired { .. })
    ));

    db.close().await.unwrap();
}

/// A record with no string `$label` carries no label, so like a member whose
/// label hashes elsewhere it names no row and is left out of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn members_without_a_string_label_are_left_out_as_stale() {
    let (_store, db, reads, exact_before) = copying_store().await;
    let inner = db.inner_db();
    let (_ada, _bob, acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    for (id, properties) in [
        (9_001, vec![Property::string("name", "nameless")]),
        (9_002, vec![Property::i64("$label", 7)]),
    ] {
        crate::transaction::put_unbatched(
            &inner,
            &DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(id)),
            }
            .to_bytes(),
            &encode_properties(&properties),
        )
        .await
        .unwrap();
    }
    // Beside Acme, the row's one labeled member, and alone.
    put_legacy_row(&inner, &node_row("Company"), &[acme, 9_001, 9_002]).await;
    put_legacy_row(&inner, &node_row("Nameless"), &[9_001, 9_002]).await;

    let mut session = LabelCopySession::new();
    copy_all(&writer_of(&db), &mut session).await;
    // The Company row leaves both out; no member names the other row, so it
    // is skipped whole.
    assert_eq!(session.totals().stale_members, 2);
    assert_eq!(
        (session.totals().copied, session.totals().skipped),
        (exact_before.len() as u64, 1)
    );
    let (exact, _legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact, exact_before);
    assert_eq!(
        exact[&exact_node_row("Company")],
        RoaringTreemap::from_iter([acme])
    );
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neighbor_rows_no_edge_label_explains_are_not_copied() {
    let (_store, db, reads, exact_before) = copying_store().await;
    let inner = db.inner_db();
    let (ada, bob, _acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    // Every HAUNTS edge is gone but its neighbor rows remain.
    put_legacy_row(&inner, &edge_row("HAUNTS"), &[9_999]).await;
    for (direction, node, neighbor) in [
        (EdgeDirection::Out, ada, bob),
        (EdgeDirection::In, bob, ada),
    ] {
        put_legacy_row(
            &inner,
            &neighbor_row(direction, node, "HAUNTS"),
            &[neighbor],
        )
        .await;
    }

    let mut session = LabelCopySession::new();
    copy_all(&writer_of(&db), &mut session).await;
    assert_eq!(
        (session.totals().copied, session.totals().skipped),
        (exact_before.len() as u64, 3)
    );
    assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

/// A neighbor row resumed in a later step, whose edge rows an earlier step
/// copied, takes its label from the scope's exact edge rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_names_neighbor_rows_from_edge_rows_an_earlier_step_copied() {
    let (_store, db, reads, exact_before) = copying_store().await;
    let inner = db.inner_db();
    let writer = writer_of(&db);
    copy_hash_label_rows_once(
        &writer,
        &mut LabelCopySession::with_limits(4, BATCH_MEMBERS),
    )
    .await
    .unwrap();
    let mut session = LabelCopySession::new();
    copy_all(&writer, &mut session).await;
    assert_eq!(session.totals().skipped, 0);
    assert_eq!(session.totals().copied as usize, exact_before.len() - 4);
    assert_eq!(label_rows(inner.as_ref()).await.0, exact_before);
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_naming_member_whose_label_no_key_holds_fails_closed() {
    let (_store, db, reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let long = "L".repeat(label::MAX_LABEL_LEN + 1);
    put_node(&inner, 9_999, &long).await;
    put_legacy_row(&inner, &node_row(&long), &[9_999]).await;

    assert!(matches!(
        copy_hash_label_rows_once(&writer_of(&db), &mut LabelCopySession::new()).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    // The store keeps serving from its hash rows.
    assert_eq!(
        storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_COPY
    );
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleanup_waits_for_publication_and_resumes_after_every_batch() {
    let (store, db, _reads, exact_before) = copying_store().await;
    let inner = db.inner_db();
    assert_eq!(clean_up(&inner, 4).await, [LabelHashCleanup::Unpublished]);
    assert_eq!(label_rows(inner.as_ref()).await.1.len(), exact_before.len());

    copy_all(&writer_of(&db), &mut LabelCopySession::new()).await;
    let tuning = MigrationTuning::default()
        .with_batch_rows(crate::config::MigrationBatchRows::new(4).unwrap());
    assert_eq!(
        delete_hash_label_rows_once(&inner, tuning).await.unwrap(),
        LabelHashCleanup::Advanced { deleted: 4 }
    );
    assert!(present(&inner, LABEL_HASH_CLEANUP_PROGRESS).await);
    assert!(!present(&inner, LABEL_HASH_CLEANUP_READY).await);
    assert_eq!(
        label_rows(inner.as_ref()).await.1.len(),
        exact_before.len() - 4
    );
    db.close().await.unwrap();

    // A later open resumes after the last deleted row, a batch per step.
    let db = open_writer(&store).await;
    let inner = db.inner_db();
    let steps = clean_up(&inner, 4).await;
    let remaining = exact_before.len() as u64 - 4;
    let (last, batches) = steps.split_last().unwrap();
    assert!(batches
        .iter()
        .all(|step| *step == LabelHashCleanup::Advanced { deleted: 4 }));
    assert_eq!(
        *last,
        LabelHashCleanup::Done {
            deleted: remaining % 4
        }
    );
    assert_eq!(batches.len() as u64, remaining / 4);
    assert!(label_rows(inner.as_ref()).await.1.is_empty());
    assert!(!present(&inner, LABEL_HASH_CLEANUP_PROGRESS).await);
    assert!(cleanup_ready(&inner).await.unwrap());
    db.close().await.unwrap();
}

/// A failover writer runs no other migration, but it finishes the cleanup of
/// a published copy whose writer it took over from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failover_writer_finishes_the_cleanup() {
    let (store, db, reads, exact_before) = copying_store().await;
    copy_all(&writer_of(&db), &mut LabelCopySession::new()).await;
    let epoch = db.inner_db().manifest().writer_epoch();
    db.close().await.unwrap();

    let db = open_failover(&store, epoch + 1)
        .await
        .expect("failover opens a published store");
    let inner = db.inner_db();
    wait_for(&inner, LABEL_HASH_CLEANUP_READY).await;
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact, exact_before);
    assert!(legacy.is_empty());
    assert_eq!(all_label_reads(&db).await, reads);
    db.close().await.unwrap();
}

/// A cleanup step reads rows of a bounded number of scopes, so one over many
/// tenants with no hash row left stays short, and the next step starts at the
/// scope it stopped before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cleanup_steps_cross_a_bounded_number_of_scopes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store).await;
    let inner = db.inner_db();
    let tenants = CLEANUP_SCOPES_PER_STEP as u128 + 44;
    let batch = Batch::begin(&inner).await.unwrap();
    for tenant in 1..=tenants {
        Mutation::put(
            &batch.transaction,
            DataKey::Data {
                scope: DataScope::Tenant(TenantId::from_u128(tenant)),
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(1)),
            }
            .to_bytes(),
            encode_properties(&[Property::string("$label", "Person")]),
        )
        .unwrap();
    }
    batch.commit(false).await.unwrap();
    let last = DataScope::Tenant(TenantId::from_u128(tenants));
    for key in [
        edge_row("KNOWS"),
        DataKey::data_prefix(last, edge_row("KNOWS")),
    ] {
        put_legacy_row(&inner, &key, &[1]).await;
    }

    let tuning = MigrationTuning::default();
    assert_eq!(
        delete_hash_label_rows_once(&inner, tuning).await.unwrap(),
        LabelHashCleanup::Advanced { deleted: 1 }
    );
    let cursor = inner
        .get(metadata(LABEL_HASH_CLEANUP_PROGRESS))
        .await
        .unwrap()
        .expect("the step recorded the scope it stopped before");
    let stopped_before =
        DataScope::Tenant(TenantId::from_u128(CLEANUP_SCOPES_PER_STEP as u128 + 1));
    assert_eq!(
        cursor,
        DataKey::data_prefix(
            stopped_before,
            label_hash::LegacyLabelScanPrefix::NodeLabels.to_bytes()
        )
    );
    assert_eq!(
        delete_hash_label_rows_once(&inner, tuning).await.unwrap(),
        LabelHashCleanup::Done { deleted: 1 }
    );
    assert!(label_rows(inner.as_ref()).await.1.is_empty());
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_markers_and_progress_fail_closed() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let writer = writer_of(&db);
    for (name, value) in [
        (
            LABEL_IDENTITY_COPY_PROGRESS,
            Bytes::from_static(b"\x07short"),
        ),
        (LABEL_HASH_CLEANUP_PROGRESS, Bytes::new()),
        (LABEL_HASH_CLEANUP_READY, Bytes::from_static(b"yes")),
    ] {
        crate::transaction::put_unbatched(&inner, &metadata(name), &value)
            .await
            .unwrap();
        let failed = if name == LABEL_IDENTITY_COPY_PROGRESS {
            copy_hash_label_rows_once(&writer, &mut LabelCopySession::new())
                .await
                .map(|_| ())
        } else {
            if name == LABEL_HASH_CLEANUP_PROGRESS {
                copy_all(&writer, &mut LabelCopySession::new()).await;
            }
            delete_hash_label_rows_once(&inner, MigrationTuning::default())
                .await
                .map(|_| ())
        };
        assert!(
            matches!(failed, Err(HelixDbError::MigrationRequired { .. })),
            "{}: {failed:?}",
            String::from_utf8_lossy(name)
        );
        inner.delete(metadata(name)).await.unwrap();
    }
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_copy_starts_only_from_the_operation_queue_version() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store).await;
    let inner = db.inner_db();
    // A published store is left alone.
    start_label_identity_copy(&inner).await.unwrap();
    assert_eq!(
        storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    let set_version = |version| {
        let inner = &inner;
        async move {
            crate::transaction::put_unbatched(
                inner,
                &marker_key(),
                &encode_metadata_value(&IndexV2MetadataValue::StorageVersion(version)),
            )
            .await
            .unwrap();
        }
    };
    set_version(IndexStorageVersion::EQUALITY_BITMAPS).await;
    assert!(matches!(
        start_label_identity_copy(&inner).await,
        Err(HelixDbError::MigrationRequired { .. })
    ));
    assert_eq!(
        storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::EQUALITY_BITMAPS
    );
    // No step copies before a writer starts the copy, and progress a copy
    // before this one left goes with the start. A store with no hash row
    // has nothing to copy and publishes at once.
    for (hash_rows, started) in [
        (false, IndexStorageVersion::LABEL_IDENTITY),
        (true, IndexStorageVersion::LABEL_COPY),
    ] {
        set_version(IndexStorageVersion::OPERATION_QUEUES).await;
        if hash_rows {
            put_legacy_row(&inner, &node_row("Person"), &[1]).await;
        }
        crate::transaction::put_unbatched(
            &inner,
            &metadata(LABEL_IDENTITY_COPY_PROGRESS),
            &Bytes::from_static(b"\x01stale"),
        )
        .await
        .unwrap();
        assert_eq!(
            copy_hash_label_rows_once(&writer_of(&db), &mut LabelCopySession::new())
                .await
                .unwrap(),
            LabelCopyStep::NotStarted
        );
        start_label_identity_copy(&inner).await.unwrap();
        assert_eq!(storage_version(inner.as_ref()).await.unwrap(), started);
        assert!(!present(&inner, LABEL_IDENTITY_COPY_PROGRESS).await);
    }
    db.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn labels_longer_than_any_label_key_are_rejected_as_invalid_queries() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_writer(&store).await;
    let label = "L".repeat(label::MAX_LABEL_LEN + 1);
    let error = db
        .query(QueryRequest::write(
            write_batch()
                .var_as(
                    "node",
                    g().add_n(label.as_str(), vec![("name", PropertyInput::from("long"))]),
                )
                .returning(Vec::<String>::new()),
        ))
        .await
        .expect_err("an unkeyable label is rejected");
    assert!(
        error.to_string().contains("label index bound"),
        "unexpected error: {error}"
    );
    assert_eq!(
        error.error_code(),
        helix_ast::error_code::QueryErrorCode::InvalidQuery
    );
    db.close().await.unwrap();
}

/// Removes `member` from `scope`'s label row `row` as a graph write does while
/// the copy runs: under its exact and its hash key in one transaction, each a
/// disjoint merge whose token is the member.
async fn dual_remove(
    db: &Db,
    scope: DataScope,
    row: crate::search::HashLabelRow<'_>,
    member: u64,
) -> std::result::Result<(), slatedb::Error> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    stage_dual_remove(&transaction, scope, row, member);
    transaction.commit().await.map(|_| ())
}

fn stage_dual_remove(
    transaction: &DbTransaction,
    scope: DataScope,
    row: crate::search::HashLabelRow<'_>,
    member: u64,
) {
    let exact = match row {
        crate::search::HashLabelRow::Node(text) => {
            ExactRows::Node.key(scope, label::LabelIdentity::new(text).unwrap())
        }
        crate::search::HashLabelRow::Edge(text) => {
            ExactRows::Edge.key(scope, label::LabelIdentity::new(text).unwrap())
        }
        crate::search::HashLabelRow::Neighbor(direction, node, text) => {
            ExactRows::Neighbor(direction, node)
                .key(scope, label::LabelIdentity::new(text).unwrap())
        }
    };
    let hash = crate::search::hash_label_key(scope, row);
    let mut removal = BitmapMembershipDelta::default();
    removal.remove(member);
    let mut merges = Mutation::merge_batch(transaction, 2).unwrap();
    for key in [&exact, &hash] {
        merges
            .bitmap(crate::transaction::merges::Key::Encoded(key), &removal)
            .unwrap();
    }
    merges.stage().unwrap();
}

/// The other order of a race: a removal whose transaction began before a
/// batch committed, and that commits after it, conflicts with the batch's
/// copy of the member, so it cannot commit beneath the copy. Its retry
/// removes the member from both layouts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removal_that_began_before_a_batch_committed_conflicts_after_it() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let writer = writer_of(&db);
    let (ada, bob, _acme) = fixture_nodes(&label_rows(inner.as_ref()).await.1);
    let scope = DataScope::LegacyUnscoped;
    let person = crate::search::HashLabelRow::Node("Person");

    let removal = inner
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    stage_dual_remove(&removal, scope, person, bob);
    // The batch copies the unscoped node and edge rows, Bob among them.
    assert_eq!(
        copy_hash_label_rows_once(
            &writer,
            &mut LabelCopySession::with_limits(4, BATCH_MEMBERS)
        )
        .await
        .unwrap(),
        LabelCopyStep::Copied
    );
    let Err(error) = removal.commit().await else {
        panic!("the removal committed beneath the copy of its member");
    };
    assert_eq!(error.kind(), slatedb::ErrorKind::Transaction);
    let people = RoaringTreemap::from_iter([ada, bob]);
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact[&exact_node_row("Person")], people);
    assert_eq!(legacy[&node_row("Person")], people);

    dual_remove(&inner, scope, person, bob).await.unwrap();
    copy_all(&writer, &mut LabelCopySession::new()).await;
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(
        exact[&exact_node_row("Person")],
        RoaringTreemap::from_iter([ada])
    );
    assert_eq!(
        legacy[&node_row("Person")],
        RoaringTreemap::from_iter([ada])
    );
    db.close().await.unwrap();
}

/// A row copied a range of members at a time conflicts only with the removal
/// of a member its step copies: a removal of one copied earlier, or yet to
/// copy, lands on its own, and the published row holds exactly the members
/// left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_range_step_conflicts_only_with_removals_of_members_it_copies() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let inner = db.inner_db();
    let writer = writer_of(&db);
    let scope = DataScope::LegacyUnscoped;
    let crowd = crate::search::HashLabelRow::Node("Crowd");
    let members = 1_001..=1_006;
    for id in members.clone() {
        put_node(&inner, id, "Crowd").await;
    }
    put_legacy_row(&inner, &node_row("Crowd"), &members.collect::<Vec<_>>()).await;
    let mut session = LabelCopySession::with_limits(BATCH_ROWS, 2);

    // Step until the crowd row's first range, 1001 and 1002, is copied.
    loop {
        assert!(copy_hash_label_rows_once(&writer, &mut session)
            .await
            .unwrap()
            .advanced());
        if copy_progress(&inner).await
            == Some(label_migration::LabelCopyProgressValue::members(
                node_row("Crowd"),
                1_002,
            ))
        {
            break;
        }
    }
    // The next step copies 1003 and 1004; removing 1004 beneath it conflicts.
    let (step, next) = step_racing(&writer, session, async {
        dual_remove(&inner, scope, crowd, 1_004).await.unwrap();
    })
    .await;
    session = next;
    assert_eq!(step, LabelCopyStep::Conflicted);
    // Taken again, with its limit halved to one member: 1003 alone.
    assert_eq!(
        copy_hash_label_rows_once(&writer, &mut session)
            .await
            .unwrap(),
        LabelCopyStep::Copied
    );
    // The next copies 1005 and 1006; removing 1001, copied two steps ago,
    // and adding 1007, after both, do not conflict with it.
    let (step, next) = step_racing(&writer, session, async {
        dual_remove(&inner, scope, crowd, 1_001).await.unwrap();
        put_node(&inner, 1_007, "Crowd").await;
        let write = inner
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let mut addition = BitmapMembershipDelta::default();
        addition.add(1_007);
        let mut merges = Mutation::merge_batch(&write, 2).unwrap();
        for key in [
            exact_node_row("Crowd"),
            crate::search::hash_label_key(scope, crowd),
        ] {
            merges
                .bitmap(crate::transaction::merges::Key::Encoded(&key), &addition)
                .unwrap();
        }
        merges.stage().unwrap();
        write.commit().await.unwrap();
    })
    .await;
    session = next;
    assert_eq!(step, LabelCopyStep::Copied);
    assert_eq!(
        copy_all(&writer, &mut session).await.last(),
        Some(&LabelCopyStep::Published)
    );
    let left = RoaringTreemap::from_iter([1_002, 1_003, 1_005, 1_006, 1_007]);
    let (exact, legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(exact[&exact_node_row("Crowd")], left);
    assert_eq!(legacy[&node_row("Crowd")], left);
    db.close().await.unwrap();
}

/// The label rows `reader`'s node and edge records imply, under exact keys.
async fn rows_of_records(reader: &(impl DbReadOps + Sync)) -> Rows {
    let mut rows = Rows::new();
    let label_of = |value: &[u8]| {
        decode_properties(value)
            .unwrap()
            .into_iter()
            .find(|property| property.name == "$label")
            .and_then(|property| property.value.as_str().map(str::to_owned))
    };
    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(reader)
        .await
        .unwrap()
    {
        let mut nodes = reader
            .scan_prefix(
                super::super::super::MigrationStage::NodeProperties.prefix(scope),
                ..,
            )
            .await
            .unwrap();
        while let Some(row) = nodes.next().await.unwrap() {
            let DataKey::Data {
                kind: DataKeyKind::NodeProperty(key),
                ..
            } = DataKey::parse_from_slice(scope, &row.key).unwrap()
            else {
                panic!("the node property prefix holds node records");
            };
            let Some(text) = label_of(&row.value) else {
                continue;
            };
            rows.entry(ExactRows::Node.key(scope, label::LabelIdentity::new(&text).unwrap()))
                .or_default()
                .insert(key.node_id());
        }
        let mut edges = reader
            .scan_prefix(
                super::super::super::MigrationStage::EdgeEndpoints.prefix(scope),
                ..,
            )
            .await
            .unwrap();
        while let Some(row) = edges.next().await.unwrap() {
            let DataKey::Data {
                kind: DataKeyKind::EdgeEndpoints(key),
                ..
            } = DataKey::parse_from_slice(scope, &row.key).unwrap()
            else {
                panic!("the edge endpoint prefix holds edge endpoints");
            };
            let (from, to) = super::super::super::decode_edge_endpoints(&row.value).unwrap();
            let record = reader
                .get(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(
                            key.edge_id(),
                        )),
                    }
                    .to_bytes(),
                )
                .await
                .unwrap()
                .expect("every edge has its record");
            let Some(text) = label_of(&record) else {
                continue;
            };
            let identity = label::LabelIdentity::new(&text).unwrap();
            for (exact, member) in [
                (ExactRows::Edge.key(scope, identity), key.edge_id()),
                (
                    ExactRows::Neighbor(EdgeDirection::Out, from).key(scope, identity),
                    to,
                ),
                (
                    ExactRows::Neighbor(EdgeDirection::In, to).key(scope, identity),
                    from,
                ),
            ] {
                rows.entry(exact).or_default().insert(member);
            }
        }
    }
    rows
}

/// Every node and edge record of `scope`, by ID.
async fn records(db: &Db, scope: DataScope) -> (Vec<u64>, Vec<u64>) {
    let mut nodes = Vec::new();
    let mut rows = db
        .scan_prefix(
            super::super::super::MigrationStage::NodeProperties.prefix(scope),
            ..,
        )
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        let DataKey::Data {
            kind: DataKeyKind::NodeProperty(key),
            ..
        } = DataKey::parse_from_slice(scope, &row.key).unwrap()
        else {
            panic!("the node property prefix holds node records");
        };
        nodes.push(key.node_id());
    }
    let mut edges = Vec::new();
    let mut rows = db
        .scan_prefix(
            super::super::super::MigrationStage::EdgeEndpoints.prefix(scope),
            ..,
        )
        .await
        .unwrap();
    while let Some(row) = rows.next().await.unwrap() {
        let DataKey::Data {
            kind: DataKeyKind::EdgeEndpoints(key),
            ..
        } = DataKey::parse_from_slice(scope, &row.key).unwrap()
        else {
            panic!("the edge endpoint prefix holds edge endpoints");
        };
        edges.push(key.edge_id());
    }
    (nodes, edges)
}

/// Graph writes of every kind race copy steps of one row and one member at a
/// time, the copy publishing midway. Whatever the interleaving, the exact
/// rows end up holding exactly what the records imply, as do the hash rows,
/// so no step lost a write's addition or brought back a removed member.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_graph_writes_and_copy_steps_converge_on_the_records() {
    let (_store, db, _reads, _exact) = copying_store().await;
    let db = Arc::new(db);
    let writer = writer_of(&db);
    // The copier takes a step per few writes, so most writes land while the
    // copy runs, both on rows it has passed and on rows it has yet to read,
    // and some race a step's commit.
    let paced = Arc::new(tokio::sync::Semaphore::new(0));
    let copier = tokio::spawn({
        let writer = Arc::clone(&writer);
        let paced = Arc::clone(&paced);
        async move {
            let mut session = LabelCopySession::with_limits(1, 1);
            loop {
                paced.acquire().await.unwrap().forget();
                let step = copy_hash_label_rows_once(&writer, &mut session)
                    .await
                    .unwrap();
                if !step.advanced() {
                    return session.totals();
                }
            }
        }
    });

    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let labels = ["Person", "Company", "Robot"];
    let edge_labels = ["KNOWS", "WORKS_AT", "LIKES"];
    let mut during_copy = 0_i64;
    let mut after_copy = 0_i64;
    while after_copy < 20 {
        let writes = during_copy + after_copy;
        if writes % 3 == 0 {
            paced.add_permits(1);
        }
        let scope = scopes()[next(3) as usize];
        let (nodes, edges) = records(&db.inner_db(), scope).await;
        let request = match next(5) {
            0 | 1 => write_batch().var_as(
                "node",
                g().add_n(
                    labels[next(3) as usize],
                    vec![("n", PropertyInput::from(writes))],
                ),
            ),
            2 if nodes.len() >= 2 => {
                let from = nodes[next(nodes.len() as u64) as usize];
                let to = nodes[next(nodes.len() as u64) as usize];
                write_batch().var_as(
                    "edge",
                    g().n(NodeRef::id(from)).add_e(
                        edge_labels[next(3) as usize],
                        NodeRef::id(to),
                        Vec::<(&str, PropertyInput)>::new(),
                    ),
                )
            }
            3 if !nodes.is_empty() => write_batch().var_as(
                "node",
                g().n(NodeRef::id(nodes[next(nodes.len() as u64) as usize]))
                    .drop(),
            ),
            4 if !edges.is_empty() => write_batch().var_as(
                "edge",
                g().e(edges[next(edges.len() as u64) as usize]).drop(),
            ),
            _ => continue,
        };
        let request = QueryRequest::write(request.returning(Vec::<String>::new()));
        // A write that conflicts with a copy step is not applied; it is
        // retried as any client retries a conflict.
        loop {
            match db.query_scoped(request.clone(), scope).await {
                Ok(_) => break,
                Err(error) if error.is_transaction_conflict() => {}
                Err(error) => panic!("graph write failed: {error}"),
            }
        }
        if copier.is_finished() {
            after_copy += 1;
        } else {
            during_copy += 1;
        }
    }
    let totals = copier.await.unwrap();

    let inner = db.inner_db();
    assert_eq!(
        storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    let (exact, _legacy) = label_rows(inner.as_ref()).await;
    assert_eq!(non_empty(exact), rows_of_records(inner.as_ref()).await);
    assert!(totals.copied > 0);
    assert!(
        during_copy >= 50,
        "{during_copy} writes overlapped the copy"
    );
    db.close().await.unwrap();
}
