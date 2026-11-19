//! Index builds through the public query path while a background migration
//! changes what their source scans read:
//!
//! - the label identity copy, which moves the label rows a build reads its
//!   members from ([`crate::index_lifecycle::label_source`]) from hash keys
//!   to exact keys, and publishes and cleans up between two build steps;
//! - the datetime string conversion, which rewrites indexed values through
//!   the ordinary write path, ahead of the scan and behind it.
//!
//! Writers open with explicit lifecycle scheduling and no migration worker,
//! so each contract interleaves build steps, query writes and migration
//! steps in a fixed order, and checks every indexed value against the writes
//! it made.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use helix_ast::batch::write_batch;
use helix_ast::expr::Predicate;
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::query::QueryRequest;
use helix_ast::traversal::{g, MutationMode, Traversal, TraversalState};
use helix_ast::value::{DateTime, PropertyInput};
use helix_planner::ir;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::datetime_strings::{self, Job, Phase};
use super::indexes::label_identity::{
    copy_hash_label_rows_once, delete_hash_label_rows_once, make_legacy_label_fixture,
    LabelCopySession,
};
use super::startup::storage_version;
use crate::config::{
    DbConfig, MigrationBatchRows, MigrationTuning, MigrationWorkerMode, SecondaryIndexDefinition,
    SecondaryIndexLifecycleBatchRows, SecondaryIndexLifecycleTuning,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::{
    IndexDdlReceipt, IndexOperationId, IndexOperationStage, IndexOperationStatus,
    IndexStorageVersion, ValidatedDynamicIndexDefinition,
};
use crate::index_lifecycle_testing::{
    LifecycleCheckpoint, LifecycleStage, LifecycleTestController, LifecycleTestScheduling,
    LifecycleWorkTarget,
};
use crate::{HelixDB, HelixStorage};

const SCOPE: DataScope = DataScope::LegacyUnscoped;

/// Rows a build step takes.
const STEP_ROWS: usize = 100;

/// A writer of `store` with explicit lifecycle scheduling and no migration
/// worker, whose migration batches take `batch_rows` rows and whose build
/// steps take [`STEP_ROWS`].
async fn open(store: &Arc<dyn ObjectStore>, name: &str, batch_rows: usize) -> HelixDB {
    HelixDB::open_with_object_store_for_index_lifecycle_testing(
        name,
        Arc::clone(store),
        DbConfig::new()
            .with_secondary_index_lifecycle_tuning(
                SecondaryIndexLifecycleTuning::default().with_batch_rows(
                    SecondaryIndexLifecycleBatchRows::new(STEP_ROWS).expect("rows are positive"),
                ),
            )
            .with_migration_tuning(
                MigrationTuning::default()
                    .with_batch_rows(
                        MigrationBatchRows::new(batch_rows).expect("rows are positive"),
                    )
                    .with_worker_mode(MigrationWorkerMode::Disabled),
            ),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("writer opens")
}

/// Runs `writes`, each returning one entity id, as one query, and returns
/// the ids in order.
async fn write_ids<S: TraversalState, M: MutationMode>(
    db: &HelixDB,
    writes: impl IntoIterator<Item = Traversal<S, M>>,
) -> Vec<u64> {
    let (batch, names) = writes.into_iter().enumerate().fold(
        (write_batch(), Vec::new()),
        |(batch, mut names), (position, write)| {
            let name = format!("w{position}");
            let batch = batch.var_as(&name, write);
            names.push(name);
            (batch, names)
        },
    );
    let response = db
        .query_scoped(QueryRequest::write(batch.returning(names.clone())), SCOPE)
        .await
        .expect("writes commit");
    names
        .iter()
        .map(|name| {
            response[name.as_str()][0]
                .as_u64()
                .unwrap_or_else(|| panic!("{name} returns one id: {response}"))
        })
        .collect()
}

/// Runs `writes`, returning nothing, as one query.
async fn write<S: TraversalState, M: MutationMode>(
    db: &HelixDB,
    writes: impl IntoIterator<Item = Traversal<S, M>>,
) {
    let batch = writes
        .into_iter()
        .enumerate()
        .fold(write_batch(), |batch, (position, write)| {
            batch.var_as(&format!("w{position}"), write)
        });
    db.query_scoped(
        QueryRequest::write(batch.returning(Vec::<String>::new())),
        SCOPE,
    )
    .await
    .expect("writes commit");
}

/// The ids `read` returns, sorted.
async fn read_ids<S: TraversalState>(db: &HelixDB, read: Traversal<S>) -> Vec<u64> {
    let response = db
        .query_scoped(
            QueryRequest::read(
                helix_ast::batch::read_batch()
                    .var_as("ids", read)
                    .returning(["ids"]),
            ),
            SCOPE,
        )
        .await
        .expect("read executes");
    let mut ids = response["ids"]
        .as_array()
        .unwrap_or_else(|| panic!("ids are a list: {response}"))
        .iter()
        .map(|id| id.as_u64().expect("an id is a number"))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

/// Accepts a build of `definition`.
async fn create(db: &HelixDB, definition: SecondaryIndexDefinition) -> IndexOperationId {
    let definition =
        ValidatedDynamicIndexDefinition::try_from(definition).expect("definition converts");
    let receipt = LifecycleTestController
        .create_index(db, SCOPE, definition, ir::IndexCreateMode::ErrorIfExists)
        .await
        .expect("create is accepted");
    let IndexDdlReceipt::Accepted { operation_id, .. } = receipt else {
        panic!("a new definition starts a build: {receipt:?}");
    };
    operation_id
}

fn target(operation_id: IndexOperationId) -> LifecycleWorkTarget {
    LifecycleWorkTarget::Operation {
        scope: SCOPE,
        operation_id,
    }
}

/// Takes one step of a build still scanning, and asserts its scan goes on.
async fn scan_step(db: &HelixDB, operation_id: IndexOperationId) {
    let evidence = LifecycleTestController
        .advance(db, target(operation_id))
        .await
        .expect("a build step runs");
    let LifecycleCheckpoint::Present {
        stage: LifecycleStage::Operation(IndexOperationStage::Scan),
        ..
    } = evidence.after
    else {
        panic!("the scan goes on after this step: {evidence:?}");
    };
}

/// Steps a build until it succeeds.
async fn finish(db: &HelixDB, operation_id: IndexOperationId) {
    for _ in 0..256 {
        match db
            .get_index_operation(SCOPE, operation_id)
            .await
            .expect("operation status reads")
        {
            IndexOperationStatus::Succeeded { .. } => return,
            IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. } => {
                LifecycleTestController
                    .advance(db, target(operation_id))
                    .await
                    .expect("a build step runs");
            }
            status @ (IndexOperationStatus::Blocked { .. }
            | IndexOperationStatus::Aborted { .. }) => {
                panic!("the build did not succeed: {status:?}")
            }
        }
    }
    panic!("the build exceeded its step bound");
}

/// The access `query`'s first MATCH starts from, as its plan names it.
async fn explained(db: &HelixDB, query: &str) -> String {
    serde_json::to_string(
        &db.explain_cypher(crate::cypher::Request::new(query))
            .await
            .expect("the query explains"),
    )
    .expect("an explanation serializes")
}

/// People and who knows whom: each person by id with their group, each
/// `KNOWS` edge by id with its ends and `since`.
#[derive(Default)]
struct Graph {
    people: BTreeMap<u64, String>,
    knows: BTreeMap<u64, (u64, u64, i64)>,
}

impl Graph {
    async fn add_people(&mut self, db: &HelixDB, groups: &[String]) {
        let ids = write_ids(
            db,
            groups.iter().map(|group| {
                g().add_n(
                    "Person",
                    vec![("group", PropertyInput::from(group.as_str()))],
                )
                .id()
            }),
        )
        .await;
        self.people
            .extend(ids.into_iter().zip(groups.iter().cloned()));
    }

    async fn add_knows(&mut self, db: &HelixDB, edges: &[(u64, u64, i64)]) {
        let ids = write_ids(
            db,
            edges.iter().map(|&(from, to, since)| {
                g().n(NodeRef::id(from))
                    .add_e(
                        "KNOWS",
                        NodeRef::id(to),
                        vec![("since", PropertyInput::from(since))],
                    )
                    .id()
            }),
        )
        .await;
        self.knows
            .extend(ids.into_iter().zip(edges.iter().copied()));
    }

    /// Drops `people` and, with them, the edges they end.
    async fn drop_people(&mut self, db: &HelixDB, people: &[u64]) {
        write(db, people.iter().map(|&id| g().n(NodeRef::id(id)).drop())).await;
        for id in people {
            self.people.remove(id);
        }
        self.knows
            .retain(|_, (from, to, _)| !people.contains(from) && !people.contains(to));
    }

    async fn regroup(&mut self, db: &HelixDB, people: &[u64], group: &str) {
        write(
            db,
            people
                .iter()
                .map(|&id| g().n(NodeRef::id(id)).set_property("group", group)),
        )
        .await;
        for id in people {
            self.people.insert(*id, group.to_owned());
        }
    }

    async fn drop_knows(&mut self, db: &HelixDB, edges: &[u64]) {
        write(
            db,
            edges.iter().map(|&id| g().drop_edge_by_id(EdgeRef::id(id))),
        )
        .await;
        for id in edges {
            self.knows.remove(id);
        }
    }

    async fn resince(&mut self, db: &HelixDB, edges: &[u64], since: i64) {
        write(
            db,
            edges
                .iter()
                .map(|&id| g().e(EdgeRef::id(id)).set_property("since", since)),
        )
        .await;
        for id in edges {
            self.knows.get_mut(id).expect("the edge exists").2 = since;
        }
    }

    /// Every `count`th person from `skip`, among those left.
    fn people_every(&self, skip: usize, count: usize) -> Vec<u64> {
        self.people
            .keys()
            .copied()
            .skip(skip)
            .step_by(count)
            .take(10)
            .collect()
    }

    fn knows_every(&self, skip: usize, count: usize) -> Vec<u64> {
        self.knows
            .keys()
            .copied()
            .skip(skip)
            .step_by(count)
            .take(10)
            .collect()
    }

    /// Every group reads, through its index, exactly the people who hold it,
    /// and every `since` exactly the edges that hold it.
    async fn assert_indexed(&self, db: &HelixDB) {
        let groups = self.people.values().cloned().collect::<BTreeSet<_>>();
        for group in groups {
            let expected = self
                .people
                .iter()
                .filter(|(_, held)| **held == group)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            let read = g()
                .n_with_label_where("Person", Predicate::eq("group", group.as_str()))
                .id();
            assert_eq!(read_ids(db, read).await, expected, "group {group}");
        }
        let sinces = self
            .knows
            .values()
            .map(|(_, _, since)| *since)
            .collect::<BTreeSet<_>>();
        for since in sinces {
            let expected = self
                .knows
                .iter()
                .filter(|(_, (_, _, held))| *held == since)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            let read = g()
                .e_with_label_where("KNOWS", Predicate::eq("since", since))
                .id();
            assert_eq!(read_ids(db, read).await, expected, "since {since}");
        }
        assert!(
            explained(db, "MATCH (p:Person) WHERE p.group = 'g3' RETURN p")
                .await
                .contains("node_eq:Person:group")
        );
    }
}

/// Node and edge builds started while the label identity copy has copied
/// nothing, so the exact label rows hold only what was written since: their
/// steps read the hash rows, beside query writes that file both, until the
/// writer's own copy publishes and cleans up between two steps, and their
/// steps after it read the exact rows. Each build ends with exactly the
/// people and edges every write left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn builds_through_the_query_path_span_the_label_identity_publication() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let name = "index-builds-label-identity";
    let db = open(&store, name, 1024).await;
    let mut graph = Graph::default();
    for batch in 0..12 {
        let groups = (0..100)
            .map(|offset| format!("g{}", (batch * 100 + offset) % 10))
            .collect::<Vec<_>>();
        graph.add_people(&db, &groups).await;
    }
    let people = graph.people.keys().copied().collect::<Vec<_>>();
    for batch in people[..600].chunks(100) {
        let edges = batch
            .iter()
            .enumerate()
            .map(|(offset, &from)| {
                let to = people[600 + usize::try_from(from % 600).expect("fits")];
                (from, to, i64::try_from(offset % 7).expect("fits"))
            })
            .collect::<Vec<_>>();
        graph.add_knows(&db, &edges).await;
    }
    make_legacy_label_fixture(&db.inner_db())
        .await
        .expect("the store rewinds before label identity");
    db.close().await.expect("writer closes");

    let db = open(&store, name, 1024).await;
    assert_eq!(
        storage_version(db.inner_db().as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_COPY,
        "the open starts the copy and copies nothing"
    );
    let groups = create(
        &db,
        SecondaryIndexDefinition::node_equality("Person", "group").expect("definition"),
    )
    .await;
    let sinces = create(
        &db,
        SecondaryIndexDefinition::edge_equality("KNOWS", "since").expect("definition"),
    )
    .await;
    // One step each of 1,200 people and 600 edges, on the hash rows: the
    // exact rows hold no member yet, so a step that read them would end the
    // scan.
    scan_step(&db, groups).await;
    scan_step(&db, sinces).await;

    // Writes filed under both keys: people and edges before the scans' cursors
    // and after them, and new ones above the builds' bounds.
    graph.drop_people(&db, &graph.people_every(3, 97)).await;
    graph.regroup(&db, &graph.people_every(5, 89), "g7").await;
    graph
        .add_people(&db, &(0..10).map(|_| "g3".to_owned()).collect::<Vec<_>>())
        .await;
    graph.drop_knows(&db, &graph.knows_every(2, 53)).await;
    graph.resince(&db, &graph.knows_every(4, 47), 99).await;
    let fresh = graph
        .people
        .keys()
        .rev()
        .take(4)
        .copied()
        .collect::<Vec<_>>();
    graph
        .add_knows(&db, &[(fresh[0], fresh[1], 3), (fresh[2], fresh[3], 99)])
        .await;
    scan_step(&db, groups).await;

    // The writer's own copy publishes and deletes every hash row.
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("the handle is a writer");
    };
    let mut session = LabelCopySession::new();
    while copy_hash_label_rows_once(writer, &mut session)
        .await
        .expect("a copy step runs")
        .advanced()
    {}
    while delete_hash_label_rows_once(writer.db(), &mut session)
        .await
        .expect("a cleanup step runs")
        .advanced()
    {}
    assert_eq!(
        storage_version(db.inner_db().as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );

    // Writes under exact keys only, then the rest of both builds.
    graph.drop_people(&db, &graph.people_every(7, 83)).await;
    graph.regroup(&db, &graph.people_every(1, 79), "g1").await;
    graph.resince(&db, &graph.knows_every(6, 41), 5).await;
    finish(&db, groups).await;
    finish(&db, sinces).await;
    graph.assert_indexed(&db).await;
    db.close().await.expect("writer closes");
}

/// The instants the events hold, each first written as its RFC3339 string.
const INSTANTS: [&str; 6] = [
    "2026-01-01T00:00:00Z",
    "2026-01-02T00:00:00Z",
    "2026-01-03T00:00:00Z",
    "2026-01-04T00:00:00Z",
    "2026-01-05T00:00:00Z",
    "2026-01-06T00:00:00Z",
];

/// A build over a property the datetime conversion rewrites while it scans:
/// each conversion batch runs between two build steps, and rewrites rows the
/// scan has read (which reach the build as writes) and rows it has yet to
/// read (which it reads converted). The index ends with every event under its
/// datetime and none under the string it was written as.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_build_spans_the_datetime_string_conversion() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open(&store, "index-builds-datetime-strings", 400).await;
    let mut events = BTreeMap::new();
    for batch in 0..12 {
        let instants = (0..100)
            .map(|offset| INSTANTS[(batch * 100 + offset) % INSTANTS.len()])
            .collect::<Vec<_>>();
        let ids = write_ids(
            &db,
            instants.iter().map(|instant| {
                g().add_n("Event", vec![("at", PropertyInput::from(*instant))])
                    .id()
            }),
        )
        .await;
        events.extend(ids.into_iter().zip(instants));
    }
    let tuning = db.config().db().migrations();
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("the handle is a writer");
    };
    // Detection reads every row before the first conversion.
    while !matches!(
        datetime_strings::load(writer.db()).await.unwrap(),
        Some(Job::Running {
            phase: Phase::Convert { .. },
            ..
        })
    ) {
        assert!(datetime_strings::process_once(&db, tuning).await.unwrap());
    }
    let operation_id = create(
        &db,
        SecondaryIndexDefinition::node_equality("Event", "at").expect("definition"),
    )
    .await;
    // A build step reads 100 rows and a conversion batch rewrites the next
    // 400: each batch rewrites rows the scan has read and rows it reads next.
    for _ in 0..3 {
        scan_step(&db, operation_id).await;
        assert!(datetime_strings::process_once(&db, tuning).await.unwrap());
    }
    while datetime_strings::process_once(&db, tuning).await.unwrap() {}
    let Some(job) = datetime_strings::load(writer.db()).await.unwrap() else {
        panic!("the job ran");
    };
    assert!(job.is_completed());
    assert_eq!(job.report().converted_values, 1_200);
    finish(&db, operation_id).await;

    for instant in INSTANTS {
        let expected = events
            .iter()
            .filter(|(_, held)| **held == instant)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        assert_eq!(expected.len(), 200);
        let at = DateTime::parse_rfc3339(instant).expect("an instant parses");
        let read = g()
            .n_with_label_where("Event", Predicate::eq("at", PropertyInput::from(at)))
            .id();
        assert_eq!(read_ids(&db, read).await, expected, "{instant}");
        let read = g()
            .n_with_label_where("Event", Predicate::eq("at", instant))
            .id();
        assert!(read_ids(&db, read).await.is_empty(), "{instant} as text");
    }
    assert!(explained(
        &db,
        "MATCH (e:Event) WHERE e.at = datetime('2026-01-01T00:00:00Z') RETURN e"
    )
    .await
    .contains("node_eq:Event:at"));
    db.close().await.expect("writer closes");
}
