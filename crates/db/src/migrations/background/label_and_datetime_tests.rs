//! A store that needs both the label identity copy and the datetime string
//! conversion: one background worker finishes both, whatever order their
//! steps interleave in, while the writer serves graph writes.

use std::sync::Arc;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::expr::Predicate;
use helix_ast::graph::NodeRef;
use helix_ast::query::{QueryParamType, QueryRequest, QueryValue};
use helix_ast::traversal::g;
use helix_ast::value::{DateTime, PropertyInput};
use serde_json::json;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::DbReadOps;

use crate::config::{self, DbConfig, MigrationTuning, SecondaryIndexDefinition};
use crate::encoding::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::DataKey;
use crate::encoding::v2::legacy::label_hash;
use crate::index_lifecycle::IndexStorageVersion;
use crate::migrations::{datetime_strings, indexes::label_identity, startup};
use crate::query_service::HelixQueryService;
use crate::{HelixDB, HelixStorage};

const DB_PATH: &str = "label-and-datetime-migrations";
const PEOPLE: usize = 4;
const EVENTS: usize = 8;

fn scopes() -> [DataScope; 2] {
    [
        DataScope::LegacyUnscoped,
        DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000A").unwrap()),
    ]
}

/// Event `index`'s instant, spelled at +02:00 as a parameter-era write stored it.
fn event_at(index: usize) -> String {
    format!("2026-{:02}-01T{:02}:30:00+02:00", index + 1, index)
}

fn attends_since(index: usize) -> String {
    format!("2025-12-{:02}T08:00:00Z", index + 1)
}

fn canonical(rfc3339: &str) -> String {
    DateTime::parse_rfc3339(rfc3339)
        .unwrap()
        .to_rfc3339()
        .unwrap()
}

fn tuned(worker: config::MigrationWorkerMode) -> DbConfig {
    // Two rows per datetime batch, so its phases span several steps that
    // interleave with the label steps.
    DbConfig::new().with_migration_tuning(
        MigrationTuning::default()
            .with_batch_rows(config::MigrationBatchRows::new(2).unwrap())
            .with_worker_mode(worker),
    )
}

/// People attending events in `scope`, every datetime stored as its string.
async fn write_graph(db: &HelixDB, scope: DataScope) {
    let mut batch = write_batch();
    for person in 0..PEOPLE {
        batch = batch.var_as(
            &format!("p{person}"),
            g().add_n(
                "Person",
                vec![("name", PropertyInput::from(format!("p{person}")))],
            ),
        );
    }
    for event in 0..EVENTS {
        batch = batch
            .var_as(
                &format!("e{event}"),
                g().add_n(
                    "Event",
                    vec![
                        ("name", PropertyInput::from(format!("e{event}"))),
                        ("at", PropertyInput::from(event_at(event))),
                    ],
                ),
            )
            .var_as(
                &format!("a{event}"),
                g().n(NodeRef::var(format!("p{}", event % PEOPLE))).add_e(
                    "ATTENDS",
                    NodeRef::var(format!("e{event}")),
                    vec![("since", PropertyInput::from(attends_since(event)))],
                ),
            );
    }
    db.query_scoped(
        QueryRequest::write(batch.returning(Vec::<String>::new())),
        scope,
    )
    .await
    .expect("fixture graph writes");
}

/// One person attending one event, written while the migrations run, with
/// its datetimes typed. A write that read the store before the label copy
/// published conflicts with publication, so it is taken again, as a client
/// retries a conflict.
async fn write_late(db: &HelixDB, index: usize) {
    let request = || {
        QueryRequest::write(
            write_batch()
                .var_as(
                    "person",
                    g().add_n(
                        "Person",
                        vec![("name", PropertyInput::from(format!("late-p{index}")))],
                    ),
                )
                .var_as(
                    "event",
                    g().add_n(
                        "Event",
                        vec![
                            ("name", PropertyInput::from(format!("late-e{index}"))),
                            (
                                "at",
                                PropertyInput::from(
                                    DateTime::parse_rfc3339(&format!(
                                        "2026-0{}-15T12:00:00Z",
                                        index + 1
                                    ))
                                    .unwrap(),
                                ),
                            ),
                        ],
                    ),
                )
                .var_as(
                    "attends",
                    g().n(NodeRef::var("person")).add_e(
                        "ATTENDS",
                        NodeRef::var("event"),
                        vec![(
                            "since",
                            PropertyInput::from(
                                DateTime::parse_rfc3339("2026-01-01T00:00:00Z").unwrap(),
                            ),
                        )],
                    ),
                )
                .returning(Vec::<String>::new()),
        )
    };
    for _ in 0..100 {
        match db.query(request()).await {
            Ok(_) => return,
            Err(error) if error.is_transaction_conflict() => {}
            Err(error) => panic!("a graph write during the migrations fails: {error}"),
        }
    }
    panic!("a graph write during the migrations kept conflicting");
}

/// Every label-served read shape: node and edge label scans and labeled
/// expansion in both directions.
async fn label_reads(db: &HelixDB, scope: DataScope) -> serde_json::Value {
    db.query_scoped(
        QueryRequest::read(
            read_batch()
                .var_as("people", g().n_with_label("Person").count())
                .var_as("events", g().n_with_label("Event").count())
                .var_as("attends", g().e_with_label("ATTENDS").count())
                .var_as(
                    "attended",
                    g().n_with_label("Person").out(Some("ATTENDS")).count(),
                )
                .var_as(
                    "attendees",
                    g().n_with_label("Event").in_(Some("ATTENDS")).count(),
                )
                .returning(["people", "events", "attends", "attended", "attendees"]),
        ),
        scope,
    )
    .await
    .expect("label reads execute")
}

/// `(name, at)` of every event in `scope`, by a label scan, sorted by name.
async fn scanned_events(db: &HelixDB, scope: DataScope) -> Vec<(String, String)> {
    let response = db
        .query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "rows",
                        g().n_with_label("Event").value_map(None::<Vec<String>>),
                    )
                    .returning(["rows"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let mut rows = response["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["name"].as_str().unwrap().to_owned(),
                row["at"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn edge_since(db: &HelixDB, scope: DataScope) -> Vec<String> {
    let response = db
        .query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as("e", g().e_with_label("ATTENDS").edge_properties())
                    .returning(["e"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let mut since = response["e"]
        .as_array()
        .unwrap()
        .iter()
        .map(|edge| edge["since"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    since.sort();
    since
}

/// Hash-keyed label rows left in any scope.
async fn legacy_label_rows(reader: &(impl DbReadOps + Sync)) -> usize {
    let mut rows = 0;
    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(reader)
        .await
        .unwrap()
    {
        for prefix in label_hash::LegacyLabelScanPrefix::ALL {
            let mut scan = reader
                .scan_prefix(DataKey::data_prefix(scope, prefix.to_bytes()), ..)
                .await
                .unwrap();
            while scan.next().await.unwrap().is_some() {
                rows += 1;
            }
        }
    }
    rows
}

/// Names of the events after `bound`, through the range index on `at`.
async fn indexed_after(service: &HelixQueryService, bound: &str) -> Vec<String> {
    let response = service
        .execute_query(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "hits",
                        g().n_with_label_where("Event", Predicate::gt_param("at", "since"))
                            .values(vec!["name"]),
                    )
                    .returning(["hits"]),
            )
            .with_typed_parameter(
                "since",
                QueryParamType::DateTime,
                QueryValue::String(bound.to_owned()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        !response
            .diagnostics()
            .insights
            .iter()
            .any(|insight| matches!(
                insight,
                helix_planner::diagnostics::PlannerInsight::MissingIndex(_)
            )),
        "the range index serves the filter"
    );
    let mut names = response.json()["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["name"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_worker_finishes_the_label_copy_and_the_datetime_conversion() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // A store as a binary before both migrations left it: hash-keyed label
    // rows, and every datetime stored as its RFC3339 string, which a range
    // index on `at` holds as a string.
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(&store),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    db.install_index_for_tests(
        SecondaryIndexDefinition::node_range("Event", "at")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    for scope in scopes() {
        write_graph(&db, scope).await;
    }
    label_identity::make_legacy_label_fixture(&db.inner_db())
        .await
        .unwrap();
    db.close().await.unwrap();

    let db = Arc::new(
        HelixDB::open_with_object_store_and_config(
            DB_PATH,
            Arc::clone(&store),
            tuned(config::MigrationWorkerMode::Background),
        )
        .await
        .unwrap(),
    );
    let inner = db.inner_db();
    // Before its background steps, the open has moved the store to the copy
    // and converted nothing: label reads still resolve through hash rows.
    for scope in scopes() {
        assert_eq!(
            label_reads(&db, scope).await,
            json!({
                "people": PEOPLE, "events": EVENTS, "attends": EVENTS,
                "attended": EVENTS, "attendees": EVENTS,
            })
        );
    }

    // Graph writes while both migrations run.
    tokio::join!(db.wait_for_background_migrations(), async {
        for index in 0..2 {
            write_late(&db, index).await;
        }
    });

    // The label copy published and its cleanup removed every hash row.
    assert_eq!(
        startup::storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    assert_eq!(legacy_label_rows(inner.as_ref()).await, 0);

    // The datetime conversion finished every scope and element.
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    let job = datetime_strings::load(writer.db()).await.unwrap().unwrap();
    assert!(job.is_completed(), "{job:?}");
    let report = job.report();
    assert_eq!(
        report.converted_values,
        u64::try_from(2 * EVENTS * scopes().len()).unwrap(),
        "{report:?}"
    );
    assert_eq!(report.unique_conflicts, 0);
    assert!(report.skipped_properties.is_empty(), "{report:?}");
    assert!(report.skipped_scopes.is_empty());
    assert_eq!(report.undecodable_rows, 0);

    for (position, scope) in scopes().into_iter().enumerate() {
        let late = if position == 0 { 2 } else { 0 };
        assert_eq!(
            label_reads(&db, scope).await,
            json!({
                "people": PEOPLE + late, "events": EVENTS + late,
                "attends": EVENTS + late, "attended": EVENTS + late,
                "attendees": EVENTS + late,
            }),
            "{scope:?}"
        );
        let mut expected = (0..EVENTS)
            .map(|event| (format!("e{event}"), canonical(&event_at(event))))
            .chain((0..late).map(|index| {
                (
                    format!("late-e{index}"),
                    format!("2026-0{}-15T12:00:00.000Z", index + 1),
                )
            }))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(scanned_events(&db, scope).await, expected, "{scope:?}");
        let mut since = (0..EVENTS)
            .map(|event| canonical(&attends_since(event)))
            .chain((0..late).map(|_| "2026-01-01T00:00:00.000Z".to_owned()))
            .collect::<Vec<_>>();
        since.sort();
        assert_eq!(edge_since(&db, scope).await, since, "{scope:?}");
    }

    // The range index holds every converted datetime: each bound selects
    // exactly what a scan of the stored values does.
    let service = HelixQueryService::new(Arc::clone(&db));
    let scanned = scanned_events(&db, DataScope::LegacyUnscoped).await;
    for bound in [
        "2025-01-01T00:00:00Z",
        "2026-01-01T00:00:00Z",
        // Event 3's instant in UTC, which its +02:00 spelling sorted after.
        "2026-04-01T01:30:00Z",
        "2026-06-01T03:30:00+02:00",
        "2026-08-01T05:30:00Z",
        "2027-01-01T00:00:00Z",
    ] {
        let instant = DateTime::parse_rfc3339(bound).unwrap();
        let expected = scanned
            .iter()
            .filter(|(_, at)| DateTime::parse_rfc3339(at).unwrap() > instant)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        assert_eq!(indexed_after(&service, bound).await, expected, "{bound}");
    }

    db.close().await.unwrap();
}
