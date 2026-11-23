//! Contracts for the background datetime string conversion.

use std::sync::Arc;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::expr::Predicate;
use helix_ast::graph::NodeRef;
use helix_ast::query::{QueryParamType, QueryRequest, QueryValue};
use helix_ast::traversal::g;
use helix_ast::value::{DateTime, PropertyInput};
use serde_json::json;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

use super::*;
use crate::config::{self, DbConfig, SecondaryIndexDefinition, TextIndexDefinition};
use crate::encoding::indexes::range::RangeIndexDirection;
use crate::encoding::v2::keys::{ManagedIndexKey, ScopedKey, SecondaryEntryLane};
use crate::encoding::v2::values::property::range_index_value::{
    project_range_value, RangeValueProjection,
};
use crate::index_lifecycle::{
    ActiveIndexHandle, ValidatedDynamicIndexDefinition, ValidatedSecondaryIndexDefinition,
};

tokio::task_local! {
    /// Holds a conversion batch between its reads and its write: the batch
    /// signals the first notify, then waits for the second.
    static BEFORE_CONVERSION: (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
}

/// Pauses a conversion batch taken inside a [`BEFORE_CONVERSION`] scope.
pub(super) async fn pause_before_conversion() {
    let Ok((reached, resume)) =
        BEFORE_CONVERSION.try_with(|(reached, resume)| (Arc::clone(reached), Arc::clone(resume)))
    else {
        return;
    };
    reached.notify_one();
    resume.notified().await;
}

fn config(batch_rows: usize, worker: config::MigrationWorkerMode) -> DbConfig {
    DbConfig::new().with_migration_tuning(
        MigrationTuning::default()
            .with_batch_rows(config::MigrationBatchRows::new(batch_rows).unwrap())
            .with_worker_mode(worker),
    )
}

async fn open_on(store: &Arc<dyn ObjectStore>, batch_rows: usize) -> HelixDB {
    HelixDB::open_with_object_store_and_config(
        "datetime-strings",
        Arc::clone(store),
        config(batch_rows, config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap()
}

async fn open(batch_rows: usize) -> HelixDB {
    open_on(
        &(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>),
        batch_rows,
    )
    .await
}

fn tuning(db: &HelixDB) -> MigrationTuning {
    db.config().db().migrations()
}

/// Runs the job to completion, bounded so a stuck job fails the test.
async fn run_to_completion(db: &HelixDB) -> Report {
    for _ in 0..10_000 {
        if !process_once(db, tuning(db)).await.unwrap() {
            let HelixStorage::Writer(writer) = db.storage() else {
                panic!("writer");
            };
            let job = load(writer.db()).await.unwrap().unwrap();
            assert!(job.is_completed());
            return job.report().clone();
        }
    }
    panic!("datetime string migration did not complete");
}

fn tenant(ulid: &str) -> DataScope {
    DataScope::Tenant(TenantId::from_ulid_str(ulid).unwrap())
}

async fn add_node(
    db: &HelixDB,
    scope: DataScope,
    label: &str,
    properties: Vec<(&str, PropertyInput)>,
) {
    db.query_scoped(
        QueryRequest::write(write_batch().var_as("n", g().add_n(label, properties))),
        scope,
    )
    .await
    .unwrap();
}

async fn values(db: &HelixDB, scope: DataScope, label: &str) -> Vec<serde_json::Value> {
    let response = db
        .query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "rows",
                        g().n_with_label(label).value_map(None::<Vec<String>>),
                    )
                    .returning(["rows"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let mut rows = response["rows"].as_array().unwrap().clone();
    rows.iter_mut().for_each(|row| {
        let row = row.as_object_mut().unwrap();
        row.remove("$id");
        row.remove("$label");
    });
    rows.sort_by_key(|row| row["name"].as_str().unwrap_or_default().to_owned());
    rows
}

fn s(value: &str) -> PropertyInput {
    PropertyInput::from(value)
}

fn dt(value: &str) -> PropertyInput {
    PropertyInput::from(DateTime::parse_rfc3339(value).unwrap())
}

#[tokio::test]
async fn converts_only_properties_whose_values_are_all_datetimes() {
    // Three rows per batch: every phase spans several batches.
    let db = open(3).await;
    let u = DataScope::LegacyUnscoped;
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("a")), ("at", s("2026-01-01T00:00:00Z"))],
    )
    .await;
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("b")), ("at", s("2026-06-01T05:00:00+09:00"))],
    )
    .await;
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("c")), ("at", dt("2026-03-01T00:00:00Z"))],
    )
    .await;
    add_node(&db, u, "Event", vec![("name", s("d"))]).await;
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("e")), ("at", s("2026-01-01T00:00:00.250Z"))],
    )
    .await;
    // Same property name on another label is judged on its own.
    add_node(
        &db,
        u,
        "Other",
        vec![("name", s("o")), ("at", s("next tuesday"))],
    )
    .await;
    // One plain string keeps every timestamp-looking string in the property.
    add_node(
        &db,
        u,
        "Note",
        vec![("name", s("n1")), ("text", s("2026-01-01T00:00:00Z"))],
    )
    .await;
    add_node(
        &db,
        u,
        "Note",
        vec![("name", s("n2")), ("text", s("hello"))],
    )
    .await;
    // Sub-millisecond digits would be lost.
    add_node(
        &db,
        u,
        "Precise",
        vec![("name", s("p")), ("at", s("2026-01-01T00:00:00.000001Z"))],
    )
    .await;
    // A number in the property.
    add_node(
        &db,
        u,
        "Mixed",
        vec![("name", s("m1")), ("at", s("2026-01-01T00:00:00Z"))],
    )
    .await;
    add_node(
        &db,
        u,
        "Mixed",
        vec![("name", s("m2")), ("at", PropertyInput::from(7))],
    )
    .await;
    // An edge property.
    db.query(QueryRequest::write(
        write_batch()
            .var_as("x", g().add_n("Person", vec![("name", s("x"))]))
            .var_as("y", g().add_n("Person", vec![("name", s("y"))]))
            .var_as(
                "follows",
                g().n(NodeRef::var("x")).add_e(
                    "FOLLOWS",
                    NodeRef::var("y"),
                    vec![("since", s("2020-02-02T02:02:02.002-01:00"))],
                ),
            ),
    ))
    .await
    .unwrap();

    let report = run_to_completion(&db).await;

    assert_eq!(report.converted_values, 4, "{report:?}");
    assert_eq!(report.unique_conflicts, 0);
    assert!(report.skipped_scopes.is_empty());
    let skipped = report
        .skipped_properties
        .iter()
        .map(|skip| {
            (
                skip.property.label.as_str(),
                skip.property.name.as_str(),
                skip.reason,
                skip.rfc3339_strings,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        skipped,
        [
            ("Mixed", "at", SkipReason::MixedTypes, Some(1)),
            ("Note", "text", SkipReason::NotRfc3339, Some(1)),
        ],
        "a skipped property is reported only when it holds convertible strings"
    );
    assert_eq!(
        values(&db, u, "Event").await,
        [
            json!({ "name": "a", "at": "2026-01-01T00:00:00.000Z" }),
            json!({ "name": "b", "at": "2026-05-31T20:00:00.000Z" }),
            json!({ "name": "c", "at": "2026-03-01T00:00:00.000Z" }),
            json!({ "name": "d" }),
            json!({ "name": "e", "at": "2026-01-01T00:00:00.250Z" }),
        ]
    );
    assert_eq!(
        values(&db, u, "Other").await,
        [json!({ "name": "o", "at": "next tuesday" })]
    );
    assert_eq!(
        values(&db, u, "Precise").await,
        [json!({ "name": "p", "at": "2026-01-01T00:00:00.000001Z" })]
    );
    assert_eq!(
        values(&db, u, "Note").await,
        [
            json!({ "name": "n1", "text": "2026-01-01T00:00:00Z" }),
            json!({ "name": "n2", "text": "hello" }),
        ]
    );
    let edges = db
        .query(QueryRequest::read(
            read_batch()
                .var_as("e", g().e_with_label("FOLLOWS").edge_properties())
                .returning(["e"]),
        ))
        .await
        .unwrap();
    assert_eq!(edges["e"][0]["since"], json!("2020-02-02T03:02:02.002Z"));

    // Converted values are datetimes: a typed range filter now sees them.
    let after = db
        .query(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "hits",
                        g().n_with_label_where("Event", Predicate::gte_param("at", "since"))
                            .count(),
                    )
                    .returning(["hits"]),
            )
            .with_typed_parameter(
                "since",
                QueryParamType::DateTime,
                QueryValue::String("2026-01-01T00:00:00Z".to_owned()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after["hits"], json!(4));

    // A finished job does no more work.
    assert!(!process_once(&db, tuning(&db)).await.unwrap());
}

#[tokio::test]
async fn empty_database_completes_in_one_scan_per_element() {
    let db = open(1024).await;
    let report = run_to_completion(&db).await;
    assert_eq!(report, Report::default());
}

#[tokio::test]
async fn converts_every_tenant_scope_independently() {
    let db = open(2).await;
    let a = tenant("01HZY3VQ6Y5Q7Z9M8K2N4P6R8T");
    let b = tenant("01HZY3VQ6Y5Q7Z9M8K2N4P6R9V");
    for scope in [DataScope::LegacyUnscoped, a, b] {
        for name in ["r1", "r2", "r3"] {
            add_node(
                &db,
                scope,
                "Event",
                vec![("name", s(name)), ("at", s("2026-01-01T00:00:00Z"))],
            )
            .await;
        }
    }
    // Tenant `b` alone holds a plain string, so only its property stays.
    add_node(&db, b, "Event", vec![("name", s("r4")), ("at", s("soon"))]).await;

    let report = run_to_completion(&db).await;

    assert_eq!(report.converted_values, 6, "{report:?}");
    assert_eq!(report.skipped_properties.len(), 1);
    assert_eq!(
        report.skipped_properties[0].scope,
        ScopeCursor::Tenant(
            TenantId::from_ulid_str("01HZY3VQ6Y5Q7Z9M8K2N4P6R9V")
                .unwrap()
                .as_u128()
        )
    );
    for scope in [DataScope::LegacyUnscoped, a] {
        assert!(values(&db, scope, "Event")
            .await
            .iter()
            .all(|row| row["at"] == json!("2026-01-01T00:00:00.000Z")));
    }
    assert!(values(&db, b, "Event")
        .await
        .iter()
        .all(|row| row["at"] != json!("2026-01-01T00:00:00.000Z")));
}

/// A collision in a unique index keeps only the colliding value a string: the
/// row's other convertible properties still convert.
#[tokio::test]
async fn a_unique_collision_keeps_only_the_colliding_value_as_a_string() {
    let db = open(1024).await;
    db.install_index_for_tests(
        SecondaryIndexDefinition::node_unique_equality("Event", "at")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    let u = DataScope::LegacyUnscoped;
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("a")), ("at", dt("2026-01-01T00:00:00Z"))],
    )
    .await;
    // The same instant, spelled as a string: a distinct unique key until converted.
    add_node(
        &db,
        u,
        "Event",
        vec![
            ("name", s("b")),
            ("at", s("2026-01-01T01:00:00+01:00")),
            ("until", s("2026-03-01T00:00:00Z")),
        ],
    )
    .await;
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("c")), ("at", s("2026-02-01T00:00:00Z"))],
    )
    .await;

    let report = run_to_completion(&db).await;

    assert_eq!(report.converted_values, 2, "{report:?}");
    assert_eq!(report.unique_conflicts, 1);
    assert_eq!(
        values(&db, u, "Event").await,
        [
            json!({ "name": "a", "at": "2026-01-01T00:00:00.000Z" }),
            json!({ "name": "b", "at": "2026-01-01T01:00:00+01:00", "until": "2026-03-01T00:00:00.000Z" }),
            json!({ "name": "c", "at": "2026-02-01T00:00:00.000Z" }),
        ]
    );
}

#[tokio::test]
async fn conversion_is_guarded_on_the_stored_string_and_replays_as_a_no_op() {
    let db = open(1024).await;
    let u = DataScope::LegacyUnscoped;
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("a")), ("at", s("2026-01-01T00:00:00Z"))],
    )
    .await;
    let report = run_to_completion(&db).await;
    assert_eq!(report.converted_values, 1);

    // A conversion planned against a string that is no longer stored changes
    // nothing: here the row already holds the datetime.
    let stale = [RowConversion {
        id: 0,
        values: vec![(
            "at".to_owned(),
            "2026-01-01T00:00:00Z".to_owned(),
            DateTime::parse_rfc3339("1999-01-01T00:00:00Z")
                .unwrap()
                .millis(),
        )],
    }];
    let response = execute(&db, u, conversion_request(Element::Node, &stale))
        .await
        .unwrap();
    assert_eq!(converted_values(&response, &stale), 0);
    assert_eq!(
        values(&db, u, "Event").await,
        [json!({ "name": "a", "at": "2026-01-01T00:00:00.000Z" })]
    );

    // Replaying the whole job from scratch converts nothing more.
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    writer.db().delete(job_key()).await.unwrap();
    assert_eq!(run_to_completion(&db).await.converted_values, 0);
}

#[tokio::test]
async fn progress_survives_reopening_the_writer() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_on(&store, 2).await;
    let u = DataScope::LegacyUnscoped;
    for index in 0..9 {
        add_node(
            &db,
            u,
            "Event",
            vec![
                ("name", s(&format!("r{index}"))),
                ("at", s("2026-01-01T00:00:00Z")),
            ],
        )
        .await;
    }
    // Detect both elements, then convert one batch.
    for _ in 0..8 {
        assert!(process_once(&db, tuning(&db)).await.unwrap());
    }
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    let partial = load(writer.db()).await.unwrap().unwrap();
    assert!(!partial.is_completed());
    assert!(partial.report().converted_values > 0, "{partial:?}");
    db.close().await.unwrap();

    let db = open_on(&store, 2).await;
    assert_eq!(run_to_completion(&db).await.converted_values, 9);
    assert!(values(&db, u, "Event")
        .await
        .iter()
        .all(|row| row["at"] == json!("2026-01-01T00:00:00.000Z")));
}

#[tokio::test]
async fn background_worker_converts_after_writer_open() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_on(&store, 1024).await;
    add_node(
        &db,
        DataScope::LegacyUnscoped,
        "Event",
        vec![("name", s("a")), ("at", s("2026-01-01T00:00:00Z"))],
    )
    .await;
    db.close().await.unwrap();

    let db = HelixDB::open_with_object_store_and_config(
        "datetime-strings",
        Arc::clone(&store),
        config(1024, config::MigrationWorkerMode::Background),
    )
    .await
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let HelixStorage::Writer(writer) = db.storage() else {
                panic!("writer");
            };
            if load(writer.db())
                .await
                .unwrap()
                .is_some_and(|job| job.is_completed())
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the background worker completes the job");
    assert_eq!(
        values(&db, DataScope::LegacyUnscoped, "Event").await,
        [json!({ "name": "a", "at": "2026-01-01T00:00:00.000Z" })]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn a_scope_with_too_many_properties_is_skipped_and_reported() {
    let db = open(1024).await;
    let properties = (0..=MAX_TRACKED_PROPERTIES)
        .map(|index| (format!("p{index}"), s("2026-01-01T00:00:00Z")))
        .collect::<Vec<_>>();
    add_node(
        &db,
        DataScope::LegacyUnscoped,
        "Wide",
        properties
            .iter()
            .map(|(name, value)| (name.as_str(), value.clone()))
            .collect(),
    )
    .await;
    let report = run_to_completion(&db).await;
    assert_eq!(report.skipped_scopes, [ScopeCursor::Unscoped]);
    assert_eq!(report.converted_values, 0);
}

#[test]
fn job_state_round_trips_through_json() {
    let job = Job::Running {
        scope: ScopeCursor::Tenant(u128::MAX),
        phase: Phase::Convert {
            element: Element::Edge,
            resume_after: Some(vec![0xFD, 1, 2]),
            candidates: vec![PropertyKey {
                element: Element::Edge,
                label: "FOLLOWS".to_owned(),
                name: "since".to_owned(),
            }],
        },
        report: Report {
            converted_values: 3,
            unique_conflicts: 1,
            skipped_properties: Vec::new(),
            unlisted_skipped_properties: 0,
            skipped_scopes: vec![ScopeCursor::Unscoped],
            undecodable_rows: 2,
        },
    };
    let encoded = encode_json(&job).unwrap();
    assert!(std::str::from_utf8(&encoded)
        .unwrap()
        .contains("ffffffffffffffffffffffffffffffff"));
    assert_eq!(decode_json::<Job>(&encoded).unwrap(), job);
}

#[test]
fn verdicts_follow_every_value_kind() {
    let key = |name: &str| PropertyKey {
        element: Element::Node,
        label: "L".to_owned(),
        name: name.to_owned(),
    };
    let mut tracked = BTreeMap::new();
    for (name, value) in [
        ("null_only", PropertyValue::Null),
        ("datetime_only", PropertyValue::DateTime(1)),
        (
            "string_then_datetime",
            PropertyValue::String("2026-01-01T00:00:00Z".to_owned()),
        ),
        ("string_then_datetime", PropertyValue::DateTime(1)),
        ("skip_is_final", PropertyValue::Bool(true)),
        (
            "skip_is_final",
            PropertyValue::String("2026-01-01T00:00:00Z".to_owned()),
        ),
        // A datetime list is a list, not a datetime, like any other array.
        (
            "string_then_datetime_list",
            PropertyValue::String("2026-01-01T00:00:00Z".to_owned()),
        ),
        (
            "string_then_datetime_list",
            PropertyValue::DateTimeArray(vec![1]),
        ),
    ] {
        observe(&mut tracked, key(name), &value);
    }
    assert_eq!(
        tracked[&key("string_then_datetime_list")].verdict,
        Verdict::Skipped(SkipReason::MixedTypes)
    );
    assert!(!tracked.contains_key(&key("null_only")));
    assert_eq!(tracked[&key("datetime_only")].verdict, Verdict::Datetimes);
    assert_eq!(
        tracked[&key("string_then_datetime")].verdict,
        Verdict::Convertible
    );
    assert_eq!(
        tracked[&key("skip_is_final")],
        Tracked {
            verdict: Verdict::Skipped(SkipReason::MixedTypes),
            rfc3339_strings: 1,
        }
    );
    assert_eq!(parse_millis("1969-12-31T23:59:59.999Z"), Some(Ok(-1)));
    assert_eq!(parse_millis("2026-01-01"), None);
}

#[tokio::test]
async fn an_undecodable_row_is_reported_without_stalling_the_job() {
    let db = open(1024).await;
    let u = DataScope::LegacyUnscoped;
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("a")), ("at", s("2026-01-01T00:00:00Z"))],
    )
    .await;
    let mut corrupt = KeyPrefix::NodeProperty.as_slice().to_vec();
    corrupt.extend_from_slice(&999_u64.to_be_bytes());
    db.inner_db()
        .put(&corrupt, b"not an rkyv property row")
        .await
        .unwrap();
    let report = run_to_completion(&db).await;
    assert_eq!(report.undecodable_rows, 1, "{report:?}");
    assert_eq!(report.converted_values, 1);
}

/// Cypher and SQL read the converted rows as datetimes: before the job the
/// legacy strings never compare with a datetime, after it every converted
/// row filters, orders and aggregates chronologically, and a property the
/// job left as stored still reads as its string.
#[tokio::test]
async fn cypher_and_sql_read_converted_rows_as_datetimes() {
    let db = open(2).await;
    let u = DataScope::LegacyUnscoped;
    for (name, at) in [
        ("a", s("2026-01-01T00:00:00Z")),
        ("b", s("2026-06-01T05:00:00+09:00")),
        ("c", dt("2026-03-01T00:00:00Z")),
        ("d", s("2025-12-31T23:59:59.999Z")),
    ] {
        add_node(&db, u, "Event", vec![("name", s(name)), ("at", at)]).await;
    }
    add_node(
        &db,
        u,
        "Note",
        vec![("name", s("n1")), ("at", s("2026-01-01T00:00:00Z"))],
    )
    .await;
    add_node(&db, u, "Note", vec![("name", s("n2")), ("at", s("soon"))]).await;
    let cypher = |text: &str| {
        let db = &db;
        let text = text.to_owned();
        async move {
            db.cypher(crate::cypher::Request::new(text.clone()))
                .await
                .unwrap_or_else(|error| panic!("{text}: {error:?}"))
                .rows
        }
    };
    let since = "MATCH (e:Event) WHERE e.at >= datetime('2026-01-01') RETURN e.name ORDER BY e.at";
    // Only the datetime written as a literal compares before the job runs.
    assert_eq!(cypher(since).await, vec![vec![json!("c")]]);
    let report = run_to_completion(&db).await;
    assert_eq!(report.converted_values, 3, "{report:?}");
    assert_eq!(
        cypher(since).await,
        vec![vec![json!("a")], vec![json!("c")], vec![json!("b")]]
    );
    assert_eq!(
        cypher("MATCH (e:Event) RETURN min(e.at), max(e.at), count(DISTINCT e.at)").await,
        vec![vec![
            json!("2025-12-31T23:59:59.999Z"),
            json!("2026-05-31T20:00:00.000Z"),
            json!(4)
        ]]
    );
    assert_eq!(
        cypher("MATCH (e:Event {name: 'b'}) RETURN e.at = datetime('2026-06-01T05:00:00+09:00')")
            .await,
        vec![vec![json!(true)]]
    );
    // The Note property mixes a plain string, so the job leaves it as text.
    assert_eq!(
        cypher("MATCH (n:Note) RETURN n.name, n.at, n.at < datetime() ORDER BY n.name").await,
        vec![
            vec![
                json!("n1"),
                json!("2026-01-01T00:00:00Z"),
                serde_json::Value::Null
            ],
            vec![json!("n2"), json!("soon"), serde_json::Value::Null],
        ]
    );
    let sql = db
        .sql(crate::cypher::sql::request::Request::new(
            "SELECT e.name FROM Event e WHERE e.at < TIMESTAMP '2026-02-01 00:00:00' ORDER BY e.at",
        ))
        .await
        .unwrap();
    assert_eq!(sql.rows, vec![vec![json!("d")], vec![json!("a")]]);
    db.close().await.unwrap();
}

/// `(id, value)` of every unscoped `Event` storing `property`, read from the
/// rows.
async fn stored(db: &HelixDB, property: &str) -> Vec<(u64, PropertyValue)> {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    read_rows(
        writer.db(),
        DataScope::LegacyUnscoped,
        Element::Node,
        None,
        MigrationTuning::default(),
    )
    .await
    .unwrap()
    .iter()
    .filter_map(|(key, value)| {
        let (id, row) = decode_row(DataScope::LegacyUnscoped, Element::Node, key, value)
            .unwrap()
            .unwrap();
        if label_of(&row) != Some("Event") {
            return None;
        }
        row.into_iter()
            .find(|stored| stored.name == property)
            .map(|stored| (id, stored.value))
    })
    .collect()
}

/// The raw rows of `Event.{property}`'s equality and range indexes, read
/// without the per-row check a query applies to index hits, hold exactly what
/// a scan of the rows does: each probed value's equality row holds the ids
/// storing that value and no other, and the range index holds one entry per
/// stored value.
async fn assert_indexes_match_rows(db: &HelixDB, property: &str, probes: &[PropertyValue]) {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    let store = writer.db();
    let stored = stored(db, property).await;
    let handles = db.active_index_handles_loaded(DataScope::LegacyUnscoped);
    let handle = |range: bool| -> &ActiveIndexHandle {
        handles
            .iter()
            .find(|handle| {
                handle
                    .secondary_definition()
                    .is_some_and(|definition| definition.property().as_str() == property)
                    && matches!(
                        (handle.secondary_definition(), range),
                        (
                            Some(ValidatedSecondaryIndexDefinition::NodeEquality { .. }),
                            false
                        ) | (
                            Some(ValidatedSecondaryIndexDefinition::NodeRange { .. }),
                            true
                        )
                    )
            })
            .expect("the index is active")
    };

    for probe in probes {
        let expected = stored
            .iter()
            .filter(|(_, value)| value == probe)
            .map(|(id, _)| *id)
            .collect::<roaring::RoaringTreemap>();
        let indexed = crate::index_lifecycle::secondary::lookup_active_equality_point_literal(
            store,
            handle(false),
            probe,
        )
        .await
        .unwrap();
        assert_eq!(indexed, expected, "{property} {probe:?}");
    }

    let range = handle(true);
    let mut rows = store
        .scan_prefix(
            ManagedIndexKey::data_prefix(
                DataScope::LegacyUnscoped,
                ScopedKey::secondary_lane_prefix(
                    range.index_id(),
                    range.generation(),
                    SecondaryEntryLane::NodeRangeAscending,
                ),
            ),
            ..,
        )
        .await
        .unwrap();
    let mut indexed = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        let ManagedIndexKey::Data {
            kind: ScopedKey::SecondaryEntry(entry),
            ..
        } = ManagedIndexKey::parse_from_slice(DataScope::LegacyUnscoped, &row.key).unwrap()
        else {
            panic!("the range lane holds secondary entries");
        };
        indexed.push((
            entry.entity_id().unwrap().get(),
            entry.range_value().unwrap().encoded().to_vec(),
        ));
    }
    indexed.sort();
    let mut expected = stored
        .iter()
        .map(|(id, value)| {
            let RangeValueProjection::Indexed(value) =
                project_range_value(value, RangeIndexDirection::Asc)
            else {
                panic!("{value:?} is range indexed");
            };
            (*id, value.encoded().to_vec())
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(indexed, expected, "{property}");
}

/// Converting a property an equality and a range index cover leaves no
/// string entry behind: afterwards the old strings find nothing, the
/// datetimes find every row, and each index holds what a scan of the rows does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conversion_leaves_no_string_entry_in_equality_or_range_indexes() {
    let db = open(2).await;
    for definition in [
        SecondaryIndexDefinition::node_equality("Event", "at"),
        SecondaryIndexDefinition::node_range("Event", "at"),
    ] {
        db.install_index_for_tests(definition.unwrap().try_into().unwrap())
            .await
            .unwrap();
    }
    let u = DataScope::LegacyUnscoped;
    // Two spellings of one instant, which one datetime written as a literal
    // shares too.
    let strings = [
        "2026-01-01T00:00:00Z",
        "2026-01-01T01:00:00+01:00",
        "2026-06-01T05:00:00+09:00",
        "2025-12-31T23:59:59.999Z",
        "2026-03-01T00:00:00.250-05:00",
    ];
    for (index, at) in strings.iter().enumerate() {
        add_node(
            &db,
            u,
            "Event",
            vec![("name", s(&format!("e{index}"))), ("at", s(at))],
        )
        .await;
    }
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("typed")), ("at", dt("2026-01-01T00:00:00Z"))],
    )
    .await;
    let probes = strings
        .iter()
        .flat_map(|at| {
            [
                PropertyValue::String((*at).to_owned()),
                PropertyValue::DateTime(parse_millis(at).unwrap().unwrap()),
            ]
        })
        .collect::<Vec<_>>();
    // Before the job the indexes hold the strings, which this check sees.
    assert_indexes_match_rows(&db, "at", &probes).await;
    assert_eq!(stored(&db, "at").await.len(), strings.len() + 1);

    let report = run_to_completion(&db).await;
    assert_eq!(report.converted_values, 5, "{report:?}");
    assert!(stored(&db, "at")
        .await
        .iter()
        .all(|(_, value)| matches!(value, PropertyValue::DateTime(_))));
    assert_indexes_match_rows(&db, "at", &probes).await;

    // Through queries too: the strings find nothing, the datetimes find
    // every row.
    let hits = |predicate: Predicate, at: Option<&str>| {
        let db = &db;
        let at = at.map(str::to_owned);
        async move {
            let request = QueryRequest::read(
                read_batch()
                    .var_as(
                        "hits",
                        g().n_with_label_where("Event", predicate)
                            .values(vec!["name"]),
                    )
                    .returning(["hits"]),
            );
            let request = match at {
                Some(at) => request
                    .with_typed_parameter("at", QueryParamType::DateTime, QueryValue::String(at))
                    .unwrap(),
                None => request,
            };
            let response = db.query(request).await.unwrap();
            let mut names = response["hits"]
                .as_array()
                .unwrap()
                .iter()
                .map(|hit| hit["name"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            names.sort();
            names
        }
    };
    for at in strings {
        assert!(hits(Predicate::eq("at", at), None).await.is_empty(), "{at}");
    }
    assert_eq!(
        hits(
            Predicate::eq_param("at", "at"),
            Some("2026-01-01T00:00:00Z")
        )
        .await,
        ["e0", "e1", "typed"]
    );
    assert_eq!(
        hits(
            Predicate::gt_param("at", "at"),
            Some("2000-01-01T00:00:00Z")
        )
        .await,
        ["e0", "e1", "e2", "e3", "e4", "typed"]
    );
    db.close().await.unwrap();
}

/// A user write committed between a conversion batch's reads and its write
/// wins: the batch leaves the property it set to a new string, set to a new
/// datetime, or removed exactly as the user left it, and still converts the
/// row's other convertible properties. A property the job skipped stays as
/// stored on every row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_between_a_batchs_reads_and_its_write_wins_and_loses_nothing() {
    let db = open(1024).await;
    let u = DataScope::LegacyUnscoped;
    for name in ["string", "datetime", "removed", "untouched"] {
        add_node(
            &db,
            u,
            "Event",
            vec![
                ("name", s(name)),
                ("at", s("2026-01-01T00:00:00Z")),
                ("until", s("2026-02-01T00:00:00+01:00")),
                ("note", s("2026-03-01T00:00:00Z")),
            ],
        )
        .await;
    }
    // One plain string leaves every `note` as stored.
    add_node(
        &db,
        u,
        "Event",
        vec![("name", s("plain")), ("note", s("soon"))],
    )
    .await;

    // Detect both elements, up to the first conversion batch.
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    while !matches!(
        load(writer.db()).await.unwrap(),
        Some(Job::Running {
            phase: Phase::Convert { .. },
            ..
        })
    ) {
        assert!(process_once(&db, tuning(&db)).await.unwrap());
    }

    // The batch reads every row, then waits while the user writes `at`.
    let pause = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let (reached, resume) = (Arc::clone(&pause.0), Arc::clone(&pause.1));
    let event = |name: &str| g().n_with_label_where("Event", Predicate::eq("name", name));
    let (stepped, ()) = tokio::join!(
        BEFORE_CONVERSION.scope(pause, process_once(&db, tuning(&db))),
        async {
            reached.notified().await;
            db.query(QueryRequest::write(
                write_batch()
                    .var_as(
                        "string",
                        event("string").set_property("at", s("2027-01-01T00:00:00Z")),
                    )
                    .var_as(
                        "datetime",
                        event("datetime").set_property("at", dt("2027-02-01T00:00:00Z")),
                    )
                    .var_as("removed", event("removed").remove_property("at")),
            ))
            .await
            .unwrap();
            resume.notify_one();
        }
    );
    assert!(stepped.unwrap());

    let report = run_to_completion(&db).await;
    // `until` on every row, and `at` only on the row the user left alone.
    assert_eq!(report.converted_values, 5, "{report:?}");
    assert_eq!(
        report
            .skipped_properties
            .iter()
            .map(|skip| (skip.property.name.as_str(), skip.reason))
            .collect::<Vec<_>>(),
        [("note", SkipReason::NotRfc3339)]
    );
    let until = "2026-01-31T23:00:00.000Z";
    let note = "2026-03-01T00:00:00Z";
    assert_eq!(
        values(&db, u, "Event").await,
        [
            json!({ "name": "datetime", "at": "2027-02-01T00:00:00.000Z", "until": until, "note": note }),
            json!({ "name": "plain", "note": "soon" }),
            json!({ "name": "removed", "until": until, "note": note }),
            json!({ "name": "string", "at": "2027-01-01T00:00:00Z", "until": until, "note": note }),
            json!({ "name": "untouched", "at": "2026-01-01T00:00:00.000Z", "until": until, "note": note }),
        ]
    );
    db.close().await.unwrap();
}

/// A row whose convertible properties are each covered by an equality and a
/// range index converts every one in one batch, and each index ends holding
/// exactly what a scan of the rows does: one write's index update never
/// undoes or duplicates another's on the same row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn converting_several_indexed_properties_of_one_row_maintains_each_index() {
    let db = open(2).await;
    for property in ["at", "until"] {
        for definition in [
            SecondaryIndexDefinition::node_equality("Event", property),
            SecondaryIndexDefinition::node_range("Event", property),
        ] {
            db.install_index_for_tests(definition.unwrap().try_into().unwrap())
                .await
                .unwrap();
        }
    }
    let u = DataScope::LegacyUnscoped;
    let rows = [
        ("e0", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
        (
            "e1",
            "2026-01-01T01:00:00+01:00",
            "2026-01-02T00:00:00.000Z",
        ),
        (
            "e2",
            "2026-06-01T05:00:00+09:00",
            "2026-06-01T05:00:00+09:00",
        ),
    ];
    for (name, at, until) in rows {
        add_node(
            &db,
            u,
            "Event",
            vec![("name", s(name)), ("at", s(at)), ("until", s(until))],
        )
        .await;
    }
    // A typed `at` beside a string `until`: only `until` converts.
    add_node(
        &db,
        u,
        "Event",
        vec![
            ("name", s("typed")),
            ("at", dt("2026-01-01T00:00:00Z")),
            ("until", s("2026-01-02T00:00:00Z")),
        ],
    )
    .await;
    let probes = rows
        .iter()
        .flat_map(|(_, at, until)| [at, until])
        .flat_map(|stored| {
            [
                PropertyValue::String((*stored).to_owned()),
                PropertyValue::DateTime(parse_millis(stored).unwrap().unwrap()),
            ]
        })
        .collect::<Vec<_>>();
    for property in ["at", "until"] {
        assert_indexes_match_rows(&db, property, &probes).await;
    }

    let report = run_to_completion(&db).await;
    assert_eq!(report.converted_values, 7, "{report:?}");
    for property in ["at", "until"] {
        let stored = stored(&db, property).await;
        assert_eq!(stored.len(), rows.len() + 1, "{property}");
        assert!(
            stored
                .iter()
                .all(|(_, value)| matches!(value, PropertyValue::DateTime(_))),
            "{property}: {stored:?}"
        );
        assert_indexes_match_rows(&db, property, &probes).await;
    }
    db.close().await.unwrap();
}

// Properties a text or vector index covers.

fn text_index(label: &str, property: &str) -> ValidatedDynamicIndexDefinition {
    TextIndexDefinition::new_node(label, property)
        .unwrap()
        .try_into()
        .unwrap()
}

/// Nodes of `label` a text search of `property` for `text` finds, by name.
async fn text_hits(
    db: &HelixDB,
    scope: DataScope,
    label: &str,
    property: &str,
    text: &str,
) -> Vec<String> {
    let response = db
        .query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "hits",
                        g().text_search_nodes(label, property, text, 100, None)
                            .values(vec!["name"]),
                    )
                    .returning(["hits"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let mut names = response["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("text search returned {response}"))
        .iter()
        .map(|hit| hit["name"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

/// One skipped property: scope, element, label, name, reason and count.
type Skip<'a> = (
    ScopeCursor,
    Element,
    &'a str,
    &'a str,
    SkipReason,
    Option<u64>,
);

fn skips(report: &Report) -> Vec<Skip<'_>> {
    report
        .skipped_properties
        .iter()
        .map(|skip| {
            (
                skip.scope,
                skip.property.element,
                skip.property.label.as_str(),
                skip.property.name.as_str(),
                skip.reason,
                skip.rfc3339_strings,
            )
        })
        .collect()
}

fn cursor(scope: DataScope) -> ScopeCursor {
    match scope {
        DataScope::LegacyUnscoped => ScopeCursor::Unscoped,
        DataScope::Tenant(tenant) => ScopeCursor::Tenant(tenant.as_u128()),
    }
}

const AT: &str = "2026-01-01T00:00:00Z";
const AT_DATETIME: &str = "2026-01-01T00:00:00.000Z";

/// Two `Doc` nodes with an RFC3339 `at` and `until` in `scope`.
async fn add_docs(db: &HelixDB, scope: DataScope) {
    for name in ["d1", "d2"] {
        add_node(
            db,
            scope,
            "Doc",
            vec![("name", s(name)), ("at", s(AT)), ("until", s(AT))],
        )
        .await;
    }
}

/// A text index keeps exactly the property it covers in exactly its scope,
/// element kind and label as stored, reported, and still searchable; every
/// other convertible property converts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_indexed_property_keeps_its_strings_in_its_scope_kind_and_label() {
    let db = open(2).await;
    let u = DataScope::LegacyUnscoped;
    let a = tenant("01HZY3VQ6Y5Q7Z9M8K2N4P6R8T");
    let b = tenant("01HZY3VQ6Y5Q7Z9M8K2N4P6R9V");
    for scope in [u, a, b] {
        add_docs(&db, scope).await;
        // The same property name on another label.
        add_node(&db, scope, "Event", vec![("name", s("e")), ("at", s(AT))]).await;
    }
    db.query(QueryRequest::write(
        write_batch()
            .var_as("x", g().add_n("Person", vec![("name", s("x"))]))
            .var_as("y", g().add_n("Person", vec![("name", s("y"))]))
            .var_as(
                "links",
                g().n(NodeRef::var("x"))
                    .add_e("LINKS", NodeRef::var("y"), vec![("since", s(AT))]),
            ),
    ))
    .await
    .unwrap();
    // Unscoped: `Doc.at` on nodes, `LINKS.since` on edges, and an edge index
    // named like the node property `Doc.until`, which covers no node.
    for definition in [
        text_index("Doc", "at"),
        TextIndexDefinition::new_edge("LINKS", "since")
            .unwrap()
            .try_into()
            .unwrap(),
        TextIndexDefinition::new_edge("Doc", "until")
            .unwrap()
            .try_into()
            .unwrap(),
    ] {
        db.install_index_for_tests(definition).await.unwrap();
    }
    // Tenant `a`: `Doc.until` only. Tenant `b`: none.
    db.install_index_in_scope_for_tests(a, text_index("Doc", "until"))
        .await
        .unwrap();

    let report = run_to_completion(&db).await;

    let node = Element::Node;
    assert_eq!(
        skips(&report),
        [
            (
                cursor(u),
                node,
                "Doc",
                "at",
                SkipReason::TextIndexed,
                Some(2)
            ),
            (
                cursor(u),
                Element::Edge,
                "LINKS",
                "since",
                SkipReason::TextIndexed,
                Some(1)
            ),
            (
                cursor(a),
                node,
                "Doc",
                "until",
                SkipReason::TextIndexed,
                Some(2)
            ),
        ]
    );
    // u: Doc.until 2 + Event.at; a: Doc.at 2 + Event.at; b: everything.
    assert_eq!(report.converted_values, 3 + 3 + 5, "{report:?}");
    let docs = |at: &str, until: &str| {
        ["d1", "d2"].map(|name| json!({ "name": name, "at": at, "until": until }))
    };
    assert_eq!(values(&db, u, "Doc").await, docs(AT, AT_DATETIME));
    assert_eq!(values(&db, a, "Doc").await, docs(AT_DATETIME, AT));
    assert_eq!(values(&db, b, "Doc").await, docs(AT_DATETIME, AT_DATETIME));
    for scope in [u, a, b] {
        assert_eq!(
            values(&db, scope, "Event").await,
            [json!({ "name": "e", "at": AT_DATETIME })]
        );
    }
    // The covered strings are still what their indexes find.
    assert_eq!(text_hits(&db, u, "Doc", "at", "2026").await, ["d1", "d2"]);
    assert_eq!(
        text_hits(&db, a, "Doc", "until", "2026").await,
        ["d1", "d2"]
    );
    db.close().await.unwrap();
}

/// A vector index whose build is blocked on string values stays Building, and
/// a datetime would fail its writes as the strings do: the property keeps them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_indexed_property_keeps_its_strings() {
    let db = open(1024).await;
    let u = DataScope::LegacyUnscoped;
    add_docs(&db, u).await;
    let blocked = db
        .install_index_for_tests(
            crate::config::VectorIndexDefinition::new_node(
                "Doc",
                "at",
                3,
                crate::search::vector::VectorDistanceMetric::Cosine,
            )
            .unwrap()
            .try_into()
            .unwrap(),
        )
        .await;
    assert!(
        matches!(&blocked, Err(HelixDbError::InvariantViolation(reason)) if reason.contains("did not succeed")),
        "{blocked:?}"
    );

    let report = run_to_completion(&db).await;

    assert_eq!(
        skips(&report),
        [(
            cursor(u),
            Element::Node,
            "Doc",
            "at",
            SkipReason::VectorIndexed,
            Some(2)
        )]
    );
    assert_eq!(report.converted_values, 2, "{report:?}");
    assert_eq!(
        values(&db, u, "Doc").await,
        ["d1", "d2"].map(|name| json!({ "name": name, "at": AT, "until": AT_DATETIME }))
    );
    db.close().await.unwrap();
}

/// A dropped text index no longer covers its property, which converts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_text_index_covers_nothing() {
    let db = open(1024).await;
    let u = DataScope::LegacyUnscoped;
    add_docs(&db, u).await;
    db.install_index_for_tests(text_index("Doc", "at"))
        .await
        .unwrap();
    let receipt = db
        .enqueue_index_drop(
            u,
            &helix_planner::ir::IndexDdlDropSpec::NodeText {
                key: helix_planner::catalog::ScopedPropertyKey::try_new("Doc", "at").unwrap(),
            },
        )
        .await
        .unwrap();
    let operation = receipt.operation_id().expect("the drop runs an operation");
    db.wake_index_worker().await;
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while !matches!(
            db.get_index_operation(u, operation).await.unwrap(),
            crate::index_lifecycle::IndexOperationStatus::Succeeded { .. }
        ) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the drop completes");

    let report = run_to_completion(&db).await;

    assert!(report.skipped_properties.is_empty(), "{report:?}");
    assert_eq!(report.converted_values, 4);
    assert_eq!(
        values(&db, u, "Doc").await,
        ["d1", "d2"].map(|name| json!({ "name": name, "at": AT_DATETIME, "until": AT_DATETIME }))
    );
    db.close().await.unwrap();
}

/// The job row a binary that did not check text indexes stored after detecting
/// a text-indexed property in a tenant, as that binary encodes it: the
/// candidates name `Doc.at`, which its every conversion batch failed on.
fn stuck_job_json(tenant: u128) -> String {
    format!(
        concat!(
            r#"{{"running":{{"scope":{{"tenant":"{tenant:032x}"}},"#,
            r#""phase":{{"convert":{{"element":"node","resume_after":null,"candidates":["#,
            r#"{{"element":"node","label":"Doc","name":"at"}},"#,
            r#"{{"element":"node","label":"Doc","name":"until"}}]}}}},"#,
            r#""report":{{"converted_values":3,"unique_conflicts":0,"skipped_properties":["#,
            r#"{{"scope":"unscoped","property":{{"element":"node","label":"Precise","name":"at"}},"#,
            r#""reason":"sub_millisecond","rfc3339_strings":1}}],"#,
            r#""unlisted_skipped_properties":0,"skipped_scopes":[],"undecodable_rows":0}}}}}}"#
        ),
        tenant = tenant
    )
}

/// A store whose job a binary without the check left failing on a
/// text-indexed candidate resumes on this one: the property keeps its strings
/// and stays searchable, the scope's other candidate and every later tenant
/// convert, and the job completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_stuck_on_a_text_indexed_candidate_resumes_and_completes() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_on(&store, 1).await;
    let stuck = tenant("01HZY3VQ6Y5Q7Z9M8K2N4P6R8T");
    let later = tenant("01HZY3VQ6Y5Q7Z9M8K2N4P6R9V");
    add_docs(&db, stuck).await;
    add_docs(&db, later).await;
    db.install_index_in_scope_for_tests(stuck, text_index("Doc", "at"))
        .await
        .unwrap();
    let DataScope::Tenant(stuck_id) = stuck else {
        unreachable!()
    };
    let json = stuck_job_json(stuck_id.as_u128());
    // The stored form is unchanged: this binary reads and writes it byte for byte.
    let job = decode_json::<Job>(json.as_bytes()).unwrap();
    assert_eq!(encode_json(&job).unwrap(), json.as_bytes());
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    writer
        .db()
        .put(job_key(), Bytes::from(json.clone()))
        .await
        .unwrap();
    // What that binary's batch wrote fails: the text index takes no datetime.
    let [d1, _] = ids(&db, stuck, "Doc").await;
    let failed = execute(
        &db,
        stuck,
        conversion_request(
            Element::Node,
            &[RowConversion {
                id: d1,
                values: vec![(
                    "at".to_owned(),
                    AT.to_owned(),
                    parse_millis(AT).unwrap().unwrap(),
                )],
            }],
        ),
    )
    .await;
    assert!(
        matches!(&failed, Err(HelixDbError::InvalidIndexSourceData { reason }) if reason.contains("Doc:at")),
        "{failed:?}"
    );
    db.close().await.unwrap();

    let db = open_on(&store, 1).await;
    let report = run_to_completion(&db).await;

    assert_eq!(
        skips(&report),
        [
            (
                ScopeCursor::Unscoped,
                Element::Node,
                "Precise",
                "at",
                SkipReason::SubMillisecond,
                Some(1)
            ),
            (
                cursor(stuck),
                Element::Node,
                "Doc",
                "at",
                SkipReason::TextIndexed,
                None
            ),
        ]
    );
    // 3 before, `Doc.until` in the stuck tenant, both properties in the later one.
    assert_eq!(report.converted_values, 3 + 2 + 4, "{report:?}");
    assert_eq!(
        values(&db, stuck, "Doc").await,
        ["d1", "d2"].map(|name| json!({ "name": name, "at": AT, "until": AT_DATETIME }))
    );
    assert_eq!(
        values(&db, later, "Doc").await,
        ["d1", "d2"].map(|name| json!({ "name": name, "at": AT_DATETIME, "until": AT_DATETIME }))
    );
    assert_eq!(
        text_hits(&db, stuck, "Doc", "at", "2026").await,
        ["d1", "d2"]
    );
    db.close().await.unwrap();
}

/// Ids of the nodes of `label` in `scope`, by name.
async fn ids<const N: usize>(db: &HelixDB, scope: DataScope, label: &str) -> [u64; N] {
    let response = db
        .query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "rows",
                        g().n_with_label(label).value_map(None::<Vec<String>>),
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
                row["$id"].as_u64().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows.into_iter()
        .map(|(_, id)| id)
        .collect::<Vec<_>>()
        .try_into()
        .unwrap()
}

/// A text index created after detection drops its property at the next
/// conversion batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_index_created_after_detection_is_skipped_at_conversion() {
    let db = open(1024).await;
    let u = DataScope::LegacyUnscoped;
    add_docs(&db, u).await;
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    while !matches!(
        load(writer.db()).await.unwrap(),
        Some(Job::Running {
            phase: Phase::Convert { .. },
            ..
        })
    ) {
        assert!(process_once(&db, tuning(&db)).await.unwrap());
    }
    db.install_index_for_tests(text_index("Doc", "at"))
        .await
        .unwrap();

    let report = run_to_completion(&db).await;

    assert_eq!(
        skips(&report),
        [(
            cursor(u),
            Element::Node,
            "Doc",
            "at",
            SkipReason::TextIndexed,
            None
        )]
    );
    assert_eq!(report.converted_values, 2);
    assert_eq!(
        values(&db, u, "Doc").await,
        ["d1", "d2"].map(|name| json!({ "name": name, "at": AT, "until": AT_DATETIME }))
    );
    assert_eq!(text_hits(&db, u, "Doc", "at", "2026").await, ["d1", "d2"]);
    db.close().await.unwrap();
}

/// A text index created between a batch's check and its write fails that
/// batch, writing nothing; the next batch skips the property.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_text_index_created_mid_batch_fails_only_that_batch() {
    let db = open(1024).await;
    let u = DataScope::LegacyUnscoped;
    add_docs(&db, u).await;
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    while !matches!(
        load(writer.db()).await.unwrap(),
        Some(Job::Running {
            phase: Phase::Convert { .. },
            ..
        })
    ) {
        assert!(process_once(&db, tuning(&db)).await.unwrap());
    }
    let pause = (
        Arc::new(tokio::sync::Notify::new()),
        Arc::new(tokio::sync::Notify::new()),
    );
    let (reached, resume) = (Arc::clone(&pause.0), Arc::clone(&pause.1));
    let (stepped, ()) = tokio::join!(
        BEFORE_CONVERSION.scope(pause, process_once(&db, tuning(&db))),
        async {
            reached.notified().await;
            // Building from its commit on: writes route to the build at once.
            crate::index_lifecycle::lifecycle::create_index_operation_from_current_source(
                writer.db(),
                u,
                text_index("Doc", "at"),
                helix_planner::ir::IndexCreateMode::IfNotExists,
            )
            .await
            .unwrap();
            resume.notify_one();
        }
    );
    assert!(
        matches!(&stepped, Err(HelixDbError::InvalidIndexSourceData { .. })),
        "{stepped:?}"
    );
    assert_eq!(
        values(&db, u, "Doc").await,
        ["d1", "d2"].map(|name| json!({ "name": name, "at": AT, "until": AT })),
        "the failed batch wrote nothing"
    );

    let report = run_to_completion(&db).await;

    assert_eq!(
        skips(&report),
        [(
            cursor(u),
            Element::Node,
            "Doc",
            "at",
            SkipReason::TextIndexed,
            None
        )]
    );
    assert_eq!(report.converted_values, 2);
    assert_eq!(
        values(&db, u, "Doc").await,
        ["d1", "d2"].map(|name| json!({ "name": name, "at": AT, "until": AT_DATETIME }))
    );
    db.close().await.unwrap();
}

#[test]
fn new_skip_reasons_round_trip_through_json() {
    let skipped = SkippedProperty {
        scope: ScopeCursor::Unscoped,
        property: PropertyKey {
            element: Element::Edge,
            label: "LINKS".to_owned(),
            name: "since".to_owned(),
        },
        reason: SkipReason::TextIndexed,
        rfc3339_strings: None,
    };
    let encoded = encode_json(&skipped).unwrap();
    assert_eq!(
        std::str::from_utf8(&encoded).unwrap(),
        r#"{"scope":"unscoped","property":{"element":"edge","label":"LINKS","name":"since"},"reason":"text_indexed"}"#
    );
    assert_eq!(decode_json::<SkippedProperty>(&encoded).unwrap(), skipped);
    let vector = SkippedProperty {
        reason: SkipReason::VectorIndexed,
        rfc3339_strings: Some(4),
        ..skipped
    };
    let encoded = encode_json(&vector).unwrap();
    assert!(std::str::from_utf8(&encoded)
        .unwrap()
        .ends_with(r#""reason":"vector_indexed","rfc3339_strings":4}"#));
    assert_eq!(decode_json::<SkippedProperty>(&encoded).unwrap(), vector);
}
