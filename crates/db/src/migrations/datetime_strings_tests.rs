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
use crate::config::{self, DbConfig, SecondaryIndexDefinition};

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
            ("Mixed", "at", SkipReason::MixedTypes, 1),
            ("Note", "text", SkipReason::NotRfc3339, 1),
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

#[tokio::test]
async fn a_unique_collision_keeps_only_the_colliding_row_as_a_string() {
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
        vec![("name", s("b")), ("at", s("2026-01-01T01:00:00+01:00"))],
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

    assert_eq!(report.converted_values, 1, "{report:?}");
    assert_eq!(report.unique_conflicts, 1);
    assert_eq!(
        values(&db, u, "Event").await,
        [
            json!({ "name": "a", "at": "2026-01-01T00:00:00.000Z" }),
            json!({ "name": "b", "at": "2026-01-01T01:00:00+01:00" }),
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
    ] {
        observe(&mut tracked, key(name), &value);
    }
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
