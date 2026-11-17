//! Typed `date_time` parameters store and compare as datetimes.
//!
//! A declared `date_time` parameter used to reach storage as its raw RFC3339
//! string, so values written through parameters and values written as literals
//! (or `now()`) sat in different comparison domains: a range filter of either
//! kind silently missed the other, and strings ordered by their spelling, not
//! by their instant. These contracts pin one domain end to end, with and
//! without a secondary index serving the filter.

use std::sync::Arc;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::expr::{Expr, Predicate};
use helix_ast::query::{QueryParamType, QueryRequest, QueryValue};
use helix_ast::traversal::g;
use helix_ast::value::{DateTime, PropertyInput};
use serde_json::json;

use crate::config::SecondaryIndexDefinition;
use crate::query_service::HelixQueryService;
use crate::{HelixDB, HelixDbSource};

async fn open(name: &str) -> Arc<HelixDB> {
    Arc::new(
        HelixDB::open(HelixDbSource::InMemory {
            database: name.to_owned(),
        })
        .await
        .unwrap(),
    )
}

fn datetime(rfc3339: &str) -> DateTime {
    DateTime::parse_rfc3339(rfc3339).unwrap()
}

fn typed(request: QueryRequest, name: &str, rfc3339: &str) -> QueryRequest {
    request
        .with_typed_parameter(
            name,
            QueryParamType::DateTime,
            QueryValue::String(rfc3339.to_owned()),
        )
        .unwrap()
}

/// `name` is written with `at` as a literal when `literal`, else as a typed
/// parameter spelled `at`.
async fn write_event(db: &HelixDB, name: &str, at: &str, literal: bool) {
    let input = if literal {
        PropertyInput::from(datetime(at))
    } else {
        PropertyInput::param("at")
    };
    let request = QueryRequest::write(write_batch().var_as(
        "event",
        g().add_n(
            "Event",
            vec![("name", PropertyInput::from(name)), ("at", input)],
        ),
    ));
    db.query(if literal {
        request
    } else {
        typed(request, "at", at)
    })
    .await
    .unwrap();
}

/// Writes the four events every range contract filters: one literal and three
/// parameters, two of which spell their instant with a fraction or an offset.
async fn seed_events(db: &HelixDB) {
    write_event(db, "literal", "2026-01-01T00:00:00Z", true).await;
    write_event(db, "param", "2026-06-01T00:00:00Z", false).await;
    write_event(db, "half_second_later", "2026-06-01T00:00:00.500Z", false).await;
    // 2026-05-31T20:00:00Z: later than `param` by spelling, earlier by instant.
    write_event(db, "offset_earlier", "2026-06-01T05:00:00+09:00", false).await;
}

fn sorted_names(response: &serde_json::Value) -> Vec<String> {
    let mut names = response["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["name"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

fn after_param(bound: &str) -> QueryRequest {
    typed(
        QueryRequest::read(
            read_batch()
                .var_as(
                    "hits",
                    g().n_with_label_where("Event", Predicate::gt_param("at", "since"))
                        .values(vec!["name"]),
                )
                .returning(["hits"]),
        ),
        "since",
        bound,
    )
}

fn after_literal(bound: &str) -> QueryRequest {
    QueryRequest::read(
        read_batch()
            .var_as(
                "hits",
                g().n_with_label_where("Event", Predicate::gt("at", datetime(bound)))
                    .values(vec!["name"]),
            )
            .returning(["hits"]),
    )
}

async fn assert_range_filters_see_one_domain(db: Arc<HelixDB>, indexed: bool) {
    seed_events(&db).await;
    let service = HelixQueryService::new(Arc::clone(&db));
    let all = ["half_second_later", "literal", "offset_earlier", "param"];
    for request in [
        after_param("2025-01-01T00:00:00Z"),
        after_literal("2025-01-01T00:00:00Z"),
    ] {
        let response = service.execute_query(request).await.unwrap();
        assert_eq!(sorted_names(&response.json()), all, "indexed={indexed}");
        let served_by_index = !response.diagnostics().insights.iter().any(|insight| {
            matches!(
                insight,
                helix_planner::diagnostics::PlannerInsight::MissingIndex(_)
            )
        });
        assert_eq!(served_by_index, indexed);
    }
    // Instants order, not spellings: the fraction sorts after the bound and the
    // +09:00 offset before it, though their strings order the other way.
    for request in [
        after_param("2026-06-01T00:00:00Z"),
        after_literal("2026-06-01T00:00:00Z"),
    ] {
        assert_eq!(
            sorted_names(&db.query(request).await.unwrap()),
            ["half_second_later"],
            "indexed={indexed}"
        );
    }
    // A bound spelled with an offset is the same instant as its UTC spelling.
    assert_eq!(
        sorted_names(
            &db.query(after_param("2026-06-01T09:00:00+09:00"))
                .await
                .unwrap()
        ),
        ["half_second_later"],
    );
}

#[tokio::test]
async fn typed_datetime_params_and_literals_share_one_range_domain_without_an_index() {
    assert_range_filters_see_one_domain(open("datetime-params-scan").await, false).await;
}

#[tokio::test]
async fn typed_datetime_params_and_literals_share_one_range_domain_through_a_range_index() {
    let db = open("datetime-params-range-index").await;
    db.install_index_for_tests(
        SecondaryIndexDefinition::node_range("Event", "at")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    assert_range_filters_see_one_domain(db, true).await;
}

#[tokio::test]
async fn typed_datetime_params_store_canonical_datetimes() {
    let db = open("datetime-params-stored-shape").await;
    seed_events(&db).await;
    let stored = db
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "events",
                    g().n_with_label("Event")
                        .value_map(Some(vec!["name", "at"])),
                )
                .returning(["events"]),
        ))
        .await
        .unwrap();
    let mut events = stored["events"].as_array().unwrap().clone();
    events.sort_by_key(|event| event["name"].as_str().unwrap().to_owned());
    assert_eq!(
        events,
        [
            json!({ "name": "half_second_later", "at": "2026-06-01T00:00:00.500Z" }),
            json!({ "name": "literal", "at": "2026-01-01T00:00:00.000Z" }),
            json!({ "name": "offset_earlier", "at": "2026-05-31T20:00:00.000Z" }),
            json!({ "name": "param", "at": "2026-06-01T00:00:00.000Z" }),
        ]
    );
}

#[tokio::test]
async fn typed_datetime_params_compare_with_now() {
    let db = open("datetime-params-now").await;
    write_event(&db, "past", "2001-01-01T00:00:00Z", false).await;
    write_event(&db, "future", "2999-01-01T00:00:00+02:00", false).await;
    let before_now = db
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "hits",
                    g().n_with_label_where("Event", Predicate::lt("at", Expr::datetime()))
                        .values(vec!["name"]),
                )
                .returning(["hits"]),
        ))
        .await
        .unwrap();
    assert_eq!(sorted_names(&before_now), ["past"]);
}

#[tokio::test]
async fn typed_datetime_params_match_equality_and_enforce_uniqueness_by_instant() {
    let db = open("datetime-params-equality").await;
    db.install_index_for_tests(
        SecondaryIndexDefinition::node_unique_equality("Event", "at")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    write_event(&db, "literal", "2026-01-01T00:00:00Z", true).await;
    let equal = |spelling: &str| {
        typed(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "hits",
                        g().n_with_label_where("Event", Predicate::eq_param("at", "at"))
                            .values(vec!["name"]),
                    )
                    .returning(["hits"]),
            ),
            "at",
            spelling,
        )
    };
    for spelling in [
        "2026-01-01T00:00:00Z",
        "2026-01-01T00:00:00.000Z",
        "2026-01-01T09:00:00+09:00",
    ] {
        assert_eq!(
            sorted_names(&db.query(equal(spelling)).await.unwrap()),
            ["literal"],
            "{spelling}"
        );
    }
    // Another spelling of the stored instant is the same unique key.
    let duplicate = typed(
        QueryRequest::write(write_batch().var_as(
            "event",
            g().add_n(
                "Event",
                vec![
                    ("name", PropertyInput::from("duplicate")),
                    ("at", PropertyInput::param("at")),
                ],
            ),
        )),
        "at",
        "2025-12-31T19:00:00-05:00",
    );
    let error = db
        .query(duplicate)
        .await
        .expect_err("the same instant violates the unique index");
    assert!(
        matches!(error, crate::HelixDbError::UniqueConstraintViolation { .. }),
        "{error:?}"
    );
}

/// `rows`: an array of objects whose `at` and `audit.seen_at` fields are
/// declared `date_time`, as the documented bulk import declares them.
fn row_type() -> QueryParamType {
    let fields = |fields: Vec<(&str, QueryParamType)>| {
        QueryParamType::Record(helix_ast::query::ObjectFields::new(fields).unwrap())
    };
    QueryParamType::Array(Box::new(fields(vec![
        ("name", QueryParamType::String),
        ("at", QueryParamType::DateTime),
        ("audit", fields(vec![("seen_at", QueryParamType::DateTime)])),
    ])))
}

fn import_request() -> QueryRequest {
    QueryRequest::write(write_batch().for_each_param(
        "rows",
        write_batch().var_as(
            "event",
            g().add_n(
                "Event",
                vec![
                    ("name", PropertyInput::param("name")),
                    ("at", PropertyInput::param("at")),
                    ("audit", PropertyInput::param("audit")),
                ],
            ),
        ),
    ))
}

/// The bulk-import JSON body for `rows`, declared with `parameter_types`.
fn import_json(rows: serde_json::Value, parameter_types: serde_json::Value) -> Vec<u8> {
    let mut request = serde_json::to_value(import_request()).unwrap();
    request["parameters"] = json!({ "rows": rows });
    request["parameter_types"] = json!({ "rows": parameter_types });
    serde_json::to_vec(&request).unwrap()
}

fn import_row(name: &str, at: &str) -> serde_json::Value {
    json!({ "name": name, "at": at, "audit": { "seen_at": "2026-06-01T09:00:00+09:00" } })
}

/// The same four instants `seed_events` writes, imported in one request
/// through the wire format every SDK sends.
async fn import_events(db: &HelixDB) {
    let rows = json!([
        import_row("literal", "2026-01-01T00:00:00Z"),
        import_row("param", "2026-06-01T00:00:00Z"),
        import_row("half_second_later", "2026-06-01T00:00:00.500Z"),
        import_row("offset_earlier", "2026-06-01T05:00:00+09:00"),
    ]);
    db.query_json(&import_json(
        rows,
        serde_json::to_value(row_type()).unwrap(),
    ))
    .await
    .unwrap();
}

async fn event_count(db: &HelixDB) -> serde_json::Value {
    db.query(QueryRequest::read(
        read_batch()
            .var_as("count", g().n_with_label("Event").count())
            .returning(["count"]),
    ))
    .await
    .unwrap()["count"]
        .clone()
}

async fn assert_imported_rows_share_the_datetime_domain(db: Arc<HelixDB>, indexed: bool) {
    import_events(&db).await;
    let service = HelixQueryService::new(Arc::clone(&db));
    for request in [
        after_param("2025-01-01T00:00:00Z"),
        after_literal("2025-01-01T00:00:00Z"),
    ] {
        let response = service.execute_query(request).await.unwrap();
        assert_eq!(
            sorted_names(&response.json()),
            ["half_second_later", "literal", "offset_earlier", "param"],
            "indexed={indexed}"
        );
        let served_by_index = !response.diagnostics().insights.iter().any(|insight| {
            matches!(
                insight,
                helix_planner::diagnostics::PlannerInsight::MissingIndex(_)
            )
        });
        assert_eq!(served_by_index, indexed);
    }
    // Instants order, not spellings, exactly as for top-level parameters.
    for request in [
        after_param("2026-06-01T00:00:00Z"),
        after_literal("2026-06-01T00:00:00Z"),
    ] {
        assert_eq!(
            sorted_names(&db.query(request).await.unwrap()),
            ["half_second_later"],
            "indexed={indexed}"
        );
    }
}

#[tokio::test]
async fn foreach_rows_with_typed_datetime_fields_match_range_filters_without_an_index() {
    assert_imported_rows_share_the_datetime_domain(open("datetime-rows-scan").await, false).await;
}

#[tokio::test]
async fn foreach_rows_with_typed_datetime_fields_match_range_filters_through_a_range_index() {
    let db = open("datetime-rows-range-index").await;
    db.install_index_for_tests(
        SecondaryIndexDefinition::node_range("Event", "at")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    assert_imported_rows_share_the_datetime_domain(db, true).await;
}

#[tokio::test]
async fn foreach_rows_store_typed_datetime_fields_canonically_at_every_level() {
    let db = open("datetime-rows-stored-shape").await;
    import_events(&db).await;
    // A row built in process, with no JSON in between, stores the same values.
    db.query(
        import_request()
            .with_typed_parameter(
                "rows",
                row_type(),
                QueryValue::Array(vec![serde_json::from_value(import_row(
                    "in_process",
                    "2026-01-02T00:00:00Z",
                ))
                .unwrap()]),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    let stored = db
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "events",
                    g().n_with_label("Event")
                        .value_map(Some(vec!["name", "at", "audit"])),
                )
                .returning(["events"]),
        ))
        .await
        .unwrap();
    let mut events = stored["events"].as_array().unwrap().clone();
    events.sort_by_key(|event| event["name"].as_str().unwrap().to_owned());
    // The nested field renders as a datetime, so it was not kept as the
    // `+09:00` text it was sent as.
    let audit = json!({ "seen_at": "2026-06-01T00:00:00.000Z" });
    assert_eq!(
        events,
        [
            json!({ "name": "half_second_later", "at": "2026-06-01T00:00:00.500Z", "audit": audit }),
            json!({ "name": "in_process", "at": "2026-01-02T00:00:00.000Z", "audit": audit }),
            json!({ "name": "literal", "at": "2026-01-01T00:00:00.000Z", "audit": audit }),
            json!({ "name": "offset_earlier", "at": "2026-05-31T20:00:00.000Z", "audit": audit }),
            json!({ "name": "param", "at": "2026-06-01T00:00:00.000Z", "audit": audit }),
        ]
    );
}

#[tokio::test]
async fn foreach_rows_with_invalid_datetime_or_mismatched_fields_write_nothing() {
    let db = open("datetime-rows-rejected").await;
    let declared = serde_json::to_value(row_type()).unwrap();
    let valid = import_row("valid", "2026-01-01T00:00:00Z");
    let with = |field: &str, value: serde_json::Value| {
        let mut row = import_row("invalid", "2026-01-01T00:00:00Z");
        row[field] = value;
        row
    };
    let without = |field: &str| {
        let mut row = import_row("invalid", "2026-01-01T00:00:00Z");
        row.as_object_mut().unwrap().remove(field);
        row
    };
    for (row, expected) in [
        // An invalid RFC3339 string is an error, never a stored string.
        (
            with("at", json!("2026-01-01")),
            "'rows[1].at' expected date_time",
        ),
        (
            with("at", json!("yesterday")),
            "'rows[1].at' expected date_time",
        ),
        (with("at", json!(null)), "'rows[1].at' expected date_time"),
        (
            with("at", json!(1_767_225_600_000_i64)),
            "'rows[1].at' expected date_time",
        ),
        (
            with("audit", json!({ "seen_at": "not a datetime" })),
            "'rows[1].audit.seen_at' expected date_time",
        ),
        (without("at"), r#"missing: [\"at\"]"#),
        (
            with("audit", json!({})),
            r#"'rows[1].audit' fields do not match"#,
        ),
        // A misspelled field is rejected rather than stored untyped.
        (
            with("createdAt", json!("2026-01-01T00:00:00Z")),
            r#"undeclared: [\"createdAt\"]"#,
        ),
        (
            json!("not an object"),
            "'rows[1]' expected object with declared fields",
        ),
    ] {
        let error = db
            .query_json(&import_json(json!([valid, row]), declared.clone()))
            .await
            .expect_err("an invalid row rejects the whole request")
            .to_string();
        assert!(error.contains(expected), "{expected}: {error}");
    }
    assert_eq!(event_count(&db).await, json!(0));

    // A declaration without fields is rejected before any row is read.
    let error = db
        .query_json(&import_json(
            json!([valid]),
            json!({ "array": { "object": {} } }),
        ))
        .await
        .expect_err("an empty field declaration is invalid")
        .to_string();
    assert!(error.contains("at least one field"), "{error}");
    assert_eq!(event_count(&db).await, json!(0));
}

/// Rows declared as a bare `object` keep RFC3339 text as strings: only a
/// declared `date_time` field becomes a datetime.
#[tokio::test]
async fn foreach_rows_declared_as_untyped_objects_keep_rfc3339_strings() {
    let db = open("datetime-rows-untyped").await;
    db.query_json(&import_json(
        json!([import_row("untyped", "2026-06-01T05:00:00+09:00")]),
        json!({ "array": "object" }),
    ))
    .await
    .unwrap();
    let stored = db
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "events",
                    g().n_with_label("Event").value_map(Some(vec!["at"])),
                )
                .returning(["events"]),
        ))
        .await
        .unwrap();
    assert_eq!(
        stored["events"],
        json!([{ "at": "2026-06-01T05:00:00+09:00" }])
    );
}

/// A nullable `date_time` field, such as an optional timestamp column: rows
/// where it is set store datetimes that typed and literal range filters
/// match, and rows where it is `null` store `null` and match neither.
#[tokio::test]
async fn foreach_rows_with_nullable_datetime_fields_store_datetimes_or_null() {
    let db = open("datetime-rows-nullable").await;
    db.query_json(&import_json(
        json!([
            unset_row("unset_first"),
            import_row("param", "2026-06-01T00:00:00Z"),
            import_row("offset_earlier", "2026-06-01T05:00:00+09:00"),
            unset_row("unset_last"),
        ]),
        nullable_row_type(),
    ))
    .await
    .unwrap();
    for request in [
        after_param("2025-01-01T00:00:00Z"),
        after_literal("2025-01-01T00:00:00Z"),
    ] {
        assert_eq!(
            sorted_names(&db.query(request).await.unwrap()),
            ["offset_earlier", "param"]
        );
    }
    assert_eq!(
        sorted_names(&db.query(after_param("2026-05-31T23:00:00Z")).await.unwrap()),
        ["param"]
    );
    let stored = db
        .query(QueryRequest::read(
            read_batch()
                .var_as(
                    "events",
                    g().n_with_label("Event")
                        .value_map(Some(vec!["name", "at", "audit"])),
                )
                .returning(["events"]),
        ))
        .await
        .unwrap();
    let mut events = stored["events"].as_array().unwrap().clone();
    events.sort_by_key(|event| event["name"].as_str().unwrap().to_owned());
    let audit = json!({ "seen_at": "2026-06-01T00:00:00.000Z" });
    let null_audit = json!({ "seen_at": null });
    assert_eq!(
        events,
        [
            json!({ "name": "offset_earlier", "at": "2026-05-31T20:00:00.000Z", "audit": audit }),
            json!({ "name": "param", "at": "2026-06-01T00:00:00.000Z", "audit": audit }),
            json!({ "name": "unset_first", "at": null, "audit": null_audit }),
            json!({ "name": "unset_last", "at": null, "audit": null_audit }),
        ]
    );

    // A set value must still be a valid datetime, and the field present.
    let db = open("datetime-rows-nullable-rejected").await;
    for (row, expected) in [
        (
            json!({ "name": "bad", "at": "2026-06-01", "audit": { "seen_at": null } }),
            "'rows[0].at' expected date_time",
        ),
        (
            json!({ "name": "bad", "audit": { "seen_at": null } }),
            r#"missing: [\"at\"]"#,
        ),
    ] {
        let error = db
            .query_json(&import_json(json!([row]), nullable_row_type()))
            .await
            .expect_err("an invalid row rejects the whole request")
            .to_string();
        assert!(error.contains(expected), "{expected}: {error}");
    }
    assert_eq!(event_count(&db).await, json!(0));
}

/// Rows with every nullable datetime set match through a range index. A
/// range index rejects any `null` value, however it was declared, so a
/// `null` row rejects the whole import.
#[tokio::test]
async fn foreach_rows_with_nullable_datetime_fields_and_a_range_index() {
    let db = open("datetime-rows-nullable-range-index").await;
    db.install_index_for_tests(
        SecondaryIndexDefinition::node_range("Event", "at")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    for declared in [nullable_row_type(), json!({ "array": "object" })] {
        let error = db
            .query_json(&import_json(
                json!([
                    import_row("param", "2026-06-01T00:00:00Z"),
                    unset_row("unset")
                ]),
                declared,
            ))
            .await
            .expect_err("a range index rejects null")
            .to_string();
        assert!(error.contains("unsupported range value type Null"), "{error}");
    }
    assert_eq!(event_count(&db).await, json!(0));

    db.query_json(&import_json(
        json!([
            import_row("param", "2026-06-01T00:00:00Z"),
            import_row("offset_earlier", "2026-06-01T05:00:00+09:00"),
        ]),
        nullable_row_type(),
    ))
    .await
    .unwrap();
    let service = HelixQueryService::new(Arc::clone(&db));
    for request in [
        after_param("2026-05-31T23:00:00Z"),
        after_literal("2026-05-31T23:00:00Z"),
    ] {
        let response = service.execute_query(request).await.unwrap();
        assert_eq!(sorted_names(&response.json()), ["param"]);
        assert!(!response
            .diagnostics()
            .insights
            .iter()
            .any(|insight| matches!(
                insight,
                helix_planner::diagnostics::PlannerInsight::MissingIndex(_)
            )));
    }
}

/// `rows` with `at` and `audit.seen_at` declared `{ "nullable": "date_time" }`.
fn nullable_row_type() -> serde_json::Value {
    json!({ "array": { "object": {
        "name": "string",
        "at": { "nullable": "date_time" },
        "audit": { "object": { "seen_at": { "nullable": "date_time" } } },
    } } })
}

fn unset_row(name: &str) -> serde_json::Value {
    json!({ "name": name, "at": null, "audit": { "seen_at": null } })
}
