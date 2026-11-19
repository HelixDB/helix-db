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
