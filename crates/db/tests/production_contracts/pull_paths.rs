//! Demand-driven (pull) execution contracts reached through public requests.
//!
//! A window (`limit`, `range`), a terminal `count` or an `exists` turns an
//! exclusive producer chain into a pull region. These tests pin what such
//! regions return for the less common operators they can contain (pure
//! branches, `repeat`, variable injection and membership, index sources and
//! set merges) and the typed errors they report, so a planner change that
//! moves a native query in or out of a region cannot change its result.

use std::collections::BTreeSet;

use db::execution::interpreter::{ExecutionScalar, ExecutionValue};
use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::batch::{self, BatchCondition};
use helix_ast::error_code::QueryErrorCode;
use helix_ast::expr::{CompareOp, Expr, Predicate, StreamBound};
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::index::IndexSpec;
use helix_ast::query::{QueryRequest, QueryValue};
use helix_ast::traversal::{self, Order, RepeatConfig};
use helix_ast::value::{PropertyInput, PropertyValue};
use helix_planner::{context, ir, planning};

/// Seeds `alice(0) -KNOWS-> bob(1) -KNOWS-> carol(2)`, `alice -LIKES-> carol`
/// and an isolated `Robot(3)`. Edge IDs follow creation order.
async fn seed_people(db: &HelixDB) {
    let person = |name: &str, rank: i64| {
        traversal::g().add_n(
            "Person",
            vec![
                ("name", PropertyInput::from(name)),
                ("rank", PropertyInput::from(rank)),
            ],
        )
    };
    let link = |from: &str, label: &str, to: &str| {
        traversal::g().n(NodeRef::var(from)).add_e(
            label,
            NodeRef::var(to),
            Vec::<(&str, PropertyInput)>::new(),
        )
    };
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as("alice", person("alice", 1))
            .var_as("bob", person("bob", 2))
            .var_as("carol", person("carol", 3))
            .var_as(
                "robot",
                traversal::g().add_n("Robot", vec![("name", PropertyInput::from("robot"))]),
            )
            .var_as("alice_knows_bob", link("alice", "KNOWS", "bob"))
            .var_as("bob_knows_carol", link("bob", "KNOWS", "carol"))
            .var_as("alice_likes_carol", link("alice", "LIKES", "carol"))
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("people fixture is committed");
}

/// Seeds three `Document` nodes and three `LINK` edges, then activates node
/// equality (`category`, `bucket`), unique (`code`) and ascending range
/// (`rank`) indexes plus edge equality (`kind`) and descending range
/// (`weight`) indexes.
///
/// | node | category | code | rank | bucket |
/// |------|----------|------|------|--------|
/// | 0    | group    | A    | 1    | 10     |
/// | 1    | group    | B    | 2    | 20     |
/// | 2    | other    | C    | 3    | 10     |
///
/// Edges: `0: 0->1 primary/1`, `1: 1->2 secondary/2`, `2: 0->2 primary/3`.
async fn seed_documents(db: &HelixDB) {
    let document = |category: &str, code: &str, rank: i64, bucket: i64| {
        traversal::g().add_n(
            "Document",
            vec![
                ("category", PropertyInput::from(category)),
                ("code", PropertyInput::from(code)),
                ("rank", PropertyInput::from(rank)),
                ("bucket", PropertyInput::from(bucket)),
            ],
        )
    };
    let link = |from: &str, to: &str, kind: &str, weight: i64| {
        traversal::g().n(NodeRef::var(from)).add_e(
            "LINK",
            NodeRef::var(to),
            vec![
                ("kind", PropertyInput::from(kind)),
                ("weight", PropertyInput::from(weight)),
            ],
        )
    };
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as("d0", document("group", "A", 1, 10))
            .var_as("d1", document("group", "B", 2, 20))
            .var_as("d2", document("other", "C", 3, 10))
            .var_as("l0", link("d0", "d1", "primary", 1))
            .var_as("l1", link("d1", "d2", "secondary", 2))
            .var_as("l2", link("d0", "d2", "primary", 3))
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("document fixture is committed");
    for spec in [
        IndexSpec::node_equality("Document", "category"),
        IndexSpec::node_equality("Document", "bucket"),
        IndexSpec::node_unique_equality("Document", "code"),
        IndexSpec::node_range("Document", "rank"),
        IndexSpec::edge_equality("LINK", "kind"),
        IndexSpec::edge_range_desc("LINK", "weight"),
    ] {
        let receipt = db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as("operation", traversal::g().create_index_if_not_exists(spec))
                    .returning(["operation"]),
            ))
            .await
            .expect("document index is accepted");
        let Some(operation_id) = receipt["operation"]["operation_id"].as_str() else {
            panic!("accepted document index has an operation ID: {receipt}");
        };
        super::await_index_operation_success(db, operation_id, "document index").await;
    }
}

/// Pure union, coalesce and optional branches inside a window stop after the
/// demanded rows, keep branch order per parent, and feed terminal counts and
/// `exists` through the same resumable cursor.
#[tokio::test]
async fn bounded_pure_branches_emit_in_parent_then_branch_order() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-bounded-branches".to_owned(),
    })
    .await
    .expect("branch fixture opens");
    seed_people(&db).await;
    let knows = || traversal::sub().out(Some("KNOWS"));
    let known_by = || traversal::sub().in_(Some("KNOWS"));
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "union_first",
                    traversal::g()
                        .n(NodeRef::id(1))
                        .union(vec![knows(), known_by()])
                        .limit(1_usize)
                        .id(),
                )
                .var_as(
                    "union_window",
                    traversal::g()
                        .n(NodeRef::all())
                        .union(vec![knows(), known_by()])
                        .limit(3_usize)
                        .id(),
                )
                .var_as(
                    "coalesce_window",
                    traversal::g()
                        .n(NodeRef::all())
                        .coalesce(vec![traversal::sub().out(Some("MISSING")), knows()])
                        .limit(2_usize)
                        .id(),
                )
                .var_as(
                    "optional_window",
                    traversal::g()
                        .n(NodeRef::all())
                        .optional(knows())
                        .limit(4_usize)
                        .id(),
                )
                .var_as(
                    "union_count",
                    traversal::g()
                        .n(NodeRef::all())
                        .union(vec![knows(), known_by()])
                        .count(),
                )
                .var_as(
                    "isolated_union_exists",
                    traversal::g()
                        .n(NodeRef::id(3))
                        .union(vec![knows(), known_by()])
                        .exists(),
                )
                .var_as(
                    "optional_filtered_count",
                    traversal::g()
                        .n(NodeRef::all())
                        .optional(knows())
                        .where_(Predicate::eq("name", "carol"))
                        .count(),
                )
                .var_as(
                    "unbound_mixed_union",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .union(vec![traversal::sub().out_e(Some("KNOWS")), knows()])
                        .limit(3_usize)
                        .id(),
                )
                .returning([
                    "union_first",
                    "union_window",
                    "coalesce_window",
                    "optional_window",
                    "union_count",
                    "isolated_union_exists",
                    "optional_filtered_count",
                    "unbound_mixed_union",
                ]),
        ))
        .await
        .expect("bounded branch read succeeds");
    assert_eq!(
        response,
        serde_json::json!({
            "union_first": [2],
            "union_window": [1, 2, 0],
            "coalesce_window": [1, 2],
            "optional_window": [1, 2, 2, 3],
            "union_count": 4,
            "isolated_union_exists": false,
            "optional_filtered_count": 2,
            "unbound_mixed_union": [0, 1],
        })
    );

    // Rows carrying bindings must keep one element kind across union
    // branches, whether the region ends in a window or a count.
    for traversal in [
        traversal::g()
            .n(NodeRef::id(0))
            .bind("start")
            .union(vec![traversal::sub().out_e(Some("KNOWS")), knows()])
            .limit(3_usize)
            .count(),
        traversal::g()
            .n(NodeRef::id(0))
            .bind("start")
            .union(vec![traversal::sub().out_e(Some("KNOWS")), knows()])
            .count(),
    ] {
        let error = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as("mixed", traversal)
                    .returning(["mixed"]),
            ))
            .await
            .expect_err("bound rows cannot mix edges and nodes");
        assert_eq!(error.error_code(), QueryErrorCode::InvalidQuery);
        assert_eq!(
            error.to_string(),
            "Query error: union row branches produced mixed current element types"
        );
    }
    db.close().await.expect("branch fixture closes");
}

/// A pure `repeat` inside a window emits each policy's frontier in depth
/// order, stops once the window is full, and a terminal count sees exactly
/// the emitted rows.
#[tokio::test]
async fn bounded_repeat_emits_each_policy_in_depth_order() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-bounded-repeat".to_owned(),
    })
    .await
    .expect("repeat fixture opens");
    seed_people(&db).await;
    let knows = || traversal::sub().out(Some("KNOWS"));
    for (policy, config, expected) in [
        (
            "emit_all",
            RepeatConfig::new(knows()).times(3).emit_all(),
            vec![0, 1, 1, 2, 2],
        ),
        (
            "emit_before",
            RepeatConfig::new(knows()).times(3).emit_before(),
            vec![0, 1, 2],
        ),
        (
            "emit_after",
            RepeatConfig::new(knows()).times(3).emit_after(),
            vec![1, 2],
        ),
        (
            "emit_if",
            RepeatConfig::new(knows())
                .times(3)
                .emit_if(Predicate::eq("name", "carol")),
            vec![2],
        ),
        (
            "until",
            RepeatConfig::new(knows()).until(Predicate::eq("name", "carol")),
            vec![2],
        ),
        ("times", RepeatConfig::new(knows()).times(2), vec![2]),
    ] {
        let response = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "ids",
                        traversal::g()
                            .n(NodeRef::id(0))
                            .repeat(config.clone())
                            .limit(10_usize)
                            .id(),
                    )
                    .var_as(
                        "count",
                        traversal::g().n(NodeRef::id(0)).repeat(config).count(),
                    )
                    .returning(["ids", "count"]),
            ))
            .await
            .unwrap_or_else(|error| panic!("{policy} repeat read fails: {error}"));
        assert_eq!(
            response,
            serde_json::json!({ "ids": expected, "count": expected.len() }),
            "{policy}"
        );
    }
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "first_two",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .repeat(RepeatConfig::new(knows()).times(3).emit_all())
                        .limit(2_usize)
                        .id(),
                )
                .returning(["first_two"]),
        ))
        .await
        .expect("early-stopped repeat succeeds");
    assert_eq!(response, serde_json::json!({ "first_two": [0, 1] }));
    db.close().await.expect("repeat fixture closes");
}

/// `inject`, `within`, `without` and `select` read earlier batch or step
/// variables inside a window: injected rows follow the input, and membership
/// accepts row and folded variables.
#[tokio::test]
async fn bounded_variable_operators_read_batch_variables() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-bounded-variables".to_owned(),
    })
    .await
    .expect("variable fixture opens");
    seed_people(&db).await;
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("seed", traversal::g().n(NodeRef::id(2)))
                .var_as(
                    "friends",
                    traversal::g().n(NodeRef::id(0)).out(Some("KNOWS")),
                )
                .var_as("folded_bob", traversal::g().n(NodeRef::id(1)).fold())
                .var_as(
                    "injected",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .inject("seed")
                        .limit(5_usize)
                        .id(),
                )
                .var_as(
                    "within",
                    traversal::g()
                        .n(NodeRef::all())
                        .within("friends")
                        .limit(5_usize)
                        .id(),
                )
                .var_as(
                    "without_count",
                    traversal::g().n(NodeRef::all()).without("friends").count(),
                )
                .var_as(
                    "within_folded",
                    traversal::g()
                        .n(NodeRef::all())
                        .within("folded_bob")
                        .limit(2_usize)
                        .id(),
                )
                .var_as(
                    "selected",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .as_("start")
                        .out(Some("KNOWS"))
                        .select("start")
                        .limit(1_usize)
                        .id(),
                )
                .returning([
                    "injected",
                    "within",
                    "without_count",
                    "within_folded",
                    "selected",
                ]),
        ))
        .await
        .expect("variable read succeeds");
    assert_eq!(
        response,
        serde_json::json!({
            "injected": [0, 2],
            "within": [1],
            "without_count": 3,
            "within_folded": [1],
            "selected": [0],
        })
    );
    db.close().await.expect("variable fixture closes");
}

/// Injection, membership and variable sources reject variables that are not
/// element streams with a typed query error naming the offending shape.
#[tokio::test]
async fn bounded_variable_operators_reject_non_stream_variables() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-variable-shapes".to_owned(),
    })
    .await
    .expect("variable-shape fixture opens");
    seed_people(&db).await;
    let inject = || {
        traversal::g()
            .n(NodeRef::id(0))
            .inject("v")
            .limit(2_usize)
            .id()
    };
    let within = || {
        traversal::g()
            .n(NodeRef::all())
            .within("v")
            .limit(2_usize)
            .id()
    };
    for (variable, consumer, expected) in [
        (
            traversal::g().n(NodeRef::all()).count(),
            inject(),
            "inject expected stream input, got count",
        ),
        (
            traversal::g().n(NodeRef::all()).count(),
            within(),
            "membership operand expected stream input, got count",
        ),
        (
            traversal::g().n(NodeRef::all()).exists(),
            inject(),
            "inject expected stream input, got boolean",
        ),
        (
            traversal::g().n(NodeRef::all()).exists(),
            within(),
            "membership operand expected stream input, got boolean",
        ),
        (
            traversal::g().n(NodeRef::all()).id(),
            inject(),
            "inject expected stream input, got scalar items",
        ),
        (
            traversal::g().n(NodeRef::all()).id(),
            within(),
            "membership operand expected stream input, got scalar items",
        ),
    ] {
        let error = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as("v", variable)
                    .var_as("r", consumer)
                    .returning(["r"]),
            ))
            .await
            .expect_err("a non-stream variable cannot feed a row operator");
        assert_eq!(
            error.error_code(),
            QueryErrorCode::InvalidQuery,
            "{expected}"
        );
        assert_eq!(error.to_string(), format!("Query error: {expected}"));
    }
    let error = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("v", traversal::g().n(NodeRef::all()).fold())
                .var_as("r", inject())
                .returning(["r"]),
        ))
        .await
        .expect_err("a folded variable must be unfolded before injection");
    assert_eq!(error.error_code(), QueryErrorCode::InvalidQuery);
    assert_eq!(
        error.to_string(),
        "Query error: inject expected stream input, got folded stream; use unfold first"
    );
    let error = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("c", traversal::g().n(NodeRef::all()).count())
                .var_as("r", traversal::g().n(NodeRef::var("c")).limit(1_usize).id())
                .returning(["r"]),
        ))
        .await
        .expect_err("a count variable is not a node source");
    assert_eq!(error.error_code(), QueryErrorCode::InvalidQuery);
    assert_eq!(
        error.to_string(),
        "Query error: variable `c` is not a node stream: Count(4)"
    );
    db.close().await.expect("variable-shape fixture closes");
}

/// Windows over folded streams fail with a typed error naming the operator,
/// windows after a capture boundary slice the captured rows eagerly, and
/// `unfold` restores a folded stream's rows for grouping.
#[tokio::test]
async fn windows_reject_folded_streams_and_slice_captured_rows() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-window-shapes".to_owned(),
    })
    .await
    .expect("window fixture opens");
    seed_people(&db).await;
    for (operator, traversal) in [
        (
            "skip",
            traversal::g().n(NodeRef::all()).fold().skip(1_usize).id(),
        ),
        (
            "range",
            traversal::g()
                .n(NodeRef::all())
                .fold()
                .range(0_usize, 1_usize)
                .id(),
        ),
        (
            "limit",
            traversal::g().n(NodeRef::all()).fold().limit(1_usize).id(),
        ),
        (
            "distinct",
            traversal::g()
                .n(NodeRef::all())
                .fold()
                .dedup()
                .limit(1_usize)
                .id(),
        ),
        ("project", traversal::g().n(NodeRef::all()).fold().exists()),
    ] {
        let error = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as("folded", traversal)
                    .returning(["folded"]),
            ))
            .await
            .expect_err("a folded stream cannot be windowed");
        assert_eq!(
            error.error_code(),
            QueryErrorCode::InvalidQuery,
            "{operator}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "Query error: {operator} expected stream input, got folded stream; use unfold first"
            )
        );
    }
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "captured_range",
                    traversal::g()
                        .n(NodeRef::all())
                        .as_("captured")
                        .range(1_usize, 3_usize),
                )
                .var_as(
                    "captured_skip",
                    traversal::g()
                        .n(NodeRef::all())
                        .as_("captured")
                        .skip(2_usize),
                )
                .var_as(
                    "expanded_skip",
                    traversal::g()
                        .n(NodeRef::all())
                        .out(Some("KNOWS"))
                        .skip(1_usize),
                )
                .var_as(
                    "stored_range",
                    traversal::g()
                        .n(NodeRef::all())
                        .store("stored")
                        .range(1_usize, 3_usize)
                        .id(),
                )
                .var_as(
                    "skip_then_limit",
                    traversal::g()
                        .n(NodeRef::all())
                        .skip(1_usize)
                        .limit(2_usize)
                        .id(),
                )
                .var_as(
                    "unfolded_groups",
                    traversal::g()
                        .n(NodeRef::all())
                        .fold()
                        .unfold()
                        .group("name"),
                )
                .returning([
                    "captured_range",
                    "captured_skip",
                    "expanded_skip",
                    "stored_range",
                    "skip_then_limit",
                    "unfolded_groups",
                ]),
        ))
        .await
        .expect("captured windows succeed");
    assert_eq!(
        response,
        serde_json::json!({
            "captured_range": [{ "$id": 1 }, { "$id": 2 }],
            "captured_skip": [{ "$id": 2 }, { "$id": 3 }],
            "expanded_skip": [{ "$id": 2 }],
            "stored_range": [1, 2],
            "skip_then_limit": [1, 2],
            "unfolded_groups": [
                { "count": 1, "ids": [0], "name": "alice" },
                { "count": 1, "ids": [1], "name": "bob" },
                { "count": 1, "ids": [2], "name": "carol" },
                { "count": 1, "ids": [3], "name": "robot" },
            ],
        })
    );
    db.close().await.expect("window fixture closes");
}

/// Bounded expansions read edges in edge-ID order for every direction and
/// label filter, and runtime `limit` parameters bound the window, including
/// a zero bound that reads nothing.
#[tokio::test]
async fn bounded_expansions_and_runtime_limits_follow_edge_order() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-bounded-expansions".to_owned(),
    })
    .await
    .expect("expansion fixture opens");
    seed_people(&db).await;
    let response = db
        .query(
            QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "labeled_out_edges",
                        traversal::g()
                            .n(NodeRef::id(0))
                            .out_e(Some("KNOWS"))
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "any_out_edges",
                        traversal::g()
                            .n(NodeRef::id(0))
                            .out_e(None::<&str>)
                            .limit(5_usize)
                            .id(),
                    )
                    .var_as(
                        "any_in_edges",
                        traversal::g()
                            .n(NodeRef::id(2))
                            .in_e(None::<&str>)
                            .limit(5_usize)
                            .id(),
                    )
                    .var_as(
                        "both_neighbours",
                        traversal::g()
                            .n(NodeRef::id(1))
                            .both(None::<&str>)
                            .limit(5_usize)
                            .id(),
                    )
                    .var_as(
                        "runtime_limit",
                        traversal::g()
                            .n(NodeRef::all())
                            .limit(StreamBound::expr(Expr::param("two")))
                            .id(),
                    )
                    .var_as(
                        "zero_runtime_limit",
                        traversal::g()
                            .n(NodeRef::all())
                            .limit(StreamBound::expr(Expr::param("zero")))
                            .id(),
                    )
                    .var_as(
                        "parameter_ids_window",
                        traversal::g().n(NodeRef::param("ids")).limit(5_usize).id(),
                    )
                    .returning([
                        "labeled_out_edges",
                        "any_out_edges",
                        "any_in_edges",
                        "both_neighbours",
                        "runtime_limit",
                        "zero_runtime_limit",
                        "parameter_ids_window",
                    ]),
            )
            .with_parameter_value("two", QueryValue::I64(2))
            .with_parameter_value("zero", QueryValue::I64(0))
            .with_parameter_value(
                "ids",
                QueryValue::Array(vec![
                    QueryValue::I64(99),
                    QueryValue::I64(0),
                    QueryValue::I64(1),
                ]),
            ),
        )
        .await
        .expect("bounded expansion read succeeds");
    // A missing parameter ID is skipped; the window covers every requested ID.
    assert_eq!(
        response,
        serde_json::json!({
            "labeled_out_edges": [0],
            "any_out_edges": [0, 2],
            "any_in_edges": [1, 2],
            "both_neighbours": [0, 2],
            "runtime_limit": [0, 1],
            "zero_runtime_limit": [],
            "parameter_ids_window": [0, 1],
        })
    );
    db.close().await.expect("expansion fixture closes");
}

/// Batch conditions skip a gated region or step without reading it: skipped
/// collections return `[]`, skipped at-most-one values return `null`, and a
/// `PrevNotEmpty` step runs only after a non-empty previous entry.
#[tokio::test]
async fn batch_conditions_skip_regions_and_steps() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-batch-conditions".to_owned(),
    })
    .await
    .expect("condition fixture opens");
    seed_people(&db).await;
    let nobody = || traversal::g().n(NodeRef::id(3)).out(Some("KNOWS"));
    let skipped = || BatchCondition::VarNotEmpty("nobody".to_owned());
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("nobody", nobody())
                .var_as_if(
                    "skipped_region",
                    skipped(),
                    traversal::g()
                        .n(NodeRef::all())
                        .out(Some("KNOWS"))
                        .limit(1_usize)
                        .id(),
                )
                .var_as_if(
                    "skipped_window",
                    skipped(),
                    traversal::g().n(NodeRef::all()).limit(2_usize).id(),
                )
                .var_as_if(
                    "skipped_list",
                    skipped(),
                    traversal::g().n(NodeRef::all()).id(),
                )
                .var_as_if(
                    "skipped_single",
                    skipped(),
                    traversal::g().n(NodeRef::id(0)).id(),
                )
                .returning([
                    "skipped_region",
                    "skipped_window",
                    "skipped_list",
                    "skipped_single",
                ]),
        ))
        .await
        .expect("skipped conditional read succeeds");
    assert_eq!(
        response,
        serde_json::json!({
            "skipped_region": null,
            "skipped_window": [],
            "skipped_list": [],
            "skipped_single": null,
        })
    );
    for (previous, expected) in [
        (
            traversal::g().n(NodeRef::id(0)).out(Some("KNOWS")),
            serde_json::json!({ "previous": [{ "$id": 1 }], "gated": [0, 1, 2, 3] }),
        ),
        (nobody(), serde_json::json!({ "previous": [], "gated": [] })),
    ] {
        let response = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as("previous", previous)
                    .var_as_if(
                        "gated",
                        BatchCondition::PrevNotEmpty,
                        traversal::g().n(NodeRef::all()).id(),
                    )
                    .returning(["previous", "gated"]),
            ))
            .await
            .expect("previous-step condition read succeeds");
        assert_eq!(response, expected);
    }
    db.close().await.expect("condition fixture closes");
}

/// Float-array membership compares numerically against stored integers, and
/// integer negation or remainder overflow fails the request with a typed
/// query error instead of wrapping.
#[tokio::test]
async fn predicate_values_cover_float_membership_and_integer_overflow() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-predicate-values".to_owned(),
    })
    .await
    .expect("predicate fixture opens");
    seed_people(&db).await;
    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "f64_members",
                    traversal::g()
                        .n(NodeRef::all())
                        .where_(Predicate::is_in(
                            "rank",
                            PropertyValue::F64Array(vec![1.0, 3.0]),
                        ))
                        .id(),
                )
                .var_as(
                    "f32_members",
                    traversal::g()
                        .n(NodeRef::all())
                        .where_(Predicate::is_in("rank", PropertyValue::F32Array(vec![2.0])))
                        .id(),
                )
                .returning(["f64_members", "f32_members"]),
        ))
        .await
        .expect("float membership read succeeds");
    assert_eq!(
        response,
        serde_json::json!({ "f64_members": [0, 2], "f32_members": [1] })
    );
    for (left, expected) in [
        (
            Expr::val(i64::MIN).neg(),
            "Query error: neg expression overflows i64",
        ),
        (
            Expr::prop("rank").modulo(Expr::val(0_i64)),
            "Query error: mod expression has zero divisor or overflows i64",
        ),
    ] {
        let error = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "overflow",
                        traversal::g()
                            .n(NodeRef::all())
                            .where_(Predicate::compare(left, CompareOp::Gt, Expr::val(0_i64)))
                            .id(),
                    )
                    .returning(["overflow"]),
            ))
            .await
            .expect_err("integer overflow fails the request");
        assert_eq!(error.error_code(), QueryErrorCode::InvalidQuery);
        assert_eq!(error.to_string(), expected);
    }
    db.close().await.expect("predicate fixture closes");
}

/// Index-backed windows return index order: reverse range reads keep the top
/// rows, ordered intersections filter the range driver by equality
/// membership (and stop on an empty filter), and runtime limits bound them.
#[tokio::test]
async fn indexed_windows_follow_range_order_and_equality_membership() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-indexed-windows".to_owned(),
    })
    .await
    .expect("indexed-window fixture opens");
    seed_documents(&db).await;
    let ranked = |category: &str| {
        traversal::g().n_with_label_where(
            "Document",
            Predicate::and(vec![
                Predicate::eq("category", category),
                Predicate::gte("rank", 1_i64),
            ]),
        )
    };
    let weighted = |kind: &str| {
        traversal::g().e_with_label_where(
            "LINK",
            Predicate::and(vec![
                Predicate::eq("kind", kind),
                Predicate::gte("weight", 1_i64),
            ]),
        )
    };
    let response = db
        .query(
            QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "rank_desc",
                        traversal::g()
                            .n_with_label("Document")
                            .order_by("rank", Order::Desc)
                            .limit(2_usize)
                            .id(),
                    )
                    .var_as(
                        "filtered_rank_desc",
                        traversal::g()
                            .n_with_label_where("Document", Predicate::gte("rank", 2_i64))
                            .order_by("rank", Order::Desc)
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "weight_asc",
                        traversal::g()
                            .e_with_label("LINK")
                            .order_by("weight", Order::Asc)
                            .limit(2_usize)
                            .id(),
                    )
                    .var_as(
                        "intersect_first",
                        ranked("group")
                            .order_by("rank", Order::Asc)
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "intersect_last",
                        ranked("group")
                            .order_by("rank", Order::Desc)
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "intersect_all",
                        ranked("group").order_by("rank", Order::Asc).id(),
                    )
                    .var_as(
                        "intersect_count",
                        ranked("group").order_by("rank", Order::Asc).count(),
                    )
                    .var_as(
                        "intersect_empty",
                        ranked("none")
                            .order_by("rank", Order::Asc)
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "edge_intersect_first",
                        weighted("primary")
                            .order_by("weight", Order::Desc)
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "edge_intersect_all",
                        weighted("primary").order_by("weight", Order::Desc).id(),
                    )
                    .var_as(
                        "edge_intersect_count",
                        weighted("primary").order_by("weight", Order::Desc).count(),
                    )
                    .var_as(
                        "edge_intersect_empty",
                        weighted("none")
                            .order_by("weight", Order::Desc)
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "runtime_range_limit",
                        traversal::g()
                            .n_with_label_where("Document", Predicate::gte("rank", 1_i64))
                            .order_by("rank", Order::Asc)
                            .limit(StreamBound::expr(Expr::param("two")))
                            .id(),
                    )
                    .var_as(
                        "runtime_label_limit",
                        traversal::g()
                            .n_with_label("Document")
                            .order_by("rank", Order::Asc)
                            .limit(StreamBound::expr(Expr::param("one")))
                            .id(),
                    )
                    .returning([
                        "rank_desc",
                        "filtered_rank_desc",
                        "weight_asc",
                        "intersect_first",
                        "intersect_last",
                        "intersect_all",
                        "intersect_count",
                        "intersect_empty",
                        "edge_intersect_first",
                        "edge_intersect_all",
                        "edge_intersect_count",
                        "edge_intersect_empty",
                        "runtime_range_limit",
                        "runtime_label_limit",
                    ]),
            )
            .with_parameter_value("two", QueryValue::I64(2))
            .with_parameter_value("one", QueryValue::I64(1)),
        )
        .await
        .expect("indexed window read succeeds");
    assert_eq!(
        response,
        serde_json::json!({
            "rank_desc": [2, 1],
            "filtered_rank_desc": [2],
            "weight_asc": [0, 1],
            "intersect_first": [0],
            "intersect_last": [1],
            "intersect_all": [0, 1],
            "intersect_count": 2,
            "intersect_empty": null,
            "edge_intersect_first": [2],
            "edge_intersect_all": [2, 0],
            "edge_intersect_count": 2,
            "edge_intersect_empty": null,
            "runtime_range_limit": [0, 1],
            "runtime_label_limit": [0],
        })
    );
    for (bound, expected) in [
        (
            QueryValue::I64(-1),
            "Query error: stream bound expression returned -1",
        ),
        (
            QueryValue::String("two".to_owned()),
            "Query error: parameter `n` is not an i64",
        ),
    ] {
        let error = db
            .query(
                QueryRequest::read(
                    batch::read_batch()
                        .var_as(
                            "bounded",
                            traversal::g()
                                .n_with_label("Document")
                                .order_by("rank", Order::Asc)
                                .limit(StreamBound::expr(Expr::param("n")))
                                .id(),
                        )
                        .returning(["bounded"]),
                )
                .with_parameter_value("n", bound),
            )
            .await
            .expect_err("an invalid runtime bound fails the request");
        assert_eq!(error.error_code(), QueryErrorCode::InvalidQuery);
        assert_eq!(error.to_string(), expected);
    }
    db.close().await.expect("indexed-window fixture closes");
}

/// Point and parameter sources filtered by an indexed property intersect with
/// the index before the window, and unique, null and typed-array equality
/// domains select exactly the rows a per-row filter would.
#[tokio::test]
async fn indexed_filters_intersect_sources_and_resolve_equality_domains() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-indexed-filters".to_owned(),
    })
    .await
    .expect("indexed-filter fixture opens");
    seed_documents(&db).await;
    let grouped_points = || {
        traversal::g()
            .n(NodeRef::ids([0, 1, 2]))
            .has_label("Document")
            .has("category", "group")
    };
    let codes = || PropertyValue::StringArray(vec!["A".to_owned(), "C".to_owned(), "Z".to_owned()]);
    let response = db
        .query(
            QueryRequest::read(
                batch::read_batch()
                    .var_as("points_first", grouped_points().limit(1_usize).id())
                    .var_as("points_all", grouped_points().id())
                    .var_as("points_count", grouped_points().count())
                    .var_as(
                        "param_first",
                        traversal::g()
                            .n(NodeRef::param("ids"))
                            .has_label("Document")
                            .has("category", "group")
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "edge_points_first",
                        traversal::g()
                            .e(EdgeRef::ids([0, 1, 2]))
                            .has_label("LINK")
                            .has("kind", "primary")
                            .limit(1_usize)
                            .id(),
                    )
                    .var_as(
                        "ranked_or_unique",
                        traversal::g()
                            .n_with_label_where(
                                "Document",
                                Predicate::or(vec![
                                    Predicate::and(vec![
                                        Predicate::eq("category", "group"),
                                        Predicate::gte("rank", 2_i64),
                                    ]),
                                    Predicate::eq("code", "C"),
                                ]),
                            )
                            .id(),
                    )
                    .var_as(
                        "unique_members",
                        traversal::g()
                            .n_with_label_where("Document", Predicate::is_in("code", codes()))
                            .id(),
                    )
                    .var_as(
                        "unique_member_count",
                        traversal::g()
                            .n_with_label_where("Document", Predicate::is_in("code", codes()))
                            .count(),
                    )
                    .var_as(
                        "unique_param",
                        traversal::g()
                            .n_with_label_where("Document", Predicate::eq_param("code", "code"))
                            .id(),
                    )
                    .var_as(
                        "unique_param_set",
                        traversal::g()
                            .n_with_label("Document")
                            .where_(Predicate::is_in_param("code", "codes"))
                            .id(),
                    )
                    .var_as(
                        "mixed_numeric_buckets",
                        traversal::g()
                            .n_with_label("Document")
                            .where_(Predicate::is_in_param("bucket", "buckets"))
                            .id(),
                    )
                    .var_as(
                        "f32_bucket",
                        traversal::g()
                            .n_with_label("Document")
                            .where_(Predicate::is_in_param("bucket", "f32_bucket"))
                            .id(),
                    )
                    .var_as(
                        "node_null_or",
                        traversal::g()
                            .n_with_label_where(
                                "Document",
                                Predicate::or(vec![
                                    Predicate::eq("category", "other"),
                                    Predicate::is_null("category"),
                                ]),
                            )
                            .id(),
                    )
                    .var_as(
                        "edge_null_or",
                        traversal::g()
                            .e_with_label_where(
                                "LINK",
                                Predicate::or(vec![
                                    Predicate::eq("kind", "secondary"),
                                    Predicate::is_null("kind"),
                                ]),
                            )
                            .id(),
                    )
                    .var_as(
                        "edge_null_equality",
                        traversal::g()
                            .e_with_label_where("LINK", Predicate::eq("kind", PropertyValue::Null))
                            .id(),
                    )
                    .returning([
                        "points_first",
                        "points_all",
                        "points_count",
                        "param_first",
                        "edge_points_first",
                        "ranked_or_unique",
                        "unique_members",
                        "unique_member_count",
                        "unique_param",
                        "unique_param_set",
                        "mixed_numeric_buckets",
                        "f32_bucket",
                        "node_null_or",
                        "edge_null_or",
                        "edge_null_equality",
                    ]),
            )
            .with_parameter_value(
                "ids",
                QueryValue::Array(vec![QueryValue::I64(1), QueryValue::I64(2)]),
            )
            .with_parameter_value("code", QueryValue::String("B".to_owned()))
            .with_parameter_value(
                "codes",
                QueryValue::Array(vec![
                    QueryValue::String("B".to_owned()),
                    QueryValue::String("C".to_owned()),
                ]),
            )
            .with_parameter_value(
                "buckets",
                QueryValue::Array(vec![QueryValue::I64(10), QueryValue::F64(20.0)]),
            )
            .with_parameter_value("f32_bucket", QueryValue::Array(vec![QueryValue::F32(20.0)])),
        )
        .await
        .expect("indexed filter read succeeds");
    assert_eq!(
        response,
        serde_json::json!({
            "points_first": [0],
            "points_all": [0, 1],
            "points_count": 2,
            "param_first": [1],
            "edge_points_first": [0],
            "ranked_or_unique": [1, 2],
            "unique_members": [0, 2],
            "unique_member_count": 2,
            "unique_param": [1],
            "unique_param_set": [1, 2],
            "mixed_numeric_buckets": [0, 1, 2],
            "f32_bucket": [1],
            "node_null_or": [2],
            "edge_null_or": [1],
            "edge_null_equality": [],
        })
    );
    db.close().await.expect("indexed-filter fixture closes");
}

/// Request parameters bound into an indexed `is_in` filter select the same
/// rows for every shape that consumes them: ID lists, windows, counts,
/// residual filters, equality conjunctions and ordered range reads. Empty,
/// duplicate, null-bearing, scalar and oversized domains are all exact.
#[tokio::test]
async fn indexed_membership_parameters_are_exact_for_every_domain_shape() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-membership-parameters".to_owned(),
    })
    .await
    .expect("membership-parameter fixture opens");
    seed_documents(&db).await;
    let strings = |values: &[&str]| {
        QueryValue::Array(
            values
                .iter()
                .map(|value| QueryValue::String((*value).to_owned()))
                .collect(),
        )
    };
    let documents = || {
        traversal::g()
            .n_with_label("Document")
            .where_(Predicate::is_in_param("category", "values"))
    };
    for (domain, values, expected) in [
        (
            "two",
            strings(&["group", "other"]),
            serde_json::json!({
                "ids": [0, 1, 2], "first": [0], "count": 3, "ranked_count": 2,
                "bucket_ids": [0, 2], "ordered": [0, 1],
            }),
        ),
        (
            "duplicate",
            strings(&["group", "group"]),
            serde_json::json!({
                "ids": [0, 1], "first": [0], "count": 2, "ranked_count": 1,
                "bucket_ids": [0], "ordered": [0, 1],
            }),
        ),
        (
            "null member",
            QueryValue::Array(vec![
                QueryValue::Null,
                QueryValue::String("other".to_owned()),
            ]),
            serde_json::json!({
                "ids": [2], "first": [2], "count": 1, "ranked_count": 1,
                "bucket_ids": [2], "ordered": [2],
            }),
        ),
        (
            "scalar",
            QueryValue::String("group".to_owned()),
            serde_json::json!({
                "ids": [0, 1], "first": [0], "count": 2, "ranked_count": 1,
                "bucket_ids": [0], "ordered": [0, 1],
            }),
        ),
        (
            "empty",
            QueryValue::Array(Vec::new()),
            serde_json::json!({
                "ids": [], "first": null, "count": 0, "ranked_count": 0,
                "bucket_ids": [], "ordered": [],
            }),
        ),
        (
            "oversized",
            QueryValue::Array(
                (0..200)
                    .map(|value| QueryValue::String(format!("missing-{value}")))
                    .collect(),
            ),
            serde_json::json!({
                "ids": [], "first": null, "count": 0, "ranked_count": 0,
                "bucket_ids": [], "ordered": [],
            }),
        ),
    ] {
        let response = db
            .query(
                QueryRequest::read(
                    batch::read_batch()
                        .var_as("ids", documents().id())
                        .var_as("first", documents().limit(1_usize).id())
                        .var_as("count", documents().count())
                        .var_as(
                            "ranked_count",
                            documents().where_(Predicate::gte("rank", 2_i64)).count(),
                        )
                        .var_as(
                            "bucket_ids",
                            traversal::g()
                                .n_with_label("Document")
                                .where_(Predicate::and(vec![
                                    Predicate::is_in_param("category", "values"),
                                    Predicate::eq("bucket", 10_i64),
                                ]))
                                .id(),
                        )
                        .var_as(
                            "ordered",
                            traversal::g()
                                .n_with_label("Document")
                                .where_(Predicate::and(vec![
                                    Predicate::is_in_param("category", "values"),
                                    Predicate::gte("rank", 1_i64),
                                ]))
                                .order_by("rank", Order::Asc)
                                .limit(2_usize)
                                .id(),
                        )
                        .returning([
                            "ids",
                            "first",
                            "count",
                            "ranked_count",
                            "bucket_ids",
                            "ordered",
                        ]),
                )
                .with_parameter_value("values", values),
            )
            .await
            .unwrap_or_else(|error| panic!("{domain} node membership fails: {error}"));
        assert_eq!(response, expected, "{domain}");
    }

    let links = || {
        traversal::g()
            .e_with_label("LINK")
            .where_(Predicate::is_in_param("kind", "values"))
    };
    for (domain, values, expected) in [
        (
            "two",
            strings(&["primary", "secondary"]),
            serde_json::json!({
                "ids": [0, 1, 2], "first": [0], "count": 3, "heavy_count": 2,
                "ordered": [2, 1], "scoped_count": 3,
            }),
        ),
        (
            "one",
            strings(&["secondary"]),
            serde_json::json!({
                "ids": [1], "first": [1], "count": 1, "heavy_count": 1,
                "ordered": [1], "scoped_count": 1,
            }),
        ),
        (
            "null member",
            QueryValue::Array(vec![
                QueryValue::Null,
                QueryValue::String("primary".to_owned()),
            ]),
            serde_json::json!({
                "ids": [0, 2], "first": [0], "count": 2, "heavy_count": 1,
                "ordered": [2, 0], "scoped_count": 2,
            }),
        ),
        (
            "empty",
            QueryValue::Array(Vec::new()),
            serde_json::json!({
                "ids": [], "first": null, "count": 0, "heavy_count": 0,
                "ordered": [], "scoped_count": 0,
            }),
        ),
    ] {
        let response = db
            .query(
                QueryRequest::read(
                    batch::read_batch()
                        .var_as("ids", links().id())
                        .var_as("first", links().limit(1_usize).id())
                        .var_as("count", links().count())
                        .var_as(
                            "heavy_count",
                            links().where_(Predicate::gte("weight", 2_i64)).count(),
                        )
                        .var_as(
                            "ordered",
                            traversal::g()
                                .e_with_label("LINK")
                                .where_(Predicate::and(vec![
                                    Predicate::is_in_param("kind", "values"),
                                    Predicate::gte("weight", 1_i64),
                                ]))
                                .order_by("weight", Order::Desc)
                                .limit(2_usize)
                                .id(),
                        )
                        .var_as(
                            "scoped_count",
                            traversal::g()
                                .e_with_label_where(
                                    "LINK",
                                    Predicate::is_in_param("kind", "values"),
                                )
                                .count(),
                        )
                        .returning([
                            "ids",
                            "first",
                            "count",
                            "heavy_count",
                            "ordered",
                            "scoped_count",
                        ]),
                )
                .with_parameter_value("values", values),
            )
            .await
            .unwrap_or_else(|error| panic!("{domain} edge membership fails: {error}"));
        assert_eq!(response, expected, "{domain}");
    }
    db.close()
        .await
        .expect("membership-parameter fixture closes");
}

/// A plan compiled once with a late-bound membership parameter classifies
/// each request's domain at run time: small exact domains (including scalar,
/// duplicate, typed-array and mixed-type values) read the equality index, a
/// NaN member matches nothing, and domains over the planner bound or with a
/// null member fall back to an authoritative scan with the same result.
#[tokio::test]
async fn late_bound_membership_plans_classify_each_request_domain() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-late-bound-membership".to_owned(),
    })
    .await
    .expect("late-bound fixture opens");
    seed_documents(&db).await;
    let values = ir::NonEmptyString::new("values").expect("parameter name is non-empty");
    let planner = context::PlannerContext {
        late_bound_params: BTreeSet::from([values.clone()]),
        limits: context::PlannerLimits {
            max_index_union_branches: context::IndexUnionBranchLimit::limited(2)
                .expect("two-branch limit is positive"),
        },
        ..db.planner_context(context::ParamBindings::default())
    };
    let strings = |values: &[&str]| {
        PropertyValue::StringArray(values.iter().map(|value| (*value).to_owned()).collect())
    };
    let nodes = |ids: &[u64]| {
        Some(ExecutionValue::Scalars(
            ids.iter().copied().map(ExecutionScalar::NodeId).collect(),
        ))
    };
    let edges = |ids: &[u64]| {
        Some(ExecutionValue::Scalars(
            ids.iter().copied().map(ExecutionScalar::EdgeId).collect(),
        ))
    };
    let count = |count: usize| Some(ExecutionValue::Count(count));
    let documents = || {
        traversal::g()
            .n_with_label("Document")
            .where_(Predicate::is_in_param("category", "values"))
    };
    let links = || {
        traversal::g()
            .e_with_label("LINK")
            .where_(Predicate::is_in_param("kind", "values"))
    };
    let buckets = || {
        traversal::g()
            .n_with_label("Document")
            .where_(Predicate::is_in_param("bucket", "values"))
    };

    // Each case plans one terminal once and runs it with one request domain.
    let mut cases = Vec::new();
    for (domain, value, ids, first, skipped, ranked, expanded, bucketed) in [
        (
            "indexed pair",
            strings(&["group", "other"]),
            vec![0, 1, 2],
            vec![0],
            2,
            2,
            3,
            vec![0, 2],
        ),
        (
            "over bound",
            strings(&["group", "other", "none"]),
            vec![0, 1, 2],
            vec![0],
            2,
            2,
            3,
            vec![0, 2],
        ),
        (
            "duplicate",
            strings(&["group", "group"]),
            vec![0, 1],
            vec![0],
            1,
            1,
            3,
            vec![0],
        ),
        (
            "scalar",
            PropertyValue::from("other"),
            vec![2],
            vec![2],
            0,
            1,
            0,
            vec![2],
        ),
        (
            "null member",
            PropertyValue::Array(vec![PropertyValue::Null, PropertyValue::from("other")]),
            vec![2],
            vec![2],
            0,
            1,
            0,
            vec![2],
        ),
        (
            "empty",
            PropertyValue::StringArray(Vec::new()),
            Vec::new(),
            Vec::new(),
            0,
            0,
            0,
            Vec::new(),
        ),
    ] {
        let total = ids.len();
        cases.extend([
            (
                format!("{domain} ids"),
                batch::read_batch().var_as("r", documents().id()),
                value.clone(),
                nodes(&ids),
            ),
            (
                format!("{domain} first"),
                batch::read_batch().var_as("r", documents().limit(1_usize).id()),
                value.clone(),
                nodes(&first),
            ),
            (
                format!("{domain} count"),
                batch::read_batch().var_as("r", documents().count()),
                value.clone(),
                count(total),
            ),
            (
                format!("{domain} skipped count"),
                batch::read_batch().var_as("r", documents().skip(1_usize).count()),
                value.clone(),
                count(skipped),
            ),
            (
                format!("{domain} ranked count"),
                batch::read_batch().var_as(
                    "r",
                    documents().where_(Predicate::gte("rank", 2_i64)).count(),
                ),
                value.clone(),
                count(ranked),
            ),
            (
                format!("{domain} expanded count"),
                batch::read_batch().var_as("r", documents().out(Some("LINK")).count()),
                value.clone(),
                count(expanded),
            ),
            (
                format!("{domain} bucket conjunction"),
                batch::read_batch().var_as(
                    "r",
                    traversal::g()
                        .n_with_label("Document")
                        .where_(Predicate::and(vec![
                            Predicate::is_in_param("category", "values"),
                            Predicate::eq("bucket", 10_i64),
                        ]))
                        .id(),
                ),
                value.clone(),
                nodes(&bucketed),
            ),
            (
                format!("{domain} scoped count"),
                batch::read_batch().var_as(
                    "r",
                    traversal::g()
                        .n_with_label_where(
                            "Document",
                            Predicate::is_in_param("category", "values"),
                        )
                        .count(),
                ),
                value,
                count(total),
            ),
        ]);
    }
    for (domain, value, ids, heavy) in [
        (
            "indexed pair",
            strings(&["primary", "secondary"]),
            vec![0, 1, 2],
            vec![2, 1],
        ),
        (
            "over bound",
            strings(&["primary", "secondary", "none"]),
            vec![0, 1, 2],
            vec![2, 1],
        ),
        ("single", strings(&["secondary"]), vec![1], vec![1]),
    ] {
        let total = ids.len();
        cases.extend([
            (
                format!("{domain} edge ids"),
                batch::read_batch().var_as("r", links().id()),
                value.clone(),
                edges(&ids),
            ),
            (
                format!("{domain} edge first"),
                batch::read_batch().var_as("r", links().limit(1_usize).id()),
                value.clone(),
                edges(&ids[..1]),
            ),
            (
                format!("{domain} edge count"),
                batch::read_batch().var_as("r", links().count()),
                value.clone(),
                count(total),
            ),
            (
                format!("{domain} edge heavy count"),
                batch::read_batch()
                    .var_as("r", links().where_(Predicate::gte("weight", 2_i64)).count()),
                value.clone(),
                count(heavy.len()),
            ),
            (
                format!("{domain} edge heavy ids"),
                batch::read_batch().var_as(
                    "r",
                    traversal::g()
                        .e_with_label("LINK")
                        .where_(Predicate::and(vec![
                            Predicate::is_in_param("kind", "values"),
                            Predicate::gte("weight", 2_i64),
                        ]))
                        .id(),
                ),
                value.clone(),
                edges(&heavy),
            ),
            (
                format!("{domain} edge scoped count"),
                batch::read_batch().var_as(
                    "r",
                    traversal::g()
                        .e_with_label_where("LINK", Predicate::is_in_param("kind", "values"))
                        .count(),
                ),
                value,
                count(total),
            ),
        ]);
    }
    for (domain, value, ids) in [
        (
            "i64 array",
            PropertyValue::I64Array(vec![10, 30]),
            vec![0, 2],
        ),
        ("f64 array", PropertyValue::F64Array(vec![20.0]), vec![1]),
        ("f32 array", PropertyValue::F32Array(vec![10.0]), vec![0, 2]),
        (
            "mixed array",
            PropertyValue::Array(vec![PropertyValue::from(20_i64), PropertyValue::from("x")]),
            vec![1],
        ),
        (
            "NaN member",
            PropertyValue::F64Array(vec![f64::NAN, 10.0]),
            vec![0, 2],
        ),
    ] {
        let total = ids.len();
        cases.extend([
            (
                format!("{domain} bucket ids"),
                batch::read_batch().var_as("r", buckets().id()),
                value.clone(),
                nodes(&ids),
            ),
            (
                format!("{domain} bucket count"),
                batch::read_batch().var_as("r", buckets().count()),
                value,
                count(total),
            ),
        ]);
    }
    for (name, read, value, expected) in cases {
        let plan = planning::plan_read_batch(&read.returning(["r"]), &planner)
            .unwrap_or_else(|error| panic!("{name} plans: {error}"));
        let result = db
            .execute(
                &plan,
                context::ParamBindings::default().with_value(values.clone(), value),
            )
            .await
            .unwrap_or_else(|error| panic!("{name} executes: {error}"));
        assert_eq!(result.last, expected, "{name}");
    }

    let plan = planning::plan_read_batch(
        &batch::read_batch()
            .var_as("r", documents().count())
            .returning(["r"]),
        &planner,
    )
    .expect("unbound membership count plans");
    let error = db
        .execute(&plan, context::ParamBindings::default())
        .await
        .expect_err("a late-bound parameter must be bound at execution");
    assert_eq!(error.error_code(), QueryErrorCode::InvalidQuery);
    assert_eq!(
        error.to_string(),
        "Query error: parameter `values` is not bound"
    );
    db.close().await.expect("late-bound fixture closes");
}

/// Reads inside a write batch observe the batch's own staged writes through
/// every index family: range windows in both directions, ordered
/// intersections, equality and unique lookups, and filtered range counts.
#[tokio::test]
async fn write_batch_index_reads_observe_staged_writes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-write-index-reads".to_owned(),
    })
    .await
    .expect("write-read fixture opens");
    seed_documents(&db).await;
    let response = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "added",
                    traversal::g().add_n(
                        "Document",
                        vec![
                            ("category", PropertyInput::from("group")),
                            ("code", PropertyInput::from("D")),
                            ("rank", PropertyInput::from(4_i64)),
                            ("bucket", PropertyInput::from(30_i64)),
                        ],
                    ),
                )
                .var_as(
                    "top",
                    traversal::g()
                        .n_with_label("Document")
                        .order_by("rank", Order::Desc)
                        .limit(2_usize)
                        .id(),
                )
                .var_as(
                    "low",
                    traversal::g()
                        .n_with_label("Document")
                        .order_by("rank", Order::Asc)
                        .limit(2_usize)
                        .id(),
                )
                .var_as(
                    "ranked",
                    traversal::g()
                        .n_with_label_where("Document", Predicate::gte("rank", 2_i64))
                        .id(),
                )
                .var_as(
                    "ranked_group_count",
                    traversal::g()
                        .n_with_label_where(
                            "Document",
                            Predicate::and(vec![
                                Predicate::eq("category", "group"),
                                Predicate::gte("rank", 2_i64),
                            ]),
                        )
                        .count(),
                )
                .var_as(
                    "grouped",
                    traversal::g()
                        .n_with_label_where("Document", Predicate::eq("category", "group"))
                        .id(),
                )
                .var_as(
                    "grouped_many_count",
                    traversal::g()
                        .n_with_label_where(
                            "Document",
                            Predicate::is_in(
                                "category",
                                PropertyValue::StringArray(vec![
                                    "group".to_owned(),
                                    "other".to_owned(),
                                ]),
                            ),
                        )
                        .count(),
                )
                .var_as(
                    "unique",
                    traversal::g()
                        .n_with_label_where("Document", Predicate::eq("code", "D"))
                        .id(),
                )
                .var_as(
                    "lightest_edge",
                    traversal::g()
                        .e_with_label("LINK")
                        .order_by("weight", Order::Asc)
                        .limit(1_usize)
                        .id(),
                )
                .var_as(
                    "primary_edges",
                    traversal::g()
                        .e_with_label_where("LINK", Predicate::eq("kind", "primary"))
                        .id(),
                )
                .var_as(
                    "heavy_primary_count",
                    traversal::g()
                        .e_with_label_where(
                            "LINK",
                            Predicate::and(vec![
                                Predicate::eq("kind", "primary"),
                                Predicate::gte("weight", 2_i64),
                            ]),
                        )
                        .count(),
                )
                .var_as(
                    "ordered_intersect",
                    traversal::g()
                        .n_with_label_where(
                            "Document",
                            Predicate::and(vec![
                                Predicate::eq("category", "group"),
                                Predicate::gte("rank", 1_i64),
                            ]),
                        )
                        .order_by("rank", Order::Desc)
                        .limit(2_usize)
                        .id(),
                )
                .var_as(
                    "edge_ordered_intersect",
                    traversal::g()
                        .e_with_label_where(
                            "LINK",
                            Predicate::and(vec![
                                Predicate::eq("kind", "primary"),
                                Predicate::gte("weight", 1_i64),
                            ]),
                        )
                        .order_by("weight", Order::Asc)
                        .limit(1_usize)
                        .id(),
                )
                .returning([
                    "added",
                    "top",
                    "low",
                    "ranked",
                    "ranked_group_count",
                    "grouped",
                    "grouped_many_count",
                    "unique",
                    "lightest_edge",
                    "primary_edges",
                    "heavy_primary_count",
                    "ordered_intersect",
                    "edge_ordered_intersect",
                ]),
        ))
        .await
        .expect("write batch with index reads commits");
    assert_eq!(
        response,
        serde_json::json!({
            "added": [{ "$id": 3 }],
            "top": [3, 2],
            "low": [0, 1],
            "ranked": [1, 2, 3],
            "ranked_group_count": 2,
            "grouped": [0, 1, 3],
            "grouped_many_count": 4,
            "unique": [3],
            "lightest_edge": [0],
            "primary_edges": [0, 2],
            "heavy_primary_count": 1,
            "ordered_intersect": [3, 1],
            "edge_ordered_intersect": [0],
        })
    );
    db.close().await.expect("write-read fixture closes");
}

/// On a reader, a parameter source intersected with an equality index
/// schedules both inputs as one parallel stage, with or without a window,
/// and returns the intersection in parameter order.
#[tokio::test]
async fn reader_parallel_stage_intersects_parameter_sources_with_indexes() {
    let token = ProcessLocalDatabaseToken::new("pull-paths-reader-parallel-intersect")
        .expect("process-local database token validates");
    let writer = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("parallel-intersect writer opens");
    seed_documents(&writer).await;
    writer
        .close()
        .await
        .expect("parallel-intersect writer closes");
    let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
        .await
        .expect("parallel-intersect reader opens");
    let grouped = || {
        traversal::g()
            .n(NodeRef::param("ids"))
            .has_label("Document")
            .has("category", "group")
    };
    let response = reader
        .query(
            QueryRequest::read(
                batch::read_batch()
                    .var_as("first", grouped().limit(1_usize).id())
                    .var_as("count", grouped().count())
                    .var_as(
                        "edge_first",
                        traversal::g()
                            .e(EdgeRef::param("edge_ids"))
                            .has_label("LINK")
                            .has("kind", "primary")
                            .limit(1_usize)
                            .id(),
                    )
                    .returning(["first", "count", "edge_first"]),
            )
            .with_parameter_value(
                "ids",
                QueryValue::Array(vec![
                    QueryValue::I64(1),
                    QueryValue::I64(0),
                    QueryValue::I64(2),
                ]),
            )
            .with_parameter_value(
                "edge_ids",
                QueryValue::Array(vec![QueryValue::I64(1), QueryValue::I64(2)]),
            ),
        )
        .await
        .expect("reader intersection succeeds");
    assert_eq!(
        response,
        serde_json::json!({ "first": [1], "count": 2, "edge_first": [2] })
    );
    // Without a window the same intersection runs as an ordinary parallel
    // stage over the reader snapshot.
    let response = reader
        .query(
            QueryRequest::read(
                batch::read_batch()
                    .var_as("ids", grouped().id())
                    .returning(["ids"]),
            )
            .with_parameter_value(
                "ids",
                QueryValue::Array(vec![QueryValue::I64(2), QueryValue::I64(0)]),
            ),
        )
        .await
        .expect("unbounded reader intersection succeeds");
    assert_eq!(response, serde_json::json!({ "ids": [0] }));
    reader
        .close()
        .await
        .expect("parallel-intersect reader closes");
}

/// A count over a range-driven intersection applies every filter of the
/// intersection, not only its range driver, alone, after `dedup`, and in a
/// union branch. Each expected count is the brute-force count over the
/// seeded `User` rows.
#[tokio::test]
async fn counts_over_range_intersections_apply_every_filter() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "pull-paths-range-intersection-counts".to_owned(),
    })
    .await
    .expect("count fixture opens");
    let users = (0..300_i64).fold(batch::write_batch(), |batch, uid| {
        batch.var_as(
            &format!("u{uid}"),
            traversal::g().add_n(
                "User",
                vec![
                    ("uid", PropertyInput::from(uid)),
                    ("rank", PropertyInput::from(uid % 60)),
                    ("tier", PropertyInput::from(uid % 3)),
                    ("name", PropertyInput::from(format!("name{}", uid % 10))),
                ],
            ),
        )
    });
    db.query(QueryRequest::write(users.returning(Vec::<String>::new())))
        .await
        .expect("users are committed");
    for spec in [
        IndexSpec::node_range("User", "rank"),
        IndexSpec::node_equality("User", "tier"),
        IndexSpec::node_unique_equality("User", "uid"),
        IndexSpec::node_equality("User", "name"),
    ] {
        let receipt = db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as("operation", traversal::g().create_index_if_not_exists(spec))
                    .returning(["operation"]),
            ))
            .await
            .expect("user index is accepted");
        let Some(operation_id) = receipt["operation"]["operation_id"].as_str() else {
            panic!("accepted user index has an operation ID: {receipt}");
        };
        super::await_index_operation_success(&db, operation_id, "user index").await;
    }
    let expected = |keep: &dyn Fn(i64) -> bool| (0..300_i64).filter(|uid| keep(*uid)).count();
    for (predicate, keep) in [
        (
            Predicate::and(vec![
                Predicate::lt("rank", 30),
                Predicate::eq("tier", 1),
                Predicate::neq("uid", 1),
            ]),
            &(|uid: i64| uid % 60 < 30 && uid % 3 == 1 && uid != 1) as &dyn Fn(i64) -> bool,
        ),
        (
            Predicate::or(vec![
                Predicate::eq("uid", 5),
                Predicate::and(vec![Predicate::eq("tier", 1), Predicate::lt("rank", 30)]),
            ]),
            &|uid: i64| uid == 5 || (uid % 3 == 1 && uid % 60 < 30),
        ),
        (
            Predicate::and(vec![
                Predicate::gte("rank", 0),
                Predicate::eq("name", "name5"),
            ]),
            &|uid: i64| uid % 10 == 5,
        ),
    ] {
        let source = || traversal::g().n_with_label_where("User", predicate.clone());
        let response = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as("count", source().count())
                    .var_as("distinct", source().dedup().count())
                    .returning(["count", "distinct"]),
            ))
            .await
            .expect("count succeeds");
        let count = expected(keep);
        assert_eq!(
            response,
            serde_json::json!({ "count": count, "distinct": count }),
            "{predicate:?}"
        );
    }
}
