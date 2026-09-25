//! End-to-end post-expansion index membership through `HelixDB::query`.
//!
//! Every query compares the membership plan with the same data served by a
//! database without the index, which keeps the per-row filter plan.

use helix_ast::{batch, expr, graph, index, query, traversal, value};
use helix_planner::{context, exec, planning};

use crate::encoding::keys::scope::{DataScope, TenantId};
use crate::{HelixDB, HelixDbSource};

const EMBEDDING_DIMENSIONS: usize = 768;

async fn open(name: &str) -> HelixDB {
    HelixDB::open(HelixDbSource::InMemory {
        database: name.into(),
    })
    .await
    .unwrap()
}

async fn create_index(db: &HelixDB, scope: DataScope, spec: index::IndexSpec) {
    let receipt = db
        .query_scoped(
            query::QueryRequest::write(
                batch::write_batch()
                    .var_as("index", traversal::g().create_index_if_not_exists(spec))
                    .returning(["index"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let operation = receipt["index"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let status = db
                .query_scoped(
                    query::QueryRequest::read(
                        batch::read_batch()
                            .var_as("status", traversal::g().get_index_operation(&operation))
                            .returning(["status"]),
                    ),
                    scope,
                )
                .await
                .unwrap();
            match status["status"]["status"].as_str().unwrap() {
                "succeeded" => break,
                "queued" | "running" => tokio::task::yield_now().await,
                other => panic!("index failed: {other}"),
            }
        }
    })
    .await
    .unwrap();
}

fn node(
    label: &str,
    uid: &str,
    kind: Option<&str>,
) -> traversal::Traversal<traversal::OnNodes, traversal::WriteEnabled> {
    let mut properties = vec![
        ("uid", value::PropertyInput::from(uid)),
        (
            "embedding",
            value::PropertyInput::from(vec![0.25_f32; EMBEDDING_DIMENSIONS]),
        ),
    ];
    if let Some(kind) = kind {
        properties.push(("kind", value::PropertyInput::from(kind)));
    }
    traversal::g().add_n(label, properties)
}

fn edge(
    from: &str,
    label: &str,
    to: &str,
) -> traversal::Traversal<traversal::OnNodes, traversal::WriteEnabled> {
    traversal::g().n(graph::NodeRef::var(from)).add_e(
        label,
        graph::NodeRef::var(to),
        Vec::<(&str, value::PropertyInput)>::new(),
    )
}

/// Group `g3` reaches attributes through two items. `a1` is reachable twice,
/// `n1` is a same-valued node of another label, and `g1` is another group.
fn seed() -> batch::WriteBatch {
    [
        ("g3", node("Group", "g3", None)),
        ("g1", node("Group", "g1", None)),
        ("i1", node("Item", "i1", None)),
        ("i2", node("Item", "i2", None)),
        ("i3", node("Item", "i3", None)),
        ("a1", node("Attribute", "a1", Some("B"))),
        ("a2", node("Attribute", "a2", Some("A"))),
        ("a3", node("Attribute", "a3", None)),
        ("a4", node("Attribute", "a4", Some("B"))),
        ("n1", node("Note", "n1", Some("B"))),
        ("n2", node("Note", "n2", Some("A"))),
        ("e1", edge("i1", "IN_GROUP", "g3")),
        ("e2", edge("i2", "IN_GROUP", "g3")),
        ("e3", edge("i3", "IN_GROUP", "g1")),
        ("e4", edge("i1", "HAS_ATTRIBUTE", "a1")),
        ("e5", edge("i1", "HAS_ATTRIBUTE", "a2")),
        ("e6", edge("i1", "HAS_ATTRIBUTE", "n1")),
        ("e7", edge("i2", "HAS_ATTRIBUTE", "a1")),
        ("e8", edge("i2", "HAS_ATTRIBUTE", "a3")),
        ("e9", edge("i2", "HAS_ATTRIBUTE", "n2")),
        ("e10", edge("i3", "HAS_ATTRIBUTE", "a4")),
    ]
    .into_iter()
    .fold(batch::write_batch(), |write, (name, traversal)| {
        write.var_as(name, traversal)
    })
}

fn attributes_where(predicate: expr::Predicate) -> batch::ReadBatch {
    batch::read_batch()
        .var_as(
            "result",
            traversal::g()
                .n_with_label_where("Group", expr::Predicate::eq("uid", "g3"))
                .in_(Some("IN_GROUP"))
                .out(Some("HAS_ATTRIBUTE"))
                .where_(predicate)
                .values(vec!["uid"]),
        )
        .returning(["result"])
}

fn uids(response: &serde_json::Value) -> Vec<String> {
    let mut uids = response["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["uid"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    uids.sort();
    uids
}

async fn seeded(name: &str, scope: DataScope, indexed: bool) -> HelixDB {
    let db = open(name).await;
    if indexed {
        create_index(
            &db,
            scope,
            index::IndexSpec::node_equality("Attribute", "kind"),
        )
        .await;
        create_index(&db, scope, index::IndexSpec::node_range("Attribute", "uid")).await;
    }
    db.query_scoped(query::QueryRequest::write(seed()), scope)
        .await
        .unwrap();
    db
}

async fn plan(db: &HelixDB, read: &batch::ReadBatch, scope: DataScope) -> exec::ExecutablePlan {
    let prepared = db
        .planner_context_scoped_prepared(context::ParamBindings::default(), scope)
        .await
        .unwrap();
    planning::plan_read_batch(read, prepared.context()).unwrap()
}

fn has_membership(plan: &exec::ExecutablePlan) -> bool {
    plan.steps()
        .iter()
        .any(|step| matches!(step.op, exec::ExecOp::IndexMembership { .. }))
}

#[tokio::test]
async fn post_expansion_membership_matches_the_per_row_filter_end_to_end() {
    let scope = DataScope::LegacyUnscoped;
    let indexed = seeded("membership-e2e-indexed", scope, true).await;
    let unindexed = seeded("membership-e2e-unindexed", scope, false).await;
    for (predicate, expected) in [
        (expr::Predicate::eq("kind", "B"), vec!["a1", "a1", "n1"]),
        (
            expr::Predicate::and(vec![
                expr::Predicate::eq("$label", "Attribute"),
                expr::Predicate::eq("kind", "B"),
            ]),
            vec!["a1", "a1"],
        ),
        (
            expr::Predicate::is_in(
                "kind",
                value::PropertyValue::StringArray(vec!["A".into(), "B".into()]),
            ),
            vec!["a1", "a1", "a2", "n1", "n2"],
        ),
        (
            expr::Predicate::and(vec![
                expr::Predicate::eq("kind", "B"),
                expr::Predicate::starts_with("uid", "n"),
            ]),
            vec!["n1"],
        ),
        (
            expr::Predicate::gte("uid", "a2"),
            vec!["a2", "a3", "n1", "n2"],
        ),
        // A missing property equals null, so null keeps the per-row filter.
        (
            expr::Predicate::eq("kind", value::PropertyValue::Null),
            vec!["a3"],
        ),
    ] {
        let read = attributes_where(predicate.clone());
        let membership = indexed
            .query(query::QueryRequest::read(read.clone()))
            .await
            .unwrap();
        let per_row = unindexed
            .query(query::QueryRequest::read(read.clone()))
            .await
            .unwrap();
        assert_eq!(uids(&membership), expected, "{predicate:?}");
        assert_eq!(uids(&per_row), expected, "{predicate:?}");
        assert_eq!(
            has_membership(&plan(&indexed, &read, scope).await),
            !matches!(
                predicate,
                expr::Predicate::Eq {
                    right: expr::Expr::Constant(value::PropertyValue::Null),
                    ..
                }
            ),
            "{predicate:?}"
        );
        assert!(!has_membership(&plan(&unindexed, &read, scope).await));
    }
    indexed.close().await.unwrap();
    unindexed.close().await.unwrap();
}

#[tokio::test]
async fn post_expansion_membership_sees_same_request_writes() {
    let db = seeded("membership-e2e-own-writes", DataScope::LegacyUnscoped, true).await;
    let write = batch::write_batch()
        .var_as(
            "item",
            traversal::g().n_with_label_where("Item", expr::Predicate::eq("uid", "i2")),
        )
        .var_as("fresh", node("Attribute", "a5", Some("B")))
        .var_as("link", edge("item", "HAS_ATTRIBUTE", "fresh"))
        .var_as(
            "moved",
            traversal::g()
                .n_with_label_where("Attribute", expr::Predicate::eq("uid", "a1"))
                .set_property("kind", "A"),
        )
        .var_as(
            "result",
            traversal::g()
                .n_with_label_where("Group", expr::Predicate::eq("uid", "g3"))
                .in_(Some("IN_GROUP"))
                .out(Some("HAS_ATTRIBUTE"))
                .where_(expr::Predicate::eq("kind", "B"))
                .values(vec!["uid"]),
        )
        .returning(["result"]);
    let prepared = db
        .planner_context_scoped_prepared(
            context::ParamBindings::default(),
            DataScope::LegacyUnscoped,
        )
        .await
        .unwrap();
    assert!(has_membership(
        &planning::plan_write_batch(&write, prepared.context()).unwrap()
    ));
    drop(prepared);

    // The pending insert, edge, and label-scoped index move are visible to the
    // membership step before the request commits.
    let response = db.query(query::QueryRequest::write(write)).await.unwrap();
    assert_eq!(uids(&response), ["a5", "n1"]);
    let committed = db
        .query(query::QueryRequest::read(attributes_where(
            expr::Predicate::eq("kind", "B"),
        )))
        .await
        .unwrap();
    assert_eq!(uids(&committed), ["a5", "n1"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn post_expansion_membership_uses_each_tenant_catalog() {
    let db = open("membership-e2e-tenants").await;
    let [indexed, unindexed] = ["00000000000000000000000001", "00000000000000000000000002"]
        .map(|id| DataScope::Tenant(TenantId::from_ulid_str(id).unwrap()));
    create_index(
        &db,
        indexed,
        index::IndexSpec::node_equality("Attribute", "kind"),
    )
    .await;
    for scope in [indexed, unindexed] {
        db.query_scoped(query::QueryRequest::write(seed()), scope)
            .await
            .unwrap();
    }
    db.query_scoped(
        query::QueryRequest::write(
            batch::write_batch().var_as("extra", node("Attribute", "only-unindexed", Some("B"))),
        ),
        unindexed,
    )
    .await
    .unwrap();
    let read = attributes_where(expr::Predicate::eq("kind", "B"));
    for (scope, membership) in [(indexed, true), (unindexed, false)] {
        assert_eq!(has_membership(&plan(&db, &read, scope).await), membership);
        let response = db
            .query_scoped(query::QueryRequest::read(read.clone()), scope)
            .await
            .unwrap();
        assert_eq!(uids(&response), ["a1", "a1", "n1"]);
    }
    db.close().await.unwrap();
}
