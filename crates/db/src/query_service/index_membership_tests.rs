//! End-to-end post-expansion index membership through `HelixDB::query`.
//!
//! Every query compares the membership plan with the same data served by a
//! database without the index, which keeps the per-row filter plan. A narrow
//! traversal stays within one record batch of node rows, so its membership
//! evaluates every row; a wide one exceeds it and resolves the index set.

use helix_ast::{batch, expr, graph, index, query, traversal, value};
use helix_planner::{context, exec, planning};

use crate::encoding::keys::scope::{DataScope, TenantId};
use crate::{HelixDB, HelixDbSource};

const EMBEDDING_DIMENSIONS: usize = 768;

/// Targets of item `iw`. `aw` targets are `Attribute` nodes and `nw` targets
/// `Note` nodes; every third target has kind `B` and the others kind `A`.
const WIDE: [&str; 17] = [
    "aw0", "nw1", "aw2", "nw3", "aw4", "nw5", "aw6", "nw7", "aw8", "nw9", "aw10", "nw11", "aw12",
    "nw13", "aw14", "nw15", "aw16",
];

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
/// Item `iw` belongs to no group and links every [`WIDE`] target.
fn seed() -> batch::WriteBatch {
    let base = [
        ("g3", node("Group", "g3", None)),
        ("g1", node("Group", "g1", None)),
        ("i1", node("Item", "i1", None)),
        ("i2", node("Item", "i2", None)),
        ("i3", node("Item", "i3", None)),
        ("iw", node("Item", "iw", None)),
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
    });
    WIDE.iter().enumerate().fold(base, |write, (index, uid)| {
        let label = if uid.starts_with('a') {
            "Attribute"
        } else {
            "Note"
        };
        let kind = if index % 3 == 0 { "B" } else { "A" };
        write
            .var_as(uid, node(label, uid, Some(kind)))
            .var_as(&format!("iw_{uid}"), edge("iw", "HAS_ATTRIBUTE", uid))
    })
}

/// `uid`s behind group `g3`'s items: at most one record batch of node rows.
fn narrow(predicate: expr::Predicate) -> traversal::Traversal<traversal::Terminal> {
    traversal::g()
        .n_with_label_where("Group", expr::Predicate::eq("uid", "g3"))
        .in_(Some("IN_GROUP"))
        .out(Some("HAS_ATTRIBUTE"))
        .where_(predicate)
        .values(vec!["uid"])
}

/// `uid`s of every target of `iw`, each reached once per target: 289 node
/// rows with the seed, more than one 256-row record batch.
fn wide(predicate: expr::Predicate) -> traversal::Traversal<traversal::Terminal> {
    traversal::g()
        .n_with_label_where("Item", expr::Predicate::eq("uid", "iw"))
        .out(Some("HAS_ATTRIBUTE"))
        .in_(Some("HAS_ATTRIBUTE"))
        .out(Some("HAS_ATTRIBUTE"))
        .where_(predicate)
        .values(vec!["uid"])
}

fn read_result(result: traversal::Traversal<traversal::Terminal>) -> batch::ReadBatch {
    batch::read_batch()
        .var_as("result", result)
        .returning(["result"])
}

/// `predicate` with its conjuncts scoped to the `Attribute` label.
fn attribute(predicate: expr::Predicate) -> expr::Predicate {
    // A nested conjunction would hide its conjuncts from the index split.
    let conjuncts = if let expr::Predicate::And { predicates } = &predicate {
        predicates.clone()
    } else {
        vec![predicate]
    };
    expr::Predicate::and(
        core::iter::once(expr::Predicate::eq("$label", "Attribute"))
            .chain(conjuncts)
            .collect(),
    )
}

/// Sorted `uid`s of one returned row array.
fn uids(rows: &serde_json::Value) -> Vec<String> {
    let mut uids = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["uid"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    uids.sort();
    uids
}

/// Sorted `uids`, each reached `times` times.
fn repeated(uids: &[&'static str], times: usize) -> Vec<&'static str> {
    let mut repeated = uids
        .iter()
        .flat_map(|uid| std::iter::repeat_n(*uid, times))
        .collect::<Vec<_>>();
    repeated.sort();
    repeated
}

/// Membership sets `db` resolved from secondary indexes. The per-row
/// fallback keeps the same rows, so only this count shows a set was read.
fn resolved(db: &HelixDB) -> usize {
    db.inner
        .resolved_index_memberships
        .load(std::sync::atomic::Ordering::Relaxed)
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

fn membership_steps(plan: &exec::ExecutablePlan) -> usize {
    plan.steps()
        .iter()
        .filter(|step| matches!(step.op, exec::ExecOp::IndexMembership { .. }))
        .count()
}

#[tokio::test]
async fn post_expansion_membership_matches_the_per_row_filter_end_to_end() {
    let scope = DataScope::LegacyUnscoped;
    let indexed = seeded("membership-e2e-indexed", scope, true).await;
    let unindexed = seeded("membership-e2e-unindexed", scope, false).await;
    for (unscoped, narrow_uids, wide_uids, answered) in [
        (
            expr::Predicate::eq("kind", "B"),
            vec!["a1", "a1", "n1"],
            vec!["aw0", "nw3", "aw6", "nw9", "aw12", "nw15"],
            true,
        ),
        (
            expr::Predicate::is_in(
                "kind",
                value::PropertyValue::StringArray(vec!["A".into(), "B".into()]),
            ),
            vec!["a1", "a1", "a2", "n1", "n2"],
            WIDE.to_vec(),
            true,
        ),
        (
            expr::Predicate::and(vec![
                expr::Predicate::eq("kind", "B"),
                expr::Predicate::starts_with("uid", "n"),
            ]),
            vec!["n1"],
            vec!["nw3", "nw9", "nw15"],
            true,
        ),
        // A range set would verify the label's whole range, so ranges keep
        // the per-row filter.
        (
            expr::Predicate::gte("uid", "a2"),
            vec!["a2", "a3", "n1", "n2"],
            WIDE.to_vec(),
            false,
        ),
        // A missing property equals null, so null keeps the per-row filter.
        (
            expr::Predicate::eq("kind", value::PropertyValue::Null),
            vec!["a3"],
            Vec::new(),
            false,
        ),
    ] {
        // Without statistics an unscoped predicate cannot prove which label
        // the expansion reaches, so it keeps the per-row filter; scoped to
        // `Attribute` it plans membership whenever an index answers it.
        let attributes = |uids: &[&'static str]| {
            uids.iter()
                .copied()
                .filter(|uid| uid.starts_with('a'))
                .collect::<Vec<_>>()
        };
        for (predicate, narrow_uids, wide_uids, planned) in [
            (
                unscoped.clone(),
                narrow_uids.clone(),
                wide_uids.clone(),
                false,
            ),
            (
                attribute(unscoped),
                attributes(&narrow_uids),
                attributes(&wide_uids),
                answered,
            ),
        ] {
            // Only the wide stream reads the set its membership plans.
            for (read, expected, resolves) in [
                (read_result(narrow(predicate.clone())), narrow_uids, 0),
                (
                    read_result(wide(predicate.clone())),
                    repeated(&wide_uids, WIDE.len()),
                    usize::from(planned),
                ),
            ] {
                let before = resolved(&indexed);
                let membership = indexed
                    .query(query::QueryRequest::read(read.clone()))
                    .await
                    .unwrap();
                assert_eq!(resolved(&indexed) - before, resolves, "{predicate:?}");
                let per_row = unindexed
                    .query(query::QueryRequest::read(read.clone()))
                    .await
                    .unwrap();
                assert_eq!(uids(&membership["result"]), expected, "{predicate:?}");
                assert_eq!(uids(&per_row["result"]), expected, "{predicate:?}");
                assert_eq!(
                    membership_steps(&plan(&indexed, &read, scope).await),
                    usize::from(planned),
                    "{predicate:?}"
                );
                assert_eq!(membership_steps(&plan(&unindexed, &read, scope).await), 0);
            }
        }
    }
    assert_eq!(resolved(&unindexed), 0);
    indexed.close().await.unwrap();
    unindexed.close().await.unwrap();
}

#[tokio::test]
async fn post_expansion_membership_sees_same_request_writes() {
    let db = seeded("membership-e2e-own-writes", DataScope::LegacyUnscoped, true).await;
    let kind_b = || attribute(expr::Predicate::eq("kind", "B"));
    // Behind `g3` the request adds B-valued `a5` and moves `a1` to A; behind
    // `iw` it adds B-valued `aw-fresh` and moves `aw0` to A.
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
            "hub",
            traversal::g().n_with_label_where("Item", expr::Predicate::eq("uid", "iw")),
        )
        .var_as("wide_fresh", node("Attribute", "aw-fresh", Some("B")))
        .var_as("wide_link", edge("hub", "HAS_ATTRIBUTE", "wide_fresh"))
        .var_as(
            "wide_moved",
            traversal::g()
                .n_with_label_where("Attribute", expr::Predicate::eq("uid", "aw0"))
                .set_property("kind", "A"),
        )
        .var_as("narrow", narrow(kind_b()))
        .var_as("wide", wide(kind_b()))
        .returning(["narrow", "wide"]);
    let prepared = db
        .planner_context_scoped_prepared(
            context::ParamBindings::default(),
            DataScope::LegacyUnscoped,
        )
        .await
        .unwrap();
    assert_eq!(
        membership_steps(&planning::plan_write_batch(&write, prepared.context()).unwrap()),
        2
    );
    drop(prepared);

    // The pending inserts, edges, and label-scoped index moves are visible to
    // both memberships before the request commits. `iw` now has 18 targets,
    // so only the wide membership reads the set, which must already hold
    // the moves.
    let expected_wide = repeated(&["aw6", "aw12", "aw-fresh"], WIDE.len() + 1);
    let before = resolved(&db);
    let response = db.query(query::QueryRequest::write(write)).await.unwrap();
    assert_eq!(resolved(&db) - before, 1);
    assert_eq!(uids(&response["narrow"]), ["a5"]);
    assert_eq!(uids(&response["wide"]), expected_wide);
    for (result, expected) in [
        (narrow(kind_b()), vec!["a5"]),
        (wide(kind_b()), expected_wide),
    ] {
        let committed = db
            .query(query::QueryRequest::read(read_result(result)))
            .await
            .unwrap();
        assert_eq!(uids(&committed["result"]), expected);
    }
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
    // Only the unindexed tenant links another B-valued attribute from `iw`,
    // so the tenants' wide results differ.
    db.query_scoped(
        query::QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "hub",
                    traversal::g().n_with_label_where("Item", expr::Predicate::eq("uid", "iw")),
                )
                .var_as("extra", node("Attribute", "only-unindexed", Some("B")))
                .var_as("link", edge("hub", "HAS_ATTRIBUTE", "extra")),
        ),
        unindexed,
    )
    .await
    .unwrap();
    let kind_b = attribute(expr::Predicate::eq("kind", "B"));
    let narrow_read = read_result(narrow(kind_b.clone()));
    let wide_read = read_result(wide(kind_b));
    // The indexed tenant decides its wide rows from its own set alone, so a
    // set read from another tenant's keys would drop them.
    for (scope, planned, wide_uids) in [
        (indexed, 1, repeated(&["aw0", "aw6", "aw12"], WIDE.len())),
        (
            unindexed,
            0,
            repeated(&["aw0", "aw6", "aw12", "only-unindexed"], WIDE.len() + 1),
        ),
    ] {
        for (read, expected, resolves) in [
            (&narrow_read, vec!["a1", "a1"], 0),
            (&wide_read, wide_uids, planned),
        ] {
            assert_eq!(membership_steps(&plan(&db, read, scope).await), planned);
            let before = resolved(&db);
            let response = db
                .query_scoped(query::QueryRequest::read(read.clone()), scope)
                .await
                .unwrap();
            assert_eq!(resolved(&db) - before, resolves);
            assert_eq!(uids(&response["result"]), expected);
        }
    }
    db.close().await.unwrap();
}
