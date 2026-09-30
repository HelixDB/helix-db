//! End-to-end membership set retention through `HelixDB::query`.
//!
//! Every request runs against a database with the `Attribute` kind index and
//! one without it, and both must answer alike. The resolve counter of the
//! indexed database shows how often a request read a set: once per request
//! unless a `ForEach` frame rebinds a parameter the set reads or a write
//! reaches its footprint.

use std::collections::BTreeMap;

use helix_ast::batch;
use helix_ast::expr::Predicate;
use helix_ast::graph::NodeRef;
use helix_ast::query::{QueryRequest, QueryValue};
use helix_ast::traversal::{self, g};
use helix_ast::value::PropertyInput;

use super::index_membership_tests::{attribute, edge, narrow, node, resolved, seeded};
use crate::encoding::keys::scope::DataScope;
use crate::HelixDB;

struct Pair {
    indexed: HelixDB,
    unindexed: HelixDB,
}

impl Pair {
    async fn seeded(name: &str) -> Self {
        Self {
            indexed: seeded(&format!("{name}-indexed"), DataScope::LegacyUnscoped, true).await,
            unindexed: seeded(
                &format!("{name}-unindexed"),
                DataScope::LegacyUnscoped,
                false,
            )
            .await,
        }
    }

    /// Run `request` on both databases, assert equal outcomes, and return the
    /// response with the sets the indexed database resolved.
    async fn query(&self, request: QueryRequest) -> (crate::Result<serde_json::Value>, usize) {
        let before = resolved(&self.indexed);
        let indexed = self.indexed.query(request.clone()).await;
        let resolves = resolved(&self.indexed) - before;
        let unindexed = self.unindexed.query(request).await;
        match (&indexed, &unindexed) {
            (Ok(indexed), Ok(unindexed)) => assert_eq!(indexed, unindexed),
            (Err(indexed), Err(unindexed)) => {
                assert_eq!(indexed.to_string(), unindexed.to_string());
            }
            (indexed, unindexed) => panic!("{indexed:?} != {unindexed:?}"),
        }
        (indexed, resolves)
    }

    async fn close(self) {
        self.indexed.close().await.unwrap();
        self.unindexed.close().await.unwrap();
    }
}

fn frames<const N: usize>(rows: Vec<[(&str, &str); N]>) -> QueryValue {
    QueryValue::Array(
        rows.into_iter()
            .map(|fields| {
                QueryValue::Object(
                    fields
                        .into_iter()
                        .map(|(name, value)| {
                            (name.to_owned(), QueryValue::String(value.to_owned()))
                        })
                        .collect::<BTreeMap<_, _>>(),
                )
            })
            .collect(),
    )
}

/// Every `Attribute` with the properties the batches write.
fn attributes() -> traversal::Traversal<traversal::Terminal> {
    g().n_with_label("Attribute")
        .values(vec!["uid", "kind", "tagged", "seen"])
}

fn run_high_stack<F: std::future::Future<Output = ()> + 'static>(contract: fn() -> F) {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(contract());
        })
        .unwrap()
        .join()
        .unwrap();
}

/// An ingest batch whose frames create an item, link it, and tag the `B`
/// attributes behind it reads the kind set once for every frame.
#[test]
fn foreach_ingest_batches_resolve_a_set_once() {
    run_high_stack(foreach_ingest_contract);
}

async fn foreach_ingest_contract() {
    let pair = Pair::seeded("retention-e2e-ingest").await;
    let body = batch::write_batch()
        .var_as(
            "item",
            g().add_n("Item", vec![("uid", PropertyInput::param("uid"))]),
        )
        .var_as(
            "a1",
            g().n_with_label_where("Attribute", Predicate::eq("uid", "a1")),
        )
        .var_as(
            "a2",
            g().n_with_label_where("Attribute", Predicate::eq("uid", "a2")),
        )
        .var_as("l1", edge("item", "HAS_ATTRIBUTE", "a1"))
        .var_as("l2", edge("item", "HAS_ATTRIBUTE", "a2"))
        .var_as(
            "tag",
            g().n(NodeRef::var("item"))
                .out(Some("HAS_ATTRIBUTE"))
                .where_(attribute(Predicate::eq("kind", "B")))
                .set_property("tagged", PropertyInput::param("uid")),
        );
    let request = QueryRequest::write(
        batch::write_batch()
            .for_each_param("rows", body)
            .var_as("attributes", attributes())
            .returning(["attributes"]),
    )
    .with_parameter_value(
        "rows",
        frames(vec![
            [("uid", "x1")],
            [("uid", "x2")],
            [("uid", "x3")],
            [("uid", "x4")],
        ]),
    );
    let (response, resolves) = pair.query(request).await;
    let response = response.unwrap();
    assert_eq!(resolves, 1);
    assert!(
        response["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["uid"] == "a1" && row["tagged"] == "x4"),
        "{response}"
    );
    pair.close().await;
}

/// A body whose set reads a frame parameter resolves it once per frame, and
/// a failing batch leaves no state behind.
#[test]
fn foreach_set_parameters_resolve_per_frame_and_failures_abort() {
    run_high_stack(foreach_set_parameter_contract);
}

async fn foreach_set_parameter_contract() {
    let pair = Pair::seeded("retention-e2e-set-param").await;
    let body = batch::write_batch().var_as(
        "seen",
        g().n_with_label_where("Item", Predicate::eq("uid", "i1"))
            .out(Some("HAS_ATTRIBUTE"))
            .where_(attribute(Predicate::eq_param("kind", "kind")))
            .set_property("seen", PropertyInput::param("tag")),
    );
    let batch = batch::write_batch()
        .for_each_param("rows", body)
        .var_as("attributes", attributes())
        .returning(["attributes"]);
    let (response, resolves) = pair
        .query(QueryRequest::write(batch.clone()).with_parameter_value(
            "rows",
            frames(vec![
                [("kind", "B"), ("tag", "t1")],
                [("kind", "A"), ("tag", "t2")],
                [("kind", "B"), ("tag", "t3")],
            ]),
        ))
        .await;
    response.unwrap();
    assert_eq!(resolves, 3);

    // The second frame binds no tag, so the batch fails and aborts.
    let (failed, _) = pair
        .query(QueryRequest::write(batch.clone()).with_parameter_value(
            "rows",
            QueryValue::Array(vec![
                QueryValue::Object(BTreeMap::from([
                    ("kind".to_owned(), QueryValue::String("A".to_owned())),
                    ("tag".to_owned(), QueryValue::String("t4".to_owned())),
                ])),
                QueryValue::Object(BTreeMap::from([(
                    "kind".to_owned(),
                    QueryValue::String("B".to_owned()),
                )])),
            ]),
        ))
        .await;
    assert!(failed.is_err());
    let (after, _) = pair
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("attributes", attributes())
                .returning(["attributes"]),
        ))
        .await;
    let after = after.unwrap();
    assert!(
        after["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["seen"] != "t4"),
        "{after}"
    );
    pair.close().await;
}

/// A statement after a write reads the set again only when the write reached
/// its footprint.
#[test]
fn statements_after_writes_resolve_again_only_when_reached() {
    run_high_stack(statements_after_writes_contract);
}

async fn statements_after_writes_contract() {
    let pair = Pair::seeded("retention-e2e-statements").await;
    let kind_b = || narrow(attribute(Predicate::eq("kind", "B")));
    let attribute_uid =
        |uid: &str| g().n_with_label_where("Attribute", Predicate::eq("uid", uid.to_owned()));
    let writes: Vec<(
        &str,
        traversal::Traversal<traversal::OnNodes, traversal::WriteEnabled>,
        usize,
    )> = vec![
        ("add a note", node("Note", "n9", Some("B")), 1),
        (
            "set an unread property",
            attribute_uid("a2").set_property("title", "t"),
            1,
        ),
        (
            "set the read property",
            attribute_uid("a2").set_property("kind", "B"),
            2,
        ),
        (
            "remove the read property",
            attribute_uid("a2").remove_property("kind"),
            2,
        ),
        ("add an attribute", node("Attribute", "a9", Some("B")), 2),
        ("drop an attribute", attribute_uid("a3").drop(), 2),
    ];
    for (description, write, expected) in writes {
        let (response, resolves) = pair
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as("before", kind_b())
                    .var_as("write", write)
                    .var_as("after", kind_b())
                    .returning(["before", "after"]),
            ))
            .await;
        response.unwrap();
        assert_eq!(resolves, expected, "{description}");
    }

    // An edge-only write reaches no node index.
    let (response, resolves) = pair
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as("before", kind_b())
                .var_as(
                    "write",
                    g().n_with_label_where("Item", Predicate::eq("uid", "i2"))
                        .out_e(Some("HAS_ATTRIBUTE"))
                        .drop(),
                )
                .var_as("after", kind_b())
                .returning(["before", "after"]),
        ))
        .await;
    let response = response.unwrap();
    assert_eq!(resolves, 1);
    assert_ne!(response["before"], response["after"]);
    pair.close().await;
}
