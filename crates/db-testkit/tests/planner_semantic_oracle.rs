#![recursion_limit = "256"]

use std::collections::BTreeMap;

use db::{HelixDB, HelixDbSource};
use helix_ast::batch;
use helix_ast::graph::NodeRef;
use helix_ast::query::QueryRequest;
use helix_ast::traversal;
use helix_ast::value::PropertyInput;
use helix_db_testkit::action::{
    AggregateKind, ElementKind, EntityRef, PropertyValue, ReadAction, WriteAction,
};
use helix_db_testkit::ids::{EntityId, LabelName, PropertyName};
use helix_db_testkit::model::{GraphModel, ModelReadResult};

#[tokio::test]
async fn planner_interpreter_results_match_the_independent_graph_model() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "testkit-planner-semantic-oracle".to_string(),
    })
    .await
    .unwrap();
    let mut model = GraphModel::default();

    let alice = insert_person(&db, "alice", 1).await;
    let bob = insert_person(&db, "bob", 2).await;
    for (id, name, rank) in [(alice, "alice", 1_i64), (bob, "bob", 2_i64)] {
        model
            .apply(&WriteAction::InsertNode {
                id: EntityId::new(id),
                label: LabelName::try_new("Person").unwrap(),
                properties: BTreeMap::from([
                    (
                        PropertyName::try_new("name").unwrap(),
                        PropertyValue::String(name.to_string()),
                    ),
                    (
                        PropertyName::try_new("rank").unwrap(),
                        PropertyValue::I64(rank),
                    ),
                ]),
            })
            .unwrap();
    }

    let edge = insert_knows(&db, alice, bob).await;
    model
        .apply(&WriteAction::InsertEdge {
            id: EntityId::new(edge),
            label: LabelName::try_new("KNOWS").unwrap(),
            from: EntityId::new(alice),
            to: EntityId::new(bob),
            properties: BTreeMap::new(),
        })
        .unwrap();

    let expected_count = model
        .read(&ReadAction::Aggregate {
            kind: ElementKind::Node,
            aggregate: AggregateKind::Count,
        })
        .unwrap();
    let ModelReadResult::Count(expected_count) = expected_count else {
        panic!("aggregate oracle must return a count");
    };
    let label_source_count = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("result", traversal::g().n_with_label("Person").count())
                .returning(["result"]),
        ))
        .await
        .unwrap();
    let filtered_count = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "result",
                    traversal::g().n(NodeRef::all()).has_label("Person").count(),
                )
                .returning(["result"]),
        ))
        .await
        .unwrap();
    assert_eq!(label_source_count["result"], expected_count);
    assert_eq!(filtered_count, label_source_count);
    let cypher_count = db
        .cypher(db::cypher::Request::new("MATCH (n:Person) RETURN count(n)"))
        .await
        .unwrap();
    assert_eq!(
        cypher_count.rows,
        vec![vec![serde_json::json!(expected_count)]]
    );

    let expected_traversal = model
        .read(&ReadAction::Traversal {
            start: EntityId::new(alice),
            direction: helix_db_testkit::action::TraversalDirection::Outgoing,
            max_depth: std::num::NonZeroU16::new(1).unwrap(),
        })
        .unwrap();
    let ModelReadResult::Entities(expected_traversal) = expected_traversal else {
        panic!("traversal oracle must return elements");
    };
    assert_eq!(
        expected_traversal,
        vec![EntityRef::Node(EntityId::new(bob))]
    );
    let actual_traversal = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "result",
                    traversal::g().n(NodeRef::id(alice)).out(Some("KNOWS")).id(),
                )
                .returning(["result"]),
        ))
        .await
        .unwrap();
    assert_eq!(actual_traversal["result"], serde_json::json!([bob]));
    let cypher_traversal = db
        .cypher(db::cypher::Request::new(
            "MATCH (a:Person {name:'alice'})-[:KNOWS]->(b) RETURN b",
        ))
        .await
        .unwrap();
    assert_eq!(cypher_traversal.rows.len(), expected_traversal.len());
    assert_eq!(cypher_traversal.rows[0][0]["id"], bob.to_string());

    let invalid_write = QueryRequest::write(
        batch::write_batch()
            .var_as(
                "created",
                traversal::g().add_n(
                    "Person",
                    vec![("name", PropertyInput::from("must-not-commit"))],
                ),
            )
            .returning(["created", "created"]),
    );
    assert!(db.query(invalid_write).await.is_err());
    let count_after_rejection = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("result", traversal::g().n_with_label("Person").count())
                .returning(["result"]),
        ))
        .await
        .unwrap();
    assert_eq!(count_after_rejection["result"], expected_count);

    db.close().await.unwrap();
}

async fn insert_person(db: &HelixDB, name: &str, rank: i64) -> u64 {
    let response = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g()
                        .add_n(
                            "Person",
                            vec![
                                ("name", PropertyInput::from(name)),
                                ("rank", PropertyInput::from(rank)),
                            ],
                        )
                        .id(),
                )
                .returning(["created"]),
        ))
        .await
        .unwrap();
    response["created"][0]
        .as_u64()
        .unwrap_or_else(|| panic!("node ID response has unexpected shape: {response}"))
}

async fn insert_knows(db: &HelixDB, source: u64, target: u64) -> u64 {
    let response = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g()
                        .n(NodeRef::id(source))
                        .add_e(
                            "KNOWS",
                            NodeRef::id(target),
                            Vec::<(&str, PropertyInput)>::new(),
                        )
                        .id(),
                )
                .returning(["created"]),
        ))
        .await
        .unwrap();
    response["created"][0]
        .as_u64()
        .unwrap_or_else(|| panic!("edge ID response has unexpected shape: {response}"))
}
