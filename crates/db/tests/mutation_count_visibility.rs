//! A count of a mutation's own rows reads no deferred index state, so it skips
//! the mid-transaction flush. Index-backed counts and expansions later in the
//! same batch must still observe every write staged before them.
use std::time::Duration;

use db::{HelixDB, HelixDbSource};
use helix_ast::expr::Predicate;
use helix_ast::graph::NodeRef;
use helix_ast::index::IndexSpec;
use helix_ast::query::QueryRequest;
use helix_ast::value::PropertyInput;
use helix_ast::{batch, traversal};
use serde_json::json;

const LABEL: &str = "Item";
const SEEDED: i64 = 20;
const ADDED: i64 = 12;

async fn seeded_db(name: &str) -> HelixDB {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: name.to_owned(),
    })
    .await
    .unwrap();
    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g()
                        .create_index_if_not_exists(IndexSpec::node_equality(LABEL, "kind")),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    let operation_id = receipt["operation"]["operation_id"].as_str().unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = db
                .query(QueryRequest::read(
                    batch::read_batch()
                        .var_as("status", traversal::g().get_index_operation(operation_id))
                        .returning(["status"]),
                ))
                .await
                .unwrap();
            match status["status"]["status"].as_str() {
                Some("succeeded") => break,
                Some("queued" | "running") => tokio::task::yield_now().await,
                state => panic!("index operation reached {state:?}"),
            }
        }
    })
    .await
    .expect("kind index activates");
    db.query(QueryRequest::write(
        (0..SEEDED)
            .fold(batch::write_batch(), |batch, rank| {
                batch.var_as(
                    &format!("seed{rank}"),
                    traversal::g().add_n(
                        LABEL,
                        vec![
                            ("kind", PropertyInput::from("seed")),
                            ("rank", PropertyInput::from(rank)),
                        ],
                    ),
                )
            })
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    db
}

#[tokio::test]
async fn mutation_counts_return_their_rows_and_index_counts_see_the_batch() {
    let db = seeded_db("mutation-count-visibility").await;
    let new_items = || traversal::g().n_with_label_where(LABEL, Predicate::eq("kind", "new"));
    let added = (0..ADDED).map(|rank| format!("added{rank}"));
    let request = added
        .clone()
        .zip(0..ADDED)
        .fold(
            batch::write_batch()
                .var_as("label_before", traversal::g().n_with_label(LABEL).count())
                .var_as("kind_before", new_items().count()),
            |batch, (name, rank)| {
                batch.var_as(
                    &name,
                    traversal::g()
                        .add_n(
                            LABEL,
                            vec![
                                ("kind", PropertyInput::from("new")),
                                ("rank", PropertyInput::from(rank)),
                            ],
                        )
                        .count(),
                )
            },
        )
        .var_as("label_after", traversal::g().n_with_label(LABEL).count())
        .var_as("kind_after", new_items().count())
        .var_as(
            "from",
            traversal::g().add_n(LABEL, vec![("kind", PropertyInput::from("edge"))]),
        )
        .var_as(
            "to",
            traversal::g().add_n(LABEL, vec![("kind", PropertyInput::from("edge"))]),
        )
        .var_as(
            "linked",
            traversal::g()
                .n(NodeRef::var("from"))
                .add_e(
                    "NEXT",
                    NodeRef::var("to"),
                    Vec::<(&str, PropertyInput)>::new(),
                )
                .count(),
        )
        .var_as(
            "reached",
            traversal::g()
                .n(NodeRef::var("from"))
                .out(Some("NEXT"))
                .count(),
        )
        .returning(
            added
                .chain(
                    [
                        "label_before",
                        "kind_before",
                        "label_after",
                        "kind_after",
                        "linked",
                        "reached",
                    ]
                    .map(str::to_owned),
                )
                .collect::<Vec<_>>(),
        );
    let result = db.query(QueryRequest::write(request)).await.unwrap();

    for rank in 0..ADDED {
        assert_eq!(result[format!("added{rank}")], json!(1), "added{rank}");
    }
    assert_eq!(result["label_before"], json!(SEEDED));
    assert_eq!(result["kind_before"], json!(0));
    assert_eq!(result["label_after"], json!(SEEDED + ADDED));
    assert_eq!(result["kind_after"], json!(ADDED));
    assert_eq!(result["linked"], json!(1));
    assert_eq!(result["reached"], json!(1));

    let committed = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("label", traversal::g().n_with_label(LABEL).count())
                .var_as("ranks", new_items().value_map(Some(vec!["rank"])))
                .var_as(
                    "reached",
                    traversal::g()
                        .n_with_label_where(LABEL, Predicate::eq("kind", "edge"))
                        .out(Some("NEXT"))
                        .count(),
                )
                .returning(["label", "ranks", "reached"]),
        ))
        .await
        .unwrap();
    assert_eq!(committed["label"], json!(SEEDED + ADDED + 2));
    assert_eq!(committed["reached"], json!(1));
    let mut ranks = committed["ranks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["rank"].as_i64().unwrap())
        .collect::<Vec<_>>();
    ranks.sort_unstable();
    assert_eq!(ranks, (0..ADDED).collect::<Vec<_>>());
}
