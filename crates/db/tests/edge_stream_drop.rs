//! Drop must delete the selected elements atomically while preserving unrelated topology.
use db::{HelixDB, HelixDbSource};
use helix_ast::{batch, graph, query, traversal, value};

async fn seed(name: &str) -> HelixDB {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: name.into(),
    })
    .await
    .unwrap();
    db.query(query::QueryRequest::write(
        batch::write_batch()
            .var_as(
                "a",
                traversal::g().add_n("Item", vec![("key", value::PropertyInput::from("a"))]),
            )
            .var_as(
                "b",
                traversal::g().add_n("Item", vec![("key", value::PropertyInput::from("b"))]),
            )
            .var_as(
                "first",
                traversal::g().n(graph::NodeRef::var("a")).add_e(
                    "LINK",
                    graph::NodeRef::var("b"),
                    vec![("rank", value::PropertyInput::from(1_i64))],
                ),
            )
            .var_as(
                "second",
                traversal::g().n(graph::NodeRef::var("a")).add_e(
                    "LINK",
                    graph::NodeRef::var("b"),
                    vec![("rank", value::PropertyInput::from(2_i64))],
                ),
            )
            .var_as(
                "loop",
                traversal::g().n(graph::NodeRef::var("a")).add_e(
                    "LINK",
                    graph::NodeRef::var("a"),
                    vec![("rank", value::PropertyInput::from(3_i64))],
                ),
            )
            .var_as(
                "other",
                traversal::g().n(graph::NodeRef::var("a")).add_e(
                    "OTHER",
                    graph::NodeRef::var("b"),
                    vec![("rank", value::PropertyInput::from(4_i64))],
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    db
}

async fn counts(db: &HelixDB) -> serde_json::Value {
    db.query(query::QueryRequest::read(
        batch::read_batch()
            .var_as("nodes", traversal::g().n(graph::NodeRef::all()).count())
            .var_as("links", traversal::g().e_with_label("LINK").count())
            .var_as("other", traversal::g().e_with_label("OTHER").count())
            .returning(["nodes", "links", "other"]),
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn edge_drop_traversal_matrix() {
    for case in 0..7 {
        let db = seed(&format!("edge-drop-{case}")).await;
        let a = || traversal::g().n_with_label("Item").has("key", "a");
        let selected = match case {
            0 => a().out_e(Some("LINK")),
            1 => traversal::g()
                .n_with_label("Item")
                .has("key", "b")
                .in_e(Some("LINK")),
            2 => a().both_e(Some("LINK")),
            3 => a().out_e(Some("LINK")).has("rank", 2_i64),
            4 => a().out_e(Some("LINK")).limit(1_usize),
            5 => a().out_e(Some("MISSING")),
            6 => {
                let result = db
                    .query(query::QueryRequest::read(
                        batch::read_batch()
                            .var_as("ids", traversal::g().e_with_label("LINK").id())
                            .returning(["ids"]),
                    ))
                    .await
                    .unwrap();
                let ids: Vec<u64> = serde_json::from_value(result["ids"].clone()).unwrap();
                let duplicates = ids.iter().chain(ids.iter()).copied().collect();
                let rejected = db
                    .query(query::QueryRequest::write(
                        batch::write_batch()
                            .var_as(
                                "gone",
                                traversal::g().e(graph::EdgeRef::Ids(duplicates)).drop(),
                            )
                            .returning(Vec::<String>::new()),
                    ))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    rejected,
                    db::error::HelixDbError::Planner(
                        helix_planner::error::PlannerError::DuplicateElementId { .. }
                    )
                ));
                traversal::g().e(graph::EdgeRef::Ids(ids))
            }
            _ => unreachable!(),
        };
        let result = db
            .query(query::QueryRequest::write(
                batch::write_batch()
                    .var_as("gone", selected.drop().count())
                    .returning(["gone"]),
            ))
            .await
            .unwrap();
        assert_eq!(result, serde_json::json!({"gone": 0}));
        let links = match case {
            0 | 2 | 6 => 0,
            1 => 1,
            3 | 4 => 2,
            5 => 3,
            _ => unreachable!(),
        };
        assert_eq!(
            counts(&db).await,
            serde_json::json!({"nodes": 2, "links": links, "other": 1})
        );
        // The unrelated parallel edge must keep the endpoint pair traversable.
        let remaining = db
            .query(query::QueryRequest::read(
                batch::read_batch()
                    .var_as("n", a().out(Some("OTHER")).count())
                    .returning(["n"]),
            ))
            .await
            .unwrap();
        assert_eq!(remaining, serde_json::json!({"n": 1}));
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn edge_drop_replacement_and_rollback() {
    let db = seed("edge-drop-replacement").await;
    for _ in 0..3 {
        db.query(query::QueryRequest::write(
            batch::write_batch()
                .var_as("a", traversal::g().n_with_label("Item").has("key", "a"))
                .var_as("b", traversal::g().n_with_label("Item").has("key", "b"))
                .var_as(
                    "gone",
                    traversal::g()
                        .n(graph::NodeRef::var("a"))
                        .out_e(Some("LINK"))
                        .drop(),
                )
                .var_as(
                    "new",
                    traversal::g().n(graph::NodeRef::var("a")).add_e(
                        "LINK",
                        graph::NodeRef::var("b"),
                        vec![("rank", value::PropertyInput::from(5_i64))],
                    ),
                )
                .returning(Vec::<String>::new()),
        ))
        .await
        .unwrap();
        assert_eq!(
            counts(&db).await,
            serde_json::json!({"nodes": 2, "links": 1, "other": 1})
        );
    }
    let failed = db
        .query(query::QueryRequest::write(
            batch::write_batch()
                .var_as("gone", traversal::g().e_with_label("LINK").drop())
                .var_as(
                    "bad",
                    traversal::g().n_with_label("Item").add_e(
                        "FAIL",
                        graph::NodeRef::id(999_999),
                        vec![("rank", value::PropertyInput::from(0_i64))],
                    ),
                )
                .returning(Vec::<String>::new()),
        ))
        .await;
    assert!(failed.is_err());
    assert_eq!(
        counts(&db).await,
        serde_json::json!({"nodes": 2, "links": 1, "other": 1})
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn edge_drop_sees_new_edges_and_node_cascades() {
    let db = seed("edge-drop-create").await;
    db.query(query::QueryRequest::write(
        batch::write_batch()
            .var_as("a", traversal::g().n_with_label("Item").has("key", "a"))
            .var_as(
                "new",
                traversal::g().n(graph::NodeRef::var("a")).add_e(
                    "NEW",
                    graph::NodeRef::var("a"),
                    vec![("rank", value::PropertyInput::from(0_i64))],
                ),
            )
            .var_as("gone", traversal::g().e(graph::EdgeRef::var("new")).drop())
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    let result = db
        .query(query::QueryRequest::read(
            batch::read_batch()
                .var_as("n", traversal::g().e_with_label("NEW").count())
                .returning(["n"]),
        ))
        .await
        .unwrap();
    assert_eq!(result, serde_json::json!({"n": 0}));
    db.query(query::QueryRequest::write(
        batch::write_batch()
            .var_as(
                "gone",
                traversal::g().n_with_label("Item").has("key", "a").drop(),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        counts(&db).await,
        serde_json::json!({"nodes": 1, "links": 0, "other": 0})
    );
    db.close().await.unwrap();
}
