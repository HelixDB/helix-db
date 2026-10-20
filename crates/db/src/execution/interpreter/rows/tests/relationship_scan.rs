use super::super::*;
use crate::encoding::v2::{keys, values::edge_endpoints::EdgeEndpointsValue};
use futures::TryStreamExt;
use helix_planner::context;
use serde_json::json;

async fn ids(db: &crate::HelixDB, query: &str) -> Vec<u64> {
    db.cypher(crate::cypher::Request::new(query))
        .await
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row[0].as_u64().unwrap())
        .collect()
}

fn steps(db: &crate::HelixDB, query: &str) -> Vec<r::MatchStep> {
    r::plan(
        helix_cypher::compile(query).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap()
    .matches()
    .values()
    .flat_map(|plan| plan.steps.clone())
    .collect()
}

/// A relationship scan reads each distinct type's bitmap once and one
/// endpoint key per edge. Pattern completion reads neither the unlabeled
/// endpoints nor the relationship, and a limit stops after its first batch.
#[tokio::test]
async fn scans_read_one_bitmap_per_type_and_one_endpoint_key_per_edge() {
    let db = crate::execution::interpreter::test_support::open_db("relationship-scan-reads").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,99) AS i CREATE (:A {k:i})-[:R {w:i}]->(:B {k:i})",
    ))
    .await
    .unwrap();
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(0,9) AS i CREATE (:A)-[:S]->(:B)",
    ))
    .await
    .unwrap();
    for (query, count, bitmaps, keys) in [
        ("MATCH ()-[r:R]->() RETURN count(r)", 100, 1, 100),
        ("MATCH (a)-[r:R|S|R]->(b) RETURN count(*)", 110, 2, 110),
        // Undirected rows bind both orientations from one endpoint read.
        ("MATCH ()-[r:S]-() RETURN count(r)", 20, 1, 10),
        // A missing type has an empty bitmap.
        ("MATCH ()-[r:Absent]->() RETURN count(r)", 0, 1, 0),
    ] {
        assert!(
            matches!(
                steps(&db, query).as_slice(),
                [r::MatchStep::ScanRelationship { relationship: 0 }]
            ),
            "{query}"
        );
        let result = db.cypher(crate::cypher::Request::new(query)).await.unwrap();
        assert_eq!(result.rows, vec![vec![json!(count)]], "{query}");
        let reads = result.resources.reads;
        assert_eq!(reads.point_gets, bitmaps, "{query}: {reads:?}");
        assert_eq!(reads.multi_get_keys, keys, "{query}: {reads:?}");
        assert_eq!(reads.scans, 0, "{query}: {reads:?}");
    }
    // The first batch follows demand; the returned relationship is hydrated.
    let result = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[r:R]->() RETURN r LIMIT 1",
        ))
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(
        result.resources.reads.multi_get_keys <= 3,
        "{:?}",
        result.resources.reads
    );
    // Checked endpoints and relationship properties are still read.
    for (query, count) in [
        ("MATCH (a {k:3})-[r:R]->(b) RETURN count(*)", 1),
        ("MATCH (a)-[r:R {w:4}]->(b) RETURN count(*)", 1),
        ("MATCH (a)-[r:R]->(b) WHERE b.k < 10 RETURN count(*)", 10),
        (
            "MATCH (a)-[r:R|S]->(b) WHERE type(r) = 'S' RETURN count(*)",
            10,
        ),
    ] {
        let result = db.cypher(crate::cypher::Request::new(query)).await.unwrap();
        assert_eq!(result.rows, vec![vec![json!(count)]], "{query}");
    }
    db.close().await.unwrap();
}

/// The type bitmap proposes edges and their stored endpoints confirm them: an
/// edge whose endpoints record is gone is skipped, while corrupt endpoints or
/// a corrupt bitmap fail the query instead of returning partial rows.
#[tokio::test]
async fn scans_skip_unconfirmed_edges_and_surface_corrupt_records() {
    let db =
        crate::execution::interpreter::test_support::open_db("relationship-scan-records").await;
    let edges = ids(
        &db,
        "UNWIND range(0,3) AS i CREATE (:A)-[r:R]->(:B) RETURN id(r)",
    )
    .await;
    let ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let endpoints = |edge| {
        ctx.storage_key(keys::DataKeyKind::EdgeEndpoints(
            keys::EdgeEndpointsKey::new(edge),
        ))
    };
    db.inner_db().delete(endpoints(edges[0])).await.unwrap();
    let count = || {
        db.cypher(crate::cypher::Request::new(
            "MATCH (a)-[r:R]->(b) RETURN count(*)",
        ))
    };
    assert_eq!(count().await.unwrap().rows, vec![vec![json!(3)]]);
    db.inner_db()
        .put(endpoints(edges[1]), bytes::Bytes::from_static(b"short"))
        .await
        .unwrap();
    assert!(count().await.is_err());
    db.inner_db()
        .put(
            endpoints(edges[1]),
            EdgeEndpointsValue::new(edges[2], edges[3]).encode(),
        )
        .await
        .unwrap();
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::PropertyIndex(
                crate::encoding::indexes::PropertyIndexKey::EdgeLabel(
                    crate::encoding::indexes::label::EdgeLabelKey::new(
                        crate::encoding::indexes::hash_property_value("R"),
                    ),
                ),
            )),
            bytes::Bytes::from_static(b"not a bitmap"),
        )
        .await
        .unwrap();
    assert!(count().await.is_err());
    db.close().await.unwrap();
}

/// Each parent replays every edge. Dropping the stream releases all
/// admission, and an expired deadline stops the scan between batches.
#[tokio::test]
async fn parents_replay_scans_and_release_admission() {
    let db = crate::execution::interpreter::test_support::open_db("relationship-scan-stream").await;
    let mut edges = ids(
        &db,
        "UNWIND range(0,9) AS i CREATE (:A)-[r:R]->(:B) RETURN id(r)",
    )
    .await;
    edges.sort_unstable();
    let pattern = r::Pattern {
        nodes: vec![],
        relationships: vec![
            r::RelationshipPattern {
                slot: r::Slot(2),
                from: r::Slot(0),
                to: r::Slot(1),
                direction: r::Direction::Outgoing,
                types: vec!["R".into()],
                properties: vec![],
            },
            r::RelationshipPattern {
                slot: r::Slot(3),
                from: r::Slot(4),
                to: r::Slot(5),
                direction: r::Direction::Outgoing,
                types: vec!["R".into()],
                properties: vec![],
            },
        ],
        paths: vec![],
    };
    let limits = Limits {
        batch_rows: 4,
        memory_bytes: 256 * 1024,
        ..Default::default()
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.enable_request_read_view().await.unwrap();
    let ids = ctx
        .relationship_scan_ids(&pattern.relationships[0])
        .await
        .unwrap();
    // The second parent already binds one edge, which uniqueness excludes.
    let parents = |ctx: &ExecutionContext<'_>| {
        memory::Rows::new(
            [
                r::Value::Null,
                r::Value::Entity(r::Entity::Relationship(edges[3])),
            ]
            .into_iter()
            .map(|other| {
                let mut row = vec![r::Value::Null; 6];
                row[3] = other;
                row
            })
            .collect(),
            ctx.row_budget(),
        )
        .unwrap()
    };
    let mut batches = Box::pin(ctx.relationship_scan_batches(
        parents(&ctx),
        &pattern,
        0,
        std::sync::Arc::clone(&ids),
        1,
        limits,
    ));
    let mut sizes = Vec::new();
    let mut bound = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        sizes.push(batch.len());
        bound.extend(batch.iter().map(|row| (row[3].clone(), row[2].clone())));
    }
    // Batches grow from the first demand up to the batch width.
    assert_eq!(sizes, [1, 2, 4, 3, 3, 4, 2]);
    let expected = [
        r::Value::Null,
        r::Value::Entity(r::Entity::Relationship(edges[3])),
    ]
    .into_iter()
    .flat_map(|other| {
        edges
            .iter()
            .map(|edge| r::Value::Entity(r::Entity::Relationship(*edge)))
            .filter(|edge| *edge != other)
            .map(|edge| (other.clone(), edge))
            .collect::<Vec<_>>()
    })
    .collect::<Vec<_>>();
    assert_eq!(bound, expected);
    drop(batches);
    drop(ids);
    assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    let ids = ctx
        .relationship_scan_ids(&pattern.relationships[0])
        .await
        .unwrap();
    ctx.fail_deadline_after(2);
    let mut batches =
        Box::pin(ctx.relationship_scan_batches(parents(&ctx), &pattern, 0, ids, 4, limits));
    let result = loop {
        match batches.try_next().await {
            Ok(Some(_)) => {}
            Ok(None) => break Ok(()),
            Err(error) => break Err(error),
        }
    };
    assert!(matches!(
        result,
        Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))
    ));
    drop(batches);
    assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    ctx.close_request_read_view().unwrap();
    db.close().await.unwrap();
}
