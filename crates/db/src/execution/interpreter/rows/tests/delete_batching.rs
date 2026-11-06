//! Deletes cost one multi-get per dependency level and one topology flush per
//! batch, and store exactly what deleting each element alone stores.
use super::super::*;
use crate::encoding::v2::keys;
use crate::execution::interpreter::{mutation::DeletionTargets, test_support};
use bytes::Bytes;
use helix_planner::context;
use rand::{rngs::StdRng, RngExt, SeedableRng};
use serde_json::json;

/// Every stored row except index catalog records, which carry a random
/// operation ID per database.
async fn all_rows(db: &crate::HelixDB) -> BTreeMap<Bytes, Bytes> {
    let mut scan = db.inner_db().scan(..).await.unwrap();
    let mut rows = BTreeMap::new();
    while let Some(row) = scan.next().await.unwrap() {
        if !row.key.starts_with(&[6, 1]) && !row.key.starts_with(&[6, 2]) {
            rows.insert(row.key, row.value);
        }
    }
    rows
}

fn targets(ctx: &ExecutionContext<'_>, entities: &[r::Entity]) -> DeletionTargets {
    let mut targets = DeletionTargets::new(ctx.row_budget()).unwrap();
    entities
        .iter()
        .for_each(|entity| targets.insert(*entity).unwrap());
    targets
}

/// A ring of `count` nodes with a parallel pair of edges between neighbors.
async fn ring(ctx: &mut ExecutionContext<'_>, count: usize) -> Vec<r::Entity> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(ctx.row_create_node("N", vec![]).await.unwrap());
    }
    for index in 0..count {
        let (from, to) = (nodes[index], nodes[(index + 1) % count]);
        for label in ["R", "S"] {
            ctx.row_create_edge(from, to, label, vec![]).await.unwrap();
        }
    }
    nodes.into_iter().map(r::Entity::Node).collect()
}

#[tokio::test]
async fn detach_delete_costs_one_read_per_level_and_one_flush_per_batch() {
    // One level for rows, one for pairs, one for edges, whatever the node
    // count. A last batch whose edges went with earlier batches needs only
    // the first.
    for (count, reads, batches) in [
        (1_usize, 3_usize, 1_usize),
        (64, 3, 1),
        (512, 3, 1),
        (513, 4, 2),
        (1025, 7, 3),
    ] {
        let db = test_support::open_db("delete-round-trips").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(64 * 1024 * 1024));
        let nodes = ring(&mut ctx, count).await;
        ctx.enable_request_write_scope().await.unwrap();
        let before = ctx.active_write_tx().unwrap().txn.storage_reads();
        let selected = targets(&ctx, &nodes);
        ctx.row_delete_entities(selected, true).await.unwrap();
        let active = ctx.active_write_tx().unwrap();
        assert_eq!(active.txn.storage_reads() - before, reads, "{count}");
        assert_eq!(active.index_context.topology_staging_flushes(), batches);
        ctx.commit_request_write_scope().await.unwrap();
        assert_eq!(ctx.row_budget().available(), 64 * 1024 * 1024);
        drop(ctx);
        for query in [
            "MATCH (n) RETURN count(n)",
            "MATCH ()-[r]->() RETURN count(r)",
        ] {
            let response = db.cypher(crate::cypher::Request::new(query)).await.unwrap();
            assert_eq!(response.rows, vec![vec![json!(0)]], "{count}");
        }
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn deleting_unattached_nodes_reads_once_and_edges_read_no_pair_twice() {
    let db = test_support::open_db("delete-reads-by-shape").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
    let first = ctx.row_create_node("N", vec![]).await.unwrap();
    let second = ctx.row_create_node("N", vec![]).await.unwrap();
    let kept = ctx.row_create_node("N", vec![]).await.unwrap();
    let joined = ctx.row_create_node("N", vec![]).await.unwrap();
    let edge = ctx
        .row_create_edge(kept, joined, "R", vec![])
        .await
        .unwrap();
    let parallel = ctx
        .row_create_edge(kept, joined, "R", vec![])
        .await
        .unwrap();
    ctx.enable_request_write_scope().await.unwrap();

    let before = ctx.active_write_tx().unwrap().txn.storage_reads();
    let unattached = targets(&ctx, &[r::Entity::Node(first), r::Entity::Node(second)]);
    ctx.row_delete_entities(unattached, false).await.unwrap();
    let after = ctx.active_write_tx().unwrap().txn.storage_reads();
    assert_eq!(after - before, 1);

    // Edge rows, the pair row, and the one parallel edge's label.
    let selected = targets(&ctx, &[r::Entity::Relationship(edge)]);
    ctx.row_delete_entities(selected, false).await.unwrap();
    let active = ctx.active_write_tx().unwrap();
    assert_eq!(active.txn.storage_reads() - after, 3);

    let both = targets(&ctx, &[r::Entity::Relationship(parallel)]);
    ctx.row_delete_entities(both, false).await.unwrap();
    ctx.commit_request_write_scope().await.unwrap();
    drop(ctx);
    let response = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[r]->() RETURN count(r)",
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(0)]]);
    db.close().await.unwrap();
}

/// A deleted node's adjacency row is a tombstone, not an empty row a removal
/// delta brought back, and its neighbors keep their other memberships.
#[tokio::test]
async fn deleted_nodes_leave_no_adjacency_row_and_neighbors_keep_theirs() {
    let db = test_support::open_db("delete-adjacency-tombstones").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(16 * 1024 * 1024));
    let nodes = ring(&mut ctx, 4).await;
    ctx.enable_request_write_scope().await.unwrap();
    let selected = targets(&ctx, &nodes[..2]);
    ctx.row_delete_entities(selected, true).await.unwrap();
    ctx.commit_request_write_scope().await.unwrap();
    let adjacency = |id| ctx.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(id)));
    let [r::Entity::Node(gone), r::Entity::Node(also_gone), r::Entity::Node(kept), ..] = nodes[..]
    else {
        panic!("the ring holds nodes");
    };
    for id in [gone, also_gone] {
        assert_eq!(db.inner_db().get(adjacency(id)).await.unwrap(), None);
    }
    assert!(db.inner_db().get(adjacency(kept)).await.unwrap().is_some());
    drop(ctx);
    let response = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[r]->() RETURN count(r)",
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(2)]]);
    db.close().await.unwrap();
}

/// Each statement combines a delete with state an earlier clause staged.
#[tokio::test]
async fn deletes_observe_everything_the_statement_staged() {
    for (statement, nodes, edges, succeeds) in [
        ("CREATE (a:N)-[r:R]->(b:N) DELETE r, a, b", 0, 0, true),
        ("CREATE (a:N)-[:R]->(:N) DETACH DELETE a", 1, 0, true),
        ("CREATE (a:N)-[:R]->(:N) DELETE a", 0, 0, false),
        ("CREATE (a:N) DELETE a, a CREATE (:N)", 1, 0, true),
        ("CREATE (a:N)-[r:R]->(b:N) DELETE r DELETE a, b", 0, 0, true),
        ("CREATE (a:N)-[r:R]->(a) DETACH DELETE a", 0, 0, true),
        ("CREATE (a:N)-[:R]->(:N) DETACH DELETE a WITH a DELETE a", 1, 0, true),
        ("CREATE (a:N)-[:R]->(b:N), (a)-[:R]->(b) MATCH (a)-[r]->(b) WITH a, b, collect(r)[0] AS r DELETE r", 2, 1, true),
    ] {
        let db = test_support::open_db("delete-statement-overlay").await;
        let result = db.cypher(crate::cypher::Request::new(statement)).await;
        assert_eq!(result.is_ok(), succeeds, "{statement}: {result:?}");
        for (query, expected) in [
            ("MATCH (n) RETURN count(n)", nodes),
            ("MATCH ()-[r]->() RETURN count(r)", edges),
        ] {
            let response = db.cypher(crate::cypher::Request::new(query)).await.unwrap();
            assert_eq!(response.rows, vec![vec![json!(expected)]], "{statement}");
        }
        db.close().await.unwrap();
    }
}

/// A refused delete reports the attached node after every selected edge has
/// been accounted for, across batches, and stores nothing.
#[tokio::test]
async fn refused_deletes_consider_selected_edges_and_commit_nothing() {
    for (select_edge, succeeds) in [(false, false), (true, true)] {
        let db = test_support::open_db("delete-refusals").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(memory::Budget::new(64 * 1024 * 1024));
        let mut entities = Vec::new();
        for _ in 0..600 {
            let id = ctx.row_create_node("N", vec![]).await.unwrap();
            entities.push(r::Entity::Node(id));
        }
        let [.., r::Entity::Node(from), r::Entity::Node(to)] = entities[..] else {
            panic!("the graph holds nodes");
        };
        let edge = ctx.row_create_edge(from, to, "R", vec![]).await.unwrap();
        if select_edge {
            entities.push(r::Entity::Relationship(edge));
        }
        let before = all_rows(&db).await;
        ctx.enable_request_write_scope().await.unwrap();
        let selected = targets(&ctx, &entities);
        let result = ctx.row_delete_entities(selected, false).await;
        assert_eq!(result.is_ok(), succeeds);
        if succeeds {
            ctx.commit_request_write_scope().await.unwrap();
            drop(ctx);
            let response = db
                .cypher(crate::cypher::Request::new("MATCH (n) RETURN count(n)"))
                .await
                .unwrap();
            assert_eq!(response.rows, vec![vec![json!(0)]]);
        } else {
            let Err(Error::Query(error)) = result else {
                panic!("a typed refusal");
            };
            assert_eq!(error.detail, "delete_connected_node");
            ctx.abort_request_write_scope();
            drop(ctx);
            assert_eq!(all_rows(&db).await, before);
        }
        db.close().await.unwrap();
    }
}

/// A batch ends at the node reaching the pair limit, so hubs delete in smaller
/// batches, and the budget shrinks a batch of rows too large to hold at once.
#[tokio::test]
async fn batches_shrink_to_hub_size_and_to_the_memory_budget() {
    // Three hubs of 3000 neighbors reach the limit at their second node.
    let db = test_support::open_db("delete-hub-batches").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(256 * 1024 * 1024));
    ctx.enable_request_write_scope().await.unwrap();
    let mut hubs = Vec::new();
    for _ in 0..3 {
        let hub = ctx.row_create_node("Hub", vec![]).await.unwrap();
        for _ in 0..3000 {
            let spoke = ctx.row_create_node("Spoke", vec![]).await.unwrap();
            ctx.row_create_edge(hub, spoke, "R", vec![]).await.unwrap();
        }
        hubs.push(r::Entity::Node(hub));
    }
    ctx.commit_request_write_scope().await.unwrap();
    ctx.enable_request_write_scope().await.unwrap();
    let selected = targets(&ctx, &hubs);
    ctx.row_delete_entities(selected, true).await.unwrap();
    let active = ctx.active_write_tx().unwrap();
    assert_eq!(active.index_context.topology_staging_flushes(), 2);
    ctx.commit_request_write_scope().await.unwrap();
    drop(ctx);
    let response = db
        .cypher(crate::cypher::Request::new(
            "MATCH ()-[r]->() RETURN count(r)",
        ))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(0)]]);
    db.close().await.unwrap();

    // Each row fits the budget, and all of them together do not.
    let db = test_support::open_db("delete-budget-batches").await;
    let mut seed = ExecutionContext::new(&db, context::ParamBindings::default());
    seed.row_memory = Some(memory::Budget::new(64 * 1024 * 1024));
    seed.enable_request_write_scope().await.unwrap();
    let mut nodes = Vec::new();
    for _ in 0..32 {
        let payload =
            crate::encoding::v2::values::property::Property::bytes("v", vec![7; 128 * 1024]);
        let id = seed.row_create_node("N", vec![payload]).await.unwrap();
        nodes.push(r::Entity::Node(id));
    }
    seed.commit_request_write_scope().await.unwrap();
    drop(seed);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(2 * 1024 * 1024));
    ctx.enable_request_write_scope().await.unwrap();
    let selected = targets(&ctx, &nodes);
    ctx.row_delete_entities(selected, false).await.unwrap();
    let active = ctx.active_write_tx().unwrap();
    assert!(active.index_context.topology_staging_flushes() > 1);
    ctx.commit_request_write_scope().await.unwrap();
    drop(ctx);
    let response = db
        .cypher(crate::cypher::Request::new("MATCH (n) RETURN count(n)"))
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(0)]]);
    db.close().await.unwrap();
}

/// Random graphs of parallel, self-loop and two-label edges, all indexed.
async fn differential_graph(
    name: &str,
    seed: u64,
    count: usize,
) -> (crate::HelixDB, Vec<u64>, Vec<u64>) {
    let config = test_support::in_memory_config(name)
        .with_equality_index("A", "k")
        .with_edge_equality_index("R", "k");
    let db = test_support::open_db_with_config(config).await;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(256 * 1024 * 1024));
    ctx.enable_request_write_scope().await.unwrap();
    let mut nodes = Vec::new();
    for index in 0..count {
        let label = ["A", "B"][index % 2];
        let property = crate::encoding::v2::values::property::Property::i64("k", index as i64 % 2);
        nodes.push(ctx.row_create_node(label, vec![property]).await.unwrap());
    }
    let mut edges = Vec::new();
    for _ in 0..rng.random_range(0..count * 2 + 2) {
        let from = nodes[rng.random_range(0..count)];
        let to = nodes[rng.random_range(0..count)];
        let label = ["R", "S"][rng.random_range(0..2)];
        let property = crate::encoding::v2::values::property::Property::i64("k", 1);
        edges.push(
            ctx.row_create_edge(from, to, label, vec![property])
                .await
                .unwrap(),
        );
    }
    ctx.commit_request_write_scope().await.unwrap();
    drop(ctx);
    (db, nodes, edges)
}

const DIFFERENTIAL_SEEDS: u64 = 32;

/// Deleting one element at a time is the sequential contract. A batch must
/// match it in outcome and in every stored byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn batched_deletes_store_the_same_bytes_as_deleting_each_element_alone() {
    // A graph of 600 nodes crosses a batch boundary.
    let seeds = (0..DIFFERENTIAL_SEEDS)
        .map(|seed| tokio::spawn(delete_differential(seed, 6)))
        .chain((0..4).map(|seed| tokio::spawn(delete_differential(seed, 600))))
        .collect::<Vec<_>>();
    for seed in seeds {
        seed.await.unwrap();
    }
}

async fn delete_differential(seed: u64, count: usize) {
    let (batched_db, nodes, edges) =
        differential_graph(&format!("delete-batched-{seed}-{count}"), seed, count).await;
    let (alone_db, ..) =
        differential_graph(&format!("delete-alone-{seed}-{count}"), seed, count).await;
    let mut rng = StdRng::seed_from_u64(seed ^ 0x5eed);
    let mut selected = nodes
        .iter()
        .filter(|_| rng.random_bool(0.5))
        .map(|id| r::Entity::Node(*id))
        .collect::<Vec<_>>();
    selected.extend(
        edges
            .iter()
            .filter(|_| rng.random_bool(0.5))
            .map(|id| r::Entity::Relationship(*id)),
    );
    let detach = rng.random_bool(0.5);

    let mut batched_ctx = ExecutionContext::new(&batched_db, context::ParamBindings::default());
    batched_ctx.row_memory = Some(memory::Budget::new(256 * 1024 * 1024));
    batched_ctx.enable_request_write_scope().await.unwrap();
    let chosen = targets(&batched_ctx, &selected);
    let batched = batched_ctx.row_delete_entities(chosen, detach).await;

    let mut alone_ctx = ExecutionContext::new(&alone_db, context::ParamBindings::default());
    alone_ctx.row_memory = Some(memory::Budget::new(256 * 1024 * 1024));
    alone_ctx.enable_request_write_scope().await.unwrap();
    // Selected edges go first, so an attachment check never counts them.
    let (mut alone_order, alone_nodes): (Vec<_>, Vec<_>) = selected
        .iter()
        .copied()
        .partition(|entity| matches!(entity, r::Entity::Relationship(_)));
    alone_order.extend(alone_nodes);
    let mut alone = Ok(());
    for entity in alone_order {
        let chosen = targets(&alone_ctx, &[entity]);
        alone = alone_ctx.row_delete_entities(chosen, detach).await;
        if alone.is_err() {
            break;
        }
    }
    assert_eq!(
        batched.as_ref().map_err(ToString::to_string),
        alone.as_ref().map_err(ToString::to_string),
        "seed {seed}"
    );
    if batched.is_ok() {
        batched_ctx.commit_request_write_scope().await.unwrap();
        alone_ctx.commit_request_write_scope().await.unwrap();
    } else {
        batched_ctx.abort_request_write_scope();
        alone_ctx.abort_request_write_scope();
    }
    drop(batched_ctx);
    drop(alone_ctx);
    let (batched_rows, alone_rows) = (all_rows(&batched_db).await, all_rows(&alone_db).await);
    let differing = batched_rows
        .keys()
        .chain(alone_rows.keys())
        .filter(|key| batched_rows.get(*key) != alone_rows.get(*key))
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        differing.is_empty(),
        "seed {seed}: {:#?}",
        differing
            .iter()
            .map(|key| (key, batched_rows.get(*key), alone_rows.get(*key)))
            .collect::<Vec<_>>()
    );
    batched_db.close().await.unwrap();
    alone_db.close().await.unwrap();
}
