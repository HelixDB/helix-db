use crate::{
    allocation_testing, cypher,
    encoding::v2::{
        keys,
        values::property::{self, property_value::PropertyValue, Property},
    },
    execution::interpreter::{test_support, ExecutionContext},
    index_lifecycle::graph_mutation::{self, map},
    query_resources,
};
use helix_ast::query::QueryValue;
use helix_planner::{context, relational};
use serde_json::json;
use std::collections::BTreeMap;

#[tokio::test]
async fn map_updates_observe_one_row_per_entity_independent_of_map_size_and_preserve_native_fields()
{
    let db = test_support::open_db("map-update-observations").await;
    let mut seed = ExecutionContext::new(&db, context::ParamBindings::default());
    let node = seed
        .row_create_node("N", vec![Property::bytes("native", vec![7; 128])])
        .await
        .unwrap();
    let edge = seed
        .row_create_edge(
            node,
            node,
            "R",
            vec![Property::bytes("native", vec![8; 128])],
        )
        .await
        .unwrap();
    for (entity, graph_entity, native) in [
        (
            relational::Entity::Node(node),
            graph_mutation::GraphEntity::node(node),
            7,
        ),
        (
            relational::Entity::Relationship(edge),
            graph_mutation::GraphEntity::edge(edge),
            8,
        ),
    ] {
        let mut previous_reads = None;
        for count in [1, 32, 256] {
            let budget = query_resources::Budget::new(4 * 1024 * 1024);
            let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
            ctx.row_memory = Some(budget.clone());
            let mut edit = map::Edit::new(map::Mode::Extend);
            for i in 0..count {
                edit.insert(format!("key_{i}"), Some(PropertyValue::I64(i)))
                    .unwrap();
            }
            ctx.row_edit_map(entity, edit).await.unwrap();
            let reads = budget.reads();
            assert_eq!(
                reads.multi_get_batches, 1,
                "one canonical observation for {count} keys"
            );
            drop(ctx);
            assert_eq!(budget.available(), 4 * 1024 * 1024);
            let encoded = db
                .inner_db()
                .get(graph_entity.property_key(keys::scope::DataScope::LegacyUnscoped))
                .await
                .unwrap()
                .unwrap();
            let stored = property::decode_properties(&encoded).unwrap();
            assert!(stored
                .iter()
                .any(|p| p.same_v1_representation(&Property::bytes("native", vec![native; 128]))));
            assert_eq!(stored.len(), count as usize + 2);
            let Some(previous) = previous_reads.replace(reads) else {
                continue;
            };
            assert_eq!(reads, previous);
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn create_metadata_is_admitted_before_copying_labels_or_allocating_ids() {
    let db = test_support::open_db("metadata-admission").await;
    let label = "L".repeat(32 * 1024);
    let runtime = tokio::runtime::Handle::current();
    for spare in [false, true] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(query_resources::Budget::new(0));
        for edge in [false, true] {
            let properties = Vec::with_capacity(usize::from(spare));
            // The rejection occurs on the first poll; no storage future is run.
            let (result, allocations) = allocation_testing::observe(|| {
                let _entered = runtime.enter();
                if edge {
                    futures::executor::block_on(ctx.row_create_edge(1, 1, &label, properties))
                        .map(|_| ())
                } else {
                    futures::executor::block_on(ctx.row_create_node(&label, properties)).map(|_| ())
                }
            });
            assert!(
                matches!(result, Err(cypher::Error::Query(ref error)) if error.detail == "MemoryLimit")
            );
            assert!(
                allocations.bytes < label.len(),
                "metadata was allocated before admission: {allocations:?}"
            );
        }
        let error = ctx.row_create_edge(1, 1, "", vec![]).await.unwrap_err();
        assert!(
            matches!(error, cypher::Error::Query(ref error) if error.detail == "NoRelationshipType")
        );
    }
    assert_eq!(
        db.cypher(cypher::Request::new("MATCH (n) RETURN count(n)"))
            .await
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn maps_keep_separate_item_order_and_rollback_every_error_without_changing_metadata() {
    let db = test_support::open_db("map-update-semantics").await;
    let query = "CREATE (n:N {a:1, old:9})-[r:R {a:2, old:8}]->(m:M) SET n += {a:3,b:4,old:null}, n.c=n.a+1, r={a:n.a,b:n.c,missing:null} RETURN properties(n),properties(r),labels(n),type(r)";
    assert_eq!(
        db.cypher(cypher::Request::new(query)).await.unwrap().rows,
        vec![vec![
            json!({"a":3,"b":4,"c":4}),
            json!({"a":3,"b":4}),
            json!(["N"]),
            json!("R")
        ]]
    );
    for query in [
        "MATCH (n:N)-[r:R]->() SET n=r, r=n RETURN properties(n),properties(r)",
        "MATCH (n:N)-[r:R]->() SET n=n, r+=r RETURN properties(n),properties(r)",
    ] {
        assert_eq!(
            db.cypher(cypher::Request::new(query)).await.unwrap().rows,
            vec![vec![json!({"a":3,"b":4}), json!({"a":3,"b":4})]]
        );
    }
    for query in [
        "MATCH (n:N)-[r:R]->() SET n={a:99},r={a:100} RETURN 1/0",
        "MATCH (n:N)-[r:R]->() SET n={a:99},r+={b:100} DELETE n",
        "MATCH (n:N)-[r:R]->() SET n={a:99},r+={`$label`:'CHANGED'}",
    ] {
        assert!(
            db.cypher(cypher::Request::new(query)).await.is_err(),
            "{query}"
        );
        assert_eq!(
            db.cypher(cypher::Request::new(
                "MATCH (n:N)-[r:R]->() RETURN properties(n),properties(r)"
            ))
            .await
            .unwrap()
            .rows,
            vec![vec![json!({"a":3,"b":4}), json!({"a":3,"b":4})]]
        );
    }
    for (entries, detail) in [
        (
            BTreeMap::from([
                ("$reserved".to_owned(), QueryValue::I64(1)),
                ("later".into(), QueryValue::Object(BTreeMap::new())),
            ]),
            "ReservedPropertyName",
        ),
        (
            BTreeMap::from([("".to_owned(), QueryValue::I64(1))]),
            "EmptyPropertyName",
        ),
    ] {
        let mut request = cypher::Request::new("MATCH (n:N) SET n += $map");
        request
            .parameters
            .insert("map".into(), QueryValue::Object(entries));
        let error = db.cypher(request).await.unwrap_err();
        assert!(
            matches!(error, cypher::Error::Query(ref error) if error.detail == detail),
            "{error:?}"
        );
    }
    assert_eq!(db.cypher(cypher::Request::new("MATCH (n:N)-[r:R]->() SET n=null, r={} RETURN properties(n),properties(r),labels(n),type(r)")).await.unwrap().rows, vec![vec![json!({}),json!({}),json!(["N"]),json!("R")]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn map_admission_failure_and_cancelled_requests_release_all_versions_and_rollback() {
    let db = test_support::open_db("map-resource-rollback").await;
    let mut seed = ExecutionContext::new(&db, context::ParamBindings::default());
    let mut nodes = Vec::new();
    for i in 0..20 {
        nodes.push(
            seed.row_create_node("N", vec![Property::i64("original", i)])
                .await
                .unwrap(),
        );
    }
    let limit = 128 * 1024;
    let budget = query_resources::Budget::new(limit);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(budget.clone());
    ctx.enable_request_write_scope().await.unwrap();
    let mut completed = 0;
    for id in &nodes {
        let mut edit = map::Edit::new(map::Mode::ReplaceUserProperties);
        edit.insert("payload".into(), Some(PropertyValue::Bytes(vec![1; 8192])))
            .unwrap();
        match ctx.row_edit_map(relational::Entity::Node(*id), edit).await {
            Ok(()) => completed += 1,
            Err(error) => {
                assert!(
                    matches!(error, cypher::Error::Query(ref error) if error.detail == "MemoryLimit"),
                    "{error:?}"
                );
                break;
            }
        }
    }
    assert!(
        completed > 0 && completed < nodes.len(),
        "several map versions must stage before exhausting the budget"
    );
    ctx.abort_request_write_scope();
    assert_eq!(budget.available(), limit);
    assert_eq!(
        db.cypher(cypher::Request::new(
            "MATCH (n:N) RETURN count(n.original),count(n.payload)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(20), json!(0)]]
    );
    ctx.enable_request_write_scope().await.unwrap();
    let mut edit = map::Edit::new(map::Mode::ReplaceUserProperties);
    edit.insert("payload".into(), Some(PropertyValue::Bytes(vec![2; 8192])))
        .unwrap();
    ctx.row_edit_map(relational::Entity::Node(nodes[0]), edit)
        .await
        .unwrap();
    assert!(budget.available() < limit);
    drop(ctx);
    assert_eq!(budget.available(), limit);
    assert_eq!(
        db.cypher(cypher::Request::new(
            "MATCH (n:N) RETURN count(n.original),count(n.payload)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(20), json!(0)]]
    );
    for entity in [
        relational::Entity::Node(u64::MAX),
        relational::Entity::Relationship(u64::MAX),
    ] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_memory = Some(budget.clone());
        let mut edit = map::Edit::new(map::Mode::Extend);
        edit.insert("a".into(), Some(PropertyValue::I64(1)))
            .unwrap();
        assert!(ctx.row_edit_map(entity, edit).await.is_err());
        drop(ctx);
        assert_eq!(budget.available(), limit);
    }
    db.close().await.unwrap();
}
