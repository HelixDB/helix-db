use crate::{
    encoding::v2::{
        keys,
        values::{
            edge_endpoints,
            property::{self, property_value::PropertyValue as P, Property},
        },
    },
    execution::interpreter::{test_support, ExecutionContext},
    index_lifecycle::graph_mutation::CanonicalPropertyRow,
    query_resources,
};
use helix_planner::{context, relational as r};
use serde_json::json;
use std::collections::BTreeMap;

#[test]
fn canonical_snapshots_retain_raw_and_decoded_owners_and_native_bits() {
    let input = vec![
        Property::string("$label", "N"),
        Property::new("null", P::Null),
        Property::new("bytes", P::Bytes(vec![7; 4096])),
        Property::new("date", P::DateTime(i64::MIN)),
        Property::new("f32", P::F32(f64::from_bits(0x7ff8_0000_0000_0017))),
        Property::new("f64", P::F64(-0.0)),
        Property::new(
            "nested",
            P::Object(BTreeMap::from([(
                "list".into(),
                P::Array(vec![P::Bool(true), P::String("native".into())]),
            )])),
        ),
    ];
    let encoded = property::encode_properties(&input);
    for admitted in [false, true] {
        let budget = query_resources::Budget::new(64 * 1024);
        let row =
            CanonicalPropertyRow::decode_with_budget(encoded.clone(), admitted.then_some(&budget))
                .unwrap();
        assert_eq!(row.encoded(), &encoded);
        assert!(row
            .properties()
            .iter()
            .zip(&input)
            .all(|(a, b)| a.same_v1_representation(b)));
        let remaining = budget.available();
        assert_eq!(remaining == 64 * 1024, !admitted);
        let (copy, allocations) = crate::allocation_testing::observe(|| row.clone());
        assert_eq!(allocations.allocations, 0);
        let raw = copy.encoded().clone();
        drop(row);
        assert_eq!(budget.available(), remaining);
        drop(copy);
        if admitted {
            assert!(budget.available() > remaining && budget.available() < 64 * 1024);
        }
        drop(raw);
        assert_eq!(budget.available(), 64 * 1024);
    }
    let limit = encoded.len() * 2;
    let budget = query_resources::Budget::new(limit);
    assert!(matches!(
        CanonicalPropertyRow::decode_with_budget(encoded, Some(&budget)),
        Err(crate::HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(budget.available(), limit);
}

#[tokio::test]
async fn node_and_edge_observation_batches_retain_snapshots_after_the_batch_drops() {
    let db = test_support::open_db("mutation-snapshot-owners").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let encoded = property::encode_properties(&[
        Property::string("$label", "N"),
        Property::new("large", P::Bytes(vec![7; 32 * 1024])),
    ]);
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                7,
            ))),
            encoded.clone(),
        )
        .await
        .unwrap();
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::EdgePropertyById(
                keys::EdgePropertyByIdKey::new(9),
            )),
            encoded.clone(),
        )
        .await
        .unwrap();
    db.inner_db()
        .put(
            ctx.storage_key(keys::DataKeyKind::EdgeEndpoints(
                keys::EdgeEndpointsKey::new(9),
            )),
            edge_endpoints::EdgeEndpointsValue::new(7, 7).encode(),
        )
        .await
        .unwrap();
    let limit = 512 * 1024;
    ctx.row_memory = Some(query_resources::Budget::new(limit));
    let scope = ctx.take_or_begin_write_scope().await.unwrap();
    let nodes = ctx
        .observe_node_rows(&scope.txn, [7, 7, u64::MAX])
        .await
        .unwrap();
    assert!(nodes.observed(u64::MAX).is_none());
    let node = nodes.observed(7).unwrap();
    let before = ctx.row_budget().available();
    assert!(before < limit - encoded.len() * 2);
    drop(nodes);
    assert!(ctx.row_budget().available() > before && ctx.row_budget().available() < limit);
    assert_eq!(node.encoded(), &encoded);
    drop(node);
    assert_eq!(ctx.row_budget().available(), limit);
    let edges = ctx
        .observe_edge_rows(&scope.txn, [9, 9, u64::MAX])
        .await
        .unwrap();
    let edge = edges.observed(9);
    let before = ctx.row_budget().available();
    drop(edges);
    assert!(ctx.row_budget().available() > before && ctx.row_budget().available() < limit);
    drop(edge);
    assert_eq!(ctx.row_budget().available(), limit);
    let budget = query_resources::Budget::new(1024);
    ctx.row_memory = Some(budget.clone());
    assert!(matches!(
        ctx.observe_node_rows(&scope.txn, [7]).await,
        Err(crate::HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(budget.available(), 1024);
    assert!(matches!(
        ctx.observe_edge_rows(&scope.txn, [9]).await,
        Err(crate::HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(budget.available(), 1024);
    assert!(matches!(
        ctx.observe_node_rows(&scope.txn, 0..1024).await,
        Err(crate::HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(budget.available(), 1024);
    ctx.row_memory = Some(query_resources::Budget::new(0));
    let nodes = ctx.observe_node_rows(&scope.txn, []).await.unwrap();
    assert!(nodes.observed(7).is_none());
    let edges = ctx.observe_edge_rows(&scope.txn, []).await.unwrap();
    drop((nodes, edges));
    assert_eq!(ctx.row_budget().available(), 0);
    drop(scope);
    db.close().await.unwrap();
}

#[tokio::test]
async fn snapshot_limit_failures_rollback_prior_writes_and_success_preserves_native_fields() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("mutation-snapshot-rollback")
            .with_equality_index("N", "key"),
    )
    .await;
    let created = db
        .cypher(crate::cypher::Request::new(
            "CREATE (n:N {key:1})-[r:R {key:1}]->(:Other) RETURN n,r",
        ))
        .await
        .unwrap();
    let node = created.rows[0][0]["id"].as_str().unwrap().parse().unwrap();
    let edge = created.rows[0][1]["id"].as_str().unwrap().parse().unwrap();
    for entity in [r::Entity::Node(node), r::Entity::Relationship(edge)] {
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let key = ctx.storage_key(match entity {
            r::Entity::Node(id) => keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id)),
            r::Entity::Relationship(id) => {
                keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(id))
            }
        });
        let original = db.inner_db().get(&key).await.unwrap().unwrap();
        let mut fields = property::decode_properties(&original).unwrap();
        fields.extend([
            Property::new("native_bytes", P::Bytes(vec![7; 32 * 1024])),
            Property::new("native_date", P::DateTime(17)),
            Property::new("native_float", P::F64(-0.0)),
        ]);
        let original = property::encode_properties(&fields);
        db.inner_db()
            .put(key.clone(), original.clone())
            .await
            .unwrap();
        for value in [None, Some(P::I64(2))] {
            ctx.row_memory = Some(query_resources::Budget::new(16 * 1024));
            ctx.enable_request_write_scope().await.unwrap();
            ctx.row_create_node("Rollback", vec![]).await.unwrap();
            assert!(
                matches!(ctx.row_edit_property(entity,"key",value).await,Err(crate::cypher::Error::Query(error)) if error.detail=="MemoryLimit")
            );
            ctx.abort_request_write_scope();
            assert_eq!(ctx.row_budget().available(), 16 * 1024);
            assert_eq!(db.inner_db().get(&key).await.unwrap().unwrap(), original);
            assert_eq!(
                db.cypher(crate::cypher::Request::new(
                    "MATCH (:Rollback) RETURN count(*)"
                ))
                .await
                .unwrap()
                .rows,
                vec![vec![json!(0)]]
            );
        }
        ctx.row_memory = Some(query_resources::Budget::new(512 * 1024));
        ctx.row_edit_property(entity, "key", Some(P::I64(2)))
            .await
            .unwrap();
        assert_eq!(ctx.row_budget().available(), 512 * 1024);
        let updated =
            property::decode_properties(&db.inner_db().get(&key).await.unwrap().unwrap()).unwrap();
        assert!(updated
            .iter()
            .any(|p| p.name == "key" && p.value == P::I64(2)));
        assert!(fields
            .iter()
            .filter(|p| p.name != "key")
            .all(|before| updated
                .iter()
                .any(|after| before.same_v1_representation(after))));
        ctx.row_edit_property(entity, "key", None).await.unwrap();
        let updated =
            property::decode_properties(&db.inner_db().get(&key).await.unwrap().unwrap()).unwrap();
        assert!(updated.iter().all(|p| p.name != "key"));
        assert!(fields
            .iter()
            .filter(|p| p.name != "key")
            .all(|before| updated
                .iter()
                .any(|after| before.same_v1_representation(after))));
    }
    for key in [1, 2] {
        assert_eq!(
            db.cypher(crate::cypher::Request::new(format!(
                "MATCH (n:N {{key:{key}}}) RETURN count(*)"
            )))
            .await
            .unwrap()
            .rows,
            vec![vec![json!(0)]]
        );
    }
    db.close().await.unwrap();
}
