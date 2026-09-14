//! Canonical row bytes move into the real transaction without another payload.
use crate::{
    cypher,
    encoding::v2::{
        keys,
        values::property::{self, Property},
    },
    execution::interpreter::{mutation::contracts, test_support, ExecutionContext},
    index_lifecycle::graph_mutation::CanonicalPropertyRow,
};
use helix_planner::{context, ir, relational};
use serde_json::json;

#[tokio::test]
async fn canonical_writes_share_payloads_through_edits_and_transaction_completion() {
    let db = test_support::open_db("canonical-owned-writes").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let mut scope = ctx.take_or_begin_write_scope().await.unwrap();
    let native = Property::bytes("native", vec![7; 64 * 1024]);
    ctx.store_node(
        &scope.txn,
        7,
        vec![Property::string("$label", "N"), native.clone()],
        &mut scope.index_context,
    )
    .await
    .unwrap();
    let row = CanonicalPropertyRow::new(vec![Property::string("$label", "R"), native.clone()]);
    ctx.store_edge(
        &scope.txn,
        contracts::EdgeMutationTarget::new(9, 7, 7),
        &ir::NonEmptyString::new("R").unwrap(),
        &row,
        &mut scope.index_context,
    )
    .await
    .unwrap();
    let edge_key = ctx.storage_key(keys::DataKeyKind::EdgePropertyById(
        keys::EdgePropertyByIdKey::new(9),
    ));
    let retained = scope.txn.get(&edge_key).await.unwrap().unwrap();
    assert_eq!(retained.as_ptr(), row.encoded().as_ptr());
    drop(row);
    assert!(property::decode_properties(&retained)
        .unwrap()
        .iter()
        .any(|field| field.same_v1_representation(&native)));
    drop(retained);

    let name = ir::NonEmptyString::new("key").unwrap();
    for entity in [
        relational::Entity::Node(7),
        relational::Entity::Relationship(9),
    ] {
        // Insert, replace and remove each use the authoritative native helpers.
        for value in [Some(1), Some(2), None] {
            let row = match entity {
                relational::Entity::Node(id) => {
                    let observed = ctx.observe_node_rows(&scope.txn, [id]).await.unwrap();
                    match value {
                        Some(value) => ctx
                            .set_node_property_observed(
                                &scope.txn,
                                id,
                                Property::i64("key", value),
                                observed.observed(id),
                                &mut scope.index_context,
                            )
                            .await
                            .unwrap(),
                        None => ctx
                            .remove_node_property_observed(
                                &scope.txn,
                                id,
                                &name,
                                observed.observed(id),
                                &mut scope.index_context,
                            )
                            .await
                            .unwrap()
                            .unwrap(),
                    }
                }
                relational::Entity::Relationship(id) => {
                    let observed = ctx.observe_edge_rows(&scope.txn, [id]).await.unwrap();
                    match value {
                        Some(value) => ctx
                            .set_edge_property_observed(
                                &scope.txn,
                                id,
                                Property::i64("key", value),
                                observed.observed(id),
                                &mut scope.index_context,
                            )
                            .await
                            .unwrap(),
                        None => ctx
                            .remove_edge_property_observed(
                                &scope.txn,
                                id,
                                &name,
                                observed.observed(id),
                                &mut scope.index_context,
                            )
                            .await
                            .unwrap()
                            .unwrap(),
                    }
                }
            };
            let key = ctx.storage_key(match entity {
                relational::Entity::Node(id) => {
                    keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id))
                }
                relational::Entity::Relationship(id) => {
                    keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(id))
                }
            });
            let retained = scope.txn.get(&key).await.unwrap().unwrap();
            assert_eq!(
                retained.as_ptr(),
                row.encoded().as_ptr(),
                "staging must share the already encoded row"
            );
            assert_eq!(retained, *row.encoded());
            drop(row);
            let fields = property::decode_properties(&retained).unwrap();
            assert!(fields
                .iter()
                .any(|field| field.same_v1_representation(&native)));
            assert_eq!(
                fields
                    .iter()
                    .find(|field| field.name == "key")
                    .and_then(|field| field.value.as_i64()),
                value
            );
        }
    }
    ctx.finish_write_scope(scope).await.unwrap();
    assert_eq!(
        db.cypher(cypher::Request::new(
            "MATCH (n:N)-[r:R]->(n) RETURN n.key,r.key"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![json!(null), json!(null)]]
    );
    let committed = db.inner_db().get(&edge_key).await.unwrap().unwrap();
    assert!(property::decode_properties(&committed)
        .unwrap()
        .iter()
        .any(|field| field.same_v1_representation(&native)));
    db.close().await.unwrap();
}
