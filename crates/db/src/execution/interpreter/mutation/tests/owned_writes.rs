//! Canonical row bytes move into the real transaction without another payload.
use crate::{
    cypher,
    encoding::v2::{
        keys,
        values::property::{self, Property},
    },
    execution::interpreter::{
        mutation::{contracts, MutationIndexContext},
        test_support, ExecutionContext,
    },
    index_lifecycle::graph_mutation::CanonicalPropertyRow,
    query_resources,
};
use helix_planner::{context, ir, relational};
use serde_json::json;
use slatedb::DbReadOps;

/// Every storage helper admits its observations, rewritten rows and pending
/// row write before building them. Sweeping the spare budget upward from zero
/// reaches each admission point in order: every shortfall fails with
/// `QueryMemoryLimitExceeded`, dropping the transaction releases everything,
/// and a sufficient allowance succeeds.
#[tokio::test]
async fn storage_helpers_fail_cleanly_across_admission_shortfalls() {
    #[derive(Debug, Clone, Copy)]
    enum Operation {
        Store,
        Set,
        Remove,
        Delete,
    }
    let db = test_support::open_db("canonical-write-admission").await;
    let mut seed = ExecutionContext::new(&db, context::ParamBindings::default());
    // Edits keep the payload, so each rewritten row needs more than the
    // observation buffers released before it.
    let payload = Property::bytes("payload", vec![7; 1024]);
    let node = seed
        .row_create_node("N", vec![payload.clone(), Property::i64("extra", 1)])
        .await
        .unwrap();
    let endpoint = seed.row_create_node("N", vec![]).await.unwrap();
    let edge = seed
        .row_create_edge(
            endpoint,
            endpoint,
            "R",
            vec![payload.clone(), Property::i64("extra", 1)],
        )
        .await
        .unwrap();
    let limit = 1024 * 1024;
    let budget = query_resources::Budget::new(limit);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(budget.clone());
    let label = ir::NonEmptyString::new("R").unwrap();
    let extra = ir::NonEmptyString::new("extra").unwrap();
    let edge_row =
        CanonicalPropertyRow::new(vec![Property::string("$label", "R"), payload.clone()]);
    for entity in [
        relational::Entity::Node(node),
        relational::Entity::Relationship(edge),
    ] {
        for operation in [
            Operation::Store,
            Operation::Set,
            Operation::Remove,
            Operation::Delete,
        ] {
            // A 16-byte stride keeps the sweep short; every owner on these
            // paths admits more than that, so each one is still reached.
            let mut allowances = (0..limit).step_by(16);
            let mut shortfalls = 0;
            loop {
                let allowance = allowances
                    .next()
                    .expect("the operation fits within the request budget");
                let txn = db
                    .inner_db()
                    .begin(slatedb::IsolationLevel::Snapshot)
                    .await
                    .unwrap();
                let mut indexes = MutationIndexContext::for_configured_index_test();
                let occupied = budget.reserve(limit - allowance).unwrap();
                let result = match (entity, operation) {
                    (relational::Entity::Node(_), Operation::Store) => {
                        ctx.store_node(
                            &txn,
                            u64::MAX,
                            vec![Property::string("$label", "N"), payload.clone()],
                            &mut indexes,
                        )
                        .await
                    }
                    (relational::Entity::Node(id), Operation::Set) => {
                        async {
                            let observed = ctx.observe_node_rows(&txn, [id]).await?;
                            ctx.set_node_property_observed(
                                &txn,
                                id,
                                Property::i64("added", 1),
                                observed.observed(id),
                                &mut indexes,
                            )
                            .await
                            .map(drop)
                        }
                        .await
                    }
                    (relational::Entity::Node(id), Operation::Remove) => {
                        async {
                            let observed = ctx.observe_node_rows(&txn, [id]).await?;
                            ctx.remove_node_property_observed(
                                &txn,
                                id,
                                &extra,
                                observed.observed(id),
                                &mut indexes,
                            )
                            .await
                            .map(drop)
                        }
                        .await
                    }
                    (relational::Entity::Node(id), Operation::Delete) => {
                        ctx.delete_node(&txn, id, &mut indexes).await
                    }
                    (relational::Entity::Relationship(_), Operation::Store) => {
                        ctx.store_edge(
                            &txn,
                            contracts::EdgeMutationTarget::new(u64::MAX, endpoint, endpoint),
                            &label,
                            &edge_row,
                            &mut indexes,
                        )
                        .await
                    }
                    (relational::Entity::Relationship(id), Operation::Set) => {
                        async {
                            let observed = ctx.observe_edge_rows(&txn, [id]).await?;
                            ctx.set_edge_property_observed(
                                &txn,
                                id,
                                Property::i64("added", 1),
                                observed.observed(id),
                                &mut indexes,
                            )
                            .await
                            .map(drop)
                        }
                        .await
                    }
                    (relational::Entity::Relationship(id), Operation::Remove) => {
                        async {
                            let observed = ctx.observe_edge_rows(&txn, [id]).await?;
                            ctx.remove_edge_property_observed(
                                &txn,
                                id,
                                &extra,
                                observed.observed(id),
                                &mut indexes,
                            )
                            .await
                            .map(drop)
                        }
                        .await
                    }
                    (relational::Entity::Relationship(id), Operation::Delete) => {
                        ctx.delete_edge(&txn, id, &mut indexes).await
                    }
                };
                drop((occupied, indexes, txn));
                assert_eq!(
                    budget.available(),
                    limit,
                    "{entity:?} {operation:?} retained admission at {allowance} spare bytes"
                );
                let Err(error) = result else {
                    break;
                };
                assert!(
                    matches!(error, crate::HelixDbError::QueryMemoryLimitExceeded),
                    "{entity:?} {operation:?} at {allowance} spare bytes: {error:?}"
                );
                shortfalls += 1;
            }
            assert!(
                shortfalls > 0,
                "{entity:?} {operation:?} must admit before writing"
            );
        }
    }
    db.close().await.unwrap();
}

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
