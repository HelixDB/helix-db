use super::*;
use crate::{
    allocation_testing,
    encoding::v2::values::property::{self, Property},
    execution::interpreter::{test_support, ExecutionContext},
    index_lifecycle::graph_mutation::CanonicalPropertyRow,
};
use helix_planner::context;

#[tokio::test]
async fn pending_property_versions_coalesce_without_releasing_shared_index_owners() {
    let db = test_support::open_db("pending-property-versions").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    let budget = query_resources::Budget::new(1024 * 1024);
    context.row_memory = Some(budget.clone());
    let mut scope = context.take_or_begin_write_scope().await.unwrap();
    let entity = GraphEntity::node(7);
    let row = CanonicalPropertyRow::new_with_budget(
        vec![Property::bytes("native", vec![7; 65536])],
        Some(&budget),
    )
    .unwrap();
    let retained_index_version = row.clone();
    let writes = &mut scope.index_context.property_writes;
    let before = budget.available();
    let (result, allocated) = allocation_testing::observe(|| {
        writes.stage(
            &scope.txn,
            context.tenant_scope,
            entity,
            Some(row.write_payload()),
            Some(&budget),
        )
    });
    result.unwrap();
    assert!(allocated.bytes <= before - budget.available());
    drop(row);
    let pending = budget.available();
    let replacement =
        CanonicalPropertyRow::new_with_budget(vec![Property::i64("small", 1)], Some(&budget))
            .unwrap();
    writes
        .stage(
            &scope.txn,
            context.tenant_scope,
            entity,
            Some(replacement.write_payload()),
            Some(&budget),
        )
        .unwrap();
    drop(replacement);
    assert!(
        budget.available() < pending,
        "the old index version remains live"
    );
    drop(retained_index_version);
    let coalesced = budget.available();
    assert!(coalesced > pending + 65536);
    for _ in 0..100 {
        let row =
            CanonicalPropertyRow::new_with_budget(vec![Property::i64("small", 1)], Some(&budget))
                .unwrap();
        writes
            .stage(
                &scope.txn,
                context.tenant_scope,
                entity,
                Some(row.write_payload()),
                Some(&budget),
            )
            .unwrap();
        drop(row);
        assert_eq!(
            budget.available(),
            coalesced,
            "overwrites do not accumulate ledger entries"
        );
    }
    assert_eq!(writes.0.as_ref().unwrap().rows.len(), 1);
    writes
        .stage(
            &scope.txn,
            context.tenant_scope,
            entity,
            None,
            Some(&budget),
        )
        .unwrap();
    assert!(scope
        .txn
        .get(entity.property_key(context.tenant_scope))
        .await
        .unwrap()
        .is_none());
    assert!(budget.available() > coalesced);
    drop(scope);
    assert_eq!(budget.available(), 1024 * 1024);
    db.close().await.unwrap();
}

#[tokio::test]
async fn pending_property_admission_precedes_backend_writes_and_survives_preparation() {
    let db = test_support::open_db("pending-property-commit").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    let budget = query_resources::Budget::new(128 * 1024);
    context.row_memory = Some(budget.clone());
    let mut scope = context.take_or_begin_write_scope().await.unwrap();
    let scope_id = context.tenant_scope;
    let entity = GraphEntity::edge(9);
    let prepared = property::write::Prepared::new(&[]).unwrap();
    let encoded = properties::Encoded::new(&prepared, &budget).unwrap();
    let full = budget.reserve(budget.available()).unwrap();
    let (result, allocation) = allocation_testing::observe(|| {
        scope.index_context.property_writes.stage(
            &scope.txn,
            scope_id,
            entity,
            Some(encoded),
            Some(&budget),
        )
    });
    assert!(matches!(
        result,
        Err(crate::HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocation.allocations, 0);
    drop(full);
    assert!(scope
        .txn
        .get(entity.property_key(scope_id))
        .await
        .unwrap()
        .is_none());
    let row = CanonicalPropertyRow::new_with_budget(
        vec![Property::bytes("native", vec![1; 4096])],
        Some(&budget),
    )
    .unwrap();
    scope
        .index_context
        .property_writes
        .stage(
            &scope.txn,
            scope_id,
            entity,
            Some(row.write_payload()),
            Some(&budget),
        )
        .unwrap();
    let storage_bytes = row.encoded().clone();
    drop(row);
    scope
        .index_context
        .prepare_topology(&scope.txn)
        .await
        .unwrap();
    scope
        .index_context
        .prepare_secondary(&scope.txn)
        .await
        .unwrap();
    scope
        .index_context
        .prepare_active_text(
            &scope.txn,
            db.config()
                .db()
                .search_index_backfill()
                .active_text_mutation(),
            db.object_store(),
            db.path(),
        )
        .await
        .unwrap();
    scope
        .index_context
        .prepare_active_vectors(&scope.txn)
        .await
        .unwrap();
    let before = budget.available();
    let prepared = scope.index_context.into_prepared().unwrap();
    assert_eq!(
        budget.available(),
        before,
        "sealing runtimes retains pending property admission"
    );
    scope.txn.commit().await.unwrap();
    assert_eq!(budget.available(), before);
    drop(prepared);
    assert_eq!(budget.available(), 128 * 1024);
    assert_eq!(
        db.inner_db()
            .get(entity.property_key(scope_id))
            .await
            .unwrap()
            .unwrap(),
        storage_bytes
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn accumulating_property_writes_fail_cleanly_and_rollback_the_whole_statement() {
    let db = test_support::open_db("pending-property-resource-rollback").await;
    let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
    let limit = 128 * 1024;
    context.row_memory = Some(query_resources::Budget::new(limit));
    context.enable_request_write_scope().await.unwrap();
    let mut completed = 0;
    let error = loop {
        let result = context
            .row_create_node("Pending", vec![Property::bytes("native", vec![7; 8192])])
            .await;
        match result {
            Ok(_) => {
                completed += 1;
                assert!(
                    completed < 20,
                    "pending property bytes must exhaust this budget"
                );
            }
            Err(error) => break error,
        }
    };
    assert!(
        completed > 0,
        "the limit failure must follow successful staged writes"
    );
    assert!(matches!(error, crate::cypher::Error::Query(error) if error.detail == "MemoryLimit"));
    context.abort_request_write_scope();
    assert_eq!(context.row_budget().available(), limit);
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (:Pending) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![serde_json::json!(0)]]
    );
    // Dropping a suspended request's active transaction has the same ownership
    // contract as an explicit abort and cannot leave a reservation behind.
    context.enable_request_write_scope().await.unwrap();
    context
        .row_create_node("Cancelled", vec![Property::bytes("native", vec![1; 8192])])
        .await
        .unwrap();
    let budget = context.row_budget().clone();
    assert!(budget.available() < limit);
    drop(context);
    assert_eq!(budget.available(), limit);
    assert_eq!(
        db.cypher(crate::cypher::Request::new(
            "MATCH (:Cancelled) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        vec![vec![serde_json::json!(0)]]
    );
    db.close().await.unwrap();
}
