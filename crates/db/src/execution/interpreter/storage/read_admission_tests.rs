use super::*;
use crate::index_lifecycle::graph_mutation::GraphEntity;
use helix_planner::context;

#[tokio::test]
async fn retained_transaction_reads_keep_admission_after_values_are_dropped() {
    let db = test_support::open_db("retained-point-read-admission").await;
    let memory_bytes = 1024 * 1024;
    let budget = crate::query_resources::Budget::new(memory_bytes);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(budget.clone());
    ctx.enable_request_write_scope().await.unwrap();
    let before = budget.available();
    let mut payload_bytes = 0;
    for id in 0..128 {
        let key = GraphEntity::node(id).property_key(ctx.tenant_scope);
        payload_bytes += key.len();
        assert!(ctx.get_raw(&key).await.unwrap().is_none());
    }
    let retained = before - budget.available();
    ctx.abort_request_write_scope();
    assert_eq!(budget.available(), memory_bytes);
    db.close().await.unwrap();
    assert!(
        retained >= payload_bytes,
        "serializable read keys escaped admission: retained {retained}, key payload {payload_bytes}"
    );
}

#[tokio::test]
async fn retained_transaction_reads_include_empty_native_scan_ranges() {
    let db = test_support::open_db("retained-empty-range-admission").await;
    let memory_bytes = 1024 * 1024;
    let budget = crate::query_resources::Budget::new(memory_bytes);
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(budget.clone());
    ctx.enable_request_write_scope().await.unwrap();
    let before = budget.available();
    let mut payload_bytes = 0;
    for id in 0..32 {
        let start = GraphEntity::node(id).property_key(ctx.tenant_scope);
        let end = GraphEntity::node(id + 1).property_key(ctx.tenant_scope);
        payload_bytes += start.len() + end.len();
        let mut scan = ctx
            .active_write_tx()
            .unwrap()
            .txn
            .scan(start..end)
            .await
            .unwrap();
        assert!(scan.next().await.unwrap().is_none());
    }
    let retained = before - budget.available();
    ctx.abort_request_write_scope();
    assert_eq!(budget.available(), memory_bytes);
    db.close().await.unwrap();
    assert!(
        retained >= payload_bytes,
        "empty serializable ranges escaped admission: retained {retained}, bound payload {payload_bytes}"
    );
}

#[tokio::test]
async fn streaming_write_read_sets_hit_the_budget_and_roll_back_all_graph_changes() {
    use crate::cypher;
    let db = test_support::open_db("streamed-transaction-read-limit").await;
    db.cypher(cypher::Request::new(
        "UNWIND range(0,1023) AS value CREATE (:ReadAdmission {value:value})",
    ))
    .await
    .unwrap();
    let limits = cypher::Limits {
        batch_rows: 8,
        ..Default::default()
    };
    let read = cypher::execute(
        &db,
        cypher::Request::new("MATCH (n:ReadAdmission) RETURN count(n.value) AS total"),
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        limits,
    )
    .await
    .unwrap();
    assert_eq!(read.rows, vec![vec![serde_json::json!(1024)]]);
    let query = "CREATE (:ReadMarker) WITH 1 AS marker MATCH (n:ReadAdmission) WITH count(n.value) AS total CREATE (:ReadSummary {total:total}) RETURN total";
    let rejected = cypher::execute(
        &db,
        cypher::Request::new(query),
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        cypher::Limits {
            memory_bytes: read.resources.peak_memory_bytes + 32 * 1024,
            ..limits
        },
    )
    .await
    .unwrap_err();
    let cypher::Error::Query(error) = rejected else {
        panic!("retained read admission must use the query error contract")
    };
    assert_eq!(
        (error.category.as_str(), error.detail.as_str(), error.phase),
        (
            "ResourceLimit",
            "MemoryLimit",
            helix_planner::relational::ErrorPhase::Runtime
        )
    );
    for label in ["ReadMarker", "ReadSummary"] {
        assert_eq!(
            db.cypher(cypher::Request::new(format!(
                "MATCH (n:{label}) RETURN count(*)"
            )))
            .await
            .unwrap()
            .rows,
            vec![vec![serde_json::json!(0)]]
        );
    }
    assert_eq!(
        db.cypher(cypher::Request::new(
            "MATCH (n:ReadAdmission) RETURN count(*)"
        ))
        .await
        .unwrap()
        .rows,
        read.rows
    );
    assert_eq!(
        db.cypher(cypher::Request::new(query)).await.unwrap().rows,
        read.rows
    );
    db.close().await.unwrap();
}
