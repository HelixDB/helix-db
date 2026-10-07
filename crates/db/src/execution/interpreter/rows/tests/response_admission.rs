use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn result_column_admission_precedes_large_alias_copies() {
    let db = test_support::open_db("response-column-admission").await;
    let alias = "column".repeat(64 * 1024);
    let query = helix_cypher::compile(&format!("RETURN 1 AS `{alias}`")).unwrap();
    let plan = r::plan(query, &context::PlannerContext::default()).unwrap();
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let limits = Limits {
        memory_bytes: 128 * 1024,
        ..Limits::default()
    };
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    let parameters = BTreeMap::new();
    let mut allocated = 0_usize;
    let result = {
        let mut execution = std::pin::pin!(ctx.row_program(&plan, &parameters, limits));
        futures::future::poll_fn(|cx| {
            let (poll, observed) =
                crate::allocation_testing::observe(|| execution.as_mut().poll(cx));
            allocated = allocated.saturating_add(observed.bytes);
            poll
        })
        .await
    };
    assert!(matches!(result,Err(Error::Query(error)) if error.detail == "MemoryLimit"));
    assert!(
        allocated < alias.len(),
        "rejected column payload was copied: {allocated}"
    );
    assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    drop(ctx);
    db.close().await.unwrap();
}

#[tokio::test]
async fn response_limits_after_create_roll_back_and_allow_retry() {
    let db = test_support::open_db("response-admission-rollback").await;
    for (alias, limits, detail) in [
        (
            "column".repeat(64 * 1024),
            Limits {
                memory_bytes: 128 * 1024,
                ..Limits::default()
            },
            "MemoryLimit",
        ),
        (
            "escaped\"column\\".into(),
            Limits {
                result_bytes: 1,
                ..Limits::default()
            },
            "ResultLimit",
        ),
    ] {
        let error = crate::cypher::execute(
            &db,
            crate::cypher::Request::new(format!(
                "CREATE (:Transient {{marker:17}}) RETURN 1 AS `{alias}`"
            )),
            crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            crate::query_service::QueryMode::Execute,
            crate::execution_control::ExecutionControl::unlimited(),
            limits,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, Error::Query(error) if error.category == "ResourceLimit" && error.phase == r::ErrorPhase::Runtime && error.detail == detail)
        );
        let after = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:Transient) RETURN count(*)",
            ))
            .await
            .unwrap();
        assert_eq!(after.rows, vec![vec![json!(0)]]);
    }
    let response = crate::cypher::execute(
        &db,
        crate::cypher::Request::new("CREATE (:Transient {marker:17}) RETURN 1 AS value"),
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        Limits {
            memory_bytes: 128 * 1024,
            ..Limits::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.rows, vec![vec![json!(1)]]);
    let after = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:Transient) RETURN n.marker",
        ))
        .await
        .unwrap();
    assert_eq!(after.rows, vec![vec![json!(17)]]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn response_capacity_and_graph_identity_hold_across_batch_boundaries() {
    let db = test_support::open_db("response-capacity-and-hydration").await;
    db.cypher(crate::cypher::Request::new(
        "UNWIND range(1,9) AS i CREATE (a:A {i:i})-[r:R {i:i}]->(b:B {i:i})",
    ))
    .await
    .unwrap();
    for count in [0, 1, 3, 5, 9] {
        for batch_rows in [1, 2, 128] {
            let response = crate::cypher::execute(
                &db,
                crate::cypher::Request::new(format!("MATCH p=(a:A)-[r:R]->(b:B) WHERE a.i <= {count} RETURN b AS last,a AS first,p AS path,r AS edge ORDER BY a.i")),
                crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
                crate::query_service::QueryMode::Execute,
                crate::execution_control::ExecutionControl::unlimited(),
                Limits { batch_rows, ..Limits::default() },
            ).await.unwrap();
            assert_eq!(response.columns, ["last", "first", "path", "edge"]);
            assert_eq!(response.rows.len(), count);
            assert_eq!(response.rows.capacity(), count);
            for (index, row) in response.rows.iter().enumerate() {
                assert_eq!(row.len(), response.columns.len());
                assert_eq!(row[0]["properties"], json!({"i":index + 1}));
                assert_eq!(row[1]["properties"], json!({"i":index + 1}));
                assert_eq!(row[2]["nodes"], json!([row[1], row[0]]));
                assert_eq!(row[2]["relationships"], json!([row[3]]));
                assert_eq!(row[3]["start"], row[1]["id"]);
                assert_eq!(row[3]["end"], row[0]["id"]);
            }
        }
    }
    let response = db
        .cypher(crate::cypher::Request::new("CREATE (:NoReturn)"))
        .await
        .unwrap();
    assert!(response.columns.is_empty());
    assert!(response.rows.is_empty());
    assert_eq!(response.rows.capacity(), 0);
    db.close().await.unwrap();
}
