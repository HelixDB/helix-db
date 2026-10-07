use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn json_preparation_failure_after_create_rolls_back_without_changing_typed_results() {
    let db = test_support::open_db("json-overlap-rollback").await;
    let alias = "alias".repeat(32 * 1024);
    let text = format!("CREATE (:Transient {{marker:17}}) RETURN 1 AS `{alias}`");
    let typed = db.cypher(crate::cypher::Request::new(&text)).await.unwrap();
    db.cypher(crate::cypher::Request::new("MATCH (n:Transient) DELETE n"))
        .await
        .unwrap();
    let error = crate::cypher::execute_json(
        &db,
        crate::cypher::Request::new(&text),
        crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        crate::query_service::QueryMode::Execute,
        crate::execution_control::ExecutionControl::unlimited(),
        Limits {
            memory_bytes: typed.resources.peak_memory_bytes + 32 * 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, Error::Query(error) if error.category == "ResourceLimit" && error.phase == r::ErrorPhase::Runtime && error.detail == "MemoryLimit")
    );
    let after = db
        .cypher(crate::cypher::Request::new(
            "MATCH (n:Transient) RETURN count(*)",
        ))
        .await
        .unwrap();
    assert_eq!(after.rows, vec![vec![json!(0)]]);
    let encoded = db
        .cypher_json(crate::cypher::Request::new(&text))
        .await
        .unwrap();
    assert_eq!(encoded.body(), serde_json::to_vec(&typed).unwrap());
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
async fn encoded_graphs_paths_empty_results_and_tagged_values_match_typed_output() {
    let db = test_support::open_db("encoded-value-parity").await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:A {i:1})-[r:R {i:2}]->(b:B {i:3})",
    ))
    .await
    .unwrap();
    for text in [
        "MATCH p=(a:A)-[r:R]->(b:B) RETURN p,a,r,b",
        "MATCH (a:Absent) RETURN a,1 AS one",
        "RETURN 9223372036854775807 AS integer, {`$type`:'literal',value:[null,true,1.5]} AS nested",
        "RETURN '列𝄞' AS `escaped\"alias`",
        "UNWIND range(1,9) AS i RETURN i ORDER BY i DESC",
        "MATCH (n:Absent) DELETE n",
    ] {
        let typed = db.cypher(crate::cypher::Request::new(text)).await.unwrap();
        let expected = serde_json::to_vec(&typed).unwrap();
        for result_bytes in [expected.len(), expected.len() - 1] {
            let result = crate::cypher::execute_json(
                &db,
                crate::cypher::Request::new(text),
                crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
                crate::query_service::QueryMode::Execute,
                crate::execution_control::ExecutionControl::unlimited(),
                Limits { result_bytes, ..Default::default() },
            ).await;
            if result_bytes == expected.len() {
                let encoded = result.unwrap();
                assert_eq!(encoded.body(), expected, "{text}");
                assert_eq!(encoded.resources.reads, typed.resources.reads, "{text}");
            } else {
                assert!(matches!(result, Err(Error::Query(error)) if error.category == "ResourceLimit" && error.phase == r::ErrorPhase::Runtime && error.detail == "ResultLimit"), "{text}");
            }
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn every_encoding_cancellation_checkpoint_before_commit_rolls_back() {
    let mut reached_commit = false;
    for checkpoint in 0..128 {
        let db = test_support::open_db("encoding-commit-checkpoints").await;
        let query =
            helix_cypher::compile("CREATE (:Checkpoint {key:1}) RETURN 1 AS value").unwrap();
        let plan = r::plan(
            query,
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let interpreter = Interpreter::new(&db, context::ParamBindings::default());
        interpreter.ctx.fail_deadline_after(checkpoint);
        let result = interpreter
            .execute_rows_with::<output::Json>(&plan, &BTreeMap::new(), Limits::default())
            .await;
        let expected = match result {
            Ok(response) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(response.body()).unwrap()["rows"],
                    json!([[1]])
                );
                reached_commit = true;
                1
            }
            Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded)) => 0,
            Err(error) => panic!("checkpoint {checkpoint}: {error:?}"),
        };
        let after = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:Checkpoint) RETURN count(*)",
            ))
            .await
            .unwrap();
        assert_eq!(
            after.rows,
            vec![vec![json!(expected)]],
            "checkpoint {checkpoint}"
        );
        db.close().await.unwrap();
        if reached_commit {
            break;
        }
    }
    assert!(reached_commit);
}

#[tokio::test]
async fn encoding_memory_scales_with_owned_columns_and_actual_wire_bytes() {
    let db = test_support::open_db("encoding-memory-scaling").await;
    for character in ['x', '"', '𝄞'] {
        let mut measurements = Vec::new();
        for bytes in [64 * 1024, 256 * 1024] {
            let alias = character.to_string().repeat(bytes / character.len_utf8());
            let text = format!("RETURN 1 AS `{alias}`");
            let encoded = db
                .cypher_json(crate::cypher::Request::new(&text))
                .await
                .unwrap();
            let payload = alias.len() + encoded.body().len();
            let peak = encoded.resources.peak_memory_bytes;
            assert!(peak >= payload, "owned columns and wire buffer overlap");
            assert!(
                peak - payload < 64 * 1024,
                "fixed encoding overhead: {peak}, {payload}"
            );
            assert_eq!(
                encoded.resources.reads,
                crate::cypher::StorageReadUsage::default()
            );
            measurements.push((payload, peak));
            drop(encoded);
            let error = crate::cypher::execute_json(
                &db,
                crate::cypher::Request::new(&text),
                crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
                crate::query_service::QueryMode::Execute,
                crate::execution_control::ExecutionControl::unlimited(),
                Limits {
                    memory_bytes: peak - 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
            assert!(
                matches!(error, Error::Query(error) if error.phase == r::ErrorPhase::Runtime && error.detail == "MemoryLimit")
            );
        }
        assert_eq!(
            measurements[1].1 - measurements[0].1,
            measurements[1].0 - measurements[0].0
        );
    }
    db.close().await.unwrap();
}
