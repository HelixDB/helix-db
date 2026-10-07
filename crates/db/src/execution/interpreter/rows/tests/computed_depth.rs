use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

fn prefix(depth: usize, map: bool) -> String {
    let wrapper = if map {
        " WITH {child:x} AS x"
    } else {
        " WITH [x] AS x"
    };
    format!("WITH 7 AS x{}", wrapper.repeat(depth))
}

#[tokio::test]
async fn computed_values_keep_depth_semantics_in_all_execution_strategies() {
    let db = test_support::open_db("computed-value-depth").await;
    for map in [false, true] {
        let nested = (0..47).fold(json!(7), |value, _| {
            if map {
                json!({"child":value})
            } else {
                json!([value])
            }
        });
        for (suffix, expected) in [
            (" RETURN x AS value", vec![vec![nested.clone()]]),
            (" RETURN DISTINCT x AS value", vec![vec![nested.clone()]]),
            (
                " RETURN x AS value, count(*)",
                vec![vec![nested.clone(), json!(1)]],
            ),
            (" RETURN count(DISTINCT x)", vec![vec![json!(1)]]),
            (
                " RETURN min(x),max(x)",
                vec![vec![nested.clone(), nested.clone()]],
            ),
            (
                " RETURN CASE WHEN false THEN [x] ELSE 17 END",
                vec![vec![json!(17)]],
            ),
            (" RETURN coalesce(17,[x])", vec![vec![json!(17)]]),
            (
                " RETURN false AND (size([x]) > 0),true OR (size([x]) > 0)",
                vec![vec![json!(false), json!(true)]],
            ),
        ] {
            let text = format!("{}{suffix}", prefix(47, map));
            let query = helix_cypher::compile(&text).unwrap();
            let plan = r::plan(
                query.clone(),
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            for strategy in [
                plan.clone(),
                plan.with_execution(r::RowExecution::Materialized),
                r::RowPlan::reference(query).unwrap(),
            ] {
                let result = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(&strategy, &BTreeMap::new(), Limits::default())
                    .await
                    .unwrap();
                assert_eq!(result.rows, expected, "{suffix}, map={map}");
            }
        }
        let text = format!("{} RETURN collect(x)", prefix(46, map));
        let result = db.cypher(crate::cypher::Request::new(&text)).await.unwrap();
        let expected = (0..46).fold(json!(7), |value, _| {
            if map {
                json!({"child":value})
            } else {
                json!([value])
            }
        });
        assert_eq!(result.rows, vec![vec![json!([expected])]]);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn excessive_computed_depth_is_a_runtime_error_and_rolls_back_prior_writes() {
    let db = test_support::open_db("computed-depth-rollback").await;
    for map in [false, true] {
        for (depth, suffix) in [
            (48, " RETURN x"),
            (49, " RETURN x"),
            (47, " RETURN [x]"),
            (47, " RETURN {child:x}"),
            (47, " RETURN [] + x"),
            (47, " RETURN x + []"),
            (47, " RETURN collect(x)"),
            (47, " RETURN collect(DISTINCT x)"),
        ] {
            // List concatenation flattens list operands; map operands become
            // single elements and therefore add a level.
            if !map && suffix.contains(" + ") {
                continue;
            }
            let text = format!("CREATE (:Rollback) {}{suffix}", prefix(depth, map));
            let query = helix_cypher::compile(&text).unwrap();
            let plan = r::plan(
                query.clone(),
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            for strategy in [
                plan.clone(),
                plan.with_execution(r::RowExecution::Materialized),
                r::RowPlan::reference(query).unwrap(),
            ] {
                let error = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(&strategy, &BTreeMap::new(), Limits::default())
                    .await
                    .unwrap_err();
                assert!(
                    matches!(error,Error::Query(error) if error.category=="ResourceLimit" && error.detail=="ValueDepth" && error.phase==r::ErrorPhase::Runtime),
                    "{suffix}, map={map}"
                );
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
            let error = db
                .cypher(crate::cypher::Request::new(&text))
                .await
                .unwrap_err();
            assert!(
                matches!(error,crate::cypher::Error::Query(error) if error.category=="ResourceLimit" && error.detail=="ValueDepth" && error.phase==r::ErrorPhase::Runtime)
            );
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
    }
    db.close().await.unwrap();
}
