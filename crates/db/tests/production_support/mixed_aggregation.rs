/// Frontends may normalize these expressions into separate projections, but
/// the shared validated plan still accepts aggregates inside scalar expressions.
#[cfg_attr(test, tokio::test)]
pub async fn mixed_aggregate_common_plans_preserve_groups_empty_input_and_graph_values() {
    use crate::{
        cypher::{output, Limits},
        execution::interpreter::Interpreter,
    };
    use helix_planner::context;
    use helix_planner::relational as r;
    use serde_json::json;
    use std::collections::BTreeMap;

    let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
        database: "mixed-aggregate-common-contract".into(),
    })
    .await
    .unwrap();
    db.cypher(crate::cypher::Request::new(
        "CREATE (:N {g:1,v:2}),(:N {g:1,v:3}),(:N {v:7})",
    ))
    .await
    .unwrap();
    for (text, expected) in [
        (
            "UNWIND [2,1,2,null,null] AS x RETURN x,count(*) AS n ORDER BY x",
            vec![
                vec![json!(1), json!(2)],
                vec![json!(2), json!(3)],
                vec![json!(null), json!(3)],
            ],
        ),
        (
            "UNWIND [] AS x RETURN count(*) AS n,sum(x) AS s",
            vec![vec![json!(1), json!(1)]],
        ),
        ("UNWIND [] AS x RETURN x,count(*) AS n", vec![]),
        (
            "MATCH (n:N) RETURN n.g AS g,sum(n.v) AS s ORDER BY g",
            vec![vec![json!(1), json!(6)], vec![json!(null), json!(8)]],
        ),
        (
            "UNWIND [1,1,2] AS x RETURN DISTINCT x,count(*) AS n ORDER BY n DESC SKIP 1 LIMIT 1",
            vec![vec![json!(2), json!(2)]],
        ),
    ] {
        let original = helix_cypher::compile(text).unwrap();
        let mut operators = original.operators().to_vec();
        let r::Operator::Project { items, .. } = operators.last_mut().unwrap() else {
            panic!("terminal aggregate projection");
        };
        *items = r::ProjectionProgram::new(
            items
                .iter()
                .cloned()
                .map(|mut item| {
                    if item.expression.has_aggregate() {
                        assert!(matches!(item.expression, r::Expression::Aggregate { .. }));
                        item.expression = r::Expression::Binary(
                            r::Binary::Add,
                            Box::new(item.expression),
                            Box::new(r::Expression::Literal(r::Value::Integer(1))),
                        );
                    }
                    item
                })
                .collect(),
        )
        .unwrap();
        let query = r::Query::new(
            original.bindings().to_vec(),
            operators,
            original.returns().to_vec(),
        )
        .unwrap();
        let selected = r::plan(
            query.clone(),
            &db.planner_context(context::ParamBindings::default()),
        )
        .unwrap();
        let materialized = selected
            .clone()
            .with_execution(r::RowExecution::Materialized);
        let reference = r::RowPlan::reference(query).unwrap();
        for plan in [&selected, &materialized, &reference] {
            for batch_rows in [1, 2, 7] {
                let result = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows_with::<output::Typed>(
                        plan,
                        &BTreeMap::new(),
                        Limits {
                            batch_rows,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(result.rows, expected, "{text}, batch={batch_rows}");
            }
        }
    }
    db.close().await.unwrap();
}
