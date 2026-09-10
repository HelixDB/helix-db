use helix_planner::relational as r;
use std::{collections::BTreeMap, sync::Arc};

#[test]
fn input_windows_compose_offsets_and_rebuild_their_proof_on_decode() {
    let query = helix_cypher::compile(
        "UNWIND [] AS x WITH x SKIP $first WITH x SKIP $second RETURN x SKIP $third LIMIT $limit",
    )
    .unwrap();
    let pipeline = r::RowPipeline::new(Arc::new(query), r::RowExecution::Batched);
    let window = pipeline.input_window(0).unwrap();
    assert_eq!(window.projection(), 3);
    assert_eq!(window.termination(), r::Termination::AfterFirstBatch);
    let values = BTreeMap::from([("first", 2), ("second", 3), ("third", 4), ("limit", 5)]);
    let mut evaluated = Vec::new();
    let demand = window
        .demand(|expression| {
            let r::Expression::Parameter(name) = expression else {
                panic!("parameter");
            };
            evaluated.push(name.clone());
            Ok(r::Value::Integer(values[name.as_str()]))
        })
        .unwrap();
    assert_eq!(demand, 14);
    assert_eq!(evaluated, ["first", "second", "third", "limit"]);
    let error = window.demand(|_| Ok(r::Value::Null)).unwrap_err();
    assert_eq!(error.detail, "InvalidArgumentType");
    let error = window.demand(|_| Ok(r::Value::Integer(-1))).unwrap_err();
    assert_eq!(error.detail, "NegativeIntegerArgument");
    let expected = r::QueryError::runtime("ParameterMissing", "MissingParameter", "missing input");
    assert_eq!(
        window.demand(|_| Err(expected.clone())).unwrap_err(),
        expected
    );

    let mut serialized = serde_json::to_value(&pipeline).unwrap();
    serialized["input_windows"] = serde_json::json!({"0":{"projection":999,"skips":[],"limit":null,"termination":"BeforeInput"}});
    let decoded: r::RowPipeline = serde_json::from_value(serialized).unwrap();
    assert_eq!(decoded, pipeline);
    let reference = r::RowPipeline::new(
        Arc::new(pipeline.query().clone()),
        r::RowExecution::Materialized,
    );
    assert!(reference.input_window(0).is_none());
    let saturation = helix_cypher::compile("UNWIND [1] AS x WITH x SKIP 9223372036854775807 WITH x SKIP 9223372036854775807 RETURN x LIMIT 9223372036854775807").unwrap();
    let pipeline = r::RowPipeline::new(Arc::new(saturation), r::RowExecution::Batched);
    assert_eq!(
        pipeline
            .input_window(0)
            .unwrap()
            .demand(|expression| {
                let r::Expression::Literal(value) = expression else {
                    panic!("literal");
                };
                Ok(value.clone())
            })
            .unwrap(),
        usize::MAX
    );
}

#[test]
fn source_demand_never_crosses_error_ordering_or_multiplicity_boundaries() {
    for text in [
        "UNWIND [1,0] AS x WITH 1/x AS y RETURN y LIMIT 1",
        "UNWIND [1] AS x WITH x WHERE x>0 RETURN x LIMIT 1",
        "UNWIND [1] AS x WITH DISTINCT x RETURN x LIMIT 1",
        "UNWIND [1] AS x WITH x ORDER BY x RETURN x LIMIT 1",
        "UNWIND [1] AS x WITH count(x) AS y RETURN y LIMIT 1",
        "UNWIND [1] AS x WITH x UNWIND [x] AS y RETURN y LIMIT 1",
        "CREATE (:N) WITH 1 AS x RETURN x LIMIT 1",
        "MATCH (n {key:1}) WITH n RETURN n LIMIT 1",
        "MATCH (n) WHERE n.key=1 WITH n RETURN n LIMIT 1",
        "MATCH (n)-[:R]->() WITH n RETURN n LIMIT 1",
        "MATCH (n),(m) WITH n RETURN n LIMIT 1",
        "MATCH (n) WITH n RETURN 1+1 LIMIT 1",
        "RETURN 1 LIMIT 1",
        "UNWIND [1] AS x WITH x RETURN x",
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let pipeline = r::RowPipeline::new(Arc::new(query), r::RowExecution::Batched);
        for source in 0..pipeline.query().operators().len() {
            assert!(pipeline.input_window(source).is_none(), "{text}");
        }
    }
    for (text, stage, termination, demand) in [
        (
            "MATCH (n) WITH n RETURN n LIMIT 1",
            2,
            r::Termination::BeforeInput,
            1,
        ),
        (
            "OPTIONAL MATCH (n) WITH n RETURN n LIMIT 0",
            2,
            r::Termination::BeforeInput,
            0,
        ),
        (
            "UNWIND [1,0] AS x WITH x LIMIT 1 WITH 1/x AS y RETURN y",
            1,
            r::Termination::AfterFirstBatch,
            1,
        ),
        (
            "UNWIND [1,0] AS x WITH x SKIP 1 LIMIT 1 WITH 1/x AS y RETURN y",
            1,
            r::Termination::AfterFirstBatch,
            2,
        ),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let pipeline = r::RowPipeline::new(Arc::new(query), r::RowExecution::Batched);
        let window = pipeline.input_window(0).unwrap();
        assert_eq!(window.projection(), stage, "{text}");
        assert_eq!(window.termination(), termination);
        assert_eq!(
            window
                .demand(|expression| {
                    let r::Expression::Literal(value) = expression else {
                        panic!("literal");
                    };
                    Ok(value.clone())
                })
                .unwrap(),
            demand
        );
    }
}
