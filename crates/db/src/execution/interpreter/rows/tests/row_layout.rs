use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

#[tokio::test]
async fn compact_rows_preserve_scopes_null_extension_paths_and_expression_values() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("cypher-compact-row-model").with_equality_index("N", "name"),
    )
    .await;
    db.cypher(crate::cypher::Request::new(
        "CREATE (a:N {name:'A',v:1})-[:R {key:3}]->(b:N {name:'B',v:2}), (:N {name:'C',v:3})",
    ))
    .await
    .unwrap();
    for (text, expected) in [
        ("UNWIND [1,2,2,null] AS x WITH x AS a WITH a AS x RETURN DISTINCT x ORDER BY x DESC",
            vec![vec![json!(null)],vec![json!(2)],vec![json!(1)]]),
        ("MATCH (n:N {name:'A'}) WITH n AS a WITH a.name AS name MATCH (other:N {name:'B'}) RETURN name,other.name",
            vec![vec![json!("A"),json!("B")]]),
        ("MATCH (n:N) WITH n AS a WITH a.name AS name MATCH (m:N) WHERE m.name=name RETURN name,m.name ORDER BY name",
            vec![vec![json!("A"),json!("A")],vec![json!("B"),json!("B")],vec![json!("C"),json!("C")]]),
        ("UNWIND [1] AS seed WITH seed AS a WITH a AS dummy MATCH (n:N),(m:N) WHERE n.v=m.v RETURN n.name,m.name ORDER BY n.name",
            vec![vec![json!("A"),json!("A")],vec![json!("B"),json!("B")],vec![json!("C"),json!("C")]]),
        ("MATCH p=(n:N)-[:R]->(m:N) WITH p AS path,n AS original WITH path AS p,original AS n OPTIONAL MATCH (n)-[s:MISSING]->(other:N) RETURN n.name,other,length(p)",
            vec![vec![json!("A"),json!(null),json!(1)]]),
        ("MATCH (n:N) WITH n AS a WITH a AS n RETURN n.name,n:N ORDER BY n.name",
            vec![vec![json!("A"),json!(true)],vec![json!("B"),json!(true)],vec![json!("C"),json!(true)]]),
        ("UNWIND [1,2] AS seed WITH seed AS a WITH a AS x RETURN {value:x,list:[x,x+1],slice:[x,x+1,x+2][1..],choice:CASE WHEN x=1 THEN -x ELSE +x END} AS value ORDER BY x",
            vec![vec![json!({"value":1,"list":[1,2],"slice":[2,3],"choice":-1})],
                vec![json!({"value":2,"list":[2,3],"slice":[3,4],"choice":2})]]),
        ("UNWIND [1,2,2,3] AS x WITH x AS a WITH a AS x RETURN x,count(*) ORDER BY x DESC SKIP 1 LIMIT 1",
            vec![vec![json!(2),json!(2)]]),
        ("UNWIND [1,2,3] AS x WITH x AS a WITH a AS x WHERE x<0 RETURN count(*),sum(x),collect(x)",
            vec![vec![json!(0),json!(0),json!([])]]),
        ("UNWIND [1,2,3,4] AS x WITH x AS a SKIP 1 WITH a AS x RETURN x ORDER BY x DESC LIMIT 2",
            vec![vec![json!(4)],vec![json!(3)]]),
    ] {
        let query = helix_cypher::compile(text).unwrap_or_else(|error| panic!("{text}: {error}"));
        let logical_width = query.bindings().len();
        let selected = r::plan(query.clone(), &db.planner_context(context::ParamBindings::default())).unwrap();
        assert!(selected.program().query().width() < logical_width, "fixture must reuse cells: {text}");
        let mut plans = vec![r::RowPlan::reference(query).unwrap()];
        for layout in [r::RowLayoutMode::Compact,r::RowLayoutMode::Identity] {
            for execution in [r::RowExecution::Batched,r::RowExecution::Materialized] {
                plans.push(selected.clone().with_layout(layout).with_execution(execution));
            }
        }
        for plan in plans {
            let (metadata_bytes, allocations) = crate::allocation_testing::observe(||
                plan.program().retained_layout_bytes());
            assert_eq!(allocations.allocations, 0, "admission measurement must not allocate");
            let result = Interpreter::new(&db, context::ParamBindings::default()).execute_rows(
                &plan,&BTreeMap::new(),Limits {batch_rows:1,..Default::default()},
            ).await.unwrap_or_else(|error| panic!("{text}: {error}"));
            assert_eq!(result.rows,expected,"{text}; {:?}",plan.program().layout_mode());
            assert!(result.resources.peak_memory_bytes >= metadata_bytes);
        }
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn physical_program_admission_rejects_insufficient_budget_before_writes() {
    let db = test_support::open_db("cypher-layout-admission").await;
    let query = helix_cypher::compile(
        "WITH 1 AS a WITH a AS b WITH b AS key CREATE (:MustNotExist {key:key}) RETURN key",
    )
    .unwrap();
    let plan = r::plan(
        query,
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let metadata = plan.program().retained_layout_bytes();
    let failure = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &plan,
            &BTreeMap::new(),
            Limits {
                memory_bytes: metadata - 1,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(failure,Error::Query(error) if error.detail=="MemoryLimit"));
    let reference = r::RowPlan::reference(
        helix_cypher::compile("MATCH (n:MustNotExist) RETURN count(*)").unwrap(),
    )
    .unwrap();
    assert_eq!(
        Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(&reference, &BTreeMap::new(), Limits::default(),)
            .await
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn identity_layout_rejects_a_row_that_cannot_fit_after_program_admission() {
    let db = test_support::open_db("cypher-identity-row-admission").await;
    // Unused logical bindings need no compact cells, but the independent
    // identity layout deliberately retains the complete binding catalog.
    let bindings = (0..4096)
        .map(|index| r::Binding {
            name: format!("unused_{index}"),
            kind: r::BindingType::Scalar,
            nullable: true,
            value_type: r::ValueType::Any,
        })
        .collect();
    let query = r::Query::new(bindings, vec![], vec![]).unwrap();
    assert_eq!(query.layout().width(), 0);
    let plan = r::RowPlan::reference(query).unwrap();
    let row_bytes = size_of::<r::Row>() + plan.program().query().width() * size_of::<r::Value>();
    let memory_bytes = row_bytes - 1;
    assert!(plan.program().retained_layout_bytes() < memory_bytes);
    let result = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(
            &plan,
            &BTreeMap::new(),
            Limits {
                memory_bytes,
                ..Default::default()
            },
        )
        .await;
    db.close().await.unwrap();
    assert!(matches!(result, Err(Error::Query(error))
        if error.category == "ResourceLimit"
        && error.detail == "MemoryLimit"
        && error.message == "row schema exceeds the query memory budget"));
}

#[tokio::test]
async fn compact_rows_preserve_new_mutation_targets_and_atomic_failures() {
    for (text, expected, stored) in [
        ("UNWIND [1,2] AS x WITH x AS a WITH a AS value CREATE (n:Created {key:value}) WITH n AS saved WITH saved AS n SET n.key=n.key+10 RETURN n.key AS key ORDER BY key",
            vec![vec![json!(11)],vec![json!(12)]],vec![vec![json!(11)],vec![json!(12)]]),
        ("UNWIND [1,2] AS x WITH x AS a WITH a AS value CREATE (n:Created {key:value}) WITH n AS saved WITH saved AS n SET n += {extra:1} REMOVE n.extra RETURN n.key AS key ORDER BY key",
            vec![vec![json!(1)],vec![json!(2)]],vec![vec![json!(1)],vec![json!(2)]]),
        ("UNWIND [1] AS x WITH x AS a WITH a AS value CREATE (n:Created {key:value}) WITH n AS saved WITH saved AS n SET n = {key:9} RETURN n.key AS key",
            vec![vec![json!(9)]],vec![vec![json!(9)]]),
        ("UNWIND [1] AS x WITH x AS a WITH a AS value CREATE (n:Created {key:value}) WITH n AS saved WITH saved AS n DELETE n RETURN 1",
            vec![vec![json!(1)]],vec![]),
    ] {
        for layout in [r::RowLayoutMode::Compact,r::RowLayoutMode::Identity] {
            for execution in [r::RowExecution::Batched,r::RowExecution::Materialized] {
                let db = test_support::open_db_with_config(
                    test_support::in_memory_config("cypher-compact-row-writes").with_equality_index("Created","key"),
                ).await;
                let query = helix_cypher::compile(text).unwrap();
                let plan = r::plan(query,&db.planner_context(context::ParamBindings::default())).unwrap()
                    .with_layout(layout).with_execution(execution);
                let result = Interpreter::new(&db,context::ParamBindings::default()).execute_rows(
                    &plan,&BTreeMap::new(),Limits::default(),
                ).await.unwrap_or_else(|error|panic!("{text}: {error}"));
                assert_eq!(result.rows,expected);
                let observation = r::RowPlan::reference(helix_cypher::compile(
                    "MATCH (n:Created) RETURN n.key ORDER BY n.key",
                ).unwrap()).unwrap();
                let actual = Interpreter::new(&db,context::ParamBindings::default()).execute_rows(
                    &observation,&BTreeMap::new(),Limits::default(),
                ).await.unwrap();
                assert_eq!(actual.rows,stored);
                let failure = r::plan(helix_cypher::compile(
                    "CREATE (n:Rollback {key:1})-[:R]->(:Target) WITH n AS a WITH a AS n SET n.key=99 DELETE n",
                ).unwrap(),&db.planner_context(context::ParamBindings::default())).unwrap()
                    .with_layout(layout).with_execution(execution);
                let error = Interpreter::new(&db,context::ParamBindings::default()).execute_rows(
                    &failure,&BTreeMap::new(),Limits::default(),
                ).await.unwrap_err();
                assert!(matches!(error,Error::Query(error)
                    if error.category=="ConstraintVerificationFailed"
                    && error.detail=="DeleteConnectedNode"
                    && error.phase==r::ErrorPhase::Runtime));
                let observation = r::RowPlan::reference(helix_cypher::compile(
                    "MATCH (n:Rollback) RETURN count(*)",
                ).unwrap()).unwrap();
                assert_eq!(Interpreter::new(&db,context::ParamBindings::default()).execute_rows(
                    &observation,&BTreeMap::new(),Limits::default(),
                ).await.unwrap().rows,vec![vec![json!(0)]]);
                db.close().await.unwrap();
            }
        }
    }
}
