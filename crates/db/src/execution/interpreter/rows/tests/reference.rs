use super::super::*;
use crate::execution::interpreter::test_support;
use helix_ast::{batch, expr, query, traversal, value};
use helix_planner::{context, exec};
use serde_json::json;

struct Case {
    query: &'static str,
    columns: &'static [&'static str],
    rows: Vec<Vec<Option<i64>>>,
}

/// The oracle is a relation over integer fixture keys and distinct edge
/// positions. It uses neither storage traversal nor planner/evaluator helpers.
#[tokio::test]
async fn optimized_and_full_scan_execution_agree_with_an_independent_multigraph_model() {
    let (mut indexed, mut hash_joined, mut correlated) = (false, false, false);
    for seed in 0..4 {
        let db = test_support::open_db_with_config(
            test_support::in_memory_config("cypher-reference-multigraph")
                .with_equality_index("N", "key"),
        )
        .await;
        let mut ids = Vec::new();
        for key in 0..6 {
            let mut properties = vec![("key", value::PropertyValue::I64(key))];
            if key < 4 {
                properties.push(("group", value::PropertyValue::I64(key % 2)));
            }
            ids.push(
                test_support::add_node_with_properties(
                    &db,
                    if key == 5 { "Other" } else { "N" },
                    properties,
                )
                .await,
            );
        }
        let mut edges = Vec::new();
        if seed > 0 {
            // Parallel edges, self loops, a cycle, and both label/type mismatch.
            edges.extend([
                (0, 1, "R"),
                (0, 1, "R"),
                (1, 1, "R"),
                (1, 2, "R"),
                (2, 0, "R"),
                (0, 5, "R"),
                (3, 2, "S"),
            ]);
            for a in 0..6 {
                for b in 0..6 {
                    if (a * 7 + b * 3 + seed) % 7 == 0 {
                        edges.push((a, b, "R"));
                    }
                }
            }
        }
        for (a, b, kind) in &edges {
            test_support::add_edge(&db, ids[*a], ids[*b], kind).await;
        }
        let directed = edges
            .iter()
            .enumerate()
            .filter(|(_, (a, b, kind))| *a < 5 && *b < 5 && *kind == "R")
            .map(|(edge, (a, b, _))| (edge, *a as i64, *b as i64))
            .collect::<Vec<_>>();
        let mut chains = Vec::new();
        let mut separate_matches = Vec::new();
        for (first, a, b) in &directed {
            for (second, c, d) in &directed {
                if b == c {
                    separate_matches.push(vec![Some(*a), Some(*b), Some(*d)]);
                    if first != second {
                        chains.push(vec![Some(*a), Some(*b), Some(*d)]);
                    }
                }
            }
        }
        let mut undirected = Vec::new();
        for (_, a, b) in &directed {
            undirected.push(vec![Some(*a), Some(*b)]);
            if a != b {
                undirected.push(vec![Some(*b), Some(*a)]);
            }
        }
        let mut optional = Vec::new();
        let mut degrees = Vec::new();
        for a in 0..5 {
            let targets = directed
                .iter()
                .filter(|(_, source, b)| *source == a && *b > 2)
                .map(|(_, _, b)| *b)
                .collect::<Vec<_>>();
            if targets.is_empty() {
                optional.push(vec![Some(a), None]);
            } else {
                optional.extend(targets.into_iter().map(|b| vec![Some(a), Some(b)]));
            }
            let count = directed
                .iter()
                .filter(|(_, source, _)| *source == a)
                .count() as i64;
            if count > 0 {
                degrees.push(vec![Some(a), Some(count)]);
            }
        }
        let join = (0..4)
            .flat_map(|a| {
                (0..4)
                    .filter(move |b| a % 2 == b % 2)
                    .map(move |b| vec![Some(a), Some(b)])
            })
            .collect();
        let distinct = directed
            .iter()
            .map(|(_, _, b)| *b)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .take(2)
            .map(|b| vec![Some(b)])
            .collect();
        let mut cases = vec![
            Case { query: "MATCH (a:N {key:2})-[r:R]->(b:N) RETURN a.key AS a,b.key AS b ORDER BY a,b",
                columns: &["a", "b"], rows: directed.iter().filter(|(_, a, _)| *a == 2)
                    .map(|(_, a, b)| vec![Some(*a), Some(*b)]).collect() },
            Case { query: "MATCH (a:N)-[r:R]->(b:N)-[s:R]->(c:N) RETURN a.key AS a,b.key AS b,c.key AS c ORDER BY a,b,c",
                columns: &["a", "b", "c"], rows: chains.clone() },
            Case { query: "MATCH (a:N)-[r:R]->(b:N) MATCH (b)-[s:R]->(c:N) RETURN a.key AS a,b.key AS b,c.key AS c ORDER BY a,b,c",
                columns: &["a", "b", "c"], rows: separate_matches },
            Case { query: "MATCH (a:N)-[r:R]-(b:N) RETURN a.key AS a,b.key AS b ORDER BY a,b",
                columns: &["a", "b"], rows: undirected },
            Case { query: "MATCH (a:N)-[r:R]->(b:N)-[s:R]->(a) RETURN a.key AS a,b.key AS b ORDER BY a,b",
                columns: &["a", "b"], rows: chains.into_iter().filter(|row| row[0] == row[2])
                    .map(|row| vec![row[0], row[1]]).collect() },
            Case { query: "MATCH (a:N)-[r:R]->(b:N) MATCH (a)-[r:R]->(b) RETURN a.key AS a,b.key AS b ORDER BY a,b",
                columns: &["a", "b"], rows: directed.iter().map(|(_, a, b)| vec![Some(*a), Some(*b)]).collect() },
            Case { query: "MATCH (a:N),(b:N) WHERE a.group=b.group RETURN a.key AS a,b.key AS b ORDER BY a,b",
                columns: &["a", "b"], rows: join },
            Case { query: "MATCH (a:N) OPTIONAL MATCH (a)-[r:R]->(b:N) WHERE b.key > 2 RETURN a.key AS a,b.key AS b ORDER BY a,b",
                columns: &["a", "b"], rows: optional.clone() },
            Case { query: "MATCH (a:N) OPTIONAL MATCH (a)-[r:R]->(b:N) WITH a,b WHERE b.key > 2 RETURN a.key AS a,b.key AS b ORDER BY a,b",
                columns: &["a", "b"], rows: optional.into_iter().filter(|row| row[1].is_some()).collect() },
            Case { query: "MATCH (a:N)-[r:R]->(b:N) WITH a.key AS a,count(*) AS c RETURN a,c ORDER BY a,c",
                columns: &["a", "c"], rows: degrees },
            Case { query: "MATCH (a:N) RETURN a.group AS g,count(*) AS c ORDER BY g,c",
                columns: &["g", "c"], rows: vec![vec![Some(0),Some(2)],vec![Some(1),Some(2)],vec![None,Some(1)]] },
            Case { query: "MATCH (a:N)-[r:R]->(b:N) RETURN DISTINCT b.key AS b ORDER BY b LIMIT 2",
                columns: &["b"], rows: distinct },
            Case { query: "UNWIND [2,2,99,null] AS key OPTIONAL MATCH (n:N {key:key}) RETURN key,n.key AS found ORDER BY key,found",
                columns: &["key", "found"], rows: vec![vec![Some(2),Some(2)],vec![Some(2),Some(2)],vec![Some(99),None],vec![None,None]] },
        ];
        for case in &mut cases {
            // Cypher ASC puts null last. Sorting the independent expected tuples
            // preserves duplicates while avoiding storage-ID ordering assumptions.
            case.rows
                .sort_by_key(|row| row.iter().map(|v| (v.is_none(), *v)).collect::<Vec<_>>());
            let query = helix_cypher::compile(case.query).unwrap();
            let selected = r::plan(
                query.clone(),
                &db.planner_context(context::ParamBindings::default()),
            )
            .unwrap();
            for plan in selected.matches().values() {
                hash_joined |= plan
                    .steps
                    .iter()
                    .any(|step| matches!(step, r::MatchStep::HashJoin { .. }));
                correlated |= plan
                    .steps
                    .iter()
                    .any(|step| matches!(step, r::MatchStep::IndexLookup(_)));
                indexed |= plan.sources.iter().any(|source| source.access.steps().iter().any(|step| matches!(&step.op,
                    exec::ExecOp::Access { plan } if matches!(plan.as_ref(), exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::Bitmap { .. } | exec::ExecNodeAccessPlan::Unique { .. }
                    )))));
            }
            let reference = r::RowPlan::reference(query).unwrap();
            assert_eq!(reference.metrics.memo_groups, 0);
            for plan in reference.matches().values() {
                assert!(plan.steps.iter().all(|step| matches!(
                    step,
                    r::MatchStep::Scan(_) | r::MatchStep::Expand { .. }
                )));
                assert!(plan.sources.iter().all(|source| source.access.steps().iter().all(|step| matches!(&step.op,
                    exec::ExecOp::Access { plan } if matches!(plan.as_ref(), exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AllScan))))));
            }
            for plan in [
                selected.clone(),
                selected.with_execution(r::RowExecution::Materialized),
                reference,
            ] {
                let response = Interpreter::new(&db, context::ParamBindings::default())
                    .execute_rows(
                        &plan,
                        &BTreeMap::new(),
                        Limits {
                            batch_rows: 2,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap_or_else(|error| panic!("seed {seed}, {}: {error}", case.query));
                assert_eq!(response.columns, case.columns, "{}", case.query);
                assert_eq!(
                    json!(response.rows),
                    json!(case.rows),
                    "seed {seed}, {}",
                    case.query
                );
            }
        }
        // Native frontend checks use the same fixture but an independent model
        // expectation. out_e preserves parallel relationships as separate rows.
        let native = db
            .query(query::QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "count",
                        traversal::g().n_with_label("N").out_e(Some("R")).count(),
                    )
                    .returning(["count"]),
            ))
            .await
            .unwrap();
        let expected = edges
            .iter()
            .filter(|(a, _, kind)| *a < 5 && *kind == "R")
            .count();
        assert_eq!(native["count"], json!(expected));
        let cypher = db
            .cypher(crate::cypher::Request::new(
                "MATCH (a:N)-[r:R]->() RETURN count(r)",
            ))
            .await
            .unwrap();
        assert_eq!(cypher.rows, vec![vec![json!(expected)]]);
        let native = db
            .query(query::QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "count",
                        traversal::g()
                            .n_with_label("N")
                            .where_(expr::Predicate::gte("key", 2))
                            .count(),
                    )
                    .returning(["count"]),
            ))
            .await
            .unwrap();
        assert_eq!(native["count"], json!(3));
        let cypher = db
            .cypher(crate::cypher::Request::new(
                "MATCH (n:N) WHERE n.key >= 2 RETURN count(n)",
            ))
            .await
            .unwrap();
        assert_eq!(cypher.rows, vec![vec![json!(3)]]);
        db.close().await.unwrap();
    }
    assert!(
        indexed && hash_joined && correlated,
        "the oracle must compare distinct physical implementations"
    );
}
