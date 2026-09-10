use super::*;
use crate::execution::interpreter::{rows::memory, test_support};
use helix_ast::value::PropertyValue;
use helix_planner::context;

/// Candidate generation may leave labels and properties for late hydration.
/// Compare that boundary to a separate tuple model over real stored edges.
#[tokio::test]
async fn late_pattern_constraints_preserve_multiplicity_paths_and_demand() {
    let db = test_support::open_db("cypher-late-pattern-model").await;
    let target =
        test_support::add_node_with_properties(&db, "N", vec![("key", PropertyValue::I64(0))])
            .await;
    let mut model = Vec::new();
    for (label, key) in [("N", 1), ("N", 2), ("Other", 1)] {
        let source = test_support::add_node_with_properties(
            &db,
            label,
            vec![("key", PropertyValue::I64(key))],
        )
        .await;
        for (kind, weight) in [("R", 5), ("R", 6), ("S", 5)] {
            let edge = test_support::add_edge_with_properties(
                &db,
                source,
                target,
                kind,
                vec![("weight", PropertyValue::I64(weight))],
            )
            .await;
            model.push((source, edge, label, key, kind, weight));
        }
    }
    let query =
        helix_cypher::compile("MATCH p=(a:N {key:1})-[r:R {weight:5}]->(b:N) RETURN p").unwrap();
    let r::Operator::Match { pattern, .. } = &query.operators()[0] else {
        panic!("match source");
    };
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    ctx.row_memory = Some(memory::Budget::new(1024 * 1024));
    ctx.enable_request_read_view().await.unwrap();
    let make_row = |source, edge| {
        let mut row = vec![r::Value::Null; query.bindings().len()];
        row[pattern.nodes[0].slot.0 as usize] = r::Value::Entity(r::Entity::Node(source));
        row[pattern.nodes[1].slot.0 as usize] = r::Value::Entity(r::Entity::Node(target));
        row[pattern.relationships[0].slot.0 as usize] =
            r::Value::Entity(r::Entity::Relationship(edge));
        row
    };
    let mut candidates = model
        .iter()
        .map(|(source, edge, ..)| make_row(*source, *edge))
        .collect::<Vec<_>>();
    // A repeated outer row must remain repeated. Nullable correlated bindings
    // cannot satisfy a mandatory pattern, even if the other endpoint exists.
    candidates.push(candidates[0].clone());
    let mut null_node = candidates[0].clone();
    null_node[pattern.nodes[0].slot.0 as usize] = r::Value::Null;
    candidates.push(null_node);
    let mut null_relationship = candidates[0].clone();
    null_relationship[pattern.relationships[0].slot.0 as usize] = r::Value::Null;
    candidates.push(null_relationship);
    let candidates = Rows::new(candidates, ctx.row_budget()).unwrap();
    let mut expected = model
        .iter()
        .filter(|(_, _, label, key, kind, weight)| {
            *label == "N" && *key == 1 && *kind == "R" && *weight == 5
        })
        .map(|(source, edge, ..)| {
            let mut row = make_row(*source, *edge);
            row[pattern.paths[0].slot.0 as usize] =
                r::Value::Path(r::Path::new(vec![*source, target], vec![*edge]).unwrap());
            row
        })
        .collect::<Vec<_>>();
    expected.push(expected[0].clone());
    for batch_rows in [1, 4, 32] {
        for demand in [0, 1, usize::MAX] {
            let mut output = RowBuffer::new(ctx.row_budget()).unwrap();
            let matched = ctx
                .finish_pattern_rows(
                    &candidates,
                    Match {
                        pattern,
                        optional: false,
                        predicate: None,
                        demand,
                    },
                    &BTreeMap::new(),
                    Limits {
                        batch_rows,
                        ..Default::default()
                    },
                    &mut output,
                )
                .await
                .unwrap();
            let actual = output.finish();
            assert_eq!(
                actual.data,
                expected.iter().take(demand).cloned().collect::<Vec<_>>()
            );
            assert_eq!(matched, demand > 0);
        }
    }
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}
