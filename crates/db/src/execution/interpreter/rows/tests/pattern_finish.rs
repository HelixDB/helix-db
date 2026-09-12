use super::*;
use crate::execution::interpreter::{rows::memory, test_support};
use helix_ast::value::PropertyValue;
use helix_planner::context;

#[tokio::test]
async fn cached_scan_sets_admit_construction_and_stop_at_exact_demand() {
    use crate::encoding::v2::{keys, values::property};
    let db = test_support::open_db("cached-scan-admission").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    // An uncompressed u64 vector for these IDs exceeds the complete query
    // budget. The result oracle comes directly from canonical fixture keys.
    let limits = Limits {
        memory_bytes: 48 * 1024,
        batch_rows: 32,
        ..Default::default()
    };
    for id in 1..=8193 {
        db.inner_db()
            .put(
                ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                    id,
                ))),
                property::encode_properties(&[property::Property::string("$label", "N")]),
            )
            .await
            .unwrap();
    }
    let plan = r::plan(
        helix_cypher::compile("MATCH (n) RETURN n").unwrap(),
        &context::PlannerContext::default(),
    )
    .unwrap();
    let source = &plan.matches()[&0].sources[0];
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.enable_request_read_view().await.unwrap();
    for demand in [0, 1, 7, 8193, usize::MAX] {
        let before = ctx.row_budget().reads();
        let ids = ctx.match_source_ids(source, demand, limits).await.unwrap();
        let count = demand.min(8193);
        assert!(ids.iter().eq(1..=count as u64));
        assert_eq!(ctx.row_budget().reads().scan_rows - before.scan_rows, count);
        assert_eq!(
            ctx.row_budget().reads().scans - before.scans,
            usize::from(demand > 0)
        );
        assert!(ctx.row_budget().peak() <= limits.memory_bytes);
        drop(ids);
        assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    }
    ctx.row_memory = Some(memory::Budget::new(4095));
    assert!(
        matches!(ctx.match_source_ids(source, usize::MAX, Limits { batch_rows: 1, ..limits }).await,
        Err(crate::cypher::Error::Query(error)) if error.detail == "MemoryLimit")
    );
    assert_eq!(ctx.row_budget().reads().scan_rows, 1);
    assert_eq!(ctx.row_budget().available(), 4095);
    ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
    ctx.fail_deadline_after(0);
    assert!(matches!(
        ctx.match_source_ids(source, usize::MAX, limits).await,
        Err(crate::cypher::Error::Storage(
            crate::HelixDbError::QueryDeadlineExceeded
        ))
    ));
    assert_eq!(
        ctx.row_budget().reads(),
        crate::query_resources::StorageReadUsage::default()
    );
    assert_eq!(ctx.row_budget().available(), limits.memory_bytes);
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}

#[tokio::test]
async fn cached_native_fallback_preserves_subplan_isolation_and_validates_result_kind() {
    use crate::execution::interpreter::ExecutionRow;
    let db = test_support::open_db("cached-native-sources").await;
    let node = test_support::add_node_with_properties(&db, "N", vec![]).await;
    let mut ctx = ExecutionContext::new(
        &db,
        context::ParamBindings::default().with_value(
            test_support::name("ids"),
            PropertyValue::I64Array(vec![node as i64]),
        ),
    );
    ctx.row_memory = Some(memory::Budget::new(64 * 1024));
    ctx.enable_request_read_view().await.unwrap();
    let outer = exec::ExecStepId::new(99).unwrap();
    ctx.step_outputs.insert(outer, ExecutionValue::Count(17));
    let variable = test_support::name("source");
    ctx.variables.insert(
        variable.clone(),
        ExecutionValue::Stream(vec![
            ExecutionRow::current(ElementRef::Node(node)),
            ExecutionRow::current(ElementRef::Node(node)),
            ExecutionRow::current(ElementRef::Edge(7)),
        ]),
    );
    let source = r::PlannedNode {
        slot: r::Slot(0),
        estimated_rows: 3,
        access: test_support::executable(
            ir::PlanKind::Read,
            vec![
                test_support::step(
                    1,
                    vec![],
                    exec::ExecOp::Variable {
                        op: exec::ExecVariableOp::SourceInject {
                            variable: variable.clone(),
                        },
                    },
                ),
                test_support::step(
                    2,
                    vec![exec::ExecStepId::new(1).unwrap()],
                    exec::ExecOp::Noop,
                ),
            ],
            2,
        ),
    };
    let ids = ctx
        .match_source_ids(&source, usize::MAX, Limits::default())
        .await
        .unwrap();
    assert_eq!(ids.iter().collect::<Vec<_>>(), vec![node]);
    drop(ids);
    assert_eq!(ctx.row_budget().available(), 64 * 1024);
    assert_eq!(ctx.step_outputs.len(), 1);
    assert_eq!(
        ctx.step_outputs.get(&outer),
        Some(&ExecutionValue::Count(17))
    );
    ctx.variables.insert(variable, ExecutionValue::Count(4));
    assert!(
        matches!(ctx.match_source_ids(&source, usize::MAX, Limits::default()).await,
        Err(crate::cypher::Error::Query(error)) if error.detail == "InvalidAccessResult")
    );
    assert_eq!(ctx.step_outputs.len(), 1);
    let source = r::PlannedNode {
        slot: r::Slot(0),
        estimated_rows: 1,
        access: test_support::executable(
            ir::PlanKind::Read,
            vec![test_support::step(
                1,
                vec![],
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam {
                            param: test_support::name("ids"),
                        },
                    )),
                },
            )],
            1,
        ),
    };
    let ids = ctx
        .match_source_ids(&source, 1, Limits::default())
        .await
        .unwrap();
    assert_eq!(ids.iter().collect::<Vec<_>>(), vec![node]);
    drop(ids);
    assert_eq!(ctx.row_budget().available(), 64 * 1024);
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}

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
            let input = Rows::new(candidates.data.clone(), ctx.row_budget()).unwrap();
            // The two matching rows must retain their allocations while path
            // assembly and predicate filtering move them into the destination.
            let allocations = [
                input[0].as_ptr() as usize,
                input[model.len()].as_ptr() as usize,
            ];
            let matched = ctx
                .finish_pattern_rows(
                    input,
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
            assert_eq!(
                actual
                    .iter()
                    .map(|row| row.as_ptr() as usize)
                    .collect::<Vec<_>>(),
                allocations.into_iter().take(demand).collect::<Vec<_>>()
            );
        }
    }
    ctx.close_request_read_view().unwrap();
    drop(ctx);
    db.close().await.unwrap();
}
