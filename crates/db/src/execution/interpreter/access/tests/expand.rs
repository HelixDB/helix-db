use super::support::*;

#[tokio::test]
async fn edge_output_expansion_honors_direction_and_labels() {
    let db = test_support::open_db("access-edge-output-directions").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let knows_id = test_support::add_edge(&db, alice, bob, "KNOWS").await;
    let follows_id = test_support::add_edge(&db, bob, alice, "FOLLOWS").await;
    let from_param = test_support::name("from");

    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::Out,
            ir::ExpandLabelPlan::Any,
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(knows_id)])
    );
    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::In,
            ir::ExpandLabelPlan::Any,
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(follows_id)])
    );
    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::Both,
            ir::ExpandLabelPlan::Any,
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(knows_id),
            ExecutionScalar::EdgeId(follows_id),
        ])
    );
    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::Out,
            ir::ExpandLabelPlan::Label(test_support::name("FOLLOWS")),
        )
        .await,
        ExecutionValue::Scalars(Vec::new())
    );
    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::In,
            ir::ExpandLabelPlan::Label(test_support::name("FOLLOWS")),
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(follows_id)])
    );
}

#[tokio::test]
async fn edge_output_expansion_preserves_input_multiplicity() {
    let db = test_support::open_db("access-edge-output-multiplicity").await;
    let alice = test_support::add_user(&db, "alice").await;
    let bob = test_support::add_user(&db, "bob").await;
    let knows_id = test_support::add_edge(&db, alice, bob, "KNOWS").await;
    let from_param = test_support::name("from");

    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64Array(vec![alice as i64, alice as i64]),
            ir::ExpandDirection::Out,
            ir::ExpandLabelPlan::Label(test_support::name("KNOWS")),
        )
        .await,
        ExecutionValue::Scalars(vec![
            ExecutionScalar::EdgeId(knows_id),
            ExecutionScalar::EdgeId(knows_id),
        ])
    );
}

#[tokio::test]
async fn edge_output_expansion_deduplicates_self_loop_for_both_direction() {
    let db = test_support::open_db("access-edge-output-self-loop").await;
    let alice = test_support::add_user(&db, "alice").await;
    let self_id = test_support::add_edge(&db, alice, alice, "SELF").await;
    let from_param = test_support::name("from");

    assert_eq!(
        run_edge_expand(
            &db,
            &from_param,
            PropertyValue::I64(alice as i64),
            ir::ExpandDirection::Both,
            ir::ExpandLabelPlan::Any,
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(self_id)])
    );
}

mod whole_value {
    use super::*;
    use crate::error::HelixDbError;
    use crate::execution::interpreter::{ElementRef, ExecutionContext, ExecutionRow};

    /// Forty parents with zero to four neighbours each, in scrambled order, a
    /// parent twice, and one node with no adjacency at all.
    async fn fixture(name: &str) -> (HelixDB, Vec<ExecutionRow>) {
        let db = test_support::open_db(name).await;
        let mut parents = Vec::new();
        for index in 0..40 {
            parents.push(test_support::add_user(&db, &format!("parent-{index}")).await);
        }
        for (index, parent) in parents.iter().enumerate() {
            for fan in 0..index % 5 {
                let target = parents[(index + fan + 1) % parents.len()];
                let label = if fan % 2 == 0 { "KNOWS" } else { "FOLLOWS" };
                test_support::add_edge(&db, *parent, target, label).await;
            }
        }
        let rows = parents
            .iter()
            .rev()
            .step_by(3)
            .chain(parents.iter().step_by(3))
            .chain(parents.iter().skip(1).step_by(3))
            .copied()
            .chain([parents[7], u64::MAX])
            .map(|id| ExecutionRow::current(ElementRef::Node(id)))
            .collect();
        (db, rows)
    }

    /// Overlapped parent reads produce exactly the rows a serial walk over
    /// the same parents does, for every direction, label and output.
    #[tokio::test]
    async fn expansion_matches_the_serial_walk() {
        let (db, rows) = fixture("access-expand-serial-oracle").await;
        // More parents than one expansion window holds.
        let rows = rows.iter().cycle().take(300).cloned().collect::<Vec<_>>();
        for direction in [
            ir::ExpandDirection::Out,
            ir::ExpandDirection::In,
            ir::ExpandDirection::Both,
        ] {
            for label in [
                ir::ExpandLabelPlan::Any,
                ir::ExpandLabelPlan::Label(test_support::name("KNOWS")),
                ir::ExpandLabelPlan::Label(test_support::name("MISSING")),
            ] {
                for output in [ir::ExpandOutput::Nodes, ir::ExpandOutput::Edges] {
                    let plan = ir::ExpandPlan {
                        direction,
                        label: label.clone(),
                        output,
                    };
                    let context = ExecutionContext::new(&db, context::ParamBindings::default());
                    let expanded = context
                        .expand(ExecutionValue::Stream(rows.clone()), &plan)
                        .await
                        .unwrap();

                    let edge_label = match output {
                        ir::ExpandOutput::Nodes => None,
                        ir::ExpandOutput::Edges => {
                            context.edge_output_label(&plan.label).await.unwrap()
                        }
                    };
                    let mut serial = Vec::new();
                    for row in &rows {
                        for id in context
                            .expansion_ids(row, &plan, edge_label.as_ref())
                            .await
                            .unwrap()
                        {
                            let mut next = row.clone();
                            next.set_current(match output {
                                ir::ExpandOutput::Nodes => ElementRef::Node(id),
                                ir::ExpandOutput::Edges => ElementRef::Edge(id),
                            });
                            serial.push(next);
                        }
                    }
                    assert!(
                        !serial.is_empty() || matches!(plan.label, ir::ExpandLabelPlan::Label(_)),
                        "{plan:?}"
                    );
                    assert_eq!(expanded, ExecutionValue::Stream(serial), "{plan:?}");
                }
            }
        }
    }

    /// Parents are read concurrently up to the request's index-read budget.
    #[tokio::test]
    async fn parents_are_read_concurrently_up_to_the_bound() {
        use std::sync::atomic::Ordering;

        let (db, rows) = fixture("access-expand-concurrency").await;
        let peak = &db.inner.peak_index_child_reads;
        peak.store(0, Ordering::SeqCst);
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        context
            .expand(
                ExecutionValue::Stream(rows),
                &ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    label: ir::ExpandLabelPlan::Label(test_support::name("KNOWS")),
                    output: ir::ExpandOutput::Nodes,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            peak.load(Ordering::SeqCst),
            crate::execution::interpreter::access::PARALLEL_INDEX_READS.get()
        );
    }

    /// The deadline stops an expansion whether it expires before a parent is
    /// read or while its neighbours become rows.
    #[tokio::test]
    async fn expansion_respects_the_deadline() {
        let (db, rows) = fixture("access-expand-deadline").await;
        for successful_checks in [0, 1, 5, 20] {
            let context = ExecutionContext::new(&db, context::ParamBindings::default());
            context.fail_deadline_after(successful_checks);
            assert!(
                matches!(
                    context
                        .expand(
                            ExecutionValue::Stream(rows.clone()),
                            &ir::ExpandPlan {
                                direction: ir::ExpandDirection::Both,
                                label: ir::ExpandLabelPlan::Any,
                                output: ir::ExpandOutput::Nodes,
                            },
                        )
                        .await,
                    Err(HelixDbError::QueryDeadlineExceeded)
                ),
                "{successful_checks}"
            );
        }
    }
}
