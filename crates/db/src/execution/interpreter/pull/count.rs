//! Structural count cursors use the same pull operators as row-returning plans.
//! The selected leaf primitives and identity rules remain unchanged.
use super::*;

enum Program<'a> {
    Input,
    Empty,
    Leaf(&'a exec::ExecCountCursorPlan),
    Apply {
        input: Box<Self>,
        op: Box<exec::ExecOp>,
    },
    Window {
        input: Box<Self>,
        window: &'a exec::ExecCountWindowPlan,
    },
    Set {
        inputs: Vec<Self>,
        intersect: bool,
    },
    OrderedDistinct(Box<Self>),
}

impl<'a> Program<'a> {
    fn new(plan: &'a exec::ExecCountCursorPlan) -> Self {
        use exec::ExecCountCursorPlan as C;
        let apply = |input: &'a C, op| Self::Apply {
            input: Box::new(Self::new(input)),
            op: Box::new(op),
        };
        let source = |plan| Self::Apply {
            input: Box::new(Self::Empty),
            op: Box::new(exec::ExecOp::Access {
                plan: Box::new(plan),
            }),
        };
        match plan {
            C::InputRows => Self::Input,
            C::NodeFullScan => source(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::AllScan,
            )),
            C::EdgeFullScan => source(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::AllScan,
            )),
            C::NodeAuthoritativeScan(predicate) => source(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::AuthoritativeScan {
                    predicate: predicate.clone(),
                },
            )),
            C::EdgeAuthoritativeScan(predicate) => source(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::AuthoritativeScan {
                    predicate: predicate.clone(),
                },
            )),
            C::NodeBitmap(bitmap) => source(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::Bitmap {
                    bitmap: bitmap.clone(),
                },
            )),
            C::EdgeBitmap(bitmap) => source(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::Bitmap {
                    bitmap: bitmap.clone(),
                },
            )),
            C::NodeDynamicEquality { index, key, param } => source(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::DynamicEquality {
                    index: index.clone(),
                    key: key.clone(),
                    param: param.clone(),
                },
            )),
            C::NodeDynamicMembership { index, key, values } => source(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::DynamicMembership {
                    index: index.clone(),
                    key: key.clone(),
                    values: values.clone(),
                },
            )),
            C::EdgeDynamicEquality { index, key, param } => source(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::DynamicEquality {
                    index: index.clone(),
                    key: key.clone(),
                    param: param.clone(),
                },
            )),
            C::EdgeDynamicMembership { index, key, values } => source(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::DynamicMembership {
                    index: index.clone(),
                    key: key.clone(),
                    values: values.clone(),
                },
            )),
            C::Filter { input, predicate } => apply(
                input,
                exec::ExecOp::Filter {
                    predicate: predicate.clone(),
                },
            ),
            C::Expand { input, plan } => apply(input, exec::ExecOp::Expand { plan: plan.clone() }),
            C::Order { input, plan } => apply(input, exec::ExecOp::Order { plan: plan.clone() }),
            C::VectorSearch { input, plan } => {
                apply(input, exec::ExecOp::VectorSearch { plan: plan.clone() })
            }
            C::TextSearch { input, plan } => {
                apply(input, exec::ExecOp::TextSearch { plan: plan.clone() })
            }
            C::Variable { input, op } => apply(
                input,
                exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::Stream(op.to_stream_op()),
                },
            ),
            C::Distinct {
                input,
                plan: exec::ExecCountDistinctPlan::HashRows,
            } => apply(input, exec::ExecOp::Distinct),
            C::Distinct {
                input,
                plan: exec::ExecCountDistinctPlan::OrderedRows,
            } => Self::OrderedDistinct(Box::new(Self::new(input))),
            C::Window { input, window } => Self::Window {
                input: Box::new(Self::new(input)),
                window,
            },
            C::Union { driver, rest } | C::Intersect { driver, rest } => Self::Set {
                inputs: std::iter::once(driver.as_ref())
                    .chain(rest.as_ref())
                    .map(Self::new)
                    .collect(),
                intersect: matches!(plan, C::Intersect { .. }),
            },
            C::NodeRange(plan) => source(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::RangeIndex {
                    index: plan.index.clone(),
                    key: plan.key.clone(),
                    range: plan.range.clone(),
                    iteration: ir::RangeScanIteration::Forward,
                },
            )),
            C::EdgeRange(plan) => source(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::RangeIndex {
                    index: plan.index.clone(),
                    key: plan.key.clone(),
                    range: plan.range.clone(),
                    iteration: ir::RangeScanIteration::Forward,
                },
            )),
            C::EmptyRows
            | C::NodeUnique { .. }
            | C::NodePointReads(_)
            | C::EdgePointReads(_)
            | C::NodeRuntimeInput(_)
            | C::EdgeRuntimeInput(_)
            | C::RuntimeInput(_)
            | C::NodeLabelBitmap(_)
            | C::EdgeLabelBitmap(_)
            | C::NodeVectorSearch { .. }
            | C::EdgeVectorSearch { .. }
            | C::NodeTextSearch { .. }
            | C::EdgeTextSearch { .. } => Self::Leaf(plan),
        }
    }

    fn validate_contract(&self, ctx: &ExecutionContext<'_>) -> Result<()> {
        match self {
            Self::Window { input, window } => {
                input.validate_contract(ctx)?;
                ctx.count_window(window)?;
            }
            Self::Apply { input, op } => {
                input.validate_contract(ctx)?;
                // Each saved operand keeps its ordinary variable-operation contract.
                let exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::Stream(op),
                } = op.as_ref()
                else {
                    return Ok(());
                };
                match op {
                    ir::StreamVariableOp::Select(variable)
                    | ir::StreamVariableOp::Inject(variable) => {
                        Shape::of(ctx.variable_value(variable)?).require_rows("count cursor")?;
                    }
                    ir::StreamVariableOp::Within(variable)
                    | ir::StreamVariableOp::Without(variable) => {
                        Shape::of(ctx.variable_value(variable)?).require_membership()?;
                    }
                    ir::StreamVariableOp::As(_)
                    | ir::StreamVariableOp::Store(_)
                    | ir::StreamVariableOp::Bind(_) => {}
                }
            }
            Self::OrderedDistinct(input) => input.validate_contract(ctx)?,
            Self::Set { inputs, .. } => {
                for input in inputs {
                    input.validate_contract(ctx)?;
                }
            }
            Self::Leaf(plan) => {
                let (exec::ExecCountCursorPlan::NodeRuntimeInput(
                    exec::ExecRuntimeInputPlan::Variable(variable),
                )
                | exec::ExecCountCursorPlan::EdgeRuntimeInput(
                    exec::ExecRuntimeInputPlan::Variable(variable),
                )
                | exec::ExecCountCursorPlan::RuntimeInput(
                    exec::ExecRuntimeInputPlan::Variable(variable),
                )) = plan
                else {
                    return Ok(());
                };
                Shape::of(ctx.variable_value(variable)?).require_rows("count cursor")?;
            }
            Self::Empty | Self::Input => {}
        }
        Ok(())
    }

    fn cursor<'b>(
        &'b self,
        ctx: &ExecutionContext<'_>,
        dependency: &mut Option<Cursor<'b>>,
    ) -> Result<Cursor<'b>> {
        let node = match self {
            Self::Empty => return Cursor::materialized(ExecutionValue::Stream(Vec::new())),
            Self::Input => {
                let input = dependency.take().ok_or_else(|| {
                    HelixDbError::InvariantViolation(
                        "count cursor consumed its row dependency more than once".into(),
                    )
                })?;
                if input.shape != Shape::Rows {
                    return Err(HelixDbError::InvariantViolation(
                        "count plan expected rows".into(),
                    ));
                }
                return Ok(input);
            }
            Self::Leaf(plan) => Node::CountLeaf {
                plan,
                dependency: None,
                pending: None,
            },
            Self::Apply { input, op } => {
                if let exec::ExecOp::Access { plan } = op.as_ref() {
                    match plan.as_ref() {
                        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::RangeIndex {
                            index,
                            key,
                            ..
                        }) => count::validate_range_index("node_range:", &index.index_id, key)?,
                        exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::RangeIndex {
                            index,
                            key,
                            ..
                        }) => count::validate_range_index("edge_range:", &index.index_id, key)?,
                        exec::ExecAccessPlan::Node(_)
                        | exec::ExecAccessPlan::Edge(_)
                        | exec::ExecAccessPlan::Limited(_) => {}
                    }
                }
                let cursor = input.cursor(ctx, dependency)?.wrap(ctx, op)?;
                // Recursive count programs consume rows at every adapter, not
                // just at their terminal. Windows must not hide scalar shapes.
                cursor.shape.require_rows("count cursor")?;
                return Ok(cursor);
            }
            Self::Window { input, window } => {
                let input = input.cursor(ctx, dependency)?;
                let window = ctx.count_window(window)?;
                Node::Window {
                    input: Box::new(input),
                    skip: window.skip,
                    remaining: window.take.map_or(Demand::All, Demand::take),
                }
            }
            Self::OrderedDistinct(input) => Node::OrderedDistinct {
                input: Box::new(input.cursor(ctx, dependency)?),
                previous: None,
            },
            Self::Set { inputs, intersect } => {
                let mut inputs = inputs
                    .iter()
                    .map(|input| input.cursor(ctx, dependency))
                    .collect::<Result<Vec<_>>>()?;
                let driver = Box::new(inputs.remove(0));
                Node::CountSet {
                    driver,
                    inputs: inputs.into_iter(),
                    current: None,
                    intersect: *intersect,
                    sets: None,
                    seen: BTreeSet::new(),
                    in_driver: true,
                }
            }
        };
        Ok(Cursor {
            shape: Shape::Rows,
            node,
            produced_rows: 0,
            row_mode: false,
            name: "count cursor",
        })
    }
}

impl ExecutionContext<'_> {
    pub(in crate::execution::interpreter) async fn pull_count_rows(
        &mut self,
        plan: &exec::ExecCountCursorPlan,
        dependency: &mut Option<ExecutionValue>,
    ) -> Result<Vec<ExecutionRow>> {
        let program = Program::new(plan);
        program.validate_contract(self)?;
        let mut input = dependency.take().map(Cursor::materialized).transpose()?;
        let mut cursor = program.cursor(self, &mut input)?;
        let value = cursor.drain(self).await?;
        self.stream_rows(value, "count cursor")
    }

    pub(in crate::execution::interpreter) async fn pull_count_cardinality(
        &mut self,
        plan: &exec::ExecCountCursorPlan,
        dependency: &mut Option<ExecutionValue>,
        skip: usize,
        take: Option<usize>,
    ) -> Result<usize> {
        let program = Program::new(plan);
        program.validate_contract(self)?;
        let mut dependency = dependency.take().map(Cursor::materialized).transpose()?;
        let input = program.cursor(self, &mut dependency)?;
        let mut cursor = Cursor {
            shape: Shape::Rows,
            node: Node::Window {
                input: Box::new(input),
                skip,
                remaining: take.map_or(Demand::All, Demand::take),
            },
            produced_rows: 0,
            row_mode: false,
            name: "count cursor",
        };
        let mut count = 0usize;
        while cursor.next(self).await?.is_some() {
            count = count
                .checked_add(1)
                .ok_or_else(|| HelixDbError::Query("count overflow".into()))?;
        }
        Ok(count)
    }
}

pub(super) async fn stream<'a>(
    ctx: &mut ExecutionContext<'_>,
    plan: &'a exec::ExecCountStreamPlan,
    input: Cursor<'a>,
) -> Result<ExecutionValue> {
    let program = Program::new(&plan.cursor);
    program.validate_contract(ctx)?;
    let mut dependency = Some(input);
    let input = program.cursor(ctx, &mut dependency)?;
    let window = ctx.count_window(&plan.window)?;
    let mut cursor = Cursor {
        shape: Shape::Rows,
        node: Node::Window {
            input: Box::new(input),
            skip: window.skip,
            remaining: window.take.map_or(Demand::All, Demand::take),
        },
        produced_rows: 0,
        row_mode: false,
        name: "count cursor",
    };
    let mut count = 0usize;
    while cursor.next(ctx).await?.is_some() {
        count = count
            .checked_add(1)
            .ok_or_else(|| HelixDbError::Query("count overflow".into()))?;
    }
    Ok(ExecutionValue::Count(count))
}

pub(in crate::execution::interpreter) fn validate_contract(
    ctx: &ExecutionContext<'_>,
    plan: &exec::ExecCountPlan,
) -> Result<()> {
    if let exec::ExecCountPlan::Stream(plan) = plan {
        Program::new(&plan.cursor).validate_contract(ctx)?;
    }
    if let Some(window) = count::count_plan_window(plan) {
        ctx.count_window(window)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn recursive_count_adapters_reject_non_rows_before_polling() {
        use exec::ExecCountCursorPlan as C;
        let db = test_support::open_db("recursive-count-row-types").await;
        let saved = test_support::name("saved");
        let select = C::Variable {
            input: Box::new(C::EmptyRows),
            op: helix_planner::logical::PureStreamVariableOp::Select(saved.clone()),
        };
        let zero = exec::ExecCountWindowPlan {
            skip: exec::ExecUsizeExpr::Literal(0),
            take: exec::ExecCountTake::AtMost(exec::ExecUsizeExpr::Literal(0)),
        };
        let mut plans = [
            select.clone(),
            C::Window {
                input: Box::new(select.clone()),
                window: zero.clone(),
            },
            C::Distinct {
                input: Box::new(select.clone()),
                plan: exec::ExecCountDistinctPlan::OrderedRows,
            },
            C::Union {
                driver: Box::new(C::EmptyRows),
                rest: ir::AtLeast::from_one(select.clone()),
            },
            C::Intersect {
                driver: Box::new(C::EmptyRows),
                rest: ir::AtLeast::from_one(select),
            },
            C::RuntimeInput(exec::ExecRuntimeInputPlan::Variable(saved.clone())),
            C::NodeRuntimeInput(exec::ExecRuntimeInputPlan::Variable(saved.clone())),
            C::EdgeRuntimeInput(exec::ExecRuntimeInputPlan::Variable(saved.clone())),
        ]
        .into_iter()
        .map(|plan| (plan, false))
        .collect::<Vec<_>>();
        for op in [
            helix_planner::logical::PureStreamVariableOp::Inject(saved.clone()),
            helix_planner::logical::PureStreamVariableOp::Within(saved.clone()),
            helix_planner::logical::PureStreamVariableOp::Without(saved.clone()),
        ] {
            let accepts_folded = matches!(
                op,
                helix_planner::logical::PureStreamVariableOp::Within(_)
                    | helix_planner::logical::PureStreamVariableOp::Without(_)
            );
            for input in [C::EmptyRows, C::InputRows] {
                let variable = C::Variable {
                    input: Box::new(input),
                    op: op.clone(),
                };
                plans.push((variable.clone(), accepts_folded));
                plans.push((
                    C::Window {
                        input: Box::new(variable),
                        window: zero.clone(),
                    },
                    accepts_folded,
                ));
            }
        }
        for value in [
            ExecutionValue::Scalars(Vec::new()),
            ExecutionValue::Scalars(vec![ExecutionScalar::Value(DbPropertyValue::I64(1))]),
            ExecutionValue::Count(0),
            ExecutionValue::Bool(false),
            ExecutionValue::FoldedStream(FoldedStream::new(Vec::new())),
            ExecutionValue::FoldedStream(FoldedStream::new(vec![ExecutionRow::current(
                access::kv::element_ref(exec::ElementKeyspace::NodeProperty, 1),
            )])),
            ExecutionValue::Stream(Vec::new()),
            ExecutionValue::Stream(vec![ExecutionRow::current(access::kv::element_ref(
                exec::ElementKeyspace::NodeProperty,
                1,
            ))]),
            ExecutionValue::Stream(vec![ExecutionRow::current(access::kv::element_ref(
                exec::ElementKeyspace::EdgeEndpoints,
                1,
            ))]),
        ]
        .into_iter()
        .map(Some)
        .chain(std::iter::once(None))
        {
            let rows_valid = matches!(value, Some(ExecutionValue::Stream(_)));
            let folded = matches!(value, Some(ExecutionValue::FoldedStream(_)));
            for op in [
                ir::StreamVariableOp::Inject(saved.clone()),
                ir::StreamVariableOp::Within(saved.clone()),
                ir::StreamVariableOp::Without(saved.clone()),
            ] {
                let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
                value.iter().for_each(|value| {
                    ctx.variables.insert(saved.clone(), value.clone());
                });
                let valid = rows_valid
                    || (folded
                        && matches!(
                            op,
                            ir::StreamVariableOp::Within(_) | ir::StreamVariableOp::Without(_)
                        ));
                let op = exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::Stream(op),
                };
                for rows in [
                    Vec::new(),
                    vec![ExecutionRow::current(access::kv::element_ref(
                        exec::ElementKeyspace::NodeProperty,
                        1,
                    ))],
                ] {
                    assert_eq!(
                        Cursor::materialized(ExecutionValue::Stream(rows))
                            .unwrap()
                            .wrap(&ctx, &op)
                            .is_ok(),
                        valid
                    );
                    assert_eq!(ctx.pull_work.snapshot().source_visits, 0);
                }
            }
            for (plan, accepts_folded) in &plans {
                let valid = rows_valid || (folded && *accepts_folded);
                let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
                value.iter().for_each(|value| {
                    ctx.variables.insert(saved.clone(), value.clone());
                });
                let dependency = || {
                    Some(ExecutionValue::Stream(vec![ExecutionRow::current(
                        access::kv::element_ref(exec::ElementKeyspace::NodeProperty, 1),
                    )]))
                };
                assert_eq!(
                    ctx.pull_count_rows(plan, &mut dependency()).await.is_ok(),
                    valid
                );
                for take in [Some(0), Some(1), None] {
                    assert_eq!(
                        ctx.pull_count_cardinality(plan, &mut dependency(), 0, take)
                            .await
                            .is_ok(),
                        valid
                    );
                }
                for window in [zero.clone(), exec::ExecCountWindowPlan::identity()] {
                    let plan = exec::ExecCountStreamPlan {
                        cursor: plan.clone(),
                        window,
                    };
                    assert_eq!(
                        stream(
                            &mut ctx,
                            &plan,
                            Cursor::materialized(ExecutionValue::Stream(Vec::new())).unwrap()
                        )
                        .await
                        .is_ok(),
                        valid
                    );
                    let op = exec::ExecOp::Count {
                        plan: Box::new(exec::ExecCountPlan::Stream(plan)),
                    };
                    // This is the constructor used even when an outer LIMIT 0
                    // suppresses the entire count node's polling.
                    assert_eq!(
                        Cursor::materialized(ExecutionValue::Stream(Vec::new()))
                            .unwrap()
                            .wrap(&ctx, &op)
                            .is_ok(),
                        valid
                    );
                }
            }
        }
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn folded_membership_matches_eager_rows_and_counts() {
        let db = test_support::open_db("folded-membership-contract").await;
        let saved = test_support::name("saved");
        let node = ExecutionRow::current(access::kv::element_ref(
            exec::ElementKeyspace::NodeProperty,
            1,
        ));
        let edge = ExecutionRow::current(access::kv::element_ref(
            exec::ElementKeyspace::EdgeEndpoints,
            1,
        ));
        let other = ExecutionRow::current(access::kv::element_ref(
            exec::ElementKeyspace::NodeProperty,
            2,
        ));
        for operand_rows in [
            vec![],
            vec![node.clone()],
            vec![edge.clone(), edge.clone(), ExecutionRow::empty()],
        ] {
            for folded in [false, true] {
                let value = if folded {
                    ExecutionValue::FoldedStream(FoldedStream::new(operand_rows.clone()))
                } else {
                    ExecutionValue::Stream(operand_rows.clone())
                };
                for rows in [
                    vec![],
                    vec![
                        node.clone(),
                        edge.clone(),
                        other.clone(),
                        node.clone(),
                        ExecutionRow::empty(),
                    ],
                ] {
                    for op in [
                        helix_planner::logical::PureStreamVariableOp::Within(saved.clone()),
                        helix_planner::logical::PureStreamVariableOp::Without(saved.clone()),
                    ] {
                        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
                        ctx.variables.insert(saved.clone(), value.clone());
                        let input = ExecutionValue::Stream(rows.clone());
                        let variable = exec::ExecOp::Variable {
                            op: exec::ExecVariableOp::Stream(op.to_stream_op()),
                        };
                        let expected_value = ctx
                            .variable(
                                input.clone(),
                                &exec::ExecVariableOp::Stream(op.to_stream_op()),
                            )
                            .unwrap();
                        let expected = ctx.stream_rows(expected_value, "test").unwrap();
                        let plan = exec::ExecCountCursorPlan::Variable {
                            input: Box::new(exec::ExecCountCursorPlan::InputRows),
                            op,
                        };
                        assert_eq!(
                            ctx.pull_count_rows(&plan, &mut Some(input.clone()))
                                .await
                                .unwrap(),
                            expected
                        );
                        for take in [Some(0), Some(1), None] {
                            let cursor = Cursor::materialized(input.clone())
                                .unwrap()
                                .wrap(&ctx, &variable)
                                .unwrap();
                            let limit = exec::ExecOp::Limit {
                                count: ir::StreamBoundPlan::Literal(take.unwrap_or(usize::MAX)),
                            };
                            let actual = cursor
                                .wrap(&ctx, &limit)
                                .unwrap()
                                .drain(&mut ctx)
                                .await
                                .unwrap();
                            let limit = take.unwrap_or(usize::MAX);
                            assert_eq!(
                                ctx.stream_rows(actual, "test").unwrap(),
                                expected.iter().take(limit).cloned().collect::<Vec<_>>()
                            );
                            for skip in [0, 1] {
                                let expected_count = expected.iter().skip(skip).take(limit).count();
                                assert_eq!(
                                    ctx.pull_count_cardinality(
                                        &plan,
                                        &mut Some(input.clone()),
                                        skip,
                                        take
                                    )
                                    .await
                                    .unwrap(),
                                    expected_count
                                );
                                let window = exec::ExecCountWindowPlan {
                                    skip: exec::ExecUsizeExpr::Literal(skip),
                                    take: take.map_or(exec::ExecCountTake::All, |take| {
                                        exec::ExecCountTake::AtMost(exec::ExecUsizeExpr::Literal(
                                            take,
                                        ))
                                    }),
                                };
                                let stream_plan = exec::ExecCountStreamPlan {
                                    cursor: plan.clone(),
                                    window,
                                };
                                let result = stream(
                                    &mut ctx,
                                    &stream_plan,
                                    Cursor::materialized(input.clone()).unwrap(),
                                )
                                .await
                                .unwrap();
                                assert!(
                                    matches!(result, ExecutionValue::Count(count) if count == expected_count)
                                );
                            }
                        }
                        assert_eq!(ctx.pull_work.snapshot().source_visits, 0);
                    }
                }
            }
        }
        db.close().await.unwrap();
    }
}
