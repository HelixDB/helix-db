//! Demand-driven polling and completion of one cursor.
use super::*;

/// What one poll of a cursor's node produced.
enum Step<'a> {
    /// The next item, or `None` once the node is exhausted.
    Item(Option<ExecutionValue>),
    /// The node is replaced; poll the replacement.
    Become(Node<'a>),
}

impl<'a> Cursor<'a> {
    pub(super) fn next<'b>(
        &'b mut self,
        ctx: &'b mut ExecutionContext<'_>,
    ) -> BoxFuture<'b, Result<Option<ExecutionValue>>>
    where
        'a: 'b,
    {
        Box::pin(async move {
            loop {
                ctx.check_execution_deadline()?;
                // Each node kind polls in its own boxed future, so every level
                // of a cursor tree costs this loop and that one kind's frame
                // on the stack, never the temporaries of every kind.
                let item = match step(&mut self.node, ctx, self.name, self.produced_rows).await? {
                    Step::Item(item) => item,
                    Step::Become(node) => {
                        self.node = node;
                        continue;
                    }
                };
                if let Some(ExecutionValue::Stream(rows)) = &item {
                    self.produced_rows = self
                        .produced_rows
                        .checked_add(rows.len())
                        .ok_or_else(|| HelixDbError::Query("pull row count overflow".into()))?;
                    self.row_mode |= rows.iter().any(|row| !row.bindings.is_empty());
                    ctx.enforce_row_mode_count(self.name, self.produced_rows, self.row_mode)?;
                }
                return Ok(item);
            }
        })
    }

    pub(super) fn drain<'b>(
        &'b mut self,
        ctx: &'b mut ExecutionContext<'_>,
    ) -> BoxFuture<'b, Result<ExecutionValue>>
    where
        'a: 'b,
    {
        Box::pin(async move {
            let mut out = self.shape.empty();
            while let Some(item) = self.next(ctx).await? {
                match &mut out {
                    Some(out) => value::append(out, item)?,
                    None => out = Some(item),
                }
            }
            out.ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "lifecycle cursor did not produce its terminal value".into(),
                )
            })
        })
    }
}

/// Poll one node kind. A kind that consumes input without producing an item
/// polls again within its own future, checking the deadline each time.
fn step<'b, 'a: 'b>(
    node: &'b mut Node<'a>,
    ctx: &'b mut ExecutionContext<'_>,
    name: &'static str,
    produced_rows: usize,
) -> BoxFuture<'b, Result<Step<'a>>> {
    match node {
        Node::Items(items) => Box::pin(async move { Ok(Step::Item(items.next())) }),
        Node::StreamCount { plan, input } => Box::pin(async move {
            let Some(input) = input.take() else {
                return Ok(Step::Item(None));
            };
            Ok(Step::Item(Some(
                cardinality::stream(ctx, plan, *input).await?,
            )))
        }),
        Node::InputCount { input, window } => Box::pin(async move {
            let Some(input) = input.take() else {
                return Ok(Step::Item(None));
            };
            let mut windowed = Cursor {
                shape: input.shape.window("count")?,
                node: Node::Window {
                    input,
                    skip: window.skip,
                    remaining: window.take.map_or(Demand::All, Demand::take),
                },
                produced_rows: 0,
                row_mode: false,
                name: "count window",
            };
            let mut count = 0usize;
            while windowed.next(ctx).await?.is_some() {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| HelixDbError::Query("count overflow".into()))?;
            }
            Ok(Step::Item(Some(ExecutionValue::Count(count))))
        }),
        Node::Inject {
            input,
            variable,
            appended,
            input_done,
        } => Box::pin(async move {
            if !*input_done {
                if let Some(item) = input.next(ctx).await? {
                    return Ok(Step::Item(Some(item)));
                }
                *input_done = true;
                ctx.check_execution_deadline()?;
            }
            if appended.is_none() {
                *appended = Some(Items::new(ExecutionValue::Stream(
                    ctx.stream_rows(ctx.variable_value(variable)?.clone(), "inject")?,
                )));
            }
            Ok(Step::Item(
                appended.as_mut().expect("injected variable opened").next(),
            ))
        }),
        Node::Membership {
            input,
            variable,
            exclude,
            members,
        } => Box::pin(async move {
            if members.is_none() {
                *members = Some(ctx.element_set(ctx.variable_value(variable)?)?);
            }
            loop {
                let Some(item) = input.next(ctx).await? else {
                    return Ok(Step::Item(None));
                };
                let rows = ctx.stream_rows(item, "membership")?;
                let member = rows[0].current.as_ref().is_some_and(|element| {
                    members
                        .as_ref()
                        .expect("prepared membership")
                        .contains(element)
                });
                if member != *exclude {
                    return Ok(Step::Item(Some(ExecutionValue::Stream(rows))));
                }
                ctx.check_execution_deadline()?;
            }
        }),
        Node::Branch(branch) => Box::pin(async move { Ok(Step::Item(branch.next(ctx).await?)) }),
        Node::Repeat(repeat) => Box::pin(async move { Ok(Step::Item(repeat.next(ctx).await?)) }),
        Node::Scoped { input, context } => Box::pin(async move {
            let scope = scope::Scope::new(ctx, context);
            Ok(Step::Item(input.next(scope.context).await?))
        }),
        Node::CountLeaf {
            plan,
            dependency,
            pending,
        } => Box::pin(async move {
            let source = match plan {
                exec::ExecCountCursorPlan::NodePointReads(ids) => Some(source::Source::ids(
                    ids.as_ref().to_vec(),
                    exec::ElementKeyspace::NodeProperty,
                    false,
                )),
                exec::ExecCountCursorPlan::EdgePointReads(ids) => Some(source::Source::ids(
                    ids.as_ref().to_vec(),
                    exec::ElementKeyspace::EdgeEndpoints,
                    false,
                )),
                exec::ExecCountCursorPlan::NodeRuntimeInput(input) => Some(source::Source::ids(
                    ctx.runtime_ids(input)?,
                    exec::ElementKeyspace::NodeProperty,
                    false,
                )),
                exec::ExecCountCursorPlan::EdgeRuntimeInput(input) => Some(source::Source::ids(
                    ctx.runtime_ids(input)?,
                    exec::ElementKeyspace::EdgeEndpoints,
                    false,
                )),
                exec::ExecCountCursorPlan::NodeLabelBitmap(label) => Some(source::Source::bitmap(
                    ctx.lookup_equality_index_set(
                        "$label",
                        &DbPropertyValue::String(label.to_string()),
                    )
                    .await?,
                    exec::ElementKeyspace::NodeProperty,
                    true,
                )),
                exec::ExecCountCursorPlan::EdgeLabelBitmap(label) => Some(source::Source::bitmap(
                    ctx.lookup_global_edge_label_index(label.as_ref()).await?,
                    exec::ElementKeyspace::EdgeEndpoints,
                    true,
                )),
                exec::ExecCountCursorPlan::EmptyRows
                | exec::ExecCountCursorPlan::InputRows
                | exec::ExecCountCursorPlan::NodeBitmap(_)
                | exec::ExecCountCursorPlan::EdgeBitmap(_)
                | exec::ExecCountCursorPlan::NodeUnique { .. }
                | exec::ExecCountCursorPlan::NodeRange(_)
                | exec::ExecCountCursorPlan::EdgeRange(_)
                | exec::ExecCountCursorPlan::NodeAuthoritativeScan(_)
                | exec::ExecCountCursorPlan::EdgeAuthoritativeScan(_)
                | exec::ExecCountCursorPlan::RuntimeInput(_)
                | exec::ExecCountCursorPlan::NodeFullScan
                | exec::ExecCountCursorPlan::EdgeFullScan
                | exec::ExecCountCursorPlan::NodeVectorSearch { .. }
                | exec::ExecCountCursorPlan::EdgeVectorSearch { .. }
                | exec::ExecCountCursorPlan::NodeTextSearch { .. }
                | exec::ExecCountCursorPlan::EdgeTextSearch { .. }
                | exec::ExecCountCursorPlan::NodeDynamicEquality { .. }
                | exec::ExecCountCursorPlan::EdgeDynamicEquality { .. }
                | exec::ExecCountCursorPlan::NodeDynamicMembership { .. }
                | exec::ExecCountCursorPlan::EdgeDynamicMembership { .. }
                | exec::ExecCountCursorPlan::Union { .. }
                | exec::ExecCountCursorPlan::Intersect { .. }
                | exec::ExecCountCursorPlan::Filter { .. }
                | exec::ExecCountCursorPlan::IndexMembership { .. }
                | exec::ExecCountCursorPlan::Window { .. }
                | exec::ExecCountCursorPlan::Order { .. }
                | exec::ExecCountCursorPlan::Expand { .. }
                | exec::ExecCountCursorPlan::VectorSearch { .. }
                | exec::ExecCountCursorPlan::TextSearch { .. }
                | exec::ExecCountCursorPlan::Variable { .. }
                | exec::ExecCountCursorPlan::Distinct { .. } => None,
            };
            if let Some(source) = source {
                return Ok(Step::Become(Node::Source {
                    source: Box::new(source),
                    input: None,
                }));
            }
            if pending.is_none() {
                *pending = Some(Items::new(ExecutionValue::Stream(
                    ctx.count_cursor(plan, dependency).await?,
                )));
            }
            Ok(Step::Item(
                pending.as_mut().expect("leaf initialized").next(),
            ))
        }),
        Node::OrderedDistinct { input, previous } => Box::pin(async move {
            loop {
                let Some(item) = input.next(ctx).await? else {
                    return Ok(Step::Item(None));
                };
                let rows = ctx.stream_rows(item, "count distinct")?;
                let row = rows.first().expect("cursor emits one row");
                if previous.as_ref() != Some(row) {
                    *previous = Some(row.clone());
                    return Ok(Step::Item(Some(ExecutionValue::Stream(rows))));
                }
                ctx.check_execution_deadline()?;
            }
        }),
        Node::CountSet {
            driver,
            inputs,
            current,
            intersect,
            sets,
            seen,
            in_driver,
        } => Box::pin(async move {
            if *intersect {
                if sets.is_none() {
                    let mut membership = Vec::new();
                    for mut input in inputs.by_ref() {
                        let value = input.drain(ctx).await?;
                        membership.push(
                            ctx.stream_rows(value, "count intersection")?
                                .into_iter()
                                .collect::<BTreeSet<_>>(),
                        );
                    }
                    *sets = Some(membership);
                }
                loop {
                    let Some(item) = driver.next(ctx).await? else {
                        return Ok(Step::Item(None));
                    };
                    let rows = ctx.stream_rows(item, "count intersection")?;
                    let row = rows.first().expect("cursor emits one row");
                    if sets
                        .as_ref()
                        .expect("membership prepared")
                        .iter()
                        .all(|set| set.contains(row))
                    {
                        return Ok(Step::Item(Some(ExecutionValue::Stream(rows))));
                    }
                    ctx.check_execution_deadline()?;
                }
            }
            loop {
                let item = if *in_driver {
                    match driver.next(ctx).await? {
                        Some(item) => item,
                        None => {
                            *in_driver = false;
                            ctx.check_execution_deadline()?;
                            continue;
                        }
                    }
                } else {
                    if current.is_none() {
                        *current = inputs.next().map(Box::new);
                    }
                    let Some(input) = current else {
                        return Ok(Step::Item(None));
                    };
                    match input.next(ctx).await? {
                        Some(item) => item,
                        None => {
                            *current = None;
                            ctx.check_execution_deadline()?;
                            continue;
                        }
                    }
                };
                let rows = ctx.stream_rows(item, "count union")?;
                let row = rows.first().expect("cursor emits one row");
                let fresh = seen.insert(row.clone());
                if *in_driver || fresh {
                    return Ok(Step::Item(Some(ExecutionValue::Stream(rows))));
                }
                ctx.check_execution_deadline()?;
            }
        }),
        Node::Concat { inputs, current } => Box::pin(async move {
            loop {
                if current.is_none() {
                    *current = inputs.next().map(Box::new);
                }
                let Some(input) = current else {
                    return Ok(Step::Item(None));
                };
                if let Some(item) = input.next(ctx).await? {
                    return Ok(Step::Item(Some(
                        ctx.limit(item, &ir::StreamBoundPlan::Literal(1))?,
                    )));
                }
                *current = None;
            }
        }),
        Node::Intersect {
            driver,
            rest,
            sets,
            emitted,
        } => Box::pin(async move {
            if let Some(rest) = rest.take() {
                for mut input in rest {
                    let value = input.drain(ctx).await?;
                    sets.push(ctx.stream_rows(value, "merge")?.into_iter().collect());
                }
            }
            loop {
                let Some(item) = driver.next(ctx).await? else {
                    return Ok(Step::Item(None));
                };
                let rows = ctx.stream_rows(item, "merge")?;
                let row = rows.first().expect("cursor emits one row");
                if emitted.insert(row.clone()) && sets.iter().all(|set| set.contains(row)) {
                    return Ok(Step::Item(Some(ExecutionValue::Stream(rows))));
                }
                ctx.check_execution_deadline()?;
            }
        }),
        Node::Source { source, input } => Box::pin(async move {
            if let Some(mut input) = input.take() {
                input.drain(ctx).await?;
            }
            Ok(Step::Item(source.next(ctx).await?))
        }),
        Node::Expand {
            plan,
            input,
            label,
            parent,
            ids,
        } => Box::pin(async move {
            loop {
                if let Some(id) = ids.next() {
                    let mut row = parent
                        .as_ref()
                        .expect("prepared expansion has a parent")
                        .clone();
                    row.set_current(match plan.output {
                        ir::ExpandOutput::Nodes => ElementRef::Node(id),
                        ir::ExpandOutput::Edges => ElementRef::Edge(id),
                    });
                    return Ok(Step::Item(Some(ExecutionValue::Stream(vec![row]))));
                }
                if label.is_none() {
                    *label = Some(match plan.output {
                        ir::ExpandOutput::Nodes => None,
                        ir::ExpandOutput::Edges => {
                            Box::pin(ctx.edge_output_label(&plan.label)).await?
                        }
                    });
                }
                let Some(item) = input.next(ctx).await? else {
                    return Ok(Step::Item(None));
                };
                let row = ctx
                    .stream_rows(item, "expand")?
                    .pop()
                    .expect("cursor emits one row");
                *ids = Box::pin(ctx.expansion_ids(
                    &row,
                    plan,
                    label.as_ref().and_then(Option::as_ref),
                ))
                .await?;
                *parent = Some(row);
                ctx.check_execution_deadline()?;
            }
        }),
        Node::Window {
            input,
            skip,
            remaining,
        } => Box::pin(async move {
            if matches!(remaining, Demand::Done) {
                tracing::trace!(
                    operator = name,
                    produced_rows,
                    reason = "demand_satisfied",
                    "pull cursor stopped"
                );
                return Ok(Step::Item(None));
            }
            while *skip > 0 {
                if input.next(ctx).await?.is_none() {
                    return Ok(Step::Item(None));
                }
                *skip -= 1;
            }
            let Some(item) = input.next(ctx).await? else {
                return Ok(Step::Item(None));
            };
            remaining.consume();
            Ok(Step::Item(Some(
                ctx.limit(item, &ir::StreamBoundPlan::Literal(1))?,
            )))
        }),
        Node::Map {
            op,
            input,
            pending,
            distinct,
        } => Box::pin(async move {
            loop {
                let Some(item) = pending.next() else {
                    let Some(item) = input.next(ctx).await? else {
                        return Ok(Step::Item(None));
                    };
                    *pending = Items::new(ctx.execute_op(op, item).await?);
                    ctx.check_execution_deadline()?;
                    continue;
                };
                let fresh = match distinct {
                    None => true,
                    Some(seen) => {
                        let ExecutionValue::Scalars(items) = &item else {
                            unreachable!("distinct binding projection emits scalars");
                        };
                        seen.insert(stream::DistinctKey(items[0].clone()))
                    }
                };
                if fresh {
                    return Ok(Step::Item(Some(item)));
                }
                ctx.check_execution_deadline()?;
            }
        }),
        Node::Distinct {
            input,
            rows,
            scalars,
        } => Box::pin(async move {
            loop {
                let Some(item) = input.next(ctx).await? else {
                    return Ok(Step::Item(None));
                };
                let item = ctx.limit(item, &ir::StreamBoundPlan::Literal(1))?;
                let fresh = match &item {
                    ExecutionValue::Stream(row) => {
                        rows.insert(stream::RowDistinctKey::from(&row[0]))
                    }
                    ExecutionValue::Scalars(items) => {
                        scalars.insert(stream::DistinctKey(items[0].clone()))
                    }
                    ExecutionValue::FoldedStream(_)
                    | ExecutionValue::Count(_)
                    | ExecutionValue::Bool(_)
                    | ExecutionValue::IndexDdlReceipt(_)
                    | ExecutionValue::IndexOperationStatus(_) => {
                        unreachable!("window normalizes each cursor item")
                    }
                };
                if fresh {
                    return Ok(Step::Item(Some(item)));
                }
                ctx.check_execution_deadline()?;
            }
        }),
        Node::Filter { predicate, input } => Box::pin(async move {
            loop {
                let Some(item) = input.next(ctx).await? else {
                    return Ok(Step::Item(None));
                };
                let rows = ctx.stream_rows(item, "filter")?;
                let row = rows.first().expect("cursor emits one row");
                if Box::pin(ctx.eval_predicate_plan(row, predicate)).await? {
                    return Ok(Step::Item(Some(ExecutionValue::Stream(rows))));
                }
                ctx.check_execution_deadline()?;
            }
        }),
        Node::IndexMembership {
            plan,
            input,
            membership,
        } => Box::pin(async move {
            loop {
                let Some(item) = input.next(ctx).await? else {
                    return Ok(Step::Item(None));
                };
                let rows = ctx.stream_rows(item, "index membership")?;
                let row = rows.first().expect("cursor emits one row");
                let keep = match Box::pin(membership.decide(ctx, plan, row)).await? {
                    stream::RowDecision::Keep => true,
                    stream::RowDecision::Drop => false,
                    stream::RowDecision::Evaluate(predicate) => {
                        Box::pin(ctx.eval_predicate_plan(row, predicate)).await?
                    }
                };
                if keep {
                    return Ok(Step::Item(Some(ExecutionValue::Stream(rows))));
                }
                ctx.check_execution_deadline()?;
            }
        }),
        Node::Exists(input) => Box::pin(async move {
            let Some(mut input) = input.take() else {
                return Ok(Step::Item(None));
            };
            Ok(Step::Item(Some(match input.next(ctx).await? {
                Some(item) => Box::pin(ctx.project(item, &ir::ProjectionPlan::Exists)).await?,
                None => ExecutionValue::Bool(false),
            })))
        }),
        Node::Full { op, input } => Box::pin(async move {
            let Some(mut input) = input.take() else {
                return Ok(Step::Item(None));
            };
            let input = input.drain(ctx).await?;
            let value = ctx.execute_op(op, input).await?;
            Ok(Step::Become(Node::Items(Items::new(value))))
        }),
    }
}
