//! Demand-driven polling and completion of one cursor.
use super::*;

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
                let item = match &mut self.node {
                    Node::Items(items) => items.next(),
                    Node::StreamCount { plan, input } => {
                        let Some(input) = input.take() else {
                            return Ok(None);
                        };
                        Some(Box::pin(cardinality::stream(ctx, plan, *input)).await?)
                    }
                    Node::InputCount { input, window } => {
                        let Some(input) = input.take() else {
                            return Ok(None);
                        };
                        let mut windowed = Self {
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
                        Some(ExecutionValue::Count(count))
                    }
                    Node::Inject {
                        input,
                        variable,
                        appended,
                        input_done,
                    } => {
                        if !*input_done {
                            match input.next(ctx).await? {
                                Some(item) => Some(item),
                                None => {
                                    *input_done = true;
                                    continue;
                                }
                            }
                        } else {
                            if appended.is_none() {
                                *appended =
                                    Some(Items::new(ExecutionValue::Stream(ctx.stream_rows(
                                        ctx.variable_value(variable)?.clone(),
                                        "inject",
                                    )?)));
                            }
                            appended.as_mut().expect("injected variable opened").next()
                        }
                    }
                    Node::Membership {
                        input,
                        variable,
                        exclude,
                        members,
                    } => {
                        if members.is_none() {
                            *members = Some(ctx.element_set(ctx.variable_value(variable)?)?);
                        }
                        let Some(item) = input.next(ctx).await? else {
                            return Ok(None);
                        };
                        let rows = ctx.stream_rows(item, "membership")?;
                        let member = rows[0].current.as_ref().is_some_and(|element| {
                            members
                                .as_ref()
                                .expect("prepared membership")
                                .contains(element)
                        });
                        if member == *exclude {
                            continue;
                        }
                        Some(ExecutionValue::Stream(rows))
                    }
                    Node::Branch(branch) => Box::pin(branch.next(ctx)).await?,
                    Node::Repeat(repeat) => Box::pin(repeat.next(ctx)).await?,
                    Node::Scoped { input, context } => {
                        let scope = scope::Scope::new(ctx, context);
                        input.next(scope.context).await?
                    }
                    Node::CountLeaf {
                        plan,
                        dependency,
                        pending,
                    } => {
                        let source = match plan {
                            exec::ExecCountCursorPlan::NodePointReads(ids) => {
                                Some(source::Source::ids(
                                    ids.as_ref().to_vec(),
                                    exec::ElementKeyspace::NodeProperty,
                                    false,
                                ))
                            }
                            exec::ExecCountCursorPlan::EdgePointReads(ids) => {
                                Some(source::Source::ids(
                                    ids.as_ref().to_vec(),
                                    exec::ElementKeyspace::EdgeEndpoints,
                                    false,
                                ))
                            }
                            exec::ExecCountCursorPlan::NodeRuntimeInput(input) => {
                                Some(source::Source::ids(
                                    ctx.runtime_ids(input)?,
                                    exec::ElementKeyspace::NodeProperty,
                                    false,
                                ))
                            }
                            exec::ExecCountCursorPlan::EdgeRuntimeInput(input) => {
                                Some(source::Source::ids(
                                    ctx.runtime_ids(input)?,
                                    exec::ElementKeyspace::EdgeEndpoints,
                                    false,
                                ))
                            }
                            exec::ExecCountCursorPlan::NodeLabelBitmap(label) => {
                                Some(source::Source::bitmap(
                                    ctx.lookup_equality_index_set(
                                        "$label",
                                        &DbPropertyValue::String(label.to_string()),
                                    )
                                    .await?,
                                    exec::ElementKeyspace::NodeProperty,
                                    true,
                                ))
                            }
                            exec::ExecCountCursorPlan::EdgeLabelBitmap(label) => {
                                Some(source::Source::bitmap(
                                    ctx.lookup_global_edge_label_index(label.as_ref()).await?,
                                    exec::ElementKeyspace::EdgeEndpoints,
                                    true,
                                ))
                            }
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
                            self.node = Node::Source {
                                source: Box::new(source),
                                input: None,
                            };
                            continue;
                        }
                        if pending.is_none() {
                            *pending = Some(Items::new(ExecutionValue::Stream(
                                ctx.count_cursor(plan, dependency).await?,
                            )));
                        }
                        pending.as_mut().expect("leaf initialized").next()
                    }
                    Node::OrderedDistinct { input, previous } => {
                        let Some(item) = input.next(ctx).await? else {
                            return Ok(None);
                        };
                        let rows = ctx.stream_rows(item, "count distinct")?;
                        let row = rows.first().expect("cursor emits one row");
                        if previous.as_ref() == Some(row) {
                            continue;
                        }
                        *previous = Some(row.clone());
                        Some(ExecutionValue::Stream(rows))
                    }
                    Node::CountSet {
                        driver,
                        inputs,
                        current,
                        intersect,
                        sets,
                        seen,
                        in_driver,
                    } => {
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
                            let Some(item) = driver.next(ctx).await? else {
                                return Ok(None);
                            };
                            let rows = ctx.stream_rows(item, "count intersection")?;
                            let row = rows.first().expect("cursor emits one row");
                            if !sets
                                .as_ref()
                                .expect("membership prepared")
                                .iter()
                                .all(|set| set.contains(row))
                            {
                                continue;
                            }
                            Some(ExecutionValue::Stream(rows))
                        } else {
                            let item = if *in_driver {
                                match driver.next(ctx).await? {
                                    Some(item) => item,
                                    None => {
                                        *in_driver = false;
                                        continue;
                                    }
                                }
                            } else {
                                if current.is_none() {
                                    *current = inputs.next().map(Box::new);
                                }
                                let Some(input) = current else {
                                    return Ok(None);
                                };
                                match input.next(ctx).await? {
                                    Some(item) => item,
                                    None => {
                                        *current = None;
                                        continue;
                                    }
                                }
                            };
                            let rows = ctx.stream_rows(item, "count union")?;
                            let row = rows.first().expect("cursor emits one row");
                            let fresh = seen.insert(row.clone());
                            if !*in_driver && !fresh {
                                continue;
                            }
                            Some(ExecutionValue::Stream(rows))
                        }
                    }
                    Node::Concat { inputs, current } => loop {
                        if current.is_none() {
                            *current = inputs.next().map(Box::new);
                        }
                        let Some(input) = current else {
                            return Ok(None);
                        };
                        if let Some(item) = input.next(ctx).await? {
                            break Some(ctx.limit(item, &ir::StreamBoundPlan::Literal(1))?);
                        }
                        *current = None;
                    },
                    Node::Intersect {
                        driver,
                        rest,
                        sets,
                        emitted,
                    } => {
                        if let Some(rest) = rest.take() {
                            for mut input in rest {
                                let value = input.drain(ctx).await?;
                                sets.push(ctx.stream_rows(value, "merge")?.into_iter().collect());
                            }
                        }
                        let Some(item) = driver.next(ctx).await? else {
                            return Ok(None);
                        };
                        let rows = ctx.stream_rows(item, "merge")?;
                        let row = rows.first().expect("cursor emits one row");
                        if !emitted.insert(row.clone()) || !sets.iter().all(|set| set.contains(row))
                        {
                            continue;
                        }
                        Some(ExecutionValue::Stream(rows))
                    }
                    Node::Source { source, input } => {
                        if let Some(mut input) = input.take() {
                            input.drain(ctx).await?;
                        }
                        Box::pin(source.next(ctx)).await?
                    }
                    Node::Expand {
                        plan,
                        input,
                        label,
                        parent,
                        ids,
                    } => {
                        if let Some(id) = ids.next() {
                            let mut row = parent
                                .as_ref()
                                .expect("prepared expansion has a parent")
                                .clone();
                            row.set_current(match plan.output {
                                ir::ExpandOutput::Nodes => ElementRef::Node(id),
                                ir::ExpandOutput::Edges => ElementRef::Edge(id),
                            });
                            Some(ExecutionValue::Stream(vec![row]))
                        } else {
                            if label.is_none() {
                                *label = Some(match plan.output {
                                    ir::ExpandOutput::Nodes => None,
                                    ir::ExpandOutput::Edges => {
                                        Box::pin(ctx.edge_output_label(&plan.label)).await?
                                    }
                                });
                            }
                            let Some(item) = input.next(ctx).await? else {
                                return Ok(None);
                            };
                            let row = ctx
                                .stream_rows(item, "expand")?
                                .pop()
                                .expect("cursor emits one row");
                            *ids = ctx
                                .expansion_ids(&row, plan, label.as_ref().and_then(Option::as_ref))
                                .await?;
                            *parent = Some(row);
                            continue;
                        }
                    }
                    Node::Window {
                        input,
                        skip,
                        remaining,
                    } => {
                        if matches!(remaining, Demand::Done) {
                            tracing::trace!(
                                operator = self.name,
                                produced_rows = self.produced_rows,
                                reason = "demand_satisfied",
                                "pull cursor stopped"
                            );
                            return Ok(None);
                        }
                        while *skip > 0 {
                            if input.next(ctx).await?.is_none() {
                                return Ok(None);
                            }
                            *skip -= 1;
                        }
                        let Some(item) = input.next(ctx).await? else {
                            return Ok(None);
                        };
                        remaining.consume();
                        Some(ctx.limit(item, &ir::StreamBoundPlan::Literal(1))?)
                    }
                    Node::Map {
                        op,
                        input,
                        pending,
                        distinct,
                    } => {
                        let Some(item) = pending.next() else {
                            let Some(item) = input.next(ctx).await? else {
                                return Ok(None);
                            };
                            *pending = Items::new(Box::pin(ctx.execute_op(op, item)).await?);
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
                        if !fresh {
                            continue;
                        }
                        Some(item)
                    }
                    Node::Distinct {
                        input,
                        rows,
                        scalars,
                    } => {
                        let Some(item) = input.next(ctx).await? else {
                            return Ok(None);
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
                        if !fresh {
                            continue;
                        }
                        Some(item)
                    }
                    Node::Filter { predicate, input } => {
                        let Some(item) = input.next(ctx).await? else {
                            return Ok(None);
                        };
                        let rows = ctx.stream_rows(item, "filter")?;
                        let row = rows.first().expect("cursor emits one row");
                        if !ctx.eval_predicate(row, predicate.predicate()).await? {
                            continue;
                        }
                        Some(ExecutionValue::Stream(rows))
                    }
                    Node::IndexMembership {
                        plan,
                        input,
                        prepared,
                    } => {
                        let Some(item) = input.next(ctx).await? else {
                            return Ok(None);
                        };
                        let rows = ctx.stream_rows(item, "index membership")?;
                        let row = rows.first().expect("cursor emits one row");
                        let decision = match (row.current.as_ref(), prepared.as_ref()) {
                            (Some(ElementRef::Node(_)), None) => {
                                let resolved = ctx.prepare_index_membership(plan).await?;
                                prepared.insert(resolved).decide(row)
                            }
                            (_, Some(prepared)) => prepared.decide(row),
                            (Some(ElementRef::Edge(_)) | None, None) => {
                                stream::RowDecision::Evaluate
                            }
                        };
                        let keep = match decision {
                            stream::RowDecision::Keep => true,
                            stream::RowDecision::Drop => false,
                            stream::RowDecision::Evaluate => {
                                ctx.eval_predicate(row, plan.predicate.predicate()).await?
                            }
                        };
                        if !keep {
                            continue;
                        }
                        Some(ExecutionValue::Stream(rows))
                    }
                    Node::Exists(input) => {
                        let Some(mut input) = input.take() else {
                            return Ok(None);
                        };
                        match input.next(ctx).await? {
                            Some(item) => Some(
                                Box::pin(ctx.project(item, &ir::ProjectionPlan::Exists)).await?,
                            ),
                            None => Some(ExecutionValue::Bool(false)),
                        }
                    }
                    Node::Full { op, input } => {
                        let Some(mut input) = input.take() else {
                            return Ok(None);
                        };
                        let input = input.drain(ctx).await?;
                        let value = Box::pin(ctx.execute_op(op, input)).await?;
                        self.node = Node::Items(Items::new(value));
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
