use super::super::ElementRef;
use super::memory::Rows;
use super::{push_row, ExecutionContext, ExecutionValue, Limits, Result, RowBuffer};
use futures::StreamExt;
use helix_planner::{exec, ir, relational as r};
use r::GraphValues;
use std::collections::BTreeMap;

#[cfg(test)]
#[path = "tests/pattern_finish.rs"]
mod tests;

#[derive(Clone, Copy)]
pub(super) struct Match<'a> {
    pub pattern: &'a r::Pattern,
    pub optional: bool,
    pub predicate: Option<&'a r::Expression>,
    pub demand: usize,
}

/// Each expansion level owns a bounded current batch and its continuation.
struct PatternFrame<'a> {
    depth: usize,
    batches: std::pin::Pin<Box<dyn futures::Stream<Item = Result<Rows>> + Send + 'a>>,
    rows: Option<super::memory::IntoRows>,
}

impl ExecutionContext<'_> {
    pub(super) async fn match_rows(
        &mut self,
        input: Rows,
        operation: Match<'_>,
        plan: &r::MatchPlan,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Rows> {
        let Match {
            pattern,
            optional,
            predicate: _,
            demand: row_demand,
        } = operation;
        if input.is_empty() || row_demand == 0 {
            return Rows::new(Vec::new(), self.row_budget());
        }
        let mut scans = BTreeMap::new();
        let mut scan_bytes = 0_usize;
        let mut scan_memory = self.row_budget().reserve(0)?;
        // Source streams are materialized once per operator, not once per
        // outer row. Adjacency expansions continue to use the current snapshot.
        for step in &plan.steps {
            let (r::MatchStep::Scan(slot) | r::MatchStep::HashJoin { slot, .. }) = step else {
                continue;
            };
            let source = plan
                .sources
                .iter()
                .find(|source| source.slot == *slot)
                .expect("planned scan has a source");
            let ids = self.match_source_ids(source, row_demand).await?;
            scan_bytes = scan_bytes
                .saturating_add(ids.capacity().saturating_mul(size_of::<u64>()))
                .saturating_add(64);
            scan_memory.resize(scan_bytes)?;
            scans.insert(*slot, ids);
        }
        let mut join_tables = BTreeMap::new();
        let mut output = RowBuffer::new(self.row_budget())?;
        for outer in input {
            if output.len() >= row_demand {
                break;
            }
            self.check_execution_deadline()?;
            // Late-bound parameters may have passed static binding as `Any`.
            // A non-graph value is a type error, not an empty graph match.
            for node in &pattern.nodes {
                if plan.incoming.contains(&node.slot)
                    && !matches!(
                        outer[node.slot.0 as usize],
                        r::Value::Null | r::Value::Entity(r::Entity::Node(_))
                    )
                {
                    return Err(r::QueryError::runtime(
                        "TypeError",
                        "ExpectedNode",
                        "a bound node pattern requires a node or null",
                    )
                    .into());
                }
            }
            for relationship in &pattern.relationships {
                if plan.incoming.contains(&relationship.slot)
                    && !matches!(
                        outer[relationship.slot.0 as usize],
                        r::Value::Null | r::Value::Entity(r::Entity::Relationship(_))
                    )
                {
                    return Err(r::QueryError::runtime(
                        "TypeError",
                        "ExpectedRelationship",
                        "a bound relationship pattern requires a relationship or null",
                    )
                    .into());
                }
            }
            if plan
                .incoming
                .iter()
                .any(|s| outer[s.0 as usize] == r::Value::Null)
            {
                if optional {
                    push_row(&mut output, outer, limits)?;
                }
                continue;
            }
            let mut candidates = Rows::new(vec![outer.clone()], self.row_budget())?;
            for step in &plan.steps {
                let mut next = RowBuffer::new(self.row_budget())?;
                match step {
                    r::MatchStep::Scan(slot) => {
                        for row in candidates {
                            for id in &scans[slot] {
                                let mut row = row.clone();
                                row[slot.0 as usize] = r::Value::Entity(r::Entity::Node(*id));
                                push_row(&mut next, row, limits)?;
                            }
                        }
                    }
                    r::MatchStep::IndexLookup(lookup) => {
                        for row in candidates {
                            use helix_ast::value::PropertyValue as P;
                            let value = match &row[lookup.probe.0 as usize] {
                                r::Value::Null => continue,
                                r::Value::Boolean(value) => Some(P::Bool(*value)),
                                r::Value::Integer(value) => Some(P::I64(*value)),
                                r::Value::Float(value) => Some(P::F64(*value)),
                                r::Value::String(value) => Some(P::String(value.clone())),
                                r::Value::List(_)
                                | r::Value::Map(_)
                                | r::Value::Entity(_)
                                | r::Value::Path(_) => None,
                            };
                            let literal =
                                value.and_then(|value| ir::SecondaryIndexLiteral::new(value).ok());
                            if let Some(literal) = literal {
                                self.index_lookup_rows(&row, lookup, literal, &mut next, limits)
                                    .await?;
                            } else {
                                // Lists/maps cannot be represented by this storage index.
                                // Load the original source once, only when such a probe occurs.
                                let ids = match scans.entry(lookup.slot) {
                                    std::collections::btree_map::Entry::Occupied(entry) => {
                                        entry.into_mut()
                                    }
                                    std::collections::btree_map::Entry::Vacant(entry) => {
                                        let source = plan
                                            .sources
                                            .iter()
                                            .find(|source| source.slot == lookup.slot)
                                            .expect("validated lookup has a fallback source");
                                        let ids = self.match_source_ids(source, row_demand).await?;
                                        scan_bytes = scan_bytes
                                            .saturating_add(
                                                ids.capacity().saturating_mul(size_of::<u64>()),
                                            )
                                            .saturating_add(64);
                                        scan_memory.resize(scan_bytes)?;
                                        entry.insert(ids)
                                    }
                                };
                                for id in ids.iter() {
                                    let mut result = row.clone();
                                    result[lookup.slot.0 as usize] =
                                        r::Value::Entity(r::Entity::Node(*id));
                                    push_row(&mut next, result, limits)?;
                                }
                            }
                        }
                    }
                    r::MatchStep::Expand { .. } => {
                        let operation = exec::ExecOp::Expand {
                            plan: ir::ExpandPlan {
                                direction: ir::ExpandDirection::Both,
                                output: ir::ExpandOutput::Edges,
                                label: ir::ExpandLabelPlan::Any,
                            },
                        };
                        self.flush_required_mutations(
                            super::super::mutation::visibility::required_for(&operation),
                        )
                        .await?;
                        for row in candidates {
                            let batches = self.expansion_batches(row, pattern, step, limits);
                            futures::pin_mut!(batches);
                            while let Some(batch) = batches.next().await {
                                for row in batch? {
                                    push_row(&mut next, row, limits)?;
                                }
                            }
                        }
                        candidates = next.finish();
                        continue;
                    }
                    r::MatchStep::HashJoin {
                        slot,
                        property,
                        probe,
                        probe_property,
                    } => {
                        if !candidates.is_empty() {
                            if !join_tables.contains_key(slot) {
                                let table = super::joins::HashJoinTable::build(
                                    self,
                                    &scans[slot],
                                    outer.len(),
                                    *slot,
                                    property,
                                    limits,
                                )
                                .await?;
                                join_tables.insert(*slot, table);
                            }
                            candidates = join_tables[slot]
                                .probe(self, candidates, *slot, *probe, probe_property, limits)
                                .await?;
                        }
                        continue;
                    }
                }
                candidates = next.finish();
                if candidates.is_empty() {
                    break;
                }
            }
            let matched = self
                .finish_pattern_rows(&candidates, operation, parameters, limits, &mut output)
                .await?;
            if optional && !matched {
                push_row(&mut output, outer, limits)?;
            }
        }
        Ok(output.finish())
    }
    async fn match_source_ids(
        &mut self,
        source: &r::PlannedNode,
        row_demand: usize,
    ) -> Result<Vec<u64>> {
        let mut steps = source.access.steps().to_vec();
        if row_demand != usize::MAX
            && let Some(step) = steps.iter_mut().find(|s| s.id == source.access.root())
            && let exec::ExecOp::Access { plan } = &mut step.op
        {
            **plan = plan.as_ref().clone().limited(
                helix_planner::properties::PositiveUsize::at_least_one(row_demand),
            );
        }
        let subplan = exec::ExecutableSubplan::new(
            ir::AtLeast::try_from_vec(steps).expect("access has steps"),
            source.access.root(),
        )
        .map_err(|e| {
            r::QueryError::runtime("InternalPlannerError", "InvalidAccessPlan", e.to_string())
        })?;
        let value = self.execute_subplan(&subplan).await?;
        let ExecutionValue::Stream(rows) = value else {
            return Err(r::QueryError::runtime(
                "InternalPlannerError",
                "InvalidAccessResult",
                "graph access did not produce rows",
            )
            .into());
        };
        let ids = rows
            .into_iter()
            .filter_map(|row| match row.current {
                Some(ElementRef::Node(id)) => Some(id),
                Some(ElementRef::Edge(_)) | None => None,
            })
            .collect::<Vec<_>>();
        Ok(ids)
    }
    /// Stream an initial independent connected pattern in the selected order.
    /// Unlike the materialized reference, the depth-first stack never retains
    /// the Cartesian product of all preceding expansion levels.
    pub(super) fn graph_match_batches<'a>(
        &'a self,
        cursor: super::scan::NodeCursor,
        width: usize,
        operation: Match<'a>,
        plan: &'a r::MatchPlan,
        parameters: &'a BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> impl futures::Stream<Item = Result<Rows>> + 'a {
        let Some(r::MatchStep::Scan(slot)) = plan.steps.first() else {
            unreachable!("validated streamed pattern has an initial scan");
        };
        assert!(plan.incoming.is_empty());
        assert!(plan.steps[1..]
            .iter()
            .all(|step| matches!(step, r::MatchStep::Expand { .. })));
        let roots = Box::pin(self.node_id_batches(cursor, width, *slot, limits));
        let stack = vec![PatternFrame {
            depth: 1,
            batches: roots,
            rows: None,
        }];
        futures::stream::try_unfold(
            (stack, None, operation.optional),
            move |(mut stack, mut memory, unmatched_optional)| async move {
                if memory.is_none() {
                    memory = Some(
                        self.row_budget().reserve(
                            plan.steps
                                .len()
                                .saturating_mul(size_of::<PatternFrame<'_>>()),
                        )?,
                    );
                    stack
                        .try_reserve_exact(plan.steps.len().saturating_sub(stack.len()))
                        .map_err(|_| {
                            super::resource("MemoryLimit", "pattern stack allocation failed")
                        })?;
                }
                loop {
                    let mut candidates = RowBuffer::new(self.row_budget())?;
                    while candidates.len() < limits.batch_rows {
                        self.check_execution_deadline()?;
                        let Some(frame) = stack.last_mut() else {
                            break;
                        };
                        let Some(row) = frame.rows.as_mut().and_then(Iterator::next) else {
                            let Some(batch) = frame.batches.next().await else {
                                stack.pop();
                                continue;
                            };
                            frame.rows = Some(batch?.into_iter());
                            continue;
                        };
                        let depth = frame.depth;
                        if depth == plan.steps.len() {
                            push_row(&mut candidates, row, limits)?;
                            continue;
                        }
                        stack.push(PatternFrame {
                            depth: depth + 1,
                            batches: Box::pin(self.expansion_batches(
                                row,
                                operation.pattern,
                                &plan.steps[depth],
                                limits,
                            )),
                            rows: None,
                        });
                    }
                    if candidates.len() == 0 {
                        if unmatched_optional {
                            push_row(&mut candidates, vec![r::Value::Null; width], limits)?;
                            return Ok(Some((candidates.finish(), (stack, memory, false))));
                        }
                        return Ok(None);
                    }
                    let mut output = RowBuffer::new(self.row_budget())?;
                    self.finish_pattern_rows(
                        &candidates.finish(),
                        operation,
                        parameters,
                        limits,
                        &mut output,
                    )
                    .await?;
                    if output.len() > 0 {
                        return Ok(Some((output.finish(), (stack, memory, false))));
                    }
                }
            },
        )
    }

    /// Validate complete candidates in batches. Both execution strategies use
    /// identical label/property, path and predicate semantics.
    async fn finish_pattern_rows(
        &self,
        candidates: &Rows,
        operation: Match<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
        output: &mut RowBuffer,
    ) -> Result<bool> {
        let Match {
            pattern,
            predicate,
            demand: row_demand,
            ..
        } = operation;
        let mut demand = BTreeMap::<r::Slot, r::PropertyDemand>::new();
        for node in &pattern.nodes {
            demand
                .entry(node.slot)
                .or_default()
                .merge(&r::PropertyDemand::Keys(
                    node.properties.iter().map(|(k, _)| k.clone()).collect(),
                ));
            for (_, expression) in &node.properties {
                expression.graph_requirements(&mut demand);
            }
        }
        for rel in &pattern.relationships {
            demand
                .entry(rel.slot)
                .or_default()
                .merge(&r::PropertyDemand::Keys(
                    rel.properties.iter().map(|(k, _)| k.clone()).collect(),
                ));
            for (_, expression) in &rel.properties {
                expression.graph_requirements(&mut demand);
            }
        }
        if let Some(expression) = predicate {
            expression.graph_requirements(&mut demand);
        }
        // Named paths are assembled after hydration; map their demand onto
        // constituent slots so expressions such as head(nodes(p)).name work.
        for path in &pattern.paths {
            if let Some(properties) = demand.remove(&path.slot) {
                for slot in path.nodes.iter().chain(&path.relationships) {
                    demand.entry(*slot).or_default().merge(&properties);
                }
            }
        }
        let mut matched = false;
        for batch in candidates.chunks(limits.batch_rows) {
            let graph = self.graph_batch_required(batch, &demand).await?;
            for row in batch {
                if output.len() >= row_demand {
                    break;
                }
                let evaluation = self.evaluate(row, parameters, &graph, limits);
                let mut valid = true;
                for node in &pattern.nodes {
                    let r::Value::Entity(entity @ r::Entity::Node(_)) = row[node.slot.0 as usize]
                    else {
                        valid = false;
                        break;
                    };
                    if node.label.as_ref().is_some_and(|label| {
                        graph
                            .label(entity)
                            .is_ok_and(|stored| stored != Some(label.as_str()))
                    }) {
                        valid = false;
                        break;
                    }
                    for (key, value) in &node.properties {
                        let stored = graph.property(entity, key)?;
                        if stored.equals(&evaluation.eval(value)?) != Some(true) {
                            valid = false;
                            break;
                        }
                    }
                }
                if !valid {
                    continue;
                }
                for rel in &pattern.relationships {
                    let r::Value::Entity(entity @ r::Entity::Relationship(_)) =
                        row[rel.slot.0 as usize]
                    else {
                        valid = false;
                        break;
                    };
                    if !rel.types.is_empty()
                        && !rel.types.iter().any(|label| {
                            graph
                                .label(entity)
                                .is_ok_and(|stored| stored == Some(label.as_str()))
                        })
                    {
                        valid = false;
                        break;
                    }
                    for (key, value) in &rel.properties {
                        let stored = graph.property(entity, key)?;
                        if stored.equals(&evaluation.eval(value)?) != Some(true) {
                            valid = false;
                            break;
                        }
                    }
                }
                if !valid {
                    continue;
                }
                let mut row = row.clone();
                for path in &pattern.paths {
                    let nodes = path
                        .nodes
                        .iter()
                        .map(|slot| {
                            let r::Value::Entity(r::Entity::Node(id)) = row[slot.0 as usize] else {
                                unreachable!("validated path references a node checked above")
                            };
                            id
                        })
                        .collect();
                    let relationships = path
                        .relationships
                        .iter()
                        .map(|slot| {
                            let r::Value::Entity(r::Entity::Relationship(id)) =
                                row[slot.0 as usize]
                            else {
                                unreachable!(
                                    "validated path references a relationship checked above"
                                )
                            };
                            id
                        })
                        .collect();
                    row[path.slot.0 as usize] = r::Value::Path(r::Path::new(nodes, relationships)?);
                }
                if let Some(predicate) = predicate
                    && self
                        .evaluate(&row, parameters, &graph, limits)
                        .eval(predicate)?
                        .truth()?
                        != Some(true)
                {
                    continue;
                }
                push_row(output, row, limits)?;
                matched = true;
            }
        }
        Ok(matched)
    }
}
