use super::super::ElementRef;
use super::memory::Rows;
use super::{push_row, ExecutionContext, ExecutionValue, Limits, Result, RowBuffer};
use crate::query_resources::bitmap;
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

impl Match<'_> {
    /// Validate late-bound graph values before inspecting nulls. A null input
    /// prevents matching; it does not suppress a type error in another binding.
    pub(super) fn validate_incoming(self, outer: &r::Row, plan: &r::MatchPlan) -> Result<bool> {
        for node in &self.pattern.nodes {
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
        for relationship in &self.pattern.relationships {
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
        Ok(!plan
            .incoming
            .iter()
            .any(|slot| outer[slot.0 as usize] == r::Value::Null))
    }
}

/// Receives surviving rows with their position in the candidate batch. The
/// position lets correlated consumers retain outer-row identity through filters.
pub(super) trait PatternOutput {
    fn len(&self) -> usize;
    fn retain(&mut self, position: usize, row: r::Row) -> Result<()>;
}
impl PatternOutput for RowBuffer {
    fn len(&self) -> usize {
        self.len()
    }
    fn retain(&mut self, _position: usize, row: r::Row) -> Result<()> {
        self.push_with(super::row_bytes(&row), || row)
    }
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
            // Demand counts complete pattern matches. A candidate source row
            // may have no qualifying expansion, so source truncation is unsound
            // when the physical schedule falls back to materialized execution.
            let ids = self.match_source_ids(source, usize::MAX, limits).await?;
            // The bitmap owns its decoded/construction admission. Cover sparse
            // B-tree cache nodes before inserting their slot and bitmap handles.
            scan_bytes = scan_bytes.saturating_add(1024);
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
            if !operation.validate_incoming(&outer, plan)? {
                if optional {
                    push_row(&mut output, outer, limits)?;
                }
                continue;
            }
            let mut initial = RowBuffer::new(self.row_budget())?;
            initial.push_with(super::row_bytes(&outer), || outer.clone())?;
            let mut candidates = initial.finish();
            for step in &plan.steps {
                let mut next = RowBuffer::new(self.row_budget())?;
                match step {
                    r::MatchStep::Scan(slot) => {
                        for row in candidates {
                            for id in scans[slot].iter() {
                                if next.len().is_multiple_of(limits.batch_rows) {
                                    self.check_execution_deadline()?;
                                }
                                next.push_replacing(
                                    &row,
                                    *slot,
                                    r::Value::Entity(r::Entity::Node(id)),
                                )?;
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
                                        let ids = self
                                            .match_source_ids(source, usize::MAX, limits)
                                            .await?;
                                        scan_bytes = scan_bytes.saturating_add(1024);
                                        scan_memory.resize(scan_bytes)?;
                                        entry.insert(ids)
                                    }
                                };
                                for id in ids.iter() {
                                    if next.len().is_multiple_of(limits.batch_rows) {
                                        self.check_execution_deadline()?;
                                    }
                                    next.push_replacing(
                                        &row,
                                        lookup.slot,
                                        r::Value::Entity(r::Entity::Node(id)),
                                    )?;
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
                        let batches = self.expansion_batches(candidates, pattern, step, limits);
                        futures::pin_mut!(batches);
                        while let Some(batch) = batches.next().await {
                            for row in batch? {
                                push_row(&mut next, row, limits)?;
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
                .finish_pattern_rows(candidates, operation, parameters, limits, &mut output)
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
        limits: Limits,
    ) -> Result<bitmap::Bitmap> {
        let mut output = bitmap::Builder::new(Some(self.row_budget()))?;
        if row_demand == 0 {
            return Ok(output.finish());
        }
        // Cypher source roots are node sets. Consume production-selected
        // cursors directly without constructing a native materialized relation.
        let cursor = if let [step] = source.access.steps() {
            // Opening a primitive directly must retain the native scheduler's
            // visibility barrier for label/equality changes staged by CREATE.
            self.flush_required_mutations(super::super::mutation::visibility::required_for(
                &step.op,
            ))
            .await?;
            self.node_cursor(&step.op).await?
        } else {
            None
        };
        if let Some(cursor) = cursor {
            let batches = self.node_id_batches(
                cursor,
                1,
                r::Slot(0),
                Limits {
                    batch_rows: limits.batch_rows.min(row_demand),
                    ..limits
                },
            );
            futures::pin_mut!(batches);
            let mut found = 0_usize;
            while let Some(batch) = batches.next().await {
                for row in batch?.iter().take(row_demand - found) {
                    let r::Value::Entity(r::Entity::Node(id)) = row[0] else {
                        unreachable!("node cursor produces node IDs");
                    };
                    output.insert(id)?;
                    found += 1;
                }
                if found == row_demand {
                    break;
                }
            }
            return Ok(output.finish());
        }
        // Keep specialized native access contracts as an explicit fallback.
        // Their result construction is still owned by the native executor.
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
        for row in rows {
            let Some(ElementRef::Node(id)) = row.current else {
                continue;
            };
            output.insert(id)?;
        }
        Ok(output.finish())
    }
    /// Stream an initial independent fixed pattern in the selected order.
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
        assert!(plan.steps[1..].iter().all(|step| matches!(
            step,
            r::MatchStep::Scan(_) | r::MatchStep::Expand { .. } | r::MatchStep::HashJoin { .. }
        )));
        // Demand bounds candidate/probe batches, not blocking source builds.
        // The consumer decides when enough complete result rows have arrived.
        let candidate_limits = Limits {
            batch_rows: limits.batch_rows.min(operation.demand.max(1)),
            ..limits
        };
        let roots = self.node_id_batches(cursor, width, *slot, candidate_limits);
        let stack = super::expansion_stack::ExpansionStack::new(
            operation.pattern,
            plan,
            1,
            roots,
            self.row_budget(),
        )
        .and_then(|stack| {
            Ok((
                stack,
                super::expansion_stack::SourceCache::new(
                    plan,
                    1,
                    limits.batch_rows,
                    self.row_budget(),
                )?,
            ))
        });
        futures::stream::try_unfold(
            (stack, operation.optional),
            move |(stack, unmatched_optional)| async move {
                let (mut stack, mut cache) = stack?;
                loop {
                    let candidates = self
                        .row_budget()
                        .admitted_future(stack.next_batch(self, candidate_limits, &mut cache))?
                        .await?;
                    let Some(candidates) = candidates else {
                        if unmatched_optional {
                            let mut candidates = RowBuffer::new(self.row_budget())?;
                            candidates.push_with(
                                size_of::<r::Row>()
                                    .saturating_add(width.saturating_mul(size_of::<r::Value>())),
                                || vec![r::Value::Null; width],
                            )?;
                            return Ok(Some((candidates.finish(), (Ok((stack, cache)), false))));
                        }
                        return Ok(None);
                    };
                    let mut output = RowBuffer::new(self.row_budget())?;
                    self.row_budget()
                        .admitted_future(self.finish_pattern_rows(
                            candidates,
                            operation,
                            parameters,
                            limits,
                            &mut output,
                        ))?
                        .await?;
                    if output.len() > 0 {
                        return Ok(Some((output.finish(), (Ok((stack, cache)), false))));
                    }
                }
            },
        )
    }

    /// Validate complete candidates in batches. Both execution strategies use
    /// identical label/property, path and predicate semantics.
    pub(super) async fn finish_pattern_rows<O: PatternOutput>(
        &self,
        candidates: Rows,
        operation: Match<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
        output: &mut O,
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
        let mut position = 0_usize;
        let mut candidates = candidates.into_iter();
        while output.len() < row_demand && !candidates.as_slice().is_empty() {
            self.check_execution_deadline()?;
            let count = candidates.as_slice().len().min(limits.batch_rows);
            let graph = self
                .graph_batch_required(&candidates.as_slice()[..count], &demand)
                .await?;
            for mut row in candidates.by_ref().take(count) {
                if output.len() >= row_demand {
                    break;
                }
                let current_position = position;
                position += 1;
                let evaluation = self.evaluate(&row, parameters, &graph, limits);
                let mut valid = true;
                for node in &pattern.nodes {
                    let r::Value::Entity(entity @ r::Entity::Node(_)) = row[node.slot.0 as usize]
                    else {
                        valid = false;
                        break;
                    };
                    let matches_label = node
                        .label
                        .as_deref()
                        .map(|expected| graph.label(entity).map(|stored| stored == Some(expected)))
                        .transpose()?;
                    if matches_label == Some(false) {
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
                    if !rel.types.is_empty() {
                        let stored = graph.label(entity)?;
                        if !rel.types.iter().any(|label| stored == Some(label.as_str())) {
                            valid = false;
                            break;
                        }
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
                // Candidate rows move through this boundary. Admit the extra
                // path ID vectors before constructing them; incoming payloads
                // remain charged to the source iterator until it is dropped.
                let _path_memory = self.row_budget().reserve(pattern.paths.iter().fold(
                    0_usize,
                    |bytes, path| {
                        bytes.saturating_add(
                            path.nodes
                                .len()
                                .saturating_add(path.relationships.len())
                                .saturating_mul(size_of::<u64>()),
                        )
                    },
                ))?;
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
                output.retain(current_position, row)?;
                matched = true;
            }
        }
        Ok(matched)
    }
}
