//! A correlated probe uses the selected native equality kernel and bounded
//! cursors. Binding and predicate validation remain owned by the MATCH contract.
use super::{ExecutionContext, GraphBatch, Limits, Result, RowBuffer};
use futures::StreamExt;
use helix_planner::{exec, ir, relational as r};
use r::GraphValues;
use std::collections::BTreeMap;

impl ExecutionContext<'_> {
    pub(super) async fn index_lookup_rows(
        &mut self,
        row: &r::Row,
        lookup: &r::PatternLookup,
        literal: ir::SecondaryIndexLiteral,
        output: &mut RowBuffer,
        limits: Limits,
    ) -> Result<()> {
        let access = exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::exact_equality(
            lookup.index.clone(),
            lookup.key.clone(),
            ir::IndexValue::Literal(literal),
        ));
        let operation = exec::ExecOp::Access {
            plan: Box::new(access),
        };
        self.flush_required_mutations(super::super::mutation::visibility::required_for(&operation))
            .await?;
        let cursor = self
            .node_cursor(&operation)
            .await?
            .expect("nonnull literal equality has a bitmap, unique, or empty cursor");
        let batches = self.node_id_batches(cursor, 1, r::Slot(0), limits);
        futures::pin_mut!(batches);
        while let Some(batch) = batches.next().await {
            for found in batch? {
                output.push_replacing(row, lookup.slot, found[0].clone())?;
            }
        }
        Ok(())
    }
}

/// The probe of each parent row of one lookup, visited in order. A probe
/// property reads the parents' records a batch at a time, as a hash-join
/// probe does.
#[derive(Default)]
pub(super) struct ProbeValues {
    graph: GraphBatch,
    hydrated_end: usize,
}
impl ProbeValues {
    /// The probe of `rows[parent]`, or `None` to scan the source instead. The
    /// scan checks the pattern against each candidate, as a plan without the
    /// index would, so a stored value that cannot be read fails only if a
    /// candidate exists.
    pub(super) async fn value<'a>(
        &'a mut self,
        context: &ExecutionContext<'_>,
        lookup: &r::PatternLookup,
        rows: &'a [r::Row],
        parent: usize,
        limits: Limits,
    ) -> Result<Option<&'a r::Value>> {
        let value = &rows[parent][lookup.probe.0 as usize];
        let Some(property) = &lookup.probe_property else {
            return Ok(Some(value));
        };
        match value {
            r::Value::Null => Ok(Some(value)),
            r::Value::Map(map) => Ok(Some(map.get(property).unwrap_or(&r::Value::Null))),
            r::Value::Entity(entity) => {
                if parent >= self.hydrated_end {
                    let end = rows.len().min(parent.saturating_add(limits.batch_rows));
                    let _demand_memory = context.row_budget().reserve(
                        r::allocation::btree_bytes::<r::Slot, r::PropertyDemand>(1)
                            .saturating_add(r::allocation::btree_bytes::<String, ()>(1))
                            .saturating_add(property.len()),
                    )?;
                    let demand = BTreeMap::from([(
                        lookup.probe,
                        r::PropertyDemand::Keys([property.clone()].into_iter().collect()),
                    )]);
                    self.graph = GraphBatch::default();
                    self.graph = context
                        .graph_batch_required(&rows[parent..end], &demand)
                        .await?;
                    self.hydrated_end = end;
                }
                Ok(self.graph.property(*entity, property).ok())
            }
            r::Value::Boolean(_)
            | r::Value::Integer(_)
            | r::Value::Float(_)
            | r::Value::String(_)
            | r::Value::List(_)
            | r::Value::Path(_) => Ok(None),
        }
    }
}

pub(super) struct IndexedCursor {
    pub(super) cursor: super::scan::NodeCursor,
    pub(super) memory: super::memory::Reservation,
}

/// Cypher equality cannot use native array equality: mixed numeric lists have
/// cross-type equality, while native typed-array keys retain their element type.
pub(super) enum Probe {
    Empty,
    Index(Box<IndexedCursor>),
    Scan,
}
impl Probe {
    pub(super) async fn new(
        context: &ExecutionContext<'_>,
        lookup: &r::PatternLookup,
        probe: &r::Value,
    ) -> Result<Self> {
        use helix_ast::value::PropertyValue as P;
        let string_bytes = match probe {
            r::Value::Null => return Ok(Self::Empty),
            r::Value::String(value) => value.len(),
            r::Value::Boolean(_) | r::Value::Integer(_) | r::Value::Float(_) => 0,
            r::Value::List(_) | r::Value::Map(_) | r::Value::Entity(_) | r::Value::Path(_) => {
                return Ok(Self::Scan);
            }
        };
        // Unique lookup plans retain independent copies for owner lookup and
        // authoritative verification. Admit both before cloning the literal or
        // metadata; this scoped reservation lasts through native cursor setup.
        let copies = match lookup.index.uniqueness {
            helix_planner::catalog::IndexUniqueness::Unique => 2_usize,
            helix_planner::catalog::IndexUniqueness::NonUnique => 1,
        };
        let _scratch = context.row_budget().reserve(
            size_of::<exec::ExecAccessPlan>()
                .saturating_add(lookup.index.index_id.len())
                .saturating_add(
                    copies.saturating_mul(
                        lookup
                            .key
                            .label
                            .len()
                            .saturating_add(lookup.key.property.len())
                            .saturating_add(string_bytes),
                    ),
                ),
        )?;
        let value = match probe {
            r::Value::Boolean(value) => P::Bool(*value),
            r::Value::Integer(value) => P::I64(*value),
            r::Value::Float(value) => P::F64(*value),
            r::Value::String(value) => P::String(value.clone()),
            r::Value::Null
            | r::Value::List(_)
            | r::Value::Map(_)
            | r::Value::Entity(_)
            | r::Value::Path(_) => {
                unreachable!("only scalar, nonnull probes reach index conversion")
            }
        };
        let literal = ir::SecondaryIndexLiteral::new(value)
            .expect("nonnull scalar literals have native equality semantics");
        // Storage rejects a lookup key this large; only the scan answers exactly.
        if literal.may_exceed_index_key() {
            return Ok(Self::Scan);
        }
        let operation = exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::exact_equality(
                    lookup.index.clone(),
                    lookup.key.clone(),
                    ir::IndexValue::Literal(literal),
                ),
            )),
        };
        // Cover continuation transfer overlap. Native bitmap storage retains
        // its own reservation after the temporary access plan is dropped.
        let memory = context
            .row_budget()
            .reserve(2 * size_of::<IndexedCursor>())?;
        let cursor = context
            .node_cursor(&operation)
            .await?
            .expect("nonnull scalar equality has an exact node cursor");
        Ok(Self::Index(Box::new(IndexedCursor { cursor, memory })))
    }
}
