//! A correlated probe uses the selected native equality kernel and bounded
//! cursors. Binding and predicate validation remain owned by the MATCH contract.
use super::{ExecutionContext, GraphBatch, Limits, Result};
use helix_planner::{exec, ir, relational as r};
use r::GraphValues;
use std::collections::BTreeMap;

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
        if lookup.matches == r::LookupMatch::Member {
            return Self::members(context, lookup, probe).await;
        }
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

    /// Candidates for every member of a probe list, read through one batched
    /// lookup. Null and NaN members select nothing. A member the index cannot
    /// answer exactly, such as a list or a string too large to index, scans
    /// the source instead.
    async fn members(
        context: &ExecutionContext<'_>,
        lookup: &r::PatternLookup,
        probe: &r::Value,
    ) -> Result<Self> {
        use helix_ast::value::PropertyValue as P;
        let members = match probe {
            r::Value::Null => return Ok(Self::Empty),
            r::Value::List(members) => members,
            r::Value::Boolean(_)
            | r::Value::Integer(_)
            | r::Value::Float(_)
            | r::Value::String(_)
            | r::Value::Map(_)
            | r::Value::Entity(_)
            | r::Value::Path(_) => return Ok(Self::Scan),
        };
        // The literals, the executable values and the plan's metadata live
        // together until the cursor has read the owners.
        let _scratch = context.row_budget().reserve(
            size_of::<exec::ExecAccessPlan>()
                .saturating_add(lookup.index.index_id.len())
                .saturating_add(lookup.key.label.len())
                .saturating_add(lookup.key.property.len())
                .saturating_add(members.iter().fold(0_usize, |bytes, member| {
                    bytes
                        .saturating_add(2 * size_of::<exec::ExecIndexedEqualityValue>())
                        .saturating_add(match member {
                            r::Value::String(value) => 2 * value.len(),
                            r::Value::Null
                            | r::Value::Boolean(_)
                            | r::Value::Integer(_)
                            | r::Value::Float(_)
                            | r::Value::List(_)
                            | r::Value::Map(_)
                            | r::Value::Entity(_)
                            | r::Value::Path(_) => 0,
                        })
                })),
        )?;
        let mut literals = Vec::with_capacity(members.len());
        for member in members {
            let value = match member {
                r::Value::Null => continue,
                r::Value::Boolean(value) => P::Bool(*value),
                r::Value::Integer(value) => P::I64(*value),
                r::Value::Float(value) => P::F64(*value),
                r::Value::String(value) => P::String(value.clone()),
                r::Value::List(_) | r::Value::Map(_) | r::Value::Entity(_) | r::Value::Path(_) => {
                    return Ok(Self::Scan);
                }
            };
            let literal = ir::SecondaryIndexLiteral::new(value)
                .expect("nonnull scalar literals have native equality semantics");
            if literal.may_exceed_index_key() {
                return Ok(Self::Scan);
            }
            match literal.semantics() {
                ir::LiteralEqualityIndexValueSemantics::Indexed => literals.push(literal),
                ir::LiteralEqualityIndexValueSemantics::NonReflexive => {}
                ir::LiteralEqualityIndexValueSemantics::AuthoritativeNull => {
                    unreachable!("null members are skipped")
                }
            }
        }
        let access = match literals.len() {
            0 => return Ok(Self::Empty),
            1 => exec::ExecNodeAccessPlan::exact_equality(
                lookup.index.clone(),
                lookup.key.clone(),
                ir::IndexValue::Literal(literals.pop().expect("one member")),
            ),
            _ => {
                let values = ir::AtLeast::try_from_vec(
                    literals
                        .into_iter()
                        .map(|literal| {
                            exec::ExecIndexedEqualityValue::try_from(literal)
                                .expect("indexed equality semantics produce an executable value")
                        })
                        .collect(),
                )
                .expect("at least two members");
                let key = lookup.key.clone();
                exec::ExecNodeAccessPlan::SecondarySet {
                    set: match lookup.index.uniqueness {
                        helix_planner::catalog::IndexUniqueness::Unique => {
                            exec::ExecNodeSecondarySetPlan::UniqueUnion {
                                index: exec::ExecNodeUniqueEqualityIndex::try_from(
                                    lookup.index.clone(),
                                )
                                .expect("unique metadata produces a unique executable index"),
                                key,
                                values,
                            }
                        }
                        helix_planner::catalog::IndexUniqueness::NonUnique => {
                            exec::ExecNodeSecondarySetPlan::Bitmap(
                                exec::ExecNodeBitmapExpr::BatchedUnionRead {
                                    index: exec::ExecNodeNonUniqueEqualityIndex::try_from(
                                        lookup.index.clone(),
                                    )
                                    .expect(
                                        "non-unique metadata produces a non-unique executable index",
                                    ),
                                    key,
                                    values,
                                },
                            )
                        }
                    },
                }
            }
        };
        let operation = exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(access)),
        };
        let memory = context
            .row_budget()
            .reserve(2 * size_of::<IndexedCursor>())?;
        let cursor = context
            .node_cursor(&operation)
            .await?
            .expect("an equality set has an exact node cursor");
        Ok(Self::Index(Box::new(IndexedCursor { cursor, memory })))
    }
}
