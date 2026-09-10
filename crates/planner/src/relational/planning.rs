//! Shared physical planning for resolved graph/row queries. Graph access uses
//! the production Cascades rules and executable lowering, not frontend ASTs.
use super::*;
use crate::{catalog, context, exec, ir, logical, optimizer, rules, trace};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct PlannedNode {
    pub slot: Slot,
    pub access: exec::ExecutablePlan,
    pub estimated_rows: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub enum MatchStep {
    Scan(Slot),
    IndexLookup(PatternLookup),
    Expand {
        relationship: usize,
        from: Slot,
        to: Slot,
        reverse: bool,
    },
    HashJoin {
        slot: Slot,
        property: String,
        probe: Slot,
        probe_property: String,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct MatchPlan {
    pub sources: Vec<PlannedNode>,
    pub steps: Vec<MatchStep>,
    pub cartesian_products: usize,
    pub estimated_rows: u64,
    pub incoming: BTreeSet<Slot>,
}

/// Physical program whose schema/effects are inherited from a validated query.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct RowPlan {
    pipeline: RowPipeline,
    matches: BTreeMap<usize, MatchPlan>,
    pub metrics: exec::PlannerMetrics,
}

impl RowPlan {
    /// Select a validated execution strategy without changing graph access,
    /// expression semantics, schemas, or effect boundaries. This also permits
    /// independent batch-versus-materialized execution checks.
    pub fn with_execution(mut self, execution: RowExecution) -> Self {
        self.pipeline = RowPipeline::new(std::sync::Arc::new(self.query().clone()), execution);
        self
    }
    /// Validated adjacent consumer. Node cursor availability also depends on the
    /// selected native access primitive; unsupported primitives keep their executor.
    pub fn batch_consumer(&self, source: usize) -> Option<BatchConsumer> {
        let consumer = self.pipeline.batch_consumer(source)?;
        if let Some(plan) = self.matches.get(&source) {
            let Some(MatchStep::Scan(start)) = plan.steps.first() else {
                return None;
            };
            if !plan.steps[1..]
                .iter()
                .all(|step| matches!(step, MatchStep::Expand { .. }))
                || !plan
                    .sources
                    .iter()
                    .any(|source| source.slot == *start && source.access.steps().len() == 1)
            {
                return None;
            }
        }
        Some(consumer)
    }
    /// Safe upstream demand for a window over a total, row-preserving projection.
    /// No filter, aggregation, ordering, distinct, or write boundary is crossed.
    pub fn input_window(&self, operator: usize) -> Option<&InputWindow> {
        self.pipeline.input_window(operator)
    }
    pub fn query(&self) -> &Query {
        self.pipeline.query()
    }
    pub fn pipeline(&self) -> &RowPipeline {
        &self.pipeline
    }
    pub fn matches(&self) -> &BTreeMap<usize, MatchPlan> {
        &self.matches
    }
}

/// Plan shared relational operators using the same access rules and immutable
/// catalog snapshot as the native frontend.
pub fn plan(query: Query, ctx: &context::PlannerContext) -> Result<RowPlan> {
    let query = std::sync::Arc::new(query);
    let (mut accesses, mut schedules, pipeline, metrics) = plan_accesses(&query, ctx)?;
    let mut matches = BTreeMap::new();
    for (index, operator) in query.operators().iter().enumerate() {
        match operator {
            Operator::Match { pattern, .. } => {
                let sources = pattern
                    .nodes
                    .iter()
                    .map(|n| n.slot)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .map(|slot| {
                        accesses
                            .remove(&(index, slot))
                            .expect("every node has a validated access plan")
                    })
                    .collect::<Vec<_>>();
                let bound = query.contracts()[index].input().slots();
                let schedule = schedules
                    .remove(&index)
                    .expect("every match has a selected schedule");
                let incoming = bound.intersection(&pattern.slots()).copied().collect();
                matches.insert(
                    index,
                    MatchPlan {
                        sources,
                        steps: schedule.steps,
                        cartesian_products: schedule.cartesian_products,
                        estimated_rows: schedule.estimated_rows,
                        incoming,
                    },
                );
            }
            Operator::Create(_)
            | Operator::Unwind { .. }
            | Operator::Project { .. }
            | Operator::Filter(_)
            | Operator::Update(_)
            | Operator::Delete { .. } => {}
        }
    }
    Ok(RowPlan {
        pipeline,
        matches,
        metrics,
    })
}

/// All source alternatives share one production Cascades memo and budget.
/// Guardrail exhaustion falls back to the seed implementation rule for an
/// unfiltered label/all scan, preserving correctness without further search.
type PlannedAccesses = BTreeMap<(usize, Slot), PlannedNode>;
type PlannedPatterns = BTreeMap<usize, PatternSchedule>;

fn plan_accesses(
    query: &std::sync::Arc<Query>,
    ctx: &context::PlannerContext,
) -> Result<(
    PlannedAccesses,
    PlannedPatterns,
    RowPipeline,
    exec::PlannerMetrics,
)> {
    use optimizer::OptimizerRule;
    let config = optimizer::OptimizerConfig::from_context(ctx);
    let mut roots = Vec::new();
    let mut nodes = Vec::new();
    for (operator_index, operator) in query.operators().iter().enumerate() {
        let Operator::Match {
            pattern, predicate, ..
        } = operator
        else {
            continue;
        };
        let mut seen = BTreeSet::new();
        for node in &pattern.nodes {
            if !seen.insert(node.slot) {
                continue;
            }
            let label = node.label.as_ref().or_else(|| {
                pattern
                    .nodes
                    .iter()
                    .find(|n| n.slot == node.slot && n.label.is_some())
                    .and_then(|n| n.label.as_ref())
            });
            let mut candidates = vec![match label {
                Some(label) => ir::NodeAccessPlan::LabelScan {
                    label: ir::NonEmptyString::new(label.clone()).expect("validated label"),
                },
                None => ir::NodeAccessPlan::AllScan,
            }];
            if let Some(label) = label {
                let mut equalities = node.properties.clone();
                if let Some(predicate) = predicate
                    && index_predicate_is_total(predicate, pattern)
                {
                    collect_equalities(predicate, node.slot, &mut equalities);
                }
                for (property, value) in equalities {
                    let Some(key) = catalog::ScopedPropertyKey::try_new(label.clone(), property)
                    else {
                        continue;
                    };
                    let Some(index) = ctx.indexes.node_eq.get(&key) else {
                        continue;
                    };
                    let value = match value {
                        Expression::Literal(v) => literal(v),
                        Expression::Parameter(name) => ctx
                            .params
                            .values
                            .get(&ir::NonEmptyString::new(name).expect("validated parameter"))
                            .cloned(),
                        _ => None,
                    };
                    let Some(value) = value else {
                        continue;
                    };
                    if value == helix_ast::value::PropertyValue::Null {
                        continue;
                    }
                    let Ok(value) = ir::SecondaryIndexLiteral::new(value) else {
                        continue;
                    };
                    candidates.push(ir::NodeAccessPlan::EqualityIndex {
                        index: index.clone(),
                        key,
                        value: ir::IndexValue::Literal(value),
                    });
                }
            }
            let node_roots = candidates
                .into_iter()
                .map(|candidate| {
                    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(
                        logical::NodeAccessPath::new(
                            ir::NodeAccessSourcePlan::new(candidate).expect("unfiltered source"),
                        ),
                    ))
                })
                .collect::<Vec<_>>();

            let indices = node_roots
                .into_iter()
                .map(|root| {
                    let index = roots.len();
                    roots.push(root);
                    index
                })
                .collect::<Vec<_>>();
            nodes.push((operator_index, node.slot, indices));
        }
    }
    // Pattern topology is a first-class root in this same memo. Source estimates
    // use the production access implementation kernel; final source alternatives
    // and pattern orders remain selected by Cascades under one shared budget.
    let access_rule = rules::AccessPathImplementationRule::default();
    let mut pattern_roots = Vec::new();
    for (operator_index, operator) in query.operators().iter().enumerate() {
        let Operator::Match {
            pattern, predicate, ..
        } = operator
        else {
            continue;
        };
        let pattern_sources = nodes
            .iter()
            .filter(|(index, _, _)| *index == operator_index)
            .map(|(_, slot, indices)| {
                let (index, alternative) = indices
                    .iter()
                    .map(|index| {
                        let optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
                            alternatives,
                        )) = access_rule.apply(optimizer::RuleInput {
                            expr: &roots[*index],
                            planner_limits: &config.planner_limits,
                            stats: &config.stats,
                            storage: &config.storage,
                            indexes: &config.indexes,
                        })
                        else {
                            unreachable!(
                                "validated unfiltered access always has a seed implementation"
                            );
                        };
                        (
                            *index,
                            alternatives
                                .iter()
                                .next()
                                .expect("nonempty physical alternatives")
                                .clone(),
                        )
                    })
                    .min_by_key(|(_, a)| {
                        crate::optimizer::ordering::alternative_key_for_cost(a, a.cost)
                    })
                    .expect("every source has an access candidate");
                PatternSource {
                    slot: *slot,
                    rows: source_rows(&roots[index], &config),
                    access_cost: alternative.cost,
                }
            })
            .collect();
        let population = |slot| {
            pattern
                .nodes
                .iter()
                .filter(|n| n.slot == slot)
                .find_map(|n| n.label.as_ref())
                .and_then(|label| ir::NonEmptyString::new(label.clone()))
                .and_then(|label| config.stats.node_label_cardinality.get(&label).copied())
                .unwrap_or(config.storage.default_unknown_scan_rows.as_rows())
                .max(1)
        };
        let relationships = pattern
            .relationships
            .iter()
            .map(|rel| {
                let edge_rows = if rel.types.is_empty() {
                    config.storage.default_unknown_scan_rows.as_rows()
                } else {
                    rel.types
                        .iter()
                        .map(|kind| {
                            config
                                .stats
                                .edge_label_cardinality
                                .get(
                                    &ir::NonEmptyString::new(kind.clone()).expect("validated type"),
                                )
                                .copied()
                                .unwrap_or(config.storage.default_unknown_scan_rows.as_rows())
                        })
                        .fold(0_u64, u64::saturating_add)
                };
                let directions = if rel.direction == Direction::Undirected {
                    2
                } else {
                    1
                };
                PatternExpansion {
                    from: rel.from,
                    to: rel.to,
                    relationship: rel.slot,
                    forward_rows: edge_rows
                        .div_ceil(population(rel.from))
                        .saturating_mul(directions),
                    reverse_rows: edge_rows
                        .div_ceil(population(rel.to))
                        .saturating_mul(directions),
                }
            })
            .collect();
        let mut order = GraphPatternOrder::new(
            pattern_sources,
            relationships,
            query.contracts()[operator_index].input().slots(),
        )?;
        // A single equality can probe an incoming scalar without moving a
        // potentially failing expression across another pattern or predicate.
        // Wider conjunctions retain scans until their error ordering is proven.
        let incoming = query.contracts()[operator_index].input().slots();
        if pattern.nodes.len() == 1 && pattern.relationships.is_empty() {
            let node = &pattern.nodes[0];
            let mut equalities = Vec::new();
            if predicate.is_none() && node.properties.len() == 1 {
                equalities.clone_from(&node.properties);
            } else if node.properties.is_empty()
                && let Some(expression @ Expression::Binary(Binary::Equal, _, _)) = predicate
            {
                collect_equalities(expression, node.slot, &mut equalities);
            }
            if !incoming.contains(&node.slot)
                && let Some(label) = &node.label
            {
                for (property, expression) in equalities {
                    let Expression::Slot(probe) = expression else {
                        continue;
                    };
                    if !incoming.contains(&probe) {
                        continue;
                    }
                    let Some(key) = catalog::ScopedPropertyKey::try_new(label.clone(), property)
                    else {
                        continue;
                    };
                    let Some(index) = ctx.indexes.node_eq.get(&key) else {
                        continue;
                    };
                    let estimated_rows = config
                        .storage
                        .equality_index_rows(config.stats.node_eq_cardinality.get(&key).copied())
                        .as_rows();
                    order = order.with_lookups(vec![PatternLookup {
                        slot: node.slot,
                        probe,
                        index: index.clone(),
                        key,
                        estimated_rows,
                    }])?;
                    break;
                }
            }
        }
        // Equality is the complete predicate here: pushing a key evaluation
        // through an earlier short-circuiting expression could expose an error.
        if pattern.relationships.is_empty()
            && pattern.paths.is_empty()
            && pattern.nodes.len() == 2
            && pattern.nodes.iter().all(|n| n.properties.is_empty())
            && let Some(Expression::Binary(Binary::Equal, left, right)) = predicate
            && let (Expression::Property(left, left_key), Expression::Property(right, right_key)) =
                (left.as_ref(), right.as_ref())
            && let (Expression::Slot(left), Expression::Slot(right)) =
                (left.as_ref(), right.as_ref())
            && left != right
            && [left, right]
                .iter()
                .all(|slot| pattern.nodes.iter().any(|n| n.slot == **slot))
        {
            order = order.with_equality(PatternEquality {
                left: *left,
                left_key: left_key.clone(),
                right: *right,
                right_key: right_key.clone(),
            })?;
        }
        pattern_roots.push((operator_index, roots.len()));
        roots.push(logical::LogicalExpr::GraphPattern(order));
    }
    let pipeline_index = roots.len();
    roots.push(logical::LogicalExpr::Rows(std::sync::Arc::clone(query)));
    let seed = rules::SeedRuleSet::default();
    let optimizer = seed.optimizer();
    let result = optimizer
        .optimize_many(
            ir::AtLeast::try_from_vec(roots.clone()).expect("nonempty roots"),
            &config,
        )
        .map_err(|e| planning_error(e.to_string()))?;
    let mut metrics = result.metrics().clone();
    let mut sources = BTreeMap::new();
    for (operator, slot, indices) in nodes {
        let selected = indices
            .iter()
            .filter_map(|index| {
                result
                    .roots()
                    .get(*index)
                    .and_then(|group| result.best_alternative(*group).ok())
                    .map(|alternative| (*index, alternative.clone()))
            })
            .min_by(|(_, a), (_, b)| {
                crate::optimizer::ordering::alternative_key_for_cost(a, a.cost).cmp(
                    &crate::optimizer::ordering::alternative_key_for_cost(b, b.cost),
                )
            });
        let (index, alternative) = match selected {
            Some(selected) => selected,
            None if metrics.guardrail_hit => {
                let index = indices[0];
                let rule = rules::AccessPathImplementationRule::default();
                let optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(alternatives)) =
                    rule.apply(optimizer::RuleInput {
                        expr: &roots[index],
                        planner_limits: &config.planner_limits,
                        stats: &config.stats,
                        storage: &config.storage,
                        indexes: &config.indexes,
                    })
                else {
                    return Err(planning_error("fallback access implementation unavailable"));
                };
                (
                    index,
                    alternatives
                        .iter()
                        .next()
                        .expect("physical effect is nonempty")
                        .clone(),
                )
            }
            None => return Err(planning_error("no executable access alternative")),
        };
        let root = &roots[index];
        let logical::LogicalExpr::AccessPath(logical::AccessPath::Node(_)) = root else {
            return Err(planning_error("node access root changed kind"));
        };
        let estimated_rows = source_rows(root, &config);
        let access = exec::ExecutablePlan::from_selected_executable_alternative(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            trace::PlanningTrace::default(),
            result.metrics().clone(),
            root,
            &alternative,
            &ctx.storage,
        )
        .map_err(|e| planning_error(e.to_string()))?;
        sources.insert(
            (operator, slot),
            PlannedNode {
                slot,
                access,
                estimated_rows: usize::try_from(estimated_rows).unwrap_or(usize::MAX),
            },
        );
    }
    let mut schedules = BTreeMap::new();
    // optimize_many also measures exploration roots that are not selected for
    // execution (for example an all-scan alternative to an equality lookup).
    // Each selected graph schedule already includes its chosen access costs.
    metrics.selected_cost = crate::cost::CostVector::ZERO;
    for (operator, index) in pattern_roots {
        let chosen = result
            .roots()
            .get(index)
            .and_then(|group| result.best_alternative(*group).ok());
        let order = match chosen.map(|a| &a.expr) {
            Some(crate::physical::PhysicalExpr::GraphPattern(order)) => order,
            None if metrics.guardrail_hit => {
                let logical::LogicalExpr::GraphPattern(order) = &roots[index] else {
                    unreachable!("pattern root retains its kind");
                };
                order
            }
            _ => return Err(planning_error("no physical graph pattern order")),
        };
        let schedule = order.schedule(&ctx.storage);
        metrics.selected_cost = metrics.selected_cost.serial(schedule.cost);
        schedules.insert(operator, schedule);
    }
    let selected = result
        .roots()
        .get(pipeline_index)
        .and_then(|group| result.best_alternative(*group).ok());
    let pipeline = match selected.map(|alternative| &alternative.expr) {
        Some(crate::physical::PhysicalExpr::Rows(pipeline)) => pipeline.clone(),
        None if metrics.guardrail_hit => {
            RowPipeline::new(std::sync::Arc::clone(query), RowExecution::Batched)
        }
        _ => return Err(planning_error("no physical row pipeline")),
    };
    metrics.selected_cost = metrics.selected_cost.serial(pipeline.cost(&ctx.storage));
    Ok((sources, schedules, pipeline, metrics))
}

// Extracting an index equality from AND may otherwise suppress an observable
// error in another conjunct. In particular NULL AND an error still evaluates
// that error; treating NULL as an early false result would be incorrect. Keep
// potentially failing arithmetic, functions, and dynamic access above a scan.
fn index_predicate_is_total(expression: &Expression, pattern: &Pattern) -> bool {
    match expression {
        Expression::Literal(Value::Boolean(_) | Value::Null) => true,
        Expression::Binary(Binary::And, left, right) => {
            index_predicate_is_total(left, pattern) && index_predicate_is_total(right, pattern)
        }
        Expression::Binary(Binary::Equal, left, right) => {
            [left, right].iter().all(|operand| match operand.as_ref() {
                Expression::Literal(_) | Expression::Parameter(_) => true,
                Expression::Property(value, _) => matches!(value.as_ref(), Expression::Slot(slot)
                    if pattern.nodes.iter().any(|node| node.slot == *slot)),
                _ => false,
            })
        }
        _ => false,
    }
}

fn collect_equalities(expression: &Expression, slot: Slot, out: &mut Vec<(String, Expression)>) {
    match expression {
        Expression::Binary(Binary::And, a, b) => {
            collect_equalities(a, slot, out);
            collect_equalities(b, slot, out);
        }
        Expression::Binary(Binary::Equal, a, b) => {
            for (a, b) in [(a, b), (b, a)] {
                let Expression::Property(value, key) = a.as_ref() else {
                    continue;
                };
                if value.as_ref() == &Expression::Slot(slot) {
                    out.push((key.clone(), b.as_ref().clone()));
                }
            }
        }
        _ => {}
    }
}

fn literal(value: Value) -> Option<helix_ast::value::PropertyValue> {
    use helix_ast::value::PropertyValue as P;
    match value {
        Value::Null => Some(P::Null),
        Value::Boolean(b) => Some(P::Bool(b)),
        Value::Integer(i) => Some(P::I64(i)),
        Value::Float(f) => Some(P::F64(f)),
        Value::String(s) => Some(P::String(s)),
        Value::List(_) | Value::Map(_) | Value::Entity(_) | Value::Path(_) => None,
    }
}

fn planning_error(message: impl Into<String>) -> QueryError {
    QueryError::compile("InternalPlannerError", "PhysicalPlanning", message)
}

fn source_rows(root: &logical::LogicalExpr, config: &optimizer::OptimizerConfig) -> u64 {
    let logical::LogicalExpr::AccessPath(logical::AccessPath::Node(node)) = root else {
        unreachable!("source candidates are node access roots");
    };
    match node.source().as_ref() {
        ir::NodeAccessPlan::EqualityIndex { key, .. } => config
            .storage
            .equality_index_rows(config.stats.node_eq_cardinality.get(key).copied())
            .as_rows(),
        ir::NodeAccessPlan::LabelScan { label } => config
            .stats
            .node_label_cardinality
            .get(label)
            .copied()
            .unwrap_or(config.storage.default_unknown_scan_rows.as_rows()),
        ir::NodeAccessPlan::AllScan => config.storage.default_unknown_scan_rows.as_rows(),
        _ => unreachable!("candidate construction admits all/label/equality access only"),
    }
}
