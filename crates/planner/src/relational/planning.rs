//! Shared physical planning for resolved graph/row queries. Graph access uses
//! the production Cascades rules and executable lowering, not frontend ASTs.
use super::*;
use crate::{analysis, catalog, context, exec, ir, logical, optimizer, rules, trace};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct PlannedNode {
    pub slot: Slot,
    pub access: Arc<exec::ExecutablePlan>,
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
    pipeline: Arc<RowPipeline>,
    matches: Arc<BTreeMap<usize, MatchPlan>>,
    #[serde(skip)]
    consumers: Arc<BTreeMap<usize, BatchConsumer>>,
    #[serde(skip)]
    program: Arc<RowProgram>,
    pub metrics: exec::PlannerMetrics,
}

impl RowPlan {
    /// Construct a diagnostic reference execution from the validated logical
    /// query. Every node source scans all nodes; graph steps follow binding order
    /// without index probes or hash joins, and row operators materialize. This
    /// does no optimizer exploration and preserves expression/effect boundaries.
    /// Estimates are unit placeholders, not production cost diagnostics.
    ///
    /// ```
    /// use helix_planner::relational as r;
    /// let query = r::Query::new(Vec::new(), Vec::new(), Vec::new()).unwrap();
    /// let reference = r::RowPlan::reference(query).unwrap();
    /// assert_eq!(reference.pipeline().execution(), r::RowExecution::Materialized);
    /// assert_eq!(reference.metrics.memo_groups, 0);
    /// ```
    pub fn reference(query: Query) -> Result<Self> {
        let matches = Arc::new(super::reference::matches(&query)?);
        let pipeline = Arc::new(RowPipeline::new(
            Arc::new(query),
            RowExecution::Materialized,
        ));
        let consumers = Arc::new(BTreeMap::new());
        let program = Arc::new(RowProgram::new(
            Arc::clone(&pipeline),
            Arc::clone(&matches),
            Arc::clone(&consumers),
            RowLayoutMode::Identity,
        ));
        Ok(Self {
            pipeline,
            matches,
            consumers,
            program,
            metrics: exec::PlannerMetrics::default(),
        })
    }

    /// Select a validated execution strategy without changing graph access,
    /// expression semantics, schemas, or effect boundaries. This also permits
    /// independent batch-versus-materialized execution checks.
    pub fn with_execution(mut self, execution: RowExecution) -> Self {
        self.pipeline = Arc::new(RowPipeline::new(
            Arc::clone(&self.pipeline.query),
            execution,
        ));
        self.consumers = Arc::new(super::consumers::prepare(&self.pipeline, &self.matches));
        self.program = Arc::new(RowProgram::new(
            Arc::clone(&self.pipeline),
            Arc::clone(&self.matches),
            Arc::clone(&self.consumers),
            self.program.layout_mode(),
        ));
        self
    }

    /// Select an execution layout without changing logical bindings, access
    /// selection or semantics. Identity is a diagnostic correctness oracle;
    /// planner cost estimates still describe the optimized layout.
    pub fn with_layout(mut self, mode: RowLayoutMode) -> Self {
        self.program = Arc::new(RowProgram::new(
            Arc::clone(&self.pipeline),
            Arc::clone(&self.matches),
            Arc::clone(&self.consumers),
            mode,
        ));
        self
    }

    /// Validated cell-addressed operators for execution. Explain and serialized
    /// plans continue to use the logical query and logical match metadata.
    pub fn program(&self) -> &RowProgram {
        &self.program
    }
    /// Validated adjacent consumer. Node cursor availability also depends on the
    /// selected native access primitive; unsupported primitives keep their executor.
    pub fn batch_consumer(&self, source: usize) -> Option<BatchConsumer> {
        self.consumers.get(&source).copied()
    }
    /// Safe upstream demand for a window over total, row-preserving projections
    /// and OPTIONAL MATCHes that cannot fail and keep at least one row per
    /// input, null-extending unmatched inputs. No
    /// filter, aggregation, ordering, distinct, or write boundary is crossed.
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
    let consumers = Arc::new(super::consumers::prepare(&pipeline, &matches));
    let matches = Arc::new(matches);
    let pipeline = Arc::new(pipeline);
    let program = Arc::new(RowProgram::new(
        Arc::clone(&pipeline),
        Arc::clone(&matches),
        Arc::clone(&consumers),
        RowLayoutMode::Compact,
    ));
    Ok(RowPlan {
        pipeline,
        consumers,
        matches,
        program,
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
    // An IN list shares the optimizer's Boolean index-union budget.
    let union_branches = match ctx.limits.max_index_union_branches {
        context::IndexUnionBranchLimit::Disabled => 0,
        context::IndexUnionBranchLimit::Limited(limit) => limit.get(),
    };
    let mut roots = Vec::new();
    let mut nodes = Vec::new();
    for (operator_index, operator) in query.operators().iter().enumerate() {
        let Operator::Match { pattern, .. } = operator else {
            continue;
        };
        let predicate = index_predicate(query, operator_index, pattern, &ctx.params);
        let totality = Totality {
            pattern,
            bindings: query.bindings(),
            params: Parameters::Bound(&ctx.params),
        };
        // An index source skips candidate nodes, so it must not hide an error
        // that another property constraint of this pattern could raise.
        let constraints_total = pattern
            .nodes
            .iter()
            .flat_map(|node| &node.properties)
            .chain(pattern.relationships.iter().flat_map(|rel| &rel.properties))
            .all(|(_, expression)| totality.operand(expression));
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
            if let Some(label) = label
                && constraints_total
            {
                let mut equalities = node.properties.clone();
                let mut memberships = Vec::new();
                let mut ranges = Vec::new();
                if let Some(predicate) = &predicate
                    && totality.predicate(predicate)
                {
                    collect_equalities(predicate, node.slot, &mut equalities);
                    collect_memberships(predicate, node.slot, &mut memberships);
                    collect_ranges(predicate, node.slot, &ctx.params, &mut ranges);
                }
                let mut exact = BTreeSet::new();
                let mut unique = false;
                for (property, value) in equalities {
                    let Some(key) =
                        catalog::ScopedPropertyKey::try_new(label.clone(), property.clone())
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
                    // Storage rejects an oversized lookup, while no indexed
                    // element can hold one, so only the scan answers exactly.
                    let Ok(value) = ir::SecondaryIndexLiteral::new(value) else {
                        continue;
                    };
                    if value.may_exceed_index_key() {
                        continue;
                    }
                    unique |= index.uniqueness == catalog::IndexUniqueness::Unique;
                    exact.insert(property);
                    candidates.push(ir::NodeAccessPlan::EqualityIndex {
                        index: index.clone(),
                        key,
                        value: ir::IndexValue::Literal(value),
                    });
                }
                // The residual predicate still checks every candidate, so each
                // membership source only needs to contain the matching nodes.
                // A small batched lookup is priced below one point read, so
                // cost alone would always pick a set. A unique lookup, or an
                // equality on the list's own property, is at least as selective
                // as the set; any other set must also estimate fewer rows than
                // the most selective equality.
                memberships.retain(|(property, _)| !unique && !exact.contains(property));
                let equality_rows = candidates
                    .iter()
                    .filter(|candidate| {
                        matches!(candidate, ir::NodeAccessPlan::EqualityIndex { .. })
                    })
                    .map(|candidate| access_rows(candidate, &config))
                    .min();
                for (property, list) in memberships {
                    let Some(key) = catalog::ScopedPropertyKey::try_new(label.clone(), property)
                    else {
                        continue;
                    };
                    let Some(index) = ctx.indexes.node_eq.get(&key) else {
                        continue;
                    };
                    let Some(values) = membership_values(&list, &ctx.params) else {
                        continue;
                    };
                    let Some(domain) = analysis::literal_equality_domain(values) else {
                        continue;
                    };
                    let values = match &domain {
                        analysis::EqualityIndexDomain::One(value) => std::slice::from_ref(value),
                        analysis::EqualityIndexDomain::Many(values) => values.as_ref(),
                        analysis::EqualityIndexDomain::Empty
                        | analysis::EqualityIndexDomain::RuntimeSet(_) => &[],
                    };
                    if values.iter().any(|value| {
                        matches!(value, ir::IndexValue::Literal(value) if value.may_exceed_index_key())
                    }) {
                        continue;
                    }
                    let equality = |value| ir::NodeAccessPlan::EqualityIndex {
                        index: index.clone(),
                        key: key.clone(),
                        value,
                    };
                    let candidate = match domain {
                        analysis::EqualityIndexDomain::Empty => ir::NodeAccessPlan::Empty,
                        analysis::EqualityIndexDomain::One(value) => equality(value),
                        analysis::EqualityIndexDomain::Many(values)
                            if values.as_ref().len() <= union_branches =>
                        {
                            ir::NodeAccessPlan::Union(
                                ir::AtLeast::try_from_vec(
                                    values
                                        .into_iter()
                                        .map(|value| {
                                            ir::NodeAccessSourcePlan::from_unfiltered(equality(
                                                value,
                                            ))
                                        })
                                        .collect(),
                                )
                                .expect("a multi-value domain has at least two values"),
                            )
                        }
                        analysis::EqualityIndexDomain::Many(_)
                        | analysis::EqualityIndexDomain::RuntimeSet(_) => continue,
                    };
                    if equality_rows.is_some_and(|rows| access_rows(&candidate, &config) >= rows) {
                        continue;
                    }
                    candidates.push(candidate);
                }
                // A range scan reads every value its bounds admit in the
                // bound's domain; the residual predicate applies strictness.
                for (property, range) in ranges {
                    let Some((key, index)) = [
                        helix_ast::index::RangeIndexDirection::Asc,
                        helix_ast::index::RangeIndexDirection::Desc,
                    ]
                    .into_iter()
                    .filter_map(|direction| {
                        catalog::ScopedPropertyDirectionKey::try_new(
                            label.clone(),
                            property.clone(),
                            direction,
                        )
                    })
                    .find_map(|key| {
                        let index = ctx.indexes.node_range.get(&key)?.clone();
                        Some((key, index))
                    }) else {
                        continue;
                    };
                    candidates.push(ir::NodeAccessPlan::RangeIndex {
                        index,
                        key,
                        range,
                        iteration: ir::RangeScanIteration::Forward,
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
        let lookup_predicate = index_predicate(query, operator_index, pattern, &ctx.params);
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
        // Probe extraction may eliminate candidates before final validation.
        // Only total constraints can cross that boundary: unbound parameters,
        // arithmetic, functions and dynamic property access retain their order.
        let incoming = query.contracts()[operator_index].input().slots();
        let totality = Totality {
            pattern,
            bindings: query.bindings(),
            params: Parameters::Bound(&ctx.params),
        };
        let constraints_total = pattern
            .nodes
            .iter()
            .flat_map(|node| &node.properties)
            .chain(pattern.relationships.iter().flat_map(|rel| &rel.properties))
            .all(|(_, expression)| match expression {
                Expression::Slot(slot) => incoming.contains(slot),
                expression => totality.operand(expression),
            });
        let predicate_total = lookup_predicate
            .as_ref()
            .is_none_or(|expression| totality.predicate(expression));
        let mut lookups = Vec::new();
        if constraints_total && predicate_total {
            // Group repeated bindings once; do not rescan the whole pattern
            // for every node in a wide disconnected conjunction.
            let mut lookup_nodes = BTreeMap::<_, Vec<_>>::new();
            for node in &pattern.nodes {
                if !incoming.contains(&node.slot) {
                    lookup_nodes.entry(node.slot).or_default().push(node);
                }
            }
            for (slot, nodes) in lookup_nodes {
                let Some(label) = nodes.iter().find_map(|node| node.label.as_ref()) else {
                    continue;
                };
                let mut equalities = nodes
                    .iter()
                    .flat_map(|node| node.properties.iter().cloned())
                    .collect();
                if let Some(predicate) = &lookup_predicate {
                    collect_equalities(predicate, slot, &mut equalities);
                }
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
                    lookups.push(PatternLookup {
                        slot,
                        probe,
                        index: index.clone(),
                        key,
                        estimated_rows,
                    });
                    break;
                }
            }
        }
        order = order.with_lookups(lookups)?;
        // Equality is the complete predicate here: pushing a key evaluation
        // through an earlier short-circuiting expression could expose an error.
        if pattern.relationships.is_empty()
            && pattern.paths.is_empty()
            && pattern.nodes.len() == 2
            && constraints_total
            && let Some(Expression::Binary(Binary::Equal, left, right)) = predicate.as_deref()
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
                access: Arc::new(access),
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

/// The predicate an index source of the MATCH at `index` may use: its own
/// WHERE conjoined with the WHERE of directly following filters and
/// pass-through WITH clauses, rewritten over the MATCH's slots.
///
/// Those later predicates drop every row they reject, so a source may skip
/// such rows as well, provided nothing in between could fail on them: each
/// projected item must be a total operand, and a later WHERE that can fail,
/// ordering, SKIP, LIMIT, DISTINCT and aggregation stop the walk. An
/// OPTIONAL MATCH keeps rows its predicate rejects, so only its own
/// predicate applies. The caller still requires the result, including the
/// MATCH's own WHERE, to be total before extracting any lookup.
fn index_predicate(
    query: &Query,
    index: usize,
    pattern: &Pattern,
    params: &context::ParamBindings,
) -> Option<Expression> {
    let Operator::Match {
        optional,
        predicate,
        ..
    } = &query.operators()[index]
    else {
        unreachable!("index predicates belong to a MATCH");
    };
    let totality = Totality {
        pattern,
        bindings: query.bindings(),
        params: Parameters::Bound(params),
    };
    let mut conjuncts = predicate
        .iter()
        .map(|predicate| predicate.expression().clone())
        .collect::<Vec<_>>();
    // Each visible slot bound to its value over the MATCH output.
    let mut bindings = query.contracts()[index]
        .output()
        .slots()
        .into_iter()
        .map(|slot| (slot, Expression::Slot(slot)))
        .collect::<BTreeMap<_, _>>();
    let substitute = |expression: &Expression, bindings: &BTreeMap<Slot, Expression>| {
        expression
            .rewrite(&mut |value| match value {
                Expression::Slot(slot) => bindings
                    .get(slot)
                    .cloned()
                    .map(Some)
                    .ok_or_else(|| planning_error("unbound slot")),
                Expression::HasLabel(slot, label) => match bindings.get(slot) {
                    Some(Expression::Slot(source)) => {
                        Ok(Some(Expression::HasLabel(*source, label.clone())))
                    }
                    _ => Err(planning_error("label test of a derived value")),
                },
                _ => Ok(None),
            })
            .ok()
    };
    for operator in query.operators()[index + 1..]
        .iter()
        .take_while(|_| !optional)
    {
        let next = match operator {
            Operator::Filter(predicate) => substitute(predicate.expression(), &bindings),
            Operator::Project {
                items,
                distinct: false,
                ordering,
                predicate,
                skip: None,
                limit: None,
            } if ordering.is_empty() => {
                let Some(projected) = items
                    .iter()
                    .map(|item| {
                        let value = substitute(&item.expression, &bindings)?;
                        totality.operand(&value).then_some((item.slot, value))
                    })
                    .collect::<Option<BTreeMap<_, _>>>()
                else {
                    break;
                };
                bindings = projected;
                let Some(predicate) = predicate else {
                    continue;
                };
                substitute(predicate.expression(), &bindings)
            }
            _ => break,
        };
        // A later WHERE that can fail may raise an error on a row the source
        // would skip, so neither it nor anything after it can select an index.
        let Some(next) = next.filter(|next| totality.predicate(next)) else {
            break;
        };
        conjuncts.push(next);
    }
    conjuncts
        .into_iter()
        .reduce(|left, right| Expression::Binary(Binary::And, Box::new(left), Box::new(right)))
}

/// What a totality proof knows about query parameters.
#[derive(Clone, Copy)]
pub(super) enum Parameters<'a> {
    /// Planning-time values; an unbound parameter could fail on any row.
    Bound(&'a context::ParamBindings),
    /// Execution rejects a missing parameter before reading any row, but the
    /// values, and so an `IN $list` operand's type, are unknown.
    Validated,
}

/// What a totality proof over one MATCH knows: its pattern, the query's
/// binding types and its parameters.
#[derive(Clone, Copy)]
pub(super) struct Totality<'a> {
    pub(super) pattern: &'a Pattern,
    pub(super) bindings: &'a [super::Binding],
    pub(super) params: Parameters<'a>,
}

impl Totality<'_> {
    /// Whether `slot` holds a node, relationship or map, or null: property
    /// access on those cannot fail.
    fn has_properties(self, slot: Slot) -> bool {
        self.pattern.nodes.iter().any(|node| node.slot == slot)
            || self
                .pattern
                .relationships
                .iter()
                .any(|rel| rel.slot == slot)
            || matches!(
                self.bindings[slot.0 as usize].value_type,
                super::ValueType::Node
                    | super::ValueType::Relationship
                    | super::ValueType::Map
                    | super::ValueType::Null
            )
    }

    /// An operand that evaluates without failing: a literal, a slot, a bound
    /// parameter, or a property of a slot that holds properties.
    pub(super) fn operand(self, operand: &Expression) -> bool {
        match operand {
            Expression::Literal(_) | Expression::Slot(_) => true,
            Expression::Parameter(name) => match self.params {
                Parameters::Bound(params) => {
                    let name = ir::NonEmptyString::new(name.clone()).expect("validated parameter");
                    params.values.contains_key(&name) || params.query_values.contains_key(&name)
                }
                Parameters::Validated => true,
            },
            Expression::Property(value, _) => {
                matches!(value.as_ref(), Expression::Slot(slot) if self.has_properties(*slot))
            }
            _ => false,
        }
    }

    // Extracting an index equality from AND may otherwise suppress an
    // observable error in another conjunct. In particular NULL AND an error
    // still evaluates that error; treating NULL as an early false result would
    // be incorrect. A total predicate is a boolean combination of comparisons,
    // string predicates, label and null tests, and IN over total operands:
    // these return null for mismatched types instead of failing. Keep
    // potentially failing arithmetic, functions, and dynamic access above a scan.
    pub(super) fn predicate(self, expression: &Expression) -> bool {
        match expression {
            Expression::Literal(Value::Boolean(_) | Value::Null) => true,
            Expression::Binary(Binary::And | Binary::Or | Binary::Xor, left, right) => {
                self.predicate(left) && self.predicate(right)
            }
            Expression::Unary(Unary::Not, negated) => self.predicate(negated),
            Expression::Unary(Unary::IsNull | Unary::IsNotNull, value) => self.operand(value),
            Expression::HasLabel(slot, _) => {
                self.pattern.nodes.iter().any(|node| node.slot == *slot)
                    || matches!(
                        self.bindings[slot.0 as usize].value_type,
                        super::ValueType::Node | super::ValueType::Null
                    )
            }
            // IN fails only for a right operand that is neither a list nor
            // null; comparing a value with list members cannot fail.
            Expression::Binary(Binary::In, value, list) => {
                self.operand(value)
                    && match list.as_ref() {
                        Expression::Literal(Value::List(_) | Value::Null) => true,
                        Expression::List(items) => items.iter().all(|item| {
                            matches!(item, Expression::Literal(_) | Expression::Parameter(_))
                                && self.operand(item)
                        }),
                        Expression::Parameter(name) => {
                            matches!(self.params, Parameters::Bound(params)
                            if matches!(
                                params.values.get(
                                    &ir::NonEmptyString::new(name.clone())
                                        .expect("validated parameter")
                                ),
                                Some(
                                    helix_ast::value::PropertyValue::Array(_)
                                        | helix_ast::value::PropertyValue::Null
                                )
                            ))
                        }
                        _ => false,
                    }
            }
            Expression::Binary(
                Binary::Equal
                | Binary::NotEqual
                | Binary::Less
                | Binary::LessEqual
                | Binary::Greater
                | Binary::GreaterEqual
                | Binary::StartsWith
                | Binary::EndsWith
                | Binary::Contains,
                left,
                right,
            ) => self.operand(left) && self.operand(right),
            _ => false,
        }
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

/// Range bounds on properties of `slot` from the conjuncts of a total
/// predicate, intersected per property. A parameter resolves to its bound
/// value at planning time; unorderable, NaN and oversized bounds are skipped,
/// so a scan never fails where the comparison would yield null.
fn collect_ranges(
    expression: &Expression,
    slot: Slot,
    params: &context::ParamBindings,
    out: &mut Vec<(String, ir::IndexRange)>,
) {
    let (op, a, b) = match expression {
        Expression::Binary(Binary::And, a, b) => {
            collect_ranges(a, slot, params, out);
            collect_ranges(b, slot, params, out);
            return;
        }
        Expression::Binary(
            op @ (Binary::Less | Binary::LessEqual | Binary::Greater | Binary::GreaterEqual),
            a,
            b,
        ) => (*op, a, b),
        _ => return,
    };
    let greater = matches!(op, Binary::Greater | Binary::GreaterEqual);
    // `n.p > v` bounds p below; `v > n.p` bounds it above.
    for (property, value, lower) in [(a, b, greater), (b, a, !greater)] {
        let Expression::Property(target, key) = property.as_ref() else {
            continue;
        };
        if target.as_ref() != &Expression::Slot(slot) {
            continue;
        }
        let value = match value.as_ref() {
            Expression::Literal(value) => literal(value.clone()),
            Expression::Parameter(name) => params
                .values
                .get(&ir::NonEmptyString::new(name.clone()).expect("validated parameter"))
                .cloned(),
            _ => None,
        };
        // Escaping can double a string's encoded key.
        let Some(value) = value
            .filter(|value| {
                !matches!(value, helix_ast::value::PropertyValue::String(text)
                    if text.len() > ir::MAX_INDEXED_EQUALITY_BYTES / 2)
            })
            .and_then(ir::RangeIndexValue::literal)
        else {
            continue;
        };
        let bound = match op {
            Binary::LessEqual | Binary::GreaterEqual => ir::IndexBound::Inclusive(value),
            _ => ir::IndexBound::Exclusive(value),
        };
        let range = if lower {
            ir::IndexRange::Lower { lower: bound }
        } else {
            ir::IndexRange::Upper { upper: bound }
        };
        match out.iter_mut().find(|(existing, _)| existing == key) {
            // Bounds that cannot be combined keep the earlier, wider range.
            Some((_, existing)) => {
                if let Some(both) = existing.intersect(&range) {
                    *existing = both;
                }
            }
            None => out.push((key.clone(), range)),
        }
    }
}

fn collect_memberships(expression: &Expression, slot: Slot, out: &mut Vec<(String, Expression)>) {
    match expression {
        Expression::Binary(Binary::And, a, b) => {
            collect_memberships(a, slot, out);
            collect_memberships(b, slot, out);
        }
        Expression::Binary(Binary::In, value, list) => {
            let Expression::Property(value, key) = value.as_ref() else {
                return;
            };
            if value.as_ref() == &Expression::Slot(slot) {
                out.push((key.clone(), list.as_ref().clone()));
            }
        }
        // `n.p = a OR n.p = b` has the three-valued result of `n.p IN [a, b]`.
        Expression::Binary(Binary::Or, ..) => {
            let mut key = None;
            let mut members = Vec::new();
            if equality_disjuncts(expression, slot, &mut key, &mut members)
                && let Some(key) = key
            {
                out.push((key, Expression::List(members)));
            }
        }
        _ => {}
    }
}

/// Whether `expression` is a disjunction of equalities between one property of
/// `slot` and literals or parameters; collects that property and the members.
fn equality_disjuncts(
    expression: &Expression,
    slot: Slot,
    key: &mut Option<String>,
    members: &mut Vec<Expression>,
) -> bool {
    match expression {
        Expression::Binary(Binary::Or, a, b) => {
            equality_disjuncts(a, slot, key, members) && equality_disjuncts(b, slot, key, members)
        }
        Expression::Binary(Binary::Equal, a, b) => {
            [(a, b), (b, a)].into_iter().any(|(property, member)| {
                let Expression::Property(value, name) = property.as_ref() else {
                    return false;
                };
                if value.as_ref() != &Expression::Slot(slot)
                    || !matches!(
                        member.as_ref(),
                        Expression::Literal(_) | Expression::Parameter(_)
                    )
                    || key.as_ref().is_some_and(|key| key != name)
                {
                    return false;
                }
                *key = Some(name.clone());
                members.push(member.as_ref().clone());
                true
            })
        }
        _ => false,
    }
}

/// Non-null members of a total IN list operand. Cypher null never equals a
/// member, and a member that is not a scalar literal prevents index access.
fn membership_values(
    list: &Expression,
    params: &context::ParamBindings,
) -> Option<Vec<helix_ast::value::PropertyValue>> {
    use helix_ast::value::PropertyValue as P;
    let parameter = |name: &String| {
        params
            .values
            .get(&ir::NonEmptyString::new(name.clone()).expect("validated parameter"))
    };
    let members = match list {
        Expression::Literal(Value::List(values)) => values
            .iter()
            .cloned()
            .map(literal)
            .collect::<Option<Vec<_>>>()?,
        Expression::List(items) => items
            .iter()
            .map(|item| match item {
                Expression::Literal(value) => literal(value.clone()),
                Expression::Parameter(name) => parameter(name).cloned(),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?,
        Expression::Parameter(name) => match parameter(name)? {
            P::Array(values) => values.clone(),
            P::Null => Vec::new(),
            _ => return None,
        },
        Expression::Literal(Value::Null) => Vec::new(),
        _ => return None,
    };
    Some(
        members
            .into_iter()
            .filter(|value| *value != P::Null)
            .collect(),
    )
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
    access_rows(node.source().as_ref(), config)
}

fn access_rows(access: &ir::NodeAccessPlan, config: &optimizer::OptimizerConfig) -> u64 {
    match access {
        ir::NodeAccessPlan::Empty => 0,
        ir::NodeAccessPlan::Union(children) => children
            .iter()
            .map(|child| access_rows(child.as_ref(), config))
            .fold(0, u64::saturating_add),
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
        ir::NodeAccessPlan::RangeIndex { key, .. } => config
            .stats
            .node_range_cardinality
            .get(key)
            .copied()
            .unwrap_or(config.storage.default_range_index_rows.as_rows()),
        _ => unreachable!("candidate construction admits all/label/equality/range/set access only"),
    }
}
