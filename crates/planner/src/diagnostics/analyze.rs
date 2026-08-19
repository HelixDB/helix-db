use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::{
    DeepTraversalInsight, MissingIndexInsight, PlannerDiagnostics, PlannerInsight,
    PlannerStatistics, PredicatePropertySet, SecondaryIndexKind, UnboundedScanInsight,
    MAX_PLANNER_INSIGHTS,
};
use crate::{analysis, catalog, context, exec, ir, rules};

const DEEP_TRAVERSAL_EXPANSION_THRESHOLD: usize = 3;

pub(super) struct Analyzer<'a> {
    ctx: &'a context::PlannerContext,
    statistics: PlannerStatistics,
    missing_indexes: BTreeMap<MissingIndexKey, usize>,
    unbounded_scans: BTreeMap<UnboundedScanKey, usize>,
    maximum_traversal_depth: usize,
}

impl<'a> Analyzer<'a> {
    pub(super) fn new(ctx: &'a context::PlannerContext) -> Self {
        Self {
            ctx,
            statistics: PlannerStatistics::default(),
            missing_indexes: BTreeMap::new(),
            unbounded_scans: BTreeMap::new(),
            maximum_traversal_depth: 0,
        }
    }

    pub(super) fn analyze(mut self, plan: &exec::ExecutablePlan) -> PlannerDiagnostics {
        self.copy_planner_work(plan.metrics());
        self.analyze_steps(plan.steps(), 0, 0);
        let insights = self.finish_insights();
        PlannerDiagnostics {
            statistics: self.statistics,
            insights,
        }
    }

    fn copy_planner_work(&mut self, metrics: &exec::PlannerMetrics) {
        self.statistics.memo_groups = metrics.memo_groups;
        self.statistics.memo_expressions = metrics.memo_exprs;
        self.statistics.rules_fired = metrics.rule_fires;
        self.statistics.rejected_alternatives = metrics.rejected_alternatives;
        self.statistics.alternatives_considered = metrics.alternatives_considered;
        self.statistics.optimization_micros = metrics.optimization_micros;
        self.statistics.guardrail_hit = metrics.guardrail_hit;
    }

    fn analyze_steps(
        &mut self,
        steps: &[exec::ExecStep],
        base_operator_depth: usize,
        base_traversal_depth: usize,
    ) {
        let steps_by_id = steps
            .iter()
            .map(|step| (step.id, step))
            .collect::<HashMap<_, _>>();
        let mut operator_depths = HashMap::new();
        let mut traversal_depths = HashMap::new();
        let mut lineage_memo = HashMap::new();
        let mut unbounded_sites = steps
            .iter()
            .filter_map(|step| {
                unbounded_scan_scope(&step.op).map(|scope| {
                    (
                        step.id,
                        UnboundedScanSite {
                            scope,
                            predicate_properties: BTreeSet::new(),
                        },
                    )
                })
            })
            .collect::<HashMap<_, _>>();

        for step in steps {
            let parent_operator_depth = step
                .dependencies
                .iter()
                .filter_map(|dependency| operator_depths.get(dependency))
                .copied()
                .max()
                .unwrap_or(base_operator_depth);
            let operator_depth = parent_operator_depth.saturating_add(1);
            operator_depths.insert(step.id, operator_depth);
            self.statistics.maximum_operator_depth =
                self.statistics.maximum_operator_depth.max(operator_depth);

            let parent_traversal_depth = step
                .dependencies
                .iter()
                .filter_map(|dependency| traversal_depths.get(dependency))
                .copied()
                .max()
                .unwrap_or(base_traversal_depth);
            let traversal_depth = traversal_depth_for_op(&step.op, parent_traversal_depth);
            traversal_depths.insert(step.id, traversal_depth);
            self.maximum_traversal_depth = self.maximum_traversal_depth.max(traversal_depth);

            self.statistics.total_operators = self.statistics.total_operators.saturating_add(1);
            self.analyze_op(
                step,
                &steps_by_id,
                &mut lineage_memo,
                &mut unbounded_sites,
                operator_depth,
                parent_traversal_depth,
            );
        }

        unbounded_sites.into_values().for_each(|site| {
            let key = UnboundedScanKey {
                element: site.scope.element.into(),
                label: site.scope.label,
                predicate_properties: PredicatePropertySet::new(site.predicate_properties),
            };
            self.unbounded_scans
                .entry(key)
                .and_modify(|occurrences| *occurrences = occurrences.saturating_add(1))
                .or_insert(1);
        });
    }

    fn analyze_op(
        &mut self,
        step: &exec::ExecStep,
        steps_by_id: &HashMap<exec::ExecStepId, &exec::ExecStep>,
        lineage_memo: &mut HashMap<exec::ExecStepId, Option<AccessLineage>>,
        unbounded_sites: &mut HashMap<exec::ExecStepId, UnboundedScanSite>,
        operator_depth: usize,
        parent_traversal_depth: usize,
    ) {
        match &step.op {
            exec::ExecOp::Access { plan } => self.analyze_access(plan),
            exec::ExecOp::Count { .. } => {}
            exec::ExecOp::KvRead(read) => self.analyze_kv_read(read),
            exec::ExecOp::Expand { .. } => {
                self.statistics.expansions = self.statistics.expansions.saturating_add(1);
            }
            exec::ExecOp::Filter { predicate } => {
                self.statistics.residual_filters =
                    self.statistics.residual_filters.saturating_add(1);
                self.analyze_filter(step, predicate, steps_by_id, lineage_memo, unbounded_sites);
            }
            exec::ExecOp::Limit { .. } => {
                self.statistics.limits = self.statistics.limits.saturating_add(1);
            }
            exec::ExecOp::Skip { .. } => {
                self.statistics.skips = self.statistics.skips.saturating_add(1);
            }
            exec::ExecOp::Range { .. } => {
                self.statistics.ranges = self.statistics.ranges.saturating_add(1);
            }
            exec::ExecOp::Order { .. } => {
                self.statistics.explicit_sorts = self.statistics.explicit_sorts.saturating_add(1);
            }
            exec::ExecOp::Merge {
                mode: exec::ExecMergeMode::Union,
            } => self.statistics.unions = self.statistics.unions.saturating_add(1),
            exec::ExecOp::Merge {
                mode: exec::ExecMergeMode::Intersect,
            } => {
                self.statistics.intersections = self.statistics.intersections.saturating_add(1);
            }
            exec::ExecOp::Branch { plan } => {
                self.statistics.branches = self.statistics.branches.saturating_add(1);
                self.analyze_branch(plan, operator_depth, parent_traversal_depth);
            }
            exec::ExecOp::Repeat { plan } => {
                self.statistics.repeats = self.statistics.repeats.saturating_add(1);
                self.analyze_steps(plan.body.steps(), operator_depth, parent_traversal_depth);
            }
            exec::ExecOp::ForEach { body, .. } => {
                self.statistics.for_each = self.statistics.for_each.saturating_add(1);
                self.analyze_steps(body.steps(), operator_depth, 0);
            }
            exec::ExecOp::Distinct
            | exec::ExecOp::VectorSearch { .. }
            | exec::ExecOp::TextSearch { .. }
            | exec::ExecOp::Project { .. }
            | exec::ExecOp::Aggregate { .. }
            | exec::ExecOp::Variable { .. }
            | exec::ExecOp::ShortestPath { .. }
            | exec::ExecOp::Mutation { .. }
            | exec::ExecOp::IndexDdl { .. }
            | exec::ExecOp::Merge {
                mode: exec::ExecMergeMode::Concat,
            }
            | exec::ExecOp::Reserved { .. }
            | exec::ExecOp::Barrier { .. }
            | exec::ExecOp::Noop => {}
        }
    }

    fn analyze_branch(
        &mut self,
        plan: &exec::ExecBranchPlan,
        operator_depth: usize,
        base_traversal_depth: usize,
    ) {
        match plan {
            exec::ExecBranchPlan::Union(plans) => plans.iter().for_each(|plan| {
                self.analyze_steps(plan.steps(), operator_depth, base_traversal_depth)
            }),
            exec::ExecBranchPlan::Choose { then_plan, .. } => {
                self.analyze_steps(then_plan.steps(), operator_depth, base_traversal_depth);
            }
            exec::ExecBranchPlan::ChooseElse {
                then_plan,
                else_plan,
                ..
            } => {
                self.analyze_steps(then_plan.steps(), operator_depth, base_traversal_depth);
                self.analyze_steps(else_plan.steps(), operator_depth, base_traversal_depth);
            }
            exec::ExecBranchPlan::Coalesce(plans) => plans.iter().for_each(|plan| {
                self.analyze_steps(plan.steps(), operator_depth, base_traversal_depth)
            }),
            exec::ExecBranchPlan::Optional(plan) => {
                self.analyze_steps(plan.steps(), operator_depth, base_traversal_depth);
            }
        }
    }

    fn analyze_access(&mut self, access: &exec::ExecAccessPlan) {
        match access {
            exec::ExecAccessPlan::Limited(limited) => {
                self.analyze_access_leaf(limited.source(), true);
            }
            exec::ExecAccessPlan::Node(_) | exec::ExecAccessPlan::Edge(_) => {
                self.analyze_access_leaf(access, false);
            }
        }
    }

    fn analyze_access_leaf(&mut self, access: &exec::ExecAccessPlan, bounded: bool) {
        match access {
            exec::ExecAccessPlan::Node(plan) => {
                if bounded {
                    self.statistics.node_accesses.bounded_accesses = self
                        .statistics
                        .node_accesses
                        .bounded_accesses
                        .saturating_add(1);
                }
                match plan {
                    exec::ExecNodeAccessPlan::AllScan => {
                        self.statistics.node_accesses.all_scans =
                            self.statistics.node_accesses.all_scans.saturating_add(1);
                    }
                    exec::ExecNodeAccessPlan::LabelScan { .. } => {
                        self.statistics.node_accesses.label_scans =
                            self.statistics.node_accesses.label_scans.saturating_add(1);
                    }
                    exec::ExecNodeAccessPlan::Bitmap { bitmap } => {
                        self.statistics.node_accesses.equality_index_lookups = self
                            .statistics
                            .node_accesses
                            .equality_index_lookups
                            .saturating_add(node_bitmap_lookup_count(bitmap));
                    }
                    exec::ExecNodeAccessPlan::Unique { .. }
                    | exec::ExecNodeAccessPlan::AuthoritativeScan { .. }
                    | exec::ExecNodeAccessPlan::DynamicEquality { .. } => {
                        self.statistics.node_accesses.equality_index_lookups = self
                            .statistics
                            .node_accesses
                            .equality_index_lookups
                            .saturating_add(1);
                    }
                    exec::ExecNodeAccessPlan::RangeIndex { .. } => {
                        self.statistics.node_accesses.range_index_scans = self
                            .statistics
                            .node_accesses
                            .range_index_scans
                            .saturating_add(1);
                    }
                    exec::ExecNodeAccessPlan::SecondarySet { set } => {
                        self.analyze_node_secondary_set(set);
                    }
                    exec::ExecNodeAccessPlan::VectorSearch { .. } => {
                        self.statistics.node_accesses.vector_searches = self
                            .statistics
                            .node_accesses
                            .vector_searches
                            .saturating_add(1);
                    }
                    exec::ExecNodeAccessPlan::TextSearch { .. } => {
                        self.statistics.node_accesses.text_searches = self
                            .statistics
                            .node_accesses
                            .text_searches
                            .saturating_add(1);
                    }
                    exec::ExecNodeAccessPlan::Empty
                    | exec::ExecNodeAccessPlan::FromParam { .. }
                    | exec::ExecNodeAccessPlan::FromVar { .. } => {}
                }
            }
            exec::ExecAccessPlan::Edge(plan) => {
                if bounded {
                    self.statistics.edge_accesses.bounded_accesses = self
                        .statistics
                        .edge_accesses
                        .bounded_accesses
                        .saturating_add(1);
                }
                match plan {
                    exec::ExecEdgeAccessPlan::AllScan => {
                        self.statistics.edge_accesses.all_scans =
                            self.statistics.edge_accesses.all_scans.saturating_add(1);
                    }
                    exec::ExecEdgeAccessPlan::LabelScan { .. } => {
                        self.statistics.edge_accesses.label_scans =
                            self.statistics.edge_accesses.label_scans.saturating_add(1);
                    }
                    exec::ExecEdgeAccessPlan::Bitmap { bitmap } => {
                        self.statistics.edge_accesses.equality_index_lookups = self
                            .statistics
                            .edge_accesses
                            .equality_index_lookups
                            .saturating_add(edge_bitmap_lookup_count(bitmap));
                    }
                    exec::ExecEdgeAccessPlan::AuthoritativeScan { .. }
                    | exec::ExecEdgeAccessPlan::DynamicEquality { .. } => {
                        self.statistics.edge_accesses.equality_index_lookups = self
                            .statistics
                            .edge_accesses
                            .equality_index_lookups
                            .saturating_add(1);
                    }
                    exec::ExecEdgeAccessPlan::RangeIndex { .. } => {
                        self.statistics.edge_accesses.range_index_scans = self
                            .statistics
                            .edge_accesses
                            .range_index_scans
                            .saturating_add(1);
                    }
                    exec::ExecEdgeAccessPlan::SecondarySet { set } => {
                        self.analyze_edge_secondary_set(set);
                    }
                    exec::ExecEdgeAccessPlan::VectorSearch { .. } => {
                        self.statistics.edge_accesses.vector_searches = self
                            .statistics
                            .edge_accesses
                            .vector_searches
                            .saturating_add(1);
                    }
                    exec::ExecEdgeAccessPlan::TextSearch { .. } => {
                        self.statistics.edge_accesses.text_searches = self
                            .statistics
                            .edge_accesses
                            .text_searches
                            .saturating_add(1);
                    }
                    exec::ExecEdgeAccessPlan::Empty
                    | exec::ExecEdgeAccessPlan::FromParam { .. }
                    | exec::ExecEdgeAccessPlan::FromVar { .. } => {}
                }
            }
            exec::ExecAccessPlan::Limited(limited) => {
                self.analyze_access_leaf(limited.source(), true);
            }
        }
    }

    fn analyze_node_secondary_set(&mut self, set: &exec::ExecNodeSecondarySetPlan) {
        match set {
            exec::ExecNodeSecondarySetPlan::Empty => {}
            exec::ExecNodeSecondarySetPlan::Bitmap(bitmap) => {
                self.statistics.node_accesses.equality_index_lookups = self
                    .statistics
                    .node_accesses
                    .equality_index_lookups
                    .saturating_add(node_bitmap_lookup_count(bitmap));
            }
            exec::ExecNodeSecondarySetPlan::Unique { .. }
            | exec::ExecNodeSecondarySetPlan::AuthoritativeScan(_)
            | exec::ExecNodeSecondarySetPlan::DynamicEquality { .. } => {
                self.statistics.node_accesses.equality_index_lookups = self
                    .statistics
                    .node_accesses
                    .equality_index_lookups
                    .saturating_add(1);
            }
            exec::ExecNodeSecondarySetPlan::Range(_) => {
                self.statistics.node_accesses.range_index_scans = self
                    .statistics
                    .node_accesses
                    .range_index_scans
                    .saturating_add(1);
            }
            exec::ExecNodeSecondarySetPlan::Intersect { driver, rest } => {
                self.statistics.intersections = self.statistics.intersections.saturating_add(1);
                core::iter::once(driver.as_ref())
                    .chain(rest.iter())
                    .for_each(|child| self.analyze_node_secondary_set(child));
            }
            exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
                self.statistics.unions = self.statistics.unions.saturating_add(1);
                core::iter::once(driver.as_ref())
                    .chain(rest.iter())
                    .for_each(|child| self.analyze_node_secondary_set(child));
            }
            exec::ExecNodeSecondarySetPlan::OrderedIntersect { filters, .. } => {
                self.statistics.intersections = self.statistics.intersections.saturating_add(1);
                self.statistics.node_accesses.range_index_scans = self
                    .statistics
                    .node_accesses
                    .range_index_scans
                    .saturating_add(1);
                filters
                    .iter()
                    .for_each(|filter| self.analyze_node_secondary_set(filter));
            }
        }
    }

    fn analyze_edge_secondary_set(&mut self, set: &exec::ExecEdgeSecondarySetPlan) {
        match set {
            exec::ExecEdgeSecondarySetPlan::Empty => {}
            exec::ExecEdgeSecondarySetPlan::Bitmap(bitmap) => {
                self.statistics.edge_accesses.equality_index_lookups = self
                    .statistics
                    .edge_accesses
                    .equality_index_lookups
                    .saturating_add(edge_bitmap_lookup_count(bitmap));
            }
            exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(_)
            | exec::ExecEdgeSecondarySetPlan::DynamicEquality { .. } => {
                self.statistics.edge_accesses.equality_index_lookups = self
                    .statistics
                    .edge_accesses
                    .equality_index_lookups
                    .saturating_add(1);
            }
            exec::ExecEdgeSecondarySetPlan::Range(_) => {
                self.statistics.edge_accesses.range_index_scans = self
                    .statistics
                    .edge_accesses
                    .range_index_scans
                    .saturating_add(1);
            }
            exec::ExecEdgeSecondarySetPlan::Intersect { driver, rest } => {
                self.statistics.intersections = self.statistics.intersections.saturating_add(1);
                core::iter::once(driver.as_ref())
                    .chain(rest.iter())
                    .for_each(|child| self.analyze_edge_secondary_set(child));
            }
            exec::ExecEdgeSecondarySetPlan::Union { driver, rest } => {
                self.statistics.unions = self.statistics.unions.saturating_add(1);
                core::iter::once(driver.as_ref())
                    .chain(rest.iter())
                    .for_each(|child| self.analyze_edge_secondary_set(child));
            }
            exec::ExecEdgeSecondarySetPlan::OrderedIntersect { filters, .. } => {
                self.statistics.intersections = self.statistics.intersections.saturating_add(1);
                self.statistics.edge_accesses.range_index_scans = self
                    .statistics
                    .edge_accesses
                    .range_index_scans
                    .saturating_add(1);
                filters
                    .iter()
                    .for_each(|filter| self.analyze_edge_secondary_set(filter));
            }
        }
    }

    fn analyze_kv_read(&mut self, read: &exec::KvReadPlan) {
        match read {
            exec::KvReadPlan::Get { key } => {
                let stats = self.access_statistics_mut(key.keyspace());
                stats.point_lookups = stats.point_lookups.saturating_add(1);
            }
            exec::KvReadPlan::MultiGet(plan) => {
                let stats = self.access_statistics_mut(plan.keyspace());
                stats.point_lookups = stats.point_lookups.saturating_add(1);
            }
            exec::KvReadPlan::RangeScan {
                keyspace,
                start: exec::KvKeyBound::Unbounded,
                end: exec::KvKeyBound::Unbounded,
                limit,
            } => {
                let stats = self.access_statistics_mut(*keyspace);
                stats.all_scans = stats.all_scans.saturating_add(1);
                if limit.is_some() {
                    stats.bounded_accesses = stats.bounded_accesses.saturating_add(1);
                }
            }
            exec::KvReadPlan::RangeScan { .. } | exec::KvReadPlan::PrefixScan { .. } => {}
        }
    }

    fn access_statistics_mut(
        &mut self,
        keyspace: exec::ElementKeyspace,
    ) -> &mut super::AccessStatistics {
        match keyspace {
            exec::ElementKeyspace::NodeProperty => &mut self.statistics.node_accesses,
            exec::ElementKeyspace::EdgeEndpoints => &mut self.statistics.edge_accesses,
        }
    }

    fn analyze_filter(
        &mut self,
        step: &exec::ExecStep,
        predicate: &ir::PredicatePlan,
        steps_by_id: &HashMap<exec::ExecStepId, &exec::ExecStep>,
        lineage_memo: &mut HashMap<exec::ExecStepId, Option<AccessLineage>>,
        unbounded_sites: &mut HashMap<exec::ExecStepId, UnboundedScanSite>,
    ) {
        let [dependency] = step.dependencies.as_slice() else {
            return;
        };
        let mut visiting = HashSet::new();
        let Some(lineage) = resolve_lineage(
            *dependency,
            steps_by_id,
            lineage_memo,
            &mut visiting,
            unbounded_sites,
        ) else {
            return;
        };
        let mut predicate_properties = BTreeSet::new();
        collect_predicate_properties(predicate.predicate(), &mut predicate_properties);
        lineage.unbounded_sources.iter().for_each(|source| {
            unbounded_sites
                .get_mut(source)
                .expect("resolved unbounded source is pre-registered")
                .predicate_properties
                .extend(predicate_properties.iter().cloned());
        });

        let AccessLineageScope::Uniform(scope) = &lineage.scope else {
            return;
        };
        let Some(label) = effective_filter_label(scope, predicate.predicate()) else {
            return;
        };
        rules::missing_index_candidates(scope.element, &label, predicate.predicate(), self.ctx)
            .into_iter()
            .for_each(|candidate| {
                let key = MissingIndexKey {
                    element: scope.element.into(),
                    label: label.clone(),
                    property: candidate.property,
                    kind: candidate.kind.into(),
                };
                self.missing_indexes
                    .entry(key)
                    .and_modify(|occurrences| *occurrences = occurrences.saturating_add(1))
                    .or_insert(1);
            });
    }

    fn finish_insights(&mut self) -> Vec<PlannerInsight> {
        let mut insights = std::mem::take(&mut self.unbounded_scans)
            .into_iter()
            .map(|(key, occurrences)| {
                PlannerInsight::UnboundedScan(UnboundedScanInsight {
                    element: key.element.into(),
                    label: key.label,
                    predicate_properties: key.predicate_properties,
                    occurrences,
                })
            })
            .chain(std::mem::take(&mut self.missing_indexes).into_iter().map(
                |(key, occurrences)| {
                    PlannerInsight::MissingIndex(MissingIndexInsight {
                        element: key.element.into(),
                        label: key.label,
                        property: key.property,
                        index_kind: key.kind.into(),
                        occurrences,
                    })
                },
            ))
            .collect::<Vec<_>>();
        if self.statistics.expansions >= DEEP_TRAVERSAL_EXPANSION_THRESHOLD
            || self.statistics.repeats > 0
        {
            insights.push(PlannerInsight::DeepTraversal(DeepTraversalInsight {
                expansion_count: self.statistics.expansions,
                repeat_count: self.statistics.repeats,
                maximum_depth: self.maximum_traversal_depth,
            }));
        }
        insights.truncate(MAX_PLANNER_INSIGHTS);
        insights
    }
}

fn traversal_depth_for_op(op: &exec::ExecOp, parent_depth: usize) -> usize {
    match op {
        exec::ExecOp::Access { .. } | exec::ExecOp::KvRead(_) => 0,
        exec::ExecOp::Expand { .. } => parent_depth.saturating_add(1),
        exec::ExecOp::Branch { plan } => {
            parent_depth.saturating_add(branch_maximum_traversal_increment(plan))
        }
        exec::ExecOp::Repeat { plan } => parent_depth.saturating_add(
            effective_repeat_iterations(plan)
                .saturating_mul(subplan_maximum_traversal_increment(plan.body.steps())),
        ),
        exec::ExecOp::Filter { .. }
        | exec::ExecOp::Count { .. }
        | exec::ExecOp::VectorSearch { .. }
        | exec::ExecOp::TextSearch { .. }
        | exec::ExecOp::Limit { .. }
        | exec::ExecOp::Skip { .. }
        | exec::ExecOp::Range { .. }
        | exec::ExecOp::Distinct
        | exec::ExecOp::Order { .. }
        | exec::ExecOp::Project { .. }
        | exec::ExecOp::Aggregate { .. }
        | exec::ExecOp::Variable { .. }
        | exec::ExecOp::ShortestPath { .. }
        | exec::ExecOp::Mutation { .. }
        | exec::ExecOp::IndexDdl { .. }
        | exec::ExecOp::Merge { .. }
        | exec::ExecOp::Reserved { .. }
        | exec::ExecOp::ForEach { .. }
        | exec::ExecOp::Barrier { .. }
        | exec::ExecOp::Noop => parent_depth,
    }
}

fn subplan_maximum_traversal_increment(steps: &[exec::ExecStep]) -> usize {
    let mut depths = HashMap::new();
    let mut maximum = 0usize;
    for step in steps {
        let parent_depth = step
            .dependencies
            .iter()
            .filter_map(|dependency| depths.get(dependency))
            .copied()
            .max()
            .unwrap_or(0);
        let depth = traversal_depth_for_op(&step.op, parent_depth);
        depths.insert(step.id, depth);
        maximum = maximum.max(depth);
    }
    maximum
}

fn branch_maximum_traversal_increment(plan: &exec::ExecBranchPlan) -> usize {
    match plan {
        exec::ExecBranchPlan::Union(plans) => plans
            .iter()
            .map(|plan| subplan_maximum_traversal_increment(plan.steps()))
            .max()
            .unwrap_or(0),
        exec::ExecBranchPlan::Coalesce(plans) => plans
            .iter()
            .map(|plan| subplan_maximum_traversal_increment(plan.steps()))
            .max()
            .unwrap_or(0),
        exec::ExecBranchPlan::Choose { then_plan, .. }
        | exec::ExecBranchPlan::Optional(then_plan) => {
            subplan_maximum_traversal_increment(then_plan.steps())
        }
        exec::ExecBranchPlan::ChooseElse {
            then_plan,
            else_plan,
            ..
        } => subplan_maximum_traversal_increment(then_plan.steps())
            .max(subplan_maximum_traversal_increment(else_plan.steps())),
    }
}

fn effective_repeat_iterations(plan: &exec::ExecRepeatPlan) -> usize {
    let early_bound = match &plan.stop {
        ir::RepeatStopPlan::Times { count } | ir::RepeatStopPlan::TimesOrUntil { count, .. } => {
            count.get()
        }
        ir::RepeatStopPlan::MaxDepthOnly | ir::RepeatStopPlan::Until { .. } => plan.max_depth.get(),
    };
    early_bound.min(plan.max_depth.get())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AccessScope {
    element: catalog::ElementKind,
    label: Option<ir::NonEmptyString>,
}

/// Common access scope for a lineage, or an explicit marker that merged inputs
/// do not share one scope.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AccessLineageScope {
    Uniform(AccessScope),
    Mixed,
}

/// Access facts that survive operators which preserve the current stream.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AccessLineage {
    scope: AccessLineageScope,
    unbounded_sources: BTreeSet<exec::ExecStepId>,
}

/// One selected unbounded source and the value-free predicate facts attached
/// to it inside its executable plan or subplan.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UnboundedScanSite {
    scope: AccessScope,
    predicate_properties: BTreeSet<ir::NonEmptyString>,
}

fn resolve_lineage(
    id: exec::ExecStepId,
    steps: &HashMap<exec::ExecStepId, &exec::ExecStep>,
    memo: &mut HashMap<exec::ExecStepId, Option<AccessLineage>>,
    visiting: &mut HashSet<exec::ExecStepId>,
    unbounded_sites: &HashMap<exec::ExecStepId, UnboundedScanSite>,
) -> Option<AccessLineage> {
    if let Some(lineage) = memo.get(&id) {
        return lineage.clone();
    }
    if !visiting.insert(id) {
        return None;
    }
    let lineage = steps
        .get(&id)
        .and_then(|step| lineage_for_step(step, steps, memo, visiting, unbounded_sites));
    visiting.remove(&id);
    memo.insert(id, lineage.clone());
    lineage
}

fn lineage_for_step(
    step: &exec::ExecStep,
    steps: &HashMap<exec::ExecStepId, &exec::ExecStep>,
    memo: &mut HashMap<exec::ExecStepId, Option<AccessLineage>>,
    visiting: &mut HashSet<exec::ExecStepId>,
    unbounded_sites: &HashMap<exec::ExecStepId, UnboundedScanSite>,
) -> Option<AccessLineage> {
    match &step.op {
        exec::ExecOp::Access { plan } => scope_for_access(plan).map(|scope| AccessLineage {
            scope: AccessLineageScope::Uniform(scope),
            unbounded_sources: unbounded_sites
                .contains_key(&step.id)
                .then_some(step.id)
                .into_iter()
                .collect(),
        }),
        exec::ExecOp::KvRead(exec::KvReadPlan::RangeScan { keyspace, .. }) => Some(AccessLineage {
            scope: AccessLineageScope::Uniform(AccessScope {
                element: element_for_keyspace(*keyspace),
                label: None,
            }),
            unbounded_sources: unbounded_sites
                .contains_key(&step.id)
                .then_some(step.id)
                .into_iter()
                .collect(),
        }),
        exec::ExecOp::Merge {
            mode: exec::ExecMergeMode::Union | exec::ExecMergeMode::Intersect,
        } => combine_lineages(
            step.dependencies
                .iter()
                .map(|dependency| {
                    resolve_lineage(*dependency, steps, memo, visiting, unbounded_sites)
                })
                .collect(),
        ),
        exec::ExecOp::Filter { .. }
        | exec::ExecOp::VectorSearch { .. }
        | exec::ExecOp::TextSearch { .. }
        | exec::ExecOp::Limit { .. }
        | exec::ExecOp::Skip { .. }
        | exec::ExecOp::Range { .. }
        | exec::ExecOp::Distinct
        | exec::ExecOp::Order { .. }
        | exec::ExecOp::Reserved { .. }
        | exec::ExecOp::Barrier { .. }
        | exec::ExecOp::Noop => {
            let [dependency] = step.dependencies.as_slice() else {
                return None;
            };
            resolve_lineage(*dependency, steps, memo, visiting, unbounded_sites)
        }
        exec::ExecOp::KvRead(
            exec::KvReadPlan::Get { .. }
            | exec::KvReadPlan::MultiGet(_)
            | exec::KvReadPlan::PrefixScan { .. },
        )
        | exec::ExecOp::Expand { .. }
        | exec::ExecOp::Count { .. }
        | exec::ExecOp::Project { .. }
        | exec::ExecOp::Aggregate { .. }
        | exec::ExecOp::Variable { .. }
        | exec::ExecOp::Branch { .. }
        | exec::ExecOp::Repeat { .. }
        | exec::ExecOp::ShortestPath { .. }
        | exec::ExecOp::Mutation { .. }
        | exec::ExecOp::IndexDdl { .. }
        | exec::ExecOp::Merge {
            mode: exec::ExecMergeMode::Concat,
        }
        | exec::ExecOp::ForEach { .. } => None,
    }
}

fn unbounded_scan_scope(op: &exec::ExecOp) -> Option<AccessScope> {
    match op {
        exec::ExecOp::Access { plan } => unbounded_access_scope(plan),
        exec::ExecOp::KvRead(read) => unbounded_kv_read_scope(read),
        exec::ExecOp::Expand { .. }
        | exec::ExecOp::Count { .. }
        | exec::ExecOp::VectorSearch { .. }
        | exec::ExecOp::TextSearch { .. }
        | exec::ExecOp::Filter { .. }
        | exec::ExecOp::Limit { .. }
        | exec::ExecOp::Skip { .. }
        | exec::ExecOp::Range { .. }
        | exec::ExecOp::Distinct
        | exec::ExecOp::Order { .. }
        | exec::ExecOp::Project { .. }
        | exec::ExecOp::Aggregate { .. }
        | exec::ExecOp::Variable { .. }
        | exec::ExecOp::Branch { .. }
        | exec::ExecOp::Repeat { .. }
        | exec::ExecOp::ShortestPath { .. }
        | exec::ExecOp::Mutation { .. }
        | exec::ExecOp::IndexDdl { .. }
        | exec::ExecOp::Merge { .. }
        | exec::ExecOp::Reserved { .. }
        | exec::ExecOp::ForEach { .. }
        | exec::ExecOp::Barrier { .. }
        | exec::ExecOp::Noop => None,
    }
}

fn unbounded_access_scope(access: &exec::ExecAccessPlan) -> Option<AccessScope> {
    match access {
        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AllScan) => Some(AccessScope {
            element: catalog::ElementKind::Node,
            label: None,
        }),
        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::LabelScan { label }) => {
            Some(AccessScope {
                element: catalog::ElementKind::Node,
                label: Some(label.clone()),
            })
        }
        exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::AllScan) => Some(AccessScope {
            element: catalog::ElementKind::Edge,
            label: None,
        }),
        exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::LabelScan { label }) => {
            Some(AccessScope {
                element: catalog::ElementKind::Edge,
                label: Some(label.clone()),
            })
        }
        exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::Empty
            | exec::ExecNodeAccessPlan::FromParam { .. }
            | exec::ExecNodeAccessPlan::FromVar { .. }
            | exec::ExecNodeAccessPlan::Bitmap { .. }
            | exec::ExecNodeAccessPlan::Unique { .. }
            | exec::ExecNodeAccessPlan::AuthoritativeScan { .. }
            | exec::ExecNodeAccessPlan::DynamicEquality { .. }
            | exec::ExecNodeAccessPlan::RangeIndex { .. }
            | exec::ExecNodeAccessPlan::SecondarySet { .. }
            | exec::ExecNodeAccessPlan::VectorSearch { .. }
            | exec::ExecNodeAccessPlan::TextSearch { .. },
        )
        | exec::ExecAccessPlan::Edge(
            exec::ExecEdgeAccessPlan::Empty
            | exec::ExecEdgeAccessPlan::FromParam { .. }
            | exec::ExecEdgeAccessPlan::FromVar { .. }
            | exec::ExecEdgeAccessPlan::Bitmap { .. }
            | exec::ExecEdgeAccessPlan::AuthoritativeScan { .. }
            | exec::ExecEdgeAccessPlan::DynamicEquality { .. }
            | exec::ExecEdgeAccessPlan::RangeIndex { .. }
            | exec::ExecEdgeAccessPlan::SecondarySet { .. }
            | exec::ExecEdgeAccessPlan::VectorSearch { .. }
            | exec::ExecEdgeAccessPlan::TextSearch { .. },
        )
        | exec::ExecAccessPlan::Limited(_) => None,
    }
}

fn unbounded_kv_read_scope(read: &exec::KvReadPlan) -> Option<AccessScope> {
    match read {
        exec::KvReadPlan::RangeScan {
            keyspace,
            start: exec::KvKeyBound::Unbounded,
            end: exec::KvKeyBound::Unbounded,
            limit: None,
        } => Some(AccessScope {
            element: element_for_keyspace(*keyspace),
            label: None,
        }),
        exec::KvReadPlan::Get { .. }
        | exec::KvReadPlan::MultiGet(_)
        | exec::KvReadPlan::RangeScan { .. }
        | exec::KvReadPlan::PrefixScan { .. } => None,
    }
}

fn scope_for_access(access: &exec::ExecAccessPlan) -> Option<AccessScope> {
    match access {
        exec::ExecAccessPlan::Limited(limited) => scope_for_access(limited.source()),
        exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::AllScan
            | exec::ExecNodeAccessPlan::LabelScan { .. }
            | exec::ExecNodeAccessPlan::Bitmap { .. }
            | exec::ExecNodeAccessPlan::Unique { .. }
            | exec::ExecNodeAccessPlan::AuthoritativeScan { .. }
            | exec::ExecNodeAccessPlan::DynamicEquality { .. }
            | exec::ExecNodeAccessPlan::RangeIndex { .. }
            | exec::ExecNodeAccessPlan::SecondarySet { .. },
        ) => Some(AccessScope {
            element: catalog::ElementKind::Node,
            label: node_access_label(access).cloned(),
        }),
        exec::ExecAccessPlan::Edge(
            exec::ExecEdgeAccessPlan::AllScan
            | exec::ExecEdgeAccessPlan::LabelScan { .. }
            | exec::ExecEdgeAccessPlan::Bitmap { .. }
            | exec::ExecEdgeAccessPlan::AuthoritativeScan { .. }
            | exec::ExecEdgeAccessPlan::DynamicEquality { .. }
            | exec::ExecEdgeAccessPlan::RangeIndex { .. }
            | exec::ExecEdgeAccessPlan::SecondarySet { .. },
        ) => Some(AccessScope {
            element: catalog::ElementKind::Edge,
            label: edge_access_label(access).cloned(),
        }),
        exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::Empty
            | exec::ExecNodeAccessPlan::FromParam { .. }
            | exec::ExecNodeAccessPlan::FromVar { .. }
            | exec::ExecNodeAccessPlan::VectorSearch { .. }
            | exec::ExecNodeAccessPlan::TextSearch { .. },
        )
        | exec::ExecAccessPlan::Edge(
            exec::ExecEdgeAccessPlan::Empty
            | exec::ExecEdgeAccessPlan::FromParam { .. }
            | exec::ExecEdgeAccessPlan::FromVar { .. }
            | exec::ExecEdgeAccessPlan::VectorSearch { .. }
            | exec::ExecEdgeAccessPlan::TextSearch { .. },
        ) => None,
    }
}

fn node_access_label(access: &exec::ExecAccessPlan) -> Option<&ir::NonEmptyString> {
    let exec::ExecAccessPlan::Node(plan) = access else {
        return None;
    };
    match plan {
        exec::ExecNodeAccessPlan::LabelScan { label } => Some(label),
        exec::ExecNodeAccessPlan::Bitmap { bitmap } => node_bitmap_label(bitmap),
        exec::ExecNodeAccessPlan::Unique { lookup, .. } => Some(&lookup.key.label),
        exec::ExecNodeAccessPlan::AuthoritativeScan { predicate } => match predicate {
            exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => Some(&key.label),
            exec::ExecNodeAuthoritativeScanPredicate::Predicate(_) => None,
        },
        exec::ExecNodeAccessPlan::DynamicEquality { key, .. } => Some(&key.label),
        exec::ExecNodeAccessPlan::RangeIndex { key, .. } => Some(&key.label),
        exec::ExecNodeAccessPlan::SecondarySet { set } => node_secondary_set_label(set),
        exec::ExecNodeAccessPlan::Empty
        | exec::ExecNodeAccessPlan::FromParam { .. }
        | exec::ExecNodeAccessPlan::FromVar { .. }
        | exec::ExecNodeAccessPlan::AllScan
        | exec::ExecNodeAccessPlan::VectorSearch { .. }
        | exec::ExecNodeAccessPlan::TextSearch { .. } => None,
    }
}

fn edge_access_label(access: &exec::ExecAccessPlan) -> Option<&ir::NonEmptyString> {
    let exec::ExecAccessPlan::Edge(plan) = access else {
        return None;
    };
    match plan {
        exec::ExecEdgeAccessPlan::LabelScan { label } => Some(label),
        exec::ExecEdgeAccessPlan::Bitmap { bitmap } => edge_bitmap_label(bitmap),
        exec::ExecEdgeAccessPlan::AuthoritativeScan { predicate } => match predicate {
            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => Some(&key.label),
            exec::ExecEdgeAuthoritativeScanPredicate::Predicate(_) => None,
        },
        exec::ExecEdgeAccessPlan::DynamicEquality { key, .. } => Some(&key.label),
        exec::ExecEdgeAccessPlan::RangeIndex { key, .. } => Some(&key.label),
        exec::ExecEdgeAccessPlan::SecondarySet { set } => edge_secondary_set_label(set),
        exec::ExecEdgeAccessPlan::Empty
        | exec::ExecEdgeAccessPlan::FromParam { .. }
        | exec::ExecEdgeAccessPlan::FromVar { .. }
        | exec::ExecEdgeAccessPlan::AllScan
        | exec::ExecEdgeAccessPlan::VectorSearch { .. }
        | exec::ExecEdgeAccessPlan::TextSearch { .. } => None,
    }
}

fn node_secondary_set_label(set: &exec::ExecNodeSecondarySetPlan) -> Option<&ir::NonEmptyString> {
    match set {
        exec::ExecNodeSecondarySetPlan::Empty => None,
        exec::ExecNodeSecondarySetPlan::Bitmap(bitmap) => node_bitmap_label(bitmap),
        exec::ExecNodeSecondarySetPlan::Unique { lookup, .. } => Some(&lookup.key.label),
        exec::ExecNodeSecondarySetPlan::AuthoritativeScan(predicate) => match predicate {
            exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => Some(&key.label),
            exec::ExecNodeAuthoritativeScanPredicate::Predicate(_) => None,
        },
        exec::ExecNodeSecondarySetPlan::DynamicEquality { key, .. } => Some(&key.label),
        exec::ExecNodeSecondarySetPlan::Range(range)
        | exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver: range, .. } => {
            Some(&range.key.label)
        }
        exec::ExecNodeSecondarySetPlan::Intersect { driver, rest }
        | exec::ExecNodeSecondarySetPlan::Union { driver, rest } => {
            let mut labels = core::iter::once(driver.as_ref())
                .chain(rest.iter())
                .filter_map(node_secondary_set_label);
            let first = labels.next()?;
            labels.all(|label| label == first).then_some(first)
        }
    }
}

fn edge_secondary_set_label(set: &exec::ExecEdgeSecondarySetPlan) -> Option<&ir::NonEmptyString> {
    match set {
        exec::ExecEdgeSecondarySetPlan::Empty => None,
        exec::ExecEdgeSecondarySetPlan::Bitmap(bitmap) => edge_bitmap_label(bitmap),
        exec::ExecEdgeSecondarySetPlan::AuthoritativeScan(predicate) => match predicate {
            exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => Some(&key.label),
            exec::ExecEdgeAuthoritativeScanPredicate::Predicate(_) => None,
        },
        exec::ExecEdgeSecondarySetPlan::DynamicEquality { key, .. } => Some(&key.label),
        exec::ExecEdgeSecondarySetPlan::Range(range)
        | exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver: range, .. } => {
            Some(&range.key.label)
        }
        exec::ExecEdgeSecondarySetPlan::Intersect { driver, rest }
        | exec::ExecEdgeSecondarySetPlan::Union { driver, rest } => {
            let mut labels = core::iter::once(driver.as_ref())
                .chain(rest.iter())
                .filter_map(edge_secondary_set_label);
            let first = labels.next()?;
            labels.all(|label| label == first).then_some(first)
        }
    }
}

fn node_bitmap_lookup_count(bitmap: &exec::ExecNodeBitmapExpr) -> usize {
    match bitmap {
        exec::ExecNodeBitmapExpr::PointRead { .. } => 1,
        exec::ExecNodeBitmapExpr::BatchedUnionRead { values, .. } => values.len(),
        exec::ExecNodeBitmapExpr::Union { driver, rest }
        | exec::ExecNodeBitmapExpr::Intersect { driver, rest } => rest
            .iter()
            .fold(node_bitmap_lookup_count(driver), |count, child| {
                count.saturating_add(node_bitmap_lookup_count(child))
            }),
    }
}

fn edge_bitmap_lookup_count(bitmap: &exec::ExecEdgeBitmapExpr) -> usize {
    match bitmap {
        exec::ExecEdgeBitmapExpr::PointRead { .. } => 1,
        exec::ExecEdgeBitmapExpr::BatchedUnionRead { values, .. } => values.len(),
        exec::ExecEdgeBitmapExpr::Union { driver, rest }
        | exec::ExecEdgeBitmapExpr::Intersect { driver, rest } => rest
            .iter()
            .fold(edge_bitmap_lookup_count(driver), |count, child| {
                count.saturating_add(edge_bitmap_lookup_count(child))
            }),
    }
}

fn node_bitmap_label(bitmap: &exec::ExecNodeBitmapExpr) -> Option<&ir::NonEmptyString> {
    match bitmap {
        exec::ExecNodeBitmapExpr::PointRead { key, .. }
        | exec::ExecNodeBitmapExpr::BatchedUnionRead { key, .. } => Some(&key.label),
        exec::ExecNodeBitmapExpr::Union { driver, rest }
        | exec::ExecNodeBitmapExpr::Intersect { driver, rest } => {
            let first = node_bitmap_label(driver)?;
            rest.iter()
                .all(|child| node_bitmap_label(child) == Some(first))
                .then_some(first)
        }
    }
}

fn edge_bitmap_label(bitmap: &exec::ExecEdgeBitmapExpr) -> Option<&ir::NonEmptyString> {
    match bitmap {
        exec::ExecEdgeBitmapExpr::PointRead { key, .. }
        | exec::ExecEdgeBitmapExpr::BatchedUnionRead { key, .. } => Some(&key.label),
        exec::ExecEdgeBitmapExpr::Union { driver, rest }
        | exec::ExecEdgeBitmapExpr::Intersect { driver, rest } => {
            let first = edge_bitmap_label(driver)?;
            rest.iter()
                .all(|child| edge_bitmap_label(child) == Some(first))
                .then_some(first)
        }
    }
}

fn combine_lineages(lineages: Vec<Option<AccessLineage>>) -> Option<AccessLineage> {
    let mut has_unresolved_input = false;
    let mut combined = lineages
        .into_iter()
        .fold(None::<AccessLineage>, |combined, next| {
            let Some(next) = next else {
                has_unresolved_input = true;
                return combined;
            };
            Some(match combined {
                Some(mut combined) => {
                    if combined.scope != next.scope {
                        combined.scope = AccessLineageScope::Mixed;
                    }
                    combined.unbounded_sources.extend(next.unbounded_sources);
                    combined
                }
                None => next,
            })
        })?;
    if has_unresolved_input {
        combined.scope = AccessLineageScope::Mixed;
    }
    Some(combined)
}

fn collect_predicate_properties(
    predicate: &helix_ast::expr::Predicate,
    properties: &mut BTreeSet<ir::NonEmptyString>,
) {
    use helix_ast::expr::Predicate;

    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Neq { left, right }
        | Predicate::Gt { left, right }
        | Predicate::Gte { left, right }
        | Predicate::Lt { left, right }
        | Predicate::Lte { left, right }
        | Predicate::StartsWith {
            value: left,
            prefix: right,
        }
        | Predicate::EndsWith {
            value: left,
            suffix: right,
        }
        | Predicate::Contains {
            value: left,
            substring: right,
        }
        | Predicate::IsIn {
            value: left,
            values: right,
        }
        | Predicate::Compare { left, right, .. } => {
            collect_expr_properties(left, properties);
            collect_expr_properties(right, properties);
        }
        Predicate::Between { value, min, max } => {
            collect_expr_properties(value, properties);
            collect_expr_properties(min, properties);
            collect_expr_properties(max, properties);
        }
        Predicate::HasKey { property }
        | Predicate::IsNull { property }
        | Predicate::IsNotNull { property } => {
            record_predicate_property(property, properties);
        }
        Predicate::And { predicates } | Predicate::Or { predicates } => predicates
            .iter()
            .for_each(|predicate| collect_predicate_properties(predicate, properties)),
        Predicate::Not { predicate } => collect_predicate_properties(predicate, properties),
    }
}

fn collect_expr_properties(
    expr: &helix_ast::expr::Expr,
    properties: &mut BTreeSet<ir::NonEmptyString>,
) {
    use helix_ast::expr::Expr;

    match expr {
        Expr::Property(property) => record_predicate_property(property, properties),
        Expr::Add { left, right }
        | Expr::Sub { left, right }
        | Expr::Mul { left, right }
        | Expr::Div { left, right }
        | Expr::Mod { left, right } => {
            collect_expr_properties(left, properties);
            collect_expr_properties(right, properties);
        }
        Expr::Neg { expr } => collect_expr_properties(expr, properties),
        Expr::Case {
            when_then,
            else_expr,
        } => {
            when_then.iter().for_each(|branch| {
                collect_predicate_properties(&branch.when, properties);
                collect_expr_properties(&branch.then, properties);
            });
            else_expr
                .iter()
                .for_each(|expr| collect_expr_properties(expr, properties));
        }
        Expr::Id | Expr::Timestamp | Expr::DateTimeNow | Expr::Constant(_) | Expr::Param(_) => {}
    }
}

fn record_predicate_property(property: &str, properties: &mut BTreeSet<ir::NonEmptyString>) {
    if property == "$label" {
        return;
    }
    properties.insert(
        ir::NonEmptyString::new(property.to_owned())
            .expect("validated predicate property names are non-empty"),
    );
}

fn effective_filter_label(
    scope: &AccessScope,
    predicate: &helix_ast::expr::Predicate,
) -> Option<ir::NonEmptyString> {
    match (scope.label.as_ref(), analysis::label_scope(predicate).ok()?) {
        (
            Some(access),
            analysis::LabelScope::Feasible(analysis::FeasibleLabelScope::Scoped(label)),
        ) if access == &label => Some(access.clone()),
        (Some(_), analysis::LabelScope::Feasible(analysis::FeasibleLabelScope::Scoped(_)))
        | (_, analysis::LabelScope::Impossible) => None,
        (Some(access), analysis::LabelScope::Feasible(analysis::FeasibleLabelScope::Unscoped)) => {
            Some(access.clone())
        }
        (None, analysis::LabelScope::Feasible(analysis::FeasibleLabelScope::Scoped(label))) => {
            Some(label)
        }
        (None, analysis::LabelScope::Feasible(analysis::FeasibleLabelScope::Unscoped)) => None,
    }
}

const fn element_for_keyspace(keyspace: exec::ElementKeyspace) -> catalog::ElementKind {
    match keyspace {
        exec::ElementKeyspace::NodeProperty => catalog::ElementKind::Node,
        exec::ElementKeyspace::EdgeEndpoints => catalog::ElementKind::Edge,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum ElementKey {
    Node,
    Edge,
}

impl From<catalog::ElementKind> for ElementKey {
    fn from(value: catalog::ElementKind) -> Self {
        match value {
            catalog::ElementKind::Node => Self::Node,
            catalog::ElementKind::Edge => Self::Edge,
        }
    }
}

impl From<ElementKey> for catalog::ElementKind {
    fn from(value: ElementKey) -> Self {
        match value {
            ElementKey::Node => Self::Node,
            ElementKey::Edge => Self::Edge,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IndexKindKey {
    Equality,
    Range,
}

impl From<rules::CandidateIndexKind> for IndexKindKey {
    fn from(value: rules::CandidateIndexKind) -> Self {
        match value {
            rules::CandidateIndexKind::Equality => Self::Equality,
            rules::CandidateIndexKind::Range => Self::Range,
        }
    }
}

impl From<IndexKindKey> for SecondaryIndexKind {
    fn from(value: IndexKindKey) -> Self {
        match value {
            IndexKindKey::Equality => Self::Equality,
            IndexKindKey::Range => Self::Range,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MissingIndexKey {
    element: ElementKey,
    label: ir::NonEmptyString,
    property: ir::NonEmptyString,
    kind: IndexKindKey,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UnboundedScanKey {
    element: ElementKey,
    label: Option<ir::NonEmptyString>,
    predicate_properties: PredicatePropertySet,
}
