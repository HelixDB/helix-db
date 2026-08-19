//! Built-in seed rule registry.
//!
//! The registry wires concrete rule implementations into the Cascades optimizer
//! while keeping optimizer-facing rule contracts separate from rule logic.

use super::*;
use crate::optimizer;

/// Built-in rule registry used by the production Cascades driver.
///
/// The registry keeps source access, stream operators, ordering, barriers, and
/// root-stream contracts wired through explicit rule families instead of
/// coupling optimizer scheduling to individual rule implementations.
#[derive(Default)]
pub struct SeedRuleSet {
    static_predicate: StaticPredicateSimplificationRule,
    filter_merge: FilterMergeRule,
    filter_pushdown: FilterPushdownRule,
    pure_pipeline_simplification: PurePipelineSimplificationRule,
    access_filter_simplification: AccessFilterSimplificationRule,
    access_filter_index: AccessFilterIndexRule,
    access_filter_implementation: AccessFilterImplementationRule,
    access_set_simplification: AccessSetSimplificationRule,
    access_range_intersection: AccessRangeIntersectionRule,
    access_equality_range_intersection: AccessEqualityRangeIntersectionRule,
    access_equality_range_union: AccessEqualityRangeUnionRule,
    access_contradiction: AccessContradictionRule,
    access_subsumption: AccessSubsumptionRule,
    access_window: AccessWindowRule,
    access_window_implementation: AccessWindowImplementationRule,
    access_order_range_direction: AccessOrderRangeDirectionRule,
    access_order: AccessOrderRule,
    access_order_implementation: AccessOrderImplementationRule,
    access_distinct: AccessDistinctRule,
    access_distinct_implementation: AccessDistinctImplementationRule,
    access_pipeline_filter: AccessPipelineFilterRule,
    access_pipeline_order: AccessPipelineOrderRule,
    access_pipeline_simplification: AccessPipelineSimplificationRule,
    access_pipeline_implementation: AccessPipelineImplementationRule,
    root_pipeline_implementation: RootPipelineImplementationRule,
    root_mutation_implementation: RootMutationImplementationRule,
    root_index_ddl_implementation: RootIndexDdlImplementationRule,
    root_shortest_path_implementation: RootShortestPathImplementationRule,
    root_control_flow_empty: RootControlFlowEmptyRule,
    root_stream_access_rewrite: RootStreamAccessRewriteRule,
    root_branch_implementation: RootBranchImplementationRule,
    root_repeat_implementation: RootRepeatImplementationRule,
    stream_reserved_implementation: StreamReservedImplementationRule,
    stream_cardinality_implementation: StreamCardinalityImplementationRule,
    stream_project_implementation: StreamProjectImplementationRule,
    stream_aggregate_implementation: StreamAggregateImplementationRule,
    stream_variable_write_implementation: StreamVariableWriteImplementationRule,
    stream_composition: StreamCompositionRule,
    pipeline: PipelineImplementationRule,
    access_path: AccessPathImplementationRule,
    variable_source: VariableSourceImplementationRule,
    simplified_predicate: SimplifiedPredicateImplementationRule,
    source_access: SourceAccessImplementationRule,
    filter: FilterImplementationRule,
    stream: StreamImplementationRule,
    order: OrderImplementationRule,
    barrier: BarrierImplementationRule,
}

impl SeedRuleSet {
    /// Build the optimizer view over the seed rule registry.
    pub fn optimizer(&self) -> optimizer::CascadesOptimizer<'_> {
        optimizer::CascadesOptimizer::new(self.registry())
    }

    /// Build the validated optimizer registry over the seed rules.
    pub fn registry(&self) -> optimizer::OptimizerRuleRegistry<'_> {
        let registry = optimizer::OptimizerRuleRegistry::try_from_known_rules(vec![
            &self.static_predicate,
            &self.filter_merge,
            &self.filter_pushdown,
            &self.pure_pipeline_simplification,
            &self.access_filter_simplification,
            &self.access_filter_index,
            &self.access_filter_implementation,
            &self.access_set_simplification,
            &self.access_range_intersection,
            &self.access_equality_range_intersection,
            &self.access_equality_range_union,
            &self.access_contradiction,
            &self.access_subsumption,
            &self.access_window,
            &self.access_window_implementation,
            &self.access_order_range_direction,
            &self.access_order,
            &self.access_order_implementation,
            &self.access_distinct,
            &self.access_distinct_implementation,
            &self.access_pipeline_filter,
            &self.access_pipeline_order,
            &self.access_pipeline_simplification,
            &self.access_pipeline_implementation,
            &self.root_pipeline_implementation,
            &self.root_mutation_implementation,
            &self.root_index_ddl_implementation,
            &self.root_shortest_path_implementation,
            &self.root_control_flow_empty,
            &self.root_stream_access_rewrite,
            &self.root_branch_implementation,
            &self.root_repeat_implementation,
            &self.stream_reserved_implementation,
            &self.stream_cardinality_implementation,
            &self.stream_project_implementation,
            &self.stream_aggregate_implementation,
            &self.stream_variable_write_implementation,
            &self.stream_composition,
            &self.pipeline,
            &self.access_path,
            &self.variable_source,
            &self.simplified_predicate,
            &self.source_access,
            &self.filter,
            &self.stream,
            &self.order,
            &self.barrier,
        ]);
        // The seed registry is a closed static field inventory. The validator
        // still runs here so future duplicate, custom, or missing built-in
        // rule IDs fail before they can corrupt provenance/scheduling.
        registry.expect("built-in seed rule registry must match the complete known rule inventory")
    }
}
