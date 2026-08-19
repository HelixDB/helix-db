//! Stable production rule ID names.

use super::KnownRuleId;

impl KnownRuleId {
    /// Stable serialized identifier for this production rule.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FilterPushdown => "filter_pushdown",
            Self::PurePipelineSimplification => "pure_pipeline_simplification",
            Self::StaticPredicateSimplification => "static_predicate_simplification",
            Self::FilterMerge => "filter_merge",
            Self::SeedPurePipeline => "seed_pure_pipeline",
            Self::SeedVariableSource => "seed_variable_source",
            Self::SeedSimplifiedPredicate => "seed_simplified_predicate",
            Self::SeedSourceAccess => "seed_source_access",
            Self::SeedFilter => "seed_filter",
            Self::SeedOrder => "seed_order",
            Self::SeedBarrier => "seed_barrier",
            Self::SeedAccessPath => "seed_access_path",
            Self::AccessWindow => "access_window",
            Self::SeedAccessWindow => "seed_access_window",
            Self::AccessFilterSimplification => "access_filter_simplification",
            Self::AccessFilterIndex => "access_filter_index",
            Self::SeedAccessFilter => "seed_access_filter",
            Self::AccessSetSimplification => "access_set_simplification",
            Self::AccessRangeIntersection => "access_range_intersection",
            Self::AccessEqualityRangeIntersection => "access_equality_range_intersection",
            Self::AccessEqualityRangeUnion => "access_equality_range_union",
            Self::AccessContradiction => "access_contradiction",
            Self::AccessSubsumption => "access_subsumption",
            Self::AccessOrderRangeDirection => "access_order_range_direction",
            Self::AccessOrder => "access_order",
            Self::SeedAccessOrder => "seed_access_order",
            Self::AccessDistinct => "access_distinct",
            Self::SeedAccessDistinct => "seed_access_distinct",
            Self::AccessPipelineFilter => "access_pipeline_filter",
            Self::AccessPipelineOrder => "access_pipeline_order",
            Self::AccessPipelineSimplification => "access_pipeline_simplification",
            Self::SeedAccessPipeline => "seed_access_pipeline",
            Self::SeedRootPipeline => "seed_root_pipeline",
            Self::SeedRootMutation => "seed_root_mutation",
            Self::SeedRootIndexDdl => "seed_root_index_ddl",
            Self::SeedRootShortestPath => "seed_root_shortest_path",
            Self::RootControlFlowEmpty => "root_control_flow_empty",
            Self::RootStreamAccessRewrite => "root_stream_access_rewrite",
            Self::SeedRootBranch => "seed_root_branch",
            Self::SeedRootRepeat => "seed_root_repeat",
            Self::SeedStreamReserved => "seed_stream_reserved",
            Self::SeedStreamCardinality => "seed_stream_cardinality",
            Self::SeedStreamProject => "seed_stream_project",
            Self::SeedStreamAggregate => "seed_stream_aggregate",
            Self::SeedStreamVariableWrite => "seed_stream_variable_write",
            Self::StreamWindowComposition => "stream_window_composition",
            Self::SeedStream => "seed_stream",
        }
    }

    /// Resolve a stable serialized identifier to a production rule ID.
    pub fn from_name(value: &str) -> Option<Self> {
        match value {
            "filter_pushdown" => Some(Self::FilterPushdown),
            "pure_pipeline_simplification" => Some(Self::PurePipelineSimplification),
            "static_predicate_simplification" => Some(Self::StaticPredicateSimplification),
            "filter_merge" => Some(Self::FilterMerge),
            "seed_pure_pipeline" => Some(Self::SeedPurePipeline),
            "seed_variable_source" => Some(Self::SeedVariableSource),
            "seed_simplified_predicate" => Some(Self::SeedSimplifiedPredicate),
            "seed_source_access" => Some(Self::SeedSourceAccess),
            "seed_filter" => Some(Self::SeedFilter),
            "seed_order" => Some(Self::SeedOrder),
            "seed_barrier" => Some(Self::SeedBarrier),
            "seed_access_path" => Some(Self::SeedAccessPath),
            "access_window" => Some(Self::AccessWindow),
            "seed_access_window" => Some(Self::SeedAccessWindow),
            "access_filter_simplification" => Some(Self::AccessFilterSimplification),
            "access_filter_index" => Some(Self::AccessFilterIndex),
            "seed_access_filter" => Some(Self::SeedAccessFilter),
            "access_set_simplification" => Some(Self::AccessSetSimplification),
            "access_range_intersection" => Some(Self::AccessRangeIntersection),
            "access_equality_range_intersection" => Some(Self::AccessEqualityRangeIntersection),
            "access_equality_range_union" => Some(Self::AccessEqualityRangeUnion),
            "access_contradiction" => Some(Self::AccessContradiction),
            "access_subsumption" => Some(Self::AccessSubsumption),
            "access_order_range_direction" => Some(Self::AccessOrderRangeDirection),
            "access_order" => Some(Self::AccessOrder),
            "seed_access_order" => Some(Self::SeedAccessOrder),
            "access_distinct" => Some(Self::AccessDistinct),
            "seed_access_distinct" => Some(Self::SeedAccessDistinct),
            "access_pipeline_filter" => Some(Self::AccessPipelineFilter),
            "access_pipeline_order" => Some(Self::AccessPipelineOrder),
            "access_pipeline_simplification" => Some(Self::AccessPipelineSimplification),
            "seed_access_pipeline" => Some(Self::SeedAccessPipeline),
            "seed_root_pipeline" => Some(Self::SeedRootPipeline),
            "seed_root_mutation" => Some(Self::SeedRootMutation),
            "seed_root_index_ddl" => Some(Self::SeedRootIndexDdl),
            "seed_root_shortest_path" => Some(Self::SeedRootShortestPath),
            "root_control_flow_empty" => Some(Self::RootControlFlowEmpty),
            "root_stream_access_rewrite" => Some(Self::RootStreamAccessRewrite),
            "seed_root_branch" => Some(Self::SeedRootBranch),
            "seed_root_repeat" => Some(Self::SeedRootRepeat),
            "seed_stream_reserved" => Some(Self::SeedStreamReserved),
            "seed_stream_cardinality" => Some(Self::SeedStreamCardinality),
            "seed_stream_project" => Some(Self::SeedStreamProject),
            "seed_stream_aggregate" => Some(Self::SeedStreamAggregate),
            "seed_stream_variable_write" => Some(Self::SeedStreamVariableWrite),
            "stream_window_composition" => Some(Self::StreamWindowComposition),
            "seed_stream" => Some(Self::SeedStream),
            _ => None,
        }
    }
}

impl AsRef<str> for KnownRuleId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for KnownRuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}
