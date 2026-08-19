//! Family-grouped production rule inventory.

use super::KnownRuleId;

impl KnownRuleId {
    /// Core pure/logical rule IDs.
    pub const CORE: &'static [Self] = &[
        Self::FilterPushdown,
        Self::PurePipelineSimplification,
        Self::StaticPredicateSimplification,
        Self::FilterMerge,
        Self::SeedPurePipeline,
        Self::SeedVariableSource,
        Self::SeedSimplifiedPredicate,
        Self::SeedSourceAccess,
        Self::SeedFilter,
        Self::SeedOrder,
        Self::SeedBarrier,
    ];

    /// Access-path, access-filter, access-window/order/distinct, and
    /// access-pipeline rule IDs.
    pub const ACCESS: &'static [Self] = &[
        Self::SeedAccessPath,
        Self::AccessWindow,
        Self::SeedAccessWindow,
        Self::AccessFilterSimplification,
        Self::AccessFilterIndex,
        Self::SeedAccessFilter,
        Self::AccessSetSimplification,
        Self::AccessRangeIntersection,
        Self::AccessEqualityRangeIntersection,
        Self::AccessEqualityRangeUnion,
        Self::AccessContradiction,
        Self::AccessSubsumption,
        Self::AccessOrderRangeDirection,
        Self::AccessOrder,
        Self::SeedAccessOrder,
        Self::AccessDistinct,
        Self::SeedAccessDistinct,
        Self::AccessPipelineFilter,
        Self::AccessPipelineOrder,
        Self::AccessPipelineSimplification,
        Self::SeedAccessPipeline,
    ];

    /// Root pipeline, mutation, DDL, control-flow, and terminal rule IDs.
    pub const ROOT: &'static [Self] = &[
        Self::SeedRootPipeline,
        Self::SeedRootMutation,
        Self::SeedRootIndexDdl,
        Self::SeedRootShortestPath,
        Self::RootControlFlowEmpty,
        Self::RootStreamAccessRewrite,
        Self::SeedRootBranch,
        Self::SeedRootRepeat,
        Self::SeedStreamReserved,
        Self::SeedStreamCardinality,
        Self::SeedStreamProject,
        Self::SeedStreamAggregate,
        Self::SeedStreamVariableWrite,
    ];

    /// Pure stream-window and ordinary stream implementation rule IDs.
    pub const STREAM: &'static [Self] = &[Self::StreamWindowComposition, Self::SeedStream];

    /// Stable ordered inventory of production rule IDs.
    pub const ALL: &'static [Self] = &[
        Self::FilterPushdown,
        Self::PurePipelineSimplification,
        Self::StaticPredicateSimplification,
        Self::FilterMerge,
        Self::SeedPurePipeline,
        Self::SeedVariableSource,
        Self::SeedSimplifiedPredicate,
        Self::SeedSourceAccess,
        Self::SeedFilter,
        Self::SeedOrder,
        Self::SeedBarrier,
        Self::SeedAccessPath,
        Self::AccessWindow,
        Self::SeedAccessWindow,
        Self::AccessFilterSimplification,
        Self::AccessFilterIndex,
        Self::SeedAccessFilter,
        Self::AccessSetSimplification,
        Self::AccessRangeIntersection,
        Self::AccessEqualityRangeIntersection,
        Self::AccessEqualityRangeUnion,
        Self::AccessContradiction,
        Self::AccessSubsumption,
        Self::AccessOrderRangeDirection,
        Self::AccessOrder,
        Self::SeedAccessOrder,
        Self::AccessDistinct,
        Self::SeedAccessDistinct,
        Self::AccessPipelineFilter,
        Self::AccessPipelineOrder,
        Self::AccessPipelineSimplification,
        Self::SeedAccessPipeline,
        Self::SeedRootPipeline,
        Self::SeedRootMutation,
        Self::SeedRootIndexDdl,
        Self::SeedRootShortestPath,
        Self::RootControlFlowEmpty,
        Self::RootStreamAccessRewrite,
        Self::SeedRootBranch,
        Self::SeedRootRepeat,
        Self::SeedStreamReserved,
        Self::SeedStreamCardinality,
        Self::SeedStreamProject,
        Self::SeedStreamAggregate,
        Self::SeedStreamVariableWrite,
        Self::StreamWindowComposition,
        Self::SeedStream,
    ];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_rule_family_slices_partition_ordered_inventory() {
        let by_family = KnownRuleId::CORE
            .iter()
            .chain(KnownRuleId::ACCESS)
            .chain(KnownRuleId::ROOT)
            .chain(KnownRuleId::STREAM)
            .copied()
            .collect::<Vec<_>>();

        assert_eq!(by_family, KnownRuleId::ALL);
    }
}
