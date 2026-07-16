//! Stable selected-root reconstruction rejection inventory.

/// Unsupported selected-root reconstruction boundary reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::planning::selected) enum Reason {
    /// A multi-root optimizer result did not contain one root per pending input.
    OptimizerRootCountMismatch,
    /// A memo group did not have a best selected physical alternative.
    BestPlanMissing,
    /// Optimized selected roots did not align with pending-root indexes.
    OptimizedRootBatchMismatch,
    /// A selected root family was paired with the wrong physical root shape.
    SelectedRootPhysicalMismatch,
    /// A selected root pipeline physical plan is shorter than its logical suffix.
    SelectedRootPipelineLogicalSuffixTooLong,
    /// A selected root pipeline physical suffix does not implement its logical suffix.
    SelectedRootPipelinePhysicalSuffixMismatch,
    /// A selected root terminal physical suffix does not implement its logical terminal.
    SelectedRootTerminalPhysicalSuffixMismatch,
    /// A recursive selected root-stream input was paired with a parent-local prefix.
    SelectedRootStreamInputNonLocalizedPrefix,
    /// A generic selected alternative is not executable-selected IR.
    SelectedAlternativeUnsupported,
    /// A selected alternative family proof did not match its logical/physical pair.
    SelectedAlternativeFamilyMismatch,
    /// Selected memo-child provenance had the wrong arity for the parent shape.
    MemoChildArityMismatch,
    /// Selected lowering needed memo-child context but was not given an optimizer result.
    MemoChildContextMissing,
    /// A selected memo-child plan was missing from optimizer output.
    MemoChildPlanMissing,
    /// Selected branch child roots did not match the branch payload arity.
    BranchRootArityMismatch,
    /// Selected branch child roots could not reconstruct the branch payload.
    BranchPlanReconstructionMismatch,
    /// Selected repeat child roots were not exactly input plus body.
    RepeatRootArityMismatch,
    /// A selected root-stream child did not have the required one-child arity.
    RootStreamChildArityMismatch,
    /// A selected root-stream child had the wrong selected root family.
    RootStreamChildKindMismatch,
    /// A selected terminal root-stream child was not a terminal root.
    TerminalRootStreamChildKindMismatch,
}

impl Reason {
    #[cfg(test)]
    pub(super) const ALL: &'static [Self] = &[
        Self::OptimizerRootCountMismatch,
        Self::BestPlanMissing,
        Self::OptimizedRootBatchMismatch,
        Self::SelectedRootPhysicalMismatch,
        Self::SelectedRootPipelineLogicalSuffixTooLong,
        Self::SelectedRootPipelinePhysicalSuffixMismatch,
        Self::SelectedRootTerminalPhysicalSuffixMismatch,
        Self::SelectedRootStreamInputNonLocalizedPrefix,
        Self::SelectedAlternativeUnsupported,
        Self::SelectedAlternativeFamilyMismatch,
        Self::MemoChildArityMismatch,
        Self::MemoChildContextMissing,
        Self::MemoChildPlanMissing,
        Self::BranchRootArityMismatch,
        Self::BranchPlanReconstructionMismatch,
        Self::RepeatRootArityMismatch,
        Self::RootStreamChildArityMismatch,
        Self::RootStreamChildKindMismatch,
        Self::TerminalRootStreamChildKindMismatch,
    ];

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::OptimizerRootCountMismatch => {
                "selected optimizer result root count did not match pending roots"
            }
            Self::BestPlanMissing => {
                "selected optimizer result did not contain a best physical alternative"
            }
            Self::OptimizedRootBatchMismatch => {
                "selected optimized roots did not match pending-root batch indexes"
            }
            Self::SelectedRootPhysicalMismatch => {
                "selected logical root family did not match physical alternative shape"
            }
            Self::SelectedRootPipelineLogicalSuffixTooLong => {
                "selected root pipeline physical plan is shorter than its logical suffix"
            }
            Self::SelectedRootPipelinePhysicalSuffixMismatch => {
                "selected root pipeline physical suffix does not match its logical suffix"
            }
            Self::SelectedRootTerminalPhysicalSuffixMismatch => {
                "selected root terminal physical suffix does not match its logical terminal"
            }
            Self::SelectedRootStreamInputNonLocalizedPrefix => {
                "selected recursive root-stream input cannot consume a parent-local prefix"
            }
            Self::SelectedAlternativeUnsupported => {
                "selected logical alternative is not an executable selected alternative"
            }
            Self::SelectedAlternativeFamilyMismatch => {
                "selected alternative family proof did not match logical and physical shapes"
            }
            Self::MemoChildArityMismatch => {
                "selected memo-child provenance arity did not match parent shape"
            }
            Self::MemoChildContextMissing => {
                "selected child-bearing root did not have optimizer memo-child context"
            }
            Self::MemoChildPlanMissing => {
                "selected memo-child provenance did not resolve to a best child plan"
            }
            Self::BranchRootArityMismatch => {
                "selected branch roots did not match branch payload arity"
            }
            Self::BranchPlanReconstructionMismatch => {
                "selected branch roots could not reconstruct branch payload"
            }
            Self::RepeatRootArityMismatch => {
                "selected repeat roots were not exactly input plus body"
            }
            Self::RootStreamChildArityMismatch => {
                "selected root-stream child provenance was not exactly one child"
            }
            Self::RootStreamChildKindMismatch => {
                "selected root-stream child had the wrong selected root family"
            }
            Self::TerminalRootStreamChildKindMismatch => {
                "selected terminal root-stream child was not a terminal root"
            }
        }
    }
}
