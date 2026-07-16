/// Unsupported selected executable-lowering boundary reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::exec) enum Reason {
    /// A selected alternative produced no executable DAG steps.
    SelectedAlternativeEmptyDag,
    /// A selected batch produced no root step.
    SelectedBatchEntriesMissingRoot,
    /// A selected batch produced no executable DAG steps.
    SelectedBatchEntriesEmptyDag,
    /// A selected run root produced no executable DAG steps.
    SelectedRunRootEmptyDag,
    /// A follow-up batch entry was lowered without a previous root.
    FollowupBeforeInitialEntry,
    /// Logical and physical alternative roots are incompatible.
    LogicalPhysicalAlternativeMismatch,
    /// A selected alternative returned a step not present in the DAG.
    UnknownLoweredStep,
    /// An access path and physical access disagree.
    AccessPathPhysicalAccessMismatch,
    /// An access-filter pipeline does not match its logical source.
    AccessFilterSourceMismatch,
    /// An access-window pipeline does not match its logical source.
    AccessWindowSourceMismatch,
    /// An access-window pipeline has an incompatible physical suffix.
    AccessWindowPhysicalSuffixMismatch,
    /// An access-window range suffix lacks a bounded logical window.
    AccessWindowRangeSuffixMissingBoundedWindow,
    /// An access-order pipeline does not match its logical source.
    AccessOrderSourceMismatch,
    /// An access-order pipeline has an incompatible physical suffix.
    AccessOrderPhysicalSuffixMismatch,
    /// An access-distinct pipeline does not match its logical source.
    AccessDistinctSourceMismatch,
    /// An access-distinct pipeline has an incompatible physical suffix.
    AccessDistinctPhysicalSuffixMismatch,
    /// An access-pipeline contains a no-op identity window.
    AccessPipelineIdentityWindow,
    /// An access-pipeline does not match its logical source.
    AccessPipelineSourceMismatch,
    /// An access-pipeline has an incompatible physical suffix.
    AccessPipelinePhysicalSuffixMismatch,
    /// A selected access-stream prefix is empty.
    AccessStreamPipelinePrefixEmpty,
    /// An access-stream path pipeline does not match its logical source.
    AccessStreamPathSourceMismatch,
    /// A variable-source root stream pipeline does not match its logical source.
    RootStreamVariableSourceMismatch,
}

impl Reason {
    #[cfg(test)]
    pub(super) const ALL: &'static [Self] = &[
        Self::SelectedAlternativeEmptyDag,
        Self::SelectedBatchEntriesMissingRoot,
        Self::SelectedBatchEntriesEmptyDag,
        Self::SelectedRunRootEmptyDag,
        Self::FollowupBeforeInitialEntry,
        Self::LogicalPhysicalAlternativeMismatch,
        Self::UnknownLoweredStep,
        Self::AccessPathPhysicalAccessMismatch,
        Self::AccessFilterSourceMismatch,
        Self::AccessWindowSourceMismatch,
        Self::AccessWindowPhysicalSuffixMismatch,
        Self::AccessWindowRangeSuffixMissingBoundedWindow,
        Self::AccessOrderSourceMismatch,
        Self::AccessOrderPhysicalSuffixMismatch,
        Self::AccessDistinctSourceMismatch,
        Self::AccessDistinctPhysicalSuffixMismatch,
        Self::AccessPipelineIdentityWindow,
        Self::AccessPipelineSourceMismatch,
        Self::AccessPipelinePhysicalSuffixMismatch,
        Self::AccessStreamPipelinePrefixEmpty,
        Self::AccessStreamPathSourceMismatch,
        Self::RootStreamVariableSourceMismatch,
    ];

    pub(in crate::exec) const fn as_str(self) -> &'static str {
        match self {
            Self::SelectedAlternativeEmptyDag => {
                "selected executable alternative lowered to an empty executable DAG"
            }
            Self::SelectedBatchEntriesMissingRoot => {
                "selected executable batch entries produced no root step"
            }
            Self::SelectedBatchEntriesEmptyDag => {
                "selected executable batch entries lowered to an empty executable DAG"
            }
            Self::SelectedRunRootEmptyDag => "selected run root lowered to an empty executable DAG",
            Self::FollowupBeforeInitialEntry => {
                "follow-up selected entry was lowered before an initial entry"
            }
            Self::LogicalPhysicalAlternativeMismatch => {
                "logical source and physical alternative are incompatible"
            }
            Self::UnknownLoweredStep => {
                "selected-alternative lowering returned an unknown executable step"
            }
            Self::AccessPathPhysicalAccessMismatch => "access path does not match physical access",
            Self::AccessFilterSourceMismatch => {
                "access-filter pipeline does not match its logical source"
            }
            Self::AccessWindowSourceMismatch => {
                "access-window pipeline does not match its logical source"
            }
            Self::AccessWindowPhysicalSuffixMismatch => {
                "access-window pipeline has an incompatible physical suffix"
            }
            Self::AccessWindowRangeSuffixMissingBoundedWindow => {
                "access-window range suffix requires a bounded window"
            }
            Self::AccessOrderSourceMismatch => {
                "access-order pipeline does not match its logical source"
            }
            Self::AccessOrderPhysicalSuffixMismatch => {
                "access-order pipeline has an incompatible physical suffix"
            }
            Self::AccessDistinctSourceMismatch => {
                "access-distinct pipeline does not match its logical source"
            }
            Self::AccessDistinctPhysicalSuffixMismatch => {
                "access-distinct pipeline has an incompatible physical suffix"
            }
            Self::AccessPipelineIdentityWindow => "access-pipeline contains an identity window",
            Self::AccessPipelineSourceMismatch => {
                "access-pipeline does not match its logical source"
            }
            Self::AccessPipelinePhysicalSuffixMismatch => {
                "access-pipeline has an incompatible physical suffix"
            }
            Self::AccessStreamPipelinePrefixEmpty => {
                "selected access-stream pipeline prefix is empty"
            }
            Self::AccessStreamPathSourceMismatch => {
                "access-stream path pipeline does not match its logical source"
            }
            Self::RootStreamVariableSourceMismatch => {
                "root stream variable-source pipeline does not match its logical source"
            }
        }
    }
}
