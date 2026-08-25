//! Access-pipeline rewrite contracts.

use crate::{logical, optimizer};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum AccessPipelineRebuild {
    Collapsed(logical::AccessPath),
    Pipeline(logical::AccessPipeline),
    NotApplicable(AccessPipelineRebuildRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AccessPipelineRebuildRejection {
    InvalidPipelineShape,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PipelineSimplification {
    Rewritten(optimizer::RuleResult),
    NotApplicable(PipelineSimplificationRejection),
}

impl PipelineSimplification {
    pub(super) const fn is_applicable(&self) -> bool {
        matches!(self, Self::Rewritten(_))
    }

    pub(super) fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::Rewritten(result) => result,
            Self::NotApplicable(_) => optimizer::RuleResult::NotApplicable,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PipelineSimplificationRejection {
    NoLocalSimplification {
        empty: EmptyPipelineRejection,
        distinct: PipelineDistinctRejection,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum EmptyPipelineResult {
    Empty(logical::AccessPath),
    NotEmpty(EmptyPipelineRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EmptyPipelineRejection {
    NonEmptyAccessSource,
    DataProducingPipelineOp,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PipelineDistinctSimplification {
    Rewritten(optimizer::RuleResult),
    NotApplicable(PipelineDistinctRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PipelineDistinctRejection {
    NoReducibleDistinct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PipelineDistinctPair {
    Adjacent { first_index: usize },
    NotFound,
}
