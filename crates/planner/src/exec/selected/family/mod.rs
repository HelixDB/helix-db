//! Ordinary selected executable alternative family contracts.
//!
//! The optimizer selects a logical source expression and a physical
//! implementation. This module classifies ordinary executable pairs before
//! they become selected executable roots, keeping root/control/terminal
//! payloads out of the generic alternative path.

mod classify;
#[cfg(test)]
mod tests;

/// Closed family inventory for ordinary selected executable alternatives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectedExecutableAlternativeFamily {
    /// Node access path implemented by a node physical access.
    NodeAccessPath,
    /// Edge access path implemented by an edge physical access.
    EdgeAccessPath,
    /// Pure source implemented directly by an LSM KV read.
    KvSource,
    /// Proven no-op logical expression.
    NoOp,
    /// Variable source injection.
    VariableSource,
    /// Access filter implemented by an access-prefixed physical pipeline.
    AccessFilterPipeline,
    /// Access window implemented by an access-prefixed physical pipeline.
    AccessWindowPipeline,
    /// Access order implemented by an access-prefixed physical pipeline.
    AccessOrderPipeline,
    /// Access distinct implemented by an access-prefixed physical pipeline.
    AccessDistinctPipeline,
    /// General access-rooted physical pipeline.
    AccessPipeline,
}

/// Ordinary selected executable alternative classification result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectedExecutableAlternativeClassification {
    /// The logical/physical pair belongs to a supported ordinary family.
    Classified(SelectedExecutableAlternativeFamily),
    /// The logical/physical pair is not an ordinary selected executable
    /// alternative.
    Unsupported,
}

impl SelectedExecutableAlternativeClassification {
    pub(crate) fn into_result(
        self,
    ) -> Result<SelectedExecutableAlternativeFamily, super::SelectedAlternativeConstructionError>
    {
        match self {
            Self::Classified(family) => Ok(family),
            Self::Unsupported => {
                Err(super::SelectedAlternativeConstructionError::UnsupportedLogicalPhysicalPair)
            }
        }
    }
}

impl SelectedExecutableAlternativeFamily {
    /// Node access path implemented by a node physical access.
    pub(crate) const NODE_ACCESS_PATH: Self = Self::NodeAccessPath;
    /// Edge access path implemented by an edge physical access.
    pub(crate) const EDGE_ACCESS_PATH: Self = Self::EdgeAccessPath;
    /// Pure source implemented directly by an LSM KV read.
    pub(crate) const KV_SOURCE: Self = Self::KvSource;
    /// Proven no-op logical expression.
    pub(crate) const NO_OP: Self = Self::NoOp;
    /// Variable source injection.
    pub(crate) const VARIABLE_SOURCE: Self = Self::VariableSource;
    /// Access filter implemented by an access-prefixed physical pipeline.
    pub(crate) const ACCESS_FILTER_PIPELINE: Self = Self::AccessFilterPipeline;
    /// Access window implemented by an access-prefixed physical pipeline.
    pub(crate) const ACCESS_WINDOW_PIPELINE: Self = Self::AccessWindowPipeline;
    /// Access order implemented by an access-prefixed physical pipeline.
    pub(crate) const ACCESS_ORDER_PIPELINE: Self = Self::AccessOrderPipeline;
    /// Access distinct implemented by an access-prefixed physical pipeline.
    pub(crate) const ACCESS_DISTINCT_PIPELINE: Self = Self::AccessDistinctPipeline;
    /// General access-rooted physical pipeline.
    pub(crate) const ACCESS_PIPELINE: Self = Self::AccessPipeline;
}
