//! Optimizer result and physical selection contract.
//!
//! The result facade owns the serializable optimization envelope. Child modules
//! own guardrail ADTs, retained physical-alternative records, and recursive
//! best-plan selection independently.

mod alternatives;
mod guardrail;
mod index;
mod selection;

pub(super) use self::alternatives::PendingPhysicalAlternative;
pub use self::alternatives::{
    GroupAlternatives, PhysicalAlternativeEntry, SelectedPhysicalAlternative,
};
pub use self::guardrail::OptimizerGuardrail;
pub use self::selection::{
    RootSelectionFailure, RootSelectionSummary, SelectionError, SelectionSession,
};

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{cost, exec, ir, memo};

/// Result of Cascades exploration.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OptimizationResult {
    pub(super) memo: memo::Memo,
    pub(super) root: memo::MemoGroupId,
    pub(super) roots: ir::AtLeast<memo::MemoGroupId, 1>,
    pub(super) physical: Vec<GroupAlternatives>,
    #[serde(skip)]
    physical_index: index::PhysicalAlternativeIndex,
    pub(super) metrics: exec::PlannerMetrics,
    guardrail: Option<OptimizerGuardrail>,
}

impl OptimizationResult {
    pub(super) fn new(
        memo: memo::Memo,
        root: memo::MemoGroupId,
        roots: ir::AtLeast<memo::MemoGroupId, 1>,
        physical: std::collections::BTreeMap<memo::MemoGroupId, Vec<PendingPhysicalAlternative>>,
        metrics: exec::PlannerMetrics,
        guardrail: Option<OptimizerGuardrail>,
    ) -> Self {
        let physical = alternatives::group_alternatives(physical);
        let mut result = Self {
            memo,
            root,
            roots,
            physical: physical.groups,
            physical_index: physical.index,
            metrics,
            guardrail,
        };
        result.metrics.selected_cost = result
            .root_selection_summary()
            .complete_selected_cost()
            .unwrap_or(cost::CostVector::ZERO);
        result
    }

    /// Memo produced by exploration.
    pub const fn memo(&self) -> &memo::Memo {
        &self.memo
    }

    /// Root memo group.
    pub const fn root(&self) -> memo::MemoGroupId {
        self.root
    }

    /// All independently seeded root memo groups.
    pub const fn roots(&self) -> &ir::AtLeast<memo::MemoGroupId, 1> {
        &self.roots
    }

    /// Physical alternatives by group.
    pub fn physical(&self) -> &[GroupAlternatives] {
        &self.physical
    }

    /// Planner metrics.
    pub const fn metrics(&self) -> &exec::PlannerMetrics {
        &self.metrics
    }

    /// Guardrail that stopped exploration, if any.
    pub const fn guardrail(&self) -> Option<OptimizerGuardrail> {
        self.guardrail
    }

    pub(super) fn physical_for_group(
        &self,
        group: memo::MemoGroupId,
    ) -> Option<&GroupAlternatives> {
        self.physical_index.group(&self.physical, group)
    }
}

impl<'de> Deserialize<'de> for OptimizationResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawOptimizationResult {
            memo: memo::Memo,
            root: memo::MemoGroupId,
            roots: ir::AtLeast<memo::MemoGroupId, 1>,
            physical: Vec<GroupAlternatives>,
            metrics: exec::PlannerMetrics,
            guardrail: Option<OptimizerGuardrail>,
        }

        let raw = RawOptimizationResult::deserialize(deserializer)?;
        let physical_index = index::PhysicalAlternativeIndex::from_groups(&raw.physical)
            .map_err(D::Error::custom)?;
        Ok(Self {
            memo: raw.memo,
            root: raw.root,
            roots: raw.roots,
            physical: raw.physical,
            physical_index,
            metrics: raw.metrics,
            guardrail: raw.guardrail,
        })
    }
}
