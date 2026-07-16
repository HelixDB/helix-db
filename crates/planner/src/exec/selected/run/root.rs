//! Recursive selected executable run-root ADT.

use super::super::control_flow::{SelectedRootBranch, SelectedRootRepeat};
use super::super::family::SelectedExecutableAlternativeFamily;
use super::super::index_ddl::SelectedRootIndexDdl;
use super::super::mutation::SelectedRootMutation;
#[cfg(test)]
use super::super::provenance::test_selected_root_provenance;
use super::super::provenance::SelectedRootProvenance;
use super::super::root_stream::{SelectedRootPipeline, SelectedRootTerminalPlan};
use super::super::shortest_path::SelectedRootShortestPath;
use super::super::SelectedAlternativeConstructionError;
use super::alternative::SelectedExecutableAlternativeRoot;
use crate::{logical, physical};

/// Root-level selected run. Recursive payloads use this ADT so selected
/// wrappers cannot accidentally contain unselected compatibility children.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedExecutableRunRoot {
    /// Ordinary selected physical alternative.
    Alternative(Box<SelectedExecutableAlternativeRoot>),
    /// Selected root mutation with selected child input payloads where needed.
    Mutation(Box<SelectedRootMutation>),
    /// Selected root index-DDL barrier.
    IndexDdl(Box<SelectedRootIndexDdl>),
    /// Selected root branch with selected input and branch payloads.
    Branch(Box<SelectedRootBranch>),
    /// Selected root repeat with selected input and body payloads.
    Repeat(Box<SelectedRootRepeat>),
    /// Selected root shortest-path query.
    ShortestPath(Box<SelectedRootShortestPath>),
    /// Selected root-stream pipeline with a selected input root.
    Pipeline(Box<SelectedRootPipeline>),
    /// Selected root-stream terminal with a selected input root.
    Terminal(Box<SelectedRootTerminalPlan>),
}

impl SelectedExecutableRunRoot {
    /// Build an ordinary selected physical alternative root.
    #[cfg(test)]
    pub fn alternative(
        source_expr: logical::LogicalExpr,
        alternative: physical::PhysicalAlternative,
    ) -> Self {
        Self::alternative_with_provenance(source_expr, alternative, test_selected_root_provenance())
    }

    /// Build an ordinary selected physical alternative root with provenance.
    #[cfg(test)]
    pub fn alternative_with_provenance(
        source_expr: logical::LogicalExpr,
        alternative: physical::PhysicalAlternative,
        provenance: SelectedRootProvenance,
    ) -> Self {
        Self::try_alternative_with_provenance(source_expr, alternative, provenance)
            .expect("selected executable alternative root must be an executable ordinary family")
    }

    /// Try to build an ordinary selected physical alternative root with
    /// provenance, rejecting unsupported logical/physical pairs.
    pub fn try_alternative_with_provenance(
        source_expr: logical::LogicalExpr,
        alternative: physical::PhysicalAlternative,
        provenance: SelectedRootProvenance,
    ) -> Result<Self, SelectedAlternativeConstructionError> {
        SelectedExecutableAlternativeRoot::new(source_expr, alternative, provenance)
            .map(Box::new)
            .map(Self::Alternative)
    }

    pub(crate) fn classified_alternative_with_provenance(
        source_expr: logical::LogicalExpr,
        alternative: physical::PhysicalAlternative,
        provenance: SelectedRootProvenance,
        family: SelectedExecutableAlternativeFamily,
    ) -> Result<Self, SelectedAlternativeConstructionError> {
        SelectedExecutableAlternativeRoot::new_classified(
            source_expr,
            alternative,
            provenance,
            family,
        )
        .map(Box::new)
        .map(Self::Alternative)
    }
}
