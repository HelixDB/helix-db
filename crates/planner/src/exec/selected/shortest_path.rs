//! Selected shortest-path contracts.
//!
//! Shortest path is a root-only physical marker. This payload keeps it out of
//! ordinary selected alternatives so executable lowering can require the exact
//! selected root contract.

#[cfg(test)]
use super::provenance::test_selected_root_provenance;
use super::provenance::SelectedRootProvenance;
use super::SelectedPhysicalPlan;
use super::SelectedRootConstructionError;
use crate::{ir, physical};

/// Selected root shortest-path query.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRootShortestPath {
    /// Selected shortest-path implementation.
    alternative: SelectedPhysicalPlan,
    /// How this selected root was produced.
    provenance: SelectedRootProvenance,
    /// Validated shortest-path payload.
    plan: ir::ShortestPathPlan,
}

impl SelectedRootShortestPath {
    /// Build a selected root shortest-path query.
    pub fn new(
        alternative: SelectedPhysicalPlan,
        provenance: SelectedRootProvenance,
        plan: ir::ShortestPathPlan,
    ) -> Result<Self, SelectedRootConstructionError> {
        if !matches!(alternative.expr(), physical::PhysicalExpr::ShortestPath) {
            return Err(SelectedRootConstructionError::IncompatiblePhysicalShape);
        }
        Ok(Self {
            alternative,
            provenance,
            plan,
        })
    }

    /// Selected shortest-path implementation.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    /// How this selected root was produced.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }

    /// Validated shortest-path payload.
    pub const fn plan(&self) -> &ir::ShortestPathPlan {
        &self.plan
    }

    pub(in crate::exec::selected) fn into_parts(
        self,
    ) -> (
        SelectedPhysicalPlan,
        SelectedRootProvenance,
        ir::ShortestPathPlan,
    ) {
        (self.alternative, self.provenance, self.plan)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::{cost, properties};
    use helix_ast::graph::NodeRef;
    use helix_ast::traversal::ShortestPathDirection;

    fn selected_physical(expr: physical::PhysicalExpr) -> SelectedPhysicalPlan {
        SelectedPhysicalPlan::new(
            expr,
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        )
    }

    fn plan() -> ir::ShortestPathPlan {
        ir::ShortestPathPlan {
            source: NodeRef::id(1),
            target: NodeRef::id(2),
            label: None,
            direction: ShortestPathDirection::Out,
            max_depth: NonZeroUsize::new(2).unwrap(),
        }
    }

    #[test]
    fn root_shortest_path_constructor_preserves_contract_parts() {
        let alternative = selected_physical(physical::PhysicalExpr::ShortestPath);
        let provenance = test_selected_root_provenance();
        let plan = plan();

        let root =
            SelectedRootShortestPath::new(alternative.clone(), provenance.clone(), plan.clone())
                .unwrap();

        assert_eq!(root.alternative(), &alternative);
        assert_eq!(root.provenance(), &provenance);
        assert_eq!(root.plan(), &plan);
        assert_eq!(root.into_parts(), (alternative, provenance, plan));
    }

    #[test]
    fn root_shortest_path_constructor_rejects_other_physical_shapes() {
        assert_eq!(
            SelectedRootShortestPath::new(
                selected_physical(physical::PhysicalExpr::Barrier),
                test_selected_root_provenance(),
                plan(),
            ),
            Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
        );
    }
}
