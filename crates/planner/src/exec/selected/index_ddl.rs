//! Selected index-DDL contracts.
//!
//! Index DDL is a root-only barrier. This payload keeps DDL out of ordinary
//! selected alternatives so executable lowering can require the root-level
//! barrier contract explicitly.

#[cfg(test)]
use super::provenance::test_selected_root_provenance;
use super::provenance::SelectedRootProvenance;
use super::SelectedPhysicalPlan;
use super::SelectedRootConstructionError;
use crate::{ir, physical};

/// Selected root index-DDL barrier.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRootIndexDdl {
    /// Selected root index-DDL implementation.
    alternative: SelectedPhysicalPlan,
    /// How this selected root was produced.
    provenance: SelectedRootProvenance,
    /// Validated DDL payload.
    plan: ir::IndexDdlPlan,
}

impl SelectedRootIndexDdl {
    /// Build a selected root index-DDL barrier.
    pub fn new(
        alternative: SelectedPhysicalPlan,
        provenance: SelectedRootProvenance,
        plan: ir::IndexDdlPlan,
    ) -> Result<Self, SelectedRootConstructionError> {
        if !matches!(alternative.expr(), physical::PhysicalExpr::Barrier) {
            return Err(SelectedRootConstructionError::IncompatiblePhysicalShape);
        }
        Ok(Self {
            alternative,
            provenance,
            plan,
        })
    }

    /// Selected root index-DDL implementation.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    /// How this selected root was produced.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }

    /// Validated DDL payload.
    pub const fn plan(&self) -> &ir::IndexDdlPlan {
        &self.plan
    }

    pub(in crate::exec::selected) fn into_parts(
        self,
    ) -> (
        SelectedPhysicalPlan,
        SelectedRootProvenance,
        ir::IndexDdlPlan,
    ) {
        (self.alternative, self.provenance, self.plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog, cost, physical, properties};

    fn selected_physical() -> SelectedPhysicalPlan {
        SelectedPhysicalPlan::new(
            physical::PhysicalExpr::Barrier,
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        )
    }

    #[test]
    fn root_index_ddl_constructor_preserves_contract_parts() {
        let alternative = selected_physical();
        let provenance = test_selected_root_provenance();
        let plan = ir::IndexDdlPlan::Drop {
            spec: ir::IndexDdlDropSpec::NodeEquality {
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                uniqueness: catalog::IndexUniqueness::NonUnique,
            },
        };

        let root = SelectedRootIndexDdl::new(alternative.clone(), provenance.clone(), plan.clone())
            .unwrap();

        assert_eq!(root.alternative(), &alternative);
        assert_eq!(root.provenance(), &provenance);
        assert_eq!(root.plan(), &plan);
        assert_eq!(root.into_parts(), (alternative, provenance, plan));
    }

    #[test]
    fn root_index_ddl_constructor_rejects_non_barrier_physical_shape() {
        let alternative = SelectedPhysicalPlan::new(
            physical::PhysicalExpr::NoOp,
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        );

        assert_eq!(
            SelectedRootIndexDdl::new(
                alternative,
                test_selected_root_provenance(),
                ir::IndexDdlPlan::Drop {
                    spec: ir::IndexDdlDropSpec::NodeEquality {
                        key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                        uniqueness: catalog::IndexUniqueness::NonUnique,
                    },
                },
            ),
            Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
        );
    }
}
