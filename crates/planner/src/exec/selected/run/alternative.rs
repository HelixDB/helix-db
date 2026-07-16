//! Ordinary selected executable alternative root contract.

use super::super::family::SelectedExecutableAlternativeFamily;
use super::super::physical::SelectedPhysicalPlan;
use super::super::provenance::SelectedRootProvenance;
use super::super::SelectedAlternativeConstructionError;
use crate::{logical, physical};

/// Selected executable root around a physical alternative and its logical
/// contract.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedExecutableAlternativeRoot {
    /// Logical expression that produced the selected physical alternative.
    source_expr: logical::LogicalExpr,
    /// Classified ordinary executable family.
    family: SelectedExecutableAlternativeFamily,
    /// How this selected root was produced.
    provenance: SelectedRootProvenance,
    /// Selected physical implementation.
    alternative: SelectedPhysicalPlan,
}

impl SelectedExecutableAlternativeRoot {
    /// Build a selected ordinary alternative root after classifying its
    /// logical/physical pair.
    pub fn new(
        source_expr: logical::LogicalExpr,
        alternative: physical::PhysicalAlternative,
        provenance: SelectedRootProvenance,
    ) -> Result<Self, SelectedAlternativeConstructionError> {
        let family =
            SelectedExecutableAlternativeFamily::try_classify(&source_expr, &alternative.expr)?;
        Self::new_classified(source_expr, alternative, provenance, family)
    }

    pub(in crate::exec::selected) fn new_classified(
        source_expr: logical::LogicalExpr,
        alternative: physical::PhysicalAlternative,
        provenance: SelectedRootProvenance,
        family: SelectedExecutableAlternativeFamily,
    ) -> Result<Self, SelectedAlternativeConstructionError> {
        match SelectedExecutableAlternativeFamily::try_classify(&source_expr, &alternative.expr) {
            Ok(actual) if actual == family => Ok(Self {
                source_expr,
                family,
                provenance,
                alternative: alternative.into(),
            }),
            Ok(_) => Err(SelectedAlternativeConstructionError::ClassifiedFamilyMismatch),
            Err(error) => Err(error),
        }
    }

    /// Logical expression that produced the selected physical alternative.
    pub const fn source_expr(&self) -> &logical::LogicalExpr {
        &self.source_expr
    }

    /// Classified ordinary executable family.
    #[cfg(test)]
    pub(crate) const fn family(&self) -> SelectedExecutableAlternativeFamily {
        self.family
    }

    /// How this selected root was produced.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }

    /// Selected physical implementation.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    pub(in crate::exec::selected) fn into_parts(
        self,
    ) -> (
        logical::LogicalExpr,
        SelectedExecutableAlternativeFamily,
        SelectedRootProvenance,
        SelectedPhysicalPlan,
    ) {
        (
            self.source_expr,
            self.family,
            self.provenance,
            self.alternative,
        )
    }
}
