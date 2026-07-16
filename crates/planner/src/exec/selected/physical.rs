//! Selected physical implementation contract.
//!
//! Optimizer alternatives carry digest metadata used for deterministic
//! selection. Once an alternative has been selected, executable lowering only
//! needs the physical expression, delivered properties, and cost. This wrapper
//! keeps that smaller contract explicit at the selected executable IR boundary.

use crate::{cost, physical, properties};

/// Physical implementation data retained after optimizer selection.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedPhysicalPlan {
    /// Selected physical expression.
    expr: physical::PhysicalExpr,
    /// Delivered properties of the selected expression.
    delivered: properties::DeliveredProperties,
    /// Estimated selected cost.
    cost: cost::CostVector,
}

impl SelectedPhysicalPlan {
    /// Build selected physical implementation data directly.
    pub const fn new(
        expr: physical::PhysicalExpr,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
    ) -> Self {
        Self {
            expr,
            delivered,
            cost,
        }
    }

    /// Selected physical expression.
    pub const fn expr(&self) -> &physical::PhysicalExpr {
        &self.expr
    }

    /// Delivered properties of the selected expression.
    pub const fn delivered(&self) -> &properties::DeliveredProperties {
        &self.delivered
    }

    /// Estimated selected cost.
    pub const fn cost(&self) -> cost::CostVector {
        self.cost
    }

    /// Clone the executable step contract carried by this selected plan.
    pub fn clone_contract(&self) -> (properties::DeliveredProperties, cost::CostVector) {
        (self.delivered.clone(), self.cost)
    }

    /// Decompose selected physical implementation data.
    pub fn into_parts(
        self,
    ) -> (
        physical::PhysicalExpr,
        properties::DeliveredProperties,
        cost::CostVector,
    ) {
        (self.expr, self.delivered, self.cost)
    }
}

impl From<physical::PhysicalAlternative> for SelectedPhysicalPlan {
    fn from(alternative: physical::PhysicalAlternative) -> Self {
        Self {
            expr: alternative.expr,
            delivered: alternative.delivered,
            cost: alternative.cost,
        }
    }
}

impl From<&physical::PhysicalAlternative> for SelectedPhysicalPlan {
    fn from(alternative: &physical::PhysicalAlternative) -> Self {
        Self {
            expr: alternative.expr.clone(),
            delivered: alternative.delivered.clone(),
            cost: alternative.cost,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cost, physical, properties};

    #[test]
    fn selected_physical_plan_drops_optimizer_digest_metadata() {
        let alternative = physical::PhysicalAlternative::new(
            physical::PhysicalExpr::NoOp,
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        );
        let selected = SelectedPhysicalPlan::from(alternative.clone());

        assert_eq!(selected.expr(), &alternative.expr);
        assert_eq!(selected.delivered(), &alternative.delivered);
        assert_eq!(selected.cost(), alternative.cost);
        assert_eq!(
            selected.clone_contract(),
            (alternative.delivered.clone(), alternative.cost)
        );
        assert_eq!(
            selected.into_parts(),
            (alternative.expr, alternative.delivered, alternative.cost)
        );
    }
}
