use serde::{Deserialize, Serialize};

use super::PhysicalExpr;
use crate::{cost, digest, properties};

/// Costed physical alternative.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhysicalAlternative {
    /// Expression.
    pub expr: PhysicalExpr,
    /// Delivered properties.
    pub delivered: properties::DeliveredProperties,
    /// Estimated cost.
    pub cost: cost::CostVector,
    /// [`Self::digest`], once computed. Most alternatives never tie on cost,
    /// so the digest is only computed when two do.
    #[serde(skip)]
    tie_break: std::sync::OnceLock<digest::PlanDigest>,
}

impl PhysicalAlternative {
    /// Build a physical alternative.
    pub fn new(
        expr: PhysicalExpr,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
    ) -> Self {
        Self {
            expr,
            delivered,
            cost,
            tie_break: std::sync::OnceLock::new(),
        }
    }

    /// Stable digest of the expression and delivered properties, used as the
    /// final deterministic tie-breaker between equally costed alternatives.
    ///
    /// ```
    /// use helix_planner::{cost, physical, properties};
    ///
    /// let alternative = |cost| {
    ///     physical::PhysicalAlternative::new(
    ///         physical::PhysicalExpr::Sort,
    ///         properties::DeliveredProperties::default(),
    ///         cost,
    ///     )
    /// };
    /// let unit = cost::CostVector {
    ///     cpu_units: 1,
    ///     ..cost::CostVector::ZERO
    /// };
    ///
    /// assert_eq!(alternative(cost::CostVector::ZERO).digest(), alternative(unit).digest());
    /// ```
    pub fn digest(&self) -> digest::PlanDigest {
        *self.tie_break.get_or_init(|| {
            digest::PlanDigest::for_tie_break(
                "physical_alternative:v1",
                &(&self.expr, &self.delivered),
            )
        })
    }
}

/// The digest is a function of the expression and delivered properties, so it
/// takes no part in equality whether or not it has been computed.
impl PartialEq for PhysicalAlternative {
    fn eq(&self, other: &Self) -> bool {
        self.expr == other.expr && self.delivered == other.delivered && self.cost == other.cost
    }
}
