use serde::{Deserialize, Serialize};

use super::PhysicalExpr;
use crate::{cost, digest, properties};

/// Costed physical alternative.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhysicalAlternative {
    /// Expression.
    pub expr: PhysicalExpr,
    /// Delivered properties.
    pub delivered: properties::DeliveredProperties,
    /// Estimated cost.
    pub cost: cost::CostVector,
    /// Stable digest used as the final deterministic tie-breaker.
    pub digest: digest::PlanDigest,
}

impl PhysicalAlternative {
    /// Build a physical alternative.
    pub fn new(
        expr: PhysicalExpr,
        delivered: properties::DeliveredProperties,
        cost: cost::CostVector,
    ) -> Self {
        let digest =
            digest::PlanDigest::for_tagged_value("physical_alternative:v1", &(&expr, &delivered));
        Self {
            expr,
            delivered,
            cost,
            digest,
        }
    }
}
