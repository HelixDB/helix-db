use serde::{Deserialize, Serialize};

use super::{RuleApplicability, RuleId, RuleKind};
use crate::properties;

/// Optimizer rule metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleMetadata {
    /// Stable rule ID.
    pub id: RuleId,
    /// Rule phase.
    pub kind: RuleKind,
    /// Top-level logical expression families this rule can match.
    pub applicability: RuleApplicability,
    /// Required input properties for this rule to be semantically valid.
    pub required: properties::RequiredProperties,
    /// Delivered properties when this rule has a known property contract.
    pub delivered: Option<properties::DeliveredProperties>,
}

impl RuleMetadata {
    /// Build rule metadata.
    pub fn new(id: RuleId, kind: RuleKind) -> Self {
        let applicability = id
            .known_inventory()
            .map(RuleApplicability::for_known_rule)
            .unwrap_or_default();
        Self {
            id,
            kind,
            applicability,
            required: properties::RequiredProperties::default(),
            delivered: None,
        }
    }

    /// Override top-level logical expression applicability.
    pub fn with_applicability(mut self, applicability: RuleApplicability) -> Self {
        self.applicability = applicability;
        self
    }

    /// Attach required input properties.
    pub fn with_required(mut self, required: properties::RequiredProperties) -> Self {
        self.required = required;
        self
    }

    /// Attach delivered output properties.
    pub fn with_delivered(mut self, delivered: properties::DeliveredProperties) -> Self {
        self.delivered = Some(delivered);
        self
    }
}
