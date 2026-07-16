use serde::{Deserialize, Serialize};

use super::{
    CardinalityBounds, DeliveredOrdering, EffectKind, ElementKind, KeyLocality, Materialization,
    RequiredOrdering,
};

/// Physical properties required from a memo group.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RequiredProperties {
    /// Required element kind, if known by the consumer.
    pub element: Option<ElementKind>,
    /// Required row ordering.
    pub ordering: RequiredOrdering,
}

/// Physical properties delivered by an expression or executable step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveredProperties {
    /// Delivered element kind, if this stream contains graph elements.
    pub element: Option<ElementKind>,
    /// Delivered row ordering.
    pub ordering: DeliveredOrdering,
    /// Cardinality bounds.
    pub cardinality: CardinalityBounds,
    /// Materialization behavior.
    pub materialization: Materialization,
    /// Side-effect behavior.
    pub effect: EffectKind,
    /// Key locality advertised for downstream KV planning.
    pub key_locality: KeyLocality,
}

impl DeliveredProperties {
    /// Build conservative unknown pure streaming properties.
    pub const fn unknown() -> Self {
        Self {
            element: None,
            ordering: DeliveredOrdering::Unordered,
            cardinality: CardinalityBounds::unknown(),
            materialization: Materialization::Streaming,
            effect: EffectKind::Pure,
            key_locality: KeyLocality::Unknown,
        }
    }

    /// True when delivered properties satisfy required properties.
    pub fn satisfies(&self, required: &RequiredProperties) -> bool {
        required
            .element
            .is_none_or(|element| self.element == Some(element))
            && self.ordering.satisfies(&required.ordering)
    }
}

impl Default for DeliveredProperties {
    fn default() -> Self {
        Self::unknown()
    }
}
