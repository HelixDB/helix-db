use helix_ast::traversal::Order;
use serde::{Deserialize, Serialize};

use crate::ir;

/// Required ordering.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredOrdering {
    /// Any order is acceptable.
    #[default]
    Any,
    /// A specific non-empty key order is required.
    ByKeys(ir::OrderKeys),
}

/// Delivered ordering.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveredOrdering {
    /// No stable ordering contract.
    #[default]
    Unordered,
    /// Ordered by a non-empty set of keys.
    ByKeys(ir::OrderKeys),
}

impl DeliveredOrdering {
    /// Returns true when this delivered order satisfies a requested order.
    pub fn satisfies(&self, required: &RequiredOrdering) -> bool {
        match (self, required) {
            (_, RequiredOrdering::Any) => true,
            (Self::ByKeys(delivered), RequiredOrdering::ByKeys(required)) => {
                delivered.as_ref().starts_with(required.as_ref())
            }
            (Self::Unordered, RequiredOrdering::ByKeys(_)) => false,
        }
    }
}

/// One ordering key used by logical/physical property derivation before an IR
/// order plan is chosen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyOrderKey {
    /// Property name.
    pub property: ir::NonEmptyString,
    /// Direction.
    pub order: Order,
}
