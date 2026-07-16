//! Shared logical, physical, and executable property contracts.
//!
//! The public `properties::*` surface is intentionally stable, while the
//! implementation is split by invariant family so ordering, cardinality, and
//! delivered-property logic can be tested independently.

mod cardinality;
mod delivered;
mod kinds;
mod ordering;
mod positive;

pub use self::{
    cardinality::CardinalityBounds,
    delivered::{DeliveredProperties, RequiredProperties},
    kinds::{EffectKind, ElementKind, KeyLocality, Materialization},
    ordering::{DeliveredOrdering, PropertyOrderKey, RequiredOrdering},
    positive::PositiveUsize,
};

#[cfg(test)]
mod tests;
