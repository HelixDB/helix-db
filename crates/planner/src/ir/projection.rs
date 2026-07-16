//! Projection and aggregate IR contract facade.
//!
//! Projection IR is split by invariant boundary: terminal projection shape,
//! property-list uniqueness, general projection aliases, binding projections,
//! and aggregate payloads. The stable public `ir::*` exports are preserved
//! through this facade.

mod aggregate;
mod binding;
mod item;
mod plan;
mod property;

pub use self::{
    aggregate::AggregatePlan,
    binding::{
        BindingProjectionItems, BindingProjectionPlan, BindingTargetPlan, BindingValueRefPlan,
    },
    item::{ProjectionItem, ProjectionItems, ProjectionItemsError},
    plan::{ProjectionDedupMode, ProjectionPlan},
    property::{PropertyNames, PropertyNamesError, PropertySelection},
};
