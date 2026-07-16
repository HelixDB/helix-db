//! Aggregate terminal payload contracts.

use helix_ast::traversal::AggregateFunction;
use serde::{Deserialize, Serialize};

use super::super::NonEmptyString;

/// Aggregate plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregatePlan {
    /// Group by property.
    Group(NonEmptyString),
    /// Group count by property.
    GroupCount(NonEmptyString),
    /// Aggregate by function/property.
    AggregateBy {
        /// Function.
        function: AggregateFunction,
        /// Property.
        property: NonEmptyString,
    },
}
