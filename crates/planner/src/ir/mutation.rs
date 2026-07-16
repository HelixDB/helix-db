//! Mutation executable-IR contract ADTs.
//!
//! These contracts encode mutation-specific invariants separately from access,
//! index, and expression planning. Public names are re-exported through
//! [`crate::ir`] so callers can keep using the stable `ir::Type` surface.

use std::collections::BTreeSet;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{ElementIds, NonEmptyString, PhysicalOp, PropertyInputPlan};

/// Invalid mutation property assignment payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyAssignmentsError {
    /// The same property was assigned more than once.
    DuplicateProperty {
        /// Duplicate property name.
        property: NonEmptyString,
    },
}

impl std::fmt::Display for PropertyAssignmentsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateProperty { property } => {
                write!(f, "duplicate property assignment `{property}`")
            }
        }
    }
}

/// Mutation property assignments with unique property names.
///
/// Empty assignments are valid for node/edge creation, but assigning the same
/// property twice is ambiguous and rejected.
///
/// ```
/// use helix_ast::value::PropertyValue;
/// use helix_planner::ir::{NonEmptyString, PropertyAssignments, PropertyInputPlan};
///
/// let name = NonEmptyString::new("name").unwrap();
/// let value = PropertyInputPlan::Value(PropertyValue::from("alice"));
///
/// assert!(PropertyAssignments::try_from_vec(vec![(name.clone(), value.clone())]).is_ok());
/// assert!(PropertyAssignments::try_from_vec(vec![(name.clone(), value.clone()), (name, value)]).is_err());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PropertyAssignments {
    items: Vec<(NonEmptyString, PropertyInputPlan)>,
}

impl PropertyAssignments {
    /// Build a possibly empty property assignment list, rejecting duplicate
    /// property names.
    pub fn try_from_vec(
        items: Vec<(NonEmptyString, PropertyInputPlan)>,
    ) -> Result<Self, PropertyAssignmentsError> {
        let mut seen = BTreeSet::new();
        if let Some(property) = items
            .iter()
            .map(|(property, _value)| property)
            .find(|property| !seen.insert((*property).clone()))
        {
            return Err(PropertyAssignmentsError::DuplicateProperty {
                property: (*property).clone(),
            });
        }

        Ok(Self { items })
    }
}

impl AsRef<[(NonEmptyString, PropertyInputPlan)]> for PropertyAssignments {
    fn as_ref(&self) -> &[(NonEmptyString, PropertyInputPlan)] {
        &self.items
    }
}

impl IntoIterator for PropertyAssignments {
    type Item = (NonEmptyString, PropertyInputPlan);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a> IntoIterator for &'a PropertyAssignments {
    type Item = &'a (NonEmptyString, PropertyInputPlan);
    type IntoIter = std::slice::Iter<'a, (NonEmptyString, PropertyInputPlan)>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl Serialize for PropertyAssignments {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.items.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PropertyAssignments {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let items = Vec::<(NonEmptyString, PropertyInputPlan)>::deserialize(deserializer)?;
        Self::try_from_vec(items).map_err(D::Error::custom)
    }
}

/// Mutation plan.
///
/// The child payload defaults to [`PhysicalOp`] for executable physical plans.
/// Logical optimizer roots specialize it to logical children so input-consuming
/// mutations cannot hide unselectable compatibility subtrees.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationPlan<T = PhysicalOp> {
    /// Add node.
    AddNode {
        /// Source or input-stream mode.
        input: MutationInput<T>,
        /// Label.
        label: NonEmptyString,
        /// Properties.
        properties: PropertyAssignments,
    },
    /// Add edge.
    AddEdge {
        /// Input plan.
        input: Box<T>,
        /// Label.
        label: NonEmptyString,
        /// Target nodes.
        to: NodeTargetPlan,
        /// Properties.
        properties: PropertyAssignments,
    },
    /// Set property.
    SetProperty {
        /// Input plan.
        input: Box<T>,
        /// Name.
        name: NonEmptyString,
        /// Value.
        value: PropertyInputPlan,
    },
    /// Remove property.
    RemoveProperty {
        /// Input plan.
        input: Box<T>,
        /// Name.
        name: NonEmptyString,
    },
    /// Drop nodes.
    Drop {
        /// Input plan.
        input: Box<T>,
    },
    /// Drop edges.
    DropEdge {
        /// Input plan.
        input: Box<T>,
        /// Target nodes.
        to: NodeTargetPlan,
    },
    /// Drop labeled edges.
    DropEdgeLabeled {
        /// Input plan.
        input: Box<T>,
        /// Target nodes.
        to: NodeTargetPlan,
        /// Label.
        label: NonEmptyString,
    },
    /// Drop edges by ID.
    DropEdgeById {
        /// Source or input-stream mode.
        input: MutationInput<T>,
        /// Edge reference.
        edges: EdgeTargetPlan,
    },
}

/// Node target reference used by mutations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeTargetPlan {
    /// All nodes.
    All,
    /// Known empty node target.
    Empty,
    /// Concrete node IDs.
    PointIds {
        /// Non-empty concrete IDs.
        ids: ElementIds,
    },
    /// Runtime parameter IDs.
    FromParam { param: NonEmptyString },
    /// Variable node set.
    FromVar { variable: NonEmptyString },
}

/// Edge target reference used by mutations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeTargetPlan {
    /// Known empty edge target.
    Empty,
    /// Concrete edge IDs.
    PointIds {
        /// Non-empty concrete IDs.
        ids: ElementIds,
    },
    /// Runtime parameter IDs.
    FromParam { param: NonEmptyString },
    /// Variable edge set.
    FromVar { variable: NonEmptyString },
}

/// Mutation input mode.
///
/// Some mutations can either create a source operation or run after an existing
/// input stream. Encoding that mode avoids using `None` as a hidden state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationInput<T = PhysicalOp> {
    /// Mutation is a source operation.
    Source,
    /// Mutation consumes a prior input stream.
    FromInput {
        /// Input plan.
        input: Box<T>,
    },
}
