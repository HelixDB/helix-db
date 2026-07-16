//! Row-binding projection contracts.

use std::collections::BTreeSet;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::super::{AtLeast, NonEmptyString};
use super::item::ProjectionItemsError;

/// Target for row-binding projections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingTargetPlan {
    /// Current traverser element.
    Current,
    /// Named row binding.
    Binding(NonEmptyString),
}

/// Reference used by row-binding projections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingValueRefPlan {
    /// Current or named binding.
    pub target: BindingTargetPlan,
    /// Source property or virtual field.
    pub source: NonEmptyString,
}

/// Projection from row-local bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingProjectionPlan {
    /// Project a single property.
    Property {
        /// Current or named binding.
        target: BindingTargetPlan,
        /// Source property or virtual field.
        source: NonEmptyString,
        /// Output field name.
        alias: NonEmptyString,
    },
    /// Project the first present non-null reference.
    Coalesce {
        /// Candidate references.
        refs: AtLeast<BindingValueRefPlan, 1>,
        /// Output field name.
        alias: NonEmptyString,
    },
}

/// Non-empty binding projection list with unique output aliases.
///
/// ```
/// use helix_planner::ir::{
///     BindingProjectionItems, BindingProjectionPlan, BindingTargetPlan, AtLeast,
///     NonEmptyString, ProjectionItemsError,
/// };
///
/// let alias = NonEmptyString::new("name").unwrap();
/// let projections = BindingProjectionItems::new(AtLeast::<_, 1>::from_one(
///     BindingProjectionPlan::Property {
///         target: BindingTargetPlan::Current,
///         source: alias.clone(),
///         alias: alias.clone(),
///     },
/// ))
/// .unwrap();
/// assert_eq!(serde_json::to_string(&projections).unwrap(), r#"[{"property":{"target":"current","source":"name","alias":"name"}}]"#);
///
/// assert!(matches!(
///     BindingProjectionItems::new(AtLeast::<_, 1>::from_one_and_rest(
///         BindingProjectionPlan::Property {
///             target: BindingTargetPlan::Current,
///             source: alias.clone(),
///             alias: alias.clone(),
///         },
///         vec![BindingProjectionPlan::Property {
///             target: BindingTargetPlan::Current,
///             source: alias.clone(),
///             alias,
///         }],
///     )),
///     Err(ProjectionItemsError::DuplicateAlias { .. })
/// ));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingProjectionItems {
    items: AtLeast<BindingProjectionPlan, 1>,
}

impl BindingProjectionItems {
    /// Build a binding projection list, returning an error for duplicate output aliases.
    pub fn new(items: AtLeast<BindingProjectionPlan, 1>) -> Result<Self, ProjectionItemsError> {
        let mut aliases = BTreeSet::new();
        for item in &items {
            let alias = match item {
                BindingProjectionPlan::Property { alias, .. }
                | BindingProjectionPlan::Coalesce { alias, .. } => alias,
            };
            if !aliases.insert(alias.clone()) {
                return Err(ProjectionItemsError::DuplicateAlias {
                    alias: alias.clone(),
                });
            }
        }
        Ok(Self { items })
    }
}

impl AsRef<[BindingProjectionPlan]> for BindingProjectionItems {
    fn as_ref(&self) -> &[BindingProjectionPlan] {
        self.items.as_ref()
    }
}

impl Serialize for BindingProjectionItems {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.items.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BindingProjectionItems {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let items = AtLeast::<BindingProjectionPlan, 1>::deserialize(deserializer)?;
        Self::new(items).map_err(|err| match err {
            ProjectionItemsError::DuplicateAlias { alias } => {
                D::Error::custom(format!("duplicate projection alias `{alias}`"))
            }
        })
    }
}
