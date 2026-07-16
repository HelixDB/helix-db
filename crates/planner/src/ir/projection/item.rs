//! General projection item contracts.

use std::collections::BTreeSet;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::super::{AtLeast, ExprPlan, NonEmptyString};

/// One general projection item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionItem {
    /// Property projection with an output alias.
    Property {
        /// Source property or virtual field.
        source: NonEmptyString,
        /// Output field name.
        alias: NonEmptyString,
    },
    /// Expression projection with an output alias.
    Expr {
        /// Output field name.
        alias: NonEmptyString,
        /// Expression payload.
        expr: ExprPlan,
    },
}

/// Invalid projection-list payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionItemsError {
    /// More than one projection writes the same output alias.
    DuplicateAlias {
        /// Duplicate output alias.
        alias: NonEmptyString,
    },
}

/// Non-empty projection list with unique output aliases.
///
/// ```
/// use helix_ast::expr::Expr;
/// use helix_planner::ir::{
///     ExprPlan, AtLeast, NonEmptyString, ProjectionItem, ProjectionItems,
///     ProjectionItemsError,
/// };
///
/// let name = NonEmptyString::new("name").unwrap();
/// let projection = ProjectionItems::new(AtLeast::<_, 1>::from_one(ProjectionItem::Property {
///     source: name.clone(),
///     alias: name,
/// }))
/// .unwrap();
///
/// assert_eq!(serde_json::to_string(&projection).unwrap(), r#"[{"property":{"source":"name","alias":"name"}}]"#);
/// assert!(matches!(
///     ProjectionItems::new(AtLeast::<_, 1>::from_one_and_rest(
///         ProjectionItem::Expr {
///             alias: NonEmptyString::new("value").unwrap(),
///             expr: ExprPlan::new(Expr::val(1)).unwrap(),
///         },
///         vec![ProjectionItem::Expr {
///             alias: NonEmptyString::new("value").unwrap(),
///             expr: ExprPlan::new(Expr::val(2)).unwrap(),
///         }],
///     )),
///     Err(ProjectionItemsError::DuplicateAlias { .. })
/// ));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionItems {
    items: AtLeast<ProjectionItem, 1>,
}

impl ProjectionItems {
    /// Build a projection list, returning an error for duplicate output aliases.
    pub fn new(items: AtLeast<ProjectionItem, 1>) -> Result<Self, ProjectionItemsError> {
        let mut aliases = BTreeSet::new();
        for item in &items {
            let alias = match item {
                ProjectionItem::Property { alias, .. } | ProjectionItem::Expr { alias, .. } => {
                    alias
                }
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

impl AsRef<[ProjectionItem]> for ProjectionItems {
    fn as_ref(&self) -> &[ProjectionItem] {
        self.items.as_ref()
    }
}

impl Serialize for ProjectionItems {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.items.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProjectionItems {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let items = AtLeast::<ProjectionItem, 1>::deserialize(deserializer)?;
        Self::new(items).map_err(|err| match err {
            ProjectionItemsError::DuplicateAlias { alias } => {
                D::Error::custom(format!("duplicate projection alias `{alias}`"))
            }
        })
    }
}
