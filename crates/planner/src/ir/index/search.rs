use helix_ast::value::PropertyValue;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ir::{NonEmptyString, PropertyInputPlan};

/// Search index execution plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchIndexPlan {
    /// Index ID.
    pub index_id: NonEmptyString,
    /// Tenant-scoping behavior for the index.
    pub tenant: SearchTenantPlan,
}

/// Search index tenant scoping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchTenantPlan {
    /// Index is not tenant-scoped.
    Unscoped,
    /// Index is tenant-scoped by a property without a query-time tenant value.
    Scoped {
        /// Tenant property configured on the index.
        property: NonEmptyString,
    },
    /// Index is tenant-scoped by a property and constrained to a tenant value.
    ScopedValue {
        /// Tenant property configured on the index.
        property: NonEmptyString,
        /// Tenant value to bind at query time.
        value: SearchTenantValuePlan,
    },
}

/// Invalid search tenant value payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchTenantValuePlanError {
    /// Literal tenant value was `Null`, which represents no tenant value.
    NullLiteral,
}

/// Query-time tenant value for tenant-scoped search indexes.
///
/// Literal `Null` values are rejected because search tenants use `Null` as
/// absence rather than as a real partition key. Static constant expressions are
/// rejected the same way; runtime expressions remain valid because their value
/// is not known during planning.
///
/// ```
/// use helix_ast::expr::Expr;
/// use helix_ast::value::PropertyValue;
/// use helix_planner::ir::{
///     ExprPlan, PropertyInputPlan, SearchTenantValuePlan, SearchTenantValuePlanError,
/// };
///
/// assert!(SearchTenantValuePlan::new(PropertyInputPlan::Value(PropertyValue::from("acme")))
///     .is_ok());
/// assert_eq!(
///     SearchTenantValuePlan::new(PropertyInputPlan::Value(PropertyValue::Null)),
///     Err(SearchTenantValuePlanError::NullLiteral)
/// );
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct SearchTenantValuePlan {
    value: PropertyInputPlan,
}

impl SearchTenantValuePlan {
    /// Build a tenant value plan, rejecting literal `Null`.
    pub fn new(value: PropertyInputPlan) -> Result<Self, SearchTenantValuePlanError> {
        match value {
            PropertyInputPlan::Value(PropertyValue::Null) => {
                Err(SearchTenantValuePlanError::NullLiteral)
            }
            value => Ok(Self { value }),
        }
    }

    /// Borrow the validated tenant value input.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{PropertyInputPlan, SearchTenantValuePlan};
    ///
    /// let plan = SearchTenantValuePlan::new(PropertyInputPlan::Value(PropertyValue::from("acme")))
    ///     .unwrap();
    /// assert!(matches!(plan.value(), PropertyInputPlan::Value(PropertyValue::String(_))));
    /// ```
    pub fn value(&self) -> &PropertyInputPlan {
        &self.value
    }
}

impl Serialize for SearchTenantValuePlan {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SearchTenantValuePlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = PropertyInputPlan::deserialize(deserializer)?;
        Self::new(value).map_err(|_| D::Error::custom("expected non-null tenant value"))
    }
}
