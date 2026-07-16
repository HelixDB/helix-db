use serde::{Deserialize, Serialize};

use crate::expr::Expr;
/// A property projection with optional rename.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PropertyProjection {
    /// Source property.
    pub source: String,
    /// Output name.
    pub alias: String,
}

impl PropertyProjection {
    /// Project without rename.
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        Self {
            source: name.clone(),
            alias: name,
        }
    }

    /// Project with rename.
    pub fn renamed(source: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            alias: alias.into(),
        }
    }
}

/// Expression-backed projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExprProjection {
    /// Output name.
    pub alias: String,
    /// Expression.
    pub expr: Expr,
}

impl ExprProjection {
    /// Create an expression projection.
    pub fn new(alias: impl Into<String>, expr: Expr) -> Self {
        Self {
            alias: alias.into(),
            expr,
        }
    }
}

/// Projection entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Projection {
    /// Property projection.
    Property(PropertyProjection),
    /// Expression projection.
    Expr(ExprProjection),
}

impl Projection {
    /// Project a property.
    pub fn property(source: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::Property(PropertyProjection::renamed(source, alias))
    }

    /// Project from the source endpoint of an edge.
    pub fn from_endpoint(source: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::property(format!("$from.{}", source.into()), alias)
    }

    /// Project from the target endpoint of an edge.
    pub fn to_endpoint(source: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::property(format!("$to.{}", source.into()), alias)
    }

    /// Project an expression.
    pub fn expr(alias: impl Into<String>, expr: Expr) -> Self {
        Self::Expr(ExprProjection::new(alias, expr))
    }
}

impl From<PropertyProjection> for Projection {
    fn from(value: PropertyProjection) -> Self {
        Self::Property(value)
    }
}

impl From<ExprProjection> for Projection {
    fn from(value: ExprProjection) -> Self {
        Self::Expr(value)
    }
}

/// Target for row-binding projections.
///
/// ```
/// use helix_ast::projection::BindingTarget;
///
/// assert_eq!(sonic_rs::to_string(&BindingTarget::current()).unwrap(), r#""current""#);
/// assert_eq!(
///     sonic_rs::to_string(&BindingTarget::binding("service")).unwrap(),
///     r#"{"binding":"service"}"#
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingTarget {
    /// Current traverser element.
    Current,
    /// Named row binding.
    Binding(String),
}

impl BindingTarget {
    /// Current traverser.
    pub fn current() -> Self {
        Self::Current
    }

    /// Named row binding.
    pub fn binding(name: impl Into<String>) -> Self {
        Self::Binding(non_empty_string(name, "binding name"))
    }
}

/// Reference used by binding projections.
///
/// ```
/// use helix_ast::projection::{BindingTarget, BindingValueRef};
///
/// let value_ref = BindingValueRef::new(BindingTarget::binding("service"), "$id");
/// assert_eq!(
///     sonic_rs::to_string(&value_ref).unwrap(),
///     r#"{"target":{"binding":"service"},"source":"$id"}"#
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingValueRef {
    /// Target element.
    pub target: BindingTarget,
    /// Property or virtual field.
    pub source: String,
}

impl BindingValueRef {
    /// Create a reference.
    pub fn new(target: BindingTarget, source: impl Into<String>) -> Self {
        Self {
            target,
            source: non_empty_string(source, "binding projection source"),
        }
    }

    /// Reference current traverser.
    pub fn current(source: impl Into<String>) -> Self {
        Self::new(BindingTarget::Current, source)
    }

    /// Reference named binding.
    pub fn binding(name: impl Into<String>, source: impl Into<String>) -> Self {
        Self::new(BindingTarget::binding(name), source)
    }
}

/// Projection from row-local bindings.
///
/// ```
/// use helix_ast::projection::{BindingProjection, BindingValueRef};
///
/// let projection = BindingProjection::coalesce(
///     vec![
///         BindingValueRef::binding("deployment", "$id"),
///         BindingValueRef::binding("owner", "$id"),
///     ],
///     "workload_id",
/// );
/// assert_eq!(
///     sonic_rs::to_string(&projection).unwrap(),
///     r#"{"coalesce":{"refs":[{"target":{"binding":"deployment"},"source":"$id"},{"target":{"binding":"owner"},"source":"$id"}],"alias":"workload_id"}}"#
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingProjection {
    /// Project a single property.
    Property {
        /// Current or named binding.
        target: BindingTarget,
        /// Source property.
        source: String,
        /// Output name.
        alias: String,
    },
    /// Project first present non-null reference.
    Coalesce {
        /// Candidate references.
        refs: Vec<BindingValueRef>,
        /// Output name.
        alias: String,
    },
}

impl BindingProjection {
    /// Project a property.
    pub fn property(
        target: BindingTarget,
        source: impl Into<String>,
        alias: impl Into<String>,
    ) -> Self {
        Self::Property {
            target,
            source: non_empty_string(source, "binding projection source"),
            alias: non_empty_string(alias, "binding projection alias"),
        }
    }

    /// Project from current traverser.
    pub fn current(source: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::property(BindingTarget::Current, source, alias)
    }

    /// Project from named binding.
    pub fn binding(
        name: impl Into<String>,
        source: impl Into<String>,
        alias: impl Into<String>,
    ) -> Self {
        Self::property(BindingTarget::binding(name), source, alias)
    }

    /// Project first present non-null reference.
    pub fn coalesce(refs: Vec<BindingValueRef>, alias: impl Into<String>) -> Self {
        assert!(!refs.is_empty(), "binding coalesce refs must not be empty");
        Self::Coalesce {
            refs,
            alias: non_empty_string(alias, "binding projection alias"),
        }
    }
}

pub(crate) fn validate_binding_name(name: impl Into<String>) -> String {
    non_empty_string(name, "binding name")
}

pub(crate) fn validate_binding_projections(
    projections: Vec<BindingProjection>,
) -> Vec<BindingProjection> {
    assert!(
        !projections.is_empty(),
        "binding projections must not be empty"
    );
    projections
}

fn non_empty_string(value: impl Into<String>, field: &str) -> String {
    let value = value.into();
    assert!(!value.is_empty(), "{field} must not be empty");
    value
}
