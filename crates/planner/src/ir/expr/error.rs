//! Expression validation error contracts.

/// Planner field whose name must be non-empty at the IR boundary.
///
/// # Examples
///
/// ```
/// use helix_planner::ir::NameField;
///
/// assert_eq!(NameField::TenantProperty.to_string(), "tenant_property");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NameField {
    /// Projection alias.
    Alias,
    /// Binding name.
    Binding,
    /// Label name.
    Label,
    /// Operation or query name.
    Name,
    /// Parameter name.
    Param,
    /// Property name.
    Property,
    /// Return variable name.
    Return,
    /// Search tenant property name.
    TenantProperty,
    /// Variable name.
    Variable,
}

impl std::fmt::Display for NameField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Alias => f.write_str("alias"),
            Self::Binding => f.write_str("binding"),
            Self::Label => f.write_str("label"),
            Self::Name => f.write_str("name"),
            Self::Param => f.write_str("param"),
            Self::Property => f.write_str("property"),
            Self::Return => f.write_str("return"),
            Self::TenantProperty => f.write_str("tenant_property"),
            Self::Variable => f.write_str("variable"),
        }
    }
}

/// Boolean predicate set operator.
///
/// # Examples
///
/// ```
/// use helix_planner::ir::PredicateSetOp;
///
/// assert_eq!(PredicateSetOp::And.to_string(), "and");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredicateSetOp {
    /// Logical conjunction.
    And,
    /// Logical disjunction.
    Or,
}

impl std::fmt::Display for PredicateSetOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::And => f.write_str("and"),
            Self::Or => f.write_str("or"),
        }
    }
}

/// Invalid expression plan payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprPlanError {
    /// Expression name was empty.
    EmptyName {
        /// Invalid field.
        field: NameField,
    },
    /// Boolean predicate did not contain any operands.
    EmptyPredicateSet {
        /// Predicate operator.
        op: PredicateSetOp,
    },
}

impl std::fmt::Display for ExprPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyName { field } => write!(f, "{field} name must not be empty"),
            Self::EmptyPredicateSet { op } => {
                write!(f, "{op} predicate must contain at least one child")
            }
        }
    }
}
