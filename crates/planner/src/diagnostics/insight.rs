use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{catalog, ir};

/// Secondary-index family recommended by the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecondaryIndexKind {
    /// Equality index.
    Equality,
    /// Range index in either physical direction.
    Range,
}

impl std::fmt::Display for SecondaryIndexKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Equality => f.write_str("equality"),
            Self::Range => f.write_str("range"),
        }
    }
}

/// Sorted, duplicate-free property names referenced by residual predicates.
///
/// The collection deliberately carries names only. Its wire shape has no
/// fields for predicate values, parameter names, comparison operators, or
/// index recommendations.
///
/// ```
/// use helix_planner::{diagnostics::PredicatePropertySet, ir::NonEmptyString};
///
/// let properties = PredicatePropertySet::new([
///     NonEmptyString::new("username").unwrap(),
///     NonEmptyString::new("age").unwrap(),
///     NonEmptyString::new("username").unwrap(),
/// ]);
/// let names = properties.iter().map(AsRef::as_ref).collect::<Vec<_>>();
///
/// assert_eq!(names, ["age", "username"]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PredicatePropertySet(BTreeSet<ir::NonEmptyString>);

impl PredicatePropertySet {
    /// Build a sorted, duplicate-free property set.
    pub fn new(properties: impl IntoIterator<Item = ir::NonEmptyString>) -> Self {
        Self(properties.into_iter().collect())
    }

    /// Return whether the set has no property names.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return the number of distinct property names.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterate over property names in lexical order.
    pub fn iter(&self) -> impl Iterator<Item = &ir::NonEmptyString> {
        self.0.iter()
    }
}

/// Actionable missing secondary-index recommendation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingIndexInsight {
    /// Node or edge index family.
    pub element: catalog::ElementKind,
    /// Concrete label scope required by the index.
    pub label: ir::NonEmptyString,
    /// Flat property name required by the index.
    pub property: ir::NonEmptyString,
    /// Equality or range index family.
    pub index_kind: SecondaryIndexKind,
    /// Number of selected residual-filter occurrences in the query.
    pub occurrences: usize,
}

/// Selected unbounded graph scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnboundedScanInsight {
    /// Node or edge scan family.
    pub element: catalog::ElementKind,
    /// Label when the scan is label-scoped.
    pub label: Option<ir::NonEmptyString>,
    /// Sorted property names referenced by residual predicates fed by this
    /// selected scan.
    #[serde(default, skip_serializing_if = "PredicatePropertySet::is_empty")]
    pub predicate_properties: PredicatePropertySet,
    /// Number of matching selected scans.
    pub occurrences: usize,
}

/// Selected graph traversal whose hop/control-flow shape can amplify work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeepTraversalInsight {
    /// Selected graph expansion operators.
    pub expansion_count: usize,
    /// Selected repeat operators.
    pub repeat_count: usize,
    /// Largest selected expansion/repeat depth observed on an executable path.
    pub maximum_depth: usize,
}

/// Stable actionable planner insight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "details", rename_all = "snake_case")]
pub enum PlannerInsight {
    /// A selected residual predicate could use a missing secondary index.
    MissingIndex(MissingIndexInsight),
    /// A selected access scans an unbounded node or edge source.
    UnboundedScan(UnboundedScanInsight),
    /// A selected traversal contains several expansions or repeat control flow.
    DeepTraversal(DeepTraversalInsight),
}

impl PlannerInsight {
    /// Human-readable message derived only from structured, non-sensitive
    /// fields.
    pub fn message(&self) -> String {
        match self {
            Self::MissingIndex(insight) => format!(
                "missing {} index for {} label `{}` property `{}` ({} occurrence{})",
                insight.index_kind,
                insight.element,
                insight.label,
                insight.property,
                insight.occurrences,
                plural_suffix(insight.occurrences),
            ),
            Self::UnboundedScan(insight) => {
                let target = match &insight.label {
                    Some(label) => {
                        format!("unbounded {} label scan for `{label}`", insight.element)
                    }
                    None => format!("unbounded {} scan", insight.element),
                };
                let property_context = match insight.predicate_properties.len() {
                    0 => String::new(),
                    1 => format!(
                        " with residual predicate property `{}`",
                        insight
                            .predicate_properties
                            .iter()
                            .next()
                            .expect("a single-property set has one property")
                    ),
                    _ => format!(
                        " with residual predicate properties {}",
                        insight
                            .predicate_properties
                            .iter()
                            .map(|property| format!("`{property}`"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                };
                format!(
                    "{target}{property_context} ({} occurrence{})",
                    insight.occurrences,
                    plural_suffix(insight.occurrences),
                )
            }
            Self::DeepTraversal(insight) => format!(
                "deep traversal with {} expansion{}, {} repeat{}, and maximum depth {}",
                insight.expansion_count,
                plural_suffix(insight.expansion_count),
                insight.repeat_count,
                plural_suffix(insight.repeat_count),
                insight.maximum_depth,
            ),
        }
    }
}

const fn plural_suffix(count: usize) -> &'static str {
    if count == 1 {
        ""
    } else {
        "s"
    }
}
