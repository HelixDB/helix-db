use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ir::NonEmptyString;

/// Internal planner trace reason.
///
/// # Examples
///
/// ```
/// use helix_planner::trace::TraceReason;
///
/// assert_eq!(TraceReason::ConcreteIds.to_string(), "concrete ids");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TraceReason {
    /// Top-level branch context was injected into a sub-traversal.
    BranchStartsFromParentStream,
    /// Direct all-node AST reference.
    NodeRefAll,
    /// Direct all-edge AST reference.
    EdgeRefAll,
    /// Direct concrete ID access.
    ConcreteIds,
    /// Empty concrete ID set.
    EmptyIdSet,
    /// Literal property filter.
    HasLiteralFilter,
    /// Label filter.
    LabelFilter,
    /// Property existence filter.
    HasKeyFilter,
    /// Residual where predicate that may also contain indexable atoms.
    WhereResidualIndexCandidate,
    /// Variable inclusion filter.
    WithinVariableFilter,
    /// Variable exclusion filter.
    WithoutVariableFilter,
    /// Edge property filter.
    EdgeHasFilter,
    /// Edge label filter.
    EdgeLabelFilter,
    /// Explicit physical limit.
    ExplicitPhysicalLimit,
    /// Store current stream as a named variable.
    StoreStreamAsVariable,
    /// Select a named variable stream.
    SelectVariableStream,
    /// Capture a row-local binding.
    CaptureRowLocalBinding,
    /// Inject a variable stream.
    InjectVariableStream,
    /// Range-backed order has not been integrated into access planning.
    RangeBackedOrderRequiresAccessPathIntegration,
    /// Reserved operation remains in the plan for executor semantics.
    PreservedForExecutorSemantics,
    /// Label predicates are contradictory.
    ContradictoryLabelConstraints,
    /// Scalar property predicates are contradictory.
    ContradictoryScalarConstraints,
    /// Plans were ordered by lowest estimated cardinality.
    LowestEstimatedCardinalityFirst,
    /// AND predicates produced indexed atoms.
    AndIndexedAtoms,
    /// No scoped indexable atom was available.
    NoScopedIndexableAtom,
    /// OR predicates require residual branches.
    OrHasResidualBranches,
    /// Predicate had no label scope.
    NoLabelScope,
    /// Nested AND predicates produced indexed atoms.
    NestedAndIndexedAtoms,
    /// OR predicates produced indexed atoms.
    OrIndexedAtoms,
    /// Native AST query root kind.
    NativeAstRoot(NonEmptyString),
    /// Native batch `ForEach` wrapper with a selected body.
    NativeForEachBody,
    /// Selected executable root family.
    SelectedRootFamily(NonEmptyString),
    /// Optimizer rule that produced a selected executable root.
    SelectedOptimizerRule(NonEmptyString),
    /// Memo group/expression/alternative that produced a selected executable root.
    SelectedMemoExpression(NonEmptyString),
    /// Child memo group referenced by a selected executable root.
    SelectedMemoChild(NonEmptyString),
    /// Selected batch `ForEach` wrapper with a selected body.
    SelectedForEachBody,
    /// Selected concrete index ID.
    IndexId(NonEmptyString),
}

impl std::fmt::Display for TraceReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BranchStartsFromParentStream => f.write_str("branch starts from parent stream"),
            Self::NodeRefAll => f.write_str("NodeRef::All"),
            Self::EdgeRefAll => f.write_str("EdgeRef::All"),
            Self::ConcreteIds => f.write_str("concrete ids"),
            Self::EmptyIdSet => f.write_str("empty id set"),
            Self::HasLiteralFilter => f.write_str("has literal filter"),
            Self::LabelFilter => f.write_str("label filter"),
            Self::HasKeyFilter => f.write_str("has_key filter"),
            Self::WhereResidualIndexCandidate => f.write_str("where residual/index candidate"),
            Self::WithinVariableFilter => f.write_str("within variable filter"),
            Self::WithoutVariableFilter => f.write_str("without variable filter"),
            Self::EdgeHasFilter => f.write_str("edge_has filter"),
            Self::EdgeLabelFilter => f.write_str("edge label filter"),
            Self::ExplicitPhysicalLimit => f.write_str("explicit physical limit"),
            Self::StoreStreamAsVariable => f.write_str("store stream as variable"),
            Self::SelectVariableStream => f.write_str("select variable stream"),
            Self::CaptureRowLocalBinding => f.write_str("capture row-local binding"),
            Self::InjectVariableStream => f.write_str("inject variable stream"),
            Self::RangeBackedOrderRequiresAccessPathIntegration => {
                f.write_str("range-backed order requires access-path integration")
            }
            Self::PreservedForExecutorSemantics => f.write_str("preserved for executor semantics"),
            Self::ContradictoryLabelConstraints => f.write_str("contradictory label constraints"),
            Self::ContradictoryScalarConstraints => {
                f.write_str("contradictory scalar property constraints")
            }
            Self::LowestEstimatedCardinalityFirst => {
                f.write_str("lowest estimated cardinality first")
            }
            Self::AndIndexedAtoms => f.write_str("AND indexed atoms"),
            Self::NoScopedIndexableAtom => f.write_str("no scoped indexable atom"),
            Self::OrHasResidualBranches => f.write_str("OR has residual branches"),
            Self::NoLabelScope => f.write_str("no label scope"),
            Self::NestedAndIndexedAtoms => f.write_str("nested AND indexed atoms"),
            Self::OrIndexedAtoms => f.write_str("OR indexed atoms"),
            Self::NativeAstRoot(kind) => write!(f, "native AST root: {kind}"),
            Self::NativeForEachBody => f.write_str("native foreach body"),
            Self::SelectedRootFamily(family) => write!(f, "selected root: {family}"),
            Self::SelectedOptimizerRule(rule_id) => {
                write!(f, "selected optimizer rule: {rule_id}")
            }
            Self::SelectedMemoExpression(summary) => write!(f, "selected memo: {summary}"),
            Self::SelectedMemoChild(summary) => write!(f, "selected memo child: {summary}"),
            Self::SelectedForEachBody => f.write_str("selected foreach body"),
            Self::IndexId(index_id) => f.write_str(index_id.as_ref()),
        }
    }
}

impl Serialize for TraceReason {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TraceReason {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "branch starts from parent stream" => Self::BranchStartsFromParentStream,
            "NodeRef::All" => Self::NodeRefAll,
            "EdgeRef::All" => Self::EdgeRefAll,
            "concrete ids" => Self::ConcreteIds,
            "empty id set" => Self::EmptyIdSet,
            "has literal filter" => Self::HasLiteralFilter,
            "label filter" => Self::LabelFilter,
            "has_key filter" => Self::HasKeyFilter,
            "where residual/index candidate" => Self::WhereResidualIndexCandidate,
            "within variable filter" => Self::WithinVariableFilter,
            "without variable filter" => Self::WithoutVariableFilter,
            "edge_has filter" => Self::EdgeHasFilter,
            "edge label filter" => Self::EdgeLabelFilter,
            "explicit physical limit" => Self::ExplicitPhysicalLimit,
            "store stream as variable" => Self::StoreStreamAsVariable,
            "select variable stream" => Self::SelectVariableStream,
            "capture row-local binding" => Self::CaptureRowLocalBinding,
            "inject variable stream" => Self::InjectVariableStream,
            "range-backed order requires access-path integration" => {
                Self::RangeBackedOrderRequiresAccessPathIntegration
            }
            "preserved for executor semantics" => Self::PreservedForExecutorSemantics,
            "contradictory label constraints" => Self::ContradictoryLabelConstraints,
            "contradictory scalar property constraints" => Self::ContradictoryScalarConstraints,
            "lowest estimated cardinality first" => Self::LowestEstimatedCardinalityFirst,
            "AND indexed atoms" => Self::AndIndexedAtoms,
            "no scoped indexable atom" => Self::NoScopedIndexableAtom,
            "OR has residual branches" => Self::OrHasResidualBranches,
            "no label scope" => Self::NoLabelScope,
            "nested AND indexed atoms" => Self::NestedAndIndexedAtoms,
            "OR indexed atoms" => Self::OrIndexedAtoms,
            "native foreach body" => Self::NativeForEachBody,
            "selected foreach body" => Self::SelectedForEachBody,
            native_ast_root if native_ast_root.starts_with("native AST root: ") => {
                Self::NativeAstRoot(
                    NonEmptyString::new(
                        native_ast_root
                            .trim_start_matches("native AST root: ")
                            .to_owned(),
                    )
                    .ok_or_else(|| D::Error::custom("expected non-empty native AST root kind"))?,
                )
            }
            selected_root if selected_root.starts_with("selected root: ") => {
                Self::SelectedRootFamily(
                    NonEmptyString::new(
                        selected_root
                            .trim_start_matches("selected root: ")
                            .to_owned(),
                    )
                    .ok_or_else(|| D::Error::custom("expected non-empty selected root family"))?,
                )
            }
            selected_rule if selected_rule.starts_with("selected optimizer rule: ") => {
                Self::SelectedOptimizerRule(
                    NonEmptyString::new(
                        selected_rule
                            .trim_start_matches("selected optimizer rule: ")
                            .to_owned(),
                    )
                    .ok_or_else(|| {
                        D::Error::custom("expected non-empty selected optimizer rule")
                    })?,
                )
            }
            selected_memo if selected_memo.starts_with("selected memo: ") => {
                Self::SelectedMemoExpression(
                    NonEmptyString::new(
                        selected_memo
                            .trim_start_matches("selected memo: ")
                            .to_owned(),
                    )
                    .ok_or_else(|| D::Error::custom("expected non-empty selected memo summary"))?,
                )
            }
            selected_memo_child if selected_memo_child.starts_with("selected memo child: ") => {
                Self::SelectedMemoChild(
                    NonEmptyString::new(
                        selected_memo_child
                            .trim_start_matches("selected memo child: ")
                            .to_owned(),
                    )
                    .ok_or_else(|| {
                        D::Error::custom("expected non-empty selected memo child summary")
                    })?,
                )
            }
            _ => Self::IndexId(
                NonEmptyString::new(value)
                    .ok_or_else(|| D::Error::custom("expected non-empty trace reason"))?,
            ),
        })
    }
}
