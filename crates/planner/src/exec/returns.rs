//! Executable return-shape contracts.
//!
//! Request returns remain a list of names. This module resolves each name to
//! the semantic output shape of its binding after executable lowering. Runtime
//! code therefore never infers shape from observed rows or cost estimates.

use std::collections::BTreeSet;

use helix_ast::expr::StreamBound;
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::traversal::AstNode;
use serde::{Deserialize, Serialize};

use crate::ir;

use super::{ExecOp, ExecPlanError, ExecStep};

/// Shape used to normalize an empty returned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReturnShape {
    /// A collection return serializes an empty value as `[]`.
    List,
    /// An at-most-one return serializes an empty value as `null`.
    Object,
    /// A scalar return has no synthetic empty representation.
    Scalar,
}

impl ExecStep {
    /// Infer the response shape of this step's bound output.
    pub fn inferred_return_shape(&self) -> ReturnShape {
        if let Some(shape) = self.semantic_return_shape {
            return shape;
        }
        match &self.op {
            ExecOp::Count { .. }
            | ExecOp::Project {
                projection: ir::ProjectionPlan::Exists,
            }
            | ExecOp::IndexDdl { .. } => ReturnShape::Scalar,
            ExecOp::Reserved {
                op: ir::ReservedOp::Fold,
            }
            | ExecOp::Mutation { .. } => ReturnShape::List,
            _ => match self.delivered.cardinality.upper() {
                Some(0 | 1) => ReturnShape::Object,
                Some(_) | None => ReturnShape::List,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SemanticMultiplicity {
    AtMostOne,
    Collection,
}

/// Infer return shape from the query contract before optimizer rewrites.
///
/// Predicate truth is deliberately ignored: proving a collection empty must
/// not turn its public response from `[]` into `null`. Explicit multiplicity
/// operators such as `limit(1)` still produce an at-most-one contract.
pub(crate) fn return_shape_from_ast(root: &AstNode) -> ReturnShape {
    match root {
        AstNode::Count { .. }
        | AstNode::Exists { .. }
        | AstNode::CreateIndex { .. }
        | AstNode::DropIndex { .. }
        | AstNode::GetIndexOperation { .. }
        | AstNode::RetryIndexOperation { .. }
        | AstNode::AbortIndexOperation { .. } => ReturnShape::Scalar,
        AstNode::AddN { .. }
        | AstNode::AddE { .. }
        | AstNode::SetProperty { .. }
        | AstNode::RemoveProperty { .. }
        | AstNode::Drop { .. }
        | AstNode::DropEdge { .. }
        | AstNode::DropEdgeLabeled { .. }
        | AstNode::DropEdgeById { .. }
        | AstNode::Group { .. }
        | AstNode::GroupCount { .. }
        | AstNode::AggregateBy { .. }
        | AstNode::Fold { .. }
        | AstNode::ShortestPath { .. } => ReturnShape::List,
        _ => match semantic_multiplicity(root, SemanticMultiplicity::Collection) {
            SemanticMultiplicity::AtMostOne => ReturnShape::Object,
            SemanticMultiplicity::Collection => ReturnShape::List,
        },
    }
}

fn semantic_multiplicity(root: &AstNode, context: SemanticMultiplicity) -> SemanticMultiplicity {
    match root {
        AstNode::Context => context,
        AstNode::Nodes { reference } => node_reference_multiplicity(reference),
        AstNode::Edges { reference } => edge_reference_multiplicity(reference),
        AstNode::NodesWhere { .. } | AstNode::EdgesWhere { .. } => SemanticMultiplicity::Collection,
        AstNode::VectorSearchNodes { k, .. }
        | AstNode::TextSearchNodes { k, .. }
        | AstNode::VectorSearchEdges { k, .. }
        | AstNode::TextSearchEdges { k, .. } => bound_multiplicity(k),
        AstNode::TextSearchNodesWithin { input, k, .. }
        | AstNode::TextSearchEdgesWithin { input, k, .. }
        | AstNode::VectorSearchNodesWithin { input, k, .. }
        | AstNode::VectorSearchEdgesWithin { input, k, .. } => {
            let input = semantic_multiplicity(input, context);
            if input == SemanticMultiplicity::AtMostOne
                || bound_multiplicity(k) == SemanticMultiplicity::AtMostOne
            {
                SemanticMultiplicity::AtMostOne
            } else {
                SemanticMultiplicity::Collection
            }
        }
        AstNode::Out { .. }
        | AstNode::In { .. }
        | AstNode::Both { .. }
        | AstNode::OutE { .. }
        | AstNode::InE { .. }
        | AstNode::BothE { .. }
        | AstNode::OutN { .. }
        | AstNode::InN { .. }
        | AstNode::OtherN { .. }
        | AstNode::Inject { .. }
        | AstNode::Select { .. }
        | AstNode::Repeat { .. }
        | AstNode::Unfold { .. }
        | AstNode::ShortestPath { .. }
        | AstNode::Union { .. }
        | AstNode::Choose { .. }
        | AstNode::Coalesce { .. }
        | AstNode::Optional { .. } => SemanticMultiplicity::Collection,
        AstNode::Has { input, .. }
        | AstNode::HasLabel { input, .. }
        | AstNode::HasKey { input, .. }
        | AstNode::Where { input, .. }
        | AstNode::Dedup { input }
        | AstNode::Within { input, .. }
        | AstNode::Without { input, .. }
        | AstNode::EdgeHas { input, .. }
        | AstNode::EdgeHasLabel { input, .. }
        | AstNode::Skip { input, .. }
        | AstNode::As { input, .. }
        | AstNode::Store { input, .. }
        | AstNode::Bind { input, .. }
        | AstNode::Id { input }
        | AstNode::Label { input }
        | AstNode::Values { input, .. }
        | AstNode::ValueMap { input, .. }
        | AstNode::Project { input, .. }
        | AstNode::ProjectBindings { input, .. }
        | AstNode::EdgeProperties { input }
        | AstNode::OrderBy { input, .. }
        | AstNode::OrderByMultiple { input, .. }
        | AstNode::Path { input }
        | AstNode::SimplePath { input }
        | AstNode::WithSack { input, .. }
        | AstNode::SackSet { input, .. }
        | AstNode::SackAdd { input, .. }
        | AstNode::SackGet { input } => semantic_multiplicity(input, context),
        AstNode::Limit { input, count } => {
            let input = semantic_multiplicity(input, context);
            if input == SemanticMultiplicity::AtMostOne
                || bound_multiplicity(count) == SemanticMultiplicity::AtMostOne
            {
                SemanticMultiplicity::AtMostOne
            } else {
                SemanticMultiplicity::Collection
            }
        }
        AstNode::Range { input, start, end } => {
            let input = semantic_multiplicity(input, context);
            let bounded = match (start, end) {
                (StreamBound::Literal(start), StreamBound::Literal(end)) => {
                    end.saturating_sub(*start) <= 1
                }
                (StreamBound::Literal(_) | StreamBound::Expr(_), _) => false,
            };
            if input == SemanticMultiplicity::AtMostOne || bounded {
                SemanticMultiplicity::AtMostOne
            } else {
                SemanticMultiplicity::Collection
            }
        }
        AstNode::Count { .. }
        | AstNode::Exists { .. }
        | AstNode::CreateIndex { .. }
        | AstNode::DropIndex { .. }
        | AstNode::GetIndexOperation { .. }
        | AstNode::RetryIndexOperation { .. }
        | AstNode::AbortIndexOperation { .. } => SemanticMultiplicity::AtMostOne,
        AstNode::AddN { .. }
        | AstNode::AddE { .. }
        | AstNode::SetProperty { .. }
        | AstNode::RemoveProperty { .. }
        | AstNode::Drop { .. }
        | AstNode::DropEdge { .. }
        | AstNode::DropEdgeLabeled { .. }
        | AstNode::DropEdgeById { .. }
        | AstNode::Group { .. }
        | AstNode::GroupCount { .. }
        | AstNode::AggregateBy { .. }
        | AstNode::Fold { .. } => SemanticMultiplicity::Collection,
    }
}

fn node_reference_multiplicity(reference: &NodeRef) -> SemanticMultiplicity {
    match reference {
        NodeRef::Ids(ids) if ids.len() <= 1 => SemanticMultiplicity::AtMostOne,
        NodeRef::All | NodeRef::Ids(_) | NodeRef::Var(_) | NodeRef::Param(_) => {
            SemanticMultiplicity::Collection
        }
    }
}

fn edge_reference_multiplicity(reference: &EdgeRef) -> SemanticMultiplicity {
    match reference {
        EdgeRef::Ids(ids) if ids.len() <= 1 => SemanticMultiplicity::AtMostOne,
        EdgeRef::All | EdgeRef::Ids(_) | EdgeRef::Var(_) | EdgeRef::Param(_) => {
            SemanticMultiplicity::Collection
        }
    }
}

fn bound_multiplicity(bound: &StreamBound) -> SemanticMultiplicity {
    match bound {
        StreamBound::Literal(bound) if *bound <= 1 => SemanticMultiplicity::AtMostOne,
        StreamBound::Literal(_) | StreamBound::Expr(_) => SemanticMultiplicity::Collection,
    }
}

/// One executable return with its planner-inferred shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutableReturn {
    name: ir::NonEmptyString,
    shape: ReturnShape,
}

impl ExecutableReturn {
    /// Build one executable return.
    ///
    /// ```
    /// use helix_planner::{exec, ir};
    ///
    /// let returned = exec::ExecutableReturn::new(
    ///     ir::NonEmptyString::from_static("user"),
    ///     exec::ReturnShape::Object,
    /// );
    /// assert_eq!(returned.name().as_ref(), "user");
    /// assert_eq!(returned.shape(), exec::ReturnShape::Object);
    /// ```
    pub fn new(name: ir::NonEmptyString, shape: ReturnShape) -> Self {
        Self { name, shape }
    }

    /// Returned variable name.
    pub const fn name(&self) -> &ir::NonEmptyString {
        &self.name
    }

    /// Planner-inferred output shape.
    pub const fn shape(&self) -> ReturnShape {
        self.shape
    }
}

/// Non-empty executable returns with unique names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutableReturnVariables {
    returns: ir::AtLeast<ExecutableReturn, 1>,
}

impl ExecutableReturnVariables {
    /// Build executable returns, rejecting duplicate names.
    pub fn new(
        returns: ir::AtLeast<ExecutableReturn, 1>,
    ) -> Result<Self, ir::ReturnVariablesError> {
        let mut names = BTreeSet::new();
        for returned in &returns {
            if !names.insert(returned.name.clone()) {
                return Err(ir::ReturnVariablesError::DuplicateName {
                    name: returned.name.clone(),
                });
            }
        }
        Ok(Self { returns })
    }
}

impl AsRef<[ExecutableReturn]> for ExecutableReturnVariables {
    fn as_ref(&self) -> &[ExecutableReturn] {
        self.returns.as_ref()
    }
}

/// Resolved executable returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutableReturns {
    /// Return no variables.
    None,
    /// Return one or more shaped variables.
    Variables(ExecutableReturnVariables),
}

impl ExecutableReturns {
    pub(super) fn resolve(
        requested: &ir::ReturnPlan,
        steps: &[ExecStep],
    ) -> Result<Self, ExecPlanError> {
        let ir::ReturnPlan::Variables(variables) = requested else {
            return Ok(Self::None);
        };
        let returns = variables
            .as_ref()
            .iter()
            .map(|name| {
                return_shape(steps, name)
                    .map(|shape| ExecutableReturn::new(name.clone(), shape))
                    .ok_or_else(|| ExecPlanError::MissingReturnBinding { name: name.clone() })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::Variables(
            ExecutableReturnVariables::new(
                ir::AtLeast::try_from_vec(returns)
                    .expect("ReturnPlan::Variables always contains at least one name"),
            )
            .expect("ReturnPlan::Variables already guarantees unique names"),
        ))
    }
}

fn return_shape(steps: &[ExecStep], name: &ir::NonEmptyString) -> Option<ReturnShape> {
    steps.iter().rev().find_map(|step| {
        if matches!(&step.output, ir::BatchOutputPlan::Bind(output) if output == name) {
            return Some(step.inferred_return_shape());
        }
        match &step.op {
            ExecOp::ForEach { body, .. } => return_shape(body.steps(), name),
            _ => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::{ExecCondition, ExecSchedule, ExecStepId};
    use crate::{cost, properties};
    use helix_ast::expr::Predicate;
    use helix_ast::graph::{EdgeRef, NodeRef};
    use helix_ast::traversal::{g, sub};
    use helix_ast::value::PropertyValue;

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn step(op: ExecOp, cardinality: properties::CardinalityBounds) -> ExecStep {
        ExecStep {
            id: ExecStepId::new(1).unwrap(),
            dependencies: Vec::new(),
            output: ir::BatchOutputPlan::Bind(name("result")),
            semantic_return_shape: None,
            condition: ExecCondition::Always,
            op,
            schedule: ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties {
                cardinality,
                ..properties::DeliveredProperties::default()
            },
            cost: cost::CostVector::ZERO,
        }
    }

    #[test]
    fn semantic_shape_is_stable_when_a_collection_predicate_is_proven_empty() {
        let empty_membership = g()
            .n_with_label("Person")
            .where_(Predicate::is_in(
                "orbit_id",
                PropertyValue::StringArray(Vec::new()),
            ))
            .into_ast();
        let empty_point = g()
            .n(NodeRef::id(7))
            .where_(Predicate::is_in(
                "orbit_id",
                PropertyValue::StringArray(Vec::new()),
            ))
            .into_ast();
        let bounded = g().n_with_label("Person").limit(1usize).into_ast();
        let scalar = g().n_with_label("Person").count().into_ast();
        let grouped = g().n_with_label("Person").group("team").into_ast();
        let selected = g().n(NodeRef::id(7)).select("cached").into_ast();
        let control = g()
            .n(NodeRef::id(7))
            .optional(sub().out(Some("KNOWS")))
            .into_ast();
        let edge_endpoint = g().e(EdgeRef::id(7)).out_n().into_ast();

        assert_eq!(return_shape_from_ast(&empty_membership), ReturnShape::List);
        assert_eq!(return_shape_from_ast(&empty_point), ReturnShape::Object);
        assert_eq!(return_shape_from_ast(&bounded), ReturnShape::Object);
        assert_eq!(return_shape_from_ast(&scalar), ReturnShape::Scalar);
        assert_eq!(return_shape_from_ast(&grouped), ReturnShape::List);
        assert_eq!(return_shape_from_ast(&selected), ReturnShape::List);
        assert_eq!(return_shape_from_ast(&control), ReturnShape::List);
        assert_eq!(return_shape_from_ast(&edge_endpoint), ReturnShape::List);
    }

    #[test]
    fn shape_depends_on_semantics_not_selected_operator_or_cost() {
        let point = step(
            ExecOp::Noop,
            properties::CardinalityBounds::zero_to(Some(1)),
        );
        let mut alternative = step(
            ExecOp::Barrier {
                name: name("physical-alternative"),
            },
            properties::CardinalityBounds::zero_to(Some(1)),
        );
        alternative.cost = cost::CostVector {
            object_reads: u64::MAX,
            cpu_units: u64::MAX,
            ..cost::CostVector::ZERO
        };
        let bounded_collection = step(
            ExecOp::Noop,
            properties::CardinalityBounds::zero_to(Some(2)),
        );
        let unknown_collection = step(ExecOp::Noop, properties::CardinalityBounds::unknown());

        assert_eq!(point.inferred_return_shape(), ReturnShape::Object);
        assert_eq!(alternative.inferred_return_shape(), ReturnShape::Object);
        assert_eq!(
            bounded_collection.inferred_return_shape(),
            ReturnShape::List
        );
        assert_eq!(
            unknown_collection.inferred_return_shape(),
            ReturnShape::List
        );
    }

    #[test]
    fn scalar_and_collection_terminals_override_row_cardinality() {
        let count = step(
            ExecOp::Count {
                plan: Box::new(super::super::ExecCountPlan::Constant(0)),
            },
            properties::CardinalityBounds::exact(1),
        );
        let exists = step(
            ExecOp::Project {
                projection: ir::ProjectionPlan::Exists,
            },
            properties::CardinalityBounds::exact(1),
        );
        let index_ddl = step(
            ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::GetOperation {
                    operation_id: ir::IndexOperationId::try_new(
                        "07070707-0707-0707-0707-070707070707",
                    )
                    .unwrap(),
                },
            },
            properties::CardinalityBounds::exact(1),
        );
        let fold = step(
            ExecOp::Reserved {
                op: ir::ReservedOp::Fold,
            },
            properties::CardinalityBounds::zero_to(Some(1)),
        );

        assert_eq!(count.inferred_return_shape(), ReturnShape::Scalar);
        assert_eq!(exists.inferred_return_shape(), ReturnShape::Scalar);
        assert_eq!(index_ddl.inferred_return_shape(), ReturnShape::Scalar);
        assert_eq!(fold.inferred_return_shape(), ReturnShape::List);
    }

    #[test]
    fn mutations_keep_the_existing_list_empty_shape() {
        let mutation = step(
            ExecOp::Mutation {
                plan: super::super::ExecMutationPlan::Drop,
            },
            properties::CardinalityBounds::zero_to(Some(1)),
        );

        assert_eq!(mutation.inferred_return_shape(), ReturnShape::List);
    }

    #[test]
    fn executable_return_variables_reject_duplicate_names() {
        let returned = ExecutableReturn::new(name("result"), ReturnShape::List);
        assert_eq!(returned.name().as_ref(), "result");
        assert_eq!(returned.shape(), ReturnShape::List);
        let single = ExecutableReturnVariables::new(ir::AtLeast::from_one(returned.clone()))
            .expect("one return name is unique");
        assert_eq!(single.as_ref(), &[returned.clone()]);
        let duplicate = ir::AtLeast::from_one_and_rest(
            returned,
            vec![ExecutableReturn::new(name("result"), ReturnShape::Object)],
        );

        assert!(matches!(
            ExecutableReturnVariables::new(duplicate),
            Err(ir::ReturnVariablesError::DuplicateName { .. })
        ));
    }

    #[test]
    fn return_resolution_rejects_names_without_executable_bindings() {
        let requested = ir::ReturnPlan::Variables(
            ir::ReturnVariables::new(ir::AtLeast::from_one(name("missing"))).unwrap(),
        );

        assert_eq!(
            ExecutableReturns::resolve(
                &requested,
                &[step(ExecOp::Noop, properties::CardinalityBounds::unknown())],
            ),
            Err(ExecPlanError::MissingReturnBinding {
                name: name("missing")
            })
        );
    }

    #[test]
    fn empty_return_declaration_resolves_without_steps() {
        assert_eq!(
            ExecutableReturns::resolve(&ir::ReturnPlan::None, &[]),
            Ok(ExecutableReturns::None)
        );
    }

    #[test]
    fn return_resolution_finds_bindings_inside_foreach_bodies() {
        let body_step = step(
            ExecOp::Noop,
            properties::CardinalityBounds::zero_to(Some(1)),
        );
        let body = super::super::ExecutableSubplan::new(
            ir::AtLeast::from_one(body_step),
            ExecStepId::new(1).unwrap(),
        )
        .unwrap();
        let mut foreach = step(
            ExecOp::ForEach {
                param: name("items"),
                body: Box::new(body),
            },
            properties::CardinalityBounds::unknown(),
        );
        foreach.output = ir::BatchOutputPlan::Discard;
        let requested = ir::ReturnPlan::Variables(
            ir::ReturnVariables::new(ir::AtLeast::from_one(name("result"))).unwrap(),
        );

        let expected = ExecutableReturns::Variables(
            ExecutableReturnVariables::new(ir::AtLeast::from_one(ExecutableReturn::new(
                name("result"),
                ReturnShape::Object,
            )))
            .unwrap(),
        );

        assert_eq!(
            ExecutableReturns::resolve(&requested, &[foreach]),
            Ok(expected)
        );
    }
}
