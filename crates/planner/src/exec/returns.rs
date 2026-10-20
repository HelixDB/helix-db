//! Executable return-shape contracts.
//!
//! Request returns remain a list of names. This module resolves each name to
//! the semantic output shape of its binding after executable lowering. Runtime
//! code therefore never infers shape from observed rows or cost estimates.

use std::collections::{BTreeMap, BTreeSet};

use helix_ast::traversal::AstNode;
use serde::{Deserialize, Serialize};

use crate::ir;

use super::{ExecOp, ExecPlanError, ExecStep};

/// Shape used to normalize an empty returned value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReturnShape {
    /// A row return serializes an empty value as `[]`.
    List,
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
            _ => ReturnShape::List,
        }
    }
}

/// Infer return shape from the query contract before optimizer rewrites.
///
/// Every row stream returns an array, so an empty one is `[]` whatever its
/// multiplicity; only scalar terminals have a scalar shape.
pub(crate) fn return_shape_from_ast(root: &AstNode) -> ReturnShape {
    match root {
        AstNode::Count { .. }
        | AstNode::Exists { .. }
        | AstNode::CreateIndex { .. }
        | AstNode::DropIndex { .. }
        | AstNode::GetIndexOperation { .. }
        | AstNode::RetryIndexOperation { .. }
        | AstNode::AbortIndexOperation { .. } => ReturnShape::Scalar,
        _ => ReturnShape::List,
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
    ///     exec::ReturnShape::List,
    /// );
    /// assert_eq!(returned.name().as_ref(), "user");
    /// assert_eq!(returned.shape(), exec::ReturnShape::List);
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
        let mut bindings = BTreeMap::new();
        binding_steps(steps, &mut bindings);
        let returns = variables
            .as_ref()
            .iter()
            .map(|name| {
                bindings
                    .get(name.as_ref())
                    .map(|step| ExecutableReturn::new(name.clone(), step.inferred_return_shape()))
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

/// The step whose output each name is bound to after `steps`: the last
/// binding wins, a `ForEach` contributes its body's bindings at its own
/// position, and a step's own binding wins over its body's. Indexing the
/// bindings once keeps resolving many returns linear in the steps.
fn binding_steps<'s>(steps: &'s [ExecStep], bindings: &mut BTreeMap<&'s str, &'s ExecStep>) {
    for step in steps {
        if let ExecOp::ForEach { body, .. } = &step.op {
            binding_steps(body.steps(), bindings);
        }
        if let ir::BatchOutputPlan::Bind(output) = &step.output {
            bindings.insert(output.as_ref(), step);
        }
    }
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
    fn every_row_stream_returns_a_list_whatever_its_multiplicity() {
        let empty_membership = g()
            .n_with_label("Person")
            .where_(Predicate::is_in(
                "orbit_id",
                PropertyValue::StringArray(Vec::new()),
            ))
            .into_ast();
        let point = g().n(NodeRef::id(7)).id().into_ast();
        let bounded = g().n_with_label("Person").limit(1usize).into_ast();
        let empty_window = g().n_with_label("Person").limit(0usize).into_ast();
        let edge_point = g().e(EdgeRef::id(7)).into_ast();
        let grouped = g().n_with_label("Person").group("team").into_ast();
        let control = g()
            .n(NodeRef::id(7))
            .optional(sub().out(Some("KNOWS")))
            .into_ast();
        for row_stream in [
            empty_membership,
            point,
            bounded,
            empty_window,
            edge_point,
            grouped,
            control,
        ] {
            assert_eq!(return_shape_from_ast(&row_stream), ReturnShape::List);
        }
        for scalar in [
            g().n(NodeRef::id(7)).count().into_ast(),
            g().n_with_label("Person").limit(1usize).exists().into_ast(),
        ] {
            assert_eq!(return_shape_from_ast(&scalar), ReturnShape::Scalar);
        }
    }

    #[test]
    fn row_shape_ignores_cardinality_operator_and_cost() {
        let point = step(
            ExecOp::Noop,
            properties::CardinalityBounds::zero_to(Some(1)),
        );
        let mut alternative = step(
            ExecOp::Barrier {
                name: name("physical-alternative"),
            },
            properties::CardinalityBounds::zero_to(Some(0)),
        );
        alternative.cost = cost::CostVector {
            object_reads: u64::MAX,
            cpu_units: u64::MAX,
            ..cost::CostVector::ZERO
        };
        let unknown_collection = step(ExecOp::Noop, properties::CardinalityBounds::unknown());

        for row_step in [point, alternative, unknown_collection] {
            assert_eq!(row_step.inferred_return_shape(), ReturnShape::List);
        }
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
        assert_eq!(single.as_ref(), std::slice::from_ref(&returned));
        let duplicate = ir::AtLeast::from_one_and_rest(
            returned,
            vec![ExecutableReturn::new(name("result"), ReturnShape::Scalar)],
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
            ExecOp::Count {
                plan: Box::new(super::super::ExecCountPlan::Constant(0)),
            },
            properties::CardinalityBounds::exact(1),
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
                ReturnShape::Scalar,
            )))
            .unwrap(),
        );

        assert_eq!(
            ExecutableReturns::resolve(&requested, &[foreach]),
            Ok(expected)
        );
    }

    /// The resolution this module used before indexing bindings: scan the
    /// steps backwards per name. An oracle for the precedence rules only.
    fn reverse_scan_shape(steps: &[ExecStep], name: &ir::NonEmptyString) -> Option<ReturnShape> {
        steps.iter().rev().find_map(|step| {
            if matches!(&step.output, ir::BatchOutputPlan::Bind(output) if output == name) {
                return Some(step.inferred_return_shape());
            }
            match &step.op {
                ExecOp::ForEach { body, .. } => reverse_scan_shape(body.steps(), name),
                _ => None,
            }
        })
    }

    #[test]
    fn indexed_bindings_resolve_as_the_reverse_scan_did() {
        let scalar = || {
            step(
                ExecOp::Count {
                    plan: Box::new(super::super::ExecCountPlan::Constant(0)),
                },
                properties::CardinalityBounds::exact(1),
            )
        };
        let list = || step(ExecOp::Noop, properties::CardinalityBounds::unknown());
        let foreach = |body: ExecStep, output: ir::BatchOutputPlan| {
            let mut foreach = step(
                ExecOp::ForEach {
                    param: name("items"),
                    body: Box::new(
                        super::super::ExecutableSubplan::new(
                            ir::AtLeast::from_one(body),
                            ExecStepId::new(1).unwrap(),
                        )
                        .unwrap(),
                    ),
                },
                properties::CardinalityBounds::unknown(),
            );
            foreach.output = output;
            foreach
        };
        let bound = ir::BatchOutputPlan::Bind(name("result"));
        let cases = [
            // The last binding wins.
            vec![scalar(), list()],
            vec![list(), scalar()],
            // A body's binding sits at its loop's position.
            vec![foreach(scalar(), ir::BatchOutputPlan::Discard), list()],
            vec![list(), foreach(scalar(), ir::BatchOutputPlan::Discard)],
            // A loop's own binding wins over its body's.
            vec![foreach(scalar(), bound.clone())],
            vec![
                scalar(),
                foreach(list(), ir::BatchOutputPlan::Discard),
                scalar(),
            ],
        ];
        let requested = ir::ReturnPlan::Variables(
            ir::ReturnVariables::new(ir::AtLeast::from_one(name("result"))).unwrap(),
        );
        for (index, steps) in cases.iter().enumerate() {
            let expected = reverse_scan_shape(steps, &name("result")).expect("result is bound");
            let Ok(ExecutableReturns::Variables(resolved)) =
                ExecutableReturns::resolve(&requested, steps)
            else {
                panic!("case {index} resolves");
            };
            assert_eq!(resolved.as_ref()[0].shape(), expected, "case {index}");
        }
        // The cases above must exercise both shapes.
        let shapes = cases
            .iter()
            .filter_map(|steps| reverse_scan_shape(steps, &name("result")))
            .collect::<Vec<_>>();
        assert!(shapes.contains(&ReturnShape::Scalar) && shapes.contains(&ReturnShape::List));
    }
}
