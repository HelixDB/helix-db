use helix_ast::expr::Predicate;

use crate::{error, ir};

/// Build a branch union payload, rejecting unions with fewer than two arms.
///
/// ```
/// use helix_planner::{ir, logical, planning::control_flow};
///
/// let node = || {
///     logical::LogicalExpr::AccessPath(logical::AccessPath::Node(
///         logical::NodeAccessPath::new(
///             ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
///         ),
///     ))
/// };
///
/// assert!(matches!(
///     control_flow::union_plan(vec![node(), node()]).unwrap(),
///     ir::BranchPlan::Union(_)
/// ));
/// ```
pub fn union_plan<T>(plans: Vec<T>) -> Result<ir::BranchPlan<T>, error::PlannerError> {
    let actual = plans.len();
    ir::AtLeast::<_, 2>::try_from_vec(plans)
        .map(ir::BranchPlan::Union)
        .ok_or(error::PlannerError::InvalidBranchArity {
            op: error::BranchOp::Union,
            min: 2,
            actual,
        })
}

/// Build a conditional branch payload.
pub fn choose_plan<T>(
    condition: Predicate,
    then_plan: T,
    else_plan: Option<T>,
) -> Result<ir::BranchPlan<T>, error::PlannerError> {
    let condition = ir::PredicatePlan::new(condition)?;
    Ok(match else_plan {
        Some(else_plan) => ir::BranchPlan::ChooseElse {
            condition,
            then_plan: Box::new(then_plan),
            else_plan: Box::new(else_plan),
        },
        None => ir::BranchPlan::Choose {
            condition,
            then_plan: Box::new(then_plan),
        },
    })
}

/// Build a coalesce payload, rejecting empty coalesce arms.
///
/// ```
/// use helix_planner::{error, planning::control_flow};
///
/// assert!(matches!(
///     control_flow::coalesce_plan(Vec::<()>::new()),
///     Err(error::PlannerError::InvalidBranchArity {
///         op: error::BranchOp::Coalesce,
///         min: 1,
///         actual: 0,
///     })
/// ));
/// ```
pub fn coalesce_plan<T>(plans: Vec<T>) -> Result<ir::BranchPlan<T>, error::PlannerError> {
    let actual = plans.len();
    ir::AtLeast::<_, 1>::try_from_vec(plans)
        .map(ir::BranchPlan::Coalesce)
        .ok_or(error::PlannerError::InvalidBranchArity {
            op: error::BranchOp::Coalesce,
            min: 1,
            actual,
        })
}

/// Build an optional branch payload.
pub fn optional_plan<T>(plan: T) -> ir::BranchPlan<T> {
    ir::BranchPlan::Optional(Box::new(plan))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical;

    fn node_source() -> ir::PhysicalOp {
        ir::PhysicalOp::NodeAccess(ir::NodeAccessPlan::AllScan)
    }

    fn edge_source() -> ir::PhysicalOp {
        ir::PhysicalOp::EdgeAccess(ir::EdgeAccessPlan::AllScan)
    }

    fn logical_node_source() -> logical::LogicalExpr {
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
        )))
    }

    #[test]
    fn union_and_coalesce_encode_arity_invariants() {
        let union = union_plan(vec![node_source(), edge_source()]).unwrap();
        assert!(matches!(union, ir::BranchPlan::Union(_)));

        let invalid_union = union_plan(vec![node_source()]).unwrap_err();
        assert!(matches!(
            invalid_union,
            error::PlannerError::InvalidBranchArity {
                op: error::BranchOp::Union,
                min: 2,
                actual: 1,
            }
        ));

        let coalesce = coalesce_plan(vec![node_source()]).unwrap();
        assert!(matches!(coalesce, ir::BranchPlan::Coalesce(_)));

        let invalid_coalesce = coalesce_plan(Vec::<ir::PhysicalOp>::new()).unwrap_err();
        assert!(matches!(
            invalid_coalesce,
            error::PlannerError::InvalidBranchArity {
                op: error::BranchOp::Coalesce,
                min: 1,
                actual: 0,
            }
        ));
    }

    #[test]
    fn choose_and_optional_encode_disjoint_branch_variants() {
        let choose = choose_plan(Predicate::eq("active", true), node_source(), None).unwrap();
        assert!(matches!(choose, ir::BranchPlan::Choose { .. }));

        let choose_else = choose_plan(
            Predicate::eq("active", true),
            node_source(),
            Some(edge_source()),
        )
        .unwrap();
        assert!(matches!(choose_else, ir::BranchPlan::ChooseElse { .. }));

        let optional = optional_plan(edge_source());
        assert!(matches!(optional, ir::BranchPlan::Optional(_)));
    }

    #[test]
    fn branch_builders_are_payload_generic() {
        let union: ir::BranchPlan<logical::LogicalExpr> =
            union_plan(vec![logical_node_source(), logical_node_source()]).unwrap();
        assert!(matches!(union, ir::BranchPlan::Union(_)));

        let choose: ir::BranchPlan<logical::LogicalExpr> =
            choose_plan(Predicate::eq("active", true), logical_node_source(), None).unwrap();
        assert!(matches!(choose, ir::BranchPlan::Choose { .. }));

        let optional: ir::BranchPlan<logical::LogicalExpr> = optional_plan(logical_node_source());
        assert!(matches!(optional, ir::BranchPlan::Optional(_)));
    }
}
