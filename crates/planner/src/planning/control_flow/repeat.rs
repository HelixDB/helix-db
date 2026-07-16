use std::num::NonZeroUsize;

use helix_ast::expr::Predicate;
use helix_ast::traversal::{EmitBehavior, RepeatConfig};

use crate::{error, ir};

/// Convert AST repeat stop settings into the repeat IR contract.
///
/// ```
/// use helix_planner::{ir, planning::control_flow};
///
/// assert!(matches!(
///     control_flow::repeat_stop(Some(2), None).unwrap(),
///     ir::RepeatStopPlan::Times { .. }
/// ));
/// ```
pub fn repeat_stop(
    times: Option<usize>,
    until: Option<Predicate>,
) -> Result<ir::RepeatStopPlan, error::PlannerError> {
    match (times, until) {
        (None, None) => Ok(ir::RepeatStopPlan::MaxDepthOnly),
        (Some(count), None) => Ok(ir::RepeatStopPlan::Times {
            count: positive_repeat_count(error::RepeatCountField::Times, count)?,
        }),
        (None, Some(predicate)) => Ok(ir::RepeatStopPlan::Until {
            predicate: ir::PredicatePlan::new(predicate)?,
        }),
        (Some(count), Some(predicate)) => Ok(ir::RepeatStopPlan::TimesOrUntil {
            count: positive_repeat_count(error::RepeatCountField::Times, count)?,
            predicate: ir::PredicatePlan::new(predicate)?,
        }),
    }
}

/// Convert AST repeat emit settings into the repeat IR contract.
pub fn repeat_emit(
    emit: EmitBehavior,
    predicate: Option<Predicate>,
) -> Result<ir::RepeatEmitPlan, error::PlannerError> {
    match (emit, predicate) {
        (EmitBehavior::None, None) => Ok(ir::RepeatEmitPlan::None),
        (EmitBehavior::Before, None) => Ok(ir::RepeatEmitPlan::Before),
        (EmitBehavior::After, None) => Ok(ir::RepeatEmitPlan::After),
        (EmitBehavior::After, Some(predicate)) => Ok(ir::RepeatEmitPlan::AfterIf {
            predicate: ir::PredicatePlan::new(predicate)?,
        }),
        (EmitBehavior::All, None) => Ok(ir::RepeatEmitPlan::All),
        (emit, Some(_)) => Err(error::PlannerError::InvalidRepeatEmit { emit }),
    }
}

/// Build a repeat payload from a validated body and AST repeat config.
pub fn repeat_plan<T>(
    body: T,
    config: &RepeatConfig,
) -> Result<ir::RepeatPlan<T>, error::PlannerError> {
    Ok(ir::RepeatPlan {
        body: Box::new(body),
        stop: repeat_stop(config.times, config.until.clone())?,
        emit: repeat_emit(config.emit, config.emit_predicate.clone())?,
        max_depth: positive_repeat_count(error::RepeatCountField::MaxDepth, config.max_depth)?,
    })
}

fn positive_repeat_count(
    field: error::RepeatCountField,
    actual: usize,
) -> Result<NonZeroUsize, error::PlannerError> {
    NonZeroUsize::new(actual).ok_or(error::PlannerError::InvalidRepeatCount { field, actual })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical;

    fn node_source() -> ir::PhysicalOp {
        ir::PhysicalOp::NodeAccess(ir::NodeAccessPlan::AllScan)
    }

    fn logical_node_source() -> logical::LogicalExpr {
        logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
        )))
    }

    #[test]
    fn repeat_stop_encodes_count_and_predicate_variants() {
        assert!(matches!(
            repeat_stop(None, None).unwrap(),
            ir::RepeatStopPlan::MaxDepthOnly
        ));
        assert!(matches!(
            repeat_stop(Some(2), None).unwrap(),
            ir::RepeatStopPlan::Times { .. }
        ));
        assert!(matches!(
            repeat_stop(None, Some(Predicate::eq("done", true))).unwrap(),
            ir::RepeatStopPlan::Until { .. }
        ));
        assert!(matches!(
            repeat_stop(Some(2), Some(Predicate::eq("done", true))).unwrap(),
            ir::RepeatStopPlan::TimesOrUntil { .. }
        ));

        let invalid = repeat_stop(Some(0), None).unwrap_err();
        assert!(matches!(
            invalid,
            error::PlannerError::InvalidRepeatCount {
                field: error::RepeatCountField::Times,
                actual: 0,
            }
        ));
    }

    #[test]
    fn repeat_emit_encodes_emit_modes_and_rejects_invalid_predicates() {
        assert!(matches!(
            repeat_emit(EmitBehavior::None, None).unwrap(),
            ir::RepeatEmitPlan::None
        ));
        assert!(matches!(
            repeat_emit(EmitBehavior::Before, None).unwrap(),
            ir::RepeatEmitPlan::Before
        ));
        assert!(matches!(
            repeat_emit(EmitBehavior::After, None).unwrap(),
            ir::RepeatEmitPlan::After
        ));
        assert!(matches!(
            repeat_emit(EmitBehavior::After, Some(Predicate::eq("emit", true))).unwrap(),
            ir::RepeatEmitPlan::AfterIf { .. }
        ));
        assert!(matches!(
            repeat_emit(EmitBehavior::All, None).unwrap(),
            ir::RepeatEmitPlan::All
        ));
        assert!(matches!(
            repeat_emit(EmitBehavior::Before, Some(Predicate::eq("emit", true))),
            Err(error::PlannerError::InvalidRepeatEmit {
                emit: EmitBehavior::Before
            })
        ));
    }

    #[test]
    fn repeat_plan_validates_depth_and_stays_payload_generic() {
        let config = RepeatConfig::new(helix_ast::traversal::sub());
        let repeat: ir::RepeatPlan<logical::LogicalExpr> =
            repeat_plan(logical_node_source(), &config).unwrap();
        assert!(matches!(
            repeat.body.as_ref(),
            logical::LogicalExpr::AccessPath(_)
        ));

        let mut invalid_config = RepeatConfig::new(helix_ast::traversal::sub());
        invalid_config.max_depth = 0;
        let invalid = repeat_plan(node_source(), &invalid_config).unwrap_err();
        assert!(matches!(
            invalid,
            error::PlannerError::InvalidRepeatCount {
                field: error::RepeatCountField::MaxDepth,
                actual: 0,
            }
        ));
    }

    #[test]
    fn positive_repeat_count_reports_the_field_that_failed() {
        assert_eq!(
            positive_repeat_count(error::RepeatCountField::Times, 3)
                .unwrap()
                .get(),
            3
        );
        assert!(matches!(
            positive_repeat_count(error::RepeatCountField::MaxDepth, 0),
            Err(error::PlannerError::InvalidRepeatCount {
                field: error::RepeatCountField::MaxDepth,
                actual: 0,
            })
        ));
    }
}
