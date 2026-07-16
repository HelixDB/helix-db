//! Native batch-condition validation.
//!
//! Initial entries and follow-up entries have different condition contracts:
//! previous-result checks are only representable for follow-ups.

use std::num::NonZeroUsize;

use helix_ast::batch::BatchCondition;

use super::names;
use crate::{error, ir};

/// Lower an initial-entry batch condition.
pub(super) fn initial(
    condition: &BatchCondition,
) -> Result<ir::BatchVariableConditionPlan, error::PlannerError> {
    match condition {
        BatchCondition::VarNotEmpty(variable) => Ok(ir::BatchVariableConditionPlan::VarNotEmpty(
            variable_name(variable)?,
        )),
        BatchCondition::VarEmpty(variable) => Ok(ir::BatchVariableConditionPlan::VarEmpty(
            variable_name(variable)?,
        )),
        BatchCondition::VarMinSize(variable, min_size) => {
            Ok(ir::BatchVariableConditionPlan::VarMinSize(
                variable_name(variable)?,
                non_zero_min_size(*min_size)?,
            ))
        }
        BatchCondition::PrevNotEmpty => Err(error::PlannerError::InvalidInitialBatchCondition {
            condition: error::InitialBatchCondition::PrevNotEmpty,
        }),
    }
}

/// Lower a follow-up batch condition.
pub(super) fn followup(
    condition: &BatchCondition,
) -> Result<ir::BatchConditionPlan, error::PlannerError> {
    match condition {
        BatchCondition::PrevNotEmpty => Ok(ir::BatchConditionPlan::PrevNotEmpty),
        BatchCondition::VarNotEmpty(_)
        | BatchCondition::VarEmpty(_)
        | BatchCondition::VarMinSize(_, _) => initial(condition).map(Into::into),
    }
}

fn variable_name(variable: &str) -> Result<ir::NonEmptyString, error::PlannerError> {
    names::non_empty(variable, ir::NameField::Variable)
}

fn non_zero_min_size(min_size: usize) -> Result<NonZeroUsize, error::PlannerError> {
    NonZeroUsize::new(min_size)
        .ok_or(error::PlannerError::InvalidBatchConditionMinSize { actual: min_size })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_conditions_reject_previous_result_checks() {
        let condition = initial(&BatchCondition::PrevNotEmpty);
        assert!(matches!(
            condition,
            Err(error::PlannerError::InvalidInitialBatchCondition {
                condition: error::InitialBatchCondition::PrevNotEmpty
            })
        ));
    }

    #[test]
    fn followup_conditions_accept_previous_result_checks() {
        assert!(matches!(
            followup(&BatchCondition::PrevNotEmpty).unwrap(),
            ir::BatchConditionPlan::PrevNotEmpty
        ));
    }

    #[test]
    fn variable_conditions_validate_names_and_min_size() {
        let empty_name = initial(&BatchCondition::VarNotEmpty(String::new()));
        assert!(matches!(
            empty_name,
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Variable
            })
        ));

        let zero_size = followup(&BatchCondition::VarMinSize("items".to_owned(), 0));
        assert!(matches!(
            zero_size,
            Err(error::PlannerError::InvalidBatchConditionMinSize { actual: 0 })
        ));
    }
}
