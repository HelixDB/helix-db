//! Search query, tenant, and result-count input validation.

use helix_ast::expr::StreamBound;
use helix_ast::value::PropertyInput;

use crate::{catalog, error, ir};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum SearchTenantValueInput {
    Value(ir::PropertyInputPlan),
    NotProvided,
}

pub(super) fn tenant_value_plan(
    tenant_value: Option<&PropertyInput>,
) -> Result<SearchTenantValueInput, error::PlannerError> {
    match tenant_value {
        Some(value) => ir::PropertyInputPlan::new(value.clone())
            .map(SearchTenantValueInput::Value)
            .map_err(Into::into),
        None => Ok(SearchTenantValueInput::NotProvided),
    }
}

pub(super) fn vector_query(
    query_vector: &PropertyInput,
) -> Result<ir::VectorQueryInputPlan, error::PlannerError> {
    ir::VectorQueryInputPlan::new(query_vector.clone()).map_err(search_input_error)
}

pub(super) fn text_query(
    query_text: &PropertyInput,
) -> Result<ir::TextQueryInputPlan, error::PlannerError> {
    ir::TextQueryInputPlan::new(query_text.clone()).map_err(search_input_error)
}

fn search_input_error(err: ir::SearchQueryInputPlanError) -> error::PlannerError {
    match err {
        ir::SearchQueryInputPlanError::InvalidLiteral { kind, expected } => {
            error::PlannerError::InvalidSearchInput { kind, expected }
        }
        ir::SearchQueryInputPlanError::Expression(err) => err.into(),
    }
}

pub(super) fn search_limit(
    kind: catalog::SearchIndexKind,
    k: &StreamBound,
) -> Result<ir::SearchLimitPlan, error::PlannerError> {
    ir::SearchLimitPlan::new(k.clone()).map_err(|err| match err {
        ir::SearchLimitPlanError::NonPositiveLiteral { actual } => {
            error::PlannerError::InvalidSearchResultCount { kind, actual }
        }
        ir::SearchLimitPlanError::StaticLiteral { expected } => {
            error::PlannerError::InvalidSearchResultCountExpression { kind, expected }
        }
        ir::SearchLimitPlanError::Expression(err) => err.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::expr::Expr;

    #[test]
    fn tenant_value_plan_distinguishes_absent_and_present_values() {
        assert!(matches!(
            tenant_value_plan(None).unwrap(),
            SearchTenantValueInput::NotProvided
        ));
        assert!(matches!(
            tenant_value_plan(Some(&PropertyInput::from("tenant-a"))).unwrap(),
            SearchTenantValueInput::Value(value)
                if matches!(value, ir::PropertyInputPlan::Value(_))
        ));
        assert!(matches!(
            tenant_value_plan(Some(&PropertyInput::from(Expr::param("tenant")))).unwrap(),
            SearchTenantValueInput::Value(value)
                if matches!(value, ir::PropertyInputPlan::Expr(_))
        ));
    }
}
