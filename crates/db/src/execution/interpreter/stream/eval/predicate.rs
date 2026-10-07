//! Native collection equality semantics retained by the resolved evaluator.
use super::*;

#[cfg(test)]
impl ExecutionContext<'_> {
    pub(in crate::execution::interpreter) async fn eval_predicate(
        &self,
        row: &ExecutionRow,
        predicate: &Predicate,
    ) -> Result<bool> {
        let plan = match ir::PredicatePlan::new(predicate.clone()) {
            Ok(plan) => plan,
            Err(ir::ExprPlanError::EmptyPredicateSet { op }) => {
                return Ok(op == ir::PredicateSetOp::And)
            }
            Err(error) => return Err(HelixDbError::Query(format!("predicate {error}"))),
        };
        self.eval_predicate_plan(row, &plan).await
    }
}

pub(in crate::execution::interpreter) fn property_value_is_in(
    value: &DbPropertyValue,
    values: &DbPropertyValue,
) -> bool {
    match values {
        DbPropertyValue::Array(values) => values.iter().any(|item| item.eq_value(value)),
        DbPropertyValue::I64Array(values) => values
            .iter()
            .any(|item| DbPropertyValue::I64(*item).eq_value(value)),
        DbPropertyValue::F64Array(values) => values
            .iter()
            .any(|item| DbPropertyValue::F64(*item).eq_value(value)),
        DbPropertyValue::F32Array(values) => values
            .iter()
            .any(|item| DbPropertyValue::F32(f64::from(*item)).eq_value(value)),
        DbPropertyValue::StringArray(values) => values
            .iter()
            .any(|item| DbPropertyValue::String(item.clone()).eq_value(value)),
        other @ (DbPropertyValue::Null
        | DbPropertyValue::Bool(_)
        | DbPropertyValue::I64(_)
        | DbPropertyValue::DateTime(_)
        | DbPropertyValue::F64(_)
        | DbPropertyValue::F32(_)
        | DbPropertyValue::String(_)
        | DbPropertyValue::Bytes(_)
        | DbPropertyValue::Object(_)) => other.eq_value(value),
    }
}
