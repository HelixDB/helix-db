//! Test entry point for exercising native syntax through production lowering.
use super::*;

#[cfg(test)]
impl ExecutionContext<'_> {
    pub(in crate::execution::interpreter) async fn eval_expr(
        &self,
        row: &ExecutionRow,
        expr: &Expr,
    ) -> Result<DbPropertyValue> {
        let plan = ir::ExprPlan::new(expr.clone()).map_err(|error| match error {
            ir::ExprPlanError::EmptyName {
                field: ir::NameField::Property,
            } => HelixDbError::Query("expression property name must not be empty".into()),
            ir::ExprPlanError::EmptyName {
                field: ir::NameField::Param,
            } => HelixDbError::Query("expression parameter name must not be empty".into()),
            error => HelixDbError::Query(error.to_string()),
        })?;
        self.eval_expr_plan(row, &plan).await
    }
}
