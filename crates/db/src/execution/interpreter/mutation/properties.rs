//! Property payload construction for executable mutations.

use super::super::stream::ast_to_db_value;
use super::contracts::reject_label_mutation;
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(super) async fn node_create_properties(
        &self,
        row: &ExecutionRow,
        label: &ir::NonEmptyString,
        assignments: &ir::PropertyAssignments,
    ) -> Result<Vec<Property>> {
        let mut properties = Vec::with_capacity(assignments.as_ref().len() + 1);
        properties.push(Property::new(
            "$label",
            DbPropertyValue::String(label.as_ref().to_string()),
        ));
        self.push_assignment_properties(row, assignments, &mut properties)
            .await?;
        Ok(properties)
    }

    pub(super) async fn edge_create_properties(
        &self,
        row: &ExecutionRow,
        label: &ir::NonEmptyString,
        assignments: &ir::PropertyAssignments,
    ) -> Result<Vec<Property>> {
        let mut properties = Vec::with_capacity(assignments.as_ref().len() + 1);
        properties.push(Property::new(
            "$label",
            DbPropertyValue::String(label.as_ref().to_string()),
        ));
        self.push_assignment_properties(row, assignments, &mut properties)
            .await?;
        Ok(properties)
    }

    async fn push_assignment_properties(
        &self,
        row: &ExecutionRow,
        assignments: &ir::PropertyAssignments,
        properties: &mut Vec<Property>,
    ) -> Result<()> {
        for (name, input) in assignments.as_ref() {
            reject_label_mutation(name)?;
            let value = self.property_input_value(row, input).await?;
            properties.push(Property::new(name.as_ref(), value));
        }
        Ok(())
    }

    pub(super) async fn property_input_value(
        &self,
        row: &ExecutionRow,
        input: &ir::PropertyInputPlan,
    ) -> Result<DbPropertyValue> {
        match input {
            ir::PropertyInputPlan::Value(value) => Ok(ast_to_db_value(value.clone())),
            ir::PropertyInputPlan::Expr(expr) => self.eval_expr(row, expr.expr().expr()).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use helix_ast::expr::Expr;
    use helix_planner::context;

    use super::super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn property_input_value_evaluates_runtime_expressions() {
        let db = test_support::open_db("mutation-property-input-expression").await;
        let param = test_support::name("value");
        let context =
            ExecutionContext::new(&db, context::ParamBindings::default().with_value(param, 7));
        let input = ir::PropertyInputPlan::Expr(
            ir::PropertyInputExprPlan::new(Expr::param("value"))
                .expect("parameter expression is valid"),
        );

        assert_eq!(
            context
                .property_input_value(&ExecutionRow::current(ElementRef::Node(1)), &input)
                .await
                .expect("runtime property expression evaluates"),
            DbPropertyValue::I64(7)
        );
    }
}
