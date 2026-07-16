//! Search query input and limit evaluation contracts.

use helix_planner::ir;

use super::tenant::search_eval_row;
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::access::search) async fn search_query_vector(
        &self,
        input: &ir::VectorQueryInputPlan,
    ) -> Result<Vec<f32>> {
        let vector = match input {
            ir::VectorQueryInputPlan::Vector(vector) => vector
                .as_ref()
                .iter()
                .map(|component| component.get())
                .collect(),
            ir::VectorQueryInputPlan::Expr(expr) => {
                db_value_to_query_vector(self.eval_expr(&search_eval_row(), expr.expr()).await?)?
            }
        };
        validate_query_vector(vector)
    }

    pub(in crate::execution::interpreter::access::search) async fn search_query_text(
        &self,
        input: &ir::TextQueryInputPlan,
    ) -> Result<String> {
        match input {
            ir::TextQueryInputPlan::Text(text) => Ok(text.as_ref().to_string()),
            ir::TextQueryInputPlan::Expr(expr) => {
                let value = self.eval_expr(&search_eval_row(), expr.expr()).await?;
                let Some(text) = value.as_str() else {
                    return Err(HelixDbError::Query(
                        "text search query expression must evaluate to a string".to_string(),
                    ));
                };
                if text.is_empty() {
                    return Err(HelixDbError::Query(
                        "text search query expression must not be empty".to_string(),
                    ));
                }
                Ok(text.to_string())
            }
        }
    }

    pub(in crate::execution::interpreter::access::search) async fn search_limit(
        &self,
        limit: &ir::SearchLimitPlan,
    ) -> Result<usize> {
        match limit {
            ir::SearchLimitPlan::Literal(limit) => Ok(limit.get()),
            ir::SearchLimitPlan::Expr(expr) => {
                let value = self
                    .eval_expr(&search_eval_row(), expr.expr())
                    .await?
                    .as_i64()
                    .ok_or_else(|| {
                        HelixDbError::Query(
                            "search limit expression must evaluate to an i64".to_string(),
                        )
                    })?;
                usize::try_from(value)
                    .ok()
                    .and_then(std::num::NonZeroUsize::new)
                    .map(|value| value.get())
                    .ok_or_else(|| {
                        HelixDbError::Query(format!(
                            "search limit expression returned non-positive value {value}"
                        ))
                    })
            }
        }
    }
}

pub(in crate::execution::interpreter::access) fn db_value_to_query_vector(
    value: DbPropertyValue,
) -> Result<Vec<f32>> {
    let vector = match value {
        DbPropertyValue::F32Array(values) => values,
        DbPropertyValue::F64Array(values) => values.into_iter().map(|value| value as f32).collect(),
        DbPropertyValue::I64Array(values) => values.into_iter().map(|value| value as f32).collect(),
        DbPropertyValue::Array(values) => values
            .into_iter()
            .map(numeric_property_to_f32)
            .collect::<Result<Vec<_>>>()?,
        other @ (DbPropertyValue::Null
        | DbPropertyValue::Bool(_)
        | DbPropertyValue::I64(_)
        | DbPropertyValue::DateTime(_)
        | DbPropertyValue::F64(_)
        | DbPropertyValue::F32(_)
        | DbPropertyValue::String(_)
        | DbPropertyValue::Bytes(_)
        | DbPropertyValue::StringArray(_)
        | DbPropertyValue::Object(_)) => {
            return Err(HelixDbError::Query(format!(
                "vector search query expression must evaluate to a numeric array, got {other:?}"
            )));
        }
    };
    validate_query_vector(vector)
}

fn numeric_property_to_f32(value: DbPropertyValue) -> Result<f32> {
    match value {
        DbPropertyValue::I64(value) => Ok(value as f32),
        DbPropertyValue::F64(value) | DbPropertyValue::F32(value) => Ok(value as f32),
        other @ (DbPropertyValue::Null
        | DbPropertyValue::Bool(_)
        | DbPropertyValue::DateTime(_)
        | DbPropertyValue::String(_)
        | DbPropertyValue::Bytes(_)
        | DbPropertyValue::I64Array(_)
        | DbPropertyValue::F64Array(_)
        | DbPropertyValue::F32Array(_)
        | DbPropertyValue::StringArray(_)
        | DbPropertyValue::Array(_)
        | DbPropertyValue::Object(_)) => Err(HelixDbError::Query(format!(
            "vector search query array item must be numeric, got {other:?}"
        ))),
    }
}

pub(in crate::execution::interpreter::access) fn validate_query_vector(
    vector: Vec<f32>,
) -> Result<Vec<f32>> {
    if vector.is_empty() {
        return Err(HelixDbError::Query(
            "vector search query must not be empty".to_string(),
        ));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(HelixDbError::Query(
            "vector search query components must be finite".to_string(),
        ));
    }
    Ok(vector)
}

#[cfg(test)]
mod tests {
    use helix_ast::expr::Expr;
    use helix_planner::context::ParamBindings;

    use super::super::super::super::test_support;
    use super::*;

    #[test]
    fn vector_query_conversion_accepts_numeric_array_shapes() {
        assert_eq!(
            db_value_to_query_vector(DbPropertyValue::F32Array(vec![1.25, 2.5])).unwrap(),
            vec![1.25, 2.5]
        );
        assert_eq!(
            db_value_to_query_vector(DbPropertyValue::F64Array(vec![1.0, 2.5])).unwrap(),
            vec![1.0, 2.5]
        );
        assert_eq!(
            db_value_to_query_vector(DbPropertyValue::I64Array(vec![1, 2])).unwrap(),
            vec![1.0, 2.0]
        );
        assert_eq!(
            db_value_to_query_vector(DbPropertyValue::Array(vec![
                DbPropertyValue::I64(1),
                DbPropertyValue::F64(2.5),
                DbPropertyValue::F32(3.25),
            ]))
            .unwrap(),
            vec![1.0, 2.5, 3.25]
        );
    }

    #[test]
    fn vector_query_conversion_rejects_invalid_shapes_and_components() {
        assert!(validate_query_vector(Vec::new()).is_err());
        assert!(validate_query_vector(vec![1.0, f32::INFINITY]).is_err());
        assert!(
            db_value_to_query_vector(DbPropertyValue::String("not a vector".to_string())).is_err()
        );
        assert!(
            db_value_to_query_vector(DbPropertyValue::Array(vec![DbPropertyValue::String(
                "bad item".to_string(),
            )]))
            .is_err()
        );
        assert!(db_value_to_query_vector(DbPropertyValue::F32Array(vec![f32::NAN])).is_err());
    }

    #[tokio::test]
    async fn expression_inputs_reject_invalid_text_and_limit_values() {
        let db = test_support::open_db("search-input-expression-errors").await;
        let context = ExecutionContext::new(
            &db,
            ParamBindings::default()
                .with_value(test_support::name("text_number"), 7_i64)
                .with_value(test_support::name("text_empty"), String::new())
                .with_value(test_support::name("limit_bool"), true)
                .with_value(test_support::name("limit_zero"), 0_i64),
        );

        let text_number = ir::TextQueryInputPlan::Expr(
            ir::SearchQueryExprPlan::new(Expr::param("text_number")).unwrap(),
        );
        assert!(matches!(
            context.search_query_text(&text_number).await,
            Err(HelixDbError::Query(message)) if message.contains("must evaluate to a string")
        ));

        let text_empty = ir::TextQueryInputPlan::Expr(
            ir::SearchQueryExprPlan::new(Expr::param("text_empty")).unwrap(),
        );
        assert!(matches!(
            context.search_query_text(&text_empty).await,
            Err(HelixDbError::Query(message)) if message.contains("must not be empty")
        ));

        let limit_bool = ir::SearchLimitPlan::Expr(
            ir::SearchLimitExprPlan::new(Expr::param("limit_bool")).unwrap(),
        );
        assert!(matches!(
            context.search_limit(&limit_bool).await,
            Err(HelixDbError::Query(message)) if message.contains("must evaluate to an i64")
        ));

        let limit_zero = ir::SearchLimitPlan::Expr(
            ir::SearchLimitExprPlan::new(Expr::param("limit_zero")).unwrap(),
        );
        assert!(matches!(
            context.search_limit(&limit_zero).await,
            Err(HelixDbError::Query(message)) if message.contains("non-positive value 0")
        ));
    }
}
