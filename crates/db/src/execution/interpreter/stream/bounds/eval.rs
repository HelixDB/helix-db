//! Stream bound expression evaluation contracts.

use helix_ast::query;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::stream::bounds) fn stream_bound(
        &self,
        count: &ir::StreamBoundPlan,
    ) -> Result<usize> {
        eval_stream_bound(count, &self.params)
    }

    pub(in crate::execution::interpreter) fn stream_range(
        &self,
        range: &ir::StreamRangePlan,
    ) -> Result<(usize, usize)> {
        match range {
            ir::StreamRangePlan::Literal(range) => Ok((range.start(), range.end())),
            ir::StreamRangePlan::Dynamic(range) => Ok((
                self.stream_bound(range.start())?,
                self.stream_bound(range.end())?,
            )),
        }
    }
}

pub(in crate::execution::interpreter) fn eval_stream_bound(
    count: &ir::StreamBoundPlan,
    params: &context::ParamBindings,
) -> Result<usize> {
    match count {
        ir::StreamBoundPlan::Literal(count) => Ok(*count),
        ir::StreamBoundPlan::Expr(expr) => {
            let ir::native::Expression::Parameter(name) = expr.expression_plan().resolved() else {
                // Preserve the native diagnostic payload, but never evaluate or
                // lower the compatibility AST on the runtime path.
                return Err(HelixDbError::Query(format!(
                    "unsupported stream bound expression {:?}",
                    expr.expr()
                )));
            };
            // Borrow before checking the scalar type. A rejected collection must
            // not be cloned or converted merely to discover it is not an i64.
            // Property-compatible bindings retain precedence, including errors.
            let value = match params.values.get(name.as_str()) {
                Some(value) => value.as_i64(),
                None => match params.query_values.get(name.as_str()) {
                    Some(query::QueryValue::I64(value)) => Some(*value),
                    Some(_) => None,
                    None => {
                        return Err(HelixDbError::Query(format!(
                            "parameter `{name}` is not bound"
                        )))
                    }
                },
            }
            .ok_or_else(|| HelixDbError::Query(format!("parameter `{name}` is not an i64")))?;
            usize::try_from(value).map_err(|_| {
                HelixDbError::Query(format!("stream bound expression returned {value}"))
            })
        }
    }
}
