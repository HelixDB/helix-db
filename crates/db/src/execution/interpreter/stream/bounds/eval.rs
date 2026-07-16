//! Stream bound expression evaluation contracts.

use helix_ast::expr::Expr;
use helix_ast::value::PropertyValue as AstPropertyValue;

use super::super::values::param_value_from;
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::stream::bounds) fn stream_bound(
        &self,
        count: &ir::StreamBoundPlan,
    ) -> Result<usize> {
        eval_stream_bound(count, &self.params)
    }

    pub(in crate::execution::interpreter::stream::bounds) fn stream_range(
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

pub(in crate::execution::interpreter::stream) fn eval_stream_bound(
    count: &ir::StreamBoundPlan,
    params: &context::ParamBindings,
) -> Result<usize> {
    match count {
        ir::StreamBoundPlan::Literal(count) => Ok(*count),
        ir::StreamBoundPlan::Expr(expr) => {
            let value = eval_bound_expr(expr.expr(), params)?;
            usize::try_from(value).map_err(|_| {
                HelixDbError::Query(format!("stream bound expression returned {value}"))
            })
        }
    }
}

pub(in crate::execution::interpreter::stream::bounds) fn eval_bound_expr(
    expr: &Expr,
    params: &context::ParamBindings,
) -> Result<i64> {
    match expr {
        Expr::Param(name) => {
            let name = ir::NonEmptyString::new(name.clone()).ok_or_else(|| {
                HelixDbError::Query("stream bound parameter name must not be empty".to_string())
            })?;
            param_value_from(params, &name)?
                .as_i64()
                .ok_or_else(|| HelixDbError::Query(format!("parameter `{name}` is not an i64")))
        }
        Expr::Constant(AstPropertyValue::I64(value)) => Ok(*value),
        Expr::Property(_)
        | Expr::Id
        | Expr::Timestamp
        | Expr::DateTimeNow
        | Expr::Constant(_)
        | Expr::Add { .. }
        | Expr::Sub { .. }
        | Expr::Mul { .. }
        | Expr::Div { .. }
        | Expr::Mod { .. }
        | Expr::Neg { .. }
        | Expr::Case { .. } => Err(HelixDbError::Query(format!(
            "unsupported stream bound expression {expr:?}"
        ))),
    }
}
