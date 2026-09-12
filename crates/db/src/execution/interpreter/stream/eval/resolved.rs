//! Shared scalar structure evaluated with the native frontend's resolved
//! operations. No AST reconstruction or lowering occurs in the row loop.
use super::*;
use helix_planner::ir::native;
use std::cmp::Ordering;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn eval_expr_plan(
        &self,
        row: &ExecutionRow,
        plan: &ir::ExprPlan,
    ) -> Result<DbPropertyValue> {
        let mut resolver = RowValueResolver::new(self);
        // Keep the recursive scalar future out of every enclosing access frame.
        Box::pin(self.eval_resolved(row, plan.resolved(), &mut resolver)).await
    }

    pub(in crate::execution::interpreter) async fn eval_predicate_plan(
        &self,
        row: &ExecutionRow,
        plan: &ir::PredicatePlan,
    ) -> Result<bool> {
        self.eval_resolved_predicate(row, plan.resolved()).await
    }

    async fn eval_resolved_predicate(
        &self,
        row: &ExecutionRow,
        expression: &native::Expression,
    ) -> Result<bool> {
        let mut resolver = RowValueResolver::new(self);
        let DbPropertyValue::Bool(value) =
            Box::pin(self.eval_resolved(row, expression, &mut resolver)).await?
        else {
            return Err(HelixDbError::InvariantViolation(
                "native predicate resolved to a non-boolean".into(),
            ));
        };
        Ok(value)
    }

    pub(in crate::execution::interpreter) async fn select_native_rows(
        &self,
        rows: Vec<ExecutionRow>,
        predicate: &ir::PredicatePlan,
    ) -> Result<Vec<ExecutionRow>> {
        let mut evaluator = NativeSelection {
            context: self,
            output: Vec::new(),
        };
        Box::pin(predicate.program().select(rows, &mut evaluator)).await?;
        Ok(evaluator.output)
    }

    pub(in crate::execution::interpreter::stream) async fn eval_resolved(
        &self,
        row: &ExecutionRow,
        expression: &native::Expression,
        resolver: &mut RowValueResolver<'_, 'db>,
    ) -> Result<DbPropertyValue> {
        use native::{Binary as B, Expression as E, Function as F, Unary as U};
        match expression {
            E::Literal(value) => Ok(super::super::values::ast_to_db_value(value.clone())),
            E::Parameter(name) => self
                .param_value(&ir::NonEmptyString::new(name.clone()).expect("validated parameter")),
            E::Property(current, key) => {
                assert_eq!(
                    current.as_ref(),
                    &E::Slot(native::CURRENT),
                    "native property uses current slot"
                );
                let key = ir::NonEmptyString::new(key.clone()).expect("validated property");
                Ok(resolver
                    .row_property(row, &key)
                    .await?
                    .unwrap_or(DbPropertyValue::Null))
            }
            E::Unary(operation, value) => {
                let value = Box::pin(self.eval_resolved(row, value, resolver)).await?;
                match operation {
                    U::IsNull => Ok(DbPropertyValue::Bool(matches!(
                        value,
                        DbPropertyValue::Null
                    ))),
                    U::IsNotNull => Ok(DbPropertyValue::Bool(!matches!(
                        value,
                        DbPropertyValue::Null
                    ))),
                    U::Not => {
                        let DbPropertyValue::Bool(value) = value else {
                            unreachable!("validated boolean operand")
                        };
                        Ok(DbPropertyValue::Bool(!value))
                    }
                    U::Negate => {
                        if let Some(value) = value.as_i64() {
                            return value.checked_neg().map(DbPropertyValue::I64).ok_or_else(
                                || HelixDbError::Query("neg expression overflows i64".into()),
                            );
                        }
                        value
                            .as_f64()
                            .map(|value| DbPropertyValue::F64(-value))
                            .ok_or_else(|| {
                                HelixDbError::Query("neg expression must be numeric".into())
                            })
                    }
                }
            }
            E::Binary(operation, left, right) => {
                let left = Box::pin(self.eval_resolved(row, left, resolver)).await?;
                if *operation == B::IntegerRemainder {
                    let left = left.as_i64().ok_or_else(|| {
                        HelixDbError::Query("mod left expression must be i64".into())
                    })?;
                    let right = Box::pin(self.eval_resolved(row, right, resolver))
                        .await?
                        .as_i64()
                        .ok_or_else(|| {
                            HelixDbError::Query("mod right expression must be i64".into())
                        })?;
                    return left
                        .checked_rem(right)
                        .map(DbPropertyValue::I64)
                        .ok_or_else(|| {
                            HelixDbError::Query(
                                "mod expression has zero divisor or overflows i64".into(),
                            )
                        });
                }
                let right = Box::pin(self.eval_resolved(row, right, resolver)).await?;
                Ok(match operation {
                    B::FloatAdd => return self.numeric_binary_values(left, right, |a, b| a + b),
                    B::FloatSubtract => {
                        return self.numeric_binary_values(left, right, |a, b| a - b)
                    }
                    B::FloatMultiply => {
                        return self.numeric_binary_values(left, right, |a, b| a * b)
                    }
                    B::FloatDivide => return self.numeric_binary_values(left, right, |a, b| a / b),
                    B::IntegerRemainder => {
                        unreachable!("remainder evaluates operand types before the next operand")
                    }
                    B::Equal => DbPropertyValue::Bool(left.eq_value(&right)),
                    B::NotEqual => DbPropertyValue::Bool(!left.eq_value(&right)),
                    B::Less => DbPropertyValue::Bool(left.compare(&right) == Some(Ordering::Less)),
                    B::LessEqual => DbPropertyValue::Bool(matches!(
                        left.compare(&right),
                        Some(Ordering::Less | Ordering::Equal)
                    )),
                    B::Greater => {
                        DbPropertyValue::Bool(left.compare(&right) == Some(Ordering::Greater))
                    }
                    B::GreaterEqual => DbPropertyValue::Bool(matches!(
                        left.compare(&right),
                        Some(Ordering::Greater | Ordering::Equal)
                    )),
                    B::StartsWith => DbPropertyValue::Bool(
                        left.as_str()
                            .zip(right.as_str())
                            .is_some_and(|(a, b)| a.starts_with(b)),
                    ),
                    B::EndsWith => DbPropertyValue::Bool(
                        left.as_str()
                            .zip(right.as_str())
                            .is_some_and(|(a, b)| a.ends_with(b)),
                    ),
                    B::Contains => DbPropertyValue::Bool(
                        left.as_str()
                            .zip(right.as_str())
                            .is_some_and(|(a, b)| a.contains(b)),
                    ),
                    B::MembershipOrScalarEquality => {
                        DbPropertyValue::Bool(property_value_is_in(&left, &right))
                    }
                })
            }
            E::Function(function, arguments) => match function {
                F::SaturatingId => {
                    assert_eq!(
                        arguments.as_slice(),
                        &[E::Slot(native::CURRENT)],
                        "native id input"
                    );
                    row.current
                        .as_ref()
                        .map(|element| {
                            DbPropertyValue::I64(element.id().try_into().unwrap_or(i64::MAX))
                        })
                        .ok_or_else(|| {
                            HelixDbError::Query("id expression has no current element".into())
                        })
                }
                F::Timestamp => Ok(DbPropertyValue::I64(chrono::Utc::now().timestamp_millis())),
                F::DateTimeNow => Ok(DbPropertyValue::DateTime(
                    chrono::Utc::now().timestamp_millis(),
                )),
                F::HasProperty(key) => {
                    assert_eq!(
                        arguments.as_slice(),
                        &[E::Slot(native::CURRENT)],
                        "native property input"
                    );
                    let key = ir::NonEmptyString::new(key.clone()).expect("validated property");
                    Ok(DbPropertyValue::Bool(
                        resolver.row_property(row, &key).await?.is_some(),
                    ))
                }
                F::Between => {
                    let [value, min, max] = arguments.as_slice() else {
                        unreachable!("validated range arity")
                    };
                    let value = Box::pin(self.eval_resolved(row, value, resolver)).await?;
                    let min = Box::pin(self.eval_resolved(row, min, resolver)).await?;
                    if !matches!(
                        value.compare(&min),
                        Some(Ordering::Greater | Ordering::Equal)
                    ) {
                        return Ok(DbPropertyValue::Bool(false));
                    }
                    let max = Box::pin(self.eval_resolved(row, max, resolver)).await?;
                    Ok(DbPropertyValue::Bool(matches!(
                        value.compare(&max),
                        Some(Ordering::Less | Ordering::Equal)
                    )))
                }
                F::All | F::Any => {
                    let all = matches!(function, F::All);
                    for child in arguments {
                        let DbPropertyValue::Bool(value) =
                            Box::pin(self.eval_resolved(row, child, resolver)).await?
                        else {
                            unreachable!("validated boolean operand")
                        };
                        if value != all {
                            return Ok(DbPropertyValue::Bool(value));
                        }
                    }
                    Ok(DbPropertyValue::Bool(all))
                }
            },
            E::Case {
                branches,
                otherwise,
            } => {
                for (condition, value) in branches {
                    let DbPropertyValue::Bool(condition) =
                        Box::pin(self.eval_resolved(row, condition, resolver)).await?
                    else {
                        unreachable!("validated CASE predicate")
                    };
                    if condition {
                        return Box::pin(self.eval_resolved(row, value, resolver)).await;
                    }
                }
                Box::pin(self.eval_resolved(row, otherwise, resolver)).await
            }
            E::Slot(_)
            | E::Index(..)
            | E::Slice { .. }
            | E::Aggregate { .. }
            | E::List(_)
            | E::Map(_)
            | E::HasLabel(..) => {
                unreachable!("native adapter only constructs supported expression contracts")
            }
        }
    }
}

struct NativeSelection<'a, 'db> {
    context: &'a ExecutionContext<'db>,
    output: Vec<ExecutionRow>,
}
impl helix_planner::relational::SelectionEvaluator<ir::ResolvedPredicate>
    for NativeSelection<'_, '_>
{
    type Row = ExecutionRow;
    type Error = HelixDbError;
    async fn evaluate(
        &mut self,
        row: &ExecutionRow,
        expression: &ir::ResolvedPredicate,
    ) -> Result<helix_planner::relational::Selection> {
        self.context.check_execution_deadline()?;
        let value = Box::pin(
            self.context
                .eval_resolved_predicate(row, expression.expression()),
        )
        .await?;
        Ok(Some(value).into())
    }
    fn retain(&mut self, row: ExecutionRow) -> Result<()> {
        self.output.push(row);
        Ok(())
    }
}
