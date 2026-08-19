//! Predicate evaluation contracts.

use std::cmp::Ordering;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn eval_predicate(
        &self,
        row: &ExecutionRow,
        predicate: &Predicate,
    ) -> Result<bool> {
        let mut resolver = RowValueResolver::new(self);
        self.eval_predicate_with_resolver(row, predicate, &mut resolver)
            .await
    }

    pub(in crate::execution::interpreter::stream) async fn eval_predicate_with_resolver(
        &self,
        row: &ExecutionRow,
        predicate: &Predicate,
        resolver: &mut RowValueResolver<'_, 'db>,
    ) -> Result<bool> {
        match predicate {
            Predicate::Eq { left, right } => {
                Ok(Box::pin(self.eval_expr_with_resolver(row, left, resolver))
                    .await?
                    .eq_value(&Box::pin(self.eval_expr_with_resolver(row, right, resolver)).await?))
            }
            Predicate::Neq { left, right } => {
                Ok(!Box::pin(self.eval_expr_with_resolver(row, left, resolver))
                    .await?
                    .eq_value(&Box::pin(self.eval_expr_with_resolver(row, right, resolver)).await?))
            }
            Predicate::Gt { left, right }
            | Predicate::Compare {
                left,
                op: CompareOp::Gt,
                right,
            } => Ok(Box::pin(self.eval_expr_with_resolver(row, left, resolver))
                .await?
                .compare(&Box::pin(self.eval_expr_with_resolver(row, right, resolver)).await?)
                == Some(Ordering::Greater)),
            Predicate::Gte { left, right }
            | Predicate::Compare {
                left,
                op: CompareOp::Gte,
                right,
            } => Ok(matches!(
                Box::pin(self.eval_expr_with_resolver(row, left, resolver))
                    .await?
                    .compare(&Box::pin(self.eval_expr_with_resolver(row, right, resolver)).await?),
                Some(Ordering::Greater | Ordering::Equal)
            )),
            Predicate::Lt { left, right }
            | Predicate::Compare {
                left,
                op: CompareOp::Lt,
                right,
            } => Ok(Box::pin(self.eval_expr_with_resolver(row, left, resolver))
                .await?
                .compare(&Box::pin(self.eval_expr_with_resolver(row, right, resolver)).await?)
                == Some(Ordering::Less)),
            Predicate::Lte { left, right }
            | Predicate::Compare {
                left,
                op: CompareOp::Lte,
                right,
            } => Ok(matches!(
                Box::pin(self.eval_expr_with_resolver(row, left, resolver))
                    .await?
                    .compare(&Box::pin(self.eval_expr_with_resolver(row, right, resolver)).await?),
                Some(Ordering::Less | Ordering::Equal)
            )),
            Predicate::Compare {
                left,
                op: CompareOp::Eq,
                right,
            } => Ok(Box::pin(self.eval_expr_with_resolver(row, left, resolver))
                .await?
                .eq_value(&Box::pin(self.eval_expr_with_resolver(row, right, resolver)).await?)),
            Predicate::Compare {
                left,
                op: CompareOp::Neq,
                right,
            } => Ok(!Box::pin(self.eval_expr_with_resolver(row, left, resolver))
                .await?
                .eq_value(&Box::pin(self.eval_expr_with_resolver(row, right, resolver)).await?)),
            Predicate::Between { value, min, max } => {
                let value = Box::pin(self.eval_expr_with_resolver(row, value, resolver)).await?;
                Ok(matches!(
                    value.compare(
                        &Box::pin(self.eval_expr_with_resolver(row, min, resolver)).await?
                    ),
                    Some(Ordering::Greater | Ordering::Equal)
                ) && matches!(
                    value.compare(
                        &Box::pin(self.eval_expr_with_resolver(row, max, resolver)).await?
                    ),
                    Some(Ordering::Less | Ordering::Equal)
                ))
            }
            Predicate::HasKey { property } => {
                let property = non_empty_predicate_property(property)?;
                Ok(resolver.row_property(row, &property).await?.is_some())
            }
            Predicate::IsNull { property } => {
                let property = non_empty_predicate_property(property)?;
                Ok(resolver
                    .row_property(row, &property)
                    .await?
                    .is_none_or(|value| matches!(value, DbPropertyValue::Null)))
            }
            Predicate::IsNotNull { property } => {
                let property = non_empty_predicate_property(property)?;
                Ok(resolver
                    .row_property(row, &property)
                    .await?
                    .is_some_and(|value| !matches!(value, DbPropertyValue::Null)))
            }
            Predicate::StartsWith { value, prefix } => {
                Ok(Box::pin(self.eval_expr_with_resolver(row, value, resolver))
                    .await?
                    .as_str()
                    .zip(
                        Box::pin(self.eval_expr_with_resolver(row, prefix, resolver))
                            .await?
                            .as_str(),
                    )
                    .is_some_and(|(value, prefix)| value.starts_with(prefix)))
            }
            Predicate::EndsWith { value, suffix } => {
                Ok(Box::pin(self.eval_expr_with_resolver(row, value, resolver))
                    .await?
                    .as_str()
                    .zip(
                        Box::pin(self.eval_expr_with_resolver(row, suffix, resolver))
                            .await?
                            .as_str(),
                    )
                    .is_some_and(|(value, suffix)| value.ends_with(suffix)))
            }
            Predicate::Contains { value, substring } => {
                Ok(Box::pin(self.eval_expr_with_resolver(row, value, resolver))
                    .await?
                    .as_str()
                    .zip(
                        Box::pin(self.eval_expr_with_resolver(row, substring, resolver))
                            .await?
                            .as_str(),
                    )
                    .is_some_and(|(value, substring)| value.contains(substring)))
            }
            Predicate::IsIn { value, values } => {
                let value = Box::pin(self.eval_expr_with_resolver(row, value, resolver)).await?;
                Ok(
                    match Box::pin(self.eval_expr_with_resolver(row, values, resolver)).await? {
                        DbPropertyValue::Array(values) => {
                            values.iter().any(|item| item.eq_value(&value))
                        }
                        DbPropertyValue::I64Array(values) => values
                            .iter()
                            .any(|item| DbPropertyValue::I64(*item).eq_value(&value)),
                        DbPropertyValue::StringArray(values) => values
                            .iter()
                            .any(|item| DbPropertyValue::String(item.clone()).eq_value(&value)),
                        other @ (DbPropertyValue::Null
                        | DbPropertyValue::Bool(_)
                        | DbPropertyValue::I64(_)
                        | DbPropertyValue::DateTime(_)
                        | DbPropertyValue::F64(_)
                        | DbPropertyValue::F32(_)
                        | DbPropertyValue::String(_)
                        | DbPropertyValue::Bytes(_)
                        | DbPropertyValue::F64Array(_)
                        | DbPropertyValue::F32Array(_)
                        | DbPropertyValue::Object(_)) => other.eq_value(&value),
                    },
                )
            }
            Predicate::And { predicates } => {
                for predicate in predicates {
                    if !Box::pin(self.eval_predicate_with_resolver(row, predicate, resolver))
                        .await?
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Predicate::Or { predicates } => {
                for predicate in predicates {
                    if Box::pin(self.eval_predicate_with_resolver(row, predicate, resolver)).await?
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Predicate::Not { predicate } => {
                Ok(!Box::pin(self.eval_predicate_with_resolver(row, predicate, resolver)).await?)
            }
        }
    }
}

fn non_empty_predicate_property(value: &str) -> Result<ir::NonEmptyString> {
    ir::NonEmptyString::new(value.to_string())
        .ok_or_else(|| HelixDbError::Query("predicate property name must not be empty".to_string()))
}
