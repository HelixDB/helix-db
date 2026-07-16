use helix_ast::value::PropertyValue;
use helix_planner::ir;

use super::super::{stream, ExecutionContext, ExecutionRow, ExecutionValue};
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::{HelixDbError, Result};

impl<'db> ExecutionContext<'db> {
    pub(super) fn with_sack(
        &self,
        input: ExecutionValue,
        initial: &PropertyValue,
    ) -> Result<ExecutionValue> {
        let value = stream::ast_to_db_value(initial.clone());
        Ok(ExecutionValue::Stream(
            self.stream_rows(input, "with_sack")?
                .into_iter()
                .map(|mut row| {
                    row.set_sack(value.clone());
                    row
                })
                .collect(),
        ))
    }

    pub(super) async fn sack_set(
        &self,
        input: ExecutionValue,
        property: &ir::NonEmptyString,
    ) -> Result<ExecutionValue> {
        let mut output = Vec::new();
        for mut row in self.stream_rows(input, "sack_set")? {
            match self.row_property(&row, property).await? {
                Some(value) => row.set_sack(value),
                None => row.clear_sack(),
            }
            output.push(row);
        }
        Ok(ExecutionValue::Stream(output))
    }

    pub(super) async fn sack_add(
        &self,
        input: ExecutionValue,
        property: &ir::NonEmptyString,
    ) -> Result<ExecutionValue> {
        let mut output = Vec::new();
        for mut row in self.stream_rows(input, "sack_add")? {
            if let Some(value) = self.row_property(&row, property).await? {
                row.set_sack(add_sack_value(row.sack.value(), &value)?);
            }
            output.push(row);
        }
        Ok(ExecutionValue::Stream(output))
    }

    pub(super) fn sack_get(&self, input: ExecutionValue) -> Result<ExecutionValue> {
        Ok(ExecutionValue::Stream(
            self.stream_rows(input, "sack_get")?
                .into_iter()
                .map(ExecutionRow::mark_sack_visible)
                .collect(),
        ))
    }
}

fn add_sack_value(
    sack: Option<&DbPropertyValue>,
    value: &DbPropertyValue,
) -> Result<DbPropertyValue> {
    let Some(sack) = sack else {
        return numeric_sack_value(value)
            .map(|_| value.clone())
            .ok_or_else(|| non_numeric_sack_error("property", value));
    };
    match (numeric_sack_value(sack), numeric_sack_value(value)) {
        (Some(SackNumber::I64(left)), Some(SackNumber::I64(right))) => left
            .checked_add(right)
            .map(DbPropertyValue::I64)
            .ok_or_else(|| HelixDbError::Query("sack_add overflowed i64".to_string())),
        (Some(left), Some(right)) => Ok(DbPropertyValue::F64(left.as_f64() + right.as_f64())),
        (None, _) => Err(non_numeric_sack_error("current sack", sack)),
        (_, None) => Err(non_numeric_sack_error("property", value)),
    }
}

#[derive(Debug, Clone, Copy)]
enum SackNumber {
    I64(i64),
    F64(f64),
}

impl SackNumber {
    fn as_f64(self) -> f64 {
        match self {
            Self::I64(value) => value as f64,
            Self::F64(value) => value,
        }
    }
}

fn numeric_sack_value(value: &DbPropertyValue) -> Option<SackNumber> {
    match value {
        DbPropertyValue::I64(value) => Some(SackNumber::I64(*value)),
        DbPropertyValue::F64(value) | DbPropertyValue::F32(value) => Some(SackNumber::F64(*value)),
        DbPropertyValue::Null
        | DbPropertyValue::Bool(_)
        | DbPropertyValue::DateTime(_)
        | DbPropertyValue::String(_)
        | DbPropertyValue::Bytes(_)
        | DbPropertyValue::I64Array(_)
        | DbPropertyValue::F64Array(_)
        | DbPropertyValue::F32Array(_)
        | DbPropertyValue::StringArray(_)
        | DbPropertyValue::Array(_)
        | DbPropertyValue::Object(_) => None,
    }
}

fn non_numeric_sack_error(kind: &'static str, value: &DbPropertyValue) -> HelixDbError {
    HelixDbError::Query(format!("sack_add expected numeric {kind}, got {value:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sack_addition_preserves_integer_results_and_promotes_mixed_numbers() {
        assert_eq!(
            add_sack_value(Some(&DbPropertyValue::I64(2)), &DbPropertyValue::I64(3)).unwrap(),
            DbPropertyValue::I64(5)
        );
        assert_eq!(
            add_sack_value(Some(&DbPropertyValue::I64(2)), &DbPropertyValue::F64(0.5)).unwrap(),
            DbPropertyValue::F64(2.5)
        );
        assert_eq!(
            add_sack_value(None, &DbPropertyValue::F32(1.25)).unwrap(),
            DbPropertyValue::F32(1.25)
        );
    }

    #[test]
    fn sack_addition_rejects_overflow_and_non_numeric_values() {
        assert!(matches!(
            add_sack_value(
                Some(&DbPropertyValue::I64(i64::MAX)),
                &DbPropertyValue::I64(1)
            ),
            Err(HelixDbError::Query(message)) if message == "sack_add overflowed i64"
        ));
        assert!(matches!(
            add_sack_value(None, &DbPropertyValue::String("bad".to_string())),
            Err(HelixDbError::Query(message)) if message.contains("numeric property")
        ));
        assert!(matches!(
            add_sack_value(
                Some(&DbPropertyValue::String("bad".to_string())),
                &DbPropertyValue::I64(1)
            ),
            Err(HelixDbError::Query(message)) if message.contains("numeric current sack")
        ));
        assert!(matches!(
            add_sack_value(
                Some(&DbPropertyValue::I64(1)),
                &DbPropertyValue::String("bad".to_string())
            ),
            Err(HelixDbError::Query(message)) if message.contains("numeric property")
        ));
    }
}
