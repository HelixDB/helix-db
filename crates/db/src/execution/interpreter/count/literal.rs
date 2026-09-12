//! Native equality literals stay admitted from conversion through lookup and
//! authoritative verification. Borrowing the owner cannot detach its guard.
use super::{stream, DbPropertyValue};
use crate::{error::Result, query_resources};
use helix_planner::exec;

pub(super) struct Value {
    value: DbPropertyValue,
    _memory: Option<query_resources::Reservation>,
}
impl Value {
    pub(super) fn new(
        value: &exec::ExecIndexedEqualityValue,
        budget: Option<&query_resources::Budget>,
    ) -> Result<Self> {
        let memory = budget
            .map(|budget| budget.reserve(copied_bytes(value)))
            .transpose()?;
        Ok(Self {
            value: stream::ast_to_db_value(value.literal().as_property_value().clone()),
            _memory: memory,
        })
    }
}
impl std::ops::Deref for Value {
    type Target = DbPropertyValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

pub(super) struct Batch {
    values: Vec<DbPropertyValue>,
    _memory: Option<query_resources::Reservation>,
}
impl Batch {
    pub(super) fn new(
        values: &[exec::ExecIndexedEqualityValue],
        budget: Option<&query_resources::Budget>,
    ) -> Result<Self> {
        let bytes = values.iter().fold(
            values.len().saturating_mul(size_of::<DbPropertyValue>()),
            |bytes, value| bytes.saturating_add(copied_bytes(value)),
        );
        let memory = budget.map(|budget| budget.reserve(bytes)).transpose()?;
        let mut output = Vec::with_capacity(values.len());
        output.extend(
            values
                .iter()
                .map(|value| stream::ast_to_db_value(value.literal().as_property_value().clone())),
        );
        Ok(Self {
            values: output,
            _memory: memory,
        })
    }
}
impl std::ops::Deref for Batch {
    type Target = [DbPropertyValue];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

fn copied_bytes(value: &exec::ExecIndexedEqualityValue) -> usize {
    use helix_ast::value::PropertyValue as P;
    // Clone copies live elements, not spare source capacity. Conversion moves
    // these flat payloads unchanged; nested values cannot enter this contract.
    match value.literal().as_property_value() {
        P::Bool(_) | P::I64(_) | P::DateTime(_) | P::F64(_) | P::F32(_) => 0,
        P::String(value) => value.len(),
        P::Bytes(value) => value.len(),
        P::I64Array(values) => values.len().saturating_mul(size_of::<i64>()),
        P::F64Array(values) => values.len().saturating_mul(size_of::<f64>()),
        P::F32Array(values) => values.len().saturating_mul(size_of::<f32>()),
        P::StringArray(values) => values.iter().fold(
            values.len().saturating_mul(size_of::<String>()),
            |bytes, value| bytes.saturating_add(value.len()),
        ),
        P::Null | P::Array(_) | P::Object(_) => {
            unreachable!("exact indexed literals are nonnull and flat")
        }
    }
}

#[cfg(test)]
mod tests;
