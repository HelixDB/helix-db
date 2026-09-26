//! Scalar terminal sequence contracts.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeSet;

use crate::encoding::property::property_value::total_cmp_objects;

use super::*;

pub(in crate::execution::interpreter::stream) fn scalar_items(
    value: ExecutionValue,
) -> Vec<ExecutionScalar> {
    match value {
        ExecutionValue::Count(count) => vec![count_scalar(count)],
        ExecutionValue::Bool(value) => vec![ExecutionScalar::Value(DbPropertyValue::Bool(value))],
        ExecutionValue::Scalars(values) => values,
        ExecutionValue::Stream(_)
        | ExecutionValue::FoldedStream(_)
        | ExecutionValue::IndexDdlReceipt(_)
        | ExecutionValue::IndexOperationStatus(_) => {
            unreachable!("scalar_items is only called for scalar execution values")
        }
    }
}

pub(in crate::execution::interpreter::stream) fn limit_scalars(
    mut values: Vec<ExecutionScalar>,
    count: usize,
) -> Vec<ExecutionScalar> {
    values.truncate(count);
    values
}

pub(in crate::execution::interpreter::stream) fn skip_scalars(
    values: Vec<ExecutionScalar>,
    count: usize,
) -> Vec<ExecutionScalar> {
    values.into_iter().skip(count).collect()
}

pub(in crate::execution::interpreter::stream) fn slice_scalars(
    values: Vec<ExecutionScalar>,
    start: usize,
    end: usize,
) -> Vec<ExecutionScalar> {
    values
        .into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

/// Order-preserving DISTINCT over scalar outputs; the first-seen value wins.
pub(in crate::execution::interpreter::stream) fn distinct_scalars(
    values: Vec<ExecutionScalar>,
) -> Vec<ExecutionScalar> {
    let mut seen = BTreeSet::new();
    let keep: Vec<bool> = values
        .iter()
        .map(|value| seen.insert(DistinctKey(value)))
        .collect();
    values
        .into_iter()
        .zip(keep)
        .filter_map(|(value, keep)| keep.then_some(value))
        .collect()
}

fn count_scalar(count: usize) -> ExecutionScalar {
    ExecutionScalar::Value(DbPropertyValue::I64(count.try_into().unwrap_or(i64::MAX)))
}

/// DISTINCT identity for scalar outputs: property values compare by storage
/// `total_order` (CanonicalNumber for numerics, so `42`, `42.0` and `42.0f32` are one
/// value), projected objects compare entry-wise the same way, ids and strings compare
/// structurally. This is the identity `WHERE`, `ORDER BY`, GROUP BY and secondary
/// indexes already use. Debug strings are not an identity.
///
/// The key wraps either a borrowed scalar (dedup over an existing sequence) or an
/// owned one (dedup while producing a sequence, retaining only first occurrences).
pub(in crate::execution::interpreter) struct DistinctKey<T>(
    pub(in crate::execution::interpreter) T,
);

impl<T: Borrow<ExecutionScalar>> PartialEq for DistinctKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: Borrow<ExecutionScalar>> Eq for DistinctKey<T> {}

impl<T: Borrow<ExecutionScalar>> PartialOrd for DistinctKey<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Borrow<ExecutionScalar>> Ord for DistinctKey<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let rank = |value: &ExecutionScalar| match value {
            ExecutionScalar::NodeId(_) => 0_u8,
            ExecutionScalar::EdgeId(_) => 1,
            ExecutionScalar::String(_) => 2,
            ExecutionScalar::Value(_) => 3,
            ExecutionScalar::Object(_) => 4,
        };
        let (this, that) = (self.0.borrow(), other.0.borrow());
        rank(this)
            .cmp(&rank(that))
            .then_with(|| match (this, that) {
                (ExecutionScalar::NodeId(left), ExecutionScalar::NodeId(right))
                | (ExecutionScalar::EdgeId(left), ExecutionScalar::EdgeId(right)) => {
                    left.cmp(right)
                }
                (ExecutionScalar::String(left), ExecutionScalar::String(right)) => left.cmp(right),
                (ExecutionScalar::Value(left), ExecutionScalar::Value(right)) => {
                    left.total_order(right)
                }
                (ExecutionScalar::Object(left), ExecutionScalar::Object(right)) => {
                    total_cmp_objects(left, right)
                }
                _ => Ordering::Equal,
            })
    }
}
