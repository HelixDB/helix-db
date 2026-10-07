//! Stable literal-set construction. Ordering groups equal values; the caller's
//! equality contract decides whether a group is reflexive and may coalesce.
//! This is an internal construction order, not query ORDER BY or storage encoding.

use std::cmp::Ordering;

use helix_ast::value::PropertyValue;
use helix_value_semantics::CanonicalNumber;

#[cfg(test)]
#[path = "tests/literal_set.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/literal_membership.rs"]
mod membership_tests;

/// Native index domains retain literal widths. Scalar proofs identify top-level
/// numbers exactly across widths, while nested values retain their native types.
#[derive(Clone, Copy, Debug)]
pub(super) enum LiteralOrder {
    Typed,
    ScalarNumeric,
}

impl LiteralOrder {
    pub(super) fn compare(self, left: &PropertyValue, right: &PropertyValue) -> Ordering {
        let kind = self.kind(left).cmp(&self.kind(right));
        if kind != Ordering::Equal {
            return kind;
        }
        if matches!(self, Self::ScalarNumeric)
            && matches!(
                left,
                PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_)
            )
        {
            let number = |value: &PropertyValue| match value {
                PropertyValue::I64(value) => Some(CanonicalNumber::from_i64(*value)),
                PropertyValue::F64(value) => CanonicalNumber::from_f64(*value),
                PropertyValue::F32(value) => CanonicalNumber::from_f32(*value),
                _ => unreachable!("the shared numeric kind contains only numbers"),
            };
            return number(left).cmp(&number(right));
        }
        match (left, right) {
            (PropertyValue::Null, PropertyValue::Null) => Ordering::Equal,
            (PropertyValue::Bool(left), PropertyValue::Bool(right)) => left.cmp(right),
            (PropertyValue::I64(left), PropertyValue::I64(right))
            | (PropertyValue::DateTime(left), PropertyValue::DateTime(right)) => left.cmp(right),
            (PropertyValue::F64(left), PropertyValue::F64(right)) => compare_f64(*left, *right),
            (PropertyValue::F32(left), PropertyValue::F32(right)) => {
                compare_f64(f64::from(*left), f64::from(*right))
            }
            (PropertyValue::String(left), PropertyValue::String(right)) => left.cmp(right),
            (PropertyValue::Bytes(left), PropertyValue::Bytes(right)) => left.cmp(right),
            (PropertyValue::I64Array(left), PropertyValue::I64Array(right)) => left.cmp(right),
            (PropertyValue::F64Array(left), PropertyValue::F64Array(right)) => left
                .iter()
                .zip(right)
                .map(|(left, right)| compare_f64(*left, *right))
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or_else(|| left.len().cmp(&right.len())),
            (PropertyValue::F32Array(left), PropertyValue::F32Array(right)) => left
                .iter()
                .zip(right)
                .map(|(left, right)| compare_f64(f64::from(*left), f64::from(*right)))
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or_else(|| left.len().cmp(&right.len())),
            (PropertyValue::StringArray(left), PropertyValue::StringArray(right)) => {
                left.cmp(right)
            }
            (PropertyValue::Array(left), PropertyValue::Array(right)) => left
                .iter()
                .zip(right)
                .map(|(left, right)| Self::Typed.compare(left, right))
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or_else(|| left.len().cmp(&right.len())),
            (PropertyValue::Object(left), PropertyValue::Object(right)) => left
                .iter()
                .zip(right)
                .map(|((left_key, left), (right_key, right))| {
                    left_key
                        .cmp(right_key)
                        .then_with(|| Self::Typed.compare(left, right))
                })
                .find(|ordering| *ordering != Ordering::Equal)
                .unwrap_or_else(|| left.len().cmp(&right.len())),
            _ => unreachable!("equal kind tags have matching variants or were numeric"),
        }
    }

    fn kind(self, value: &PropertyValue) -> u8 {
        match value {
            PropertyValue::Null => 0,
            PropertyValue::Bool(_) => 1,
            PropertyValue::I64(_) => 2,
            PropertyValue::DateTime(_) => 3,
            PropertyValue::F64(_) if matches!(self, Self::ScalarNumeric) => 2,
            PropertyValue::F32(_) if matches!(self, Self::ScalarNumeric) => 2,
            PropertyValue::F64(_) => 4,
            PropertyValue::F32(_) => 5,
            PropertyValue::String(_) => 6,
            PropertyValue::Bytes(_) => 7,
            PropertyValue::I64Array(_) => 8,
            PropertyValue::F64Array(_) => 9,
            PropertyValue::F32Array(_) => 10,
            PropertyValue::StringArray(_) => 11,
            PropertyValue::Array(_) => 12,
            PropertyValue::Object(_) => 13,
        }
    }
}

fn compare_f64(left: f64, right: f64) -> Ordering {
    // Native equality identifies signed zero. NaNs receive an ordering only;
    // the separate equality callback prevents non-reflexive deduplication.
    let left = if left == 0.0 { 0.0 } else { left };
    let right = if right == 0.0 { 0.0 } else { right };
    left.total_cmp(&right)
}

/// Retain each equality class's first input in original order. `compare` must
/// be total: comparison-equivalent values must be equal or both non-reflexive.
/// Neither callback may change its result during construction. Large inputs use
/// O(n log n) comparisons and one temporary index vector; payloads are moved,
/// never cloned. Small inputs keep the existing allocation and comparison path.
pub(crate) fn dedup_by<T>(
    mut values: Vec<T>,
    compare: impl Fn(&T, &T) -> Ordering,
    equal: impl Fn(&T, &T) -> bool,
) -> Vec<T> {
    if values.len() <= 16 {
        return values.into_iter().fold(Vec::new(), |mut unique, value| {
            if !unique.iter().any(|existing| equal(existing, &value)) {
                unique.push(value);
            }
            unique
        });
    }
    let mut order: Vec<_> = (0..values.len()).collect();
    order.sort_unstable_by(|left, right| {
        compare(&values[*left], &values[*right]).then_with(|| left.cmp(right))
    });
    // The original-position tie-break retains the first representative even
    // for differently encoded equal values, such as signed zeros.
    order.dedup_by(|left, right| equal(&values[*left], &values[*right]));
    order.sort_unstable();
    let mut keep = order.into_iter().peekable();
    let mut position = 0;
    values.retain(|_| {
        let selected = keep.peek() == Some(&position);
        if selected {
            keep.next();
        }
        position += 1;
        selected
    });
    drop(keep);
    // Duplicate-heavy domains must not retain the original wide-list capacity.
    values.shrink_to_fit();
    values
}

/// Borrow a finite domain for repeated membership checks. The caller retains
/// probe order and payload ownership. Comparison and equality have the same
/// consistency contract as [`dedup_by`]; non-reflexive values never match.
/// For m domain values and n probes, wide inputs use O(m log m + n log m)
/// comparisons and O(m) borrowed references. Small inputs allocate no index.
/// `probes` selects the construction strategy; it does not restrict lookups.
pub(crate) fn membership_by<T>(
    values: &[T],
    probes: usize,
    compare: impl Fn(&T, &T) -> Ordering,
    equal: impl Fn(&T, &T) -> bool,
) -> impl Fn(&T) -> bool {
    let ordered = (values.len() > 16 && probes > 16).then(|| {
        let mut candidates: Vec<_> = values.iter().collect();
        candidates.sort_unstable_by(|left, right| compare(left, right));
        candidates
    });
    move |value| {
        let Some(candidates) = &ordered else {
            return values.iter().any(|candidate| equal(value, candidate));
        };
        // Ordering only finds the equality class. A NaN or nested non-reflexive
        // value must still fail the caller's actual equality contract.
        candidates
            .binary_search_by(|candidate| compare(candidate, value))
            .is_ok_and(|index| equal(value, candidates[index]))
    }
}
