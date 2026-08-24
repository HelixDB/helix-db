//! Exact runtime equality-domain classification and execution.

use helix_planner::{catalog, exec, ir};

use super::super::{ElementRef, ExecutionContext, ExecutionRow};
use crate::encoding::v2::values::property::{equality_index_value, property_value::PropertyValue};
use crate::error::Result;

#[derive(Debug, PartialEq)]
enum RuntimeEqualityDomain {
    Indexed(Vec<PropertyValue>),
    Authoritative(PropertyValue),
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn dynamic_membership_ids(
        &self,
        kind: crate::index_lifecycle::IndexElementKind,
        key: &catalog::ScopedPropertyKey,
        plan: &ir::RuntimeEqualitySet,
    ) -> Result<roaring::RoaringTreemap> {
        match self.runtime_equality_domain(plan)? {
            RuntimeEqualityDomain::Indexed(values) => {
                self.lookup_managed_equality_union(kind, key, &values).await
            }
            RuntimeEqualityDomain::Authoritative(values) => {
                let (keyspace, element) = match kind {
                    crate::index_lifecycle::IndexElementKind::Node => (
                        exec::ElementKeyspace::NodeProperty,
                        ElementRef::Node as fn(u64) -> ElementRef,
                    ),
                    crate::index_lifecycle::IndexElementKind::Edge => (
                        exec::ElementKeyspace::EdgeEndpoints,
                        ElementRef::Edge as fn(u64) -> ElementRef,
                    ),
                };
                let ids = self.scan_element_ids(keyspace, None).await?;
                let mut matches = roaring::RoaringTreemap::new();
                for id in ids {
                    self.check_execution_deadline()?;
                    let row = ExecutionRow::current(element(id));
                    if self.scoped_membership_matches(&row, key, &values).await? {
                        matches.insert(id);
                    }
                }
                Ok(matches)
            }
        }
    }

    fn runtime_equality_domain(
        &self,
        plan: &ir::RuntimeEqualitySet,
    ) -> Result<RuntimeEqualityDomain> {
        Ok(runtime_equality_domain_from_value(
            self.param_value(plan.param())?,
            plan.max_values(),
        ))
    }

    async fn scoped_membership_matches(
        &self,
        row: &ExecutionRow,
        key: &catalog::ScopedPropertyKey,
        values: &PropertyValue,
    ) -> Result<bool> {
        let properties = self.row_properties(row).await?;
        if properties
            .iter()
            .find(|property| property.name == "$label")
            .and_then(|property| property.value.as_str())
            != Some(key.label.as_ref())
        {
            return Ok(false);
        }
        let value = properties
            .iter()
            .find(|property| property.name == key.property.as_ref())
            .map_or(&PropertyValue::Null, |property| &property.value);
        Ok(super::super::stream::property_value_is_in(value, values))
    }
}

fn runtime_equality_domain_from_value(
    original: PropertyValue,
    max_values: std::num::NonZeroUsize,
) -> RuntimeEqualityDomain {
    let indexed = match &original {
        PropertyValue::I64Array(values) => {
            bounded_index_members(values.iter().copied().map(PropertyValue::I64), max_values)
        }
        PropertyValue::F64Array(values) => {
            bounded_index_members(values.iter().copied().map(PropertyValue::F64), max_values)
        }
        PropertyValue::F32Array(values) => bounded_index_members(
            values
                .iter()
                .copied()
                .map(|value| PropertyValue::F32(f64::from(value))),
            max_values,
        ),
        PropertyValue::StringArray(values) => bounded_index_members(
            values.iter().cloned().map(PropertyValue::String),
            max_values,
        ),
        PropertyValue::Array(values) => bounded_index_members(values.iter().cloned(), max_values),
        value @ (PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::Object(_)) => {
            bounded_index_members(core::iter::once(value.clone()), max_values)
        }
    };
    match indexed {
        Some(values) => RuntimeEqualityDomain::Indexed(values),
        None => RuntimeEqualityDomain::Authoritative(original),
    }
}

/// Collect a query-equality domain with memory bounded by `max_values`.
///
/// `None` means authoritative evaluation is required, either because a member
/// has no exact secondary-index representation or because the finite domain
/// exceeds the planner-selected bound.
fn bounded_index_members(
    values: impl IntoIterator<Item = PropertyValue>,
    max_values: std::num::NonZeroUsize,
) -> Option<Vec<PropertyValue>> {
    values
        .into_iter()
        .try_fold(Vec::with_capacity(max_values.get()), |mut unique, value| {
            match equality_index_value::project_equality_value(&value) {
                equality_index_value::EqualityValueProjection::Indexed(_) => {}
                equality_index_value::EqualityValueProjection::NonReflexive => {
                    return Some(unique);
                }
                equality_index_value::EqualityValueProjection::AuthoritativeNull
                | equality_index_value::EqualityValueProjection::Unsupported(_)
                | equality_index_value::EqualityValueProjection::Oversized { .. } => {
                    return None;
                }
            }
            if unique
                .iter()
                .any(|existing: &PropertyValue| existing.eq_value(&value))
            {
                return Some(unique);
            }
            if unique.len() == max_values.get() {
                return None;
            }
            unique.push(value);
            Some(unique)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_domains_normalize_every_array_representation_and_scalar_family() {
        let limit = std::num::NonZeroUsize::new(4).unwrap();
        let indexed = [
            (
                PropertyValue::I64Array(vec![1, 2]),
                vec![PropertyValue::I64(1), PropertyValue::I64(2)],
            ),
            (
                PropertyValue::F64Array(vec![1.5, 2.5]),
                vec![PropertyValue::F64(1.5), PropertyValue::F64(2.5)],
            ),
            (
                PropertyValue::F32Array(vec![1.25, 2.25]),
                vec![PropertyValue::F32(1.25), PropertyValue::F32(2.25)],
            ),
            (
                PropertyValue::StringArray(vec!["a".to_owned(), "b".to_owned()]),
                vec![
                    PropertyValue::String("a".to_owned()),
                    PropertyValue::String("b".to_owned()),
                ],
            ),
            (
                PropertyValue::Array(vec![PropertyValue::Bool(true), PropertyValue::I64(1)]),
                vec![PropertyValue::Bool(true), PropertyValue::I64(1)],
            ),
        ];
        for (input, expected) in indexed {
            assert_eq!(
                runtime_equality_domain_from_value(input, limit),
                RuntimeEqualityDomain::Indexed(expected)
            );
        }

        for input in [
            PropertyValue::Bool(true),
            PropertyValue::I64(1),
            PropertyValue::DateTime(1),
            PropertyValue::F64(1.5),
            PropertyValue::F32(1.25),
            PropertyValue::String("value".to_owned()),
            PropertyValue::Bytes(vec![1, 2]),
        ] {
            assert_eq!(
                runtime_equality_domain_from_value(input.clone(), limit),
                RuntimeEqualityDomain::Indexed(vec![input])
            );
        }

        for input in [
            PropertyValue::Null,
            PropertyValue::Object(Default::default()),
            PropertyValue::Array(vec![PropertyValue::Null]),
        ] {
            assert_eq!(
                runtime_equality_domain_from_value(input.clone(), limit),
                RuntimeEqualityDomain::Authoritative(input)
            );
        }
    }

    #[test]
    fn bounded_members_deduplicate_by_query_equality_and_skip_non_reflexive_values() {
        let values = [
            PropertyValue::I64(1),
            PropertyValue::F64(1.0),
            PropertyValue::F64(f64::NAN),
            PropertyValue::F32(2.0),
        ];

        assert_eq!(
            bounded_index_members(values, std::num::NonZeroUsize::new(2).unwrap()),
            Some(vec![PropertyValue::I64(1), PropertyValue::F32(2.0)])
        );
    }

    #[test]
    fn bounded_members_require_authoritative_evaluation_for_unsafe_or_large_domains() {
        let limit = std::num::NonZeroUsize::new(2).unwrap();
        assert_eq!(bounded_index_members(Vec::new(), limit), Some(Vec::new()));
        assert_eq!(
            bounded_index_members(
                [
                    PropertyValue::I64(1),
                    PropertyValue::I64(2),
                    PropertyValue::I64(3),
                ],
                limit,
            ),
            None
        );
        assert_eq!(bounded_index_members([PropertyValue::Null], limit), None);
        assert_eq!(
            bounded_index_members([PropertyValue::Array(Vec::new())], limit),
            None
        );
        assert_eq!(
            bounded_index_members([PropertyValue::Object(Default::default())], limit),
            None
        );

        let oversized_bytes =
            PropertyValue::Bytes(vec![0; equality_index_value::MAX_EQUALITY_CANONICAL_LEN]);
        assert_eq!(
            runtime_equality_domain_from_value(oversized_bytes.clone(), limit),
            RuntimeEqualityDomain::Authoritative(oversized_bytes)
        );

        let oversized_strings = PropertyValue::StringArray(vec![
            "x".repeat(equality_index_value::MAX_EQUALITY_CANONICAL_LEN)
        ]);
        assert_eq!(
            runtime_equality_domain_from_value(oversized_strings.clone(), limit),
            RuntimeEqualityDomain::Authoritative(oversized_strings)
        );
    }
}
