//! Exact runtime equality-domain classification and execution.

use core::num::NonZeroUsize;

use futures::FutureExt;
use helix_planner::{catalog, ir};

use super::super::ExecutionContext;
use crate::encoding::v2::values::property::{equality_index_value, property_value::PropertyValue};
use crate::error::Result;

/// Runtime classification of an equality-domain parameter.
#[derive(Debug, PartialEq)]
pub(in crate::execution::interpreter) enum RuntimeEqualityDomain {
    /// Every member has an exact index representation. Members are distinct
    /// by query equality and in parameter order.
    Indexed(Vec<PropertyValue>),
    /// Some member (null, or a value no lane entry can hold) only rows outside
    /// every lane bitmap can equal. `indexed` are the members with an exact
    /// index representation; `domain` is the whole bound parameter.
    WithUnindexed {
        indexed: Vec<PropertyValue>,
        domain: PropertyValue,
    },
}

/// One read of a runtime domain.
enum DomainRead<'a> {
    /// One index union of indexed members.
    Indexed(&'a [PropertyValue]),
    /// The verified label rows outside the lane, for the whole domain.
    Unindexed(&'a PropertyValue),
}

impl<'db> ExecutionContext<'db> {
    /// Rows of `key` whose property equals a member of the runtime domain,
    /// narrowed to `within` when it is given.
    ///
    /// Indexed members are read in index unions of at most
    /// `plan.max_values()` values each. Members no lane can hold are answered
    /// by the verified label rows outside the lane, which `within` bounds.
    /// The unions and the label read are read concurrently within `reads`, in
    /// plan order, as children of one budgeted read, so memory stays bounded
    /// by one union's keys per read in flight plus the result. The keyspace
    /// is never scanned, whatever the domain's size.
    pub(in crate::execution::interpreter) async fn dynamic_membership_ids(
        &self,
        kind: crate::index_lifecycle::IndexElementKind,
        key: &catalog::ScopedPropertyKey,
        plan: &ir::RuntimeEqualitySet,
        reads: NonZeroUsize,
        within: Option<&roaring::RoaringTreemap>,
    ) -> Result<roaring::RoaringTreemap> {
        let (indexed, unindexed) = match self.runtime_equality_domain(plan)? {
            RuntimeEqualityDomain::Indexed(indexed) => (indexed, None),
            RuntimeEqualityDomain::WithUnindexed { indexed, domain } => (indexed, Some(domain)),
        };
        let parts = indexed
            .chunks(plan.max_values().get())
            .map(DomainRead::Indexed)
            .chain(unindexed.as_ref().map(DomainRead::Unindexed))
            .collect::<Vec<_>>();
        let ids = super::union(
            self.read_children(parts.iter().collect(), reads, |part, _| match part {
                DomainRead::Indexed(values) => self
                    .lookup_managed_equality_union(kind, key, values)
                    .boxed(),
                DomainRead::Unindexed(domain) => self
                    .unindexed_label_rows(
                        kind,
                        key,
                        |value| {
                            super::super::stream::property_value_is_in(
                                value.unwrap_or(&PropertyValue::Null),
                                domain,
                            )
                        },
                        within,
                    )
                    .boxed(),
            }),
        )
        .await?;
        Ok(match within {
            Some(within) => ids & within,
            None => ids,
        })
    }

    pub(in crate::execution::interpreter) fn runtime_equality_domain(
        &self,
        plan: &ir::RuntimeEqualitySet,
    ) -> Result<RuntimeEqualityDomain> {
        Ok(runtime_equality_domain_from_value(
            self.param_value(plan.param())?,
        ))
    }
}

fn runtime_equality_domain_from_value(original: PropertyValue) -> RuntimeEqualityDomain {
    let (indexed, unindexed) = match &original {
        PropertyValue::I64Array(values) => {
            classify_members(values.iter().copied().map(PropertyValue::I64))
        }
        PropertyValue::F64Array(values) => {
            classify_members(values.iter().copied().map(PropertyValue::F64))
        }
        PropertyValue::F32Array(values) => classify_members(
            values
                .iter()
                .copied()
                .map(|value| PropertyValue::F32(f64::from(value))),
        ),
        PropertyValue::StringArray(values) => {
            classify_members(values.iter().cloned().map(PropertyValue::String))
        }
        PropertyValue::Array(values) => classify_members(values.iter().cloned()),
        value @ (PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::Object(_)) => classify_members(core::iter::once(value.clone())),
    };
    match unindexed {
        MemberCoverage::Indexed => RuntimeEqualityDomain::Indexed(indexed),
        MemberCoverage::WithUnindexed => RuntimeEqualityDomain::WithUnindexed {
            indexed,
            domain: original,
        },
    }
}

/// Whether every member of a domain has an exact index representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberCoverage {
    Indexed,
    WithUnindexed,
}

/// Split a query-equality domain into its distinct indexed members, in order,
/// and whether any member (null, or a value no lane entry can hold) needs the
/// label rows outside the lane. Non-reflexive members match nothing and are
/// dropped. Members are deduplicated by their canonical equality bytes.
fn classify_members(
    values: impl IntoIterator<Item = PropertyValue>,
) -> (Vec<PropertyValue>, MemberCoverage) {
    let mut seen = std::collections::HashSet::new();
    let mut coverage = MemberCoverage::Indexed;
    let indexed = values
        .into_iter()
        .filter(
            |value| match equality_index_value::project_equality_value(value) {
                equality_index_value::EqualityValueProjection::Indexed(canonical) => {
                    seen.insert(canonical)
                }
                equality_index_value::EqualityValueProjection::NonReflexive => false,
                equality_index_value::EqualityValueProjection::AuthoritativeNull
                | equality_index_value::EqualityValueProjection::Unsupported(_)
                | equality_index_value::EqualityValueProjection::Oversized { .. } => {
                    coverage = MemberCoverage::WithUnindexed;
                    false
                }
            },
        )
        .collect();
    (indexed, coverage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_domains_normalize_every_array_representation_and_scalar_family() {
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
                runtime_equality_domain_from_value(input),
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
                runtime_equality_domain_from_value(input.clone()),
                RuntimeEqualityDomain::Indexed(vec![input])
            );
        }

        for input in [
            PropertyValue::Null,
            PropertyValue::Object(Default::default()),
            PropertyValue::Array(vec![PropertyValue::Null]),
        ] {
            assert_eq!(
                runtime_equality_domain_from_value(input.clone()),
                RuntimeEqualityDomain::WithUnindexed {
                    indexed: Vec::new(),
                    domain: input,
                }
            );
        }
    }

    #[test]
    fn members_deduplicate_by_canonical_equality_and_skip_non_reflexive_values() {
        let values = [
            PropertyValue::I64(1),
            PropertyValue::F64(1.0),
            PropertyValue::F64(f64::NAN),
            PropertyValue::F32(2.0),
        ];

        assert_eq!(
            classify_members(values),
            (
                vec![PropertyValue::I64(1), PropertyValue::F32(2.0)],
                MemberCoverage::Indexed
            )
        );
    }

    #[test]
    fn domains_of_any_size_stay_indexed_and_unsafe_members_need_label_rows() {
        assert_eq!(
            classify_members(Vec::new()),
            (Vec::new(), MemberCoverage::Indexed)
        );
        let many = (0..1_000).map(PropertyValue::I64).collect::<Vec<_>>();
        assert_eq!(
            classify_members(many.clone()),
            (many, MemberCoverage::Indexed)
        );
        for unsafe_member in [
            PropertyValue::Null,
            PropertyValue::Array(Vec::new()),
            PropertyValue::Object(Default::default()),
        ] {
            assert_eq!(
                classify_members([PropertyValue::I64(1), unsafe_member, PropertyValue::I64(2),]),
                (
                    vec![PropertyValue::I64(1), PropertyValue::I64(2)],
                    MemberCoverage::WithUnindexed
                )
            );
        }

        let oversized_bytes =
            PropertyValue::Bytes(vec![0; equality_index_value::MAX_EQUALITY_CANONICAL_LEN]);
        assert_eq!(
            runtime_equality_domain_from_value(oversized_bytes.clone()),
            RuntimeEqualityDomain::WithUnindexed {
                indexed: Vec::new(),
                domain: oversized_bytes,
            }
        );

        let oversized_strings = PropertyValue::StringArray(vec![
            "x".repeat(equality_index_value::MAX_EQUALITY_CANONICAL_LEN),
            "y".to_owned(),
        ]);
        assert_eq!(
            runtime_equality_domain_from_value(oversized_strings.clone()),
            RuntimeEqualityDomain::WithUnindexed {
                indexed: vec![PropertyValue::String("y".to_owned())],
                domain: oversized_strings,
            }
        );
    }
}
