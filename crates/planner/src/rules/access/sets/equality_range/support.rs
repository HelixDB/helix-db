//! Shared equality/range proof helpers.

use super::super::{edge_union_from_sources, node_union_from_sources};
use crate::{catalog, digest, ir};

pub(super) struct NodeRangeBucketEntry<'a> {
    pub(super) index: usize,
    pub(super) key: &'a catalog::ScopedPropertyDirectionKey,
    pub(super) range: &'a ir::IndexRange,
}

pub(super) struct EdgeRangeBucketEntry<'a> {
    pub(super) index: usize,
    pub(super) key: &'a catalog::ScopedPropertyDirectionKey,
    pub(super) range: &'a ir::IndexRange,
}

pub(super) fn scoped_property_digest(
    tag: &'static str,
    label: &ir::NonEmptyString,
    property: &ir::NonEmptyString,
) -> digest::PlanDigest {
    digest::PlanDigest::for_tagged_value(tag, &(label.as_ref(), property.as_ref()))
}

pub(super) fn node_range_bucket_entry<'a>(
    tag: &'static str,
    index: usize,
    source: &'a ir::NodeAccessSourcePlan,
) -> Option<(digest::PlanDigest, NodeRangeBucketEntry<'a>)> {
    let (key, range) = node_range_index_parts(source)?;
    Some((
        scoped_property_digest(tag, &key.label, &key.property),
        NodeRangeBucketEntry { index, key, range },
    ))
}

pub(super) fn edge_range_bucket_entry<'a>(
    tag: &'static str,
    index: usize,
    source: &'a ir::EdgeAccessSourcePlan,
) -> Option<(digest::PlanDigest, EdgeRangeBucketEntry<'a>)> {
    let (key, range) = edge_range_index_parts(source)?;
    Some((
        scoped_property_digest(tag, &key.label, &key.property),
        EdgeRangeBucketEntry { index, key, range },
    ))
}

pub(super) fn node_range_index_parts(
    source: &ir::NodeAccessSourcePlan,
) -> Option<(&catalog::ScopedPropertyDirectionKey, &ir::IndexRange)> {
    match source.as_ref() {
        ir::NodeAccessPlan::RangeIndex { key, range, .. } => Some((key, range)),
        _ => None,
    }
}

pub(super) fn edge_range_index_parts(
    source: &ir::EdgeAccessSourcePlan,
) -> Option<(&catalog::ScopedPropertyDirectionKey, &ir::IndexRange)> {
    match source.as_ref() {
        ir::EdgeAccessPlan::RangeIndex { key, range, .. } => Some((key, range)),
        _ => None,
    }
}

pub(super) fn node_literal_equality_parts(
    source: &ir::NodeAccessSourcePlan,
) -> Option<(&catalog::ScopedPropertyKey, &ir::SecondaryIndexLiteral)> {
    match source.as_ref() {
        ir::NodeAccessPlan::EqualityIndex {
            key,
            value: ir::IndexValue::Literal(value),
            ..
        } => Some((key, value)),
        _ => None,
    }
}

pub(super) fn edge_literal_equality_parts(
    source: &ir::EdgeAccessSourcePlan,
) -> Option<(&catalog::ScopedPropertyKey, &ir::SecondaryIndexLiteral)> {
    match source.as_ref() {
        ir::EdgeAccessPlan::EqualityIndex {
            key,
            value: ir::IndexValue::Literal(value),
            ..
        } => Some((key, value)),
        _ => None,
    }
}

pub(super) fn node_source_from_union_candidates(
    plans: Vec<ir::NodeAccessSourcePlan>,
) -> ir::NodeAccessSourcePlan {
    ir::NodeAccessSourcePlan::from_unfiltered(node_union_from_sources(plans))
}

pub(super) fn edge_source_from_union_candidates(
    plans: Vec<ir::EdgeAccessSourcePlan>,
) -> ir::EdgeAccessSourcePlan {
    ir::EdgeAccessSourcePlan::from_unfiltered(edge_union_from_sources(plans))
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::index::RangeIndexDirection;

    #[test]
    fn range_bucket_entries_preserve_typed_source_parts() {
        let key =
            catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                .unwrap();
        let source = ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::try_new("node_age").unwrap(),
            key: key.clone(),
            range: ir::IndexRange::All,
        });

        let (digest, entry) = node_range_bucket_entry("test_bucket:v1", 7, &source).unwrap();

        assert_eq!(
            digest,
            scoped_property_digest("test_bucket:v1", &key.label, &key.property)
        );
        assert_eq!(entry.index, 7);
        assert_eq!(entry.key, &key);
        assert_eq!(entry.range, &ir::IndexRange::All);
        assert!(node_range_bucket_entry(
            "test_bucket:v1",
            8,
            &ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan)
        )
        .is_none());
    }
}
