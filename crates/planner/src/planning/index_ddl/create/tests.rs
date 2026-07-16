use helix_ast::index::{IndexSpec, RangeIndexDirection, VectorDistanceMetric};
use std::num::NonZeroUsize;

use super::*;
use crate::{catalog, error, ir};

fn dimension(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test dimension is positive")
}

#[test]
fn create_spec_preserves_secondary_index_attributes() {
    let cases = [
        (
            IndexSpec::node_unique_equality("User", "email"),
            ir::IndexDdlCreateSpec::NodeEquality {
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                uniqueness: catalog::IndexUniqueness::Unique,
            },
        ),
        (
            IndexSpec::node_range_desc("User", "age"),
            ir::IndexDdlCreateSpec::NodeRange {
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "User",
                    "age",
                    RangeIndexDirection::Desc,
                )
                .unwrap(),
            },
        ),
        (
            IndexSpec::edge_equality("FOLLOWS", "status"),
            ir::IndexDdlCreateSpec::EdgeEquality {
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            },
        ),
        (
            IndexSpec::edge_range("FOLLOWS", "since"),
            ir::IndexDdlCreateSpec::EdgeRange {
                key: catalog::ScopedPropertyDirectionKey::try_new(
                    "FOLLOWS",
                    "since",
                    RangeIndexDirection::Asc,
                )
                .unwrap(),
            },
        ),
    ];

    for (raw, expected) in cases {
        assert_eq!(index_ddl_create_spec(&raw).unwrap(), expected);
    }

    assert_eq!(
        index_ddl_create_spec(&IndexSpec::node_equality("User", "username")).unwrap(),
        ir::IndexDdlCreateSpec::NodeEquality {
            key: catalog::ScopedPropertyKey::try_new("User", "username").unwrap(),
            uniqueness: catalog::IndexUniqueness::NonUnique,
        }
    );
}

#[test]
fn create_spec_preserves_search_index_attributes() {
    let cases = [
        (
            IndexSpec::node_vector(
                "Doc",
                "embedding",
                dimension(3),
                VectorDistanceMetric::Cosine,
                Some("tenant_id"),
            ),
            ir::IndexDdlCreateSpec::NodeVector {
                key: catalog::ScopedPropertyKey::try_new("Doc", "embedding").unwrap(),
                dimension: ir::VectorIndexDimension::new(3).unwrap(),
                metric: ir::VectorIndexMetric::Cosine,
                scope: catalog::SearchIndexScope::Tenant {
                    property: ir::NonEmptyString::new("tenant_id").unwrap(),
                },
            },
        ),
        (
            IndexSpec::node_text("Doc", "body", None::<&str>),
            ir::IndexDdlCreateSpec::NodeText {
                key: catalog::ScopedPropertyKey::try_new("Doc", "body").unwrap(),
                scope: catalog::SearchIndexScope::Unscoped,
            },
        ),
        (
            IndexSpec::edge_vector(
                "MENTIONS",
                "embedding",
                dimension(4),
                VectorDistanceMetric::Euclidean,
                Some("tenant_id"),
            ),
            ir::IndexDdlCreateSpec::EdgeVector {
                key: catalog::ScopedPropertyKey::try_new("MENTIONS", "embedding").unwrap(),
                dimension: ir::VectorIndexDimension::new(4).unwrap(),
                metric: ir::VectorIndexMetric::Euclidean,
                scope: catalog::SearchIndexScope::Tenant {
                    property: ir::NonEmptyString::new("tenant_id").unwrap(),
                },
            },
        ),
        (
            IndexSpec::edge_text("MENTIONS", "body", None::<&str>),
            ir::IndexDdlCreateSpec::EdgeText {
                key: catalog::ScopedPropertyKey::try_new("MENTIONS", "body").unwrap(),
                scope: catalog::SearchIndexScope::Unscoped,
            },
        ),
    ];

    for (raw, expected) in cases {
        assert_eq!(index_ddl_create_spec(&raw).unwrap(), expected);
    }
}

#[test]
fn create_spec_rejects_invalid_names_at_the_create_boundary() {
    assert!(matches!(
        index_ddl_create_spec(&IndexSpec::node_equality("", "email")),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Label
        })
    ));
    assert!(matches!(
        index_ddl_create_spec(&IndexSpec::edge_equality("FOLLOWS", "")),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Property
        })
    ));
    assert!(matches!(
        index_ddl_create_spec(&IndexSpec::node_text("Doc", "body", Some(""))),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::TenantProperty
        })
    ));
}
