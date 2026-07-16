use helix_ast::index::{IndexSpec, RangeIndexDirection, VectorDistanceMetric};
use std::num::NonZeroUsize;

use super::*;
use crate::{catalog, error, ir};

fn dimension(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test dimension is positive")
}

#[test]
fn drop_spec_keeps_secondary_index_identity_fields() {
    let cases = [
        (
            IndexSpec::node_unique_equality("User", "email"),
            ir::IndexDdlDropSpec::NodeEquality {
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                uniqueness: catalog::IndexUniqueness::Unique,
            },
        ),
        (
            IndexSpec::node_range_desc("User", "age"),
            ir::IndexDdlDropSpec::NodeRange {
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
            ir::IndexDdlDropSpec::EdgeEquality {
                key: catalog::ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            },
        ),
        (
            IndexSpec::edge_range("FOLLOWS", "since"),
            ir::IndexDdlDropSpec::EdgeRange {
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
        assert_eq!(index_ddl_drop_spec(&raw).unwrap(), expected);
    }
}

#[test]
fn drop_spec_keeps_search_index_identity_fields_only() {
    let cases = [
        (
            IndexSpec::node_vector(
                "Doc",
                "embedding",
                dimension(3),
                VectorDistanceMetric::Cosine,
                Some("tenant_id"),
            ),
            ir::IndexDdlDropSpec::NodeVector {
                key: catalog::ScopedPropertyKey::try_new("Doc", "embedding").unwrap(),
            },
        ),
        (
            IndexSpec::node_text("Doc", "body", Some("tenant_id")),
            ir::IndexDdlDropSpec::NodeText {
                key: catalog::ScopedPropertyKey::try_new("Doc", "body").unwrap(),
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
            ir::IndexDdlDropSpec::EdgeVector {
                key: catalog::ScopedPropertyKey::try_new("MENTIONS", "embedding").unwrap(),
            },
        ),
        (
            IndexSpec::edge_text("MENTIONS", "body", Some("tenant_id")),
            ir::IndexDdlDropSpec::EdgeText {
                key: catalog::ScopedPropertyKey::try_new("MENTIONS", "body").unwrap(),
            },
        ),
    ];

    for (raw, expected) in cases {
        assert_eq!(index_ddl_drop_spec(&raw).unwrap(), expected);
    }
}

#[test]
fn drop_spec_rejects_invalid_identity_names_but_ignores_create_only_tenant_scope() {
    assert!(matches!(
        index_ddl_drop_spec(&IndexSpec::node_equality("", "email")),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Label
        })
    ));
    assert!(matches!(
        index_ddl_drop_spec(&IndexSpec::edge_text("MENTIONS", "", Some(""))),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Property
        })
    ));
    assert_eq!(
        index_ddl_drop_spec(&IndexSpec::node_text("Doc", "body", Some(""))).unwrap(),
        ir::IndexDdlDropSpec::NodeText {
            key: catalog::ScopedPropertyKey::try_new("Doc", "body").unwrap(),
        }
    );
}
