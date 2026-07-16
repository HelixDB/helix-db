use super::super::*;

fn vector_dimension(value: usize) -> std::num::NonZeroUsize {
    std::num::NonZeroUsize::new(value).expect("test vector dimension is positive")
}

#[test]
fn index_ddl_terminals_preserve_specs() {
    let cases = vec![
        (
            IndexSpec::node_unique_equality("User", "email"),
            IndexDdlCreateSpec::NodeEquality {
                key: ScopedPropertyKey::try_new("User", "email").unwrap(),
                uniqueness: IndexUniqueness::Unique,
            },
        ),
        (
            IndexSpec::node_range_desc("User", "age"),
            IndexDdlCreateSpec::NodeRange {
                key: ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc)
                    .unwrap(),
            },
        ),
        (
            IndexSpec::edge_equality("FOLLOWS", "status"),
            IndexDdlCreateSpec::EdgeEquality {
                key: ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            },
        ),
        (
            IndexSpec::edge_range("FOLLOWS", "since"),
            IndexDdlCreateSpec::EdgeRange {
                key: ScopedPropertyDirectionKey::try_new(
                    "FOLLOWS",
                    "since",
                    RangeIndexDirection::Asc,
                )
                .unwrap(),
            },
        ),
        (
            IndexSpec::node_vector(
                "Doc",
                "embedding",
                vector_dimension(3),
                helix_ast::index::VectorDistanceMetric::Cosine,
                Some("tenant_id"),
            ),
            IndexDdlCreateSpec::NodeVector {
                key: ScopedPropertyKey::try_new("Doc", "embedding").unwrap(),
                dimension: crate::ir::VectorIndexDimension::new(3).unwrap(),
                metric: crate::ir::VectorIndexMetric::Cosine,
                scope: SearchIndexScope::Tenant {
                    property: NonEmptyString::new("tenant_id").unwrap(),
                },
            },
        ),
        (
            IndexSpec::node_text("Doc", "body", None::<&str>),
            IndexDdlCreateSpec::NodeText {
                key: ScopedPropertyKey::try_new("Doc", "body").unwrap(),
                scope: SearchIndexScope::Unscoped,
            },
        ),
        (
            IndexSpec::edge_vector(
                "MENTIONS",
                "embedding",
                vector_dimension(4),
                helix_ast::index::VectorDistanceMetric::Euclidean,
                Some("tenant_id"),
            ),
            IndexDdlCreateSpec::EdgeVector {
                key: ScopedPropertyKey::try_new("MENTIONS", "embedding").unwrap(),
                dimension: crate::ir::VectorIndexDimension::new(4).unwrap(),
                metric: crate::ir::VectorIndexMetric::Euclidean,
                scope: SearchIndexScope::Tenant {
                    property: NonEmptyString::new("tenant_id").unwrap(),
                },
            },
        ),
        (
            IndexSpec::edge_text("MENTIONS", "body", None::<&str>),
            IndexDdlCreateSpec::EdgeText {
                key: ScopedPropertyKey::try_new("MENTIONS", "body").unwrap(),
                scope: SearchIndexScope::Unscoped,
            },
        ),
    ];

    for (raw, typed) in cases {
        assert_eq!(
            ddl_of(AstNode::CreateIndex {
                spec: raw,
                if_not_exists: true,
            }),
            IndexDdlPlan::Create {
                spec: typed,
                mode: IndexCreateMode::IfNotExists,
            }
        );
    }

    assert_eq!(
        ddl_of(AstNode::CreateIndex {
            spec: IndexSpec::node_equality("User", "username"),
            if_not_exists: false,
        }),
        IndexDdlPlan::Create {
            spec: IndexDdlCreateSpec::NodeEquality {
                key: ScopedPropertyKey::try_new("User", "username").unwrap(),
                uniqueness: IndexUniqueness::NonUnique,
            },
            mode: IndexCreateMode::ErrorIfExists,
        }
    );

    let drop_cases = vec![
        (
            IndexSpec::node_unique_equality("User", "email"),
            IndexDdlDropSpec::NodeEquality {
                key: ScopedPropertyKey::try_new("User", "email").unwrap(),
                uniqueness: IndexUniqueness::Unique,
            },
        ),
        (
            IndexSpec::node_range_desc("User", "age"),
            IndexDdlDropSpec::NodeRange {
                key: ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc)
                    .unwrap(),
            },
        ),
        (
            IndexSpec::edge_equality("FOLLOWS", "status"),
            IndexDdlDropSpec::EdgeEquality {
                key: ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
            },
        ),
        (
            IndexSpec::edge_range("FOLLOWS", "since"),
            IndexDdlDropSpec::EdgeRange {
                key: ScopedPropertyDirectionKey::try_new(
                    "FOLLOWS",
                    "since",
                    RangeIndexDirection::Asc,
                )
                .unwrap(),
            },
        ),
        (
            IndexSpec::node_vector(
                "Doc",
                "embedding",
                vector_dimension(3),
                helix_ast::index::VectorDistanceMetric::Cosine,
                Some("tenant_id"),
            ),
            IndexDdlDropSpec::NodeVector {
                key: ScopedPropertyKey::try_new("Doc", "embedding").unwrap(),
            },
        ),
        (
            IndexSpec::node_text("Doc", "body", Some("tenant_id")),
            IndexDdlDropSpec::NodeText {
                key: ScopedPropertyKey::try_new("Doc", "body").unwrap(),
            },
        ),
        (
            IndexSpec::edge_vector(
                "MENTIONS",
                "embedding",
                vector_dimension(4),
                helix_ast::index::VectorDistanceMetric::Euclidean,
                Some("tenant_id"),
            ),
            IndexDdlDropSpec::EdgeVector {
                key: ScopedPropertyKey::try_new("MENTIONS", "embedding").unwrap(),
            },
        ),
        (
            IndexSpec::edge_text("MENTIONS", "body", Some("tenant_id")),
            IndexDdlDropSpec::EdgeText {
                key: ScopedPropertyKey::try_new("MENTIONS", "body").unwrap(),
            },
        ),
    ];

    for (raw, typed) in drop_cases {
        assert_eq!(
            ddl_of(AstNode::DropIndex { spec: raw }),
            IndexDdlPlan::Drop { spec: typed }
        );
    }
}
