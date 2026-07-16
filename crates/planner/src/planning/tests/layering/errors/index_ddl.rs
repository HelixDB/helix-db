use super::*;

fn node_vector(label: &str, property: &str, tenant_property: Option<&str>) -> IndexSpec {
    IndexSpec::node_vector(
        label,
        property,
        std::num::NonZeroUsize::new(3).expect("test dimension is positive"),
        helix_ast::index::VectorDistanceMetric::Cosine,
        tenant_property,
    )
}

fn edge_vector(label: &str, property: &str, tenant_property: Option<&str>) -> IndexSpec {
    IndexSpec::edge_vector(
        label,
        property,
        std::num::NonZeroUsize::new(3).expect("test dimension is positive"),
        helix_ast::index::VectorDistanceMetric::Euclidean,
        tenant_property,
    )
}

#[test]
fn index_ddl_fields_reject_empty_names_from_raw_ast() {
    let cases = [
        (
            AstNode::CreateIndex {
                spec: IndexSpec::node_equality("", "email"),
                if_not_exists: true,
            },
            NameField::Label,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::node_equality("User", ""),
                if_not_exists: true,
            },
            NameField::Property,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::node_range("", "age"),
                if_not_exists: true,
            },
            NameField::Label,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::node_range("User", ""),
                if_not_exists: true,
            },
            NameField::Property,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::edge_equality("", "status"),
                if_not_exists: true,
            },
            NameField::Label,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::edge_equality("FOLLOWS", ""),
                if_not_exists: true,
            },
            NameField::Property,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::edge_range("", "since"),
                if_not_exists: true,
            },
            NameField::Label,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::edge_range("FOLLOWS", ""),
                if_not_exists: true,
            },
            NameField::Property,
        ),
        (
            AstNode::CreateIndex {
                spec: node_vector("", "embedding", None),
                if_not_exists: true,
            },
            NameField::Label,
        ),
        (
            AstNode::CreateIndex {
                spec: node_vector("Doc", "", None),
                if_not_exists: true,
            },
            NameField::Property,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::node_text("", "body", None::<&str>),
                if_not_exists: true,
            },
            NameField::Label,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::node_text("Doc", "", None::<&str>),
                if_not_exists: true,
            },
            NameField::Property,
        ),
        (
            AstNode::CreateIndex {
                spec: edge_vector("", "embedding", None),
                if_not_exists: true,
            },
            NameField::Label,
        ),
        (
            AstNode::CreateIndex {
                spec: edge_vector("MENTIONS", "", None),
                if_not_exists: true,
            },
            NameField::Property,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::edge_text("", "body", None::<&str>),
                if_not_exists: true,
            },
            NameField::Label,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::edge_text("MENTIONS", "", None::<&str>),
                if_not_exists: true,
            },
            NameField::Property,
        ),
        (
            AstNode::CreateIndex {
                spec: node_vector("Doc", "embedding", Some("")),
                if_not_exists: true,
            },
            NameField::TenantProperty,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::node_text("Doc", "body", Some("")),
                if_not_exists: true,
            },
            NameField::TenantProperty,
        ),
        (
            AstNode::CreateIndex {
                spec: edge_vector("MENTIONS", "embedding", Some("")),
                if_not_exists: true,
            },
            NameField::TenantProperty,
        ),
        (
            AstNode::CreateIndex {
                spec: IndexSpec::edge_text("MENTIONS", "body", Some("")),
                if_not_exists: true,
            },
            NameField::TenantProperty,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::node_equality("", "email"),
            },
            NameField::Label,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::node_equality("User", ""),
            },
            NameField::Property,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::node_range("", "age"),
            },
            NameField::Label,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::node_range("User", ""),
            },
            NameField::Property,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::edge_equality("", "status"),
            },
            NameField::Label,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::edge_equality("FOLLOWS", ""),
            },
            NameField::Property,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::edge_range("", "since"),
            },
            NameField::Label,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::edge_range("FOLLOWS", ""),
            },
            NameField::Property,
        ),
        (
            AstNode::DropIndex {
                spec: node_vector("", "embedding", None),
            },
            NameField::Label,
        ),
        (
            AstNode::DropIndex {
                spec: node_vector("Doc", "", None),
            },
            NameField::Property,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::node_text("", "body", None::<&str>),
            },
            NameField::Label,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::node_text("Doc", "", None::<&str>),
            },
            NameField::Property,
        ),
        (
            AstNode::DropIndex {
                spec: edge_vector("", "embedding", None),
            },
            NameField::Label,
        ),
        (
            AstNode::DropIndex {
                spec: edge_vector("MENTIONS", "", None),
            },
            NameField::Property,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::edge_text("", "body", None::<&str>),
            },
            NameField::Label,
        ),
        (
            AstNode::DropIndex {
                spec: IndexSpec::edge_text("MENTIONS", "", None::<&str>),
            },
            NameField::Property,
        ),
    ];

    for (root, field) in cases {
        let batch = raw_read(root);

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName { field }
        );
    }
}
