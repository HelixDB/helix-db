use super::super::*;
use super::support::SurfaceCase;

fn vector_dimension(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test vector dimension is positive")
}

pub(super) fn surface_cases() -> Vec<SurfaceCase> {
    let context = PlannerContext::default();

    vec![
        SurfaceCase {
            name: "add_n_source",
            root: AstNode::AddN {
                input: None,
                label: "User".to_owned(),
                properties: vec![("name".to_owned(), PropertyInput::from("alice"))],
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "add_n_input",
            root: AstNode::AddN {
                input: Some(boxed(nodes_root())),
                label: "Audit".to_owned(),
                properties: vec![("kind".to_owned(), PropertyInput::from("login"))],
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "add_e",
            root: AstNode::AddE {
                input: boxed(nodes_root()),
                label: "FOLLOWS".to_owned(),
                to: NodeRef::ids([7]),
                properties: Vec::new(),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "set_property",
            root: AstNode::SetProperty {
                input: boxed(nodes_root()),
                name: "active".to_owned(),
                value: PropertyInput::from(true),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "remove_property",
            root: AstNode::RemoveProperty {
                input: boxed(nodes_root()),
                name: "stale".to_owned(),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop",
            root: AstNode::Drop {
                input: boxed(nodes_root()),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_edge",
            root: AstNode::DropEdge {
                input: boxed(nodes_root()),
                to: NodeRef::ids([2, 3]),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_edge_labeled",
            root: AstNode::DropEdgeLabeled {
                input: boxed(nodes_root()),
                to: NodeRef::var("targets"),
                label: "LIKES".to_owned(),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_edge_by_id_source",
            root: AstNode::DropEdgeById {
                input: None,
                edges: EdgeRef::ids([8, 9]),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_edge_by_id_input",
            root: AstNode::DropEdgeById {
                input: Some(boxed(nodes_root())),
                edges: EdgeRef::var("edge_ids"),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_node_equality",
            root: AstNode::CreateIndex {
                spec: IndexSpec::node_equality("User", "email"),
                if_not_exists: true,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_node_unique_equality",
            root: AstNode::CreateIndex {
                spec: IndexSpec::node_unique_equality("User", "handle"),
                if_not_exists: false,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_node_range",
            root: AstNode::CreateIndex {
                spec: IndexSpec::node_range_desc("User", "age"),
                if_not_exists: true,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_edge_equality",
            root: AstNode::CreateIndex {
                spec: IndexSpec::edge_equality("FOLLOWS", "status"),
                if_not_exists: true,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_edge_range",
            root: AstNode::CreateIndex {
                spec: IndexSpec::edge_range("FOLLOWS", "since"),
                if_not_exists: true,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_node_vector_tenant",
            root: AstNode::CreateIndex {
                spec: IndexSpec::node_vector(
                    "Doc",
                    "embedding",
                    vector_dimension(3),
                    helix_ast::index::VectorDistanceMetric::Cosine,
                    Some("tenant_id"),
                ),
                if_not_exists: true,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_node_text",
            root: AstNode::CreateIndex {
                spec: IndexSpec::node_text("Doc", "body", None::<&str>),
                if_not_exists: true,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_edge_vector_tenant",
            root: AstNode::CreateIndex {
                spec: IndexSpec::edge_vector(
                    "MENTIONS",
                    "embedding",
                    vector_dimension(4),
                    helix_ast::index::VectorDistanceMetric::Euclidean,
                    Some("tenant_id"),
                ),
                if_not_exists: true,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "create_index_edge_text",
            root: AstNode::CreateIndex {
                spec: IndexSpec::edge_text("MENTIONS", "body", None::<&str>),
                if_not_exists: true,
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_node_equality",
            root: AstNode::DropIndex {
                spec: IndexSpec::node_equality("User", "email"),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_node_unique_equality",
            root: AstNode::DropIndex {
                spec: IndexSpec::node_unique_equality("User", "handle"),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_node_range",
            root: AstNode::DropIndex {
                spec: IndexSpec::node_range_desc("User", "age"),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_edge_equality",
            root: AstNode::DropIndex {
                spec: IndexSpec::edge_equality("FOLLOWS", "status"),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_edge_range",
            root: AstNode::DropIndex {
                spec: IndexSpec::edge_range("FOLLOWS", "since"),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_node_vector",
            root: AstNode::DropIndex {
                spec: IndexSpec::node_vector(
                    "Doc",
                    "embedding",
                    vector_dimension(3),
                    helix_ast::index::VectorDistanceMetric::Cosine,
                    Some("tenant_id"),
                ),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_node_text",
            root: AstNode::DropIndex {
                spec: IndexSpec::node_text("Doc", "body", Some("tenant_id")),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_edge_vector",
            root: AstNode::DropIndex {
                spec: IndexSpec::edge_vector(
                    "MENTIONS",
                    "embedding",
                    vector_dimension(4),
                    helix_ast::index::VectorDistanceMetric::Euclidean,
                    Some("tenant_id"),
                ),
            },
            context: context.clone(),
        },
        SurfaceCase {
            name: "drop_index_edge_text",
            root: AstNode::DropIndex {
                spec: IndexSpec::edge_text("MENTIONS", "body", Some("tenant_id")),
            },
            context,
        },
    ]
}
