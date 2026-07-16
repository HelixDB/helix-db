use super::super::*;

pub(super) struct SurfaceCase {
    pub(super) name: &'static str,
    pub(super) root: AstNode,
    pub(super) context: PlannerContext,
}

pub(super) fn read_root(
    root: AstNode,
    context: &PlannerContext,
) -> Result<ExecutablePlan, PlannerError> {
    let batch = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("result".to_owned()),
            root,
            condition: None,
        }))],
        returns: vec!["result".to_owned()],
    };

    crate::planning::plan_read_batch(&batch, context)
}

pub(super) fn write_root(
    root: AstNode,
    context: &PlannerContext,
) -> Result<ExecutablePlan, PlannerError> {
    let batch = helix_ast::batch::WriteBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("result".to_owned()),
            root,
            condition: None,
        }))],
        returns: vec!["result".to_owned()],
    };

    crate::planning::plan_write_batch(&batch, context)
}

pub(super) fn search_context() -> PlannerContext {
    ctx(builtin_label_indexes()
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap(),
            SearchIndexScope::Unscoped,
        ))
}

pub(super) fn tenant_search_context() -> PlannerContext {
    ctx(builtin_label_indexes()
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Node, "TenantDoc", "embedding").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Node, "TenantDoc", "body").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Edge, "TENANT_MENTIONS", "embedding").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Edge, "TENANT_MENTIONS", "body").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        ))
}

pub(super) fn node_vector_search() -> AstNode {
    AstNode::VectorSearchNodes {
        label: "Doc".to_owned(),
        property: "embedding".to_owned(),
        tenant_value: None,
        query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1, 0.2])),
        k: StreamBound::Literal(3),
    }
}

pub(super) fn node_text_search() -> AstNode {
    AstNode::TextSearchNodes {
        label: "Doc".to_owned(),
        property: "body".to_owned(),
        tenant_value: None,
        query_text: PropertyInput::from("planner"),
        k: StreamBound::Literal(4),
    }
}

pub(super) fn edge_vector_search() -> AstNode {
    AstNode::VectorSearchEdges {
        label: "MENTIONS".to_owned(),
        property: "embedding".to_owned(),
        tenant_value: None,
        query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.3, 0.4])),
        k: StreamBound::Literal(5),
    }
}

pub(super) fn edge_text_search() -> AstNode {
    AstNode::TextSearchEdges {
        label: "MENTIONS".to_owned(),
        property: "body".to_owned(),
        tenant_value: None,
        query_text: PropertyInput::from("cascades"),
        k: StreamBound::Literal(6),
    }
}

pub(super) fn tenant_node_vector_search() -> AstNode {
    AstNode::VectorSearchNodes {
        label: "TenantDoc".to_owned(),
        property: "embedding".to_owned(),
        tenant_value: Some(PropertyInput::from("tenant-a")),
        query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.1, 0.2])),
        k: StreamBound::expr(Expr::param("k")),
    }
}

pub(super) fn tenant_node_text_search() -> AstNode {
    AstNode::TextSearchNodes {
        label: "TenantDoc".to_owned(),
        property: "body".to_owned(),
        tenant_value: Some(PropertyInput::from(Expr::param("tenant"))),
        query_text: PropertyInput::from(Expr::param("query")),
        k: StreamBound::Literal(4),
    }
}

pub(super) fn tenant_edge_vector_search() -> AstNode {
    AstNode::VectorSearchEdges {
        label: "TENANT_MENTIONS".to_owned(),
        property: "embedding".to_owned(),
        tenant_value: Some(PropertyInput::from("tenant-a")),
        query_vector: PropertyInput::from(PropertyValue::F32Array(vec![0.3, 0.4])),
        k: StreamBound::Literal(5),
    }
}

pub(super) fn tenant_edge_text_search() -> AstNode {
    AstNode::TextSearchEdges {
        label: "TENANT_MENTIONS".to_owned(),
        property: "body".to_owned(),
        tenant_value: Some(PropertyInput::from(Expr::param("tenant"))),
        query_text: PropertyInput::from("cascades"),
        k: StreamBound::expr(Expr::param("k")),
    }
}

pub(super) fn project_root() -> AstNode {
    AstNode::Project {
        input: boxed(nodes_root()),
        projections: vec![
            Projection::property("name", "name"),
            Projection::expr("age_plus_one", Expr::prop("age").add(Expr::val(1))),
        ],
    }
}

pub(super) fn project_bindings_root() -> AstNode {
    AstNode::ProjectBindings {
        input: boxed(AstNode::Bind {
            input: boxed(nodes_root()),
            name: "current".to_owned(),
        }),
        projections: vec![BindingProjection::property(
            BindingTarget::binding("current"),
            "name",
            "name",
        )],
        distinct: true,
    }
}
