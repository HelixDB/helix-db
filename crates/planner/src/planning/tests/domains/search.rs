use crate::planning::tests::support::*;

#[test]
fn node_vector_and_text_searches_require_and_preserve_index_metadata() {
    let indexes = builtin_label_indexes()
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        );
    let vector_plan = executable_traversal(
        g().vector_search_nodes(
            "Doc",
            "embedding",
            vec![0.1f32, 0.2],
            5,
            Some("acme".into()),
        ),
        ctx(indexes.clone()),
    );
    let text_plan = executable_traversal(
        g().text_search_nodes_with(
            "Doc",
            "body",
            PropertyInput::param("query"),
            StreamBound::expr(Expr::param("limit")),
            Some(PropertyInput::param("tenant")),
        ),
        ctx(indexes),
    );
    let scoped_unbound_plan = executable_traversal(
        g().text_search_nodes("Doc", "body", "query", 4, None),
        ctx(builtin_label_indexes().with_text(
            SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap(),
            SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        )),
    );

    let ExecAccessPlan::Node(ExecNodeAccessPlan::VectorSearch {
        key,
        index,
        query_vector,
        k,
    }) = first_exec_access(&vector_plan)
    else {
        panic!(
            "expected node vector search: {:?}",
            first_exec_access(&vector_plan)
        );
    };
    assert_eq!(key.label, "Doc");
    assert_eq!(key.property, "embedding");
    assert_eq!(
        index,
        &SearchIndexPlan {
            index_id: NonEmptyString::new("vector:node:Doc:embedding").unwrap(),
            tenant: SearchTenantPlan::ScopedValue {
                property: NonEmptyString::new("tenant_id").unwrap(),
                value: SearchTenantValuePlan::new(
                    PropertyInputPlan::new(PropertyInput::from("acme")).unwrap(),
                )
                .unwrap(),
            },
        }
    );
    assert_eq!(
        query_vector,
        &VectorQueryInputPlan::Vector(SearchVector::new(vec![0.1, 0.2]).unwrap())
    );
    assert_eq!(k, &SearchLimitPlan::Literal(NonZeroUsize::new(5).unwrap()));

    let ExecAccessPlan::Node(ExecNodeAccessPlan::TextSearch {
        key,
        index,
        query_text,
        k,
    }) = first_exec_access(&text_plan)
    else {
        panic!(
            "expected node text search: {:?}",
            first_exec_access(&text_plan)
        );
    };
    assert_eq!(key.label, "Doc");
    assert_eq!(key.property, "body");
    assert_eq!(
        index,
        &SearchIndexPlan {
            index_id: NonEmptyString::new("text:node:Doc:body").unwrap(),
            tenant: SearchTenantPlan::ScopedValue {
                property: NonEmptyString::new("tenant_id").unwrap(),
                value: SearchTenantValuePlan::new(
                    PropertyInputPlan::new(PropertyInput::param("tenant")).unwrap(),
                )
                .unwrap(),
            },
        }
    );
    assert_eq!(
        query_text,
        &TextQueryInputPlan::Expr(SearchQueryExprPlan::new(Expr::param("query")).unwrap())
    );
    assert_eq!(
        k,
        &SearchLimitPlan::Expr(SearchLimitExprPlan::new(Expr::param("limit")).unwrap())
    );

    assert!(matches!(
        first_exec_access(&scoped_unbound_plan),
        ExecAccessPlan::Node(ExecNodeAccessPlan::TextSearch { index, .. })
            if matches!(&index.tenant, SearchTenantPlan::Scoped { property } if property.as_ref() == "tenant_id")
    ));
}

#[test]
fn edge_vector_and_text_searches_require_and_preserve_index_metadata() {
    let indexes = builtin_label_indexes()
        .with_vector(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
            SearchIndexScope::Unscoped,
        )
        .with_text(
            SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap(),
            SearchIndexScope::Unscoped,
        );
    let vector_plan = executable_traversal(
        g().vector_search_edges("MENTIONS", "embedding", vec![0.3f32, 0.4], 3, None),
        ctx(indexes.clone()),
    );
    let text_plan = executable_traversal(
        g().text_search_edges("MENTIONS", "body", "planner", 7, None),
        ctx(indexes),
    );

    assert!(matches!(
        first_exec_access(&vector_plan),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::VectorSearch { key, index, k, .. })
            if key.label == "MENTIONS"
                && key.property == "embedding"
                && index.tenant == SearchTenantPlan::Unscoped
                && k == &SearchLimitPlan::Literal(NonZeroUsize::new(3).unwrap())
    ));
    assert!(matches!(
        first_exec_access(&text_plan),
        ExecAccessPlan::Edge(ExecEdgeAccessPlan::TextSearch { key, index, k, .. })
            if key.label == "MENTIONS"
                && key.property == "body"
                && index.tenant == SearchTenantPlan::Unscoped
                && k == &SearchLimitPlan::Literal(NonZeroUsize::new(7).unwrap())
    ));
}
