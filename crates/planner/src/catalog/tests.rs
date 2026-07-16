use helix_ast::index::RangeIndexDirection;

use super::*;

#[test]
fn property_keys_reject_empty_parts() {
    assert!(ScopedPropertyKey::try_new("", "email").is_none());
    assert!(ScopedPropertyKey::try_new("User", "").is_none());
    assert!(ScopedPropertyDirectionKey::try_new("", "age", RangeIndexDirection::Asc).is_none());
    assert!(ScopedPropertyDirectionKey::try_new("User", "", RangeIndexDirection::Asc).is_none());
}

#[test]
fn property_key_display_includes_scope_property_and_direction() {
    let equality = ScopedPropertyKey::try_new("User", "email").unwrap();
    let range =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap();

    assert_eq!(equality.to_string(), "User:email");
    assert_eq!(range.to_string(), "User:age:Desc");
}

#[test]
fn search_scope_rejects_empty_tenant_property() {
    assert_eq!(
        SearchIndexScope::try_new(Option::<String>::None),
        Some(SearchIndexScope::Unscoped)
    );
    assert!(SearchIndexScope::try_new(Some("")).is_none());
    assert_eq!(
        SearchIndexScope::try_new(Some("tenant_id")).unwrap(),
        SearchIndexScope::Tenant {
            property: crate::ir::NonEmptyString::new("tenant_id").unwrap()
        }
    );
}

#[test]
fn typed_search_keys_reject_wrong_element_during_deserialization() {
    let node = NodeSearchIndexKey::try_new("Doc", "embedding").unwrap();
    let node_json = serde_json::to_string(&node).unwrap();
    let general: SearchIndexKey = serde_json::from_str(&node_json).unwrap();
    assert_eq!(general.element, ElementKind::Node);

    let edge_json = serde_json::to_string(
        &SearchIndexKey::try_new(ElementKind::Edge, "Doc", "embedding").unwrap(),
    )
    .unwrap();
    let error = serde_json::from_str::<NodeSearchIndexKey>(&edge_json).unwrap_err();
    assert!(error.to_string().contains("expected node search index key"));

    let node_json = serde_json::to_string(
        &SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
    )
    .unwrap();
    let error = serde_json::from_str::<EdgeSearchIndexKey>(&node_json).unwrap_err();
    assert!(error.to_string().contains("expected edge search index key"));
}

#[test]
fn snapshot_builders_generate_catalog_metadata() {
    let node_eq = ScopedPropertyKey::try_new("User", "email").unwrap();
    let node_range =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Desc).unwrap();
    let edge_eq = ScopedPropertyKey::try_new("LIKES", "weight").unwrap();
    let edge_range =
        ScopedPropertyDirectionKey::try_new("LIKES", "created_at", RangeIndexDirection::Asc)
            .unwrap();
    let vector = SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap();
    let text = SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap();

    let catalog = IndexCatalogSnapshot::default()
        .with_node_eq(node_eq.clone())
        .with_node_range(node_range.clone())
        .with_edge_eq(edge_eq.clone())
        .with_edge_range(edge_range.clone())
        .with_vector(vector.clone(), SearchIndexScope::Unscoped)
        .with_text(text.clone(), SearchIndexScope::Unscoped);

    assert_eq!(
        catalog.node_eq[&node_eq].index_id.as_ref(),
        "node_eq:User:email"
    );
    assert_eq!(
        catalog.node_range[&node_range].index_id.as_ref(),
        "node_range:User:age:Desc"
    );
    assert_eq!(
        catalog.edge_eq[&edge_eq].index_id.as_ref(),
        "edge_eq:LIKES:weight"
    );
    assert_eq!(
        catalog.edge_range[&edge_range].index_id.as_ref(),
        "edge_range:LIKES:created_at:Asc"
    );
    assert_eq!(
        catalog.vector[&vector].index_id.as_ref(),
        "vector:node:Doc:embedding"
    );
    assert_eq!(
        catalog.text[&text].index_id.as_ref(),
        "text:edge:MENTIONS:body"
    );
    assert_eq!(catalog.vector[&vector].scope, SearchIndexScope::Unscoped);
    assert_eq!(catalog.text[&text].scope, SearchIndexScope::Unscoped);
}
