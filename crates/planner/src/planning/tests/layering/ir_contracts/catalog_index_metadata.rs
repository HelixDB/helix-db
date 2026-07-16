use super::*;

#[test]
fn catalog_keys_and_index_metadata_reject_empty_names() {
    let scoped = ScopedPropertyKey::try_new("User", "email").unwrap();
    assert_eq!(scoped.label, "User");
    assert_eq!(scoped.property, "email");
    assert_eq!(
        ScopedPropertyKey::new(
            NonEmptyString::new("User").unwrap(),
            NonEmptyString::new("email").unwrap()
        ),
        scoped
    );
    assert!(ScopedPropertyKey::try_new("", "email").is_none());
    assert!(ScopedPropertyKey::try_new("User", "").is_none());

    let ranged =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    assert_eq!(ranged.label, "User");
    assert_eq!(ranged.property, "age");
    assert_eq!(
        ScopedPropertyDirectionKey::new(
            NonEmptyString::new("User").unwrap(),
            NonEmptyString::new("age").unwrap(),
            RangeIndexDirection::Asc
        ),
        ranged
    );
    assert!(ScopedPropertyDirectionKey::try_new("", "age", RangeIndexDirection::Asc).is_none());
    assert!(ScopedPropertyDirectionKey::try_new("User", "", RangeIndexDirection::Asc).is_none());

    let search = SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap();
    assert_eq!(search.label, "Doc");
    assert_eq!(search.property, "body");
    assert_eq!(
        SearchIndexKey::new(
            ElementKind::Node,
            NonEmptyString::new("Doc").unwrap(),
            NonEmptyString::new("body").unwrap()
        ),
        search
    );
    assert!(SearchIndexKey::try_new(ElementKind::Node, "", "body").is_none());
    assert!(SearchIndexKey::try_new(ElementKind::Node, "Doc", "").is_none());
    let node_search = NodeSearchIndexKey::try_new("Doc", "body").unwrap();
    let edge_search = EdgeSearchIndexKey::try_new("MENTIONS", "body").unwrap();
    assert_eq!(
        NodeSearchIndexKey::new(
            NonEmptyString::new("Doc").unwrap(),
            NonEmptyString::new("body").unwrap()
        ),
        node_search
    );
    assert_eq!(
        EdgeSearchIndexKey::new(
            NonEmptyString::new("MENTIONS").unwrap(),
            NonEmptyString::new("body").unwrap()
        ),
        edge_search
    );
    assert_eq!(
        SearchIndexKey::from(node_search.clone()),
        SearchIndexKey::try_new(ElementKind::Node, "Doc", "body").unwrap()
    );
    assert_eq!(
        SearchIndexKey::from(edge_search.clone()),
        SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "body").unwrap()
    );
    assert_eq!(node_search.to_string(), "node:Doc:body");
    assert_eq!(edge_search.to_string(), "edge:MENTIONS:body");
    assert_eq!(
        serde_json::to_string(&node_search).unwrap(),
        r#"{"element":"node","label":"Doc","property":"body"}"#
    );
    assert_eq!(
        serde_json::to_string(&edge_search).unwrap(),
        r#"{"element":"edge","label":"MENTIONS","property":"body"}"#
    );
    assert!(NodeSearchIndexKey::try_new("", "body").is_none());
    assert!(NodeSearchIndexKey::try_new("Doc", "").is_none());
    assert!(EdgeSearchIndexKey::try_new("", "body").is_none());
    assert!(EdgeSearchIndexKey::try_new("MENTIONS", "").is_none());
    let parsed_node_search: NodeSearchIndexKey =
        serde_json::from_str(r#"{"element":"node","label":"Doc","property":"body"}"#).unwrap();
    let parsed_edge_search: EdgeSearchIndexKey =
        serde_json::from_str(r#"{"element":"edge","label":"MENTIONS","property":"body"}"#).unwrap();
    assert_eq!(parsed_node_search, node_search);
    assert_eq!(parsed_edge_search, edge_search);

    let node_eq = NodeEqualityIndexMeta::try_new("node_eq:User:email").unwrap();
    assert_eq!(node_eq.index_id, "node_eq:User:email");
    assert_eq!(node_eq.uniqueness, IndexUniqueness::NonUnique);
    assert!(matches!(node_eq.uniqueness, IndexUniqueness::NonUnique));
    assert_eq!(
        node_eq
            .clone()
            .with_uniqueness(IndexUniqueness::Unique)
            .uniqueness,
        IndexUniqueness::Unique
    );
    assert!(NodeEqualityIndexMeta::try_new("").is_none());

    let edge_eq = EdgeEqualityIndexMeta::try_new("edge_eq:FOLLOWS:status").unwrap();
    assert_eq!(edge_eq.index_id, "edge_eq:FOLLOWS:status");
    assert!(EdgeEqualityIndexMeta::try_new("").is_none());

    let node_range = NodeRangeIndexMeta::try_new("node_range:User:age").unwrap();
    assert_eq!(node_range.index_id, "node_range:User:age");
    assert!(NodeRangeIndexMeta::try_new("").is_none());
    let edge_range = EdgeRangeIndexMeta::try_new("edge_range:FOLLOWS:since").unwrap();
    assert_eq!(edge_range.index_id, "edge_range:FOLLOWS:since");
    assert!(EdgeRangeIndexMeta::try_new("").is_none());

    let vector = VectorIndexMeta::try_new("vector:Doc:embedding", Some("tenant_id")).unwrap();
    assert_eq!(vector.index_id, "vector:Doc:embedding");
    assert_eq!(
        vector.scope,
        SearchIndexScope::Tenant {
            property: NonEmptyString::new("tenant_id").unwrap()
        }
    );
    let vector_without_tenant =
        VectorIndexMeta::try_new("vector:Doc:embedding", None::<&str>).unwrap();
    assert_eq!(vector_without_tenant.scope, SearchIndexScope::Unscoped);
    assert!(VectorIndexMeta::try_new("", Some("tenant_id")).is_none());
    assert!(VectorIndexMeta::try_new("vector:Doc:embedding", Some("")).is_none());

    let text = TextIndexMeta::try_new("text:Doc:body", Some("tenant_id")).unwrap();
    assert_eq!(text.index_id, "text:Doc:body");
    assert_eq!(
        text.scope,
        SearchIndexScope::Tenant {
            property: NonEmptyString::new("tenant_id").unwrap()
        }
    );
    let text_without_tenant = TextIndexMeta::try_new("text:Doc:body", None::<&str>).unwrap();
    assert_eq!(text_without_tenant.scope, SearchIndexScope::Unscoped);
    assert!(TextIndexMeta::try_new("", Some("tenant_id")).is_none());
    assert!(TextIndexMeta::try_new("text:Doc:body", Some("")).is_none());
    assert_eq!(
        SearchIndexScope::try_new(None::<&str>),
        Some(SearchIndexScope::Unscoped)
    );
    assert_eq!(
        SearchIndexScope::new(Some(NonEmptyString::new("tenant_id").unwrap())),
        SearchIndexScope::Tenant {
            property: NonEmptyString::new("tenant_id").unwrap()
        }
    );
    assert_eq!(SearchIndexScope::new(None), SearchIndexScope::Unscoped);
    assert_eq!(
        NodeEqualityIndexMeta::new(NonEmptyString::new("node_eq:User:email").unwrap()),
        node_eq
    );
    assert_eq!(
        EdgeEqualityIndexMeta::new(NonEmptyString::new("edge_eq:FOLLOWS:status").unwrap()),
        edge_eq
    );
    assert_eq!(
        NodeRangeIndexMeta::new(NonEmptyString::new("node_range:User:age").unwrap()),
        node_range
    );
    assert_eq!(
        EdgeRangeIndexMeta::new(NonEmptyString::new("edge_range:FOLLOWS:since").unwrap()),
        edge_range
    );
    assert_eq!(
        VectorIndexMeta::new(
            NonEmptyString::new("vector:Doc:embedding").unwrap(),
            SearchIndexScope::Unscoped
        ),
        vector_without_tenant
    );
    assert_eq!(
        TextIndexMeta::new(
            NonEmptyString::new("text:Doc:body").unwrap(),
            SearchIndexScope::Unscoped
        ),
        text_without_tenant
    );
    assert!(SearchIndexScope::try_new(Some("")).is_none());

    assert!(
        serde_json::from_str::<ScopedPropertyKey>(r#"{"label":"","property":"email"}"#).is_err()
    );
    assert!(serde_json::from_str::<SearchIndexKey>(
        r#"{"element":"node","label":"Doc","property":""}"#
    )
    .is_err());
    assert!(serde_json::from_str::<NodeSearchIndexKey>(
        r#"{"element":"edge","label":"MENTIONS","property":"body"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<NodeSearchIndexKey>(
        r#"{"element":"node","label":"Doc","property":""}"#
    )
    .is_err());
    assert!(serde_json::from_str::<EdgeSearchIndexKey>(
        r#"{"element":"node","label":"Doc","property":"body"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<EdgeSearchIndexKey>(
        r#"{"element":"edge","label":"","property":"body"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<NodeEqualityIndexMeta>(
        r#"{"index_id":"","uniqueness":"non_unique"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<EdgeEqualityIndexMeta>(r#"{"index_id":""}"#).is_err());
    assert!(serde_json::from_str::<NodeRangeIndexMeta>(r#"{"index_id":""}"#).is_err());
    assert!(serde_json::from_str::<EdgeRangeIndexMeta>(r#"{"index_id":""}"#).is_err());
    assert!(serde_json::from_str::<EdgeEqualityIndexMeta>(
        r#"{"index_id":"edge_eq:FOLLOWS:status","uniqueness":"unique"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<NodeRangeIndexMeta>(
        r#"{"index_id":"node_range:User:age","uniqueness":"unique"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<VectorIndexMeta>(
        r#"{"index_id":"vector:Doc:embedding","tenant_property":""}"#
    )
    .is_err());
    assert!(serde_json::from_str::<TextIndexMeta>(
        r#"{"index_id":"text:Doc:body","tenant_property":""}"#
    )
    .is_err());
    assert!(serde_json::from_str::<VectorIndexMeta>(
        r#"{"index_id":"vector:Doc:embedding","tenant_property":"tenant_id"}"#
    )
    .is_err());
    assert!(serde_json::from_str::<SearchIndexScope>(r#"{"tenant":{"property":""}}"#).is_err());
}

#[test]
fn index_ddl_specs_keep_node_equality_uniqueness_in_drop_identity() {
    let equality_key = ScopedPropertyKey::try_new("User", "email").unwrap();
    let create_equality = IndexDdlCreateSpec::NodeEquality {
        key: equality_key.clone(),
        uniqueness: IndexUniqueness::Unique,
    };
    let drop_equality = IndexDdlDropSpec::NodeEquality {
        key: equality_key.clone(),
        uniqueness: IndexUniqueness::Unique,
    };

    assert_eq!(
        serde_json::to_string(&create_equality).unwrap(),
        r#"{"node_equality":{"key":{"label":"User","property":"email"},"uniqueness":"unique"}}"#
    );
    assert_eq!(
        serde_json::to_string(&drop_equality).unwrap(),
        r#"{"node_equality":{"key":{"label":"User","property":"email"},"uniqueness":"unique"}}"#
    );
    assert_eq!(
        serde_json::from_str::<IndexDdlCreateSpec>(
            r#"{"node_equality":{"key":{"label":"User","property":"email"},"uniqueness":"unique"}}"#
        )
        .unwrap(),
        create_equality
    );
    assert_eq!(
        serde_json::from_str::<IndexDdlDropSpec>(
            r#"{"node_equality":{"key":{"label":"User","property":"email"},"uniqueness":"unique"}}"#
        )
        .unwrap(),
        drop_equality
    );
    assert!(serde_json::from_str::<IndexDdlDropSpec>(
        r#"{"node_equality":{"key":{"label":"User","property":"email"}}}"#
    )
    .is_err());

    let search_key = ScopedPropertyKey::try_new("Doc", "embedding").unwrap();
    let create_search = IndexDdlCreateSpec::NodeVector {
        key: search_key.clone(),
        dimension: crate::ir::VectorIndexDimension::new(3).unwrap(),
        metric: crate::ir::VectorIndexMetric::Cosine,
        scope: SearchIndexScope::Tenant {
            property: NonEmptyString::new("tenant_id").unwrap(),
        },
    };
    let drop_search = IndexDdlDropSpec::NodeVector {
        key: search_key.clone(),
    };

    assert_eq!(
        serde_json::to_value(&create_search).unwrap(),
        serde_json::json!({
            "node_vector": {
                "key": {
                    "label": "Doc",
                    "property": "embedding"
                },
                "dimension": 3,
                "metric": "cosine",
                "scope": {
                    "tenant": {
                        "property": "tenant_id"
                    }
                }
            }
        })
    );
    assert_eq!(
        serde_json::to_value(&drop_search).unwrap(),
        serde_json::json!({
            "node_vector": {
                "key": {
                    "label": "Doc",
                    "property": "embedding"
                }
            }
        })
    );
    assert!(
        serde_json::from_value::<IndexDdlDropSpec>(serde_json::json!({
            "node_vector": {
                "key": {
                    "label": "Doc",
                    "property": "embedding"
                },
                "scope": {
                    "tenant": {
                        "property": "tenant_id"
                    }
                }
            }
        }))
        .is_err()
    );
}

#[test]
fn search_index_plan_encodes_tenant_scope_as_variants() {
    let unscoped = SearchIndexPlan {
        index_id: NonEmptyString::new("vector:Doc:embedding").unwrap(),
        tenant: SearchTenantPlan::Unscoped,
    };
    let scoped = SearchIndexPlan {
        index_id: NonEmptyString::new("text:Doc:body").unwrap(),
        tenant: SearchTenantPlan::Scoped {
            property: NonEmptyString::new("tenant_id").unwrap(),
        },
    };
    let tenant_value =
        SearchTenantValuePlan::new(PropertyInputPlan::new(PropertyInput::param("tenant")).unwrap())
            .unwrap();
    assert_eq!(
        tenant_value,
        SearchTenantValuePlan::new(PropertyInputPlan::new(PropertyInput::param("tenant")).unwrap())
            .unwrap()
    );
    assert_eq!(
        SearchTenantValuePlan::new(PropertyInputPlan::Value(PropertyValue::Null)).unwrap_err(),
        SearchTenantValuePlanError::NullLiteral
    );
    assert_eq!(
        SearchTenantValuePlan::new(
            PropertyInputPlan::new(PropertyInput::from(Expr::val(PropertyValue::Null))).unwrap()
        )
        .unwrap_err(),
        SearchTenantValuePlanError::NullLiteral
    );
    assert!(serde_json::from_str::<SearchTenantValuePlan>(r#"{"value":"null"}"#).is_err());
    assert!(
        serde_json::from_str::<SearchTenantValuePlan>(r#"{"expr":{"constant":"null"}}"#).is_err()
    );
    let scoped_value = SearchIndexPlan {
        index_id: NonEmptyString::new("text:Doc:body").unwrap(),
        tenant: SearchTenantPlan::ScopedValue {
            property: NonEmptyString::new("tenant_id").unwrap(),
            value: tenant_value,
        },
    };

    assert_eq!(
        serde_json::to_string(&unscoped).unwrap(),
        r#"{"index_id":"vector:Doc:embedding","tenant":"unscoped"}"#
    );

    let serialized = serde_json::to_string(&scoped).unwrap();
    assert_eq!(
        serde_json::from_str::<SearchIndexPlan>(&serialized).unwrap(),
        scoped
    );
    let serialized = serde_json::to_string(&scoped_value).unwrap();
    assert_eq!(
        serde_json::from_str::<SearchIndexPlan>(&serialized).unwrap(),
        scoped_value
    );
    assert!(serde_json::from_str::<SearchIndexPlan>(
        r#"{"index_id":"text:Doc:body","tenant":{"scoped":{"property":""}}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<SearchIndexPlan>(
        r#"{"index_id":"text:Doc:body","tenant":{"scoped":{"property":"tenant_id","value":{"param":"tenant"}}}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<SearchIndexPlan>(
        r#"{"index_id":"text:Doc:body","tenant":{"scoped_value":{"property":"tenant_id","value":{"param":""}}}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<SearchIndexPlan>(
        r#"{"index_id":"text:Doc:body","tenant":{"scoped_value":{"property":"tenant_id","value":{"value":"null"}}}}"#
    )
    .is_err());
}
