//! Production-boundary search access and lifecycle capability tests.

use super::support::*;

#[tokio::test]
async fn node_vector_search_stays_closed_before_fully_ready() {
    let config = test_support::in_memory_config("access-node-vector-search")
        .with_node_vector_index(
            "User",
            "embedding",
            2,
            search::vector::VectorDistanceMetric::Cosine,
        );
    let db = test_support::open_db_with_config(config).await;
    let index_name =
        search::vector_index_name(config::VectorElementType::Node, "User", "embedding");
    let definition = config::VectorIndexDefinition::new_node(
        "User",
        "embedding",
        2,
        search::vector::VectorDistanceMetric::Cosine,
    )
    .expect("valid vector index definition");
    seed_vector_index::<search::vector::distance::Cosine>(&db, &definition, &[]).await;

    let plan = exec::ExecNodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new("User", "embedding").expect("valid search key"),
        index: search_index(&index_name),
        query_vector: ir::VectorQueryInputPlan::Vector(
            ir::SearchVector::new(vec![0.0, 1.0]).expect("valid vector"),
        ),
        k: literal_search_limit(1),
    };
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(plan),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Vector,
            reason: crate::error::IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable,
        })
    ));
}

#[tokio::test]
async fn manhattan_vector_search_stays_closed_before_fully_ready() {
    let config = test_support::in_memory_config("access-node-vector-search-manhattan")
        .with_node_vector_index(
            "Place",
            "location",
            2,
            search::vector::VectorDistanceMetric::Manhattan,
        );
    let db = test_support::open_db_with_config(config).await;
    let index_name =
        search::vector_index_name(config::VectorElementType::Node, "Place", "location");
    let definition = config::VectorIndexDefinition::new_node(
        "Place",
        "location",
        2,
        search::vector::VectorDistanceMetric::Manhattan,
    )
    .expect("valid vector index definition");
    seed_vector_index::<search::vector::distance::Manhattan>(&db, &definition, &[]).await;
    let plan = exec::ExecNodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new("Place", "location").expect("valid search key"),
        index: search_index(&index_name),
        query_vector: ir::VectorQueryInputPlan::Vector(
            ir::SearchVector::new(vec![2.0, 3.0]).expect("valid vector"),
        ),
        k: literal_search_limit(1),
    };
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(plan),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Vector,
            reason: crate::error::IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable,
        })
    ));
}

#[tokio::test]
async fn edge_vector_search_stays_closed_before_fully_ready() {
    let config = test_support::in_memory_config("access-edge-vector-search")
        .with_edge_vector_index(
            "SIMILAR",
            "embedding",
            2,
            search::vector::VectorDistanceMetric::Euclidean,
        );
    let db = test_support::open_db_with_config(config).await;
    let index_name =
        search::vector_index_name(config::VectorElementType::Edge, "SIMILAR", "embedding");
    let definition = config::VectorIndexDefinition::new_edge(
        "SIMILAR",
        "embedding",
        2,
        search::vector::VectorDistanceMetric::Euclidean,
    )
    .expect("valid vector index definition");
    seed_vector_index::<search::vector::distance::Euclidean>(&db, &definition, &[]).await;

    let query = test_support::name("query");
    let limit = test_support::name("limit");
    let plan = exec::ExecEdgeAccessPlan::VectorSearch {
        key: catalog::EdgeSearchIndexKey::try_new("SIMILAR", "embedding")
            .expect("valid search key"),
        index: search_index(&index_name),
        query_vector: ir::VectorQueryInputPlan::Expr(
            ir::SearchQueryExprPlan::new(Expr::param(query.as_ref())).expect("valid query expr"),
        ),
        k: ir::SearchLimitPlan::Expr(
            ir::SearchLimitExprPlan::new(Expr::param(limit.as_ref())).expect("valid limit expr"),
        ),
    };
    assert!(matches!(
        db.execute(
            &edge_access_ids_plan(plan),
            context::ParamBindings::default()
                .with_value(query, PropertyValue::I64Array(vec![2, 3]))
                .with_value(limit, PropertyValue::I64(1)),
        )
        .await,
        Err(crate::error::HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Vector,
            reason: crate::error::IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable,
        })
    ));
}

#[tokio::test]
async fn node_text_search_stays_closed_before_fully_ready_even_with_manifest() {
    let database = "access-node-text-search";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = test_support::in_memory_config_with_store(database, Arc::clone(&store))
        .with_node_text_index("Doc", "body");
    let db = test_support::open_db_with_config(config).await;
    let rust_doc = test_support::add_node_with_properties(
        &db,
        "Doc",
        vec![("body", PropertyValue::from("rust planner execution"))],
    )
    .await;
    let graph_doc = test_support::add_node_with_properties(
        &db,
        "Doc",
        vec![("body", PropertyValue::from("graph storage"))],
    )
    .await;
    let definition =
        config::TextIndexDefinition::new_node("Doc", "body").expect("valid text index definition");
    let index_name = search::text_index_name(config::TextElementType::Node, "Doc", "body");
    seed_text_manifest(
        &db,
        &store,
        database,
        &definition,
        &index_name,
        &[
            search::text::TextDocumentInput::new(rust_doc, "rust planner execution"),
            search::text::TextDocumentInput::new(graph_doc, "graph storage"),
        ],
    )
    .await;

    let query = test_support::name("query");
    let limit = test_support::name("limit");
    let plan = exec::ExecNodeAccessPlan::TextSearch {
        key: catalog::NodeSearchIndexKey::try_new("Doc", "body").expect("valid search key"),
        index: search_index(&index_name),
        query_text: ir::TextQueryInputPlan::Expr(
            ir::SearchQueryExprPlan::new(Expr::param(query.as_ref())).expect("valid query expr"),
        ),
        k: ir::SearchLimitPlan::Expr(
            ir::SearchLimitExprPlan::new(Expr::param(limit.as_ref())).expect("valid limit expr"),
        ),
    };
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(plan),
            context::ParamBindings::default()
                .with_value(query, PropertyValue::from("rust"))
                .with_value(limit, PropertyValue::I64(1)),
        )
        .await,
        Err(crate::error::HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Text,
            reason: crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
        })
    ));
}

#[tokio::test]
async fn edge_text_search_stays_closed_before_fully_ready_even_with_manifest() {
    let database = "access-edge-text-search";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = test_support::in_memory_config_with_store(database, Arc::clone(&store))
        .with_edge_text_index("MENTIONS", "body");
    let db = test_support::open_db_with_config(config).await;
    let a = test_support::add_user(&db, "a").await;
    let b = test_support::add_user(&db, "b").await;
    let planner_edge = test_support::add_edge_with_properties(
        &db,
        a,
        b,
        "MENTIONS",
        vec![("body", PropertyValue::from("planner architecture"))],
    )
    .await;
    let storage_edge = test_support::add_edge_with_properties(
        &db,
        b,
        a,
        "MENTIONS",
        vec![("body", PropertyValue::from("storage maintenance"))],
    )
    .await;
    let definition = config::TextIndexDefinition::new_edge("MENTIONS", "body")
        .expect("valid text index definition");
    let index_name = search::text_index_name(config::TextElementType::Edge, "MENTIONS", "body");
    seed_text_manifest(
        &db,
        &store,
        database,
        &definition,
        &index_name,
        &[
            search::text::TextDocumentInput::new(planner_edge, "planner architecture"),
            search::text::TextDocumentInput::new(storage_edge, "storage maintenance"),
        ],
    )
    .await;

    let plan = exec::ExecEdgeAccessPlan::TextSearch {
        key: catalog::EdgeSearchIndexKey::try_new("MENTIONS", "body").expect("valid search key"),
        index: search_index(&index_name),
        query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
        k: literal_search_limit(1),
    };
    assert!(matches!(
        db.execute(
            &edge_access_ids_plan(plan),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Text,
            reason: crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
        })
    ));
}

#[test]
fn search_vector_runtime_values_validate_shape_and_components() {
    assert_eq!(
        db_value_to_query_vector(DbPropertyValue::I64Array(vec![1, 2])).unwrap(),
        vec![1.0, 2.0]
    );
    assert_eq!(
        db_value_to_query_vector(DbPropertyValue::Array(vec![
            DbPropertyValue::I64(1),
            DbPropertyValue::F64(2.5),
        ]))
        .unwrap(),
        vec![1.0, 2.5]
    );
    assert!(validate_query_vector(Vec::new()).is_err());
    assert!(validate_query_vector(vec![1.0, f32::NAN]).is_err());
    assert!(db_value_to_query_vector(DbPropertyValue::String("nope".to_string())).is_err());
}

#[test]
fn vector_search_tenant_validation_enforces_tenant_shape() {
    let unscoped = config::VectorIndexDefinition::new_node(
        "Doc",
        "embedding",
        2,
        search::vector::VectorDistanceMetric::Cosine,
    )
    .expect("valid vector index definition");
    validate_vector_search_tenant(&unscoped, &ir::SearchTenantPlan::Unscoped, None).unwrap();
    assert!(validate_vector_search_tenant(
        &unscoped,
        &ir::SearchTenantPlan::Scoped {
            property: test_support::name("tenant_id"),
        },
        None,
    )
    .is_err());

    let tenant_value =
        ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(PropertyValue::from("acme")))
            .expect("valid tenant value");
    let scoped = config::VectorIndexDefinition::new_node(
        "Doc",
        "embedding",
        2,
        search::vector::VectorDistanceMetric::Cosine,
    )
    .expect("valid vector index definition")
    .with_tenant_property("tenant_id")
    .expect("valid tenant property");
    let tenant = DbPropertyValue::String("acme".to_string());
    validate_vector_search_tenant(
        &scoped,
        &ir::SearchTenantPlan::ScopedValue {
            property: test_support::name("tenant_id"),
            value: tenant_value,
        },
        Some(&tenant),
    )
    .unwrap();
    assert!(validate_vector_search_tenant(&scoped, &ir::SearchTenantPlan::Unscoped, None).is_err());
}

#[tokio::test]
async fn text_search_missing_manifest_stays_closed_before_fully_ready() {
    let database = "access-text-missing-manifest";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = test_support::in_memory_config_with_store(database, store)
        .with_node_text_index("Doc", "body");
    let db = test_support::open_db_with_config(config).await;
    let index_name = search::text_index_name(config::TextElementType::Node, "Doc", "body");

    let plan = exec::ExecNodeAccessPlan::TextSearch {
        key: catalog::NodeSearchIndexKey::try_new("Doc", "body").expect("valid search key"),
        index: search_index(&index_name),
        query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
        k: literal_search_limit(1),
    };
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(plan),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::IndexLifecycleUnavailable {
            family: crate::error::IndexFamily::Text,
            reason: crate::error::IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
        })
    ));
}
