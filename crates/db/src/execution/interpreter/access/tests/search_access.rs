//! Production-boundary search access and lifecycle capability tests.

use super::support::*;

#[tokio::test]
async fn node_vector_search_works_without_runtime_dependencies() {
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
    assert_eq!(
        run_node_access(&db, plan).await,
        ExecutionValue::Scalars(Vec::new())
    );
}

#[tokio::test]
async fn manhattan_vector_search_works_without_runtime_dependencies() {
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
    assert_eq!(
        run_node_access(&db, plan).await,
        ExecutionValue::Scalars(Vec::new())
    );
}

#[tokio::test]
async fn edge_vector_search_works_without_runtime_dependencies() {
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
    assert_eq!(
        run_edge_access_with_params(
            &db,
            plan,
            context::ParamBindings::default()
                .with_value(query, PropertyValue::I64Array(vec![2, 3]))
                .with_value(limit, PropertyValue::I64(1)),
        )
        .await,
        ExecutionValue::Scalars(Vec::new())
    );
}

#[tokio::test]
async fn ready_vector_dispatch_covers_all_metrics_and_typed_element_results() {
    for (database, label, metric) in [
        (
            "access-ready-vector-cosine",
            "CosineDoc",
            search::vector::VectorDistanceMetric::Cosine,
        ),
        (
            "access-ready-vector-euclidean",
            "EuclideanDoc",
            search::vector::VectorDistanceMetric::Euclidean,
        ),
        (
            "access-ready-vector-manhattan",
            "ManhattanDoc",
            search::vector::VectorDistanceMetric::Manhattan,
        ),
    ] {
        let definition =
            config::VectorIndexDefinition::new_node(label, "embedding", 2, metric).unwrap();
        let token = crate::ProcessLocalDatabaseToken::new(database).unwrap();
        let bootstrap = HelixDB::open(crate::HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .unwrap();
        let entity_id = test_support::add_node_with_properties(
            &bootstrap,
            label,
            vec![("name", PropertyValue::from("matched"))],
        )
        .await;
        bootstrap.close().await.unwrap();
        let db = HelixDB::open_with_process_local_token_for_tests(token)
            .await
            .unwrap();
        match metric {
            search::vector::VectorDistanceMetric::Cosine => {
                seed_vector_index::<search::vector::distance::Cosine>(
                    &db,
                    &definition,
                    &[(entity_id, vec![1.0, 0.0])],
                )
                .await;
            }
            search::vector::VectorDistanceMetric::Euclidean => {
                seed_vector_index::<search::vector::distance::Euclidean>(
                    &db,
                    &definition,
                    &[(entity_id, vec![1.0, 0.0])],
                )
                .await;
            }
            search::vector::VectorDistanceMetric::Manhattan => {
                seed_vector_index::<search::vector::distance::Manhattan>(
                    &db,
                    &definition,
                    &[(entity_id, vec![1.0, 0.0])],
                )
                .await;
            }
        }
        db.refresh_runtime_catalog(crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped)
            .await
            .unwrap();
        let index_name =
            search::vector_index_name(config::VectorElementType::Node, label, "embedding");
        let result = run_node_access(
            &db,
            exec::ExecNodeAccessPlan::VectorSearch {
                key: catalog::NodeSearchIndexKey::try_new(label, "embedding").unwrap(),
                index: search_index(&index_name),
                query_vector: ir::VectorQueryInputPlan::Vector(
                    ir::SearchVector::new(vec![1.0, 0.0]).unwrap(),
                ),
                k: literal_search_limit(1),
            },
        )
        .await;
        assert_eq!(
            result,
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(entity_id)])
        );
    }

    let edge_definition = config::VectorIndexDefinition::new_edge(
        "SIMILAR",
        "embedding",
        2,
        search::vector::VectorDistanceMetric::Euclidean,
    )
    .unwrap();
    let edge_token = crate::ProcessLocalDatabaseToken::new("access-ready-vector-edge").unwrap();
    let bootstrap = HelixDB::open(crate::HelixDbSource::InMemoryToken {
        token: edge_token.clone(),
    })
    .await
    .unwrap();
    let from = test_support::add_user(&bootstrap, "from").await;
    let to = test_support::add_user(&bootstrap, "to").await;
    let edge_id = test_support::add_edge(&bootstrap, from, to, "SIMILAR").await;
    bootstrap.close().await.unwrap();
    let edge_db = HelixDB::open_with_process_local_token_for_tests(edge_token)
        .await
        .unwrap();
    seed_vector_index::<search::vector::distance::Euclidean>(
        &edge_db,
        &edge_definition,
        &[(edge_id, vec![1.0, 0.0])],
    )
    .await;
    edge_db
        .refresh_runtime_catalog(crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped)
        .await
        .unwrap();
    let edge_index_name =
        search::vector_index_name(config::VectorElementType::Edge, "SIMILAR", "embedding");
    let result = run_edge_access(
        &edge_db,
        exec::ExecEdgeAccessPlan::VectorSearch {
            key: catalog::EdgeSearchIndexKey::try_new("SIMILAR", "embedding").unwrap(),
            index: search_index(&edge_index_name),
            query_vector: ir::VectorQueryInputPlan::Vector(
                ir::SearchVector::new(vec![1.0, 0.0]).unwrap(),
            ),
            k: literal_search_limit(1),
        },
    )
    .await;
    assert_eq!(
        result,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(edge_id)])
    );
}

#[tokio::test]
async fn vector_dispatch_missing_definition_fails_before_input_evaluation() {
    let label = "CosineDoc";
    let token = crate::ProcessLocalDatabaseToken::new("access-missing-vector").unwrap();
    let db = HelixDB::open_with_process_local_token_for_tests(token)
        .await
        .unwrap();
    let index_name = search::vector_index_name(config::VectorElementType::Node, label, "embedding");
    let plan = exec::ExecNodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new(label, "embedding").unwrap(),
        index: search_index(&index_name),
        query_vector: ir::VectorQueryInputPlan::Vector(
            ir::SearchVector::new(vec![1.0, 0.0]).unwrap(),
        ),
        k: literal_search_limit(1),
    };

    assert!(matches!(
        db.execute(
            &node_access_ids_plan(plan),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::IndexNotFound(_))
    ));

    let missing_query_plan = exec::ExecNodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new(label, "embedding").unwrap(),
        index: search_index(&index_name),
        query_vector: ir::VectorQueryInputPlan::Expr(
            ir::SearchQueryExprPlan::new(Expr::param("missing")).unwrap(),
        ),
        k: literal_search_limit(1),
    };
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(missing_query_plan),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::IndexNotFound(_))
    ));
}

#[tokio::test]
async fn node_text_search_works_without_lifecycle_runtime_when_manifest_exists() {
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
    db.close().await.expect("first Active text write settles");
    let db = test_support::open_db_with_object_store(database, Arc::clone(&store)).await;
    let graph_doc = test_support::add_node_with_properties(
        &db,
        "Doc",
        vec![("body", PropertyValue::from("graph storage"))],
    )
    .await;
    db.close().await.expect("second Active text write settles");
    let db = test_support::open_db_with_object_store(database, Arc::clone(&store)).await;
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
    db.close().await.expect("ready text fixture closes");
    let db = test_support::open_db_with_object_store(database, store).await;

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
    assert_eq!(
        run_node_access_with_params(
            &db,
            plan,
            context::ParamBindings::default()
                .with_value(query, PropertyValue::from("rust"))
                .with_value(limit, PropertyValue::I64(1)),
        )
        .await,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(rust_doc)])
    );
    db.close().await.expect("writer closes");
}

#[tokio::test]
async fn edge_text_search_works_without_lifecycle_runtime_when_manifest_exists() {
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
    db.close()
        .await
        .expect("first Active text edge write settles");
    let db = test_support::open_db_with_object_store(database, Arc::clone(&store)).await;
    let storage_edge = test_support::add_edge_with_properties(
        &db,
        b,
        a,
        "MENTIONS",
        vec![("body", PropertyValue::from("storage maintenance"))],
    )
    .await;
    db.close()
        .await
        .expect("second Active text edge write settles");
    let db = test_support::open_db_with_object_store(database, Arc::clone(&store)).await;
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
    db.close().await.expect("ready text fixture closes");
    let db = test_support::open_db_with_object_store(database, store).await;

    let plan = exec::ExecEdgeAccessPlan::TextSearch {
        key: catalog::EdgeSearchIndexKey::try_new("MENTIONS", "body").expect("valid search key"),
        index: search_index(&index_name),
        query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
        k: literal_search_limit(1),
    };
    assert_eq!(
        run_edge_access(&db, plan).await,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(planner_edge)])
    );
    db.close().await.expect("writer closes");
}

#[tokio::test]
async fn ready_text_dispatch_returns_managed_node_and_edge_hits() {
    let node_token = crate::ProcessLocalDatabaseToken::new("access-ready-text-node").unwrap();
    let bootstrap = HelixDB::open(crate::HelixDbSource::InMemoryToken {
        token: node_token.clone(),
    })
    .await
    .unwrap();
    let node_id = test_support::add_node_with_properties(
        &bootstrap,
        "Doc",
        vec![("body", PropertyValue::from("rust planner execution"))],
    )
    .await;
    bootstrap.close().await.unwrap();
    let node_definition = config::TextIndexDefinition::new_node("Doc", "body").unwrap();
    let node_db = HelixDB::open_with_process_local_token_for_tests(node_token)
        .await
        .unwrap();
    seed_managed_text_index(
        &node_db,
        &node_definition,
        &[search::text::TextDocumentInput::new(
            node_id,
            "rust planner execution",
        )],
    )
    .await;
    node_db
        .refresh_runtime_catalog(crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped)
        .await
        .unwrap();
    let node_index_name = search::text_index_name(config::TextElementType::Node, "Doc", "body");
    let node_result = run_node_access(
        &node_db,
        exec::ExecNodeAccessPlan::TextSearch {
            key: catalog::NodeSearchIndexKey::try_new("Doc", "body").unwrap(),
            index: search_index(&node_index_name),
            query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
            k: literal_search_limit(1),
        },
    )
    .await;
    assert_eq!(
        node_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(node_id)])
    );

    let edge_token = crate::ProcessLocalDatabaseToken::new("access-ready-text-edge").unwrap();
    let bootstrap = HelixDB::open(crate::HelixDbSource::InMemoryToken {
        token: edge_token.clone(),
    })
    .await
    .unwrap();
    let from = test_support::add_user(&bootstrap, "from").await;
    let to = test_support::add_user(&bootstrap, "to").await;
    let edge_id = test_support::add_edge_with_properties(
        &bootstrap,
        from,
        to,
        "MENTIONS",
        vec![("body", PropertyValue::from("storage planner architecture"))],
    )
    .await;
    bootstrap.close().await.unwrap();
    let edge_definition = config::TextIndexDefinition::new_edge("MENTIONS", "body").unwrap();
    let edge_db = HelixDB::open_with_process_local_token_for_tests(edge_token)
        .await
        .unwrap();
    seed_managed_text_index(
        &edge_db,
        &edge_definition,
        &[search::text::TextDocumentInput::new(
            edge_id,
            "storage planner architecture",
        )],
    )
    .await;
    edge_db
        .refresh_runtime_catalog(crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped)
        .await
        .unwrap();
    let edge_index_name =
        search::text_index_name(config::TextElementType::Edge, "MENTIONS", "body");
    let edge_result = run_edge_access(
        &edge_db,
        exec::ExecEdgeAccessPlan::TextSearch {
            key: catalog::EdgeSearchIndexKey::try_new("MENTIONS", "body").unwrap(),
            index: search_index(&edge_index_name),
            query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
            k: literal_search_limit(1),
        },
    )
    .await;
    assert_eq!(
        edge_result,
        ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(edge_id)])
    );
}

#[tokio::test]
async fn text_dispatch_reports_missing_definition_and_manifest_corruption() {
    let missing_token =
        crate::ProcessLocalDatabaseToken::new("access-missing-text-generation").unwrap();
    let missing_db = HelixDB::open_with_process_local_token_for_tests(missing_token)
        .await
        .unwrap();
    let missing_index_name =
        search::text_index_name(config::TextElementType::Node, "MissingDoc", "body");
    let missing_plan = exec::ExecNodeAccessPlan::TextSearch {
        key: catalog::NodeSearchIndexKey::try_new("MissingDoc", "body").unwrap(),
        index: search_index(&missing_index_name),
        query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
        k: literal_search_limit(1),
    };
    assert!(matches!(
        missing_db
            .execute(
                &node_access_ids_plan(missing_plan),
                context::ParamBindings::default(),
            )
            .await,
        Err(crate::error::HelixDbError::IndexNotFound(_))
    ));
    let missing_query_plan = exec::ExecNodeAccessPlan::TextSearch {
        key: catalog::NodeSearchIndexKey::try_new("MissingDoc", "body").unwrap(),
        index: search_index(&missing_index_name),
        query_text: ir::TextQueryInputPlan::Expr(
            ir::SearchQueryExprPlan::new(Expr::param("missing")).unwrap(),
        ),
        k: literal_search_limit(1),
    };
    assert!(matches!(
        missing_db
            .execute(
                &node_access_ids_plan(missing_query_plan),
                context::ParamBindings::default(),
            )
            .await,
        Err(crate::error::HelixDbError::IndexNotFound(_))
    ));
    let token = crate::ProcessLocalDatabaseToken::new("access-corrupt-text-manifest").unwrap();
    let bootstrap = HelixDB::open(crate::HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .unwrap();
    let document = test_support::add_node_with_properties(
        &bootstrap,
        "Doc",
        vec![("body", PropertyValue::from("rust planner execution"))],
    )
    .await;
    bootstrap.close().await.unwrap();
    let definition = config::TextIndexDefinition::new_node("Doc", "body").unwrap();
    let db = HelixDB::open_with_process_local_token_for_tests(token)
        .await
        .unwrap();
    let root = seed_managed_text_index(
        &db,
        &definition,
        &[search::text::TextDocumentInput::new(
            document,
            "rust planner execution",
        )],
    )
    .await;
    db.refresh_runtime_catalog(crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped)
        .await
        .unwrap();
    let root_key = crate::encoding::v2::keys::ManagedIndexKey::Data {
        scope: crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        kind: crate::encoding::v2::keys::ScopedKey::TextManifestRoot(root),
    }
    .to_bytes();
    let page_key = crate::encoding::v2::keys::ManagedIndexKey::Data {
        scope: crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
        kind: crate::encoding::v2::keys::ScopedKey::TextManifestPage(
            crate::encoding::v2::keys::TextManifestPageKey { root, page: 0 },
        ),
    }
    .to_bytes();
    let original_root = db
        .inner_db()
        .get(&root_key)
        .await
        .expect("manifest root reads")
        .expect("managed fixture has a root");
    let original_page = db
        .inner_db()
        .get(&page_key)
        .await
        .expect("manifest page reads")
        .expect("managed fixture has a page");
    db.inner_db()
        .put(
            &root_key,
            bytes::Bytes::from_static(b"corrupt manifest root"),
        )
        .await
        .expect("corrupt manifest root writes");
    let index_name = search::text_index_name(config::TextElementType::Node, "Doc", "body");
    let plan = || exec::ExecNodeAccessPlan::TextSearch {
        key: catalog::NodeSearchIndexKey::try_new("Doc", "body").unwrap(),
        index: search_index(&index_name),
        query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
        k: literal_search_limit(1),
    };
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(plan()),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::Encoding(_))
    ));

    db.inner_db()
        .put(&root_key, original_root)
        .await
        .expect("valid manifest root restores");
    db.inner_db()
        .put(
            &page_key,
            bytes::Bytes::from_static(b"corrupt manifest page"),
        )
        .await
        .expect("corrupt manifest page writes");
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(plan()),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::Encoding(_))
    ));

    db.inner_db()
        .put(&page_key, original_page)
        .await
        .expect("valid manifest page restores");
    let partition = crate::index_lifecycle::work::TextPartition::Unpartitioned;
    let mismatched_root = crate::index_lifecycle::work::TextManifestRootValue::try_new(
        root.index_id,
        root.generation,
        partition,
        crate::index_lifecycle::TextManifestRevision::new(2)
            .expect("one prepared page advances the root revision"),
        1,
        2,
    )
    .expect("one page may declare two total splits");
    db.inner_db()
        .put(
            &root_key,
            crate::encoding::v2::values::encode_manifest_root(&mismatched_root),
        )
        .await
        .expect("mismatched manifest root writes");
    assert!(matches!(
        db.execute(
            &node_access_ids_plan(plan()),
            context::ParamBindings::default(),
        )
        .await,
        Err(crate::error::HelixDbError::IndexCatalogCorruption(message))
            if message.contains("disagree with their root split count")
    ));
}

#[tokio::test]
async fn text_dispatch_returns_empty_for_an_absent_managed_tenant_partition() {
    let definition = config::TextIndexDefinition::new_node("Doc", "body")
        .unwrap()
        .with_tenant_property("tenant_id")
        .unwrap();
    let token = crate::ProcessLocalDatabaseToken::new("access-absent-text-partition").unwrap();
    let db = HelixDB::open_with_process_local_token_for_tests(token)
        .await
        .unwrap();
    let index_id = crate::index_lifecycle::IndexId::initial();
    let generation = crate::index_lifecycle::IndexGenerationId::initial();
    let validated =
        crate::index_lifecycle::ValidatedTextIndexDefinition::try_from_runtime(&definition)
            .expect("tenant text definition validates");
    let record = crate::index_lifecycle::IndexRecordV2::building(
        index_id,
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Text(validated),
        crate::index_lifecycle::IndexRevision::initial(),
        crate::index_lifecycle::PhysicalGeneration::Text { generation },
        crate::index_lifecycle::IndexOperationId::new_v4(),
    )
    .expect("tenant text record starts building")
    .transition(crate::index_lifecycle::IndexStateTransition::Activate)
    .expect("tenant text record activates");
    db.inner_db()
        .put(
            crate::encoding::v2::keys::ManagedIndexKey::Data {
                scope: crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
                kind: crate::encoding::v2::keys::ScopedKey::index_record(record.identity().clone()),
            }
            .to_bytes(),
            crate::encoding::v2::values::encode_index_record(&record),
        )
        .await
        .expect("active tenant text record writes");
    db.refresh_runtime_catalog(crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped)
        .await
        .expect("active tenant text definition refreshes");
    let index_name = search::text_index_name(config::TextElementType::Node, "Doc", "body");
    let plan = exec::ExecNodeAccessPlan::TextSearch {
        key: catalog::NodeSearchIndexKey::try_new("Doc", "body").unwrap(),
        index: ir::SearchIndexPlan {
            index_id: test_support::name(&index_name),
            tenant: ir::SearchTenantPlan::ScopedValue {
                property: test_support::name("tenant_id"),
                value: ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Expr(
                    ir::PropertyInputExprPlan::new(Expr::param("tenant")).unwrap(),
                ))
                .unwrap(),
            },
        },
        query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
        k: literal_search_limit(1),
    };

    let result = db
        .execute(
            &node_access_ids_plan(plan),
            context::ParamBindings::default()
                .with_value(test_support::name("tenant"), PropertyValue::Null),
        )
        .await
        .expect("absent managed tenant is an empty search")
        .last
        .expect("project step returns a value");
    assert_eq!(result, ExecutionValue::Scalars(Vec::new()));
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
async fn text_search_without_a_manifest_is_empty() {
    let database = "access-text-missing-manifest";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let config = test_support::in_memory_config_with_store(database, Arc::clone(&store))
        .with_node_text_index("Doc", "body");
    let db = test_support::open_db_with_config(config).await;
    db.close().await.expect("ready text fixture closes");
    let db = test_support::open_db_with_object_store(database, store).await;
    let index_name = search::text_index_name(config::TextElementType::Node, "Doc", "body");

    let plan = exec::ExecNodeAccessPlan::TextSearch {
        key: catalog::NodeSearchIndexKey::try_new("Doc", "body").expect("valid search key"),
        index: search_index(&index_name),
        query_text: ir::TextQueryInputPlan::Text(test_support::name("planner")),
        k: literal_search_limit(1),
    };
    assert_eq!(
        run_node_access(&db, plan).await,
        ExecutionValue::Scalars(Vec::new())
    );
    db.close().await.expect("writer closes");
}
