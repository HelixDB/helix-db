//! Public production-linked compatibility contracts.
//!
//! This target imports the compiled `db` library without `cfg(test)`. It covers
//! only supported public contracts. Physical vector rows, cache stores, raw
//! metadata DTOs, and the direct HNSW facade are exercised by the feature-gated
//! internal production-contract target instead of being kept public for tests.

use std::{
    cmp::Ordering, collections::BTreeMap, future::Future, num::NonZeroUsize, sync::Arc,
    time::Duration,
};

use db::config::{self, VectorIndexDefinition};
use db::encoding::v2::values::indexes::vector::{decode_layer0_neighbors, encode_layer0_neighbors};
use db::execution::interpreter::{
    ElementRef, ExecutionResult, ExecutionScalar, ExecutionValue, ReturnedValue,
};
use db::search::vector::distance::{Cosine, Distance, Euclidean, Manhattan};
use db::search::vector::unaligned_vector::{SimHashError, UnalignedVector};
use db::search::vector::{
    CollisionThreshold, Connections, ConstructionBeamWidth, DistanceScore, FailureProbability,
    Item, Layer0Connections, LayerMultiplier, ResultCount, SameDimensionPair, SearchBeamWidth,
    SearchParams, SimHashMode, SimHasher, UnitInterval, VectorDimension, VectorDistanceMetric,
    VectorParameterError, VectorRef,
};
use db::{HelixDB, HelixDbMode, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::batch;
use helix_ast::expr::{CompareOp, Expr, Predicate, StreamBound};
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::index;
use helix_ast::projection::{BindingProjection, BindingValueRef, Projection};
use helix_ast::query::{QueryRequest, QueryValue};
use helix_ast::traversal;
use helix_ast::value::{PropertyInput, PropertyValue};
use helix_planner::{catalog, context, cost, exec, ir, planning, properties, trace};
use slatedb::object_store::memory::InMemory;

fn run_high_stack_contract<F, Fut>(name: &'static str, contract: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("production contract runtime builds")
                .block_on(contract());
        })
        .expect("production contract thread starts")
        .join()
        .expect("production contract thread completes");
}

#[test]
fn current_layer0_neighbor_bytes_are_stable_through_the_public_codec() {
    let encoded = encode_layer0_neighbors(&[9, 2, 9]);
    assert_eq!(
        encoded.as_ref(),
        &[
            0x12, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
        ]
    );
    assert_eq!(decode_layer0_neighbors(&encoded).unwrap(), vec![2, 9]);
}

#[test]
#[allow(deprecated)]
fn root_tenant_compatibility_path_reexports_v2_scope_types() {
    use db::encoding::keys::tenant::{DataScope, TenantId};

    let tenant = TenantId::from_ulid_str("00000000000000000000000001")
        .expect("compatibility path decodes tenant IDs");
    let compatibility_scope = DataScope::Tenant(tenant);
    let canonical_scope: db::encoding::v2::keys::scope::DataScope = compatibility_scope;

    assert_eq!(canonical_scope, DataScope::Tenant(tenant));
}

#[test]
fn public_vector_definition_owns_validated_configuration() {
    let definition =
        VectorIndexDefinition::new_node("Document", "embedding", 384, VectorDistanceMetric::Cosine)
            .unwrap();
    assert_eq!(definition.dimension(), 384);
    assert_eq!(definition.m0(), definition.m() * 2);
    assert_eq!(VectorDistanceMetric::Cosine.as_str(), "cosine");
    assert_eq!(VectorDistanceMetric::Euclidean.as_str(), "euclidean");
    assert_eq!(VectorDistanceMetric::Manhattan.as_str(), "manhattan");

    assert!(VectorIndexDefinition::new_node(
        "Document",
        "embedding",
        0,
        VectorDistanceMetric::Cosine,
    )
    .is_err());
    assert!(definition.clone().with_m(0).is_err());
    assert!(definition.clone().with_m0(0).is_err());
    assert!(definition.clone().with_ef_construction(0).is_err());
    assert!(definition.clone().with_ml(f32::NAN).is_err());
    assert!(definition.clone().with_sampling_ratio(2.0).is_err());
    assert!(definition.with_adaptive_failure_prob(1.0).is_err());
}

#[tokio::test]
async fn public_in_memory_writer_opens_and_closes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-coverage-baseline".to_owned(),
    })
    .await
    .expect("in-memory writer opens");

    assert_eq!(db.mode(), HelixDbMode::Writer);
    assert!(db.is_writer_mode());
    assert!(!db.is_reader_mode());
    db.close().await.expect("in-memory writer closes");
}

#[tokio::test]
async fn public_hybrid_cache_opens_with_bounded_foyer_partitions() {
    let root = tempfile::tempdir().expect("temporary cache root");
    let foyer_root = root.path().join("foyer");
    let disk_bytes = 16 * 1024 * 1024;
    let cache = config::CacheConfig::new(
        config::VectorMemorySettings::default(),
        config::CacheMode::Hybrid {
            slate_db: config::SlateHybridCacheConfig::try_new(1024 * 1024, &foyer_root, disk_bytes)
                .expect("valid Slate hybrid cache"),
            object_store: config::SlateObjectStoreCacheSettings::try_new(
                root.path().join("object-store"),
                Some(1024 * 1024),
                4096,
                false,
                config::ObjectStoreWarmLevel::Off,
                None,
                1,
            )
            .expect("valid object-store cache"),
            slate_warm: config::SlateWarmConfig::Off,
            fts: None,
        },
    );
    let db = HelixDB::open_with_object_store_and_config(
        "production-hybrid-cache-contract",
        Arc::new(InMemory::new()),
        config::DbConfig::new().with_cache(cache),
    )
    .await
    .expect("hybrid-cache writer opens");

    assert_eq!(
        db.cache_stats().foyer_hybrid_disk.state,
        db::CacheTierState::Ready {
            used_bytes: 0,
            capacity_bytes: Some(disk_bytes as u64),
        }
    );
    assert_eq!(
        std::fs::read_dir(&foyer_root)
            .expect("Foyer cache directory exists")
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("foyer-storage-direct-fs-")
            })
            .count(),
        256
    );
    db.close().await.expect("hybrid-cache writer closes");
}

#[test]
fn public_query_response_exposes_telemetry_safe_planner_diagnostics() {
    const SECRET_LITERAL: &str = "secret-query-value";

    let response = db::query_service::QueryResponse::from_execution_result_with_diagnostics(
        ExecutionResult {
            last: None,
            variables: BTreeMap::from([(
                helix_planner::ir::NonEmptyString::from_static("private"),
                ExecutionValue::Scalars(vec![ExecutionScalar::String(SECRET_LITERAL.to_owned())]),
            )]),
            returns: BTreeMap::from([(
                helix_planner::ir::NonEmptyString::from_static("count"),
                ReturnedValue::Present(ExecutionValue::Count(0)),
            )]),
        },
        helix_planner::diagnostics::PlannerDiagnostics {
            statistics: helix_planner::diagnostics::PlannerStatistics::default(),
            insights: vec![
                helix_planner::diagnostics::PlannerInsight::UnboundedScan(
                    helix_planner::diagnostics::UnboundedScanInsight {
                        element: helix_planner::catalog::ElementKind::Node,
                        label: Some(helix_planner::ir::NonEmptyString::from_static("User")),
                        predicate_properties: helix_planner::diagnostics::PredicatePropertySet::new(
                            [helix_planner::ir::NonEmptyString::from_static("username")],
                        ),
                        occurrences: 1,
                    },
                ),
                helix_planner::diagnostics::PlannerInsight::MissingIndex(
                    helix_planner::diagnostics::MissingIndexInsight {
                        element: helix_planner::catalog::ElementKind::Node,
                        label: helix_planner::ir::NonEmptyString::from_static("User"),
                        property: helix_planner::ir::NonEmptyString::from_static("username"),
                        index_kind: helix_planner::diagnostics::SecondaryIndexKind::Equality,
                        occurrences: 1,
                    },
                ),
            ],
        },
    )
    .expect("diagnostic response converts");

    assert_eq!(response.returns().get("count"), Some(&serde_json::json!(0)));
    let missing_index = response
        .diagnostics()
        .insights
        .iter()
        .find_map(|insight| match insight {
            helix_planner::diagnostics::PlannerInsight::MissingIndex(insight) => Some(insight),
            helix_planner::diagnostics::PlannerInsight::UnboundedScan(_)
            | helix_planner::diagnostics::PlannerInsight::DeepTraversal(_) => None,
        })
        .expect("selected residual filter recommends an index");
    assert_eq!(missing_index.label.to_string(), "User");
    assert_eq!(missing_index.property.to_string(), "username");
    let unbounded_scan = response
        .diagnostics()
        .insights
        .iter()
        .find_map(|insight| match insight {
            helix_planner::diagnostics::PlannerInsight::UnboundedScan(insight) => Some(insight),
            helix_planner::diagnostics::PlannerInsight::MissingIndex(_)
            | helix_planner::diagnostics::PlannerInsight::DeepTraversal(_) => None,
        })
        .expect("unbounded scan facts survive the response metadata boundary");
    assert_eq!(unbounded_scan.label.as_ref().unwrap().as_ref(), "User");
    assert_eq!(
        unbounded_scan
            .predicate_properties
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>(),
        ["username"]
    );

    let diagnostics_json = serde_json::to_string(response.diagnostics())
        .expect("diagnostics serialize for metadata transports");
    assert!(!diagnostics_json.contains(SECRET_LITERAL));
    assert_eq!(
        response
            .to_json_bytes()
            .expect("public response serializes"),
        br#"{"count":0}"#
    );

    let default_response =
        db::query_service::QueryResponse::from_execution_result(ExecutionResult {
            last: None,
            variables: BTreeMap::new(),
            returns: BTreeMap::new(),
        })
        .expect("default diagnostic response converts");
    assert!(default_response.diagnostics().insights.is_empty());
}

#[tokio::test]
async fn public_shared_runtime_keeps_search_families_available() {
    let db = HelixDB::open_with_object_store(
        "production-search-runtime-unavailable",
        Arc::new(InMemory::new()),
    )
    .await
    .expect("shared object-store writer opens without an external authority");
    let access_plan = |access| {
        let root_id = exec::ExecStepId::first();
        exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::from_one(exec::ExecStep {
                id: root_id,
                dependencies: Vec::new(),
                output: ir::BatchOutputPlan::Discard,
                semantic_return_shape: None,
                condition: exec::ExecCondition::Always,
                op: exec::ExecOp::Access {
                    plan: Box::new(access),
                },
                schedule: exec::ExecSchedule::Pipeline,
                delivered: properties::DeliveredProperties::default(),
                cost: cost::CostVector::ZERO,
            }),
            root_id,
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("single-step unavailable-search plan validates")
    };
    let search_index = |index_id: &str| ir::SearchIndexPlan {
        index_id: ir::NonEmptyString::new(index_id).expect("index ID is non-empty"),
        tenant: ir::SearchTenantPlan::Unscoped,
    };
    let k = || ir::SearchLimitPlan::Literal(NonZeroUsize::MIN);

    let vector = access_plan(exec::ExecAccessPlan::Node(
        exec::ExecNodeAccessPlan::VectorSearch {
            key: catalog::NodeSearchIndexKey::try_new("Document", "embedding")
                .expect("vector search key validates"),
            index: search_index("missing-vector"),
            query_vector: ir::VectorQueryInputPlan::Vector(
                ir::SearchVector::new(vec![1.0, 0.0]).expect("query vector validates"),
            ),
            k: k(),
        },
    ));
    let error = db
        .execute(&vector, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert_eq!(error.index_error_code(), Some("index_not_found"));

    let vector = access_plan(exec::ExecAccessPlan::Edge(
        exec::ExecEdgeAccessPlan::VectorSearch {
            key: catalog::EdgeSearchIndexKey::try_new("LINK", "embedding")
                .expect("edge vector search key validates"),
            index: search_index("missing-edge-vector"),
            query_vector: ir::VectorQueryInputPlan::Vector(
                ir::SearchVector::new(vec![1.0, 0.0]).expect("query vector validates"),
            ),
            k: k(),
        },
    ));
    let error = db
        .execute(&vector, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert_eq!(error.index_error_code(), Some("index_not_found"));

    let text = access_plan(exec::ExecAccessPlan::Node(
        exec::ExecNodeAccessPlan::TextSearch {
            key: catalog::NodeSearchIndexKey::try_new("Document", "body")
                .expect("text search key validates"),
            index: search_index("unavailable-text"),
            query_text: ir::TextQueryInputPlan::Text(
                ir::NonEmptyString::new("needle").expect("text query is non-empty"),
            ),
            k: k(),
        },
    ));
    let error = db
        .execute(&text, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert_eq!(error.index_error_code(), Some("index_not_found"));

    let text = access_plan(exec::ExecAccessPlan::Edge(
        exec::ExecEdgeAccessPlan::TextSearch {
            key: catalog::EdgeSearchIndexKey::try_new("LINK", "body")
                .expect("edge text search key validates"),
            index: search_index("unavailable-edge-text"),
            query_text: ir::TextQueryInputPlan::Text(
                ir::NonEmptyString::new("needle").expect("text query is non-empty"),
            ),
            k: k(),
        },
    ));
    let error = db
        .execute(&text, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert_eq!(error.index_error_code(), Some("index_not_found"));

    for (index, spec) in [
        index::IndexSpec::node_equality("Document", "category"),
        index::IndexSpec::node_vector(
            "Document",
            "embedding",
            NonZeroUsize::new(2).expect("vector dimension is nonzero"),
            index::VectorDistanceMetric::Euclidean,
            None::<String>,
        ),
        index::IndexSpec::node_text("Document", "body", None::<String>),
    ]
    .into_iter()
    .enumerate()
    {
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as("operation", traversal::g().create_index_if_not_exists(spec))
                .returning(["operation"]),
        ))
        .await
        .unwrap_or_else(|error| panic!("available index family case {index} failed: {error}"));
    }

    db.close().await.expect("shared writer closes");
}

#[tokio::test]
async fn public_query_boundary_covers_mutation_projection_aggregate_and_range() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-corpus".to_owned(),
    })
    .await
    .expect("production interpreter fixture opens");
    let requests = [
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g()
                        .add_n(
                            "Document",
                            vec![
                                ("name", PropertyInput::from("alice")),
                                ("rank", PropertyInput::from(1_i64)),
                            ],
                        )
                        .id(),
                )
                .returning(["created"]),
        ),
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "point",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .values(vec!["name", "rank"]),
                )
                .var_as(
                    "projection",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .value_map(Some(vec!["name", "rank"])),
                )
                .var_as(
                    "sparse_values",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .values(vec!["missing", "name"]),
                )
                .var_as(
                    "selected_map",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .value_map(Some(vec!["$id", "missing", "name"])),
                )
                .var_as("count", traversal::g().n(NodeRef::all()).count())
                .returning([
                    "point",
                    "projection",
                    "sparse_values",
                    "selected_map",
                    "count",
                ]),
        ),
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "updated",
                    traversal::g().n(NodeRef::id(0)).set_property("rank", 2_i64),
                )
                .returning(Vec::<String>::new()),
        ),
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "range",
                    traversal::g()
                        .n(NodeRef::all())
                        .range(0_usize, 8_usize)
                        .values(vec!["rank"]),
                )
                .var_as("count", traversal::g().n(NodeRef::all()).count())
                .returning(["range", "count"]),
        ),
    ];
    let expected = [
        serde_json::json!({ "created": [0] }),
        serde_json::json!({
            "count": 1,
            "point": [{ "name": "alice", "rank": 1 }],
            "projection": [{ "name": "alice", "rank": 1 }],
            "sparse_values": [{ "name": "alice" }],
            "selected_map": [{ "$id": 0, "name": "alice" }],
        }),
        serde_json::json!({}),
        serde_json::json!({
            "count": 1,
            "range": [{ "rank": 2 }],
        }),
    ];

    for (request, expected) in requests.into_iter().zip(expected) {
        assert_eq!(db.query(request).await.unwrap(), expected);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_source_fed_mutations() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-source-fed-node-creation".to_owned(),
    })
    .await
    .expect("source-fed node fixture opens");

    let seeds = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n("Seed", vec![("name", PropertyInput::from("alpha"))]),
        )
        .var_as(
            "second",
            traversal::g().add_n("Seed", vec![("name", PropertyInput::from("beta"))]),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(seeds)).await.unwrap();

    let created = batch::write_batch()
        .var_as(
            "created",
            traversal::g()
                .n_with_label("Seed")
                .add_n(
                    "Audit",
                    vec![
                        ("origin", PropertyInput::from(Expr::prop("name"))),
                        ("kind", PropertyInput::from("source-fed")),
                    ],
                )
                .id(),
        )
        .returning(["created"]);
    assert_eq!(
        db.query(QueryRequest::write(created)).await.unwrap(),
        serde_json::json!({ "created": [2, 3] })
    );

    let empty = batch::write_batch()
        .var_as(
            "created",
            traversal::g()
                .n_with_label("Missing")
                .add_n("Audit", vec![("kind", PropertyInput::from("unreachable"))])
                .id(),
        )
        .returning(["created"]);
    assert_eq!(
        db.query(QueryRequest::write(empty)).await.unwrap(),
        serde_json::json!({ "created": [] })
    );

    let audit_rows = batch::read_batch()
        .var_as(
            "audits",
            traversal::g()
                .n_with_label("Audit")
                .value_map(Some(vec!["$label", "origin", "kind"])),
        )
        .returning(["audits"]);
    assert_eq!(
        db.query(QueryRequest::read(audit_rows)).await.unwrap(),
        serde_json::json!({
            "audits": [
                { "$label": "Audit", "origin": "alpha", "kind": "source-fed" },
                { "$label": "Audit", "origin": "beta", "kind": "source-fed" },
            ]
        })
    );

    let created_edges = batch::write_batch()
        .var_as(
            "created",
            traversal::g()
                .n_with_label("Seed")
                .add_e(
                    "AUDITS",
                    NodeRef::ids([2, 3]),
                    vec![("origin", PropertyInput::from(Expr::prop("name")))],
                )
                .id(),
        )
        .returning(["created"]);
    assert_eq!(
        db.query(QueryRequest::write(created_edges)).await.unwrap(),
        serde_json::json!({ "created": [0, 1, 2, 3] })
    );

    let empty_edges = batch::write_batch()
        .var_as(
            "created",
            traversal::g()
                .n_with_label("Missing")
                .add_e(
                    "AUDITS",
                    NodeRef::id(2),
                    Vec::<(&str, PropertyInput)>::new(),
                )
                .id(),
        )
        .returning(["created"]);
    assert_eq!(
        db.query(QueryRequest::write(empty_edges)).await.unwrap(),
        serde_json::json!({ "created": [] })
    );

    let missing_targets = batch::write_batch()
        .var_as(
            "created",
            traversal::g().n_with_label("Seed").add_e(
                "AUDITS",
                NodeRef::ids([]),
                Vec::<(&str, PropertyInput)>::new(),
            ),
        )
        .returning(Vec::<String>::new());
    let error = db
        .query(QueryRequest::write(missing_targets))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Query error: addE() requires at least one target vertex"
    );

    let edge_rows = batch::read_batch()
        .var_as(
            "audits",
            traversal::g().e_with_label("AUDITS").project(vec![
                Projection::property("$id", "id"),
                Projection::property("origin", "origin"),
                Projection::from_endpoint("$id", "from"),
                Projection::to_endpoint("$id", "to"),
            ]),
        )
        .returning(["audits"]);
    assert_eq!(
        db.query(QueryRequest::read(edge_rows)).await.unwrap(),
        serde_json::json!({
            "audits": [
                { "id": 0, "origin": "alpha", "from": 0, "to": 2 },
                { "id": 1, "origin": "alpha", "from": 0, "to": 3 },
                { "id": 2, "origin": "beta", "from": 1, "to": 2 },
                { "id": 3, "origin": "beta", "from": 1, "to": 3 },
            ]
        })
    );

    db.close().await.unwrap();
}

#[test]
fn public_query_boundary_covers_expand_predicate_and_branch_control() {
    run_high_stack_contract(
        "public-expand-predicate-branch-control",
        public_query_boundary_covers_expand_predicate_and_branch_control_contract,
    );
}

async fn public_query_boundary_covers_expand_predicate_and_branch_control_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-graph-control".to_owned(),
    })
    .await
    .expect("production graph-control fixture opens");

    let fixture = batch::write_batch()
        .var_as(
            "alice",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("alice")),
                    ("rank", PropertyInput::from(1_i64)),
                    ("active", PropertyInput::from(true)),
                ],
            ),
        )
        .var_as(
            "bob",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("bob")),
                    ("rank", PropertyInput::from(2_i64)),
                    ("active", PropertyInput::from(false)),
                ],
            ),
        )
        .var_as(
            "carol",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("carol")),
                    ("rank", PropertyInput::from(3_i64)),
                    ("active", PropertyInput::from(true)),
                ],
            ),
        )
        .var_as("alice_id", traversal::g().n(NodeRef::var("alice")).id())
        .var_as("bob_id", traversal::g().n(NodeRef::var("bob")).id())
        .var_as("carol_id", traversal::g().n(NodeRef::var("carol")).id())
        .var_as(
            "alice_knows_bob",
            traversal::g()
                .n(NodeRef::var("alice"))
                .add_e(
                    "KNOWS",
                    NodeRef::var("bob"),
                    vec![("since", PropertyInput::from(2024_i64))],
                )
                .count(),
        )
        .var_as(
            "bob_knows_carol",
            traversal::g()
                .n(NodeRef::var("bob"))
                .add_e(
                    "KNOWS",
                    NodeRef::var("carol"),
                    vec![("since", PropertyInput::from(2025_i64))],
                )
                .count(),
        )
        .returning([
            "alice_id",
            "bob_id",
            "carol_id",
            "alice_knows_bob",
            "bob_knows_carol",
        ]);
    assert_eq!(
        db.query(QueryRequest::write(fixture)).await.unwrap(),
        serde_json::json!({
            "alice_id": [0],
            "bob_id": [1],
            "carol_id": [2],
            "alice_knows_bob": 1,
            "bob_knows_carol": 1,
        })
    );

    let read = batch::read_batch()
        .var_as(
            "filtered",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::and(vec![
                    Predicate::has_key("name"),
                    Predicate::not(Predicate::eq("active", false)),
                ]))
                .id(),
        )
        .var_as(
            "out",
            traversal::g().n(NodeRef::id(0)).out(Some("KNOWS")).id(),
        )
        .var_as(
            "in",
            traversal::g().n(NodeRef::id(2)).in_(Some("KNOWS")).id(),
        )
        .var_as(
            "edge_round_trip",
            traversal::g()
                .n(NodeRef::id(0))
                .out_e(Some("KNOWS"))
                .where_(Predicate::gte("since", 2024_i64))
                .out_n()
                .id(),
        )
        .var_as("edge_reverse", traversal::g().e(EdgeRef::id(0)).in_n().id())
        .var_as(
            "any_out_edges",
            traversal::g().n(NodeRef::id(0)).out_e(None::<&str>).id(),
        )
        .var_as(
            "any_in_edges",
            traversal::g().n(NodeRef::id(2)).in_e(None::<&str>).id(),
        )
        .var_as(
            "any_both_edges",
            traversal::g().n(NodeRef::id(1)).both_e(None::<&str>).id(),
        )
        .var_as(
            "labeled_both_edges",
            traversal::g().n(NodeRef::id(1)).both_e(Some("KNOWS")).id(),
        )
        .var_as(
            "missing_edge_label",
            traversal::g().n(NodeRef::id(0)).out_e(Some("MISSING")).id(),
        )
        .var_as(
            "choose",
            traversal::g()
                .n(NodeRef::ids([0, 1]))
                .choose(
                    Predicate::eq("active", true),
                    traversal::sub().out(Some("KNOWS")),
                    Some(traversal::sub().in_(Some("KNOWS"))),
                )
                .id(),
        )
        .var_as(
            "union",
            traversal::g()
                .n(NodeRef::id(1))
                .union(vec![
                    traversal::sub().out(Some("KNOWS")),
                    traversal::sub().in_(Some("KNOWS")),
                ])
                .id(),
        )
        .var_as(
            "coalesce",
            traversal::g()
                .n(NodeRef::id(1))
                .coalesce(vec![
                    traversal::sub().out(Some("MISSING")),
                    traversal::sub().in_(Some("KNOWS")),
                ])
                .id(),
        )
        .var_as(
            "optional",
            traversal::g()
                .n(NodeRef::id(2))
                .optional(traversal::sub().out(Some("KNOWS")))
                .id(),
        )
        .var_as("source", traversal::g().n(NodeRef::id(0)))
        .var_as("target", traversal::g().n(NodeRef::id(2)))
        .var_as(
            "shortest",
            traversal::g().shortest_path(NodeRef::id(0), NodeRef::id(2), 3),
        )
        .var_as(
            "variable_shortest",
            traversal::g().shortest_path(NodeRef::var("source"), NodeRef::var("target"), 3),
        )
        .var_as(
            "parameter_shortest",
            traversal::g().shortest_path_with(
                NodeRef::var("source"),
                NodeRef::param("target_id"),
                Some("KNOWS"),
                traversal::ShortestPathDirection::Out,
                2,
            ),
        )
        .var_as(
            "reverse_shortest",
            traversal::g().shortest_path_with(
                NodeRef::id(2),
                NodeRef::id(0),
                None::<&str>,
                traversal::ShortestPathDirection::In,
                3,
            ),
        )
        .var_as(
            "depth_cutoff",
            traversal::g().shortest_path(NodeRef::id(0), NodeRef::id(2), 1),
        )
        .var_as(
            "missing_label",
            traversal::g().shortest_path_with(
                NodeRef::id(0),
                NodeRef::id(2),
                Some("MISSING"),
                traversal::ShortestPathDirection::Both,
                3,
            ),
        )
        .var_as(
            "identity",
            traversal::g().shortest_path(NodeRef::id(0), NodeRef::id(0), 1),
        )
        .var_as(
            "missing_source_node",
            traversal::g().shortest_path(NodeRef::id(999), NodeRef::id(2), 3),
        )
        .var_as(
            "missing_target_node",
            traversal::g().shortest_path(NodeRef::id(0), NodeRef::id(999), 3),
        )
        .var_as(
            "bidirectional_shortest",
            traversal::g().shortest_path_with(
                NodeRef::id(0),
                NodeRef::id(2),
                None::<&str>,
                traversal::ShortestPathDirection::Both,
                3,
            ),
        )
        .var_as(
            "repeat",
            traversal::g()
                .n(NodeRef::id(0))
                .repeat(traversal::RepeatConfig::new(traversal::sub().out(Some("KNOWS"))).times(2))
                .id(),
        )
        .var_as("group", traversal::g().n(NodeRef::all()).group("active"))
        .var_as(
            "group_count",
            traversal::g().n(NodeRef::all()).group_count("active"),
        )
        .var_as(
            "aggregate_count",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Count, "rank"),
        )
        .var_as(
            "aggregate_sum",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Sum, "rank"),
        )
        .var_as(
            "aggregate_min",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Min, "rank"),
        )
        .var_as(
            "aggregate_max",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Max, "rank"),
        )
        .var_as(
            "aggregate_mean",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Mean, "rank"),
        )
        .returning([
            "filtered",
            "out",
            "in",
            "edge_round_trip",
            "edge_reverse",
            "any_out_edges",
            "any_in_edges",
            "any_both_edges",
            "labeled_both_edges",
            "missing_edge_label",
            "choose",
            "union",
            "coalesce",
            "optional",
            "shortest",
            "variable_shortest",
            "parameter_shortest",
            "reverse_shortest",
            "depth_cutoff",
            "missing_label",
            "identity",
            "missing_source_node",
            "missing_target_node",
            "bidirectional_shortest",
            "repeat",
            "group",
            "group_count",
            "aggregate_count",
            "aggregate_sum",
            "aggregate_min",
            "aggregate_max",
            "aggregate_mean",
        ]);
    let read = QueryRequest::read(read).with_parameter_value("target_id", QueryValue::I64(2));
    assert_eq!(
        db.query(read).await.unwrap(),
        serde_json::json!({
            "filtered": [0, 2],
            "out": [1],
            "in": [1],
            "edge_round_trip": [1],
            "edge_reverse": [0],
            "any_out_edges": [0],
            "any_in_edges": [1],
            "any_both_edges": [0, 1],
            "labeled_both_edges": [0, 1],
            "missing_edge_label": [],
            "choose": [1, 0],
            "union": [2, 0],
            "coalesce": [0],
            "optional": [2],
            "shortest": [0, 1, 2],
            "variable_shortest": [0, 1, 2],
            "parameter_shortest": [0, 1, 2],
            "reverse_shortest": [2, 1, 0],
            "depth_cutoff": [],
            "missing_label": [],
            "identity": [0],
            "missing_source_node": [],
            "missing_target_node": [],
            "bidirectional_shortest": [0, 1, 2],
            "repeat": [2],
            "group": [
                { "active": false, "count": 1, "ids": [1] },
                { "active": true, "count": 2, "ids": [0, 2] },
            ],
            "group_count": [
                { "active": false, "count": 1 },
                { "active": true, "count": 2 },
            ],
            "aggregate_count": [{ "rank_Count": 3.0 }],
            "aggregate_sum": [{ "rank_Sum": 6.0 }],
            "aggregate_min": [{ "rank_Min": 1.0 }],
            "aggregate_max": [{ "rank_Max": 3.0 }],
            "aggregate_mean": [{ "rank_Mean": 2.0 }],
        })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "isolated",
                traversal::g().add_n("Isolated", Vec::<(&str, PropertyInput)>::new()),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "path",
                    traversal::g().shortest_path(NodeRef::id(3), NodeRef::id(0), 3),
                )
                .returning(["path"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "path": [] })
    );

    for (path, expected) in [
        (
            traversal::g().shortest_path(NodeRef::all(), NodeRef::id(2), 3),
            "Query error: shortest_path.source must resolve to exactly one node, got all nodes",
        ),
        (
            traversal::g().shortest_path(NodeRef::ids([0, 1]), NodeRef::id(2), 3),
            "Query error: shortest_path.source must resolve to exactly one node, got 2 nodes",
        ),
        (
            traversal::g().shortest_path(NodeRef::param("missing_source"), NodeRef::id(2), 3),
            "Query error: parameter `missing_source` is not bound",
        ),
        (
            traversal::g().shortest_path(NodeRef::id(0), NodeRef::param("missing_target"), 3),
            "Query error: parameter `missing_target` is not bound",
        ),
    ] {
        let error = db
            .query(QueryRequest::read(
                batch::read_batch().var_as("path", path).returning(["path"]),
            ))
            .await
            .expect_err("an unbound shortest-path endpoint must fail");
        assert_eq!(error.to_string(), expected);
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_write_boundary_covers_topology_read_your_writes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-topology-read-your-writes".to_owned(),
    })
    .await
    .expect("production topology read-your-writes fixture opens");
    let write = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("first"))]),
        )
        .var_as(
            "second",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("second"))]),
        )
        .var_as(
            "third",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("third"))]),
        )
        .var_as(
            "link",
            traversal::g()
                .n(NodeRef::var("first"))
                .add_e(
                    "LINK",
                    NodeRef::var("second"),
                    vec![("weight", PropertyInput::from(1_i64))],
                )
                .id(),
        )
        .var_as(
            "other_link",
            traversal::g()
                .n(NodeRef::var("first"))
                .add_e(
                    "FOLLOWS",
                    NodeRef::var("second"),
                    vec![("weight", PropertyInput::from(2_i64))],
                )
                .id(),
        )
        .var_as(
            "incoming_link",
            traversal::g()
                .n(NodeRef::var("third"))
                .add_e(
                    "LINK",
                    NodeRef::var("first"),
                    vec![("weight", PropertyInput::from(3_i64))],
                )
                .id(),
        )
        .var_as(
            "out",
            traversal::g()
                .n(NodeRef::var("first"))
                .out(Some("LINK"))
                .id(),
        )
        .var_as(
            "incoming",
            traversal::g()
                .n(NodeRef::var("second"))
                .in_(Some("LINK"))
                .id(),
        )
        .var_as(
            "out_edges",
            traversal::g()
                .n(NodeRef::var("first"))
                .out_e(Some("LINK"))
                .id(),
        )
        .var_as(
            "in_edges",
            traversal::g()
                .n(NodeRef::var("second"))
                .in_e(Some("LINK"))
                .id(),
        )
        .var_as(
            "any_out_edges",
            traversal::g()
                .n(NodeRef::var("first"))
                .out_e(None::<&str>)
                .id(),
        )
        .var_as(
            "any_in_edges",
            traversal::g()
                .n(NodeRef::var("first"))
                .in_e(None::<&str>)
                .id(),
        )
        .var_as(
            "any_both_edges",
            traversal::g()
                .n(NodeRef::var("first"))
                .both_e(None::<&str>)
                .id(),
        )
        .var_as(
            "labeled_both_edges",
            traversal::g()
                .n(NodeRef::var("first"))
                .both_e(Some("LINK"))
                .id(),
        )
        .var_as(
            "labeled_both_nodes",
            traversal::g()
                .n(NodeRef::var("first"))
                .both(Some("LINK"))
                .id(),
        )
        .var_as(
            "labeled_edges",
            traversal::g().e_with_label("LINK").project(vec![
                Projection::property("$id", "id"),
                Projection::property("$label", "label"),
                Projection::from_endpoint("$id", "from"),
                Projection::to_endpoint("$id", "to"),
            ]),
        )
        .var_as("all_nodes", traversal::g().n(NodeRef::all()).id())
        .var_as("all_edges", traversal::g().e(EdgeRef::all()).id())
        .returning([
            "link",
            "other_link",
            "incoming_link",
            "out",
            "incoming",
            "out_edges",
            "in_edges",
            "any_out_edges",
            "any_in_edges",
            "any_both_edges",
            "labeled_both_edges",
            "labeled_both_nodes",
            "labeled_edges",
            "all_nodes",
            "all_edges",
        ]);
    assert_eq!(
        db.query(QueryRequest::write(write)).await.unwrap(),
        serde_json::json!({
            "link": [0],
            "other_link": [1],
            "incoming_link": [2],
            "out": [1],
            "incoming": [0],
            "out_edges": [0],
            "in_edges": [0],
            "any_out_edges": [0, 1],
            "any_in_edges": [2],
            "any_both_edges": [0, 1, 2],
            "labeled_both_edges": [0, 2],
            "labeled_both_nodes": [1, 2],
            "labeled_edges": [
                { "id": 0, "label": "LINK", "from": 0, "to": 1 },
                { "id": 2, "label": "LINK", "from": 2, "to": 0 },
            ],
            "all_nodes": [0, 1, 2],
            "all_edges": [0, 1, 2],
        })
    );
    db.close().await.unwrap();
}

#[test]
fn public_query_boundary_covers_branch_partition_edges() {
    run_high_stack_contract(
        "public-branch-partition-edges",
        public_query_boundary_covers_branch_partition_edges_contract,
    );
}

async fn public_query_boundary_covers_branch_partition_edges_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-branch-partitions".to_owned(),
    })
    .await
    .expect("production branch-partition fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n("Person", vec![("active", PropertyInput::from(true))]),
        )
        .var_as(
            "second",
            traversal::g().add_n("Person", vec![("active", PropertyInput::from(false))]),
        )
        .var_as(
            "link",
            traversal::g().n(NodeRef::var("first")).add_e(
                "LINK",
                NodeRef::var("second"),
                Vec::<(&str, PropertyInput)>::new(),
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let branches = batch::read_batch()
        .var_as(
            "choose_none",
            traversal::g()
                .n(NodeRef::id(1))
                .choose(
                    Predicate::eq("active", true),
                    traversal::sub().out(Some("LINK")),
                    None,
                )
                .id(),
        )
        .var_as(
            "choose_all",
            traversal::g()
                .n(NodeRef::id(0))
                .choose(
                    Predicate::eq("active", true),
                    traversal::sub().out(Some("LINK")),
                    None,
                )
                .id(),
        )
        .var_as(
            "choose_else_none",
            traversal::g()
                .n(NodeRef::id(1))
                .choose(
                    Predicate::eq("active", true),
                    traversal::sub().out(Some("LINK")),
                    Some(traversal::sub().in_(Some("LINK"))),
                )
                .id(),
        )
        .var_as(
            "choose_else_all",
            traversal::g()
                .n(NodeRef::id(0))
                .choose(
                    Predicate::eq("active", true),
                    traversal::sub().out(Some("LINK")),
                    Some(traversal::sub().in_(Some("LINK"))),
                )
                .id(),
        )
        .var_as(
            "choose_else_split",
            traversal::g()
                .n(NodeRef::all())
                .choose(
                    Predicate::eq("active", true),
                    traversal::sub().out(Some("LINK")),
                    Some(traversal::sub().in_(Some("LINK"))),
                )
                .id(),
        )
        .var_as(
            "optional_match",
            traversal::g()
                .n(NodeRef::id(0))
                .optional(traversal::sub().out(Some("LINK")))
                .id(),
        )
        .var_as(
            "optional_no_match",
            traversal::g()
                .n(NodeRef::id(1))
                .optional(traversal::sub().out(Some("LINK")))
                .id(),
        )
        .var_as(
            "coalesce_none",
            traversal::g()
                .n(NodeRef::id(1))
                .coalesce(vec![
                    traversal::sub().out(Some("LINK")),
                    traversal::sub().out(Some("MISSING")),
                ])
                .id(),
        )
        .var_as(
            "unbound_mixed_union",
            traversal::g()
                .n(NodeRef::id(0))
                .union(vec![
                    traversal::sub().out(Some("LINK")),
                    traversal::sub().out_e(Some("LINK")),
                ])
                .id(),
        )
        .returning([
            "choose_none",
            "choose_all",
            "choose_else_none",
            "choose_else_all",
            "choose_else_split",
            "optional_match",
            "optional_no_match",
            "coalesce_none",
            "unbound_mixed_union",
        ]);
    assert_eq!(
        db.query(QueryRequest::read(branches)).await.unwrap(),
        serde_json::json!({
            "choose_none": [],
            "choose_all": [1],
            "choose_else_none": [0],
            "choose_else_all": [1],
            "choose_else_split": [1, 0],
            "optional_match": [1],
            "optional_no_match": [1],
            "coalesce_none": [],
            "unbound_mixed_union": [1, 0],
        })
    );

    let bound_same_kind_union = batch::read_batch()
        .var_as(
            "same_kind",
            traversal::g()
                .n(NodeRef::id(0))
                .bind("origin")
                .union(vec![
                    traversal::sub().out(Some("LINK")),
                    traversal::sub().out(Some("LINK")),
                ])
                .project_bindings(vec![
                    BindingProjection::binding("origin", "$id", "origin"),
                    BindingProjection::current("$id", "current"),
                ]),
        )
        .returning(["same_kind"]);
    assert_eq!(
        db.query(QueryRequest::read(bound_same_kind_union))
            .await
            .unwrap(),
        serde_json::json!({
            "same_kind": [
                { "origin": 0, "current": 1 },
                { "origin": 0, "current": 1 },
            ],
        })
    );

    let mixed_union = batch::read_batch()
        .var_as(
            "mixed",
            traversal::g()
                .n(NodeRef::id(0))
                .bind("origin")
                .union(vec![
                    traversal::sub().out(Some("LINK")),
                    traversal::sub().out_e(Some("LINK")),
                ])
                .project_bindings(vec![BindingProjection::current("$id", "id")]),
        )
        .returning(["mixed"]);
    assert_eq!(
        db.query(QueryRequest::read(mixed_union))
            .await
            .unwrap_err()
            .to_string(),
        "Query error: union row branches produced mixed current element types"
    );

    let step_id = |value| exec::ExecStepId::new(value).expect("step ID is positive");
    let step = |id, dependencies, op| exec::ExecStep {
        id: step_id(id),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    };
    let subplan = |op| {
        exec::ExecutableSubplan::new(
            ir::AtLeast::<_, 1>::from_one(step(1, Vec::new(), op)),
            step_id(1),
        )
        .expect("branch subplan validates")
    };
    let context_subplan = || {
        subplan(exec::ExecOp::Variable {
            op: exec::ExecVariableOp::SourceInject {
                variable: ir::NonEmptyString::new("$context")
                    .expect("context variable is non-empty"),
            },
        })
    };
    let scalar_subplan = || {
        subplan(exec::ExecOp::Count {
            plan: Box::new(exec::ExecCountPlan::Constant(0)),
        })
    };
    let branch_plan = |plan| {
        let access = step(
            1,
            Vec::new(),
            exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::AllScan,
                )),
            },
        );
        let branch = step(2, vec![step_id(1)], exec::ExecOp::Branch { plan });
        exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::from_one_and_rest(access, vec![branch]),
            step_id(2),
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("branch error plan validates")
    };
    let missing_parameter = || {
        ir::PredicatePlan::new(Predicate::contains_param("active", "missing"))
            .expect("missing runtime parameter remains a valid predicate plan")
    };
    let split = || {
        ir::PredicatePlan::new(Predicate::eq("active", true))
            .expect("split branch predicate validates")
    };
    for (plan, expected) in [
        (
            exec::ExecBranchPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                scalar_subplan(),
                context_subplan(),
            )),
            "Query error: branch.union expected stream input, got Count(0)",
        ),
        (
            exec::ExecBranchPlan::Coalesce(ir::AtLeast::<_, 1>::from_one(scalar_subplan())),
            "Query error: branch.coalesce expected stream input, got Count(0)",
        ),
        (
            exec::ExecBranchPlan::Optional(Box::new(scalar_subplan())),
            "Query error: branch.optional expected stream input, got Count(0)",
        ),
        (
            exec::ExecBranchPlan::Choose {
                condition: missing_parameter(),
                then_plan: Box::new(context_subplan()),
            },
            "Query error: parameter `missing` is not bound",
        ),
        (
            exec::ExecBranchPlan::ChooseElse {
                condition: missing_parameter(),
                then_plan: Box::new(context_subplan()),
                else_plan: Box::new(context_subplan()),
            },
            "Query error: parameter `missing` is not bound",
        ),
        (
            exec::ExecBranchPlan::ChooseElse {
                condition: split(),
                then_plan: Box::new(scalar_subplan()),
                else_plan: Box::new(context_subplan()),
            },
            "Query error: branch.choose_else.then expected stream input, got Count(0)",
        ),
        (
            exec::ExecBranchPlan::ChooseElse {
                condition: split(),
                then_plan: Box::new(context_subplan()),
                else_plan: Box::new(scalar_subplan()),
            },
            "Query error: branch.choose_else.else expected stream input, got Count(0)",
        ),
    ] {
        assert_eq!(
            db.execute(&branch_plan(plan), context::ParamBindings::default())
                .await
                .expect_err("malformed branch value shape is rejected")
                .to_string(),
            expected
        );
    }

    let nested_access = step(
        1,
        Vec::new(),
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::AllScan,
            )),
        },
    );
    let nested_branch = step(
        2,
        vec![step_id(1)],
        exec::ExecOp::Branch {
            plan: exec::ExecBranchPlan::Optional(Box::new(context_subplan())),
        },
    );
    let restored_context = step(
        3,
        vec![step_id(2)],
        exec::ExecOp::Variable {
            op: exec::ExecVariableOp::SourceInject {
                variable: ir::NonEmptyString::new("$context")
                    .expect("context variable is non-empty"),
            },
        },
    );
    let nested_subplan = exec::ExecutableSubplan::new(
        ir::AtLeast::<_, 1>::from_one_and_rest(
            nested_access,
            vec![nested_branch, restored_context],
        ),
        step_id(3),
    )
    .expect("nested branch subplan validates");
    let result = db
        .execute(
            &branch_plan(exec::ExecBranchPlan::Choose {
                condition: split(),
                then_plan: Box::new(nested_subplan),
            }),
            context::ParamBindings::default(),
        )
        .await
        .expect("nested branch restores its outer context");
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("nested branch root must return the restored outer context")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].current, Some(ElementRef::Node(0)));

    let missing_variable =
        ir::NonEmptyString::new("missing").expect("missing variable name is non-empty");
    let conditional_step = exec::ExecStep {
        condition: exec::ExecCondition::Variable(ir::BatchVariableConditionPlan::VarNotEmpty(
            missing_variable,
        )),
        ..step(1, Vec::new(), exec::ExecOp::Noop)
    };
    let conditional_plan = exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::from_one(conditional_step),
        step_id(1),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("missing-variable condition plan validates");
    assert_eq!(
        db.execute(&conditional_plan, context::ParamBindings::default())
            .await
            .expect_err("missing condition variable is rejected")
            .to_string(),
        "Query error: variable `missing` is not bound"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_nested_and_endpoint_property_paths() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-property-paths".to_owned(),
    })
    .await
    .expect("production property-path fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "source",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("alice")),
                    ("plain", PropertyInput::from("root")),
                    ("scalar", PropertyInput::from(7_i64)),
                    (
                        "metadata",
                        PropertyInput::from(PropertyValue::Object(BTreeMap::from([
                            ("score".to_owned(), PropertyValue::I64(9)),
                            (
                                "deep".to_owned(),
                                PropertyValue::Object(BTreeMap::from([(
                                    "exact".to_owned(),
                                    PropertyValue::String("source".to_owned()),
                                )])),
                            ),
                        ]))),
                    ),
                ],
            ),
        )
        .var_as(
            "target",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("bob"))]),
        )
        .var_as(
            "edge",
            traversal::g().n(NodeRef::var("source")).add_e(
                "LINK",
                NodeRef::var("target"),
                vec![("since", PropertyInput::from(2026_i64))],
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let read = batch::read_batch()
        .var_as(
            "node",
            traversal::g().n(NodeRef::id(0)).project(vec![
                Projection::property("metadata.score", "score"),
                Projection::property("metadata.deep.exact", "exact"),
                Projection::property("metadata.missing", "missing"),
                Projection::property("metadata.", "trailing"),
                Projection::property(".metadata", "leading"),
                Projection::property("scalar.child", "scalar_child"),
                Projection::property("plain", "plain"),
            ]),
        )
        .var_as(
            "edge",
            traversal::g().e(EdgeRef::id(0)).project(vec![
                Projection::property("$from", "from"),
                Projection::property("$to", "to"),
                Projection::property("$from.$id", "from_id"),
                Projection::property("$to.$id", "to_id"),
                Projection::property("$from.name", "from_name"),
                Projection::property("$to.name", "to_name"),
                Projection::property("$from.metadata.score", "from_score"),
                Projection::property("$from.metadata.deep.exact", "from_exact"),
                Projection::property("$from.metadata.missing", "missing"),
                Projection::property("$from.", "empty_endpoint_path"),
                Projection::property("$from.scalar.child", "scalar_child"),
            ]),
        )
        .returning(["node", "edge"]);
    assert_eq!(
        db.query(QueryRequest::read(read)).await.unwrap(),
        serde_json::json!({
            "node": [{ "score": 9, "exact": "source", "plain": "root" }],
            "edge": [{
                "from": 0,
                "to": 1,
                "from_id": 0,
                "to_id": 1,
                "from_name": "alice",
                "to_name": "bob",
                "from_score": 9,
                "from_exact": "source",
            }],
        })
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_execute_boundary_covers_typed_id_parameter_shapes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-typed-id-parameters".to_owned(),
    })
    .await
    .expect("production typed-ID fixture opens");
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "first",
                traversal::g().add_n("Item", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "second",
                traversal::g().add_n("Item", Vec::<(&str, PropertyInput)>::new()),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();

    let batch = batch::read_batch()
        .var_as("ids", traversal::g().n(NodeRef::param("ids")).id())
        .returning(["ids"]);
    let plan = planning::plan_read_batch(&batch, &context::PlannerContext::default())
        .expect("typed ID-parameter batch plans");
    let ids = ir::NonEmptyString::new("ids").expect("parameter name is non-empty");

    let error = db
        .execute(&plan, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Query error: parameter `ids` is not bound"
    );

    for (value, expected) in [
        (PropertyValue::I64(1), vec![ExecutionScalar::NodeId(1)]),
        (
            PropertyValue::I64Array(vec![1, 0]),
            vec![ExecutionScalar::NodeId(1), ExecutionScalar::NodeId(0)],
        ),
        (
            PropertyValue::Array(vec![PropertyValue::I64(0), PropertyValue::I64(1)]),
            vec![ExecutionScalar::NodeId(0), ExecutionScalar::NodeId(1)],
        ),
    ] {
        let result = db
            .execute(
                &plan,
                context::ParamBindings::default().with_value(ids.clone(), value),
            )
            .await
            .expect("valid typed ID parameter executes")
            .last
            .expect("typed ID plan returns its root value");
        assert_eq!(result, ExecutionValue::Scalars(expected));
    }

    for (value, message) in [
        (
            PropertyValue::Array(vec![PropertyValue::String("invalid".to_owned())]),
            "parameter `ids` must contain integer ids",
        ),
        (
            PropertyValue::I64Array(vec![-1]),
            "parameter `ids` contains negative id -1",
        ),
        (
            PropertyValue::Object(BTreeMap::new()),
            "parameter `ids` must be an integer id or array of integer ids",
        ),
    ] {
        let error = db
            .execute(
                &plan,
                context::ParamBindings::default().with_value(ids.clone(), value),
            )
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), format!("Query error: {message}"));
    }

    for (value, message) in [
        (
            QueryValue::Array(vec![QueryValue::String("invalid".to_owned())]),
            "parameter `ids` must contain integer ids",
        ),
        (
            QueryValue::Array(vec![QueryValue::I64(-1)]),
            "parameter `ids` contains negative id -1",
        ),
    ] {
        let error = db
            .query(QueryRequest::read(batch.clone()).with_parameter_value("ids", value))
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), format!("Query error: {message}"));
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_variable_id_source_shapes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-variable-id-sources".to_owned(),
    })
    .await
    .expect("production variable-ID fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n("Item", Vec::<(&str, PropertyInput)>::new()),
        )
        .var_as(
            "second",
            traversal::g().add_n("Item", Vec::<(&str, PropertyInput)>::new()),
        )
        .var_as(
            "edge",
            traversal::g().n(NodeRef::var("first")).add_e(
                "LINK",
                NodeRef::var("second"),
                Vec::<(&str, PropertyInput)>::new(),
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let sources = batch::read_batch()
        .var_as("nodes", traversal::g().n(NodeRef::all()))
        .var_as("edges", traversal::g().e(EdgeRef::all()))
        .var_as("folded_nodes", traversal::g().n(NodeRef::all()).fold())
        .var_as("node_ids", traversal::g().n(NodeRef::var("nodes")).id())
        .var_as("edge_ids", traversal::g().e(EdgeRef::var("edges")).id())
        .var_as(
            "folded_node_ids",
            traversal::g().n(NodeRef::var("folded_nodes")).id(),
        )
        .var_as(
            "nodes_from_edges",
            traversal::g().n(NodeRef::var("edges")).id(),
        )
        .var_as(
            "edges_from_nodes",
            traversal::g().e(EdgeRef::var("nodes")).id(),
        )
        .var_as(
            "missing_nodes",
            traversal::g().n(NodeRef::var("missing")).id(),
        )
        .var_as(
            "missing_edges",
            traversal::g().e(EdgeRef::var("missing")).id(),
        )
        .returning([
            "node_ids",
            "edge_ids",
            "folded_node_ids",
            "nodes_from_edges",
            "edges_from_nodes",
            "missing_nodes",
            "missing_edges",
        ]);
    assert_eq!(
        db.query(QueryRequest::read(sources)).await.unwrap(),
        serde_json::json!({
            "node_ids": [0, 1],
            "edge_ids": [0],
            "folded_node_ids": [0, 1],
            "nodes_from_edges": [],
            "edges_from_nodes": [],
            "missing_nodes": [],
            "missing_edges": [],
        })
    );

    let folded_edges = ir::NonEmptyString::new("folded_edges").expect("variable name is non-empty");
    let edge_access = exec::ExecStepId::new(1).expect("step ID is positive");
    let fold = exec::ExecStepId::new(2).expect("step ID is positive");
    let folded_access = exec::ExecStepId::new(3).expect("step ID is positive");
    let folded_edge_plan = exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(vec![
            exec::ExecStep {
                id: edge_access,
                dependencies: Vec::new(),
                output: ir::BatchOutputPlan::Discard,
                semantic_return_shape: None,
                condition: exec::ExecCondition::Always,
                op: exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::AllScan,
                    )),
                },
                schedule: exec::ExecSchedule::Pipeline,
                delivered: properties::DeliveredProperties::default(),
                cost: cost::CostVector::ZERO,
            },
            exec::ExecStep {
                id: fold,
                dependencies: vec![edge_access],
                output: ir::BatchOutputPlan::Bind(folded_edges.clone()),
                semantic_return_shape: None,
                condition: exec::ExecCondition::Always,
                op: exec::ExecOp::Reserved {
                    op: ir::ReservedOp::Fold,
                },
                schedule: exec::ExecSchedule::Barrier,
                delivered: properties::DeliveredProperties::default(),
                cost: cost::CostVector::ZERO,
            },
            exec::ExecStep {
                id: folded_access,
                dependencies: vec![fold],
                output: ir::BatchOutputPlan::Discard,
                semantic_return_shape: None,
                condition: exec::ExecCondition::Always,
                op: exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::FromVar {
                            variable: folded_edges,
                        },
                    )),
                },
                schedule: exec::ExecSchedule::Pipeline,
                delivered: properties::DeliveredProperties::default(),
                cost: cost::CostVector::ZERO,
            },
            exec::ExecStep {
                id: exec::ExecStepId::new(4).expect("step ID is positive"),
                dependencies: vec![folded_access],
                output: ir::BatchOutputPlan::Discard,
                semantic_return_shape: None,
                condition: exec::ExecCondition::Always,
                op: exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
                schedule: exec::ExecSchedule::Pipeline,
                delivered: properties::DeliveredProperties::default(),
                cost: cost::CostVector::ZERO,
            },
        ])
        .expect("folded-edge plan is non-empty"),
        exec::ExecStepId::new(4).expect("root step ID is positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("folded-edge variable plan validates");
    assert_eq!(
        db.execute(&folded_edge_plan, context::ParamBindings::default(),)
            .await
            .expect("folded-edge variable plan executes")
            .last,
        Some(ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(0)]))
    );

    let scalar_node_source = batch::read_batch()
        .var_as("scalar", traversal::g().n(NodeRef::all()).count())
        .var_as("invalid", traversal::g().n(NodeRef::var("scalar")).id())
        .returning(["invalid"]);
    assert_eq!(
        db.query(QueryRequest::read(scalar_node_source))
            .await
            .unwrap_err()
            .to_string(),
        "Query error: variable `scalar` is not a node stream: Count(2)"
    );

    let scalar_edge_source = batch::read_batch()
        .var_as("scalar", traversal::g().e(EdgeRef::all()).count())
        .var_as("invalid", traversal::g().e(EdgeRef::var("scalar")).id())
        .returning(["invalid"]);
    assert_eq!(
        db.query(QueryRequest::read(scalar_edge_source))
            .await
            .unwrap_err()
            .to_string(),
        "Query error: variable `scalar` is not an edge stream: Count(1)"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_label_mutation_contracts() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-label-mutations".to_owned(),
    })
    .await
    .expect("production label-mutation fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("alice"))]),
        )
        .var_as(
            "second",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("bob"))]),
        )
        .var_as(
            "edge",
            traversal::g().n(NodeRef::var("first")).add_e(
                "LINK",
                NodeRef::var("second"),
                Vec::<(&str, PropertyInput)>::new(),
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let invalid = [
        (
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "invalid",
                        traversal::g()
                            .add_n("Person", vec![("$label", PropertyInput::from("Other"))]),
                    )
                    .returning(Vec::<String>::new()),
            ),
            "Query error: mutating `$label` directly is not supported by executable mutations",
        ),
        (
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "invalid",
                        traversal::g().n(NodeRef::id(0)).add_e(
                            "LINK",
                            NodeRef::id(1),
                            vec![("$label", PropertyInput::from("Other"))],
                        ),
                    )
                    .returning(Vec::<String>::new()),
            ),
            "Query error: mutating `$label` directly is not supported by executable mutations",
        ),
        (
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "invalid",
                        traversal::g().n(NodeRef::id(0)).remove_property("$label"),
                    )
                    .returning(Vec::<String>::new()),
            ),
            "Query error: mutating `$label` directly is not supported by executable mutations",
        ),
        (
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "invalid",
                        traversal::g().e(EdgeRef::id(0)).remove_property("$label"),
                    )
                    .returning(Vec::<String>::new()),
            ),
            "Query error: mutating `$label` directly is not supported by executable mutations",
        ),
        (
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "invalid",
                        traversal::g()
                            .e(EdgeRef::id(0))
                            .set_property("$label", "Other"),
                    )
                    .returning(Vec::<String>::new()),
            ),
            "Query error: edge `$label` mutations are not supported by executable mutations",
        ),
        (
            QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "invalid",
                        traversal::g()
                            .n(NodeRef::id(0))
                            .set_property("$label", 7_i64),
                    )
                    .returning(Vec::<String>::new()),
            ),
            "Query error: node `$label` mutations require a string value",
        ),
    ];
    for (request, message) in invalid {
        assert_eq!(db.query(request).await.unwrap_err().to_string(), message);
    }

    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("nodes", traversal::g().n(NodeRef::all()).label())
                .var_as("edges", traversal::g().e(EdgeRef::all()).label())
                .returning(["nodes", "edges"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "nodes": ["Person", "Person"], "edges": ["LINK"] })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "renamed",
                traversal::g()
                    .n(NodeRef::id(0))
                    .set_property("$label", "Admin"),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("node label update succeeds");
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("admin", traversal::g().n_with_label("Admin").id())
                .var_as("person", traversal::g().n_with_label("Person").id())
                .returning(["admin", "person"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "admin": [0], "person": [1] })
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_write_boundary_covers_mutation_noops_and_node_cascade() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-mutation-tail".to_owned(),
    })
    .await
    .expect("production mutation-tail fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "center",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("center"))]),
        )
        .var_as(
            "out",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("out"))]),
        )
        .var_as(
            "incoming",
            traversal::g().add_n("Person", vec![("name", PropertyInput::from("incoming"))]),
        )
        .var_as(
            "out_edge",
            traversal::g().n(NodeRef::var("center")).add_e(
                "LINK",
                NodeRef::var("out"),
                vec![("weight", PropertyInput::from(1_i64))],
            ),
        )
        .var_as(
            "in_edge",
            traversal::g().n(NodeRef::var("incoming")).add_e(
                "LINK",
                NodeRef::var("center"),
                vec![("weight", PropertyInput::from(2_i64))],
            ),
        )
        .var_as(
            "self_edge",
            traversal::g().n(NodeRef::var("center")).add_e(
                "SELF",
                NodeRef::var("center"),
                vec![("weight", PropertyInput::from(3_i64))],
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let noops = batch::write_batch()
        .var_as(
            "same_label",
            traversal::g()
                .n(NodeRef::id(0))
                .set_property("$label", "Person"),
        )
        .var_as(
            "missing_node_property",
            traversal::g().n(NodeRef::id(0)).remove_property("missing"),
        )
        .var_as(
            "missing_edge_property",
            traversal::g().e(EdgeRef::id(0)).remove_property("missing"),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(noops)).await.unwrap();
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "center",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .value_map(None::<Vec<&str>>)
                )
                .var_as("edges", traversal::g().e(EdgeRef::all()).id())
                .returning(["center", "edges"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({
            "center": [{ "$id": 0, "$label": "Person", "name": "center" }],
            "edges": [0, 1, 2],
        })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as("deleted", traversal::g().n(NodeRef::id(0)).drop())
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("node cascade delete succeeds");
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("nodes", traversal::g().n(NodeRef::all()).id())
                .var_as("edges", traversal::g().e(EdgeRef::all()).id())
                .returning(["nodes", "edges"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "nodes": [1, 2], "edges": [] })
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_write_boundary_covers_empty_and_unlabeled_edge_deletions() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-empty-edge-deletions".to_owned(),
    })
    .await
    .expect("production empty-edge-deletion fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "source",
            traversal::g().add_n("Person", Vec::<(&str, PropertyInput)>::new()),
        )
        .var_as(
            "target",
            traversal::g().add_n("Person", Vec::<(&str, PropertyInput)>::new()),
        )
        .var_as(
            "first_edge",
            traversal::g().n(NodeRef::var("source")).add_e(
                "KNOWS",
                NodeRef::var("target"),
                Vec::<(&str, PropertyInput)>::new(),
            ),
        )
        .var_as(
            "second_edge",
            traversal::g().n(NodeRef::var("source")).add_e(
                "FOLLOWS",
                NodeRef::var("target"),
                Vec::<(&str, PropertyInput)>::new(),
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let noops = batch::write_batch()
        .var_as("missing_node", traversal::g().n(NodeRef::id(99)).drop())
        .var_as(
            "empty_source",
            traversal::g().n(NodeRef::id(99)).drop_edge(NodeRef::id(1)),
        )
        .var_as(
            "empty_targets",
            traversal::g().n(NodeRef::id(0)).drop_edge(NodeRef::ids([])),
        )
        .var_as(
            "empty_input",
            traversal::g()
                .n(NodeRef::id(99))
                .drop_edge_by_id(EdgeRef::id(0)),
        )
        .var_as(
            "empty_edge_ids",
            traversal::g().drop_edge_by_id(EdgeRef::ids([])),
        )
        .var_as(
            "missing_edge_id",
            traversal::g().drop_edge_by_id(EdgeRef::id(99)),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(noops)).await.unwrap();
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("edges", traversal::g().e(EdgeRef::all()).id())
                .returning(["edges"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "edges": [0, 1] })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "removed",
                traversal::g().n(NodeRef::id(0)).drop_edge(NodeRef::id(1)),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("unlabeled deletion removes every edge between the pair");
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("edges", traversal::g().e(EdgeRef::all()).id())
                .var_as(
                    "outgoing",
                    traversal::g().n(NodeRef::id(0)).out(None::<&str>).id(),
                )
                .var_as(
                    "incoming",
                    traversal::g().n(NodeRef::id(1)).in_(None::<&str>).id(),
                )
                .returning(["edges", "outgoing", "incoming"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "edges": [], "outgoing": [], "incoming": [] })
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_active_range_index_access_and_mutation() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-range-index".to_owned(),
    })
    .await
    .expect("production range-index fixture opens");

    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n("Document", vec![("rank", PropertyInput::from(1_i64))]),
        )
        .var_as(
            "second",
            traversal::g().add_n("Document", vec![("rank", PropertyInput::from(2_i64))]),
        )
        .var_as(
            "third",
            traversal::g().add_n("Document", vec![("rank", PropertyInput::from(3_i64))]),
        )
        .returning(Vec::<String>::new());
    assert_eq!(
        db.query(QueryRequest::write(fixture)).await.unwrap(),
        serde_json::json!({})
    );

    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_index_if_not_exists(index::IndexSpec::node_range(
                        "Document", "rank",
                    )),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    assert_eq!(receipt["operation"]["kind"], "accepted");
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .expect("accepted range-index operation has an ID");
    await_index_operation_success(&db, operation_id, "range index").await;

    let step_id = |value| exec::ExecStepId::new(value).expect("step ID is positive");
    let step = |id, dependencies, op| exec::ExecStep {
        id: step_id(id),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    };
    let range_index_id = ir::NonEmptyString::new("node_range:Document:rank:asc")
        .expect("range index ID is non-empty");
    let range_access = step(
        1,
        Vec::new(),
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::RangeIndex {
                    index: catalog::NodeRangeIndexMeta::new(range_index_id.clone()),
                    key: catalog::ScopedPropertyDirectionKey::try_new(
                        "Document",
                        "rank",
                        helix_ast::index::RangeIndexDirection::Asc,
                    )
                    .expect("range key validates"),
                    range: ir::IndexRange::All,
                },
            )),
        },
    );
    let range_order = step(
        2,
        vec![step_id(1)],
        exec::ExecOp::Order {
            plan: ir::OrderPlan::RangeIndex {
                key: ir::OrderKey {
                    property: ir::NonEmptyString::new("rank").expect("order property is non-empty"),
                    order: traversal::Order::Asc,
                },
                index_id: range_index_id,
            },
        },
    );
    let project_ids = step(
        3,
        vec![step_id(2)],
        exec::ExecOp::Project {
            projection: ir::ProjectionPlan::Id,
        },
    );
    let range_order_plan = exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::from_one_and_rest(range_access, vec![range_order, project_ids]),
        step_id(3),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("range-delivered order plan validates");
    assert_eq!(
        db.execute(&range_order_plan, context::ParamBindings::default())
            .await
            .expect("range-delivered order executes")
            .last,
        Some(ExecutionValue::Scalars(vec![
            ExecutionScalar::NodeId(0),
            ExecutionScalar::NodeId(1),
            ExecutionScalar::NodeId(2),
        ]))
    );

    let range_request = |minimum_rank| {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .n_with_label_where("Document", Predicate::gte("rank", minimum_rank))
                        .id(),
                )
                .returning(["ids"]),
        )
    };
    assert_eq!(
        db.query(range_request(2_i64)).await.unwrap(),
        serde_json::json!({ "ids": [1, 2] })
    );

    let bounded_ranges = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "lower_exclusive",
                traversal::g()
                    .n_with_label_where("Document", Predicate::gt("rank", 1_i64))
                    .id(),
            )
            .var_as(
                "upper_inclusive",
                traversal::g()
                    .n_with_label_where("Document", Predicate::lte("rank", 2_i64))
                    .id(),
            )
            .var_as(
                "upper_exclusive",
                traversal::g()
                    .n_with_label_where("Document", Predicate::lt("rank", 3_i64))
                    .id(),
            )
            .var_as(
                "between",
                traversal::g()
                    .n_with_label_where("Document", Predicate::between("rank", 1_i64, 2_i64))
                    .id(),
            )
            .var_as(
                "parameter_lower",
                traversal::g()
                    .n_with_label_where("Document", Predicate::gte_param("rank", "minimum"))
                    .id(),
            )
            .var_as(
                "ordered_limit",
                traversal::g()
                    .n_with_label("Document")
                    .order_by("rank", traversal::Order::Asc)
                    .limit(2_usize)
                    .id(),
            )
            .returning([
                "lower_exclusive",
                "upper_inclusive",
                "upper_exclusive",
                "between",
                "parameter_lower",
                "ordered_limit",
            ]),
    )
    .with_parameter_value("minimum", QueryValue::I64(2));
    assert_eq!(
        db.query(bounded_ranges).await.unwrap(),
        serde_json::json!({
            "lower_exclusive": [1, 2],
            "upper_inclusive": [0, 1],
            "upper_exclusive": [0, 1],
            "between": [0, 1],
            "parameter_lower": [1, 2],
            "ordered_limit": [0, 1],
        })
    );
    let read_your_writes = batch::write_batch()
        .var_as(
            "updated",
            traversal::g().n(NodeRef::id(1)).set_property("rank", 4_i64),
        )
        .var_as(
            "ids",
            traversal::g()
                .n_with_label_where("Document", Predicate::gte("rank", 3_i64))
                .id(),
        )
        .returning(["ids"]);
    assert_eq!(
        db.query(QueryRequest::write(read_your_writes))
            .await
            .unwrap(),
        serde_json::json!({ "ids": [2, 1] })
    );
    assert_eq!(
        db.query(range_request(3_i64)).await.unwrap(),
        serde_json::json!({ "ids": [2, 1] })
    );
    db.close().await.unwrap();
}

#[test]
fn public_query_boundary_covers_active_text_index_mutations() {
    run_high_stack_contract(
        "public-active-text-index-mutations",
        public_query_boundary_covers_active_text_index_mutations_contract,
    );
}

async fn public_query_boundary_covers_active_text_index_mutations_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-text-index".to_owned(),
    })
    .await
    .expect("production text-index fixture opens");

    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n(
                "Document",
                vec![("body", PropertyInput::from("shared alpha"))],
            ),
        )
        .var_as(
            "second",
            traversal::g().add_n(
                "Document",
                vec![("body", PropertyInput::from("shared beta"))],
            ),
        )
        .var_as(
            "third",
            traversal::g().add_n("Document", vec![("body", PropertyInput::from("gamma"))]),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_text_index_nodes("Document", "body", None::<String>),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    assert_eq!(receipt["operation"]["kind"], "accepted");
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .expect("accepted text-index operation has an ID");
    await_index_operation_success(&db, operation_id, "text index").await;

    let search = |query| {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_nodes("Document", "body", query, 8, None)
                        .id(),
                )
                .returning(["ids"]),
        )
    };
    assert_eq!(
        db.query(search("alpha")).await.unwrap(),
        serde_json::json!({ "ids": [0] })
    );

    let read_your_writes = batch::write_batch()
        .var_as(
            "retired",
            traversal::g()
                .n(NodeRef::id(0))
                .set_property("body", "retired"),
        )
        .var_as(
            "replacement",
            traversal::g()
                .n(NodeRef::id(2))
                .set_property("body", "alpha replacement"),
        )
        .var_as(
            "alpha_ids",
            traversal::g()
                .text_search_nodes("Document", "body", "alpha", 8, None)
                .id(),
        )
        .var_as(
            "retired_ids",
            traversal::g()
                .text_search_nodes("Document", "body", "retired", 8, None)
                .id(),
        )
        .returning(["alpha_ids", "retired_ids"]);
    assert_eq!(
        db.query(QueryRequest::write(read_your_writes))
            .await
            .unwrap(),
        serde_json::json!({ "alpha_ids": [2], "retired_ids": [0] })
    );
    assert_eq!(
        db.query(search("alpha")).await.unwrap(),
        serde_json::json!({ "ids": [2] })
    );
    assert_eq!(
        db.query(search("retired")).await.unwrap(),
        serde_json::json!({ "ids": [0] })
    );

    const MAX_TRANSACTION_CONFLICT_RETRIES: usize = 3;
    for attempt in 0..MAX_TRANSACTION_CONFLICT_RETRIES {
        match db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "removed",
                        traversal::g().n(NodeRef::id(0)).remove_property("body"),
                    )
                    .var_as(
                        "newcomer",
                        traversal::g().add_n(
                            "Document",
                            vec![("body", PropertyInput::from("alpha newcomer"))],
                        ),
                    )
                    .returning(Vec::<String>::new()),
            ))
            .await
        {
            Ok(_) => break,
            Err(error)
                if error.is_transaction_conflict()
                    && attempt + 1 < MAX_TRANSACTION_CONFLICT_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("text-index mutation commits after maintenance: {error}"),
        }
    }
    assert_eq!(
        db.query(search("retired")).await.unwrap(),
        serde_json::json!({ "ids": [] })
    );
    assert_eq!(
        db.query(search("alpha")).await.unwrap(),
        serde_json::json!({ "ids": [2, 3] })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as("dropped", traversal::g().n(NodeRef::id(2)).drop())
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query(search("alpha")).await.unwrap(),
        serde_json::json!({ "ids": [3] })
    );

    let edge = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g()
                        .n(NodeRef::id(0))
                        .add_e(
                            "LINK",
                            NodeRef::id(1),
                            vec![("body", PropertyInput::from("edge alpha"))],
                        )
                        .id(),
                )
                .returning(["created"]),
        ))
        .await
        .unwrap();
    assert_eq!(edge, serde_json::json!({ "created": [0] }));
    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_text_index_edges("LINK", "body", None::<String>),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    assert_eq!(receipt["operation"]["kind"], "accepted");
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .expect("accepted edge text-index operation has an ID");
    await_index_operation_success(&db, operation_id, "edge text index").await;
    let search_edges = |query| {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_edges("LINK", "body", query, 8, None)
                        .id(),
                )
                .returning(["ids"]),
        )
    };
    assert_eq!(
        db.query(search_edges("alpha")).await.unwrap(),
        serde_json::json!({ "ids": [0] })
    );
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "updated",
                traversal::g()
                    .e(EdgeRef::id(0))
                    .set_property("body", "edge retired"),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query(search_edges("alpha")).await.unwrap(),
        serde_json::json!({ "ids": [] })
    );
    assert_eq!(
        db.query(search_edges("retired")).await.unwrap(),
        serde_json::json!({ "ids": [0] })
    );
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "removed",
                traversal::g().e(EdgeRef::id(0)).remove_property("body"),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query(search_edges("retired")).await.unwrap(),
        serde_json::json!({ "ids": [] })
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_restricts_node_and_edge_text_search_to_the_current_stream() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-restricted-text-search".to_owned(),
    })
    .await
    .expect("restricted text-search fixture opens");

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "first",
                traversal::g().add_n(
                    "Document",
                    vec![("body", PropertyInput::from("needle needle needle"))],
                ),
            )
            .var_as(
                "second",
                traversal::g().add_n(
                    "Document",
                    vec![("body", PropertyInput::from("needle needle"))],
                ),
            )
            .var_as(
                "third",
                traversal::g().add_n("Document", vec![("body", PropertyInput::from("needle"))]),
            )
            .var_as(
                "fourth",
                traversal::g().add_n(
                    "Document",
                    vec![("body", PropertyInput::from("needle filler filler"))],
                ),
            )
            .var_as(
                "edge_zero",
                traversal::g().n(NodeRef::id(0)).add_e(
                    "LINK",
                    NodeRef::id(1),
                    vec![("body", PropertyInput::from("needle needle needle"))],
                ),
            )
            .var_as(
                "edge_one",
                traversal::g().n(NodeRef::id(0)).add_e(
                    "LINK",
                    NodeRef::id(2),
                    vec![("body", PropertyInput::from("needle needle"))],
                ),
            )
            .var_as(
                "edge_two",
                traversal::g().n(NodeRef::id(0)).add_e(
                    "LINK",
                    NodeRef::id(3),
                    vec![("body", PropertyInput::from("needle"))],
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();

    for (operation, description) in [
        (
            traversal::g().create_text_index_nodes("Document", "body", None::<String>),
            "restricted node text index",
        ),
        (
            traversal::g().create_text_index_edges("LINK", "body", None::<String>),
            "restricted edge text index",
        ),
    ] {
        let receipt = db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as("operation", operation)
                    .returning(["operation"]),
            ))
            .await
            .unwrap();
        let operation_id = receipt["operation"]["operation_id"]
            .as_str()
            .expect("accepted text-index operation has an ID");
        await_index_operation_success(&db, operation_id, description).await;
    }

    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "all_nodes",
                    traversal::g()
                        .text_search_nodes("Document", "body", "needle", 10, None)
                        .id(),
                )
                .var_as(
                    "restricted_nodes",
                    traversal::g()
                        .n(NodeRef::ids([1, 2, 3]))
                        .text_search("Document", "body", "needle", 2, None)
                        .id(),
                )
                .var_as(
                    "single_node",
                    traversal::g()
                        .n(NodeRef::ids([2]))
                        .text_search("Document", "body", "needle", 10, None)
                        .id(),
                )
                .var_as(
                    "empty_nodes",
                    traversal::g()
                        .n(NodeRef::ids([]))
                        .text_search("Document", "body", "needle", 10, None)
                        .id(),
                )
                .var_as(
                    "node_state",
                    traversal::g()
                        .n(NodeRef::ids([1, 2]))
                        .bind("upstream")
                        .text_search("Document", "body", "needle", 2, None)
                        .project_bindings(vec![
                            BindingProjection::current("$id", "id"),
                            BindingProjection::current("$score", "score"),
                            BindingProjection::binding("upstream", "$id", "upstream_id"),
                        ]),
                )
                .var_as(
                    "node_sacks",
                    traversal::g()
                        .n(NodeRef::ids([1, 2]))
                        .with_sack(PropertyValue::from(12_i64))
                        .text_search("Document", "body", "needle", 2, None)
                        .sack_get(),
                )
                .var_as(
                    "all_edges",
                    traversal::g()
                        .text_search_edges("LINK", "body", "needle", 10, None)
                        .id(),
                )
                .var_as(
                    "restricted_edges",
                    traversal::g()
                        .e(EdgeRef::ids([1, 2]))
                        .text_search("LINK", "body", "needle", 2, None)
                        .id(),
                )
                .returning([
                    "all_nodes",
                    "restricted_nodes",
                    "single_node",
                    "empty_nodes",
                    "node_state",
                    "node_sacks",
                    "all_edges",
                    "restricted_edges",
                ]),
        ))
        .await
        .unwrap();

    let expected_nodes = response["all_nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|id| matches!(id.as_u64(), Some(1..=3)))
        .take(2)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        response["restricted_nodes"],
        serde_json::Value::Array(expected_nodes)
    );
    assert_eq!(response["single_node"], serde_json::json!([2]));
    assert_eq!(response["empty_nodes"], serde_json::Value::Null);
    assert!(response["node_sacks"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["sack"] == 12));
    assert!(response["node_state"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| {
            row["id"] == row["upstream_id"] && row["score"].as_f64().is_some_and(f64::is_finite)
        }));

    let expected_edges = response["all_edges"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|id| matches!(id.as_u64(), Some(1 | 2)))
        .take(2)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        response["restricted_edges"],
        serde_json::Value::Array(expected_edges)
    );

    let mutation_visible = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "retired_node",
                    traversal::g()
                        .n(NodeRef::id(1))
                        .set_property("body", "retired"),
                )
                .var_as("deleted_node", traversal::g().n(NodeRef::id(2)).drop())
                .var_as(
                    "retired_edge",
                    traversal::g()
                        .e(EdgeRef::id(1))
                        .set_property("body", "retired"),
                )
                .var_as(
                    "deleted_edge",
                    traversal::g().n(NodeRef::id(0)).drop_edge(NodeRef::id(3)),
                )
                .var_as(
                    "nodes",
                    traversal::g()
                        .n(NodeRef::ids([1, 2, 3]))
                        .text_search("Document", "body", "needle", 10, None)
                        .id(),
                )
                .var_as(
                    "edges",
                    traversal::g()
                        .e(EdgeRef::ids([1, 2]))
                        .text_search("LINK", "body", "needle", 10, None)
                        .id(),
                )
                .returning(["nodes", "edges"]),
        ))
        .await
        .unwrap();
    assert_eq!(
        mutation_visible,
        serde_json::json!({ "nodes": [3], "edges": [] })
    );
    let committed = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "nodes",
                    traversal::g()
                        .n(NodeRef::ids([1, 2, 3]))
                        .text_search("Document", "body", "needle", 10, None)
                        .id(),
                )
                .var_as(
                    "edges",
                    traversal::g()
                        .e(EdgeRef::ids([1, 2]))
                        .text_search("LINK", "body", "needle", 10, None)
                        .id(),
                )
                .returning(["nodes", "edges"]),
        ))
        .await
        .unwrap();
    assert_eq!(committed, serde_json::json!({ "nodes": [3], "edges": [] }));

    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_restricted_text_search_respects_tenant_partitions() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-restricted-text-tenant".to_owned(),
    })
    .await
    .expect("restricted tenant text-search fixture opens");
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "acme_first",
                traversal::g().add_n(
                    "Document",
                    vec![
                        ("body", PropertyInput::from("needle needle needle")),
                        ("tenant", PropertyInput::from("acme")),
                    ],
                ),
            )
            .var_as(
                "acme_second",
                traversal::g().add_n(
                    "Document",
                    vec![
                        ("body", PropertyInput::from("needle padding padding")),
                        ("tenant", PropertyInput::from("acme")),
                    ],
                ),
            )
            .var_as(
                "globex_first",
                traversal::g().add_n(
                    "Document",
                    vec![
                        ("body", PropertyInput::from("needle needle needle needle")),
                        ("tenant", PropertyInput::from("globex")),
                    ],
                ),
            )
            .var_as(
                "globex_second",
                traversal::g().add_n(
                    "Document",
                    vec![
                        (
                            "body",
                            PropertyInput::from("needle padding padding padding"),
                        ),
                        ("tenant", PropertyInput::from("globex")),
                    ],
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();

    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_text_index_nodes("Document", "body", Some("tenant")),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .expect("accepted tenant text-index operation has an ID");
    await_index_operation_success(&db, operation_id, "tenant restricted text index").await;

    let response = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "acme",
                    traversal::g()
                        .n(NodeRef::ids([0, 1, 2]))
                        .text_search(
                            "Document",
                            "body",
                            "needle",
                            10,
                            Some(PropertyValue::from("acme")),
                        )
                        .id(),
                )
                .var_as(
                    "globex",
                    traversal::g()
                        .n(NodeRef::ids([0, 2, 3]))
                        .text_search(
                            "Document",
                            "body",
                            "needle",
                            10,
                            Some(PropertyValue::from("globex")),
                        )
                        .id(),
                )
                .returning(["acme", "globex"]),
        ))
        .await
        .unwrap();
    assert_eq!(response["acme"], serde_json::json!([0, 1]));
    assert_eq!(response["globex"], serde_json::json!([2, 3]));
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_dynamic_text_search_inputs() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-dynamic-text-search".to_owned(),
    })
    .await
    .expect("production dynamic text-search fixture opens");
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "document",
                traversal::g().add_n(
                    "Document",
                    vec![("body", PropertyInput::from("alpha document"))],
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();

    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_text_index_nodes("Document", "body", None::<String>),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .expect("accepted text-index operation has an ID");
    await_index_operation_success(&db, operation_id, "dynamic text index").await;

    let dynamic_search = || {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .text_search_nodes_with(
                            "Document",
                            "body",
                            PropertyInput::param("query"),
                            StreamBound::expr(Expr::param("limit")),
                            None,
                        )
                        .id(),
                )
                .returning(["ids"]),
        )
    };
    let bound_search = |query: QueryValue, limit: QueryValue| {
        dynamic_search()
            .with_parameter_value("query", query)
            .with_parameter_value("limit", limit)
    };
    assert_eq!(
        db.query(bound_search(
            QueryValue::String("alpha".to_owned()),
            QueryValue::I64(1),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "ids": [0] })
    );

    let error = db
        .query(bound_search(QueryValue::I64(7), QueryValue::I64(1)))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("must evaluate to a string"),
        "{error}"
    );
    let error = db
        .query(bound_search(
            QueryValue::String(String::new()),
            QueryValue::I64(1),
        ))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must not be empty"), "{error}");
    let error = db
        .query(bound_search(
            QueryValue::String("alpha".to_owned()),
            QueryValue::Bool(true),
        ))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("must evaluate to an i64"),
        "{error}"
    );
    let error = db
        .query(bound_search(
            QueryValue::String("alpha".to_owned()),
            QueryValue::I64(0),
        ))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("non-positive value 0"),
        "{error}"
    );
    let error = db
        .query(
            dynamic_search().with_parameter_value("query", QueryValue::String("alpha".to_owned())),
        )
        .await
        .expect_err("an unbound dynamic search limit is rejected");
    assert_eq!(
        error.to_string(),
        "Query error: parameter `limit` is not bound"
    );
    db.close().await.unwrap();
}

async fn await_index_operation_success(db: &HelixDB, operation_id: &str, description: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = db
                .query(QueryRequest::read(
                    batch::read_batch()
                        .var_as("status", traversal::g().get_index_operation(operation_id))
                        .returning(["status"]),
                ))
                .await
                .unwrap();
            match status["status"]["status"].as_str() {
                Some("succeeded") => break,
                Some("queued" | "running") => tokio::task::yield_now().await,
                state => panic!("{description} operation reached unexpected state {state:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{description} activates within thirty seconds"));
}

#[test]
fn public_query_boundary_covers_dynamic_foreach_and_batch_conditions() {
    run_high_stack_contract(
        "public-dynamic-foreach-conditions",
        public_query_boundary_covers_dynamic_foreach_and_batch_conditions_contract,
    );
}

async fn public_query_boundary_covers_dynamic_foreach_and_batch_conditions_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-foreach".to_owned(),
    })
    .await
    .expect("production foreach fixture opens");

    let body = batch::write_batch().var_as(
        "created",
        traversal::g().add_n(
            "Audit",
            vec![
                ("event_id", PropertyInput::param("event_id")),
                ("rank", PropertyInput::param("rank")),
                ("active", PropertyInput::param("active")),
                ("ratio_f64", PropertyInput::param("ratio_f64")),
                ("ratio_f32", PropertyInput::param("ratio_f32")),
                ("nothing", PropertyInput::param("nothing")),
                ("items", PropertyInput::param("items")),
                ("metadata", PropertyInput::param("metadata")),
            ],
        ),
    );
    let foreach_batch = batch::write_batch()
        .for_each_param("events", body)
        .var_as("count", traversal::g().n_with_label("Audit").count())
        .var_as(
            "values",
            traversal::g().n_with_label("Audit").values(vec![
                "event_id",
                "rank",
                "active",
                "ratio_f64",
                "ratio_f32",
                "nothing",
                "items",
                "metadata",
            ]),
        )
        .returning(["count", "values"]);
    let events = QueryValue::Array(vec![
        QueryValue::Object(BTreeMap::from([
            (
                "event_id".to_owned(),
                QueryValue::String("event-1".to_owned()),
            ),
            ("rank".to_owned(), QueryValue::I64(1)),
            ("active".to_owned(), QueryValue::Bool(true)),
            ("ratio_f64".to_owned(), QueryValue::F64(1.5)),
            ("ratio_f32".to_owned(), QueryValue::F32(1.25)),
            ("nothing".to_owned(), QueryValue::Null),
            (
                "items".to_owned(),
                QueryValue::Array(vec![
                    QueryValue::I64(7),
                    QueryValue::String("nested".to_owned()),
                ]),
            ),
            (
                "metadata".to_owned(),
                QueryValue::Object(BTreeMap::from([(
                    "source".to_owned(),
                    QueryValue::String("api".to_owned()),
                )])),
            ),
        ])),
        QueryValue::Object(BTreeMap::from([
            (
                "event_id".to_owned(),
                QueryValue::String("event-2".to_owned()),
            ),
            ("rank".to_owned(), QueryValue::I64(2)),
            ("active".to_owned(), QueryValue::Bool(false)),
            ("ratio_f64".to_owned(), QueryValue::F64(2.5)),
            ("ratio_f32".to_owned(), QueryValue::F32(2.25)),
            ("nothing".to_owned(), QueryValue::Null),
            (
                "items".to_owned(),
                QueryValue::Array(vec![QueryValue::Bool(true), QueryValue::Null]),
            ),
            (
                "metadata".to_owned(),
                QueryValue::Object(BTreeMap::from([(
                    "source".to_owned(),
                    QueryValue::String("worker".to_owned()),
                )])),
            ),
        ])),
    ]);
    let response = db
        .query(QueryRequest::write(foreach_batch.clone()).with_parameter_value("events", events))
        .await
        .unwrap();
    let expected_values = serde_json::json!([
        {
            "event_id": "event-1",
            "rank": 1,
            "active": true,
            "ratio_f64": 1.5,
            "ratio_f32": 1.25,
            "nothing": null,
            "items": [7, "nested"],
            "metadata": { "source": "api" },
        },
        {
            "event_id": "event-2",
            "rank": 2,
            "active": false,
            "ratio_f64": 2.5,
            "ratio_f32": 2.25,
            "nothing": null,
            "items": [true, null],
            "metadata": { "source": "worker" },
        },
    ]);
    assert_eq!(
        response,
        serde_json::json!({
            "count": 2,
            "values": expected_values.clone(),
        })
    );

    let error = db
        .query(
            QueryRequest::write(foreach_batch.clone())
                .with_parameter_value("events", QueryValue::I64(1)),
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("expected `events` to be an array of objects"));
    let error = db
        .query(
            QueryRequest::write(foreach_batch.clone()).with_parameter_value(
                "events",
                QueryValue::Array(vec![QueryValue::String("not-an-object".to_owned())]),
            ),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("items to be objects"), "{error}");
    let error = db
        .query(
            QueryRequest::write(foreach_batch.clone()).with_parameter_value(
                "events",
                QueryValue::Array(vec![QueryValue::Object(BTreeMap::from([(
                    String::new(),
                    QueryValue::I64(1),
                )]))]),
            ),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("field names must not be empty"),
        "{error}"
    );
    assert_eq!(
        db.query(
            QueryRequest::write(foreach_batch)
                .with_parameter_value("events", QueryValue::Array(Vec::new()))
        )
        .await
        .unwrap(),
        serde_json::json!({
            "count": 2,
            "values": expected_values,
        })
    );

    let literal_write = batch::write_batch()
        .var_as(
            "created",
            traversal::g()
                .add_n(
                    "LiteralAudit",
                    vec![
                        ("null_value", PropertyInput::from(PropertyValue::Null)),
                        ("bool_value", PropertyInput::from(true)),
                        ("i64_value", PropertyInput::from(-7_i64)),
                        (
                            "datetime_value",
                            PropertyInput::from(PropertyValue::datetime_millis(0)),
                        ),
                        ("f64_value", PropertyInput::from(1.5_f64)),
                        ("f32_value", PropertyInput::from(1.25_f32)),
                        ("string_value", PropertyInput::from("literal")),
                        ("bytes_value", PropertyInput::from(vec![0_u8, 255_u8])),
                        ("i64_array", PropertyInput::from(vec![-1_i64, 2_i64])),
                        ("f64_array", PropertyInput::from(vec![1.5_f64, 2.5_f64])),
                        ("f32_array", PropertyInput::from(vec![1.25_f32, 2.25_f32])),
                        (
                            "string_array",
                            PropertyInput::from(vec!["alpha".to_owned(), "beta".to_owned()]),
                        ),
                        (
                            "array_value",
                            PropertyInput::from(PropertyValue::Array(vec![
                                PropertyValue::Null,
                                PropertyValue::I64(3),
                                PropertyValue::Object(BTreeMap::from([(
                                    "nested".to_owned(),
                                    PropertyValue::Bool(true),
                                )])),
                            ])),
                        ),
                        (
                            "object_value",
                            PropertyInput::from(PropertyValue::Object(BTreeMap::from([
                                (
                                    "array".to_owned(),
                                    PropertyValue::Array(vec![PropertyValue::String(
                                        "nested".to_owned(),
                                    )]),
                                ),
                                ("enabled".to_owned(), PropertyValue::Bool(false)),
                            ]))),
                        ),
                    ],
                )
                .count(),
        )
        .returning(["created"]);
    assert_eq!(
        db.query(QueryRequest::write(literal_write)).await.unwrap(),
        serde_json::json!({ "created": 1 })
    );
    let literal_read = batch::read_batch()
        .var_as(
            "values",
            traversal::g().n_with_label("LiteralAudit").values(vec![
                "null_value",
                "bool_value",
                "i64_value",
                "datetime_value",
                "f64_value",
                "f32_value",
                "string_value",
                "bytes_value",
                "i64_array",
                "f64_array",
                "f32_array",
                "string_array",
                "array_value",
                "object_value",
            ]),
        )
        .returning(["values"]);
    assert_eq!(
        db.query(QueryRequest::read(literal_read)).await.unwrap(),
        serde_json::json!({
            "values": [{
                "null_value": null,
                "bool_value": true,
                "i64_value": -7,
                "datetime_value": "1970-01-01T00:00:00.000Z",
                "f64_value": 1.5,
                "f32_value": 1.25,
                "string_value": "literal",
                "bytes_value": [0, 255],
                "i64_array": [-1, 2],
                "f64_array": [1.5, 2.5],
                "f32_array": [1.25, 2.25],
                "string_array": ["alpha", "beta"],
                "array_value": [null, 3, { "nested": true }],
                "object_value": { "array": ["nested"], "enabled": false },
            }],
        })
    );

    let conditions = batch::read_batch()
        .var_as("seed", traversal::g().n_with_label("Audit"))
        .var_as_if(
            "non_empty",
            batch::BatchCondition::VarNotEmpty("seed".to_owned()),
            traversal::g().n_with_label("Audit").count(),
        )
        .var_as_if(
            "empty",
            batch::BatchCondition::VarEmpty("seed".to_owned()),
            traversal::g().n_with_label("Audit").count(),
        )
        .var_as_if(
            "min_size",
            batch::BatchCondition::VarMinSize("seed".to_owned(), 2),
            traversal::g().n_with_label("Audit").count(),
        )
        .var_as_if(
            "previous",
            batch::BatchCondition::PrevNotEmpty,
            traversal::g().n_with_label("Audit").count(),
        )
        .returning(["non_empty", "empty", "min_size", "previous"]);
    assert_eq!(
        db.query(QueryRequest::read(conditions)).await.unwrap(),
        serde_json::json!({
            "non_empty": 2,
            "empty": [],
            "min_size": 2,
            "previous": 2,
        })
    );
    db.close().await.unwrap();
}

#[test]
fn public_execute_boundary_covers_typed_foreach_parameter_frames() {
    run_high_stack_contract(
        "public-typed-foreach-parameter-frames",
        public_execute_boundary_covers_typed_foreach_parameter_frames_contract,
    );
}

async fn public_execute_boundary_covers_typed_foreach_parameter_frames_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-typed-foreach".to_owned(),
    })
    .await
    .expect("production typed-foreach fixture opens");
    let body = batch::write_batch().var_as(
        "created",
        traversal::g().add_n(
            "TypedAudit",
            vec![
                ("name", PropertyInput::param("name")),
                ("rank", PropertyInput::param("rank")),
            ],
        ),
    );
    let foreach = batch::write_batch()
        .for_each_param("events", body)
        .returning(Vec::<String>::new());
    let plan = planning::plan_write_batch(&foreach, &context::PlannerContext::default())
        .expect("typed foreach batch plans");
    let events = ir::NonEmptyString::new("events").expect("parameter name is non-empty");

    let error = db
        .execute(&plan, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Query error: foreach parameter `events` is not bound"
    );

    for (value, message) in [
        (
            PropertyValue::I64(1),
            "expected `events` to be an array of objects",
        ),
        (
            PropertyValue::Array(vec![PropertyValue::String("not-an-object".to_owned())]),
            "expected `events` items to be objects",
        ),
        (
            PropertyValue::Array(vec![PropertyValue::Object(BTreeMap::from([(
                String::new(),
                PropertyValue::I64(1),
            )]))]),
            "foreach object field names must not be empty",
        ),
    ] {
        let error = db
            .execute(
                &plan,
                context::ParamBindings::default().with_value(events.clone(), value),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
    }
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("count", traversal::g().n_with_label("TypedAudit").count(),)
                .returning(["count"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "count": 0 })
    );

    db.execute(
        &plan,
        context::ParamBindings::default().with_value(
            events,
            PropertyValue::Array(vec![PropertyValue::Object(BTreeMap::from([
                ("name".to_owned(), PropertyValue::String("typed".to_owned())),
                ("rank".to_owned(), PropertyValue::I64(7)),
            ]))]),
        ),
    )
    .await
    .expect("typed foreach executes");
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "values",
                    traversal::g()
                        .n_with_label("TypedAudit")
                        .values(vec!["name", "rank"]),
                )
                .returning(["values"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "values": [{ "name": "typed", "rank": 7 }] })
    );
    db.close().await.unwrap();
}

#[test]
fn public_query_boundary_covers_predicate_sets_and_stream_operators() {
    run_high_stack_contract(
        "public-predicate-stream-operators",
        public_query_boundary_covers_predicate_sets_and_stream_operators_contract,
    );
}

async fn public_query_boundary_covers_predicate_sets_and_stream_operators_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-predicate-streams".to_owned(),
    })
    .await
    .expect("production predicate-stream fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "alpha",
            traversal::g().add_n(
                "Item",
                vec![
                    ("name", PropertyInput::from("alpha")),
                    ("rank", PropertyInput::from(1_i64)),
                ],
            ),
        )
        .var_as(
            "beta",
            traversal::g().add_n(
                "Item",
                vec![
                    ("name", PropertyInput::from("beta")),
                    ("rank", PropertyInput::from(2_i64)),
                    ("note", PropertyInput::from("present")),
                ],
            ),
        )
        .var_as(
            "alphabet",
            traversal::g().add_n(
                "Item",
                vec![
                    ("name", PropertyInput::from("alphabet")),
                    ("rank", PropertyInput::from(3_i64)),
                ],
            ),
        )
        .var_as(
            "gamma",
            traversal::g().add_n(
                "Item",
                vec![
                    ("name", PropertyInput::from("gamma")),
                    ("rank", PropertyInput::from(4_i64)),
                    ("note", PropertyInput::from(PropertyValue::Null)),
                ],
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let read = batch::read_batch()
        .var_as("excluded", traversal::g().n(NodeRef::ids([1, 3])))
        .var_as(
            "or",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::or(vec![
                    Predicate::lt("rank", 2_i64),
                    Predicate::gt("rank", 3_i64),
                ]))
                .id(),
        )
        .var_as(
            "between",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::between("rank", 2_i64, 3_i64))
                .id(),
        )
        .var_as(
            "not_equal",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::neq("name", "beta"))
                .id(),
        )
        .var_as(
            "starts_with",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::starts_with("name", "alp"))
                .id(),
        )
        .var_as(
            "ends_with",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::ends_with("name", "a"))
                .id(),
        )
        .var_as(
            "contains",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::contains("name", "ph"))
                .id(),
        )
        .var_as(
            "is_in",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::is_in(
                    "rank",
                    PropertyValue::I64Array(vec![1, 4]),
                ))
                .id(),
        )
        .var_as(
            "is_null",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::is_null("note"))
                .id(),
        )
        .var_as(
            "is_not_null",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::is_not_null("note"))
                .id(),
        )
        .var_as(
            "compare_eq",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::Compare {
                    left: Expr::prop("rank"),
                    op: CompareOp::Eq,
                    right: Expr::val(2_i64),
                })
                .id(),
        )
        .var_as(
            "compare_neq",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::Compare {
                    left: Expr::prop("rank"),
                    op: CompareOp::Neq,
                    right: Expr::val(2_i64),
                })
                .id(),
        )
        .var_as(
            "compare_gt",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::Compare {
                    left: Expr::prop("rank"),
                    op: CompareOp::Gt,
                    right: Expr::val(2_i64),
                })
                .id(),
        )
        .var_as(
            "compare_gte",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::Compare {
                    left: Expr::prop("rank"),
                    op: CompareOp::Gte,
                    right: Expr::val(2_i64),
                })
                .id(),
        )
        .var_as(
            "compare_lt",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::Compare {
                    left: Expr::prop("rank"),
                    op: CompareOp::Lt,
                    right: Expr::val(2_i64),
                })
                .id(),
        )
        .var_as(
            "compare_lte",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::Compare {
                    left: Expr::prop("rank"),
                    op: CompareOp::Lte,
                    right: Expr::val(2_i64),
                })
                .id(),
        )
        .var_as(
            "generic_is_in",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::IsIn {
                    value: Expr::prop("rank"),
                    values: Expr::val(PropertyValue::Array(vec![
                        PropertyValue::I64(2),
                        PropertyValue::I64(3),
                    ])),
                })
                .id(),
        )
        .var_as(
            "string_is_in",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::IsIn {
                    value: Expr::prop("name"),
                    values: Expr::val(PropertyValue::StringArray(vec![
                        "alpha".to_owned(),
                        "gamma".to_owned(),
                    ])),
                })
                .id(),
        )
        .var_as(
            "scalar_is_in",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::IsIn {
                    value: Expr::prop("rank"),
                    values: Expr::val(2_i64),
                })
                .id(),
        )
        .var_as(
            "non_string_starts_with",
            traversal::g()
                .n(NodeRef::all())
                .where_(Predicate::StartsWith {
                    value: Expr::prop("rank"),
                    prefix: Expr::val("2"),
                })
                .id(),
        )
        .var_as(
            "ordered_window",
            traversal::g()
                .n(NodeRef::all())
                .order_by("rank", traversal::Order::Desc)
                .skip(1_usize)
                .limit(2_usize)
                .id(),
        )
        .var_as(
            "deduped",
            traversal::g()
                .n(NodeRef::all())
                .union(vec![
                    traversal::sub().where_(Predicate::has_key("name")),
                    traversal::sub().where_(Predicate::has_key("name")),
                ])
                .dedup()
                .id(),
        )
        .var_as(
            "folded",
            traversal::g().n(NodeRef::all()).fold().unfold().id(),
        )
        .var_as(
            "folded_members",
            traversal::g().n(NodeRef::ids([1, 3])).fold(),
        )
        .var_as(
            "within",
            traversal::g().n(NodeRef::all()).within("excluded").id(),
        )
        .var_as(
            "within_folded",
            traversal::g()
                .n(NodeRef::all())
                .within("folded_members")
                .id(),
        )
        .var_as(
            "without",
            traversal::g().n(NodeRef::all()).without("excluded").id(),
        )
        .var_as("exists", traversal::g().n_with_label("Item").exists())
        .var_as("labels", traversal::g().n(NodeRef::all()).label())
        .returning([
            "or",
            "between",
            "not_equal",
            "starts_with",
            "ends_with",
            "contains",
            "is_in",
            "is_null",
            "is_not_null",
            "compare_eq",
            "compare_neq",
            "compare_gt",
            "compare_gte",
            "compare_lt",
            "compare_lte",
            "generic_is_in",
            "string_is_in",
            "scalar_is_in",
            "non_string_starts_with",
            "ordered_window",
            "deduped",
            "folded",
            "within",
            "within_folded",
            "without",
            "exists",
            "labels",
        ]);
    assert_eq!(
        db.query(QueryRequest::read(read)).await.unwrap(),
        serde_json::json!({
            "or": [0, 3],
            "between": [1, 2],
            "not_equal": [0, 2, 3],
            "starts_with": [0, 2],
            "ends_with": [0, 1, 3],
            "contains": [0, 2],
            "is_in": [0, 3],
            "is_null": [0, 2, 3],
            "is_not_null": [1],
            "compare_eq": [1],
            "compare_neq": [0, 2, 3],
            "compare_gt": [2, 3],
            "compare_gte": [1, 2, 3],
            "compare_lt": [0],
            "compare_lte": [0, 1],
            "generic_is_in": [1, 2],
            "string_is_in": [0, 3],
            "scalar_is_in": [1],
            "non_string_starts_with": [],
            "ordered_window": [2, 1],
            "deduped": [0, 1, 2, 3],
            "folded": [0, 1, 2, 3],
            "within": [1, 3],
            "within_folded": [1, 3],
            "without": [0, 2],
            "exists": true,
            "labels": ["Item", "Item", "Item", "Item"],
        })
    );

    let scalar_members = batch::read_batch()
        .var_as("members", traversal::g().n(NodeRef::all()).count())
        .var_as(
            "result",
            traversal::g().n(NodeRef::all()).within("members").id(),
        )
        .returning(["result"]);
    let error = db
        .query(QueryRequest::read(scalar_members))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Query error: variable operation expected stream value, got Count(4)"
    );

    for (index, predicate) in [
        Predicate::Eq {
            left: Expr::param("missing"),
            right: Expr::val(1_i64),
        },
        Predicate::Neq {
            left: Expr::param("missing"),
            right: Expr::val(1_i64),
        },
        Predicate::Gt {
            left: Expr::param("missing"),
            right: Expr::val(1_i64),
        },
        Predicate::Gte {
            left: Expr::param("missing"),
            right: Expr::val(1_i64),
        },
        Predicate::Lt {
            left: Expr::param("missing"),
            right: Expr::val(1_i64),
        },
        Predicate::Lte {
            left: Expr::param("missing"),
            right: Expr::val(1_i64),
        },
        Predicate::Compare {
            left: Expr::param("missing"),
            op: CompareOp::Eq,
            right: Expr::val(1_i64),
        },
        Predicate::Compare {
            left: Expr::param("missing"),
            op: CompareOp::Neq,
            right: Expr::val(1_i64),
        },
        Predicate::StartsWith {
            value: Expr::param("missing"),
            prefix: Expr::val("a"),
        },
        Predicate::StartsWith {
            value: Expr::val("alpha"),
            prefix: Expr::param("missing"),
        },
        Predicate::EndsWith {
            value: Expr::param("missing"),
            suffix: Expr::val("a"),
        },
        Predicate::EndsWith {
            value: Expr::val("alpha"),
            suffix: Expr::param("missing"),
        },
        Predicate::Contains {
            value: Expr::param("missing"),
            substring: Expr::val("a"),
        },
        Predicate::And {
            predicates: vec![Predicate::Eq {
                left: Expr::param("missing"),
                right: Expr::val(1_i64),
            }],
        },
    ]
    .into_iter()
    .enumerate()
    {
        let invalid = batch::read_batch()
            .var_as(
                "invalid",
                traversal::g().n(NodeRef::id(0)).where_(predicate).id(),
            )
            .returning(["invalid"]);
        let error = db.query(QueryRequest::read(invalid)).await.unwrap_err();
        assert_eq!(
            error.to_string(),
            "Query error: parameter `missing` is not bound",
            "predicate propagation case {index}"
        );
    }
    db.close().await.unwrap();
}

#[test]
fn public_query_boundary_covers_projection_expressions_and_row_bindings() {
    run_high_stack_contract(
        "public-projection-expressions-row-bindings",
        public_query_boundary_covers_projection_expressions_and_row_bindings_contract,
    );
}

async fn public_query_boundary_covers_projection_expressions_and_row_bindings_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-projection-bindings".to_owned(),
    })
    .await
    .expect("production projection-binding fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "alice",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("alice")),
                    ("score", PropertyInput::from(8_i64)),
                    ("active", PropertyInput::from(true)),
                ],
            ),
        )
        .var_as(
            "bob",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("bob")),
                    ("score", PropertyInput::from(3_i64)),
                    ("active", PropertyInput::from(false)),
                ],
            ),
        )
        .var_as(
            "knows",
            traversal::g()
                .n(NodeRef::var("alice"))
                .add_e(
                    "KNOWS",
                    NodeRef::var("bob"),
                    vec![("weight", PropertyInput::from(3_i64))],
                )
                .id(),
        )
        .returning(["knows"]);
    assert_eq!(
        db.query(QueryRequest::write(fixture)).await.unwrap(),
        serde_json::json!({ "knows": [0] })
    );

    let read = batch::read_batch()
        .var_as(
            "computed",
            traversal::g()
                .n(NodeRef::all())
                .order_by("score", traversal::Order::Desc)
                .project(vec![
                    Projection::property("$id", "id"),
                    Projection::property("$label", "label"),
                    Projection::property("name", "name"),
                    Projection::expr("added", Expr::prop("score").add_expr(Expr::param("bonus"))),
                    Projection::expr("subtracted", Expr::prop("score").sub_expr(Expr::val(1))),
                    Projection::expr("multiplied", Expr::prop("score").mul_expr(Expr::val(2))),
                    Projection::expr("divided", Expr::prop("score").div_expr(Expr::val(2))),
                    Projection::expr("remainder", Expr::prop("score").modulo(Expr::val(3))),
                    Projection::expr("negated", Expr::prop("score").neg_expr()),
                    Projection::expr(
                        "state",
                        Expr::case(
                            vec![(Predicate::eq("active", true), Expr::val("active"))],
                            Some(Expr::val("inactive")),
                        ),
                    ),
                    Projection::expr("expression_id", Expr::id()),
                ]),
        )
        .var_as(
            "edge",
            traversal::g().e(EdgeRef::id(0)).project(vec![
                Projection::property("$id", "id"),
                Projection::property("$label", "label"),
                Projection::property("weight", "weight"),
                Projection::from_endpoint("$id", "from"),
                Projection::to_endpoint("$id", "to"),
            ]),
        )
        .var_as(
            "bound",
            traversal::g()
                .n(NodeRef::id(0))
                .bind("source")
                .out(Some("KNOWS"))
                .project_bindings(vec![
                    BindingProjection::binding("source", "name", "source_name"),
                    BindingProjection::current("name", "target_name"),
                    BindingProjection::coalesce(
                        vec![
                            BindingValueRef::binding("source", "nickname"),
                            BindingValueRef::binding("source", "name"),
                        ],
                        "owner",
                    ),
                    BindingProjection::binding("missing", "name", "missing_binding"),
                    BindingProjection::current("missing", "missing_property"),
                    BindingProjection::coalesce(
                        vec![
                            BindingValueRef::binding("missing", "name"),
                            BindingValueRef::current("missing"),
                        ],
                        "missing_value",
                    ),
                ]),
        )
        .var_as(
            "distinct",
            traversal::g()
                .n(NodeRef::id(0))
                .union(vec![
                    traversal::sub().out(Some("KNOWS")),
                    traversal::sub().out(Some("KNOWS")),
                ])
                .project_distinct_bindings(vec![BindingProjection::current("name", "name")]),
        )
        .var_as(
            "edge_properties",
            traversal::g().e(EdgeRef::id(0)).edge_properties(),
        )
        .returning(["computed", "edge", "bound", "distinct", "edge_properties"]);
    let read = QueryRequest::read(read).with_parameter_value("bonus", QueryValue::I64(2));
    assert_eq!(
        db.query(read).await.unwrap(),
        serde_json::json!({
            "computed": [
                {
                    "id": 0,
                    "label": "Person",
                    "name": "alice",
                    "added": 10.0,
                    "subtracted": 7.0,
                    "multiplied": 16.0,
                    "divided": 4.0,
                    "remainder": 2,
                    "negated": -8,
                    "state": "active",
                    "expression_id": 0,
                },
                {
                    "id": 1,
                    "label": "Person",
                    "name": "bob",
                    "added": 5.0,
                    "subtracted": 2.0,
                    "multiplied": 6.0,
                    "divided": 1.5,
                    "remainder": 0,
                    "negated": -3,
                    "state": "inactive",
                    "expression_id": 1,
                },
            ],
            "edge": [{ "id": 0, "label": "KNOWS", "weight": 3, "from": 0, "to": 1 }],
            "bound": [{ "source_name": "alice", "target_name": "bob", "owner": "alice" }],
            "distinct": [{ "name": "bob" }],
            "edge_properties": [{
                "$id": 0,
                "$from": 0,
                "$to": 1,
                "$label": "KNOWS",
                "weight": 3,
            }],
        })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "removed",
                traversal::g().n(NodeRef::id(1)).remove_property("active"),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    let missing_order_values = batch::read_batch()
        .var_as(
            "ascending",
            traversal::g()
                .n(NodeRef::ids([0, 1]))
                .order_by("active", traversal::Order::Asc)
                .id(),
        )
        .var_as(
            "descending",
            traversal::g()
                .n(NodeRef::ids([0, 1]))
                .order_by("active", traversal::Order::Desc)
                .id(),
        )
        .var_as(
            "all_missing",
            traversal::g()
                .n(NodeRef::ids([0, 1]))
                .order_by("missing", traversal::Order::Asc)
                .id(),
        )
        .var_as(
            "reverse_input",
            traversal::g()
                .n(NodeRef::ids([1, 0]))
                .order_by("active", traversal::Order::Asc)
                .id(),
        )
        .returning(["ascending", "descending", "all_missing", "reverse_input"]);
    assert_eq!(
        db.query(QueryRequest::read(missing_order_values))
            .await
            .unwrap(),
        serde_json::json!({
            "ascending": [1, 0],
            "descending": [0, 1],
            "all_missing": [0, 1],
            "reverse_input": [1, 0],
        })
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_aggregate_and_expression_value_edges() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-value-edges".to_owned(),
    })
    .await
    .expect("production value-edge fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "integer",
            traversal::g().add_n(
                "Metric",
                vec![
                    ("name", PropertyInput::from("integer")),
                    ("score", PropertyInput::from(10_i64)),
                ],
            ),
        )
        .var_as(
            "float",
            traversal::g().add_n(
                "Metric",
                vec![
                    ("name", PropertyInput::from("float")),
                    ("score", PropertyInput::from(2.5_f64)),
                ],
            ),
        )
        .var_as(
            "numeric_text",
            traversal::g().add_n(
                "Metric",
                vec![
                    ("name", PropertyInput::from("numeric_text")),
                    ("score", PropertyInput::from("3.5")),
                ],
            ),
        )
        .var_as(
            "boolean",
            traversal::g().add_n(
                "Metric",
                vec![
                    ("name", PropertyInput::from("boolean")),
                    ("score", PropertyInput::from(true)),
                ],
            ),
        )
        .var_as(
            "missing",
            traversal::g().add_n("Metric", vec![("name", PropertyInput::from("missing"))]),
        )
        .returning(Vec::<String>::new());
    assert_eq!(
        db.query(QueryRequest::write(fixture)).await.unwrap(),
        serde_json::json!({})
    );

    let aggregates = batch::read_batch()
        .var_as(
            "count",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Count, "score"),
        )
        .var_as(
            "sum",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Sum, "score"),
        )
        .var_as(
            "minimum",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Min, "score"),
        )
        .var_as(
            "maximum",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Max, "score"),
        )
        .var_as(
            "mean",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Mean, "score"),
        )
        .var_as(
            "empty_count",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Count, "absent"),
        )
        .var_as(
            "empty_sum",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Sum, "absent"),
        )
        .var_as(
            "empty_minimum",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Min, "absent"),
        )
        .var_as(
            "empty_maximum",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Max, "absent"),
        )
        .var_as(
            "empty_mean",
            traversal::g()
                .n(NodeRef::all())
                .aggregate_by(traversal::AggregateFunction::Mean, "absent"),
        )
        .returning([
            "count",
            "sum",
            "minimum",
            "maximum",
            "mean",
            "empty_count",
            "empty_sum",
            "empty_minimum",
            "empty_maximum",
            "empty_mean",
        ]);
    assert_eq!(
        db.query(QueryRequest::read(aggregates)).await.unwrap(),
        serde_json::json!({
            "count": [{ "score_Count": 3.0 }],
            "sum": [{ "score_Sum": 16.0 }],
            "minimum": [{ "score_Min": 2.5 }],
            "maximum": [{ "score_Max": 10.0 }],
            "mean": [{ "score_Mean": 5.333333333333333 }],
            "empty_count": [{ "absent_Count": 0.0 }],
            "empty_sum": [{ "absent_Sum": 0.0 }],
            "empty_minimum": [{ "absent_Min": 0.0 }],
            "empty_maximum": [{ "absent_Max": 0.0 }],
            "empty_mean": [{ "absent_Mean": 0.0 }],
        })
    );

    let expressions = batch::read_batch()
        .var_as(
            "expressions",
            traversal::g().n(NodeRef::id(1)).project(vec![
                Projection::expr("timestamp", Expr::Timestamp),
                Projection::expr("datetime", Expr::DateTimeNow),
                Projection::expr("negated", Expr::prop("score").neg_expr()),
                Projection::expr(
                    "no_match",
                    Expr::case(
                        vec![(Predicate::eq("name", "other"), Expr::val("matched"))],
                        None,
                    ),
                ),
            ]),
        )
        .returning(["expressions"]);
    let result = db.query(QueryRequest::read(expressions)).await.unwrap();
    let object = result["expressions"][0]
        .as_object()
        .expect("projection returns an object");
    assert!(object["timestamp"].as_i64().is_some());
    assert!(object["datetime"].as_str().is_some());
    assert_eq!(object["negated"], serde_json::json!(-2.5));
    assert_eq!(object["no_match"], serde_json::Value::Null);

    for (expr, message) in [
        (
            Expr::prop("score").add_expr(Expr::val(1)),
            "left expression must be numeric",
        ),
        (
            Expr::val(1).add_expr(Expr::prop("score")),
            "right expression must be numeric",
        ),
        (
            Expr::prop("score").modulo(Expr::val(1)),
            "mod left expression must be i64",
        ),
        (
            Expr::val(1).modulo(Expr::prop("score")),
            "mod right expression must be i64",
        ),
        (
            Expr::param("missing").modulo(Expr::val(1)),
            "parameter `missing` is not bound",
        ),
        (
            Expr::val(1).modulo(Expr::param("missing")),
            "parameter `missing` is not bound",
        ),
        (
            Expr::case(
                vec![(
                    Predicate::Eq {
                        left: Expr::param("missing"),
                        right: Expr::val(1_i64),
                    },
                    Expr::val("matched"),
                )],
                None,
            ),
            "parameter `missing` is not bound",
        ),
        (
            Expr::prop("score").neg_expr(),
            "neg expression must be numeric",
        ),
        (Expr::param("missing"), "parameter `missing` is not bound"),
    ] {
        let invalid = batch::read_batch()
            .var_as(
                "invalid",
                traversal::g()
                    .n(NodeRef::id(2))
                    .project(vec![Projection::expr("value", expr)]),
            )
            .returning(["invalid"]);
        assert_eq!(
            db.query(QueryRequest::read(invalid))
                .await
                .unwrap_err()
                .to_string(),
            format!("Query error: {message}")
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_mutation_rollback_and_graph_cleanup() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-mutation-cleanup".to_owned(),
    })
    .await
    .expect("production mutation-cleanup fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n("Item", Vec::<(&str, PropertyInput)>::new()),
        )
        .var_as(
            "second",
            traversal::g().add_n("Item", Vec::<(&str, PropertyInput)>::new()),
        )
        .var_as(
            "third",
            traversal::g().add_n("Item", Vec::<(&str, PropertyInput)>::new()),
        )
        .var_as(
            "knows",
            traversal::g()
                .n(NodeRef::var("first"))
                .add_e(
                    "KNOWS",
                    NodeRef::var("second"),
                    vec![("weight", PropertyInput::from(1_i64))],
                )
                .id(),
        )
        .var_as(
            "likes",
            traversal::g()
                .n(NodeRef::var("first"))
                .add_e(
                    "LIKES",
                    NodeRef::var("second"),
                    vec![("weight", PropertyInput::from(2_i64))],
                )
                .id(),
        )
        .var_as(
            "parallel",
            traversal::g()
                .n(NodeRef::var("first"))
                .add_e(
                    "KNOWS",
                    NodeRef::var("second"),
                    vec![("weight", PropertyInput::from(3_i64))],
                )
                .id(),
        )
        .var_as(
            "tail",
            traversal::g()
                .n(NodeRef::var("second"))
                .add_e(
                    "KNOWS",
                    NodeRef::var("third"),
                    vec![("weight", PropertyInput::from(4_i64))],
                )
                .id(),
        )
        .returning(["knows", "likes", "parallel", "tail"]);
    assert_eq!(
        db.query(QueryRequest::write(fixture)).await.unwrap(),
        serde_json::json!({
            "knows": [0],
            "likes": [1],
            "parallel": [2],
            "tail": [3],
        })
    );

    let invalid = batch::write_batch()
        .var_as(
            "changed",
            traversal::g()
                .e(EdgeRef::id(1))
                .set_property("weight", 99_i64),
        )
        .var_as(
            "invalid",
            traversal::g()
                .e(EdgeRef::id(1))
                .set_property("$label", "BROKEN"),
        )
        .returning(Vec::<String>::new());
    let error = db.query(QueryRequest::write(invalid)).await.unwrap_err();
    assert!(error.to_string().contains("edge `$label` mutations"));
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "unchanged",
                    traversal::g()
                        .e(EdgeRef::id(1))
                        .value_map(Some(vec!["$label", "weight"])),
                )
                .returning(["unchanged"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "unchanged": [{ "$label": "LIKES", "weight": 2 }] })
    );

    let drop_labeled = batch::write_batch()
        .var_as(
            "removed",
            traversal::g()
                .n(NodeRef::id(0))
                .drop_edge_labeled(NodeRef::param("target"), "KNOWS"),
        )
        .returning(Vec::<String>::new());
    let drop_labeled =
        QueryRequest::write(drop_labeled).with_parameter_value("target", QueryValue::I64(1));
    db.query(drop_labeled).await.unwrap();
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("edge_ids", traversal::g().e(EdgeRef::all()).id())
                .var_as(
                    "remaining_neighbor",
                    traversal::g().n(NodeRef::id(0)).out(None::<&str>).id(),
                )
                .returning(["edge_ids", "remaining_neighbor"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "edge_ids": [1, 3], "remaining_neighbor": [1] })
    );

    let drop_by_id = batch::write_batch()
        .var_as("selected", traversal::g().e(EdgeRef::id(1)))
        .var_as(
            "removed_from_variable",
            traversal::g().drop_edge_by_id(EdgeRef::var("selected")),
        )
        .var_as(
            "removed_from_input",
            traversal::g()
                .n(NodeRef::id(1))
                .drop_edge_by_id(EdgeRef::param("edge_ids")),
        )
        .returning(Vec::<String>::new());
    let drop_by_id = QueryRequest::write(drop_by_id)
        .with_parameter_value("edge_ids", QueryValue::Array(vec![QueryValue::I64(3)]));
    db.query(drop_by_id).await.unwrap();
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("edge_count", traversal::g().e(EdgeRef::all()).count())
                .var_as(
                    "first_out",
                    traversal::g().n(NodeRef::id(0)).out(None::<&str>).id(),
                )
                .var_as(
                    "third_in",
                    traversal::g().n(NodeRef::id(2)).in_(None::<&str>).id(),
                )
                .returning(["edge_count", "first_out", "third_in"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "edge_count": 0, "first_out": [], "third_in": [] })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "head",
                traversal::g().n(NodeRef::id(0)).add_e(
                    "LINK",
                    NodeRef::id(1),
                    Vec::<(&str, PropertyInput)>::new(),
                ),
            )
            .var_as(
                "tail",
                traversal::g().n(NodeRef::id(1)).add_e(
                    "LINK",
                    NodeRef::id(2),
                    Vec::<(&str, PropertyInput)>::new(),
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    let drop_node = batch::write_batch()
        .var_as("removed", traversal::g().n(NodeRef::param("nodes")).drop())
        .returning(Vec::<String>::new());
    let drop_node = QueryRequest::write(drop_node)
        .with_parameter_value("nodes", QueryValue::Array(vec![QueryValue::I64(1)]));
    db.query(drop_node).await.unwrap();
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as("node_ids", traversal::g().n(NodeRef::all()).id())
                .var_as("edge_ids", traversal::g().e(EdgeRef::all()).id())
                .var_as(
                    "first_out",
                    traversal::g().n(NodeRef::id(0)).out(None::<&str>).id(),
                )
                .var_as(
                    "third_in",
                    traversal::g().n(NodeRef::id(2)).in_(None::<&str>).id(),
                )
                .returning(["node_ids", "edge_ids", "first_out", "third_in"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({
            "node_ids": [0, 2],
            "edge_ids": [],
            "first_out": [],
            "third_in": [],
        })
    );
    db.close().await.unwrap();
}

#[test]
fn public_query_boundary_covers_active_secondary_index_families() {
    run_high_stack_contract(
        "public-secondary-index-families",
        public_query_boundary_covers_active_secondary_index_families_contract,
    );
}

async fn public_query_boundary_covers_active_secondary_index_families_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-secondary-indexes".to_owned(),
    })
    .await
    .expect("production secondary-index fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n(
                "Document",
                vec![
                    ("category", PropertyInput::from("group")),
                    ("code", PropertyInput::from("A")),
                    ("rank", PropertyInput::from(1_i64)),
                ],
            ),
        )
        .var_as(
            "second",
            traversal::g().add_n(
                "Document",
                vec![
                    ("category", PropertyInput::from("group")),
                    ("code", PropertyInput::from("B")),
                    ("rank", PropertyInput::from(2_i64)),
                ],
            ),
        )
        .var_as(
            "third",
            traversal::g().add_n(
                "Document",
                vec![
                    ("category", PropertyInput::from("other")),
                    ("code", PropertyInput::from("C")),
                    ("rank", PropertyInput::from(3_i64)),
                ],
            ),
        )
        .var_as(
            "first_link",
            traversal::g().n(NodeRef::var("first")).add_e(
                "LINK",
                NodeRef::var("second"),
                vec![
                    ("kind", PropertyInput::from("primary")),
                    ("status", PropertyInput::from("active")),
                    ("weight", PropertyInput::from(1_i64)),
                ],
            ),
        )
        .var_as(
            "second_link",
            traversal::g().n(NodeRef::var("second")).add_e(
                "LINK",
                NodeRef::var("third"),
                vec![
                    ("kind", PropertyInput::from("primary")),
                    ("status", PropertyInput::from("inactive")),
                    ("weight", PropertyInput::from(2_i64)),
                ],
            ),
        )
        .var_as(
            "third_link",
            traversal::g().n(NodeRef::var("first")).add_e(
                "LINK",
                NodeRef::var("third"),
                vec![
                    ("kind", PropertyInput::from("secondary")),
                    ("status", PropertyInput::from("active")),
                    ("weight", PropertyInput::from(3_i64)),
                ],
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    for (spec, description) in [
        (
            index::IndexSpec::node_equality("Document", "category"),
            "node equality index",
        ),
        (
            index::IndexSpec::node_unique_equality("Document", "code"),
            "unique node equality index",
        ),
        (
            index::IndexSpec::node_range("Document", "rank"),
            "ascending node range index",
        ),
        (
            index::IndexSpec::edge_equality("LINK", "kind"),
            "edge equality index",
        ),
        (
            index::IndexSpec::edge_equality("LINK", "status"),
            "second edge equality index",
        ),
        (
            index::IndexSpec::edge_range_desc("LINK", "weight"),
            "descending edge range index",
        ),
    ] {
        let receipt = db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as("operation", traversal::g().create_index_if_not_exists(spec))
                    .returning(["operation"]),
            ))
            .await
            .unwrap();
        assert_eq!(receipt["operation"]["kind"], "accepted");
        let operation_id = receipt["operation"]["operation_id"]
            .as_str()
            .expect("accepted secondary-index operation has an ID");
        await_index_operation_success(&db, operation_id, description).await;
    }

    let lifecycle_plan = |consumer: exec::ExecOp, include_empty_dependency: bool| {
        let receipt_id = exec::ExecStepId::new(1).expect("receipt step ID is positive");
        let mut steps = vec![exec::ExecStep {
            id: receipt_id,
            dependencies: Vec::new(),
            output: ir::BatchOutputPlan::Discard,
            semantic_return_shape: None,
            condition: exec::ExecCondition::Always,
            op: exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec: ir::IndexDdlCreateSpec::NodeEquality {
                        key: catalog::ScopedPropertyKey::try_new("Document", "category")
                            .expect("active index key validates"),
                        uniqueness: catalog::IndexUniqueness::NonUnique,
                    },
                    mode: ir::IndexCreateMode::IfNotExists,
                },
            },
            schedule: exec::ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: cost::CostVector::ZERO,
        }];
        let dependencies = if include_empty_dependency {
            let empty_id = exec::ExecStepId::new(2).expect("empty step ID is positive");
            steps.push(exec::ExecStep {
                id: empty_id,
                dependencies: Vec::new(),
                output: ir::BatchOutputPlan::Discard,
                semantic_return_shape: None,
                condition: exec::ExecCondition::Always,
                op: exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty)),
                },
                schedule: exec::ExecSchedule::Pipeline,
                delivered: properties::DeliveredProperties::default(),
                cost: cost::CostVector::ZERO,
            });
            vec![receipt_id, empty_id]
        } else {
            vec![receipt_id]
        };
        let root_id = exec::ExecStepId::new(steps.len() + 1).expect("root step ID is positive");
        steps.push(exec::ExecStep {
            id: root_id,
            dependencies,
            output: ir::BatchOutputPlan::Discard,
            semantic_return_shape: None,
            condition: exec::ExecCondition::Always,
            op: consumer,
            schedule: exec::ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: cost::CostVector::ZERO,
        });
        exec::ExecutablePlan::new(
            ir::PlanKind::Write,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::try_from_vec(steps).expect("lifecycle plan is non-empty"),
            root_id,
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("lifecycle consumer plan validates")
    };
    for (consumer, expected) in [
        (
            exec::ExecOp::Count {
                plan: Box::new(exec::ExecCountPlan::InputRows {
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            },
            "Query error: count cannot consume an index lifecycle value",
        ),
        (
            exec::ExecOp::Aggregate {
                aggregate: ir::AggregatePlan::Group(
                    ir::NonEmptyString::new("category").expect("property name is non-empty"),
                ),
            },
            "Query error: aggregate cannot consume an index lifecycle value",
        ),
        (
            exec::ExecOp::Distinct,
            "Query error: distinct cannot consume an index lifecycle value",
        ),
        (
            exec::ExecOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            },
            "Query error: limit cannot consume an index lifecycle value",
        ),
        (
            exec::ExecOp::Skip {
                count: ir::StreamBoundPlan::Literal(1),
            },
            "Query error: skip cannot consume an index lifecycle value",
        ),
        (
            exec::ExecOp::Range {
                range: ir::StreamRangePlan::new(0_usize.into(), 1_usize.into())
                    .expect("literal lifecycle range validates"),
            },
            "Query error: range cannot consume an index lifecycle value",
        ),
    ] {
        let error = db
            .execute(
                &lifecycle_plan(consumer, false),
                context::ParamBindings::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), expected);
    }
    let error = db
        .execute(
            &lifecycle_plan(exec::ExecOp::Noop, true),
            context::ParamBindings::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Query error: cannot concatenate index lifecycle dependency outputs"
    );
    let lifecycle_value = db
        .execute(
            &lifecycle_plan(exec::ExecOp::Noop, false),
            context::ParamBindings::default(),
        )
        .await
        .expect("already-active lifecycle value is observable")
        .last
        .expect("already-active lifecycle plan returns its receipt");
    let error = db
        .execute(
            &lifecycle_plan(
                exec::ExecOp::Filter {
                    predicate: ir::PredicatePlan::new(Predicate::has_key("category"))
                        .expect("lifecycle filter predicate validates"),
                },
                false,
            ),
            context::ParamBindings::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        format!("Query error: filter expected stream input, got {lifecycle_value:?}")
    );

    let indexed_read = || {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "group",
                    traversal::g()
                        .n_with_label_where("Document", Predicate::eq("category", "group"))
                        .id(),
                )
                .var_as(
                    "code_b",
                    traversal::g()
                        .n_with_label_where("Document", Predicate::eq("code", "B"))
                        .id(),
                )
                .var_as(
                    "primary",
                    traversal::g()
                        .e_with_label_where("LINK", Predicate::eq("kind", "primary"))
                        .id(),
                )
                .var_as(
                    "heavy",
                    traversal::g()
                        .e_with_label_where("LINK", Predicate::gte("weight", 2_i64))
                        .id(),
                )
                .returning(["group", "code_b", "primary", "heavy"]),
        )
    };
    assert_eq!(
        db.query(indexed_read()).await.unwrap(),
        serde_json::json!({
            "group": [0, 1],
            "code_b": [1],
            "primary": [0, 1],
            "heavy": [2, 1],
        })
    );

    let secondary_id_plan = |access: exec::ExecAccessPlan| {
        let access_id = exec::ExecStepId::new(1).expect("access step ID is positive");
        let project_id = exec::ExecStepId::new(2).expect("project step ID is positive");
        exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::try_from_vec(vec![
                exec::ExecStep {
                    id: access_id,
                    dependencies: Vec::new(),
                    output: ir::BatchOutputPlan::Discard,
                    semantic_return_shape: None,
                    condition: exec::ExecCondition::Always,
                    op: exec::ExecOp::Access {
                        plan: Box::new(access),
                    },
                    schedule: exec::ExecSchedule::Pipeline,
                    delivered: properties::DeliveredProperties::default(),
                    cost: cost::CostVector::ZERO,
                },
                exec::ExecStep {
                    id: project_id,
                    dependencies: vec![access_id],
                    output: ir::BatchOutputPlan::Discard,
                    semantic_return_shape: None,
                    condition: exec::ExecCondition::Always,
                    op: exec::ExecOp::Project {
                        projection: ir::ProjectionPlan::Id,
                    },
                    schedule: exec::ExecSchedule::Pipeline,
                    delivered: properties::DeliveredProperties::default(),
                    cost: cost::CostVector::ZERO,
                },
            ])
            .expect("secondary-set execution plan has two steps"),
            project_id,
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("secondary-set execution plan validates")
    };
    let node_equality = |property: &str, values: &[&str]| {
        let index = catalog::NodeEqualityIndexMeta::try_new(format!("node_eq:Document:{property}"))
            .expect("logical node equality index name is non-empty")
            .with_uniqueness(if property == "code" {
                catalog::IndexUniqueness::Unique
            } else {
                catalog::IndexUniqueness::NonUnique
            });
        exec::ExecNodeSecondarySetPlan::exact_equalities(
            index,
            catalog::ScopedPropertyKey::try_new("Document", property)
                .expect("logical node equality key validates"),
            ir::AtLeast::try_from_vec(
                values
                    .iter()
                    .map(|value| {
                        ir::IndexValue::Literal(
                            ir::SecondaryIndexLiteral::new(PropertyValue::from(*value))
                                .expect("string equality value is index-compatible"),
                        )
                    })
                    .collect(),
            )
            .expect("node equality set has at least one value"),
        )
    };
    let edge_equality = |property: &str, values: &[&str]| {
        exec::ExecEdgeSecondarySetPlan::exact_equalities(
            catalog::EdgeEqualityIndexMeta::try_new(format!("edge_eq:LINK:{property}"))
                .expect("logical edge equality index name is non-empty"),
            catalog::ScopedPropertyKey::try_new("LINK", property)
                .expect("logical edge equality key validates"),
            ir::AtLeast::try_from_vec(
                values
                    .iter()
                    .map(|value| {
                        ir::IndexValue::Literal(
                            ir::SecondaryIndexLiteral::new(PropertyValue::from(*value))
                                .expect("string equality value is index-compatible"),
                        )
                    })
                    .collect(),
            )
            .expect("edge equality set has at least one value"),
        )
    };
    let node_range = || exec::ExecNodeSecondaryRangePlan {
        index: catalog::NodeRangeIndexMeta::try_new("node_range:Document:rank:asc")
            .expect("logical node range index name is non-empty"),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "Document",
            "rank",
            index::RangeIndexDirection::Asc,
        )
        .expect("logical node range key validates"),
        range: ir::IndexRange::All,
    };
    let edge_range = || exec::ExecEdgeSecondaryRangePlan {
        index: catalog::EdgeRangeIndexMeta::try_new("edge_range:LINK:weight:desc")
            .expect("logical edge range index name is non-empty"),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "LINK",
            "weight",
            index::RangeIndexDirection::Desc,
        )
        .expect("logical edge range key validates"),
        range: ir::IndexRange::All,
    };
    let node_ids = |ids: &[u64]| {
        ExecutionValue::Scalars(ids.iter().copied().map(ExecutionScalar::NodeId).collect())
    };
    let edge_ids = |ids: &[u64]| {
        ExecutionValue::Scalars(ids.iter().copied().map(ExecutionScalar::EdgeId).collect())
    };

    for (set, expected) in [
        (
            node_equality("category", &["group", "other"]),
            node_ids(&[0, 1, 2]),
        ),
        (
            exec::ExecNodeSecondarySetPlan::Union {
                driver: Box::new(node_equality("category", &["other"])),
                rest: ir::AtLeast::from_one(node_equality("code", &["B"])),
            },
            node_ids(&[1, 2]),
        ),
        (
            exec::ExecNodeSecondarySetPlan::Intersect {
                driver: Box::new(node_equality("category", &["group"])),
                rest: ir::AtLeast::from_one(node_equality("code", &["B"])),
            },
            node_ids(&[1]),
        ),
        (
            exec::ExecNodeSecondarySetPlan::Range(node_range()),
            node_ids(&[0, 1, 2]),
        ),
        (exec::ExecNodeSecondarySetPlan::Empty, node_ids(&[])),
    ] {
        let result = db
            .execute(
                &secondary_id_plan(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::SecondarySet { set },
                )),
                context::ParamBindings::default(),
            )
            .await
            .expect("node secondary-set execution succeeds");
        assert_eq!(result.last, Some(expected));
    }
    let ordered_nodes = exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
        set: exec::ExecNodeSecondarySetPlan::OrderedIntersect {
            driver: node_range(),
            filters: ir::AtLeast::from_one(node_equality("category", &["group"])),
        },
    })
    .limited(properties::PositiveUsize::new(1).expect("ordered node limit is positive"));
    assert_eq!(
        db.execute(
            &secondary_id_plan(ordered_nodes),
            context::ParamBindings::default(),
        )
        .await
        .expect("ordered node secondary intersection succeeds")
        .last,
        Some(node_ids(&[0]))
    );

    for (set, expected) in [
        (
            edge_equality("kind", &["primary", "secondary"]),
            edge_ids(&[0, 1, 2]),
        ),
        (
            exec::ExecEdgeSecondarySetPlan::Union {
                driver: Box::new(edge_equality("kind", &["primary"])),
                rest: ir::AtLeast::from_one(edge_equality("status", &["active"])),
            },
            edge_ids(&[0, 1, 2]),
        ),
        (
            exec::ExecEdgeSecondarySetPlan::Intersect {
                driver: Box::new(edge_equality("kind", &["primary"])),
                rest: ir::AtLeast::from_one(edge_equality("status", &["active"])),
            },
            edge_ids(&[0]),
        ),
        (
            exec::ExecEdgeSecondarySetPlan::Range(edge_range()),
            edge_ids(&[2, 1, 0]),
        ),
        (exec::ExecEdgeSecondarySetPlan::Empty, edge_ids(&[])),
    ] {
        let result = db
            .execute(
                &secondary_id_plan(exec::ExecAccessPlan::Edge(
                    exec::ExecEdgeAccessPlan::SecondarySet { set },
                )),
                context::ParamBindings::default(),
            )
            .await
            .expect("edge secondary-set execution succeeds");
        assert_eq!(result.last, Some(expected));
    }
    let ordered_edges = exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
        set: exec::ExecEdgeSecondarySetPlan::OrderedIntersect {
            driver: edge_range(),
            filters: ir::AtLeast::from_one(edge_equality("status", &["active"])),
        },
    })
    .limited(properties::PositiveUsize::new(2).expect("ordered edge limit is positive"));
    assert_eq!(
        db.execute(
            &secondary_id_plan(ordered_edges),
            context::ParamBindings::default(),
        )
        .await
        .expect("ordered edge secondary intersection succeeds")
        .last,
        Some(edge_ids(&[2, 0]))
    );

    let edge_range_bounds = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "lower_exclusive",
                traversal::g()
                    .e_with_label_where("LINK", Predicate::gt("weight", 1_i64))
                    .id(),
            )
            .var_as(
                "upper_inclusive",
                traversal::g()
                    .e_with_label_where("LINK", Predicate::lte("weight", 2_i64))
                    .id(),
            )
            .var_as(
                "upper_exclusive",
                traversal::g()
                    .e_with_label_where("LINK", Predicate::lt("weight", 3_i64))
                    .id(),
            )
            .var_as(
                "between",
                traversal::g()
                    .e_with_label_where("LINK", Predicate::between("weight", 1_i64, 2_i64))
                    .id(),
            )
            .var_as(
                "parameter_upper",
                traversal::g()
                    .e_with_label_where("LINK", Predicate::lte_param("weight", "maximum"))
                    .id(),
            )
            .var_as(
                "ordered_limit",
                traversal::g()
                    .e_with_label("LINK")
                    .order_by("weight", traversal::Order::Desc)
                    .limit(2_usize)
                    .id(),
            )
            .returning([
                "lower_exclusive",
                "upper_inclusive",
                "upper_exclusive",
                "between",
                "parameter_upper",
                "ordered_limit",
            ]),
    )
    .with_parameter_value("maximum", QueryValue::I64(2));
    assert_eq!(
        db.query(edge_range_bounds).await.unwrap(),
        serde_json::json!({
            "lower_exclusive": [2, 1],
            "upper_inclusive": [1, 0],
            "upper_exclusive": [1, 0],
            "between": [1, 0],
            "parameter_upper": [1, 0],
            "ordered_limit": [2, 1],
        })
    );

    let conflicting = batch::write_batch()
        .var_as(
            "moved",
            traversal::g()
                .n(NodeRef::id(0))
                .set_property("category", "moved"),
        )
        .var_as(
            "duplicate",
            traversal::g().n(NodeRef::id(1)).set_property("code", "A"),
        )
        .returning(Vec::<String>::new());
    let error = db
        .query(QueryRequest::write(conflicting))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .to_lowercase()
            .contains("unique constraint violated"),
        "{error}"
    );
    assert_eq!(
        db.query(indexed_read()).await.unwrap(),
        serde_json::json!({
            "group": [0, 1],
            "code_b": [1],
            "primary": [0, 1],
            "heavy": [2, 1],
        })
    );

    let read_your_writes = batch::write_batch()
        .var_as(
            "moved",
            traversal::g()
                .n(NodeRef::id(0))
                .set_property("category", "moved"),
        )
        .var_as(
            "moved_ids",
            traversal::g()
                .n_with_label_where("Document", Predicate::eq("category", "moved"))
                .id(),
        )
        .var_as(
            "retagged",
            traversal::g()
                .e(EdgeRef::id(1))
                .set_property("kind", "secondary"),
        )
        .var_as(
            "primary_ids",
            traversal::g()
                .e_with_label_where("LINK", Predicate::eq("kind", "primary"))
                .id(),
        )
        .var_as(
            "heavier",
            traversal::g()
                .e(EdgeRef::id(0))
                .set_property("weight", 4_i64),
        )
        .var_as(
            "heavy_ids",
            traversal::g()
                .e_with_label_where("LINK", Predicate::gte("weight", 2_i64))
                .id(),
        )
        .returning(["moved_ids", "primary_ids", "heavy_ids"]);
    assert_eq!(
        db.query(QueryRequest::write(read_your_writes))
            .await
            .unwrap(),
        serde_json::json!({
            "moved_ids": [0],
            "primary_ids": [0],
            "heavy_ids": [0, 2, 1],
        })
    );
    assert_eq!(
        db.query(indexed_read()).await.unwrap(),
        serde_json::json!({
            "group": [1],
            "code_b": [1],
            "primary": [0],
            "heavy": [0, 2, 1],
        })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "node_property_removed",
                traversal::g().n(NodeRef::id(1)).remove_property("category"),
            )
            .var_as(
                "edge_property_removed",
                traversal::g().e(EdgeRef::id(0)).remove_property("kind"),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query(indexed_read()).await.unwrap(),
        serde_json::json!({
            "group": [],
            "code_b": [1],
            "primary": [],
            "heavy": [0, 2, 1],
        })
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn ordered_multi_range_intersections_match_explicit_sort_prefixes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-ordered-multi-range-intersections".to_owned(),
    })
    .await
    .expect("ordered multi-range fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n(
                "Metric",
                vec![
                    ("age", PropertyInput::from(1_i64)),
                    ("score", PropertyInput::from(30_i64)),
                ],
            ),
        )
        .var_as(
            "second",
            traversal::g().add_n(
                "Metric",
                vec![
                    ("age", PropertyInput::from(2_i64)),
                    ("score", PropertyInput::from(10_i64)),
                ],
            ),
        )
        .var_as(
            "third",
            traversal::g().add_n(
                "Metric",
                vec![
                    ("age", PropertyInput::from(3_i64)),
                    ("score", PropertyInput::from(20_i64)),
                ],
            ),
        )
        .var_as(
            "first_edge",
            traversal::g().n(NodeRef::var("first")).add_e(
                "MEASURED",
                NodeRef::var("second"),
                vec![
                    ("age", PropertyInput::from(30_i64)),
                    ("score", PropertyInput::from(1_i64)),
                ],
            ),
        )
        .var_as(
            "second_edge",
            traversal::g().n(NodeRef::var("second")).add_e(
                "MEASURED",
                NodeRef::var("third"),
                vec![
                    ("age", PropertyInput::from(20_i64)),
                    ("score", PropertyInput::from(2_i64)),
                ],
            ),
        )
        .var_as(
            "third_edge",
            traversal::g().n(NodeRef::var("first")).add_e(
                "MEASURED",
                NodeRef::var("third"),
                vec![
                    ("age", PropertyInput::from(10_i64)),
                    ("score", PropertyInput::from(3_i64)),
                ],
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    for (spec, description) in [
        (
            index::IndexSpec::node_range("Metric", "age"),
            "ascending node age range index",
        ),
        (
            index::IndexSpec::node_range("Metric", "score"),
            "ascending node score range index",
        ),
        (
            index::IndexSpec::edge_range_desc("MEASURED", "age"),
            "descending edge age range index",
        ),
        (
            index::IndexSpec::edge_range_desc("MEASURED", "score"),
            "descending edge score range index",
        ),
    ] {
        let receipt = db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as("operation", traversal::g().create_index_if_not_exists(spec))
                    .returning(["operation"]),
            ))
            .await
            .unwrap();
        let operation_id = receipt["operation"]["operation_id"]
            .as_str()
            .expect("accepted range-index operation has an ID");
        await_index_operation_success(&db, operation_id, description).await;
    }

    let node_batch = batch::read_batch()
        .var_as(
            "result",
            traversal::g()
                .n_with_label_where(
                    "Metric",
                    Predicate::and(vec![
                        Predicate::gte("age", 0_i64),
                        Predicate::gte("score", 0_i64),
                    ]),
                )
                .order_by("score", traversal::Order::Asc)
                .limit(2_usize),
        )
        .returning(["result"]);
    let edge_batch = batch::read_batch()
        .var_as(
            "result",
            traversal::g()
                .e_with_label_where(
                    "MEASURED",
                    Predicate::and(vec![
                        Predicate::lte("age", 100_i64),
                        Predicate::lte("score", 100_i64),
                    ]),
                )
                .order_by("score", traversal::Order::Desc)
                .limit(2_usize),
        )
        .returning(["result"]);
    let mut indexed_context = db.planner_context(context::ParamBindings::default());
    indexed_context.stats = context::StatsSnapshot::default()
        .with_node_range_cardinality(
            catalog::ScopedPropertyDirectionKey::try_new(
                "Metric",
                "age",
                index::RangeIndexDirection::Asc,
            )
            .unwrap(),
            1,
        )
        .with_node_range_cardinality(
            catalog::ScopedPropertyDirectionKey::try_new(
                "Metric",
                "score",
                index::RangeIndexDirection::Asc,
            )
            .unwrap(),
            20,
        )
        .with_edge_range_cardinality(
            catalog::ScopedPropertyDirectionKey::try_new(
                "MEASURED",
                "age",
                index::RangeIndexDirection::Desc,
            )
            .unwrap(),
            2,
        )
        .with_edge_range_cardinality(
            catalog::ScopedPropertyDirectionKey::try_new(
                "MEASURED",
                "score",
                index::RangeIndexDirection::Desc,
            )
            .unwrap(),
            20,
        );

    for (read, expected_driver, expected) in [
        (
            &node_batch,
            properties::ElementKind::Node,
            vec![ElementRef::Node(1), ElementRef::Node(2)],
        ),
        (
            &edge_batch,
            properties::ElementKind::Edge,
            vec![ElementRef::Edge(2), ElementRef::Edge(1)],
        ),
    ] {
        let indexed = planning::plan_read_batch(read, &indexed_context)
            .expect("indexed ordered multi-range query plans");
        let scan = planning::plan_read_batch(read, &context::PlannerContext::default())
            .expect("full-scan ordered multi-range query plans");
        let indexed_access = indexed
            .steps()
            .iter()
            .find_map(|step| match &step.op {
                exec::ExecOp::Access { plan } => Some(match plan.as_ref() {
                    exec::ExecAccessPlan::Limited(limited) => limited.source(),
                    access @ (exec::ExecAccessPlan::Node(_) | exec::ExecAccessPlan::Edge(_)) => {
                        access
                    }
                }),
                exec::ExecOp::Count { .. }
                | exec::ExecOp::KvRead(_)
                | exec::ExecOp::Expand { .. }
                | exec::ExecOp::VectorSearch { .. }
                | exec::ExecOp::TextSearch { .. }
                | exec::ExecOp::Filter { .. }
                | exec::ExecOp::Limit { .. }
                | exec::ExecOp::Skip { .. }
                | exec::ExecOp::Range { .. }
                | exec::ExecOp::Distinct
                | exec::ExecOp::Order { .. }
                | exec::ExecOp::Project { .. }
                | exec::ExecOp::Aggregate { .. }
                | exec::ExecOp::Variable { .. }
                | exec::ExecOp::Branch { .. }
                | exec::ExecOp::Repeat { .. }
                | exec::ExecOp::ShortestPath { .. }
                | exec::ExecOp::Mutation { .. }
                | exec::ExecOp::IndexDdl { .. }
                | exec::ExecOp::Merge { .. }
                | exec::ExecOp::Reserved { .. }
                | exec::ExecOp::ForEach { .. }
                | exec::ExecOp::Barrier { .. }
                | exec::ExecOp::Noop => None,
            })
            .expect("indexed plan has an access step");
        match (expected_driver, indexed_access) {
            (
                properties::ElementKind::Node,
                exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::SecondarySet {
                    set: exec::ExecNodeSecondarySetPlan::OrderedIntersect { driver, .. },
                }),
            ) => assert_eq!(
                driver.key.property.as_ref(),
                "score",
                "unexpected indexed node plan: {:?}; trace: {:?}",
                indexed.steps(),
                indexed.trace()
            ),
            (
                properties::ElementKind::Edge,
                exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::SecondarySet {
                    set: exec::ExecEdgeSecondarySetPlan::OrderedIntersect { driver, .. },
                }),
            ) => assert_eq!(
                driver.key.property.as_ref(),
                "score",
                "unexpected indexed edge plan: {:?}; trace: {:?}",
                indexed.steps(),
                indexed.trace()
            ),
            (_, access) => panic!("expected ordered secondary intersection, got {access:?}"),
        }
        assert!(
            indexed
                .steps()
                .iter()
                .all(|step| !matches!(step.op, exec::ExecOp::Order { .. })),
            "indexed plan should elide explicit sorting: {:?}",
            indexed.steps()
        );
        assert!(
            scan.steps()
                .iter()
                .any(|step| matches!(step.op, exec::ExecOp::Order { .. })),
            "full-scan reference should sort explicitly: {:?}",
            scan.steps()
        );

        let indexed_result = db
            .execute(&indexed, context::ParamBindings::default())
            .await
            .expect("indexed ordered multi-range query executes");
        let scan_result = db
            .execute(&scan, context::ParamBindings::default())
            .await
            .expect("full-scan ordered multi-range query executes");
        assert_eq!(indexed_result.last, scan_result.last);
        assert_eq!(indexed_result.returns, scan_result.returns);
        let Some(ExecutionValue::Stream(rows)) = indexed_result.last else {
            panic!("ordered multi-range query should return a stream");
        };
        let actual = rows
            .into_iter()
            .map(|row| row.current.expect("result row has a current element"))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_dynamic_bounds_and_parameter_errors() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-dynamic-bounds".to_owned(),
    })
    .await
    .expect("production dynamic-bound fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "alpha",
            traversal::g().add_n(
                "Item",
                vec![
                    ("name", PropertyInput::from("alpha")),
                    ("rank", PropertyInput::from(1_i64)),
                ],
            ),
        )
        .var_as(
            "beta",
            traversal::g().add_n(
                "Item",
                vec![
                    ("name", PropertyInput::from("beta")),
                    ("rank", PropertyInput::from(2_i64)),
                ],
            ),
        )
        .var_as(
            "gamma",
            traversal::g().add_n(
                "Item",
                vec![
                    ("name", PropertyInput::from("gamma")),
                    ("rank", PropertyInput::from(3_i64)),
                ],
            ),
        )
        .var_as(
            "delta",
            traversal::g().add_n(
                "Item",
                vec![
                    ("name", PropertyInput::from("delta")),
                    ("rank", PropertyInput::from(4_i64)),
                ],
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let dynamic = batch::read_batch()
        .var_as(
            "window",
            traversal::g()
                .n(NodeRef::param("ids"))
                .order_by("rank", traversal::Order::Desc)
                .skip(StreamBound::expr(Expr::param("offset")))
                .limit(StreamBound::expr(Expr::param("limit")))
                .id(),
        )
        .var_as(
            "range",
            traversal::g()
                .n(NodeRef::all())
                .order_by("rank", traversal::Order::Asc)
                .range(
                    StreamBound::expr(Expr::param("start")),
                    StreamBound::expr(Expr::param("end")),
                )
                .id(),
        )
        .var_as(
            "filtered",
            traversal::g()
                .n_with_label("Item")
                .where_(Predicate::and(vec![
                    Predicate::gte_param("rank", "minimum"),
                    Predicate::contains_param("name", "needle"),
                    Predicate::is_in_param("rank", "allowed"),
                ]))
                .id(),
        )
        .returning(["window", "range", "filtered"]);
    let dynamic = QueryRequest::read(dynamic)
        .with_parameter_value(
            "ids",
            QueryValue::Array(vec![
                QueryValue::I64(0),
                QueryValue::I64(1),
                QueryValue::I64(2),
                QueryValue::I64(3),
            ]),
        )
        .with_parameter_value("offset", QueryValue::I64(1))
        .with_parameter_value("limit", QueryValue::I64(2))
        .with_parameter_value("start", QueryValue::I64(1))
        .with_parameter_value("end", QueryValue::I64(3))
        .with_parameter_value("minimum", QueryValue::I64(3))
        .with_parameter_value("needle", QueryValue::String("a".to_owned()))
        .with_parameter_value(
            "allowed",
            QueryValue::Array(vec![QueryValue::I64(3), QueryValue::I64(4)]),
        );
    assert_eq!(
        db.query(dynamic).await.unwrap(),
        serde_json::json!({
            "window": [2, 1],
            "range": [1, 2],
            "filtered": [2, 3],
        })
    );

    let mutation = batch::write_batch()
        .var_as(
            "adjusted",
            traversal::g().n(NodeRef::param("mutated")).set_property(
                "adjusted",
                Expr::prop("rank").add_expr(Expr::param("delta")),
            ),
        )
        .returning(Vec::<String>::new());
    let mutation = QueryRequest::write(mutation)
        .with_parameter_value(
            "mutated",
            QueryValue::Array(vec![QueryValue::I64(0), QueryValue::I64(1)]),
        )
        .with_parameter_value("delta", QueryValue::I64(10));
    db.query(mutation).await.unwrap();
    assert_eq!(
        db.query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "adjusted",
                    traversal::g()
                        .n(NodeRef::ids([0, 1]))
                        .values(vec!["adjusted"]),
                )
                .returning(["adjusted"]),
        ))
        .await
        .unwrap(),
        serde_json::json!({ "adjusted": [{ "adjusted": 11.0 }, { "adjusted": 12.0 }] })
    );

    let invalid_ids = QueryRequest::read(
        batch::read_batch()
            .var_as("ids", traversal::g().n(NodeRef::param("ids")).id())
            .returning(["ids"]),
    )
    .with_parameter_value("ids", QueryValue::I64(-1));
    let error = db.query(invalid_ids).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("must be an integer id or array of integer ids"),
        "{error}"
    );

    let invalid_bound = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "ids",
                traversal::g()
                    .n(NodeRef::all())
                    .limit(StreamBound::expr(Expr::param("limit")))
                    .id(),
            )
            .returning(["ids"]),
    )
    .with_parameter_value("limit", QueryValue::I64(-1));
    let error = db.query(invalid_bound).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("stream bound expression returned -1"),
        "{error}"
    );

    let invalid_bound_type = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "ids",
                traversal::g()
                    .n(NodeRef::all())
                    .limit(StreamBound::expr(Expr::param("limit")))
                    .id(),
            )
            .returning(["ids"]),
    )
    .with_parameter_value("limit", QueryValue::String("many".to_owned()));
    let error = db.query(invalid_bound_type).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "Query error: parameter `limit` is not an i64"
    );

    let unsupported_bound = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "ids",
                traversal::g()
                    .n(NodeRef::all())
                    .limit(StreamBound::expr(Expr::prop("rank")))
                    .id(),
            )
            .returning(["ids"]),
    );
    let error = db.query(unsupported_bound).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("unsupported stream bound expression Property(\"rank\")"),
        "{error}"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_directional_paths_and_repeat_modes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-path-repeat".to_owned(),
    })
    .await
    .expect("production path-repeat fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "alice",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("alice")),
                    ("rank", PropertyInput::from(1_i64)),
                ],
            ),
        )
        .var_as(
            "bob",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("bob")),
                    ("rank", PropertyInput::from(2_i64)),
                ],
            ),
        )
        .var_as(
            "carol",
            traversal::g().add_n(
                "Person",
                vec![
                    ("name", PropertyInput::from("carol")),
                    ("rank", PropertyInput::from(3_i64)),
                ],
            ),
        )
        .var_as(
            "alice_to_bob",
            traversal::g().n(NodeRef::var("alice")).add_e(
                "KNOWS",
                NodeRef::var("bob"),
                vec![("weight", PropertyInput::from(1_i64))],
            ),
        )
        .var_as(
            "bob_to_carol",
            traversal::g().n(NodeRef::var("bob")).add_e(
                "KNOWS",
                NodeRef::var("carol"),
                vec![("weight", PropertyInput::from(2_i64))],
            ),
        )
        .var_as(
            "carol_to_alice",
            traversal::g().n(NodeRef::var("carol")).add_e(
                "KNOWS",
                NodeRef::var("alice"),
                vec![("weight", PropertyInput::from(3_i64))],
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let read = batch::read_batch()
        .var_as("seed", traversal::g().n(NodeRef::ids([0, 2])))
        .var_as(
            "both_nodes",
            traversal::g().n(NodeRef::id(1)).both(Some("KNOWS")).id(),
        )
        .var_as(
            "both_edges",
            traversal::g()
                .n(NodeRef::id(1))
                .both_e(Some("KNOWS"))
                .edge_has_label("KNOWS")
                .edge_has("weight", 2_i64)
                .id(),
        )
        .var_as(
            "incoming_source",
            traversal::g()
                .n(NodeRef::id(1))
                .in_e(Some("KNOWS"))
                .in_n()
                .id(),
        )
        .var_as(
            "other_nodes",
            traversal::g()
                .n(NodeRef::id(1))
                .both_e(Some("KNOWS"))
                .other_n()
                .id(),
        )
        .var_as(
            "visible_path",
            traversal::g()
                .n(NodeRef::id(0))
                .out(Some("KNOWS"))
                .out(Some("KNOWS"))
                .path(),
        )
        .var_as(
            "simple_cycle_count",
            traversal::g()
                .n(NodeRef::id(0))
                .out(Some("KNOWS"))
                .out(Some("KNOWS"))
                .out(Some("KNOWS"))
                .simple_path()
                .count(),
        )
        .var_as(
            "selected_origin",
            traversal::g()
                .n(NodeRef::id(0))
                .as_("origin")
                .out(Some("KNOWS"))
                .select("origin")
                .id(),
        )
        .var_as(
            "stored_origin",
            traversal::g()
                .n(NodeRef::id(0))
                .store("stored")
                .out(Some("KNOWS"))
                .select("stored")
                .id(),
        )
        .var_as("injected", traversal::g().inject("seed").id())
        .var_as(
            "appended",
            traversal::g().n(NodeRef::id(1)).inject("seed").id(),
        )
        .var_as(
            "repeat_before",
            traversal::g()
                .n(NodeRef::id(0))
                .repeat(
                    traversal::RepeatConfig::new(traversal::sub().out(Some("KNOWS")))
                        .times(1)
                        .emit_before(),
                )
                .id(),
        )
        .var_as(
            "repeat_after",
            traversal::g()
                .n(NodeRef::id(0))
                .repeat(
                    traversal::RepeatConfig::new(traversal::sub().out(Some("KNOWS")))
                        .times(2)
                        .emit_after(),
                )
                .id(),
        )
        .var_as(
            "repeat_all",
            traversal::g()
                .n(NodeRef::id(0))
                .repeat(
                    traversal::RepeatConfig::new(traversal::sub().out(Some("KNOWS")))
                        .times(1)
                        .emit_all(),
                )
                .id(),
        )
        .var_as(
            "repeat_if",
            traversal::g()
                .n(NodeRef::id(0))
                .repeat(
                    traversal::RepeatConfig::new(traversal::sub().out(Some("KNOWS")))
                        .times(2)
                        .emit_if(Predicate::eq("name", "carol")),
                )
                .id(),
        )
        .var_as(
            "repeat_until",
            traversal::g()
                .n(NodeRef::id(0))
                .repeat(
                    traversal::RepeatConfig::new(traversal::sub().out(Some("KNOWS")))
                        .until(Predicate::eq("name", "carol"))
                        .max_depth(3),
                )
                .id(),
        )
        .returning([
            "both_nodes",
            "both_edges",
            "incoming_source",
            "other_nodes",
            "visible_path",
            "simple_cycle_count",
            "selected_origin",
            "stored_origin",
            "injected",
            "appended",
            "repeat_before",
            "repeat_after",
            "repeat_all",
            "repeat_if",
            "repeat_until",
        ]);
    assert_eq!(
        db.query(QueryRequest::read(read)).await.unwrap(),
        serde_json::json!({
            "both_nodes": [0, 2],
            "both_edges": [1],
            "incoming_source": [0],
            "other_nodes": [0, 2],
            "visible_path": [{
                "current": { "node": 2 },
                "bindings": {},
                "path": [{ "node": 0 }, { "node": 1 }, { "node": 2 }],
            }],
            "simple_cycle_count": 0,
            "selected_origin": [0],
            "stored_origin": [0],
            "injected": [0, 2],
            "appended": [1, 0, 2],
            "repeat_before": [0],
            "repeat_after": [1, 2],
            "repeat_all": [0, 1],
            "repeat_if": [2],
            "repeat_until": [2],
        })
    );

    for config in [
        traversal::RepeatConfig::new(traversal::sub().out(Some("KNOWS")))
            .times(1)
            .emit_if(Predicate::contains_param("name", "missing")),
        traversal::RepeatConfig::new(traversal::sub().out(Some("KNOWS")))
            .until(Predicate::contains_param("name", "missing"))
            .max_depth(1),
    ] {
        let error = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "invalid_repeat",
                        traversal::g().n(NodeRef::id(0)).repeat(config),
                    )
                    .returning(["invalid_repeat"]),
            ))
            .await
            .expect_err("missing repeat predicate parameter is rejected");
        assert_eq!(
            error.to_string(),
            "Query error: parameter `missing` is not bound"
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_covers_sack_state_and_numeric_errors() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-sack".to_owned(),
    })
    .await
    .expect("production sack fixture opens");
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "integer",
                traversal::g().add_n("Metric", vec![("score", PropertyInput::from(2_i64))]),
            )
            .var_as(
                "float",
                traversal::g().add_n("Metric", vec![("score", PropertyInput::from(0.5_f64))]),
            )
            .var_as(
                "missing",
                traversal::g().add_n("Metric", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "text",
                traversal::g().add_n(
                    "Metric",
                    vec![("score", PropertyInput::from("not numeric"))],
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();

    let success = batch::read_batch()
        .var_as(
            "integer_add",
            traversal::g()
                .n(NodeRef::id(0))
                .with_sack(PropertyValue::from(1_i64))
                .sack_add("score")
                .sack_get(),
        )
        .var_as(
            "mixed_add",
            traversal::g()
                .n(NodeRef::id(1))
                .with_sack(PropertyValue::from(1_i64))
                .sack_add("score")
                .sack_get(),
        )
        .var_as(
            "set",
            traversal::g()
                .n(NodeRef::id(0))
                .sack_set("score")
                .sack_get(),
        )
        .var_as(
            "initialize_from_property",
            traversal::g()
                .n(NodeRef::id(0))
                .sack_add("score")
                .sack_get(),
        )
        .var_as(
            "clear_and_ignore_missing",
            traversal::g()
                .n(NodeRef::id(2))
                .with_sack(PropertyValue::from(1_i64))
                .sack_set("score")
                .sack_add("score")
                .sack_get(),
        )
        .returning([
            "integer_add",
            "mixed_add",
            "set",
            "initialize_from_property",
            "clear_and_ignore_missing",
        ]);
    assert_eq!(
        db.query(QueryRequest::read(success)).await.unwrap(),
        serde_json::json!({
            "integer_add": [{
                "current": { "node": 0 }, "bindings": {}, "sack": 3,
            }],
            "mixed_add": [{
                "current": { "node": 1 }, "bindings": {}, "sack": 1.5,
            }],
            "set": [{
                "current": { "node": 0 }, "bindings": {}, "sack": 2,
            }],
            "initialize_from_property": [{
                "current": { "node": 0 }, "bindings": {}, "sack": 2,
            }],
            "clear_and_ignore_missing": [{
                "current": { "node": 2 }, "bindings": {}, "sack": null,
            }],
        })
    );

    let overflow = batch::read_batch()
        .var_as(
            "value",
            traversal::g()
                .n(NodeRef::id(0))
                .with_sack(PropertyValue::from(i64::MAX))
                .sack_add("score")
                .sack_get(),
        )
        .returning(["value"]);
    let error = db.query(QueryRequest::read(overflow)).await.unwrap_err();
    assert!(error.to_string().contains("overflowed i64"), "{error}");

    let non_numeric_current = batch::read_batch()
        .var_as(
            "value",
            traversal::g()
                .n(NodeRef::id(0))
                .with_sack(PropertyValue::from("not numeric"))
                .sack_add("score")
                .sack_get(),
        )
        .returning(["value"]);
    let error = db
        .query(QueryRequest::read(non_numeric_current))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("numeric current sack"),
        "{error}"
    );

    let non_numeric_property = batch::read_batch()
        .var_as(
            "value",
            traversal::g()
                .n(NodeRef::id(3))
                .with_sack(PropertyValue::from(1_i64))
                .sack_add("score")
                .sack_get(),
        )
        .returning(["value"]);
    let error = db
        .query(QueryRequest::read(non_numeric_property))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("numeric property"), "{error}");
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_rejects_folded_stream_consumers() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-folded-stream-errors".to_owned(),
    })
    .await
    .expect("production folded-stream fixture opens");
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "node",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();

    for (operation, traversal) in [
        (
            "limit",
            traversal::g().n(NodeRef::all()).fold().limit(1_usize).id(),
        ),
        (
            "skip",
            traversal::g().n(NodeRef::all()).fold().skip(1_usize).id(),
        ),
        (
            "range",
            traversal::g()
                .n(NodeRef::all())
                .fold()
                .range(0_usize, 1_usize)
                .id(),
        ),
        (
            "distinct",
            traversal::g().n(NodeRef::all()).fold().dedup().id(),
        ),
        ("project", traversal::g().n(NodeRef::all()).fold().id()),
        (
            "filter",
            traversal::g()
                .n(NodeRef::all())
                .fold()
                .where_(Predicate::has_key("name"))
                .id(),
        ),
    ] {
        let request = QueryRequest::read(
            batch::read_batch()
                .var_as("result", traversal)
                .returning(["result"]),
        );
        let error = db.query(request).await.unwrap_err();
        assert!(
            error.to_string().contains(&format!(
                "{operation} expected stream input, got folded stream; use unfold first"
            )),
            "{error}"
        );
    }
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_reader_executes_parallel_stage_with_stable_output_order() {
    let token = ProcessLocalDatabaseToken::new("production-interpreter-parallel-stage")
        .expect("process-local database token validates");
    let writer = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("parallel-stage writer opens");
    writer
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "seed",
                    traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
                )
                .returning(Vec::<String>::new()),
        ))
        .await
        .expect("parallel-stage fixture is committed");
    writer.close().await.expect("parallel-stage writer closes");

    let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
        .await
        .expect("parallel-stage reader opens");
    let first_id = exec::ExecStepId::new(1).expect("first step ID is positive");
    let second_id = exec::ExecStepId::new(2).expect("second step ID is positive");
    let root_id = exec::ExecStepId::new(3).expect("root step ID is positive");
    let seen = ir::NonEmptyString::new("seen").expect("output binding is non-empty");
    let first = exec::ExecStep {
        id: first_id,
        dependencies: Vec::new(),
        output: ir::BatchOutputPlan::Bind(seen.clone()),
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op: exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty)),
        },
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    };
    let second = exec::ExecStep {
        id: second_id,
        dependencies: Vec::new(),
        output: ir::BatchOutputPlan::Bind(seen.clone()),
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op: exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::AllScan,
            )),
        },
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    };
    let root = exec::ExecStep {
        id: root_id,
        dependencies: vec![first_id, second_id],
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op: exec::ExecOp::Variable {
            op: exec::ExecVariableOp::SourceInject {
                variable: seen.clone(),
            },
        },
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    };
    let plan = exec::ExecutablePlan::new(
        ir::PlanKind::Read,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::from_one_and_rest(first, vec![second, root]),
        root_id,
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("parallel read plan validates");
    let order = plan.execution_order();
    let exec::ExecExecutionStage::Parallel(stage) = &order.stages()[0] else {
        panic!("the two independent access steps must form a parallel stage")
    };
    assert_eq!(stage.max_concurrency().get(), 2);

    let result = reader
        .execute(&plan, context::ParamBindings::default())
        .await
        .expect("public reader executes the parallel stage");
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("root variable injection must expose the stage-order-winning stream")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].current, Some(ElementRef::Node(0)));
    reader.close().await.expect("parallel-stage reader closes");
}

#[tokio::test]
async fn public_executable_plan_boundary_covers_typed_kv_read_modes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-typed-kv-reads".to_owned(),
    })
    .await
    .expect("production typed-KV fixture opens");
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "first",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "second",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "third",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "first_edge",
                traversal::g().n(NodeRef::var("first")).add_e(
                    "LINK",
                    NodeRef::var("second"),
                    Vec::<(&str, PropertyInput)>::new(),
                ),
            )
            .var_as(
                "second_edge",
                traversal::g().n(NodeRef::var("second")).add_e(
                    "LINK",
                    NodeRef::var("third"),
                    Vec::<(&str, PropertyInput)>::new(),
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .expect("typed-KV fixture is committed");

    let node_keyspace = exec::ElementKeyspace::NodeProperty;
    let edge_keyspace = exec::ElementKeyspace::EdgeEndpoints;
    let cases = [
        (
            exec::KvReadPlan::Get {
                key: node_keyspace.point_key(1),
            },
            vec![ElementRef::Node(1)],
        ),
        (
            exec::KvReadPlan::Get {
                key: node_keyspace.point_key(99),
            },
            Vec::new(),
        ),
        (
            exec::KvReadPlan::Get {
                key: edge_keyspace.point_key(0),
            },
            vec![ElementRef::Edge(0)],
        ),
        (
            exec::KvReadPlan::MultiGet(
                exec::KvMultiGetPlan::new(
                    vec![
                        node_keyspace.point_key(2),
                        node_keyspace.point_key(99),
                        node_keyspace.point_key(0),
                    ],
                    properties::KeyLocality::Close,
                    properties::PositiveUsize::new(3).expect("batch limit is positive"),
                )
                .expect("single-keyspace multiget validates"),
            ),
            vec![ElementRef::Node(2), ElementRef::Node(0)],
        ),
        (
            exec::KvReadPlan::RangeScan {
                keyspace: node_keyspace,
                start: exec::KvKeyBound::included_id(1),
                end: exec::KvKeyBound::excluded_id(3),
                limit: None,
            },
            vec![ElementRef::Node(1), ElementRef::Node(2)],
        ),
        (
            exec::KvReadPlan::RangeScan {
                keyspace: node_keyspace,
                start: exec::KvKeyBound::excluded_id(0),
                end: exec::KvKeyBound::included_id(1),
                limit: None,
            },
            vec![ElementRef::Node(1)],
        ),
        (
            exec::KvReadPlan::RangeScan {
                keyspace: edge_keyspace,
                start: exec::KvKeyBound::Unbounded,
                end: exec::KvKeyBound::Unbounded,
                limit: Some(properties::PositiveUsize::new(1).expect("limit is positive")),
            },
            vec![ElementRef::Edge(0)],
        ),
        (
            exec::KvReadPlan::PrefixScan {
                keyspace: node_keyspace,
                prefix: ir::AtLeast::from_one_and_rest(0, Vec::new()),
                limit: Some(properties::PositiveUsize::new(2).expect("limit is positive")),
            },
            vec![ElementRef::Node(0), ElementRef::Node(1)],
        ),
        (
            exec::KvReadPlan::PrefixScan {
                keyspace: edge_keyspace,
                prefix: ir::AtLeast::from_one_and_rest(0, Vec::new()),
                limit: None,
            },
            vec![ElementRef::Edge(0), ElementRef::Edge(1)],
        ),
    ];

    for (index, (read, expected)) in cases.into_iter().enumerate() {
        let root_id = exec::ExecStepId::new(1).expect("root step ID is positive");
        let plan = exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::from_one_and_rest(
                exec::ExecStep {
                    id: root_id,
                    dependencies: Vec::new(),
                    output: ir::BatchOutputPlan::Discard,
                    semantic_return_shape: None,
                    condition: exec::ExecCondition::Always,
                    op: exec::ExecOp::KvRead(read),
                    schedule: exec::ExecSchedule::Pipeline,
                    delivered: properties::DeliveredProperties::default(),
                    cost: cost::CostVector::ZERO,
                },
                Vec::new(),
            ),
            root_id,
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("single-step typed-KV plan validates");
        let result = db
            .execute(&plan, context::ParamBindings::default())
            .await
            .unwrap_or_else(|error| panic!("typed-KV case {index} failed: {error}"));
        let Some(ExecutionValue::Stream(rows)) = result.last else {
            panic!("typed-KV case {index} must return an element stream")
        };
        assert_eq!(
            rows.into_iter()
                .map(|row| row.current.expect("typed-KV rows have current elements"))
                .collect::<Vec<_>>(),
            expected,
            "typed-KV case {index}",
        );
    }

    db.close().await.expect("typed-KV fixture closes");
}

#[tokio::test]
async fn public_executable_plan_boundary_covers_scalar_stream_composition() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-scalar-plans".to_owned(),
    })
    .await
    .expect("production scalar-plan fixture opens");
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "first",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "second",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "link",
                traversal::g().n(NodeRef::id(0)).add_e(
                    "LINK",
                    NodeRef::id(1),
                    Vec::<(&str, PropertyInput)>::new(),
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();

    let access = || exec::ExecOp::Access {
        plan: Box::new(exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::AllScan,
        )),
    };
    let project = |projection| exec::ExecOp::Project { projection };
    let count_rows = || exec::ExecOp::Count {
        plan: Box::new(exec::ExecCountPlan::InputRows {
            window: exec::ExecCountWindowPlan::identity(),
        }),
    };
    let count_scalars = || exec::ExecOp::Count {
        plan: Box::new(exec::ExecCountPlan::InputScalars {
            window: exec::ExecCountWindowPlan::identity(),
        }),
    };
    let linear_plan = |operations: Vec<exec::ExecOp>| {
        let root = operations.len();
        let steps = operations
            .into_iter()
            .enumerate()
            .map(|(index, op)| {
                let id = index + 1;
                let schedule = if matches!(
                    &op,
                    exec::ExecOp::Aggregate { .. }
                        | exec::ExecOp::Reserved {
                            op: ir::ReservedOp::Fold,
                        }
                ) {
                    exec::ExecSchedule::Barrier
                } else {
                    exec::ExecSchedule::Pipeline
                };
                exec::ExecStep {
                    id: exec::ExecStepId::new(id).expect("step ID is positive"),
                    dependencies: (id > 1)
                        .then(|| exec::ExecStepId::new(id - 1).expect("dependency ID is positive"))
                        .into_iter()
                        .collect(),
                    output: ir::BatchOutputPlan::Discard,
                    semantic_return_shape: None,
                    condition: exec::ExecCondition::Always,
                    op,
                    schedule,
                    delivered: properties::DeliveredProperties::default(),
                    cost: cost::CostVector::ZERO,
                }
            })
            .collect::<Vec<_>>();
        exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::try_from_vec(steps).expect("plan is non-empty"),
            exec::ExecStepId::new(root).expect("root ID is positive"),
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("linear scalar plan validates")
    };

    let scalar_cases = [
        (
            vec![
                exec::ExecOp::Access {
                    plan: Box::new(
                        exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::AllScan).limited(
                            properties::PositiveUsize::new(1).expect("access limit is positive"),
                        ),
                    ),
                },
                project(ir::ProjectionPlan::Id),
            ],
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(0)]),
        ),
        (
            vec![
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::Empty)),
                },
                count_rows(),
            ],
            ExecutionValue::Count(0),
        ),
        (
            vec![
                exec::ExecOp::Access {
                    plan: Box::new(
                        exec::ExecAccessPlan::Edge(exec::ExecEdgeAccessPlan::AllScan).limited(
                            properties::PositiveUsize::new(1).expect("access limit is positive"),
                        ),
                    ),
                },
                project(ir::ProjectionPlan::Id),
            ],
            ExecutionValue::Scalars(vec![ExecutionScalar::EdgeId(0)]),
        ),
        (
            vec![
                access(),
                project(ir::ProjectionPlan::Id),
                exec::ExecOp::Limit {
                    count: ir::StreamBoundPlan::Literal(1),
                },
            ],
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(0)]),
        ),
        (
            vec![
                access(),
                project(ir::ProjectionPlan::Id),
                exec::ExecOp::Skip {
                    count: ir::StreamBoundPlan::Literal(1),
                },
            ],
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(1)]),
        ),
        (
            vec![
                access(),
                project(ir::ProjectionPlan::Id),
                exec::ExecOp::Range {
                    range: ir::StreamRangePlan::new(1_usize.into(), 2_usize.into())
                        .expect("literal scalar range validates"),
                },
            ],
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(1)]),
        ),
        (
            vec![
                access(),
                count_rows(),
                exec::ExecOp::Distinct,
                count_scalars(),
            ],
            ExecutionValue::Count(1),
        ),
        (
            vec![access(), project(ir::ProjectionPlan::Id), count_scalars()],
            ExecutionValue::Count(2),
        ),
        (
            vec![
                access(),
                count_rows(),
                exec::ExecOp::Aggregate {
                    aggregate: ir::AggregatePlan::AggregateBy {
                        function: traversal::AggregateFunction::Count,
                        property: ir::NonEmptyString::new("unused")
                            .expect("aggregate property is non-empty"),
                    },
                },
            ],
            ExecutionValue::Count(1),
        ),
        (
            vec![
                access(),
                project(ir::ProjectionPlan::Id),
                project(ir::ProjectionPlan::Exists),
            ],
            ExecutionValue::Bool(true),
        ),
        (
            vec![
                access(),
                count_rows(),
                exec::ExecOp::Limit {
                    count: ir::StreamBoundPlan::Literal(1),
                },
                count_scalars(),
            ],
            ExecutionValue::Count(1),
        ),
        (
            vec![
                access(),
                project(ir::ProjectionPlan::Exists),
                exec::ExecOp::Limit {
                    count: ir::StreamBoundPlan::Literal(1),
                },
                count_scalars(),
            ],
            ExecutionValue::Count(1),
        ),
        (
            vec![
                access(),
                project(ir::ProjectionPlan::Id),
                project(ir::ProjectionPlan::Id),
            ],
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(0), ExecutionScalar::NodeId(1)]),
        ),
        (
            vec![access(), count_rows(), project(ir::ProjectionPlan::Id)],
            ExecutionValue::Scalars(Vec::new()),
        ),
        (
            vec![access(), project(ir::ProjectionPlan::EdgeProperties)],
            ExecutionValue::Scalars(Vec::new()),
        ),
        (
            vec![
                access(),
                exec::ExecOp::Barrier {
                    name: ir::NonEmptyString::new("row_mode").expect("barrier name is non-empty"),
                },
                project(ir::ProjectionPlan::Id),
            ],
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(0), ExecutionScalar::NodeId(1)]),
        ),
        (
            vec![
                access(),
                exec::ExecOp::Reserved {
                    op: ir::ReservedOp::Unfold,
                },
                count_rows(),
            ],
            ExecutionValue::Count(2),
        ),
    ];
    for (operations, expected) in scalar_cases {
        let result = db
            .execute(&linear_plan(operations), context::ParamBindings::default())
            .await
            .expect("scalar plan executes")
            .last
            .expect("scalar plan returns its root value");
        assert_eq!(result, expected);
    }

    let static_limit =
        ir::NonEmptyString::new("static_limit").expect("parameter name is non-empty");
    let result = db
        .execute(
            &linear_plan(vec![
                access(),
                exec::ExecOp::Limit {
                    count: ir::StreamBoundPlan::Expr(
                        ir::StreamBoundExprPlan::new(Expr::param(static_limit.as_ref()))
                            .expect("static parameter bound validates"),
                    ),
                },
            ]),
            context::ParamBindings::default().with_value(static_limit, PropertyValue::I64(1)),
        )
        .await
        .expect("static AST-bound limit executes")
        .last
        .expect("static AST-bound limit returns its root value");
    let ExecutionValue::Stream(rows) = result else {
        panic!("static AST-bound limit must return a stream")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].current, Some(ElementRef::Node(0)));

    let invalid_projection = linear_plan(vec![
        access(),
        project(ir::ProjectionPlan::Id),
        project(ir::ProjectionPlan::Label),
    ]);
    let error = db
        .execute(&invalid_projection, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("expected element stream input, got scalar terminal input"),
        "{error}"
    );

    let invalid_unfold = linear_plan(vec![
        access(),
        count_rows(),
        exec::ExecOp::Reserved {
            op: ir::ReservedOp::Unfold,
        },
    ]);
    let error = db
        .execute(&invalid_unfold, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Query error: unfold expected stream or folded stream input, got Count(2)"
    );

    let scalar_group = linear_plan(vec![
        access(),
        count_rows(),
        exec::ExecOp::Aggregate {
            aggregate: ir::AggregatePlan::Group(
                ir::NonEmptyString::new("unused").expect("group property is non-empty"),
            ),
        },
    ]);
    let error = db
        .execute(&scalar_group, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("expected element stream input, got scalar terminal input"),
        "{error}"
    );

    let folded_aggregate = linear_plan(vec![
        access(),
        exec::ExecOp::Reserved {
            op: ir::ReservedOp::Fold,
        },
        exec::ExecOp::Aggregate {
            aggregate: ir::AggregatePlan::AggregateBy {
                function: traversal::AggregateFunction::Count,
                property: ir::NonEmptyString::new("unused")
                    .expect("aggregate property is non-empty"),
            },
        },
    ]);
    let error = db
        .execute(&folded_aggregate, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Query error: aggregate expected stream input, got folded stream; use unfold first"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_executable_plan_boundary_covers_merge_and_dependency_shapes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-merge-plans".to_owned(),
    })
    .await
    .expect("production merge-plan fixture opens");
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "first",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "second",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .var_as(
                "third",
                traversal::g().add_n("Node", Vec::<(&str, PropertyInput)>::new()),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();

    let first = ir::NonEmptyString::new("first").unwrap();
    let second = ir::NonEmptyString::new("second").unwrap();
    let third = ir::NonEmptyString::new("third").unwrap();
    let merge_plan = |mode, params: Vec<ir::NonEmptyString>| {
        let merge_id = params.len() + 1;
        let mut steps = params
            .into_iter()
            .enumerate()
            .map(|(index, param)| exec::ExecStep {
                id: exec::ExecStepId::new(index + 1).expect("access step ID is positive"),
                dependencies: Vec::new(),
                output: ir::BatchOutputPlan::Discard,
                semantic_return_shape: None,
                condition: exec::ExecCondition::Always,
                op: exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param },
                    )),
                },
                schedule: exec::ExecSchedule::Pipeline,
                delivered: properties::DeliveredProperties::default(),
                cost: cost::CostVector::ZERO,
            })
            .collect::<Vec<_>>();
        steps.push(exec::ExecStep {
            id: exec::ExecStepId::new(merge_id).expect("merge step ID is positive"),
            dependencies: (1..merge_id)
                .map(|id| exec::ExecStepId::new(id).expect("dependency ID is positive"))
                .collect(),
            output: ir::BatchOutputPlan::Discard,
            semantic_return_shape: None,
            condition: exec::ExecCondition::Always,
            op: exec::ExecOp::Merge { mode },
            schedule: exec::ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: cost::CostVector::ZERO,
        });
        exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::try_from_vec(steps).expect("merge plan is non-empty"),
            exec::ExecStepId::new(merge_id).expect("merge root ID is positive"),
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("merge plan validates")
    };
    let params = context::ParamBindings::default()
        .with_value(first.clone(), PropertyValue::I64Array(vec![0, 1, 2]))
        .with_value(second.clone(), PropertyValue::I64Array(vec![1, 2]))
        .with_value(third.clone(), PropertyValue::I64Array(vec![1]));

    for (mode, inputs, expected) in [
        (
            exec::ExecMergeMode::Concat,
            vec![first.clone(), second.clone()],
            vec![0, 1, 2, 1, 2],
        ),
        (
            exec::ExecMergeMode::Union,
            vec![first.clone(), second.clone()],
            vec![0, 1, 2],
        ),
        (
            exec::ExecMergeMode::Intersect,
            vec![first.clone(), second.clone(), third.clone()],
            vec![1],
        ),
    ] {
        let value = db
            .execute(&merge_plan(mode, inputs), params.clone())
            .await
            .expect("merge plan executes")
            .last
            .expect("merge root returns a stream");
        let ExecutionValue::Stream(rows) = value else {
            panic!("merge root must preserve stream input")
        };
        let actual = rows
            .into_iter()
            .map(
                |row| match row.current.expect("merged row has a current element") {
                    ElementRef::Node(id) => id,
                    ElementRef::Edge(id) => panic!("expected merged node row, got edge {id}"),
                },
            )
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
    assert_eq!(
        db.execute(
            &merge_plan(exec::ExecMergeMode::Intersect, Vec::new()),
            params.clone(),
        )
        .await
        .expect("empty intersect plan executes")
        .last,
        Some(ExecutionValue::Stream(Vec::new()))
    );

    let dependency_plan = |operations: Vec<(exec::ExecOp, exec::ExecSchedule)>| {
        let root_id = operations.len() + 1;
        let mut steps = operations
            .into_iter()
            .enumerate()
            .map(|(index, (op, schedule))| exec::ExecStep {
                id: exec::ExecStepId::new(index + 1).expect("dependency step ID is positive"),
                dependencies: Vec::new(),
                output: ir::BatchOutputPlan::Discard,
                semantic_return_shape: None,
                condition: exec::ExecCondition::Always,
                op,
                schedule,
                delivered: properties::DeliveredProperties::default(),
                cost: cost::CostVector::ZERO,
            })
            .collect::<Vec<_>>();
        steps.push(exec::ExecStep {
            id: exec::ExecStepId::new(root_id).expect("dependency root ID is positive"),
            dependencies: (1..root_id)
                .map(|id| exec::ExecStepId::new(id).expect("dependency ID is positive"))
                .collect(),
            output: ir::BatchOutputPlan::Discard,
            semantic_return_shape: None,
            condition: exec::ExecCondition::Always,
            op: exec::ExecOp::Noop,
            schedule: exec::ExecSchedule::Pipeline,
            delivered: properties::DeliveredProperties::default(),
            cost: cost::CostVector::ZERO,
        });
        exec::ExecutablePlan::new(
            ir::PlanKind::Read,
            ir::ReturnPlan::None,
            ir::AtLeast::<_, 1>::try_from_vec(steps).expect("dependency plan is non-empty"),
            exec::ExecStepId::new(root_id).expect("dependency root ID is positive"),
            trace::PlanningTrace::default(),
            exec::PlannerMetrics::default(),
        )
        .expect("dependency plan validates")
    };
    let scalar_plan = dependency_plan(vec![
        (
            exec::ExecOp::Count {
                plan: Box::new(exec::ExecCountPlan::Constant(0)),
            },
            exec::ExecSchedule::Pipeline,
        ),
        (
            exec::ExecOp::Project {
                projection: ir::ProjectionPlan::Exists,
            },
            exec::ExecSchedule::Pipeline,
        ),
        (
            exec::ExecOp::Project {
                projection: ir::ProjectionPlan::Id,
            },
            exec::ExecSchedule::Pipeline,
        ),
    ]);
    let Some(ExecutionValue::Scalars(scalars)) = db
        .execute(&scalar_plan, context::ParamBindings::default())
        .await
        .expect("scalar dependency outputs concatenate")
        .last
    else {
        panic!("scalar dependency merge must return scalar values")
    };
    assert_eq!(scalars.len(), 2);
    assert!(matches!(
        &scalars[0],
        ExecutionScalar::Value(value) if value.as_i64() == Some(0)
    ));
    assert!(matches!(
        &scalars[1],
        ExecutionScalar::Value(value) if value.as_bool() == Some(false)
    ));

    let mixed_plan = dependency_plan(vec![
        (
            exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty)),
            },
            exec::ExecSchedule::Pipeline,
        ),
        (
            exec::ExecOp::Count {
                plan: Box::new(exec::ExecCountPlan::Constant(0)),
            },
            exec::ExecSchedule::Pipeline,
        ),
    ]);
    let error = db
        .execute(&mixed_plan, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot concatenate mixed stream and scalar dependency outputs"),
        "{error}"
    );

    let folded_plan = dependency_plan(vec![
        (
            exec::ExecOp::Reserved {
                op: ir::ReservedOp::Fold,
            },
            exec::ExecSchedule::Barrier,
        ),
        (
            exec::ExecOp::Access {
                plan: Box::new(exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Empty)),
            },
            exec::ExecSchedule::Pipeline,
        ),
    ]);
    let error = db
        .execute(&folded_plan, context::ParamBindings::default())
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot concatenate folded stream dependency output; unfold it first"),
        "{error}"
    );
    db.close().await.unwrap();
}

#[test]
fn public_query_boundary_covers_active_vector_index_mutations() {
    run_high_stack_contract(
        "public-active-vector-index-mutations",
        public_query_boundary_covers_active_vector_index_mutations_contract,
    );
}

async fn public_query_boundary_covers_active_vector_index_mutations_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-vector-index".to_owned(),
    })
    .await
    .expect("production vector-index fixture opens");
    let fixture = batch::write_batch()
        .var_as(
            "first",
            traversal::g().add_n(
                "Document",
                vec![("embedding", PropertyInput::from(vec![1.0_f32, 0.0]))],
            ),
        )
        .var_as(
            "second",
            traversal::g().add_n(
                "Document",
                vec![("embedding", PropertyInput::from(vec![0.0_f32, 1.0]))],
            ),
        )
        .var_as(
            "third",
            traversal::g().add_n(
                "Document",
                vec![("embedding", PropertyInput::from(vec![0.9_f32, 0.1]))],
            ),
        )
        .var_as(
            "first_link",
            traversal::g().n(NodeRef::var("first")).add_e(
                "LINK",
                NodeRef::var("second"),
                vec![("embedding", PropertyInput::from(vec![1.0_f32, 0.0]))],
            ),
        )
        .var_as(
            "second_link",
            traversal::g().n(NodeRef::var("second")).add_e(
                "LINK",
                NodeRef::var("third"),
                vec![("embedding", PropertyInput::from(vec![0.0_f32, 1.0]))],
            ),
        )
        .returning(Vec::<String>::new());
    db.query(QueryRequest::write(fixture)).await.unwrap();

    let dimension = NonZeroUsize::new(2).unwrap();
    for (spec, description) in [
        (
            index::IndexSpec::node_vector(
                "Document",
                "embedding",
                dimension,
                index::VectorDistanceMetric::Euclidean,
                None::<String>,
            ),
            "node vector index",
        ),
        (
            index::IndexSpec::edge_vector(
                "LINK",
                "embedding",
                dimension,
                index::VectorDistanceMetric::Euclidean,
                None::<String>,
            ),
            "edge vector index",
        ),
    ] {
        let receipt = db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as("operation", traversal::g().create_index_if_not_exists(spec))
                    .returning(["operation"]),
            ))
            .await
            .unwrap();
        assert_eq!(receipt["operation"]["kind"], "accepted");
        let operation_id = receipt["operation"]["operation_id"]
            .as_str()
            .expect("accepted vector-index operation has an ID");
        await_index_operation_success(&db, operation_id, description).await;
    }

    let search = |k| {
        QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "nodes",
                    traversal::g()
                        .vector_search_nodes_with(
                            "Document",
                            "embedding",
                            PropertyInput::param("query"),
                            StreamBound::expr(Expr::param("k")),
                            None,
                        )
                        .id(),
                )
                .var_as(
                    "edges",
                    traversal::g()
                        .vector_search_edges_with(
                            "LINK",
                            "embedding",
                            PropertyInput::param("query"),
                            StreamBound::expr(Expr::param("k")),
                            None,
                        )
                        .id(),
                )
                .returning(["nodes", "edges"]),
        )
        .with_parameter_value(
            "query",
            QueryValue::Array(vec![QueryValue::F32(1.0), QueryValue::F32(0.0)]),
        )
        .with_parameter_value("k", QueryValue::I64(k))
    };
    assert_eq!(
        db.query(search(2)).await.unwrap(),
        serde_json::json!({ "nodes": [0, 2], "edges": [0, 1] })
    );

    let typed_search = batch::read_batch()
        .var_as(
            "node",
            traversal::g()
                .vector_search_nodes_with(
                    "Document",
                    "embedding",
                    PropertyInput::param("typed_query"),
                    1_usize,
                    None,
                )
                .id(),
        )
        .returning(["node"]);
    let typed_search_plan = planning::plan_read_batch(
        &typed_search,
        &db.planner_context(context::ParamBindings::default()),
    )
    .expect("typed vector-search batch plans");
    let typed_query =
        ir::NonEmptyString::new("typed_query").expect("typed query parameter is non-empty");
    for query in [
        PropertyValue::F32Array(vec![1.0, 0.0]),
        PropertyValue::F64Array(vec![1.0, 0.0]),
        PropertyValue::I64Array(vec![1, 0]),
        PropertyValue::Array(vec![PropertyValue::I64(1), PropertyValue::F64(0.0)]),
    ] {
        assert_eq!(
            db.execute(
                &typed_search_plan,
                context::ParamBindings::default().with_value(typed_query.clone(), query),
            )
            .await
            .expect("typed runtime vector executes")
            .last,
            Some(ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(0)]))
        );
    }

    let pushed_search_limit = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "node",
                traversal::g()
                    .vector_search_nodes_with(
                        "Document",
                        "embedding",
                        PropertyInput::from(vec![1.0_f32, 0.0]),
                        3_usize,
                        None,
                    )
                    .limit(1_usize)
                    .id(),
            )
            .var_as(
                "node_label",
                traversal::g()
                    .vector_search_nodes_with(
                        "Document",
                        "embedding",
                        PropertyInput::from(vec![1.0_f32, 0.0]),
                        1_usize,
                        None,
                    )
                    .label(),
            )
            .returning(["node", "node_label"]),
    );
    assert_eq!(
        db.query(pushed_search_limit).await.unwrap(),
        serde_json::json!({ "node": [0], "node_label": ["Document"] })
    );

    let virtual_distance = batch::read_batch()
        .var_as(
            "distance",
            traversal::g()
                .vector_search_nodes_with(
                    "Document",
                    "embedding",
                    PropertyInput::from(vec![1.0_f32, 0.0]),
                    1_usize,
                    None,
                )
                .bind("match")
                .project_bindings(vec![
                    BindingProjection::current("$distance", "current_distance"),
                    BindingProjection::binding("match", "$distance", "bound_distance"),
                ]),
        )
        .returning(["distance"]);
    assert_eq!(
        db.query(QueryRequest::read(virtual_distance))
            .await
            .unwrap(),
        serde_json::json!({
            "distance": [{ "current_distance": 0.0, "bound_distance": 0.0 }],
        })
    );

    let read_your_writes = batch::write_batch()
        .var_as(
            "node_updated",
            traversal::g()
                .n(NodeRef::id(1))
                .set_property("embedding", vec![1.0_f32, 0.01]),
        )
        .var_as(
            "edge_updated",
            traversal::g()
                .e(EdgeRef::id(1))
                .set_property("embedding", vec![1.0_f32, 0.02]),
        )
        .var_as(
            "nodes",
            traversal::g()
                .vector_search_nodes_with(
                    "Document",
                    "embedding",
                    PropertyInput::from(vec![1.0_f32, 0.0]),
                    3_usize,
                    None,
                )
                .id(),
        )
        .var_as(
            "edges",
            traversal::g()
                .vector_search_edges_with(
                    "LINK",
                    "embedding",
                    PropertyInput::from(vec![1.0_f32, 0.0]),
                    3_usize,
                    None,
                )
                .id(),
        )
        .returning(["nodes", "edges"]);
    assert_eq!(
        db.query(QueryRequest::write(read_your_writes))
            .await
            .unwrap(),
        serde_json::json!({ "nodes": [0, 1, 2], "edges": [0, 1] })
    );
    assert_eq!(
        db.query(search(3)).await.unwrap(),
        serde_json::json!({ "nodes": [0, 1, 2], "edges": [0, 1] })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "node_removed",
                traversal::g()
                    .n(NodeRef::id(0))
                    .remove_property("embedding"),
            )
            .var_as(
                "edge_removed",
                traversal::g()
                    .e(EdgeRef::id(0))
                    .remove_property("embedding"),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query(search(3)).await.unwrap(),
        serde_json::json!({ "nodes": [1, 2], "edges": [1] })
    );

    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "fourth",
                traversal::g().add_n(
                    "Document",
                    vec![("embedding", PropertyInput::from(vec![1.0_f32, 0.0]))],
                ),
            )
            .var_as(
                "third_link",
                traversal::g().n(NodeRef::id(2)).add_e(
                    "LINK",
                    NodeRef::var("fourth"),
                    vec![("embedding", PropertyInput::from(vec![1.0_f32, 0.0]))],
                ),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(
        db.query(search(3)).await.unwrap(),
        serde_json::json!({ "nodes": [3, 1, 2], "edges": [2, 1] })
    );

    let invalid = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "nodes",
                traversal::g()
                    .vector_search_nodes_with(
                        "Document",
                        "embedding",
                        PropertyInput::param("query"),
                        1_usize,
                        None,
                    )
                    .id(),
            )
            .returning(["nodes"]),
    )
    .with_parameter_value("query", QueryValue::Array(vec![QueryValue::F32(1.0)]));
    let error = db.query(invalid).await.unwrap_err();
    assert!(error.to_string().contains("dimension"), "{error}");
    db.close().await.unwrap();
}

#[tokio::test]
async fn public_query_boundary_rejects_invalid_dynamic_vector_search_inputs() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-interpreter-invalid-vector-search".to_owned(),
    })
    .await
    .expect("production invalid vector-search fixture opens");
    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_index_if_not_exists(index::IndexSpec::node_vector(
                        "Document",
                        "embedding",
                        NonZeroUsize::new(2).unwrap(),
                        index::VectorDistanceMetric::Euclidean,
                        None::<String>,
                    )),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    let operation_id = receipt["operation"]["operation_id"]
        .as_str()
        .expect("accepted vector-index operation has an ID");
    await_index_operation_success(&db, operation_id, "empty vector index").await;

    let dynamic_search = |query: QueryValue, limit: QueryValue| {
        let mut request = QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "ids",
                    traversal::g()
                        .vector_search_nodes_with(
                            "Document",
                            "embedding",
                            PropertyInput::param("query"),
                            StreamBound::expr(Expr::param("limit")),
                            None,
                        )
                        .id(),
                )
                .returning(["ids"]),
        );
        request.try_insert_untyped_parameter("query", query)?;
        request.try_insert_untyped_parameter("limit", limit)?;
        Ok::<_, helix_ast::query::QueryError>(request)
    };
    assert_eq!(
        db.query(
            dynamic_search(
                QueryValue::Array(vec![QueryValue::F32(1.0), QueryValue::F32(0.0)]),
                QueryValue::I64(1),
            )
            .unwrap()
        )
        .await
        .unwrap(),
        serde_json::json!({ "ids": [] })
    );

    for (query, expected) in [
        (
            QueryValue::Array(Vec::new()),
            "vector search query must not be empty",
        ),
        (
            QueryValue::String("not a vector".to_owned()),
            "must evaluate to a numeric array",
        ),
        (
            QueryValue::Array(vec![QueryValue::String("not numeric".to_owned())]),
            "array item must be numeric",
        ),
    ] {
        let error = db
            .query(dynamic_search(query, QueryValue::I64(1)).unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }

    let error = dynamic_search(
        QueryValue::Array(vec![QueryValue::F32(f32::NAN), QueryValue::F32(0.0)]),
        QueryValue::I64(1),
    )
    .unwrap_err();
    assert!(error.to_string().contains("non-finite f32"), "{error}");

    let error = db
        .query(
            dynamic_search(
                QueryValue::Array(vec![QueryValue::F32(1.0), QueryValue::F32(0.0)]),
                QueryValue::Bool(true),
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("must evaluate to an i64"),
        "{error}"
    );
    let error = db
        .query(
            dynamic_search(
                QueryValue::Array(vec![QueryValue::F32(1.0), QueryValue::F32(0.0)]),
                QueryValue::I64(0),
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("non-positive value 0"),
        "{error}"
    );
    db.close().await.unwrap();
}

#[test]
fn public_vector_parameter_types_reject_invalid_states() {
    let connections = Connections::try_new(4).unwrap();
    assert_eq!(connections.get(), 4);
    assert_eq!(connections.checked_double().unwrap().get(), 8);
    assert!(matches!(
        Connections::try_new(0),
        Err(VectorParameterError::Zero { .. })
    ));
    assert!(matches!(
        Connections::try_new(usize::MAX).unwrap().checked_double(),
        Err(VectorParameterError::ArithmeticOverflow { .. })
    ));

    assert_eq!(Layer0Connections::try_new(8, connections).unwrap().get(), 8);
    assert!(Layer0Connections::try_new(3, connections).is_err());
    assert_eq!(
        ConstructionBeamWidth::try_new(12, connections)
            .unwrap()
            .get(),
        12
    );
    assert!(ConstructionBeamWidth::try_new(3, connections).is_err());

    let result_count = ResultCount::try_new(3).unwrap();
    assert_eq!(result_count.get(), 3);
    assert!(ResultCount::try_new(0).is_err());
    assert_eq!(SearchBeamWidth::try_new(8, result_count).unwrap().get(), 8);
    assert!(SearchBeamWidth::try_new(2, result_count).is_err());

    assert_eq!(LayerMultiplier::try_new(0.5).unwrap().get(), 0.5);
    for invalid in [0.0, -1.0, f32::NAN, f32::INFINITY] {
        assert!(LayerMultiplier::try_new(invalid).is_err());
    }
    for valid in [0.0, 0.5, 1.0, -0.0] {
        assert!(UnitInterval::try_new(valid).is_ok());
    }
    for invalid in [-0.1, 1.1, f32::NAN] {
        assert!(UnitInterval::try_new(invalid).is_err());
    }
    assert_eq!(FailureProbability::try_new(0.5).unwrap().get(), 0.5);
    for invalid in [0.0, 1.0, f32::NAN] {
        assert!(FailureProbability::try_new(invalid).is_err());
    }

    let bits = NonZeroUsize::new(64).unwrap();
    assert_eq!(CollisionThreshold::try_new(64, bits).unwrap().get(), 64);
    assert!(CollisionThreshold::try_new(65, bits).is_err());

    let negative_zero = DistanceScore::try_new(-0.0).unwrap();
    let positive = DistanceScore::try_new(0.25).unwrap();
    assert_eq!(negative_zero.get(), 0.0);
    assert_eq!(negative_zero.cmp(&positive), Ordering::Less);
    assert_eq!(negative_zero.partial_cmp(&positive), Some(Ordering::Less));
    assert!(DistanceScore::try_new(-0.1).is_err());
    assert!(DistanceScore::try_new(f32::NAN).is_err());
}

#[test]
fn public_vector_dimension_types_bind_exact_lengths() {
    let dimension = VectorDimension::try_new(3).unwrap();
    assert_eq!(dimension.get(), 3);
    assert!(VectorDimension::try_new(0).is_err());
    assert!(VectorDimension::try_new_with_max(4, NonZeroUsize::new(3).unwrap()).is_err());

    let left = UnalignedVector::<f32>::from_slice(&[1.0, 2.0, 3.0]);
    let right = UnalignedVector::<f32>::from_slice(&[3.0, 2.0, 1.0]);
    let left_ref = VectorRef::try_new(&left, dimension).unwrap();
    assert_eq!(left_ref.dimension(), dimension);
    assert_eq!(left_ref.values().len(), 3);
    assert!(VectorRef::try_new(&left, VectorDimension::try_new(2).unwrap()).is_err());

    let pair = SameDimensionPair::try_new(&left, &right).unwrap();
    assert_eq!(pair.dimension(), dimension);
    assert_eq!(pair.left().values().len(), pair.right().values().len());
    let short = UnalignedVector::<f32>::from_slice(&[1.0]);
    assert!(SameDimensionPair::try_new(&left, &short).is_err());
}

#[test]
fn public_vector_search_parameters_reject_invalid_overrides() {
    let default = SearchParams::new(7).unwrap();
    assert_eq!(default.k(), 7);
    assert!(default.ef() >= default.k());
    assert!(SearchParams::new(0).is_err());
    assert!(SearchParams::new(7).unwrap().with_ef(2).is_err());

    assert!(SearchParams::new(3)
        .unwrap()
        .with_pre_simhash_sampling_ratio(f32::NAN)
        .is_err());
    assert!(SearchParams::new(3)
        .unwrap()
        .with_simhash_sampling_ratio(f32::INFINITY)
        .is_err());
    assert!(SearchParams::new(3)
        .unwrap()
        .with_simhash_failure_prob(2.0)
        .is_err());
    assert!(SearchParams::new(3)
        .unwrap()
        .with_simhash_bypass_tuning(0, 1, 0.5, 1)
        .is_err());

    for mode in [SimHashMode::Off, SimHashMode::Always, SimHashMode::Adaptive] {
        let params = SearchParams::new(3)
            .unwrap()
            .with_simhash_mode(mode)
            .with_pre_simhash_sampling_ratio(0.75)
            .unwrap()
            .clear_pre_simhash_sampling_ratio_override()
            .with_pre_simhash_sampling_ratio(0.5)
            .unwrap()
            .with_simhash_sampling_ratio(0.25)
            .unwrap()
            .clear_simhash_sampling_ratio_override()
            .with_simhash_sampling_ratio(0.4)
            .unwrap()
            .with_simhash_failure_prob(0.2)
            .unwrap()
            .clear_simhash_failure_prob_override()
            .with_simhash_failure_prob(0.3)
            .unwrap()
            .with_simhash_bypass_tuning(2, 3, 0.5, 4)
            .unwrap();
        assert_eq!(params.k(), 3);
        assert!(params.ef() >= 3);
    }

    let throughput = SearchParams::throughput_profile_floor_92(10).unwrap();
    assert_eq!(throughput.k(), 10);
    assert!(throughput.ef() >= 48);
}

#[test]
fn public_float_distance_kernels_cover_long_vectors_and_invalid_zero_cosine() {
    let left_values = (0..33).map(|value| value as f32).collect::<Vec<_>>();
    let right_values = (0..33)
        .map(|value| (value as f32) + 1.0)
        .collect::<Vec<_>>();

    let cosine_left = Item::<Cosine>::new(left_values.clone());
    let cosine_right = Item::<Cosine>::new(right_values.clone());
    let cosine_zero = Item::<Cosine>::new(vec![0.0; 33]);
    assert_eq!(Cosine::name(), "cosine");
    assert!(Cosine::distance(&cosine_left, &cosine_right).is_finite());
    assert!(Cosine::distance(&cosine_left, &cosine_zero).is_nan());
    assert!(format!("{:?}", cosine_left.header).contains("norm"));

    let euclidean_left = Item::<Euclidean>::new(left_values.clone());
    let euclidean_right = Item::<Euclidean>::new(right_values.clone());
    assert_eq!(Euclidean::name(), "euclidean");
    assert_eq!(Euclidean::distance(&euclidean_left, &euclidean_right), 33.0);
    assert!(Euclidean::norm(&euclidean_left).is_finite());
    assert!(format!("{:?}", euclidean_left.header).contains("bias"));

    let manhattan_left = Item::<Manhattan>::new(left_values);
    let manhattan_right = Item::<Manhattan>::new(right_values);
    assert_eq!(Manhattan::name(), "manhattan");
    assert_eq!(Manhattan::distance(&manhattan_left, &manhattan_right), 33.0);
    assert!(Manhattan::norm(&manhattan_left).is_finite());
    assert!(format!("{:?}", manhattan_left.header).contains("bias"));

    let hasher = SimHasher::new(3);
    assert_eq!(
        hasher.hash_from_slice(&[1.0, 2.0]),
        Err(SimHashError::DimensionMismatch {
            expected: 3,
            actual: 2,
        })
    );
}
