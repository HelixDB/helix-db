//! Production-linked vector DDL and graph-mutation contracts.
//!
//! These tests execute current planner IR through [`db::HelixDB`] without
//! `cfg(test)` constructors. They wait for accepted asynchronous DDL operations
//! to reach a terminal state before proving that dynamic vector-index creation
//! backfills existing graph rows and that committed node-property mutations
//! keep the active physical generation synchronized.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use db::config::{DbConfig, SearchIndexBackfillLimits, SearchIndexBatchLimits, VectorElementType};
use db::encoding::v2::keys::scope::DataScope;
use db::execution::interpreter::{
    ElementRef, ExecutionResult, ExecutionRow, ExecutionScalar, ExecutionValue,
};
use db::index_lifecycle::{IndexDdlReceipt, IndexOperationBlockerCode, IndexOperationStatus};
use db::search::{vector::VectorDistanceMetric, vector_index_name};
use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::batch;
use helix_ast::expr::Expr;
use helix_ast::query::QueryRequest;
use helix_ast::traversal;
use helix_ast::value::PropertyValue;
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};

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
                .expect("vector planner contract runtime builds")
                .block_on(contract());
        })
        .expect("vector planner contract thread starts")
        .join()
        .expect("vector planner contract thread completes");
}

/// Constructs a non-empty planner identifier used by the executable fixtures.
fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("fixture identifiers are non-empty")
}

/// Constructs one executable step with the planner's neutral scheduling data.
fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("fixture step ids are positive"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

/// Seals a linear fixture DAG behind the same validated plan boundary used by production.
fn executable(kind: ir::PlanKind, steps: Vec<exec::ExecStep>, root: usize) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("fixture plans are non-empty"),
        exec::ExecStepId::new(root).expect("fixture root ids are positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("fixture dependencies form a valid executable plan")
}

/// Converts literal graph properties into the planner's duplicate-free assignment type.
fn assignments(items: Vec<(&str, PropertyValue)>) -> ir::PropertyAssignments {
    ir::PropertyAssignments::try_from_vec(
        items
            .into_iter()
            .map(|(property, value)| (name(property), ir::PropertyInputPlan::Value(value)))
            .collect(),
    )
    .expect("fixture property names are unique")
}

/// Constructs a non-empty, validated physical ID list for point targets.
fn ids(values: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(
        ir::AtLeast::<_, 1>::try_from_vec(values).expect("fixture id lists are non-empty"),
    )
    .expect("fixture ids are valid")
}

/// Builds dynamic node-vector DDL with an explicit dimension and distance identity.
fn node_vector_ddl_plan(
    label: &str,
    property: &str,
    metric: ir::VectorIndexMetric,
) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec: ir::IndexDdlCreateSpec::NodeVector {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture vector key is valid"),
                        dimension: ir::VectorIndexDimension::new(2)
                            .expect("fixture vector dimension is positive"),
                        metric,
                        scope: catalog::SearchIndexScope::Unscoped,
                    },
                    mode: ir::IndexCreateMode::ErrorIfExists,
                },
            },
        )],
        1,
    )
}

/// Builds tenant-scoped dynamic node-vector DDL.
fn node_vector_tenant_ddl_plan(
    label: &str,
    property: &str,
    tenant_property: &str,
) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec: ir::IndexDdlCreateSpec::NodeVector {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture vector key is valid"),
                        dimension: ir::VectorIndexDimension::new(2)
                            .expect("fixture vector dimension is positive"),
                        metric: ir::VectorIndexMetric::Euclidean,
                        scope: catalog::SearchIndexScope::Tenant {
                            property: name(tenant_property),
                        },
                    },
                    mode: ir::IndexCreateMode::ErrorIfExists,
                },
            },
        )],
        1,
    )
}

/// Builds dynamic node-vector drop DDL for the same scoped catalog identity.
fn node_vector_drop_plan(label: &str, property: &str) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Drop {
                    spec: ir::IndexDdlDropSpec::NodeVector {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture vector key is valid"),
                    },
                },
            },
        )],
        1,
    )
}

/// Builds dynamic edge-vector DDL with an explicit dimension and distance identity.
fn edge_vector_ddl_plan(label: &str, property: &str) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec: ir::IndexDdlCreateSpec::EdgeVector {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture vector key is valid"),
                        dimension: ir::VectorIndexDimension::new(2)
                            .expect("fixture vector dimension is positive"),
                        metric: ir::VectorIndexMetric::Euclidean,
                        scope: catalog::SearchIndexScope::Unscoped,
                    },
                    mode: ir::IndexCreateMode::ErrorIfExists,
                },
            },
        )],
        1,
    )
}

/// Builds a source-node mutation and returns the created row as the plan root.
fn add_node_plan(label: &str, properties: Vec<(&str, PropertyValue)>) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::AddNodeSource {
                    label: name(label),
                    properties: assignments(properties),
                },
            },
        )],
        1,
    )
}

/// Builds an edge mutation whose source is selected through a bound node ID.
fn add_edge_plan(
    from_param: ir::NonEmptyString,
    to: u64,
    label: &str,
    properties: Vec<(&str, PropertyValue)>,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: from_param },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddEdge {
                        label: name(label),
                        to: ir::NodeTargetPlan::PointIds { ids: ids(vec![to]) },
                        properties: assignments(properties),
                    },
                },
            ),
        ],
        2,
    )
}

/// Builds a top-one vector search followed by an ID projection.
fn node_vector_search_plan(label: &str, property: &str, query: Vec<f32>) -> exec::ExecutablePlan {
    node_vector_search_plan_with_tenant(label, property, query, ir::SearchTenantPlan::Unscoped)
}

/// Builds a top-one vector search with an explicit tenant execution plan.
fn node_vector_search_plan_with_tenant(
    label: &str,
    property: &str,
    query: Vec<f32>,
    tenant: ir::SearchTenantPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    let index_name = vector_index_name(VectorElementType::Node, label, property);
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::VectorSearch {
                            key: catalog::NodeSearchIndexKey::try_new(label, property)
                                .expect("fixture search key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&index_name),
                                tenant,
                            },
                            query_vector: ir::VectorQueryInputPlan::Vector(
                                ir::SearchVector::new(query)
                                    .expect("fixture query vector is non-empty and finite"),
                            ),
                            k: ir::SearchLimitPlan::Literal(NonZeroUsize::MIN),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}

/// Builds vector ranking over node IDs supplied by an upstream traversal parameter.
fn restricted_node_vector_search_plan(
    label: &str,
    property: &str,
    query: Vec<f32>,
    candidates: ir::NonEmptyString,
    k: usize,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    let search_id = exec::ExecStepId::new(2).expect("fixture search id is positive");
    let index_name = vector_index_name(VectorElementType::Node, label, property);
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: candidates },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::VectorSearch {
                    plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                        key: catalog::NodeSearchIndexKey::try_new(label, property)
                            .expect("fixture search key is valid"),
                        index: ir::SearchIndexPlan {
                            index_id: name(&index_name),
                            tenant: ir::SearchTenantPlan::Unscoped,
                        },
                        query_vector: ir::VectorQueryInputPlan::Vector(
                            ir::SearchVector::new(query)
                                .expect("fixture query vector is non-empty and finite"),
                        ),
                        k: ir::SearchLimitPlan::Literal(
                            NonZeroUsize::new(k).expect("fixture search limit is positive"),
                        ),
                    }),
                },
            ),
            step(
                3,
                vec![search_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    )
}

/// Builds a top-one edge-vector search followed by an ID projection.
fn edge_vector_search_plan(label: &str, property: &str, query: Vec<f32>) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    let index_name = vector_index_name(VectorElementType::Edge, label, property);
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::VectorSearch {
                            key: catalog::EdgeSearchIndexKey::try_new(label, property)
                                .expect("fixture search key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&index_name),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_vector: ir::VectorQueryInputPlan::Vector(
                                ir::SearchVector::new(query)
                                    .expect("fixture query vector is non-empty and finite"),
                            ),
                            k: ir::SearchLimitPlan::Literal(NonZeroUsize::MIN),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}

/// Builds a property mutation for a node selected through a bound ID parameter.
fn node_property_mutation_plan(
    node_param: ir::NonEmptyString,
    mutation: exec::ExecMutationPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: node_param },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation { plan: mutation },
            ),
        ],
        2,
    )
}

/// Builds a property mutation for an edge selected through a bound ID parameter.
fn edge_property_mutation_plan(
    edge_param: ir::NonEmptyString,
    mutation: exec::ExecMutationPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access id is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(
                        exec::ExecEdgeAccessPlan::FromParam { param: edge_param },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Mutation { plan: mutation },
            ),
        ],
        2,
    )
}

/// Builds a source mutation that drops one edge by its exact physical ID.
fn drop_edge_by_id_plan(edge_id: u64) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::DropEdgeByIdSource {
                    edges: ir::EdgeTargetPlan::PointIds {
                        ids: ids(vec![edge_id]),
                    },
                },
            },
        )],
        1,
    )
}

/// Extracts the single node produced by an add-node fixture.
fn created_node_id(result: ExecutionResult) -> u64 {
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("add-node fixture should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Node(id)),
        ..
    }) = rows.first()
    else {
        panic!("add-node fixture should return one node row");
    };
    *id
}

/// Extracts the single edge produced by an add-edge fixture.
fn created_edge_id(result: ExecutionResult) -> u64 {
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("add-edge fixture should return a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Edge(id)),
        ..
    }) = rows.first()
    else {
        panic!("add-edge fixture should return one edge row");
    };
    *id
}

/// Extracts node IDs from a vector-search projection without accepting mixed scalar kinds.
fn projected_node_ids(result: ExecutionResult) -> Vec<u64> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("vector-search fixture should return projected scalars");
    };
    values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::NodeId(id) = value else {
                panic!("node-vector projection should contain only node ids");
            };
            id
        })
        .collect()
}

/// Extracts edge IDs from a vector-search projection without accepting mixed scalar kinds.
fn projected_edge_ids(result: ExecutionResult) -> Vec<u64> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("vector-search fixture should return projected scalars");
    };
    values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::EdgeId(id) = value else {
                panic!("edge-vector projection should contain only edge ids");
            };
            id
        })
        .collect()
}

/// Executes one DDL plan and waits for its durable operation to terminate.
async fn execute_ddl_to_success(db: &HelixDB, plan: &exec::ExecutablePlan) {
    let result = db
        .execute(plan, context::ParamBindings::default())
        .await
        .expect("fixture DDL is durably accepted");
    let Some(ExecutionValue::IndexDdlReceipt(IndexDdlReceipt::Accepted { operation_id, .. })) =
        result.last
    else {
        panic!("new fixture DDL should return an accepted operation");
    };

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match db
                .get_index_operation(
                    db::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
                    operation_id,
                )
                .await
                .expect("accepted fixture operation remains readable")
            {
                IndexOperationStatus::Succeeded { .. } => break,
                IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. } => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                IndexOperationStatus::Blocked { .. } | IndexOperationStatus::Aborted { .. } => {
                    panic!("fixture DDL should complete successfully")
                }
            }
        }
    })
    .await
    .expect("fixture DDL worker should converge");
    db.planner_context_scoped(
        context::ParamBindings::default(),
        db::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
    )
    .await
    .expect("terminal DDL is visible through a refreshed planner catalog");
}

/// Executes a vector search with no parameters and returns projected node IDs.
async fn search_node_ids(db: &HelixDB, query: Vec<f32>) -> Vec<u64> {
    projected_node_ids(
        db.execute(
            &node_vector_search_plan("Doc", "embedding", query),
            context::ParamBindings::default(),
        )
        .await
        .expect("fixture vector search succeeds"),
    )
}

/// Executes a tenant-scoped vector search and returns projected node IDs.
async fn search_node_ids_in_tenant(
    db: &HelixDB,
    property: &str,
    query: Vec<f32>,
    tenant: &str,
) -> Vec<u64> {
    let tenant =
        ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(PropertyValue::from(tenant)))
            .expect("fixture tenant is non-null");
    projected_node_ids(
        db.execute(
            &node_vector_search_plan_with_tenant(
                "Doc",
                property,
                query,
                ir::SearchTenantPlan::ScopedValue {
                    property: name("tenant_id"),
                    value: tenant,
                },
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("tenant vector search succeeds"),
    )
}

/// Executes an edge-vector search with no parameters and returns projected IDs.
async fn search_edge_ids(db: &HelixDB, query: Vec<f32>) -> Vec<u64> {
    projected_edge_ids(
        db.execute(
            &edge_vector_search_plan("LINK", "embedding", query),
            context::ParamBindings::default(),
        )
        .await
        .expect("fixture edge-vector search succeeds"),
    )
}

/// Stable logical vector identities used before physical node IDs are known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum VectorSlot {
    First,
    Second,
}

/// Closed action alphabet for one complete vector lifecycle execution.
#[derive(Debug, Clone, Copy)]
enum VectorAction {
    Insert { slot: VectorSlot, vector: [f32; 2] },
    Create,
    Search { query: [f32; 2] },
    Update { slot: VectorSlot, vector: [f32; 2] },
    Delete { slot: VectorSlot },
    Reopen,
    Drop,
    Recreate,
    RetryAfterHigherLimit,
    AbortBlockedBuild,
}

/// Invalid public mutation values and the exact error family each must retain.
#[derive(Debug, Clone, Copy)]
enum InvalidVectorUpdate {
    WrongDimension,
    NotANumber,
    PositiveInfinity,
    NegativeInfinity,
    UnsupportedProperty,
}

impl InvalidVectorUpdate {
    const ALL: [Self; 5] = [
        Self::WrongDimension,
        Self::NotANumber,
        Self::PositiveInfinity,
        Self::NegativeInfinity,
        Self::UnsupportedProperty,
    ];

    fn value(self) -> PropertyValue {
        match self {
            Self::WrongDimension => PropertyValue::F32Array(vec![1.0]),
            Self::NotANumber => PropertyValue::F32Array(vec![f32::NAN, 1.0]),
            Self::PositiveInfinity => PropertyValue::F32Array(vec![1.0, f32::INFINITY]),
            Self::NegativeInfinity => PropertyValue::F32Array(vec![f32::NEG_INFINITY, 1.0]),
            Self::UnsupportedProperty => PropertyValue::String("not-a-vector".to_string()),
        }
    }

    fn assert_error(self, error: &db::error::HelixDbError) {
        match self {
            Self::WrongDimension => assert!(
                matches!(
                    error,
                    db::error::HelixDbError::InvalidDimension {
                        expected: 2,
                        got: 1
                    }
                ),
                "wrong-dimension update returned {error:?}"
            ),
            Self::NotANumber => assert!(
                matches!(
                    error,
                    db::error::HelixDbError::InvalidVectorComponent { index: 0 }
                ),
                "NaN update returned {error:?}"
            ),
            Self::PositiveInfinity => assert!(
                matches!(
                    error,
                    db::error::HelixDbError::InvalidVectorComponent { index: 1 }
                ),
                "positive-infinity update returned {error:?}"
            ),
            Self::NegativeInfinity => assert!(
                matches!(
                    error,
                    db::error::HelixDbError::InvalidVectorComponent { index: 0 }
                ),
                "negative-infinity update returned {error:?}"
            ),
            Self::UnsupportedProperty => assert!(
                matches!(error, db::error::HelixDbError::Query(reason) if reason.contains("numeric array")),
                "unsupported-property update returned {error:?}"
            ),
        }
    }
}

/// Independent brute-force oracle for visible vector membership.
#[derive(Default)]
struct VectorReferenceModel {
    active: bool,
    vectors: BTreeMap<VectorSlot, (u64, [f32; 2])>,
}

impl VectorReferenceModel {
    /// Returns the exact Euclidean nearest neighbor for one deterministic query.
    fn nearest(&self, query: [f32; 2]) -> Vec<u64> {
        if !self.active {
            return Vec::new();
        }
        self.vectors
            .values()
            .min_by(|(_, left), (_, right)| {
                squared_euclidean(*left, query).total_cmp(&squared_euclidean(*right, query))
            })
            .map_or_else(Vec::new, |(entity_id, _)| vec![*entity_id])
    }
}

/// Runtime state driven by the vector action alphabet.
struct VectorMachine {
    token: ProcessLocalDatabaseToken,
    db: HelixDB,
    model: VectorReferenceModel,
}

impl VectorMachine {
    /// Opens one coordinated writer whose token survives model reopen actions.
    async fn open(database: &'static str) -> Self {
        let token = ProcessLocalDatabaseToken::new(database).expect("vector lifecycle token is valid");
        let db = HelixDB::open(HelixDbSource::InMemoryToken {
            token: token.clone(),
        })
        .await
        .expect("vector lifecycle writer opens");
        Self {
            token,
            db,
            model: VectorReferenceModel::default(),
        }
    }

    /// Applies one action to production and the independent semantic model.
    async fn apply(&mut self, action: VectorAction) {
        match action {
            VectorAction::Insert { slot, vector } => {
                let entity_id = created_node_id(
                    self.db
                        .execute(
                            &add_node_plan(
                                "Doc",
                                vec![("embedding", PropertyValue::F32Array(vector.to_vec()))],
                            ),
                            context::ParamBindings::default(),
                        )
                        .await
                        .expect("vector model node insertion commits"),
                );
                assert!(
                    self.model
                        .vectors
                        .insert(slot, (entity_id, vector))
                        .is_none(),
                    "vector model insertion uses a fresh logical slot"
                );
                if self.model.active {
                    self.assert_search(vector).await;
                }
            }
            VectorAction::Create => {
                execute_ddl_to_success(
                    &self.db,
                    &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
                )
                .await;
                self.model.active = true;
            }
            VectorAction::Search { query } => self.assert_search(query).await,
            VectorAction::Update { slot, vector } => {
                assert!(self.model.active, "model update requires an Active index");
                let entity_id = self
                    .model
                    .vectors
                    .get(&slot)
                    .expect("model update names an existing vector")
                    .0;
                let parameter = name("vector_model_node");
                self.db
                    .execute(
                        &node_property_mutation_plan(
                            parameter.clone(),
                            exec::ExecMutationPlan::SetProperty {
                                name: name("embedding"),
                                value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(
                                    vector.to_vec(),
                                )),
                            },
                        ),
                        context::ParamBindings::default().with_value(
                            parameter,
                            PropertyValue::I64(
                                i64::try_from(entity_id).expect("fixture node ID fits i64"),
                            ),
                        ),
                    )
                    .await
                    .expect("vector model update commits");
                self.model.vectors.insert(slot, (entity_id, vector));
                self.assert_search(vector).await;
            }
            VectorAction::Delete { slot } => {
                assert!(self.model.active, "model delete requires an Active index");
                let (entity_id, _) = self
                    .model
                    .vectors
                    .remove(&slot)
                    .expect("model delete names an existing vector");
                let parameter = name("vector_model_node");
                self.db
                    .execute(
                        &node_property_mutation_plan(
                            parameter.clone(),
                            exec::ExecMutationPlan::RemoveProperty {
                                name: name("embedding"),
                            },
                        ),
                        context::ParamBindings::default().with_value(
                            parameter,
                            PropertyValue::I64(
                                i64::try_from(entity_id).expect("fixture node ID fits i64"),
                            ),
                        ),
                    )
                    .await
                    .expect("vector model deletion commits");
            }
            VectorAction::Reopen => self.reopen(DbConfig::new()).await,
            VectorAction::Drop => {
                execute_ddl_to_success(&self.db, &node_vector_drop_plan("Doc", "embedding")).await;
                self.model.active = false;
                assert!(self
                    .db
                    .execute(
                        &node_vector_search_plan("Doc", "embedding", vec![1.0, 0.0]),
                        context::ParamBindings::default(),
                    )
                    .await
                    .is_err());
            }
            VectorAction::Recreate => {
                assert!(!self.model.active, "model recreate starts from Dropped");
                execute_ddl_to_success(
                    &self.db,
                    &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
                )
                .await;
                self.model.active = true;
            }
            VectorAction::RetryAfterHigherLimit => self.exercise_limit_retry().await,
            VectorAction::AbortBlockedBuild => self.exercise_blocked_abort().await,
        }
    }

    /// Proves invalid public updates roll back graph, physical, and cache state.
    async fn reject_invalid_updates(&self, slot: VectorSlot) {
        assert!(self.model.active, "invalid update requires an Active index");
        let (entity_id, vector) = *self
            .model
            .vectors
            .get(&slot)
            .expect("invalid update names an existing vector");
        for case in InvalidVectorUpdate::ALL {
            let parameter = name("invalid_vector_model_node");
            let error = self
                .db
                .execute(
                    &node_property_mutation_plan(
                        parameter.clone(),
                        exec::ExecMutationPlan::SetProperty {
                            name: name("embedding"),
                            value: ir::PropertyInputPlan::Value(case.value()),
                        },
                    ),
                    context::ParamBindings::default().with_value(
                        parameter,
                        PropertyValue::I64(
                            i64::try_from(entity_id).expect("fixture node ID fits i64"),
                        ),
                    ),
                )
                .await
                .expect_err("invalid vector update fails closed");
            case.assert_error(&error);
            self.assert_search(vector).await;
        }
    }

    /// Reopens the same physical database under one explicit runtime policy.
    async fn reopen(&mut self, config: DbConfig) {
        self.db
            .close()
            .await
            .expect("vector lifecycle writer closes before reopen");
        self.db = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: self.token.clone(),
            },
            config,
        )
        .await
        .expect("vector lifecycle writer reopens");
    }

    /// Compares one production HNSW result with the brute-force model.
    async fn assert_search(&self, query: [f32; 2]) {
        assert_eq!(
            search_node_ids(&self.db, query.to_vec()).await,
            self.model.nearest(query)
        );
    }

    /// Proves a typed vector resource block resumes from its exact checkpoint.
    async fn exercise_limit_retry(&mut self) {
        let limited_label = "VectorLimitDoc";
        created_node_id(
            self.db
                .execute(
                    &add_node_plan(
                        limited_label,
                        vec![("embedding", PropertyValue::F32Array(vec![0.25, 0.75]))],
                    ),
                    context::ParamBindings::default(),
                )
                .await
                .expect("limit-retry vector source commits"),
        );
        self.reopen(blocked_vector_limit_config()).await;
        let operation_id = accepted_operation_id(
            self.db
                .execute(
                    &node_vector_ddl_plan(
                        limited_label,
                        "embedding",
                        ir::VectorIndexMetric::Euclidean,
                    ),
                    context::ParamBindings::default(),
                )
                .await
                .expect("limit-retry vector DDL is accepted"),
        );
        let blocked =
            wait_for_expected(&self.db, operation_id, ExpectedVectorTerminal::Blocked).await;
        let progress = blocked.common().progress;
        self.reopen(DbConfig::new()).await;
        let retried = self
            .db
            .retry_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("blocked vector build requeues");
        assert_eq!(retried.common().progress, progress);
        wait_for_expected(&self.db, operation_id, ExpectedVectorTerminal::Succeeded).await;
        execute_ddl_to_success(&self.db, &node_vector_drop_plan(limited_label, "embedding")).await;
    }

    /// Proves abort reuses one blocked vector build and completes cleanup.
    async fn exercise_blocked_abort(&mut self) {
        let limited_label = "VectorLimitDoc";
        self.reopen(blocked_vector_limit_config()).await;
        let operation_id = accepted_operation_id(
            self.db
                .execute(
                    &node_vector_ddl_plan(
                        limited_label,
                        "embedding",
                        ir::VectorIndexMetric::Euclidean,
                    ),
                    context::ParamBindings::default(),
                )
                .await
                .expect("blocked vector DDL is accepted"),
        );
        wait_for_expected(&self.db, operation_id, ExpectedVectorTerminal::Blocked).await;

        let operation_id_string = operation_id.as_uuid().to_string();
        let blocked = self
            .db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "status",
                        traversal::g().get_index_operation(operation_id_string.clone()),
                    )
                    .returning(["status"]),
            ))
            .await
            .expect("blocked vector operation is readable through the query boundary");
        assert_eq!(blocked["status"]["status"], "blocked");

        let retried = self
            .db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "status",
                        traversal::g().retry_index_operation(operation_id_string.clone()),
                    )
                    .returning(["status"]),
            ))
            .await
            .expect("blocked vector operation retries through the query boundary");
        assert!(matches!(
            retried["status"]["status"].as_str(),
            Some("queued" | "running" | "blocked")
        ));
        wait_for_expected(&self.db, operation_id, ExpectedVectorTerminal::Blocked).await;

        self.reopen(DbConfig::new()).await;
        let aborting = self
            .db
            .query(QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "status",
                        traversal::g().abort_index_operation(operation_id_string),
                    )
                    .returning(["status"]),
            ))
            .await
            .expect("blocked vector build enters abort cleanup through the query boundary");
        assert!(matches!(
            aborting["status"]["status"].as_str(),
            Some("queued" | "running" | "aborted")
        ));
        wait_for_expected(&self.db, operation_id, ExpectedVectorTerminal::Aborted).await;
    }
}

/// Returns squared Euclidean distance for one exact two-dimensional pair.
fn squared_euclidean(left: [f32; 2], right: [f32; 2]) -> f32 {
    left.into_iter()
        .zip(right)
        .map(|(left, right)| (left - right).powi(2))
        .sum()
}

/// Independent test-only component limit for one active float metric.
fn magnitude_limit(metric: ir::VectorIndexMetric, dimension: u32) -> f32 {
    let factor = match metric {
        ir::VectorIndexMetric::Euclidean => 8_u64,
        ir::VectorIndexMetric::Manhattan => 4_u64,
        ir::VectorIndexMetric::Cosine => panic!("cosine has no magnitude limit"),
    };
    let divisor = u64::from(dimension)
        .checked_mul(factor)
        .expect("test-only magnitude divisor remains bounded");
    let exact = match metric {
        ir::VectorIndexMetric::Euclidean => (f64::from(f32::MAX) / divisor as f64).sqrt(),
        ir::VectorIndexMetric::Manhattan => f64::from(f32::MAX) / divisor as f64,
        ir::VectorIndexMetric::Cosine => unreachable!("cosine rejected above"),
    };
    let rounded = exact as f32;
    if f64::from(rounded) > exact {
        f32::from_bits(rounded.to_bits() - 1)
    } else {
        rounded
    }
}

/// Returns the next representable float above one positive finite value.
fn next_up(value: f32) -> f32 {
    assert!(value.is_finite() && value > 0.0);
    f32::from_bits(value.to_bits() + 1)
}

/// Records the exact public magnitude-domain error contract.
fn record_public_rejection<T>(
    failures: &mut Vec<String>,
    case: &str,
    result: Result<T, db::error::HelixDbError>,
    metric: VectorDistanceMetric,
    observed_magnitude: f32,
) {
    let inclusive_maximum = magnitude_limit(
        match metric {
            VectorDistanceMetric::Cosine => {
                panic!("cosine has no magnitude rejection contract")
            }
            VectorDistanceMetric::Euclidean => ir::VectorIndexMetric::Euclidean,
            VectorDistanceMetric::Manhattan => ir::VectorIndexMetric::Manhattan,
        },
        2,
    );
    match result {
        Err(db::error::HelixDbError::VectorComponentMagnitudeExceeded {
            metric: actual_metric,
            dimension,
            component_index,
            observed_magnitude: actual_magnitude,
            inclusive_maximum: actual_maximum,
        }) if actual_metric == metric
            && dimension == 2
            && component_index == 0
            && actual_magnitude == observed_magnitude
            && actual_maximum == inclusive_maximum => {}
        Err(error) => failures.push(format!(
            "{case}: returned {error:?}, expected VectorComponentMagnitudeExceeded {{ metric: {metric:?}, dimension: 2, component_index: 0, observed_magnitude: {observed_magnitude}, inclusive_maximum: {inclusive_maximum} }}"
        )),
        Ok(_) => failures.push(format!("{case}: accepted an out-of-domain finite vector")),
    }
}

/// Waits for a public DDL operation to reach any terminal state.
async fn wait_for_terminal(
    db: &HelixDB,
    operation_id: db::index_lifecycle::IndexOperationId,
) -> IndexOperationStatus {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = db
                .get_index_operation(DataScope::LegacyUnscoped, operation_id)
                .await
                .expect("magnitude operation remains readable");
            match status {
                IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. } => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                IndexOperationStatus::Blocked { .. }
                | IndexOperationStatus::Succeeded { .. }
                | IndexOperationStatus::Aborted { .. } => return status,
            }
        }
    })
    .await
    .expect("magnitude operation reaches a terminal state")
}

/// Extracts one accepted or resumed DDL operation ID.
fn accepted_operation_id(result: ExecutionResult) -> db::index_lifecycle::IndexOperationId {
    let Some(ExecutionValue::IndexDdlReceipt(receipt)) = result.last else {
        panic!("vector lifecycle DDL returns one receipt");
    };
    match receipt {
        IndexDdlReceipt::Accepted { operation_id, .. }
        | IndexDdlReceipt::ExistingOperation { operation_id } => operation_id,
        IndexDdlReceipt::AlreadyActive { .. } => {
            panic!("fresh or recreated vector fixture is not already Active")
        }
    }
}

/// Terminal operation state required by the vector model.
#[derive(Debug, Clone, Copy)]
enum ExpectedVectorTerminal {
    Blocked,
    Succeeded,
    Aborted,
}

/// Waits for one exact public vector operation state.
async fn wait_for_expected(
    db: &HelixDB,
    operation_id: db::index_lifecycle::IndexOperationId,
    expected: ExpectedVectorTerminal,
) -> IndexOperationStatus {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = db
                .get_index_operation(DataScope::LegacyUnscoped, operation_id)
                .await
                .expect("vector model operation remains readable");
            let reached = matches!(
                (expected, &status),
                (
                    ExpectedVectorTerminal::Blocked,
                    IndexOperationStatus::Blocked { .. }
                ) | (
                    ExpectedVectorTerminal::Succeeded,
                    IndexOperationStatus::Succeeded { .. }
                ) | (
                    ExpectedVectorTerminal::Aborted,
                    IndexOperationStatus::Aborted { .. }
                )
            );
            if reached {
                return status;
            }
            assert!(matches!(
                status,
                IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. }
            ));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("vector model operation reaches its terminal state")
}

/// Returns a policy whose first vector source row triggers a typed block.
fn blocked_vector_limit_config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let limits = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            NonZeroU64::MIN,
            batch.max_output_operations(),
            batch.max_output_bytes(),
            batch.max_single_vector_output_bytes(),
        )
        .expect("blocked vector limits remain internally consistent"),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .expect("blocked vector policy preserves cross-budget invariants");
    DbConfig::new().with_search_index_backfill_limits(limits)
}

/// Drives every vector lifecycle action against one brute-force model.
#[test]
fn public_vector_lifecycle_matches_reference_model() {
    run_high_stack_contract(
        "public-vector-lifecycle-model",
        public_vector_lifecycle_matches_reference_model_contract,
    );
}

async fn public_vector_lifecycle_matches_reference_model_contract() {
    let mut machine = VectorMachine::open("production-vector-lifecycle-state-machine").await;
    for action in [
        VectorAction::Insert {
            slot: VectorSlot::First,
            vector: [1.0, 0.0],
        },
        VectorAction::Create,
        VectorAction::Search { query: [1.0, 0.0] },
        VectorAction::Insert {
            slot: VectorSlot::Second,
            vector: [0.0, 1.0],
        },
        VectorAction::Search { query: [0.0, 1.0] },
        VectorAction::Update {
            slot: VectorSlot::First,
            vector: [0.75, 0.25],
        },
        VectorAction::Search {
            query: [0.75, 0.25],
        },
        VectorAction::Delete {
            slot: VectorSlot::Second,
        },
        VectorAction::Reopen,
        VectorAction::Search {
            query: [0.75, 0.25],
        },
        VectorAction::Drop,
        VectorAction::Recreate,
        VectorAction::Search {
            query: [0.75, 0.25],
        },
        VectorAction::Drop,
        VectorAction::RetryAfterHigherLimit,
        VectorAction::AbortBlockedBuild,
    ] {
        machine.apply(action).await;
    }
    machine
        .db
        .close()
        .await
        .expect("vector lifecycle writer closes cleanly");
}

/// Proves every invalid vector value rolls back and cannot poison a rebuild.
#[test]
fn public_invalid_vector_updates_are_atomic_across_reopen_and_rebuild() {
    std::thread::Builder::new()
        .name("public-invalid-vector-updates".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(16 * 1024 * 1024)
                .build()
                .expect("invalid-vector runtime should build")
                .block_on(async {
                    let mut machine =
                        VectorMachine::open("production-vector-invalid-update-state-machine").await;
                    machine
                        .apply(VectorAction::Insert {
                            slot: VectorSlot::First,
                            vector: [0.75, 0.25],
                        })
                        .await;
                    machine.apply(VectorAction::Create).await;
                    Box::pin(machine.reject_invalid_updates(VectorSlot::First)).await;
                    machine.apply(VectorAction::Reopen).await;
                    machine
                        .apply(VectorAction::Search {
                            query: [0.75, 0.25],
                        })
                        .await;
                    machine.apply(VectorAction::Drop).await;
                    machine.apply(VectorAction::Recreate).await;
                    machine
                        .apply(VectorAction::Search {
                            query: [0.75, 0.25],
                        })
                        .await;
                    machine.apply(VectorAction::Drop).await;
                    machine
                        .db
                        .close()
                        .await
                        .expect("invalid-vector writer closes cleanly");
                });
        })
        .expect("invalid-vector test thread should spawn")
        .join()
        .expect("invalid-vector test thread should not panic");
}

#[tokio::test]
async fn public_dynamic_vector_ddl_backfills_existing_nodes() {
    let database = "production-vector-ddl-backfill";
    let token = ProcessLocalDatabaseToken::new(database).expect("fixture token is valid");
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("writer opens");

    let first = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("first node commits before DDL"),
    );
    created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![0.0, 1.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("second node commits before DDL"),
    );

    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;

    assert_eq!(search_node_ids(&db, vec![1.0, 0.0]).await, vec![first]);
    db.close().await.expect("first writer closes");

    let reopened = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .expect("managed writer reopens");
    assert_eq!(
        search_node_ids(&reopened, vec![1.0, 0.0]).await,
        vec![first]
    );
    execute_ddl_to_success(&reopened, &node_vector_drop_plan("Doc", "embedding")).await;
    assert!(reopened
        .execute(
            &node_vector_search_plan("Doc", "embedding", vec![1.0, 0.0]),
            context::ParamBindings::default(),
        )
        .await
        .is_err());
    reopened.close().await.expect("reopened writer closes");
}

#[tokio::test]
async fn public_managed_search_executes_every_active_vector_metric() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-active-metrics")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    let mut node_ids = Vec::new();
    for offset in 0_u16..20 {
        let displacement = f32::from(offset) / 20.0;
        node_ids.push(created_node_id(
            db.execute(
                &add_node_plan(
                    "Doc",
                    vec![
                        (
                            "cosine_embedding",
                            PropertyValue::F32Array(vec![1.0 - displacement, displacement]),
                        ),
                        (
                            "manhattan_embedding",
                            PropertyValue::F32Array(vec![1.0 - displacement, displacement]),
                        ),
                    ],
                ),
                context::ParamBindings::default(),
            )
            .await
            .expect("fixture node commits before DDL"),
        ));
    }

    for (property, metric) in [
        ("cosine_embedding", ir::VectorIndexMetric::Cosine),
        ("manhattan_embedding", ir::VectorIndexMetric::Manhattan),
    ] {
        execute_ddl_to_success(&db, &node_vector_ddl_plan("Doc", property, metric)).await;
        assert_eq!(
            projected_node_ids(
                db.execute(
                    &node_vector_search_plan("Doc", property, vec![1.0, 0.0]),
                    context::ParamBindings::default(),
                )
                .await
                .expect("managed metric-specific vector search succeeds"),
            ),
            vec![node_ids[0]]
        );
    }

    let node = name("zero_cosine_node");
    let error = db
        .execute(
            &node_property_mutation_plan(
                node.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name("cosine_embedding"),
                    value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(vec![0.0, -0.0])),
                },
            ),
            context::ParamBindings::default().with_value(
                node,
                PropertyValue::I64(i64::try_from(node_ids[0]).expect("fixture node ID fits i64")),
            ),
        )
        .await
        .expect_err("zero-norm cosine update fails closed");
    assert!(
        matches!(error, db::error::HelixDbError::ZeroNormCosineVector),
        "zero-norm cosine update returned {error:?}"
    );
    assert_eq!(
        projected_node_ids(
            db.execute(
                &node_vector_search_plan("Doc", "cosine_embedding", vec![1.0, 0.0]),
                context::ParamBindings::default(),
            )
            .await
            .expect("cosine search survives rejected zero-norm update"),
        ),
        vec![node_ids[0]]
    );

    db.close().await.expect("writer closes");
}

#[test]
fn public_managed_vector_search_enforces_tenant_partitions() {
    run_high_stack_contract(
        "public-vector-tenant-partitions",
        public_managed_vector_search_enforces_tenant_partitions_contract,
    );
}

async fn public_managed_vector_search_enforces_tenant_partitions_contract() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-vector-tenant-partitions".to_owned(),
    })
    .await
    .expect("tenant vector fixture opens");
    let acme_id = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![
                    ("tenant_id", PropertyValue::from("acme")),
                    ("embedding", PropertyValue::F32Array(vec![1.0, 0.0])),
                    (
                        "unscoped_embedding",
                        PropertyValue::F32Array(vec![1.0, 0.0]),
                    ),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("acme vector commits"),
    );
    let globex_id = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![
                    ("tenant_id", PropertyValue::from("globex")),
                    ("embedding", PropertyValue::F32Array(vec![0.0, 1.0])),
                    (
                        "unscoped_embedding",
                        PropertyValue::F32Array(vec![0.0, 1.0]),
                    ),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("globex vector commits"),
    );
    execute_ddl_to_success(
        &db,
        &node_vector_tenant_ddl_plan("Doc", "embedding", "tenant_id"),
    )
    .await;
    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan(
            "Doc",
            "unscoped_embedding",
            ir::VectorIndexMetric::Euclidean,
        ),
    )
    .await;

    let tenant_literal =
        ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(PropertyValue::from("acme")))
            .expect("literal tenant value is non-null");
    assert_eq!(
        projected_node_ids(
            db.execute(
                &node_vector_search_plan_with_tenant(
                    "Doc",
                    "embedding",
                    vec![1.0, 0.0],
                    ir::SearchTenantPlan::ScopedValue {
                        property: name("tenant_id"),
                        value: tenant_literal,
                    },
                ),
                context::ParamBindings::default(),
            )
            .await
            .expect("literal tenant search succeeds"),
        ),
        vec![acme_id]
    );

    let tenant_param = name("tenant");
    let tenant_expression = ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Expr(
        ir::PropertyInputExprPlan::new(Expr::param(tenant_param.as_ref()))
            .expect("tenant parameter expression is valid"),
    ))
    .expect("runtime tenant expression is valid");
    assert_eq!(
        projected_node_ids(
            db.execute(
                &node_vector_search_plan_with_tenant(
                    "Doc",
                    "embedding",
                    vec![0.0, 1.0],
                    ir::SearchTenantPlan::ScopedValue {
                        property: name("tenant_id"),
                        value: tenant_expression,
                    },
                ),
                context::ParamBindings::default()
                    .with_value(tenant_param, PropertyValue::from("globex")),
            )
            .await
            .expect("runtime tenant search succeeds"),
        ),
        vec![globex_id]
    );

    let moving_node = name("moving_node");
    db.execute(
        &node_property_mutation_plan(
            moving_node.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name("tenant_id"),
                value: ir::PropertyInputPlan::Value(PropertyValue::from("globex")),
            },
        ),
        context::ParamBindings::default().with_value(
            moving_node.clone(),
            PropertyValue::I64(i64::try_from(acme_id).expect("fixture node ID fits i64")),
        ),
    )
    .await
    .expect("vector tenant move commits");
    assert!(
        search_node_ids_in_tenant(&db, "embedding", vec![1.0, 0.0], "acme")
            .await
            .is_empty()
    );
    assert_eq!(
        search_node_ids_in_tenant(&db, "embedding", vec![1.0, 0.0], "globex").await,
        vec![acme_id]
    );

    db.execute(
        &node_property_mutation_plan(
            moving_node.clone(),
            exec::ExecMutationPlan::RemoveProperty {
                name: name("tenant_id"),
            },
        ),
        context::ParamBindings::default().with_value(
            moving_node.clone(),
            PropertyValue::I64(i64::try_from(acme_id).expect("fixture node ID fits i64")),
        ),
    )
    .await
    .expect("vector tenant removal commits");
    assert!(
        search_node_ids_in_tenant(&db, "embedding", vec![1.0, 0.0], "acme")
            .await
            .is_empty()
    );
    assert_eq!(
        search_node_ids_in_tenant(&db, "embedding", vec![1.0, 0.0], "globex").await,
        vec![globex_id]
    );

    db.execute(
        &node_property_mutation_plan(
            moving_node.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name("tenant_id"),
                value: ir::PropertyInputPlan::Value(PropertyValue::from("acme")),
            },
        ),
        context::ParamBindings::default().with_value(
            moving_node,
            PropertyValue::I64(i64::try_from(acme_id).expect("fixture node ID fits i64")),
        ),
    )
    .await
    .expect("vector tenant reinsertion commits");
    assert_eq!(
        search_node_ids_in_tenant(&db, "embedding", vec![1.0, 0.0], "acme").await,
        vec![acme_id]
    );

    let invalid_tenant_plans = [
        (
            ir::SearchTenantPlan::Unscoped,
            "requires tenant value for partition property 'tenant_id'",
        ),
        (
            ir::SearchTenantPlan::Scoped {
                property: name("tenant_id"),
            },
            "requires tenant value for partition property 'tenant_id'",
        ),
        (
            ir::SearchTenantPlan::Scoped {
                property: name("workspace_id"),
            },
            "is scoped by 'tenant_id' not 'workspace_id'",
        ),
        (
            ir::SearchTenantPlan::ScopedValue {
                property: name("workspace_id"),
                value: ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(
                    PropertyValue::from("acme"),
                ))
                .expect("wrong-property tenant value remains structurally valid"),
            },
            "is scoped by 'tenant_id' not 'workspace_id'",
        ),
    ];
    for (tenant, expected) in invalid_tenant_plans {
        let error = db
            .execute(
                &node_vector_search_plan_with_tenant("Doc", "embedding", vec![1.0, 0.0], tenant),
                context::ParamBindings::default(),
            )
            .await
            .expect_err("invalid tenant shape fails closed");
        assert!(error.to_string().contains(expected), "{error}");
    }

    let unscoped_index_invalid_plans = [
        (
            ir::SearchTenantPlan::Scoped {
                property: name("tenant_id"),
            },
            "is not tenant-scoped by 'tenant_id'",
        ),
        (
            ir::SearchTenantPlan::ScopedValue {
                property: name("tenant_id"),
                value: ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(
                    PropertyValue::from("acme"),
                ))
                .expect("unscoped-index tenant value remains structurally valid"),
            },
            "does not support tenant value for 'tenant_id'",
        ),
    ];
    for (tenant, expected) in unscoped_index_invalid_plans {
        let error = db
            .execute(
                &node_vector_search_plan_with_tenant(
                    "Doc",
                    "unscoped_embedding",
                    vec![1.0, 0.0],
                    tenant,
                ),
                context::ParamBindings::default(),
            )
            .await
            .expect_err("unscoped index rejects tenant input");
        assert!(error.to_string().contains(expected), "{error}");
    }
    db.close().await.expect("tenant vector fixture closes");
}

#[tokio::test]
async fn public_restricted_search_deduplicates_and_omits_other_labels() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-restricted-membership")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    let nearest = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("nearest node commits"),
    );
    let farther = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![0.0, 1.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("farther node commits"),
    );
    let other_label = created_node_id(
        db.execute(
            &add_node_plan(
                "Other",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("other-label node commits"),
    );
    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;

    let parameter = name("traversal_candidates");
    let candidate_ids = [farther, nearest, nearest, other_label]
        .into_iter()
        .map(|id| i64::try_from(id).expect("fixture node ID fits i64"))
        .collect();
    let result = db
        .execute(
            &restricted_node_vector_search_plan(
                "Doc",
                "embedding",
                vec![1.0, 0.0],
                parameter.clone(),
                10,
            ),
            context::ParamBindings::default()
                .with_value(parameter, PropertyValue::I64Array(candidate_ids)),
        )
        .await
        .expect("restricted vector search succeeds");

    assert_eq!(projected_node_ids(result), vec![nearest, farther]);
    db.close().await.expect("writer closes");
}

#[tokio::test]
async fn public_node_mutations_keep_vector_generation_synchronized() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-node-mutations")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;

    let node_id = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("node insertion updates the vector generation"),
    );
    assert_eq!(search_node_ids(&db, vec![1.0, 0.0]).await, vec![node_id]);

    let node_param = name("node");
    db.execute(
        &node_property_mutation_plan(
            node_param.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name("embedding"),
                value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(vec![0.0, 1.0])),
            },
        ),
        context::ParamBindings::default()
            .with_value(node_param.clone(), PropertyValue::I64(node_id as i64)),
    )
    .await
    .expect("set-property replaces the indexed vector");
    assert_eq!(search_node_ids(&db, vec![0.0, 1.0]).await, vec![node_id]);

    db.execute(
        &node_property_mutation_plan(
            node_param.clone(),
            exec::ExecMutationPlan::RemoveProperty {
                name: name("embedding"),
            },
        ),
        context::ParamBindings::default()
            .with_value(node_param.clone(), PropertyValue::I64(node_id as i64)),
    )
    .await
    .expect("remove-property deletes the indexed vector");
    assert!(search_node_ids(&db, vec![0.0, 1.0]).await.is_empty());

    let dropped_id = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("replacement node updates the vector generation"),
    );
    db.execute(
        &node_property_mutation_plan(node_param.clone(), exec::ExecMutationPlan::Drop),
        context::ParamBindings::default()
            .with_value(node_param, PropertyValue::I64(dropped_id as i64)),
    )
    .await
    .expect("drop-node removes the indexed vector");
    assert!(search_node_ids(&db, vec![1.0, 0.0]).await.is_empty());
}

#[tokio::test]
async fn public_edge_mutations_keep_vector_generation_synchronized() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-edge-mutations")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    execute_ddl_to_success(&db, &edge_vector_ddl_plan("LINK", "embedding")).await;

    let source = created_node_id(
        db.execute(
            &add_node_plan("Source", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("source node commits"),
    );
    let target = created_node_id(
        db.execute(
            &add_node_plan("Target", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("target node commits"),
    );
    let source_param = name("source");
    let edge_id = created_edge_id(
        db.execute(
            &add_edge_plan(
                source_param.clone(),
                target,
                "LINK",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default()
                .with_value(source_param.clone(), PropertyValue::I64(source as i64)),
        )
        .await
        .expect("edge insertion updates the vector generation"),
    );
    assert_eq!(search_edge_ids(&db, vec![1.0, 0.0]).await, vec![edge_id]);

    let edge_param = name("edge");
    db.execute(
        &edge_property_mutation_plan(
            edge_param.clone(),
            exec::ExecMutationPlan::SetProperty {
                name: name("embedding"),
                value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(vec![0.0, 1.0])),
            },
        ),
        context::ParamBindings::default()
            .with_value(edge_param.clone(), PropertyValue::I64(edge_id as i64)),
    )
    .await
    .expect("edge set-property replaces the indexed vector");
    assert_eq!(search_edge_ids(&db, vec![0.0, 1.0]).await, vec![edge_id]);

    db.execute(
        &edge_property_mutation_plan(
            edge_param.clone(),
            exec::ExecMutationPlan::RemoveProperty {
                name: name("embedding"),
            },
        ),
        context::ParamBindings::default()
            .with_value(edge_param, PropertyValue::I64(edge_id as i64)),
    )
    .await
    .expect("edge remove-property deletes the indexed vector");
    assert!(search_edge_ids(&db, vec![0.0, 1.0]).await.is_empty());

    let dropped_edge_id = created_edge_id(
        db.execute(
            &add_edge_plan(
                source_param.clone(),
                target,
                "LINK",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default()
                .with_value(source_param, PropertyValue::I64(source as i64)),
        )
        .await
        .expect("replacement edge updates the vector generation"),
    );
    db.execute(
        &drop_edge_by_id_plan(dropped_edge_id),
        context::ParamBindings::default(),
    )
    .await
    .expect("drop-edge-by-id removes the indexed vector");
    assert!(search_edge_ids(&db, vec![1.0, 0.0]).await.is_empty());
}

#[tokio::test]
async fn public_node_magnitude_rejection_is_atomic_for_insert_and_update() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-node-magnitude-red")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;
    let limit = magnitude_limit(ir::VectorIndexMetric::Euclidean, 2);
    let outside = next_up(limit);

    let invalid_insert = db
        .execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![outside, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await;
    let target = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![0.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("target node commits"),
    );
    created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("control node commits"),
    );
    let node = name("magnitude_node");
    let invalid_update = db
        .execute(
            &node_property_mutation_plan(
                node.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name("embedding"),
                    value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(vec![
                        outside, 0.0,
                    ])),
                },
            ),
            context::ParamBindings::default().with_value(node, PropertyValue::I64(target as i64)),
        )
        .await;
    let nearest_after = db
        .execute(
            &node_vector_search_plan("Doc", "embedding", vec![0.0, 0.0]),
            context::ParamBindings::default(),
        )
        .await
        .map(projected_node_ids);
    db.close().await.expect("writer closes");

    let mut failures = Vec::new();
    record_public_rejection(
        &mut failures,
        "public node insert",
        invalid_insert,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    record_public_rejection(
        &mut failures,
        "public node update",
        invalid_update,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    match nearest_after {
        Ok(ids) if ids == vec![target] => {}
        Ok(ids) => failures.push(format!(
            "public node update changed the active vector generation: expected [{target}], got {ids:?}"
        )),
        Err(error) => failures.push(format!(
            "public node rollback verification could not search the prior generation: {error}"
        )),
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn public_edge_magnitude_rejection_is_atomic_for_insert_and_update() {
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: ProcessLocalDatabaseToken::new("production-vector-edge-magnitude-red")
            .expect("fixture token is valid"),
    })
    .await
    .expect("writer opens");
    execute_ddl_to_success(&db, &edge_vector_ddl_plan("LINK", "embedding")).await;
    let source = created_node_id(
        db.execute(
            &add_node_plan("Source", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("source node commits"),
    );
    let target = created_node_id(
        db.execute(
            &add_node_plan("Target", Vec::new()),
            context::ParamBindings::default(),
        )
        .await
        .expect("target node commits"),
    );
    let source_param = name("magnitude_source");
    let limit = magnitude_limit(ir::VectorIndexMetric::Euclidean, 2);
    let outside = next_up(limit);
    let invalid_insert = db
        .execute(
            &add_edge_plan(
                source_param.clone(),
                target,
                "LINK",
                vec![("embedding", PropertyValue::F32Array(vec![outside, 0.0]))],
            ),
            context::ParamBindings::default()
                .with_value(source_param.clone(), PropertyValue::I64(source as i64)),
        )
        .await;
    let indexed = created_edge_id(
        db.execute(
            &add_edge_plan(
                source_param.clone(),
                target,
                "LINK",
                vec![("embedding", PropertyValue::F32Array(vec![0.0, 0.0]))],
            ),
            context::ParamBindings::default()
                .with_value(source_param.clone(), PropertyValue::I64(source as i64)),
        )
        .await
        .expect("indexed edge commits"),
    );
    created_edge_id(
        db.execute(
            &add_edge_plan(
                source_param,
                target,
                "LINK",
                vec![("embedding", PropertyValue::F32Array(vec![1.0, 0.0]))],
            ),
            context::ParamBindings::default()
                .with_value(name("magnitude_source"), PropertyValue::I64(source as i64)),
        )
        .await
        .expect("control edge commits"),
    );
    let edge = name("magnitude_edge");
    let invalid_update = db
        .execute(
            &edge_property_mutation_plan(
                edge.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name("embedding"),
                    value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(vec![
                        outside, 0.0,
                    ])),
                },
            ),
            context::ParamBindings::default().with_value(edge, PropertyValue::I64(indexed as i64)),
        )
        .await;
    let nearest_after = db
        .execute(
            &edge_vector_search_plan("LINK", "embedding", vec![0.0, 0.0]),
            context::ParamBindings::default(),
        )
        .await
        .map(projected_edge_ids);
    db.close().await.expect("writer closes");

    let mut failures = Vec::new();
    record_public_rejection(
        &mut failures,
        "public edge insert",
        invalid_insert,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    record_public_rejection(
        &mut failures,
        "public edge update",
        invalid_update,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    match nearest_after {
        Ok(ids) if ids == vec![indexed] => {}
        Ok(ids) => failures.push(format!(
            "public edge update changed the active vector generation: expected [{indexed}], got {ids:?}"
        )),
        Err(error) => failures.push(format!(
            "public edge rollback verification could not search the prior generation: {error}"
        )),
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn public_magnitude_queries_cover_unrestricted_restricted_and_tenant_indexes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-vector-query-magnitude-red".to_owned(),
    })
    .await
    .expect("writer opens");
    let first = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![
                    ("tenant_id", PropertyValue::from("acme")),
                    ("embedding", PropertyValue::F32Array(vec![0.0, 0.0])),
                    ("tenant_embedding", PropertyValue::F32Array(vec![0.0, 0.0])),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("first query node commits"),
    );
    let second = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![
                    ("tenant_id", PropertyValue::from("acme")),
                    ("embedding", PropertyValue::F32Array(vec![1.0, 0.0])),
                    ("tenant_embedding", PropertyValue::F32Array(vec![1.0, 0.0])),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("second query node commits"),
    );
    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;
    execute_ddl_to_success(
        &db,
        &node_vector_tenant_ddl_plan("Doc", "tenant_embedding", "tenant_id"),
    )
    .await;
    let limit = magnitude_limit(ir::VectorIndexMetric::Euclidean, 2);
    let outside = next_up(limit);
    let unrestricted = db
        .execute(
            &node_vector_search_plan("Doc", "embedding", vec![outside, 0.0]),
            context::ParamBindings::default(),
        )
        .await;
    let catastrophic = db
        .execute(
            &node_vector_search_plan("Doc", "embedding", vec![f32::MAX, -f32::MAX]),
            context::ParamBindings::default(),
        )
        .await;
    let candidates = name("magnitude_candidates");
    let restricted = db
        .execute(
            &restricted_node_vector_search_plan(
                "Doc",
                "embedding",
                vec![outside, 0.0],
                candidates.clone(),
                1,
            ),
            context::ParamBindings::default().with_value(
                candidates,
                PropertyValue::I64Array(vec![first as i64, second as i64]),
            ),
        )
        .await;
    let tenant = db
        .execute(
            &node_vector_search_plan_with_tenant(
                "Doc",
                "tenant_embedding",
                vec![outside, 0.0],
                ir::SearchTenantPlan::ScopedValue {
                    property: name("tenant_id"),
                    value: ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(
                        PropertyValue::from("acme"),
                    ))
                    .expect("tenant value validates"),
                },
            ),
            context::ParamBindings::default(),
        )
        .await;
    db.close().await.expect("writer closes");

    let mut failures = Vec::new();
    record_public_rejection(
        &mut failures,
        "public unrestricted query",
        unrestricted,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    record_public_rejection(
        &mut failures,
        "public catastrophic unrestricted query",
        catastrophic,
        VectorDistanceMetric::Euclidean,
        f32::MAX,
    );
    record_public_rejection(
        &mut failures,
        "public restricted query",
        restricted,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    record_public_rejection(
        &mut failures,
        "public tenant query",
        tenant,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn public_preexisting_magnitude_violation_blocks_ddl_build() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-vector-build-magnitude-red".to_owned(),
    })
    .await
    .expect("writer opens");
    let limit = magnitude_limit(ir::VectorIndexMetric::Euclidean, 2);
    let entity_id = created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![(
                    "embedding",
                    PropertyValue::F32Array(vec![next_up(limit), 0.0]),
                )],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("out-of-domain authoritative source commits before DDL"),
    );
    let operation_id = accepted_operation_id(
        db.execute(
            &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
            context::ParamBindings::default(),
        )
        .await
        .expect("magnitude DDL is durably accepted"),
    );
    let terminal = wait_for_terminal(&db, operation_id).await;
    let mut failures = Vec::new();
    if matches!(
        terminal,
        IndexOperationStatus::Blocked {
            blocker_code: IndexOperationBlockerCode::InvalidSourceData,
            ..
        }
    ) {
        let entity = name("corrected_magnitude_entity");
        db.execute(
            &node_property_mutation_plan(
                entity.clone(),
                exec::ExecMutationPlan::SetProperty {
                    name: name("embedding"),
                    value: ir::PropertyInputPlan::Value(PropertyValue::F32Array(vec![0.0, 0.0])),
                },
            ),
            context::ParamBindings::default()
                .with_value(entity, PropertyValue::I64(entity_id as i64)),
        )
        .await
        .expect("authoritative source correction commits");
        db.retry_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("corrected magnitude build requeues");
        let retried = wait_for_terminal(&db, operation_id).await;
        if !matches!(retried, IndexOperationStatus::Succeeded { .. }) {
            failures.push(format!(
                "corrected out-of-domain source did not activate on retry: {retried:?}"
            ));
        }
    } else {
        failures.push(format!(
            "out-of-domain authoritative build source must block as invalid source data, got {terminal:?}"
        ));
    }
    db.close().await.expect("writer closes");

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn public_magnitude_validation_survives_reopen_and_drop_recreate() {
    let token = ProcessLocalDatabaseToken::new("production-vector-reopen-magnitude-red")
        .expect("fixture token is valid");
    let db = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("writer opens");
    execute_ddl_to_success(
        &db,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;
    created_node_id(
        db.execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![0.0, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("baseline vector commits"),
    );
    db.close().await.expect("first writer closes");

    let reopened = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .expect("writer reopens");
    let outside = next_up(magnitude_limit(ir::VectorIndexMetric::Euclidean, 2));
    let after_reopen = reopened
        .execute(
            &node_vector_search_plan("Doc", "embedding", vec![outside, 0.0]),
            context::ParamBindings::default(),
        )
        .await;
    execute_ddl_to_success(&reopened, &node_vector_drop_plan("Doc", "embedding")).await;
    execute_ddl_to_success(
        &reopened,
        &node_vector_ddl_plan("Doc", "embedding", ir::VectorIndexMetric::Euclidean),
    )
    .await;
    let after_recreate = reopened
        .execute(
            &add_node_plan(
                "Doc",
                vec![("embedding", PropertyValue::F32Array(vec![outside, 0.0]))],
            ),
            context::ParamBindings::default(),
        )
        .await;
    reopened.close().await.expect("reopened writer closes");

    let mut failures = Vec::new();
    record_public_rejection(
        &mut failures,
        "query after reopen",
        after_reopen,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    record_public_rejection(
        &mut failures,
        "insert after drop/recreate",
        after_recreate,
        VectorDistanceMetric::Euclidean,
        outside,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
