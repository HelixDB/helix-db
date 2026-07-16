//! Shared fixtures for interpreter access contract tests.

use std::num::NonZeroUsize;
pub(super) use std::sync::Arc;

use bytes::Bytes;
pub(super) use helix_ast::expr::Expr;
pub(super) use helix_ast::value::PropertyValue;
pub(super) use helix_planner::{catalog, context, exec, ir, properties};
pub(super) use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::IsolationLevel;

pub(super) use super::super::super::test_support;
pub(super) use super::super::super::{ExecutionScalar, ExecutionValue};
pub(super) use super::super::indexes::limited_index_ids;
pub(super) use super::super::search::{
    db_value_to_query_vector, limited_search_k, validate_query_vector,
    validate_vector_search_tenant,
};
pub(super) use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
pub(super) use crate::{config, search, HelixDB};

pub(super) fn expand_edge_ids_plan(
    from_param: ir::NonEmptyString,
    direction: ir::ExpandDirection,
    label: ir::ExpandLabelPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let expand_id = exec::ExecStepId::new(2).expect("positive step id");
    test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: from_param },
                    )),
                },
            ),
            test_support::step(
                2,
                vec![access_id],
                exec::ExecOp::Expand {
                    plan: ir::ExpandPlan {
                        direction,
                        label,
                        output: ir::ExpandOutput::Edges,
                    },
                },
            ),
            test_support::step(
                3,
                vec![expand_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        3,
    )
}

pub(super) async fn run_edge_expand(
    db: &HelixDB,
    from_param: &ir::NonEmptyString,
    from_value: PropertyValue,
    direction: ir::ExpandDirection,
    label: ir::ExpandLabelPlan,
) -> ExecutionValue {
    let plan = expand_edge_ids_plan(from_param.clone(), direction, label);
    db.execute(
        &plan,
        context::ParamBindings::default().with_value(from_param.clone(), from_value),
    )
    .await
    .expect("edge expansion succeeds")
    .last
    .expect("project step returns a value")
}

pub(super) fn edge_access_ids_plan(plan: exec::ExecEdgeAccessPlan) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(plan)),
                },
            ),
            test_support::step(
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

pub(super) async fn run_edge_access(
    db: &HelixDB,
    plan: exec::ExecEdgeAccessPlan,
) -> ExecutionValue {
    run_edge_access_with_params(db, plan, context::ParamBindings::default()).await
}

pub(super) async fn run_edge_access_with_params(
    db: &HelixDB,
    plan: exec::ExecEdgeAccessPlan,
    params: context::ParamBindings,
) -> ExecutionValue {
    db.execute(&edge_access_ids_plan(plan), params)
        .await
        .expect("edge access succeeds")
        .last
        .expect("project step returns a value")
}

pub(super) fn limited_edge_access_ids_plan(
    plan: exec::ExecEdgeAccessPlan,
    limit: usize,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let limit = properties::PositiveUsize::new(limit).expect("test access limit must be positive");
    test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Edge(plan).limited(limit)),
                },
            ),
            test_support::step(
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

pub(super) async fn run_limited_edge_access(
    db: &HelixDB,
    plan: exec::ExecEdgeAccessPlan,
    limit: usize,
) -> ExecutionValue {
    db.execute(
        &limited_edge_access_ids_plan(plan, limit),
        context::ParamBindings::default(),
    )
    .await
    .expect("limited edge access succeeds")
    .last
    .expect("project step returns a value")
}

pub(super) fn node_access_ids_plan(plan: exec::ExecNodeAccessPlan) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(plan)),
                },
            ),
            test_support::step(
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

pub(super) fn kv_read_ids_plan(read: exec::KvReadPlan) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(1, Vec::new(), exec::ExecOp::KvRead(read)),
            test_support::step(
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

pub(super) fn element_range_scan_ids_plan(
    keyspace: exec::ElementKeyspace,
    start: exec::KvKeyBound,
    end: exec::KvKeyBound,
) -> exec::ExecutablePlan {
    kv_read_ids_plan(exec::KvReadPlan::RangeScan {
        keyspace,
        start,
        end,
        limit: None,
    })
}

pub(super) fn limited_node_access_ids_plan(
    plan: exec::ExecNodeAccessPlan,
    limit: usize,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("positive step id");
    let limit = properties::PositiveUsize::new(limit).expect("test access limit must be positive");
    test_support::executable(
        ir::PlanKind::Read,
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(plan).limited(limit)),
                },
            ),
            test_support::step(
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

pub(super) async fn run_node_access(
    db: &HelixDB,
    plan: exec::ExecNodeAccessPlan,
) -> ExecutionValue {
    run_node_access_with_params(db, plan, context::ParamBindings::default()).await
}

pub(super) async fn run_limited_node_access(
    db: &HelixDB,
    plan: exec::ExecNodeAccessPlan,
    limit: usize,
) -> ExecutionValue {
    run_limited_node_access_with_params(db, plan, limit, context::ParamBindings::default()).await
}

pub(super) async fn run_node_access_with_params(
    db: &HelixDB,
    plan: exec::ExecNodeAccessPlan,
    params: context::ParamBindings,
) -> ExecutionValue {
    db.execute(&node_access_ids_plan(plan), params)
        .await
        .expect("node access succeeds")
        .last
        .expect("project step returns a value")
}

pub(super) async fn run_limited_node_access_with_params(
    db: &HelixDB,
    plan: exec::ExecNodeAccessPlan,
    limit: usize,
    params: context::ParamBindings,
) -> ExecutionValue {
    db.execute(&limited_node_access_ids_plan(plan, limit), params)
        .await
        .expect("limited node access succeeds")
        .last
        .expect("project step returns a value")
}

pub(super) fn search_index(index_name: &str) -> ir::SearchIndexPlan {
    ir::SearchIndexPlan {
        index_id: test_support::name(index_name),
        tenant: ir::SearchTenantPlan::Unscoped,
    }
}

pub(super) fn literal_search_limit(value: usize) -> ir::SearchLimitPlan {
    ir::SearchLimitPlan::Literal(NonZeroUsize::new(value).expect("positive search limit"))
}

pub(super) fn range_literal_i64(value: i64) -> ir::RangeIndexValue {
    ir::RangeIndexValue::literal(PropertyValue::I64(value)).expect("range value is indexable")
}

pub(super) fn exclusive_i64_bound(value: i64) -> ir::IndexBound {
    ir::IndexBound::Exclusive(range_literal_i64(value))
}

pub(super) fn exclusive_i64_between(lower: i64, upper: i64) -> ir::IndexRange {
    ir::IndexRange::Between(
        ir::IndexBetweenRange::new(exclusive_i64_bound(lower), exclusive_i64_bound(upper))
            .expect("range bounds are ordered"),
    )
}

pub(super) fn parameterized_i64_between(
    lower: ir::NonEmptyString,
    upper: ir::NonEmptyString,
) -> ir::IndexRange {
    ir::IndexRange::Between(
        ir::IndexBetweenRange::new(
            ir::IndexBound::Inclusive(ir::RangeIndexValue::Param(lower)),
            ir::IndexBound::Exclusive(ir::RangeIndexValue::Param(upper)),
        )
        .expect("dynamic range bounds are valid"),
    )
}

pub(super) async fn seed_vector_index<D: search::vector::Distance>(
    db: &HelixDB,
    definition: &config::VectorIndexDefinition,
    vectors: &[(u64, Vec<f32>)],
) {
    let definition = crate::index_v2::ValidatedVectorIndexDefinition::try_from_runtime(definition)
        .expect("test vector definition satisfies V2 validation");
    let transaction = db
        .inner_db()
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("active generation seed transaction starts");
    let index_id = crate::index_v2::repository::allocate_index_id(&transaction)
        .await
        .expect("test logical index ID allocates");
    let physical_index_id = crate::index_v2::repository::allocate_vector_physical_id(&transaction)
        .await
        .expect("test physical vector ID allocates");
    let record = crate::index_v2::IndexRecordV2::building(
        index_id,
        crate::index_v2::ValidatedDynamicIndexDefinition::Vector(definition.clone()),
        crate::index_v2::IndexRevision::initial(),
        crate::index_v2::PhysicalGeneration::Vector {
            generation: crate::index_v2::IndexGenerationId::initial(),
            layout: crate::index_v2::VectorPhysicalLayout::Unpartitioned { physical_index_id },
            descriptor: crate::index_v2::VectorGenerationDescriptor::for_definition(&definition),
        },
        crate::index_v2::IndexOperationId::new_v4(),
    )
    .expect("test building vector record is valid")
    .transition(crate::index_v2::IndexStateTransition::Activate)
    .expect("test vector generation activates");
    let active = crate::index_v2::ActiveIndexHandle::try_from_record(
        crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
        &record,
    )
    .expect("active test vector record projects a handle");
    let generation = search::vector::ValidatedVectorGenerationHandle::try_from_active::<D>(
        &active,
        physical_index_id,
    )
    .expect("test distance matches the canonical vector definition");
    let index = search::vector::VectorIndex::<D>::from_generation(&generation);
    index
        .create(
            &transaction,
            search::vector::VectorIndexConfig::from_v2_definition(
                &definition,
                generation.physical_name(),
            ),
        )
        .await
        .expect("V2 vector index creates");
    for (entity_id, vector) in vectors {
        index
            .insert(&transaction, *entity_id, vector)
            .await
            .expect("vector inserts");
    }
    let key = crate::encoding::v1::keys::Key::Data {
        scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
        kind: crate::encoding::v1::keys::DataKeyKind::IndexV2(
            crate::encoding::v1::keys::index_v2::IndexV2Key::index_record(
                record.identity().clone(),
            ),
        ),
    }
    .to_bytes();
    transaction
        .put(
            key,
            crate::encoding::v1::values::index_v2::encode_index_record(&record),
        )
        .expect("active canonical vector row stages");
    transaction
        .commit()
        .await
        .expect("active generation seed commits");
}

pub(super) async fn seed_text_manifest(
    db: &HelixDB,
    store: &Arc<dyn ObjectStore>,
    database: &str,
    definition: &config::TextIndexDefinition,
    index_name: &str,
    documents: &[search::text::TextDocumentInput],
) {
    let manifest = search::text::persist_documents_as_manifest(
        store, database, definition, index_name, documents,
    )
    .await
    .expect("manifest persists")
    .expect("documents produce a manifest");
    let txn = db
        .inner_db()
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("manifest transaction starts");
    txn.put(
        search::make_text_index_manifest_key(index_name),
        Bytes::from(serde_json::to_vec(&manifest).expect("manifest serializes")),
    )
    .expect("manifest row writes");
    txn.commit().await.expect("manifest commits");
}
