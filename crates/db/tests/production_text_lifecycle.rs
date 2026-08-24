//! Production-linked text-index lifecycle state-machine contracts.
//!
//! The test uses only public DDL, mutation, status, retry, abort, catalog, and
//! search boundaries. Feature-gated row observers decode the resulting durable
//! state without adding a second lifecycle implementation.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use db::config::{DbConfig, SearchIndexBackfillLimits, SearchIndexBatchLimits, TextElementType};
use db::encoding::v2::keys::scope::DataScope;
use db::execution::interpreter::{
    ElementRef, ExecutionResult, ExecutionRow, ExecutionScalar, ExecutionValue,
};
use db::index_lifecycle::{IndexDdlReceipt, IndexOperationId, IndexOperationStatus};
use db::search::text_index_name;
use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::expr::Expr;
use helix_ast::value::PropertyValue;
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};

const LABEL: &str = "TextModelDocument";
const PROPERTY: &str = "body";
const OPERATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Stable logical names used by the reference model before physical IDs exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EntitySlot {
    First,
    Second,
}

/// Closed action alphabet for the text lifecycle model.
#[derive(Debug, Clone, Copy)]
enum TextAction {
    Insert {
        slot: EntitySlot,
        text: &'static str,
    },
    Create,
    Search {
        term: &'static str,
    },
    Update {
        slot: EntitySlot,
        text: &'static str,
    },
    RejectInvalidValue {
        slot: EntitySlot,
    },
    Delete {
        slot: EntitySlot,
    },
    Reopen,
    Drop,
    Recreate,
    RetryAfterHigherLimit,
    AbortBlockedBuild,
}

/// Independent semantic model for lifecycle visibility and text membership.
#[derive(Default)]
struct TextReferenceModel {
    active: bool,
    entities: BTreeMap<EntitySlot, (u64, String)>,
}

impl TextReferenceModel {
    /// Returns the exact IDs whose current source text contains one query term.
    fn expected_for(&self, term: &str) -> Vec<u64> {
        if !self.active {
            return Vec::new();
        }
        let mut ids = self
            .entities
            .values()
            .filter_map(|(entity_id, text)| text.contains(term).then_some(*entity_id))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }
}

/// Runtime state driven by the closed action alphabet.
struct TextMachine {
    token: ProcessLocalDatabaseToken,
    db: HelixDB,
    model: TextReferenceModel,
}

impl TextMachine {
    /// Opens one coordinated writer whose process-local token survives reopen.
    async fn open() -> Self {
        let token = ProcessLocalDatabaseToken::new("production-text-lifecycle-state-machine")
            .expect("text lifecycle token is valid");
        let db = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: token.clone(),
            },
            lifecycle_db_config(),
        )
        .await
        .expect("text lifecycle writer opens");
        Self {
            token,
            db,
            model: TextReferenceModel::default(),
        }
    }

    /// Applies one action to production and the independent semantic model.
    async fn apply(&mut self, action: TextAction) {
        match action {
            TextAction::Insert { slot, text } => {
                let entity_id = created_node_id(
                    self.db
                        .execute(
                            &add_node_plan(LABEL, vec![(PROPERTY, PropertyValue::from(text))]),
                            context::ParamBindings::default(),
                        )
                        .await
                        .expect("text model node insertion commits"),
                );
                assert!(
                    self.model
                        .entities
                        .insert(slot, (entity_id, text.to_string()))
                        .is_none(),
                    "model insertion uses a fresh logical slot"
                );
                if self.model.active {
                    self.assert_search(text).await;
                    self.assert_steady_rows().await;
                }
            }
            TextAction::Create => {
                execute_ddl_to_success(&self.db, &text_create_plan(LABEL, PROPERTY)).await;
                self.model.active = true;
                self.assert_steady_rows().await;
            }
            TextAction::Search { term } => self.assert_search(term).await,
            TextAction::Update { slot, text } => {
                assert!(self.model.active, "model update requires an Active index");
                let (entity_id, stored) = self
                    .model
                    .entities
                    .get_mut(&slot)
                    .expect("model update names an existing entity");
                let parameter = name("text_model_node");
                self.db
                    .execute(
                        &node_mutation_plan(
                            parameter.clone(),
                            exec::ExecMutationPlan::SetProperty {
                                name: name(PROPERTY),
                                value: ir::PropertyInputPlan::Value(PropertyValue::from(text)),
                            },
                        ),
                        context::ParamBindings::default().with_value(
                            parameter,
                            PropertyValue::I64(
                                i64::try_from(*entity_id).expect("fixture node ID fits i64"),
                            ),
                        ),
                    )
                    .await
                    .expect("text model property update commits");
                *stored = text.to_string();
                self.assert_search(text).await;
                self.assert_steady_rows().await;
            }
            TextAction::RejectInvalidValue { slot } => {
                assert!(self.model.active, "invalid update requires an Active index");
                let (entity_id, stored) = self
                    .model
                    .entities
                    .get(&slot)
                    .expect("invalid update names an existing entity");
                let entity_id = *entity_id;
                let stored = stored.clone();
                let parameter = name("invalid_text_model_node");
                let error = self
                    .db
                    .execute(
                        &node_mutation_plan(
                            parameter.clone(),
                            exec::ExecMutationPlan::SetProperty {
                                name: name(PROPERTY),
                                value: ir::PropertyInputPlan::Value(PropertyValue::I64(7)),
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
                    .expect_err("non-text dynamic values fail closed");
                assert_eq!(error.index_error_code(), Some("invalid_index_source_data"));
                assert!(
                    matches!(
                        &error,
                        db::error::HelixDbError::InvalidIndexSourceData { reason }
                            if reason.contains("indexed property has an unsupported value")
                    ),
                    "{error}"
                );
                self.assert_search(&stored).await;
                self.assert_steady_rows().await;
            }
            TextAction::Delete { slot } => {
                assert!(self.model.active, "model delete requires an Active index");
                let (entity_id, _) = self
                    .model
                    .entities
                    .remove(&slot)
                    .expect("model delete names an existing entity");
                let parameter = name("text_model_node");
                self.db
                    .execute(
                        &node_mutation_plan(parameter.clone(), exec::ExecMutationPlan::Drop),
                        context::ParamBindings::default().with_value(
                            parameter,
                            PropertyValue::I64(
                                i64::try_from(entity_id).expect("fixture node ID fits i64"),
                            ),
                        ),
                    )
                    .await
                    .expect("text model node deletion commits");
                self.assert_steady_rows().await;
            }
            TextAction::Reopen => {
                self.reopen(lifecycle_db_config()).await;
                self.assert_steady_rows().await;
            }
            TextAction::Drop => {
                execute_ddl_to_success(&self.db, &text_drop_plan(LABEL, PROPERTY)).await;
                self.model.active = false;
                assert!(self.search("textmodelalpha").await.is_err());
                db::production_coverage::index_lifecycle_text_dropped_row_contracts(&self.db).await;
            }
            TextAction::Recreate => {
                assert!(!self.model.active, "model recreate starts from Dropped");
                execute_ddl_to_success(&self.db, &text_create_plan(LABEL, PROPERTY)).await;
                self.model.active = true;
                self.assert_steady_rows().await;
            }
            TextAction::RetryAfterHigherLimit => self.exercise_limit_retry().await,
            TextAction::AbortBlockedBuild => self.exercise_blocked_abort().await,
        }
    }

    /// Reopens the same physical database under one explicit runtime policy.
    async fn reopen(&mut self, config: DbConfig) {
        self.db
            .close()
            .await
            .expect("text lifecycle writer closes before reopen");
        self.db = HelixDB::open_with_config(
            HelixDbSource::InMemoryToken {
                token: self.token.clone(),
            },
            config,
        )
        .await
        .expect("text lifecycle writer reopens");
    }

    /// Searches through the public text-index plan without test-only storage access.
    async fn search(&self, term: &str) -> Result<Vec<u64>, db::error::HelixDbError> {
        self.db
            .execute(
                &text_search_plan(LABEL, PROPERTY, term),
                context::ParamBindings::default(),
            )
            .await
            .map(projected_node_ids)
    }

    /// Compares one production search with the deterministic semantic oracle.
    async fn assert_search(&self, term: &str) {
        let mut actual = self.search(term).await.expect("text model search succeeds");
        actual.sort_unstable();
        assert_eq!(actual, self.model.expected_for(term));
    }

    /// Cross-checks all settled text rows against the current model membership.
    async fn assert_steady_rows(&self) {
        db::production_coverage::index_lifecycle_text_steady_state_contracts(
            &self.db,
            self.model.entities.len(),
        )
        .await;
    }

    /// Proves a typed resource block resumes from its exact checkpoint.
    async fn exercise_limit_retry(&mut self) {
        let limited_label = "TextLimitDocument";
        created_node_id(
            self.db
                .execute(
                    &add_node_plan(
                        limited_label,
                        vec![(PROPERTY, PropertyValue::from("limitretrytoken"))],
                    ),
                    context::ParamBindings::default(),
                )
                .await
                .expect("limit-retry source node commits"),
        );
        self.reopen(blocked_limit_config()).await;
        let operation_id = execute_ddl(&self.db, &text_create_plan(limited_label, PROPERTY)).await;
        let blocked = wait_for_terminal(&self.db, operation_id, ExpectedTerminal::Blocked).await;
        let blocked_progress = blocked.common().progress;
        self.reopen(lifecycle_db_config()).await;
        let retried = self
            .db
            .retry_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("blocked text build requeues");
        assert_eq!(retried.common().progress, blocked_progress);
        wait_for_terminal(&self.db, operation_id, ExpectedTerminal::Succeeded).await;
        execute_ddl_to_success(&self.db, &text_drop_plan(limited_label, PROPERTY)).await;
        db::production_coverage::index_lifecycle_text_dropped_row_contracts(&self.db).await;
    }

    /// Proves abort reuses one blocked build and removes its complete row graph.
    async fn exercise_blocked_abort(&mut self) {
        let limited_label = "TextLimitDocument";
        self.reopen(blocked_limit_config()).await;
        let operation_id = execute_ddl(&self.db, &text_create_plan(limited_label, PROPERTY)).await;
        wait_for_terminal(&self.db, operation_id, ExpectedTerminal::Blocked).await;
        self.reopen(lifecycle_db_config()).await;
        self.db
            .abort_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("blocked text build enters abort cleanup");
        wait_for_terminal(&self.db, operation_id, ExpectedTerminal::Aborted).await;
        db::production_coverage::index_lifecycle_text_dropped_row_contracts(&self.db).await;
    }
}

/// Constructs one validated planner identifier.
fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("fixture identifier is non-empty")
}

/// Constructs one executable step with neutral scheduling metadata.
fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("fixture step IDs are positive"),
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

/// Seals a fixture DAG behind the production executable-plan validator.
fn executable(kind: ir::PlanKind, steps: Vec<exec::ExecStep>, root: usize) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("fixture plan is non-empty"),
        exec::ExecStepId::new(root).expect("fixture root ID is positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("fixture dependencies form a valid executable plan")
}

/// Converts duplicate-free literal graph properties into planner assignments.
fn assignments(items: Vec<(&str, PropertyValue)>) -> ir::PropertyAssignments {
    ir::PropertyAssignments::try_from_vec(
        items
            .into_iter()
            .map(|(property, value)| (name(property), ir::PropertyInputPlan::Value(value)))
            .collect(),
    )
    .expect("fixture property names are unique")
}

/// Builds one public node insertion plan.
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

/// Builds one node mutation selected through a bound physical ID.
fn node_mutation_plan(
    parameter: ir::NonEmptyString,
    mutation: exec::ExecMutationPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access ID is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::FromParam { param: parameter },
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

/// Builds one public text CREATE plan.
fn text_create_plan(label: &str, property: &str) -> exec::ExecutablePlan {
    text_create_plan_with_scope(label, property, catalog::SearchIndexScope::Unscoped)
}

/// Builds one public text CREATE plan with explicit tenant scope.
fn text_create_plan_with_scope(
    label: &str,
    property: &str,
    scope: catalog::SearchIndexScope,
) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Create {
                    spec: ir::IndexDdlCreateSpec::NodeText {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture text key is valid"),
                        scope,
                    },
                    mode: ir::IndexCreateMode::ErrorIfExists,
                },
            },
        )],
        1,
    )
}

/// Builds one public text DROP plan.
fn text_drop_plan(label: &str, property: &str) -> exec::ExecutablePlan {
    executable(
        ir::PlanKind::Write,
        vec![step(
            1,
            Vec::new(),
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::Drop {
                    spec: ir::IndexDdlDropSpec::NodeText {
                        key: catalog::ScopedPropertyKey::try_new(label, property)
                            .expect("fixture text key is valid"),
                    },
                },
            },
        )],
        1,
    )
}

/// Builds one public text search followed by an ID projection.
fn text_search_plan(label: &str, property: &str, query: &str) -> exec::ExecutablePlan {
    text_search_plan_with_tenant(label, property, query, ir::SearchTenantPlan::Unscoped)
}

/// Builds one public text search with explicit tenant execution semantics.
fn text_search_plan_with_tenant(
    label: &str,
    property: &str,
    query: &str,
    tenant: ir::SearchTenantPlan,
) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("fixture access ID is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::TextSearch {
                            key: catalog::NodeSearchIndexKey::try_new(label, property)
                                .expect("fixture text search key is valid"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&text_index_name(
                                    TextElementType::Node,
                                    label,
                                    property,
                                )),
                                tenant,
                            },
                            query_text: ir::TextQueryInputPlan::Text(name(query)),
                            k: ir::SearchLimitPlan::Literal(
                                std::num::NonZeroUsize::new(10)
                                    .expect("fixture text result limit is positive"),
                            ),
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

/// Extracts the single node produced by an insertion plan.
fn created_node_id(result: ExecutionResult) -> u64 {
    let Some(ExecutionValue::Stream(rows)) = result.last else {
        panic!("node insertion returns a stream");
    };
    let Some(ExecutionRow {
        current: Some(ElementRef::Node(id)),
        ..
    }) = rows.first()
    else {
        panic!("node insertion returns one current node");
    };
    *id
}

/// Extracts homogeneous node IDs from a projected text result.
fn projected_node_ids(result: ExecutionResult) -> Vec<u64> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("text projection returns scalar IDs");
    };
    values
        .into_iter()
        .map(|value| {
            let ExecutionScalar::NodeId(id) = value else {
                panic!("text node projection contains only node IDs");
            };
            id
        })
        .collect()
}

/// Executes one literal tenant-scoped text search and projects node IDs.
async fn search_node_ids_in_tenant(
    db: &HelixDB,
    label: &str,
    query: &str,
    tenant: &str,
) -> Vec<u64> {
    let tenant =
        ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(PropertyValue::from(tenant)))
            .expect("fixture text tenant is non-null");
    projected_node_ids(
        db.execute(
            &text_search_plan_with_tenant(
                label,
                PROPERTY,
                query,
                ir::SearchTenantPlan::ScopedValue {
                    property: name("tenant_id"),
                    value: tenant,
                },
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("tenant text search succeeds"),
    )
}

/// Executes DDL and returns its exact durable operation ID.
async fn execute_ddl(db: &HelixDB, plan: &exec::ExecutablePlan) -> IndexOperationId {
    let result = db
        .execute(plan, context::ParamBindings::default())
        .await
        .expect("fixture DDL is durably accepted");
    let Some(ExecutionValue::IndexDdlReceipt(receipt)) = result.last else {
        panic!("fixture DDL returns one receipt");
    };
    match receipt {
        IndexDdlReceipt::Accepted { operation_id, .. }
        | IndexDdlReceipt::ExistingOperation { operation_id } => operation_id,
        IndexDdlReceipt::AlreadyActive { .. } => {
            panic!("fresh or recreated fixture DDL is not already Active")
        }
    }
}

/// Executes one accepted DDL operation through terminal success and refreshes catalog state.
async fn execute_ddl_to_success(db: &HelixDB, plan: &exec::ExecutablePlan) {
    let operation_id = execute_ddl(db, plan).await;
    wait_for_terminal(db, operation_id, ExpectedTerminal::Succeeded).await;
    db.planner_context_scoped(context::ParamBindings::default(), DataScope::LegacyUnscoped)
        .await
        .expect("terminal DDL is visible through the refreshed catalog");
}

/// Terminal state expected by one bounded operation waiter.
#[derive(Debug, Clone, Copy)]
enum ExpectedTerminal {
    Blocked,
    Succeeded,
    Aborted,
}

/// Waits for one exact durable operation state.
async fn wait_for_terminal(
    db: &HelixDB,
    operation_id: IndexOperationId,
    expected: ExpectedTerminal,
) -> IndexOperationStatus {
    let started = Instant::now();
    loop {
        let status = db
            .get_index_operation(DataScope::LegacyUnscoped, operation_id)
            .await
            .expect("fixture operation remains readable");
        let reached = matches!(
            (expected, &status),
            (
                ExpectedTerminal::Blocked,
                IndexOperationStatus::Blocked { .. }
            ) | (
                ExpectedTerminal::Succeeded,
                IndexOperationStatus::Succeeded { .. }
            ) | (
                ExpectedTerminal::Aborted,
                IndexOperationStatus::Aborted { .. }
            )
        );
        if reached {
            return status;
        }
        assert!(
            matches!(
                status,
                IndexOperationStatus::Queued { .. } | IndexOperationStatus::Running { .. }
            ),
            "fixture operation reached an unexpected terminal state: {status:?}"
        );
        assert!(
            started.elapsed() < OPERATION_TIMEOUT,
            "fixture operation did not reach {expected:?} within five minutes: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Returns a policy whose first non-empty source row triggers a typed block.
fn blocked_limit_config() -> DbConfig {
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
        .expect("blocked text limits remain internally consistent"),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .expect("blocked text policy preserves cross-budget invariants");
    lifecycle_db_config().with_search_index_backfill_limits(limits)
}

/// Keeps the lifecycle model independent from SlateDB's 15-minute compactor checkpoints.
fn lifecycle_db_config() -> DbConfig {
    let slate = slatedb::Settings {
        compactor_options: None,
        l0_max_ssts: 1_024,
        l0_max_ssts_per_key: 1_024,
        ..slatedb::Settings::default()
    };
    DbConfig::new().with_slate_settings(slate)
}

/// Drives the complete text lifecycle against its independent reference model.
#[tokio::test]
async fn public_text_lifecycle_matches_reference_and_durable_row_models() {
    let mut machine = TextMachine::open().await;
    for action in [
        TextAction::Insert {
            slot: EntitySlot::First,
            text: "textmodelalpha",
        },
        TextAction::Create,
        TextAction::RejectInvalidValue {
            slot: EntitySlot::First,
        },
        TextAction::Search {
            term: "textmodelalpha",
        },
        TextAction::Insert {
            slot: EntitySlot::Second,
            text: "textmodelbeta",
        },
        TextAction::Search {
            term: "textmodelbeta",
        },
        TextAction::Update {
            slot: EntitySlot::First,
            text: "textmodelgamma",
        },
        TextAction::Search {
            term: "textmodelalpha",
        },
        TextAction::Search {
            term: "textmodelgamma",
        },
        TextAction::Delete {
            slot: EntitySlot::Second,
        },
        TextAction::Search {
            term: "textmodelbeta",
        },
        TextAction::Reopen,
        TextAction::Search {
            term: "textmodelgamma",
        },
        TextAction::Drop,
        TextAction::Recreate,
        TextAction::Search {
            term: "textmodelgamma",
        },
        TextAction::Drop,
        TextAction::RetryAfterHigherLimit,
        TextAction::AbortBlockedBuild,
    ] {
        machine.apply(action).await;
    }
    machine
        .db
        .close()
        .await
        .expect("text lifecycle writer closes cleanly");
}

#[tokio::test]
async fn public_managed_text_search_isolates_tenant_partitions() {
    const TENANT_LABEL: &str = "TenantTextDocument";
    const SHARED_TOKEN: &str = "sharedtenanttoken";

    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-text-tenant-partitions".to_owned(),
    })
    .await
    .expect("tenant text fixture opens");
    let acme_id = created_node_id(
        db.execute(
            &add_node_plan(
                TENANT_LABEL,
                vec![
                    ("tenant_id", PropertyValue::from("acme")),
                    (PROPERTY, PropertyValue::from(SHARED_TOKEN)),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("acme text commits"),
    );
    let globex_id = created_node_id(
        db.execute(
            &add_node_plan(
                TENANT_LABEL,
                vec![
                    ("tenant_id", PropertyValue::from("globex")),
                    (PROPERTY, PropertyValue::from(SHARED_TOKEN)),
                ],
            ),
            context::ParamBindings::default(),
        )
        .await
        .expect("globex text commits"),
    );
    execute_ddl_to_success(
        &db,
        &text_create_plan_with_scope(
            TENANT_LABEL,
            PROPERTY,
            catalog::SearchIndexScope::Tenant {
                property: name("tenant_id"),
            },
        ),
    )
    .await;

    let literal =
        ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(PropertyValue::from("acme")))
            .expect("literal text tenant is non-null");
    assert_eq!(
        projected_node_ids(
            db.execute(
                &text_search_plan_with_tenant(
                    TENANT_LABEL,
                    PROPERTY,
                    SHARED_TOKEN,
                    ir::SearchTenantPlan::ScopedValue {
                        property: name("tenant_id"),
                        value: literal,
                    },
                ),
                context::ParamBindings::default(),
            )
            .await
            .expect("literal text tenant search succeeds"),
        ),
        vec![acme_id]
    );

    let tenant_param = name("tenant");
    let expression = ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Expr(
        ir::PropertyInputExprPlan::new(Expr::param(tenant_param.as_ref()))
            .expect("text tenant parameter expression is valid"),
    ))
    .expect("runtime text tenant expression is valid");
    assert_eq!(
        projected_node_ids(
            db.execute(
                &text_search_plan_with_tenant(
                    TENANT_LABEL,
                    PROPERTY,
                    SHARED_TOKEN,
                    ir::SearchTenantPlan::ScopedValue {
                        property: name("tenant_id"),
                        value: expression,
                    },
                ),
                context::ParamBindings::default()
                    .with_value(tenant_param, PropertyValue::from("globex")),
            )
            .await
            .expect("runtime text tenant search succeeds"),
        ),
        vec![globex_id]
    );

    let moving_node = name("moving_node");
    db.execute(
        &node_mutation_plan(
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
    .expect("text tenant move commits");
    assert!(
        search_node_ids_in_tenant(&db, TENANT_LABEL, SHARED_TOKEN, "acme")
            .await
            .is_empty()
    );
    let mut globex_after_move =
        search_node_ids_in_tenant(&db, TENANT_LABEL, SHARED_TOKEN, "globex").await;
    globex_after_move.sort_unstable();
    assert_eq!(globex_after_move, vec![acme_id, globex_id]);

    let error = db
        .execute(
            &node_mutation_plan(
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
        .expect_err("removing a required text tenant fails closed");
    assert_eq!(error.index_error_code(), Some("invalid_index_source_data"));
    assert!(
        matches!(
            &error,
            db::error::HelixDbError::InvalidIndexSourceData { reason }
                if reason.contains("indexed document is missing its tenant property")
        ),
        "{error}"
    );
    assert!(
        search_node_ids_in_tenant(&db, TENANT_LABEL, SHARED_TOKEN, "acme")
            .await
            .is_empty()
    );
    let mut globex_after_rejected_removal =
        search_node_ids_in_tenant(&db, TENANT_LABEL, SHARED_TOKEN, "globex").await;
    globex_after_rejected_removal.sort_unstable();
    assert_eq!(globex_after_rejected_removal, vec![acme_id, globex_id]);

    let retry_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match db
            .execute(
                &node_mutation_plan(
                    moving_node.clone(),
                    exec::ExecMutationPlan::SetProperty {
                        name: name("tenant_id"),
                        value: ir::PropertyInputPlan::Value(PropertyValue::from("acme")),
                    },
                ),
                context::ParamBindings::default().with_value(
                    moving_node.clone(),
                    PropertyValue::I64(i64::try_from(acme_id).expect("fixture node ID fits i64")),
                ),
            )
            .await
        {
            Ok(_) => break,
            Err(error) if error.is_transaction_conflict() && Instant::now() < retry_deadline => {
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("text tenant move back failed: {error}"),
        }
    }
    assert_eq!(
        search_node_ids_in_tenant(&db, TENANT_LABEL, SHARED_TOKEN, "acme").await,
        vec![acme_id]
    );
    assert_eq!(
        search_node_ids_in_tenant(&db, TENANT_LABEL, SHARED_TOKEN, "globex").await,
        vec![globex_id]
    );

    for (tenant, expected) in [
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
            ir::SearchTenantPlan::ScopedValue {
                property: name("workspace_id"),
                value: ir::SearchTenantValuePlan::new(ir::PropertyInputPlan::Value(
                    PropertyValue::from("acme"),
                ))
                .expect("wrong-property text tenant remains structurally valid"),
            },
            "is scoped by 'tenant_id' not 'workspace_id'",
        ),
    ] {
        let error = db
            .execute(
                &text_search_plan_with_tenant(TENANT_LABEL, PROPERTY, SHARED_TOKEN, tenant),
                context::ParamBindings::default(),
            )
            .await
            .expect_err("invalid text tenant shape fails closed");
        assert!(error.to_string().contains(expected), "{error}");
    }
    db.close().await.expect("tenant text fixture closes");
}
