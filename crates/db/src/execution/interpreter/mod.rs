//! Executable-plan interpreter.
//!
//! This interpreter consumes [`helix_planner::exec::ExecutablePlan`] directly.
//! It does not choose access paths, rewrite predicates, or infer dependencies;
//! those are planner responsibilities encoded in the executable DAG.

mod access;
mod control;
mod ddl;
mod dependencies;
mod dispatch;
mod mutation;
pub(crate) mod read_view;
mod reserved;
mod row_mode;
mod runtime_context;
mod scheduler;
mod shortest_path;
mod state;
mod storage;
mod stream;
mod subplan;
#[cfg(test)]
mod test_support;
mod types;

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/interpreter_contracts.rs"]
pub mod production_contracts;

pub use types::{
    ElementRef, ExecutionResult, ExecutionRow, ExecutionScalar, ExecutionValue, FoldedStream,
    RowPath, RowSack, RowVirtualProperties,
};
use types::{ExecutionValueSlot, ExecutionValueStore};

use bytes::Bytes;
use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_planner::{context, exec, ir};

use self::runtime_context::ExecutionContext;
use crate::encoding::keys;
use crate::encoding::keys::tenant::DataScope;
use crate::encoding::property::decode_properties;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::encoding::property::Property;
use crate::encoding::v1::values;
use crate::error::{HelixDbError, Result};
use crate::HelixDB;

const WRITE_RECEIPT_GC_SCAN_LIMIT: usize = 64;

/// Step-by-step executor for planner executable IR.
pub struct Interpreter<'db> {
    db: &'db HelixDB,
    ctx: ExecutionContext<'db>,
}

/// Prepared idempotent write that has not crossed its durable commit boundary.
pub(crate) struct PendingWrite<'db> {
    ctx: ExecutionContext<'db>,
}

impl PendingWrite<'_> {
    /// Stages the exact public response beside the graph mutation.
    pub(crate) fn stage_receipt(
        &mut self,
        key: Bytes,
        receipt: &values::write_receipt::WriteReceiptValue,
    ) -> Result<()> {
        self.ctx.stage_request_write_receipt(key, receipt.encode())
    }

    /// Commits the graph mutation and receipt as one serializable transaction.
    pub(crate) async fn commit(mut self) -> Result<()> {
        self.ctx.commit_request_write_scope().await
    }
}

/// Result of preparing an idempotent write request.
pub(crate) enum PreparedWrite<'db> {
    /// A current receipt already proves this request committed.
    Replayed(values::write_receipt::WriteReceiptValue),
    /// The query executed inside an uncommitted request transaction.
    Pending {
        transaction: Box<PendingWrite<'db>>,
        result: ExecutionResult,
    },
}

impl<'db> Interpreter<'db> {
    /// Create an interpreter for one request.
    pub fn new(db: &'db HelixDB, params: context::ParamBindings) -> Self {
        Self::new_scoped(db, params, DataScope::LegacyUnscoped)
    }

    /// Create an interpreter for one tenant-scoped request.
    pub fn new_scoped(
        db: &'db HelixDB,
        params: context::ParamBindings,
        tenant_scope: DataScope,
    ) -> Self {
        Self::new_scoped_controlled(
            db,
            params,
            tenant_scope,
            crate::execution_control::ExecutionControl::unlimited(),
        )
    }

    /// Create an interpreter with request-scoped monotonic cancellation.
    pub fn new_scoped_controlled(
        db: &'db HelixDB,
        params: context::ParamBindings,
        tenant_scope: DataScope,
        execution_control: crate::execution_control::ExecutionControl,
    ) -> Self {
        Self {
            db,
            ctx: ExecutionContext::new_scoped_controlled(
                db,
                params,
                tenant_scope,
                execution_control,
            ),
        }
    }

    /// Create an interpreter coupled to the catalog observation used for planning.
    pub(crate) fn new_scoped_controlled_prepared(
        db: &'db HelixDB,
        params: context::ParamBindings,
        tenant_scope: DataScope,
        execution_control: crate::execution_control::ExecutionControl,
        proof: crate::CatalogRefreshProof,
    ) -> Self {
        let catalog_freshness = if db.catalog_refresh_proof_belongs_to(&proof, tenant_scope) {
            runtime_context::PendingCatalogFreshness::Prepared(proof)
        } else {
            runtime_context::PendingCatalogFreshness::Unverified
        };
        Self {
            db,
            ctx: ExecutionContext::new_scoped_controlled_with_catalog_freshness(
                db,
                params,
                tenant_scope,
                execution_control,
                catalog_freshness,
            ),
        }
    }

    /// Execute a validated executable plan.
    pub async fn execute(mut self, plan: &exec::ExecutablePlan) -> Result<ExecutionResult> {
        self.ctx.check_execution_deadline()?;
        let request_mode = RequestExecutionMode::try_from(plan)?;
        match request_mode {
            RequestExecutionMode::Read | RequestExecutionMode::IndexDdl => {
                self.ctx.enable_request_read_view().await?
            }
            RequestExecutionMode::GraphWrite => {
                self.ensure_writer()?;
                self.ctx.enable_request_write_scope().await?;
            }
        }

        if let Err(err) = self.ctx.check_execution_deadline() {
            self.ctx.abort_request_write_scope();
            return Err(err);
        }

        let result = self
            .ctx
            .execute_steps(plan.steps(), plan.execution_order(), plan.root())
            .await;
        if let Err(err) = result {
            self.ctx.abort_request_write_scope();
            return Err(err);
        }

        if let Err(err) = self.ctx.check_execution_deadline() {
            self.ctx.abort_request_write_scope();
            return Err(err);
        }

        if request_mode == RequestExecutionMode::GraphWrite
            && let Err(err) = self.ctx.commit_request_write_scope().await
        {
            self.ctx.abort_request_write_scope();
            return Err(err);
        }
        if request_mode == RequestExecutionMode::Read
            && let Err(error) = self.ctx.validate_request_read_view()
        {
            return Err(error);
        }
        let result = self.ctx.finish(plan.root(), plan.returns());
        if result.is_ok()
            && matches!(
                request_mode,
                RequestExecutionMode::Read | RequestExecutionMode::IndexDdl
            )
        {
            self.ctx.close_request_read_view()?;
        }
        result
    }

    /// Executes a write plan but leaves its transaction open so the caller can
    /// serialize and stage the public response before the durable commit.
    pub(crate) async fn prepare_idempotent_write(
        mut self,
        plan: &exec::ExecutablePlan,
        receipt_key: &[u8],
        now_unix_ms: u64,
    ) -> Result<PreparedWrite<'db>> {
        if plan.kind() != ir::PlanKind::Write {
            return Err(HelixDbError::InvariantViolation(
                "idempotent write preparation requires a write plan".to_string(),
            ));
        }
        self.ctx.check_execution_deadline()?;
        self.ensure_writer()?;
        self.ctx.enable_request_write_scope().await?;

        let existing = match self.ctx.request_write_receipt(receipt_key).await {
            Ok(existing) => existing,
            Err(error) => {
                self.ctx.abort_request_write_scope();
                return Err(error);
            }
        };
        if let Some(existing) = existing {
            let receipt = match values::write_receipt::WriteReceiptValue::decode(&existing) {
                Ok(receipt) => receipt,
                Err(error) => {
                    self.ctx.abort_request_write_scope();
                    return Err(error.into());
                }
            };
            if receipt.expires_at_unix_ms() > now_unix_ms {
                self.ctx.abort_request_write_scope();
                return Ok(PreparedWrite::Replayed(receipt));
            }
        }
        let receipt_prefix =
            keys::metadata::write_receipt_scan_prefix_scoped(self.ctx.tenant_scope);
        if let Err(error) = self
            .ctx
            .prune_expired_write_receipts(&receipt_prefix, now_unix_ms, WRITE_RECEIPT_GC_SCAN_LIMIT)
            .await
        {
            self.ctx.abort_request_write_scope();
            return Err(error);
        }

        if let Err(error) = self.ctx.check_execution_deadline() {
            self.ctx.abort_request_write_scope();
            return Err(error);
        }
        if let Err(error) = self
            .ctx
            .execute_steps(plan.steps(), plan.execution_order())
            .await
        {
            self.ctx.abort_request_write_scope();
            return Err(error);
        }
        if let Err(error) = self.ctx.check_execution_deadline() {
            self.ctx.abort_request_write_scope();
            return Err(error);
        }
        let result = match self.ctx.finish(plan.root(), plan.returns()) {
            Ok(result) => result,
            Err(error) => {
                self.ctx.abort_request_write_scope();
                return Err(error);
            }
        };
        Ok(PreparedWrite::Pending {
            transaction: Box::new(PendingWrite { ctx: self.ctx }),
            result,
        })
    }
}

/// Request-owned storage boundary derived from the validated executable plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestExecutionMode {
    Read,
    GraphWrite,
    IndexDdl,
}

/// Side effects reachable from one executable operation, including subplans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestSideEffects {
    None,
    GraphMutation,
    IndexDdl,
    Mixed,
}

impl RequestSideEffects {
    const fn combine(self, other: Self) -> Self {
        match (self, other) {
            (Self::None, effect) | (effect, Self::None) => effect,
            (Self::GraphMutation, Self::GraphMutation) => Self::GraphMutation,
            (Self::IndexDdl, Self::IndexDdl) => Self::IndexDdl,
            (Self::Mixed, _)
            | (_, Self::Mixed)
            | (Self::GraphMutation, Self::IndexDdl)
            | (Self::IndexDdl, Self::GraphMutation) => Self::Mixed,
        }
    }

    fn subplan(plan: &exec::ExecutableSubplan) -> Self {
        plan.steps().iter().fold(Self::None, |effects, step| {
            effects.combine(Self::operation(&step.op))
        })
    }

    fn operation(operation: &exec::ExecOp) -> Self {
        match operation {
            exec::ExecOp::Mutation { .. } => Self::GraphMutation,
            exec::ExecOp::IndexDdl {
                plan: ir::IndexDdlPlan::GetOperation { .. },
            } => Self::None,
            exec::ExecOp::IndexDdl {
                plan:
                    ir::IndexDdlPlan::Create { .. }
                    | ir::IndexDdlPlan::Drop { .. }
                    | ir::IndexDdlPlan::RetryOperation { .. }
                    | ir::IndexDdlPlan::AbortOperation { .. },
            } => Self::IndexDdl,
            exec::ExecOp::Branch { plan } => match plan {
                exec::ExecBranchPlan::Union(plans) => {
                    plans.iter().fold(Self::None, |effects, plan| {
                        effects.combine(Self::subplan(plan))
                    })
                }
                exec::ExecBranchPlan::Coalesce(plans) => {
                    plans.iter().fold(Self::None, |effects, plan| {
                        effects.combine(Self::subplan(plan))
                    })
                }
                exec::ExecBranchPlan::Choose { then_plan, .. } => Self::subplan(then_plan),
                exec::ExecBranchPlan::ChooseElse {
                    then_plan,
                    else_plan,
                    ..
                } => Self::subplan(then_plan).combine(Self::subplan(else_plan)),
                exec::ExecBranchPlan::Optional(plan) => Self::subplan(plan),
            },
            exec::ExecOp::Repeat { plan } => Self::subplan(&plan.body),
            exec::ExecOp::ForEach { body, .. } => Self::subplan(body),
            exec::ExecOp::Access { .. }
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
            | exec::ExecOp::ShortestPath { .. }
            | exec::ExecOp::Merge { .. }
            | exec::ExecOp::Reserved { .. }
            | exec::ExecOp::Barrier { .. }
            | exec::ExecOp::Noop => Self::None,
        }
    }
}

impl TryFrom<&exec::ExecutablePlan> for RequestExecutionMode {
    type Error = HelixDbError;

    fn try_from(plan: &exec::ExecutablePlan) -> Result<Self> {
        let side_effects = plan
            .steps()
            .iter()
            .fold(RequestSideEffects::None, |effects, step| {
                effects.combine(RequestSideEffects::operation(&step.op))
            });
        match (plan.kind(), side_effects) {
            (ir::PlanKind::Read, RequestSideEffects::None) => Ok(Self::Read),
            (ir::PlanKind::Write, RequestSideEffects::None | RequestSideEffects::GraphMutation) => {
                Ok(Self::GraphWrite)
            }
            (ir::PlanKind::Write, RequestSideEffects::IndexDdl) => Ok(Self::IndexDdl),
            (ir::PlanKind::Read, _) | (ir::PlanKind::Write, RequestSideEffects::Mixed) => {
                Err(HelixDbError::InvariantViolation(
                    "validated executable plan mixes incompatible request transaction modes"
                        .to_string(),
                ))
            }
        }
    }
}

#[cfg(test)]
mod request_execution_mode_tests {
    use super::*;

    fn mutation() -> exec::ExecOp {
        exec::ExecOp::Mutation {
            plan: exec::ExecMutationPlan::AddNodeSource {
                label: test_support::name("User"),
                properties: test_support::assignments(Vec::new()),
            },
        }
    }

    fn index_ddl() -> exec::ExecOp {
        exec::ExecOp::IndexDdl {
            plan: ir::IndexDdlPlan::RetryOperation {
                operation_id: ir::IndexOperationId::try_new("07070707-0707-0707-0707-070707070707")
                    .unwrap(),
            },
        }
    }

    fn index_status() -> exec::ExecOp {
        exec::ExecOp::IndexDdl {
            plan: ir::IndexDdlPlan::GetOperation {
                operation_id: ir::IndexOperationId::try_new("07070707-0707-0707-0707-070707070707")
                    .unwrap(),
            },
        }
    }

    fn for_each(body: exec::ExecutableSubplan) -> exec::ExecOp {
        exec::ExecOp::ForEach {
            param: test_support::name("items"),
            body: Box::new(body),
        }
    }

    #[test]
    fn nested_index_ddl_uses_operation_owned_catalog_transaction_mode() {
        let body = test_support::subplan(vec![test_support::step(1, Vec::new(), index_ddl())], 1);
        let plan = test_support::executable(
            ir::PlanKind::Write,
            vec![test_support::step(1, Vec::new(), for_each(body))],
            1,
        );

        assert_eq!(
            RequestExecutionMode::try_from(&plan).unwrap(),
            RequestExecutionMode::IndexDdl
        );
    }

    #[test]
    fn nested_graph_mutation_uses_request_write_transaction_mode() {
        let body = test_support::subplan(vec![test_support::step(1, Vec::new(), mutation())], 1);
        let plan = test_support::executable(
            ir::PlanKind::Write,
            vec![test_support::step(1, Vec::new(), for_each(body))],
            1,
        );

        assert_eq!(
            RequestExecutionMode::try_from(&plan).unwrap(),
            RequestExecutionMode::GraphWrite
        );
    }

    #[test]
    fn nested_mixed_writes_are_rejected_before_any_transaction_opens() {
        let first = exec::ExecStepId::new(1).unwrap();
        let body = test_support::subplan(
            vec![
                test_support::step(1, Vec::new(), mutation()),
                test_support::step(2, vec![first], index_ddl()),
            ],
            2,
        );
        let plan = test_support::executable(
            ir::PlanKind::Write,
            vec![test_support::step(1, Vec::new(), for_each(body))],
            1,
        );

        assert!(matches!(
            RequestExecutionMode::try_from(&plan),
            Err(HelixDbError::InvariantViolation(message))
                if message.contains("incompatible request transaction modes")
        ));
    }

    #[test]
    fn read_plan_cannot_hide_a_nested_mutation() {
        let body = test_support::subplan(vec![test_support::step(1, Vec::new(), mutation())], 1);
        let plan = test_support::executable(
            ir::PlanKind::Read,
            vec![test_support::step(1, Vec::new(), for_each(body))],
            1,
        );

        assert!(matches!(
            RequestExecutionMode::try_from(&plan),
            Err(HelixDbError::InvariantViolation(_))
        ));
    }

    #[test]
    fn index_operation_status_remains_a_read_request() {
        let plan = test_support::executable(
            ir::PlanKind::Read,
            vec![test_support::step(1, Vec::new(), index_status())],
            1,
        );

        assert_eq!(
            RequestExecutionMode::try_from(&plan).unwrap(),
            RequestExecutionMode::Read
        );
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[tokio::test]
    async fn elapsed_deadline_aborts_write_before_any_mutation() {
        let db = test_support::open_db("deadline-precommit-abort").await;
        let plan = test_support::executable(
            ir::PlanKind::Write,
            vec![test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: test_support::name("User"),
                        properties: test_support::assignments(Vec::new()),
                    },
                },
            )],
            1,
        );

        let error = db
            .execute_scoped_controlled(
                &plan,
                context::ParamBindings::default(),
                DataScope::LegacyUnscoped,
                crate::execution_control::ExecutionControl::from_timeout(std::time::Duration::ZERO),
            )
            .await
            .expect_err("expired write must not execute");
        assert!(matches!(error, HelixDbError::QueryDeadlineExceeded));

        let node_key = keys::Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(0)),
        }
        .to_bytes();
        assert!(db.inner_db().get(node_key).await.unwrap().is_none());
    }
}

#[cfg(test)]
mod cutover_tests {
    use helix_ast::value::PropertyValue;

    use super::*;

    #[tokio::test]
    async fn graph_write_records_vector_build_delta_in_its_graph_transaction() {
        let db = test_support::open_db("mutation-v2-vector-build-delta").await;
        let definition = crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
            crate::config::VectorIndexDefinition::new_node(
                "User",
                "embedding",
                3,
                crate::search::vector::VectorDistanceMetric::Cosine,
            )
            .expect("vector definition"),
        )
        .expect("validated vector definition");
        let source_upper_bound = crate::index_lifecycle::IndexCursor::try_new(
            keys::Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(0)),
            }
            .to_bytes(),
        )
        .expect("typed source cursor");
        let crate::HelixStorage::Writer(writer) = db.storage() else {
            panic!("test database is a writer");
        };
        let receipt = crate::index_lifecycle::lifecycle::create_index_operation(
            writer.db(),
            DataScope::LegacyUnscoped,
            definition,
            ir::IndexCreateMode::ErrorIfExists,
            crate::index_lifecycle::lifecycle::InitialBuildProgress::vector(source_upper_bound),
        )
        .await
        .expect("vector build fixture is enqueued");
        let crate::index_lifecycle::IndexDdlReceipt::Accepted {
            index_id,
            generation,
            ..
        } = receipt
        else {
            panic!("new vector build must be accepted");
        };
        let plan = test_support::executable(
            ir::PlanKind::Write,
            vec![test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: test_support::name("User"),
                        properties: test_support::assignments(vec![
                            ("email", PropertyValue::from("indexed@example.com")),
                            ("embedding", PropertyValue::F32Array(vec![1.0, 0.0, 0.0])),
                        ]),
                    },
                },
            )],
            1,
        );

        db.execute(&plan, context::ParamBindings::default())
            .await
            .expect("graph mutation and vector delta commit together");

        let delta_key = crate::encoding::v2::keys::Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v2::keys::ScopedKey::BuildDelta(
                crate::encoding::v2::keys::IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: crate::encoding::v2::keys::IndexEntity {
                        kind: crate::index_lifecycle::IndexElementKind::Node,
                        id: crate::index_lifecycle::IndexEntityId::new(0),
                    },
                },
            ),
        }
        .to_bytes();
        assert!(db.inner_db().get(delta_key).await.unwrap().is_some());

        let allocator_key = keys::Key::Global {
            kind: keys::GlobalKeyKind::Metadata(keys::metadata::MetadataKey::next_node_id_key()),
        }
        .to_bytes();
        assert!(db.inner_db().get(&allocator_key).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn graph_write_records_text_build_delta_in_its_graph_transaction() {
        let db = test_support::open_db("mutation-v2-text-build-delta").await;
        let definition = crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
            crate::config::TextIndexDefinition::new_node("User", "bio").expect("text definition"),
        )
        .expect("validated text definition");
        let source_upper_bound = crate::index_lifecycle::IndexCursor::try_new(
            keys::Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(0)),
            }
            .to_bytes(),
        )
        .expect("typed source cursor");
        let crate::HelixStorage::Writer(writer) = db.storage() else {
            panic!("test database is a writer");
        };
        let receipt = crate::index_lifecycle::lifecycle::create_index_operation(
            writer.db(),
            DataScope::LegacyUnscoped,
            definition,
            ir::IndexCreateMode::ErrorIfExists,
            crate::index_lifecycle::lifecycle::InitialBuildProgress::text(source_upper_bound),
        )
        .await
        .expect("text build fixture is enqueued");
        let crate::index_lifecycle::IndexDdlReceipt::Accepted {
            index_id,
            generation,
            ..
        } = receipt
        else {
            panic!("new text build must be accepted");
        };
        let plan = test_support::executable(
            ir::PlanKind::Write,
            vec![test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: test_support::name("User"),
                        properties: test_support::assignments(vec![(
                            "bio",
                            PropertyValue::from("coalesced text delta"),
                        )]),
                    },
                },
            )],
            1,
        );

        db.execute(&plan, context::ParamBindings::default())
            .await
            .expect("graph mutation and text delta commit together");

        let delta_key = crate::encoding::v2::keys::Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v2::keys::ScopedKey::BuildDelta(
                crate::encoding::v2::keys::IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: crate::encoding::v2::keys::IndexEntity {
                        kind: crate::index_lifecycle::IndexElementKind::Node,
                        id: crate::index_lifecycle::IndexEntityId::new(0),
                    },
                },
            ),
        }
        .to_bytes();
        let value = db
            .inner_db()
            .get(delta_key)
            .await
            .expect("text delta read succeeds")
            .expect("text delta committed with graph row");
        let delta =
            crate::encoding::v2::values::decode_build_delta(&value).expect("text delta decodes");
        assert_eq!(delta.index_id, index_id);
        assert_eq!(delta.generation, generation);
        assert_eq!(
            delta.entity_kind,
            crate::index_lifecycle::IndexElementKind::Node
        );
        assert_eq!(
            delta.entity_id,
            crate::index_lifecycle::IndexEntityId::new(0)
        );
    }
}
