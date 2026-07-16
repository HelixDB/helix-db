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
mod read_view;
mod reserved;
mod row_mode;
mod runtime_context;
mod scheduler;
mod search_index;
mod shortest_path;
mod state;
mod storage;
mod stream;
mod subplan;
#[cfg(test)]
mod test_support;
mod types;

pub use types::{
    ElementRef, ExecutionResult, ExecutionRow, ExecutionScalar, ExecutionValue, FoldedStream,
    RowPath, RowSack, RowVirtualProperties,
};

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

/// Step-by-step executor for planner executable IR.
pub struct Interpreter<'db> {
    db: &'db HelixDB,
    ctx: ExecutionContext<'db>,
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
        Self {
            db,
            ctx: ExecutionContext::new_scoped(db, params, tenant_scope),
        }
    }

    /// Execute a validated executable plan.
    pub async fn execute(mut self, plan: &exec::ExecutablePlan) -> Result<ExecutionResult> {
        match plan.kind() {
            ir::PlanKind::Read => self.ctx.enable_request_read_view().await?,
            ir::PlanKind::Write => {
                self.ensure_writer()?;
                self.ctx.enable_request_write_scope().await?;
            }
        }

        let result = self
            .ctx
            .execute_steps(plan.steps(), plan.execution_order())
            .await;
        if let Err(err) = result {
            self.ctx.abort_request_write_scope();
            self.ctx.release_index_read_leases().await;
            return Err(err);
        }

        if plan.kind() == ir::PlanKind::Write
            && let Err(err) = self.ctx.commit_request_write_scope().await
        {
            self.ctx.abort_request_write_scope();
            self.ctx.release_index_read_leases().await;
            return Err(err);
        }
        if plan.kind() == ir::PlanKind::Read
            && let Err(error) = self.ctx.validate_request_read_view()
        {
            self.ctx.release_index_read_leases().await;
            return Err(error);
        }
        self.ctx.validate_and_release_index_read_leases().await?;

        self.ctx.finish(plan.root(), plan.returns())
    }
}

#[cfg(test)]
mod cutover_tests {
    use helix_ast::value::PropertyValue;

    use super::*;

    #[tokio::test]
    async fn graph_write_records_vector_build_delta_in_its_graph_transaction() {
        let db = test_support::open_db("mutation-v2-vector-build-delta").await;
        let definition = crate::index_v2::ValidatedDynamicIndexDefinition::try_from(
            crate::config::VectorIndexDefinition::new_node(
                "User",
                "embedding",
                3,
                crate::search::vector::VectorDistanceMetric::Cosine,
            )
            .expect("vector definition"),
        )
        .expect("validated vector definition");
        let source_upper_bound = crate::index_v2::IndexCursor::try_new(
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
        let receipt = crate::index_v2::lifecycle::create_index_operation(
            writer.db(),
            DataScope::LegacyUnscoped,
            definition,
            ir::IndexCreateMode::ErrorIfExists,
            crate::index_v2::lifecycle::InitialBuildProgress::vector(source_upper_bound),
        )
        .await
        .expect("vector build fixture is enqueued");
        let crate::index_v2::IndexDdlReceipt::Accepted {
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

        let delta_key = keys::Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: keys::DataKeyKind::IndexV2(keys::index_v2::IndexV2Key::BuildDelta(
                keys::index_v2::IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: keys::index_v2::IndexEntity {
                        kind: crate::index_v2::IndexElementKind::Node,
                        id: crate::index_v2::IndexEntityId::new(0),
                    },
                },
            )),
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
        let definition = crate::index_v2::ValidatedDynamicIndexDefinition::try_from(
            crate::config::TextIndexDefinition::new_node("User", "bio").expect("text definition"),
        )
        .expect("validated text definition");
        let source_upper_bound = crate::index_v2::IndexCursor::try_new(
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
        let receipt = crate::index_v2::lifecycle::create_index_operation(
            writer.db(),
            DataScope::LegacyUnscoped,
            definition,
            ir::IndexCreateMode::ErrorIfExists,
            crate::index_v2::lifecycle::InitialBuildProgress::text(source_upper_bound),
        )
        .await
        .expect("text build fixture is enqueued");
        let crate::index_v2::IndexDdlReceipt::Accepted {
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

        let delta_key = keys::Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: keys::DataKeyKind::IndexV2(keys::index_v2::IndexV2Key::BuildDelta(
                keys::index_v2::IndexEntityStateKey {
                    index_id,
                    generation,
                    entity: keys::index_v2::IndexEntity {
                        kind: crate::index_v2::IndexElementKind::Node,
                        id: crate::index_v2::IndexEntityId::new(0),
                    },
                },
            )),
        }
        .to_bytes();
        let value = db
            .inner_db()
            .get(delta_key)
            .await
            .expect("text delta read succeeds")
            .expect("text delta committed with graph row");
        let values::index_v2::IndexV2WorkValue::CoalescedBuildDelta(delta) =
            values::index_v2::decode_work_value(&value).expect("text delta decodes")
        else {
            panic!("text delta key contains its typed coalesced value");
        };
        assert_eq!(delta.index_id, index_id);
        assert_eq!(delta.generation, generation);
        assert_eq!(delta.entity_kind, crate::index_v2::IndexElementKind::Node);
        assert_eq!(delta.entity_id, crate::index_v2::IndexEntityId::new(0));
    }
}
