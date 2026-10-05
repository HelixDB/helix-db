//! Request-owned write transaction lifecycle for executable mutations.
//!
//! [`RequestWriteScopeState`] makes it impossible to retain an active request
//! transaction without its catalog snapshot and transaction-local vector-cache
//! writes. Canonical V2 generations remain in the transaction-owned family
//! mutation sets.
//!
//! Membership sets resolved in the request transaction stay cached across its
//! writes: each operation forgets only the sets whose node index footprint
//! its writes reached. Opening, committing, or aborting the transaction, and
//! isolated mutation scopes, forget every set. Committed pending search sets
//! ([`super::super::access::PendingSets`]) stay exact across every write of
//! the transaction and are forgotten at those same points.

use std::sync::Arc;

use slatedb::{DbTransaction, IsolationLevel};

use super::super::runtime_context::{
    ActiveWriteTx, PendingCatalogFreshness, RequestWriteScopeState,
};
use super::*;

/// Temporarily extracted mutation state returned after one operation finishes.
pub(super) struct MutationWriteScope {
    pub(super) txn: DbTransaction,
    pub(super) index_context: MutationIndexContext,
    request_scoped: bool,
}

impl<'db> ExecutionContext<'db> {
    /// Releases a planning-only catalog permit before operation-owned DDL.
    pub(in crate::execution::interpreter) fn discard_pending_catalog_freshness(&mut self) {
        self.pending_catalog_freshness = PendingCatalogFreshness::Consumed;
        self.release_read_catalog_permit_for_ddl();
    }

    /// Opens the request transaction before the first plan step.
    ///
    /// The transaction, catalog snapshot, and cache write set enter the request
    /// state together, so callers cannot observe partially initialized state.
    pub(in crate::execution::interpreter) async fn enable_request_write_scope(
        &mut self,
    ) -> Result<()> {
        assert!(
            matches!(self.request_write_scope, RequestWriteScopeState::Disabled),
            "a request write scope can only be enabled once"
        );
        let (txn, index_context) = self.begin_write_tx().await?;
        // Sets read before the transaction came from another snapshot.
        self.prepared_memberships.clear();
        self.pending_sets = Arc::default();
        self.request_write_scope =
            RequestWriteScopeState::Active(Box::new(ActiveWriteTx { txn, index_context }));
        Ok(())
    }

    /// Drops any active transaction to abort the write request.
    pub(in crate::execution::interpreter) fn abort_request_write_scope(&mut self) {
        self.prepared_memberships.clear();
        self.pending_sets = Arc::default();
        self.request_write_scope = RequestWriteScopeState::Disabled;
    }

    /// Commits the active request transaction and its deferred cache effects.
    pub(in crate::execution::interpreter) async fn commit_request_write_scope(
        &mut self,
    ) -> Result<()> {
        // Later reads use another snapshot.
        self.prepared_memberships.clear();
        self.pending_sets = Arc::default();
        let state = std::mem::replace(
            &mut self.request_write_scope,
            RequestWriteScopeState::Disabled,
        );
        let active = match state {
            RequestWriteScopeState::Active(active) => *active,
            RequestWriteScopeState::Disabled => return Ok(()),
        };
        self.commit_write_tx(active).await
    }

    /// Opens the one snapshot transaction owned by the request scope.
    pub(super) async fn begin_write_tx(&mut self) -> Result<(DbTransaction, MutationIndexContext)> {
        self.check_execution_deadline()?;
        let catalog_freshness = std::mem::replace(
            &mut self.pending_catalog_freshness,
            PendingCatalogFreshness::Consumed,
        );
        let catalog_permit = match catalog_freshness {
            PendingCatalogFreshness::Prepared(proof) => self
                .db
                .consume_catalog_refresh_proof(proof, self.tenant_scope),
            PendingCatalogFreshness::Unverified | PendingCatalogFreshness::Consumed => None,
        };
        let catalog_permit = match catalog_permit {
            Some(permit) => permit,
            None => {
                let permit = self.db.index_catalog_scope_permit(self.tenant_scope).await;
                self.check_execution_deadline()?;
                self.db.refresh_runtime_catalog(self.tenant_scope).await?;
                permit
            }
        };
        self.check_execution_deadline()?;
        let scope_permit = self.db.index_mutation_scope_permit(self.tenant_scope).await;
        self.check_execution_deadline()?;
        let transaction = self
            .writer()?
            .db()
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        self.check_execution_deadline()?;
        let mutation_catalog =
            crate::index_lifecycle::mutation_catalog::MutationIndexCatalog::load(
                &transaction,
                self.tenant_scope,
            )
            .await?;
        drop(catalog_permit);
        self.check_execution_deadline()?;
        Ok((
            transaction,
            MutationIndexContext::new(scope_permit, mutation_catalog, self.tenant_scope),
        ))
    }

    /// Extracts active request state or begins an isolated mutation scope.
    ///
    /// Direct focused mutation calls that did not enable a request scope open an
    /// isolated transaction. Request execution already owns its transaction.
    pub(super) async fn take_or_begin_write_scope(&mut self) -> Result<MutationWriteScope> {
        let state = std::mem::replace(
            &mut self.request_write_scope,
            RequestWriteScopeState::Disabled,
        );
        match state {
            RequestWriteScopeState::Active(active) => Ok(MutationWriteScope {
                txn: active.txn,
                index_context: active.index_context,
                request_scoped: true,
            }),
            RequestWriteScopeState::Disabled => {
                let (txn, index_context) = self.begin_write_tx().await?;
                Ok(MutationWriteScope {
                    txn,
                    index_context,
                    request_scoped: false,
                })
            }
        }
    }

    /// Returns request state to the ADT or commits an isolated mutation scope.
    ///
    /// Membership sets read in the request transaction stay exact unless this
    /// operation's writes reached their footprint, so only those are
    /// forgotten. An isolated scope read and committed its own snapshot,
    /// which may be newer than the view the cached sets came from, so it
    /// forgets every set. Only direct operation calls reach it: request
    /// execution opens its write scope first.
    pub(super) async fn finish_write_scope(&mut self, mut scope: MutationWriteScope) -> Result<()> {
        if scope.request_scoped {
            assert!(
                matches!(self.request_write_scope, RequestWriteScopeState::Disabled),
                "taken request write state must remain empty until returned"
            );
            self.prepared_memberships
                .forget_writes(&scope.index_context.take_node_index_writes());
            self.request_write_scope = RequestWriteScopeState::Active(Box::new(ActiveWriteTx {
                txn: scope.txn,
                index_context: scope.index_context,
            }));
            return Ok(());
        }

        self.prepared_memberships.clear();
        self.pending_sets = Arc::default();
        self.commit_write_tx(ActiveWriteTx {
            txn: scope.txn,
            index_context: scope.index_context,
        })
        .await
    }

    /// Flushes only deferred families consumed by the next operation.
    pub(in crate::execution::interpreter) async fn flush_required_mutations(
        &mut self,
        required: super::visibility::RequiredMutationVisibility,
    ) -> Result<()> {
        if required.is_empty() || !self.request_write_scope.is_active() {
            return Ok(());
        }
        let RequestWriteScopeState::Active(active) = &mut self.request_write_scope else {
            return Ok(());
        };
        if required.contains(super::visibility::DeferredMutationFamily::Topology) {
            active.index_context.flush_topology(&active.txn).await?;
        }
        if required.contains(super::visibility::DeferredMutationFamily::Secondary) {
            active.index_context.flush_secondary(&active.txn).await?;
        }
        Ok(())
    }

    /// Preserves the production-linked conservative barrier oracle.
    #[allow(dead_code)]
    #[cfg(any(test, feature = "production-coverage"))]
    pub(in crate::execution::interpreter) async fn flush_active_index_mutations(
        &mut self,
    ) -> Result<()> {
        self.flush_required_mutations(super::visibility::RequiredMutationVisibility::all())
            .await
    }

    /// Commits graph rows, staged index rows, and queued vector/text
    /// operations in one durable transaction.
    ///
    /// Queued capacity is reserved for every touched index before commit and
    /// finalized only from the commit outcome; the index worker is woken after
    /// a durable enqueue.
    pub(super) async fn commit_write_tx(&self, mut active: ActiveWriteTx) -> Result<()> {
        active.index_context.prepare_topology(&active.txn).await?;
        active.index_context.prepare_secondary(&active.txn).await?;
        let staged_queue = active.index_context.finalize_queued(
            self.db.index_operand_limit(),
            self.db.active_text_mutation_limits(),
        )?;
        let ActiveWriteTx { txn, index_context } = active;
        let prepared_index_context = index_context.into_prepared()?;
        // Reserve every touched index atomically, then stage one blind
        // operand per queue key in the same transaction as the graph change.
        let mut reservation = if staged_queue.is_empty() {
            None
        } else {
            let reservation = staged_queue
                .reserve(self.db.index_operation_backlog(), &txn)
                .await?;
            for staged in staged_queue.operands {
                self.db.index_queue_store().stage_enqueue(
                    &txn,
                    staged.target,
                    staged.operand,
                    &staged.operations,
                )?;
            }
            Some(reservation)
        };
        if let Some(reservation) = reservation.as_mut() {
            reservation.begin_commit();
        }
        match txn.commit().await {
            Ok(_) => {}
            Err(error) => {
                // Only a definite conflict proves nothing committed; any other
                // failure keeps the capacity charged until reconciliation.
                if let Some(reservation) = reservation {
                    if error.kind() == slatedb::ErrorKind::Transaction {
                        reservation.aborted();
                    } else {
                        reservation.uncertain();
                    }
                }
                return Err(prepared_index_context
                    .classify_commit_error(self.writer()?.db(), error)
                    .await);
            }
        };
        let queued_committed = reservation.is_some();
        if let Some(reservation) = reservation {
            reservation.committed();
        }
        if queued_committed {
            self.db.wake_index_worker().await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod additional_tests {

    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use helix_planner::context;

    use super::super::super::test_support;
    use super::*;

    /// Writes one typed row so the request has a real SlateDB commit sequence.
    fn stage_storage_write(active: &ActiveWriteTx, value: &'static [u8]) {
        let key = crate::encoding::v2::keys::DataKey::Data {
            scope: crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            kind: crate::encoding::v2::keys::DataKeyKind::NodeProperty(
                crate::encoding::v2::keys::NodePropertyKey::new(99),
            ),
        }
        .to_bytes();
        active.txn.put(key, Bytes::from_static(value)).unwrap();
    }

    #[tokio::test]
    async fn prepared_catalog_authority_covers_write_open_then_mutation_commit() {
        let db = Arc::new(test_support::open_db("mutation-catalog-authority-transfer").await);
        let scope = crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped;
        let prepared = db
            .planner_context_scoped_prepared(context::ParamBindings::default(), scope)
            .await
            .unwrap();
        let mut context = ExecutionContext::new_scoped_controlled_with_catalog_freshness(
            db.as_ref(),
            context::ParamBindings::default(),
            scope,
            crate::execution_control::ExecutionControl::unlimited(),
            PendingCatalogFreshness::Prepared(prepared.into_catalog_proof()),
        );

        let catalog_change = {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                let permit = db
                    .inner
                    .index_scope_gates
                    .catalog_change_permit(scope)
                    .await;
                drop(permit);
            })
        };
        tokio::task::yield_now().await;
        assert!(
            !catalog_change.is_finished(),
            "planning proof must exclude canonical catalog changes"
        );

        let (transaction, index_context) =
            tokio::time::timeout(Duration::from_secs(5), context.begin_write_tx())
                .await
                .expect("prepared write view opens while catalog change waits")
                .unwrap();
        tokio::time::timeout(Duration::from_secs(5), catalog_change)
            .await
            .expect("write open releases planning-only catalog authority")
            .expect("catalog-change waiter joins");

        let lifecycle = {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                let permit = db.inner.index_scope_gates.lifecycle_permit(scope).await;
                drop(permit);
            })
        };
        tokio::task::yield_now().await;
        assert!(
            !lifecycle.is_finished(),
            "opened mutation view must exclude Active publication"
        );

        drop(transaction);
        drop(index_context);
        tokio::time::timeout(Duration::from_secs(5), lifecycle)
            .await
            .expect("aborting the graph view releases mutation authority")
            .expect("lifecycle waiter joins");
        drop(context);
        Arc::try_unwrap(db)
            .unwrap_or_else(|_| panic!("test owns the only database reference"))
            .close()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn overlapping_refresh_keeps_prepared_catalog_until_write_snapshot_opens() {
        use crate::config::{TextAnalyzerKind, TextIndexDefinition, VectorIndexDefinition};
        use crate::encoding::v2::keys::ManagedIndexKey;
        use crate::encoding::v2::keys::ScopedKey;
        use crate::encoding::v2::values::encode_index_record;
        use crate::index_lifecycle::{
            ActiveIndexHandle, IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2,
            IndexRevision, IndexStateTransition, PhysicalGeneration,
            ValidatedDynamicIndexDefinition, VectorGenerationDescriptor, VectorPhysicalIndexId,
            VectorPhysicalLayout,
        };
        use crate::search::vector::VectorDistanceMetric;

        let db = Arc::new(test_support::open_db("mutation-recreated-catalog-settings").await);
        let scope = crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped;
        let old_vector = ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .unwrap(),
        )
        .unwrap();
        let new_vector = ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                4,
                VectorDistanceMetric::Euclidean,
            )
            .unwrap(),
        )
        .unwrap();
        let old_text = ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "body").unwrap(),
        )
        .unwrap();
        let new_text = ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "body")
                .unwrap()
                .with_analyzer(TextAnalyzerKind::StandardStemEn)
                .with_positions_enabled(true),
        )
        .unwrap();
        let active_record =
            |index_id: u64,
             generation: u64,
             revision: u64,
             definition: ValidatedDynamicIndexDefinition| {
                let generation = IndexGenerationId::new(generation).unwrap();
                let physical = match &definition {
                    ValidatedDynamicIndexDefinition::Vector(definition) => {
                        PhysicalGeneration::Vector {
                            generation,
                            layout: VectorPhysicalLayout::Unpartitioned {
                                physical_index_id: VectorPhysicalIndexId::new(
                                    index_id + generation.get() + 100,
                                )
                                .unwrap(),
                            },
                            descriptor: VectorGenerationDescriptor::for_definition(definition),
                        }
                    }
                    ValidatedDynamicIndexDefinition::Text(_) => {
                        PhysicalGeneration::Text { generation }
                    }
                    ValidatedDynamicIndexDefinition::Secondary(_) => {
                        panic!("fixture only builds vector and text records")
                    }
                };
                IndexRecordV2::building(
                    IndexId::new(index_id).unwrap(),
                    definition,
                    IndexRevision::new(revision).unwrap(),
                    physical,
                    IndexOperationId::new_v4(),
                )
                .unwrap()
                .transition(IndexStateTransition::Activate)
                .unwrap()
            };
        let old_records = [
            active_record(1, 1, 1, old_vector),
            active_record(2, 1, 1, old_text),
        ];
        let new_records = [
            active_record(1, 2, 3, new_vector),
            active_record(2, 2, 3, new_text),
        ];

        let seed = db
            .inner_db()
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        for record in &old_records {
            seed.put(
                ManagedIndexKey::Data {
                    scope,
                    kind: ScopedKey::index_record(record.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(record),
            )
            .unwrap();
        }
        seed.commit().await.unwrap();
        db.refresh_runtime_catalog(scope).await.unwrap();

        let prepared = db
            .planner_context_scoped_prepared(context::ParamBindings::default(), scope)
            .await
            .unwrap();
        db.refresh_runtime_catalog(scope)
            .await
            .expect("an overlapping in-memory refresh succeeds");
        let mut context = ExecutionContext::new_scoped_controlled_with_catalog_freshness(
            db.as_ref(),
            context::ParamBindings::default(),
            scope,
            crate::execution_control::ExecutionControl::unlimited(),
            PendingCatalogFreshness::Prepared(prepared.into_catalog_proof()),
        );

        let lifecycle = {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                let permit = db.inner.index_scope_gates.lifecycle_permit(scope).await;
                let transaction = db
                    .inner_db()
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .unwrap();
                for record in &new_records {
                    transaction
                        .put(
                            ManagedIndexKey::Data {
                                scope,
                                kind: ScopedKey::index_record(record.identity().clone()),
                            }
                            .to_bytes(),
                            encode_index_record(record),
                        )
                        .unwrap();
                }
                transaction.commit().await.unwrap();
                db.refresh_runtime_catalog(scope).await.unwrap();
                drop(permit);
            })
        };
        tokio::task::yield_now().await;
        assert!(
            !lifecycle.is_finished(),
            "prepared planning authority must block lifecycle publication"
        );

        let (transaction, index_context) =
            tokio::time::timeout(Duration::from_secs(5), context.begin_write_tx())
                .await
                .expect("write snapshot opens under the prepared catalog gate")
                .unwrap();
        assert!(index_context.active_generations().iter().any(|handle| {
            matches!(
                handle,
                ActiveIndexHandle::Vector {
                    generation,
                    definition,
                    ..
                } if generation.get() == 1
                    && definition.dimension() == 3
                    && definition.metric() == VectorDistanceMetric::Cosine
            )
        }));
        assert!(index_context.active_generations().iter().any(|handle| {
            matches!(
                handle,
                ActiveIndexHandle::Text {
                    generation,
                    definition,
                    ..
                } if generation.get() == 1
                    && definition.analyzer() == TextAnalyzerKind::Standard
                    && !definition.positions_enabled()
            )
        }));
        assert!(
            !lifecycle.is_finished(),
            "the transaction-owned mutation catalog must retain publication authority"
        );

        drop(transaction);
        drop(index_context);
        tokio::time::timeout(Duration::from_secs(5), lifecycle)
            .await
            .expect("queued lifecycle publication finishes after the write closes")
            .expect("lifecycle task joins");
        drop(context);
        Arc::try_unwrap(db)
            .unwrap_or_else(|_| panic!("test owns the only database reference"))
            .close()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn commit_and_cache_finalization_are_not_cancelled_after_commit_boundary() {
        let db = test_support::open_db("mutation-deadline-after-commit-start").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let key = crate::encoding::keys::DataKey::Data {
            scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
            kind: crate::encoding::keys::DataKeyKind::NodeProperty(
                crate::encoding::keys::NodePropertyKey::new(100),
            ),
        }
        .to_bytes();
        context
            .active_write_tx()
            .unwrap()
            .txn
            .put(key.clone(), Bytes::from_static(b"committed"))
            .unwrap();
        context.execution_control =
            crate::execution_control::ExecutionControl::from_timeout(Duration::ZERO);

        context.commit_request_write_scope().await.unwrap();
        assert_eq!(
            db.inner_db().get(&key).await.unwrap(),
            Some(Bytes::from_static(b"committed"))
        );
    }

    #[tokio::test]
    async fn abort_discards_staged_storage() {
        let db = test_support::open_db("mutation-request-abort").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        stage_storage_write(context.active_write_tx().unwrap(), b"aborted");

        context.abort_request_write_scope();
        let key = crate::encoding::keys::DataKey::Data {
            scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
            kind: crate::encoding::keys::DataKeyKind::NodeProperty(
                crate::encoding::keys::NodePropertyKey::new(99),
            ),
        }
        .to_bytes();
        assert!(db.inner_db().get(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dropped_request_context_rolls_back_staged_storage() {
        let db = test_support::open_db("mutation-request-context-drop").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let active = context.active_write_tx().unwrap();
        stage_storage_write(active, b"cancelled");

        drop(context);

        let key = crate::encoding::keys::DataKey::Data {
            scope: crate::encoding::keys::scope::DataScope::LegacyUnscoped,
            kind: crate::encoding::keys::DataKeyKind::NodeProperty(
                crate::encoding::keys::NodePropertyKey::new(99),
            ),
        }
        .to_bytes();
        assert!(db.inner_db().get(key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mutation_scope_projects_canonical_active_definitions() {
        use crate::config::SecondaryIndexDefinition;
        use crate::encoding::v2::keys::ManagedIndexKey;
        use crate::encoding::v2::keys::ScopedKey;
        use crate::encoding::v2::values::encode_index_record;
        use crate::index_lifecycle::{
            IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
            IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
        };

        let db = test_support::open_db("mutation-configured-catalog-authority").await;
        let scope = crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
        )
        .unwrap();
        let active = IndexRecordV2::building(
            IndexId::initial(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        db.inner_db()
            .put(
                ManagedIndexKey::Data {
                    scope,
                    kind: ScopedKey::index_record(active.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(&active),
            )
            .await
            .unwrap();

        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        context.active_write_tx().unwrap();
        let property = crate::config::scoped_secondary_index_property("User", "email");
        assert!(db
            .runtime_config_snapshot_loaded(scope)
            .contains_node_equality_scoped(&property));
        context.abort_request_write_scope();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn vector_drop_racing_graph_commit_returns_stale_generation() {
        use slatedb::object_store::memory::InMemory;
        use slatedb::object_store::ObjectStore;

        use crate::config::VectorIndexDefinition;
        use crate::encoding::v2::keys::{DataKey as GraphKey, DataKeyKind};
        use crate::encoding::v2::keys::{ManagedIndexKey, ScopedKey};
        use crate::encoding::v2::values::encode_index_record;
        use crate::index_lifecycle::{
            IndexGenerationId, IndexOperationId, IndexRecordV2, IndexRevision,
            IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
            ValidatedVectorIndexDefinition, VectorGenerationDescriptor, VectorPhysicalLayout,
        };
        use crate::search::vector::VectorDistanceMetric;

        let runtime = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Euclidean,
        )
        .unwrap();
        let definition = ValidatedVectorIndexDefinition::try_from_runtime(&runtime).unwrap();
        let dynamic = ValidatedDynamicIndexDefinition::Vector(definition.clone());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let raw = slatedb::Db::builder("mutation-vector-drop-conflict", Arc::clone(&object_store))
            .build()
            .await
            .unwrap();
        crate::migrations::startup::bootstrap_writer(&raw)
            .await
            .unwrap();
        let seed = raw
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let index_id = crate::index_lifecycle::repository::allocate_index_id(&seed)
            .await
            .unwrap();
        let physical_index_id =
            crate::index_lifecycle::repository::allocate_vector_physical_id(&seed)
                .await
                .unwrap();
        let active = IndexRecordV2::building(
            index_id,
            dynamic,
            IndexRevision::initial(),
            PhysicalGeneration::Vector {
                generation: IndexGenerationId::initial(),
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                descriptor: VectorGenerationDescriptor::for_definition(&definition),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        seed.put(
            ManagedIndexKey::Data {
                scope: crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
                kind: ScopedKey::index_record(active.identity().clone()),
            }
            .to_bytes(),
            encode_index_record(&active),
        )
        .unwrap();
        seed.commit().await.unwrap();
        raw.close().await.unwrap();

        let db = crate::HelixDB::open_with_object_store_for_tests(
            "mutation-vector-drop-conflict",
            object_store,
        )
        .await
        .unwrap();
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        stage_storage_write(context.active_write_tx().unwrap(), b"must-abort");

        let dropping = active
            .transition(IndexStateTransition::BeginDrop {
                drop_operation_id: IndexOperationId::new_v4(),
            })
            .unwrap();
        db.inner_db()
            .put(
                ManagedIndexKey::Data {
                    scope: crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
                    kind: ScopedKey::index_record(dropping.identity().clone()),
                }
                .to_bytes(),
                encode_index_record(&dropping),
            )
            .await
            .unwrap();

        assert!(matches!(
            context.commit_request_write_scope().await,
            Err(HelixDbError::StaleIndexGeneration {
                index_id: stale_index_id,
                generation: 1,
                record_revision: 2,
            }) if stale_index_id == index_id.get()
        ));
        let staged_graph_key = GraphKey::Data {
            scope: crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(crate::encoding::v2::keys::NodePropertyKey::new(99)),
        }
        .to_bytes();
        assert!(db.inner_db().get(staged_graph_key).await.unwrap().is_none());
        db.close().await.unwrap();
    }
}
