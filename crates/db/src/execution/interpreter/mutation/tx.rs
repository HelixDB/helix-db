//! Request-owned write transaction lifecycle for executable mutations.
//!
//! [`RequestWriteScopeState`] makes it impossible to retain an active request
//! transaction without its catalog snapshot and transaction-local vector-cache
//! writes. Retained static-index maintenance receives a configured-only
//! capability, while canonical V2 generations remain in the transaction-owned
//! family mutation sets.

use slatedb::{DbTransaction, IsolationLevel};

use super::super::runtime_context::{ActiveWriteTx, RequestWriteScopeState};
use super::super::search_index::TextIndexMaintenanceOutcome;
use super::index_context::LoadedMutationIndexState;
use super::*;
use crate::index_v2::text::active_resolution;

/// Temporarily extracted mutation state returned after one operation finishes.
pub(super) struct MutationWriteScope {
    pub(super) txn: DbTransaction,
    pub(super) text_indexes: TextIndexMaintenanceOutcome,
    pub(super) configured_indexes: crate::index_v2::ConfiguredIndexCatalog,
    pub(super) index_context: MutationIndexContext,
    request_scoped: bool,
}

impl<'db> ExecutionContext<'db> {
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
        let configured_indexes = self.db.configured_index_catalog_loaded(self.tenant_scope);
        self.request_write_scope = RequestWriteScopeState::Active(Box::new(ActiveWriteTx {
            txn,
            text_indexes: TextIndexMaintenanceOutcome::default(),
            configured_indexes,
            index_context,
        }));
        Ok(())
    }

    /// Drops any active transaction to abort the write request.
    pub(in crate::execution::interpreter) fn abort_request_write_scope(&mut self) {
        self.request_write_scope = RequestWriteScopeState::Disabled;
    }

    /// Commits the active request transaction and its deferred cache effects.
    pub(in crate::execution::interpreter) async fn commit_request_write_scope(
        &mut self,
    ) -> Result<()> {
        let state = std::mem::replace(
            &mut self.request_write_scope,
            RequestWriteScopeState::Disabled,
        );
        let active = match state {
            RequestWriteScopeState::Active(active) => *active,
            RequestWriteScopeState::Disabled => return Ok(()),
        };
        self.commit_write_tx(
            active.txn,
            active.text_indexes,
            active.configured_indexes.runtime(),
            active.index_context,
        )
        .await
    }

    /// Opens the one snapshot transaction owned by the request scope.
    pub(super) async fn begin_write_tx(&self) -> Result<(DbTransaction, MutationIndexContext)> {
        let scope_permit = self.db.index_mutation_scope_permit(self.tenant_scope).await;
        self.db.refresh_runtime_catalog(self.tenant_scope).await?;
        let transaction = self
            .writer()?
            .db()
            .begin(IsolationLevel::SerializableSnapshot)
            .await?;
        let active_generations = self.db.active_index_handles_loaded(self.tenant_scope);
        for handle in &active_generations {
            crate::index_v2::repository::revalidate_active_handle(&transaction, handle).await?;
        }
        let vector =
            crate::index_v2::vector::load_mutation_set(&transaction, self.tenant_scope).await?;
        let secondary =
            crate::index_v2::secondary::load_mutation_set(&transaction, self.tenant_scope).await?;
        let text =
            crate::index_v2::text::mutation::load_mutation_set(&transaction, self.tenant_scope)
                .await?;
        let active_text_runtime = self.db.active_text_mutation_runtime().await;
        Ok((
            transaction,
            MutationIndexContext::new(
                scope_permit,
                LoadedMutationIndexState::new(active_generations, secondary, vector, text),
                active_text_runtime,
                std::sync::Arc::clone(self.db.simhasher_registry()),
            ),
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
                text_indexes: active.text_indexes,
                configured_indexes: active.configured_indexes,
                index_context: active.index_context,
                request_scoped: true,
            }),
            RequestWriteScopeState::Disabled => {
                let (txn, index_context) = self.begin_write_tx().await?;
                Ok(MutationWriteScope {
                    txn,
                    text_indexes: TextIndexMaintenanceOutcome::default(),
                    configured_indexes: self.db.configured_index_catalog_loaded(self.tenant_scope),
                    index_context,
                    request_scoped: false,
                })
            }
        }
    }

    /// Returns request state to the ADT or commits an isolated mutation scope.
    pub(super) async fn finish_write_scope(&mut self, scope: MutationWriteScope) -> Result<()> {
        if scope.request_scoped {
            assert!(
                matches!(self.request_write_scope, RequestWriteScopeState::Disabled),
                "taken request write state must remain empty until returned"
            );
            self.request_write_scope = RequestWriteScopeState::Active(Box::new(ActiveWriteTx {
                txn: scope.txn,
                text_indexes: scope.text_indexes,
                configured_indexes: scope.configured_indexes,
                index_context: scope.index_context,
            }));
            return Ok(());
        }

        self.commit_write_tx(
            scope.txn,
            scope.text_indexes,
            scope.configured_indexes.runtime(),
            scope.index_context,
        )
        .await
    }

    /// Commits storage before publishing deferred cache effects.
    ///
    /// Post-commit text compaction does not mutate the request transaction.
    pub(super) async fn commit_write_tx(
        &self,
        txn: DbTransaction,
        text_indexes: TextIndexMaintenanceOutcome,
        indexes: &crate::config::RuntimeIndexCatalog,
        index_context: MutationIndexContext,
    ) -> Result<()> {
        let pending_vector_cache = index_context
            .vector_cache_writes()
            .entries()
            .iter()
            .filter_map(|write| self.db.vector_cache_registry().prepare_commit(write))
            .collect::<Vec<_>>();
        let commit = match txn.commit().await {
            Ok(committed) => Ok(committed),
            Err(error) => Err(index_context
                .classify_commit_error(self.writer()?.db(), error)
                .await),
        };
        let active_text_outbox = index_context.into_active_text_outbox();

        match commit {
            Ok(committed) => {
                let committed_sequence = committed.map(|committed| committed.seqnum());
                assert!(
                    pending_vector_cache.is_empty() || committed_sequence.is_some(),
                    "dirty vector cache rows imply a non-empty committed storage batch"
                );
                for pending in pending_vector_cache {
                    let Some(committed_sequence) = committed_sequence else {
                        unreachable!("dirty vector cache rows require the asserted sequence")
                    };
                    pending.evict_after_commit(committed_sequence).await;
                }
                if !active_text_outbox.is_empty() {
                    match active_resolution::resolve_active_text_transaction_outbox(
                        self.writer()?.db(),
                        active_text_outbox,
                        active_resolution::ActiveTextGraphCommitObservation::Committed,
                    )
                    .await
                    .map_err(active_resolution::ActiveTextResolutionError::into_database_error)?
                    {
                        active_resolution::ActiveTextGraphResolution::Committed(
                            active_resolution::ActiveTextFinalization::Complete,
                        ) => {}
                        active_resolution::ActiveTextGraphResolution::Committed(
                            active_resolution::ActiveTextFinalization::Deferred,
                        ) => {
                            tracing::warn!(
                                "committed Active text mutation retained durable finalization work"
                            );
                        }
                        active_resolution::ActiveTextGraphResolution::Aborted { .. } => {
                            return Err(HelixDbError::InvariantViolation(
                                "a successful graph commit resolved as an Active text abort"
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
            Err(commit_error) => {
                if active_text_outbox.is_empty() {
                    return Err(commit_error);
                }
                match active_resolution::resolve_active_text_transaction_outbox(
                    self.writer()?.db(),
                    active_text_outbox,
                    active_resolution::ActiveTextGraphCommitObservation::Ambiguous(commit_error),
                )
                .await
                .map_err(active_resolution::ActiveTextResolutionError::into_database_error)?
                {
                    active_resolution::ActiveTextGraphResolution::Committed(finalization) => {
                        for pending in pending_vector_cache {
                            pending.evict_after_proven_commit_without_sequence().await;
                        }
                        if finalization == active_resolution::ActiveTextFinalization::Deferred {
                            tracing::warn!(
                                "proven Active text commit retained durable finalization work"
                            );
                        }
                    }
                    active_resolution::ActiveTextGraphResolution::Aborted { commit_error } => {
                        return Err(commit_error)
                    }
                }
            }
        }
        self.compact_text_indexes_after_commit(text_indexes, indexes)
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod additional_tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;

    use bytes::Bytes;
    use helix_planner::context;

    use super::super::super::test_support;
    use super::*;

    /// Builds one descriptor identity for transaction/cache boundary tests.
    fn cache_handle() -> crate::search::vector::ValidatedVectorGenerationHandle {
        crate::search::vector::ValidatedVectorGenerationHandle::create_current::<
            crate::search::vector::distance::Cosine,
        >(
            crate::search::vector::VectorGenerationIdentity::try_new(
                crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
                6,
                "transaction-cache-generation".to_string(),
                60,
                NonZeroU64::MIN,
                1,
                crate::index_v2::IndexElementKind::Node,
                crate::search::vector::VectorDimension::try_new(2).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    /// Installs one ready cache row and returns its retained store.
    fn ready_store(
        db: &crate::HelixDB,
        handle: &crate::search::vector::ValidatedVectorGenerationHandle,
    ) -> Arc<crate::search::vector::VectorMemoryStore> {
        let store = Arc::new(crate::search::vector::VectorMemoryStore::new(
            crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
            handle.physical_index_id(),
            0,
        ));
        store.insert_upper_vector(7, Bytes::from_static(b"cached"));
        let (entry, owns_hydration) = db.vector_cache_registry().entry_for(handle);
        assert!(owns_hydration);
        assert!(entry.finish_hydration(Arc::clone(&store)));
        store
    }

    /// Writes one typed row so the request has a real SlateDB commit sequence.
    fn stage_storage_write(active: &ActiveWriteTx, value: &'static [u8]) {
        let key = crate::encoding::keys::Key::Data {
            scope: crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
            kind: crate::encoding::keys::DataKeyKind::NodeProperty(
                crate::encoding::keys::NodePropertyKey::new(99),
            ),
        }
        .to_bytes();
        active.txn.put(key, Bytes::from_static(value)).unwrap();
    }

    #[tokio::test]
    async fn successful_commit_evicts_exact_dirty_generation_after_storage_commit() {
        let db = test_support::open_db("mutation-vector-cache-commit").await;
        let handle = cache_handle();
        let store = ready_store(&db, &handle);
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let active = context.active_write_tx().unwrap();
        active
            .index_context
            .vector_cache_writes()
            .dirty_rows_for(&handle)
            .mark_node_dirty(7);
        stage_storage_write(active, b"commit");

        context.commit_request_write_scope().await.unwrap();
        assert!(store.get_upper_vector(7).is_none());
    }

    #[tokio::test]
    async fn abort_drops_vector_write_set_without_cache_eviction() {
        let db = test_support::open_db("mutation-vector-cache-abort").await;
        let handle = cache_handle();
        let store = ready_store(&db, &handle);
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        context
            .active_write_tx()
            .unwrap()
            .index_context
            .vector_cache_writes()
            .dirty_rows_for(&handle)
            .mark_node_dirty(7);

        context.abort_request_write_scope();
        assert!(store.get_upper_vector(7).is_some());
    }

    #[tokio::test]
    async fn commit_conflict_releases_pending_rows_without_cache_eviction() {
        let db = test_support::open_db("mutation-vector-cache-conflict").await;
        let handle = cache_handle();
        let store = ready_store(&db, &handle);
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let active = context.active_write_tx().unwrap();
        active
            .index_context
            .vector_cache_writes()
            .dirty_rows_for(&handle)
            .mark_node_dirty(7);
        stage_storage_write(active, b"request");

        let competing = db
            .inner_db()
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .unwrap();
        let key = crate::encoding::keys::Key::Data {
            scope: crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
            kind: crate::encoding::keys::DataKeyKind::NodeProperty(
                crate::encoding::keys::NodePropertyKey::new(99),
            ),
        }
        .to_bytes();
        competing
            .put(key, Bytes::from_static(b"competing"))
            .unwrap();
        competing.commit().await.unwrap();

        let error = context.commit_request_write_scope().await.unwrap_err();
        assert!(error.is_transaction_conflict());
        assert!(store.get_upper_vector(7).is_some());
        let lease = db.vector_cache_registry().lease_for(&handle).unwrap();
        assert!(!lease.pending_dirty().is_node_dirty(7));
    }

    #[tokio::test]
    async fn mutation_scope_excludes_canonical_active_definitions_from_static_maintenance() {
        use crate::config::SecondaryIndexDefinition;
        use crate::encoding::v1::keys::index_v2::IndexV2Key;
        use crate::encoding::v1::keys::{DataKeyKind, Key};
        use crate::encoding::v1::values::index_v2::encode_index_record;
        use crate::index_v2::{
            IndexGenerationId, IndexId, IndexOperationId, IndexRecordV2, IndexRevision,
            IndexStateTransition, PhysicalGeneration, ValidatedDynamicIndexDefinition,
        };

        let db = test_support::open_db("mutation-configured-catalog-authority").await;
        let scope = crate::encoding::keys::tenant::DataScope::LegacyUnscoped;
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
                Key::Data {
                    scope,
                    kind: DataKeyKind::IndexV2(IndexV2Key::index_record(active.identity().clone())),
                }
                .to_bytes(),
                encode_index_record(&active),
            )
            .await
            .unwrap();

        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        context.enable_request_write_scope().await.unwrap();
        let active = context.active_write_tx().unwrap();
        let property = crate::config::scoped_secondary_index_property("User", "email");
        assert!(db
            .runtime_config_snapshot_loaded(scope)
            .contains_node_equality_scoped(&property));
        assert!(!active
            .configured_indexes
            .runtime()
            .contains_node_equality_scoped(&property));

        context.abort_request_write_scope();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn vector_drop_racing_graph_commit_returns_stale_generation() {
        use slatedb::object_store::memory::InMemory;
        use slatedb::object_store::ObjectStore;

        use crate::config::{IndexConfig, VectorIndexDefinition};
        use crate::encoding::v1::keys::index_v2::IndexV2Key;
        use crate::encoding::v1::keys::{DataKeyKind, Key};
        use crate::encoding::v1::values::index_v2::encode_index_record;
        use crate::index_v2::{
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
        crate::index_v2::repository::bootstrap_writer(&raw)
            .await
            .unwrap();
        let seed = raw
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let index_id = crate::index_v2::repository::allocate_index_id(&seed)
            .await
            .unwrap();
        let physical_index_id = crate::index_v2::repository::allocate_vector_physical_id(&seed)
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
            Key::Data {
                scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                kind: DataKeyKind::IndexV2(IndexV2Key::index_record(active.identity().clone())),
            }
            .to_bytes(),
            encode_index_record(&active),
        )
        .unwrap();
        seed.commit().await.unwrap();
        raw.close().await.unwrap();

        let db = crate::HelixDB::open_with_object_store_and_index_config_for_tests(
            "mutation-vector-drop-conflict",
            object_store,
            IndexConfig::new().with_vector_index(runtime),
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
                Key::Data {
                    scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::index_record(
                        dropping.identity().clone(),
                    )),
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
        let staged_graph_key = Key::Data {
            scope: crate::encoding::v1::keys::tenant::DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(crate::encoding::v1::keys::NodePropertyKey::new(99)),
        }
        .to_bytes();
        assert!(db.inner_db().get(staged_graph_key).await.unwrap().is_none());
        db.close().await.unwrap();
    }
}
