//! Transaction-owned V2 index state for one graph mutation.
//!
//! The context holds the shared scope permit for the full graph transaction,
//! canonical secondary/vector/text generation work selected in that snapshot,
//! and vector cache writes. The permit prevents an exclusive
//! activation/cleanup checkpoint from crossing the mutation commit boundary;
//! vector cache writes remain publish-after-commit state.

use std::sync::Arc;

use crate::search::vector;

/// Index state that is valid for exactly one graph mutation transaction.
#[derive(Debug)]
pub(crate) struct MutationIndexContext {
    _scope_permit: Option<crate::index_lifecycle::IndexScopeMutationPermit>,
    active: crate::index_lifecycle::mutation_catalog::ActiveMutationCatalog,
    secondary: crate::index_lifecycle::secondary::SecondaryMutationSet,
    secondary_runtime: crate::index_lifecycle::secondary::SecondaryMutationRuntime,
    vector: crate::index_lifecycle::vector::VectorMutationSet,
    text: crate::index_lifecycle::text::mutation::TextMutationSet,
    routes: crate::index_lifecycle::mutation_catalog::MutationRouteCatalog,
    topology_runtime: super::topology::TopologyMutationRuntime,
    active_text_runtime: crate::index_lifecycle::text::active_runtime::ActiveTextMutationRuntime,
    active_vector_deletions: crate::index_lifecycle::vector::ActiveVectorDeletionBatch,
    active_vector_runtime: vector::ActiveVectorMutationRuntime,
    vector_cache_writes: vector::VectorCacheWriteSet,
    text_compaction_staged: bool,
}

/// Commit-owned index state after every transaction-local runtime is sealed.
pub(crate) struct PreparedMutationIndexContext {
    _scope_permit: Option<crate::index_lifecycle::IndexScopeMutationPermit>,
    active_generations: Vec<crate::index_lifecycle::ActiveIndexHandle>,
    _secondary: crate::index_lifecycle::secondary::SecondaryMutationSet,
    _vector: crate::index_lifecycle::vector::VectorMutationSet,
    _text: crate::index_lifecycle::text::mutation::TextMutationSet,
    vector_cache_writes: vector::VectorCacheWriteSet,
    text_compaction_staged: bool,
}

impl MutationIndexContext {
    /// Creates transaction-local generation and cache tracking.
    pub(crate) fn new(
        scope_permit: crate::index_lifecycle::IndexScopeMutationPermit,
        loaded: crate::index_lifecycle::mutation_catalog::MutationIndexCatalog,
        simhasher_registry: Arc<vector::SimHasherRegistry>,
        vector_retained_payload_limit: std::num::NonZeroU64,
    ) -> Self {
        let (active, secondary, vector, text, routes) = loaded.into_components();
        Self {
            _scope_permit: Some(scope_permit),
            active,
            secondary,
            secondary_runtime: crate::index_lifecycle::secondary::SecondaryMutationRuntime::default(
            ),
            vector,
            text,
            routes,
            topology_runtime: super::topology::TopologyMutationRuntime::default(),
            active_text_runtime:
                crate::index_lifecycle::text::active_runtime::ActiveTextMutationRuntime::new(),
            active_vector_deletions:
                crate::index_lifecycle::vector::ActiveVectorDeletionBatch::default(),
            active_vector_runtime: vector::ActiveVectorMutationRuntime::new(
                vector_retained_payload_limit,
            ),
            vector_cache_writes: vector::VectorCacheWriteSet::new(simhasher_registry),
            text_compaction_staged: false,
        }
    }

    /// Creates an uncoordinated empty V2 context for focused configured-index tests.
    #[cfg(test)]
    pub(crate) fn for_configured_index_test(
        simhasher_registry: Arc<vector::SimHasherRegistry>,
    ) -> Self {
        Self {
            _scope_permit: None,
            active: crate::index_lifecycle::mutation_catalog::ActiveMutationCatalog::default(),
            secondary: crate::index_lifecycle::secondary::SecondaryMutationSet::empty(),
            secondary_runtime: crate::index_lifecycle::secondary::SecondaryMutationRuntime::default(
            ),
            vector: crate::index_lifecycle::vector::VectorMutationSet::empty(),
            text: crate::index_lifecycle::text::mutation::TextMutationSet::empty(),
            routes: crate::index_lifecycle::mutation_catalog::MutationRouteCatalog::default(),
            topology_runtime: super::topology::TopologyMutationRuntime::default(),
            active_text_runtime:
                crate::index_lifecycle::text::active_runtime::ActiveTextMutationRuntime::new(),
            active_vector_deletions:
                crate::index_lifecycle::vector::ActiveVectorDeletionBatch::default(),
            active_vector_runtime: vector::ActiveVectorMutationRuntime::new(
                std::num::NonZeroU64::new(8 * 1024 * 1024)
                    .expect("the focused-test vector payload limit is non-zero"),
            ),
            vector_cache_writes: vector::VectorCacheWriteSet::new(simhasher_registry),
            text_compaction_staged: false,
        }
    }

    /// Returns vector rows dirtied by the transaction, grouped by generation.
    #[cfg(test)]
    pub(crate) const fn vector_cache_writes(&self) -> &vector::VectorCacheWriteSet {
        &self.vector_cache_writes
    }

    /// Returns exact Active capabilities loaded with the graph transaction.
    #[cfg(test)]
    pub(crate) fn active_generations(&self) -> &[crate::index_lifecycle::ActiveIndexHandle] {
        self.active.generations()
    }

    /// Resolves one Active generation from this transaction's canonical catalog scan.
    pub(crate) fn active_handle(
        &self,
        identity: &crate::index_lifecycle::IndexIdentity,
    ) -> Option<&crate::index_lifecycle::ActiveIndexHandle> {
        self.active.handle(identity)
    }

    /// Counts graph entities retained by the current Active text epoch.
    #[cfg(test)]
    pub(crate) fn pending_active_text_entities(&self) -> usize {
        self.active_text_runtime.pending_entity_count()
    }

    /// Routes one complete graph transition through every configured family.
    pub(crate) async fn maintain_graph_indexes(
        &mut self,
        transaction: &slatedb::DbTransaction,
        graph: crate::index_lifecycle::graph_mutation::GraphMutationTransition,
        text_limits: crate::config::ActiveTextMutationLimits,
        object_store: &Arc<dyn slatedb::object_store::ObjectStore>,
        database: &str,
    ) -> Result<(), crate::HelixDbError> {
        let routes_include_vector = {
            let routes = self.routes.targets_for(&graph);
            routes.iter().any(|target| {
                matches!(
                    target,
                    crate::index_lifecycle::mutation_catalog::MutationRouteTarget::Vector(_)
                )
            })
        };
        if graph.after().is_some() && routes_include_vector {
            self.flush_active_vector_deletions(transaction).await?;
        }
        let routes = self.routes.targets_for(&graph);
        self.secondary_runtime
            .collect(graph.scope(), &self.secondary, &routes, &graph)?;
        crate::index_lifecycle::vector::maintain_routed_entity_with_runtime(
            transaction,
            graph.scope(),
            &self.vector,
            &routes,
            &mut self.active_vector_deletions,
            &mut self.active_vector_runtime,
            &self.vector_cache_writes,
            &graph,
        )
        .await?;
        let text_relevant = routes.iter().any(|target| {
            matches!(
                target,
                crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextBuilding(_)
                    | crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextActive(_)
            )
        });
        match self
            .active_text_runtime
            .try_collect_routed(graph, text_relevant, text_limits)?
        {
            crate::index_lifecycle::text::active_runtime::ActiveTextCollection::Collected => Ok(()),
            crate::index_lifecycle::text::active_runtime::ActiveTextCollection::DrainRequired(
                mutation,
            ) => {
                self.flush_topology(transaction).await?;
                self.flush_secondary(transaction).await?;
                self.flush_active_vectors(transaction).await?;
                self.flush_active_text(transaction, text_limits, object_store, database)
                    .await?;
                self.active_text_runtime.consume_admissible(mutation)
            }
        }
    }

    /// Borrows the transaction-local topology collector.
    pub(super) const fn topology_mutations(
        &mut self,
    ) -> &mut super::topology::TopologyMutationRuntime {
        &mut self.topology_runtime
    }

    /// Flushes one topology epoch before topology-dependent reads.
    pub(crate) async fn flush_topology(
        &mut self,
        transaction: &slatedb::DbTransaction,
    ) -> Result<(), crate::HelixDbError> {
        self.topology_runtime.flush(transaction).await
    }

    /// Reads current topology rows through the runtime's staged overlay.
    pub(crate) async fn observe_topology(
        &self,
        transaction: &slatedb::DbTransaction,
        keys: &[bytes::Bytes],
    ) -> Result<Vec<Option<bytes::Bytes>>, crate::HelixDbError> {
        self.topology_runtime.observe(transaction, keys).await
    }

    /// Flushes and seals topology state at the commit boundary.
    pub(crate) async fn prepare_topology(
        &mut self,
        transaction: &slatedb::DbTransaction,
    ) -> Result<(), crate::HelixDbError> {
        self.topology_runtime.prepare(transaction).await
    }

    /// Flushes routed secondary mutations through one ordered observation batch.
    pub(crate) async fn flush_secondary(
        &mut self,
        transaction: &slatedb::DbTransaction,
    ) -> Result<(), crate::HelixDbError> {
        self.secondary_runtime
            .flush(transaction, &self.secondary)
            .await
    }

    /// Flushes and seals the final secondary mutation epoch.
    pub(crate) async fn prepare_secondary(
        &mut self,
        transaction: &slatedb::DbTransaction,
    ) -> Result<(), crate::HelixDbError> {
        self.secondary_runtime
            .prepare(transaction, &self.secondary)
            .await
    }

    /// Flushes Active vector rows before a non-mutation operation reads the transaction.
    pub(crate) async fn flush_active_vectors(
        &mut self,
        transaction: &slatedb::DbTransaction,
    ) -> Result<(), crate::HelixDbError> {
        self.flush_active_vector_deletions(transaction).await?;
        self.active_vector_runtime.flush(transaction).await
    }

    /// Transfers logical terminal deletions into grouped physical HNSW repair.
    async fn flush_active_vector_deletions(
        &mut self,
        transaction: &slatedb::DbTransaction,
    ) -> Result<(), crate::HelixDbError> {
        crate::index_lifecycle::vector::flush_active_deletions(
            transaction,
            &self.vector,
            &mut self.active_vector_deletions,
            &mut self.active_vector_runtime,
            &self.vector_cache_writes,
        )
        .await
    }

    /// Drains one Active text epoch before a transaction-visible read.
    pub(crate) async fn flush_active_text(
        &mut self,
        transaction: &slatedb::DbTransaction,
        limits: crate::config::ActiveTextMutationLimits,
        object_store: &Arc<dyn slatedb::object_store::ObjectStore>,
        database: &str,
    ) -> Result<(), crate::HelixDbError> {
        let outcome = self
            .active_text_runtime
            .flush(
                transaction,
                &self.text,
                &self.routes,
                limits,
                object_store,
                database,
            )
            .await?;
        self.text_compaction_staged |= outcome.compaction_staged();
        Ok(())
    }

    /// Flushes the final Active text epoch and seals its runtime.
    pub(crate) async fn prepare_active_text(
        &mut self,
        transaction: &slatedb::DbTransaction,
        limits: crate::config::ActiveTextMutationLimits,
        object_store: &Arc<dyn slatedb::object_store::ObjectStore>,
        database: &str,
    ) -> Result<(), crate::HelixDbError> {
        let outcome = self
            .active_text_runtime
            .prepare(
                transaction,
                &self.text,
                &self.routes,
                limits,
                object_store,
                database,
            )
            .await?;
        self.text_compaction_staged |= outcome.compaction_staged();
        Ok(())
    }

    /// Seals Active vector state after its final deterministic flush.
    pub(crate) async fn prepare_active_vectors(
        &mut self,
        transaction: &slatedb::DbTransaction,
    ) -> Result<(), crate::HelixDbError> {
        self.flush_active_vector_deletions(transaction).await?;
        self.active_vector_runtime.prepare(transaction).await
    }

    /// Stages a descriptor-proven Active vector directly for interpreter barrier tests.
    #[cfg(test)]
    pub(crate) async fn stage_active_vector_for_test(
        &mut self,
        transaction: &slatedb::DbTransaction,
        generation: &vector::ValidatedVectorGenerationHandle,
        entity_id: u64,
        value: &[f32],
        create: bool,
    ) -> Result<(), crate::HelixDbError> {
        self.active_vector_runtime
            .upsert(
                transaction,
                generation,
                &self.vector_cache_writes,
                entity_id,
                value,
                create,
            )
            .await
    }

    /// Consumes the sealed runtime and transfers all state to the commit boundary.
    pub(crate) fn into_prepared(self) -> Result<PreparedMutationIndexContext, crate::HelixDbError> {
        let Self {
            _scope_permit,
            active,
            secondary,
            secondary_runtime,
            vector,
            text,
            routes: _,
            topology_runtime,
            active_text_runtime,
            active_vector_deletions,
            active_vector_runtime,
            vector_cache_writes,
            text_compaction_staged,
        } = self;
        topology_runtime.consume_prepared()?;
        active_vector_deletions.consume_drained()?;
        active_vector_runtime.consume_prepared()?;
        active_text_runtime.consume_prepared()?;
        secondary_runtime.consume_prepared()?;
        Ok(PreparedMutationIndexContext {
            _scope_permit,
            active_generations: active.into_generations(),
            _secondary: secondary,
            _vector: vector,
            _text: text,
            vector_cache_writes,
            text_compaction_staged,
        })
    }

    /// Reclassifies a backend commit conflict when canonical DDL invalidated
    /// one of the exact active generations read by this graph transaction.
    ///
    /// Ordinary row conflicts retain the backend transaction error. A changed
    /// active record instead returns the stable `stale_index_generation`
    /// contract so callers know the graph mutation must restart with a fresh
    /// lifecycle snapshot.
    #[cfg(test)]
    pub(crate) async fn classify_commit_error(
        &self,
        reader: &(impl slatedb::DbReadOps + Sync),
        error: slatedb::Error,
    ) -> crate::HelixDbError {
        classify_commit_error(self.active.generations(), reader, error).await
    }
}

impl PreparedMutationIndexContext {
    /// Returns the exact vector cache effects guarded by this commit state.
    pub(crate) const fn vector_cache_writes(&self) -> &vector::VectorCacheWriteSet {
        &self.vector_cache_writes
    }

    /// Returns whether a successful commit should wake Active text compaction.
    pub(crate) const fn text_compaction_staged(&self) -> bool {
        self.text_compaction_staged
    }

    /// Reclassifies a failed storage commit against the retained generation set.
    pub(crate) async fn classify_commit_error(
        &self,
        reader: &(impl slatedb::DbReadOps + Sync),
        error: slatedb::Error,
    ) -> crate::HelixDbError {
        classify_commit_error(&self.active_generations, reader, error).await
    }
}

async fn classify_commit_error(
    active_generations: &[crate::index_lifecycle::ActiveIndexHandle],
    reader: &(impl slatedb::DbReadOps + Sync),
    error: slatedb::Error,
) -> crate::HelixDbError {
    if error.kind() != slatedb::ErrorKind::Transaction {
        return error.into();
    }
    for handle in active_generations {
        let Err(error) =
            crate::index_lifecycle::repository::revalidate_active_handle(reader, handle).await
        else {
            continue;
        };
        return error;
    }
    error.into()
}

#[cfg(test)]
mod tests {
    use super::super::super::test_support;
    use super::*;
    use crate::encoding::v2::keys;
    use crate::{config, index_lifecycle};

    #[tokio::test]
    async fn backend_commit_errors_are_preserved_when_generations_are_current() {
        let db = test_support::open_db("mutation-index-context-non-transaction-error").await;
        let mut context =
            MutationIndexContext::for_configured_index_test(Arc::clone(db.simhasher_registry()));

        let inner = db.inner_db();
        let error = context
            .classify_commit_error(
                inner.as_ref(),
                slatedb::Error::invalid("injected non-transaction commit failure".to_string()),
            )
            .await;

        assert!(matches!(
            error,
            crate::HelixDbError::Storage(error)
                if error.kind() == slatedb::ErrorKind::Invalid
        ));

        let scope = keys::scope::DataScope::LegacyUnscoped;
        let definition = index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
            config::SecondaryIndexDefinition::node_equality("User", "email")
                .expect("secondary definition validates"),
        )
        .expect("secondary definition has a canonical identity");
        let record = index_lifecycle::IndexRecordV2::building(
            index_lifecycle::IndexId::initial(),
            definition,
            index_lifecycle::IndexRevision::initial(),
            index_lifecycle::PhysicalGeneration::Secondary {
                generation: index_lifecycle::IndexGenerationId::initial(),
            },
            index_lifecycle::IndexOperationId::new_v4(),
        )
        .expect("secondary record starts building")
        .transition(index_lifecycle::IndexStateTransition::Activate)
        .expect("secondary record activates");
        let handle = index_lifecycle::ActiveIndexHandle::try_from_record(scope, &record)
            .expect("active record projects an active handle");
        inner
            .put(
                crate::encoding::v2::keys::ManagedIndexKey::Data {
                    scope,
                    kind: crate::encoding::v2::keys::ScopedKey::index_record(
                        record.identity().clone(),
                    ),
                }
                .to_bytes(),
                crate::encoding::v2::values::encode_index_record(&record),
            )
            .await
            .expect("active record persists");
        context.active.insert_for_test(handle);

        let error = context
            .classify_commit_error(
                inner.as_ref(),
                slatedb::Error::transaction("injected transaction commit conflict".to_string()),
            )
            .await;
        assert!(matches!(
            error,
            crate::HelixDbError::Storage(error)
                if error.kind() == slatedb::ErrorKind::Transaction
        ));
        db.close().await.expect("test database closes");
    }
}
