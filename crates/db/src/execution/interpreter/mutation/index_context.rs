//! Transaction-owned V2 index state for one graph mutation.
//!
//! The context holds the shared scope permit for the full graph transaction,
//! canonical secondary/vector/text generation work selected in that snapshot,
//! and vector cache writes. The permit prevents an exclusive
//! activation/cleanup checkpoint from crossing the mutation commit boundary;
//! vector cache writes remain publish-after-commit state.

use std::sync::Arc;

use crate::search::vector;

/// Canonical family state resolved in one graph mutation snapshot.
///
/// Grouping the active handles with each family-specific mutation set keeps
/// transaction construction from accepting a loose, reorderable list of
/// unrelated runtime arguments.
pub(crate) struct LoadedMutationIndexState {
    active_generations: Vec<crate::index_v2::ActiveIndexHandle>,
    secondary: crate::index_v2::secondary::SecondaryMutationSet,
    vector: crate::index_v2::vector::VectorMutationSet,
    text: crate::index_v2::text::mutation::TextMutationSet,
}

impl LoadedMutationIndexState {
    /// Binds all family mutation state to the snapshot that validated it.
    pub(crate) fn new(
        active_generations: Vec<crate::index_v2::ActiveIndexHandle>,
        secondary: crate::index_v2::secondary::SecondaryMutationSet,
        vector: crate::index_v2::vector::VectorMutationSet,
        text: crate::index_v2::text::mutation::TextMutationSet,
    ) -> Self {
        Self {
            active_generations,
            secondary,
            vector,
            text,
        }
    }
}

/// Index state that is valid for exactly one graph mutation transaction.
#[derive(Debug)]
pub(crate) struct MutationIndexContext {
    _scope_permit: Option<crate::index_v2::IndexScopeMutationPermit>,
    active_generations: Vec<crate::index_v2::ActiveIndexHandle>,
    secondary: crate::index_v2::secondary::SecondaryMutationSet,
    vector: crate::index_v2::vector::VectorMutationSet,
    text: crate::index_v2::text::mutation::TextMutationSet,
    active_text_runtime: crate::index_v2::text::active_publication::ActiveTextMutationRuntime,
    active_text_outbox: crate::index_v2::text::active_resolution::ActiveTextTransactionOutbox,
    vector_cache_writes: vector::VectorCacheWriteSet,
}

impl MutationIndexContext {
    /// Creates transaction-local generation and cache tracking.
    pub(crate) fn new(
        scope_permit: crate::index_v2::IndexScopeMutationPermit,
        loaded: LoadedMutationIndexState,
        active_text_runtime: crate::index_v2::text::active_publication::ActiveTextMutationRuntime,
        simhasher_registry: Arc<vector::SimHasherRegistry>,
    ) -> Self {
        Self {
            _scope_permit: Some(scope_permit),
            active_generations: loaded.active_generations,
            secondary: loaded.secondary,
            vector: loaded.vector,
            text: loaded.text,
            active_text_runtime,
            active_text_outbox: Default::default(),
            vector_cache_writes: vector::VectorCacheWriteSet::new(simhasher_registry),
        }
    }

    /// Creates an uncoordinated empty V2 context for focused configured-index tests.
    #[cfg(test)]
    pub(crate) fn for_configured_index_test(
        simhasher_registry: Arc<vector::SimHasherRegistry>,
    ) -> Self {
        Self {
            _scope_permit: None,
            active_generations: Vec::new(),
            secondary: crate::index_v2::secondary::SecondaryMutationSet::empty(),
            vector: crate::index_v2::vector::VectorMutationSet::empty(),
            text: crate::index_v2::text::mutation::TextMutationSet::empty(),
            active_text_runtime:
                crate::index_v2::text::active_publication::ActiveTextMutationRuntime::Unavailable,
            active_text_outbox: Default::default(),
            vector_cache_writes: vector::VectorCacheWriteSet::new(simhasher_registry),
        }
    }

    /// Returns vector rows dirtied by the transaction, grouped by generation.
    pub(crate) const fn vector_cache_writes(&self) -> &vector::VectorCacheWriteSet {
        &self.vector_cache_writes
    }

    /// Returns generation-qualified secondary work derived in this transaction.
    pub(crate) const fn secondary(&self) -> &crate::index_v2::secondary::SecondaryMutationSet {
        &self.secondary
    }

    /// Returns generation-qualified vector work derived in this transaction.
    pub(crate) const fn vector(&self) -> &crate::index_v2::vector::VectorMutationSet {
        &self.vector
    }

    /// Returns generation-qualified text work derived in this transaction.
    pub(crate) const fn text(&self) -> &crate::index_v2::text::mutation::TextMutationSet {
        &self.text
    }

    /// Returns upload authority installed for Active text request publication.
    pub(crate) const fn active_text_runtime(
        &self,
    ) -> &crate::index_v2::text::active_publication::ActiveTextMutationRuntime {
        &self.active_text_runtime
    }

    /// Retains a staged Active request until this transaction commits or aborts.
    pub(crate) fn active_text_outbox_mut(
        &mut self,
    ) -> &mut crate::index_v2::text::active_resolution::ActiveTextTransactionOutbox {
        &mut self.active_text_outbox
    }

    /// Transfers all retained Active request outcomes to the commit boundary.
    pub(crate) fn into_active_text_outbox(
        self,
    ) -> crate::index_v2::text::active_resolution::ActiveTextTransactionOutbox {
        self.active_text_outbox
    }

    /// Reclassifies a backend commit conflict when canonical DDL invalidated
    /// one of the exact active generations read by this graph transaction.
    ///
    /// Ordinary row conflicts retain the backend transaction error. A changed
    /// active record instead returns the stable `stale_index_generation`
    /// contract so callers know the graph mutation must restart with a fresh
    /// lifecycle snapshot.
    pub(crate) async fn classify_commit_error(
        &self,
        reader: &(impl slatedb::DbReadOps + Sync),
        error: slatedb::Error,
    ) -> crate::HelixDbError {
        if error.kind() != slatedb::ErrorKind::Transaction {
            return error.into();
        }
        for handle in &self.active_generations {
            let Err(error) =
                crate::index_v2::repository::revalidate_active_handle(reader, handle).await
            else {
                continue;
            };
            return error;
        }
        error.into()
    }
}
