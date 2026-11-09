//! Transaction-owned V2 index state for one graph mutation.
//!
//! The context holds the shared scope permit for the full graph transaction,
//! canonical secondary/vector/text generation work selected in that snapshot,
//! and the complete vector/text operations the transaction enqueues at
//! commit. The permit prevents an exclusive activation/cleanup checkpoint from
//! crossing the mutation commit boundary. Vector and text maintenance never
//! touches physical index rows here; the publication worker applies it later.

use std::collections::BTreeSet;

use crate::encoding::v2::keys;
use crate::index_lifecycle::queue::producer::{QueuedMutationCollector, StagedQueueWrites};

/// Node rows read to learn only whether they exist, with no read dependency.
///
/// An edge insert needs its endpoints to exist, not to stay unchanged. Only a
/// node delete ends a node's existence, and it always tombstones the node's
/// adjacency row in the same transaction. A tombstone is an exclusive write,
/// which conflicts with any disjoint merge on that row whatever its members.
/// So once this transaction stages a merge on `adjacency(n)`, it and any
/// delete of `n` cannot both commit, and a dependency on `n`'s row would only
/// abort it for a property change it never looked at.
///
/// A row left without such a merge takes the dependency at commit, so every
/// other read of this kind keeps its serializable meaning. An absent row
/// always does: no delete protects the claim that it does not exist.
#[derive(Default)]
struct ExistenceReads {
    present: BTreeSet<u64>,
    absent: BTreeSet<u64>,
    budget: Option<crate::query_resources::Budget>,
    /// Holds room for the dependency each read may take at commit.
    memory: Option<crate::query_resources::Reservation>,
}
impl std::fmt::Debug for ExistenceReads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExistenceReads")
            .field("present", &self.present)
            .field("absent", &self.absent)
            .finish_non_exhaustive()
    }
}
impl ExistenceReads {
    /// A set entry and its key in the transaction's dependency tables.
    const fn bytes(entries: usize) -> usize {
        entries.saturating_mul(size_of::<[usize; 24]>())
    }
}

/// Index state that is valid for exactly one graph mutation transaction.
#[derive(Debug)]
pub(crate) struct MutationIndexContext {
    pub(super) property_writes: super::property_writes::Pending,
    _scope_permit: Option<crate::index_lifecycle::IndexScopeMutationPermit>,
    active: crate::index_lifecycle::mutation_catalog::ActiveMutationCatalog,
    secondary: crate::index_lifecycle::secondary::SecondaryMutationSet,
    secondary_runtime: crate::index_lifecycle::secondary::SecondaryMutationRuntime,
    vector: crate::index_lifecycle::vector::VectorMutationSet,
    text: crate::index_lifecycle::text::mutation::TextMutationSet,
    routes: crate::index_lifecycle::mutation_catalog::MutationRouteCatalog,
    topology_runtime: super::topology::TopologyMutationRuntime,
    /// Complete vector/text operations staged at commit.
    queued: QueuedMutationCollector,
    /// Node index footprint of transitions not yet reported to the request's
    /// membership cache.
    node_index_writes: super::NodeIndexWrites,
    existence_reads: parking_lot::Mutex<ExistenceReads>,
}

/// Commit-owned index state after every transaction-local runtime is sealed.
pub(crate) struct PreparedMutationIndexContext {
    _property_writes: super::property_writes::Pending,
    _scope_permit: Option<crate::index_lifecycle::IndexScopeMutationPermit>,
    active_generations: Vec<crate::index_lifecycle::ActiveIndexHandle>,
    _secondary: crate::index_lifecycle::secondary::SecondaryMutationSet,
    _vector: crate::index_lifecycle::vector::VectorMutationSet,
    _text: crate::index_lifecycle::text::mutation::TextMutationSet,
}

impl MutationIndexContext {
    /// Creates transaction-local generation and queue tracking.
    pub(crate) fn new(
        scope_permit: crate::index_lifecycle::IndexScopeMutationPermit,
        loaded: crate::index_lifecycle::mutation_catalog::MutationIndexCatalog,
        scope: crate::encoding::v2::keys::scope::DataScope,
        budget: Option<&crate::query_resources::Budget>,
    ) -> Self {
        let (active, secondary, vector, text, routes) = loaded.into_components();
        Self {
            property_writes: super::property_writes::Pending::default(),
            _scope_permit: Some(scope_permit),
            active,
            secondary,
            secondary_runtime: crate::index_lifecycle::secondary::SecondaryMutationRuntime::default(
            ),
            vector,
            text,
            routes,
            topology_runtime: super::topology::TopologyMutationRuntime::new(budget),
            queued: QueuedMutationCollector::new(scope),
            node_index_writes: super::NodeIndexWrites::default(),
            existence_reads: parking_lot::Mutex::new(ExistenceReads {
                budget: budget.cloned(),
                ..ExistenceReads::default()
            }),
        }
    }

    /// Creates an uncoordinated empty V2 context for focused configured-index tests.
    #[cfg(test)]
    pub(crate) fn for_configured_index_test() -> Self {
        Self {
            property_writes: super::property_writes::Pending::default(),
            _scope_permit: None,
            active: crate::index_lifecycle::mutation_catalog::ActiveMutationCatalog::default(),
            secondary: crate::index_lifecycle::secondary::SecondaryMutationSet::empty(),
            secondary_runtime: crate::index_lifecycle::secondary::SecondaryMutationRuntime::default(
            ),
            vector: crate::index_lifecycle::vector::VectorMutationSet::empty(),
            text: crate::index_lifecycle::text::mutation::TextMutationSet::empty(),
            routes: crate::index_lifecycle::mutation_catalog::MutationRouteCatalog::default(),
            topology_runtime: super::topology::TopologyMutationRuntime::default(),
            queued: QueuedMutationCollector::new(
                crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped,
            ),
            node_index_writes: super::NodeIndexWrites::default(),
            existence_reads: parking_lot::Mutex::default(),
        }
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

    /// Routes one complete graph transition through every configured family.
    ///
    /// Every node index and `$label` bitmap change passes through here, so
    /// the transition's node index footprint is recorded for the request's
    /// membership cache before any family sees it. Secondary maintenance is
    /// collected for staging; vector/text work becomes complete queued
    /// operations and reads no index state.
    pub(crate) fn maintain_graph_indexes(
        &mut self,
        graph: crate::index_lifecycle::graph_mutation::GraphMutationTransition,
        budget: Option<&crate::query_resources::Budget>,
    ) -> Result<(), crate::HelixDbError> {
        self.node_index_writes.record(&graph);
        let routes = self.routes.targets_for_with_budget(&graph, budget)?;
        self.secondary_runtime
            .collect(graph.scope(), &self.secondary, &routes, &graph)?;
        self.queued
            .collect(&self.vector, &self.text, &routes, &graph)
    }

    /// Returns queued vector/text work staged by this transaction.
    pub(crate) const fn queued_mutations(&self) -> &QueuedMutationCollector {
        &self.queued
    }

    /// Builds this transaction's immutable queue operands and charges.
    pub(crate) fn finalize_queued(
        &self,
        max_operand_bytes: u64,
        text_limits: crate::config::ActiveTextMutationLimits,
    ) -> Result<StagedQueueWrites, crate::HelixDbError> {
        self.queued.finalize(max_operand_bytes, text_limits)
    }

    /// Takes the node index footprint recorded since the last call.
    pub(super) fn take_node_index_writes(&mut self) -> super::NodeIndexWrites {
        core::mem::take(&mut self.node_index_writes)
    }

    /// Records what `observed` says about nodes read with no dependency.
    /// Admission precedes the read that needs it, so a request refused for
    /// these dependencies fails before it stages work on their result.
    pub(super) fn admit_existence_reads(&self, count: usize) -> Result<(), crate::HelixDbError> {
        let mut reads = self.existence_reads.lock();
        let ExistenceReads {
            present,
            absent,
            budget,
            memory,
        } = &mut *reads;
        let Some(budget) = budget else {
            return Ok(());
        };
        let bytes = ExistenceReads::bytes(present.len() + absent.len() + count);
        match memory {
            Some(memory) => memory.resize(bytes),
            None => {
                *memory = Some(budget.reserve(bytes)?);
                Ok(())
            }
        }
    }

    /// Records the outcome of reads [`Self::admit_existence_reads`] admitted.
    pub(super) fn record_existence_reads(&self, observed: impl Iterator<Item = (u64, bool)>) {
        let mut reads = self.existence_reads.lock();
        observed.for_each(|(node_id, exists)| {
            if exists {
                reads.present.insert(node_id);
            } else {
                reads.absent.insert(node_id);
            }
        });
    }

    /// Takes the nodes whose existence reads no adjacency merge protects,
    /// releasing the room held for them. Call after the final topology flush.
    pub(super) fn take_uncovered_existence_reads(
        &mut self,
        scope: keys::scope::DataScope,
    ) -> Vec<u64> {
        let reads = std::mem::take(self.existence_reads.get_mut());
        let covered = reads.present.into_iter().filter(|node_id| {
            !self.topology_runtime.staged(
                &keys::DataKey::Data {
                    scope,
                    kind: keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(*node_id)),
                }
                .to_bytes(),
            )
        });
        covered.chain(reads.absent).collect()
    }

    /// Borrows the transaction-local topology collector.
    pub(super) const fn topology_mutations(
        &mut self,
    ) -> &mut super::topology::TopologyMutationRuntime {
        &mut self.topology_runtime
    }

    /// Topology flushes that staged at least one row.
    #[cfg(test)]
    pub(crate) const fn topology_staging_flushes(&self) -> usize {
        self.topology_runtime.staging_flushes
    }

    /// Rows topology flushes have staged.
    #[cfg(test)]
    pub(crate) const fn topology_staged_rows(&self) -> usize {
        self.topology_runtime.staged_rows
    }

    /// Rows the next topology flush would stage.
    pub(super) fn pending_topology_rows(&self) -> usize {
        self.topology_runtime.pending_rows()
    }

    /// Flushes one topology epoch before topology-dependent reads.
    pub(crate) fn flush_topology(
        &mut self,
        transaction: &impl crate::transaction::Mutation,
    ) -> Result<(), crate::HelixDbError> {
        self.topology_runtime.flush(transaction)
    }

    /// Reads current topology rows through the runtime's staged overlay.
    pub(crate) async fn observe_topology(
        &self,
        transaction: &impl crate::transaction::Mutation,
        keys: &[bytes::Bytes],
    ) -> Result<Vec<Option<bytes::Bytes>>, crate::HelixDbError> {
        self.topology_runtime.observe(transaction, keys).await
    }

    /// Flushes and seals topology state at the commit boundary.
    pub(crate) fn prepare_topology(
        &mut self,
        transaction: &impl crate::transaction::Mutation,
    ) -> Result<(), crate::HelixDbError> {
        self.topology_runtime.prepare(transaction)
    }

    /// Flushes routed secondary mutations through one ordered observation batch.
    pub(crate) async fn flush_secondary(
        &mut self,
        transaction: &impl crate::transaction::Mutation,
    ) -> Result<(), crate::HelixDbError> {
        self.secondary_runtime
            .flush(transaction, &self.secondary)
            .await
    }

    /// Flushes and seals the final secondary mutation epoch.
    pub(crate) async fn prepare_secondary(
        &mut self,
        transaction: &impl crate::transaction::Mutation,
    ) -> Result<(), crate::HelixDbError> {
        self.secondary_runtime
            .prepare(transaction, &self.secondary)
            .await
    }

    /// Consumes the sealed runtime and transfers all state to the commit boundary.
    pub(crate) fn into_prepared(self) -> Result<PreparedMutationIndexContext, crate::HelixDbError> {
        let Self {
            property_writes,
            _scope_permit,
            active,
            secondary,
            secondary_runtime,
            vector,
            text,
            routes: _,
            topology_runtime,
            queued: _,
            node_index_writes: _,
            existence_reads: _,
        } = self;
        topology_runtime.consume_prepared()?;
        secondary_runtime.consume_prepared()?;
        Ok(PreparedMutationIndexContext {
            _property_writes: property_writes,
            _scope_permit,
            active_generations: active.into_generations(),
            _secondary: secondary,
            _vector: vector,
            _text: text,
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
    if error.kind() == slatedb::ErrorKind::Closed(slatedb::CloseReason::Fenced) {
        return crate::HelixDbError::from_storage_commit(error);
    }
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
        let mut context = MutationIndexContext::for_configured_index_test();

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
