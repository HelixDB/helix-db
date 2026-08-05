//! Explicit transaction-owned lifecycle for Active text mutation epochs.

use std::collections::BTreeMap;
use std::sync::Arc;

use slatedb::object_store::ObjectStore;
use slatedb::DbTransaction;

use crate::config::ActiveTextMutationLimits;
use crate::encoding::v2::keys::scope::DataScope;
use crate::error::{ActiveTextMutationResource, HelixDbError, Result};
use crate::index_lifecycle::graph_mutation::{GraphEntity, GraphMutationTransition};

use super::active_batch::CoalescedActiveTextMutation;

type EntityKey = (DataScope, GraphEntity);

/// Transaction-local Active text runtime with a terminal prepared state.
#[derive(Debug)]
pub(crate) struct ActiveTextMutationRuntime {
    state: ActiveTextMutationRuntimeState,
}

#[derive(Debug)]
enum ActiveTextMutationRuntimeState {
    Collecting(ActiveTextEpoch),
    Prepared,
}

/// One internally bounded set of coalesced text mutations.
#[derive(Debug, Default)]
struct ActiveTextEpoch {
    mutations: BTreeMap<EntityKey, CoalescedActiveTextMutation>,
    retained_bytes: u64,
}

/// A single mutation proven to fit a fresh empty text epoch.
#[derive(Debug)]
pub(crate) struct AdmissibleTextMutation(CoalescedActiveTextMutation);

/// Result of admitting one mutation without implicitly publishing storage.
#[derive(Debug)]
pub(crate) enum ActiveTextCollection {
    /// The current epoch now contains the transition's coalesced effect.
    Collected,
    /// The current epoch must drain before consuming this proven mutation.
    DrainRequired(AdmissibleTextMutation),
}

/// Index work staged by one successfully drained epoch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ActiveTextFlushOutcome {
    compaction_staged: bool,
}

impl ActiveTextFlushOutcome {
    /// Returns whether root/pointer work should wake compaction after commit.
    pub(crate) const fn compaction_staged(self) -> bool {
        self.compaction_staged
    }
}

impl ActiveTextMutationRuntime {
    /// Starts with one empty collecting epoch.
    pub(crate) fn new() -> Self {
        Self {
            state: ActiveTextMutationRuntimeState::Collecting(ActiveTextEpoch::default()),
        }
    }

    /// Validates the supplied before state and coalesces one entity transition.
    #[cfg(test)]
    pub(crate) async fn collect(
        &mut self,
        transaction: &DbTransaction,
        mutation: GraphMutationTransition,
        limits: ActiveTextMutationLimits,
    ) -> Result<()> {
        let expected_before = mutation.before().map(|row| row.encoded().clone());
        if transaction.get(mutation.graph_key()).await? != expected_before {
            return Err(HelixDbError::InvariantViolation(
                "Active text graph source disagrees with its supplied before state".to_string(),
            ));
        }
        match self.try_collect_routed(mutation, true, limits)? {
            ActiveTextCollection::Collected => Ok(()),
            ActiveTextCollection::DrainRequired(_) => Err(HelixDbError::InvariantViolation(
                "focused text collection requires an explicit epoch drain".to_string(),
            )),
        }
    }

    /// Coalesces a caller-observed transition only when text can observe it.
    ///
    /// Mutation helpers construct the transition from their transaction-owned
    /// source row. Avoiding another point read here preserves that observation
    /// as the single graph-row authority for foreground writes.
    pub(crate) fn try_collect_routed(
        &mut self,
        mutation: GraphMutationTransition,
        text_relevant: bool,
        limits: ActiveTextMutationLimits,
    ) -> Result<ActiveTextCollection> {
        let ActiveTextMutationRuntimeState::Collecting(epoch) = &mut self.state else {
            return Err(HelixDbError::InvariantViolation(
                "prepared Active text runtime cannot collect another mutation".to_string(),
            ));
        };
        let key = (mutation.scope(), mutation.entity());
        if !text_relevant && !epoch.mutations.contains_key(&key) {
            return Ok(ActiveTextCollection::Collected);
        }
        let (scope, entity, before, after) = mutation.into_states();
        let key = (scope, entity);
        let incoming = CoalescedActiveTextMutation {
            scope,
            entity,
            original: before,
            final_state: after,
        };
        if incoming.original == incoming.final_state {
            return Ok(ActiveTextCollection::Collected);
        }
        let existing = epoch.mutations.get(&key);
        if existing.is_some_and(|existing| existing.final_state != incoming.original) {
            return Err(HelixDbError::InvariantViolation(
                "Active text entity transitions are discontinuous within one epoch".to_string(),
            ));
        }
        let candidate = CoalescedActiveTextMutation {
            scope,
            entity,
            original: existing.map_or_else(
                || incoming.original.clone(),
                |existing| existing.original.clone(),
            ),
            final_state: incoming.final_state.clone(),
        };
        let candidate_is_empty = candidate.original == candidate.final_state;
        let retained_bytes = epoch
            .retained_bytes
            .saturating_sub(existing.map_or(0, CoalescedActiveTextMutation::retained_input_bytes))
            .saturating_add(if candidate_is_empty {
                0
            } else {
                candidate.retained_input_bytes()
            });
        let entity_count = epoch
            .mutations
            .len()
            .saturating_add(usize::from(existing.is_none() && !candidate_is_empty))
            .saturating_sub(usize::from(existing.is_some() && candidate_is_empty));
        let entity_count = u64::try_from(entity_count).unwrap_or(u64::MAX);
        let entity_limit = u64::try_from(limits.max_entities().get()).unwrap_or(u64::MAX);
        let exceeds_epoch =
            entity_count > entity_limit || retained_bytes > limits.max_input_bytes().get();
        if exceeds_epoch {
            let incoming_bytes = incoming.retained_input_bytes();
            if incoming_bytes > limits.max_input_bytes().get() {
                return Err(HelixDbError::ActiveTextMutationLimitExceeded {
                    resource: ActiveTextMutationResource::InputBytes,
                    observed: incoming_bytes,
                    limit: limits.max_input_bytes().get(),
                });
            }
            return Ok(ActiveTextCollection::DrainRequired(AdmissibleTextMutation(
                incoming,
            )));
        }
        epoch.retained_bytes = retained_bytes;
        if candidate_is_empty {
            epoch.mutations.remove(&key);
        } else {
            epoch.mutations.insert(key, candidate);
        }
        Ok(ActiveTextCollection::Collected)
    }

    /// Consumes a proof token after the previous epoch has drained.
    pub(crate) fn consume_admissible(&mut self, mutation: AdmissibleTextMutation) -> Result<()> {
        let ActiveTextMutationRuntimeState::Collecting(epoch) = &mut self.state else {
            return Err(HelixDbError::InvariantViolation(
                "prepared Active text runtime cannot consume a mutation".to_string(),
            ));
        };
        if !epoch.mutations.is_empty() || epoch.retained_bytes != 0 {
            return Err(HelixDbError::InvariantViolation(
                "admissible text mutation requires a freshly drained epoch".to_string(),
            ));
        }
        let mutation = mutation.0;
        let key = (mutation.scope, mutation.entity);
        epoch.retained_bytes = mutation.retained_input_bytes();
        assert!(epoch.mutations.insert(key, mutation).is_none());
        Ok(())
    }

    /// Drains one epoch, stages its text effects, and returns to empty collecting.
    pub(crate) async fn flush(
        &mut self,
        transaction: &DbTransaction,
        mutations: &super::mutation::TextMutationSet,
        routes: &crate::index_lifecycle::mutation_catalog::MutationRouteCatalog,
        limits: ActiveTextMutationLimits,
        object_store: &Arc<dyn ObjectStore>,
        database: &str,
    ) -> Result<ActiveTextFlushOutcome> {
        let state = std::mem::replace(
            &mut self.state,
            ActiveTextMutationRuntimeState::Collecting(ActiveTextEpoch::default()),
        );
        let epoch = match state {
            ActiveTextMutationRuntimeState::Collecting(epoch) => epoch,
            ActiveTextMutationRuntimeState::Prepared => {
                self.state = ActiveTextMutationRuntimeState::Prepared;
                return Err(HelixDbError::InvariantViolation(
                    "prepared Active text runtime cannot flush another epoch".to_string(),
                ));
            }
        };
        if epoch.mutations.is_empty() {
            assert_eq!(epoch.retained_bytes, 0);
            return Ok(ActiveTextFlushOutcome::default());
        }
        #[cfg(feature = "production-coverage")]
        let entity_count = epoch.mutations.len();
        let epoch = epoch.mutations.into_values().collect::<Vec<_>>();
        let prepared = super::active_batch::prepare_active_text_epoch(
            transaction,
            mutations,
            routes,
            epoch,
            limits,
        )
        .await?;
        #[cfg(feature = "production-coverage")]
        let upload_count = prepared.upload_count();
        let published = super::active_publication::publish_active_text_epoch(
            object_store,
            database,
            prepared,
            limits,
        )
        .await?;
        #[cfg(feature = "production-coverage")]
        crate::execution::interpreter::mutation::benchmark_telemetry::record_text_epoch(
            entity_count,
        );
        #[cfg(feature = "production-coverage")]
        crate::execution::interpreter::mutation::benchmark_telemetry::record_text_uploads(
            upload_count,
        );
        super::active_batch::stage_active_text_epoch(transaction, &published)?;
        Ok(ActiveTextFlushOutcome {
            compaction_staged: published.has_destination_work(),
        })
    }

    /// Flushes the final epoch and transitions irreversibly to prepared.
    pub(crate) async fn prepare(
        &mut self,
        transaction: &DbTransaction,
        mutations: &super::mutation::TextMutationSet,
        routes: &crate::index_lifecycle::mutation_catalog::MutationRouteCatalog,
        limits: ActiveTextMutationLimits,
        object_store: &Arc<dyn ObjectStore>,
        database: &str,
    ) -> Result<ActiveTextFlushOutcome> {
        let outcome = self
            .flush(
                transaction,
                mutations,
                routes,
                limits,
                object_store,
                database,
            )
            .await?;
        self.state = ActiveTextMutationRuntimeState::Prepared;
        Ok(outcome)
    }

    /// Consumes only the terminal prepared state at the commit boundary.
    pub(crate) fn consume_prepared(self) -> Result<()> {
        match self.state {
            ActiveTextMutationRuntimeState::Prepared => Ok(()),
            ActiveTextMutationRuntimeState::Collecting(_) => Err(HelixDbError::InvariantViolation(
                "Active text mutation runtime reached commit before prepare".to_string(),
            )),
        }
    }

    /// Counts coalesced entities retained by the collecting epoch.
    #[cfg(test)]
    pub(crate) fn pending_entity_count(&self) -> usize {
        match &self.state {
            ActiveTextMutationRuntimeState::Collecting(epoch) => epoch.mutations.len(),
            ActiveTextMutationRuntimeState::Prepared => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::{
        SearchIndexBackfillLimits, SearchIndexBatchLimits, TextBackfillCompactionLimits,
        TextBuildArtifactLimits,
    };
    use crate::encoding::v2::values::property::{encode_properties, Property};
    use crate::index_lifecycle::graph_mutation::{
        CanonicalPropertyRow, PropertyEdit, PropertyEditOutcome,
    };

    async fn test_db(name: &str) -> (Db, Arc<dyn ObjectStore>) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder(
            format!("index-lifecycle-active-text-runtime/{name}"),
            Arc::clone(&object_store),
        )
        .build()
        .await
        .expect("Active text runtime fixture opens");
        (db, object_store)
    }

    fn entity(id: u64) -> GraphEntity {
        GraphEntity::node(id)
    }

    fn properties(text: &str) -> Vec<Property> {
        vec![
            Property::string("$label", "Document"),
            Property::string("body", text),
        ]
    }

    fn small_input_limits() -> ActiveTextMutationLimits {
        SearchIndexBackfillLimits::try_new(
            SearchIndexBatchLimits::try_new(
                NonZeroUsize::new(2).unwrap(),
                NonZeroU64::new(4_096).unwrap(),
                NonZeroU64::new(4_096).unwrap(),
                NonZeroU64::new(4_096).unwrap(),
                NonZeroU64::MIN,
            )
            .unwrap(),
            NonZeroUsize::MIN,
            TextBuildArtifactLimits::new(NonZeroUsize::MIN, NonZeroU64::MIN),
            TextBackfillCompactionLimits::new(
                NonZeroUsize::MIN,
                NonZeroU64::new(512).unwrap(),
                NonZeroU64::new(4_096).unwrap(),
                NonZeroU64::new(4_096).unwrap(),
                NonZeroU64::new(4_096).unwrap(),
            ),
        )
        .unwrap()
        .active_text_mutation()
    }

    fn create(scope: DataScope, id: u64, properties: &[Property]) -> GraphMutationTransition {
        GraphMutationTransition::create(
            scope,
            entity(id),
            CanonicalPropertyRow::new(properties.to_vec()),
        )
    }

    fn replace(
        scope: DataScope,
        id: u64,
        before: &[Property],
        after: &[Property],
    ) -> GraphMutationTransition {
        let body = after
            .iter()
            .find(|property| property.name == "body")
            .expect("test replacement has a body")
            .clone();
        let PropertyEditOutcome::Changed(transition) = GraphMutationTransition::edit(
            scope,
            entity(id),
            CanonicalPropertyRow::new(before.to_vec()),
            PropertyEdit::set(body),
        ) else {
            panic!("test replacement changes the body");
        };
        transition
    }

    fn delete(scope: DataScope, id: u64, properties: &[Property]) -> GraphMutationTransition {
        GraphMutationTransition::delete(
            scope,
            entity(id),
            CanonicalPropertyRow::new(properties.to_vec()),
        )
    }

    fn collecting(
        runtime: &ActiveTextMutationRuntime,
    ) -> &BTreeMap<EntityKey, CoalescedActiveTextMutation> {
        let ActiveTextMutationRuntimeState::Collecting(epoch) = &runtime.state else {
            panic!("runtime should remain collecting");
        };
        &epoch.mutations
    }

    #[tokio::test]
    async fn collection_validates_before_and_coalesces_to_original_and_final() {
        let (db, _) = test_db("coalescing").await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("coalescing transaction opens");
        let scope = DataScope::LegacyUnscoped;
        let limits = SearchIndexBackfillLimits::default().active_text_mutation();
        let mut runtime = ActiveTextMutationRuntime::new();
        let created = properties("created");
        let updated = properties("updated");
        let create = create(scope, 7, &created);
        let graph_key = create.graph_key();
        runtime
            .collect(&transaction, create, limits)
            .await
            .expect("create collects against an absent row");
        transaction
            .put(&graph_key, encode_properties(&created))
            .expect("created graph row stages");
        runtime
            .collect(&transaction, replace(scope, 7, &created, &updated), limits)
            .await
            .expect("update observes the transaction-visible create");
        transaction
            .put(&graph_key, encode_properties(&updated))
            .expect("updated graph row stages");

        let retained = collecting(&runtime)
            .values()
            .next()
            .expect("one coalesced entity remains");
        assert_eq!(retained.original, None);
        assert_eq!(
            retained
                .final_state
                .as_ref()
                .map(CanonicalPropertyRow::properties),
            Some(updated.as_slice())
        );

        runtime
            .collect(&transaction, delete(scope, 7, &updated), limits)
            .await
            .expect("delete observes the transaction-visible update");
        transaction
            .delete(&graph_key)
            .expect("deleted graph row stages");
        assert!(
            collecting(&runtime).is_empty(),
            "create then delete is net zero"
        );

        assert!(matches!(
            runtime
                .collect(
                    &transaction,
                    replace(scope, 8, &properties("invented"), &properties("final")),
                    limits,
                )
                .await,
            Err(HelixDbError::InvariantViolation(reason))
                if reason == "Active text graph source disagrees with its supplied before state"
        ));
        drop(transaction);
        db.close().await.expect("coalescing fixture closes");
    }

    #[test]
    fn oversized_single_mutation_preserves_the_current_epoch_exactly() {
        let scope = DataScope::LegacyUnscoped;
        let limits = small_input_limits();
        let mut runtime = ActiveTextMutationRuntime::new();
        assert!(matches!(
            runtime
                .try_collect_routed(delete(scope, 1, &properties("small")), true, limits)
                .unwrap(),
            ActiveTextCollection::Collected
        ));
        let before = collecting(&runtime).clone();
        let retained_before = match &runtime.state {
            ActiveTextMutationRuntimeState::Collecting(epoch) => epoch.retained_bytes,
            ActiveTextMutationRuntimeState::Prepared => unreachable!(),
        };

        let error = runtime
            .try_collect_routed(
                delete(scope, 2, &properties(&"x".repeat(2_048))),
                true,
                limits,
            )
            .expect_err("an individually oversized mutation is rejected");

        assert!(matches!(
            error,
            HelixDbError::ActiveTextMutationLimitExceeded {
                resource: ActiveTextMutationResource::InputBytes,
                ..
            }
        ));
        assert_eq!(collecting(&runtime), &before);
        let ActiveTextMutationRuntimeState::Collecting(epoch) = &runtime.state else {
            unreachable!()
        };
        assert_eq!(epoch.retained_bytes, retained_before);
    }

    #[tokio::test]
    async fn flush_drains_one_epoch_and_prepare_is_terminal() {
        let (db, object_store) = test_db("lifecycle").await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("lifecycle transaction opens");
        let scope = DataScope::LegacyUnscoped;
        let limits = SearchIndexBackfillLimits::default().active_text_mutation();
        let mutations = super::super::mutation::TextMutationSet::empty();
        let routes = crate::index_lifecycle::mutation_catalog::MutationRouteCatalog::default();
        let mut runtime = ActiveTextMutationRuntime::new();
        let first = properties("first epoch");
        let first_mutation = create(scope, 1, &first);
        let first_key = first_mutation.graph_key();
        runtime
            .collect(&transaction, first_mutation, limits)
            .await
            .expect("first epoch collects");
        transaction
            .put(first_key, encode_properties(&first))
            .expect("first graph row stages");
        runtime
            .flush(
                &transaction,
                &mutations,
                &routes,
                limits,
                &object_store,
                "lifecycle",
            )
            .await
            .expect("first epoch flushes");
        assert!(collecting(&runtime).is_empty());

        let final_properties = properties("final epoch");
        let final_mutation = create(scope, 2, &final_properties);
        let final_key = final_mutation.graph_key();
        runtime
            .collect(&transaction, final_mutation, limits)
            .await
            .expect("collection resumes after flush");
        transaction
            .put(final_key, encode_properties(&final_properties))
            .expect("final graph row stages");
        runtime
            .prepare(
                &transaction,
                &mutations,
                &routes,
                limits,
                &object_store,
                "lifecycle",
            )
            .await
            .expect("prepare flushes and seals the final epoch");
        assert!(matches!(
            runtime
                .flush(
                    &transaction,
                    &mutations,
                    &routes,
                    limits,
                    &object_store,
                    "lifecycle",
                )
                .await,
            Err(HelixDbError::InvariantViolation(reason))
                if reason == "prepared Active text runtime cannot flush another epoch"
        ));
        assert!(matches!(
            runtime
                .collect(
                    &transaction,
                    create(scope, 3, &properties("late")),
                    limits,
                )
                .await,
            Err(HelixDbError::InvariantViolation(reason))
                if reason == "prepared Active text runtime cannot collect another mutation"
        ));
        runtime
            .consume_prepared()
            .expect("only prepared runtime reaches commit");
        drop(transaction);
        db.close().await.expect("lifecycle fixture closes");
    }

    #[tokio::test]
    async fn ten_thousand_deletions_drain_into_bounded_zero_upload_epochs() {
        let (db, object_store) = test_db("ten-thousand-deletions").await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("10k transaction opens");
        let scope = DataScope::LegacyUnscoped;
        let limits = SearchIndexBackfillLimits::default().active_text_mutation();
        assert_eq!(limits.max_entities().get(), 512);
        let mutations = super::super::mutation::TextMutationSet::empty();
        let routes = crate::index_v2::mutation_catalog::MutationRouteCatalog::default();
        let mut runtime = ActiveTextMutationRuntime::new();
        let deleted = properties("deleted");
        let mut drained_epochs = 0;

        for entity_id in 0..10_000 {
            match runtime
                .try_collect_routed(delete(scope, entity_id, &deleted), true, limits)
                .expect("deletion admission succeeds")
            {
                ActiveTextCollection::Collected => {}
                ActiveTextCollection::DrainRequired(admissible) => {
                    let outcome = runtime
                        .flush(
                            &transaction,
                            &mutations,
                            &routes,
                            limits,
                            &object_store,
                            "ten-thousand-deletions",
                        )
                        .await
                        .expect("bounded deletion epoch drains");
                    assert!(!outcome.compaction_staged());
                    runtime
                        .consume_admissible(admissible)
                        .expect("proven deletion starts the next epoch");
                    drained_epochs += 1;
                }
            }
        }
        let outcome = runtime
            .prepare(
                &transaction,
                &mutations,
                &routes,
                limits,
                &object_store,
                "ten-thousand-deletions",
            )
            .await
            .expect("final deletion epoch prepares");

        assert_eq!(drained_epochs, 19);
        assert!(!outcome.compaction_staged());
        runtime.consume_prepared().unwrap();
        transaction.rollback();
        db.close().await.expect("10k fixture closes");
    }

    #[tokio::test]
    async fn flush_rejects_a_graph_row_that_disagrees_with_the_coalesced_final_state() {
        let (db, object_store) = test_db("final-validation").await;
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("final-validation transaction opens");
        let scope = DataScope::LegacyUnscoped;
        let expected = properties("expected");
        let actual = properties("actual");
        let mutation = create(scope, 3, &expected);
        let key = mutation.graph_key();
        let mut runtime = ActiveTextMutationRuntime::new();
        let routes = crate::index_lifecycle::mutation_catalog::MutationRouteCatalog::default();
        runtime
            .collect(
                &transaction,
                mutation,
                SearchIndexBackfillLimits::default().active_text_mutation(),
            )
            .await
            .expect("supplied before state validates");
        transaction
            .put(key, encode_properties(&actual))
            .expect("divergent graph row stages");
        assert!(matches!(
            runtime
                .flush(
                    &transaction,
                    &super::super::mutation::TextMutationSet::empty(),
                    &routes,
                    SearchIndexBackfillLimits::default().active_text_mutation(),
                    &object_store,
                    "final-validation",
                )
                .await,
            Err(HelixDbError::InvariantViolation(reason))
                if reason == "Active text graph row disagrees with its coalesced final state"
        ));
        drop(transaction);
        db.close().await.expect("final-validation fixture closes");
    }

    #[test]
    fn collecting_runtime_cannot_reach_commit() {
        assert!(matches!(
            ActiveTextMutationRuntime::new().consume_prepared(),
            Err(HelixDbError::InvariantViolation(reason))
                if reason == "Active text mutation runtime reached commit before prepare"
        ));
    }
}
