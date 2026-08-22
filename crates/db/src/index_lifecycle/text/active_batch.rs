//! Transaction-epoch batching for Active V3 text mutations.
//!
//! Graph writes remain owned by the ordinary mutation path. This module reads
//! their coalesced final state, composes BUILD/statistics effects, and prepares
//! at most one optional immutable split for each generation partition.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use slatedb::DbTransaction;
use tokio::sync::Semaphore;

use crate::config::ActiveTextMutationLimits;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::property::Property;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::Key;
use crate::encoding::v2::values as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::graph_mutation::{CanonicalPropertyRow, GraphEntity};
use crate::index_lifecycle::{self, work};

use super::active_preflight::{ActiveTextMutationMeasurements, ActiveTextMutationUsage};
use super::mutation;

const TANTIVY_FOREGROUND_WRITER_BYTES: u64 = 15_000_000;

/// One entity reduced to its original and final property-row states.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoalescedActiveTextMutation {
    pub(crate) scope: DataScope,
    pub(crate) entity: GraphEntity,
    pub(crate) original: Option<CanonicalPropertyRow>,
    pub(crate) final_state: Option<CanonicalPropertyRow>,
}

impl CoalescedActiveTextMutation {
    fn graph_key(&self) -> Bytes {
        self.entity.property_key(self.scope)
    }

    fn original_properties(&self) -> &[Property] {
        self.original
            .as_ref()
            .map_or(&[], CanonicalPropertyRow::properties)
    }

    fn final_properties(&self) -> &[Property] {
        self.final_state
            .as_ref()
            .map_or(&[], CanonicalPropertyRow::properties)
    }

    pub(super) fn retained_input_bytes(&self) -> u64 {
        let key_bytes = u64::try_from(self.graph_key().len()).unwrap_or(u64::MAX);
        let original_bytes = self.original.as_ref().map_or(0, |row| {
            u64::try_from(row.encoded_len()).unwrap_or(u64::MAX)
        });
        let final_bytes = self.final_state.as_ref().map_or(0, |row| {
            u64::try_from(row.encoded_len()).unwrap_or(u64::MAX)
        });
        key_bytes
            .saturating_mul(2)
            .saturating_add(original_bytes)
            .saturating_add(final_bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DestinationKey {
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    partition: work::TextPartition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveDocument {
    analyzed: crate::search::text::IndexedTextAnalysis,
    requires_existing_live_state: bool,
}

struct DestinationWork {
    key: DestinationKey,
    handle: index_lifecycle::ActiveIndexHandle,
    definition: index_lifecycle::ValidatedTextIndexDefinition,
    live: BTreeMap<index_keys::IndexEntity, LiveDocument>,
    retirements: BTreeSet<index_keys::IndexEntity>,
}

impl DestinationWork {
    fn new(
        handle: &index_lifecycle::ActiveIndexHandle,
        definition: &index_lifecycle::ValidatedTextIndexDefinition,
        partition: work::TextPartition,
    ) -> Self {
        Self {
            key: DestinationKey {
                scope: handle.scope(),
                index_id: handle.index_id(),
                generation: handle.generation(),
                partition,
            },
            handle: handle.clone(),
            definition: definition.clone(),
            live: BTreeMap::new(),
            retirements: BTreeSet::new(),
        }
    }

    fn build_reservation_bytes(&self) -> u64 {
        if self.live.is_empty() {
            return 1;
        }
        self.live
            .values()
            .fold(TANTIVY_FOREGROUND_WRITER_BYTES, |total, document| {
                total.saturating_add(document.analyzed.retained_bytes())
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RowObservation {
    key: Bytes,
    value: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedRow {
    key: Bytes,
    value: Bytes,
}

/// One generation/partition prepared with one shared logical version.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedDestination {
    key: DestinationKey,
    observations: Vec<RowObservation>,
    writes: Vec<PreparedRow>,
    payload: Option<Bytes>,
    split: Option<work::SplitRef>,
    measurements: ActiveTextMutationMeasurements,
}

/// All text effects for one drained transaction flush epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedActiveTextEpoch {
    build_deltas: Vec<mutation::PreparedTextBuildDeltas>,
    statistics: super::statistics::PreparedTextStatisticsBatch,
    destinations: Vec<PreparedDestination>,
    measurements: ActiveTextMutationMeasurements,
}

impl PreparedActiveTextEpoch {
    /// Moves exact immutable payloads out in deterministic destination order.
    pub(crate) fn take_uploads(&mut self) -> Vec<(Bytes, work::SplitRef)> {
        self.destinations
            .iter_mut()
            .filter_map(|destination| destination.payload.take().zip(destination.split))
            .collect()
    }

    /// Returns the exact number of live destinations requiring one upload.
    pub(crate) fn upload_count(&self) -> usize {
        self.destinations
            .iter()
            .filter(|destination| destination.payload.is_some())
            .count()
    }

    /// Returns whether root/pointer work should wake compaction after commit.
    pub(crate) const fn has_destination_work(&self) -> bool {
        !self.destinations.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveTextDocument {
    partition: work::TextPartition,
    text: String,
}

struct AnalyzedActiveTextDocument {
    partition: work::TextPartition,
    analyzed: crate::search::text::IndexedTextAnalysis,
}

/// Prepares a complete epoch without staging graph or index rows.
pub(crate) async fn prepare_active_text_epoch(
    transaction: &DbTransaction,
    mutations: &mutation::TextMutationSet,
    routes: &index_lifecycle::mutation_catalog::MutationRouteCatalog,
    graphs: Vec<CoalescedActiveTextMutation>,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedActiveTextEpoch> {
    let entity_count = u64::try_from(graphs.len()).unwrap_or(u64::MAX);
    if entity_count > u64::try_from(limits.max_entities().get()).unwrap_or(u64::MAX) {
        return Err(HelixDbError::ActiveTextMutationLimitExceeded {
            resource: crate::error::ActiveTextMutationResource::Entities,
            observed: entity_count,
            limit: u64::try_from(limits.max_entities().get()).unwrap_or(u64::MAX),
        });
    }
    validate_final_graph_states(transaction, &graphs).await?;

    let mut active_handles = mutations
        .active_handles()
        .iter()
        .filter(|handle| handle.text_definition().is_some())
        .collect::<Vec<_>>();
    active_handles.sort_by_key(|handle| (handle.index_id(), handle.generation()));
    let mut identities = HashSet::with_capacity(active_handles.len());
    for handle in &active_handles {
        if !identities.insert((handle.scope(), handle.index_id(), handle.generation())) {
            return Err(corruption(
                "Active text epoch contains a duplicate canonical generation",
            ));
        }
    }

    let mut statistics = super::statistics::PreparedTextStatisticsBatch::default();
    let statistics_marker_keys = graphs
        .iter()
        .flat_map(|graph| {
            let entity = graph.entity.index_entity();
            routes
                .targets_for_states(
                    entity.kind,
                    graph.original_properties(),
                    graph.final_properties(),
                )
                .iter()
                .filter_map(|target| {
                    let identity = match target {
                        index_lifecycle::mutation_catalog::MutationRouteTarget::TextBuilding(
                            ordinal,
                        ) => mutations.building_identity(ordinal),
                        index_lifecycle::mutation_catalog::MutationRouteTarget::TextActive(
                            ordinal,
                        ) => mutations
                            .active_handles()
                            .get(ordinal)
                            .map(|handle| (handle.index_id(), handle.generation())),
                        index_lifecycle::mutation_catalog::MutationRouteTarget::Secondary(_)
                        | index_lifecycle::mutation_catalog::MutationRouteTarget::Vector(_) => None,
                    }?;
                    Some(super::statistics::entity_key(
                        graph.scope,
                        identity.0,
                        identity.1,
                        entity,
                    ))
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    statistics
        .observe_rows(transaction, statistics_marker_keys)
        .await?;
    let mut build_deltas = Vec::with_capacity(graphs.len());
    let mut destinations = BTreeMap::<DestinationKey, DestinationWork>::new();
    let mut analysis_budget =
        crate::search::text::TextAnalysisMemoryBudget::new(limits.max_input_bytes());
    let mut graph_input_bytes = 0_u64;
    for graph in &graphs {
        graph_input_bytes = graph_input_bytes.saturating_add(graph.retained_input_bytes());
        let entity = graph.entity.index_entity();
        let graph_routes = routes.targets_for_states(
            entity.kind,
            graph.original_properties(),
            graph.final_properties(),
        );
        build_deltas.push(
            mutation::prepare_text_build_deltas_in_batch(
                transaction,
                graph.scope,
                mutations,
                &graph_routes,
                mutation::TextEntityMutation::new(
                    entity.kind,
                    entity.id.get(),
                    graph.original_properties(),
                    graph.final_properties(),
                ),
                &mut statistics,
            )
            .await?,
        );

        for ordinal in graph_routes.iter().filter_map(|target| match target {
            index_lifecycle::mutation_catalog::MutationRouteTarget::TextActive(ordinal) => {
                Some(ordinal)
            }
            index_lifecycle::mutation_catalog::MutationRouteTarget::Secondary(_)
            | index_lifecycle::mutation_catalog::MutationRouteTarget::Vector(_)
            | index_lifecycle::mutation_catalog::MutationRouteTarget::TextBuilding(_) => None,
        }) {
            let handle = mutations.active_handles().get(ordinal).ok_or_else(|| {
                corruption("text mutation route named an Active target outside its catalog")
            })?;
            if handle.scope() != graph.scope {
                return Err(corruption(
                    "Active text generation scope disagrees with its graph mutation",
                ));
            }
            let definition = handle
                .text_definition()
                .expect("the filtered Active handle is text-typed");
            if definition.identity() != *handle.identity() {
                return Err(corruption(
                    "Active text handle definition disagrees with its canonical identity",
                ));
            }
            if definition.element_kind() != entity.kind {
                continue;
            }
            let before = active_document(definition, graph.original_properties())?;
            let after = active_document(definition, graph.final_properties())?;
            if before == after {
                continue;
            }
            let before = before
                .map(|document| analyze_document(definition, document, &mut analysis_budget))
                .transpose()?;
            let after = after
                .map(|document| analyze_document(definition, document, &mut analysis_budget))
                .transpose()?;
            let before_contribution = contribution(definition, before.as_ref())?;
            let after_contribution = contribution(definition, after.as_ref())?;
            let transition = super::statistics::prepare_active_in_batch(
                transaction,
                &statistics,
                super::statistics::ActiveTextStatisticsMutation::new(
                    graph.scope,
                    handle.index_id(),
                    handle.generation(),
                    entity,
                    before_contribution,
                    after_contribution,
                ),
            )
            .await?;
            statistics.push(transition)?;
            group_effect(&mut destinations, handle, definition, entity, before, after)?;
        }
    }

    let build_budget_bytes = limits.max_input_bytes().get();
    let build_budget_permits = usize::try_from(build_budget_bytes.min(u32::MAX.into()))
        .expect("u32 byte budgets fit usize");
    let build_budget = Arc::new(Semaphore::new(build_budget_permits));
    let destination_concurrency = super::active_text_destination_concurrency(destinations.len());
    let mut prepared_destinations = stream::iter(destinations.into_values().enumerate())
        .map(|(ordinal, destination)| {
            let build_budget = Arc::clone(&build_budget);
            async move {
                let reservation = destination
                    .build_reservation_bytes()
                    .min(build_budget_bytes)
                    .min(u64::from(u32::MAX));
                let permit = build_budget
                    .acquire_many_owned(
                        u32::try_from(reservation).expect("reservation is clamped to u32"),
                    )
                    .await
                    .map_err(|_| {
                        HelixDbError::InvariantViolation(
                            "Active text build byte budget closed during preparation".to_string(),
                        )
                    })?;
                prepare_destination(transaction, destination, limits)
                    .await
                    .map(|prepared| {
                        drop(permit);
                        (ordinal, prepared)
                    })
            }
        })
        .buffer_unordered(destination_concurrency.get())
        .try_collect::<Vec<_>>()
        .await?;
    prepared_destinations.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    let prepared_destinations = prepared_destinations
        .into_iter()
        .map(|(_, destination)| destination)
        .collect::<Vec<_>>();

    let build_measurements = build_deltas.iter().fold(
        mutation::TextBuildDeltaMeasurements::default(),
        |total, delta| {
            let measured = delta.row_measurements();
            mutation::TextBuildDeltaMeasurements::from_parts(
                total.input_bytes().saturating_add(measured.input_bytes()),
                total
                    .output_operations()
                    .saturating_add(measured.output_operations()),
                total.output_bytes().saturating_add(measured.output_bytes()),
            )
        },
    );
    let statistics_measurements = statistics.measurements();
    let destination_measurements = prepared_destinations.iter().fold(
        (0_u64, 0_u64, 0_u64, 0_u64, 0_u64, 0_u64),
        |(input, operations, output, split, retained, page), destination| {
            let measured = destination.measurements;
            (
                input.saturating_add(measured.input_bytes()),
                operations.saturating_add(measured.output_operations()),
                output.saturating_add(measured.output_bytes()),
                split.max(measured.split_bytes()),
                retained.saturating_add(measured.retained_split_bytes()),
                page.max(measured.manifest_page_bytes()),
            )
        },
    );
    let measurements = ActiveTextMutationMeasurements::try_admit_epoch(
        limits,
        ActiveTextMutationUsage {
            entities: entity_count,
            input_bytes: graph_input_bytes
                .saturating_add(build_measurements.input_bytes())
                .saturating_add(statistics_measurements.0)
                .saturating_add(destination_measurements.0),
            output_operations: build_measurements
                .output_operations()
                .saturating_add(statistics_measurements.1)
                .saturating_add(destination_measurements.1),
            output_bytes: build_measurements
                .output_bytes()
                .saturating_add(statistics_measurements.2)
                .saturating_add(destination_measurements.2),
            split_bytes: destination_measurements.3,
            retained_split_bytes: destination_measurements.4,
            manifest_page_bytes: destination_measurements.5,
        },
    )?;
    Ok(PreparedActiveTextEpoch {
        build_deltas,
        statistics,
        destinations: prepared_destinations,
        measurements,
    })
}

/// Validates transaction-visible graph authority in one bounded multi-get.
async fn validate_final_graph_states(
    transaction: &DbTransaction,
    graphs: &[CoalescedActiveTextMutation],
) -> Result<()> {
    const GRAPH_ROWS_PER_CHUNK: usize = 512;

    for chunk in graphs.chunks(GRAPH_ROWS_PER_CHUNK) {
        let keys = chunk
            .iter()
            .map(CoalescedActiveTextMutation::graph_key)
            .collect::<Vec<_>>();
        let values = transaction.multi_get(&keys).await?;
        for (graph, observed) in chunk.iter().zip(values) {
            let expected = graph.final_state.as_ref().map(|row| row.encoded().clone());
            if observed != expected {
                return Err(HelixDbError::InvariantViolation(
                    "Active text graph row disagrees with its coalesced final state".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn active_document(
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    properties: &[Property],
) -> Result<Option<ActiveTextDocument>> {
    match super::projection::project(definition, properties).map_err(|error| {
        HelixDbError::InvalidIndexSourceData {
            reason: format!(
                "text index {}:{}: {error}",
                definition.label().as_str(),
                definition.property().as_str(),
            ),
        }
    })? {
        super::projection::TextSourceProjection::NotIndexed => Ok(None),
        super::projection::TextSourceProjection::Indexed { partition, text } => {
            Ok(Some(ActiveTextDocument { partition, text }))
        }
    }
}

fn contribution(
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    document: Option<&AnalyzedActiveTextDocument>,
) -> Result<work::TextStatisticsContribution> {
    match document {
        Some(document) => super::statistics::present_contribution_from_analysis(
            definition.analyzer(),
            document.partition.clone(),
            document.analyzed.statistics(),
        ),
        None => Ok(work::TextStatisticsContribution::Absent),
    }
}

fn analyze_document(
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    document: ActiveTextDocument,
    budget: &mut crate::search::text::TextAnalysisMemoryBudget,
) -> Result<AnalyzedActiveTextDocument> {
    Ok(AnalyzedActiveTextDocument {
        partition: document.partition,
        analyzed: crate::search::text::analyze_text_for_indexing(
            definition.analyzer(),
            document.text,
            budget,
        )?,
    })
}

fn group_effect(
    destinations: &mut BTreeMap<DestinationKey, DestinationWork>,
    handle: &index_lifecycle::ActiveIndexHandle,
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    entity: index_keys::IndexEntity,
    before: Option<AnalyzedActiveTextDocument>,
    after: Option<AnalyzedActiveTextDocument>,
) -> Result<()> {
    match (before, after) {
        (None, None) => Ok(()),
        (None, Some(current)) => {
            insert_live(destinations, handle, definition, entity, current, false)
        }
        (Some(previous), None) => {
            insert_retirement(destinations, handle, definition, entity, previous.partition)
        }
        (Some(previous), Some(current)) if previous.partition == current.partition => {
            insert_live(destinations, handle, definition, entity, current, true)
        }
        (Some(previous), Some(current)) => {
            insert_retirement(destinations, handle, definition, entity, previous.partition)?;
            insert_live(destinations, handle, definition, entity, current, false)
        }
    }
}

fn destination_mut<'destination>(
    destinations: &'destination mut BTreeMap<DestinationKey, DestinationWork>,
    handle: &index_lifecycle::ActiveIndexHandle,
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    partition: work::TextPartition,
) -> &'destination mut DestinationWork {
    let key = DestinationKey {
        scope: handle.scope(),
        index_id: handle.index_id(),
        generation: handle.generation(),
        partition: partition.clone(),
    };
    destinations
        .entry(key)
        .or_insert_with(|| DestinationWork::new(handle, definition, partition))
}

fn insert_live(
    destinations: &mut BTreeMap<DestinationKey, DestinationWork>,
    handle: &index_lifecycle::ActiveIndexHandle,
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    entity: index_keys::IndexEntity,
    document: AnalyzedActiveTextDocument,
    requires_existing_live_state: bool,
) -> Result<()> {
    let destination = destination_mut(destinations, handle, definition, document.partition);
    if destination.retirements.contains(&entity)
        || destination
            .live
            .insert(
                entity,
                LiveDocument {
                    analyzed: document.analyzed,
                    requires_existing_live_state,
                },
            )
            .is_some()
    {
        return Err(corruption(
            "Active text epoch produced duplicate work for one destination entity",
        ));
    }
    Ok(())
}

fn insert_retirement(
    destinations: &mut BTreeMap<DestinationKey, DestinationWork>,
    handle: &index_lifecycle::ActiveIndexHandle,
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    entity: index_keys::IndexEntity,
    partition: work::TextPartition,
) -> Result<()> {
    let destination = destination_mut(destinations, handle, definition, partition);
    if destination.live.contains_key(&entity) || !destination.retirements.insert(entity) {
        return Err(corruption(
            "Active text epoch produced duplicate work for one destination entity",
        ));
    }
    Ok(())
}

async fn prepare_destination(
    transaction: &DbTransaction,
    destination: DestinationWork,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedDestination> {
    let DestinationWork {
        key,
        handle,
        definition,
        live,
        retirements,
    } = destination;
    let (record_key, record_value) =
        index_lifecycle::repository::revalidate_active_handle_row(transaction, &handle).await?;
    let mut observations = vec![RowObservation {
        key: record_key,
        value: Some(record_value),
    }];
    let root_typed = index_keys::TextManifestRootKey {
        index_id: key.index_id,
        generation: key.generation,
        partition: key.partition.fingerprint(),
    };
    let root_key = scoped_key(
        key.scope,
        index_keys::ScopedKey::TextManifestRoot(root_typed),
    );
    let root_bytes = transaction.get(&root_key).await?;
    observations.push(RowObservation {
        key: root_key.clone(),
        value: root_bytes.clone(),
    });
    let root = match root_bytes {
        Some(bytes) => index_values::decode_manifest_root(&bytes)?,
        None => {
            work::TextManifestRootValue::empty(key.index_id, key.generation, key.partition.clone())
        }
    };
    if root.index_id() != key.index_id
        || root.generation() != key.generation
        || root.partition() != &key.partition
    {
        return Err(corruption(
            "Active text destination root key/value ownership mismatch",
        ));
    }
    if !retirements.is_empty() && root.page_count() == 0 {
        return Err(corruption(
            "Active text delete-only destination has an empty manifest",
        ));
    }
    let corpus_key =
        super::statistics::corpus_key(key.scope, key.index_id, key.generation, &key.partition);
    let corpus_bytes = transaction.get(&corpus_key).await?;
    observations.push(RowObservation {
        key: corpus_key,
        value: corpus_bytes.clone(),
    });
    super::statistics::validate_manifest_corpus(
        corpus_bytes.as_deref(),
        key.index_id,
        key.generation,
        &key.partition,
        root.split_count(),
    )?;

    let last_page = if root.page_count() == 0 {
        None
    } else {
        let page_number = root.page_count() - 1;
        let page_typed = index_keys::TextManifestPageKey {
            root: root_typed,
            page: page_number,
        };
        let page_key = scoped_key(
            key.scope,
            index_keys::ScopedKey::TextManifestPage(page_typed),
        );
        let Some(page_bytes) = transaction.get(&page_key).await? else {
            return Err(corruption(
                "Active text destination is missing its last contiguous page",
            ));
        };
        observations.push(RowObservation {
            key: page_key,
            value: Some(page_bytes.clone()),
        });
        let page = index_values::decode_manifest_page(&page_bytes)?;
        if page.index_id() != key.index_id
            || page.generation() != key.generation
            || page.partition() != &key.partition
            || page.page() != page_number
        {
            return Err(corruption(
                "Active text destination page key/value ownership mismatch",
            ));
        }
        Some(page)
    };
    let next_revision = root
        .revision()
        .checked_next()
        .map_err(|_| corruption("Active text destination manifest revision is exhausted"))?;
    let logical_version = index_lifecycle::TextLogicalVersion::new(next_revision.get())
        .expect("a non-zero manifest revision forms a logical version");

    let state_requests = live
        .iter()
        .map(|(entity, document)| (*entity, document.requires_existing_live_state, true))
        .chain(retirements.iter().map(|entity| (*entity, true, false)))
        .map(|(entity, requires_live, next_live)| {
            let state_key = scoped_key(
                key.scope,
                index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                    root: root_typed,
                    entity,
                }),
            );
            (entity, requires_live, next_live, state_key)
        })
        .collect::<Vec<_>>();
    let state_keys = state_requests
        .iter()
        .map(|(_, _, _, key)| key.clone())
        .collect::<Vec<_>>();
    let state_values = transaction.multi_get(&state_keys).await?;
    let mut state_writes = Vec::with_capacity(state_requests.len());
    for ((entity, requires_live, next_live, state_key), state_bytes) in
        state_requests.into_iter().zip(state_values)
    {
        observations.push(RowObservation {
            key: state_key.clone(),
            value: state_bytes.clone(),
        });
        validate_existing_state(
            state_bytes.as_deref(),
            &key,
            entity,
            root.revision().get(),
            requires_live,
        )?;
        state_writes.push(PreparedRow {
            key: state_key,
            value: encode_state(&key, entity, logical_version, next_live),
        });
    }
    state_writes.sort_by(|left, right| left.key.cmp(&right.key));

    let (payload, split) = if live.is_empty() {
        (None, None)
    } else {
        if handle.text_definition() != Some(&definition) {
            return Err(corruption(
                "Active text destination definition disagrees with its handle",
            ));
        }
        let runtime_definition = definition.to_runtime();
        let documents = live
            .into_iter()
            .map(
                |(entity, document)| crate::search::text::AnalyzedTextDocumentInput {
                    entity_id: entity.id.get(),
                    logical_version: logical_version.get(),
                    analyzed: document.analyzed,
                },
            )
            .collect::<Vec<_>>();
        let built = tokio::task::spawn_blocking(move || {
            let Some(unpublished) = crate::search::text::build_analyzed_documents_as_split(
                &runtime_definition,
                documents,
            )?
            else {
                return Err(corruption(
                    "non-empty Active text destination produced no immutable split",
                ));
            };
            let (payload, split, pruning) = unpublished.into_parts();
            let split = work::SplitRef::try_new(
                work::BlobRef::new(split.blob.sha256, split.blob.size_bytes),
                split.footer_offset,
                split.footer_len,
                split.hotcache_len,
                split.total_size_bytes,
                pruning,
            )
            .map_err(|error| {
                corruption(format!("Active text split metadata is invalid: {error}"))
            })?;
            Ok::<_, HelixDbError>((payload, split))
        })
        .await
        .map_err(|error| {
            HelixDbError::InvariantViolation(format!(
                "Active text destination builder task failed: {error}"
            ))
        })??;
        if built.1.blob().size() > limits.max_split_bytes().get() {
            return Err(HelixDbError::ActiveTextMutationLimitExceeded {
                resource: crate::error::ActiveTextMutationResource::SplitBytes,
                observed: built.1.blob().size(),
                limit: limits.max_split_bytes().get(),
            });
        }
        (Some(built.0), Some(built.1))
    };

    let mut writes = Vec::with_capacity(state_writes.len() + 3);
    let (next_root, page_write, pointer_page) = match split {
        Some(split) => {
            append_split(
                transaction,
                AppendSplitRequest {
                    key: &key,
                    root_typed,
                    root: &root,
                    last_page,
                    split,
                    next_revision,
                    limits,
                    observations: &mut observations,
                },
            )
            .await?
        }
        None => {
            let pointer_page = root.page_count() - 1;
            let next_root = work::TextManifestRootValue::try_new(
                key.index_id,
                key.generation,
                key.partition.clone(),
                next_revision,
                root.page_count(),
                root.split_count(),
            )
            .map_err(|error| corruption(format!("delete-only root is invalid: {error}")))?;
            (next_root, None, pointer_page)
        }
    };
    writes.push(PreparedRow {
        key: root_key,
        value: index_values::encode_manifest_root(&next_root),
    });
    if let Some(page_write) = page_write {
        writes.push(page_write);
    }
    writes.extend(state_writes);
    let target = index_keys::TextCompactionTarget::try_new(
        key.scope,
        handle.identity().clone(),
        key.index_id,
        key.generation,
        key.partition.fingerprint(),
        pointer_page,
    )?;
    let pointer_key = Key::Global {
        kind: index_keys::GlobalKey::TextCompactionPointer(target),
    }
    .to_bytes();
    // Refresh scheduling after the root revision changes. This tail hint does
    // not promise that compaction will reclaim any particular retired entity.
    writes.push(PreparedRow {
        key: pointer_key,
        value: index_values::encode_metadata_value(
            &index_lifecycle::IndexV2MetadataValue::TextCompactionPointer(
                index_lifecycle::TextCompactionPointerValue {
                    revision: next_revision,
                },
            ),
        ),
    });
    writes.sort_by(|left, right| left.key.cmp(&right.key));

    let input_bytes = observations.iter().fold(0_u64, |total, observation| {
        total
            .saturating_add(u64::try_from(observation.key.len()).unwrap_or(u64::MAX))
            .saturating_add(
                observation
                    .value
                    .as_ref()
                    .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
            )
    });
    let output_bytes = writes.iter().fold(0_u64, |total, write| {
        total
            .saturating_add(u64::try_from(write.key.len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(write.value.len()).unwrap_or(u64::MAX))
    });
    let split_bytes = split.map_or(0, |split| split.blob().size());
    let page_bytes = writes
        .iter()
        .filter_map(|write| {
            Key::parse_from_slice(key.scope, &write.key)
                .ok()
                .and_then(|parsed| match parsed {
                    Key::Data {
                        kind: index_keys::ScopedKey::TextManifestPage(_),
                        ..
                    } => Some(u64::try_from(write.value.len()).unwrap_or(u64::MAX)),
                    Key::Global { .. } | Key::Data { .. } => None,
                })
        })
        .max()
        .unwrap_or(0);
    let measurements = ActiveTextMutationMeasurements::try_admit_epoch(
        limits,
        ActiveTextMutationUsage {
            entities: 0,
            input_bytes,
            output_operations: u64::try_from(writes.len()).unwrap_or(u64::MAX),
            output_bytes,
            split_bytes,
            retained_split_bytes: split_bytes,
            manifest_page_bytes: page_bytes,
        },
    )?;
    Ok(PreparedDestination {
        key,
        observations,
        writes,
        payload,
        split,
        measurements,
    })
}

fn validate_existing_state(
    state_bytes: Option<&[u8]>,
    key: &DestinationKey,
    entity: index_keys::IndexEntity,
    root_revision: u64,
    requires_live: bool,
) -> Result<()> {
    let Some(state_bytes) = state_bytes else {
        if requires_live {
            return Err(corruption(
                "Active text destination found no required live entity state",
            ));
        }
        return Ok(());
    };
    let state = index_values::decode_text_entity_state(state_bytes)?;
    if state.index_id != key.index_id
        || state.generation != key.generation
        || state.partition != key.partition
        || state.entity_kind != entity.kind
        || state.entity_id != entity.id
        || state.logical_version.get() > root_revision
        || (requires_live && !state.live)
    {
        return Err(corruption(
            "Active text entity-state ownership or live version mismatch",
        ));
    }
    Ok(())
}

fn encode_state(
    key: &DestinationKey,
    entity: index_keys::IndexEntity,
    logical_version: index_lifecycle::TextLogicalVersion,
    live: bool,
) -> Bytes {
    index_values::encode_text_entity_state(&work::TextEntityStateValue {
        index_id: key.index_id,
        generation: key.generation,
        partition: key.partition.clone(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version,
        live,
    })
}

struct AppendSplitRequest<'a> {
    key: &'a DestinationKey,
    root_typed: index_keys::TextManifestRootKey,
    root: &'a work::TextManifestRootValue,
    last_page: Option<work::TextManifestPageValue>,
    split: work::SplitRef,
    next_revision: index_lifecycle::TextManifestRevision,
    limits: ActiveTextMutationLimits,
    observations: &'a mut Vec<RowObservation>,
}

async fn append_split(
    transaction: &DbTransaction,
    request: AppendSplitRequest<'_>,
) -> Result<(work::TextManifestRootValue, Option<PreparedRow>, u32)> {
    let AppendSplitRequest {
        key,
        root_typed,
        root,
        last_page,
        split,
        next_revision,
        limits,
        observations,
    } = request;
    let (page_typed, page, next_root) = match last_page {
        None => {
            let page_typed = index_keys::TextManifestPageKey {
                root: root_typed,
                page: 0,
            };
            let page_key = scoped_key(
                key.scope,
                index_keys::ScopedKey::TextManifestPage(page_typed),
            );
            let existing = transaction.get(&page_key).await?;
            observations.push(RowObservation {
                key: page_key,
                value: existing.clone(),
            });
            if existing.is_some() {
                return Err(corruption(
                    "empty Active text manifest has an occupied first page",
                ));
            }
            let page = work::TextManifestPageValue::try_new(
                key.index_id,
                key.generation,
                key.partition.clone(),
                0,
                vec![split],
            )
            .expect("one split forms a valid first page");
            let next_root = root
                .append_page(0, NonZeroU32::MIN)
                .expect("a validated empty root accepts its first page");
            (page_typed, page, next_root)
        }
        Some(last_page) => {
            let page_number = last_page.page();
            let appended = (last_page.entries().len() < work::TextManifestPageValue::MAX_ENTRIES)
                .then(|| {
                    work::TextManifestPageValue::try_new(
                        key.index_id,
                        key.generation,
                        key.partition.clone(),
                        page_number,
                        last_page
                            .entries()
                            .iter()
                            .copied()
                            .chain(std::iter::once(split))
                            .collect(),
                    )
                    .expect("split appended below the entry cap remains valid")
                });
            let append_fits = appended.as_ref().is_some_and(|page| {
                u64::try_from(index_values::encode_manifest_page(page).len()).unwrap_or(u64::MAX)
                    <= limits.max_manifest_page_bytes().get()
            });
            if append_fits {
                let page = appended.expect("a fitting appended page was constructed");
                let next_root = work::TextManifestRootValue::try_new(
                    key.index_id,
                    key.generation,
                    key.partition.clone(),
                    next_revision,
                    root.page_count(),
                    root.split_count()
                        .checked_add(1)
                        .ok_or_else(|| corruption("Active text split count is exhausted"))?,
                )
                .map_err(|error| {
                    corruption(format!("Active text root append is invalid: {error}"))
                })?;
                (
                    index_keys::TextManifestPageKey {
                        root: root_typed,
                        page: page_number,
                    },
                    page,
                    next_root,
                )
            } else {
                let page_number = root.page_count();
                let page_typed = index_keys::TextManifestPageKey {
                    root: root_typed,
                    page: page_number,
                };
                let page_key = scoped_key(
                    key.scope,
                    index_keys::ScopedKey::TextManifestPage(page_typed),
                );
                let existing = transaction.get(&page_key).await?;
                observations.push(RowObservation {
                    key: page_key,
                    value: existing.clone(),
                });
                if existing.is_some() {
                    return Err(corruption(
                        "Active text next contiguous manifest page is occupied",
                    ));
                }
                let page = work::TextManifestPageValue::try_new(
                    key.index_id,
                    key.generation,
                    key.partition.clone(),
                    page_number,
                    vec![split],
                )
                .expect("one split forms a valid next page");
                let next_root = root
                    .append_page(page_number, NonZeroU32::MIN)
                    .map_err(|error| corruption(format!("Active text root is full: {error}")))?;
                (page_typed, page, next_root)
            }
        }
    };
    let page_value = index_values::encode_manifest_page(&page);
    let page_bytes = u64::try_from(page_value.len()).unwrap_or(u64::MAX);
    if page_bytes > limits.max_manifest_page_bytes().get() {
        return Err(HelixDbError::ActiveTextMutationLimitExceeded {
            resource: crate::error::ActiveTextMutationResource::ManifestPageBytes,
            observed: page_bytes,
            limit: limits.max_manifest_page_bytes().get(),
        });
    }
    Ok((
        next_root,
        Some(PreparedRow {
            key: scoped_key(
                key.scope,
                index_keys::ScopedKey::TextManifestPage(page_typed),
            ),
            value: page_value,
        }),
        page_typed.page,
    ))
}

/// Stages index-owned rows prepared from this transaction's observed snapshot.
pub(crate) fn stage_active_text_epoch(
    transaction: &DbTransaction,
    published: &super::active_publication::PublishedActiveTextEpoch,
) -> Result<()> {
    let prepared = published.prepared();
    for destination in &prepared.destinations {
        debug_assert!(destination.payload.is_none());
    }

    for build in &prepared.build_deltas {
        mutation::stage_prepared_text_build_delta_rows(transaction, build)?;
    }
    prepared
        .statistics
        .stage_transaction_observed(transaction)?;
    for destination in &prepared.destinations {
        for write in &destination.writes {
            transaction.put(&write.key, &write.value)?;
        }
    }
    Ok(())
}

fn scoped_key(scope: DataScope, key: index_keys::ScopedKey) -> Bytes {
    Key::Data { scope, kind: key }.to_bytes()
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::SearchIndexBackfillLimits;

    #[tokio::test]
    async fn destination_snapshot_observation_conflicts_without_a_second_read() {
        let store = Arc::new(InMemory::new());
        let db = Db::builder(
            "active-text-destination-serializable-conflict",
            store.clone(),
        )
        .build()
        .await
        .expect("destination conflict database opens");
        let object_store: Arc<dyn ObjectStore> = store;
        let scope = DataScope::LegacyUnscoped;
        let index_id = index_lifecycle::IndexId::initial();
        let generation = index_lifecycle::IndexGenerationId::initial();
        let partition = work::TextPartition::Unpartitioned;
        let destination_key = DestinationKey {
            scope,
            index_id,
            generation,
            partition: partition.clone(),
        };
        let root_key = scoped_key(
            scope,
            index_keys::ScopedKey::TextManifestRoot(index_keys::TextManifestRootKey {
                index_id,
                generation,
                partition: partition.fingerprint(),
            }),
        );
        let loser_value = index_values::encode_manifest_root(
            &work::TextManifestRootValue::try_new(
                index_id,
                generation,
                partition.clone(),
                index_lifecycle::TextManifestRevision::new(2).unwrap(),
                0,
                0,
            )
            .expect("loser manifest root is valid"),
        );
        let winning_value = index_values::encode_manifest_root(
            &work::TextManifestRootValue::empty(index_id, generation, partition),
        );
        let limits = SearchIndexBackfillLimits::default().active_text_mutation();
        let measured = ActiveTextMutationMeasurements::try_admit(
            limits,
            u64::try_from(root_key.len()).unwrap(),
            1,
            u64::try_from(root_key.len() + loser_value.len()).unwrap(),
            0,
            0,
        )
        .expect("destination work is within policy");

        let loser = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("destination loser opens");
        assert_eq!(
            loser
                .get(&root_key)
                .await
                .expect("destination root observation succeeds"),
            None
        );
        let prepared = PreparedActiveTextEpoch {
            build_deltas: Vec::new(),
            statistics: super::super::statistics::PreparedTextStatisticsBatch::default(),
            destinations: vec![PreparedDestination {
                key: destination_key,
                observations: vec![RowObservation {
                    key: root_key.clone(),
                    value: None,
                }],
                writes: vec![PreparedRow {
                    key: root_key.clone(),
                    value: loser_value,
                }],
                payload: None,
                split: None,
                measurements: measured,
            }],
            measurements: measured,
        };
        let published = super::super::active_publication::publish_active_text_epoch(
            &object_store,
            "active-text-destination-serializable-conflict",
            prepared,
            limits,
        )
        .await
        .expect("empty destination publication succeeds");
        stage_active_text_epoch(&loser, &published)
            .expect("destination stages from its conflict-tracked observation");

        db.put(root_key.clone(), winning_value.clone())
            .await
            .expect("competing destination root commits");
        assert_eq!(
            loser
                .commit()
                .await
                .expect_err("stale destination preparation must conflict")
                .kind(),
            slatedb::ErrorKind::Transaction
        );
        assert_eq!(
            db.get(&root_key).await.expect("winning root reads"),
            Some(winning_value)
        );
        db.close()
            .await
            .expect("destination conflict database closes");
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/index_lifecycle_text_active_batch.rs"]
mod external_contracts;
