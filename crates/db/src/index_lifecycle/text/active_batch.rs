//! Transaction-epoch batching for Active V3 text mutations.
//!
//! Graph writes remain owned by the ordinary mutation path. This module reads
//! their coalesced final state, composes BUILD/statistics effects, and prepares
//! at most one optional immutable split for each generation partition.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use bytes::Bytes;
use futures::{stream, StreamExt, TryStreamExt};
use slatedb::DbTransaction;
use tokio::sync::Semaphore;

use crate::config::ActiveTextMutationLimits;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::values as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{self, work};

use super::active_preflight::{ActiveTextMutationMeasurements, ActiveTextMutationUsage};

const TANTIVY_FOREGROUND_WRITER_BYTES: u64 = 15_000_000;

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

/// Prepares every destination and admits the complete epoch's measurements,
/// counting the acknowledgement staged beside them as output.
async fn finish_epoch(
    transaction: &DbTransaction,
    statistics: super::statistics::PreparedTextStatisticsBatch,
    destinations: BTreeMap<DestinationKey, DestinationWork>,
    entity_count: u64,
    graph_input_bytes: u64,
    acknowledgement: index_lifecycle::queue::storage::AcknowledgementOutput,
    limits: ActiveTextMutationLimits,
) -> Result<PreparedActiveTextEpoch> {
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
                .saturating_add(statistics_measurements.0)
                .saturating_add(destination_measurements.0),
            output_operations: statistics_measurements
                .1
                .saturating_add(destination_measurements.1)
                .saturating_add(acknowledgement.operations),
            output_bytes: statistics_measurements
                .2
                .saturating_add(destination_measurements.2)
                .saturating_add(acknowledgement.bytes),
            split_bytes: destination_measurements.3,
            retained_split_bytes: destination_measurements.4,
            manifest_page_bytes: destination_measurements.5,
        },
    )?;
    Ok(PreparedActiveTextEpoch {
        statistics,
        destinations: prepared_destinations,
        measurements,
    })
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
    before: Option<work::TextPartition>,
    after: Option<AnalyzedActiveTextDocument>,
) -> Result<()> {
    match (before, after) {
        (None, None) => Ok(()),
        (None, Some(current)) => {
            insert_live(destinations, handle, definition, entity, current, false)
        }
        (Some(previous), None) => {
            insert_retirement(destinations, handle, definition, entity, previous)
        }
        (Some(previous), Some(current)) if previous == current.partition => {
            insert_live(destinations, handle, definition, entity, current, true)
        }
        (Some(previous), Some(current)) => {
            insert_retirement(destinations, handle, definition, entity, previous)?;
            insert_live(destinations, handle, definition, entity, current, false)
        }
    }
}

/// One entity's collapsed queued text effect: its final replacement, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueuedTextEffect {
    pub(crate) entity: index_keys::IndexEntity,
    pub(crate) replacement: Option<(work::TextPartition, String)>,
}

/// Prepares one bounded publication epoch from queued text payloads.
///
/// The previous indexed representation comes from the physical statistics
/// marker read through the publication transaction, never from graph rows.
/// Every effect replaces that representation with the queued final state,
/// retiring the old partition's document when the partition changed or the
/// document was deleted. Only an effect that deletes a never-indexed entity
/// produces no work: a statistics contribution records the partition, token
/// count, and unique terms but not term frequencies or positions, so an equal
/// contribution does not prove the indexed document is unchanged. The epoch's
/// output admission includes `acknowledgement`, the queue writes the same
/// transaction stages.
pub(crate) async fn prepare_queued_text_epoch(
    transaction: &DbTransaction,
    handle: &index_lifecycle::ActiveIndexHandle,
    effects: Vec<QueuedTextEffect>,
    limits: ActiveTextMutationLimits,
    acknowledgement: index_lifecycle::queue::storage::AcknowledgementOutput,
) -> Result<PreparedActiveTextEpoch> {
    let Some(definition) = handle.text_definition() else {
        return Err(corruption(
            "queued text publication received another family handle",
        ));
    };
    let entity_count = u64::try_from(effects.len()).unwrap_or(u64::MAX);
    let mut statistics = super::statistics::PreparedTextStatisticsBatch::default();
    let mut destinations = BTreeMap::<DestinationKey, DestinationWork>::new();
    let mut analysis_budget =
        crate::search::text::TextAnalysisMemoryBudget::new(limits.max_input_bytes());
    let mut input_bytes = 0_u64;
    for effect in effects {
        if effect.entity.kind != definition.element_kind() {
            return Err(corruption(
                "queued text operation names another element kind",
            ));
        }
        let before = super::statistics::accounted_contribution(
            transaction,
            &statistics,
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            effect.entity,
        )
        .await?;
        input_bytes = input_bytes.saturating_add(
            effect
                .replacement
                .as_ref()
                .map_or(0, |(_, text)| u64::try_from(text.len()).unwrap_or(u64::MAX)),
        );
        let after = effect
            .replacement
            .map(|(partition, text)| {
                analyze_document(
                    definition,
                    ActiveTextDocument { partition, text },
                    &mut analysis_budget,
                )
            })
            .transpose()?;
        if before == work::TextStatisticsContribution::Absent && after.is_none() {
            continue;
        }
        let after_contribution = contribution(definition, after.as_ref())?;
        let before_partition = match &before {
            work::TextStatisticsContribution::Present { partition, .. } => Some(partition.clone()),
            work::TextStatisticsContribution::Absent => None,
        };
        let transition = super::statistics::prepare_mutation_in_batch(
            transaction,
            &statistics,
            super::statistics::TextStatisticsMutation::new(
                handle.scope(),
                handle.index_id(),
                handle.generation(),
                effect.entity,
                before,
                after_contribution,
            ),
        )
        .await?;
        statistics.push(transition)?;
        group_effect(
            &mut destinations,
            handle,
            definition,
            effect.entity,
            before_partition,
            after,
        )?;
    }
    finish_epoch(
        transaction,
        statistics,
        destinations,
        entity_count,
        input_bytes,
        acknowledgement,
        limits,
    )
    .await
}

/// Rejects a queued final document that one publication could not replace.
///
/// Queue producers call this before commit, while the write can still fail
/// with a typed [`HelixDbError::ActiveTextMutationLimitExceeded`]. An ASCII
/// document whose provable bound
/// ([`crate::search::text::TextAnalysisTotals::ascii_bound`]) fits is admitted
/// without analysis. The bound is never below the exact totals, so it only
/// accepts documents [`admit_document`] accepts and every decision is exact.
/// No storage is read.
pub(crate) fn admit_queued_document(
    scope: DataScope,
    record: &index_lifecycle::IndexRecordV2,
    entity: index_keys::IndexEntity,
    partition: &work::TextPartition,
    text: &str,
    limits: ActiveTextMutationLimits,
) -> Result<()> {
    let index_lifecycle::ValidatedDynamicIndexDefinition::Text(definition) = record.definition()
    else {
        return Err(corruption("queued text admission received another family"));
    };
    let bounded = crate::search::text::TextAnalysisTotals::ascii_bound(text)
        .map(|bound| TextDocumentFootprint::measure(scope, record, entity, partition, bound))
        .transpose()?
        .is_some_and(|footprint| footprint.first_exceeded(limits).is_none());
    if bounded {
        return Ok(());
    }
    admit_document(scope, record, definition, entity, partition, text, limits).map(drop)
}

/// Analyzes one document of `record`'s generation within the publisher's
/// analysis budget and admits its [`TextDocumentFootprint`].
///
/// The analysis charge is exactly the one [`prepare_queued_text_epoch`] makes
/// for the document when it publishes alone, and analysis stops at the first
/// token over it. Builds use the returned statistics as the document's
/// contribution.
pub(crate) fn admit_document(
    scope: DataScope,
    record: &index_lifecycle::IndexRecordV2,
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    entity: index_keys::IndexEntity,
    partition: &work::TextPartition,
    text: &str,
    limits: ActiveTextMutationLimits,
) -> Result<crate::search::text::AnalyzedText> {
    let (analyzed, totals) = crate::search::text::analyze_text_within_budget(
        definition.analyzer(),
        text,
        &mut crate::search::text::TextAnalysisMemoryBudget::new(limits.max_input_bytes()),
    )?;
    TextDocumentFootprint::measure(scope, record, entity, partition, totals)?.admit(limits)?;
    Ok(analyzed)
}

/// Largest output allowance of one document.
///
/// Each term adds a row and a marker entry, each holding at least the term's
/// bytes and a 4-byte length, so a document's statistics rows are at least
/// twice its marker's term list. Within twice one length-delimited field, that
/// list fits the one field the stored marker encodes it in.
const MAX_DOCUMENT_OUTPUT_BYTES: u64 = 2 * work::MAX_LENGTH_DELIMITED_FIELD as u64;

/// Upper bound of one indexed document's share of a single-entity publication.
///
/// [`prepare_queued_text_epoch`] replaces an entity's accounted document (its
/// statistics marker, written by a build or an earlier publication) with its
/// final queued document, collapsing every document queued in between. Either
/// side can be any document the entity has held, so neither is known when the
/// other is admitted. Builds and queue producers therefore admit every indexed
/// document alone against half of each per-entity row allowance
/// ([`Self::admit`]): any accounted/final pair then fits one publication, and
/// a publisher trimmed to one entity always makes progress. A build's
/// partition scan replaces the accounted document of an entity changed during
/// the build the same way, so its first entity always makes progress too.
/// Publication analyzes only the final document, so its analysis charge gets
/// the whole analysis budget.
///
/// Each count matches the stored encoders for the rows publication touches
/// for the document in either role: its term rows, corpus row, and entity
/// marker, plus its partition's manifest root, entity state, compaction
/// pointer, and one manifest-page operation. Input also counts the document
/// text, the canonical index record, a second corpus read, and a second page
/// key. Page values are bounded by the manifest-page limit instead: a
/// publication writes one page and reads at most one existing page per
/// destination partition.
///
/// Admission does not build the document's split. It bounds the split that
/// publishes the document alone by its analysis charge
/// ([`crate::search::text::single_document_split_bytes`]) and admits that
/// bound within the split and retained-split ceilings, so publishing any
/// admitted document alone fits every ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TextDocumentFootprint {
    analysis_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
    input_bytes: u64,
    single_split_page_bytes: u64,
}

impl TextDocumentFootprint {
    /// Footprint of the smallest indexable one-term document: the text `a`,
    /// unpartitioned, in the legacy scope of a node index whose label and
    /// property are one character long.
    ///
    /// Every valid [`crate::config::SearchIndexBackfillLimits`] admits it.
    /// Unit tests pin it to [`Self::measure`].
    pub(crate) const SMALLEST: Self = Self {
        analysis_bytes: 1 + crate::search::text::indexed_token_charge(1),
        output_operations: 7,
        output_bytes: 636,
        input_bytes: 768,
        single_split_page_bytes: 124,
    };

    /// Returns the analysis bytes this document is charged.
    #[cfg(test)]
    pub(crate) const fn analysis_bytes(self) -> u64 {
        self.analysis_bytes
    }

    /// Returns the output operations this document is charged.
    #[cfg(test)]
    pub(crate) const fn output_operations(self) -> u64 {
        self.output_operations
    }

    /// Returns the output bytes this document is charged.
    #[cfg(test)]
    pub(crate) const fn output_bytes(self) -> u64 {
        self.output_bytes
    }

    /// Returns the input bytes this document is charged.
    #[cfg(test)]
    pub(crate) const fn input_bytes(self) -> u64 {
        self.input_bytes
    }

    /// Returns the encoded bytes of a one-split page of this document's partition.
    #[cfg(test)]
    pub(crate) const fn single_split_page_bytes(self) -> u64 {
        self.single_split_page_bytes
    }

    /// Measures one document of `record`'s generation in `partition` from its
    /// analysis totals.
    pub(crate) fn measure(
        scope: DataScope,
        record: &index_lifecycle::IndexRecordV2,
        entity: index_keys::IndexEntity,
        partition: &work::TextPartition,
        totals: crate::search::text::TextAnalysisTotals,
    ) -> Result<Self> {
        let len = |bytes: &[u8]| u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let index_id = record.index_id();
        let generation = record.state().generation();
        let statistics_bytes = super::statistics::contribution_row_bytes(
            scope,
            index_id,
            generation,
            entity,
            partition,
            totals.unique_terms,
            totals.unique_term_bytes,
        )?;
        let corpus = super::statistics::corpus_row_bytes(scope, index_id, generation, partition)?;
        let root_typed = index_keys::TextManifestRootKey {
            index_id,
            generation,
            partition: partition.fingerprint(),
        };
        let root = len(&scoped_key(
            scope,
            index_keys::ScopedKey::TextManifestRoot(root_typed),
        ))
        .saturating_add(len(&index_values::encode_manifest_root(
            &work::TextManifestRootValue::empty(index_id, generation, partition.clone()),
        )));
        let state = len(&scoped_key(
            scope,
            index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                root: root_typed,
                entity,
            }),
        ))
        .saturating_add(len(&encode_state(
            &DestinationKey {
                scope,
                index_id,
                generation,
                partition: partition.clone(),
            },
            entity,
            index_lifecycle::TextLogicalVersion::initial(),
            true,
        )));
        let pointer_target = index_keys::TextCompactionTarget::try_new(
            scope,
            record.identity().clone(),
            index_id,
            generation,
            partition.fingerprint(),
            0,
        )?;
        let pointer = len(&ManagedIndexKey::Global {
            kind: index_keys::GlobalKey::TextCompactionPointer(pointer_target),
        }
        .to_bytes())
        .saturating_add(len(&index_values::encode_metadata_value(
            &index_lifecycle::IndexV2MetadataValue::TextCompactionPointer(
                index_lifecycle::TextCompactionPointerValue {
                    revision: index_lifecycle::TextManifestRevision::initial(),
                },
            ),
        )));
        let page_key = len(&scoped_key(
            scope,
            index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
                root: root_typed,
                page: 0,
            }),
        ));
        let split = work::SplitRef::try_new(
            work::BlobRef::new([0; 32], 1),
            0,
            0,
            0,
            1,
            work::SplitPruning::from_terms(std::iter::empty::<&[u8]>()),
        )
        .map_err(|error| corruption(format!("footprint split reference is invalid: {error}")))?;
        let single_split_page = work::TextManifestPageValue::try_new(
            index_id,
            generation,
            partition.clone(),
            0,
            vec![split],
        )
        .map_err(|error| corruption(format!("footprint manifest page is invalid: {error}")))?;
        let record_row = len(&ManagedIndexKey::Data {
            scope,
            kind: index_keys::ScopedKey::index_record(record.identity().clone()),
        }
        .to_bytes())
        .saturating_add(len(&index_values::encode_index_record(record)));
        Ok(Self {
            analysis_bytes: totals.analysis_bytes,
            // Term rows, marker, corpus, root, page, state, and pointer.
            output_operations: totals.unique_terms.saturating_add(6),
            output_bytes: [statistics_bytes, corpus, root, state, pointer, page_key]
                .into_iter()
                .fold(0, u64::saturating_add),
            input_bytes: [
                totals.text_bytes,
                statistics_bytes,
                corpus,
                corpus,
                record_row,
                root,
                state,
                page_key,
                page_key,
            ]
            .into_iter()
            .fold(0, u64::saturating_add),
            single_split_page_bytes: len(&index_values::encode_manifest_page(&single_split_page)),
        })
    }

    /// Returns the first allowance this document exceeds under `limits`, with
    /// its usage and that allowance.
    ///
    /// The analysis budget, a document's page value, and its lone split are
    /// whole-publication ceilings; every other allowance is half of what one
    /// publication may spend on the entity once its page values are reserved.
    /// Output is also capped at [`MAX_DOCUMENT_OUTPUT_BYTES`] so the marker
    /// stays encodable. The split bound must fit both the split ceiling and
    /// the retained-split budget, which is the analysis budget, so a document
    /// may use the analysis budget only up to the split's fixed layout.
    pub(crate) fn first_exceeded(
        self,
        limits: ActiveTextMutationLimits,
    ) -> Option<(crate::error::ActiveTextMutationResource, u64, u64)> {
        let page = limits.max_manifest_page_bytes().get();
        let split = crate::search::text::single_document_split_bytes(self.analysis_bytes);
        [
            (
                crate::error::ActiveTextMutationResource::AnalysisBytes,
                self.analysis_bytes,
                limits.max_input_bytes().get(),
            ),
            (
                crate::error::ActiveTextMutationResource::ManifestPageBytes,
                self.single_split_page_bytes,
                page,
            ),
            (
                crate::error::ActiveTextMutationResource::OutputOperations,
                self.output_operations,
                limits.max_output_operations().get() / 2,
            ),
            (
                crate::error::ActiveTextMutationResource::OutputBytes,
                self.output_bytes,
                (limits.max_output_bytes().get().saturating_sub(page) / 2)
                    .min(MAX_DOCUMENT_OUTPUT_BYTES),
            ),
            (
                crate::error::ActiveTextMutationResource::InputBytes,
                self.input_bytes,
                limits
                    .max_input_bytes()
                    .get()
                    .saturating_sub(page.saturating_mul(2))
                    / 2,
            ),
            (
                crate::error::ActiveTextMutationResource::SplitBytes,
                split,
                limits.max_split_bytes().get(),
            ),
            (
                crate::error::ActiveTextMutationResource::RetainedSplitBytes,
                split,
                limits.max_input_bytes().get(),
            ),
        ]
        .into_iter()
        .find(|(_, observed, allowance)| observed > allowance)
    }

    /// Admits a document whose publication beside any other admitted
    /// document fits `limits`, or reports the first exceeded allowance.
    pub(crate) fn admit(self, limits: ActiveTextMutationLimits) -> Result<()> {
        let Some((resource, observed, limit)) = self.first_exceeded(limits) else {
            return Ok(());
        };
        Err(HelixDbError::ActiveTextMutationLimitExceeded {
            resource,
            observed,
            limit,
        })
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

    let mut state_writes = Vec::with_capacity(live.len() + retirements.len());
    for (entity, document) in &live {
        let state_key = scoped_key(
            key.scope,
            index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                root: root_typed,
                entity: *entity,
            }),
        );
        let state_bytes = transaction.get(&state_key).await?;
        observations.push(RowObservation {
            key: state_key.clone(),
            value: state_bytes.clone(),
        });
        validate_existing_state(
            state_bytes.as_deref(),
            &key,
            *entity,
            root.revision().get(),
            document.requires_existing_live_state,
        )?;
        state_writes.push(PreparedRow {
            key: state_key,
            value: encode_state(&key, *entity, logical_version, true),
        });
    }
    for entity in &retirements {
        let state_key = scoped_key(
            key.scope,
            index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                root: root_typed,
                entity: *entity,
            }),
        );
        let state_bytes = transaction.get(&state_key).await?;
        observations.push(RowObservation {
            key: state_key.clone(),
            value: state_bytes.clone(),
        });
        validate_existing_state(
            state_bytes.as_deref(),
            &key,
            *entity,
            root.revision().get(),
            true,
        )?;
        state_writes.push(PreparedRow {
            key: state_key,
            value: encode_state(&key, *entity, logical_version, false),
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
    let pointer_key = ManagedIndexKey::Global {
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
            ManagedIndexKey::parse_from_slice(key.scope, &write.key)
                .ok()
                .and_then(|parsed| match parsed {
                    ManagedIndexKey::Data {
                        kind: index_keys::ScopedKey::TextManifestPage(_),
                        ..
                    } => Some(u64::try_from(write.value.len()).unwrap_or(u64::MAX)),
                    ManagedIndexKey::Global { .. } | ManagedIndexKey::Data { .. } => None,
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
    ManagedIndexKey::Data { scope, kind: key }.to_bytes()
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
