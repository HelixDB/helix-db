//! Exact transactional corpus, term, and entity accounting for text generations.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use slatedb::{DbReadOps, DbTransaction};

use crate::config::TextAnalyzerKind;
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::values as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{self, work};

/// Stable-view statistics for one analyzed OR-term query.
#[derive(Debug, Clone)]
pub(crate) enum LoadedTextQueryStatistics {
    /// The analyzer emitted no Tantivy-accepted query terms.
    EmptyQuery,
    /// The partition has no live documents.
    EmptyCorpus,
    /// Complete totals and every requested document frequency.
    Ready(TextBm25Statistics),
}

/// Immutable provider shared by every split and page in one request.
#[derive(Debug, Clone)]
pub(crate) struct TextBm25Statistics {
    total_document_count: u64,
    total_token_count: u64,
    document_frequencies: BTreeMap<Bytes, u64>,
}

#[cfg(feature = "production-coverage")]
impl TextBm25Statistics {
    /// Constructs exact corpus statistics for the production FTS benchmark fixture.
    pub(crate) fn for_benchmark(
        total_document_count: u64,
        total_token_count: u64,
        document_frequencies: BTreeMap<Bytes, u64>,
    ) -> Self {
        assert!(
            document_frequencies
                .values()
                .all(|frequency| *frequency <= total_document_count),
            "benchmark document frequencies cannot exceed the corpus"
        );
        Self {
            total_document_count,
            total_token_count,
            document_frequencies,
        }
    }
}

impl tantivy::query::Bm25StatisticsProvider for TextBm25Statistics {
    fn total_num_tokens(&self, _field: tantivy::schema::Field) -> tantivy::Result<u64> {
        Ok(self.total_token_count)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.total_document_count)
    }

    fn doc_freq(&self, term: &tantivy::Term) -> tantivy::Result<u64> {
        Ok(self
            .document_frequencies
            .get(term.serialized_value_bytes())
            .copied()
            .unwrap_or(0))
    }
}

/// Loads query terms and all matching statistics exactly once from a stable view.
pub(crate) async fn load_query_statistics(
    reader: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    partition: &work::TextPartition,
    analyzer: TextAnalyzerKind,
    query: &str,
) -> Result<LoadedTextQueryStatistics> {
    let analyzed = crate::search::text::analyze_text(analyzer, query);
    if analyzed.unique_terms.is_empty() {
        return Ok(LoadedTextQueryStatistics::EmptyQuery);
    }
    let corpus_key = corpus_key(scope, index_id, generation, partition);
    let Some(corpus_bytes) = reader.get(corpus_key).await? else {
        return Ok(LoadedTextQueryStatistics::EmptyCorpus);
    };
    let corpus = decode_corpus(&corpus_bytes)?;
    validate_corpus_owner(&corpus, index_id, generation, partition)?;
    if corpus.document_count == 0 {
        return Ok(LoadedTextQueryStatistics::EmptyCorpus);
    }
    let mut document_frequencies = BTreeMap::new();
    for term in analyzed.unique_terms {
        let key = term_key(scope, index_id, generation, partition, &term);
        let document_frequency = match reader.get(key).await? {
            Some(value) => {
                let statistics = decode_term(&value)?;
                validate_term_owner(&statistics, index_id, generation, partition, &term)?;
                if statistics.document_frequency > corpus.document_count {
                    return Err(corruption(
                        "text term document frequency exceeds corpus document count",
                    ));
                }
                statistics.document_frequency
            }
            None => 0,
        };
        document_frequencies.insert(term, document_frequency);
    }
    Ok(LoadedTextQueryStatistics::Ready(TextBm25Statistics {
        total_document_count: corpus.document_count,
        total_token_count: corpus.total_token_count,
        document_frequencies,
    }))
}

/// Canonical contribution produced by one validated source document.
pub(crate) fn present_contribution(
    analyzer: TextAnalyzerKind,
    partition: work::TextPartition,
    text: &str,
) -> Result<work::TextStatisticsContribution> {
    let analyzed = crate::search::text::analyze_text(analyzer, text);
    present_contribution_from_analysis(analyzer, partition, &analyzed)
}

/// Constructs a contribution from the exact token stream retained for indexing.
pub(crate) fn present_contribution_from_analysis(
    analyzer: TextAnalyzerKind,
    partition: work::TextPartition,
    analyzed: &crate::search::text::AnalyzedText,
) -> Result<work::TextStatisticsContribution> {
    let mut fingerprint = Sha256::new();
    fingerprint.update(b"helix-text-statistics-contribution-v1");
    fingerprint.update(analyzer.as_str().as_bytes());
    let partition_bytes = partition.canonical_bytes();
    fingerprint.update(
        u64::try_from(partition_bytes.len())
            .expect("bounded partition length fits u64")
            .to_be_bytes(),
    );
    fingerprint.update(&partition_bytes);
    fingerprint.update(analyzed.token_count.to_be_bytes());
    fingerprint.update(
        u64::try_from(analyzed.unique_terms.len())
            .expect("resident term count fits u64")
            .to_be_bytes(),
    );
    for term in &analyzed.unique_terms {
        fingerprint.update(
            u64::try_from(term.len())
                .expect("bounded term length fits u64")
                .to_be_bytes(),
        );
        fingerprint.update(term);
    }
    work::TextStatisticsContribution::try_present(
        partition,
        fingerprint.finalize().into(),
        analyzed.token_count,
        analyzed.unique_terms.clone(),
    )
    .map_err(model_error)
}

/// One exact row observation and its desired replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedStatisticsRow {
    pub(crate) key: Bytes,
    pub(crate) observed: Option<Bytes>,
    pub(crate) replacement: Option<Bytes>,
}

/// Complete transaction-local statistics transition for one entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedTextStatisticsTransition {
    rows: Vec<PreparedStatisticsRow>,
}

/// Composed statistics work for multiple entities in one atomic source batch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PreparedTextStatisticsBatch {
    rows: BTreeMap<Bytes, PreparedStatisticsRow>,
}

struct TextStatisticsTransitionRequest<'a> {
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
    marker_observed: Option<Bytes>,
    accounted: Option<&'a work::TextStatisticsContribution>,
    desired: work::TextStatisticsContribution,
    missing_absent_marker: MissingAbsentMarkerPolicy,
    batch: Option<&'a PreparedTextStatisticsBatch>,
}

/// Closed persistence policy for a missing marker whose desired state is absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissingAbsentMarkerPolicy {
    /// Active has no delayed source scan, so a never-accounted entity stays unrepresented.
    KeepMissing,
    /// BUILD must tombstone a concurrent absence before a delayed source scan reaches it.
    Persist,
}

impl PreparedTextStatisticsBatch {
    /// Observes sorted unique rows once so later entity transitions compose in memory.
    pub(crate) async fn observe_rows(
        &mut self,
        transaction: &DbTransaction,
        keys: impl IntoIterator<Item = Bytes>,
    ) -> Result<()> {
        const ROWS_PER_CHUNK: usize = 512;

        let mut keys = keys.into_iter().collect::<Vec<_>>();
        keys.sort_unstable();
        keys.dedup();
        for chunk in keys.chunks(ROWS_PER_CHUNK) {
            let values = transaction.multi_get(chunk).await?;
            for (key, observed) in chunk.iter().cloned().zip(values) {
                match self.rows.entry(key.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(PreparedStatisticsRow {
                            key,
                            observed: observed.clone(),
                            replacement: observed,
                        });
                    }
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        if entry.get().replacement != observed {
                            return Err(corruption(
                                "text statistics observation disagrees with composed state",
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Adds one transition while preserving the first database observation and
    /// exposing the latest replacement to later transition preparation.
    pub(crate) fn push(&mut self, transition: PreparedTextStatisticsTransition) -> Result<()> {
        for row in transition.rows {
            match self.rows.get_mut(&row.key) {
                Some(composed) => {
                    if composed.replacement != row.observed {
                        return Err(corruption(
                            "text statistics batch contains a discontinuous row transition",
                        ));
                    }
                    composed.replacement = row.replacement;
                }
                None => {
                    self.rows.insert(row.key.clone(), row);
                }
            }
        }
        Ok(())
    }

    fn effective_value(&self, key: &[u8]) -> Option<Option<Bytes>> {
        self.rows.get(key).map(|row| row.replacement.clone())
    }

    /// Revalidates the first observation of every composed row.
    pub(crate) async fn validate(&self, transaction: &DbTransaction) -> Result<()> {
        let keys = self.rows.keys().cloned().collect::<Vec<_>>();
        let observations = if keys.is_empty() {
            Vec::new()
        } else {
            transaction.multi_get(&keys).await?
        };
        for (row, observed) in self.rows.values().zip(observations) {
            if observed != row.observed {
                return Err(corruption(
                    "text statistics batch input changed after transactional preparation",
                ));
            }
        }
        Ok(())
    }

    /// Stages only the final replacement for every composed row.
    pub(crate) fn stage_validated(&self, transaction: &DbTransaction) -> Result<()> {
        self.stage_transaction_observed(transaction)
    }

    /// Stages a batch prepared from this transaction's conflict-tracked reads.
    ///
    /// No second read can strengthen validation inside the same snapshot. The
    /// serializable commit remains the authority for concurrent changes.
    pub(crate) fn stage_transaction_observed(&self, transaction: &DbTransaction) -> Result<()> {
        for row in self.rows.values() {
            if row.replacement == row.observed {
                continue;
            }
            match &row.replacement {
                Some(value) => transaction.put(&row.key, value)?,
                None => transaction.delete(&row.key)?,
            }
        }
        Ok(())
    }

    /// Returns exact unique database reads and final writes for the composed batch.
    pub(crate) fn measurements(&self) -> (u64, u64, u64) {
        self.rows
            .values()
            .fold((0_u64, 0_u64, 0_u64), |(input, operations, output), row| {
                let key_len = u64::try_from(row.key.len()).unwrap_or(u64::MAX);
                (
                    input.saturating_add(
                        key_len.saturating_add(
                            row.observed
                                .as_ref()
                                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                        ),
                    ),
                    operations.saturating_add(u64::from(row.replacement != row.observed)),
                    output.saturating_add(if row.replacement != row.observed {
                        key_len.saturating_add(
                            row.replacement
                                .as_ref()
                                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                        )
                    } else {
                        0
                    }),
                )
            })
    }
}

impl PreparedTextStatisticsTransition {
    /// Exact serialized reads and writes admitted with the surrounding mutation.
    pub(crate) fn measurements(&self) -> (u64, u64, u64) {
        self.rows
            .iter()
            .fold((0_u64, 0_u64, 0_u64), |(input, operations, output), row| {
                let key_len = u64::try_from(row.key.len()).unwrap_or(u64::MAX);
                (
                    input.saturating_add(
                        key_len.saturating_add(
                            row.observed
                                .as_ref()
                                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                        ),
                    ),
                    operations.saturating_add(u64::from(row.replacement != row.observed)),
                    output.saturating_add(if row.replacement != row.observed {
                        key_len.saturating_add(
                            row.replacement
                                .as_ref()
                                .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                        )
                    } else {
                        0
                    }),
                )
            })
    }

    /// Borrows the sorted unique physical row transitions.
    pub(crate) fn rows(&self) -> &[PreparedStatisticsRow] {
        &self.rows
    }
}

/// One Active statistics transition against the latest composed epoch state.
pub(crate) struct ActiveTextStatisticsMutation {
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
    before: work::TextStatisticsContribution,
    after: work::TextStatisticsContribution,
}

impl ActiveTextStatisticsMutation {
    pub(crate) fn new(
        scope: DataScope,
        index_id: index_lifecycle::IndexId,
        generation: index_lifecycle::IndexGenerationId,
        entity: index_keys::IndexEntity,
        before: work::TextStatisticsContribution,
        after: work::TextStatisticsContribution,
    ) -> Self {
        Self {
            scope,
            index_id,
            generation,
            entity,
            before,
            after,
        }
    }
}

/// Prepares one Active transition against the latest composed epoch state.
pub(crate) async fn prepare_active_in_batch(
    transaction: &DbTransaction,
    batch: &PreparedTextStatisticsBatch,
    mutation: ActiveTextStatisticsMutation,
) -> Result<PreparedTextStatisticsTransition> {
    prepare_active_from(transaction, Some(batch), mutation).await
}

async fn prepare_active_from(
    transaction: &DbTransaction,
    batch: Option<&PreparedTextStatisticsBatch>,
    mutation: ActiveTextStatisticsMutation,
) -> Result<PreparedTextStatisticsTransition> {
    let ActiveTextStatisticsMutation {
        scope,
        index_id,
        generation,
        entity,
        before,
        after,
    } = mutation;
    let marker = read_marker(transaction, batch, scope, index_id, generation, entity).await?;
    match (
        &before,
        marker.as_ref().map(|(_, marker)| &marker.contribution),
    ) {
        (work::TextStatisticsContribution::Present { .. }, Some(actual)) if actual == &before => {}
        (work::TextStatisticsContribution::Present { .. }, _) => {
            return Err(corruption(
                "Active text statistics marker is absent or disagrees with indexed graph state",
            ));
        }
        (
            work::TextStatisticsContribution::Absent,
            Some(work::TextStatisticsContribution::Present { .. }),
        ) => {
            return Err(corruption(
                "Active text statistics marker retains a document absent from graph state",
            ));
        }
        (work::TextStatisticsContribution::Absent, None | Some(_)) => {}
    }
    let accounted = marker.as_ref().map(|(_, marker)| &marker.contribution);
    prepare_transition(
        transaction,
        TextStatisticsTransitionRequest {
            scope,
            index_id,
            generation,
            entity,
            marker_observed: marker.as_ref().map(|(bytes, _)| bytes.clone()),
            accounted,
            desired: after,
            missing_absent_marker: MissingAbsentMarkerPolicy::KeepMissing,
            batch,
        },
    )
    .await
}

/// Prepares a BUILD mutation; an absent marker means the source scan never accounted it.
pub(crate) async fn prepare_build_mutation(
    transaction: &DbTransaction,
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
    current: work::TextStatisticsContribution,
) -> Result<PreparedTextStatisticsTransition> {
    prepare_build_mutation_from(
        transaction,
        None,
        scope,
        index_id,
        generation,
        entity,
        current,
    )
    .await
}

/// Prepares one BUILD mutation against the latest composed epoch state.
pub(crate) async fn prepare_build_mutation_in_batch(
    transaction: &DbTransaction,
    batch: &PreparedTextStatisticsBatch,
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
    current: work::TextStatisticsContribution,
) -> Result<PreparedTextStatisticsTransition> {
    prepare_build_mutation_from(
        transaction,
        Some(batch),
        scope,
        index_id,
        generation,
        entity,
        current,
    )
    .await
}

async fn prepare_build_mutation_from(
    transaction: &DbTransaction,
    batch: Option<&PreparedTextStatisticsBatch>,
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
    current: work::TextStatisticsContribution,
) -> Result<PreparedTextStatisticsTransition> {
    let marker = read_marker(transaction, batch, scope, index_id, generation, entity).await?;
    let accounted = marker.as_ref().map(|(_, marker)| &marker.contribution);
    prepare_transition(
        transaction,
        TextStatisticsTransitionRequest {
            scope,
            index_id,
            generation,
            entity,
            marker_observed: marker.as_ref().map(|(bytes, _)| bytes.clone()),
            accounted,
            desired: current,
            missing_absent_marker: MissingAbsentMarkerPolicy::Persist,
            batch,
        },
    )
    .await
}

/// Prepares one source contribution against the latest state of a composed batch.
pub(crate) async fn prepare_source_scan_in_batch(
    transaction: &DbTransaction,
    batch: &PreparedTextStatisticsBatch,
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
    current: work::TextStatisticsContribution,
) -> Result<Option<PreparedTextStatisticsTransition>> {
    if read_marker(
        transaction,
        Some(batch),
        scope,
        index_id,
        generation,
        entity,
    )
    .await?
    .is_some()
    {
        return Ok(None);
    }
    prepare_transition(
        transaction,
        TextStatisticsTransitionRequest {
            scope,
            index_id,
            generation,
            entity,
            marker_observed: None,
            accounted: None,
            desired: current,
            missing_absent_marker: MissingAbsentMarkerPolicy::Persist,
            batch: Some(batch),
        },
    )
    .await
    .map(Some)
}

/// Loads and validates one exact generation-owned entity accounting marker.
pub(crate) async fn load_entity_contribution(
    reader: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
) -> Result<Option<work::TextStatisticsContribution>> {
    let key = scoped_key(
        scope,
        index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
            index_id,
            generation,
            entity,
        }),
    );
    let Some(value) = reader.get(key).await? else {
        return Ok(None);
    };
    let marker = index_values::decode_statistics_entity(&value)?;
    if marker.index_id != index_id
        || marker.generation != generation
        || marker.entity_kind != entity.kind
        || marker.entity_id != entity.id
    {
        return Err(corruption(
            "text statistics entity key/value ownership mismatch",
        ));
    }
    Ok(Some(marker.contribution))
}

/// Revalidates every observed row without buffering writes.
#[cfg(test)]
pub(crate) async fn validate(
    transaction: &DbTransaction,
    prepared: &PreparedTextStatisticsTransition,
) -> Result<()> {
    for row in &prepared.rows {
        if transaction.get(&row.key).await? != row.observed {
            return Err(corruption(
                "text statistics input changed after transactional preparation",
            ));
        }
    }
    Ok(())
}

/// Buffers a transition after its complete request-level validation pass.
#[cfg(test)]
pub(crate) fn stage_validated(
    transaction: &DbTransaction,
    prepared: &PreparedTextStatisticsTransition,
) -> Result<()> {
    for row in &prepared.rows {
        if row.replacement == row.observed {
            continue;
        }
        match &row.replacement {
            Some(value) => transaction.put(&row.key, value)?,
            None => transaction.delete(&row.key)?,
        }
    }
    Ok(())
}

async fn prepare_transition(
    transaction: &DbTransaction,
    request: TextStatisticsTransitionRequest<'_>,
) -> Result<PreparedTextStatisticsTransition> {
    let TextStatisticsTransitionRequest {
        scope,
        index_id,
        generation,
        entity,
        marker_observed,
        accounted,
        desired,
        missing_absent_marker,
        batch,
    } = request;
    let mut partition_deltas = BTreeMap::<work::TextPartition, (i8, i128)>::new();
    let mut term_deltas = BTreeMap::<(work::TextPartition, Bytes), i8>::new();
    if let Some(work::TextStatisticsContribution::Present {
        partition,
        token_count,
        terms,
        ..
    }) = accounted
    {
        add_partition_delta(
            &mut partition_deltas,
            partition.clone(),
            -1,
            -i128::from(*token_count),
        )?;
        for term in terms {
            add_term_delta(&mut term_deltas, partition.clone(), term.clone(), -1)?;
        }
    }
    if let work::TextStatisticsContribution::Present {
        partition,
        token_count,
        terms,
        ..
    } = &desired
    {
        add_partition_delta(
            &mut partition_deltas,
            partition.clone(),
            1,
            i128::from(*token_count),
        )?;
        for term in terms {
            add_term_delta(&mut term_deltas, partition.clone(), term.clone(), 1)?;
        }
    }

    let mut rows = Vec::new();
    let mut resulting_document_counts = BTreeMap::new();
    for (partition, (document_delta, token_delta)) in partition_deltas {
        let key = corpus_key(scope, index_id, generation, &partition);
        let observed = read_value(transaction, batch, &key).await?;
        let (document_count, total_token_count) = match observed.as_deref() {
            Some(value) => {
                let statistics = decode_corpus(value)?;
                validate_corpus_owner(&statistics, index_id, generation, &partition)?;
                (statistics.document_count, statistics.total_token_count)
            }
            None => (0, 0),
        };
        let next_document_count = apply_signed(document_count, i128::from(document_delta))?;
        let next_total_token_count = apply_signed(total_token_count, token_delta)?;
        if next_document_count == 0 && next_total_token_count != 0 {
            return Err(corruption(
                "empty text corpus retains a non-zero token count",
            ));
        }
        let replacement = Some(index_values::encode_corpus_statistics(
            &work::TextCorpusStatisticsValue::try_new(
                index_id,
                generation,
                partition.clone(),
                next_document_count,
                next_total_token_count,
            )
            .map_err(model_error)?,
        ));
        resulting_document_counts.insert(partition, next_document_count);
        rows.push(PreparedStatisticsRow {
            key,
            observed,
            replacement,
        });
    }

    for ((partition, term), delta) in term_deltas {
        let document_count = match resulting_document_counts.get(&partition) {
            Some(count) => *count,
            None => {
                read_document_count(transaction, batch, scope, index_id, generation, &partition)
                    .await?
            }
        };
        let key = term_key(scope, index_id, generation, &partition, &term);
        let observed = read_value(transaction, batch, &key).await?;
        let document_frequency = match observed.as_deref() {
            Some(value) => {
                let statistics = decode_term(value)?;
                validate_term_owner(&statistics, index_id, generation, &partition, &term)?;
                statistics.document_frequency
            }
            None => 0,
        };
        let next_document_frequency = apply_signed(document_frequency, i128::from(delta))?;
        if next_document_frequency > document_count {
            return Err(corruption(
                "text term document frequency exceeds corpus document count",
            ));
        }
        let replacement = if next_document_frequency == 0 {
            None
        } else {
            Some(index_values::encode_term_statistics(
                &work::TextTermStatisticsValue::try_new(
                    index_id,
                    generation,
                    partition,
                    term,
                    next_document_frequency,
                )
                .map_err(model_error)?,
            ))
        };
        rows.push(PreparedStatisticsRow {
            key,
            observed,
            replacement,
        });
    }

    let marker_key = scoped_key(
        scope,
        index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
            index_id,
            generation,
            entity,
        }),
    );
    let marker_replacement = match (&desired, marker_observed.as_ref(), missing_absent_marker) {
        (
            work::TextStatisticsContribution::Absent,
            None,
            MissingAbsentMarkerPolicy::KeepMissing,
        ) => None,
        (_, _, MissingAbsentMarkerPolicy::KeepMissing | MissingAbsentMarkerPolicy::Persist) => {
            Some(index_values::encode_statistics_entity(
                &work::TextStatisticsEntityValue {
                    index_id,
                    generation,
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                    contribution: desired,
                },
            ))
        }
    };
    rows.push(PreparedStatisticsRow {
        key: marker_key,
        observed: marker_observed,
        replacement: marker_replacement,
    });
    rows.sort_by(|left, right| left.key.cmp(&right.key));
    let mut unique = BTreeSet::new();
    if !rows.iter().all(|row| unique.insert(row.key.clone())) {
        return Err(corruption(
            "text statistics transition produced a duplicate row",
        ));
    }
    Ok(PreparedTextStatisticsTransition { rows })
}

fn add_partition_delta(
    deltas: &mut BTreeMap<work::TextPartition, (i8, i128)>,
    partition: work::TextPartition,
    documents: i8,
    tokens: i128,
) -> Result<()> {
    let entry = deltas.entry(partition).or_default();
    entry.0 = entry
        .0
        .checked_add(documents)
        .ok_or_else(|| corruption("text document-count delta overflowed"))?;
    entry.1 = entry
        .1
        .checked_add(tokens)
        .ok_or_else(|| corruption("text token-count delta overflowed"))?;
    Ok(())
}

fn add_term_delta(
    deltas: &mut BTreeMap<(work::TextPartition, Bytes), i8>,
    partition: work::TextPartition,
    term: Bytes,
    delta: i8,
) -> Result<()> {
    let entry = deltas.entry((partition, term)).or_default();
    *entry = entry
        .checked_add(delta)
        .ok_or_else(|| corruption("text document-frequency delta overflowed"))?;
    Ok(())
}

fn apply_signed(current: u64, delta: i128) -> Result<u64> {
    let next = i128::from(current)
        .checked_add(delta)
        .ok_or_else(|| corruption("text statistics arithmetic overflowed"))?;
    u64::try_from(next).map_err(|_| corruption("text statistics arithmetic underflowed"))
}

async fn read_document_count(
    transaction: &DbTransaction,
    batch: Option<&PreparedTextStatisticsBatch>,
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    partition: &work::TextPartition,
) -> Result<u64> {
    let key = corpus_key(scope, index_id, generation, partition);
    let Some(value) = read_value(transaction, batch, &key).await? else {
        return Ok(0);
    };
    let statistics = decode_corpus(&value)?;
    validate_corpus_owner(&statistics, index_id, generation, partition)?;
    Ok(statistics.document_count)
}

async fn read_marker(
    transaction: &DbTransaction,
    batch: Option<&PreparedTextStatisticsBatch>,
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
) -> Result<Option<(Bytes, work::TextStatisticsEntityValue)>> {
    let key = entity_key(scope, index_id, generation, entity);
    let Some(value) = read_value(transaction, batch, &key).await? else {
        return Ok(None);
    };
    let marker = index_values::decode_statistics_entity(&value)?;
    if marker.index_id != index_id
        || marker.generation != generation
        || marker.entity_kind != entity.kind
        || marker.entity_id != entity.id
    {
        return Err(corruption(
            "text statistics entity key/value ownership mismatch",
        ));
    }
    Ok(Some((value, marker)))
}

/// Builds the exact entity-accounting key shared by Active and Building epochs.
pub(super) fn entity_key(
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    entity: index_keys::IndexEntity,
) -> Bytes {
    scoped_key(
        scope,
        index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
            index_id,
            generation,
            entity,
        }),
    )
}

async fn read_value(
    transaction: &DbTransaction,
    batch: Option<&PreparedTextStatisticsBatch>,
    key: &[u8],
) -> Result<Option<Bytes>> {
    if let Some(value) = batch.and_then(|batch| batch.effective_value(key)) {
        return Ok(value);
    }
    Ok(transaction.get(key).await?)
}

/// Builds the exact corpus-statistics key for one generation partition.
pub(super) fn corpus_key(
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    partition: &work::TextPartition,
) -> Bytes {
    scoped_key(
        scope,
        index_keys::ScopedKey::TextCorpusStatistics(index_keys::TextCorpusStatisticsKey {
            index_id,
            generation,
            partition: partition.fingerprint(),
        }),
    )
}

fn term_key(
    scope: DataScope,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    partition: &work::TextPartition,
    term: &[u8],
) -> Bytes {
    scoped_key(
        scope,
        index_keys::ScopedKey::TextTermStatistics(index_keys::TextTermStatisticsKey {
            corpus: index_keys::TextCorpusStatisticsKey {
                index_id,
                generation,
                partition: partition.fingerprint(),
            },
            term: index_keys::TextTermFingerprint::new(Sha256::digest(term).into()),
        }),
    )
}

fn scoped_key(scope: DataScope, key: index_keys::ScopedKey) -> Bytes {
    ManagedIndexKey::Data { scope, kind: key }.to_bytes()
}

/// Decodes one corpus-statistics value without accepting another work kind.
pub(super) fn decode_corpus(value: &[u8]) -> Result<work::TextCorpusStatisticsValue> {
    let statistics = index_values::decode_corpus_statistics(value)?;
    Ok(statistics)
}

fn decode_term(value: &[u8]) -> Result<work::TextTermStatisticsValue> {
    let statistics = index_values::decode_term_statistics(value)?;
    Ok(statistics)
}

/// Cross-checks corpus-statistics value ownership against its typed key.
pub(super) fn validate_corpus_owner(
    statistics: &work::TextCorpusStatisticsValue,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    partition: &work::TextPartition,
) -> Result<()> {
    if statistics.index_id != index_id
        || statistics.generation != generation
        || statistics.partition != *partition
    {
        return Err(corruption(
            "text corpus-statistics key/value ownership mismatch",
        ));
    }
    Ok(())
}

/// Validates the exact relationship between one manifest root and its corpus.
///
/// An empty root may have no row or one exact owned zero/zero row. A non-empty
/// root must have one exact owned row, including zero/zero when every immutable
/// split is stale and the live corpus is empty.
pub(super) fn validate_manifest_corpus(
    value: Option<&[u8]>,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    partition: &work::TextPartition,
    root_split_count: u64,
) -> Result<()> {
    let corpus = match value {
        Some(value) => {
            let corpus = decode_corpus(value)?;
            validate_corpus_owner(&corpus, index_id, generation, partition)?;
            Some(corpus)
        }
        None => None,
    };
    match (root_split_count, corpus) {
        (0, None) => Ok(()),
        (0, Some(corpus)) if corpus.document_count == 0 && corpus.total_token_count == 0 => Ok(()),
        (0, Some(_)) => Err(corruption(
            "empty Active text manifest retains non-empty corpus statistics",
        )),
        (_, None) => Err(corruption(
            "non-empty Active text manifest has no corpus statistics",
        )),
        (_, Some(_)) => Ok(()),
    }
}

fn validate_term_owner(
    statistics: &work::TextTermStatisticsValue,
    index_id: index_lifecycle::IndexId,
    generation: index_lifecycle::IndexGenerationId,
    partition: &work::TextPartition,
    term: &[u8],
) -> Result<()> {
    if statistics.index_id != index_id
        || statistics.generation != generation
        || statistics.partition != *partition
        || statistics.term.as_ref() != term
        || Sha256::digest(&statistics.term).as_slice() != Sha256::digest(term).as_slice()
    {
        return Err(corruption(
            "text term-statistics key/value ownership or hash mismatch",
        ));
    }
    Ok(())
}

fn model_error(error: work::IndexWorkModelError) -> HelixDbError {
    HelixDbError::InvariantViolation(format!("invalid text statistics model: {error}"))
}

fn corruption(reason: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(reason.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn corpus_value(
        index_id: index_lifecycle::IndexId,
        generation: index_lifecycle::IndexGenerationId,
        partition: &work::TextPartition,
        document_count: u64,
        total_token_count: u64,
    ) -> Bytes {
        index_values::encode_corpus_statistics(
            &work::TextCorpusStatisticsValue::try_new(
                index_id,
                generation,
                partition.clone(),
                document_count,
                total_token_count,
            )
            .expect("test corpus totals are valid"),
        )
    }

    #[test]
    fn manifest_corpus_accepts_only_canonical_root_relationships() {
        let index_id = index_lifecycle::IndexId::initial();
        let generation = index_lifecycle::IndexGenerationId::initial();
        let partition = work::TextPartition::Unpartitioned;
        let empty = corpus_value(index_id, generation, &partition, 0, 0);
        let populated = corpus_value(index_id, generation, &partition, 1, 1);

        assert!(validate_manifest_corpus(None, index_id, generation, &partition, 0).is_ok());
        assert!(
            validate_manifest_corpus(Some(&empty), index_id, generation, &partition, 0).is_ok()
        );
        assert!(
            validate_manifest_corpus(Some(&empty), index_id, generation, &partition, 1).is_ok()
        );
        assert!(
            validate_manifest_corpus(Some(&populated), index_id, generation, &partition, 1).is_ok()
        );
        assert!(matches!(
            validate_manifest_corpus(Some(&populated), index_id, generation, &partition, 0),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "empty Active text manifest retains non-empty corpus statistics"
        ));
        assert!(matches!(
            validate_manifest_corpus(None, index_id, generation, &partition, 1),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "non-empty Active text manifest has no corpus statistics"
        ));

        let cross_owned = corpus_value(
            index_lifecycle::IndexId::new(2).expect("cross-owned index ID is valid"),
            generation,
            &partition,
            0,
            0,
        );
        assert!(matches!(
            validate_manifest_corpus(
                Some(&cross_owned),
                index_id,
                generation,
                &partition,
                0
            ),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "text corpus-statistics key/value ownership mismatch"
        ));
    }
}

#[cfg(test)]
#[path = "../../../tests/unit/index_lifecycle_text_statistics_contracts.rs"]
mod external_contracts;
