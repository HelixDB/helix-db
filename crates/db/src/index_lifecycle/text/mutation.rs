//! Transactional V2 text-index mutation routing.
//!
//! A graph transaction loads canonical text generations in its serializable
//! snapshot. Hidden `Building` generations prepare one coalesced entity marker
//! whenever a label, indexed property, or tenant partition input changes. The
//! marker intentionally stores no document payload: catch-up re-reads the
//! authoritative graph row. The same transaction-loaded set retains every
//! canonical Active text handle so request-level code derives append,
//! retirement, and move effects from one complete catalog snapshot. The
//! request orchestrator is the only staging path: callers prepare, admit,
//! validate, and stage the complete request through [`super::active_request`].

use std::collections::HashSet;

use bytes::Bytes;
use slatedb::DbTransaction;

use crate::encoding::property::Property;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
#[cfg(any(test, feature = "index-lifecycle-testing"))]
use crate::encoding::v2::keys::RecordKind;
use crate::encoding::v2::keys::{IndexEntity, IndexEntityStateKey, ScopedKey};
#[cfg(any(test, feature = "index-lifecycle-testing"))]
use crate::encoding::v2::values::decode_index_record;
use crate::encoding::v2::values::encode_build_delta;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::work::{CoalescedBuildDeltaState, CoalescedBuildDeltaValue};
#[cfg(any(test, feature = "index-lifecycle-testing"))]
use crate::index_lifecycle::IndexStateV2;
use crate::index_lifecycle::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, ValidatedDynamicIndexDefinition,
    ValidatedTextIndexDefinition,
};

/// One hidden generation and the definition used to classify entity changes.
#[derive(Debug, Clone)]
struct TextMutationTarget {
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: ValidatedTextIndexDefinition,
}

/// One exact hidden-build delta row retained before request admission.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedTextBuildDelta {
    key: Bytes,
    observed: Option<Bytes>,
    value: Bytes,
    statistics: super::statistics::PreparedTextStatisticsTransition,
}

/// Exact serialized measurements for every hidden-build delta in one request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct TextBuildDeltaMeasurements {
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
}

impl TextBuildDeltaMeasurements {
    /// Composes exact row-only measurements across one epoch.
    pub(super) const fn from_parts(
        input_bytes: u64,
        output_operations: u64,
        output_bytes: u64,
    ) -> Self {
        Self {
            input_bytes,
            output_operations,
            output_bytes,
        }
    }

    /// Returns exact unique bytes read during conflict-tracked preparation.
    pub(super) const fn input_bytes(self) -> u64 {
        self.input_bytes
    }

    /// Returns the exact number of coalesced delta writes.
    pub(super) const fn output_operations(self) -> u64 {
        self.output_operations
    }

    /// Returns complete serialized key/value bytes for those writes.
    pub(super) const fn output_bytes(self) -> u64 {
        self.output_bytes
    }
}

/// Prepared hidden-build effects for one authoritative entity transition.
///
/// Private rows ensure downstream code cannot substitute unmeasured deltas or
/// stage them before request-level admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedTextBuildDeltas {
    rows: Vec<PreparedTextBuildDelta>,
    measurements: TextBuildDeltaMeasurements,
}

impl PreparedTextBuildDeltas {
    /// Returns exact work contributed to the enclosing request preflight.
    #[cfg(test)]
    pub(super) const fn measurements(&self) -> TextBuildDeltaMeasurements {
        self.measurements
    }

    /// Returns only unique BUILD-delta row work; epoch statistics are composed separately.
    pub(super) fn row_measurements(&self) -> TextBuildDeltaMeasurements {
        self.rows
            .iter()
            .fold(TextBuildDeltaMeasurements::default(), |measured, row| {
                let observed_bytes = u64::try_from(row.key.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(
                        row.observed
                            .as_ref()
                            .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                    );
                TextBuildDeltaMeasurements {
                    input_bytes: measured.input_bytes.saturating_add(observed_bytes),
                    output_operations: measured.output_operations.saturating_add(1),
                    output_bytes: measured
                        .output_bytes
                        .saturating_add(u64::try_from(row.key.len()).unwrap_or(u64::MAX))
                        .saturating_add(u64::try_from(row.value.len()).unwrap_or(u64::MAX)),
                }
            })
    }
}

/// Revalidated hidden-build rows ready for infallible staging.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) struct ValidatedTextBuildDeltas {
    rows: Vec<PreparedTextBuildDelta>,
}

/// Transaction-local text generations that accept ordinary mutation work.
#[derive(Debug, Clone, Default)]
pub(crate) struct TextMutationSet {
    targets: Vec<TextMutationTarget>,
    active_handles: Vec<crate::index_lifecycle::ActiveIndexHandle>,
}

/// Family-local target ordinal produced by the canonical catalog classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::index_lifecycle) enum TextMutationTargetOrdinal {
    /// Hidden build target ordinal.
    Building(usize),
    /// Active generation handle ordinal.
    Active(usize),
}

/// Complete authoritative property transition for one graph entity.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TextEntityMutation<'a> {
    entity_kind: IndexElementKind,
    entity_id: IndexEntityId,
    before: &'a [Property],
    after: &'a [Property],
}

impl<'a> TextEntityMutation<'a> {
    /// Binds one entity to its complete before/after property snapshots.
    pub(crate) const fn new(
        entity_kind: IndexElementKind,
        entity_id: u64,
        before: &'a [Property],
        after: &'a [Property],
    ) -> Self {
        Self {
            entity_kind,
            entity_id: IndexEntityId::new(entity_id),
            before,
            after,
        }
    }
}

impl TextMutationSet {
    /// Constructs an empty set for focused configured-index tests.
    #[cfg(test)]
    pub(crate) const fn empty() -> Self {
        Self {
            targets: Vec::new(),
            active_handles: Vec::new(),
        }
    }

    /// Constructs one hidden-build target for request-composition tests.
    #[cfg(test)]
    pub(super) fn one_build_target(
        index_id: IndexId,
        generation: IndexGenerationId,
        definition: ValidatedTextIndexDefinition,
    ) -> Self {
        Self {
            targets: vec![TextMutationTarget {
                index_id,
                generation,
                definition,
            }],
            active_handles: Vec::new(),
        }
    }

    /// Returns every canonical Active text handle loaded in this transaction.
    pub(super) fn active_handles(&self) -> &[crate::index_lifecycle::ActiveIndexHandle] {
        &self.active_handles
    }

    /// Returns whether any routed text definition can observe this transition.
    ///
    /// Only definite label/property absence is skipped here. Present candidate
    /// documents remain routed so complete projection preserves the existing
    /// fail-closed validation behavior for malformed indexed values or tenants.
    pub(crate) fn routed_transition_relevant(
        &self,
        routes: &crate::index_lifecycle::mutation_catalog::RoutedMutationTargets<'_>,
        transition: &crate::index_lifecycle::graph_mutation::GraphMutationTransition,
    ) -> Result<bool> {
        let before = transition.before().map_or(
            &[][..],
            crate::index_lifecycle::graph_mutation::CanonicalPropertyRow::properties,
        );
        let after = transition.after().map_or(
            &[][..],
            crate::index_lifecycle::graph_mutation::CanonicalPropertyRow::properties,
        );
        for route in routes.iter() {
            let definition = match route {
                crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextBuilding(
                    ordinal,
                ) => {
                    let Some(target) = self.targets.get(ordinal) else {
                        return Err(corruption(
                            "text mutation route named a build target outside its catalog",
                        ));
                    };
                    &target.definition
                }
                crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextActive(
                    ordinal,
                ) => {
                    let Some(handle) = self.active_handles.get(ordinal) else {
                        return Err(corruption(
                            "text mutation route named an Active target outside its catalog",
                        ));
                    };
                    let Some(definition) = handle.text_definition() else {
                        return Err(corruption(
                            "text mutation route named a non-text Active target",
                        ));
                    };
                    definition
                }
                crate::index_lifecycle::mutation_catalog::MutationRouteTarget::Secondary(_)
                | crate::index_lifecycle::mutation_catalog::MutationRouteTarget::Vector(_) => {
                    continue;
                }
            };
            let before_is_candidate = matches!(
                super::projection::source_candidate(definition, before),
                super::projection::TextSourceCandidate::Candidate(_)
            );
            let after_is_candidate = matches!(
                super::projection::source_candidate(definition, after),
                super::projection::TextSourceCandidate::Candidate(_)
            );
            if before_is_candidate || after_is_candidate {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns whether request-level Active outcome authority must be retained.
    #[cfg(test)]
    pub(crate) const fn has_active_handles(&self) -> bool {
        !self.active_handles.is_empty()
    }

    /// Counts classified records for the one-scan catalog contract.
    #[cfg(test)]
    pub(in crate::index_lifecycle) const fn catalog_entry_count(&self) -> usize {
        self.targets.len() + self.active_handles.len()
    }

    /// Classifies one same-snapshot canonical text record.
    pub(in crate::index_lifecycle) fn include_catalog_entry(
        &mut self,
        entry: crate::index_lifecycle::mutation_catalog::MutationCatalogEntry<'_>,
    ) -> Result<TextMutationTargetOrdinal> {
        match entry {
            crate::index_lifecycle::mutation_catalog::MutationCatalogEntry::Building(record) => {
                let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
                    return Err(corruption(
                        "text mutation classifier received another family",
                    ));
                };
                let ordinal = self.targets.len();
                self.targets.push(TextMutationTarget {
                    index_id: record.index_id(),
                    generation: record.state().generation(),
                    definition: definition.clone(),
                });
                Ok(TextMutationTargetOrdinal::Building(ordinal))
            }
            crate::index_lifecycle::mutation_catalog::MutationCatalogEntry::Active {
                record,
                handle,
            } => {
                if !matches!(
                    handle,
                    crate::index_lifecycle::ActiveIndexHandle::Text { .. }
                ) || !matches!(
                    record.definition(),
                    ValidatedDynamicIndexDefinition::Text(_)
                ) {
                    return Err(corruption(
                        "active text record carried another family handle",
                    ));
                }
                let ordinal = self.active_handles.len();
                self.active_handles.push(handle.clone());
                Ok(TextMutationTargetOrdinal::Active(ordinal))
            }
        }
    }
}

/// Loads every canonical text generation whose state owns mutation behavior.
///
/// The scan belongs to the caller's serializable graph transaction. A later
/// activation/drop revision therefore conflicts with that graph commit.
/// Building generations become coalesced-delta targets. Active generations
/// become definition-bearing handles consumed only by the complete request
/// orchestrator.
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) async fn load_mutation_set(
    transaction: &DbTransaction,
    scope: DataScope,
) -> Result<TextMutationSet> {
    let logical_prefix = ScopedKey::logical_prefix(RecordKind::IndexRecord);
    let physical_prefix = ManagedIndexKey::data_prefix(scope, logical_prefix);
    let mut rows = transaction.scan_prefix(&physical_prefix, ..).await?;
    let mut mutations = TextMutationSet::default();
    while let Some(row) = rows.next().await? {
        let ManagedIndexKey::Data {
            kind: ScopedKey::IndexRecord(key),
            ..
        } = ManagedIndexKey::parse_from_slice(scope, &row.key)?
        else {
            return Err(corruption(
                "text mutation catalog prefix yielded another key kind",
            ));
        };
        let record = decode_index_record(&row.value)?;
        if key.identity != *record.identity() {
            return Err(corruption(
                "text mutation catalog key/value identity mismatch",
            ));
        }
        let ValidatedDynamicIndexDefinition::Text(_) = record.definition() else {
            continue;
        };
        let active_handle = match record.state() {
            IndexStateV2::Building { .. } => None,
            IndexStateV2::Active { .. } => Some(
                crate::index_lifecycle::ActiveIndexHandle::try_from_record(scope, &record)
                    .ok_or_else(|| corruption("active text record did not project a handle"))?,
            ),
            IndexStateV2::Aborting { .. }
            | IndexStateV2::Dropping { .. }
            | IndexStateV2::Dropped { .. } => continue,
        };
        let entry = match active_handle.as_ref() {
            Some(handle) => {
                crate::index_lifecycle::mutation_catalog::MutationCatalogEntry::Active {
                    record: &record,
                    handle,
                }
            }
            None => {
                crate::index_lifecycle::mutation_catalog::MutationCatalogEntry::Building(&record)
            }
        };
        let _ = mutations.include_catalog_entry(entry)?;
    }
    Ok(mutations)
}

/// Prepares one marker per affected hidden text generation and entity.
///
/// Only definition inputs participate in change detection. Writing another
/// property therefore creates no lifecycle work. Every source row is observed
/// once here and once during validation, and every canonical replacement row is
/// measured without staging.
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) async fn prepare_text_build_deltas(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &TextMutationSet,
    entity: TextEntityMutation<'_>,
) -> Result<PreparedTextBuildDeltas> {
    let routes = crate::index_lifecycle::mutation_catalog::RoutedMutationTargets::Owned(
        mutations
            .targets
            .iter()
            .enumerate()
            .filter(|(_, target)| target.definition.element_kind() == entity.entity_kind)
            .map(|(ordinal, _)| {
                crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextBuilding(ordinal)
            })
            .collect(),
    );
    prepare_text_build_deltas_from(transaction, scope, mutations, &routes, entity, None).await
}

/// Prepares BUILD deltas while composing their shared statistics rows for one epoch.
pub(super) async fn prepare_text_build_deltas_in_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &TextMutationSet,
    routes: &crate::index_lifecycle::mutation_catalog::RoutedMutationTargets<'_>,
    entity: TextEntityMutation<'_>,
    statistics_batch: &mut super::statistics::PreparedTextStatisticsBatch,
) -> Result<PreparedTextBuildDeltas> {
    prepare_text_build_deltas_from(
        transaction,
        scope,
        mutations,
        routes,
        entity,
        Some(statistics_batch),
    )
    .await
}

async fn prepare_text_build_deltas_from(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &TextMutationSet,
    routes: &crate::index_lifecycle::mutation_catalog::RoutedMutationTargets<'_>,
    entity: TextEntityMutation<'_>,
    mut statistics_batch: Option<&mut super::statistics::PreparedTextStatisticsBatch>,
) -> Result<PreparedTextBuildDeltas> {
    let mut rows = Vec::new();
    let mut destination_keys = HashSet::new();
    let mut candidates = Vec::new();
    for ordinal in routes.iter().filter_map(|target| match target {
        crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextBuilding(ordinal) => {
            Some(ordinal)
        }
        crate::index_lifecycle::mutation_catalog::MutationRouteTarget::Secondary(_)
        | crate::index_lifecycle::mutation_catalog::MutationRouteTarget::Vector(_)
        | crate::index_lifecycle::mutation_catalog::MutationRouteTarget::TextActive(_) => None,
    }) {
        let target = mutations.targets.get(ordinal).ok_or_else(|| {
            corruption("text mutation route named a build target outside its catalog")
        })?;
        let before_is_candidate = matches!(
            super::projection::source_candidate(&target.definition, entity.before),
            super::projection::TextSourceCandidate::Candidate(_)
        );
        let after_is_candidate = matches!(
            super::projection::source_candidate(&target.definition, entity.after),
            super::projection::TextSourceCandidate::Candidate(_)
        );
        if !before_is_candidate && !after_is_candidate {
            continue;
        }
        let relevant_property_changed = std::iter::once("$label")
            .chain(std::iter::once(target.definition.property().as_str()))
            .chain(
                target
                    .definition
                    .tenant_property()
                    .map(|property| property.as_str()),
            )
            .any(|name| {
                entity
                    .before
                    .iter()
                    .find(|property| property.name == name)
                    .map(|property| &property.value)
                    != entity
                        .after
                        .iter()
                        .find(|property| property.name == name)
                        .map(|property| &property.value)
            });
        if !relevant_property_changed {
            continue;
        }

        let key = scoped_index_key(
            scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id: target.index_id,
                generation: target.generation,
                entity: IndexEntity {
                    kind: entity.entity_kind,
                    id: entity.entity_id,
                },
            }),
        );
        let value = CoalescedBuildDeltaValue {
            index_id: target.index_id,
            generation: target.generation,
            entity_kind: entity.entity_kind,
            entity_id: entity.entity_id,
            state: CoalescedBuildDeltaState::Marker,
        };
        if !destination_keys.insert(key.clone()) {
            return Err(corruption(
                "text mutation set produced a duplicate hidden-build delta",
            ));
        }
        candidates.push((ordinal, key, encode_build_delta(&value)));
    }

    let candidate_keys = candidates
        .iter()
        .map(|(_, key, _)| key.clone())
        .collect::<Vec<_>>();
    let observations = if candidate_keys.is_empty() {
        Vec::new()
    } else {
        transaction.multi_get(&candidate_keys).await?
    };
    for ((ordinal, key, value), observed) in candidates.into_iter().zip(observations) {
        let target = mutations.targets.get(ordinal).ok_or_else(|| {
            corruption("text mutation route named a build target outside its catalog")
        })?;
        let projection =
            super::projection::project(&target.definition, entity.after).map_err(|error| {
                HelixDbError::InvalidIndexSourceData {
                    reason: format!(
                        "text index {}:{}: {error}",
                        target.definition.label().as_str(),
                        target.definition.property().as_str(),
                    ),
                }
            })?;
        let contribution = match projection {
            super::projection::TextSourceProjection::NotIndexed => {
                crate::index_lifecycle::work::TextStatisticsContribution::Absent
            }
            super::projection::TextSourceProjection::Indexed { partition, text } => {
                super::statistics::present_contribution(
                    target.definition.analyzer(),
                    partition,
                    &text,
                )?
            }
        };
        let statistics = match statistics_batch.as_deref() {
            Some(batch) => {
                super::statistics::prepare_build_mutation_in_batch(
                    transaction,
                    batch,
                    scope,
                    target.index_id,
                    target.generation,
                    IndexEntity {
                        kind: entity.entity_kind,
                        id: entity.entity_id,
                    },
                    contribution,
                )
                .await?
            }
            None => {
                super::statistics::prepare_build_mutation(
                    transaction,
                    scope,
                    target.index_id,
                    target.generation,
                    IndexEntity {
                        kind: entity.entity_kind,
                        id: entity.entity_id,
                    },
                    contribution,
                )
                .await?
            }
        };
        if let Some(batch) = statistics_batch.as_deref_mut() {
            batch.push(statistics.clone())?;
        }
        rows.push(PreparedTextBuildDelta {
            observed,
            key,
            value,
            statistics,
        });
    }

    let measurements = rows
        .iter()
        .fold(TextBuildDeltaMeasurements::default(), |measured, row| {
            let observed_bytes = u64::try_from(row.key.len())
                .unwrap_or(u64::MAX)
                .saturating_add(
                    row.observed
                        .as_ref()
                        .map_or(0, |value| u64::try_from(value.len()).unwrap_or(u64::MAX)),
                );
            let (statistics_input, statistics_operations, statistics_output) =
                row.statistics.measurements();
            TextBuildDeltaMeasurements {
                input_bytes: measured
                    .input_bytes
                    .saturating_add(observed_bytes)
                    .saturating_add(statistics_input),
                output_operations: measured
                    .output_operations
                    .saturating_add(1)
                    .saturating_add(statistics_operations),
                output_bytes: measured
                    .output_bytes
                    .saturating_add(u64::try_from(row.key.len()).unwrap_or(u64::MAX))
                    .saturating_add(u64::try_from(row.value.len()).unwrap_or(u64::MAX))
                    .saturating_add(statistics_output),
            }
        });
    Ok(PreparedTextBuildDeltas { rows, measurements })
}

/// Stages one foreground batch whose preparation reads are conflict-tracked.
pub(super) fn stage_prepared_text_build_delta_rows(
    transaction: &DbTransaction,
    prepared: &PreparedTextBuildDeltas,
) -> Result<()> {
    for row in &prepared.rows {
        transaction.put(&row.key, &row.value)?;
    }
    Ok(())
}

/// Revalidates every hidden-build delta source without staging any write.
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) async fn validate_text_build_deltas(
    transaction: &DbTransaction,
    prepared: &PreparedTextBuildDeltas,
) -> Result<ValidatedTextBuildDeltas> {
    let keys = prepared
        .rows
        .iter()
        .map(|row| row.key.clone())
        .collect::<Vec<_>>();
    let observations = if keys.is_empty() {
        Vec::new()
    } else {
        transaction.multi_get(&keys).await?
    };
    for (row, observed) in prepared.rows.iter().zip(observations) {
        if observed != row.observed {
            return Err(corruption(
                "text hidden-build delta changed after serialized preflight",
            ));
        }
        super::statistics::validate(transaction, &row.statistics).await?;
    }
    Ok(ValidatedTextBuildDeltas {
        rows: prepared.rows.clone(),
    })
}

/// Stages hidden-build rows only after the complete request has validated.
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) fn stage_validated_text_build_deltas(
    transaction: &DbTransaction,
    validated: ValidatedTextBuildDeltas,
) -> Result<()> {
    for row in validated.rows {
        transaction.put(row.key, row.value)?;
        super::statistics::stage_validated(transaction, &row.statistics)?;
    }
    Ok(())
}

/// Encodes one scoped V2 key through the canonical `encoding/v2` boundary.
fn scoped_index_key(scope: DataScope, key: ScopedKey) -> bytes::Bytes {
    ManagedIndexKey::Data { scope, kind: key }.to_bytes()
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::TextAnalyzerKind;
    use crate::encoding::property::property_value::PropertyValue;
    use crate::encoding::v2::values::{decode_build_delta, encode_index_record};
    use crate::index_lifecycle::{
        graph_mutation, mutation_catalog, IndexOperationId, IndexRevision, IndexStateTransition,
        PhysicalGeneration,
    };

    /// Opens an isolated in-memory database for one mutation contract.
    async fn test_db(name: &str) -> Db {
        Db::builder(
            format!("index-lifecycle-text-mutation/{name}"),
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .expect("text mutation test database opens")
    }

    /// Constructs the partitioned text definition used by change detection.
    fn definition() -> ValidatedTextIndexDefinition {
        ValidatedTextIndexDefinition::try_new(
            IndexElementKind::Node,
            "Document",
            "body",
            Some("tenant"),
            TextAnalyzerKind::Standard,
            false,
        )
        .expect("text mutation definition is valid")
    }

    /// Returns complete graph properties for one text mutation snapshot.
    fn properties(body: &str, tenant: &str, unrelated: i64) -> Vec<Property> {
        vec![
            Property::new("$label", PropertyValue::String("Document".to_string())),
            Property::new("body", PropertyValue::String(body.to_string())),
            Property::new("tenant", PropertyValue::String(tenant.to_string())),
            Property::new("unrelated", PropertyValue::I64(unrelated)),
        ]
    }

    #[test]
    fn routed_relevance_skips_only_definite_absence_and_rejects_invalid_ordinals() {
        let mutations = TextMutationSet::one_build_target(
            IndexId::initial(),
            IndexGenerationId::initial(),
            definition(),
        );
        let missing_property = graph_mutation::GraphMutationTransition::delete(
            DataScope::LegacyUnscoped,
            graph_mutation::GraphEntity::node(1),
            graph_mutation::CanonicalPropertyRow::new(vec![
                Property::new("$label", PropertyValue::String("Document".to_string())),
                Property::new("tenant", PropertyValue::String("acme".to_string())),
            ]),
        );
        let candidate = graph_mutation::GraphMutationTransition::delete(
            DataScope::LegacyUnscoped,
            graph_mutation::GraphEntity::node(2),
            graph_mutation::CanonicalPropertyRow::new(properties("indexed", "acme", 1)),
        );
        let building = mutation_catalog::RoutedMutationTargets::Owned(vec![
            mutation_catalog::MutationRouteTarget::TextBuilding(0),
        ]);
        assert!(!mutations
            .routed_transition_relevant(&building, &missing_property)
            .expect("missing property is a valid relevance decision"));
        assert!(mutations
            .routed_transition_relevant(&building, &candidate)
            .expect("present property remains a candidate"));

        let unrelated = mutation_catalog::RoutedMutationTargets::Owned(vec![
            mutation_catalog::MutationRouteTarget::Secondary(0),
            mutation_catalog::MutationRouteTarget::Vector(0),
        ]);
        assert!(!mutations
            .routed_transition_relevant(&unrelated, &candidate)
            .expect("other families are not text relevant"));

        for (route, reason) in [
            (
                mutation_catalog::MutationRouteTarget::TextBuilding(1),
                "text mutation route named a build target outside its catalog",
            ),
            (
                mutation_catalog::MutationRouteTarget::TextActive(0),
                "text mutation route named an Active target outside its catalog",
            ),
        ] {
            let invalid = mutation_catalog::RoutedMutationTargets::Owned(vec![route]);
            assert!(matches!(
                mutations.routed_transition_relevant(&invalid, &candidate),
                Err(HelixDbError::IndexCatalogCorruption(actual)) if actual == reason
            ));
        }
    }

    #[tokio::test]
    async fn relevant_changes_coalesce_while_other_properties_and_entity_kinds_do_not() {
        let db = test_db("coalesced-relevant-inputs").await;
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let mutations = TextMutationSet {
            targets: vec![TextMutationTarget {
                index_id,
                generation,
                definition: definition(),
            }],
            active_handles: Vec::new(),
        };
        let original = properties("before", "acme", 1);
        let unrelated = properties("before", "acme", 2);
        let changed_body = properties("after", "acme", 2);
        let moved_tenant = properties("after", "globex", 2);
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("text mutation transaction opens");

        for entity in [
            TextEntityMutation::new(IndexElementKind::Node, 7, &original, &unrelated),
            TextEntityMutation::new(IndexElementKind::Edge, 7, &original, &changed_body),
        ] {
            let prepared = prepare_text_build_deltas(&transaction, scope, &mutations, entity)
                .await
                .expect("hidden-build delta preparation succeeds");
            assert_eq!(prepared.measurements().output_operations(), 0);
        }
        let prepared = prepare_text_build_deltas(
            &transaction,
            scope,
            &mutations,
            TextEntityMutation::new(IndexElementKind::Node, 7, &original, &moved_tenant),
        )
        .await
        .expect("coalesced hidden-build delta preparation succeeds");
        {
            let validated = validate_text_build_deltas(&transaction, &prepared)
                .await
                .expect("hidden-build delta validation succeeds");
            stage_validated_text_build_deltas(&transaction, validated)
                .expect("validated hidden-build delta stages");
        }
        transaction
            .commit()
            .await
            .expect("coalesced text delta commits");

        let prefix = ManagedIndexKey::data_prefix(
            scope,
            ScopedKey::generation_prefix(RecordKind::BuildDelta, index_id, generation),
        );
        let mut rows = db
            .scan_prefix(prefix, ..)
            .await
            .expect("text delta prefix is readable");
        let row = rows
            .next()
            .await
            .expect("text delta row is readable")
            .expect("one relevant delta exists");
        assert!(rows
            .next()
            .await
            .expect("text delta exhaustion is readable")
            .is_none());
        let delta = decode_build_delta(&row.value).expect("coalesced text delta decodes");
        assert_eq!(delta.index_id, index_id);
        assert_eq!(delta.generation, generation);
        assert_eq!(delta.entity_kind, IndexElementKind::Node);
        assert_eq!(delta.entity_id, IndexEntityId::new(7));
    }

    #[tokio::test]
    async fn missing_property_delete_prepares_no_hidden_build_delta() {
        let db = test_db("missing-property-delete-no-delta").await;
        let scope = DataScope::LegacyUnscoped;
        let mutations = TextMutationSet::one_build_target(
            IndexId::initial(),
            IndexGenerationId::initial(),
            definition(),
        );
        let before = vec![
            Property::new("$label", PropertyValue::String("Document".to_string())),
            Property::new("tenant", PropertyValue::String("acme".to_string())),
            Property::new("unrelated", PropertyValue::I64(1)),
        ];
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("missing-property text transaction opens");

        let prepared = prepare_text_build_deltas(
            &transaction,
            scope,
            &mutations,
            TextEntityMutation::new(IndexElementKind::Node, 8, &before, &[]),
        )
        .await
        .expect("missing-property delete preparation succeeds");
        assert_eq!(prepared.measurements().output_operations(), 0);
        assert_eq!(prepared.measurements().output_bytes(), 0);

        drop(transaction);
        db.close()
            .await
            .expect("missing-property text database closes");
    }

    #[tokio::test]
    async fn prepared_delta_measurement_is_exact_and_stale_validation_writes_nothing() {
        let db = test_db("prepared-delta-stale-validation").await;
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let mutations = TextMutationSet::one_build_target(index_id, generation, definition());
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(8),
        };
        let before = properties("before", "acme", 1);
        let after = properties("after", "acme", 1);
        let key = scoped_index_key(
            scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id,
                generation,
                entity,
            }),
        );
        let value = encode_build_delta(&CoalescedBuildDeltaValue {
            index_id,
            generation,
            entity_kind: entity.kind,
            entity_id: entity.id,
            state: CoalescedBuildDeltaState::Marker,
        });

        let original = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("text delta preflight transaction opens");
        let prepared = prepare_text_build_deltas(
            &original,
            scope,
            &mutations,
            TextEntityMutation::new(entity.kind, entity.id.get(), &before, &after),
        )
        .await
        .expect("one changed hidden build prepares one delta");
        let measured = prepared.measurements();
        let statistics_rows = prepared.rows[0].statistics.rows();
        let statistics_input = statistics_rows.iter().fold(0_u64, |bytes, row| {
            bytes.saturating_add(
                u64::try_from(
                    row.key
                        .len()
                        .saturating_add(row.observed.as_ref().map_or(0, Bytes::len)),
                )
                .unwrap_or(u64::MAX),
            )
        });
        let statistics_operations = u64::try_from(
            statistics_rows
                .iter()
                .filter(|row| row.replacement != row.observed)
                .count(),
        )
        .unwrap_or(u64::MAX);
        let statistics_output = statistics_rows.iter().fold(0_u64, |bytes, row| {
            if row.replacement == row.observed {
                return bytes;
            }
            bytes.saturating_add(
                u64::try_from(
                    row.key
                        .len()
                        .saturating_add(row.replacement.as_ref().map_or(0, Bytes::len)),
                )
                .unwrap_or(u64::MAX),
            )
        });
        assert_eq!(
            measured.input_bytes(),
            u64::try_from(key.len())
                .unwrap()
                .saturating_add(statistics_input)
        );
        assert_eq!(
            measured.output_operations(),
            1_u64.saturating_add(statistics_operations)
        );
        assert_eq!(
            measured.output_bytes(),
            u64::try_from(key.len() + value.len())
                .unwrap()
                .saturating_add(statistics_output)
        );
        drop(original);

        let concurrent = Bytes::from_static(b"concurrent hidden-build delta");
        db.put(key.clone(), concurrent.clone())
            .await
            .expect("concurrent delta commits");
        let replay = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("text delta replay transaction opens");
        assert!(matches!(
            validate_text_build_deltas(&replay, &prepared).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "text hidden-build delta changed after serialized preflight"
        ));
        replay
            .commit()
            .await
            .expect("failed validation buffers no write");
        assert_eq!(
            db.get(key)
                .await
                .expect("delta remains readable")
                .as_deref(),
            Some(concurrent.as_ref())
        );

        let duplicate = TextMutationTarget {
            index_id,
            generation,
            definition: definition(),
        };
        let duplicate_mutations = TextMutationSet {
            targets: vec![duplicate.clone(), duplicate],
            active_handles: Vec::new(),
        };
        let duplicate_transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("duplicate text delta transaction opens");
        assert!(matches!(
            prepare_text_build_deltas(
                &duplicate_transaction,
                scope,
                &duplicate_mutations,
                TextEntityMutation::new(entity.kind, entity.id.get(), &before, &after),
            )
            .await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "text mutation set produced a duplicate hidden-build delta"
        ));
    }

    #[tokio::test]
    async fn serialized_preparation_conflicts_for_hidden_delta_and_statistics_rows() {
        let db = test_db("prepared-delta-serializable-conflicts").await;
        let scope = DataScope::LegacyUnscoped;
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let mutations = TextMutationSet::one_build_target(index_id, generation, definition());
        let before = properties("before", "acme", 1);
        let after = properties("after", "acme", 1);

        let delta_entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(9),
        };
        let delta_key = scoped_index_key(
            scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id,
                generation,
                entity: delta_entity,
            }),
        );
        let delta_loser = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("hidden-delta loser opens");
        let delta_prepared = prepare_text_build_deltas(
            &delta_loser,
            scope,
            &mutations,
            TextEntityMutation::new(delta_entity.kind, delta_entity.id.get(), &before, &after),
        )
        .await
        .expect("hidden-delta loser prepares from its conflict-tracked read");
        stage_prepared_text_build_delta_rows(&delta_loser, &delta_prepared)
            .expect("hidden-delta loser stages without a second read");
        let winning_delta = Bytes::from_static(b"winning hidden-build delta");
        db.put(delta_key.clone(), winning_delta.clone())
            .await
            .expect("competing hidden delta commits");
        assert_eq!(
            delta_loser
                .commit()
                .await
                .expect_err("stale hidden-delta preparation must conflict")
                .kind(),
            slatedb::ErrorKind::Transaction
        );
        assert_eq!(
            db.get(&delta_key).await.expect("winning delta reads"),
            Some(winning_delta)
        );

        let statistics_entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(10),
        };
        let statistics_loser = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("statistics loser opens");
        let statistics_prepared = prepare_text_build_deltas(
            &statistics_loser,
            scope,
            &mutations,
            TextEntityMutation::new(
                statistics_entity.kind,
                statistics_entity.id.get(),
                &before,
                &after,
            ),
        )
        .await
        .expect("statistics loser prepares from its conflict-tracked reads");
        stage_prepared_text_build_delta_rows(&statistics_loser, &statistics_prepared)
            .expect("statistics loser stages its hidden delta");
        let statistics = &statistics_prepared.rows[0].statistics;
        super::super::statistics::stage_validated(&statistics_loser, statistics)
            .expect("statistics loser stages without second reads");
        let statistics_key = statistics.rows()[0].key.clone();
        let winning_statistics = Bytes::from_static(b"winning statistics row");
        db.put(statistics_key.clone(), winning_statistics.clone())
            .await
            .expect("competing statistics row commits");
        assert_eq!(
            statistics_loser
                .commit()
                .await
                .expect_err("stale statistics preparation must conflict")
                .kind(),
            slatedb::ErrorKind::Transaction
        );
        assert_eq!(
            db.get(&statistics_key)
                .await
                .expect("winning statistics row reads"),
            Some(winning_statistics)
        );
        db.close().await.expect("conflict test database closes");
    }

    #[tokio::test]
    async fn catalog_loads_active_text_for_the_complete_request_orchestrator() {
        let db = test_db("active-request-authority").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::Text(definition());
        let record = crate::index_lifecycle::IndexRecordV2::building(
            IndexId::initial(),
            definition,
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x41; 16]).expect("operation ID is non-nil"),
        )
        .expect("building text record is valid")
        .transition(IndexStateTransition::Activate)
        .expect("text record activates");
        db.put(
            scoped_index_key(scope, ScopedKey::index_record(record.identity().clone())),
            encode_index_record(&record),
        )
        .await
        .expect("active text record is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("active text mutation transaction opens");

        let mutations = load_mutation_set(&transaction, scope)
            .await
            .expect("the transaction retains its complete Active text set");
        assert_eq!(mutations.active_handles().len(), 1);
        let ValidatedDynamicIndexDefinition::Text(expected_definition) = record.definition() else {
            panic!("the seeded Active record remains text-typed");
        };
        assert_eq!(
            mutations.active_handles()[0]
                .text_definition()
                .expect("loaded handle remains text-typed"),
            expected_definition
        );
        assert!(mutations.has_active_handles());
    }

    #[tokio::test]
    async fn catalog_ignores_other_families_and_rejects_text_key_value_disagreement() {
        let db = test_db("catalog-family-and-identity-checks").await;
        let scope = DataScope::LegacyUnscoped;
        let secondary_definition =
            crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
                crate::config::SecondaryIndexDefinition::node_equality("Document", "slug")
                    .expect("secondary definition is valid"),
            )
            .expect("secondary definition validates for V2");
        let secondary_record = crate::index_lifecycle::IndexRecordV2::building(
            IndexId::initial(),
            secondary_definition,
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x42; 16]).expect("operation ID is non-nil"),
        )
        .expect("building secondary record is valid");
        db.put(
            scoped_index_key(
                scope,
                ScopedKey::index_record(secondary_record.identity().clone()),
            ),
            encode_index_record(&secondary_record),
        )
        .await
        .expect("secondary record is written");
        let dropped_text_record = crate::index_lifecycle::IndexRecordV2::building(
            IndexId::new(2).expect("second index ID is non-zero"),
            ValidatedDynamicIndexDefinition::Text(definition()),
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x43; 16]).expect("operation ID is non-nil"),
        )
        .expect("building text record is valid")
        .transition(IndexStateTransition::BeginAbort)
        .expect("text build begins abort")
        .transition(IndexStateTransition::CompleteAbort)
        .expect("text build completes abort");
        db.put(
            scoped_index_key(
                scope,
                ScopedKey::index_record(dropped_text_record.identity().clone()),
            ),
            encode_index_record(&dropped_text_record),
        )
        .await
        .expect("dropped text record is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("other-family transaction opens");
        assert!(load_mutation_set(&transaction, scope)
            .await
            .expect("other families are ignored")
            .targets
            .is_empty());
        drop(transaction);

        let building_text_record = crate::index_lifecycle::IndexRecordV2::building(
            IndexId::new(4).expect("fourth index ID is non-zero"),
            ValidatedDynamicIndexDefinition::Text(definition()),
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x45; 16]).expect("operation ID is non-nil"),
        )
        .expect("building text record is valid");
        db.put(
            scoped_index_key(
                scope,
                ScopedKey::index_record(building_text_record.identity().clone()),
            ),
            encode_index_record(&building_text_record),
        )
        .await
        .expect("building text record is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("building text transaction opens");
        let loaded = load_mutation_set(&transaction, scope)
            .await
            .expect("building text generation is mutation-visible");
        assert_eq!(loaded.targets.len(), 1);
        assert_eq!(loaded.targets[0].index_id, building_text_record.index_id());
        drop(transaction);

        let text_record = crate::index_lifecycle::IndexRecordV2::building(
            IndexId::new(3).expect("third index ID is non-zero"),
            ValidatedDynamicIndexDefinition::Text(
                ValidatedTextIndexDefinition::try_new(
                    IndexElementKind::Node,
                    "Document",
                    "summary",
                    Some("tenant"),
                    TextAnalyzerKind::Standard,
                    false,
                )
                .expect("second text definition is valid"),
            ),
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::from_bytes([0x44; 16]).expect("operation ID is non-nil"),
        )
        .expect("building text record is valid");
        let wrong_definition = ValidatedTextIndexDefinition::try_new(
            IndexElementKind::Node,
            "Document",
            "title",
            Some("tenant"),
            TextAnalyzerKind::Standard,
            false,
        )
        .expect("different text identity is valid");
        db.put(
            scoped_index_key(scope, ScopedKey::index_record(wrong_definition.identity())),
            encode_index_record(&text_record),
        )
        .await
        .expect("disagreeing text key/value fixture is written");
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("disagreeing text transaction opens");
        assert!(matches!(
            load_mutation_set(&transaction, scope).await,
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
    }
}
