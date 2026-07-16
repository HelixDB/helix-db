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
use crate::encoding::v1::keys::index_v2::{
    IndexEntity, IndexEntityStateKey, IndexV2Key, IndexV2RecordKind,
};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::values::index_v2::{
    decode_index_record, encode_work_value, IndexV2WorkValue,
};
use crate::error::{HelixDbError, Result};
use crate::index_v2::work::CoalescedBuildDeltaValue;
use crate::index_v2::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexStateV2,
    ValidatedDynamicIndexDefinition, ValidatedTextIndexDefinition,
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
}

/// Exact serialized measurements for every hidden-build delta in one request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct TextBuildDeltaMeasurements {
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
}

impl TextBuildDeltaMeasurements {
    /// Returns bytes read during preparation and pre-staging revalidation.
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
pub(super) struct PreparedTextBuildDeltas {
    rows: Vec<PreparedTextBuildDelta>,
    measurements: TextBuildDeltaMeasurements,
}

impl PreparedTextBuildDeltas {
    /// Returns exact work contributed to the enclosing request preflight.
    pub(super) const fn measurements(&self) -> TextBuildDeltaMeasurements {
        self.measurements
    }
}

/// Revalidated hidden-build rows ready for infallible staging.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ValidatedTextBuildDeltas {
    rows: Vec<PreparedTextBuildDelta>,
}

/// Transaction-local text generations that accept ordinary mutation work.
#[derive(Debug, Clone, Default)]
pub(crate) struct TextMutationSet {
    targets: Vec<TextMutationTarget>,
    active_handles: Vec<crate::index_v2::ActiveIndexHandle>,
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

    /// Adds the complete Active set used by request-composition tests.
    #[cfg(test)]
    pub(super) fn with_active_handles(
        mut self,
        active_handles: Vec<crate::index_v2::ActiveIndexHandle>,
    ) -> Self {
        self.active_handles = active_handles;
        self
    }

    /// Returns every canonical Active text handle loaded in this transaction.
    pub(super) fn active_handles(&self) -> &[crate::index_v2::ActiveIndexHandle] {
        &self.active_handles
    }

    /// Returns whether request-level Active outcome authority must be retained.
    pub(crate) const fn has_active_handles(&self) -> bool {
        !self.active_handles.is_empty()
    }
}

/// Loads every canonical text generation whose state owns mutation behavior.
///
/// The scan belongs to the caller's serializable graph transaction. A later
/// activation/drop revision therefore conflicts with that graph commit.
/// Building generations become coalesced-delta targets. Active generations
/// become definition-bearing handles consumed only by the complete request
/// orchestrator.
pub(crate) async fn load_mutation_set(
    transaction: &DbTransaction,
    scope: DataScope,
) -> Result<TextMutationSet> {
    let logical_prefix = IndexV2Key::logical_prefix(IndexV2RecordKind::IndexRecord);
    let physical_prefix = Key::data_prefix(scope, logical_prefix);
    let mut rows = transaction.scan_prefix(&physical_prefix, ..).await?;
    let mut targets = Vec::new();
    let mut active_handles = Vec::new();
    while let Some(row) = rows.next().await? {
        let Key::Data {
            kind: DataKeyKind::IndexV2(IndexV2Key::IndexRecord(key)),
            ..
        } = Key::parse_from_slice(scope, &row.key)?
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
        let ValidatedDynamicIndexDefinition::Text(definition) = record.definition() else {
            continue;
        };
        match record.state() {
            IndexStateV2::Building { .. } => targets.push(TextMutationTarget {
                index_id: record.index_id(),
                generation: record.state().generation(),
                definition: definition.clone(),
            }),
            IndexStateV2::Active { .. } => active_handles.push(
                crate::index_v2::ActiveIndexHandle::try_from_record(scope, &record)
                    .ok_or_else(|| corruption("active text record did not project a handle"))?,
            ),
            IndexStateV2::Aborting { .. }
            | IndexStateV2::Dropping { .. }
            | IndexStateV2::Dropped { .. } => {}
        }
    }
    Ok(TextMutationSet {
        targets,
        active_handles,
    })
}

/// Prepares one marker per affected hidden text generation and entity.
///
/// Only definition inputs participate in change detection. Writing another
/// property therefore creates no lifecycle work. Every source row is observed
/// once here and once during validation, and every canonical replacement row is
/// measured without staging.
pub(super) async fn prepare_text_build_deltas(
    transaction: &DbTransaction,
    scope: DataScope,
    mutations: &TextMutationSet,
    entity: TextEntityMutation<'_>,
) -> Result<PreparedTextBuildDeltas> {
    let mut rows = Vec::new();
    let mut destination_keys = HashSet::new();
    for target in mutations
        .targets
        .iter()
        .filter(|target| target.definition.element_kind() == entity.entity_kind)
    {
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
            IndexV2Key::BuildDelta(IndexEntityStateKey {
                index_id: target.index_id,
                generation: target.generation,
                entity: IndexEntity {
                    kind: entity.entity_kind,
                    id: entity.entity_id,
                },
            }),
        );
        let value = IndexV2WorkValue::CoalescedBuildDelta(CoalescedBuildDeltaValue {
            index_id: target.index_id,
            generation: target.generation,
            entity_kind: entity.entity_kind,
            entity_id: entity.entity_id,
        });
        if !destination_keys.insert(key.clone()) {
            return Err(corruption(
                "text mutation set produced a duplicate hidden-build delta",
            ));
        }
        rows.push(PreparedTextBuildDelta {
            observed: transaction.get(&key).await?,
            key,
            value: encode_work_value(&value),
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
            TextBuildDeltaMeasurements {
                input_bytes: measured
                    .input_bytes
                    .saturating_add(observed_bytes.saturating_mul(2)),
                output_operations: measured.output_operations.saturating_add(1),
                output_bytes: measured
                    .output_bytes
                    .saturating_add(u64::try_from(row.key.len()).unwrap_or(u64::MAX))
                    .saturating_add(u64::try_from(row.value.len()).unwrap_or(u64::MAX)),
            }
        });
    Ok(PreparedTextBuildDeltas { rows, measurements })
}

/// Revalidates every hidden-build delta source without staging any write.
pub(super) async fn validate_text_build_deltas(
    transaction: &DbTransaction,
    prepared: &PreparedTextBuildDeltas,
) -> Result<ValidatedTextBuildDeltas> {
    for row in &prepared.rows {
        if transaction.get(&row.key).await? != row.observed {
            return Err(corruption(
                "text hidden-build delta changed after serialized preflight",
            ));
        }
    }
    Ok(ValidatedTextBuildDeltas {
        rows: prepared.rows.clone(),
    })
}

/// Stages hidden-build rows only after the complete request has validated.
pub(super) fn stage_validated_text_build_deltas(
    transaction: &DbTransaction,
    validated: ValidatedTextBuildDeltas,
) -> Result<()> {
    for row in validated.rows {
        transaction.put(row.key, row.value)?;
    }
    Ok(())
}

/// Encodes one scoped V2 key through the canonical `encoding/v1` boundary.
fn scoped_index_key(scope: DataScope, key: IndexV2Key) -> bytes::Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(key),
    }
    .to_bytes()
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
    use crate::encoding::v1::values::index_v2::{decode_work_value, encode_index_record};
    use crate::index_v2::{
        IndexOperationId, IndexRevision, IndexStateTransition, PhysicalGeneration,
    };

    /// Opens an isolated in-memory database for one mutation contract.
    async fn test_db(name: &str) -> Db {
        Db::builder(
            format!("index-v2-text-mutation/{name}"),
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
            TextEntityMutation::new(IndexElementKind::Node, 7, &unrelated, &changed_body),
            TextEntityMutation::new(IndexElementKind::Node, 7, &changed_body, &moved_tenant),
        ] {
            let prepared = prepare_text_build_deltas(&transaction, scope, &mutations, entity)
                .await
                .expect("hidden-build delta preparation succeeds");
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

        let prefix = Key::data_prefix(
            scope,
            IndexV2Key::generation_prefix(IndexV2RecordKind::BuildDelta, index_id, generation),
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
        let IndexV2WorkValue::CoalescedBuildDelta(delta) =
            decode_work_value(&row.value).expect("coalesced text delta decodes")
        else {
            panic!("text build delta key contains its typed value");
        };
        assert_eq!(delta.index_id, index_id);
        assert_eq!(delta.generation, generation);
        assert_eq!(delta.entity_kind, IndexElementKind::Node);
        assert_eq!(delta.entity_id, IndexEntityId::new(7));
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
            IndexV2Key::BuildDelta(IndexEntityStateKey {
                index_id,
                generation,
                entity,
            }),
        );
        let value = encode_work_value(&IndexV2WorkValue::CoalescedBuildDelta(
            CoalescedBuildDeltaValue {
                index_id,
                generation,
                entity_kind: entity.kind,
                entity_id: entity.id,
            },
        ));

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
        assert_eq!(
            measured.input_bytes(),
            u64::try_from(key.len() * 2).unwrap()
        );
        assert_eq!(measured.output_operations(), 1);
        assert_eq!(
            measured.output_bytes(),
            u64::try_from(key.len() + value.len()).unwrap()
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
    async fn catalog_loads_active_text_for_the_complete_request_orchestrator() {
        let db = test_db("active-request-authority").await;
        let scope = DataScope::LegacyUnscoped;
        let definition = ValidatedDynamicIndexDefinition::Text(definition());
        let record = crate::index_v2::IndexRecordV2::building(
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
            scoped_index_key(scope, IndexV2Key::index_record(record.identity().clone())),
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
        let secondary_definition = crate::index_v2::ValidatedDynamicIndexDefinition::try_from(
            crate::config::SecondaryIndexDefinition::node_equality("Document", "slug")
                .expect("secondary definition is valid"),
        )
        .expect("secondary definition validates for V2");
        let secondary_record = crate::index_v2::IndexRecordV2::building(
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
                IndexV2Key::index_record(secondary_record.identity().clone()),
            ),
            encode_index_record(&secondary_record),
        )
        .await
        .expect("secondary record is written");
        let dropped_text_record = crate::index_v2::IndexRecordV2::building(
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
                IndexV2Key::index_record(dropped_text_record.identity().clone()),
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

        let building_text_record = crate::index_v2::IndexRecordV2::building(
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
                IndexV2Key::index_record(building_text_record.identity().clone()),
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

        let text_record = crate::index_v2::IndexRecordV2::building(
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
            scoped_index_key(scope, IndexV2Key::index_record(wrong_definition.identity())),
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
