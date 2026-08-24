//! Lease-adjacent serving reads for Active V2 text generations.
//!
//! The interpreter owns the request lease and calls these boundaries only
//! inside an admitted physical batch. This module point-loads the exact
//! partition root, loads bounded manifest pages, and resolves candidate live
//! state from generation-qualified kind-`0x0C` rows. Every
//! decoded value is cross-checked against its typed key before it can reach
//! Tantivy or object storage.

use std::collections::BTreeMap;

use futures::{StreamExt, TryStreamExt};
use slatedb::DbReadOps;

use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::ManagedIndexKey;
use crate::encoding::v2::values as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::work;
use crate::index_lifecycle::{
    ActiveIndexHandle, IndexElementKind, IndexEntityId, IndexGenerationId, IndexId,
    TextLogicalVersion, TextManifestRevision, ValidatedTextIndexDefinition,
};

/// Family-refined Active authority retained after lease acquisition.
///
/// Private fields make a secondary/vector generation or mismatched definition
/// impossible to pass into text manifest serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActiveTextServingAuthority {
    scope: DataScope,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: Box<ValidatedTextIndexDefinition>,
}

impl ActiveTextServingAuthority {
    /// Refines the family-wide Active handle into exact text authority.
    pub(crate) fn try_from_active(handle: &ActiveIndexHandle) -> Result<Self> {
        let ActiveIndexHandle::Text {
            scope,
            index_id,
            generation,
            definition,
            ..
        } = handle
        else {
            return Err(corruption(
                "text serving authority received another Active family",
            ));
        };
        Ok(Self {
            scope: *scope,
            index_id: *index_id,
            generation: *generation,
            definition: definition.clone(),
        })
    }

    /// Returns the data scope containing this generation.
    pub(crate) const fn scope(&self) -> DataScope {
        self.scope
    }

    /// Returns the stable logical index owner.
    pub(crate) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Returns the exact physical generation owner.
    pub(crate) const fn generation(&self) -> IndexGenerationId {
        self.generation
    }

    /// Borrows the canonical settings used by page-backed Tantivy reads.
    pub(crate) const fn definition(&self) -> &ValidatedTextIndexDefinition {
        &self.definition
    }
}

/// Validated root authority for one Active text partition.
///
/// This value contains no lease itself. Callers must retain the lease paired
/// with the Active handle from which the root was loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValidatedActiveTextManifestRoot {
    scope: DataScope,
    key: index_keys::TextManifestRootKey,
    partition: work::TextPartition,
    revision: TextManifestRevision,
    page_count: u32,
    split_count: u64,
    element_kind: IndexElementKind,
}

impl ValidatedActiveTextManifestRoot {
    /// Returns the stable logical index owner.
    pub(crate) const fn index_id(&self) -> IndexId {
        self.key.index_id
    }

    /// Returns the exact physical generation owner.
    pub(crate) const fn generation(&self) -> IndexGenerationId {
        self.key.generation
    }

    /// Returns the canonical partition represented by the root fingerprint.
    pub(crate) const fn partition(&self) -> &work::TextPartition {
        &self.partition
    }

    /// Returns the number of contiguous non-empty pages starting at zero.
    pub(crate) const fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Returns the exact split total declared across every page.
    pub(crate) const fn split_count(&self) -> u64 {
        self.split_count
    }
}

/// Minimal checked live-state projection consumed by text candidate filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ActiveTextEntityState {
    logical_version: TextLogicalVersion,
    live: bool,
}

impl ActiveTextEntityState {
    /// Returns the document version that may remain visible in a split.
    pub(crate) const fn logical_version(self) -> u64 {
        self.logical_version.get()
    }

    /// Returns whether the exact version remains live.
    pub(crate) const fn is_live(self) -> bool {
        self.live
    }
}

/// Loads the exact partition root authorized by an Active text handle.
///
/// A missing tenant partition is an empty result. An unpartitioned Active
/// generation must retain its canonical root even when it contains no splits,
/// so absence in that shape is corruption.
pub(crate) async fn load_active_manifest_root(
    reader: &(impl DbReadOps + Sync),
    authority: &ActiveTextServingAuthority,
    partition: &work::TextPartition,
) -> Result<Option<ValidatedActiveTextManifestRoot>> {
    let definition = authority.definition();
    let partition_mode_matches = matches!(
        (definition.tenant_property(), partition),
        (None, work::TextPartition::Unpartitioned) | (Some(_), work::TextPartition::TenantValue(_))
    );
    if !partition_mode_matches {
        return Err(corruption(
            "text manifest partition shape disagrees with its Active definition",
        ));
    }

    let typed_key = index_keys::TextManifestRootKey {
        index_id: authority.index_id(),
        generation: authority.generation(),
        partition: partition.fingerprint(),
    };
    let key = scoped_key(
        authority.scope(),
        index_keys::ScopedKey::TextManifestRoot(typed_key),
    );
    let Some(value) = reader.get(key).await? else {
        return match partition {
            work::TextPartition::TenantValue(_) => Ok(None),
            work::TextPartition::Unpartitioned => Err(corruption(
                "Active unpartitioned text generation has no manifest root",
            )),
        };
    };
    let root = crate::index_lifecycle::expect_typed_value(
        index_values::decode_manifest_root(&value),
        "text manifest root key contains another typed value kind",
    )?;
    if root.index_id() != authority.index_id()
        || root.generation() != authority.generation()
        || root.partition() != partition
        || typed_key.partition != root.partition().fingerprint()
    {
        return Err(corruption(
            "text manifest root key/value ownership mismatch",
        ));
    }
    let revision_is_valid = if root.page_count() == 0 {
        root.split_count() == 0
    } else {
        root.revision().get() >= u64::from(root.page_count()).saturating_add(1)
            && root.split_count() != 0
    };
    if !revision_is_valid {
        return Err(corruption(
            "Active text manifest root has invalid page, split, or revision state",
        ));
    }
    let corpus_key = super::statistics::corpus_key(
        authority.scope(),
        authority.index_id(),
        authority.generation(),
        partition,
    );
    let corpus_value = reader.get(corpus_key).await?;
    super::statistics::validate_manifest_corpus(
        corpus_value.as_deref(),
        authority.index_id(),
        authority.generation(),
        partition,
        root.split_count(),
    )?;

    Ok(Some(ValidatedActiveTextManifestRoot {
        scope: authority.scope(),
        key: typed_key,
        partition: partition.clone(),
        revision: root.revision(),
        page_count: root.page_count(),
        split_count: root.split_count(),
        element_kind: definition.element_kind(),
    }))
}

/// Enumerates and validates every current partition root for one exact Active
/// text generation. This is the startup-warming counterpart to point loading;
/// serving still resolves its request partition directly.
pub(crate) async fn load_active_manifest_roots(
    reader: &(impl DbReadOps + Sync),
    authority: &ActiveTextServingAuthority,
) -> Result<Vec<ValidatedActiveTextManifestRoot>> {
    let logical_prefix = index_keys::ScopedKey::generation_prefix(
        index_keys::RecordKind::TextManifestRoot,
        authority.index_id(),
        authority.generation(),
    );
    let prefix = ManagedIndexKey::data_prefix(authority.scope(), logical_prefix);
    let mut rows = reader.scan_prefix(&prefix, ..).await?;
    let mut roots = Vec::new();
    while let Some(row) = rows.next().await? {
        let ManagedIndexKey::Data {
            kind: index_keys::ScopedKey::TextManifestRoot(key),
            ..
        } = ManagedIndexKey::parse_from_slice(authority.scope(), &row.key)?
        else {
            return Err(corruption(
                "text manifest-root prefix yielded another typed key",
            ));
        };
        let root = crate::index_lifecycle::expect_typed_value(
            index_values::decode_manifest_root(&row.value),
            "text manifest root key contains another typed value kind",
        )?;
        if key.index_id != authority.index_id()
            || key.generation != authority.generation()
            || root.index_id() != authority.index_id()
            || root.generation() != authority.generation()
            || key.partition != root.partition().fingerprint()
        {
            return Err(corruption(
                "text manifest-root key/value ownership mismatch",
            ));
        }
        let partition_mode_matches = matches!(
            (authority.definition().tenant_property(), root.partition()),
            (None, work::TextPartition::Unpartitioned)
                | (Some(_), work::TextPartition::TenantValue(_))
        );
        if !partition_mode_matches {
            return Err(corruption(
                "text manifest-root partition disagrees with its Active definition",
            ));
        }
        let revision_is_valid = if root.page_count() == 0 {
            root.split_count() == 0
        } else {
            root.revision().get() >= u64::from(root.page_count()).saturating_add(1)
                && root.split_count() != 0
        };
        if !revision_is_valid {
            return Err(corruption(
                "Active text manifest root has invalid page, split, or revision state",
            ));
        }
        let corpus_key = super::statistics::corpus_key(
            authority.scope(),
            authority.index_id(),
            authority.generation(),
            root.partition(),
        );
        let corpus_value = reader.get(corpus_key).await?;
        super::statistics::validate_manifest_corpus(
            corpus_value.as_deref(),
            authority.index_id(),
            authority.generation(),
            root.partition(),
            root.split_count(),
        )?;
        roots.push(ValidatedActiveTextManifestRoot {
            scope: authority.scope(),
            key,
            partition: root.partition().clone(),
            revision: root.revision(),
            page_count: root.page_count(),
            split_count: root.split_count(),
            element_kind: authority.definition().element_kind(),
        });
    }
    if roots.is_empty() && authority.definition().tenant_property().is_none() {
        return Err(corruption(
            "Active unpartitioned text generation has no manifest root",
        ));
    }
    Ok(roots)
}

/// Loads and validates one contiguous non-empty page under a checked root.
pub(crate) async fn load_active_manifest_page(
    reader: &(impl DbReadOps + Sync),
    root: &ValidatedActiveTextManifestRoot,
    page: u32,
) -> Result<Vec<work::SplitRef>> {
    if page >= root.page_count {
        return Err(corruption(
            "text serving requested a page outside the manifest root",
        ));
    }
    let typed_key = index_keys::TextManifestPageKey {
        root: root.key,
        page,
    };
    let key = scoped_key(
        root.scope,
        index_keys::ScopedKey::TextManifestPage(typed_key),
    );
    let Some(value) = reader.get(key).await? else {
        return Err(corruption(
            "Active text manifest root references a missing page",
        ));
    };
    let value = crate::index_lifecycle::expect_typed_value(
        index_values::decode_manifest_page(&value),
        "text manifest page key contains another typed value kind",
    )?;
    if value.index_id() != root.index_id()
        || value.generation() != root.generation()
        || value.partition() != root.partition()
        || value.page() != page
        || typed_key.root.partition != value.partition().fingerprint()
    {
        return Err(corruption(
            "text manifest page key/value ownership mismatch",
        ));
    }
    Ok(value.entries().to_vec())
}

/// Point-loads the exact V2 state used to accept or reject one split candidate.
///
/// Missing state is corruption for a V2 candidate: unlike configured-static
/// manifests, every document admitted to a V2 split has a canonical
/// generation-qualified state row.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn load_active_entity_state(
    reader: &(impl DbReadOps + Send + Sync),
    root: &ValidatedActiveTextManifestRoot,
    entity_id: u64,
) -> Result<ActiveTextEntityState> {
    Ok(load_active_entity_states(reader, root, &[entity_id])
        .await?
        .remove(&entity_id)
        .expect("a validated requested entity has one returned state"))
}

/// Batch-loads up to 512 exact V2 states and validates live contribution rows.
pub(crate) async fn load_active_entity_states(
    reader: &(impl DbReadOps + Send + Sync),
    root: &ValidatedActiveTextManifestRoot,
    entity_ids: &[u64],
) -> Result<BTreeMap<u64, ActiveTextEntityState>> {
    const MAX_STATE_BATCH: usize = 512;
    const CONTRIBUTION_READ_CONCURRENCY: usize = 8;
    if entity_ids.len() > MAX_STATE_BATCH {
        return Err(HelixDbError::InvariantViolation(format!(
            "text entity-state batch has {} entries; maximum is {MAX_STATE_BATCH}",
            entity_ids.len()
        )));
    }
    let entities = entity_ids
        .iter()
        .copied()
        .map(|entity_id| index_keys::IndexEntity {
            kind: root.element_kind,
            id: IndexEntityId::new(entity_id),
        })
        .collect::<Vec<_>>();
    let keys = entities
        .iter()
        .copied()
        .map(|entity| {
            scoped_key(
                root.scope,
                index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                    root: root.key,
                    entity,
                }),
            )
        })
        .collect::<Vec<_>>();
    let values = reader.multi_get(&keys).await?;
    let mut states = BTreeMap::new();
    let mut live_entities = Vec::new();
    for ((entity, key), value) in entities.into_iter().zip(keys).zip(values) {
        let Some(value) = value else {
            return Err(corruption(
                "Active V2 text split candidate has no entity state",
            ));
        };
        let state = crate::index_lifecycle::expect_typed_value(
            index_values::decode_text_entity_state(&value),
            "text entity-state key contains another typed value kind",
        )?;
        let typed_key = index_keys::TextEntityStateKey {
            root: root.key,
            entity,
        };
        if key
            != scoped_key(
                root.scope,
                index_keys::ScopedKey::TextEntityState(typed_key),
            )
            || state.index_id != root.index_id()
            || state.generation != root.generation()
            || state.partition != root.partition
            || state.entity_kind != entity.kind
            || state.entity_id != entity.id
            || typed_key.root.partition != state.partition.fingerprint()
            || state.logical_version.get() > root.revision.get()
        {
            return Err(corruption(
                "text entity-state key/value ownership or revision mismatch",
            ));
        }
        if state.live {
            live_entities.push(entity);
        }
        states.insert(
            entity.id.get(),
            ActiveTextEntityState {
                logical_version: state.logical_version,
                live: state.live,
            },
        );
    }
    futures::stream::iter(live_entities)
        .map(|entity| async move {
            let Some(contribution) = super::statistics::load_entity_contribution(
                reader,
                root.scope,
                root.index_id(),
                root.generation(),
                entity,
            )
            .await?
            else {
                return Err(corruption(
                    "live Active text entity has no statistics marker",
                ));
            };
            if !matches!(
                contribution,
                work::TextStatisticsContribution::Present { partition, .. }
                    if partition == root.partition
            ) {
                return Err(corruption(
                    "live Active text entity disagrees with its statistics marker",
                ));
            }
            Ok(())
        })
        .buffer_unordered(CONTRIBUTION_READ_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(states)
}

/// Encodes one typed V2 data key in its exact scope.
fn scoped_key(scope: DataScope, key: index_keys::ScopedKey) -> bytes::Bytes {
    ManagedIndexKey::Data { scope, kind: key }.to_bytes()
}

/// Classifies malformed or cross-owned persisted text rows consistently.
fn corruption(message: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.into())
}

#[cfg(any(test, feature = "production-coverage"))]
#[path = "../../../tests/production_support/index_lifecycle_text_serving.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::Db;

    use super::*;
    use crate::config::{SecondaryIndexDefinition, TextAnalyzerKind};
    use crate::index_lifecycle::{
        IndexOperationId, IndexRecordV2, IndexRevision, IndexStateTransition, PhysicalGeneration,
        ValidatedDynamicIndexDefinition,
    };

    #[tokio::test]
    async fn production_text_serving_matrix_runs_in_workspace_tests() {
        production_contracts::run().await;
    }

    /// Opens one isolated in-memory SlateDB fixture.
    async fn test_db(name: &str) -> Db {
        Db::builder(name, Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap()
    }

    /// Constructs family-refined authority for one Active text definition.
    fn active_authority(definition: ValidatedTextIndexDefinition) -> ActiveTextServingAuthority {
        let record = IndexRecordV2::building(
            IndexId::initial(),
            ValidatedDynamicIndexDefinition::Text(definition),
            IndexRevision::initial(),
            PhysicalGeneration::Text {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        let active =
            ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record).unwrap();
        ActiveTextServingAuthority::try_from_active(&active).unwrap()
    }

    async fn put_corpus_statistics(
        db: &Db,
        authority: &ActiveTextServingAuthority,
        partition: &work::TextPartition,
        document_count: u64,
        total_token_count: u64,
    ) {
        db.put(
            super::super::statistics::corpus_key(
                authority.scope(),
                authority.index_id(),
                authority.generation(),
                partition,
            ),
            index_values::encode_corpus_statistics(
                &work::TextCorpusStatisticsValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    document_count,
                    total_token_count,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    }

    async fn put_present_statistics_marker(
        db: &Db,
        authority: &ActiveTextServingAuthority,
        partition: &work::TextPartition,
        entity: index_keys::IndexEntity,
    ) {
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextStatisticsEntity(index_keys::TextStatisticsEntityKey {
                    index_id: authority.index_id(),
                    generation: authority.generation(),
                    entity,
                }),
            ),
            index_values::encode_statistics_entity(&work::TextStatisticsEntityValue {
                index_id: authority.index_id(),
                generation: authority.generation(),
                entity_kind: entity.kind,
                entity_id: entity.id,
                contribution: work::TextStatisticsContribution::try_present(
                    partition.clone(),
                    [0x31; 32],
                    1,
                    vec![bytes::Bytes::from_static(b"body")],
                )
                .unwrap(),
            }),
        )
        .await
        .unwrap();
    }

    #[test]
    fn serving_authority_rejects_another_active_family() {
        let record = IndexRecordV2::building(
            IndexId::initial(),
            ValidatedDynamicIndexDefinition::try_from(
                SecondaryIndexDefinition::node_equality("Document", "body").unwrap(),
            )
            .unwrap(),
            IndexRevision::initial(),
            PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
            IndexOperationId::new_v4(),
        )
        .unwrap()
        .transition(IndexStateTransition::Activate)
        .unwrap();
        let active =
            ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record).unwrap();
        assert!(matches!(
            ActiveTextServingAuthority::try_from_active(&active),
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "text serving authority received another Active family"
        ));
    }

    #[tokio::test]
    async fn root_page_and_entity_state_reads_crosscheck_exact_ownership() {
        let db = test_db("text-serving-owned-rows").await;
        let authority = active_authority(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let partition = work::TextPartition::Unpartitioned;
        let root_key = index_keys::TextManifestRootKey {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition: partition.fingerprint(),
        };
        let split = work::SplitRef::try_new(
            work::BlobRef::new([7; 32], 100),
            80,
            20,
            0,
            100,
            work::SplitPruning::Unavailable,
        )
        .unwrap();
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextManifestRoot(root_key),
            ),
            index_values::encode_manifest_root(
                &work::TextManifestRootValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    TextManifestRevision::new(2).unwrap(),
                    1,
                    1,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        put_corpus_statistics(&db, &authority, &partition, 1, 1).await;
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
                    root: root_key,
                    page: 0,
                }),
            ),
            index_values::encode_manifest_page(
                &work::TextManifestPageValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    0,
                    vec![split],
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(42),
        };
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                    root: root_key,
                    entity,
                }),
            ),
            index_values::encode_text_entity_state(&work::TextEntityStateValue {
                index_id: authority.index_id(),
                generation: authority.generation(),
                partition: partition.clone(),
                entity_kind: entity.kind,
                entity_id: entity.id,
                logical_version: TextLogicalVersion::initial(),
                live: true,
            }),
        )
        .await
        .unwrap();
        put_present_statistics_marker(&db, &authority, &partition, entity).await;

        let root = load_active_manifest_root(&db, &authority, &partition)
            .await
            .unwrap()
            .unwrap();
        let roots = load_active_manifest_roots(&db, &authority).await.unwrap();
        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].partition(), &partition);
        assert_eq!(root.page_count(), 1);
        assert_eq!(root.split_count(), 1);
        assert_eq!(
            load_active_manifest_page(&db, &root, 0).await.unwrap(),
            vec![split]
        );
        assert!(load_active_manifest_page(&db, &root, 1).await.is_err());
        let state = load_active_entity_state(&db, &root, 42).await.unwrap();
        assert_eq!(state.logical_version(), 1);
        assert!(state.is_live());
        assert!(load_active_entity_state(&db, &root, 99).await.is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn missing_roots_distinguish_tenant_absence_from_unpartitioned_corruption() {
        let db = test_db("text-serving-missing-roots").await;
        let unpartitioned = active_authority(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        assert!(load_active_manifest_root(
            &db,
            &unpartitioned,
            &work::TextPartition::Unpartitioned,
        )
        .await
        .is_err());
        assert!(load_active_manifest_roots(&db, &unpartitioned)
            .await
            .is_err());

        let partitioned = active_authority(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                Some("tenant_id"),
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let tenant =
            work::TextPartition::try_tenant_value(bytes::Bytes::from_static(b"acme")).unwrap();
        assert!(load_active_manifest_root(&db, &partitioned, &tenant)
            .await
            .unwrap()
            .is_none());
        assert!(load_active_manifest_roots(&db, &partitioned)
            .await
            .unwrap()
            .is_empty());
        assert!(
            load_active_manifest_root(&db, &partitioned, &work::TextPartition::Unpartitioned,)
                .await
                .is_err()
        );
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn empty_root_corpus_states_match_for_point_and_enumeration_serving() {
        let db = test_db("text-serving-empty-root-corpus-states").await;
        let authority = active_authority(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let partition = work::TextPartition::Unpartitioned;
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextManifestRoot(index_keys::TextManifestRootKey {
                    index_id: authority.index_id(),
                    generation: authority.generation(),
                    partition: partition.fingerprint(),
                }),
            ),
            index_values::encode_manifest_root(&work::TextManifestRootValue::empty(
                authority.index_id(),
                authority.generation(),
                partition.clone(),
            )),
        )
        .await
        .unwrap();

        assert!(load_active_manifest_root(&db, &authority, &partition)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            load_active_manifest_roots(&db, &authority)
                .await
                .unwrap()
                .len(),
            1
        );

        put_corpus_statistics(&db, &authority, &partition, 0, 0).await;
        assert!(load_active_manifest_root(&db, &authority, &partition)
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            load_active_manifest_roots(&db, &authority)
                .await
                .unwrap()
                .len(),
            1
        );

        put_corpus_statistics(&db, &authority, &partition, 1, 1).await;
        for error in [
            load_active_manifest_root(&db, &authority, &partition)
                .await
                .expect_err("point serving rejects live corpus totals under an empty root"),
            load_active_manifest_roots(&db, &authority)
                .await
                .expect_err("enumeration rejects live corpus totals under an empty root"),
        ] {
            assert!(matches!(
                error,
                HelixDbError::IndexCatalogCorruption(reason)
                    if reason
                        == "empty Active text manifest retains non-empty corpus statistics"
            ));
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn page_and_state_reads_reject_cross_owned_values() {
        let db = test_db("text-serving-cross-owned-rows").await;
        let authority = active_authority(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let partition = work::TextPartition::Unpartitioned;
        let root_key = index_keys::TextManifestRootKey {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition: partition.fingerprint(),
        };
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextManifestRoot(root_key),
            ),
            index_values::encode_manifest_root(
                &work::TextManifestRootValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    TextManifestRevision::new(2).unwrap(),
                    1,
                    1,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        put_corpus_statistics(&db, &authority, &partition, 1, 1).await;
        let split = work::SplitRef::try_new(
            work::BlobRef::new([9; 32], 100),
            80,
            20,
            0,
            100,
            work::SplitPruning::Unavailable,
        )
        .unwrap();
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
                    root: root_key,
                    page: 0,
                }),
            ),
            index_values::encode_manifest_page(
                &work::TextManifestPageValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    1,
                    vec![split],
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        let entity = index_keys::IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(42),
        };
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                    root: root_key,
                    entity,
                }),
            ),
            index_values::encode_text_entity_state(&work::TextEntityStateValue {
                index_id: authority.index_id(),
                generation: authority.generation(),
                partition: partition.clone(),
                entity_kind: entity.kind,
                entity_id: entity.id,
                logical_version: TextLogicalVersion::new(2).unwrap(),
                live: true,
            }),
        )
        .await
        .unwrap();

        let root = load_active_manifest_root(&db, &authority, &partition)
            .await
            .unwrap()
            .unwrap();
        assert!(load_active_manifest_page(&db, &root, 0).await.is_err());
        assert!(load_active_entity_state(&db, &root, 42).await.is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn serving_reads_reject_missing_and_wrong_typed_rows() {
        let db = test_db("text-serving-missing-and-wrong-typed-rows").await;
        let authority = active_authority(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Document",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let partition = work::TextPartition::Unpartitioned;
        let root_key = index_keys::TextManifestRootKey {
            index_id: authority.index_id(),
            generation: authority.generation(),
            partition: partition.fingerprint(),
        };
        let scoped_root_key = scoped_key(
            authority.scope(),
            index_keys::ScopedKey::TextManifestRoot(root_key),
        );
        let split = work::SplitRef::try_new(
            work::BlobRef::new([19; 32], 100),
            80,
            20,
            0,
            100,
            work::SplitPruning::Unavailable,
        )
        .unwrap();
        db.put(
            scoped_root_key.clone(),
            index_values::encode_manifest_page(
                &work::TextManifestPageValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    0,
                    vec![split],
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            load_active_manifest_root(&db, &authority, &partition).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "text manifest root key contains another typed value kind"
        ));

        db.put(
            scoped_root_key.clone(),
            index_values::encode_manifest_root(
                &work::TextManifestRootValue::try_new(
                    IndexId::new(2).unwrap(),
                    authority.generation(),
                    partition.clone(),
                    TextManifestRevision::initial(),
                    1,
                    1,
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            load_active_manifest_root(&db, &authority, &partition).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "text manifest root key/value ownership mismatch"
        ));

        let root_value = work::TextManifestRootValue::try_new(
            authority.index_id(),
            authority.generation(),
            partition.clone(),
            TextManifestRevision::new(2).unwrap(),
            1,
            1,
        )
        .unwrap();
        let encoded_root = index_values::encode_manifest_root(&root_value);
        db.put(scoped_root_key, encoded_root.clone()).await.unwrap();
        put_corpus_statistics(&db, &authority, &partition, 1, 1).await;
        let root = load_active_manifest_root(&db, &authority, &partition)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            load_active_manifest_page(&db, &root, 0).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "Active text manifest root references a missing page"
        ));

        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextManifestPage(index_keys::TextManifestPageKey {
                    root: root_key,
                    page: 0,
                }),
            ),
            encoded_root.clone(),
        )
        .await
        .unwrap();
        assert!(matches!(
            load_active_manifest_page(&db, &root, 0).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "text manifest page key contains another typed value kind"
        ));

        db.put(
            scoped_key(
                authority.scope(),
                index_keys::ScopedKey::TextEntityState(index_keys::TextEntityStateKey {
                    root: root_key,
                    entity: index_keys::IndexEntity {
                        kind: IndexElementKind::Node,
                        id: IndexEntityId::new(42),
                    },
                }),
            ),
            encoded_root,
        )
        .await
        .unwrap();
        assert!(matches!(
            load_active_entity_state(&db, &root, 42).await,
            Err(HelixDbError::IndexCatalogCorruption(reason))
                if reason == "text entity-state key contains another typed value kind"
        ));

        db.close().await.unwrap();
    }
}
