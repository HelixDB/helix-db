//! Lease-adjacent serving reads for Active V2 text generations.
//!
//! The interpreter owns the request lease and calls these boundaries only
//! inside an admitted physical batch. This module point-loads the exact
//! partition root, streams one bounded manifest page at a time, and resolves
//! candidate live state from generation-qualified kind-`0x0C` rows. Every
//! decoded value is cross-checked against its typed key before it can reach
//! Tantivy or object storage.

use slatedb::DbReadOps;

use crate::encoding::v1::keys::index_v2 as index_keys;
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::values::index_v2 as index_values;
use crate::error::{HelixDbError, Result};
use crate::index_v2::work;
use crate::index_v2::{
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
        index_keys::IndexV2Key::TextManifestRoot(typed_key),
    );
    let Some(value) = reader.get(key).await? else {
        return match partition {
            work::TextPartition::TenantValue(_) => Ok(None),
            work::TextPartition::Unpartitioned => Err(corruption(
                "Active unpartitioned text generation has no manifest root",
            )),
        };
    };
    let index_values::IndexV2WorkValue::TextManifestRoot(root) =
        index_values::decode_work_value(&value)?
    else {
        return Err(corruption(
            "text manifest root key contains another typed value kind",
        ));
    };
    if root.index_id() != authority.index_id()
        || root.generation() != authority.generation()
        || root.partition() != partition
        || typed_key.partition != root.partition().fingerprint()
    {
        return Err(corruption(
            "text manifest root key/value ownership mismatch",
        ));
    }

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
        index_keys::IndexV2Key::TextManifestPage(typed_key),
    );
    let Some(value) = reader.get(key).await? else {
        return Err(corruption(
            "Active text manifest root references a missing page",
        ));
    };
    let index_values::IndexV2WorkValue::TextManifestPage(value) =
        index_values::decode_work_value(&value)?
    else {
        return Err(corruption(
            "text manifest page key contains another typed value kind",
        ));
    };
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
pub(crate) async fn load_active_entity_state(
    reader: &(impl DbReadOps + Sync),
    root: &ValidatedActiveTextManifestRoot,
    entity_id: u64,
) -> Result<ActiveTextEntityState> {
    let entity = index_keys::IndexEntity {
        kind: root.element_kind,
        id: IndexEntityId::new(entity_id),
    };
    let typed_key = index_keys::TextEntityStateKey {
        root: root.key,
        entity,
    };
    let key = scoped_key(
        root.scope,
        index_keys::IndexV2Key::TextEntityState(typed_key),
    );
    let Some(value) = reader.get(key).await? else {
        return Err(corruption(
            "Active V2 text split candidate has no entity state",
        ));
    };
    let index_values::IndexV2WorkValue::TextEntityState(state) =
        index_values::decode_work_value(&value)?
    else {
        return Err(corruption(
            "text entity-state key contains another typed value kind",
        ));
    };
    if state.index_id != root.index_id()
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
    Ok(ActiveTextEntityState {
        logical_version: state.logical_version,
        live: state.live,
    })
}

/// Encodes one typed V2 data key in its exact scope.
fn scoped_key(scope: DataScope, key: index_keys::IndexV2Key) -> bytes::Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexV2(key),
    }
    .to_bytes()
}

/// Classifies malformed or cross-owned persisted text rows consistently.
fn corruption(message: impl Into<String>) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::Db;

    use super::*;
    use crate::config::TextAnalyzerKind;
    use crate::index_v2::{
        IndexOperationId, IndexRecordV2, IndexRevision, IndexStateTransition, PhysicalGeneration,
        ValidatedDynamicIndexDefinition,
    };

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
        let split =
            work::SplitRef::try_new(work::BlobRef::new([7; 32], 100), 80, 20, 0, 100).unwrap();
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::IndexV2Key::TextManifestRoot(root_key),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    TextManifestRevision::initial(),
                    1,
                    1,
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                    root: root_key,
                    page: 0,
                }),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                work::TextManifestPageValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    0,
                    vec![split],
                )
                .unwrap(),
            )),
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
                index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
                    root: root_key,
                    entity,
                }),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextEntityState(
                work::TextEntityStateValue {
                    index_id: authority.index_id(),
                    generation: authority.generation(),
                    partition: partition.clone(),
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                    logical_version: TextLogicalVersion::initial(),
                    live: true,
                },
            )),
        )
        .await
        .unwrap();

        let root = load_active_manifest_root(&db, &authority, &partition)
            .await
            .unwrap()
            .unwrap();
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
        assert!(
            load_active_manifest_root(&db, &partitioned, &work::TextPartition::Unpartitioned,)
                .await
                .is_err()
        );
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
                index_keys::IndexV2Key::TextManifestRoot(root_key),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestRoot(
                work::TextManifestRootValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    TextManifestRevision::initial(),
                    1,
                    1,
                )
                .unwrap(),
            )),
        )
        .await
        .unwrap();
        let split =
            work::SplitRef::try_new(work::BlobRef::new([9; 32], 100), 80, 20, 0, 100).unwrap();
        db.put(
            scoped_key(
                authority.scope(),
                index_keys::IndexV2Key::TextManifestPage(index_keys::TextManifestPageKey {
                    root: root_key,
                    page: 0,
                }),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextManifestPage(
                work::TextManifestPageValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    1,
                    vec![split],
                )
                .unwrap(),
            )),
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
                index_keys::IndexV2Key::TextEntityState(index_keys::TextEntityStateKey {
                    root: root_key,
                    entity,
                }),
            ),
            index_values::encode_work_value(&index_values::IndexV2WorkValue::TextEntityState(
                work::TextEntityStateValue {
                    index_id: authority.index_id(),
                    generation: authority.generation(),
                    partition: partition.clone(),
                    entity_kind: entity.kind,
                    entity_id: entity.id,
                    logical_version: TextLogicalVersion::new(2).unwrap(),
                    live: true,
                },
            )),
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
}
