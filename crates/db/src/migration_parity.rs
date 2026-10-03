#![allow(deprecated)]

//! Feature-gated migration diagnostics.
//!
//! This module is intentionally excluded from the default crate surface. The
//! production migration contract tests use it to observe persisted index state
//! and damage text statistics before and after storage-format migrations.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::object_store::ObjectStore;
use slatedb::DbReadOps;

use crate::encoding::keys::scope::DataScope;
use crate::encoding::property::{property_value::PropertyValue, Property};
use crate::encoding::v1::keys::{DataKeyKind, Key, KeyPrefix, MetadataKey};
use crate::encoding::v1::read_u64;
use crate::encoding::v1::values;
use crate::encoding::v1::values::id_allocation::IdAllocationWatermarkValue;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{GlobalKey, ScopedKey, GLOBAL_SENTINEL};
use crate::encoding::v2::values::{
    decode_corpus_statistics, decode_index_record, decode_metadata_value, decode_operation_record,
    decode_statistics_entity, decode_term_statistics,
};
use crate::{migrations, search, HelixDB, HelixStorage, Result};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ParityValue {
    Null,
    Bool(bool),
    I64(i64),
    DateTime(i64),
    F64Bits(u64),
    F32Bits(u64),
    String(String),
    Bytes(Vec<u8>),
    I64Array(Vec<i64>),
    F64ArrayBits(Vec<u64>),
    F32ArrayBits(Vec<u32>),
    StringArray(Vec<String>),
    Array(Vec<ParityValue>),
    Object(BTreeMap<String, ParityValue>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ParityProperty {
    pub name: String,
    pub value: ParityValue,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ParityEdge {
    pub edge_id: u64,
    pub from: u64,
    pub to: u64,
    pub has_endpoint: bool,
    pub properties: Vec<ParityProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ParityLegacyEdgePair {
    pub from: u64,
    pub to: u64,
    pub properties: Vec<ParityProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ParityPairIndex {
    pub from: u64,
    pub to: u64,
    pub edge_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ParityAdjacency {
    pub node_id: u64,
    pub outgoing: Vec<u64>,
    pub incoming: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityTextHit {
    pub entity_id: u64,
    pub score_bits: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityTextSplit {
    pub sha256: [u8; 32],
    pub size_bytes: u64,
    pub footer_offset: u64,
    pub footer_len: u32,
    pub hotcache_len: u32,
    pub total_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityTextSearch {
    pub identity: String,
    pub analyzer: String,
    pub partition_bytes: Vec<u8>,
    pub index_id: u64,
    pub generation: u64,
    pub physical_index_name: String,
    pub format_version: u32,
    pub generation_id: String,
    pub splits: Vec<MigrationParityTextSplit>,
    pub hits: Vec<MigrationParityTextHit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParitySnapshot {
    pub nodes: BTreeMap<u64, Vec<ParityProperty>>,
    pub current_edges: BTreeMap<u64, ParityEdge>,
    pub legacy_edge_pairs: Vec<ParityLegacyEdgePair>,
    pub pair_indexes: Vec<ParityPairIndex>,
    pub adjacency: BTreeMap<u64, ParityAdjacency>,
    pub migration_jobs: Vec<migrations::MigrationParityJobStatus>,
    pub allocator_watermarks: BTreeMap<String, u64>,
    pub vector_non_metadata_namespace_digests: BTreeMap<u64, String>,
    pub v2: MigrationParityV2State,
    pub raw_counts: BTreeMap<String, u64>,
    pub consistency_findings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityVectorMigrationStats {
    pub adopted_indexes: u64,
    pub rebuilt_indexes: u64,
    pub validated_rows: u64,
    pub validated_bytes: u64,
    pub logical_output_operations: u64,
    pub logical_output_bytes: u64,
    pub reused_physical_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityV2Record {
    pub identity: String,
    pub definition: BTreeMap<String, String>,
    pub index_id: u64,
    pub revision: u64,
    pub state: String,
    pub generation: u64,
    pub physical: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MigrationParityTextCorpusStatistics {
    pub index_id: u64,
    pub generation: u64,
    pub partition_bytes: Vec<u8>,
    pub document_count: u64,
    pub total_token_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MigrationParityTextTermStatistics {
    pub index_id: u64,
    pub generation: u64,
    pub partition_bytes: Vec<u8>,
    pub term: Vec<u8>,
    pub document_frequency: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MigrationParityTextEntityContribution {
    Absent,
    Present {
        partition_bytes: Vec<u8>,
        fingerprint: [u8; 32],
        token_count: u64,
        terms: Vec<Vec<u8>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MigrationParityTextEntityStatistics {
    pub index_id: u64,
    pub generation: u64,
    pub entity_kind: String,
    pub entity_id: u64,
    pub contribution: MigrationParityTextEntityContribution,
}

/// Exact statistics mutation performed by the feature-gated corruption harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationParityTextStatisticsDamage {
    /// Remove the corpus row for one unpartitioned or named tenant partition.
    MissingCorpus { tenant: Option<String> },
    /// Remove the generation-owned accounting marker for one graph entity.
    MissingEntityMarker { entity_id: u64 },
}

fn migration_parity_text_partition(
    definition: &crate::index_lifecycle::ValidatedTextIndexDefinition,
    tenant: Option<String>,
) -> Result<crate::index_lifecycle::work::TextPartition> {
    match (definition.tenant_property(), tenant) {
        (None, None) => Ok(crate::index_lifecycle::work::TextPartition::Unpartitioned),
        (Some(_), Some(tenant)) => crate::index_lifecycle::work::TextPartition::try_tenant_value(
            crate::encoding::v1::property::encode_index_partition_value(&PropertyValue::String(
                tenant,
            )),
        )
        .map_err(|error| crate::error::HelixDbError::Config(error.to_string())),
        (Some(_), None) => Err(crate::error::HelixDbError::Config(
            "partitioned text statistics damage requires a tenant".to_string(),
        )),
        (None, Some(_)) => Err(crate::error::HelixDbError::Config(
            "unpartitioned text statistics damage cannot select a tenant".to_string(),
        )),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityV2State {
    pub storage_version: Option<u16>,
    pub canonical_records: Vec<MigrationParityV2Record>,
    pub operation_statuses: Vec<String>,
    pub scoped_row_counts: BTreeMap<String, u64>,
    pub global_row_counts: BTreeMap<String, u64>,
    pub runtime_active_identities: Vec<String>,
    pub legacy_definition_rows: u64,
    pub pending_operation_pointers: u64,
    pub vector_migration: MigrationParityVectorMigrationStats,
    pub text_corpus_statistics: Vec<MigrationParityTextCorpusStatistics>,
    pub text_term_statistics: Vec<MigrationParityTextTermStatistics>,
    pub text_entity_statistics: Vec<MigrationParityTextEntityStatistics>,
}

impl HelixDB {
    /// Exposes the writer DB only to feature-gated migration diagnostics.
    pub(crate) fn migration_parity_inner_db(&self) -> Result<Arc<slatedb::Db>> {
        match self.storage() {
            HelixStorage::Writer(writer) => Ok(Arc::clone(&writer.db)),
            HelixStorage::Reader(_) => Err(crate::error::HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            }),
        }
    }

    /// Converts a complete current writer store into the exact version-2
    /// fixture consumed by storage-version compatibility tests.
    ///
    /// Normal runtime code must never move a durable version backwards.
    #[cfg(test)]
    pub(crate) async fn migration_parity_make_storage_v2_fixture(&self) -> Result<()> {
        let db = self.migration_parity_inner_db()?;
        crate::migrations::make_legacy_equality_fixture(&db, 2).await
    }

    /// Deletes one exact text-statistics row in an otherwise valid Active generation.
    pub async fn migration_parity_damage_text_statistics(
        &self,
        definition: &crate::config::TextIndexDefinition,
        damage: MigrationParityTextStatisticsDamage,
    ) -> Result<()> {
        let definition =
            crate::index_lifecycle::ValidatedTextIndexDefinition::try_from_runtime(definition)
                .map_err(|error| crate::error::HelixDbError::Config(error.to_string()))?;
        let handles = self.active_index_handles_loaded(DataScope::LegacyUnscoped);
        let mut authority = None;
        for handle in &handles {
            let crate::index_lifecycle::ActiveIndexHandle::Text { .. } = handle else {
                continue;
            };
            let candidate =
                crate::index_lifecycle::text::serving::ActiveTextServingAuthority::try_from_active(
                    handle,
                )?;
            if candidate.definition() == &definition {
                authority = Some(candidate);
                break;
            }
        }
        let Some(authority) = authority else {
            return Err(crate::error::HelixDbError::IndexNotFound(format!(
                "{:?}:{}:{}",
                definition.element_kind(),
                definition.label().as_str(),
                definition.property().as_str(),
            )));
        };
        let key = match damage {
            MigrationParityTextStatisticsDamage::MissingCorpus { tenant } => IndexKey::Data {
                scope: authority.scope(),
                kind: ScopedKey::TextCorpusStatistics(
                    crate::encoding::v2::keys::TextCorpusStatisticsKey {
                        index_id: authority.index_id(),
                        generation: authority.generation(),
                        partition: migration_parity_text_partition(&definition, tenant)?
                            .fingerprint(),
                    },
                ),
            },
            MigrationParityTextStatisticsDamage::MissingEntityMarker { entity_id } => {
                IndexKey::Data {
                    scope: authority.scope(),
                    kind: ScopedKey::TextStatisticsEntity(
                        crate::encoding::v2::keys::TextStatisticsEntityKey {
                            index_id: authority.index_id(),
                            generation: authority.generation(),
                            entity: crate::encoding::v2::keys::IndexEntity {
                                kind: definition.element_kind(),
                                id: crate::index_lifecycle::IndexEntityId::new(entity_id),
                            },
                        },
                    ),
                }
            }
        };
        self.migration_parity_inner_db()?
            .delete(key.to_bytes())
            .await?;
        Ok(())
    }

    /// Execute the same text query against every durable manifest and retain
    /// score bit patterns and referenced-blob metadata for cross-version parity.
    pub async fn migration_parity_text_search(
        &self,
        query: &str,
        k: usize,
    ) -> Result<Vec<MigrationParityTextSearch>> {
        let handles = self.active_index_handles_loaded(DataScope::LegacyUnscoped);
        match self.storage() {
            HelixStorage::Writer(writer) => {
                // One snapshot: automatic publication may commit between reads.
                let snapshot = writer.db().snapshot().await?;
                text_search_from_read(
                    &*snapshot,
                    self.object_store(),
                    self.path(),
                    &handles,
                    MigrationParityTextQuery {
                        definition: None,
                        partition: None,
                        query,
                        k,
                    },
                )
                .await
            }
            HelixStorage::Reader(reader) => {
                text_search_from_read(
                    reader.as_ref(),
                    self.object_store(),
                    self.path(),
                    &handles,
                    MigrationParityTextQuery {
                        definition: None,
                        partition: None,
                        query,
                        k,
                    },
                )
                .await
            }
        }
    }

    /// Executes the text-score observer for one canonical string tenant.
    pub async fn migration_parity_text_search_tenant(
        &self,
        tenant: &str,
        query: &str,
        k: usize,
    ) -> Result<Vec<MigrationParityTextSearch>> {
        let partition = crate::index_lifecycle::work::TextPartition::try_tenant_value(
            crate::encoding::v1::property::encode_index_partition_value(&PropertyValue::String(
                tenant.to_string(),
            )),
        )
        .map_err(|error| crate::error::HelixDbError::Config(error.to_string()))?;
        let handles = self.active_index_handles_loaded(DataScope::LegacyUnscoped);
        match self.storage() {
            HelixStorage::Writer(writer) => {
                // One snapshot: automatic publication may commit between reads.
                let snapshot = writer.db().snapshot().await?;
                text_search_from_read(
                    &*snapshot,
                    self.object_store(),
                    self.path(),
                    &handles,
                    MigrationParityTextQuery {
                        definition: None,
                        partition: Some(&partition),
                        query,
                        k,
                    },
                )
                .await
            }
            HelixStorage::Reader(reader) => {
                text_search_from_read(
                    reader.as_ref(),
                    self.object_store(),
                    self.path(),
                    &handles,
                    MigrationParityTextQuery {
                        definition: None,
                        partition: Some(&partition),
                        query,
                        k,
                    },
                )
                .await
            }
        }
    }

    /// Executes one text query against one exact logical definition.
    pub async fn migration_parity_text_search_definition(
        &self,
        definition: &crate::config::TextIndexDefinition,
        tenant: Option<&str>,
        query: &str,
        k: usize,
    ) -> Result<MigrationParityTextSearch> {
        let validated =
            crate::index_lifecycle::ValidatedTextIndexDefinition::try_from_runtime(definition)
                .map_err(|error| crate::error::HelixDbError::Config(error.to_string()))?;
        let partition = tenant
            .map(|tenant| {
                crate::index_lifecycle::work::TextPartition::try_tenant_value(
                    crate::encoding::v1::property::encode_index_partition_value(
                        &PropertyValue::String(tenant.to_string()),
                    ),
                )
                .map_err(|error| crate::error::HelixDbError::Config(error.to_string()))
            })
            .transpose()?;
        let handles = self.active_index_handles_loaded(DataScope::LegacyUnscoped);
        let searches = match self.storage() {
            HelixStorage::Writer(writer) => {
                // One snapshot: automatic publication may commit between reads.
                let snapshot = writer.db().snapshot().await?;
                text_search_from_read(
                    &*snapshot,
                    self.object_store(),
                    self.path(),
                    &handles,
                    MigrationParityTextQuery {
                        definition: Some(&validated),
                        partition: partition.as_ref(),
                        query,
                        k,
                    },
                )
                .await?
            }
            HelixStorage::Reader(reader) => {
                text_search_from_read(
                    reader.as_ref(),
                    self.object_store(),
                    self.path(),
                    &handles,
                    MigrationParityTextQuery {
                        definition: Some(&validated),
                        partition: partition.as_ref(),
                        query,
                        k,
                    },
                )
                .await?
            }
        };
        let [search] = searches.as_slice() else {
            return Err(crate::error::HelixDbError::IndexCatalogCorruption(format!(
                "expected one Active text generation for {:?}:{}:{}, found {}",
                definition.element_type(),
                definition.label(),
                definition.property(),
                searches.len()
            )));
        };
        Ok(search.clone())
    }

    pub async fn migration_parity_snapshot(&self) -> Result<MigrationParitySnapshot> {
        let scope = DataScope::LegacyUnscoped;
        let mut snapshot = match self.storage() {
            HelixStorage::Writer(writer) => {
                snapshot_from_read(&*writer.db().snapshot().await?, scope).await?
            }
            HelixStorage::Reader(reader) => snapshot_from_read(reader.as_ref(), scope).await?,
        };
        let writer_jobs = match self.storage() {
            HelixStorage::Writer(writer) => {
                migrations::migration_parity_job_statuses(writer.db(), scope).await?
            }
            HelixStorage::Reader(_) => Vec::new(),
        };
        snapshot.migration_jobs = writer_jobs;
        Ok(snapshot)
    }

    /// Read only the canonical V2 catalog and lifecycle state.
    pub async fn migration_parity_v2_state(&self) -> Result<MigrationParityV2State> {
        let scope = DataScope::LegacyUnscoped;
        let mut state = MigrationParityV2State::default();
        match self.storage() {
            HelixStorage::Writer(writer) => {
                scan_v2_state(&*writer.db().snapshot().await?, scope, &mut state).await?
            }
            HelixStorage::Reader(reader) => {
                scan_v2_state(reader.as_ref(), scope, &mut state).await?
            }
        }
        Ok(state)
    }
}

struct MigrationParityTextQuery<'a> {
    definition: Option<&'a crate::index_lifecycle::ValidatedTextIndexDefinition>,
    partition: Option<&'a crate::index_lifecycle::work::TextPartition>,
    query: &'a str,
    k: usize,
}

async fn text_search_from_read(
    read: &(impl DbReadOps + Send + Sync),
    object_store: &Arc<dyn ObjectStore>,
    database: &str,
    handles: &[crate::index_lifecycle::ActiveIndexHandle],
    request: MigrationParityTextQuery<'_>,
) -> Result<Vec<MigrationParityTextSearch>> {
    let mut searches = Vec::new();
    for handle in handles {
        let crate::index_lifecycle::ActiveIndexHandle::Text { .. } = handle else {
            continue;
        };
        let authority =
            crate::index_lifecycle::text::serving::ActiveTextServingAuthority::try_from_active(
                handle,
            )?;
        if request
            .definition
            .is_some_and(|definition| definition != authority.definition())
        {
            continue;
        }
        let partition = match (authority.definition().tenant_property(), request.partition) {
            (None, None) => crate::index_lifecycle::work::TextPartition::Unpartitioned,
            (Some(_), Some(partition)) => partition.clone(),
            (Some(_), None) => {
                return Err(crate::error::HelixDbError::Config(
                    "migration parity text search requires a tenant partition".to_string(),
                ));
            }
            (None, Some(_)) => continue,
        };
        let Some(root) = crate::index_lifecycle::text::serving::load_active_manifest_root(
            read, &authority, &partition,
        )
        .await?
        else {
            if authority.definition().tenant_property().is_none() {
                return Err(crate::error::HelixDbError::IndexCatalogCorruption(
                    "unpartitioned Active text index has no manifest root".to_string(),
                ));
            }
            continue;
        };
        let statistics = match crate::index_lifecycle::text::statistics::load_query_statistics(
            read,
            authority.scope(),
            root.index_id(),
            root.generation(),
            root.partition(),
            authority.definition().analyzer(),
            request.query,
        )
        .await?
        {
            crate::index_lifecycle::text::statistics::LoadedTextQueryStatistics::EmptyQuery => None,
            crate::index_lifecycle::text::statistics::LoadedTextQueryStatistics::EmptyCorpus => {
                if root.split_count() != 0 {
                    return Err(crate::error::HelixDbError::IndexCatalogCorruption(
                        "migration parity text manifest has no corpus statistics".to_string(),
                    ));
                }
                None
            }
            crate::index_lifecycle::text::statistics::LoadedTextQueryStatistics::Ready(
                statistics,
            ) => Some(statistics),
        };
        let mut splits = Vec::new();
        let mut hits_by_entity = BTreeMap::<u64, f32>::new();
        for page in 0..root.page_count() {
            let entries =
                crate::index_lifecycle::text::serving::load_active_manifest_page(read, &root, page)
                    .await?;
            let page_splits = entries
                .into_iter()
                .map(|split| search::text::TextSplitRef {
                    blob: search::text::TextBlobRef {
                        sha256: *split.blob().hash(),
                        size_bytes: split.blob().size(),
                    },
                    footer_offset: split.footer_offset(),
                    footer_len: split.footer_length(),
                    hotcache_len: split.hot_cache_length(),
                    total_size_bytes: split.total_size(),
                })
                .collect::<Vec<_>>();
            let Some(primary) = page_splits.first().cloned() else {
                return Err(crate::error::HelixDbError::IndexCatalogCorruption(
                    "V2 text manifest page is empty".to_string(),
                ));
            };
            splits.extend(page_splits.iter().map(|split| MigrationParityTextSplit {
                sha256: split.blob.sha256,
                size_bytes: split.blob.size_bytes,
                footer_offset: split.footer_offset,
                footer_len: split.footer_len,
                hotcache_len: split.hotcache_len,
                total_size_bytes: split.total_size_bytes,
            }));
            let mut manifest = search::text::TextIndexGenerationManifest::new_split(
                format!(
                    "index-v2-text-{}-{}-page-{page}",
                    root.index_id().get(),
                    root.generation().get(),
                ),
                root.generation().get().to_string(),
                authority.definition().analyzer(),
                authority.definition().positions_enabled(),
                primary,
            );
            manifest.splits = page_splits;
            if let Some(statistics) = &statistics {
                for hit in search::text::search_manifest_with_v2_live_state_scoped_and_scope(
                    read,
                    search::text::TextSearchRuntime::new(object_store, database, None),
                    &root,
                    &manifest,
                    statistics,
                    search::text::TextSearchRequest::new(
                        request.query,
                        request.k,
                        search::text::TextSearchScope::Unrestricted,
                    ),
                )
                .await?
                {
                    hits_by_entity
                        .entry(hit.entity_id)
                        .and_modify(|score| *score = score.max(hit.score))
                        .or_insert(hit.score);
                }
            }
        }
        let mut hits = hits_by_entity
            .into_iter()
            .map(|(entity_id, score)| MigrationParityTextHit {
                entity_id,
                score_bits: score.to_bits(),
            })
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            f32::from_bits(right.score_bits)
                .partial_cmp(&f32::from_bits(left.score_bits))
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.entity_id.cmp(&right.entity_id))
        });
        hits.truncate(request.k);
        searches.push(MigrationParityTextSearch {
            identity: parity_identity(&authority.definition().identity()),
            analyzer: authority.definition().analyzer().as_str().to_string(),
            partition_bytes: root.partition().canonical_bytes().to_vec(),
            index_id: root.index_id().get(),
            generation: root.generation().get(),
            physical_index_name: format!(
                "index-v2-text-{}-{}",
                root.index_id().get(),
                root.generation().get(),
            ),
            format_version: search::text::TEXT_INDEX_MANIFEST_FORMAT_V2,
            generation_id: root.generation().get().to_string(),
            splits,
            hits,
        });
    }
    searches.sort_by(|left, right| left.physical_index_name.cmp(&right.physical_index_name));
    Ok(searches)
}

pub(crate) fn parity_properties(properties: &[Property]) -> Vec<ParityProperty> {
    properties
        .iter()
        .map(|property| ParityProperty {
            name: property.name.clone(),
            value: parity_value(&property.value),
        })
        .collect()
}

pub(crate) fn parity_value(value: &PropertyValue) -> ParityValue {
    match value {
        PropertyValue::Null => ParityValue::Null,
        PropertyValue::Bool(value) => ParityValue::Bool(*value),
        PropertyValue::I64(value) => ParityValue::I64(*value),
        PropertyValue::DateTime(value) => ParityValue::DateTime(*value),
        PropertyValue::F64(value) => ParityValue::F64Bits(value.to_bits()),
        PropertyValue::F32(value) => ParityValue::F32Bits(value.to_bits()),
        PropertyValue::String(value) => ParityValue::String(value.clone()),
        PropertyValue::Bytes(value) => ParityValue::Bytes(value.clone()),
        PropertyValue::I64Array(value) => ParityValue::I64Array(value.clone()),
        PropertyValue::F64Array(value) => {
            ParityValue::F64ArrayBits(value.iter().map(|value| value.to_bits()).collect())
        }
        PropertyValue::F32Array(value) => {
            ParityValue::F32ArrayBits(value.iter().map(|value| value.to_bits()).collect())
        }
        PropertyValue::StringArray(value) => ParityValue::StringArray(value.clone()),
        PropertyValue::Array(value) => ParityValue::Array(value.iter().map(parity_value).collect()),
        PropertyValue::Object(value) => ParityValue::Object(
            value
                .iter()
                .map(|(key, value)| (key.clone(), parity_value(value)))
                .collect(),
        ),
    }
}

async fn snapshot_from_read(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<MigrationParitySnapshot> {
    let mut snapshot = MigrationParitySnapshot {
        nodes: BTreeMap::new(),
        current_edges: BTreeMap::new(),
        legacy_edge_pairs: Vec::new(),
        pair_indexes: Vec::new(),
        adjacency: BTreeMap::new(),
        migration_jobs: Vec::new(),
        allocator_watermarks: BTreeMap::new(),
        vector_non_metadata_namespace_digests: BTreeMap::new(),
        v2: MigrationParityV2State::default(),
        raw_counts: BTreeMap::new(),
        consistency_findings: Vec::new(),
    };

    scan_adjacency(read, scope, &mut snapshot).await?;
    scan_node_properties(read, scope, &mut snapshot).await?;
    scan_edge_properties(read, scope, &mut snapshot).await?;
    scan_edge_endpoints(read, scope, &mut snapshot).await?;
    scan_pair_indexes(read, scope, &mut snapshot).await?;
    scan_property_index_counts(read, scope, &mut snapshot).await?;
    scan_allocator_watermarks(read, scope, &mut snapshot).await?;
    snapshot.vector_non_metadata_namespace_digests =
        scan_vector_non_metadata_digests(read, scope).await?;
    scan_v2_state(read, scope, &mut snapshot.v2).await?;
    record_consistency_findings(&mut snapshot);
    Ok(snapshot)
}

async fn scan_vector_non_metadata_digests(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<BTreeMap<u64, String>> {
    let mut digests = BTreeMap::<u64, Sha256>::new();
    for lane in crate::encoding::v1::keys::vectors::VectorStorageLane::ALL {
        let mut logical_prefix = lane.prefix_key(0).to_bytes();
        logical_prefix.truncate(
            logical_prefix
                .len()
                .checked_sub(core::mem::size_of::<u64>())
                .expect("typed vector lane prefix contains an index ID"),
        );
        let mut rows = read
            .scan_prefix(Key::data_prefix(scope, logical_prefix), ..)
            .await?;
        while let Some(row) = rows.next().await? {
            let Some(logical) = scope.strip_key(&row.key) else {
                return Err(crate::error::HelixDbError::InvariantViolation(
                    "vector parity scan escaped its data scope".to_string(),
                ));
            };
            let key = if lane == crate::encoding::v1::keys::vectors::VectorStorageLane::Core {
                match crate::encoding::v1::keys::vectors::VectorMetadataScanPrefix::new()
                    .parse_row(logical)?
                {
                    None
                    | Some(
                        crate::encoding::v1::keys::vectors::VectorMetadataScanRow::IndexMetadata(_),
                    ) => continue,
                    Some(crate::encoding::v1::keys::vectors::VectorMetadataScanRow::TxnGuard(
                        key,
                    )) => crate::encoding::v1::keys::vectors::VectorKey::TxnGuard(key),
                }
            } else {
                crate::encoding::v1::keys::vectors::VectorKey::parse_from_slice(logical)?
            };
            if matches!(
                key,
                crate::encoding::v1::keys::vectors::VectorKey::SimHashDirectory(_)
            ) {
                continue;
            }
            let digest = digests.entry(key.index_id()).or_default();
            digest.update(
                u64::try_from(row.key.len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            digest.update(&row.key);
            digest.update(
                u64::try_from(row.value.len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            digest.update(&row.value);
        }
    }
    Ok(digests
        .into_iter()
        .map(|(physical_id, digest)| {
            (
                physical_id,
                digest
                    .finalize()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
            )
        })
        .collect())
}

fn parity_identity(identity: &crate::index_lifecycle::IndexIdentity) -> String {
    format!(
        "{:?}:{:?}:{}:{}",
        identity.family(),
        identity.element_kind(),
        identity.label().as_str(),
        identity.property().as_str()
    )
}

fn parity_definition(
    definition: &crate::index_lifecycle::ValidatedDynamicIndexDefinition,
) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    match definition {
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Secondary(definition) => {
            fields.insert("family".to_string(), "secondary".to_string());
            fields.insert(
                "element_kind".to_string(),
                format!("{:?}", definition.element_kind()),
            );
            fields.insert("label".to_string(), definition.label().as_str().to_string());
            fields.insert(
                "property".to_string(),
                definition.property().as_str().to_string(),
            );
            fields.insert("unique".to_string(), definition.unique().to_string());
            fields.insert(
                "direction".to_string(),
                format!("{:?}", definition.direction()),
            );
        }
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Vector(definition) => {
            fields.insert("family".to_string(), "vector".to_string());
            fields.insert(
                "element_kind".to_string(),
                format!("{:?}", definition.element_kind()),
            );
            fields.insert("label".to_string(), definition.label().as_str().to_string());
            fields.insert(
                "property".to_string(),
                definition.property().as_str().to_string(),
            );
            fields.insert(
                "tenant_property".to_string(),
                definition
                    .tenant_property()
                    .map(|property| property.as_str())
                    .unwrap_or_default()
                    .to_string(),
            );
            fields.insert("dimension".to_string(), definition.dimension().to_string());
            fields.insert("metric".to_string(), format!("{:?}", definition.metric()));
            fields.insert("codec".to_string(), format!("{:?}", definition.codec()));
            fields.insert("m".to_string(), definition.m().to_string());
            fields.insert("m0".to_string(), definition.m0().to_string());
            fields.insert(
                "ef_construction".to_string(),
                definition.ef_construction().to_string(),
            );
            fields.insert("ml_bits".to_string(), definition.ml().to_bits().to_string());
            fields.insert(
                "simhash_threshold".to_string(),
                definition.simhash_threshold().to_string(),
            );
            fields.insert(
                "sampling_ratio_bits".to_string(),
                definition.sampling_ratio().to_bits().to_string(),
            );
            fields.insert(
                "adaptive_enabled".to_string(),
                definition.adaptive_enabled().to_string(),
            );
            fields.insert(
                "adaptive_failure_probability_bits".to_string(),
                definition
                    .adaptive_failure_probability()
                    .to_bits()
                    .to_string(),
            );
        }
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Text(definition) => {
            fields.insert("family".to_string(), "text".to_string());
            fields.insert(
                "element_kind".to_string(),
                format!("{:?}", definition.element_kind()),
            );
            fields.insert("label".to_string(), definition.label().as_str().to_string());
            fields.insert(
                "property".to_string(),
                definition.property().as_str().to_string(),
            );
            fields.insert(
                "tenant_property".to_string(),
                definition
                    .tenant_property()
                    .map(|property| property.as_str())
                    .unwrap_or_default()
                    .to_string(),
            );
            fields.insert(
                "analyzer".to_string(),
                definition.analyzer().as_str().to_string(),
            );
            fields.insert(
                "positions_enabled".to_string(),
                definition.positions_enabled().to_string(),
            );
        }
    }
    fields
}

async fn scan_allocator_watermarks(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    snapshot: &mut MigrationParitySnapshot,
) -> Result<()> {
    for (name, metadata) in [
        ("next_node_id", MetadataKey::next_node_id_key()),
        ("next_edge_id", MetadataKey::next_edge_id_key()),
    ] {
        let key = Key::Data {
            scope,
            kind: DataKeyKind::IndexMetadata(metadata),
        }
        .to_bytes();
        let value = read
            .get(key)
            .await?
            .map(|bytes| IdAllocationWatermarkValue::decode(&bytes))
            .transpose()?
            .map(IdAllocationWatermarkValue::exclusive_id)
            .unwrap_or_default();
        snapshot
            .allocator_watermarks
            .insert(name.to_string(), value);
    }
    Ok(())
}

async fn scan_v2_state(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    state: &mut MigrationParityV2State,
) -> Result<()> {
    let mut completed_vector_builds =
        BTreeMap::<(u64, u64), crate::index_lifecycle::OperationCounters>::new();
    let scoped_prefix = IndexKey::data_prefix(scope, Bytes::from(vec![ScopedKey::key_prefix()]));
    let mut scoped = read.scan_prefix(scoped_prefix, ..).await?;
    while let Some(row) = scoped.next().await? {
        let IndexKey::Data { kind: key, .. } = IndexKey::parse_from_slice(scope, &row.key)? else {
            continue;
        };
        let kind = format!("{:?}", key.record_kind());
        increment_count(&mut state.scoped_row_counts, &kind);
        match key {
            ScopedKey::IndexRecord(_) => {
                let record = decode_index_record(&row.value)?;
                let identity = parity_identity(record.identity());
                if record.state().name() == "active" {
                    state.runtime_active_identities.push(identity.clone());
                }
                state.canonical_records.push(MigrationParityV2Record {
                    identity,
                    definition: parity_definition(record.definition()),
                    index_id: record.index_id().get(),
                    revision: record.revision().get(),
                    state: record.state().name().to_string(),
                    generation: record.state().generation().get(),
                    physical: record
                        .state()
                        .physical()
                        .map(|physical| format!("{physical:?}")),
                });
            }
            ScopedKey::Operation(_) => {
                let operation = decode_operation_record(&row.value)?;
                if matches!(
                    operation.execution_state(),
                    crate::index_lifecycle::IndexOperationExecutionState::Completed(
                        crate::index_lifecycle::IndexOperationOutcome::Build(
                            crate::index_lifecycle::BuildOperationOutcome::Succeeded,
                        ),
                    )
                ) && let crate::index_lifecycle::IndexOperationProgress::VectorBuild(
                    crate::index_lifecycle::VectorBuildProgress::Constructing(
                        crate::index_lifecycle::VectorBuildStage::Activate(progress),
                    ),
                ) = operation.progress()
                {
                    completed_vector_builds.insert(
                        (operation.index_id().get(), operation.generation().get()),
                        progress.counters,
                    );
                }
                state.operation_statuses.push(
                    serde_json::to_string(
                        &crate::index_lifecycle::IndexOperationStatus::from_record(&operation),
                    )
                    .map_err(|error| {
                        crate::error::HelixDbError::Config(format!(
                            "failed to serialize V2 operation status: {error}"
                        ))
                    })?,
                );
            }
            ScopedKey::TextCorpusStatistics(key) => {
                let statistics = decode_corpus_statistics(&row.value)?;
                if statistics.index_id != key.index_id
                    || statistics.generation != key.generation
                    || statistics.partition.fingerprint() != key.partition
                {
                    return Err(crate::error::HelixDbError::InvariantViolation(
                        "text corpus-statistics key/value ownership mismatch".to_string(),
                    ));
                }
                state
                    .text_corpus_statistics
                    .push(MigrationParityTextCorpusStatistics {
                        index_id: statistics.index_id.get(),
                        generation: statistics.generation.get(),
                        partition_bytes: statistics.partition.canonical_bytes().to_vec(),
                        document_count: statistics.document_count,
                        total_token_count: statistics.total_token_count,
                    });
            }
            ScopedKey::TextTermStatistics(key) => {
                let statistics = decode_term_statistics(&row.value)?;
                if statistics.index_id != key.corpus.index_id
                    || statistics.generation != key.corpus.generation
                    || statistics.partition.fingerprint() != key.corpus.partition
                    || crate::encoding::v2::keys::TextTermFingerprint::new(
                        Sha256::digest(&statistics.term).into(),
                    ) != key.term
                {
                    return Err(crate::error::HelixDbError::InvariantViolation(
                        "text term-statistics key/value ownership mismatch".to_string(),
                    ));
                }
                state
                    .text_term_statistics
                    .push(MigrationParityTextTermStatistics {
                        index_id: statistics.index_id.get(),
                        generation: statistics.generation.get(),
                        partition_bytes: statistics.partition.canonical_bytes().to_vec(),
                        term: statistics.term.to_vec(),
                        document_frequency: statistics.document_frequency,
                    });
            }
            ScopedKey::TextStatisticsEntity(key) => {
                let statistics = decode_statistics_entity(&row.value)?;
                if statistics.index_id != key.index_id
                    || statistics.generation != key.generation
                    || statistics.entity_kind != key.entity.kind
                    || statistics.entity_id != key.entity.id
                {
                    return Err(crate::error::HelixDbError::InvariantViolation(
                        "text entity-statistics key/value ownership mismatch".to_string(),
                    ));
                }
                let contribution = match statistics.contribution {
                    crate::index_lifecycle::work::TextStatisticsContribution::Absent => {
                        MigrationParityTextEntityContribution::Absent
                    }
                    crate::index_lifecycle::work::TextStatisticsContribution::Present {
                        partition,
                        fingerprint,
                        token_count,
                        terms,
                    } => MigrationParityTextEntityContribution::Present {
                        partition_bytes: partition.canonical_bytes().to_vec(),
                        fingerprint,
                        token_count,
                        terms: terms.into_iter().map(|term| term.to_vec()).collect(),
                    },
                };
                state
                    .text_entity_statistics
                    .push(MigrationParityTextEntityStatistics {
                        index_id: statistics.index_id.get(),
                        generation: statistics.generation.get(),
                        entity_kind: format!("{:?}", statistics.entity_kind),
                        entity_id: statistics.entity_id.get(),
                        contribution,
                    });
            }
            ScopedKey::BuildDelta(_)
            | ScopedKey::AppliedState(_)
            | ScopedKey::IndexOperationQueue(_)
            | ScopedKey::IndexOperationRow(_)
            | ScopedKey::SecondaryEntry(_)
            | ScopedKey::SecondaryEqualityBitmap(_)
            | ScopedKey::TextManifestRoot(_)
            | ScopedKey::TextManifestPage(_)
            | ScopedKey::TextBuildArtifact(_)
            | ScopedKey::TextEntityState(_)
            | ScopedKey::VectorPartitionMapping(_) => {}
        }
    }
    state
        .canonical_records
        .sort_by(|left, right| left.identity.cmp(&right.identity));
    state.runtime_active_identities.sort();
    state.operation_statuses.sort();
    state.text_corpus_statistics.sort();
    state.text_term_statistics.sort();
    state.text_entity_statistics.sort();

    let mut global = read
        .scan_prefix(Bytes::copy_from_slice(&GLOBAL_SENTINEL), ..)
        .await?;
    while let Some(row) = global.next().await? {
        let key = GlobalKey::parse_from_slice(&row.key)?;
        let kind = format!("{:?}", key.kind());
        increment_count(&mut state.global_row_counts, &kind);
        match key {
            GlobalKey::StorageVersion => {
                let crate::index_lifecycle::IndexV2MetadataValue::StorageVersion(version) =
                    decode_metadata_value(&row.value)?
                else {
                    return Err(crate::error::HelixDbError::InvariantViolation(
                        "storage-version key contains another metadata value".to_string(),
                    ));
                };
                state.storage_version = Some(version.get());
            }
            GlobalKey::OperationPointer(_) => {
                state.pending_operation_pointers += 1;
            }
            GlobalKey::LegacyVectorPhysicalReservation(physical_id) => {
                let crate::index_lifecycle::IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    reservation,
                ) = decode_metadata_value(&row.value)?
                else {
                    return Err(crate::error::HelixDbError::InvariantViolation(
                        "vector reservation key contains another metadata value".to_string(),
                    ));
                };
                if let crate::index_lifecycle::LegacyVectorPhysicalReservation::AdoptedActive {
                    index_id,
                    generation,
                } = reservation
                {
                    state.vector_migration.adopted_indexes = state
                        .vector_migration
                        .adopted_indexes
                        .checked_add(1)
                        .ok_or_else(|| {
                            crate::error::HelixDbError::InvariantViolation(
                                "adopted vector parity count overflowed".to_string(),
                            )
                        })?;
                    state
                        .vector_migration
                        .reused_physical_ids
                        .push(physical_id.get());
                    let counters = completed_vector_builds
                        .remove(&(index_id.get(), generation.get()))
                        .ok_or_else(|| {
                            crate::error::HelixDbError::InvariantViolation(
                                "adopted vector has no retained completed build operation"
                                    .to_string(),
                            )
                        })?;
                    state.vector_migration.validated_rows = state
                        .vector_migration
                        .validated_rows
                        .checked_add(counters.entities)
                        .ok_or_else(|| {
                            crate::error::HelixDbError::InvariantViolation(
                                "adopted vector validated row count overflowed".to_string(),
                            )
                        })?;
                    state.vector_migration.validated_bytes = state
                        .vector_migration
                        .validated_bytes
                        .checked_add(counters.input_bytes)
                        .ok_or_else(|| {
                            crate::error::HelixDbError::InvariantViolation(
                                "adopted vector validated byte count overflowed".to_string(),
                            )
                        })?;
                    let metadata_key = Key::Data {
                        scope,
                        kind: DataKeyKind::Vector(
                            crate::encoding::v1::keys::vectors::VectorKey::IndexMetadata(
                                crate::encoding::v1::keys::vectors::VectorIndexMetadataKey::new(
                                    physical_id.get(),
                                ),
                            ),
                        ),
                    }
                    .to_bytes();
                    let metadata_value = read.get(&metadata_key).await?.ok_or_else(|| {
                        crate::error::HelixDbError::InvariantViolation(
                            "adopted vector metadata is absent from its physical namespace"
                                .to_string(),
                        )
                    })?;
                    state.vector_migration.logical_output_operations = state
                        .vector_migration
                        .logical_output_operations
                        .checked_add(1)
                        .ok_or_else(|| {
                            crate::error::HelixDbError::InvariantViolation(
                                "adopted vector logical output operation count overflowed"
                                    .to_string(),
                            )
                        })?;
                    let encoded_bytes =
                        u64::try_from(metadata_key.len().saturating_add(metadata_value.len()))
                            .map_err(|_| {
                                crate::error::HelixDbError::InvariantViolation(
                                    "adopted vector metadata length does not fit u64".to_string(),
                                )
                            })?;
                    state.vector_migration.logical_output_bytes = state
                        .vector_migration
                        .logical_output_bytes
                        .checked_add(encoded_bytes)
                        .ok_or_else(|| {
                            crate::error::HelixDbError::InvariantViolation(
                                "adopted vector logical output byte count overflowed".to_string(),
                            )
                        })?;
                }
            }
            GlobalKey::TextCompactionPointer(_)
            | GlobalKey::LogicalIndexIdWatermark
            | GlobalKey::VectorPhysicalIdWatermark => {}
        }
    }
    state.vector_migration.rebuilt_indexes =
        u64::try_from(completed_vector_builds.len()).map_err(|_| {
            crate::error::HelixDbError::InvariantViolation(
                "rebuilt vector parity count does not fit u64".to_string(),
            )
        })?;
    state.vector_migration.reused_physical_ids.sort_unstable();

    let legacy_prefix = crate::encoding::v2::legacy::index_catalog::catalog_scan_prefix(scope);
    let mut legacy = read.scan_prefix(legacy_prefix, ..).await?;
    while legacy.next().await?.is_some() {
        state.legacy_definition_rows += 1;
    }
    Ok(())
}

async fn scan_adjacency(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    snapshot: &mut MigrationParitySnapshot,
) -> Result<()> {
    let prefix = Key::data_prefix(
        scope,
        Bytes::copy_from_slice(KeyPrefix::Adjacency.as_slice()),
    );
    let mut iter = read.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        increment_count(&mut snapshot.raw_counts, "adjacency_rows");
        let Key::Data {
            kind: DataKeyKind::Adjacency(key),
            ..
        } = Key::parse_from_slice(scope, &kv.key)?
        else {
            continue;
        };
        let edges = values::edges::decode_edges(&kv.value)?;
        snapshot.adjacency.insert(
            key.node_id(),
            ParityAdjacency {
                node_id: key.node_id(),
                outgoing: edges.iter_out().collect(),
                incoming: edges.iter_in().collect(),
            },
        );
    }
    Ok(())
}

async fn scan_node_properties(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    snapshot: &mut MigrationParitySnapshot,
) -> Result<()> {
    let prefix = Key::data_prefix(
        scope,
        Bytes::copy_from_slice(KeyPrefix::NodeProperty.as_slice()),
    );
    let mut iter = read.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        increment_count(&mut snapshot.raw_counts, "node_property_rows");
        let Key::Data {
            kind: DataKeyKind::NodeProperty(key),
            ..
        } = Key::parse_from_slice(scope, &kv.key)?
        else {
            continue;
        };
        let properties = crate::encoding::property::decode_properties(&kv.value)?;
        snapshot
            .nodes
            .insert(key.node_id(), parity_properties(&properties));
    }
    Ok(())
}

async fn scan_edge_properties(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    snapshot: &mut MigrationParitySnapshot,
) -> Result<()> {
    let prefix = Key::data_prefix(
        scope,
        Bytes::copy_from_slice(KeyPrefix::EdgePropertyPair.as_slice()),
    );
    let mut iter = read.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        let Key::Data { kind, .. } = Key::parse_from_slice(scope, &kv.key)? else {
            continue;
        };
        match kind {
            DataKeyKind::EdgePropertyById(key) => {
                increment_count(&mut snapshot.raw_counts, "edge_property_by_id_rows");
                let properties = crate::encoding::property::decode_properties(&kv.value)?;
                snapshot
                    .current_edges
                    .entry(key.edge_id())
                    .or_insert(ParityEdge {
                        edge_id: key.edge_id(),
                        from: 0,
                        to: 0,
                        has_endpoint: false,
                        properties: parity_properties(&properties),
                    });
            }
            DataKeyKind::EdgePropertyPair(key) => {
                increment_count(&mut snapshot.raw_counts, "legacy_edge_pair_rows");
                let properties = crate::encoding::property::decode_properties(&kv.value)?;
                snapshot.legacy_edge_pairs.push(ParityLegacyEdgePair {
                    from: key.from(),
                    to: key.to(),
                    properties: parity_properties(&properties),
                });
            }
            DataKeyKind::Adjacency(_)
            | DataKeyKind::NodeProperty(_)
            | DataKeyKind::PropertyIndex(_)
            | DataKeyKind::EdgeEndpoints(_)
            | DataKeyKind::EdgePairIndex(_)
            | DataKeyKind::Vector(_)
            | DataKeyKind::IndexMetadata(_) => {}
        }
    }
    snapshot.legacy_edge_pairs.sort();
    Ok(())
}

async fn scan_edge_endpoints(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    snapshot: &mut MigrationParitySnapshot,
) -> Result<()> {
    let prefix = Key::data_prefix(
        scope,
        Bytes::copy_from_slice(KeyPrefix::EdgeEndpoints.as_slice()),
    );
    let mut iter = read.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        increment_count(&mut snapshot.raw_counts, "edge_endpoint_rows");
        let Key::Data {
            kind: DataKeyKind::EdgeEndpoints(key),
            ..
        } = Key::parse_from_slice(scope, &kv.key)?
        else {
            continue;
        };
        let (from, to) = decode_endpoints(&kv.value)?;
        let edge = snapshot
            .current_edges
            .entry(key.edge_id())
            .or_insert(ParityEdge {
                edge_id: key.edge_id(),
                from,
                to,
                has_endpoint: true,
                properties: Vec::new(),
            });
        edge.from = from;
        edge.to = to;
        edge.has_endpoint = true;
    }
    Ok(())
}

async fn scan_pair_indexes(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    snapshot: &mut MigrationParitySnapshot,
) -> Result<()> {
    let prefix = Key::data_prefix(
        scope,
        Bytes::copy_from_slice(KeyPrefix::EdgePairIndex.as_slice()),
    );
    let mut iter = read.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        increment_count(&mut snapshot.raw_counts, "edge_pair_index_rows");
        let Key::Data {
            kind: DataKeyKind::EdgePairIndex(key),
            ..
        } = Key::parse_from_slice(scope, &kv.key)?
        else {
            continue;
        };
        let bitmap = decode_roaring_treemap(&kv.value).map_err(|err| {
            crate::error::HelixDbError::Config(format!(
                "pair-index key {:?} has an invalid bitmap: {err}",
                kv.key
            ))
        })?;
        snapshot.pair_indexes.push(ParityPairIndex {
            from: key.from(),
            to: key.to(),
            edge_ids: bitmap.iter().collect(),
        });
    }
    snapshot.pair_indexes.sort();
    Ok(())
}

async fn scan_property_index_counts(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    snapshot: &mut MigrationParitySnapshot,
) -> Result<()> {
    let prefix = Key::data_prefix(
        scope,
        Bytes::copy_from_slice(KeyPrefix::PropertyIndex.as_slice()),
    );
    let mut iter = read.scan_prefix(prefix, ..).await?;
    while let Some(_kv) = iter.next().await? {
        increment_count(&mut snapshot.raw_counts, "property_index_rows");
    }
    Ok(())
}

fn record_consistency_findings(snapshot: &mut MigrationParitySnapshot) {
    for edge in snapshot.current_edges.values() {
        if !edge.has_endpoint {
            snapshot.consistency_findings.push(format!(
                "edge {} has properties but no endpoint row",
                edge.edge_id
            ));
            continue;
        }
        let Some(outgoing) = snapshot.adjacency.get(&edge.from) else {
            snapshot.consistency_findings.push(format!(
                "edge {} missing adjacency row for source {}",
                edge.edge_id, edge.from
            ));
            continue;
        };
        if !outgoing.outgoing.contains(&edge.to) {
            snapshot.consistency_findings.push(format!(
                "edge {} missing outgoing adjacency {} -> {}",
                edge.edge_id, edge.from, edge.to
            ));
        }
        let Some(incoming) = snapshot.adjacency.get(&edge.to) else {
            snapshot.consistency_findings.push(format!(
                "edge {} missing adjacency row for target {}",
                edge.edge_id, edge.to
            ));
            continue;
        };
        if !incoming.incoming.contains(&edge.from) {
            snapshot.consistency_findings.push(format!(
                "edge {} missing incoming adjacency {} -> {}",
                edge.edge_id, edge.from, edge.to
            ));
        }
    }

    for pair in &snapshot.pair_indexes {
        let mut seen = BTreeSet::new();
        for edge_id in &pair.edge_ids {
            if !seen.insert(*edge_id) {
                snapshot.consistency_findings.push(format!(
                    "pair index {} -> {} contains duplicate edge {}",
                    pair.from, pair.to, edge_id
                ));
            }
            match snapshot.current_edges.get(edge_id) {
                Some(edge) if edge.from == pair.from && edge.to == pair.to => {}
                Some(edge) => snapshot.consistency_findings.push(format!(
                    "pair index {} -> {} points at edge {} with endpoints {} -> {}",
                    pair.from, pair.to, edge_id, edge.from, edge.to
                )),
                None => snapshot.consistency_findings.push(format!(
                    "pair index {} -> {} points at missing edge {}",
                    pair.from, pair.to, edge_id
                )),
            }
        }
    }

    snapshot.consistency_findings.sort();
}

fn decode_endpoints(data: &[u8]) -> Result<(u64, u64)> {
    Ok((
        read_u64(data, 0)?,
        read_u64(data, core::mem::size_of::<u64>())?,
    ))
}

fn decode_roaring_treemap(data: &[u8]) -> Result<RoaringTreemap> {
    RoaringTreemap::deserialize_from(Cursor::new(data)).map_err(|err| {
        crate::error::HelixDbError::Config(format!("failed to decode parity bitmap: {err}"))
    })
}

fn increment_count(counts: &mut BTreeMap<String, u64>, name: &str) {
    counts
        .entry(name.to_string())
        .and_modify(|count| *count = count.saturating_add(1))
        .or_insert(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn parity_state_reports_current_storage_without_a_write_mode() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "migration-parity-default-membership".to_string(),
        })
        .await
        .unwrap();
        let mut state = MigrationParityV2State::default();
        scan_v2_state(
            db.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &mut state,
        )
        .await
        .unwrap();
        assert_eq!(state.storage_version, Some(4));
        assert!(serde_json::to_value(&state)
            .unwrap()
            .get("membership_delta_write_mode")
            .is_none());
        db.close().await.unwrap();
    }
}
