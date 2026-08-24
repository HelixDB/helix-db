//! Feature-gated migration parity diagnostics.
//!
//! This module is intentionally excluded from the default crate surface. It is
//! used by the local cross-repo migration harness to compare logical graph
//! facts before and after storage-format migrations.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use helix_ast::batch::{read_batch, write_batch};
use helix_ast::expr::Predicate;
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::query::QueryRequest;
use helix_ast::traversal::g;
use helix_ast::value::{PropertyInput, PropertyValue as AstPropertyValue};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::config::ScanOptions;
use slatedb::object_store::ObjectStore;
use slatedb::DbReadOps;

use crate::encoding::keys::scope::DataScope;
use crate::encoding::property::{property_value::PropertyValue, Property};
use crate::encoding::v2::keys::{DataKeyKind, DataKey as Key, KeyPrefix, MetadataKey};
use crate::encoding::v2::keys::codec::read_u64;
use crate::encoding::v2::values::adjacency;
use crate::encoding::v2::values::id_allocation::IdAllocationWatermarkValue;
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::keys::{GlobalKey, ScopedKey, SecondaryEntryKey, GLOBAL_SENTINEL};
use crate::encoding::v2::values::{
    decode_corpus_statistics, decode_index_record, decode_metadata_value, decode_operation_record,
    decode_secondary_entry, decode_statistics_entity, decode_term_statistics,
    encode_corpus_statistics, SecondaryEqualityBitmapValue,
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
pub struct MigrationParityCompactionStatus {
    pub id: String,
    pub status: String,
    pub spec: String,
    pub bytes_processed: u64,
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
pub struct MigrationParityVectorHit {
    pub node_id: u64,
    pub distance_bits: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationParityVectorMetadata {
    pub index_name: String,
    pub property_name: String,
    pub dimension: usize,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub ml: f32,
    pub simhash_threshold: usize,
    pub sampling_ratio: f32,
    pub adaptive_enabled: bool,
    pub adaptive_failure_prob: f32,
    pub entry_point: Option<u64>,
    pub max_layer: u16,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityCrudState {
    pub left_node_id: u64,
    pub right_node_id: u64,
    pub parallel_edge_ids: Vec<u64>,
    pub self_edge_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityQueryCorpus {
    pub exact_node_ids: Vec<u64>,
    pub outgoing_node_ids: Vec<u64>,
    pub incoming_node_ids: Vec<u64>,
    pub both_node_ids: Vec<u64>,
    pub outgoing_edge_ids: Vec<u64>,
    pub equality_node_ids: Vec<u64>,
    pub updated_equality_node_ids: Vec<u64>,
    pub range_node_ids: Vec<u64>,
    pub equality_edge_ids: Vec<u64>,
    pub updated_equality_edge_ids: Vec<u64>,
    pub range_edge_ids: Vec<u64>,
    pub text_initial_node_ids: Vec<u64>,
    pub text_updated_node_ids: Vec<u64>,
    pub vector_updated_node_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityCrudEvidence {
    pub state: MigrationParityCrudState,
    pub after_create: MigrationParityQueryCorpus,
    pub after_update: MigrationParityQueryCorpus,
    pub after_delete: MigrationParityQueryCorpus,
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
pub enum MigrationParitySecondaryValue {
    Equality { digest: [u8; 8], canonical: Vec<u8> },
    Range(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MigrationParitySecondaryMembership {
    pub index_id: u64,
    pub generation: u64,
    pub lane: u8,
    pub value: MigrationParitySecondaryValue,
    pub entity_id: u64,
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
    /// Replace one corpus row with typed totals for a fail-closed regression.
    ReplaceCorpus {
        tenant: Option<String>,
        document_count: u64,
        total_token_count: u64,
    },
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
            crate::encoding::v2::values::property::encode_index_partition_value(&PropertyValue::String(
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

/// Compact index-only evidence for the cross-version migration harness.
///
/// Graph parity is proven separately by the harness's bounded external-sort
/// oracle. Keeping graph rows out of this value makes scale reports and peak
/// memory independent of the migrated graph size.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationParityIndexState {
    pub v2: MigrationParityV2State,
    pub vector_non_metadata_namespace_digests: BTreeMap<u64, String>,
}

/// Exact legacy equality storage version emitted by the Kind fixture gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationParityLegacyStorageVersion {
    V2,
    V3,
}

impl MigrationParityLegacyStorageVersion {
    const fn get(self) -> u16 {
        match self {
            Self::V2 => 2,
            Self::V3 => 3,
        }
    }
}

/// Stable index-outbox crash boundaries exercised by the cross-version harness.
pub fn migration_parity_index_outbox_failpoints() -> [&'static str; 16] {
    crate::index_lifecycle::failpoints::IndexOutboxFailpoint::ALL
        .map(crate::index_lifecycle::failpoints::IndexOutboxFailpoint::as_str)
}

impl HelixDB {
    /// Exposes the writer DB only to the feature-gated cross-version harness.
    pub fn migration_parity_inner_db(&self) -> Result<Arc<slatedb::Db>> {
        match self.storage() {
            HelixStorage::Writer(writer) => Ok(Arc::clone(&writer.db)),
            HelixStorage::Reader(_) => Err(crate::error::HelixDbError::WriterModeRequired {
                actual: self.mode().as_str(),
            }),
        }
    }

    /// Converts a complete current writer store into the exact version-2
    /// fixture consumed by cross-repository migration tests.
    ///
    /// The feature-gated parity harness is the only supported caller. Normal
    /// runtime code must never move a durable version backwards.
    pub async fn migration_parity_make_storage_v2_fixture(&self) -> Result<()> {
        self.migration_parity_make_legacy_equality_fixture(MigrationParityLegacyStorageVersion::V2)
            .await
    }

    /// Converts current equality bitmaps into a complete V2 or V3 fixture.
    ///
    /// The caller must stop query admission for the duration. This method is
    /// absent unless the explicit migration-parity feature is enabled.
    pub async fn migration_parity_make_legacy_equality_fixture(
        &self,
        version: MigrationParityLegacyStorageVersion,
    ) -> Result<()> {
        let db = self.migration_parity_inner_db()?;
        crate::index_lifecycle::make_legacy_equality_fixture(&db, version.get()).await
    }

    /// Mutates one exact text-statistics row in an otherwise valid Active generation.
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
        let (key, replacement) = match damage {
            MigrationParityTextStatisticsDamage::MissingCorpus { tenant } => {
                let partition = migration_parity_text_partition(&definition, tenant)?;
                (
                    IndexKey::Data {
                        scope: authority.scope(),
                        kind: ScopedKey::TextCorpusStatistics(
                            crate::encoding::v2::keys::TextCorpusStatisticsKey {
                                index_id: authority.index_id(),
                                generation: authority.generation(),
                                partition: partition.fingerprint(),
                            },
                        ),
                    }
                    .to_bytes(),
                    None,
                )
            }
            MigrationParityTextStatisticsDamage::ReplaceCorpus {
                tenant,
                document_count,
                total_token_count,
            } => {
                let partition = migration_parity_text_partition(&definition, tenant)?;
                let statistics = crate::index_lifecycle::work::TextCorpusStatisticsValue::try_new(
                    authority.index_id(),
                    authority.generation(),
                    partition.clone(),
                    document_count,
                    total_token_count,
                )
                .map_err(|error| crate::error::HelixDbError::Config(error.to_string()))?;
                (
                    IndexKey::Data {
                        scope: authority.scope(),
                        kind: ScopedKey::TextCorpusStatistics(
                            crate::encoding::v2::keys::TextCorpusStatisticsKey {
                                index_id: authority.index_id(),
                                generation: authority.generation(),
                                partition: partition.fingerprint(),
                            },
                        ),
                    }
                    .to_bytes(),
                    Some(encode_corpus_statistics(&statistics)),
                )
            }
            MigrationParityTextStatisticsDamage::MissingEntityMarker { entity_id } => (
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
                .to_bytes(),
                None,
            ),
        };
        let db = self.migration_parity_inner_db()?;
        match replacement {
            Some(value) => db.put(key, value).await?,
            None => db.delete(key).await?,
        };
        Ok(())
    }

    /// Run real planner/interpreter writes against a migrated database and
    /// verify all affected query paths before the caller performs a cold reopen.
    pub async fn migration_parity_run_crud_rehearsal(&self) -> Result<MigrationParityCrudEvidence> {
        let create = write_batch()
            .var_as(
                "left",
                g().add_n(
                    "User",
                    vec![
                        ("tier", PropertyInput::from("migration-crud")),
                        ("rank", PropertyInput::from(9_001_i64)),
                        ("bio", PropertyInput::from("crudmigrationtoken left")),
                        (
                            "embedding",
                            PropertyInput::from(AstPropertyValue::F32Array(vec![100.0, 0.0, 0.0])),
                        ),
                    ],
                ),
            )
            .var_as("left_id", g().n(NodeRef::var("left")).id())
            .var_as(
                "right",
                g().add_n(
                    "User",
                    vec![
                        ("tier", PropertyInput::from("migration-crud")),
                        ("rank", PropertyInput::from(9_002_i64)),
                        ("bio", PropertyInput::from("crudmigrationtoken right")),
                        (
                            "embedding",
                            PropertyInput::from(AstPropertyValue::F32Array(vec![101.0, 0.0, 0.0])),
                        ),
                    ],
                ),
            )
            .var_as("right_id", g().n(NodeRef::var("right")).id())
            .var_as(
                "parallel_a",
                g().n(NodeRef::var("left")).add_e(
                    "FOLLOWS",
                    NodeRef::var("right"),
                    vec![
                        ("kind", PropertyInput::from("migration-crud-parallel-a")),
                        ("since", PropertyInput::from(9_001_i64)),
                    ],
                ),
            )
            .var_as(
                "parallel_b",
                g().n(NodeRef::var("left")).add_e(
                    "FOLLOWS",
                    NodeRef::var("right"),
                    vec![
                        ("kind", PropertyInput::from("migration-crud-parallel-b")),
                        ("since", PropertyInput::from(9_002_i64)),
                    ],
                ),
            )
            .var_as(
                "self_loop",
                g().n(NodeRef::var("left")).add_e(
                    "FOLLOWS",
                    NodeRef::var("left"),
                    vec![
                        ("kind", PropertyInput::from("migration-crud-self")),
                        ("since", PropertyInput::from(9_003_i64)),
                    ],
                ),
            )
            .var_as(
                "edge_ids",
                g().n(NodeRef::var("left")).out_e(Some("FOLLOWS")).id(),
            )
            .returning(["left_id", "right_id", "edge_ids"]);
        let created = self.query(QueryRequest::write(create)).await?;
        let left_ids = query_corpus_ids(&created, "left_id")?;
        let right_ids = query_corpus_ids(&created, "right_id")?;
        let edge_ids = query_corpus_ids(&created, "edge_ids")?;
        let [left_node_id] = left_ids.as_slice() else {
            return Err(crate::error::HelixDbError::InvariantViolation(format!(
                "CRUD rehearsal created {} left nodes instead of one",
                left_ids.len()
            )));
        };
        let [right_node_id] = right_ids.as_slice() else {
            return Err(crate::error::HelixDbError::InvariantViolation(format!(
                "CRUD rehearsal created {} right nodes instead of one",
                right_ids.len()
            )));
        };
        let [parallel_a, parallel_b, self_edge_id] = edge_ids.as_slice() else {
            return Err(crate::error::HelixDbError::InvariantViolation(format!(
                "CRUD rehearsal created {} outgoing edges instead of three",
                edge_ids.len()
            )));
        };
        let state = MigrationParityCrudState {
            left_node_id: *left_node_id,
            right_node_id: *right_node_id,
            parallel_edge_ids: vec![*parallel_a, *parallel_b],
            self_edge_id: *self_edge_id,
        };
        let after_create = self.migration_parity_crud_query_corpus(&state).await?;

        let update = write_batch()
            .var_as(
                "left_tier",
                g().n(NodeRef::id(state.left_node_id))
                    .set_property("tier", PropertyInput::from("migration-crud-updated")),
            )
            .var_as(
                "left_rank",
                g().n(NodeRef::id(state.left_node_id))
                    .set_property("rank", PropertyInput::from(9_101_i64)),
            )
            .var_as(
                "left_bio",
                g().n(NodeRef::id(state.left_node_id))
                    .set_property("bio", PropertyInput::from("updated exclusive token")),
            )
            .var_as(
                "left_embedding",
                g().n(NodeRef::id(state.left_node_id)).set_property(
                    "embedding",
                    PropertyInput::from(AstPropertyValue::F32Array(vec![200.0, 0.0, 0.0])),
                ),
            )
            .var_as(
                "self_kind",
                g().e(EdgeRef::id(state.self_edge_id))
                    .set_property("kind", PropertyInput::from("migration-crud-updated")),
            )
            .var_as(
                "self_since",
                g().e(EdgeRef::id(state.self_edge_id))
                    .set_property("since", PropertyInput::from(9_101_i64)),
            );
        self.query(QueryRequest::write(update)).await?;
        let after_update = self.migration_parity_crud_query_corpus(&state).await?;

        let delete = write_batch()
            .var_as(
                "drop_parallel_a",
                g().drop_edge_by_id(EdgeRef::id(state.parallel_edge_ids[0])),
            )
            .var_as("drop_right", g().n(NodeRef::id(state.right_node_id)).drop());
        self.query(QueryRequest::write(delete)).await?;
        let after_delete = self.migration_parity_crud_query_corpus(&state).await?;
        validate_crud_rehearsal(&state, &after_create, &after_update, &after_delete)?;

        Ok(MigrationParityCrudEvidence {
            state,
            after_create,
            after_update,
            after_delete,
        })
    }

    /// Execute the normalized CRUD query corpus without mutating data. This is
    /// used both during the rehearsal and after a cold database reopen.
    pub async fn migration_parity_crud_query_corpus(
        &self,
        state: &MigrationParityCrudState,
    ) -> Result<MigrationParityQueryCorpus> {
        let read = read_batch()
            .var_as(
                "exact_node_ids",
                g().n(NodeRef::ids([state.left_node_id, state.right_node_id]))
                    .id(),
            )
            .var_as(
                "outgoing_node_ids",
                g().n(NodeRef::id(state.left_node_id))
                    .out(Some("FOLLOWS"))
                    .id(),
            )
            .var_as(
                "incoming_node_ids",
                g().n(NodeRef::id(state.right_node_id))
                    .in_(Some("FOLLOWS"))
                    .id(),
            )
            .var_as(
                "both_node_ids",
                g().n(NodeRef::id(state.left_node_id))
                    .both(Some("FOLLOWS"))
                    .id(),
            )
            .var_as(
                "outgoing_edge_ids",
                g().n(NodeRef::id(state.left_node_id))
                    .out_e(Some("FOLLOWS"))
                    .id(),
            )
            .var_as(
                "equality_node_ids",
                g().n_with_label_where("User", Predicate::eq("tier", "migration-crud"))
                    .id(),
            )
            .var_as(
                "updated_equality_node_ids",
                g().n_with_label_where("User", Predicate::eq("tier", "migration-crud-updated"))
                    .id(),
            )
            .var_as(
                "range_node_ids",
                g().n_with_label_where("User", Predicate::between("rank", 9_000_i64, 9_200_i64))
                    .id(),
            )
            .var_as(
                "equality_edge_ids",
                g().e_with_label_where(
                    "FOLLOWS",
                    Predicate::or(vec![
                        Predicate::eq("kind", "migration-crud-parallel-a"),
                        Predicate::eq("kind", "migration-crud-parallel-b"),
                        Predicate::eq("kind", "migration-crud-self"),
                    ]),
                )
                .id(),
            )
            .var_as(
                "updated_equality_edge_ids",
                g().e_with_label_where("FOLLOWS", Predicate::eq("kind", "migration-crud-updated"))
                    .id(),
            )
            .var_as(
                "range_edge_ids",
                g().e_with_label_where(
                    "FOLLOWS",
                    Predicate::between("since", 9_000_i64, 9_200_i64),
                )
                .id(),
            )
            .var_as(
                "text_initial_node_ids",
                g().text_search_nodes("User", "bio", "crudmigrationtoken", 8, None)
                    .id(),
            )
            .var_as(
                "text_updated_node_ids",
                g().text_search_nodes("User", "bio", "updated exclusive", 8, None)
                    .id(),
            )
            .var_as(
                "vector_updated_node_ids",
                g().vector_search_nodes("User", "embedding", vec![200.0, 0.0, 0.0], 2, None)
                    .id(),
            )
            .returning([
                "exact_node_ids",
                "outgoing_node_ids",
                "incoming_node_ids",
                "both_node_ids",
                "outgoing_edge_ids",
                "equality_node_ids",
                "updated_equality_node_ids",
                "range_node_ids",
                "equality_edge_ids",
                "updated_equality_edge_ids",
                "range_edge_ids",
                "text_initial_node_ids",
                "text_updated_node_ids",
                "vector_updated_node_ids",
            ]);
        let response = self.query(QueryRequest::read(read)).await?;
        Ok(MigrationParityQueryCorpus {
            exact_node_ids: query_corpus_ids(&response, "exact_node_ids")?,
            outgoing_node_ids: query_corpus_ids(&response, "outgoing_node_ids")?,
            incoming_node_ids: query_corpus_ids(&response, "incoming_node_ids")?,
            both_node_ids: query_corpus_ids(&response, "both_node_ids")?,
            outgoing_edge_ids: query_corpus_ids(&response, "outgoing_edge_ids")?,
            equality_node_ids: query_corpus_ids(&response, "equality_node_ids")?,
            updated_equality_node_ids: query_corpus_ids(&response, "updated_equality_node_ids")?,
            range_node_ids: query_corpus_ids(&response, "range_node_ids")?,
            equality_edge_ids: query_corpus_ids(&response, "equality_edge_ids")?,
            updated_equality_edge_ids: query_corpus_ids(&response, "updated_equality_edge_ids")?,
            range_edge_ids: query_corpus_ids(&response, "range_edge_ids")?,
            text_initial_node_ids: query_corpus_ids(&response, "text_initial_node_ids")?,
            text_updated_node_ids: query_corpus_ids(&response, "text_updated_node_ids")?,
            vector_updated_node_ids: query_corpus_ids(&response, "vector_updated_node_ids")?,
        })
    }

    /// Execute a deterministic Euclidean top-k query against one migrated
    /// vector index while retaining exact distance bit patterns.
    pub async fn migration_parity_vector_search(
        &self,
        index_name: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<MigrationParityVectorHit>> {
        let index = self.migration_parity_vector_index(index_name)?;
        let parameters = search::vector::SearchParams::new(k).map_err(|error| {
            crate::error::HelixDbError::Config(format!(
                "invalid migration parity vector search parameters: {error}"
            ))
        })?;
        let hits = match self.storage() {
            HelixStorage::Writer(writer) => index.search(writer.db(), query, &parameters).await?,
            HelixStorage::Reader(reader) => {
                index.search(reader.as_ref(), query, &parameters).await?
            }
        };
        Ok(hits
            .into_iter()
            .map(|hit| MigrationParityVectorHit {
                node_id: hit.entity_id(),
                distance_bits: hit.score().get().to_bits(),
            })
            .collect())
    }

    /// Read the generation-qualified physical metadata used by vector serving.
    pub async fn migration_parity_vector_metadata(
        &self,
        index_name: &str,
    ) -> Result<MigrationParityVectorMetadata> {
        let index = self.migration_parity_vector_index(index_name)?;
        let metadata = match self.storage() {
            HelixStorage::Writer(writer) => index.get_metadata(writer.db()).await?,
            HelixStorage::Reader(reader) => index.get_metadata(reader.as_ref()).await?,
        }
        .ok_or_else(|| crate::error::HelixDbError::IndexNotFound(index_name.to_string()))?;
        Ok(MigrationParityVectorMetadata {
            index_name: metadata.config.index_name,
            property_name: metadata.config.property_name,
            dimension: metadata.config.dimension,
            m: metadata.config.m,
            m0: metadata.config.m0,
            ef_construction: metadata.config.ef_construction,
            ml: metadata.config.ml,
            simhash_threshold: metadata.config.simhash_threshold,
            sampling_ratio: metadata.config.sampling_ratio,
            adaptive_enabled: metadata.config.adaptive_enabled,
            adaptive_failure_prob: metadata.config.adaptive_failure_prob,
            entry_point: metadata.entry_point,
            max_layer: metadata.max_layer,
            count: metadata.count,
        })
    }

    fn migration_parity_vector_index(
        &self,
        index_name: &str,
    ) -> Result<search::vector::VectorIndex<search::vector::distance::Euclidean>> {
        let handles = self.active_index_handles_loaded(DataScope::LegacyUnscoped);
        let active = handles
            .iter()
            .find(|handle| {
                let crate::index_lifecycle::ActiveIndexHandle::Vector { definition, .. } = handle
                else {
                    return false;
                };
                let definition = definition.to_runtime();
                search::vector_index_name(
                    definition.element_type(),
                    definition.label(),
                    definition.property(),
                ) == index_name
            })
            .ok_or_else(|| crate::error::HelixDbError::IndexNotFound(index_name.to_string()))?;
        let crate::index_lifecycle::ActiveIndexHandle::Vector { layout, .. } = active else {
            unreachable!("filtered active vector handle changed family")
        };
        let physical_index_id = layout.physical_index_id().ok_or_else(|| {
            crate::error::HelixDbError::Config(
                "migration parity vector search requires an unpartitioned definition".to_string(),
            )
        })?;
        let generation = search::vector::ValidatedVectorGenerationHandle::try_from_active::<
            search::vector::distance::Euclidean,
        >(active, physical_index_id)
        .map_err(|error| crate::error::HelixDbError::Config(error.to_string()))?;
        Ok(search::vector::VectorIndex::<
            search::vector::distance::Euclidean,
        >::from_generation(&generation))
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
                text_search_from_read(
                    writer.db(),
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
            crate::encoding::v2::values::property::encode_index_partition_value(&PropertyValue::String(
                tenant.to_string(),
            )),
        )
        .map_err(|error| crate::error::HelixDbError::Config(error.to_string()))?;
        let handles = self.active_index_handles_loaded(DataScope::LegacyUnscoped);
        match self.storage() {
            HelixStorage::Writer(writer) => {
                text_search_from_read(
                    writer.db(),
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
                    crate::encoding::v2::values::property::encode_index_partition_value(
                        &PropertyValue::String(tenant.to_string()),
                    ),
                )
                .map_err(|error| crate::error::HelixDbError::Config(error.to_string()))
            })
            .transpose()?;
        let handles = self.active_index_handles_loaded(DataScope::LegacyUnscoped);
        let searches = match self.storage() {
            HelixStorage::Writer(writer) => {
                text_search_from_read(
                    writer.db(),
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
            HelixStorage::Writer(writer) => snapshot_from_read(writer.db(), scope).await?,
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
            HelixStorage::Writer(writer) => scan_v2_state(writer.db(), scope, &mut state).await?,
            HelixStorage::Reader(reader) => {
                scan_v2_state(reader.as_ref(), scope, &mut state).await?
            }
        }
        Ok(state)
    }

    /// Read only compact V2 lifecycle and vector-physical evidence.
    pub async fn migration_parity_index_state(&self) -> Result<MigrationParityIndexState> {
        let scope = DataScope::LegacyUnscoped;
        let mut state = MigrationParityIndexState::default();
        match self.storage() {
            HelixStorage::Writer(writer) => {
                scan_v2_state(writer.db(), scope, &mut state.v2).await?;
                state.vector_non_metadata_namespace_digests =
                    scan_vector_non_metadata_digests(writer.db(), scope).await?;
            }
            HelixStorage::Reader(reader) => {
                scan_v2_state(reader.as_ref(), scope, &mut state.v2).await?;
                state.vector_non_metadata_namespace_digests =
                    scan_vector_non_metadata_digests(reader.as_ref(), scope).await?;
            }
        }
        Ok(state)
    }

    /// Read only the typed durable migration lifecycles without materializing graph rows.
    pub async fn migration_parity_job_statuses(
        &self,
    ) -> Result<Vec<migrations::MigrationParityJobStatus>> {
        let scope = DataScope::LegacyUnscoped;
        match self.storage() {
            HelixStorage::Writer(writer) => {
                migrations::migration_parity_job_statuses(writer.db(), scope).await
            }
            HelixStorage::Reader(_) => Ok(Vec::new()),
        }
    }

    /// Return whether the migrated parity index is planner-active.
    pub fn migration_parity_definition_migration_active(&self) -> bool {
        let key = helix_planner::catalog::ScopedPropertyKey::try_new("User", "tier")
            .expect("parity fixture names are non-empty");
        self.index_catalog_snapshot().node_eq.contains_key(&key)
    }

    /// Read the pinned SlateDB compactor lifecycle for release evidence.
    pub async fn migration_parity_compaction_statuses(
        &self,
    ) -> Result<Vec<MigrationParityCompactionStatus>> {
        let admin = slatedb::admin::AdminBuilder::new(
            self.path(),
            std::sync::Arc::clone(self.object_store()),
        )
        .build();
        let mut latest = BTreeMap::new();
        for version in admin.list_compactions(..).await? {
            for compaction in version.recent_compactions() {
                latest.insert(
                    compaction.id(),
                    MigrationParityCompactionStatus {
                        id: compaction.id().to_string(),
                        status: format!("{:?}", compaction.status()).to_lowercase(),
                        spec: format!("{:?}", compaction.spec()),
                        bytes_processed: compaction.bytes_processed(),
                    },
                );
            }
        }
        Ok(latest.into_values().collect())
    }
}

/// Encodes source metadata into the target's exact deployed vector wire DTO.
pub fn migration_parity_encode_vector_metadata(metadata: MigrationParityVectorMetadata) -> Vec<u8> {
    let metadata = search::vector::VectorIndexMetadata {
        config: search::vector::VectorIndexConfig {
            index_name: metadata.index_name,
            property_name: metadata.property_name,
            dimension: metadata.dimension,
            m: metadata.m,
            m0: metadata.m0,
            ef_construction: metadata.ef_construction,
            ml: metadata.ml,
            simhash_threshold: metadata.simhash_threshold,
            sampling_ratio: metadata.sampling_ratio,
            adaptive_enabled: metadata.adaptive_enabled,
            adaptive_failure_prob: metadata.adaptive_failure_prob,
        },
        entry_point: metadata.entry_point,
        max_layer: metadata.max_layer,
        count: metadata.count,
    };
    search::vector::encode_metadata(&metadata).to_vec()
}

/// Decodes and canonically re-encodes one target vector metadata value.
pub fn migration_parity_normalize_vector_metadata(value: &[u8]) -> Result<(String, Vec<u8>)> {
    let metadata = search::vector::decode_metadata(value).map_err(|error| {
        crate::error::HelixDbError::Config(format!(
            "target vector metadata did not decode: {error}"
        ))
    })?;
    let index_name = metadata.config.index_name.clone();
    Ok((
        index_name,
        search::vector::encode_metadata(&metadata).to_vec(),
    ))
}

/// Builds an empty current-format vector metadata row for passthrough coverage.
pub fn migration_parity_empty_vector_metadata(
    index_name: &str,
    property_name: &str,
    dimension: usize,
) -> Vec<u8> {
    let runtime = crate::config::VectorIndexDefinition::new_node(
        "MigrationParity",
        property_name,
        dimension,
        search::vector::VectorDistanceMetric::Euclidean,
    )
    .expect("parity vector metadata definition is valid");
    let definition =
        crate::index_lifecycle::ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
            .expect("parity vector metadata validates for V2");
    let metadata = search::vector::VectorIndexMetadata::new(
        search::vector::VectorIndexConfig::from_v2_definition(&definition, index_name),
    );
    search::vector::encode_metadata(&metadata).to_vec()
}

fn query_corpus_ids(response: &serde_json::Value, name: &str) -> Result<Vec<u64>> {
    let Some(values) = response.get(name).and_then(serde_json::Value::as_array) else {
        return Err(crate::error::HelixDbError::InvariantViolation(format!(
            "CRUD query corpus result `{name}` is not an array"
        )));
    };
    let mut ids = values
        .iter()
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                crate::error::HelixDbError::InvariantViolation(format!(
                    "CRUD query corpus result `{name}` contains a non-u64 value"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ids.sort_unstable();
    Ok(ids)
}

fn validate_crud_rehearsal(
    state: &MigrationParityCrudState,
    after_create: &MigrationParityQueryCorpus,
    after_update: &MigrationParityQueryCorpus,
    after_delete: &MigrationParityQueryCorpus,
) -> Result<()> {
    let mut nodes = vec![state.left_node_id, state.right_node_id];
    nodes.sort_unstable();
    let mut all_edges = state.parallel_edge_ids.clone();
    all_edges.push(state.self_edge_id);
    all_edges.sort_unstable();
    let self_and_right = nodes.clone();

    let create_expected = MigrationParityQueryCorpus {
        exact_node_ids: nodes.clone(),
        outgoing_node_ids: self_and_right.clone(),
        incoming_node_ids: vec![state.left_node_id],
        both_node_ids: self_and_right.clone(),
        outgoing_edge_ids: all_edges.clone(),
        equality_node_ids: nodes.clone(),
        updated_equality_node_ids: Vec::new(),
        range_node_ids: nodes.clone(),
        equality_edge_ids: all_edges.clone(),
        updated_equality_edge_ids: Vec::new(),
        range_edge_ids: all_edges.clone(),
        text_initial_node_ids: nodes.clone(),
        text_updated_node_ids: Vec::new(),
        vector_updated_node_ids: nodes.clone(),
    };
    if after_create != &create_expected {
        return Err(crate::error::HelixDbError::InvariantViolation(format!(
            "CRUD query corpus after create differs: expected {create_expected:?}, got {after_create:?}"
        )));
    }

    let mut parallel_edges = state.parallel_edge_ids.clone();
    parallel_edges.sort_unstable();
    let update_expected = MigrationParityQueryCorpus {
        exact_node_ids: nodes,
        outgoing_node_ids: self_and_right.clone(),
        incoming_node_ids: vec![state.left_node_id],
        both_node_ids: self_and_right,
        outgoing_edge_ids: all_edges.clone(),
        equality_node_ids: vec![state.right_node_id],
        updated_equality_node_ids: vec![state.left_node_id],
        range_node_ids: vec![state.left_node_id, state.right_node_id],
        equality_edge_ids: parallel_edges,
        updated_equality_edge_ids: vec![state.self_edge_id],
        range_edge_ids: all_edges,
        text_initial_node_ids: vec![state.right_node_id],
        text_updated_node_ids: vec![state.left_node_id],
        vector_updated_node_ids: vec![state.left_node_id, state.right_node_id],
    };
    if after_update != &update_expected {
        return Err(crate::error::HelixDbError::InvariantViolation(format!(
            "CRUD query corpus after update differs: expected {update_expected:?}, got {after_update:?}"
        )));
    }

    let delete_expected_without_vector = MigrationParityQueryCorpus {
        exact_node_ids: vec![state.left_node_id],
        outgoing_node_ids: vec![state.left_node_id],
        incoming_node_ids: Vec::new(),
        both_node_ids: vec![state.left_node_id],
        outgoing_edge_ids: vec![state.self_edge_id],
        equality_node_ids: Vec::new(),
        updated_equality_node_ids: vec![state.left_node_id],
        range_node_ids: vec![state.left_node_id],
        equality_edge_ids: Vec::new(),
        updated_equality_edge_ids: vec![state.self_edge_id],
        range_edge_ids: vec![state.self_edge_id],
        text_initial_node_ids: Vec::new(),
        text_updated_node_ids: vec![state.left_node_id],
        vector_updated_node_ids: after_delete.vector_updated_node_ids.clone(),
    };
    if after_delete != &delete_expected_without_vector
        || !after_delete
            .vector_updated_node_ids
            .contains(&state.left_node_id)
        || after_delete
            .vector_updated_node_ids
            .contains(&state.right_node_id)
    {
        return Err(crate::error::HelixDbError::InvariantViolation(format!(
            "CRUD query corpus after delete differs: expected non-vector fields {delete_expected_without_vector:?} with the updated node present and deleted node absent from vector results, got {after_delete:?}"
        )));
    }
    Ok(())
}

/// Return an exact persisted legacy catalog row consumed by automatic V2 migration.
pub fn migration_parity_secondary_catalog_row() -> Result<(Bytes, Bytes)> {
    let definition = crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
        crate::config::SecondaryIndexDefinition::node_equality("User", "rank")?,
    )?;
    migrations::migration_parity_legacy_catalog_row(&definition, false)
}

/// Decode a materialized adjacency value into exact outgoing and incoming sets.
pub fn decode_parity_adjacency(bytes: &[u8]) -> Result<(Vec<u64>, Vec<u64>)> {
    let edges = adjacency::decode_edges(bytes)?;
    Ok((edges.iter_out().collect(), edges.iter_in().collect()))
}

/// Decode one materialized bitmap index value into its exact sorted membership.
pub fn decode_parity_bitmap(bytes: &[u8]) -> Result<Vec<u64>> {
    Ok(decode_roaring_treemap(bytes)?.iter().collect())
}

/// Return the pre-envelope physical identity for a typed tenant data key.
///
/// Migration parity compares logical ownership across storage versions. V4
/// prepends the tenant sentinel, so a migrated `[0xFD][tenant][logical]` key
/// must compare with its source `[tenant][logical]` key. Untyped `0xFD` keys
/// are returned unchanged instead of being guessed to be tenant data.
pub fn migration_parity_legacy_tenant_key_identity(key: &[u8]) -> Cow<'_, [u8]> {
    let Some((tenant, logical)) = DataScope::strip_tenant_envelope(key) else {
        return Cow::Borrowed(key);
    };
    if DataKeyKind::parse_from_slice(logical).is_err()
        && ScopedKey::parse_from_slice(logical).is_err()
    {
        return Cow::Borrowed(key);
    }

    let mut identity = Vec::with_capacity(core::mem::size_of::<u128>() + logical.len());
    identity.extend_from_slice(&tenant.as_u128().to_be_bytes());
    identity.extend_from_slice(logical);
    Cow::Owned(identity)
}

/// Construct bounded high-throughput options for independent parity scans.
pub fn migration_parity_scan_options(
    read_ahead_bytes: usize,
    maximum_fetch_tasks: usize,
) -> ScanOptions {
    ScanOptions::default()
        .with_read_ahead_bytes(read_ahead_bytes)
        .with_cache_blocks(false)
        .with_max_fetch_tasks(maximum_fetch_tasks.max(1))
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

pub fn parity_properties(properties: &[Property]) -> Vec<ParityProperty> {
    properties
        .iter()
        .map(|property| ParityProperty {
            name: property.name.clone(),
            value: parity_value(&property.value),
        })
        .collect()
}

/// Decode one persisted property vector into its order-preserving parity form.
///
/// This is intentionally a decoder owned by the target implementation. The
/// cross-version verifier has a separate source decoder and compares their
/// canonical outputs, including duplicate names and floating-point bit
/// patterns.
pub fn decode_parity_properties(bytes: &[u8]) -> Result<Vec<ParityProperty>> {
    let properties = crate::encoding::property::decode_properties(bytes)?;
    Ok(parity_properties(&properties))
}

pub fn parity_value(value: &PropertyValue) -> ParityValue {
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

fn property_value_from_parity(value: &ParityValue) -> PropertyValue {
    match value {
        ParityValue::Null => PropertyValue::Null,
        ParityValue::Bool(value) => PropertyValue::Bool(*value),
        ParityValue::I64(value) => PropertyValue::I64(*value),
        ParityValue::DateTime(value) => PropertyValue::DateTime(*value),
        ParityValue::F64Bits(bits) => PropertyValue::F64(f64::from_bits(*bits)),
        ParityValue::F32Bits(bits) => PropertyValue::F32(f64::from_bits(*bits)),
        ParityValue::String(value) => PropertyValue::String(value.clone()),
        ParityValue::Bytes(value) => PropertyValue::Bytes(value.clone()),
        ParityValue::I64Array(value) => PropertyValue::I64Array(value.clone()),
        ParityValue::F64ArrayBits(value) => {
            PropertyValue::F64Array(value.iter().copied().map(f64::from_bits).collect())
        }
        ParityValue::F32ArrayBits(value) => {
            PropertyValue::F32Array(value.iter().copied().map(f32::from_bits).collect())
        }
        ParityValue::StringArray(value) => PropertyValue::StringArray(value.clone()),
        ParityValue::Array(value) => {
            PropertyValue::Array(value.iter().map(property_value_from_parity).collect())
        }
        ParityValue::Object(value) => PropertyValue::Object(
            value
                .iter()
                .map(|(key, value)| (key.clone(), property_value_from_parity(value)))
                .collect(),
        ),
    }
}

/// Exact V1 graph keys generated by the production encoders. This adapter is
/// available only to the feature-gated cross-repo migration harness.
pub fn migration_parity_graph_hash_contract(
    property: &str,
    value: &str,
    label: &str,
    source: u64,
    target: u64,
    edge_id: u64,
) -> BTreeMap<String, Vec<u8>> {
    search::migration_parity_graph_hash_contract(property, value, label, source, target, edge_id)
}

/// Exact text/vector names and typed tenant hash produced by production code.
pub fn migration_parity_index_name_hash_contract(
    label: &str,
    property: &str,
    tenant_property: &str,
    tenant_value: &ParityValue,
) -> BTreeMap<String, String> {
    let tenant_value = property_value_from_parity(tenant_value);
    let mut rows = BTreeMap::new();
    for (name, vector_kind, text_kind) in [
        (
            "node",
            crate::config::VectorElementType::Node,
            crate::config::TextElementType::Node,
        ),
        (
            "edge",
            crate::config::VectorElementType::Edge,
            crate::config::TextElementType::Edge,
        ),
    ] {
        rows.insert(
            format!("vector_{name}"),
            search::vector_index_name(vector_kind, label, property),
        );
        rows.insert(
            format!("text_{name}"),
            search::text_index_name(text_kind, label, property),
        );
        rows.insert(
            format!("vector_tenant_{name}"),
            search::vector_tenant_index_name(
                vector_kind,
                label,
                property,
                tenant_property,
                &tenant_value,
            ),
        );
        rows.insert(
            format!("text_tenant_{name}"),
            search::text_tenant_index_name(
                text_kind,
                label,
                property,
                tenant_property,
                &tenant_value,
            ),
        );
    }
    rows.insert(
        "index_component_label".to_string(),
        format!(
            "{:016x}",
            search::migration_parity_hash_index_component(label)
        ),
    );
    rows.insert(
        "index_component_property".to_string(),
        format!(
            "{:016x}",
            search::migration_parity_hash_index_component(property)
        ),
    );
    rows.insert(
        "tenant_value_hash".to_string(),
        format!(
            "{:016x}",
            search::migration_parity_hash_property_value_component(&tenant_value)
        ),
    );
    rows
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
    for lane in crate::encoding::v2::keys::indexes::vector::VectorStorageLane::ALL {
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
            let key = if lane == crate::encoding::v2::keys::indexes::vector::VectorStorageLane::Core {
                match crate::encoding::v2::keys::indexes::vector::VectorMetadataScanPrefix::new()
                    .parse_row(logical)?
                {
                    None
                    | Some(
                        crate::encoding::v2::keys::indexes::vector::VectorMetadataScanRow::IndexMetadata(_),
                    ) => continue,
                    Some(crate::encoding::v2::keys::indexes::vector::VectorMetadataScanRow::TxnGuard(
                        key,
                    )) => crate::encoding::v2::keys::indexes::vector::VectorKey::TxnGuard(key),
                }
            } else {
                crate::encoding::v2::keys::indexes::vector::VectorKey::parse_from_slice(logical)?
            };
            if matches!(
                key,
                crate::encoding::v2::keys::indexes::vector::VectorKey::SimHashDirectory(_)
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
                            crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
                                crate::encoding::v2::keys::indexes::vector::VectorIndexMetadataKey::new(
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

/// Decode one scoped V2 secondary row into generation-qualified logical memberships.
pub fn decode_migration_parity_secondary_memberships(
    key: &[u8],
    value: &[u8],
) -> Result<Vec<MigrationParitySecondaryMembership>> {
    let IndexKey::Data { kind, .. } = IndexKey::parse_from_slice(DataScope::LegacyUnscoped, key)?
    else {
        return Ok(Vec::new());
    };

    match kind {
        ScopedKey::SecondaryEntry(key) => {
            let entry = decode_secondary_entry(key.lane(), value)?;
            if entry.index_id != key.index_id()
                || entry.generation != key.generation()
                || entry.lane != key.lane()
                || key
                    .entity_id()
                    .is_some_and(|entity_id| entity_id != entry.entity_id)
            {
                return Err(crate::error::HelixDbError::Query(
                    "V2 secondary key/value ownership mismatch".to_string(),
                ));
            }
            let canonical_value = match &key {
                SecondaryEntryKey::Equality(key) => MigrationParitySecondaryValue::Equality {
                    digest: *key.value.digest(),
                    canonical: key.value.canonical().to_vec(),
                },
                SecondaryEntryKey::Range(key) => {
                    MigrationParitySecondaryValue::Range(key.value.encoded().to_vec())
                }
            };
            Ok(vec![MigrationParitySecondaryMembership {
                index_id: key.index_id().get(),
                generation: key.generation().get(),
                lane: key.lane().as_u8(),
                value: canonical_value,
                entity_id: entry.entity_id.get(),
            }])
        }
        ScopedKey::SecondaryEqualityBitmap(key) => {
            let lane = match key.element_kind {
                crate::index_lifecycle::IndexElementKind::Node => 0x01,
                crate::index_lifecycle::IndexElementKind::Edge => 0x05,
            };
            let canonical_value = MigrationParitySecondaryValue::Equality {
                digest: *key.value.digest(),
                canonical: key.value.canonical().to_vec(),
            };
            Ok(SecondaryEqualityBitmapValue::decode(value)?
                .into_ids()
                .iter()
                .map(|entity_id| MigrationParitySecondaryMembership {
                    index_id: key.index_id.get(),
                    generation: key.generation.get(),
                    lane,
                    value: canonical_value.clone(),
                    entity_id,
                })
                .collect())
        }
        ScopedKey::IndexRecord(_)
        | ScopedKey::Operation(_)
        | ScopedKey::BuildDelta(_)
        | ScopedKey::AppliedState(_)
        | ScopedKey::TextManifestRoot(_)
        | ScopedKey::TextManifestPage(_)
        | ScopedKey::TextBuildArtifact(_)
        | ScopedKey::TextEntityState(_)
        | ScopedKey::VectorPartitionMapping(_)
        | ScopedKey::TextCorpusStatistics(_)
        | ScopedKey::TextTermStatistics(_)
        | ScopedKey::TextStatisticsEntity(_) => Ok(Vec::new()),
    }
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
        let edges = adjacency::decode_edges(&kv.value)?;
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
    use crate::encoding::v2::keys::NodePropertyKey;
    use crate::encoding::v2::values::property::equality_index_value::{
        project_equality_value, EqualityValueProjection,
    };
    use crate::encoding::v2::keys::{
        CanonicalSecondaryValue, SecondaryEntryKey, SecondaryEntryLane, SecondaryEqualityBitmapKey,
    };
    use crate::encoding::v2::values::encode_secondary_entry;
    use crate::index_lifecycle::work::SecondaryEntryValue;
    use crate::index_lifecycle::{
        IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexOperationId,
    };

    #[test]
    fn tenant_envelope_parity_identity_is_typed_and_legacy_shaped() {
        let scope = DataScope::Tenant(crate::encoding::keys::scope::TenantId::from_u128(
            0x7777_7777_7777_7777_7777_7777_7777_7777,
        ));
        let key = Key::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(8)),
        }
        .to_bytes();
        let identity = migration_parity_legacy_tenant_key_identity(&key);

        assert_eq!(identity.as_ref(), &key[core::mem::size_of::<u8>()..]);
        let identity = identity.into_owned();
        assert_eq!(
            migration_parity_legacy_tenant_key_identity(&identity).as_ref(),
            identity.as_slice()
        );

        let managed = IndexKey::Data {
            scope,
            kind: ScopedKey::operation(IndexOperationId::from_bytes([0x11; 16]).unwrap()),
        }
        .to_bytes();
        assert_eq!(
            migration_parity_legacy_tenant_key_identity(&managed).as_ref(),
            &managed[core::mem::size_of::<u8>()..]
        );

        let mut untyped = vec![crate::encoding::keys::scope::TENANT_KEY_PREFIX];
        untyped.extend_from_slice(&[0x77; core::mem::size_of::<u128>()]);
        untyped.extend_from_slice(b"not-a-typed-logical-key");
        assert!(matches!(
            migration_parity_legacy_tenant_key_identity(&untyped),
            Cow::Borrowed(bytes) if bytes == untyped.as_slice()
        ));
    }

    #[test]
    fn v3_rows_and_v4_bitmap_decode_to_identical_memberships() {
        let index_id = IndexId::new(7).unwrap();
        let generation = IndexGenerationId::new(11).unwrap();
        let EqualityValueProjection::Indexed(canonical) =
            project_equality_value(&PropertyValue::String("shared".to_string()))
        else {
            panic!("shared string is indexable");
        };

        let v3_memberships = [3_u64, 9]
            .into_iter()
            .flat_map(|entity_id| {
                let entity_id = IndexEntityId::new(entity_id);
                let key = IndexKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: ScopedKey::SecondaryEntry(
                        SecondaryEntryKey::try_new(
                            index_id,
                            generation,
                            SecondaryEntryLane::NodeEquality,
                            CanonicalSecondaryValue::Equality(canonical.clone()),
                            Some(entity_id),
                        )
                        .unwrap(),
                    ),
                }
                .to_bytes();
                let value = encode_secondary_entry(&SecondaryEntryValue {
                    index_id,
                    generation,
                    lane: SecondaryEntryLane::NodeEquality,
                    entity_id,
                });
                decode_migration_parity_secondary_memberships(&key, &value).unwrap()
            })
            .collect::<Vec<_>>();

        let bitmap_key = IndexKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: ScopedKey::SecondaryEqualityBitmap(
                SecondaryEqualityBitmapKey::try_new(
                    index_id,
                    generation,
                    IndexElementKind::Node,
                    canonical,
                )
                .unwrap(),
            ),
        }
        .to_bytes();
        let bitmap_value =
            SecondaryEqualityBitmapValue::new(RoaringTreemap::from_iter([3_u64, 9])).encode();
        let v4_memberships =
            decode_migration_parity_secondary_memberships(&bitmap_key, &bitmap_value).unwrap();

        assert_eq!(v3_memberships, v4_memberships);
    }
}
