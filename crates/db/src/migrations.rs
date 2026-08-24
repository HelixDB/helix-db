#![allow(deprecated)]

//! Durable KV migration jobs.
//!
//! Migration progress is stored in the same scoped data-metadata namespace as
//! runtime index metadata. Blocking startup migrations are stepped to
//! completion before a writer handle is returned; background migrations use the
//! same job state and can resume after restart.

mod vector_properties;
mod vector_retirement;
#[cfg(feature = "production-scale")]
mod vector_scale;
mod vector_simhash_directory;

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
use std::sync::Mutex;
use std::time::Instant;

use bytes::Bytes;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::config;
use crate::encoding::keys::scope::DataScope;
use crate::encoding::property::{decode_properties, Property};
use crate::encoding::v1::keys::vectors::VectorKey;
use crate::encoding::v1::keys::{
    AdjacencyKey, DataKeyKind, EdgeEndpointsKey, EdgePairIndexKey, EdgePropertyByIdKey, Key,
    KeyPrefix, MetadataKey,
};
use crate::encoding::v2::keys::ManagedIndexKey as IndexKey;
use crate::encoding::v2::legacy::index_catalog::{
    self as legacy_index_catalog, LegacyDefinitionRow, LegacyDynamicIndexCatalogEntry,
    LegacyDynamicIndexDefinition, LegacyDynamicIndexKey, LegacyRangeDirection,
    LegacySecondaryElementType, LegacySecondaryIndexDefinition, LegacySecondaryKind,
    LegacyTextAnalyzer, LegacyTextElementType, LegacyTextIndexDefinition, LegacyVectorElementType,
    LegacyVectorIndexDefinition, LegacyVectorMetric,
};
use crate::encoding::{EdgeId, NodeId};
use crate::error::{HelixDbError, Result};
use crate::search;
use crate::HelixWriter;

const MIGRATION_JOB_PREFIX: &[u8] = b"kv_migration_job:";
const GRAPH_FORMAT_V1_READY: &[u8] = b"kv_migration_ready:graph_format_v1";
const INDEX_V2_MIGRATION_READY: &[u8] = b"kv_migration_ready:index_v2_catalog_v1";
const TENANT_KEY_ENVELOPE_READY: &[u8] = b"kv_migration_ready:tenant_key_envelope_v1";
const INDEX_STORAGE_V4_CLEANUP_READY: &[u8] = b"kv_migration_ready:index_storage_v4_cleanup";
const STORAGE_SCHEMA_VERSION: u64 = 1;
const STORAGE_SCHEMA_COMPLETE: &[u8] = b"storage_schema_complete:v1";
const LEGACY_DYNAMIC_INDEX_CATALOG_METADATA: [&[u8]; 3] = [
    b"dynamic_index_catalog_blob",
    b"dynamic_index_catalog_token",
    b"dynamic_index_manifest_ack_token",
];

fn legacy_vector_metric_into_runtime(
    metric: LegacyVectorMetric,
) -> search::vector::VectorDistanceMetric {
    match metric {
        LegacyVectorMetric::Cosine => search::vector::VectorDistanceMetric::Cosine,
        LegacyVectorMetric::Euclidean => search::vector::VectorDistanceMetric::Euclidean,
        LegacyVectorMetric::Manhattan => search::vector::VectorDistanceMetric::Manhattan,
    }
}

fn legacy_catalog_corruption(error: legacy_index_catalog::LegacyIndexCatalogError) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(error.to_string())
}

/// Runs one fixed-size vector migration scale and resource-boundedness contract.
///
/// The feature-gated production test entry points select the entity count. The
/// implementation remains inside this module so it can measure the same private
/// controller boundary used by startup and background migration workers.
#[cfg(feature = "production-scale")]
pub(crate) async fn run_vector_migration_scale_contract(entity_count: u64) {
    vector_scale::run(entity_count).await;
}

impl LegacySecondaryIndexDefinition {
    fn into_runtime(self) -> Result<config::SecondaryIndexDefinition> {
        Ok(
            match (self.element_type, self.kind, self.unique, self.direction) {
                (
                    LegacySecondaryElementType::Node,
                    LegacySecondaryKind::Equality,
                    false,
                    LegacyRangeDirection::Asc,
                ) => config::SecondaryIndexDefinition::node_equality(self.label, self.property)?,
                (
                    LegacySecondaryElementType::Node,
                    LegacySecondaryKind::Equality,
                    true,
                    LegacyRangeDirection::Asc,
                ) => config::SecondaryIndexDefinition::node_unique_equality(
                    self.label,
                    self.property,
                )?,
                (
                    LegacySecondaryElementType::Node,
                    LegacySecondaryKind::Range,
                    false,
                    direction,
                ) => config::SecondaryIndexDefinition::node_range_with_direction(
                    self.label,
                    self.property,
                    match direction {
                        LegacyRangeDirection::Asc => config::RangeIndexDirection::Asc,
                        LegacyRangeDirection::Desc => config::RangeIndexDirection::Desc,
                    },
                )?,
                (
                    LegacySecondaryElementType::Edge,
                    LegacySecondaryKind::Equality,
                    false,
                    LegacyRangeDirection::Asc,
                ) => config::SecondaryIndexDefinition::edge_equality(self.label, self.property)?,
                (
                    LegacySecondaryElementType::Edge,
                    LegacySecondaryKind::Range,
                    false,
                    direction,
                ) => config::SecondaryIndexDefinition::edge_range_with_direction(
                    self.label,
                    self.property,
                    match direction {
                        LegacyRangeDirection::Asc => config::RangeIndexDirection::Asc,
                        LegacyRangeDirection::Desc => config::RangeIndexDirection::Desc,
                    },
                )?,
                (element_type, kind, true, _) => {
                    return Err(HelixDbError::Config(format!(
                        "invalid legacy unique secondary definition: {element_type:?} {kind:?}"
                    )))
                }
                (_, LegacySecondaryKind::Equality, false, LegacyRangeDirection::Desc) => {
                    return Err(HelixDbError::Config(
                        "legacy equality definition has descending direction".to_string(),
                    ))
                }
            },
        )
    }
}

impl LegacyVectorIndexDefinition {
    fn into_runtime(self) -> Result<config::VectorIndexDefinition> {
        let definition = match self.element_type {
            LegacyVectorElementType::Node => config::VectorIndexDefinition::new_node(
                self.label,
                self.property,
                self.dimension,
                legacy_vector_metric_into_runtime(self.metric),
            )?,
            LegacyVectorElementType::Edge => config::VectorIndexDefinition::new_edge(
                self.label,
                self.property,
                self.dimension,
                legacy_vector_metric_into_runtime(self.metric),
            )?,
        }
        .with_tenant_property_option(self.tenant_property)?
        .with_m(self.m)?
        .with_m0(self.m0)?
        .with_ef_construction(self.ef_construction)?
        .with_ml(self.ml)?
        .with_simhash_threshold(self.simhash_threshold)?
        .with_sampling_ratio(self.sampling_ratio)?
        .with_adaptive_enabled(self.adaptive_enabled)
        .with_adaptive_failure_prob(self.adaptive_failure_prob)?;
        Ok(definition)
    }
}

impl LegacyTextIndexDefinition {
    fn into_runtime(self) -> Result<config::TextIndexDefinition> {
        let definition = match self.element_type {
            LegacyTextElementType::Node => {
                config::TextIndexDefinition::new_node(self.label, self.property)?
            }
            LegacyTextElementType::Edge => {
                config::TextIndexDefinition::new_edge(self.label, self.property)?
            }
        }
        .with_tenant_property_option(self.tenant_property)?
        .with_analyzer(match self.analyzer {
            LegacyTextAnalyzer::Standard => config::TextAnalyzerKind::Standard,
            LegacyTextAnalyzer::StandardStemEn => config::TextAnalyzerKind::StandardStemEn,
            LegacyTextAnalyzer::WhitespaceLowercase => {
                config::TextAnalyzerKind::WhitespaceLowercase
            }
        })
        .with_positions_enabled(self.positions_enabled);
        Ok(definition)
    }
}

impl LegacyDynamicIndexDefinition {
    fn key(&self) -> LegacyDynamicIndexKey {
        match self {
            Self::Secondary(definition) => LegacyDynamicIndexKey::Secondary(definition.clone()),
            Self::Vector(definition) => LegacyDynamicIndexKey::Vector {
                element_type: definition.element_type,
                label: definition.label.clone(),
                property: definition.property.clone(),
            },
            Self::Text(definition) => LegacyDynamicIndexKey::Text {
                element_type: definition.element_type,
                label: definition.label.clone(),
                property: definition.property.clone(),
            },
        }
    }

    fn into_validated(self) -> Result<crate::index_lifecycle::ValidatedDynamicIndexDefinition> {
        Ok(match self {
            Self::Secondary(definition) => definition.into_runtime()?.try_into()?,
            Self::Vector(definition) => definition.into_runtime()?.try_into()?,
            Self::Text(definition) => definition.into_runtime()?.try_into()?,
        })
    }
}

impl LegacyDynamicIndexKey {
    fn identity(&self) -> Result<crate::index_lifecycle::IndexIdentity> {
        let (family, element_kind, label, property) = match self {
            Self::Secondary(definition) => {
                return Ok(
                    crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
                        definition.clone().into_runtime()?,
                    )?
                    .identity(),
                )
            }
            Self::Vector {
                element_type,
                label,
                property,
            } => (
                crate::index_lifecycle::IndexIdentityFamily::Vector,
                match element_type {
                    LegacyVectorElementType::Node => crate::index_lifecycle::IndexElementKind::Node,
                    LegacyVectorElementType::Edge => crate::index_lifecycle::IndexElementKind::Edge,
                },
                label,
                property,
            ),
            Self::Text {
                element_type,
                label,
                property,
            } => (
                crate::index_lifecycle::IndexIdentityFamily::Text,
                match element_type {
                    LegacyTextElementType::Node => crate::index_lifecycle::IndexElementKind::Node,
                    LegacyTextElementType::Edge => crate::index_lifecycle::IndexElementKind::Edge,
                },
                label,
                property,
            ),
        };
        Ok(crate::index_lifecycle::IndexIdentity::new(
            family,
            element_kind,
            crate::index_lifecycle::IndexComponent::try_new("label", label)?,
            crate::index_lifecycle::IndexComponent::try_new("property", property)?,
        ))
    }
}

#[cfg(any(test, feature = "migration-parity", feature = "production-coverage"))]
pub fn migration_parity_legacy_catalog_row(
    definition: &crate::index_lifecycle::ValidatedDynamicIndexDefinition,
    tombstone: bool,
) -> Result<(Bytes, Bytes)> {
    use crate::index_lifecycle::{IndexElementKind, ValidatedDynamicIndexDefinition};

    let legacy = match definition {
        ValidatedDynamicIndexDefinition::Secondary(definition) => {
            let runtime = definition.to_runtime();
            LegacyDynamicIndexDefinition::Secondary(LegacySecondaryIndexDefinition {
                element_type: match runtime.element_type() {
                    config::SecondaryIndexElementType::Node => LegacySecondaryElementType::Node,
                    config::SecondaryIndexElementType::Edge => LegacySecondaryElementType::Edge,
                },
                kind: match runtime.kind() {
                    config::SecondaryIndexKind::Equality => LegacySecondaryKind::Equality,
                    config::SecondaryIndexKind::Range => LegacySecondaryKind::Range,
                },
                label: runtime.label().to_string(),
                property: runtime.property().to_string(),
                unique: runtime.unique(),
                direction: match runtime.direction() {
                    config::RangeIndexDirection::Asc => LegacyRangeDirection::Asc,
                    config::RangeIndexDirection::Desc => LegacyRangeDirection::Desc,
                },
            })
        }
        ValidatedDynamicIndexDefinition::Vector(definition) => {
            LegacyDynamicIndexDefinition::Vector(LegacyVectorIndexDefinition {
                element_type: match definition.element_kind() {
                    IndexElementKind::Node => LegacyVectorElementType::Node,
                    IndexElementKind::Edge => LegacyVectorElementType::Edge,
                },
                label: definition.label().as_str().to_string(),
                property: definition.property().as_str().to_string(),
                tenant_property: definition
                    .tenant_property()
                    .map(|property| property.as_str().to_string()),
                dimension: usize::try_from(definition.dimension()).map_err(|_| {
                    HelixDbError::InvariantViolation(
                        "validated vector dimension does not fit usize".to_string(),
                    )
                })?,
                metric: match definition.metric() {
                    search::vector::VectorDistanceMetric::Cosine => LegacyVectorMetric::Cosine,
                    search::vector::VectorDistanceMetric::Euclidean => {
                        LegacyVectorMetric::Euclidean
                    }
                    search::vector::VectorDistanceMetric::Manhattan => {
                        LegacyVectorMetric::Manhattan
                    }
                },
                m: usize::try_from(definition.m()).map_err(|_| {
                    HelixDbError::InvariantViolation(
                        "validated vector m does not fit usize".to_string(),
                    )
                })?,
                m0: usize::try_from(definition.m0()).map_err(|_| {
                    HelixDbError::InvariantViolation(
                        "validated vector m0 does not fit usize".to_string(),
                    )
                })?,
                ef_construction: usize::try_from(definition.ef_construction()).map_err(|_| {
                    HelixDbError::InvariantViolation(
                        "validated vector ef_construction does not fit usize".to_string(),
                    )
                })?,
                ml: definition.ml(),
                simhash_threshold: usize::try_from(definition.simhash_threshold()).map_err(
                    |_| {
                        HelixDbError::InvariantViolation(
                            "validated vector simhash threshold does not fit usize".to_string(),
                        )
                    },
                )?,
                sampling_ratio: definition.sampling_ratio(),
                adaptive_enabled: definition.adaptive_enabled(),
                adaptive_failure_prob: definition.adaptive_failure_probability(),
            })
        }
        ValidatedDynamicIndexDefinition::Text(definition) => {
            LegacyDynamicIndexDefinition::Text(LegacyTextIndexDefinition {
                element_type: match definition.element_kind() {
                    IndexElementKind::Node => LegacyTextElementType::Node,
                    IndexElementKind::Edge => LegacyTextElementType::Edge,
                },
                label: definition.label().as_str().to_string(),
                property: definition.property().as_str().to_string(),
                tenant_property: definition
                    .tenant_property()
                    .map(|property| property.as_str().to_string()),
                analyzer: match definition.analyzer() {
                    config::TextAnalyzerKind::Standard => LegacyTextAnalyzer::Standard,
                    config::TextAnalyzerKind::StandardStemEn => LegacyTextAnalyzer::StandardStemEn,
                    config::TextAnalyzerKind::WhitespaceLowercase => {
                        LegacyTextAnalyzer::WhitespaceLowercase
                    }
                },
                positions_enabled: definition.positions_enabled(),
            })
        }
    };
    let identity = legacy.key();
    let entry = if tombstone {
        LegacyDynamicIndexCatalogEntry::Tombstone { tombstone: true }
    } else {
        LegacyDynamicIndexCatalogEntry::Definition(legacy)
    };
    legacy_index_catalog::encode_row_for_contract(DataScope::LegacyUnscoped, &identity, &entry)
        .map_err(|error| HelixDbError::Config(error.to_string()))
}

/// Stable crash-injection boundaries used by the release recovery harness.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationFailpoint {
    JobCreationBeforeCommit,
    JobCreationAfterCommit,
    AllocatorReservationBefore,
    AllocatorReservationAfter,
    BatchReadBefore,
    BatchReadAfter,
    BatchWriteBefore,
    BatchWriteAfter,
    BatchCommitBefore,
    BatchCommitAfter,
    StageTransitionBefore,
    StageTransitionAfter,
    RewriteCompletionBefore,
    RewriteCompletionAfter,
    CleanupEnqueueBefore,
    CleanupEnqueueAfter,
    CleanupDeleteBefore,
    CleanupDeleteAfter,
    LegacyVectorReservationBefore,
    LegacyVectorReservationAfter,
    LegacyDefinitionEnqueueBefore,
    LegacyDefinitionEnqueueAfter,
    LegacyVectorValidationCheckpointBefore,
    LegacyVectorValidationCheckpointAfter,
    LegacyVectorMetadataPublicationBefore,
    LegacyVectorMetadataPublicationAfter,
    LegacyVectorReservationTransitionBefore,
    LegacyVectorReservationTransitionAfter,
    LegacyDefinitionRetirementBefore,
    LegacyDefinitionRetirementAfter,
    MigrationReadyPublicationBefore,
    MigrationReadyPublicationAfter,
    StorageSchemaCompletionBefore,
    StorageSchemaCompletionAfter,
    VectorDirectoryPreflightCommitBefore,
    VectorDirectoryPreflightCommitAfter,
    VectorDirectoryBackfillCommitBefore,
    VectorDirectoryBackfillCommitAfter,
    VectorDirectoryVerificationCommitBefore,
    VectorDirectoryVerificationCommitAfter,
    VectorDirectoryPublicationCommitBefore,
    VectorDirectoryPublicationCommitAfter,
}

/// Durable legacy-text rebuild boundary selected by migration acceptance tests.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyTextMigrationCheckpoint {
    BeforeEnqueue,
    SourceScan,
    CatchUp,
    ValidatePages,
    ValidateRoots,
    ValidateEntityStates,
    AfterActivationBeforeRetirement,
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
#[derive(Debug, Default)]
struct LegacyTextMigrationCheckpointState {
    target: Option<LegacyTextMigrationCheckpoint>,
    triggered: bool,
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
static LEGACY_TEXT_MIGRATION_CHECKPOINT: Mutex<LegacyTextMigrationCheckpointState> =
    Mutex::new(LegacyTextMigrationCheckpointState {
        target: None,
        triggered: false,
    });

/// Arms one non-persisted, process-local migration interruption.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub fn inject_legacy_text_migration_checkpoint_once(
    checkpoint: LegacyTextMigrationCheckpoint,
) -> Result<()> {
    let mut state = LEGACY_TEXT_MIGRATION_CHECKPOINT
        .lock()
        .expect("legacy text migration checkpoint mutex is healthy");
    if state.target.is_some() {
        return Err(HelixDbError::Config(
            "a legacy text migration checkpoint is already armed".to_string(),
        ));
    }
    state.target = Some(checkpoint);
    state.triggered = false;
    Ok(())
}

/// Clears the feature-gated migration interruption before recovery reopen.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub fn clear_legacy_text_migration_checkpoint() {
    *LEGACY_TEXT_MIGRATION_CHECKPOINT
        .lock()
        .expect("legacy text migration checkpoint mutex is healthy") =
        LegacyTextMigrationCheckpointState::default();
}

/// Reports whether the selected durable migration boundary was observed.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub fn legacy_text_migration_checkpoint_was_triggered() -> bool {
    LEGACY_TEXT_MIGRATION_CHECKPOINT
        .lock()
        .expect("legacy text migration checkpoint mutex is healthy")
        .triggered
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
fn trip_legacy_text_migration_checkpoint(checkpoint: LegacyTextMigrationCheckpoint) -> Result<()> {
    let mut state = LEGACY_TEXT_MIGRATION_CHECKPOINT
        .lock()
        .expect("legacy text migration checkpoint mutex is healthy");
    if state.target == Some(checkpoint) {
        state.triggered = true;
    }
    if state.triggered {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "injected legacy text migration interruption at {:?}",
                state
                    .target
                    .expect("triggered checkpoint retains its target")
            ),
        });
    }
    Ok(())
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
fn check_legacy_text_migration_interruption() -> Result<()> {
    let state = LEGACY_TEXT_MIGRATION_CHECKPOINT
        .lock()
        .expect("legacy text migration checkpoint mutex is healthy");
    if state.triggered {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "injected legacy text migration interruption at {:?}",
                state
                    .target
                    .expect("triggered checkpoint retains its target")
            ),
        });
    }
    Ok(())
}

/// Observes an exact queued text-build stage before a worker acquires it.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub(crate) fn observe_legacy_text_migration_operation(
    operation: &crate::index_lifecycle::IndexOperationRecord,
) -> Result<()> {
    use crate::index_lifecycle::{
        IndexOperationProgress, TextBuildProgress, TextBuildStage, TextManifestValidationProgress,
    };

    let IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(stage)) =
        operation.progress()
    else {
        return check_legacy_text_migration_interruption();
    };
    let checkpoint = match stage {
        TextBuildStage::ScanSource(_) => Some(LegacyTextMigrationCheckpoint::SourceScan),
        TextBuildStage::CatchUp(_) => Some(LegacyTextMigrationCheckpoint::CatchUp),
        TextBuildStage::ValidateManifests(validation) => Some(match validation {
            TextManifestValidationProgress::Pages(_) => {
                LegacyTextMigrationCheckpoint::ValidatePages
            }
            TextManifestValidationProgress::Roots(_) => {
                LegacyTextMigrationCheckpoint::ValidateRoots
            }
            TextManifestValidationProgress::EntityStates(_) => {
                LegacyTextMigrationCheckpoint::ValidateEntityStates
            }
        }),
        TextBuildStage::ScanPartitions(_)
        | TextBuildStage::Compact(_)
        | TextBuildStage::PrepareManifests(_)
        | TextBuildStage::Activate(_) => None,
    };
    match checkpoint {
        Some(checkpoint) => trip_legacy_text_migration_checkpoint(checkpoint),
        None => check_legacy_text_migration_interruption(),
    }
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
impl MigrationFailpoint {
    pub const ALL: [Self; 42] = [
        Self::JobCreationBeforeCommit,
        Self::JobCreationAfterCommit,
        Self::AllocatorReservationBefore,
        Self::AllocatorReservationAfter,
        Self::BatchReadBefore,
        Self::BatchReadAfter,
        Self::BatchWriteBefore,
        Self::BatchWriteAfter,
        Self::BatchCommitBefore,
        Self::BatchCommitAfter,
        Self::StageTransitionBefore,
        Self::StageTransitionAfter,
        Self::RewriteCompletionBefore,
        Self::RewriteCompletionAfter,
        Self::CleanupEnqueueBefore,
        Self::CleanupEnqueueAfter,
        Self::CleanupDeleteBefore,
        Self::CleanupDeleteAfter,
        Self::LegacyVectorReservationBefore,
        Self::LegacyVectorReservationAfter,
        Self::LegacyDefinitionEnqueueBefore,
        Self::LegacyDefinitionEnqueueAfter,
        Self::LegacyVectorValidationCheckpointBefore,
        Self::LegacyVectorValidationCheckpointAfter,
        Self::LegacyVectorMetadataPublicationBefore,
        Self::LegacyVectorMetadataPublicationAfter,
        Self::LegacyVectorReservationTransitionBefore,
        Self::LegacyVectorReservationTransitionAfter,
        Self::LegacyDefinitionRetirementBefore,
        Self::LegacyDefinitionRetirementAfter,
        Self::MigrationReadyPublicationBefore,
        Self::MigrationReadyPublicationAfter,
        Self::StorageSchemaCompletionBefore,
        Self::StorageSchemaCompletionAfter,
        Self::VectorDirectoryPreflightCommitBefore,
        Self::VectorDirectoryPreflightCommitAfter,
        Self::VectorDirectoryBackfillCommitBefore,
        Self::VectorDirectoryBackfillCommitAfter,
        Self::VectorDirectoryVerificationCommitBefore,
        Self::VectorDirectoryVerificationCommitAfter,
        Self::VectorDirectoryPublicationCommitBefore,
        Self::VectorDirectoryPublicationCommitAfter,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JobCreationBeforeCommit => "job_creation_before_commit",
            Self::JobCreationAfterCommit => "job_creation_after_commit",
            Self::AllocatorReservationBefore => "allocator_reservation_before",
            Self::AllocatorReservationAfter => "allocator_reservation_after",
            Self::BatchReadBefore => "batch_read_before",
            Self::BatchReadAfter => "batch_read_after",
            Self::BatchWriteBefore => "batch_write_before",
            Self::BatchWriteAfter => "batch_write_after",
            Self::BatchCommitBefore => "batch_commit_before",
            Self::BatchCommitAfter => "batch_commit_after",
            Self::StageTransitionBefore => "stage_transition_before",
            Self::StageTransitionAfter => "stage_transition_after",
            Self::RewriteCompletionBefore => "rewrite_completion_before",
            Self::RewriteCompletionAfter => "rewrite_completion_after",
            Self::CleanupEnqueueBefore => "cleanup_enqueue_before",
            Self::CleanupEnqueueAfter => "cleanup_enqueue_after",
            Self::CleanupDeleteBefore => "cleanup_delete_before",
            Self::CleanupDeleteAfter => "cleanup_delete_after",
            Self::LegacyVectorReservationBefore => "legacy_vector_reservation_before",
            Self::LegacyVectorReservationAfter => "legacy_vector_reservation_after",
            Self::LegacyDefinitionEnqueueBefore => "legacy_definition_enqueue_before",
            Self::LegacyDefinitionEnqueueAfter => "legacy_definition_enqueue_after",
            Self::LegacyVectorValidationCheckpointBefore => {
                "legacy_vector_validation_checkpoint_before"
            }
            Self::LegacyVectorValidationCheckpointAfter => {
                "legacy_vector_validation_checkpoint_after"
            }
            Self::LegacyVectorMetadataPublicationBefore => {
                "legacy_vector_metadata_publication_before"
            }
            Self::LegacyVectorMetadataPublicationAfter => {
                "legacy_vector_metadata_publication_after"
            }
            Self::LegacyVectorReservationTransitionBefore => {
                "legacy_vector_reservation_transition_before"
            }
            Self::LegacyVectorReservationTransitionAfter => {
                "legacy_vector_reservation_transition_after"
            }
            Self::LegacyDefinitionRetirementBefore => "legacy_definition_retirement_before",
            Self::LegacyDefinitionRetirementAfter => "legacy_definition_retirement_after",
            Self::MigrationReadyPublicationBefore => "migration_ready_publication_before",
            Self::MigrationReadyPublicationAfter => "migration_ready_publication_after",
            Self::StorageSchemaCompletionBefore => "storage_schema_completion_before",
            Self::StorageSchemaCompletionAfter => "storage_schema_completion_after",
            Self::VectorDirectoryPreflightCommitBefore => {
                "vector_directory_preflight_commit_before"
            }
            Self::VectorDirectoryPreflightCommitAfter => "vector_directory_preflight_commit_after",
            Self::VectorDirectoryBackfillCommitBefore => "vector_directory_backfill_commit_before",
            Self::VectorDirectoryBackfillCommitAfter => "vector_directory_backfill_commit_after",
            Self::VectorDirectoryVerificationCommitBefore => {
                "vector_directory_verification_commit_before"
            }
            Self::VectorDirectoryVerificationCommitAfter => {
                "vector_directory_verification_commit_after"
            }
            Self::VectorDirectoryPublicationCommitBefore => {
                "vector_directory_publication_commit_before"
            }
            Self::VectorDirectoryPublicationCommitAfter => {
                "vector_directory_publication_commit_after"
            }
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|failpoint| failpoint.as_str() == value)
    }
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
static INJECTED_MIGRATION_FAILPOINT: Mutex<Option<MigrationFailpoint>> = Mutex::new(None);
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
static MIGRATION_FAILPOINT_TRIGGERED: AtomicBool = AtomicBool::new(false);

/// Inject one typed migration error in this process for recovery verification.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub fn inject_migration_failpoint_once(failpoint: MigrationFailpoint) -> Result<()> {
    let mut injected = INJECTED_MIGRATION_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation("migration failpoint mutex was poisoned".to_string())
    })?;
    *injected = Some(failpoint);
    MIGRATION_FAILPOINT_TRIGGERED.store(false, Ordering::SeqCst);
    Ok(())
}

/// Return whether the currently requested one-shot failpoint fired.
#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub fn migration_failpoint_was_triggered() -> bool {
    MIGRATION_FAILPOINT_TRIGGERED.load(Ordering::SeqCst)
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub(crate) fn trip_migration_failpoint(failpoint: MigrationFailpoint) -> Result<()> {
    let mut injected = INJECTED_MIGRATION_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation("migration failpoint mutex was poisoned".to_string())
    })?;
    if *injected == Some(failpoint) {
        *injected = None;
        MIGRATION_FAILPOINT_TRIGGERED.store(true, Ordering::SeqCst);
        return Err(HelixDbError::Config(format!(
            "injected migration failpoint {}",
            failpoint.as_str()
        )));
    }
    drop(injected);
    if std::env::var("HELIX_MIGRATION_FAILPOINT").as_deref() != Ok(failpoint.as_str()) {
        return Ok(());
    }
    if std::env::var("HELIX_MIGRATION_FAIL_ACTION").as_deref() == Ok("abort") {
        std::process::abort();
    }
    Err(HelixDbError::Config(format!(
        "injected migration failpoint {}",
        failpoint.as_str()
    )))
}

/// Durable migration identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationId {
    /// Rewrite legacy graph rows and rebuild current-format indexes.
    GraphFormatV1Rewrite,
    /// Restore vector properties removed from legacy graph rows.
    LegacyVectorPropertyMaterialization,
    /// Delete legacy-only vector namespaces after exact V2 activation.
    LegacyVectorPhysicalCleanup,
    /// Remove obsolete legacy graph rows after the rewrite is durable.
    GraphFormatV1Cleanup,
}

impl MigrationId {
    const fn storage_name(self) -> &'static [u8] {
        match self {
            Self::GraphFormatV1Rewrite => b"graph_format_v1_rewrite",
            Self::LegacyVectorPropertyMaterialization => b"legacy_vector_property_materialization",
            Self::LegacyVectorPhysicalCleanup => b"legacy_vector_physical_cleanup",
            Self::GraphFormatV1Cleanup => b"graph_format_v1_cleanup",
        }
    }

    const fn initial_stage(self) -> MigrationStage {
        match self {
            Self::GraphFormatV1Rewrite => MigrationStage::PropertyIndexes,
            Self::LegacyVectorPropertyMaterialization => MigrationStage::NodeProperties,
            Self::LegacyVectorPhysicalCleanup => MigrationStage::FenceLegacyVectorSources,
            Self::GraphFormatV1Cleanup => MigrationStage::LegacyEdgePairs,
        }
    }

    const fn log_name(self) -> &'static str {
        match self {
            Self::GraphFormatV1Rewrite => "graph_format_v1_rewrite",
            Self::LegacyVectorPropertyMaterialization => "legacy_vector_property_materialization",
            Self::LegacyVectorPhysicalCleanup => "legacy_vector_physical_cleanup",
            Self::GraphFormatV1Cleanup => "graph_format_v1_cleanup",
        }
    }
}

/// Migration execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationMode {
    /// Must finish before startup completes.
    BlockingStartup,
    /// May run while the writer is open.
    Background,
}

impl MigrationMode {
    const fn log_name(self) -> &'static str {
        match self {
            Self::BlockingStartup => "blocking_startup",
            Self::Background => "background",
        }
    }
}

/// Source keyspace stage for resumable scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MigrationStage {
    /// Obsolete non-vector secondary and label index rows.
    PropertyIndexes,
    /// Existing node property rows.
    NodeProperties,
    /// Legacy pair-addressed edge property rows.
    LegacyEdgePairs,
    /// Current edge endpoint rows.
    EdgeEndpoints,
    /// Legacy vector reservations transitioning into migration ownership.
    FenceLegacyVectorSources,
    /// Dedicated hot-lane rows owned by fenced legacy sources.
    LegacyVectorHotRows,
    /// Dedicated layer-zero rows owned by fenced legacy sources.
    LegacyVectorLayer0Rows,
    /// Core metadata and transaction guards owned by fenced legacy sources.
    LegacyVectorCoreRows,
    /// Legacy vector definition rows whose exact V2 generations are active.
    LegacyVectorDefinitions,
    /// Empty fenced reservations ready for release.
    ReleaseLegacyVectorReservations,
}

impl MigrationStage {
    fn prefix(self, scope: DataScope) -> Bytes {
        let logical = match self {
            Self::PropertyIndexes => KeyPrefix::PropertyIndex.as_slice(),
            Self::NodeProperties => KeyPrefix::NodeProperty.as_slice(),
            Self::LegacyEdgePairs => KeyPrefix::EdgePropertyPair.as_slice(),
            Self::EdgeEndpoints => KeyPrefix::EdgeEndpoints.as_slice(),
            Self::FenceLegacyVectorSources | Self::ReleaseLegacyVectorReservations => {
                return crate::encoding::v2::keys::GlobalKey::logical_prefix(
                    crate::encoding::v2::keys::GlobalKind::LegacyVectorPhysicalReservation,
                );
            }
            Self::LegacyVectorHotRows => {
                return Key::data_prefix(
                    scope,
                    crate::encoding::v1::keys::vectors::VectorStorageLane::Hot.scan_prefix(),
                );
            }
            Self::LegacyVectorLayer0Rows => {
                return Key::data_prefix(
                    scope,
                    crate::encoding::v1::keys::vectors::VectorStorageLane::Layer0.scan_prefix(),
                );
            }
            Self::LegacyVectorCoreRows => {
                return crate::encoding::v2::keys::GlobalKey::logical_prefix(
                    crate::encoding::v2::keys::GlobalKind::LegacyVectorPhysicalReservation,
                );
            }
            Self::LegacyVectorDefinitions => {
                return legacy_index_catalog::catalog_scan_prefix(scope);
            }
        };
        Key::data_prefix(scope, Bytes::copy_from_slice(logical))
    }

    const fn log_name(self) -> &'static str {
        match self {
            Self::PropertyIndexes => "property_indexes",
            Self::NodeProperties => "node_properties",
            Self::LegacyEdgePairs => "legacy_edge_pairs",
            Self::EdgeEndpoints => "edge_endpoints",
            Self::FenceLegacyVectorSources => "fence_legacy_vector_sources",
            Self::LegacyVectorHotRows => "legacy_vector_hot_rows",
            Self::LegacyVectorLayer0Rows => "legacy_vector_layer_zero_rows",
            Self::LegacyVectorCoreRows => "legacy_vector_core_rows",
            Self::LegacyVectorDefinitions => "legacy_vector_definitions",
            Self::ReleaseLegacyVectorReservations => "release_legacy_vector_reservations",
        }
    }
}

/// Non-empty physical storage key used as a resume point.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub(crate) struct MigrationResumeKey(Vec<u8>);

impl MigrationResumeKey {
    fn new(bytes: Vec<u8>) -> Option<Self> {
        (!bytes.is_empty()).then_some(Self(bytes))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl TryFrom<Vec<u8>> for MigrationResumeKey {
    type Error = &'static str;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        Self::new(value).ok_or("migration resume key cannot be empty")
    }
}

impl From<MigrationResumeKey> for Vec<u8> {
    fn from(value: MigrationResumeKey) -> Self {
        value.0
    }
}

/// Durable migration lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub(crate) enum MigrationJobState {
    /// Scanning source rows.
    Running {
        /// Current source keyspace stage.
        stage: MigrationStage,
        /// Last committed physical key in the current stage.
        resume_after_key: Option<MigrationResumeKey>,
        /// Rows processed across all stages.
        processed_rows: u64,
    },
    /// Migration finished successfully.
    Completed {
        /// Rows processed across all stages.
        processed_rows: u64,
    },
    /// Inspectable failure state that may be retried from its durable resume
    /// key.
    Failed {
        /// Stage that failed.
        stage: MigrationStage,
        /// Last committed physical key in the failed stage.
        resume_after_key: Option<MigrationResumeKey>,
        /// Rows processed before failure.
        processed_rows: u64,
        /// Non-empty error text.
        error: String,
    },
}

impl MigrationJobState {
    fn running_stage(&self) -> Option<MigrationStage> {
        let Self::Running { stage, .. } = self else {
            return None;
        };
        Some(*stage)
    }

    fn processed_rows(&self) -> u64 {
        match self {
            Self::Running { processed_rows, .. }
            | Self::Completed { processed_rows }
            | Self::Failed { processed_rows, .. } => *processed_rows,
        }
    }

    fn log_name(&self) -> &'static str {
        match self {
            Self::Running { .. } => "running",
            Self::Completed { .. } => "completed",
            Self::Failed { .. } => "failed",
        }
    }

    fn is_failed(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }
}

/// Persisted migration job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MigrationJob {
    /// Migration identity.
    id: MigrationId,
    /// Execution mode.
    mode: MigrationMode,
    /// Durable lifecycle state.
    state: MigrationJobState,
}

impl MigrationJob {
    fn new(id: MigrationId, mode: MigrationMode) -> Self {
        Self {
            id,
            mode,
            state: MigrationJobState::Running {
                stage: id.initial_stage(),
                resume_after_key: None,
                processed_rows: 0,
            },
        }
    }

    fn is_runnable(&self) -> bool {
        matches!(self.state, MigrationJobState::Running { .. })
    }

    fn is_completed(&self) -> bool {
        matches!(self.state, MigrationJobState::Completed { .. })
    }

    fn is_failed(&self) -> bool {
        self.state.is_failed()
    }

    fn record_advanced(&mut self, resume_after_key: MigrationResumeKey, rows: u64) {
        let MigrationJobState::Running {
            stage,
            processed_rows,
            ..
        } = self.state
        else {
            return;
        };
        self.state = MigrationJobState::Running {
            stage,
            resume_after_key: Some(resume_after_key),
            processed_rows: processed_rows.saturating_add(rows),
        };
    }

    fn advance_stage(&mut self, next_stage: MigrationStage) {
        let processed_rows = self.state.processed_rows();
        self.state = MigrationJobState::Running {
            stage: next_stage,
            resume_after_key: None,
            processed_rows,
        };
    }

    fn complete(&mut self) {
        let processed_rows = self.state.processed_rows();
        self.state = MigrationJobState::Completed { processed_rows };
    }

    fn fail(&mut self, error: impl Into<String>) {
        let error = error.into();
        let MigrationJobState::Running {
            stage,
            resume_after_key,
            processed_rows,
        } = &self.state
        else {
            return;
        };
        self.state = MigrationJobState::Failed {
            stage: *stage,
            resume_after_key: resume_after_key.clone(),
            processed_rows: *processed_rows,
            error,
        };
    }

    fn retry(&mut self) {
        let MigrationJobState::Failed {
            stage,
            resume_after_key,
            processed_rows,
            ..
        } = &self.state
        else {
            return;
        };
        self.state = MigrationJobState::Running {
            stage: *stage,
            resume_after_key: resume_after_key.clone(),
            processed_rows: *processed_rows,
        };
    }
}

/// Metadata key for one migration job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MigrationJobKey(Bytes);

impl MigrationJobKey {
    pub(crate) fn new(scope: DataScope, id: MigrationId) -> Self {
        let mut name = Vec::with_capacity(MIGRATION_JOB_PREFIX.len() + id.storage_name().len());
        name.extend_from_slice(MIGRATION_JOB_PREFIX);
        name.extend_from_slice(id.storage_name());
        Self(scoped_metadata_key(scope, &name))
    }

    fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl AsRef<[u8]> for MigrationJobKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

/// Prefix used to scan all migration jobs for one scope.
#[cfg(any(test, feature = "migration-parity", feature = "production-coverage"))]
pub(crate) fn migration_job_scan_prefix_scoped(scope: DataScope) -> Bytes {
    scoped_metadata_key(scope, MIGRATION_JOB_PREFIX)
}

/// Ensure a migration job exists unless it already completed or is running.
pub(crate) async fn ensure_migration_job(
    db: &Db,
    scope: DataScope,
    id: MigrationId,
    mode: MigrationMode,
) -> Result<()> {
    let key = MigrationJobKey::new(scope, id);
    let txn = db.begin(IsolationLevel::Snapshot).await?;
    if txn.get(key.as_ref()).await?.is_some() {
        txn.rollback();
        return Ok(());
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::JobCreationBeforeCommit)?;
    let job = MigrationJob::new(id, mode);
    txn.put(key.into_bytes(), encode_json(&job)?)?;
    txn.commit().await?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::JobCreationAfterCommit)?;
    tracing::info!(
        migration_id = id.log_name(),
        migration_mode = mode.log_name(),
        scope = ?scope,
        "kv migration job ensured"
    );
    Ok(())
}

/// Return true when the migration has completed for this scope.
pub(crate) async fn migration_completed(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    id: MigrationId,
) -> Result<bool> {
    let key = MigrationJobKey::new(scope, id);
    let Some(value) = read.get(key.as_ref()).await? else {
        return Ok(false);
    };
    let job = decode_json::<MigrationJob>(&value)?;
    Ok(job.is_completed())
}

async fn migration_processed_rows(db: &Db, scope: DataScope, id: MigrationId) -> Result<u64> {
    let key = MigrationJobKey::new(scope, id);
    let Some(value) = db.get(key.as_ref()).await? else {
        return Ok(0);
    };
    Ok(decode_json::<MigrationJob>(&value)?.state.processed_rows())
}

/// Return whether a writer has durably completed the graph-format rewrite.
pub(crate) async fn graph_format_v1_ready(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<bool> {
    Ok(read
        .get(scoped_metadata_key(scope, GRAPH_FORMAT_V1_READY))
        .await?
        .as_deref()
        == Some(b"1"))
}

/// Persist the readiness marker for stores whose durable rewrite job already
/// completed before the marker was introduced.
pub(crate) async fn ensure_graph_format_v1_ready(db: &Db, scope: DataScope) -> Result<()> {
    if graph_format_v1_ready(db, scope).await? {
        return Ok(());
    }
    let txn = db.begin(IsolationLevel::Snapshot).await?;
    txn.put(
        scoped_metadata_key(scope, GRAPH_FORMAT_V1_READY),
        Bytes::from_static(b"1"),
    )?;
    txn.commit().await?;
    Ok(())
}

/// Returns whether graph and legacy-definition migration is durably complete.
pub(crate) async fn index_v2_migration_ready(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<bool> {
    Ok(read
        .get(scoped_metadata_key(scope, INDEX_V2_MIGRATION_READY))
        .await?
        .as_deref()
        == Some(b"1"))
}

/// Returns whether obsolete V3 managed-index rows were durably removed.
pub(crate) async fn index_storage_v4_cleanup_ready(
    read: &(impl DbReadOps + Send + Sync),
) -> Result<bool> {
    match read
        .get(scoped_metadata_key(
            DataScope::LegacyUnscoped,
            INDEX_STORAGE_V4_CLEANUP_READY,
        ))
        .await?
        .as_deref()
    {
        None => Ok(false),
        Some(b"1") => Ok(true),
        Some(_) => Err(HelixDbError::MigrationRequired {
            reason: "index storage V4 cleanup readiness marker is malformed".to_string(),
        }),
    }
}

/// Returns whether every tenant-owned physical key uses the one-byte envelope.
pub(crate) async fn tenant_key_envelope_ready(
    read: &(impl DbReadOps + Send + Sync),
) -> Result<bool> {
    match read
        .get(scoped_metadata_key(
            DataScope::LegacyUnscoped,
            TENANT_KEY_ENVELOPE_READY,
        ))
        .await?
        .as_deref()
    {
        None => Ok(false),
        Some(b"1") => Ok(true),
        Some(_) => Err(HelixDbError::MigrationRequired {
            reason: "tenant key envelope readiness marker is malformed".to_string(),
        }),
    }
}

/// Stages tenant-envelope completion in the caller's migration transaction.
pub(crate) fn stage_tenant_key_envelope_ready(transaction: &DbTransaction) -> Result<()> {
    transaction.put(
        scoped_metadata_key(DataScope::LegacyUnscoped, TENANT_KEY_ENVELOPE_READY),
        Bytes::from_static(b"1"),
    )?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn stage_reader_compatible_storage_schema_for_tests(
    transaction: &DbTransaction,
) -> Result<()> {
    for marker in [
        GRAPH_FORMAT_V1_READY,
        INDEX_V2_MIGRATION_READY,
        STORAGE_SCHEMA_COMPLETE,
    ] {
        transaction.put(
            scoped_metadata_key(DataScope::LegacyUnscoped, marker),
            Bytes::from_static(b"1"),
        )?;
    }
    stage_tenant_key_envelope_ready(transaction)
}

/// Stages V4 cleanup completion in the caller's existing transaction.
pub(crate) fn stage_index_storage_v4_cleanup_ready(transaction: &DbTransaction) -> Result<()> {
    transaction.put(
        scoped_metadata_key(DataScope::LegacyUnscoped, INDEX_STORAGE_V4_CLEANUP_READY),
        Bytes::from_static(b"1"),
    )?;
    Ok(())
}

/// Reopens legacy-definition migration for a production-coverage fixture.
///
/// The caller must atomically stage a valid legacy catalog source in the same
/// transaction. Writer restart will then exercise the ordinary migration and
/// lifecycle recovery path instead of observing a completed schema beside
/// newly injected legacy state.
#[cfg(all(
    feature = "index-lifecycle-testing",
    any(test, feature = "production-coverage")
))]
pub(crate) fn stage_index_v2_migration_reopen_for_fixture(
    transaction: &DbTransaction,
    scope: DataScope,
) -> Result<()> {
    transaction.delete(scoped_metadata_key(scope, INDEX_V2_MIGRATION_READY))?;
    Ok(())
}

/// Returns whether the full legacy-to-current writer-open pipeline completed.
pub(crate) async fn storage_schema_complete(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<bool> {
    Ok(read
        .get(scoped_metadata_key(scope, STORAGE_SCHEMA_COMPLETE))
        .await?
        .as_deref()
        == Some(b"1"))
}

/// Valid writer-resumable progress through the ordered storage-schema migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageSchemaProgress {
    /// No readiness marker has been published.
    NotStarted,
    /// The graph-format migration is complete.
    GraphReady,
    /// Graph and index migrations are complete.
    IndexReady,
    /// Every writer-owned storage migration is complete.
    Complete,
}

/// Reads and strictly validates the ordered storage-schema readiness markers.
///
/// Missing markers encode incomplete work. Present markers must contain the exact
/// readiness value, and readiness may only advance in prefix order. This keeps a
/// corrupt marker set from being represented as resumable writer work.
pub(crate) async fn storage_schema_progress(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<StorageSchemaProgress> {
    let graph = read
        .get(scoped_metadata_key(scope, GRAPH_FORMAT_V1_READY))
        .await?;
    let index = read
        .get(scoped_metadata_key(scope, INDEX_V2_MIGRATION_READY))
        .await?;
    let schema = read
        .get(scoped_metadata_key(scope, STORAGE_SCHEMA_COMPLETE))
        .await?;
    let parse = |name: &'static str, value: Option<Bytes>| -> Result<bool> {
        match value.as_deref() {
            None => Ok(false),
            Some(b"1") => Ok(true),
            Some(_) => Err(HelixDbError::MigrationRequired {
                reason: format!("storage migration readiness marker {name} is malformed"),
            }),
        }
    };
    let readiness = (
        parse("graph_format_v1", graph)?,
        parse("index_v2_catalog_v1", index)?,
        parse("storage_schema_complete_v1", schema)?,
    );
    match readiness {
        (false, false, false) => Ok(StorageSchemaProgress::NotStarted),
        (true, false, false) => Ok(StorageSchemaProgress::GraphReady),
        (true, true, false) => Ok(StorageSchemaProgress::IndexReady),
        (true, true, true) => Ok(StorageSchemaProgress::Complete),
        _ => Err(HelixDbError::MigrationRequired {
            reason: format!(
                "storage migration readiness is not an ordered prefix (graph={}, index={}, schema={})",
                readiness.0, readiness.1, readiness.2
            ),
        }),
    }
}

async fn publish_storage_schema_completion(db: &Db, scope: DataScope) -> Result<()> {
    let started = Instant::now();
    if !graph_format_v1_ready(db, scope).await? || !index_v2_migration_ready(db, scope).await? {
        return Err(HelixDbError::MigrationRequired {
            reason: "storage schema completion requires graph and index migration readiness"
                .to_string(),
        });
    }
    if !load_legacy_definition_rows(db, scope).await?.is_empty() {
        return Err(HelixDbError::MigrationRequired {
            reason: "storage schema completion requires an empty legacy index catalog".to_string(),
        });
    }
    if storage_schema_complete(db, scope).await? {
        tracing::info!(
            migration_version = STORAGE_SCHEMA_VERSION,
            migration_step = "storage_schema_completion",
            migration_outcome = "skipped",
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            scanned_count = 3,
            written_count = 0,
            "storage schema migration step finished"
        );
        return Ok(());
    }

    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::StorageSchemaCompletionBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    if !graph_format_v1_ready(&transaction, scope).await?
        || !index_v2_migration_ready(&transaction, scope).await?
        || !load_legacy_definition_rows(&transaction, scope)
            .await?
            .is_empty()
    {
        transaction.rollback();
        return Err(HelixDbError::MigrationRequired {
            reason: "storage schema prerequisites changed before completion publication"
                .to_string(),
        });
    }
    transaction.put(
        scoped_metadata_key(scope, STORAGE_SCHEMA_COMPLETE),
        Bytes::from_static(b"1"),
    )?;
    transaction.commit().await?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::StorageSchemaCompletionAfter)?;
    tracing::info!(
        migration_version = STORAGE_SCHEMA_VERSION,
        migration_step = "storage_schema_completion",
        migration_outcome = "applied",
        duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        scanned_count = 3,
        written_count = 1,
        "storage schema migration step finished"
    );
    Ok(())
}

/// Runs graph rewrite before converging every persisted legacy definition.
pub(crate) async fn run_blocking_startup_migration(
    writer: &HelixWriter,
    tuning: config::MigrationTuning,
) -> Result<()> {
    let started = Instant::now();
    let scope = DataScope::LegacyUnscoped;
    ensure_migration_job(
        writer.db(),
        scope,
        MigrationId::GraphFormatV1Rewrite,
        MigrationMode::BlockingStartup,
    )
    .await?;
    let rewrite_completed =
        migration_completed(writer.db(), scope, MigrationId::GraphFormatV1Rewrite).await?;
    if !rewrite_completed {
        reserve_allocators_above_existing(writer, scope).await?;
    }
    while !migration_completed(writer.db(), scope, MigrationId::GraphFormatV1Rewrite).await? {
        if !process_migration_once_by_id(writer, scope, tuning, MigrationId::GraphFormatV1Rewrite)
            .await?
        {
            return Err(HelixDbError::Config(
                "blocking graph-format migration did not advance".to_string(),
            ));
        }
    }
    ensure_graph_format_v1_ready(writer.db(), scope).await?;
    ensure_migration_job(
        writer.db(),
        scope,
        MigrationId::LegacyVectorPropertyMaterialization,
        MigrationMode::BlockingStartup,
    )
    .await?;
    let vector_property_catalog =
        vector_properties::LegacyVectorPropertyCatalog::load(writer.db(), scope).await?;
    while !migration_completed(
        writer.db(),
        scope,
        MigrationId::LegacyVectorPropertyMaterialization,
    )
    .await?
    {
        if !process_migration_once_by_id_with_catalog(
            writer,
            scope,
            tuning,
            MigrationId::LegacyVectorPropertyMaterialization,
            MigrationRunCatalog::VectorProperties(&vector_property_catalog),
        )
        .await?
        {
            return Err(HelixDbError::Config(
                "blocking legacy vector-property materialization did not advance".to_string(),
            ));
        }
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::CleanupEnqueueBefore)?;
    ensure_migration_job(
        writer.db(),
        scope,
        MigrationId::GraphFormatV1Cleanup,
        MigrationMode::Background,
    )
    .await?;
    let processed_rows =
        migration_processed_rows(writer.db(), scope, MigrationId::GraphFormatV1Rewrite).await?;
    tracing::info!(
        migration_version = 1,
        migration_step = "graph_format_v1_rewrite",
        migration_outcome = if rewrite_completed {
            "skipped"
        } else {
            "applied"
        },
        duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        scanned_count = processed_rows,
        written_count = if rewrite_completed { 0 } else { processed_rows },
        "storage schema migration step finished"
    );
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::CleanupEnqueueAfter)?;
    Ok(())
}

/// Converges persisted legacy catalog rows through normal V2 lifecycle work.
pub(crate) async fn migrate_legacy_definitions(db: &crate::HelixDB) -> Result<()> {
    let started = Instant::now();
    let scope = DataScope::LegacyUnscoped;
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        return Err(HelixDbError::WriterModeRequired {
            actual: db.mode().as_str(),
        });
    };
    if index_v2_migration_ready(writer.db(), scope).await? {
        db.refresh_runtime_catalog(scope).await?;
        publish_storage_schema_completion(writer.db(), scope).await?;
        tracing::info!(
            migration_version = 1,
            migration_step = "legacy_index_definitions",
            migration_outcome = "skipped",
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            scanned_count = 0,
            written_count = 0,
            "storage schema migration step finished"
        );
        return Ok(());
    }
    if !graph_format_v1_ready(writer.db(), scope).await? {
        return Err(HelixDbError::MigrationRequired {
            reason: "graph rewrite must complete before legacy index definitions migrate"
                .to_string(),
        });
    }
    if !migration_completed(
        writer.db(),
        scope,
        MigrationId::LegacyVectorPropertyMaterialization,
    )
    .await?
    {
        return Err(HelixDbError::MigrationRequired {
            reason:
                "legacy vector properties must be materialized before index definitions migrate"
                    .to_string(),
        });
    }
    preflight_legacy_vector_reservations(writer.db()).await?;

    let legacy_rows = load_legacy_definition_rows(writer.db(), scope).await?;
    let scanned_count = u64::try_from(legacy_rows.len()).unwrap_or(u64::MAX);
    for row in &legacy_rows {
        match &row.entry {
            LegacyDynamicIndexCatalogEntry::Definition(legacy) => {
                if legacy.key() != row.identity {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "legacy dynamic definition key/value identity mismatch".to_string(),
                    ));
                }
                let definition = legacy.clone().into_validated()?;
                if definition.identity() != row.identity.identity()? {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "validated legacy definition changed logical identity".to_string(),
                    ));
                }
                converge_legacy_definition(db, scope, &definition).await?;
            }
            LegacyDynamicIndexCatalogEntry::Tombstone { tombstone } => {
                if !*tombstone {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "legacy dynamic catalog tombstone marker is false".to_string(),
                    ));
                }
                converge_legacy_tombstone(db, scope, &row.identity).await?;
            }
        }
    }

    // Convergence above is intentionally complete before this loop. Legacy
    // secondary property hashes can collide, so retiring one shared physical
    // lane is safe only after every current full-string identity is Active.
    for row in legacy_rows {
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        trip_legacy_text_migration_checkpoint(
            LegacyTextMigrationCheckpoint::AfterActivationBeforeRetirement,
        )?;
        match row.entry {
            LegacyDynamicIndexCatalogEntry::Definition(legacy) => {
                let definition = legacy.into_validated()?;
                if matches!(
                    definition,
                    crate::index_lifecycle::ValidatedDynamicIndexDefinition::Vector(_)
                ) {
                    continue;
                }
                retire_legacy_definition_row(
                    writer.db(),
                    scope,
                    row.storage_key,
                    Some(&definition),
                    &row.identity,
                )
                .await?;
            }
            LegacyDynamicIndexCatalogEntry::Tombstone { .. } => {
                retire_legacy_definition_row(
                    writer.db(),
                    scope,
                    row.storage_key,
                    None,
                    &row.identity,
                )
                .await?;
            }
        }
    }

    ensure_migration_job(
        writer.db(),
        scope,
        MigrationId::LegacyVectorPhysicalCleanup,
        MigrationMode::BlockingStartup,
    )
    .await?;
    let vector_retirement_catalog =
        vector_retirement::LegacyVectorRetirementCatalog::load(writer.db(), scope).await?;
    while !migration_completed(writer.db(), scope, MigrationId::LegacyVectorPhysicalCleanup).await?
    {
        if !process_migration_once_by_id_with_catalog(
            writer,
            scope,
            db.config().db().migrations(),
            MigrationId::LegacyVectorPhysicalCleanup,
            MigrationRunCatalog::VectorRetirement(&vector_retirement_catalog),
        )
        .await?
        {
            return Err(HelixDbError::Config(
                "blocking legacy vector physical cleanup did not advance".to_string(),
            ));
        }
    }

    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::MigrationReadyPublicationBefore)?;
    let transaction = writer
        .db()
        .begin(IsolationLevel::SerializableSnapshot)
        .await?;
    if !load_legacy_definition_rows(&transaction, scope)
        .await?
        .is_empty()
    {
        return Err(HelixDbError::MigrationRequired {
            reason: "legacy definition rows remain after V2 convergence".to_string(),
        });
    }
    if !migration_completed(
        &transaction,
        scope,
        MigrationId::LegacyVectorPhysicalCleanup,
    )
    .await?
    {
        return Err(HelixDbError::MigrationRequired {
            reason: "legacy vector physical cleanup is not durably complete".to_string(),
        });
    }
    transaction.put(
        scoped_metadata_key(scope, INDEX_V2_MIGRATION_READY),
        Bytes::from_static(b"1"),
    )?;
    for name in LEGACY_DYNAMIC_INDEX_CATALOG_METADATA {
        transaction.delete(scoped_metadata_key(scope, name))?;
    }
    transaction.commit().await?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::MigrationReadyPublicationAfter)?;
    db.refresh_runtime_catalog(scope).await?;
    publish_storage_schema_completion(writer.db(), scope).await?;
    tracing::info!(
        migration_version = 1,
        migration_step = "legacy_index_definitions",
        migration_outcome = "applied",
        duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        scanned_count,
        written_count = scanned_count,
        "storage schema migration step finished"
    );
    Ok(())
}

/// Backfills and publishes routing directories for already-active legacy vectors.
pub(crate) async fn migrate_active_vector_simhash_directories(db: &crate::HelixDB) -> Result<()> {
    vector_simhash_directory::run(
        db,
        DataScope::LegacyUnscoped,
        db.config().db().search_index_backfill().batch(),
    )
    .await
}

/// Exact persisted source row retained until one vector adoption activates.
pub(crate) struct LegacyVectorAdoptionSource {
    storage_key: Bytes,
    physical_name: String,
}

impl LegacyVectorAdoptionSource {
    pub(crate) fn storage_key(&self) -> &Bytes {
        &self.storage_key
    }

    pub(crate) fn physical_name(&self) -> &str {
        &self.physical_name
    }
}

async fn load_legacy_definition_rows(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<Vec<LegacyDefinitionRow>> {
    let prefix = legacy_index_catalog::catalog_scan_prefix(scope);
    let mut rows = read.scan_prefix(prefix, ..).await?;
    let mut definitions = Vec::new();
    while let Some(row) = rows.next().await? {
        definitions.push(
            LegacyDefinitionRow::decode(scope, row.key, &row.value)
                .map_err(legacy_catalog_corruption)?,
        );
    }
    Ok(definitions)
}

/// Re-reads and proves the single exact legacy vector source for activation.
pub(crate) async fn legacy_vector_adoption_source(
    read: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    expected: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
) -> Result<LegacyVectorAdoptionSource> {
    let expected_definition =
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Vector(expected.clone());
    let expected_identity = expected_definition.identity();
    let mut source = None;
    for row in load_legacy_definition_rows(read, scope).await? {
        if row.identity.identity()? != expected_identity {
            continue;
        }
        let LegacyDynamicIndexCatalogEntry::Definition(legacy) = row.entry else {
            return Err(HelixDbError::MigrationRequired {
                reason: "legacy vector adoption source became a tombstone".to_string(),
            });
        };
        if legacy.key() != row.identity {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector adoption key/value identity mismatch".to_string(),
            ));
        }
        if legacy.into_validated()? != expected_definition {
            return Err(HelixDbError::MigrationRequired {
                reason: "legacy vector adoption definition changed before activation".to_string(),
            });
        }
        if source.is_some() {
            return Err(HelixDbError::IndexCatalogCorruption(
                "multiple legacy rows claim one vector adoption identity".to_string(),
            ));
        }
        let runtime = expected.to_runtime();
        source = Some(LegacyVectorAdoptionSource {
            storage_key: row.storage_key,
            physical_name: crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            ),
        });
    }
    source.ok_or_else(|| HelixDbError::MigrationRequired {
        reason: "legacy vector adoption source disappeared before activation".to_string(),
    })
}

#[derive(Clone)]
struct LegacyVectorPreflightSource {
    definition: crate::index_lifecycle::ValidatedVectorIndexDefinition,
    physical_name: String,
}

#[derive(Clone, Copy)]
enum V2VectorOwnerState {
    Building(crate::index_lifecycle::IndexOperationId),
    Aborting(crate::index_lifecycle::IndexOperationId),
    Active,
    Dropping,
}

struct V2VectorPhysicalOwner {
    index_id: crate::index_lifecycle::IndexId,
    generation: crate::index_lifecycle::IndexGenerationId,
    definition: crate::index_lifecycle::ValidatedVectorIndexDefinition,
    state: V2VectorOwnerState,
}

/// Reserves and cross-validates every hash-derived legacy vector namespace.
///
/// Writer open calls this before queue reconciliation or lifecycle allocation.
/// The serializable transaction either installs the complete reservation set or
/// leaves every legacy catalog and physical row unchanged.
pub(crate) async fn preflight_legacy_vector_reservations(db: &Db) -> Result<()> {
    use crate::encoding::v1::keys::vectors::{VectorMetadataScanPrefix, VectorMetadataScanRow};
    use crate::encoding::v1::values::vectors::metadata::{decode_legacy_metadata, decode_metadata};
    use crate::encoding::v2::keys::{GlobalKey, GlobalKind, RecordKind, ScopedKey};
    use crate::encoding::v2::values::{
        decode_index_record, decode_metadata_value, decode_partition_mapping, encode_metadata_value,
    };
    use crate::index_lifecycle::{
        IndexStateV2, IndexV2MetadataValue, LegacyVectorPhysicalReservation, PhysicalGeneration,
        ValidatedDynamicIndexDefinition, VectorPhysicalIndexId, VectorPhysicalLayout,
    };
    use crate::search::vector::{self, VectorIndex};

    let scope = DataScope::LegacyUnscoped;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let reservation_prefix = GlobalKey::logical_prefix(GlobalKind::LegacyVectorPhysicalReservation);
    let mut retiring_owners = BTreeMap::new();
    let mut retiring_rows = transaction
        .scan_prefix(reservation_prefix.clone(), ..)
        .await?;
    while let Some(row) = retiring_rows.next().await? {
        let GlobalKey::LegacyVectorPhysicalReservation(physical_id) =
            GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector reservation prefix yielded another key".to_string(),
            ));
        };
        let IndexV2MetadataValue::LegacyVectorPhysicalReservation(reservation) =
            decode_metadata_value(&row.value)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector reservation contains another value kind".to_string(),
            ));
        };
        if let LegacyVectorPhysicalReservation::RetiringSource {
            index_id,
            generation,
        } = reservation
            && retiring_owners
                .insert(physical_id, (index_id, generation))
                .is_some()
        {
            return Err(HelixDbError::IndexCatalogCorruption(
                "duplicate retiring legacy vector reservation".to_string(),
            ));
        }
    }
    let metadata_prefix = Key::data_prefix(scope, VectorMetadataScanPrefix::new().to_bytes());
    let mut legacy_metadata_names = BTreeMap::<String, u64>::new();
    let mut metadata_rows = transaction.scan_prefix(metadata_prefix, ..).await?;
    while let Some(row) = metadata_rows.next().await? {
        let Some(logical) = scope.strip_key(&row.key) else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "vector metadata scan escaped its data scope".to_string(),
            ));
        };
        let key = match VectorMetadataScanPrefix::new().parse_row(logical)? {
            None => continue,
            Some(VectorMetadataScanRow::IndexMetadata(key)) => key,
            Some(VectorMetadataScanRow::TxnGuard(_)) => continue,
        };
        match decode_legacy_metadata(&row.value) {
            Ok(metadata) => {
                metadata.validated_state()?;
                if legacy_metadata_names
                    .insert(metadata.config.index_name, key.index_id())
                    .is_some()
                {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "duplicate legacy vector physical name".to_string(),
                    ));
                }
            }
            Err(legacy_error) => {
                if decode_metadata(&row.value).is_err() {
                    return Err(HelixDbError::IndexCatalogCorruption(format!(
                        "vector metadata decodes as neither legacy nor current: {legacy_error}"
                    )));
                }
            }
        }
    }
    let mut legacy_sources = BTreeMap::<VectorPhysicalIndexId, LegacyVectorPreflightSource>::new();
    for row in load_legacy_definition_rows(&transaction, scope).await? {
        let LegacyDynamicIndexCatalogEntry::Definition(legacy) = row.entry else {
            continue;
        };
        let LegacyDynamicIndexDefinition::Vector(_) = &legacy else {
            continue;
        };
        if legacy.key() != row.identity {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector preflight found a key/value identity mismatch".to_string(),
            ));
        }
        let ValidatedDynamicIndexDefinition::Vector(definition) = legacy.into_validated()? else {
            unreachable!("legacy vector variant validates as vector")
        };
        let runtime = definition.to_runtime();
        let physical_names = match runtime.tenant_property() {
            None => vec![crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            )],
            Some(tenant_property) => {
                let prefix = crate::search::vector_tenant_index_name_prefix(
                    runtime.element_type(),
                    runtime.label(),
                    runtime.property(),
                    tenant_property,
                );
                legacy_metadata_names
                    .keys()
                    .filter(|name| name.starts_with(&prefix))
                    .cloned()
                    .collect()
            }
        };
        for physical_name in physical_names {
            let physical_id = vector::index_id_from_name(&physical_name);
            if physical_id != 0
                && VectorPhysicalIndexId::new(physical_id)
                    .ok()
                    .is_some_and(|physical_id| retiring_owners.contains_key(&physical_id))
            {
                continue;
            }
            let legacy = match definition.metric() {
                vector::VectorDistanceMetric::Cosine => {
                    VectorIndex::<vector::distance::Cosine>::for_legacy_migration(
                        &physical_name,
                        scope,
                    )
                    .validate_legacy_metadata_contract(&transaction, &definition)
                    .await
                }
                vector::VectorDistanceMetric::Euclidean => {
                    VectorIndex::<vector::distance::Euclidean>::for_legacy_migration(
                        &physical_name,
                        scope,
                    )
                    .validate_legacy_metadata_contract(&transaction, &definition)
                    .await
                }
                vector::VectorDistanceMetric::Manhattan => {
                    VectorIndex::<vector::distance::Manhattan>::for_legacy_migration(
                        &physical_name,
                        scope,
                    )
                    .validate_legacy_metadata_contract(&transaction, &definition)
                    .await
                }
            };
            legacy?;
            let physical_id = legacy_metadata_names
                .get(&physical_name)
                .copied()
                .unwrap_or(physical_id);
            if physical_id != vector::index_id_from_name(&physical_name) {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "legacy vector physical name hashes to another metadata namespace".to_string(),
                ));
            }
            if physical_id == 0 {
                continue;
            }
            let physical_id = VectorPhysicalIndexId::new(physical_id)?;
            if legacy_sources
                .insert(
                    physical_id,
                    LegacyVectorPreflightSource {
                        definition: definition.clone(),
                        physical_name,
                    },
                )
                .is_some()
            {
                return Err(HelixDbError::IndexCatalogCorruption(format!(
                    "multiple legacy vector definitions hash to physical ID {}",
                    physical_id.get()
                )));
            }
        }
    }

    let mut owners = BTreeMap::<VectorPhysicalIndexId, V2VectorPhysicalOwner>::new();
    let mut active_generations = BTreeMap::new();
    let index_prefix = Key::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::IndexRecord));
    let mut index_rows = transaction.scan_prefix(index_prefix, ..).await?;
    while let Some(row) = index_rows.next().await? {
        let record = decode_index_record(&row.value)?;
        let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
            continue;
        };
        if matches!(record.state(), IndexStateV2::Active { .. })
            && active_generations
                .insert(
                    (record.index_id(), record.state().generation()),
                    definition.clone(),
                )
                .is_some()
        {
            return Err(HelixDbError::IndexCatalogCorruption(
                "multiple active vector records share one owner generation".to_string(),
            ));
        }
        let Some(PhysicalGeneration::Vector {
            layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
            ..
        }) = record.state().physical()
        else {
            continue;
        };
        let state = match record.state() {
            IndexStateV2::Building {
                build_operation_id, ..
            } => V2VectorOwnerState::Building(*build_operation_id),
            IndexStateV2::Aborting {
                build_operation_id, ..
            } => V2VectorOwnerState::Aborting(*build_operation_id),
            IndexStateV2::Active { .. } => V2VectorOwnerState::Active,
            IndexStateV2::Dropping { .. } => V2VectorOwnerState::Dropping,
            IndexStateV2::Dropped { .. } => continue,
        };
        if owners
            .insert(
                *physical_index_id,
                V2VectorPhysicalOwner {
                    index_id: record.index_id(),
                    generation: record.state().generation(),
                    definition: definition.clone(),
                    state,
                },
            )
            .is_some()
        {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "multiple V2 vector records own physical ID {}",
                physical_index_id.get()
            )));
        }
    }

    let mapping_prefix = Key::data_prefix(
        scope,
        ScopedKey::logical_prefix(RecordKind::VectorPartitionMapping),
    );
    let mut partitioned_ids = BTreeSet::new();
    let mut mapping_rows = transaction.scan_prefix(mapping_prefix, ..).await?;
    while let Some(row) = mapping_rows.next().await? {
        let mapping = decode_partition_mapping(&row.value)?;
        partitioned_ids.insert(mapping.physical_index_id);
    }

    let mut reservations = BTreeMap::new();
    let mut reservation_rows = transaction.scan_prefix(reservation_prefix, ..).await?;
    while let Some(row) = reservation_rows.next().await? {
        let GlobalKey::LegacyVectorPhysicalReservation(physical_id) =
            GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector reservation prefix yielded another key".to_string(),
            ));
        };
        let IndexV2MetadataValue::LegacyVectorPhysicalReservation(reservation) =
            decode_metadata_value(&row.value)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector reservation contains another value kind".to_string(),
            ));
        };
        if reservations.insert(physical_id, reservation).is_some() {
            return Err(HelixDbError::IndexCatalogCorruption(
                "duplicate legacy vector reservation key".to_string(),
            ));
        }
    }

    for (physical_id, reservation) in &reservations {
        let legacy = legacy_sources.get(physical_id);
        let owner = owners.get(physical_id);
        let valid = match reservation {
            LegacyVectorPhysicalReservation::LegacySource => legacy.is_some() && owner.is_none(),
            LegacyVectorPhysicalReservation::AdoptionBuilding {
                index_id,
                generation,
                operation_id,
            } => legacy.is_some_and(|legacy| {
                owner.is_some_and(|owner| {
                    owner.index_id == *index_id
                        && owner.generation == *generation
                        && owner.definition == legacy.definition
                        && matches!(
                            owner.state,
                            V2VectorOwnerState::Building(owner_operation)
                                | V2VectorOwnerState::Aborting(owner_operation)
                                if owner_operation == *operation_id
                        )
                })
            }),
            LegacyVectorPhysicalReservation::AdoptedActive {
                index_id,
                generation,
            } => {
                legacy.is_none()
                    && owner.is_some_and(|owner| {
                        owner.index_id == *index_id
                            && owner.generation == *generation
                            && matches!(
                                owner.state,
                                V2VectorOwnerState::Active | V2VectorOwnerState::Dropping
                            )
                    })
            }
            LegacyVectorPhysicalReservation::RetiringSource {
                index_id,
                generation,
            } => {
                legacy.is_none()
                    && owner.is_none()
                    && active_generations.contains_key(&(*index_id, *generation))
            }
        };
        if !valid || partitioned_ids.contains(physical_id) {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "legacy vector reservation {} has inconsistent physical ownership",
                    physical_id.get()
                ),
            });
        }
    }

    for (physical_id, legacy) in legacy_sources {
        if reservations.contains_key(&physical_id) {
            continue;
        }
        if owners.contains_key(&physical_id) || partitioned_ids.contains(&physical_id) {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "legacy vector namespace {} collides with existing V2 ownership",
                    physical_id.get()
                ),
            });
        }
        tracing::info!(
            physical_index_id = physical_id.get(),
            physical_name = legacy.physical_name,
            "reserved legacy vector physical namespace"
        );
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        trip_migration_failpoint(MigrationFailpoint::LegacyVectorReservationBefore)?;
        transaction.put(
            IndexKey::Global {
                kind: GlobalKey::LegacyVectorPhysicalReservation(physical_id),
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                LegacyVectorPhysicalReservation::LegacySource,
            )),
        )?;
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        trip_migration_failpoint(MigrationFailpoint::LegacyVectorReservationAfter)?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn converge_legacy_definition(
    db: &crate::HelixDB,
    scope: DataScope,
    definition: &crate::index_lifecycle::ValidatedDynamicIndexDefinition,
) -> Result<()> {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        unreachable!("legacy definition migration requires writer storage")
    };
    loop {
        let current = crate::index_lifecycle::repository::load_index_record(
            writer.db(),
            scope,
            &definition.identity(),
        )
        .await?;
        let Some(current) = current else {
            enqueue_legacy_definition(db, scope, definition.clone()).await?;
            continue;
        };
        if current.definition() != definition {
            return Err(HelixDbError::IndexDefinitionConflict {
                existing: Box::new(current.definition().clone()),
                requested: Box::new(definition.clone()),
                differing_fields: config::NonEmptyDefinitionDifferences::between(
                    current.definition(),
                    definition,
                )
                .expect("unequal validated definitions have non-empty differences"),
            });
        }
        match current.state() {
            crate::index_lifecycle::IndexStateV2::Active { .. } => return Ok(()),
            crate::index_lifecycle::IndexStateV2::Dropped { .. } => {
                enqueue_legacy_definition(db, scope, definition.clone()).await?;
            }
            crate::index_lifecycle::IndexStateV2::Building {
                build_operation_id, ..
            } => {
                if matches!(
                    definition,
                    crate::index_lifecycle::ValidatedDynamicIndexDefinition::Text(_)
                ) && matches!(
                    db.get_index_operation(scope, *build_operation_id).await?,
                    crate::index_lifecycle::IndexOperationStatus::Blocked {
                        blocker_code:
                            crate::index_lifecycle::IndexOperationBlockerCode::InvalidSourceData,
                        ..
                    }
                ) {
                    db.retry_index_operation(scope, *build_operation_id).await?;
                }
                wait_for_index_operation(
                    db,
                    scope,
                    *build_operation_id,
                    matches!(
                        definition,
                        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Secondary(_)
                    ),
                )
                .await?;
            }
            crate::index_lifecycle::IndexStateV2::Aborting {
                build_operation_id, ..
            } => {
                wait_for_index_operation(
                    db,
                    scope,
                    *build_operation_id,
                    matches!(
                        definition,
                        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Secondary(_)
                    ),
                )
                .await?
            }
            crate::index_lifecycle::IndexStateV2::Dropping {
                drop_operation_id, ..
            } => {
                wait_for_index_operation(
                    db,
                    scope,
                    *drop_operation_id,
                    matches!(
                        definition,
                        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Secondary(_)
                    ),
                )
                .await?
            }
        }
    }
}

async fn enqueue_legacy_definition(
    db: &crate::HelixDB,
    scope: DataScope,
    definition: crate::index_lifecycle::ValidatedDynamicIndexDefinition,
) -> Result<()> {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        unreachable!("legacy definition migration requires writer storage")
    };
    let family = match definition.family() {
        crate::index_lifecycle::IndexDefinitionFamily::Secondary => {
            crate::error::IndexFamily::Secondary
        }
        crate::index_lifecycle::IndexDefinitionFamily::Vector => crate::error::IndexFamily::Vector,
        crate::index_lifecycle::IndexDefinitionFamily::Text => crate::error::IndexFamily::Text,
    };
    let is_secondary = matches!(
        &definition,
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Secondary(_)
    );
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    if matches!(
        &definition,
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Text(_)
    ) {
        trip_legacy_text_migration_checkpoint(LegacyTextMigrationCheckpoint::BeforeEnqueue)?;
    }
    if let Some(reason) = db.index_lifecycle_unavailable_reason(family) {
        return Err(HelixDbError::IndexLifecycleUnavailable { family, reason });
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::LegacyDefinitionEnqueueBefore)?;
    let adoption_physical_id = match &definition {
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Vector(vector)
            if vector.tenant_property().is_none() =>
        {
            let runtime = vector.to_runtime();
            let physical_name = crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            );
            let raw_physical_id = crate::search::vector::index_id_from_name(&physical_name);
            let watermark =
                crate::index_lifecycle::repository::load_vector_physical_watermark(writer.db())
                    .await?;
            match watermark.eligible_legacy_source(raw_physical_id) {
                None => None,
                Some(physical_id) => {
                    match crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                        writer.db(),
                        physical_id,
                    )
                    .await?
                    {
                        Some(crate::index_lifecycle::LegacyVectorPhysicalReservation::LegacySource) => {
                            Some(physical_id)
                        }
                        Some(_) => {
                            return Err(HelixDbError::MigrationRequired {
                                reason: "eligible legacy vector reservation is already owned"
                                    .to_string(),
                            })
                        }
                        None => {
                            return Err(HelixDbError::MigrationRequired {
                                reason: "eligible legacy vector has no preflight reservation"
                                    .to_string(),
                            })
                        }
                    }
                }
            }
        }
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Secondary(_)
        | crate::index_lifecycle::ValidatedDynamicIndexDefinition::Vector(_)
        | crate::index_lifecycle::ValidatedDynamicIndexDefinition::Text(_) => None,
    };
    let receipt = match adoption_physical_id {
        Some(physical_id) => {
            tracing::info!(
                physical_index_id = physical_id.get(),
                "enqueuing in-place legacy vector adoption"
            );
            crate::index_lifecycle::lifecycle::create_legacy_vector_adoption_operation(
                writer.db(),
                scope,
                definition,
                physical_id,
            )
            .await?
        }
        None => enqueue_rebuild(writer.db(), scope, definition).await?,
    };
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::LegacyDefinitionEnqueueAfter)?;
    db.wake_index_worker().await;
    if let Some(operation_id) = receipt_operation_id(receipt) {
        wait_for_index_operation(db, scope, operation_id, is_secondary).await?;
    }
    Ok(())
}

async fn enqueue_rebuild(
    db: &Db,
    scope: DataScope,
    definition: crate::index_lifecycle::ValidatedDynamicIndexDefinition,
) -> Result<crate::index_lifecycle::IndexDdlReceipt> {
    tracing::info!(family = ?definition.family(), "enqueuing legacy definition rebuild");
    crate::index_lifecycle::lifecycle::create_index_operation_from_current_source(
        db,
        scope,
        definition,
        helix_planner::ir::IndexCreateMode::IfNotExists,
    )
    .await
}

async fn converge_legacy_tombstone(
    db: &crate::HelixDB,
    scope: DataScope,
    identity: &LegacyDynamicIndexKey,
) -> Result<()> {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        unreachable!("legacy definition migration requires writer storage")
    };
    loop {
        let (receipt, is_secondary) = {
            let _catalog_permit = db
                .inner
                .index_scope_gates
                .catalog_change_permit(scope)
                .await;
            let Some(current) = crate::index_lifecycle::repository::load_index_record(
                writer.db(),
                scope,
                &identity.identity()?,
            )
            .await?
            else {
                return Ok(());
            };
            if matches!(
                current.state(),
                crate::index_lifecycle::IndexStateV2::Dropped { .. }
            ) {
                return Ok(());
            }
            let is_secondary = matches!(
                current.definition(),
                crate::index_lifecycle::ValidatedDynamicIndexDefinition::Secondary(_)
            );
            let receipt = crate::index_lifecycle::lifecycle::drop_index_operation(
                writer.db(),
                scope,
                current.definition(),
            )
            .await?;
            (receipt, is_secondary)
        };
        db.wake_index_worker().await;
        if let Some(operation_id) = receipt_operation_id(receipt) {
            wait_for_index_operation(db, scope, operation_id, is_secondary).await?;
        }
    }
}

fn receipt_operation_id(
    receipt: crate::index_lifecycle::IndexDdlReceipt,
) -> Option<crate::index_lifecycle::IndexOperationId> {
    match receipt {
        crate::index_lifecycle::IndexDdlReceipt::Accepted { operation_id, .. }
        | crate::index_lifecycle::IndexDdlReceipt::ExistingOperation { operation_id } => {
            Some(operation_id)
        }
        crate::index_lifecycle::IndexDdlReceipt::AlreadyActive { .. } => None,
    }
}

async fn wait_for_index_operation(
    db: &crate::HelixDB,
    scope: DataScope,
    operation_id: crate::index_lifecycle::IndexOperationId,
    drive_disabled_secondary: bool,
) -> Result<()> {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        unreachable!("legacy definition migration requires writer storage")
    };
    let writer_epoch = db.index_worker_epoch().await?;
    loop {
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        check_legacy_text_migration_interruption()?;
        let snapshot = writer.db().snapshot().await?;
        let Some(operation) =
            crate::index_lifecycle::outbox::read_operation(snapshot.as_ref(), scope, operation_id)
                .await?
        else {
            return Err(HelixDbError::IndexOperationNotFound {
                operation_id: operation_id.as_uuid().to_string(),
            });
        };
        if operation
            .queue_schedule()
            .is_some_and(|schedule| schedule.transient_failure_from(writer_epoch))
        {
            let status = crate::index_lifecycle::IndexOperationStatus::from_record(&operation);
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "legacy definition operation {operation_id:?} encountered a transient lifecycle failure at {:?} for {:?} in the current writer epoch after attempt {}",
                    status.common().stage,
                    status.common().family,
                    status.common().attempt,
                ),
            });
        }
        match crate::index_lifecycle::IndexOperationStatus::from_record(&operation) {
            crate::index_lifecycle::IndexOperationStatus::Succeeded { .. }
            | crate::index_lifecycle::IndexOperationStatus::Aborted { .. } => return Ok(()),
            crate::index_lifecycle::IndexOperationStatus::Blocked {
                common,
                blocker_code,
                ..
            } => {
                return Err(HelixDbError::MigrationRequired {
                    reason: format!(
                        "legacy definition operation {operation_id:?} is blocked at {:?} for {:?}: {blocker_code:?}",
                        common.stage, common.family
                    ),
                })
            }
            crate::index_lifecycle::IndexOperationStatus::Queued { .. }
            | crate::index_lifecycle::IndexOperationStatus::Running { .. } => {
                if drive_disabled_secondary
                    && db
                        .config()
                        .db()
                        .secondary_index_lifecycle()
                        .worker_mode()
                        == config::SecondaryIndexLifecycleWorkerMode::Disabled
                {
                    if db.process_secondary_index_lifecycle_once().await? {
                        continue;
                    }
                    let status = db.get_index_operation(scope, operation_id).await?;
                    match status {
                        crate::index_lifecycle::IndexOperationStatus::Succeeded { .. }
                        | crate::index_lifecycle::IndexOperationStatus::Aborted { .. } => return Ok(()),
                        crate::index_lifecycle::IndexOperationStatus::Blocked {
                            common,
                            blocker_code,
                            ..
                        } => {
                            return Err(HelixDbError::MigrationRequired {
                                reason: format!(
                                    "legacy definition operation {operation_id:?} is blocked at {:?} for {:?}: {blocker_code:?}",
                                    common.stage, common.family
                                ),
                            })
                        }
                        crate::index_lifecycle::IndexOperationStatus::Queued { common }
                        | crate::index_lifecycle::IndexOperationStatus::Running { common } => {
                            return Err(HelixDbError::MigrationRequired {
                                reason: format!(
                                    "legacy definition operation {operation_id:?} remains nonterminal after a complete Disabled-mode queue scan at {:?} for {:?}",
                                    common.stage, common.family
                                ),
                            })
                        }
                    }
                }
                db.wake_index_worker().await;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
}

async fn retire_legacy_definition_row(
    db: &Db,
    scope: DataScope,
    storage_key: Bytes,
    definition: Option<&crate::index_lifecycle::ValidatedDynamicIndexDefinition>,
    identity: &LegacyDynamicIndexKey,
) -> Result<()> {
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::LegacyDefinitionRetirementBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let legacy_row_present = transaction.get(&storage_key).await?.is_some();
    let current = crate::index_lifecycle::repository::load_index_record(
        &transaction,
        scope,
        &identity.identity()?,
    )
    .await?;
    match (definition, current.as_ref()) {
        (Some(expected), Some(current))
            if current.definition() == expected
                && matches!(
                    current.state(),
                    crate::index_lifecycle::IndexStateV2::Active { .. }
                ) =>
        {
            if legacy_row_present {
                retire_legacy_physical_rows(&transaction, scope, expected).await?;
            } else {
                let Some(crate::index_lifecycle::PhysicalGeneration::Vector {
                    generation,
                    layout:
                        crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                            physical_index_id,
                        },
                    ..
                }) = current.state().physical()
                else {
                    return Err(HelixDbError::MigrationRequired {
                        reason: "legacy definition disappeared without adopted vector ownership"
                            .to_string(),
                    });
                };
                if !matches!(
                    expected,
                    crate::index_lifecycle::ValidatedDynamicIndexDefinition::Vector(definition)
                        if definition.tenant_property().is_none()
                ) || crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                    &transaction,
                    *physical_index_id,
                )
                .await?
                    != Some(
                        crate::index_lifecycle::LegacyVectorPhysicalReservation::AdoptedActive {
                            index_id: current.index_id(),
                            generation: *generation,
                        },
                    )
                {
                    return Err(HelixDbError::MigrationRequired {
                        reason:
                            "legacy definition disappeared without exact adopted vector ownership"
                                .to_string(),
                    });
                }
            }
        }
        (None, None) if legacy_row_present => {}
        (None, Some(current))
            if legacy_row_present
                && matches!(
                    current.state(),
                    crate::index_lifecycle::IndexStateV2::Dropped { .. }
                ) => {}
        _ => {
            return Err(HelixDbError::MigrationRequired {
                reason: "legacy definition retirement lost exact V2 ownership".to_string(),
            })
        }
    }
    if legacy_row_present {
        transaction.delete(storage_key)?;
    }
    transaction.commit().await?;
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::LegacyDefinitionRetirementAfter)?;
    Ok(())
}

async fn retire_legacy_physical_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    definition: &crate::index_lifecycle::ValidatedDynamicIndexDefinition,
) -> Result<()> {
    match definition {
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Secondary(definition) => {
            let runtime = definition.to_runtime();
            let property = runtime.scoped_property();
            match (runtime.element_type(), runtime.kind()) {
                (config::SecondaryIndexElementType::Node, config::SecondaryIndexKind::Equality) => {
                    crate::search::delete_equality_index_entries_for_property(
                        transaction,
                        &property,
                    )
                    .await?;
                }
                (config::SecondaryIndexElementType::Node, config::SecondaryIndexKind::Range) => {
                    crate::search::delete_range_index_entries_for_property_with_direction(
                        transaction,
                        &property,
                        legacy_range_direction(runtime.direction()),
                    )
                    .await?;
                }
                (config::SecondaryIndexElementType::Edge, config::SecondaryIndexKind::Equality) => {
                    crate::search::delete_edge_equality_index_entries_for_property(
                        transaction,
                        &property,
                    )
                    .await?;
                    crate::search::delete_global_edge_equality_index_entries_for_property(
                        transaction,
                        &property,
                    )
                    .await?;
                }
                (config::SecondaryIndexElementType::Edge, config::SecondaryIndexKind::Range) => {
                    let direction = legacy_range_direction(runtime.direction());
                    crate::search::delete_edge_range_index_entries_for_property_with_direction(
                        transaction,
                        &property,
                        direction,
                    )
                    .await?;
                    crate::search::delete_global_edge_range_index_entries_for_property_with_direction(
                        transaction,
                        &property,
                        direction,
                    )
                    .await?;
                }
            }
        }
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Vector(_) => {
            return Err(HelixDbError::InvariantViolation(
                "vector definitions must retire through the bounded physical cleanup job"
                    .to_string(),
            ));
        }
        crate::index_lifecycle::ValidatedDynamicIndexDefinition::Text(definition) => {
            retire_legacy_text_rows(transaction, scope, &definition.to_runtime()).await?;
        }
    }
    Ok(())
}

async fn retire_legacy_text_rows(
    transaction: &DbTransaction,
    scope: DataScope,
    definition: &config::TextIndexDefinition,
) -> Result<()> {
    for (manifest_key, manifest) in
        crate::search::text::load_manifests_for_definition_scoped(transaction, scope, definition)
            .await?
    {
        transaction.delete(manifest_key)?;
        for prefix in [
            crate::encoding::v2::legacy::text::storage_keys::live_state_prefix(
                scope,
                &manifest.physical_index_name,
            ),
            crate::encoding::v2::legacy::text::storage_keys::transaction_guard_key(
                scope,
                &manifest.physical_index_name,
            ),
            crate::encoding::v2::legacy::text::storage_keys::version_counter_key(
                scope,
                &manifest.physical_index_name,
            ),
        ] {
            let mut rows = transaction.scan_prefix(prefix, ..).await?;
            while let Some(row) = rows.next().await? {
                transaction.delete(row.key)?;
            }
        }
    }
    Ok(())
}

const fn legacy_range_direction(
    direction: config::RangeIndexDirection,
) -> crate::encoding::v1::indexes::range::RangeIndexDirection {
    match direction {
        config::RangeIndexDirection::Asc => {
            crate::encoding::v1::indexes::range::RangeIndexDirection::Asc
        }
        config::RangeIndexDirection::Desc => {
            crate::encoding::v1::indexes::range::RangeIndexDirection::Desc
        }
    }
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MigrationParityId {
    GraphFormatV1Rewrite,
    LegacyVectorPropertyMaterialization,
    LegacyVectorPhysicalCleanup,
    GraphFormatV1Cleanup,
    VectorSimHashDirectoryV1,
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MigrationParityMode {
    BlockingStartup,
    Background,
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MigrationParityStage {
    PropertyIndexes,
    NodeProperties,
    LegacyEdgePairs,
    EdgeEndpoints,
    FenceLegacyVectorSources,
    LegacyVectorHotRows,
    LegacyVectorLayer0Rows,
    LegacyVectorCoreRows,
    LegacyVectorDefinitions,
    ReleaseLegacyVectorReservations,
    VectorDirectorySelectTarget,
    VectorDirectoryPreflight,
    VectorDirectoryBackfill,
    VectorDirectoryVerify,
    VectorDirectoryPublish,
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum MigrationParityState {
    Running {
        stage: MigrationParityStage,
        processed_rows: u64,
        has_resume_key: bool,
    },
    Completed {
        processed_rows: u64,
    },
    Failed {
        stage: MigrationParityStage,
        processed_rows: u64,
        has_resume_key: bool,
        error: String,
    },
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
impl MigrationParityState {
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    pub const fn processed_rows(&self) -> u64 {
        match self {
            Self::Running { processed_rows, .. }
            | Self::Completed { processed_rows }
            | Self::Failed { processed_rows, .. } => *processed_rows,
        }
    }
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MigrationParityJobStatus {
    pub id: MigrationParityId,
    pub mode: MigrationParityMode,
    pub state: MigrationParityState,
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
const fn parity_id(id: MigrationId) -> MigrationParityId {
    match id {
        MigrationId::GraphFormatV1Rewrite => MigrationParityId::GraphFormatV1Rewrite,
        MigrationId::LegacyVectorPropertyMaterialization => {
            MigrationParityId::LegacyVectorPropertyMaterialization
        }
        MigrationId::LegacyVectorPhysicalCleanup => MigrationParityId::LegacyVectorPhysicalCleanup,
        MigrationId::GraphFormatV1Cleanup => MigrationParityId::GraphFormatV1Cleanup,
    }
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
const fn parity_mode(mode: MigrationMode) -> MigrationParityMode {
    match mode {
        MigrationMode::BlockingStartup => MigrationParityMode::BlockingStartup,
        MigrationMode::Background => MigrationParityMode::Background,
    }
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
const fn parity_stage(stage: MigrationStage) -> MigrationParityStage {
    match stage {
        MigrationStage::PropertyIndexes => MigrationParityStage::PropertyIndexes,
        MigrationStage::NodeProperties => MigrationParityStage::NodeProperties,
        MigrationStage::LegacyEdgePairs => MigrationParityStage::LegacyEdgePairs,
        MigrationStage::EdgeEndpoints => MigrationParityStage::EdgeEndpoints,
        MigrationStage::FenceLegacyVectorSources => MigrationParityStage::FenceLegacyVectorSources,
        MigrationStage::LegacyVectorHotRows => MigrationParityStage::LegacyVectorHotRows,
        MigrationStage::LegacyVectorLayer0Rows => MigrationParityStage::LegacyVectorLayer0Rows,
        MigrationStage::LegacyVectorCoreRows => MigrationParityStage::LegacyVectorCoreRows,
        MigrationStage::LegacyVectorDefinitions => MigrationParityStage::LegacyVectorDefinitions,
        MigrationStage::ReleaseLegacyVectorReservations => {
            MigrationParityStage::ReleaseLegacyVectorReservations
        }
    }
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
fn parity_state(state: &MigrationJobState) -> MigrationParityState {
    match state {
        MigrationJobState::Running {
            stage,
            resume_after_key,
            processed_rows,
        } => MigrationParityState::Running {
            stage: parity_stage(*stage),
            processed_rows: *processed_rows,
            has_resume_key: resume_after_key.is_some(),
        },
        MigrationJobState::Completed { processed_rows } => MigrationParityState::Completed {
            processed_rows: *processed_rows,
        },
        MigrationJobState::Failed {
            stage,
            resume_after_key,
            processed_rows,
            error,
        } => MigrationParityState::Failed {
            stage: parity_stage(*stage),
            processed_rows: *processed_rows,
            has_resume_key: resume_after_key.is_some(),
            error: error.clone(),
        },
    }
}

#[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
pub async fn migration_parity_job_statuses(
    db: &Db,
    scope: DataScope,
) -> Result<Vec<MigrationParityJobStatus>> {
    let prefix = migration_job_scan_prefix_scoped(scope);
    let mut iter = db.scan_prefix(prefix, ..).await?;
    let mut statuses = Vec::new();
    while let Some(kv) = iter.next().await? {
        let job = decode_json::<MigrationJob>(&kv.value)?;
        statuses.push(MigrationParityJobStatus {
            id: parity_id(job.id),
            mode: parity_mode(job.mode),
            state: parity_state(&job.state),
        });
    }
    if let Some(status) = vector_simhash_directory::parity_status(db, scope).await? {
        statuses.push(status);
    }
    statuses.sort_by_key(|status| status.id);
    Ok(statuses)
}

/// Process one migration batch for the requested migration id.
pub(crate) async fn process_migration_once_by_id(
    writer: &HelixWriter,
    scope: DataScope,
    tuning: config::MigrationTuning,
    id: MigrationId,
) -> Result<bool> {
    match id {
        MigrationId::LegacyVectorPropertyMaterialization => {
            let catalog =
                vector_properties::LegacyVectorPropertyCatalog::load(writer.db(), scope).await?;
            process_migration_once_by_id_with_catalog(
                writer,
                scope,
                tuning,
                id,
                MigrationRunCatalog::VectorProperties(&catalog),
            )
            .await
        }
        MigrationId::LegacyVectorPhysicalCleanup => {
            let catalog =
                vector_retirement::LegacyVectorRetirementCatalog::load(writer.db(), scope).await?;
            process_migration_once_by_id_with_catalog(
                writer,
                scope,
                tuning,
                id,
                MigrationRunCatalog::VectorRetirement(&catalog),
            )
            .await
        }
        MigrationId::GraphFormatV1Rewrite | MigrationId::GraphFormatV1Cleanup => {
            process_migration_once_by_id_with_catalog(
                writer,
                scope,
                tuning,
                id,
                MigrationRunCatalog::None,
            )
            .await
        }
    }
}

#[derive(Clone, Copy)]
enum MigrationRunCatalog<'a> {
    None,
    VectorProperties(&'a vector_properties::LegacyVectorPropertyCatalog),
    VectorRetirement(&'a vector_retirement::LegacyVectorRetirementCatalog),
}

/// One migration controller turn before its enclosing transaction commits.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(
    not(feature = "production-scale"),
    allow(
        dead_code,
        reason = "row and byte observations are consumed only by the production scale harness"
    )
)]
struct MigrationStep {
    advanced: bool,
    rows: u64,
    admitted_bytes: u64,
}

impl MigrationStep {
    const IDLE: Self = Self {
        advanced: false,
        rows: 0,
        admitted_bytes: 0,
    };
}

async fn process_migration_once_by_id_with_catalog(
    writer: &HelixWriter,
    scope: DataScope,
    tuning: config::MigrationTuning,
    id: MigrationId,
    catalog: MigrationRunCatalog<'_>,
) -> Result<bool> {
    Ok(
        process_migration_once_by_id_with_catalog_measured(writer, scope, tuning, id, catalog)
            .await?
            .advanced,
    )
}

async fn process_migration_once_by_id_with_catalog_measured(
    writer: &HelixWriter,
    scope: DataScope,
    tuning: config::MigrationTuning,
    id: MigrationId,
    catalog: MigrationRunCatalog<'_>,
) -> Result<MigrationStep> {
    let started = Instant::now();
    let key = MigrationJobKey::new(scope, id);
    let txn = writer.db().begin(IsolationLevel::Snapshot).await?;
    let Some(value) = txn.get(key.as_ref()).await? else {
        txn.rollback();
        return Ok(MigrationStep::IDLE);
    };
    let mut job = decode_json::<MigrationJob>(&value)?;
    if job.is_failed() {
        tracing::info!(
            migration_id = id.log_name(),
            scope = ?scope,
            processed_rows = job.state.processed_rows(),
            "retrying failed kv migration from its last durable resume point"
        );
        job.retry();
    }
    if !job.is_runnable() {
        txn.rollback();
        return Ok(MigrationStep::IDLE);
    }

    let result = process_loaded_job(&txn, writer, scope, tuning, &mut job, catalog).await;
    match result {
        Ok(step) => {
            let rewrite_completed = id == MigrationId::GraphFormatV1Rewrite && job.is_completed();
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            if rewrite_completed {
                trip_migration_failpoint(MigrationFailpoint::RewriteCompletionBefore)?;
            }
            txn.put(key.into_bytes(), encode_json(&job)?)?;
            if rewrite_completed {
                txn.put(
                    scoped_metadata_key(scope, GRAPH_FORMAT_V1_READY),
                    Bytes::from_static(b"1"),
                )?;
            }
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            trip_migration_failpoint(MigrationFailpoint::BatchCommitBefore)?;
            txn.commit().await?;
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            trip_migration_failpoint(MigrationFailpoint::BatchCommitAfter)?;
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            if rewrite_completed {
                trip_migration_failpoint(MigrationFailpoint::RewriteCompletionAfter)?;
            }
            Ok(step)
        }
        Err(err) => {
            let stage = job.state.running_stage();
            tracing::warn!(
                migration_version = 1,
                migration_step = stage.map(MigrationStage::log_name),
                migration_outcome = "failed",
                duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                scanned_count = 0_u64,
                written_count = 0_u64,
                migration_id = id.log_name(),
                migration_stage = stage.map(MigrationStage::log_name),
                scope = ?scope,
                error = %err,
                "kv migration batch failed; rolling back batch transaction"
            );
            job.fail(err.to_string());
            txn.rollback();
            let failed_txn = writer.db().begin(IsolationLevel::Snapshot).await?;
            failed_txn.put(
                MigrationJobKey::new(scope, id).into_bytes(),
                encode_json(&job)?,
            )?;
            failed_txn.commit().await?;
            Err(err)
        }
    }
}

/// Process one runnable background migration job.
#[cfg(any(test, feature = "migration-parity", feature = "production-coverage"))]
pub(crate) async fn process_migration_once(
    writer: &HelixWriter,
    scope: DataScope,
    tuning: config::MigrationTuning,
) -> Result<bool> {
    let prefix = migration_job_scan_prefix_scoped(scope);
    let mut iter = writer.db().scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        let job = decode_json::<MigrationJob>(&kv.value)?;
        if job.mode != MigrationMode::Background || (!job.is_runnable() && !job.is_failed()) {
            continue;
        }
        return process_migration_once_by_id(writer, scope, tuning, job.id).await;
    }
    Ok(false)
}

async fn process_loaded_job(
    txn: &DbTransaction,
    writer: &HelixWriter,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &mut MigrationJob,
    catalog: MigrationRunCatalog<'_>,
) -> Result<MigrationStep> {
    let started = Instant::now();
    let Some(stage) = job.state.running_stage() else {
        return Ok(MigrationStep::IDLE);
    };
    let id = job.id;
    let batch = match job.id {
        MigrationId::GraphFormatV1Rewrite => match stage {
            MigrationStage::PropertyIndexes => {
                clear_property_index_batch(txn, scope, tuning, job).await?
            }
            MigrationStage::NodeProperties => {
                rebuild_node_property_batch(txn, scope, tuning, job).await?
            }
            MigrationStage::LegacyEdgePairs => {
                rewrite_legacy_edge_pair_batch(txn, writer, scope, tuning, job).await?
            }
            MigrationStage::EdgeEndpoints => {
                rebuild_edge_endpoint_batch(txn, scope, tuning, job).await?
            }
            MigrationStage::FenceLegacyVectorSources
            | MigrationStage::LegacyVectorHotRows
            | MigrationStage::LegacyVectorLayer0Rows
            | MigrationStage::LegacyVectorCoreRows
            | MigrationStage::LegacyVectorDefinitions
            | MigrationStage::ReleaseLegacyVectorReservations => {
                return Err(HelixDbError::InvariantViolation(
                    "graph rewrite job persisted a vector cleanup stage".to_string(),
                ));
            }
        },
        MigrationId::LegacyVectorPropertyMaterialization => {
            let MigrationRunCatalog::VectorProperties(catalog) = catalog else {
                return Err(HelixDbError::InvariantViolation(
                    "legacy vector-property job has no immutable definition catalog".to_string(),
                ));
            };
            match stage {
                MigrationStage::NodeProperties => {
                    vector_properties::materialize_node_batch(txn, scope, tuning, job, catalog)
                        .await?
                }
                MigrationStage::EdgeEndpoints => {
                    vector_properties::materialize_edge_batch(txn, scope, tuning, job, catalog)
                        .await?
                }
                MigrationStage::PropertyIndexes
                | MigrationStage::LegacyEdgePairs
                | MigrationStage::FenceLegacyVectorSources
                | MigrationStage::LegacyVectorHotRows
                | MigrationStage::LegacyVectorLayer0Rows
                | MigrationStage::LegacyVectorCoreRows
                | MigrationStage::LegacyVectorDefinitions
                | MigrationStage::ReleaseLegacyVectorReservations => {
                    return Err(HelixDbError::InvariantViolation(
                        "legacy vector-property job persisted an illegal stage".to_string(),
                    ));
                }
            }
        }
        MigrationId::LegacyVectorPhysicalCleanup => {
            let MigrationRunCatalog::VectorRetirement(catalog) = catalog else {
                return Err(HelixDbError::InvariantViolation(
                    "legacy vector cleanup job has no immutable retirement catalog".to_string(),
                ));
            };
            match stage {
                MigrationStage::FenceLegacyVectorSources => {
                    vector_retirement::fence_sources_batch(txn, scope, tuning, job, catalog).await?
                }
                MigrationStage::LegacyVectorHotRows => {
                    vector_retirement::delete_dedicated_lane_batch(
                        txn,
                        scope,
                        tuning,
                        job,
                        crate::encoding::v1::keys::vectors::VectorStorageLane::Hot,
                    )
                    .await?
                }
                MigrationStage::LegacyVectorLayer0Rows => {
                    vector_retirement::delete_dedicated_lane_batch(
                        txn,
                        scope,
                        tuning,
                        job,
                        crate::encoding::v1::keys::vectors::VectorStorageLane::Layer0,
                    )
                    .await?
                }
                MigrationStage::LegacyVectorCoreRows => {
                    vector_retirement::delete_core_batch(txn, scope, tuning, job).await?
                }
                MigrationStage::LegacyVectorDefinitions => {
                    vector_retirement::delete_definitions_batch(txn, scope, tuning, job, catalog)
                        .await?
                }
                MigrationStage::ReleaseLegacyVectorReservations => {
                    vector_retirement::release_reservations_batch(txn, scope, tuning, job, catalog)
                        .await?
                }
                MigrationStage::PropertyIndexes
                | MigrationStage::NodeProperties
                | MigrationStage::LegacyEdgePairs
                | MigrationStage::EdgeEndpoints => {
                    return Err(HelixDbError::InvariantViolation(
                        "legacy vector cleanup job persisted an unrelated stage".to_string(),
                    ));
                }
            }
        }
        MigrationId::GraphFormatV1Cleanup => match stage {
            MigrationStage::LegacyEdgePairs => {
                cleanup_legacy_edge_pair_batch(txn, scope, tuning, job).await?
            }
            MigrationStage::PropertyIndexes
            | MigrationStage::NodeProperties
            | MigrationStage::EdgeEndpoints => MigrationBatch::StageComplete,
            MigrationStage::FenceLegacyVectorSources
            | MigrationStage::LegacyVectorHotRows
            | MigrationStage::LegacyVectorLayer0Rows
            | MigrationStage::LegacyVectorCoreRows
            | MigrationStage::LegacyVectorDefinitions
            | MigrationStage::ReleaseLegacyVectorReservations => {
                return Err(HelixDbError::InvariantViolation(
                    "graph cleanup job persisted a vector cleanup stage".to_string(),
                ));
            }
        },
    };

    match batch {
        MigrationBatch::Advanced {
            resume_after_key,
            rows,
            source_bytes,
        } => {
            job.record_advanced(resume_after_key, rows);
            tracing::debug!(
                migration_version = 1,
                migration_step = stage.log_name(),
                migration_outcome = "applied",
                duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                scanned_count = rows,
                written_count = rows,
                migration_id = id.log_name(),
                migration_stage = stage.log_name(),
                scope = ?scope,
                batch_rows = rows,
                batch_source_bytes = source_bytes,
                processed_rows = job.state.processed_rows(),
                has_resume_key = true,
                "kv migration batch committed"
            );
            Ok(MigrationStep {
                advanced: true,
                rows,
                admitted_bytes: source_bytes,
            })
        }
        MigrationBatch::StageComplete => {
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            trip_migration_failpoint(MigrationFailpoint::StageTransitionBefore)?;
            advance_or_complete(job);
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            trip_migration_failpoint(MigrationFailpoint::StageTransitionAfter)?;
            tracing::info!(
                migration_version = 1,
                migration_step = stage.log_name(),
                migration_outcome = "applied",
                duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                scanned_count = 0,
                written_count = 0,
                migration_id = id.log_name(),
                migration_stage = stage.log_name(),
                scope = ?scope,
                processed_rows = job.state.processed_rows(),
                state = job.state.log_name(),
                next_stage = job.state.running_stage().map(MigrationStage::log_name),
                "kv migration stage completed"
            );
            Ok(MigrationStep {
                advanced: true,
                rows: 0,
                admitted_bytes: 0,
            })
        }
    }
}

async fn clear_property_index_batch(
    txn: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
) -> Result<MigrationBatch> {
    scan_stage_batch(
        txn,
        scope,
        MigrationStage::PropertyIndexes,
        tuning,
        MigrationRowLimit::Tuned,
        job,
        |key, _value| async move {
            let Some(logical) = scope.strip_key(&key) else {
                return Err(HelixDbError::InvariantViolation(
                    "property-index migration key does not match its tenant scope".to_string(),
                ));
            };
            if VectorKey::is_vector_keyspace(logical) {
                match VectorKey::parse_from_slice(logical) {
                    Ok(_) => {
                        // Legacy vector rows are owned by the subsequent V2
                        // definition migration. Preserve them here instead of
                        // admitting a legacy metadata codec into live runtime.
                        return Ok(());
                    }
                    Err(error) if matches!(logical.first(), Some(&0xF0) | Some(&0xF1)) => {
                        return Err(HelixDbError::Config(format!(
                            "malformed dedicated vector key during migration: {error}"
                        )));
                    }
                    // Default-keyspace secondary index rows share the 0x03,
                    // 0x03 prefix with vector metadata. Non-vector shapes fall
                    // through to the normal clear-and-rebuild path.
                    Err(_) => {}
                }
            }
            if logical.first().copied() != Some(KeyPrefix::PropertyIndex.as_u8()) {
                return Err(HelixDbError::InvariantViolation(
                    "property-index migration scan returned a non-index key".to_string(),
                ));
            }
            // Persisted legacy definitions still own these physical rows until
            // their exact canonical V2 generation reaches `Active`.
            Ok(())
        },
    )
    .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MigrationBatch {
    Advanced {
        resume_after_key: MigrationResumeKey,
        rows: u64,
        source_bytes: u64,
    },
    StageComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationRowLimit {
    Tuned,
    ByteBudgetOnly,
}

impl MigrationRowLimit {
    fn admits(self, scanned_rows: u64, tuning: config::MigrationTuning) -> bool {
        match self {
            Self::Tuned => {
                scanned_rows < u64::try_from(tuning.batch_rows().get()).unwrap_or(u64::MAX)
            }
            Self::ByteBudgetOnly => true,
        }
    }
}

async fn rebuild_node_property_batch(
    txn: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
) -> Result<MigrationBatch> {
    scan_stage_batch(
        txn,
        scope,
        MigrationStage::NodeProperties,
        tuning,
        MigrationRowLimit::Tuned,
        job,
        |key, _value| async move {
            let Key::Data {
                kind: DataKeyKind::NodeProperty(node_key),
                ..
            } = Key::parse_from_slice(scope, &key)?
            else {
                return Ok(());
            };
            let _ = node_key;
            Ok(())
        },
    )
    .await
}

async fn rewrite_legacy_edge_pair_batch(
    txn: &DbTransaction,
    writer: &HelixWriter,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
) -> Result<MigrationBatch> {
    let source = read_stage_batch(
        txn,
        scope,
        MigrationStage::LegacyEdgePairs,
        tuning,
        MigrationRowLimit::Tuned,
        job,
    )
    .await?;
    let MigrationSourceBatch::Advanced {
        rows: source_rows,
        resume_after_key,
        source_bytes,
    } = source
    else {
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        trip_migration_failpoint(MigrationFailpoint::BatchReadAfter)?;
        return Ok(MigrationBatch::StageComplete);
    };

    let source_row_count = source_rows.len();
    let mut rows = Vec::with_capacity(source_row_count);
    for (key, value) in source_rows {
        let Key::Data {
            kind: DataKeyKind::EdgePropertyPair(pair_key),
            ..
        } = Key::parse_from_slice(scope, &key)?
        else {
            continue;
        };
        rows.push(LegacyEdgePairMigrationRow {
            from: pair_key.from(),
            to: pair_key.to(),
            properties: decode_properties(&value)?,
            candidates: RoaringTreemap::new(),
            equivalent: false,
        });
    }

    let pair_keys = rows
        .iter()
        .map(|row| {
            Key::Data {
                scope,
                kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(row.from, row.to)),
            }
            .to_bytes()
        })
        .collect::<Vec<_>>();
    if !pair_keys.is_empty() {
        let pair_rows = txn.multi_get(&pair_keys).await?;
        for (row, pair_row) in rows.iter_mut().zip(pair_rows) {
            if let Some(pair_row) = pair_row {
                row.candidates = search::decode_roaring_treemap(&pair_row)?;
            }
        }
    }

    let mut candidates = Vec::with_capacity(tuning.batch_rows().get());
    for row_index in 0..rows.len() {
        let candidate_ids = core::mem::take(&mut rows[row_index].candidates);
        for edge_id in candidate_ids.iter() {
            if rows[row_index].equivalent {
                break;
            }
            candidates.push((row_index, edge_id));
            if candidates.len() == tuning.batch_rows().get() {
                mark_legacy_edge_equivalents(txn, scope, &mut rows, &candidates).await?;
                candidates.clear();
            }
        }
    }
    if !candidates.is_empty() {
        mark_legacy_edge_equivalents(txn, scope, &mut rows, &candidates).await?;
    }

    for row in rows {
        if !row.equivalent {
            let edge_id = writer.edge_ids().allocate().await?;
            maintain_current_edge_rows(
                txn,
                scope,
                edge_id,
                row.from,
                row.to,
                Some(&row.properties),
            )
            .await?;
        }
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::BatchReadAfter)?;

    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: u64::try_from(source_row_count).map_err(|_| {
            HelixDbError::InvariantViolation(
                "legacy edge-pair batch row count does not fit in u64".to_string(),
            )
        })?,
        source_bytes,
    })
}

async fn rebuild_edge_endpoint_batch(
    txn: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
) -> Result<MigrationBatch> {
    let source = read_stage_batch(
        txn,
        scope,
        MigrationStage::EdgeEndpoints,
        tuning,
        MigrationRowLimit::Tuned,
        job,
    )
    .await?;
    let MigrationSourceBatch::Advanced {
        rows: source_rows,
        resume_after_key,
        source_bytes,
    } = source
    else {
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        trip_migration_failpoint(MigrationFailpoint::BatchReadAfter)?;
        return Ok(MigrationBatch::StageComplete);
    };

    let source_row_count = source_rows.len();
    let mut rows = Vec::with_capacity(source_row_count);
    for (key, value) in source_rows {
        let Key::Data {
            kind: DataKeyKind::EdgeEndpoints(endpoint_key),
            ..
        } = Key::parse_from_slice(scope, &key)?
        else {
            return Err(HelixDbError::InvariantViolation(
                "edge endpoint migration scan returned a different key kind".to_string(),
            ));
        };
        let (from, to) = decode_edge_endpoints(&value)?;
        rows.push((endpoint_key.edge_id(), from, to));
    }
    let property_keys = rows
        .iter()
        .map(|(edge_id, _, _)| {
            Key::Data {
                scope,
                kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(*edge_id)),
            }
            .to_bytes()
        })
        .collect::<Vec<_>>();
    let property_rows = txn.multi_get(&property_keys).await?;
    for ((edge_id, from, to), property_row) in rows.into_iter().zip(property_rows) {
        let properties = match property_row {
            Some(property_row) => decode_properties(&property_row)?,
            None => Vec::new(),
        };
        maintain_current_edge_rows(txn, scope, edge_id, from, to, Some(&properties)).await?;
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::BatchReadAfter)?;

    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: u64::try_from(source_row_count).map_err(|_| {
            HelixDbError::InvariantViolation(
                "edge endpoint batch row count does not fit in u64".to_string(),
            )
        })?,
        source_bytes,
    })
}

async fn cleanup_legacy_edge_pair_batch(
    txn: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
) -> Result<MigrationBatch> {
    // A cleanup write contains only the source key plus a tombstone, so its
    // staged bytes are bounded by the scanned source key/value bytes. Use the
    // typed byte budget as the transaction boundary: applying the normal row
    // limit here would repeatedly reopen the same growing LSM range and makes
    // object-store requests super-linear at high row counts.
    scan_stage_batch(
        txn,
        scope,
        MigrationStage::LegacyEdgePairs,
        tuning,
        MigrationRowLimit::ByteBudgetOnly,
        job,
        |key, _| async move {
            let Key::Data {
                kind: DataKeyKind::EdgePropertyPair(_),
                ..
            } = Key::parse_from_slice(scope, &key)?
            else {
                return Ok(());
            };
            // Cleanup jobs are created only after the blocking rewrite is
            // durably complete. That rewrite either found an exactly equivalent
            // current edge or created one for every legacy row. Re-reading pair
            // indexes and edge rows here would turn cleanup into random point
            // lookups per legacy row on object storage. Current writers never
            // create pair-addressed legacy rows, so an intervening delete of the
            // current edge also makes deleting this obsolete source row correct.
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            trip_migration_failpoint(MigrationFailpoint::CleanupDeleteBefore)?;
            txn.delete(key)?;
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            trip_migration_failpoint(MigrationFailpoint::CleanupDeleteAfter)?;
            Ok(())
        },
    )
    .await
}

async fn scan_stage_batch<F, Fut>(
    txn: &DbTransaction,
    scope: DataScope,
    stage: MigrationStage,
    tuning: config::MigrationTuning,
    row_limit: MigrationRowLimit,
    job: &MigrationJob,
    mut process_row: F,
) -> Result<MigrationBatch>
where
    F: FnMut(Bytes, Bytes) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let source = read_stage_batch(txn, scope, stage, tuning, row_limit, job).await?;
    let MigrationSourceBatch::Advanced {
        rows,
        resume_after_key,
        source_bytes,
    } = source
    else {
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        trip_migration_failpoint(MigrationFailpoint::BatchReadAfter)?;
        return Ok(MigrationBatch::StageComplete);
    };
    let row_count = u64::try_from(rows.len()).map_err(|_| {
        HelixDbError::InvariantViolation(
            "migration batch row count does not fit in u64".to_string(),
        )
    })?;
    for (key, value) in rows {
        process_row(key, value).await?;
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::BatchReadAfter)?;

    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: row_count,
        source_bytes,
    })
}

#[derive(Debug)]
enum MigrationSourceBatch {
    Advanced {
        rows: Vec<(Bytes, Bytes)>,
        resume_after_key: MigrationResumeKey,
        source_bytes: u64,
    },
    StageComplete,
}

async fn read_stage_batch(
    txn: &DbTransaction,
    scope: DataScope,
    stage: MigrationStage,
    tuning: config::MigrationTuning,
    row_limit: MigrationRowLimit,
    job: &MigrationJob,
) -> Result<MigrationSourceBatch> {
    let MigrationJobState::Running {
        resume_after_key, ..
    } = &job.state
    else {
        return Ok(MigrationSourceBatch::StageComplete);
    };
    let prefix = stage.prefix(scope);
    let bounds = scan_bounds_for_prefix(prefix.as_ref(), resume_after_key.as_ref());
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::BatchReadBefore)?;
    let mut iter = txn.scan(bounds).await?;
    let mut scanned_rows = 0_u64;
    let mut scanned_bytes = 0_usize;
    let mut resume_after_key = None;
    let mut rows = Vec::with_capacity(tuning.batch_rows().get());

    while row_limit.admits(scanned_rows, tuning) {
        let Some(kv) = iter.next().await? else {
            break;
        };
        let row_bytes = kv.key.len().checked_add(kv.value.len()).ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "migration source row byte length overflowed usize".to_string(),
            )
        })?;
        if row_bytes > tuning.batch_bytes().get() {
            return Err(HelixDbError::Config(format!(
                "migration source row is {row_bytes} bytes, exceeding the {} byte batch limit",
                tuning.batch_bytes().get()
            )));
        }
        let next_scanned_bytes = scanned_bytes.checked_add(row_bytes).ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "migration batch source byte length overflowed usize".to_string(),
            )
        })?;
        if next_scanned_bytes > tuning.batch_bytes().get() {
            break;
        }
        resume_after_key = MigrationResumeKey::new(kv.key.to_vec());
        scanned_rows = scanned_rows.saturating_add(1);
        scanned_bytes = next_scanned_bytes;
        rows.push((kv.key, kv.value));
    }

    match (scanned_rows, resume_after_key) {
        (0, None) => Ok(MigrationSourceBatch::StageComplete),
        (0, Some(_)) => Err(HelixDbError::InvariantViolation(
            "migration batch has a resume key without rows".to_string(),
        )),
        (row_count, Some(resume_after_key)) => {
            assert_eq!(
                row_count,
                u64::try_from(rows.len()).unwrap_or(u64::MAX),
                "migration batch row count must match its bounded row buffer"
            );
            Ok(MigrationSourceBatch::Advanced {
                rows,
                resume_after_key,
                source_bytes: u64::try_from(scanned_bytes).map_err(|_| {
                    HelixDbError::InvariantViolation(
                        "migration batch source bytes do not fit in u64".to_string(),
                    )
                })?,
            })
        }
        (_, None) => Err(HelixDbError::InvariantViolation(
            "migration batch processed rows without a resume key".to_string(),
        )),
    }
}

struct LegacyEdgePairMigrationRow {
    from: NodeId,
    to: NodeId,
    properties: Vec<Property>,
    candidates: RoaringTreemap,
    equivalent: bool,
}

async fn mark_legacy_edge_equivalents(
    txn: &DbTransaction,
    scope: DataScope,
    rows: &mut [LegacyEdgePairMigrationRow],
    candidates: &[(usize, EdgeId)],
) -> Result<()> {
    let mut keys = Vec::with_capacity(candidates.len().saturating_mul(2));
    keys.extend(candidates.iter().map(|(_, edge_id)| {
        Key::Data {
            scope,
            kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(*edge_id)),
        }
        .to_bytes()
    }));
    keys.extend(candidates.iter().map(|(_, edge_id)| {
        Key::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(*edge_id)),
        }
        .to_bytes()
    }));
    let mut values = txn.multi_get(&keys).await?;
    let property_values = values.split_off(candidates.len());
    assert_eq!(
        values.len(),
        candidates.len(),
        "endpoint multi-get must preserve candidate cardinality"
    );
    assert_eq!(
        property_values.len(),
        candidates.len(),
        "property multi-get must preserve candidate cardinality"
    );

    for (((row_index, _), endpoint_value), property_value) in
        candidates.iter().zip(values).zip(property_values)
    {
        if rows[*row_index].equivalent {
            continue;
        }
        let Some(endpoint_value) = endpoint_value else {
            continue;
        };
        if decode_edge_endpoints(&endpoint_value)? != (rows[*row_index].from, rows[*row_index].to) {
            continue;
        }
        let properties = match property_value {
            Some(property_value) => decode_properties(&property_value)?,
            None => Vec::new(),
        };
        rows[*row_index].equivalent = properties == rows[*row_index].properties;
    }
    Ok(())
}

async fn maintain_current_edge_rows(
    txn: &DbTransaction,
    scope: DataScope,
    edge_id: EdgeId,
    from: NodeId,
    to: NodeId,
    properties: Option<&[Property]>,
) -> Result<()> {
    if let Some(properties) = properties {
        search::store_edge_endpoints_scoped(txn, edge_id, from, to, scope).await?;
        search::store_edge_properties_by_id_scoped(txn, edge_id, properties, scope).await?;
    }
    search::add_to_edge_pair_index_scoped(txn, from, to, edge_id, scope).await?;
    add_adjacency(txn, scope, from, to, EdgeAdjacencyDirection::Out)?;
    add_adjacency(txn, scope, to, from, EdgeAdjacencyDirection::In)?;

    let properties = properties.unwrap_or(&[]);
    if let Some(label) = label_of(properties) {
        search::add_to_edge_label_index_scoped(txn, from, to, label, scope).await?;
        search::add_to_global_edge_label_index_scoped(txn, label, edge_id, scope).await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EdgeAdjacencyDirection {
    Out,
    In,
}

fn add_adjacency(
    txn: &DbTransaction,
    scope: DataScope,
    node: NodeId,
    neighbor: NodeId,
    direction: EdgeAdjacencyDirection,
) -> Result<()> {
    let key = Key::Data {
        scope,
        kind: DataKeyKind::Adjacency(AdjacencyKey::new(node)),
    }
    .to_bytes();
    const ADD_OUT: u8 = 0x00;
    const ADD_IN: u8 = 0x02;
    const OP_LEN: usize = core::mem::size_of::<u8>();
    const NODE_ID_LEN: usize = core::mem::size_of::<NodeId>();
    let mut operand = Vec::with_capacity(OP_LEN + NODE_ID_LEN);
    operand.push(match direction {
        EdgeAdjacencyDirection::Out => ADD_OUT,
        EdgeAdjacencyDirection::In => ADD_IN,
    });
    operand.extend_from_slice(&neighbor.to_be_bytes());
    txn.merge(&key, Bytes::from(operand))?;
    Ok(())
}

/// Reserve both allocators above every durable current-format entity id.
pub(crate) async fn reserve_allocators_above_existing(
    writer: &HelixWriter,
    scope: DataScope,
) -> Result<()> {
    let started = Instant::now();
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::AllocatorReservationBefore)?;
    let max_node_id = max_existing_node_id(writer.db(), scope).await?;
    let max_edge_id = max_existing_edge_id(writer.db(), scope).await?;
    let next_node_id = max_node_id
        .map(|node_id| {
            node_id.checked_add(1).ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "cannot reserve node allocator above the maximum u64 node id".to_string(),
                )
            })
        })
        .transpose()?;
    let next_edge_id = max_edge_id
        .map(|edge_id| {
            edge_id.checked_add(1).ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "cannot reserve edge allocator above the maximum u64 edge id".to_string(),
                )
            })
        })
        .transpose()?;
    if let Some(next_node_id) = next_node_id {
        writer.node_ids().reserve_at_least(next_node_id).await?;
    }
    if let Some(next_edge_id) = next_edge_id {
        writer.edge_ids().reserve_at_least(next_edge_id).await?;
    }
    tracing::info!(
        migration_version = 1,
        migration_step = "allocator_watermarks",
        migration_outcome = "applied",
        duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        scanned_count = u64::from(max_node_id.is_some()) + u64::from(max_edge_id.is_some()),
        written_count = u64::from(next_node_id.is_some()) + u64::from(next_edge_id.is_some()),
        "storage schema migration step finished"
    );
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    trip_migration_failpoint(MigrationFailpoint::AllocatorReservationAfter)?;
    Ok(())
}

async fn max_existing_node_id<R>(read: &R, scope: DataScope) -> Result<Option<NodeId>>
where
    R: DbReadOps + Send + Sync,
{
    let prefix = Key::data_prefix(
        scope,
        Bytes::copy_from_slice(KeyPrefix::NodeProperty.as_slice()),
    );
    let mut iter = read.scan_prefix(prefix, ..).await?;
    let mut max_node_id = None;
    while let Some(kv) = iter.next().await? {
        let Key::Data {
            kind: DataKeyKind::NodeProperty(key),
            ..
        } = Key::parse_from_slice(scope, &kv.key)?
        else {
            continue;
        };
        max_node_id =
            Some(max_node_id.map_or(key.node_id(), |current: NodeId| current.max(key.node_id())));
    }
    Ok(max_node_id)
}

async fn max_existing_edge_id<R>(read: &R, scope: DataScope) -> Result<Option<EdgeId>>
where
    R: DbReadOps + Send + Sync,
{
    let prefix = Key::data_prefix(
        scope,
        Bytes::copy_from_slice(KeyPrefix::EdgeEndpoints.as_slice()),
    );
    let mut iter = read.scan_prefix(prefix, ..).await?;
    let mut max_edge_id = None;
    while let Some(kv) = iter.next().await? {
        let Key::Data {
            kind: DataKeyKind::EdgeEndpoints(key),
            ..
        } = Key::parse_from_slice(scope, &kv.key)?
        else {
            continue;
        };
        max_edge_id =
            Some(max_edge_id.map_or(key.edge_id(), |current: EdgeId| current.max(key.edge_id())));
    }
    Ok(max_edge_id)
}

fn advance_or_complete(job: &mut MigrationJob) {
    let Some(stage) = job.state.running_stage() else {
        return;
    };
    match (job.id, stage) {
        (MigrationId::GraphFormatV1Rewrite, MigrationStage::PropertyIndexes) => {
            job.advance_stage(MigrationStage::NodeProperties);
        }
        (MigrationId::GraphFormatV1Rewrite, MigrationStage::NodeProperties) => {
            job.advance_stage(MigrationStage::LegacyEdgePairs);
        }
        (MigrationId::GraphFormatV1Rewrite, MigrationStage::LegacyEdgePairs) => {
            job.advance_stage(MigrationStage::EdgeEndpoints);
        }
        (MigrationId::GraphFormatV1Rewrite, MigrationStage::EdgeEndpoints)
        | (MigrationId::LegacyVectorPropertyMaterialization, MigrationStage::EdgeEndpoints)
        | (MigrationId::GraphFormatV1Cleanup, MigrationStage::LegacyEdgePairs)
        | (MigrationId::GraphFormatV1Cleanup, MigrationStage::PropertyIndexes)
        | (MigrationId::GraphFormatV1Cleanup, MigrationStage::NodeProperties)
        | (MigrationId::GraphFormatV1Cleanup, MigrationStage::EdgeEndpoints) => {
            job.complete();
        }
        (MigrationId::LegacyVectorPropertyMaterialization, MigrationStage::NodeProperties) => {
            job.advance_stage(MigrationStage::EdgeEndpoints)
        }
        (MigrationId::LegacyVectorPhysicalCleanup, MigrationStage::FenceLegacyVectorSources) => {
            job.advance_stage(MigrationStage::LegacyVectorHotRows)
        }
        (MigrationId::LegacyVectorPhysicalCleanup, MigrationStage::LegacyVectorHotRows) => {
            job.advance_stage(MigrationStage::LegacyVectorLayer0Rows)
        }
        (MigrationId::LegacyVectorPhysicalCleanup, MigrationStage::LegacyVectorLayer0Rows) => {
            job.advance_stage(MigrationStage::LegacyVectorCoreRows)
        }
        (MigrationId::LegacyVectorPhysicalCleanup, MigrationStage::LegacyVectorCoreRows) => {
            job.advance_stage(MigrationStage::LegacyVectorDefinitions)
        }
        (MigrationId::LegacyVectorPhysicalCleanup, MigrationStage::LegacyVectorDefinitions) => {
            job.advance_stage(MigrationStage::ReleaseLegacyVectorReservations)
        }
        (
            MigrationId::LegacyVectorPhysicalCleanup,
            MigrationStage::ReleaseLegacyVectorReservations,
        ) => job.complete(),
        (
            MigrationId::LegacyVectorPropertyMaterialization,
            MigrationStage::PropertyIndexes
            | MigrationStage::LegacyEdgePairs
            | MigrationStage::FenceLegacyVectorSources
            | MigrationStage::LegacyVectorHotRows
            | MigrationStage::LegacyVectorLayer0Rows
            | MigrationStage::LegacyVectorCoreRows
            | MigrationStage::LegacyVectorDefinitions
            | MigrationStage::ReleaseLegacyVectorReservations,
        ) => unreachable!("materialization job construction excludes unrelated stages"),
        (
            MigrationId::LegacyVectorPhysicalCleanup,
            MigrationStage::PropertyIndexes
            | MigrationStage::NodeProperties
            | MigrationStage::LegacyEdgePairs
            | MigrationStage::EdgeEndpoints,
        ) => unreachable!("vector cleanup job construction excludes unrelated stages"),
        (
            MigrationId::GraphFormatV1Rewrite | MigrationId::GraphFormatV1Cleanup,
            MigrationStage::FenceLegacyVectorSources
            | MigrationStage::LegacyVectorHotRows
            | MigrationStage::LegacyVectorLayer0Rows
            | MigrationStage::LegacyVectorCoreRows
            | MigrationStage::LegacyVectorDefinitions
            | MigrationStage::ReleaseLegacyVectorReservations,
        ) => unreachable!("graph migration jobs exclude vector cleanup stages"),
    }
}

fn label_of(properties: &[Property]) -> Option<&str> {
    properties
        .iter()
        .find(|property| property.name == "$label")
        .and_then(|property| property.value.as_str())
}

fn decode_edge_endpoints(value: &[u8]) -> Result<(NodeId, NodeId)> {
    const NODE_ID_LEN: usize = core::mem::size_of::<NodeId>();
    if value.len() < NODE_ID_LEN + NODE_ID_LEN {
        return Err(HelixDbError::Encoding(
            crate::encoding::error::EncodingError::BufferTooShort {
                expected: NODE_ID_LEN + NODE_ID_LEN,
                actual: value.len(),
            },
        ));
    }
    let from = NodeId::from_be_bytes(
        value[0..NODE_ID_LEN]
            .try_into()
            .expect("from node id slice is 8 bytes"),
    );
    let to = NodeId::from_be_bytes(
        value[NODE_ID_LEN..NODE_ID_LEN + NODE_ID_LEN]
            .try_into()
            .expect("to node id slice is 8 bytes"),
    );
    Ok((from, to))
}

fn scoped_metadata_key(scope: DataScope, name: &[u8]) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexMetadata(MetadataKey::new(name)),
    }
    .to_bytes()
}

fn scan_bounds_for_prefix(
    prefix: &[u8],
    resume_after: Option<&MigrationResumeKey>,
) -> (Bound<Bytes>, Bound<Bytes>) {
    let start = match resume_after {
        Some(key) => Bound::Excluded(Bytes::copy_from_slice(key.as_bytes())),
        None => Bound::Included(Bytes::copy_from_slice(prefix)),
    };
    let end = increment_prefix(prefix)
        .map(Bound::Excluded)
        .unwrap_or(Bound::Unbounded);
    (start, end)
}

fn increment_prefix(prefix: &[u8]) -> Option<Bytes> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return Some(Bytes::from(upper));
        }
    }
    None
}

fn encode_json<T: Serialize>(value: &T) -> Result<Bytes> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|err| HelixDbError::Config(format!("failed to encode migration job: {err}")))
}

fn decode_json<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes)
        .map_err(|err| HelixDbError::Config(format!("failed to decode migration job: {err}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use roaring::RoaringTreemap;
    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::encoding::keys::scope::TenantId;
    use crate::encoding::keys::{
        AdjacencyKey, EdgeEndpointsKey, EdgePairIndexKey, EdgePropertyByIdKey,
    };
    use crate::encoding::property;
    use crate::encoding::v1::values;
    use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey as EdgePropertyPairKey;
    use crate::error::WriterMigrationRequirement;
    use crate::{DbConfig, HelixDB};

    fn migration_test_config() -> DbConfig {
        migration_test_config_with_batch_rows(config::MigrationTuning::DEFAULT_BATCH_ROWS)
    }

    fn migration_test_config_with_batch_rows(batch_rows: usize) -> DbConfig {
        DbConfig::new()
            .with_secondary_index_lifecycle_tuning(
                config::SecondaryIndexLifecycleTuning::default()
                    .with_worker_mode(config::SecondaryIndexLifecycleWorkerMode::Disabled),
            )
            .with_migration_tuning(
                config::MigrationTuning::default()
                    .with_batch_rows(
                        config::MigrationBatchRows::new(batch_rows)
                            .expect("test batch size is nonzero"),
                    )
                    .with_worker_mode(config::MigrationWorkerMode::Disabled),
            )
    }

    async fn all_test_rows(read: &(impl DbReadOps + Send + Sync)) -> Vec<(Bytes, Bytes)> {
        let mut rows = read.scan(..).await.expect("migration test scans");
        let mut collected = Vec::new();
        while let Some(row) = rows.next().await.expect("migration test row reads") {
            collected.push((row.key, row.value));
        }
        collected
    }

    fn edge_delta(op: u8, node_id: NodeId) -> Bytes {
        let mut bytes = vec![op];
        bytes.extend_from_slice(&node_id.to_be_bytes());
        Bytes::from(bytes)
    }

    #[tokio::test]
    async fn index_storage_v4_cleanup_readiness_is_byte_frozen_and_strict() {
        let key = scoped_metadata_key(DataScope::LegacyUnscoped, INDEX_STORAGE_V4_CLEANUP_READY);
        assert_eq!(
            key.as_ref(),
            b"\xFFkv_migration_ready:index_storage_v4_cleanup"
        );

        let db = Db::builder(
            "index-storage-v4-cleanup-readiness",
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .unwrap();
        assert!(!index_storage_v4_cleanup_ready(&db).await.unwrap());

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        stage_index_storage_v4_cleanup_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(
            db.get(&key).await.unwrap().as_deref(),
            Some(b"1".as_slice())
        );
        assert!(index_storage_v4_cleanup_ready(&db).await.unwrap());

        db.put(key, Bytes::from_static(b"invalid")).await.unwrap();
        assert!(matches!(
            index_storage_v4_cleanup_ready(&db).await,
            Err(HelixDbError::MigrationRequired { reason })
                if reason == "index storage V4 cleanup readiness marker is malformed"
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn tenant_key_envelope_readiness_is_byte_frozen_and_strict() {
        let key = scoped_metadata_key(DataScope::LegacyUnscoped, TENANT_KEY_ENVELOPE_READY);
        assert_eq!(
            key.as_ref(),
            b"\xFFkv_migration_ready:tenant_key_envelope_v1"
        );

        let db = Db::builder("tenant-key-envelope-readiness", Arc::new(InMemory::new()))
            .build()
            .await
            .unwrap();
        assert!(!tenant_key_envelope_ready(&db).await.unwrap());

        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        stage_tenant_key_envelope_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(
            db.get(&key).await.unwrap().as_deref(),
            Some(b"1".as_slice())
        );
        assert!(tenant_key_envelope_ready(&db).await.unwrap());

        db.put(key, Bytes::from_static(b"invalid")).await.unwrap();
        assert!(matches!(
            tenant_key_envelope_ready(&db).await,
            Err(HelixDbError::MigrationRequired { reason })
                if reason == "tenant key envelope readiness marker is malformed"
        ));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn storage_schema_progress_accepts_only_ordered_prefixes() {
        let cases = [
            ((false, false, false), StorageSchemaProgress::NotStarted),
            ((true, false, false), StorageSchemaProgress::GraphReady),
            ((true, true, false), StorageSchemaProgress::IndexReady),
            ((true, true, true), StorageSchemaProgress::Complete),
        ];
        for (index, ((graph, catalog, schema), expected)) in cases.into_iter().enumerate() {
            let db = Db::builder(
                format!("storage-schema-progress-{index}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap();
            if graph {
                db.put(
                    scoped_metadata_key(DataScope::LegacyUnscoped, GRAPH_FORMAT_V1_READY),
                    Bytes::from_static(b"1"),
                )
                .await
                .unwrap();
            }
            if catalog {
                db.put(
                    scoped_metadata_key(DataScope::LegacyUnscoped, INDEX_V2_MIGRATION_READY),
                    Bytes::from_static(b"1"),
                )
                .await
                .unwrap();
            }
            if schema {
                db.put(
                    scoped_metadata_key(DataScope::LegacyUnscoped, STORAGE_SCHEMA_COMPLETE),
                    Bytes::from_static(b"1"),
                )
                .await
                .unwrap();
            }
            assert_eq!(
                storage_schema_progress(&db, DataScope::LegacyUnscoped)
                    .await
                    .unwrap(),
                expected
            );
            db.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn storage_schema_progress_rejects_malformed_and_unordered_markers() {
        type StorageSchemaMarkers<'a> = (Option<&'a [u8]>, Option<&'a [u8]>, Option<&'a [u8]>);

        let cases: [StorageSchemaMarkers<'static>; 8] = [
            (None, Some(b"1"), None),
            (None, None, Some(b"1")),
            (None, Some(b"1"), Some(b"1")),
            (Some(b"1"), None, Some(b"1")),
            (Some(b"invalid"), None, None),
            (Some(b"1"), Some(b"invalid"), None),
            (Some(b"1"), Some(b"1"), Some(b"invalid")),
            (Some(b"0"), None, None),
        ];
        for (index, (graph, catalog, schema)) in cases.into_iter().enumerate() {
            let db = Db::builder(
                format!("invalid-storage-schema-progress-{index}"),
                Arc::new(InMemory::new()),
            )
            .build()
            .await
            .unwrap();
            for (name, value) in [
                (GRAPH_FORMAT_V1_READY, graph),
                (INDEX_V2_MIGRATION_READY, catalog),
                (STORAGE_SCHEMA_COMPLETE, schema),
            ] {
                let Some(value) = value else { continue };
                db.put(
                    scoped_metadata_key(DataScope::LegacyUnscoped, name),
                    Bytes::copy_from_slice(value),
                )
                .await
                .unwrap();
            }
            assert!(matches!(
                storage_schema_progress(&db, DataScope::LegacyUnscoped).await,
                Err(HelixDbError::MigrationRequired { .. })
            ));
            db.close().await.unwrap();
        }
    }

    #[test]
    fn migration_job_keys_are_scoped() {
        let tenant_a = TenantId::from_ulid_str("0000000000000000000000000A").expect("valid tenant");
        let tenant_b = TenantId::from_ulid_str("0000000000000000000000000B").expect("valid tenant");
        let scope_a = DataScope::Tenant(tenant_a);
        let scope_b = DataScope::Tenant(tenant_b);

        let key_a = MigrationJobKey::new(scope_a, MigrationId::GraphFormatV1Rewrite);
        let key_b = MigrationJobKey::new(scope_b, MigrationId::GraphFormatV1Rewrite);

        assert_ne!(key_a, key_b);
        assert!(key_a
            .as_ref()
            .starts_with(migration_job_scan_prefix_scoped(scope_a).as_ref()));
        assert!(key_b
            .as_ref()
            .starts_with(migration_job_scan_prefix_scoped(scope_b).as_ref()));
        assert!(!key_a
            .as_ref()
            .starts_with(migration_job_scan_prefix_scoped(scope_b).as_ref()));
    }

    #[test]
    fn failed_job_retries_from_exact_durable_checkpoint() {
        let resume =
            MigrationResumeKey::try_from(vec![0x02, 0x01]).expect("test resume key is non-empty");
        let mut job = MigrationJob::new(
            MigrationId::GraphFormatV1Rewrite,
            MigrationMode::BlockingStartup,
        );
        job.record_advanced(resume.clone(), 17);
        job.fail("transient object-store timeout");
        assert!(job.is_failed());

        job.retry();

        assert_eq!(
            job.state,
            MigrationJobState::Running {
                stage: MigrationStage::PropertyIndexes,
                resume_after_key: Some(resume),
                processed_rows: 17,
            }
        );
    }

    #[test]
    fn post_completion_failure_preserves_the_completed_counter() {
        let resume =
            MigrationResumeKey::try_from(vec![0x02, 0x01]).expect("test resume key is non-empty");
        let mut job = MigrationJob::new(
            MigrationId::LegacyVectorPhysicalCleanup,
            MigrationMode::BlockingStartup,
        );
        job.record_advanced(resume, 17);
        job.complete();

        job.fail("error observed after the committed transition");

        assert_eq!(
            job.state,
            MigrationJobState::Completed { processed_rows: 17 }
        );
    }

    #[test]
    fn migration_job_json_rejects_empty_resume_keys_and_unknown_fields() {
        let empty_resume = br#"{
            "id":"graph_format_v1_rewrite",
            "mode":"blocking_startup",
            "state":{"running":{"stage":"node_properties","resume_after_key":[],"processed_rows":0}}
        }"#;
        let error = decode_json::<MigrationJob>(empty_resume)
            .expect_err("empty resume keys must fail closed");
        assert!(error
            .to_string()
            .contains("migration resume key cannot be empty"));

        let unknown_field = br#"{
            "id":"graph_format_v1_rewrite",
            "mode":"blocking_startup",
            "state":{"completed":{"processed_rows":0}},
            "surprise":true
        }"#;
        let error = decode_json::<MigrationJob>(unknown_field)
            .expect_err("unknown job fields must fail closed");
        assert!(error.to_string().contains("unknown field"));
    }

    #[tokio::test]
    async fn scan_batch_obeys_typed_source_byte_budget() {
        let object_store = Arc::new(InMemory::new());
        let raw = Db::builder("migration-byte-budget", object_store)
            .build()
            .await
            .expect("raw db opens");
        let first_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(crate::encoding::keys::NodePropertyKey::new(1)),
        }
        .to_bytes();
        let second_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(crate::encoding::keys::NodePropertyKey::new(2)),
        }
        .to_bytes();
        raw.put(&first_key, Bytes::from_static(b"first-value"))
            .await
            .expect("first row writes");
        raw.put(&second_key, Bytes::from_static(b"other-value"))
            .await
            .expect("second row writes");
        let first_row_bytes = first_key.len() + b"first-value".len();
        let tuning = config::MigrationTuning::default()
            .with_batch_rows(config::MigrationBatchRows::new(10).expect("positive row limit"))
            .with_batch_bytes(
                config::MigrationBatchBytes::new(first_row_bytes)
                    .expect("positive source byte limit"),
            );
        let txn = raw
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("migration transaction begins");
        let job = MigrationJob::new(
            MigrationId::GraphFormatV1Rewrite,
            MigrationMode::BlockingStartup,
        );

        let batch = scan_stage_batch(
            &txn,
            DataScope::LegacyUnscoped,
            MigrationStage::NodeProperties,
            tuning,
            MigrationRowLimit::Tuned,
            &job,
            |_, _| async { Ok(()) },
        )
        .await
        .expect("bounded scan succeeds");
        txn.rollback();

        assert!(matches!(
            batch,
            MigrationBatch::Advanced {
                rows: 1,
                source_bytes,
                ..
            } if source_bytes == u64::try_from(first_row_bytes).expect("row bytes fit in u64")
        ));
        raw.close().await.expect("raw db closes");
    }

    #[tokio::test]
    async fn scan_batch_fails_closed_when_one_row_exceeds_byte_budget() {
        let object_store = Arc::new(InMemory::new());
        let raw = Db::builder("migration-oversized-row", object_store)
            .build()
            .await
            .expect("raw db opens");
        let key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(crate::encoding::keys::NodePropertyKey::new(1)),
        }
        .to_bytes();
        raw.put(&key, Bytes::from_static(b"oversized"))
            .await
            .expect("row writes");
        let tuning = config::MigrationTuning::default().with_batch_bytes(
            config::MigrationBatchBytes::new(1).expect("positive source byte limit"),
        );
        let txn = raw
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("migration transaction begins");
        let job = MigrationJob::new(
            MigrationId::GraphFormatV1Rewrite,
            MigrationMode::BlockingStartup,
        );

        let error = scan_stage_batch(
            &txn,
            DataScope::LegacyUnscoped,
            MigrationStage::NodeProperties,
            tuning,
            MigrationRowLimit::Tuned,
            &job,
            |_, _| async { Ok(()) },
        )
        .await
        .expect_err("oversized source row must fail closed");
        txn.rollback();

        assert!(error
            .to_string()
            .contains("exceeding the 1 byte batch limit"));
        raw.close().await.expect("raw db closes");
    }

    #[tokio::test]
    async fn cleanup_uses_byte_budget_instead_of_reopening_for_each_row_batch() {
        let object_store = Arc::new(InMemory::new());
        let raw = Db::builder("migration-cleanup-byte-batched", object_store)
            .build()
            .await
            .expect("raw db opens");
        let keys = (1_u64..=3)
            .map(|node_id| {
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(
                        node_id,
                        node_id + 10,
                    )),
                }
                .to_bytes()
            })
            .collect::<Vec<_>>();
        for key in &keys {
            raw.put(key, property::encode_properties(&[]))
                .await
                .expect("legacy edge row writes");
        }
        let tuning = config::MigrationTuning::default()
            .with_batch_rows(config::MigrationBatchRows::new(1).expect("positive row limit"));
        let txn = raw
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("cleanup transaction begins");
        let job = MigrationJob::new(MigrationId::GraphFormatV1Cleanup, MigrationMode::Background);

        let batch = cleanup_legacy_edge_pair_batch(&txn, DataScope::LegacyUnscoped, tuning, &job)
            .await
            .expect("cleanup scan succeeds");
        assert!(matches!(batch, MigrationBatch::Advanced { rows: 3, .. }));
        txn.commit().await.expect("cleanup transaction commits");
        for key in keys {
            assert!(raw
                .get(&key)
                .await
                .expect("legacy edge row reads")
                .is_none());
        }
        raw.close().await.expect("raw db closes");
    }

    #[tokio::test]
    async fn batched_legacy_equivalence_checks_align_endpoints_and_properties() {
        let object_store = Arc::new(InMemory::new());
        let raw = Db::builder("migration-batched-legacy-equivalence", object_store)
            .build()
            .await
            .expect("raw db opens");
        let expected_properties = vec![Property::string("kind", "expected")];
        let wrong_properties = vec![Property::string("kind", "wrong")];
        let endpoint_rows = [
            (10_u64, 1_u64, 99_u64),
            (11_u64, 1_u64, 2_u64),
            (12_u64, 3_u64, 4_u64),
        ];
        for (edge_id, from, to) in endpoint_rows {
            raw.put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
                }
                .to_bytes(),
                Bytes::copy_from_slice(&[from.to_be_bytes(), to.to_be_bytes()].concat()),
            )
            .await
            .expect("endpoint row writes");
        }
        for (edge_id, properties) in [(10_u64, &wrong_properties), (11_u64, &expected_properties)] {
            raw.put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
                }
                .to_bytes(),
                property::encode_properties(properties),
            )
            .await
            .expect("property row writes");
        }

        let txn = raw
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("migration transaction begins");
        let mut rows = vec![
            LegacyEdgePairMigrationRow {
                from: 1,
                to: 2,
                properties: expected_properties,
                candidates: RoaringTreemap::new(),
                equivalent: false,
            },
            LegacyEdgePairMigrationRow {
                from: 3,
                to: 4,
                properties: Vec::new(),
                candidates: RoaringTreemap::new(),
                equivalent: false,
            },
        ];

        mark_legacy_edge_equivalents(
            &txn,
            DataScope::LegacyUnscoped,
            &mut rows,
            &[(0, 10), (0, 11), (1, 12)],
        )
        .await
        .expect("batched candidate lookup succeeds");
        txn.rollback();

        assert!(rows[0].equivalent);
        assert!(rows[1].equivalent);
        raw.close().await.expect("raw db closes");
    }

    #[tokio::test]
    async fn startup_reserves_node_and_edge_allocators_above_migrated_ids() {
        let object_store = Arc::new(InMemory::new());
        let database = "migration-reserves-both-allocators";
        let raw = Db::builder(database, object_store.clone())
            .build()
            .await
            .expect("raw db opens");
        let node_id = 41_u64;
        let edge_id = 73_u64;
        raw.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(crate::encoding::keys::NodePropertyKey::new(
                    node_id,
                )),
            }
            .to_bytes(),
            property::encode_properties(&[]),
        )
        .await
        .expect("node property writes");
        raw.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
            }
            .to_bytes(),
            property::encode_properties(&[]),
        )
        .await
        .expect("edge property writes");
        raw.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
            }
            .to_bytes(),
            Bytes::copy_from_slice(&[node_id.to_be_bytes(), node_id.to_be_bytes()].concat()),
        )
        .await
        .expect("edge endpoints write");
        raw.close().await.expect("raw db closes");

        let db = HelixDB::open_with_object_store_and_config(
            database,
            object_store,
            migration_test_config(),
        )
        .await
        .expect("migrated db opens");
        let raw = db.inner_db();
        let node_watermark = raw
            .get(MetadataKey::next_node_id_key().to_bytes())
            .await
            .expect("node watermark reads")
            .expect("node watermark exists");
        let edge_watermark = raw
            .get(MetadataKey::next_edge_id_key().to_bytes())
            .await
            .expect("edge watermark reads")
            .expect("edge watermark exists");
        assert!(
            u64::from_be_bytes(node_watermark.as_ref().try_into().expect("u64 watermark"))
                > node_id
        );
        assert!(
            u64::from_be_bytes(edge_watermark.as_ref().try_into().expect("u64 watermark"))
                > edge_id
        );
        db.close().await.expect("migrated db closes");
    }

    #[tokio::test]
    async fn startup_fails_closed_at_maximum_node_id() {
        let object_store = Arc::new(InMemory::new());
        let database = "migration-rejects-maximum-node-id";
        let raw = Db::builder(database, object_store.clone())
            .build()
            .await
            .expect("raw db opens");
        raw.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::NodeProperty(crate::encoding::keys::NodePropertyKey::new(
                    u64::MAX,
                )),
            }
            .to_bytes(),
            property::encode_properties(&[]),
        )
        .await
        .expect("maximum node property writes");
        raw.close().await.expect("raw db closes");

        let Err(error) = HelixDB::open_with_object_store_and_config(
            database,
            object_store,
            migration_test_config(),
        )
        .await
        else {
            panic!("maximum node id must fail closed");
        };
        assert!(error
            .to_string()
            .contains("cannot reserve node allocator above the maximum u64 node id"));
    }

    #[tokio::test]
    async fn startup_fails_closed_at_maximum_edge_id() {
        let object_store = Arc::new(InMemory::new());
        let database = "migration-rejects-maximum-edge-id";
        let raw = Db::builder(database, object_store.clone())
            .build()
            .await
            .expect("raw db opens");
        raw.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(u64::MAX)),
            }
            .to_bytes(),
            Bytes::copy_from_slice(&[1_u64.to_be_bytes(), 2_u64.to_be_bytes()].concat()),
        )
        .await
        .expect("maximum edge endpoints write");
        raw.close().await.expect("raw db closes");

        let Err(error) = HelixDB::open_with_object_store_and_config(
            database,
            object_store,
            migration_test_config(),
        )
        .await
        else {
            panic!("maximum edge id must fail closed");
        };
        assert!(error
            .to_string()
            .contains("cannot reserve edge allocator above the maximum u64 edge id"));
    }

    #[tokio::test]
    async fn reader_rejects_incomplete_tenant_migration_without_mutating_storage() {
        let root = tempfile::tempdir().expect("temporary object-store root");
        let database = "reader-requires-migration";
        let object_store = Arc::new(
            slatedb::object_store::local::LocalFileSystem::new_with_prefix(root.path())
                .expect("local object store opens"),
        );
        let raw = Db::builder(database, object_store)
            .build()
            .await
            .expect("raw pre-migration db opens");
        let tenant = TenantId::from_ulid_str("01KZ6WZ9QREKZZ87492YXBTFJ3")
            .expect("production tenant ID is valid");
        let mut legacy_tenant_key = Vec::new();
        legacy_tenant_key.extend_from_slice(&tenant.as_u128().to_be_bytes());
        DataKeyKind::IndexMetadata(MetadataKey::next_node_id_key())
            .encode_into(&mut legacy_tenant_key);
        raw.put(
            &legacy_tenant_key,
            Bytes::copy_from_slice(&1_u64.to_be_bytes()),
        )
        .await
        .expect("legacy tenant row writes");
        let legacy_rows = all_test_rows(&raw).await;
        raw.close().await.expect("raw pre-migration db closes");
        let source = crate::HelixDbSource::Disk {
            root: root.path().to_path_buf(),
            database: database.to_string(),
        };

        let Err(error) = HelixDB::open_reader(source.clone()).await else {
            panic!("incomplete tenant migration must not become query-routable")
        };
        assert!(matches!(
            error,
            HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            }
        ));

        let object_store = Arc::new(
            slatedb::object_store::local::LocalFileSystem::new_with_prefix(root.path())
                .expect("local object store reopens"),
        );
        let raw = Db::builder(database, object_store)
            .build()
            .await
            .expect("raw pre-migration db reopens");
        assert_eq!(all_test_rows(&raw).await, legacy_rows);
        assert!(
            !tenant_key_envelope_ready(&raw)
                .await
                .expect("tenant migration marker reads"),
            "reader must not create the tenant migration marker"
        );
        assert!(
            !index_v2_migration_ready(&raw, DataScope::LegacyUnscoped)
                .await
                .expect("migration completion marker reads"),
            "reader must not create the migration completion marker"
        );
        assert!(
            !storage_schema_complete(&raw, DataScope::LegacyUnscoped)
                .await
                .expect("storage schema completion marker reads"),
            "reader must not create the storage schema completion marker"
        );
        crate::index_lifecycle::repository::bootstrap_writer(&raw)
            .await
            .expect("writer bootstrap tuple commits");
        raw.flush().await.expect("bootstrap tuple flushes");
        let tuple_rows = all_test_rows(&raw).await;
        raw.close().await.expect("raw tuple-only db closes");
        let Err(error) = HelixDB::open_reader(source.clone()).await else {
            panic!("tuple-only storage must not become query-routable")
        };
        assert!(matches!(
            error,
            HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            }
        ));
        let object_store = Arc::new(
            slatedb::object_store::local::LocalFileSystem::new_with_prefix(root.path())
                .expect("local object store reopens after tuple rejection"),
        );
        let raw = Db::builder(database, object_store)
            .build()
            .await
            .expect("raw tuple-only db reopens");
        assert_eq!(all_test_rows(&raw).await, tuple_rows);
        raw.close().await.expect("raw tuple-only db closes again");

        let writer = HelixDB::open_with_config(source.clone(), migration_test_config())
            .await
            .expect("writer completes migration and marks readiness");
        writer
            .inner_db()
            .flush()
            .await
            .expect("readiness marker flushes");
        let reader = HelixDB::open_reader_with_config(source, migration_test_config())
            .await
            .expect("reader opens after writer migration");
        let crate::HelixStorage::Reader(storage) = reader.storage() else {
            panic!("expected reader storage");
        };
        assert!(
            storage_schema_complete(storage.as_ref(), DataScope::LegacyUnscoped)
                .await
                .expect("completed storage schema marker reads")
        );
        reader.close().await.expect("reader closes");
        writer.close().await.expect("writer closes");
    }

    #[tokio::test]
    async fn startup_rewrites_legacy_pair_edges_and_cleanup_preserves_current_label_indexes() {
        let object_store = Arc::new(InMemory::new());
        let database = "migration-startup-rewrites-legacy-pair";
        let raw = Db::builder(database, object_store.clone())
            .build()
            .await
            .expect("raw db opens");
        let from = 11;
        let to = 17;
        let legacy_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(from, to)),
        }
        .to_bytes();
        let properties = vec![
            Property::string("$label", "FOLLOWS"),
            Property::i64("weight", 7),
        ];
        raw.put(&legacy_key, property::encode_properties(&properties))
            .await
            .expect("legacy pair row writes");
        raw.close().await.expect("raw db closes");

        let db = HelixDB::open_with_object_store_and_config(
            database,
            object_store,
            migration_test_config(),
        )
        .await
        .expect("db opens and runs startup migration");

        let pair_ids = search::lookup_edge_pair_index(db.inner_db().as_ref(), from, to)
            .await
            .expect("pair index lookup succeeds");
        let edge_ids = pair_ids.iter().collect::<Vec<_>>();
        assert_eq!(edge_ids.len(), 1);
        let edge_id = edge_ids[0];
        assert_eq!(
            search::get_edge_endpoints(db.inner_db().as_ref(), edge_id)
                .await
                .expect("endpoints lookup succeeds"),
            Some((from, to))
        );
        assert_eq!(
            search::get_edge_properties_by_id(db.inner_db().as_ref(), edge_id)
                .await
                .expect("edge properties lookup succeeds"),
            properties
        );
        assert!(
            search::lookup_global_edge_label_index(db.inner_db().as_ref(), "FOLLOWS")
                .await
                .expect("label lookup succeeds")
                .contains(edge_id)
        );
        assert!(db
            .inner_db()
            .get(&legacy_key)
            .await
            .expect("legacy key read succeeds")
            .is_some());

        db.process_migration_once()
            .await
            .expect("cleanup batch runs");
        assert!(db
            .inner_db()
            .get(&legacy_key)
            .await
            .expect("legacy key read succeeds")
            .is_none());
        assert!(
            search::lookup_global_edge_label_index(db.inner_db().as_ref(), "FOLLOWS")
                .await
                .expect("label lookup succeeds")
                .contains(edge_id)
        );
    }

    #[tokio::test]
    async fn startup_rewrites_legacy_pair_when_pair_index_has_non_equivalent_edge() {
        let object_store = Arc::new(InMemory::new());
        let database = "migration-startup-preserves-non-equivalent-legacy-pair";
        let raw = Db::builder(database, object_store.clone())
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("raw db opens");
        let from = 11;
        let to = 17;
        let existing_neighbor = 23;
        let existing_edge_id = 41;
        let from_adjacency_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::Adjacency(AdjacencyKey::new(from)),
        }
        .to_bytes();
        let legacy_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(from, to)),
        }
        .to_bytes();
        let existing_property_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(existing_edge_id)),
        }
        .to_bytes();
        let existing_endpoint_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(existing_edge_id)),
        }
        .to_bytes();
        let pair_index_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(from, to)),
        }
        .to_bytes();
        let legacy_properties = vec![
            Property::string("$label", "FOLLOWS"),
            Property::i64("weight", 7),
        ];
        let existing_properties = vec![
            Property::string("$label", "FOLLOWS"),
            Property::i64("weight", 99),
        ];
        let mut existing_edges = RoaringTreemap::new();
        existing_edges.insert(existing_edge_id);

        raw.put(&legacy_key, property::encode_properties(&legacy_properties))
            .await
            .expect("legacy pair row writes");
        raw.put(
            &existing_property_key,
            property::encode_properties(&existing_properties),
        )
        .await
        .expect("existing edge properties write");
        raw.put(
            &existing_endpoint_key,
            Bytes::copy_from_slice(&[from.to_be_bytes(), to.to_be_bytes()].concat()),
        )
        .await
        .expect("existing endpoints write");
        raw.put(
            &pair_index_key,
            search::encode_roaring_treemap(&existing_edges),
        )
        .await
        .expect("existing pair index writes");
        raw.merge(&from_adjacency_key, edge_delta(0x00, existing_neighbor))
            .await
            .expect("existing adjacency merge writes");
        raw.close().await.expect("raw db closes");

        let db = HelixDB::open_with_object_store_and_config(
            database,
            object_store,
            migration_test_config_with_batch_rows(1),
        )
        .await
        .expect("db opens and runs startup migration");

        let pair_ids = search::lookup_edge_pair_index(db.inner_db().as_ref(), from, to)
            .await
            .expect("pair index lookup succeeds");
        assert_eq!(pair_ids.len(), 2);
        assert!(pair_ids.contains(existing_edge_id));

        let mut found_legacy_equivalent = false;
        for edge_id in pair_ids.iter() {
            assert_eq!(
                search::get_edge_endpoints(db.inner_db().as_ref(), edge_id)
                    .await
                    .expect("endpoints lookup succeeds"),
                Some((from, to))
            );
            let properties = search::get_edge_properties_by_id(db.inner_db().as_ref(), edge_id)
                .await
                .expect("edge properties lookup succeeds");
            if properties == legacy_properties {
                found_legacy_equivalent = true;
            }
        }
        assert!(found_legacy_equivalent);

        let from_adjacency = db
            .inner_db()
            .get(&from_adjacency_key)
            .await
            .expect("adjacency read succeeds")
            .expect("adjacency exists");
        let from_edges = values::edges::decode_edges(&from_adjacency).expect("adjacency decodes");
        assert!(from_edges.contains_out(existing_neighbor));
        assert!(from_edges.contains_out(to));

        while db
            .process_migration_once()
            .await
            .expect("cleanup batch runs")
        {}
        assert!(db
            .inner_db()
            .get(&legacy_key)
            .await
            .expect("legacy key read succeeds")
            .is_none());
    }
}

/// Production-linked migration contracts compiled without `cfg(test)`.
#[cfg(feature = "production-coverage")]
pub(crate) mod production_contracts {
    use std::sync::Arc;

    use helix_ast::batch::{read_batch, write_batch};
    use helix_ast::graph::NodeRef;
    use helix_ast::query::QueryRequest;
    use helix_ast::traversal::g;
    use helix_ast::value::{PropertyInput, PropertyValue as AstPropertyValue};
    use sha2::{Digest, Sha256};
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::config::{
        MigrationBatchRows, MigrationTuning, SecondaryIndexDefinition,
        SecondaryIndexLifecycleBatchRows, SecondaryIndexLifecycleTuning, TextIndexDefinition,
        VectorIndexDefinition,
    };
    use crate::encoding::property;
    use crate::encoding::v1::keys::vectors::{
        VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorSimHashKey,
    };
    use crate::encoding::v2::keys::{GlobalKey, ScopedKey};
    use crate::encoding::v2::values::{
        decode_metadata_value, encode_index_record, encode_metadata_value,
    };
    use crate::index_lifecycle::{
        IndexId, IndexStateV2, IndexStorageVersion, IndexV2MetadataValue, LogicalIndexIdWatermark,
        PhysicalGeneration, ValidatedDynamicIndexDefinition, VectorPhysicalIdWatermark,
        VectorPhysicalIndexId, VectorPhysicalLayout,
    };
    use crate::search::vector::VectorDistanceMetric;
    use crate::{DbConfig, HelixDB};

    /// Serializes every migration contract because ordinary migration work can
    /// cross and consume a process-global generic failpoint installed by the
    /// recovery matrix.
    static MIGRATION_FAILPOINT_CONTRACT_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    /// Serializes feature-gated migration fixtures that install a process-global
    /// failpoint from the integration-owned production support module.
    pub(crate) async fn failpoint_contract_guard() -> tokio::sync::MutexGuard<'static, ()> {
        MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await
    }

    /// Covers the production reader boundary for every resumable schema stage.
    pub(crate) async fn run_writer_migration_requirement_contracts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database("writer-migration-requirements");
        let fixture = raw(&database, store).await;
        crate::index_lifecycle::repository::bootstrap_writer(&fixture)
            .await
            .expect("current bootstrap tuple commits");

        for marker in [
            GRAPH_FORMAT_V1_READY,
            INDEX_V2_MIGRATION_READY,
            STORAGE_SCHEMA_COMPLETE,
        ] {
            let error =
                crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&fixture)
                    .await
                    .expect_err("every incomplete current schema requires a writer");
            let HelixDbError::WriterMigrationRequired { requirement } = error else {
                panic!("incomplete current schema must remain typed: {error}")
            };
            assert_eq!(
                requirement,
                crate::error::WriterMigrationRequirement::IncompleteStorageSchema
            );
            assert_eq!(
                requirement.to_string(),
                "storage schema migration is incomplete"
            );
            fixture
                .put(
                    scoped_metadata_key(DataScope::LegacyUnscoped, marker),
                    Bytes::from_static(b"1"),
                )
                .await
                .expect("ordered readiness marker writes");
        }
        crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&fixture)
            .await
            .expect("complete current schema is reader-ready");

        fixture
            .delete(scoped_metadata_key(
                DataScope::LegacyUnscoped,
                INDEX_STORAGE_V4_CLEANUP_READY,
            ))
            .await
            .expect("legacy fixture clears the V4 cleanup marker");
        for legacy_version in [2, 3] {
            fixture
                .put(
                    global(GlobalKey::StorageVersion),
                    encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                        IndexStorageVersion::new(legacy_version)
                            .expect("legacy reader version is nonzero"),
                    )),
                )
                .await
                .expect("legacy storage marker writes");
            let before = all_rows(&fixture).await;
            assert_eq!(
                crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&fixture)
                    .await
                    .expect("complete versions two and three are explicitly reader-compatible"),
                crate::index_lifecycle::repository::ReaderStorageCompatibility::LegacyEqualityUnion
            );
            assert_eq!(all_rows(&fixture).await, before);
        }
        fixture.close().await.expect("fixture closes");
    }

    fn database(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::new_v4())
    }

    async fn raw(database: &str, store: Arc<dyn ObjectStore>) -> Db {
        Db::builder(database, store)
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("migration contract raw database opens")
    }

    async fn all_rows(read: &(impl DbReadOps + Send + Sync)) -> Vec<(Bytes, Bytes)> {
        let mut rows = read.scan(..).await.expect("migration contract scans");
        let mut collected = Vec::new();
        while let Some(row) = rows.next().await.expect("migration contract row reads") {
            collected.push((row.key, row.value));
        }
        collected
    }

    fn global(key: GlobalKey) -> Bytes {
        IndexKey::Global { kind: key }.to_bytes()
    }

    async fn assert_current_storage_version(reader: &(impl DbReadOps + Sync)) {
        let marker = reader
            .get(global(GlobalKey::StorageVersion))
            .await
            .expect("storage marker reads")
            .expect("storage marker exists");
        assert_eq!(
            decode_metadata_value(&marker).expect("storage marker decodes"),
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT)
        );
    }

    fn one_row_config() -> DbConfig {
        DbConfig::new()
            .with_migration_tuning(MigrationTuning::default().with_batch_rows(
                MigrationBatchRows::new(1).expect("one migration row is positive"),
            ))
            .with_secondary_index_lifecycle_tuning(
                SecondaryIndexLifecycleTuning::default().with_batch_rows(
                    SecondaryIndexLifecycleBatchRows::new(1)
                        .expect("one lifecycle row is positive"),
                ),
            )
    }

    /// Persists one exact migration state without changing any row codec.
    async fn put_migration_job(raw: &Db, mut job: MigrationJob) {
        if job.id == MigrationId::GraphFormatV1Rewrite {
            job.complete();
        }
        raw.put(
            MigrationJobKey::new(DataScope::LegacyUnscoped, job.id).into_bytes(),
            encode_json(&job).expect("migration fixture job encodes"),
        )
        .await
        .expect("migration fixture job writes");
    }

    /// Skips the graph rewrite so generic boundaries target vector work.
    async fn mark_graph_rewrite_complete(raw: &Db) {
        put_migration_job(
            raw,
            MigrationJob::new(
                MigrationId::GraphFormatV1Rewrite,
                MigrationMode::BlockingStartup,
            ),
        )
        .await;
        raw.put(
            scoped_metadata_key(DataScope::LegacyUnscoped, GRAPH_FORMAT_V1_READY),
            Bytes::from_static(b"1"),
        )
        .await
        .expect("graph-format readiness writes");
    }

    /// Marks vector materialization complete for a graph-authoritative fixture.
    async fn mark_vector_materialization_complete(raw: &Db) {
        let mut job = MigrationJob::new(
            MigrationId::LegacyVectorPropertyMaterialization,
            MigrationMode::BlockingStartup,
        );
        job.complete();
        put_migration_job(raw, job).await;
    }

    /// Resets physical cleanup to its first durable stage after adding a source.
    async fn reset_vector_cleanup_job(raw: &Db) {
        put_migration_job(
            raw,
            MigrationJob::new(
                MigrationId::LegacyVectorPhysicalCleanup,
                MigrationMode::BlockingStartup,
            ),
        )
        .await;
    }

    fn definitions() -> Vec<ValidatedDynamicIndexDefinition> {
        vec![
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
            SecondaryIndexDefinition::node_unique_equality("User", "slug")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
            SecondaryIndexDefinition::node_range("User", "rank")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
            SecondaryIndexDefinition::node_range_desc("User", "age")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
            SecondaryIndexDefinition::edge_equality("FOLLOWS", "kind")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
            SecondaryIndexDefinition::edge_range("FOLLOWS", "rank")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
            SecondaryIndexDefinition::edge_range_desc("FOLLOWS", "created_at")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
            VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
            VectorIndexDefinition::new_edge(
                "RELATED",
                "embedding",
                3,
                VectorDistanceMetric::Euclidean,
            )
            .expect("valid definition")
            .try_into()
            .expect("valid V2 definition"),
            VectorIndexDefinition::new_node(
                "Location",
                "coordinates",
                5,
                VectorDistanceMetric::Manhattan,
            )
            .expect("valid definition")
            .with_m(8)
            .expect("valid connection count")
            .with_m0(16)
            .expect("valid layer-zero connection count")
            .with_ef_construction(64)
            .expect("valid construction beam")
            .with_ml(0.5)
            .expect("valid layer multiplier")
            .with_simhash_threshold(9)
            .expect("valid SimHash threshold")
            .with_sampling_ratio(0.5)
            .expect("valid sampling ratio")
            .with_adaptive_enabled(false)
            .with_adaptive_failure_prob(0.2)
            .expect("valid adaptive failure probability")
            .try_into()
            .expect("valid V2 definition"),
            TextIndexDefinition::new_node("Document", "body")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
            TextIndexDefinition::new_edge("RELATED", "notes")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition"),
        ]
    }

    async fn seed_legacy_rows(
        database: &str,
        store: Arc<dyn ObjectStore>,
        definitions: &[ValidatedDynamicIndexDefinition],
        tombstone: Option<&ValidatedDynamicIndexDefinition>,
    ) {
        let raw = raw(database, store).await;
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy seed transaction opens");
        transaction
            .put(
                scoped_metadata_key(DataScope::LegacyUnscoped, b"parity-passthrough"),
                Bytes::from_static(b"preserve-me"),
            )
            .expect("passthrough row stages");
        for definition in definitions {
            let (key, value) = migration_parity_legacy_catalog_row(definition, false)
                .expect("legacy definition encodes");
            transaction.put(key, value).expect("legacy row stages");
            let ValidatedDynamicIndexDefinition::Vector(vector) = definition else {
                continue;
            };
            if vector.tenant_property().is_some() {
                continue;
            }
            let runtime = vector.to_runtime();
            let physical_name = crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            );
            let physical_id = crate::search::vector::index_id_from_name(&physical_name);
            let metadata = crate::search::vector::VectorIndexMetadata::new(
                crate::search::vector::VectorIndexConfig::from_v2_definition(
                    vector,
                    &physical_name,
                ),
            );
            transaction
                .put(
                    Key::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: crate::encoding::v1::keys::DataKeyKind::Vector(
                            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(physical_id)),
                        ),
                    }
                    .to_bytes(),
                    Bytes::copy_from_slice(
                        &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                            &metadata,
                        ),
                    ),
                )
                .expect("legacy vector metadata stages");
        }
        if let Some(definition) = tombstone {
            let (key, value) = migration_parity_legacy_catalog_row(definition, true)
                .expect("legacy tombstone encodes");
            transaction
                .put(key, value)
                .expect("legacy tombstone stages");
        }
        transaction.commit().await.expect("legacy rows commit");
        raw.close().await.expect("legacy seed database closes");
    }

    async fn assert_legacy_catalog_empty(db: &HelixDB) {
        assert!(
            load_legacy_definition_rows(db.inner_db().as_ref(), DataScope::LegacyUnscoped)
                .await
                .expect("legacy catalog scans")
                .is_empty(),
            "legacy catalog must be retired only after V2 convergence"
        );
        assert!(
            index_v2_migration_ready(db.inner_db().as_ref(), DataScope::LegacyUnscoped)
                .await
                .expect("readiness marker reads")
        );
        assert!(
            storage_schema_complete(db.inner_db().as_ref(), DataScope::LegacyUnscoped)
                .await
                .expect("storage schema completion marker reads")
        );
    }

    async fn assert_reader_migration_gate(
        database: &str,
        store: Arc<dyn ObjectStore>,
        expected_ready: bool,
    ) {
        let reader = slatedb::DbReader::builder(database.to_string(), store)
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("migration-gate reader storage opens");
        let result =
            crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&reader).await;
        assert_eq!(
            result.is_ok(),
            expected_ready,
            "reader readiness must match durable schema completion: {result:?}"
        );
        reader.close().await.expect("migration-gate reader closes");
    }

    async fn assert_failpoint_blocks_reader_until_recovery(failpoint: MigrationFailpoint) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database(&format!("reader-gate-{}", failpoint.as_str()));
        inject_migration_failpoint_once(failpoint).expect("reader-gate failpoint injects");
        assert!(
            HelixDB::open_with_object_store_for_migration_parity(
                database.clone(),
                Arc::clone(&store),
                one_row_config(),
            )
            .await
            .is_err(),
            "{} must interrupt writer migration",
            failpoint.as_str()
        );
        assert!(
            migration_failpoint_was_triggered(),
            "{} must trigger",
            failpoint.as_str()
        );
        let inspection = raw(&database, Arc::clone(&store)).await;
        let graph_ready = graph_format_v1_ready(&inspection, DataScope::LegacyUnscoped)
            .await
            .expect("graph readiness reads");
        let index_ready = index_v2_migration_ready(&inspection, DataScope::LegacyUnscoped)
            .await
            .expect("index readiness reads");
        assert!(
            !storage_schema_complete(&inspection, DataScope::LegacyUnscoped)
                .await
                .expect("schema completion reads"),
            "failed writer open must not publish full schema completion"
        );
        #[allow(clippy::wildcard_enum_match_arm)]
        match failpoint {
            MigrationFailpoint::BatchCommitAfter => {
                assert!(!graph_ready, "partial graph rewrite must remain unready");
                assert!(
                    !index_ready,
                    "index migration cannot precede graph readiness"
                );
            }
            MigrationFailpoint::MigrationReadyPublicationAfter
            | MigrationFailpoint::StorageSchemaCompletionBefore => {
                assert!(graph_ready, "graph rewrite must already be ready");
                assert!(index_ready, "index readiness must already be durable");
            }
            _ => panic!("unsupported reader-gate failpoint"),
        }
        inspection
            .close()
            .await
            .expect("reader-gate inspection closes");
        assert_reader_migration_gate(&database, Arc::clone(&store), false).await;

        let recovered = HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            one_row_config(),
        )
        .await
        .expect("writer recovery completes every schema stage");
        recovered.close().await.expect("recovered writer closes");
        assert_reader_migration_gate(&database, store, true).await;
    }

    async fn populate_legacy_vector<D: crate::search::vector::Distance>(
        raw: &Db,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
        entity_id: u64,
        vector: &[f32],
    ) {
        let runtime = definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        populate_named_legacy_vector::<D>(raw, definition, physical_name, entity_id, vector).await;
    }

    async fn populate_named_legacy_vector<D: crate::search::vector::Distance>(
        raw: &Db,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
        physical_name: String,
        entity_id: u64,
        vector: &[f32],
    ) {
        let physical_id = crate::search::vector::index_id_from_name(&physical_name);
        let metadata_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::IndexMetadata(
                VectorIndexMetadataKey::new(physical_id),
            )),
        }
        .to_bytes();
        let index = crate::search::vector::VectorIndex::<D>::for_legacy_migration(
            physical_name,
            DataScope::LegacyUnscoped,
        );
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("current vector metadata transaction opens");
        transaction
            .put(
                &metadata_key,
                Bytes::copy_from_slice(
                    &crate::encoding::v1::values::vectors::metadata::encode_metadata(
                        &crate::search::vector::VectorIndexMetadata::new(
                            crate::search::vector::VectorIndexConfig::from_v2_definition(
                                definition,
                                index.name(),
                            ),
                        ),
                    ),
                ),
            )
            .expect("current vector metadata stages");
        transaction
            .commit()
            .await
            .expect("current vector metadata commits");
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy vector population transaction opens");
        index
            .insert(&transaction, entity_id, vector)
            .await
            .expect("legacy vector inserts");
        transaction
            .commit()
            .await
            .expect("legacy vector population commits");
        let metadata = index
            .get_metadata(raw)
            .await
            .expect("populated metadata reads")
            .expect("populated metadata exists");
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy metadata transcode transaction opens");
        transaction
            .put(
                metadata_key,
                Bytes::copy_from_slice(
                    &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .expect("legacy metadata stages");
        transaction.commit().await.expect("legacy metadata commits");
    }

    /// Writes the graph-authoritative portion retained by the pinned legacy
    /// writer after it moved the indexed embedding into HNSW.
    async fn seed_legacy_node_without_vector(raw: &Db, entity_id: u64, label: &str) {
        raw.put(
            Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
                    crate::encoding::v1::keys::NodePropertyKey::new(entity_id),
                ),
            }
            .to_bytes(),
            property::encode_properties(&[
                Property::string("$label", label),
                Property::string("title", "retained graph property"),
            ]),
        )
        .await
        .expect("legacy graph property row writes");
    }

    /// Writes the current edge rows retained after the legacy writer moved the
    /// indexed embedding into HNSW.
    async fn seed_legacy_edge_without_vector(
        raw: &Db,
        edge_id: u64,
        from: u64,
        to: u64,
        label: &str,
    ) {
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy edge transaction opens");
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
                }
                .to_bytes(),
                crate::encoding::v1::values::edge_endpoints::EdgeEndpointsValue::new(from, to)
                    .encode(),
            )
            .expect("legacy edge endpoints stage");
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
                }
                .to_bytes(),
                property::encode_properties(&[
                    Property::string("$label", label),
                    Property::string("title", "retained edge property"),
                ]),
            )
            .expect("legacy edge properties stage");
        transaction.commit().await.expect("legacy edge rows commit");
    }

    /// Proves migration restored the omitted embedding without disturbing
    /// other graph properties.
    async fn assert_materialized_node_vector(raw: &Db, entity_id: u64, expected: &[f32]) {
        let key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
                crate::encoding::v1::keys::NodePropertyKey::new(entity_id),
            ),
        }
        .to_bytes();
        let properties = property::decode_properties(
            &raw.get(key)
                .await
                .expect("materialized graph property reads")
                .expect("materialized graph property exists"),
        )
        .expect("materialized graph properties decode");
        assert!(properties.contains(&Property::string("title", "retained graph property")));
        assert!(properties.contains(&Property::f32_array("embedding", expected.to_vec())));
    }

    /// Proves edge materialization restored the omitted embedding without
    /// disturbing the current edge-property row.
    async fn assert_materialized_edge_vector(raw: &Db, edge_id: u64, expected: &[f32]) {
        let key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
        }
        .to_bytes();
        let properties = property::decode_properties(
            &raw.get(key)
                .await
                .expect("materialized edge property reads")
                .expect("materialized edge property exists"),
        )
        .expect("materialized edge properties decode");
        assert!(properties.contains(&Property::string("title", "retained edge property")));
        assert!(properties.contains(&Property::f32_array("embedding", expected.to_vec())));
    }

    async fn non_metadata_vector_digest(
        read: &(impl slatedb::DbReadOps + Send + Sync),
        physical_id: u64,
    ) -> [u8; 32] {
        let mut digest = Sha256::new();
        for lane in crate::encoding::v1::keys::vectors::VectorStorageLane::ALL {
            let mut rows = read
                .scan_prefix(
                    Key::data_prefix(
                        DataScope::LegacyUnscoped,
                        lane.prefix_key(physical_id).to_bytes(),
                    ),
                    ..,
                )
                .await
                .expect("vector lane scans");
            while let Some(row) = rows.next().await.expect("vector lane row reads") {
                let logical = DataScope::LegacyUnscoped
                    .strip_key(&row.key)
                    .expect("vector row has the expected scope");
                if matches!(
                    VectorKey::parse_from_slice(logical).expect("vector row key parses"),
                    VectorKey::IndexMetadata(_) | VectorKey::SimHashDirectory(_)
                ) {
                    continue;
                }
                digest.update(u64::try_from(row.key.len()).unwrap().to_be_bytes());
                digest.update(&row.key);
                digest.update(u64::try_from(row.value.len()).unwrap().to_be_bytes());
                digest.update(&row.value);
            }
        }
        digest.finalize().into()
    }

    async fn vector_namespace_rows(
        read: &(impl slatedb::DbReadOps + Send + Sync),
        physical_id: u64,
    ) -> Vec<(Bytes, Bytes)> {
        let mut result = Vec::new();
        for lane in crate::encoding::v1::keys::vectors::VectorStorageLane::ALL {
            let mut rows = read
                .scan_prefix(
                    Key::data_prefix(
                        DataScope::LegacyUnscoped,
                        lane.prefix_key(physical_id).to_bytes(),
                    ),
                    ..,
                )
                .await
                .expect("vector namespace scans");
            while let Some(row) = rows.next().await.expect("vector namespace row reads") {
                result.push((row.key, row.value));
            }
        }
        result
    }

    async fn assert_vector_search<D: crate::search::vector::Distance>(
        db: &HelixDB,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
        query: &[f32],
        expected_entity_id: u64,
    ) {
        let active = db
            .active_index_handles_loaded(DataScope::LegacyUnscoped)
            .into_iter()
            .find(|handle| {
                matches!(
                    handle,
                    crate::index_lifecycle::ActiveIndexHandle::Vector {
                        definition: active,
                        ..
                    } if active.as_ref() == definition
                )
            })
            .expect("adopted vector is runtime-active");
        let crate::index_lifecycle::ActiveIndexHandle::Vector { layout, .. } = &active else {
            panic!("adopted definition projected another family")
        };
        let physical_id = layout
            .physical_index_id()
            .expect("adopted vector is unpartitioned");
        let generation =
            crate::search::vector::ValidatedVectorGenerationHandle::try_from_active::<D>(
                &active,
                physical_id,
            )
            .expect("active vector generation validates");
        let hits = crate::search::vector::VectorIndex::<D>::from_generation(&generation)
            .search(
                db.inner_db().as_ref(),
                query,
                &crate::search::vector::SearchParams::new(1).expect("one result is valid"),
            )
            .await
            .expect("adopted vector searches");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entity_id(), expected_entity_id);
    }

    fn returned_ids(response: &serde_json::Value, name: &str) -> Vec<u64> {
        let values = response
            .get(name)
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("query result `{name}` is an array"));
        let mut ids = values
            .iter()
            .map(|value| {
                value
                    .as_u64()
                    .unwrap_or_else(|| panic!("query result `{name}` contains a u64"))
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    async fn planner_vector_search_ids(db: &HelixDB, query: Vec<f32>) -> Vec<u64> {
        let request = QueryRequest::read(
            read_batch()
                .var_as(
                    "ids",
                    g().vector_search_nodes("Document", "embedding", query, 8, None)
                        .id(),
                )
                .returning(["ids"]),
        );
        returned_ids(
            &db.query(request)
                .await
                .expect("planner vector search succeeds"),
            "ids",
        )
    }

    async fn planner_tenant_vector_search_ids(
        db: &HelixDB,
        tenant: &str,
        query: Vec<f32>,
    ) -> Vec<u64> {
        let request = QueryRequest::read(
            read_batch()
                .var_as(
                    "ids",
                    g().vector_search_nodes(
                        "Document",
                        "embedding",
                        query,
                        8,
                        Some(AstPropertyValue::String(tenant.to_string())),
                    )
                    .id(),
                )
                .returning(["ids"]),
        );
        returned_ids(
            &db.query(request)
                .await
                .expect("planner tenant vector search succeeds"),
            "ids",
        )
    }

    async fn exercise_adopted_vector_runtime(
        db: &HelixDB,
        definition: &crate::index_lifecycle::ValidatedVectorIndexDefinition,
        physical_id: VectorPhysicalIndexId,
    ) {
        let active = db
            .active_index_handles_loaded(DataScope::LegacyUnscoped)
            .into_iter()
            .find(|handle| {
                matches!(
                    handle,
                    crate::index_lifecycle::ActiveIndexHandle::Vector {
                        definition: active,
                        ..
                    } if active.as_ref() == definition
                )
            })
            .expect("adopted vector is runtime-active");
        let generation = crate::search::vector::ValidatedVectorGenerationHandle::try_from_active::<
            crate::search::vector::distance::Cosine,
        >(&active, physical_id)
        .expect("adopted generation validates for cache hydration");
        db.refresh_vector_memory_cache()
            .await
            .expect("adopted vector cache refreshes");
        let cache_guard = db
            .vector_cache_registry()
            .read_guard_for(&generation)
            .expect("adopted generation is admitted to the vector cache");
        assert!(cache_guard.store().estimated_bytes() > 0);
        drop(cache_guard);

        let create = QueryRequest::write(
            write_batch()
                .var_as(
                    "created",
                    g().add_n(
                        "Document",
                        vec![(
                            "embedding",
                            PropertyInput::from(AstPropertyValue::F32Array(vec![0.0, 1.0, 0.0])),
                        )],
                    ),
                )
                .var_as("created_id", g().n(NodeRef::var("created")).id())
                .returning(["created_id"]),
        );
        let created = db
            .query(create)
            .await
            .expect("planner vector create succeeds");
        let created_ids = returned_ids(&created, "created_id");
        let [created_id] = created_ids.as_slice() else {
            panic!("planner vector create returns one node ID")
        };
        assert!(planner_vector_search_ids(db, vec![0.0, 1.0, 0.0])
            .await
            .contains(created_id));

        db.query(QueryRequest::write(write_batch().var_as(
            "updated",
            g().n(NodeRef::id(*created_id)).set_property(
                "embedding",
                PropertyInput::from(AstPropertyValue::F32Array(vec![0.0, 0.0, 1.0])),
            ),
        )))
        .await
        .expect("planner vector update succeeds");
        assert!(planner_vector_search_ids(db, vec![0.0, 0.0, 1.0])
            .await
            .contains(created_id));

        db.query(QueryRequest::write(
            write_batch().var_as("deleted", g().n(NodeRef::id(*created_id)).drop()),
        ))
        .await
        .expect("planner vector delete succeeds");
        assert!(!planner_vector_search_ids(db, vec![0.0, 0.0, 1.0])
            .await
            .contains(created_id));
    }

    /// Proves a populated physical namespace is adopted byte-for-byte, remains
    /// searchable after cold reopen, and is later owned by ordinary DROP and
    /// recreate lifecycle behavior.
    pub(crate) async fn run_vector_adoption_contract() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database("migration-vector-adoption");
        let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .expect("valid vector definition")
        .try_into()
        .expect("valid V2 vector definition");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector runtime definition validates as vector")
        };
        seed_legacy_rows(
            &database,
            Arc::clone(&store),
            std::slice::from_ref(&definition),
            None,
        )
        .await;
        let legacy_db = raw(&database, Arc::clone(&store)).await;
        seed_legacy_node_without_vector(&legacy_db, 77, "Document").await;
        populate_legacy_vector::<crate::search::vector::distance::Cosine>(
            &legacy_db,
            vector_definition,
            77,
            &[1.0, 0.0, 0.0],
        )
        .await;
        let colliding_property_key = crate::encoding::v1::indexes::range::EdgeRangeIndexKey::new(
            crate::encoding::v1::indexes::EdgeDirection::Out,
            crate::encoding::v1::indexes::range::EdgeRangeIndexDirection::Asc,
            1,
            [1, 2, 3, 4],
            std::borrow::Cow::Borrowed("twenty-byte-value---"),
            9,
        );
        let colliding_property_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v1::keys::DataKeyKind::PropertyIndex(
                crate::encoding::v1::indexes::IndexKey::EdgeRange(colliding_property_key),
            ),
        }
        .to_bytes();
        assert_eq!(colliding_property_key.len(), 43);
        let collision_transaction = legacy_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("property collision transaction opens");
        collision_transaction
            .put(colliding_property_key, Bytes::new())
            .expect("property collision row stages");
        collision_transaction
            .commit()
            .await
            .expect("property collision row commits");
        let runtime = vector_definition.to_runtime();
        let legacy_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_id = crate::search::vector::index_id_from_name(&legacy_name);
        let guard_transaction = legacy_db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("legacy transaction guard transaction opens");
        guard_transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::TxnGuard(
                        crate::encoding::v1::keys::vectors::VectorTxnGuardKey::new(physical_id),
                    )),
                }
                .to_bytes(),
                crate::encoding::v1::values::vectors::markers::encode_active_txn_guard(),
            )
            .expect("legacy transaction guard stages");
        guard_transaction
            .commit()
            .await
            .expect("legacy transaction guard commits");
        let before_digest = non_metadata_vector_digest(&legacy_db, physical_id).await;
        let empty_digest: [u8; 32] = Sha256::digest([]).into();
        assert_ne!(before_digest, empty_digest);
        legacy_db.close().await.expect("populated source closes");

        let migrated = Box::pin(HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            one_row_config(),
        ))
        .await
        .expect("populated vector adopts");
        assert_legacy_catalog_empty(&migrated).await;
        assert_materialized_node_vector(migrated.inner_db().as_ref(), 77, &[1.0, 0.0, 0.0]).await;
        let full = migrated
            .query(QueryRequest::read(
                read_batch()
                    .var_as(
                        "full",
                        g().n(NodeRef::id(77)).value_map(None::<Vec<String>>),
                    )
                    .returning(["full"]),
            ))
            .await
            .expect("full materialized property read succeeds");
        let full = full
            .get("full")
            .and_then(serde_json::Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(serde_json::Value::as_object)
            .expect("full materialized read returns one object");
        assert_eq!(
            full.get("title").and_then(serde_json::Value::as_str),
            Some("retained graph property")
        );
        assert_eq!(
            full.get("embedding")
                .and_then(serde_json::Value::as_array)
                .expect("full read includes the embedding")
                .iter()
                .map(|value| value.as_f64().expect("embedding component is numeric"))
                .collect::<Vec<_>>(),
            vec![1.0, 0.0, 0.0]
        );
        let projected = migrated
            .query(QueryRequest::read(
                read_batch()
                    .var_as(
                        "projected",
                        g().n(NodeRef::id(77)).values(vec!["embedding"]),
                    )
                    .returning(["projected"]),
            ))
            .await
            .expect("projected materialized property read succeeds");
        let projected = projected
            .get("projected")
            .and_then(serde_json::Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(serde_json::Value::as_object)
            .and_then(|row| row.get("embedding"))
            .and_then(serde_json::Value::as_array)
            .expect("projected read includes only the requested embedding");
        assert_eq!(
            projected
                .iter()
                .map(|value| value.as_f64().expect("embedding component is numeric"))
                .collect::<Vec<_>>(),
            vec![1.0, 0.0, 0.0]
        );
        let record = crate::index_lifecycle::repository::load_index_record(
            migrated.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &definition.identity(),
        )
        .await
        .expect("adopted record reads")
        .expect("adopted record exists");
        let crate::index_lifecycle::IndexStateV2::Active {
            physical:
                crate::index_lifecycle::PhysicalGeneration::Vector {
                    generation,
                    layout:
                        crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                            physical_index_id,
                        },
                    ..
                },
            ..
        } = record.state()
        else {
            panic!("adopted record is not one active unpartitioned vector")
        };
        assert_eq!(physical_index_id.get(), physical_id);
        assert_eq!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                migrated.inner_db().as_ref(),
                *physical_index_id,
            )
            .await
            .expect("active reservation reads"),
            Some(
                crate::index_lifecycle::LegacyVectorPhysicalReservation::AdoptedActive {
                    index_id: record.index_id(),
                    generation: *generation,
                }
            )
        );
        assert_eq!(
            non_metadata_vector_digest(migrated.inner_db().as_ref(), physical_id).await,
            before_digest,
            "adoption must not rewrite any HNSW row"
        );
        let metadata_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::IndexMetadata(
                VectorIndexMetadataKey::new(physical_id),
            )),
        }
        .to_bytes();
        let metadata = crate::encoding::v1::values::vectors::metadata::decode_metadata(
            &migrated
                .inner_db()
                .get(metadata_key)
                .await
                .expect("metadata reads")
                .expect("metadata exists"),
        )
        .expect("adopted metadata uses the current codec");
        assert_eq!(
            metadata.config.index_name,
            format!(
                "v2-vector-{}-{}-{}",
                record.index_id().get(),
                generation.get(),
                physical_id
            )
        );
        assert_vector_search::<crate::search::vector::distance::Cosine>(
            &migrated,
            vector_definition,
            &[1.0, 0.0, 0.0],
            77,
        )
        .await;
        migrated
            .query(QueryRequest::write(write_batch().var_as(
                "moved",
                g().n(NodeRef::id(77)).set_property(
                    "$label",
                    PropertyInput::from(AstPropertyValue::String("Archived".to_string())),
                ),
            )))
            .await
            .expect("label move without an embedding commits");
        assert!(!planner_vector_search_ids(&migrated, vec![1.0, 0.0, 0.0])
            .await
            .contains(&77));
        migrated
            .query(QueryRequest::write(write_batch().var_as(
                "restored",
                g().n(NodeRef::id(77)).set_property(
                    "$label",
                    PropertyInput::from(AstPropertyValue::String("Document".to_string())),
                ),
            )))
            .await
            .expect("label restoration without an embedding commits");
        assert!(planner_vector_search_ids(&migrated, vec![1.0, 0.0, 0.0])
            .await
            .contains(&77));
        exercise_adopted_vector_runtime(&migrated, vector_definition, *physical_index_id).await;
        migrated.close().await.expect("adopted database closes");

        let reopened = Box::pin(HelixDB::open_with_object_store_for_migration_parity(
            database,
            store,
            one_row_config(),
        ))
        .await
        .expect("adopted database cold-reopens");
        assert_vector_search::<crate::search::vector::distance::Cosine>(
            &reopened,
            vector_definition,
            &[1.0, 0.0, 0.0],
            77,
        )
        .await;
        let receipt = crate::index_lifecycle::lifecycle::drop_index_operation(
            reopened.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &definition,
        )
        .await
        .expect("adopted vector drop enqueues");
        let operation_id = receipt.operation_id().expect("drop has an operation");
        reopened.wake_index_worker().await;
        wait_for_index_operation(&reopened, DataScope::LegacyUnscoped, operation_id, false)
            .await
            .expect("adopted vector drop completes");
        assert!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                reopened.inner_db().as_ref(),
                *physical_index_id,
            )
            .await
            .expect("dropped reservation reads")
            .is_none()
        );
        assert_eq!(
            non_metadata_vector_digest(reopened.inner_db().as_ref(), physical_id).await,
            empty_digest,
            "ordinary DROP must delete the adopted physical namespace"
        );
        reopened
            .install_index_for_tests(definition.clone())
            .await
            .expect("dropped vector recreates");
        let recreated = crate::index_lifecycle::repository::load_index_record(
            reopened.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &definition.identity(),
        )
        .await
        .expect("recreated record reads")
        .expect("recreated record exists");
        let Some(crate::index_lifecycle::PhysicalGeneration::Vector {
            layout:
                crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: recreated_physical_id,
                },
            ..
        }) = recreated.state().physical()
        else {
            panic!("recreated vector has one physical namespace")
        };
        assert_ne!(recreated_physical_id.get(), physical_id);
        assert_vector_search::<crate::search::vector::distance::Cosine>(
            &reopened,
            vector_definition,
            &[1.0, 0.0, 0.0],
            77,
        )
        .await;
        reopened.close().await.expect("recreated database closes");
    }

    /// Requires every adoption-checkpoint failure to return from writer-open,
    /// preserve the adopted HNSW source, and cold-recover exact ownership.
    pub(crate) async fn run_vector_adoption_failpoint_recovery_contracts() -> Vec<&'static str> {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        const BOUNDARIES: [MigrationFailpoint; 8] = [
            MigrationFailpoint::LegacyVectorValidationCheckpointBefore,
            MigrationFailpoint::LegacyVectorValidationCheckpointAfter,
            MigrationFailpoint::LegacyVectorMetadataPublicationBefore,
            MigrationFailpoint::LegacyVectorMetadataPublicationAfter,
            MigrationFailpoint::LegacyVectorReservationTransitionBefore,
            MigrationFailpoint::LegacyVectorReservationTransitionAfter,
            MigrationFailpoint::LegacyDefinitionRetirementBefore,
            MigrationFailpoint::LegacyDefinitionRetirementAfter,
        ];

        let mut recovered_boundaries = Vec::with_capacity(BOUNDARIES.len());
        for failpoint in BOUNDARIES {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let database = database(&format!("migration-vector-adoption-{}", failpoint.as_str()));
            let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .expect("adoption recovery vector definition validates")
            .try_into()
            .expect("adoption recovery vector definition converts");
            let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
                unreachable!("adoption recovery definition remains vector")
            };
            seed_legacy_rows(
                &database,
                Arc::clone(&store),
                std::slice::from_ref(&definition),
                None,
            )
            .await;
            let source = raw(&database, Arc::clone(&store)).await;
            seed_legacy_node_without_vector(&source, 77, "Document").await;
            populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                &source,
                vector_definition,
                77,
                &[1.0, 0.0, 0.0],
            )
            .await;
            let runtime = vector_definition.to_runtime();
            let legacy_name = crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            );
            let physical_id = crate::search::vector::index_id_from_name(&legacy_name);
            let source_digest = non_metadata_vector_digest(&source, physical_id).await;
            source
                .close()
                .await
                .expect("adoption recovery source closes");

            inject_migration_failpoint_once(failpoint)
                .expect("adoption recovery failpoint injects");
            let interrupted = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                HelixDB::open_with_object_store_for_migration_parity(
                    database.clone(),
                    Arc::clone(&store),
                    one_row_config(),
                ),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} adoption recovery writer-open exceeded five seconds",
                    failpoint.as_str()
                )
            });
            let Err(error) = interrupted else {
                panic!("{} must interrupt adoption", failpoint.as_str());
            };
            assert!(
                matches!(error, HelixDbError::MigrationRequired { .. }),
                "{} returned the wrong typed failure: {error}",
                failpoint.as_str()
            );
            assert!(
                migration_failpoint_was_triggered(),
                "{} must trigger during adoption",
                failpoint.as_str()
            );
            let inspection = raw(&database, Arc::clone(&store)).await;
            assert_eq!(
                non_metadata_vector_digest(&inspection, physical_id).await,
                source_digest,
                "{} must preserve every non-metadata HNSW row",
                failpoint.as_str()
            );
            assert_eq!(
                load_legacy_definition_rows(&inspection, DataScope::LegacyUnscoped)
                    .await
                    .expect("adoption recovery legacy catalog scans")
                    .len(),
                1,
                "{} must retain its legacy definition until activation",
                failpoint.as_str()
            );
            assert!(
                !index_v2_migration_ready(&inspection, DataScope::LegacyUnscoped)
                    .await
                    .expect("adoption recovery readiness reads"),
                "{} must not publish readiness",
                failpoint.as_str()
            );
            let current_before = crate::index_lifecycle::repository::load_index_record(
                &inspection,
                DataScope::LegacyUnscoped,
                &definition.identity(),
            )
            .await
            .expect("adoption recovery current record reads")
            .expect("adoption recovery current record exists");
            let IndexStateV2::Building {
                build_operation_id, ..
            } = current_before.state()
            else {
                panic!("failed adoption remains Building");
            };
            let failed_operation = crate::index_lifecycle::outbox::read_operation(
                &inspection,
                DataScope::LegacyUnscoped,
                *build_operation_id,
            )
            .await
            .expect("adoption recovery failed operation reads")
            .expect("adoption recovery failed operation exists");
            assert!(
                matches!(
                    failed_operation.queue_schedule(),
                    Some(
                        crate::index_lifecycle::IndexOperationQueueSchedule::DelayedAfterTransientFailure {
                            ..
                        }
                    )
                ),
                "{} did not retain its typed transient retry",
                failpoint.as_str()
            );
            let ownership_before = (
                current_before.index_id(),
                current_before.state().generation(),
            );
            inspection
                .close()
                .await
                .expect("adoption recovery inspection closes");

            let recovered = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                HelixDB::open_with_object_store_for_migration_parity(
                    database,
                    store,
                    one_row_config(),
                ),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} adoption recovery cold open exceeded five seconds",
                    failpoint.as_str()
                )
            })
            .expect("adoption recovery cold open converges");
            let record = crate::index_lifecycle::repository::load_index_record(
                recovered.inner_db().as_ref(),
                DataScope::LegacyUnscoped,
                &definition.identity(),
            )
            .await
            .expect("adoption recovery final record reads")
            .expect("adoption recovery final record exists");
            let IndexStateV2::Active {
                physical:
                    PhysicalGeneration::Vector {
                        generation,
                        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                        ..
                    },
                ..
            } = record.state()
            else {
                panic!("adoption recovery final record is active and unpartitioned")
            };
            assert_eq!(physical_index_id.get(), physical_id);
            assert_eq!(
                ownership_before,
                (record.index_id(), *generation),
                "{} changed allocated vector ownership",
                failpoint.as_str(),
            );
            assert_eq!(
                non_metadata_vector_digest(recovered.inner_db().as_ref(), physical_id).await,
                source_digest,
                "{} rebuilt non-metadata HNSW rows",
                failpoint.as_str()
            );
            assert_legacy_catalog_empty(&recovered).await;
            assert_materialized_node_vector(recovered.inner_db().as_ref(), 77, &[1.0, 0.0, 0.0])
                .await;
            assert_vector_search::<crate::search::vector::distance::Cosine>(
                &recovered,
                vector_definition,
                &[1.0, 0.0, 0.0],
                77,
            )
            .await;
            recovered
                .close()
                .await
                .expect("adoption recovery writer closes");
            recovered_boundaries.push(failpoint.as_str());
        }
        recovered_boundaries
    }

    /// Proves malformed rows in each physical lane block ownership transfer
    /// while preserving the exact source catalog and vector namespace.
    pub(crate) async fn run_vector_corruption_contracts() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        enum CorruptionLane {
            Core,
            Hot,
            Layer0,
        }

        for (name, lane) in [
            ("core", CorruptionLane::Core),
            ("hot", CorruptionLane::Hot),
            ("layer-zero", CorruptionLane::Layer0),
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let database = database(&format!("migration-vector-corrupt-{name}"));
            let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .expect("valid vector definition")
            .try_into()
            .expect("valid V2 vector definition");
            let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
                unreachable!("vector runtime definition validates as vector")
            };
            seed_legacy_rows(
                &database,
                Arc::clone(&store),
                std::slice::from_ref(&definition),
                None,
            )
            .await;
            let source_db = raw(&database, Arc::clone(&store)).await;
            populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                &source_db,
                vector_definition,
                77,
                &[1.0, 0.0, 0.0],
            )
            .await;
            let runtime = vector_definition.to_runtime();
            let physical_name = crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            );
            let physical_id = crate::search::vector::index_id_from_name(&physical_name);
            let corrupt_key = match lane {
                CorruptionLane::Core => Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::IndexMetadata(
                        VectorIndexMetadataKey::new(physical_id),
                    )),
                }
                .to_bytes(),
                CorruptionLane::Hot => Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::SimHash(
                        crate::encoding::v1::keys::vectors::VectorSimHashKey::new(physical_id, 77),
                    )),
                }
                .to_bytes(),
                CorruptionLane::Layer0 => {
                    let simhash_key = Key::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::SimHash(
                            crate::encoding::v1::keys::vectors::VectorSimHashKey::new(
                                physical_id,
                                77,
                            ),
                        )),
                    }
                    .to_bytes();
                    let simhash = crate::encoding::v1::values::vectors::simhash::decode_simhash(
                        &source_db
                            .get(simhash_key)
                            .await
                            .expect("source SimHash reads")
                            .expect("source SimHash exists"),
                    )
                    .expect("source SimHash decodes");
                    Key::Data {
                        scope: DataScope::LegacyUnscoped,
                        kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::Vector(
                            crate::encoding::v1::keys::vectors::VectorItemKey::new(
                                physical_id,
                                crate::search::vector::simhash::order_code_from_simhash_bits(
                                    simhash,
                                ),
                                77,
                            ),
                        )),
                    }
                    .to_bytes()
                }
            };
            let transaction = source_db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("corruption transaction opens");
            transaction
                .put(corrupt_key, Bytes::from_static(b"malformed"))
                .expect("corrupt row stages");
            transaction.commit().await.expect("corrupt row commits");
            let source_rows = vector_namespace_rows(&source_db, physical_id).await;
            source_db.close().await.expect("corrupt source closes");

            assert!(
                HelixDB::open_with_object_store_for_migration_parity(
                    database.clone(),
                    Arc::clone(&store),
                    one_row_config(),
                )
                .await
                .is_err(),
                "malformed {name} lane must fail writer migration"
            );
            let inspection = raw(&database, Arc::clone(&store)).await;
            assert_eq!(
                load_legacy_definition_rows(&inspection, DataScope::LegacyUnscoped)
                    .await
                    .expect("legacy catalog scans")
                    .len(),
                1,
                "malformed {name} lane must retain its legacy catalog row"
            );
            assert_eq!(
                vector_namespace_rows(&inspection, physical_id).await,
                source_rows,
                "malformed {name} lane must retain every physical byte"
            );
            inspection.close().await.expect("corrupt inspection closes");
        }
    }

    /// Proves previously consumed and tenant-partitioned legacy definitions use
    /// the existing reconstruction path instead of in-place adoption.
    pub(crate) async fn run_vector_ineligible_contracts() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database_name = database("migration-vector-consumed-id");
        let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .expect("valid vector definition")
        .try_into()
        .expect("valid V2 vector definition");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector runtime definition validates as vector")
        };
        seed_legacy_rows(
            &database_name,
            Arc::clone(&store),
            std::slice::from_ref(&definition),
            None,
        )
        .await;
        let raw = raw(&database_name, Arc::clone(&store)).await;
        seed_legacy_node_without_vector(&raw, 77, "Document").await;
        populate_legacy_vector::<crate::search::vector::distance::Cosine>(
            &raw,
            vector_definition,
            77,
            &[1.0, 0.0, 0.0],
        )
        .await;
        let runtime = vector_definition.to_runtime();
        let legacy_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let legacy_physical_id = crate::search::vector::index_id_from_name(&legacy_name);
        let consumed_next = VectorPhysicalIndexId::new(
            legacy_physical_id
                .checked_add(1)
                .expect("fixture hash leaves room for a later physical ID"),
        )
        .expect("later fixture physical ID is nonzero");
        seed_bootstrap_tuple(
            &raw,
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::CURRENT,
            )),
            true,
            true,
        )
        .await;
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("consumed watermark transaction opens");
        transaction
            .put(
                global(GlobalKey::VectorPhysicalIdWatermark),
                encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                    VectorPhysicalIdWatermark {
                        next_id: consumed_next,
                    },
                )),
            )
            .expect("consumed watermark stages");
        transaction
            .commit()
            .await
            .expect("consumed watermark commits");
        assert!(!vector_namespace_rows(&raw, legacy_physical_id)
            .await
            .is_empty());
        raw.close().await.expect("consumed source closes");

        let rebuilt = HelixDB::open_with_object_store_for_migration_parity(
            database_name,
            store,
            one_row_config(),
        )
        .await
        .expect("consumed legacy namespace rebuilds");
        assert_legacy_catalog_empty(&rebuilt).await;
        let record = crate::index_lifecycle::repository::load_index_record(
            rebuilt.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &definition.identity(),
        )
        .await
        .expect("rebuilt record reads")
        .expect("rebuilt record exists");
        let Some(crate::index_lifecycle::PhysicalGeneration::Vector {
            layout:
                crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: rebuilt_physical_id,
                },
            ..
        }) = record.state().physical()
        else {
            panic!("rebuilt vector remains unpartitioned")
        };
        assert_ne!(rebuilt_physical_id.get(), legacy_physical_id);
        assert_materialized_node_vector(rebuilt.inner_db().as_ref(), 77, &[1.0, 0.0, 0.0]).await;
        assert_vector_search::<crate::search::vector::distance::Cosine>(
            &rebuilt,
            vector_definition,
            &[1.0, 0.0, 0.0],
            77,
        )
        .await;
        assert!(
            vector_namespace_rows(rebuilt.inner_db().as_ref(), legacy_physical_id)
                .await
                .is_empty(),
            "normal rebuild retires its legacy physical namespace"
        );
        assert!(
            crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                rebuilt.inner_db().as_ref(),
                VectorPhysicalIndexId::new(legacy_physical_id)
                    .expect("legacy fixture physical ID is nonzero"),
            )
            .await
            .expect("source reservation reads")
            .is_none(),
            "normal rebuild releases its source reservation"
        );
        rebuilt.close().await.expect("rebuilt database closes");

        let tenant_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tenant_database = database("migration-vector-tenant-partitioned");
        let tenant_definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .expect("valid vector definition")
        .with_tenant_property("tenant")
        .expect("valid tenant property")
        .try_into()
        .expect("valid V2 vector definition");
        seed_legacy_rows(
            &tenant_database,
            Arc::clone(&tenant_store),
            std::slice::from_ref(&tenant_definition),
            None,
        )
        .await;
        let ValidatedDynamicIndexDefinition::Vector(tenant_vector_definition) = &tenant_definition
        else {
            unreachable!("tenant vector definition validates as vector")
        };
        let tenant_source = self::raw(&tenant_database, Arc::clone(&tenant_store)).await;
        let tenant_entity_id = 91;
        let source_tenant = Property::string("tenant", "alpha");
        tenant_source
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
                        crate::encoding::v1::keys::NodePropertyKey::new(tenant_entity_id),
                    ),
                }
                .to_bytes(),
                property::encode_properties(&[
                    Property::string("$label", "Document"),
                    Property::string("title", "retained graph property"),
                    source_tenant.clone(),
                ]),
            )
            .await
            .expect("tenant legacy graph row writes");
        let tenant_runtime = tenant_vector_definition.to_runtime();
        let tenant_physical_name = crate::search::vector_tenant_index_name(
            tenant_runtime.element_type(),
            tenant_runtime.label(),
            tenant_runtime.property(),
            tenant_runtime
                .tenant_property()
                .expect("fixture is tenant partitioned"),
            &source_tenant.value,
        );
        populate_named_legacy_vector::<crate::search::vector::distance::Cosine>(
            &tenant_source,
            tenant_vector_definition,
            tenant_physical_name,
            tenant_entity_id,
            &[0.0, 1.0, 0.0],
        )
        .await;
        tenant_source
            .close()
            .await
            .expect("tenant legacy source closes");
        let tenant = HelixDB::open_with_object_store_for_migration_parity(
            tenant_database,
            tenant_store,
            one_row_config(),
        )
        .await
        .expect("tenant-partitioned legacy definition rebuilds");
        let tenant_record = crate::index_lifecycle::repository::load_index_record(
            tenant.inner_db().as_ref(),
            DataScope::LegacyUnscoped,
            &tenant_definition.identity(),
        )
        .await
        .expect("tenant record reads")
        .expect("tenant record exists");
        assert!(matches!(
            tenant_record.state().physical(),
            Some(crate::index_lifecycle::PhysicalGeneration::Vector {
                layout: crate::index_lifecycle::VectorPhysicalLayout::Partitioned,
                ..
            })
        ));
        assert_legacy_catalog_empty(&tenant).await;
        assert_materialized_node_vector(
            tenant.inner_db().as_ref(),
            tenant_entity_id,
            &[0.0, 1.0, 0.0],
        )
        .await;
        assert!(
            planner_tenant_vector_search_ids(&tenant, "alpha", vec![0.0, 1.0, 0.0])
                .await
                .contains(&tenant_entity_id)
        );
        tenant
            .query(QueryRequest::write(write_batch().var_as(
                "moved",
                g().n(NodeRef::id(tenant_entity_id)).set_property(
                    "tenant",
                    PropertyInput::from(AstPropertyValue::String("beta".to_string())),
                ),
            )))
            .await
            .expect("tenant move without an embedding commits");
        assert!(
            !planner_tenant_vector_search_ids(&tenant, "alpha", vec![0.0, 1.0, 0.0])
                .await
                .contains(&tenant_entity_id)
        );
        assert!(
            planner_tenant_vector_search_ids(&tenant, "beta", vec![0.0, 1.0, 0.0])
                .await
                .contains(&tenant_entity_id)
        );
        tenant.close().await.expect("tenant database closes");
    }

    /// Proves physical-name and reservation ownership inconsistencies fail
    /// before changing the persisted legacy catalog or vector namespace.
    pub(crate) async fn run_vector_ownership_conflict_contracts() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        enum Conflict {
            PhysicalName,
            MalformedReservation,
            ReservationWithoutOwner,
            ExistingV2Owner,
        }

        for (name, conflict) in [
            ("physical-name", Conflict::PhysicalName),
            ("malformed-reservation", Conflict::MalformedReservation),
            (
                "reservation-without-owner",
                Conflict::ReservationWithoutOwner,
            ),
            ("existing-v2-owner", Conflict::ExistingV2Owner),
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let database = database(&format!("migration-vector-conflict-{name}"));
            let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .expect("valid vector definition")
            .try_into()
            .expect("valid V2 vector definition");
            let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
                unreachable!("vector runtime definition validates as vector")
            };
            seed_legacy_rows(
                &database,
                Arc::clone(&store),
                std::slice::from_ref(&definition),
                None,
            )
            .await;
            let source_db = raw(&database, Arc::clone(&store)).await;
            let runtime = vector_definition.to_runtime();
            let physical_name = crate::search::vector_index_name(
                runtime.element_type(),
                runtime.label(),
                runtime.property(),
            );
            let raw_physical_id = crate::search::vector::index_id_from_name(&physical_name);
            let physical_id = VectorPhysicalIndexId::new(raw_physical_id)
                .expect("legacy fixture physical ID is nonzero");
            if !matches!(conflict, Conflict::PhysicalName) {
                seed_bootstrap_tuple(
                    &source_db,
                    encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                        IndexStorageVersion::CURRENT,
                    )),
                    true,
                    true,
                )
                .await;
            }
            let transaction = source_db
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("ownership conflict transaction opens");
            match conflict {
                Conflict::PhysicalName => {
                    let metadata = crate::search::vector::VectorIndexMetadata::new(
                        crate::search::vector::VectorIndexConfig::from_v2_definition(
                            vector_definition,
                            "another-physical-name",
                        ),
                    );
                    transaction
                        .put(
                            Key::Data {
                                scope: DataScope::LegacyUnscoped,
                                kind: crate::encoding::v1::keys::DataKeyKind::Vector(
                                    VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                                        raw_physical_id,
                                    )),
                                ),
                            }
                            .to_bytes(),
                            Bytes::copy_from_slice(
                                &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                                    &metadata,
                                ),
                            ),
                        )
                        .expect("mismatched metadata stages");
                }
                Conflict::MalformedReservation => {
                    transaction
                        .put(
                            global(GlobalKey::LegacyVectorPhysicalReservation(physical_id)),
                            Bytes::from_static(b"malformed"),
                        )
                        .expect("malformed reservation stages");
                }
                Conflict::ReservationWithoutOwner => {
                    transaction
                        .put(
                            global(GlobalKey::LegacyVectorPhysicalReservation(
                                physical_id,
                            )),
                            encode_metadata_value(
                                &IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                                    crate::index_lifecycle::LegacyVectorPhysicalReservation::AdoptedActive {
                                        index_id: IndexId::initial(),
                                        generation: crate::index_lifecycle::IndexGenerationId::initial(),
                                    },
                                ),
                            ),
                        )
                        .expect("inconsistent reservation stages");
                }
                Conflict::ExistingV2Owner => {
                    let owner_definition: ValidatedDynamicIndexDefinition =
                        VectorIndexDefinition::new_node(
                            "ExistingOwner",
                            "embedding",
                            3,
                            VectorDistanceMetric::Cosine,
                        )
                        .expect("valid existing owner definition")
                        .try_into()
                        .expect("valid existing V2 owner definition");
                    let ValidatedDynamicIndexDefinition::Vector(owner_vector) = &owner_definition
                    else {
                        unreachable!("existing V2 owner validates as vector")
                    };
                    let owner_descriptor =
                        crate::index_lifecycle::VectorGenerationDescriptor::for_definition(
                            owner_vector,
                        );
                    let record = crate::index_lifecycle::IndexRecordV2::building(
                        IndexId::initial(),
                        owner_definition,
                        crate::index_lifecycle::IndexRevision::initial(),
                        crate::index_lifecycle::PhysicalGeneration::Vector {
                            generation: crate::index_lifecycle::IndexGenerationId::initial(),
                            layout: crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                                physical_index_id: physical_id,
                            },
                            descriptor: owner_descriptor,
                        },
                        crate::index_lifecycle::IndexOperationId::new_v4(),
                    )
                    .expect("existing owner record builds")
                    .transition(crate::index_lifecycle::IndexStateTransition::Activate)
                    .expect("existing owner record activates");
                    transaction
                        .put(
                            IndexKey::Data {
                                scope: DataScope::LegacyUnscoped,
                                kind: ScopedKey::index_record(record.identity().clone()),
                            }
                            .to_bytes(),
                            encode_index_record(&record),
                        )
                        .expect("existing V2 owner stages");
                    transaction
                        .put(
                            global(GlobalKey::LogicalIndexIdWatermark),
                            encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                                LogicalIndexIdWatermark {
                                    next_id: IndexId::new(2).expect("second logical ID is valid"),
                                },
                            )),
                        )
                        .expect("advanced logical watermark stages");
                }
            }
            transaction
                .commit()
                .await
                .expect("ownership conflict commits");
            let source_rows = vector_namespace_rows(&source_db, raw_physical_id).await;
            let (catalog_key, catalog_value) =
                migration_parity_legacy_catalog_row(&definition, false)
                    .expect("legacy catalog row encodes");
            source_db.close().await.expect("conflict source closes");

            assert!(
                HelixDB::open_with_object_store_for_migration_parity(
                    database.clone(),
                    Arc::clone(&store),
                    one_row_config(),
                )
                .await
                .is_err(),
                "{name} conflict must fail writer migration"
            );
            let inspection = raw(&database, store).await;
            assert_eq!(
                inspection
                    .get(catalog_key)
                    .await
                    .expect("legacy catalog reads"),
                Some(catalog_value),
                "{name} conflict must retain its exact legacy catalog row"
            );
            assert_eq!(
                vector_namespace_rows(&inspection, raw_physical_id).await,
                source_rows,
                "{name} conflict must retain every physical byte"
            );
            inspection
                .close()
                .await
                .expect("conflict inspection closes");
        }
    }

    /// Covers empty/non-empty bootstrap, reader gating, every legacy definition
    /// shape, one-row checkpoints, durable reopen, and idempotent convergence.
    pub(crate) async fn run_migration_contracts() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        let pristine_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let pristine_database = database("reader-gate-pristine");
        let pristine = raw(&pristine_database, Arc::clone(&pristine_store)).await;
        pristine
            .close()
            .await
            .expect("pristine legacy database closes");
        assert_reader_migration_gate(&pristine_database, pristine_store, true).await;

        let tuple_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let tuple_database = database("reader-gate-tuple-only");
        let tuple = raw(&tuple_database, Arc::clone(&tuple_store)).await;
        crate::index_lifecycle::repository::bootstrap_writer(&tuple)
            .await
            .expect("tuple-only writer bootstrap commits");
        assert_current_storage_version(&tuple).await;
        tuple.close().await.expect("tuple-only database closes");
        assert_reader_migration_gate(&tuple_database, Arc::clone(&tuple_store), false).await;
        let tuple_recovered = HelixDB::open_with_object_store_for_migration_parity(
            tuple_database.clone(),
            Arc::clone(&tuple_store),
            one_row_config(),
        )
        .await
        .expect("tuple-only store completes migration");
        tuple_recovered
            .close()
            .await
            .expect("tuple-only recovered writer closes");
        assert_reader_migration_gate(&tuple_database, tuple_store, true).await;

        for (name, graph, index, schema) in [
            (
                "malformed-graph",
                b"bad".as_slice(),
                b"1".as_slice(),
                b"1".as_slice(),
            ),
            (
                "malformed-index",
                b"1".as_slice(),
                b"bad".as_slice(),
                b"1".as_slice(),
            ),
            (
                "malformed-schema",
                b"1".as_slice(),
                b"1".as_slice(),
                b"bad".as_slice(),
            ),
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let database = database(&format!("reader-gate-{name}"));
            let fixture = raw(&database, Arc::clone(&store)).await;
            crate::index_lifecycle::repository::bootstrap_writer(&fixture)
                .await
                .expect("malformed-readiness bootstrap commits");
            fixture
                .put(
                    scoped_metadata_key(DataScope::LegacyUnscoped, GRAPH_FORMAT_V1_READY),
                    Bytes::copy_from_slice(graph),
                )
                .await
                .expect("graph readiness fixture writes");
            fixture
                .put(
                    scoped_metadata_key(DataScope::LegacyUnscoped, INDEX_V2_MIGRATION_READY),
                    Bytes::copy_from_slice(index),
                )
                .await
                .expect("index readiness fixture writes");
            fixture
                .put(
                    scoped_metadata_key(DataScope::LegacyUnscoped, STORAGE_SCHEMA_COMPLETE),
                    Bytes::copy_from_slice(schema),
                )
                .await
                .expect("schema readiness fixture writes");
            fixture
                .close()
                .await
                .expect("malformed-readiness fixture closes");
            assert_reader_migration_gate(&database, store, false).await;
        }

        assert_failpoint_blocks_reader_until_recovery(MigrationFailpoint::BatchCommitAfter).await;
        assert_failpoint_blocks_reader_until_recovery(
            MigrationFailpoint::MigrationReadyPublicationAfter,
        )
        .await;
        assert_failpoint_blocks_reader_until_recovery(
            MigrationFailpoint::StorageSchemaCompletionBefore,
        )
        .await;

        let empty_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let empty_database = database("migration-empty");
        let empty = HelixDB::open_with_object_store_for_migration_parity(
            empty_database,
            Arc::clone(&empty_store),
            one_row_config(),
        )
        .await
        .expect("empty legacy store bootstraps");
        assert_legacy_catalog_empty(&empty).await;
        empty.close().await.expect("empty database closes");

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database("migration-definitions");
        let definitions = definitions();
        let tombstone: ValidatedDynamicIndexDefinition =
            SecondaryIndexDefinition::node_equality("Deleted", "value")
                .expect("valid tombstone definition")
                .try_into()
                .expect("valid V2 tombstone definition");
        seed_legacy_rows(
            &database,
            Arc::clone(&store),
            &definitions,
            Some(&tombstone),
        )
        .await;

        let reader = slatedb::DbReader::builder(database.clone(), Arc::clone(&store))
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("pre-migration reader storage opens");
        crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(&reader)
            .await
            .expect("pristine legacy rows remain readable before writer bootstrap");
        reader.close().await.expect("pre-migration reader closes");

        let migrated = HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            one_row_config(),
        )
        .await
        .expect("all persisted legacy definitions migrate");
        assert_current_storage_version(migrated.inner_db().as_ref()).await;
        assert_legacy_catalog_empty(&migrated).await;
        crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(
            migrated.inner_db().as_ref(),
        )
        .await
        .expect("completed storage schema opens for readers");
        assert_eq!(
            migrated
                .active_index_handles_loaded(DataScope::LegacyUnscoped)
                .len(),
            definitions.len(),
            "every non-tombstoned legacy definition must be Active"
        );
        for definition in &definitions {
            let ValidatedDynamicIndexDefinition::Vector(vector) = definition else {
                continue;
            };
            let runtime = vector.to_runtime();
            let legacy_physical_id =
                crate::search::vector::index_id_from_name(&crate::search::vector_index_name(
                    runtime.element_type(),
                    runtime.label(),
                    runtime.property(),
                ));
            let record = crate::index_lifecycle::repository::load_index_record(
                migrated.inner_db().as_ref(),
                DataScope::LegacyUnscoped,
                &definition.identity(),
            )
            .await
            .expect("migrated vector record reads")
            .expect("migrated vector record exists");
            let crate::index_lifecycle::IndexStateV2::Active {
                physical:
                    crate::index_lifecycle::PhysicalGeneration::Vector {
                        generation,
                        layout:
                            crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                                physical_index_id,
                            },
                        ..
                    },
                ..
            } = record.state()
            else {
                panic!("eligible empty legacy vector adopts one active namespace")
            };
            assert_eq!(physical_index_id.get(), legacy_physical_id);
            assert_eq!(
                crate::index_lifecycle::repository::load_legacy_vector_physical_reservation(
                    migrated.inner_db().as_ref(),
                    *physical_index_id,
                )
                .await
                .expect("empty adopted vector reservation reads"),
                Some(
                    crate::index_lifecycle::LegacyVectorPhysicalReservation::AdoptedActive {
                        index_id: record.index_id(),
                        generation: *generation,
                    }
                )
            );
        }
        migrated.close().await.expect("migrated database closes");

        let reopened =
            HelixDB::open_with_object_store_for_migration_parity(database, store, one_row_config())
                .await
                .expect("converged migration cold-reopens");
        assert_legacy_catalog_empty(&reopened).await;
        assert_eq!(
            reopened
                .active_index_handles_loaded(DataScope::LegacyUnscoped)
                .len(),
            definitions.len(),
            "repeated writer open must retain the exact Active projection"
        );
        reopened.close().await.expect("reopened database closes");
    }

    /// A disabled secondary worker must never leave writer-open waiting on work
    /// that no production surface can advance.
    pub(crate) async fn run_disabled_secondary_worker_open_contract() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database("migration-disabled-secondary-worker");
        let definition: ValidatedDynamicIndexDefinition =
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("valid secondary definition")
                .try_into()
                .expect("valid V2 definition");
        seed_legacy_rows(
            &database,
            Arc::clone(&store),
            std::slice::from_ref(&definition),
            None,
        )
        .await;
        let prerequisite = raw(&database, Arc::clone(&store)).await;
        mark_graph_rewrite_complete(&prerequisite).await;
        mark_vector_materialization_complete(&prerequisite).await;
        prerequisite
            .close()
            .await
            .expect("migration prerequisite fixture closes");
        let config = one_row_config().with_secondary_index_lifecycle_tuning(
            SecondaryIndexLifecycleTuning::default()
                .with_worker_mode(crate::config::SecondaryIndexLifecycleWorkerMode::Disabled),
        );

        let opened = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            HelixDB::open_with_object_store_for_migration_parity(database, store, config),
        )
        .await;

        assert!(
            opened.is_ok(),
            "writer open must return success or a typed error when legacy secondary definitions exist and the lifecycle worker is disabled"
        );
        if let Ok(Ok(db)) = opened {
            db.close().await.expect("completed writer closes");
        }
    }

    async fn seed_bootstrap_tuple(
        raw: &Db,
        marker: Bytes,
        include_logical: bool,
        include_vector: bool,
    ) {
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("bootstrap seed transaction opens");
        transaction
            .put(global(GlobalKey::StorageVersion), marker)
            .expect("marker stages");
        if include_logical {
            transaction
                .put(
                    global(GlobalKey::LogicalIndexIdWatermark),
                    encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                        LogicalIndexIdWatermark {
                            next_id: IndexId::initial(),
                        },
                    )),
                )
                .expect("logical watermark stages");
        }
        if include_vector {
            transaction
                .put(
                    global(GlobalKey::VectorPhysicalIdWatermark),
                    encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                        VectorPhysicalIdWatermark {
                            next_id: VectorPhysicalIndexId::initial(),
                        },
                    )),
                )
                .expect("vector watermark stages");
        }
        transaction.commit().await.expect("bootstrap seed commits");
    }

    /// Covers missing, malformed, partial, older, and future bootstrap metadata.
    pub(crate) async fn run_bootstrap_rejection_contracts() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        for (name, marker, include_logical, include_vector) in [
            ("malformed", Bytes::from_static(b"bad"), true, true),
            (
                "partial",
                encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                    IndexStorageVersion::CURRENT,
                )),
                false,
                true,
            ),
            (
                "older",
                encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                    IndexStorageVersion::new(1).expect("legacy marker 1 is nonzero"),
                )),
                true,
                true,
            ),
            (
                "future",
                encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                    IndexStorageVersion::new(IndexStorageVersion::CURRENT.get() + 1)
                        .expect("future version is nonzero"),
                )),
                true,
                true,
            ),
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let database = database(name);
            let raw = raw(&database, Arc::clone(&store)).await;
            seed_bootstrap_tuple(&raw, marker, include_logical, include_vector).await;
            let error = crate::index_lifecycle::repository::bootstrap_writer(&raw)
                .await
                .expect_err("invalid bootstrap tuple must fail closed");
            assert!(
                matches!(
                    error,
                    HelixDbError::MigrationRequired { .. }
                        | HelixDbError::UnsupportedIndexStorageVersion { .. }
                ),
                "unexpected bootstrap rejection: {error}"
            );
            raw.close()
                .await
                .expect("invalid bootstrap database closes");
        }
    }

    /// Proves an injected migration error preserves the legacy source row and
    /// that a later clean reopen converges it through the normal V2 worker.
    pub(crate) async fn run_failure_preservation_contract() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database("migration-failure-preservation");
        let definition: ValidatedDynamicIndexDefinition =
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition");
        seed_legacy_rows(
            &database,
            Arc::clone(&store),
            std::slice::from_ref(&definition),
            None,
        )
        .await;
        inject_migration_failpoint_once(MigrationFailpoint::LegacyDefinitionEnqueueBefore)
            .expect("failpoint injects");
        assert!(
            HelixDB::open_with_object_store_for_migration_parity(
                database.clone(),
                Arc::clone(&store),
                one_row_config(),
            )
            .await
            .is_err(),
            "injected enqueue failure must fail the writer open"
        );
        let raw = raw(&database, Arc::clone(&store)).await;
        assert_eq!(
            load_legacy_definition_rows(&raw, DataScope::LegacyUnscoped)
                .await
                .expect("legacy rows scan")
                .len(),
            1,
            "failed migration must preserve its source definition"
        );
        raw.close().await.expect("inspection database closes");
        let recovered =
            HelixDB::open_with_object_store_for_migration_parity(database, store, one_row_config())
                .await
                .expect("clean reopen resumes failed migration");
        assert_legacy_catalog_empty(&recovered).await;
        recovered.close().await.expect("recovered database closes");
    }

    /// Closed set of vector migration stages that own generic batch work.
    ///
    /// Encoding the two materialization stages and six retirement stages as
    /// variants prevents the recovery harness from constructing an unrelated
    /// migration/stage pair.
    #[derive(Debug, Clone, Copy)]
    enum VectorRecoveryStage {
        MaterializeNodes,
        MaterializeEdges,
        FenceLegacySources,
        RetireHotRows,
        RetireLayerZeroRows,
        RetireCoreRows,
        RetireDefinitions,
        ReleaseReservations,
    }

    impl VectorRecoveryStage {
        const ALL: [Self; 8] = [
            Self::MaterializeNodes,
            Self::MaterializeEdges,
            Self::FenceLegacySources,
            Self::RetireHotRows,
            Self::RetireLayerZeroRows,
            Self::RetireCoreRows,
            Self::RetireDefinitions,
            Self::ReleaseReservations,
        ];

        const fn migration_id(self) -> MigrationId {
            match self {
                Self::MaterializeNodes | Self::MaterializeEdges => {
                    MigrationId::LegacyVectorPropertyMaterialization
                }
                Self::FenceLegacySources
                | Self::RetireHotRows
                | Self::RetireLayerZeroRows
                | Self::RetireCoreRows
                | Self::RetireDefinitions
                | Self::ReleaseReservations => MigrationId::LegacyVectorPhysicalCleanup,
            }
        }

        const fn migration_stage(self) -> MigrationStage {
            match self {
                Self::MaterializeNodes => MigrationStage::NodeProperties,
                Self::MaterializeEdges => MigrationStage::EdgeEndpoints,
                Self::FenceLegacySources => MigrationStage::FenceLegacyVectorSources,
                Self::RetireHotRows => MigrationStage::LegacyVectorHotRows,
                Self::RetireLayerZeroRows => MigrationStage::LegacyVectorLayer0Rows,
                Self::RetireCoreRows => MigrationStage::LegacyVectorCoreRows,
                Self::RetireDefinitions => MigrationStage::LegacyVectorDefinitions,
                Self::ReleaseReservations => MigrationStage::ReleaseLegacyVectorReservations,
            }
        }

        const fn name(self) -> &'static str {
            match self {
                Self::MaterializeNodes => "materialize-nodes",
                Self::MaterializeEdges => "materialize-edges",
                Self::FenceLegacySources => "fence-legacy-sources",
                Self::RetireHotRows => "retire-hot-rows",
                Self::RetireLayerZeroRows => "retire-layer-zero-rows",
                Self::RetireCoreRows => "retire-core-rows",
                Self::RetireDefinitions => "retire-definitions",
                Self::ReleaseReservations => "release-reservations",
            }
        }

        fn definition(self) -> ValidatedDynamicIndexDefinition {
            let definition = match self {
                Self::MaterializeEdges => VectorIndexDefinition::new_edge(
                    "RELATED",
                    "embedding",
                    3,
                    VectorDistanceMetric::Cosine,
                ),
                Self::MaterializeNodes
                | Self::FenceLegacySources
                | Self::RetireHotRows
                | Self::RetireLayerZeroRows
                | Self::RetireCoreRows
                | Self::RetireDefinitions
                | Self::ReleaseReservations => VectorIndexDefinition::new_node(
                    "Document",
                    "embedding",
                    3,
                    VectorDistanceMetric::Cosine,
                ),
            }
            .expect("recovery vector definition validates");
            definition
                .try_into()
                .expect("recovery V2 vector definition validates")
        }

        fn stage_rank(self, stage: MigrationStage) -> Option<u8> {
            match self.migration_id() {
                MigrationId::LegacyVectorPropertyMaterialization => match stage {
                    MigrationStage::NodeProperties => Some(0),
                    MigrationStage::EdgeEndpoints => Some(1),
                    MigrationStage::PropertyIndexes
                    | MigrationStage::LegacyEdgePairs
                    | MigrationStage::FenceLegacyVectorSources
                    | MigrationStage::LegacyVectorHotRows
                    | MigrationStage::LegacyVectorLayer0Rows
                    | MigrationStage::LegacyVectorCoreRows
                    | MigrationStage::LegacyVectorDefinitions
                    | MigrationStage::ReleaseLegacyVectorReservations => None,
                },
                MigrationId::LegacyVectorPhysicalCleanup => match stage {
                    MigrationStage::FenceLegacyVectorSources => Some(0),
                    MigrationStage::LegacyVectorHotRows => Some(1),
                    MigrationStage::LegacyVectorLayer0Rows => Some(2),
                    MigrationStage::LegacyVectorCoreRows => Some(3),
                    MigrationStage::LegacyVectorDefinitions => Some(4),
                    MigrationStage::ReleaseLegacyVectorReservations => Some(5),
                    MigrationStage::PropertyIndexes
                    | MigrationStage::NodeProperties
                    | MigrationStage::LegacyEdgePairs
                    | MigrationStage::EdgeEndpoints => None,
                },
                MigrationId::GraphFormatV1Rewrite | MigrationId::GraphFormatV1Cleanup => None,
            }
        }
    }

    /// Observable state that must survive retirement of a legacy namespace.
    #[derive(Debug)]
    struct RetainedVectorGeneration {
        physical_id: u64,
        digest: [u8; 32],
        retired_physical_id: u64,
    }

    /// Case-specific result checked after a clean recovery open.
    #[derive(Debug)]
    enum VectorRecoveryExpectation {
        MaterializedNode,
        MaterializedEdge,
        Retired(RetainedVectorGeneration),
    }

    /// Fully persisted fixture positioned immediately before its target stage.
    struct PreparedVectorRecovery {
        database: String,
        store: Arc<dyn ObjectStore>,
        definition: ValidatedDynamicIndexDefinition,
        checkpoint: MigrationJob,
        expectation: VectorRecoveryExpectation,
    }

    /// Reads one typed durable migration job from its canonical metadata key.
    async fn read_migration_job(raw: &Db, id: MigrationId) -> MigrationJob {
        let value = raw
            .get(MigrationJobKey::new(DataScope::LegacyUnscoped, id).as_ref())
            .await
            .expect("recovery migration job reads")
            .expect("recovery migration job exists");
        decode_json(&value).expect("recovery migration job decodes")
    }

    /// Uses real controller turns to stop a persisted job at the requested
    /// stage without manufacturing a job-state encoding.
    async fn advance_vector_recovery_job(
        database: &str,
        store: Arc<dyn ObjectStore>,
        recovery_stage: VectorRecoveryStage,
    ) -> MigrationJob {
        let raw = Arc::new(raw(database, store).await);
        let config = one_row_config();
        let writer = crate::HelixWriter::new(Arc::clone(&raw), config.id_lease_size());
        let target = recovery_stage.migration_stage();
        let target_rank = recovery_stage
            .stage_rank(target)
            .expect("recovery target belongs to its migration");
        let checkpoint = loop {
            let job = read_migration_job(raw.as_ref(), recovery_stage.migration_id()).await;
            let MigrationJobState::Running { stage, .. } = &job.state else {
                panic!("recovery fixture completed or failed before {target:?}: {job:?}")
            };
            let rank = recovery_stage
                .stage_rank(*stage)
                .expect("recovery job remains in its closed stage set");
            assert!(rank <= target_rank, "recovery fixture advanced past target");
            if *stage == target {
                break job;
            }
            assert!(
                process_migration_once_by_id(
                    &writer,
                    DataScope::LegacyUnscoped,
                    config.migrations(),
                    recovery_stage.migration_id(),
                )
                .await
                .expect("recovery fixture controller turn succeeds"),
                "recovery fixture controller must advance"
            );
        };
        drop(writer);
        raw.close()
            .await
            .expect("positioned recovery fixture closes");
        checkpoint
    }

    /// Confirms the injected failure left either the old durable checkpoint or
    /// a complete controller turn, never an unrelated or partially advanced
    /// stage.
    fn assert_recovery_checkpoint(
        recovery_stage: VectorRecoveryStage,
        failpoint: MigrationFailpoint,
        before: &MigrationJob,
        after: &MigrationJob,
    ) {
        assert_eq!(before.id, recovery_stage.migration_id());
        assert_eq!(after.id, recovery_stage.migration_id());
        let MigrationJobState::Running {
            stage: before_stage,
            resume_after_key: before_key,
            processed_rows: before_rows,
        } = &before.state
        else {
            panic!("prepared recovery checkpoint must be running")
        };
        assert_eq!(*before_stage, recovery_stage.migration_stage());
        let target_rank = recovery_stage
            .stage_rank(*before_stage)
            .expect("prepared stage belongs to the recovery migration");
        match &after.state {
            MigrationJobState::Running {
                stage,
                resume_after_key,
                processed_rows,
            }
            | MigrationJobState::Failed {
                stage,
                resume_after_key,
                processed_rows,
                ..
            } => {
                let after_rank = recovery_stage
                    .stage_rank(*stage)
                    .expect("failed checkpoint belongs to the recovery migration");
                if failpoint == MigrationFailpoint::StageTransitionAfter {
                    assert_eq!(after_rank, target_rank.saturating_add(1));
                } else {
                    assert_eq!(after_rank, target_rank);
                }
                assert!(*processed_rows >= *before_rows);
                if processed_rows == before_rows {
                    assert_eq!(resume_after_key, before_key);
                }
            }
            MigrationJobState::Completed { processed_rows } => {
                assert_eq!(failpoint, MigrationFailpoint::StageTransitionAfter);
                assert!(*processed_rows >= *before_rows);
            }
        }
    }

    /// Builds one materialization or retirement fixture and positions it at the
    /// exact stage under test through the real migration controller.
    async fn prepare_vector_recovery(
        recovery_stage: VectorRecoveryStage,
    ) -> PreparedVectorRecovery {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database(&format!("migration-vector-{}", recovery_stage.name()));
        let definition = recovery_stage.definition();
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("recovery definition is always vector")
        };

        let expectation = match recovery_stage {
            VectorRecoveryStage::MaterializeNodes | VectorRecoveryStage::MaterializeEdges => {
                seed_legacy_rows(
                    &database,
                    Arc::clone(&store),
                    std::slice::from_ref(&definition),
                    None,
                )
                .await;
                let source = raw(&database, Arc::clone(&store)).await;
                mark_graph_rewrite_complete(&source).await;
                put_migration_job(
                    &source,
                    MigrationJob::new(
                        MigrationId::LegacyVectorPropertyMaterialization,
                        MigrationMode::BlockingStartup,
                    ),
                )
                .await;
                let expectation = match recovery_stage {
                    VectorRecoveryStage::MaterializeNodes => {
                        seed_legacy_node_without_vector(&source, 77, "Document").await;
                        populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                            &source,
                            vector_definition,
                            77,
                            &[1.0, 0.0, 0.0],
                        )
                        .await;
                        VectorRecoveryExpectation::MaterializedNode
                    }
                    VectorRecoveryStage::MaterializeEdges => {
                        seed_legacy_edge_without_vector(&source, 77, 11, 12, "RELATED").await;
                        populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                            &source,
                            vector_definition,
                            77,
                            &[1.0, 0.0, 0.0],
                        )
                        .await;
                        VectorRecoveryExpectation::MaterializedEdge
                    }
                    VectorRecoveryStage::FenceLegacySources
                    | VectorRecoveryStage::RetireHotRows
                    | VectorRecoveryStage::RetireLayerZeroRows
                    | VectorRecoveryStage::RetireCoreRows
                    | VectorRecoveryStage::RetireDefinitions
                    | VectorRecoveryStage::ReleaseReservations => {
                        unreachable!("materialization branch excludes retirement recovery stages")
                    }
                };
                source.close().await.expect("materialization source closes");
                expectation
            }
            VectorRecoveryStage::FenceLegacySources
            | VectorRecoveryStage::RetireHotRows
            | VectorRecoveryStage::RetireLayerZeroRows
            | VectorRecoveryStage::RetireCoreRows
            | VectorRecoveryStage::RetireDefinitions
            | VectorRecoveryStage::ReleaseReservations => {
                let active = HelixDB::open_with_object_store_for_migration_parity(
                    database.clone(),
                    Arc::clone(&store),
                    one_row_config(),
                )
                .await
                .expect("retirement Active fixture opens");
                active
                    .inner_db()
                    .put(
                        Key::Data {
                            scope: DataScope::LegacyUnscoped,
                            kind: DataKeyKind::NodeProperty(
                                crate::encoding::v1::keys::NodePropertyKey::new(77),
                            ),
                        }
                        .to_bytes(),
                        property::encode_properties(&[
                            Property::string("$label", "Document"),
                            Property::string("title", "retained graph property"),
                            Property::f32_array("embedding", vec![1.0, 0.0, 0.0]),
                        ]),
                    )
                    .await
                    .expect("current graph vector row writes");
                active
                    .install_index_for_tests(definition.clone())
                    .await
                    .expect("retirement Active fixture builds");
                let current_handle = active
                    .active_index_handles_loaded(DataScope::LegacyUnscoped)
                    .into_iter()
                    .find(|handle| {
                        matches!(
                            handle,
                            crate::index_lifecycle::ActiveIndexHandle::Vector {
                                definition: active_definition,
                                ..
                            } if active_definition.as_ref() == vector_definition
                        )
                    })
                    .expect("retained current vector handle is active");
                let crate::index_lifecycle::ActiveIndexHandle::Vector { layout, .. } =
                    &current_handle
                else {
                    unreachable!("matched current handle remains vector")
                };
                let current_handle_physical_id = layout
                    .physical_index_id()
                    .expect("retained current vector is unpartitioned");
                let current_generation =
                    crate::search::vector::ValidatedVectorGenerationHandle::try_from_active::<
                        crate::search::vector::distance::Cosine,
                    >(&current_handle, current_handle_physical_id)
                    .expect("retained current vector handle validates");
                let transaction = active
                    .inner_db()
                    .begin(IsolationLevel::SerializableSnapshot)
                    .await
                    .expect("retained current vector transaction opens");
                crate::search::vector::VectorIndex::<
                    crate::search::vector::distance::Cosine,
                >::from_generation(&current_generation)
                .insert(&transaction, 77, &[1.0, 0.0, 0.0])
                .await
                .expect("retained current vector inserts");
                transaction
                    .commit()
                    .await
                    .expect("retained current vector commits");
                let active_record = crate::index_lifecycle::repository::load_index_record(
                    active.inner_db().as_ref(),
                    DataScope::LegacyUnscoped,
                    &definition.identity(),
                )
                .await
                .expect("current vector record reads")
                .expect("current vector record exists");
                let crate::index_lifecycle::IndexStateV2::Active {
                    physical:
                        PhysicalGeneration::Vector {
                            layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                            ..
                        },
                    ..
                } = active_record.state()
                else {
                    panic!("current vector record is active and unpartitioned")
                };
                let current_physical_id = physical_index_id.get();
                let current_digest =
                    non_metadata_vector_digest(active.inner_db().as_ref(), current_physical_id)
                        .await;
                let empty_digest: [u8; 32] = Sha256::digest([]).into();
                assert_ne!(current_digest, empty_digest);
                active
                    .close()
                    .await
                    .expect("retirement Active fixture closes");

                let source = raw(&database, Arc::clone(&store)).await;
                source
                    .delete(scoped_metadata_key(
                        DataScope::LegacyUnscoped,
                        INDEX_V2_MIGRATION_READY,
                    ))
                    .await
                    .expect("readiness resets before legacy source insertion");
                let (definition_key, definition_value) =
                    migration_parity_legacy_catalog_row(&definition, false)
                        .expect("legacy vector definition encodes");
                source
                    .put(definition_key, definition_value)
                    .await
                    .expect("legacy vector definition writes");
                populate_legacy_vector::<crate::search::vector::distance::Cosine>(
                    &source,
                    vector_definition,
                    77,
                    &[1.0, 0.0, 0.0],
                )
                .await;
                let runtime = vector_definition.to_runtime();
                let legacy_name = crate::search::vector_index_name(
                    runtime.element_type(),
                    runtime.label(),
                    runtime.property(),
                );
                let retired_physical_id = crate::search::vector::index_id_from_name(&legacy_name);
                assert_ne!(retired_physical_id, current_physical_id);
                let retired_physical_id_typed = VectorPhysicalIndexId::new(retired_physical_id)
                    .expect("legacy physical id is positive");
                source
                    .put(
                        IndexKey::Global {
                            kind: GlobalKey::LegacyVectorPhysicalReservation(
                                retired_physical_id_typed,
                            ),
                        }
                        .to_bytes(),
                        encode_metadata_value(
                            &IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                                crate::index_lifecycle::LegacyVectorPhysicalReservation::LegacySource,
                            ),
                        ),
                    )
                    .await
                    .expect("legacy Active reservation writes");
                mark_graph_rewrite_complete(&source).await;
                mark_vector_materialization_complete(&source).await;
                reset_vector_cleanup_job(&source).await;
                source.close().await.expect("retirement source closes");
                VectorRecoveryExpectation::Retired(RetainedVectorGeneration {
                    physical_id: current_physical_id,
                    digest: current_digest,
                    retired_physical_id,
                })
            }
        };
        let checkpoint =
            advance_vector_recovery_job(&database, Arc::clone(&store), recovery_stage).await;
        PreparedVectorRecovery {
            database,
            store,
            definition,
            checkpoint,
            expectation,
        }
    }

    /// Proves every generic boundary is recoverable at both materialization
    /// stages and every physical-retirement stage (8 × 8 = 64 cases).
    pub(crate) async fn run_vector_failpoint_recovery_contracts() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        const BOUNDARIES: [MigrationFailpoint; 8] = [
            MigrationFailpoint::BatchReadBefore,
            MigrationFailpoint::BatchReadAfter,
            MigrationFailpoint::BatchWriteBefore,
            MigrationFailpoint::BatchWriteAfter,
            MigrationFailpoint::BatchCommitBefore,
            MigrationFailpoint::BatchCommitAfter,
            MigrationFailpoint::StageTransitionBefore,
            MigrationFailpoint::StageTransitionAfter,
        ];

        for recovery_stage in VectorRecoveryStage::ALL {
            for failpoint in BOUNDARIES {
                let prepared = prepare_vector_recovery(recovery_stage).await;
                inject_migration_failpoint_once(failpoint)
                    .expect("stage-targeted vector failpoint injects");
                assert!(
                    HelixDB::open_with_object_store_for_migration_parity(
                        prepared.database.clone(),
                        Arc::clone(&prepared.store),
                        one_row_config(),
                    )
                    .await
                    .is_err(),
                    "{} must interrupt {}",
                    failpoint.as_str(),
                    recovery_stage.name()
                );
                assert!(
                    migration_failpoint_was_triggered(),
                    "{} must fire in {}",
                    failpoint.as_str(),
                    recovery_stage.name()
                );
                let inspection = raw(&prepared.database, Arc::clone(&prepared.store)).await;
                assert!(
                    !index_v2_migration_ready(&inspection, DataScope::LegacyUnscoped)
                        .await
                        .expect("failed recovery readiness reads"),
                    "readiness must remain unpublished after an injected failure"
                );
                let failed_job =
                    read_migration_job(&inspection, recovery_stage.migration_id()).await;
                assert_recovery_checkpoint(
                    recovery_stage,
                    failpoint,
                    &prepared.checkpoint,
                    &failed_job,
                );
                inspection
                    .close()
                    .await
                    .expect("failed recovery inspection closes");

                let recovered = HelixDB::open_with_object_store_for_migration_parity(
                    prepared.database,
                    prepared.store,
                    one_row_config(),
                )
                .await
                .expect("clean recovery open converges");
                assert!(migration_completed(
                    recovered.inner_db().as_ref(),
                    DataScope::LegacyUnscoped,
                    recovery_stage.migration_id(),
                )
                .await
                .expect("recovered migration state reads"));
                assert_legacy_catalog_empty(&recovered).await;
                let ValidatedDynamicIndexDefinition::Vector(vector_definition) =
                    &prepared.definition
                else {
                    unreachable!("recovery definition remains vector")
                };
                match prepared.expectation {
                    VectorRecoveryExpectation::MaterializedNode => {
                        assert_materialized_node_vector(
                            recovered.inner_db().as_ref(),
                            77,
                            &[1.0, 0.0, 0.0],
                        )
                        .await;
                    }
                    VectorRecoveryExpectation::MaterializedEdge => {
                        assert_materialized_edge_vector(
                            recovered.inner_db().as_ref(),
                            77,
                            &[1.0, 0.0, 0.0],
                        )
                        .await;
                    }
                    VectorRecoveryExpectation::Retired(retained) => {
                        assert_eq!(
                            non_metadata_vector_digest(
                                recovered.inner_db().as_ref(),
                                retained.physical_id,
                            )
                            .await,
                            retained.digest,
                            "current V2 physical rows must survive legacy retirement"
                        );
                        assert!(
                            vector_namespace_rows(
                                recovered.inner_db().as_ref(),
                                retained.retired_physical_id,
                            )
                            .await
                            .is_empty(),
                            "legacy vector namespace must be fully retired"
                        );
                    }
                }
                assert_vector_search::<crate::search::vector::distance::Cosine>(
                    &recovered,
                    vector_definition,
                    &[1.0, 0.0, 0.0],
                    77,
                )
                .await;
                recovered.close().await.expect("recovery database closes");
            }
        }
    }

    /// Proves zero-cosine failure retains the exact durable cursor and retries
    /// the same entity after its legacy payload is repaired.
    pub(crate) async fn run_vector_zero_cosine_recovery_contract() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = database("migration-vector-zero-cosine-recovery");
        let definition: ValidatedDynamicIndexDefinition = VectorIndexDefinition::new_node(
            "Document",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .expect("valid vector definition")
        .try_into()
        .expect("valid V2 vector definition");
        let ValidatedDynamicIndexDefinition::Vector(vector_definition) = &definition else {
            unreachable!("vector definition validates as vector")
        };
        seed_legacy_rows(
            &database,
            Arc::clone(&store),
            std::slice::from_ref(&definition),
            None,
        )
        .await;
        let source = raw(&database, Arc::clone(&store)).await;
        mark_graph_rewrite_complete(&source).await;
        let preceding_entity_id = 76;
        let zero_entity_id = 77;
        seed_legacy_node_without_vector(&source, preceding_entity_id, "Document").await;
        seed_legacy_node_without_vector(&source, zero_entity_id, "Document").await;
        let runtime = vector_definition.to_runtime();
        let physical_name = crate::search::vector_index_name(
            runtime.element_type(),
            runtime.label(),
            runtime.property(),
        );
        let physical_id = crate::search::vector::index_id_from_name(&physical_name);
        let simhash_bits = 0_u64;
        let mut metadata = crate::search::vector::VectorIndexMetadata::new(
            crate::search::vector::VectorIndexConfig::from_v2_definition(
                vector_definition,
                &physical_name,
            ),
        );
        metadata.entry_point = Some(zero_entity_id);
        metadata.count = 1;
        let transaction = source
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("zero-cosine fixture transaction opens");
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::Vector(
                        VectorKey::IndexMetadata(VectorIndexMetadataKey::new(physical_id)),
                    ),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v1::values::vectors::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .expect("zero-cosine metadata stages");
        transaction
            .put(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::SimHash(
                        VectorSimHashKey::new(physical_id, zero_entity_id),
                    )),
                }
                .to_bytes(),
                Bytes::copy_from_slice(
                    &crate::encoding::v1::values::vectors::simhash::encode_simhash(simhash_bits),
                ),
            )
            .expect("zero-cosine SimHash stages");
        let item_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v1::keys::DataKeyKind::Vector(VectorKey::Vector(
                VectorItemKey::new(
                    physical_id,
                    crate::search::vector::simhash::order_code_from_simhash_bits(simhash_bits),
                    zero_entity_id,
                ),
            )),
        }
        .to_bytes();
        transaction
            .put(
                item_key.clone(),
                crate::search::vector::encode_item(&crate::search::vector::Item::<
                    crate::search::vector::distance::Cosine,
                >::new(vec![0.0, 0.0, 0.0])),
            )
            .expect("zero-cosine item stages");
        transaction
            .commit()
            .await
            .expect("zero-cosine fixture commits");
        source.close().await.expect("zero-cosine source closes");

        let error = HelixDB::open_with_object_store_for_migration_parity(
            database.clone(),
            Arc::clone(&store),
            one_row_config(),
        )
        .await
        .err()
        .expect("zero-cosine materialization must block writer open");
        assert!(matches!(
            error,
            HelixDbError::LegacyZeroNormCosineVector {
                element_kind: crate::index_lifecycle::IndexElementKind::Node,
                entity_id: 77,
                ..
            }
        ));
        let inspection = raw(&database, Arc::clone(&store)).await;
        let persisted = inspection
            .get(
                MigrationJobKey::new(
                    DataScope::LegacyUnscoped,
                    MigrationId::LegacyVectorPropertyMaterialization,
                )
                .as_ref(),
            )
            .await
            .expect("failed materialization job reads")
            .expect("failed materialization job exists");
        let failed =
            decode_json::<MigrationJob>(&persisted).expect("failed materialization job decodes");
        let MigrationJobState::Failed {
            stage: MigrationStage::NodeProperties,
            resume_after_key: Some(resume_after_key),
            processed_rows: 1,
            ..
        } = failed.state
        else {
            panic!("zero-cosine failure must retain the prior committed node cursor")
        };
        let preceding_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: crate::encoding::v1::keys::DataKeyKind::NodeProperty(
                crate::encoding::v1::keys::NodePropertyKey::new(preceding_entity_id),
            ),
        }
        .to_bytes();
        assert_eq!(resume_after_key.as_bytes(), preceding_key.as_ref());
        inspection
            .put(
                item_key,
                crate::search::vector::encode_item(&crate::search::vector::Item::<
                    crate::search::vector::distance::Cosine,
                >::new(vec![1.0, 0.0, 0.0])),
            )
            .await
            .expect("corrected cosine payload writes");
        inspection
            .close()
            .await
            .expect("zero-cosine inspection closes");

        let recovered =
            HelixDB::open_with_object_store_for_migration_parity(database, store, one_row_config())
                .await
                .expect("corrected zero-cosine migration resumes");
        assert_materialized_node_vector(
            recovered.inner_db().as_ref(),
            zero_entity_id,
            &[1.0, 0.0, 0.0],
        )
        .await;
        assert_legacy_catalog_empty(&recovered).await;
        recovered
            .close()
            .await
            .expect("zero-cosine recovery closes");
    }

    async fn seed_legacy_row_after_v2(
        database: &str,
        store: Arc<dyn ObjectStore>,
        definition: &ValidatedDynamicIndexDefinition,
    ) {
        let raw = raw(database, store).await;
        let transaction = raw
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .expect("post-V2 legacy transaction opens");
        transaction
            .delete(scoped_metadata_key(
                DataScope::LegacyUnscoped,
                INDEX_V2_MIGRATION_READY,
            ))
            .expect("readiness marker deletion stages");
        let (key, value) = migration_parity_legacy_catalog_row(definition, false)
            .expect("legacy definition encodes");
        transaction.put(key, value).expect("legacy row stages");
        transaction
            .commit()
            .await
            .expect("post-V2 legacy row commits");
        raw.close().await.expect("post-V2 seed database closes");
    }

    /// Proves an exact already-Active definition retires its legacy row while a
    /// same-identity settings conflict fails closed and preserves both sources.
    pub(crate) async fn run_existing_active_and_conflict_contracts() {
        let _failpoint_guard = MIGRATION_FAILPOINT_CONTRACT_LOCK.lock().await;
        let exact_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let exact_database = database("migration-already-active");
        let exact: ValidatedDynamicIndexDefinition =
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition");
        let exact_db = HelixDB::open_with_object_store_for_migration_parity(
            exact_database.clone(),
            Arc::clone(&exact_store),
            one_row_config(),
        )
        .await
        .expect("exact Active fixture opens");
        exact_db
            .install_index_for_tests(exact.clone())
            .await
            .expect("exact Active fixture builds");
        exact_db.close().await.expect("exact fixture closes");
        seed_legacy_row_after_v2(&exact_database, Arc::clone(&exact_store), &exact).await;
        let exact_reopened = HelixDB::open_with_object_store_for_migration_parity(
            exact_database,
            exact_store,
            one_row_config(),
        )
        .await
        .expect("exact Active legacy row retires");
        assert_legacy_catalog_empty(&exact_reopened).await;
        exact_reopened
            .close()
            .await
            .expect("exact reopened database closes");

        let conflict_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let conflict_database = database("migration-conflict");
        let active: ValidatedDynamicIndexDefinition =
            SecondaryIndexDefinition::node_equality("User", "email")
                .expect("valid definition")
                .try_into()
                .expect("valid V2 definition");
        let conflict: ValidatedDynamicIndexDefinition =
            SecondaryIndexDefinition::node_unique_equality("User", "email")
                .expect("valid conflicting definition")
                .try_into()
                .expect("valid conflicting V2 definition");
        let conflict_db = HelixDB::open_with_object_store_for_migration_parity(
            conflict_database.clone(),
            Arc::clone(&conflict_store),
            one_row_config(),
        )
        .await
        .expect("conflict fixture opens");
        conflict_db
            .install_index_for_tests(active)
            .await
            .expect("conflict Active fixture builds");
        conflict_db.close().await.expect("conflict fixture closes");
        seed_legacy_row_after_v2(&conflict_database, Arc::clone(&conflict_store), &conflict).await;
        assert!(
            HelixDB::open_with_object_store_for_migration_parity(
                conflict_database.clone(),
                Arc::clone(&conflict_store),
                one_row_config(),
            )
            .await
            .is_err(),
            "same-identity definition conflict must fail closed"
        );
        let raw = raw(&conflict_database, conflict_store).await;
        assert_eq!(
            load_legacy_definition_rows(&raw, DataScope::LegacyUnscoped)
                .await
                .expect("conflicting legacy row scans")
                .len(),
            1,
            "conflict must not delete legacy source data"
        );
        assert_eq!(
            crate::index_lifecycle::repository::load_scope_catalog(&raw, DataScope::LegacyUnscoped)
                .await
                .expect("canonical Active catalog remains valid")
                .active_handles()
                .count(),
            1,
            "conflict must not delete canonical V2 ownership"
        );
        raw.close().await.expect("conflict inspection closes");
    }
}

#[cfg(test)]
#[path = "../tests/unit/migrations_contracts.rs"]
mod external_contracts;
