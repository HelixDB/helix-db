//! Bounded retirement of legacy-only vector namespaces.
//!
//! Rebuilt V2 generations use freshly allocated physical IDs, so the old
//! hash-derived HNSW rows must be deleted without risking adopted or newly
//! allocated namespaces. A durable `RetiringSource` reservation fences each
//! eligible source before lane deletion starts. Every stage advances with the
//! generic migration row and byte limits, and the legacy definition plus
//! reservation remain until all physical rows are durably absent.

use std::collections::BTreeMap;

use bytes::Bytes;
use slatedb::{DbReadOps, DbTransaction};

use crate::config;
use crate::encoding::keys::tenant::DataScope;
use crate::encoding::v1::keys::vectors::{
    VectorIndexMetadataKey, VectorKey, VectorStorageLane, VectorTxnGuardKey,
};
use crate::encoding::v1::keys::{DataKeyKind, Key};
use crate::encoding::v1::values::vectors::metadata::decode_legacy_metadata;
use crate::encoding::v2::keys::Key as IndexKey;
use crate::encoding::v2::keys::{GlobalKey, RecordKind, ScopedKey};
use crate::encoding::v2::values::{
    decode_index_record, decode_metadata_value, encode_metadata_value,
};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{
    IndexGenerationId, IndexId, IndexStateV2, IndexV2MetadataValue,
    LegacyVectorPhysicalReservation, ValidatedDynamicIndexDefinition,
    ValidatedVectorIndexDefinition, VectorPhysicalIndexId,
};
use crate::search::vector::{self, VectorIndexConfig};

use super::{
    scan_bounds_for_prefix, LegacyDefinitionRow, LegacyDynamicIndexCatalogEntry,
    LegacyDynamicIndexDefinition, MigrationBatch, MigrationJob, MigrationJobState,
    MigrationResumeKey, MigrationStage,
};

#[derive(Clone)]
struct ActiveVectorOwner {
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: ValidatedVectorIndexDefinition,
}

struct LegacyVectorRetirementSource {
    owner: ActiveVectorOwner,
    exact_name: Option<String>,
    tenant_prefix: Option<String>,
}

/// Immutable active-generation and remaining-source authority for one cleanup run.
///
/// Rebuilding this catalog after a crash is safe because migration is exclusive
/// and every retained `RetiringSource` names its exact active generation.
pub(super) struct LegacyVectorRetirementCatalog {
    active: BTreeMap<(IndexId, IndexGenerationId), ValidatedVectorIndexDefinition>,
    sources: Vec<LegacyVectorRetirementSource>,
}

impl LegacyVectorRetirementCatalog {
    /// Loads canonical active vector owners and validates every remaining legacy source.
    pub(super) async fn load(
        read: &(impl DbReadOps + Send + Sync),
        scope: DataScope,
    ) -> Result<Self> {
        let prefix =
            IndexKey::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::IndexRecord));
        let mut rows = read.scan_prefix(prefix, ..).await?;
        let mut active = BTreeMap::new();
        while let Some(row) = rows.next().await? {
            let IndexKey::Data {
                kind: ScopedKey::IndexRecord(key),
                ..
            } = IndexKey::parse_from_slice(scope, &row.key)?
            else {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "vector retirement catalog scan returned another key kind".to_string(),
                ));
            };
            let record = decode_index_record(&row.value)?;
            if key.identity != *record.identity() {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "vector retirement found an index key/value identity mismatch".to_string(),
                ));
            }
            let ValidatedDynamicIndexDefinition::Vector(definition) = record.definition() else {
                continue;
            };
            if !matches!(record.state(), IndexStateV2::Active { .. }) {
                continue;
            }
            let owner = (record.index_id(), record.state().generation());
            if active.insert(owner, definition.clone()).is_some() {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "multiple active vector records share one owner generation".to_string(),
                ));
            }
        }

        let mut sources = Vec::new();
        for row in super::load_legacy_definition_rows(read, scope).await? {
            let LegacyDynamicIndexCatalogEntry::Definition(legacy) = row.entry else {
                return Err(HelixDbError::MigrationRequired {
                    reason: "legacy tombstone remained before vector physical cleanup".to_string(),
                });
            };
            if legacy.key() != row.identity {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "vector retirement found a legacy key/value identity mismatch".to_string(),
                ));
            }
            let LegacyDynamicIndexDefinition::Vector(_) = &legacy else {
                return Err(HelixDbError::MigrationRequired {
                    reason: "non-vector legacy definition remained before vector physical cleanup"
                        .to_string(),
                });
            };
            let ValidatedDynamicIndexDefinition::Vector(definition) = legacy.into_validated()?
            else {
                unreachable!("legacy vector definition validates as vector")
            };
            let mut owners = active
                .iter()
                .filter(|(_, active_definition)| *active_definition == &definition)
                .map(
                    |((index_id, generation), active_definition)| ActiveVectorOwner {
                        index_id: *index_id,
                        generation: *generation,
                        definition: active_definition.clone(),
                    },
                );
            let Some(owner) = owners.next() else {
                return Err(HelixDbError::MigrationRequired {
                    reason: "legacy vector cleanup requires its exact V2 definition to be active"
                        .to_string(),
                });
            };
            if owners.next().is_some() {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "multiple active generations match one legacy vector definition".to_string(),
                ));
            }
            let runtime = definition.to_runtime();
            let (exact_name, tenant_prefix) = match runtime.tenant_property() {
                None => (
                    Some(crate::search::vector_index_name(
                        runtime.element_type(),
                        runtime.label(),
                        runtime.property(),
                    )),
                    None,
                ),
                Some(tenant_property) => (
                    None,
                    Some(crate::search::vector_tenant_index_name_prefix(
                        runtime.element_type(),
                        runtime.label(),
                        runtime.property(),
                        tenant_property,
                    )),
                ),
            };
            sources.push(LegacyVectorRetirementSource {
                owner,
                exact_name,
                tenant_prefix,
            });
        }
        Ok(Self { active, sources })
    }

    fn active_owner(
        &self,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Result<&ValidatedVectorIndexDefinition> {
        self.active
            .get(&(index_id, generation))
            .ok_or_else(|| HelixDbError::MigrationRequired {
                reason: format!(
                    "retiring legacy vector owner {} generation {} is not active",
                    index_id.get(),
                    generation.get()
                ),
            })
    }

    fn source_for_name(&self, physical_name: &str) -> Result<&LegacyVectorRetirementSource> {
        let mut matches = self.sources.iter().filter(|source| {
            source.exact_name.as_deref() == Some(physical_name)
                || source
                    .tenant_prefix
                    .as_deref()
                    .is_some_and(|prefix| physical_name.starts_with(prefix))
        });
        let Some(source) = matches.next() else {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "legacy vector namespace '{physical_name}' has no active definition owner"
                ),
            });
        };
        if matches.next().is_some() {
            return Err(HelixDbError::IndexCatalogCorruption(format!(
                "legacy vector namespace '{physical_name}' matches multiple definitions"
            )));
        }
        Ok(source)
    }
}

fn reservation_key(physical_id: VectorPhysicalIndexId) -> Bytes {
    IndexKey::Global {
        kind: GlobalKey::LegacyVectorPhysicalReservation(physical_id),
    }
    .to_bytes()
}

fn decode_reservation(value: &[u8]) -> Result<LegacyVectorPhysicalReservation> {
    let IndexV2MetadataValue::LegacyVectorPhysicalReservation(reservation) =
        decode_metadata_value(value)?
    else {
        return Err(HelixDbError::IndexCatalogCorruption(
            "legacy vector reservation key contains another metadata value".to_string(),
        ));
    };
    Ok(reservation)
}

fn batch_limit_error(
    entity: impl std::fmt::Display,
    observed: usize,
    limit: usize,
) -> HelixDbError {
    HelixDbError::Config(format!(
        "legacy vector retirement item {entity} requires {observed} bytes, exceeding the {limit} byte migration batch limit"
    ))
}

/// Fences one bounded page of `LegacySource` reservations for exact active owners.
pub(super) async fn fence_sources_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
    catalog: &LegacyVectorRetirementCatalog,
) -> Result<MigrationBatch> {
    let MigrationJobState::Running {
        resume_after_key, ..
    } = &job.state
    else {
        return Ok(MigrationBatch::StageComplete);
    };
    let prefix = MigrationStage::FenceLegacyVectorSources.prefix(scope);
    let bounds = scan_bounds_for_prefix(&prefix, resume_after_key.as_ref());
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadBefore)?;
    let mut rows = transaction.scan(bounds).await?;
    let mut processed_rows = 0_u64;
    let mut admitted_bytes = 0_usize;
    let mut committed_cursor = None;
    while processed_rows < u64::try_from(tuning.batch_rows().get()).unwrap_or(u64::MAX) {
        let Some(row) = rows.next().await? else {
            break;
        };
        let GlobalKey::LegacyVectorPhysicalReservation(physical_id) =
            GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector fence scan returned another global key".to_string(),
            ));
        };
        let reservation = decode_reservation(&row.value)?;
        let mut output = None;
        let mut hydrated_bytes = 0_usize;
        match reservation {
            LegacyVectorPhysicalReservation::LegacySource => {
                let metadata_key = Key::Data {
                    scope,
                    kind: DataKeyKind::Vector(VectorKey::IndexMetadata(
                        VectorIndexMetadataKey::new(physical_id.get()),
                    )),
                }
                .to_bytes();
                let Some(metadata_value) = transaction.get(&metadata_key).await? else {
                    return Err(HelixDbError::MigrationRequired {
                        reason: format!(
                            "legacy vector source {} lost metadata before it was fenced",
                            physical_id.get()
                        ),
                    });
                };
                hydrated_bytes = metadata_key
                    .len()
                    .checked_add(metadata_value.len())
                    .ok_or_else(|| {
                        HelixDbError::InvariantViolation(
                            "legacy vector fence input bytes overflowed usize".to_string(),
                        )
                    })?;
                let metadata = decode_legacy_metadata(&metadata_value)?;
                metadata.validated_state()?;
                if vector::index_id_from_name(&metadata.config.index_name) != physical_id.get() {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "legacy vector source name hashes to another reservation".to_string(),
                    ));
                }
                let source = catalog.source_for_name(&metadata.config.index_name)?;
                let expected = VectorIndexConfig::from_v2_definition(
                    &source.owner.definition,
                    &metadata.config.index_name,
                );
                if !metadata.config.has_same_physical_contract(&expected) {
                    return Err(HelixDbError::IndexCatalogCorruption(
                        "legacy vector source metadata conflicts with its active definition"
                            .to_string(),
                    ));
                }
                let Some(retiring) =
                    reservation.begin_retirement(source.owner.index_id, source.owner.generation)
                else {
                    return Err(HelixDbError::InvariantViolation(
                        "legacy source refused its legal retirement transition".to_string(),
                    ));
                };
                output = Some(encode_metadata_value(
                    &IndexV2MetadataValue::LegacyVectorPhysicalReservation(retiring),
                ));
            }
            LegacyVectorPhysicalReservation::RetiringSource {
                index_id,
                generation,
            }
            | LegacyVectorPhysicalReservation::AdoptedActive {
                index_id,
                generation,
            } => {
                catalog.active_owner(index_id, generation)?;
            }
            LegacyVectorPhysicalReservation::AdoptionBuilding { .. } => {
                return Err(HelixDbError::MigrationRequired {
                    reason: "legacy vector cleanup encountered an unfinished adoption".to_string(),
                });
            }
        }
        let output_bytes = output
            .as_ref()
            .map_or(0, |value| row.key.len() + value.len());
        let row_bytes = row
            .key
            .len()
            .checked_add(row.value.len())
            .and_then(|bytes| bytes.checked_add(hydrated_bytes))
            .and_then(|bytes| bytes.checked_add(output_bytes))
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector fence batch bytes overflowed usize".to_string(),
                )
            })?;
        let Some(next_admitted_bytes) = admitted_bytes.checked_add(row_bytes) else {
            return Err(HelixDbError::InvariantViolation(
                "legacy vector fence admitted bytes overflowed usize".to_string(),
            ));
        };
        if next_admitted_bytes > tuning.batch_bytes().get() {
            if processed_rows == 0 {
                return Err(batch_limit_error(
                    physical_id.get(),
                    row_bytes,
                    tuning.batch_bytes().get(),
                ));
            }
            break;
        }
        if let Some(value) = output {
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteBefore)?;
            transaction.put(row.key.clone(), value)?;
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteAfter)?;
        }
        admitted_bytes = next_admitted_bytes;
        processed_rows = processed_rows.saturating_add(1);
        committed_cursor = MigrationResumeKey::new(row.key.to_vec());
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadAfter)?;
    let Some(resume_after_key) = committed_cursor else {
        return Ok(MigrationBatch::StageComplete);
    };
    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: processed_rows,
        source_bytes: u64::try_from(admitted_bytes).map_err(|_| {
            HelixDbError::InvariantViolation("legacy vector fence bytes do not fit u64".to_string())
        })?,
    })
}

/// Deletes one bounded page from a dedicated hot or layer-zero vector lane.
pub(super) async fn delete_dedicated_lane_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
    lane: VectorStorageLane,
) -> Result<MigrationBatch> {
    assert!(matches!(
        lane,
        VectorStorageLane::Hot | VectorStorageLane::Layer0
    ));
    let MigrationJobState::Running {
        resume_after_key, ..
    } = &job.state
    else {
        return Ok(MigrationBatch::StageComplete);
    };
    let stage = match lane {
        VectorStorageLane::Hot => MigrationStage::LegacyVectorHotRows,
        VectorStorageLane::Layer0 => MigrationStage::LegacyVectorLayer0Rows,
        VectorStorageLane::Core => unreachable!("core retirement has bounded point reads"),
    };
    let prefix = stage.prefix(scope);
    let bounds = scan_bounds_for_prefix(&prefix, resume_after_key.as_ref());
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadBefore)?;
    let mut rows = transaction.scan(bounds).await?;
    let mut processed_rows = 0_u64;
    let mut admitted_bytes = 0_usize;
    let mut committed_cursor = None;
    while processed_rows < u64::try_from(tuning.batch_rows().get()).unwrap_or(u64::MAX) {
        let Some(row) = rows.next().await? else {
            break;
        };
        let Some(logical) = scope.strip_key(&row.key) else {
            return Err(HelixDbError::InvariantViolation(
                "legacy vector lane scan escaped its data scope".to_string(),
            ));
        };
        let key = VectorKey::parse_from_slice(logical)?;
        if key.storage_lane() != lane {
            return Err(HelixDbError::IndexCatalogCorruption(
                "dedicated vector lane decoded a key from another lane".to_string(),
            ));
        }
        let physical_id = VectorPhysicalIndexId::new(key.index_id())?;
        let reservation_key = reservation_key(physical_id);
        let reservation_value = transaction.get(&reservation_key).await?;
        let reservation = reservation_value
            .as_deref()
            .map(decode_reservation)
            .transpose()?;
        let delete = match reservation {
            Some(LegacyVectorPhysicalReservation::RetiringSource { .. }) => true,
            Some(LegacyVectorPhysicalReservation::AdoptedActive { .. }) | None => false,
            Some(
                LegacyVectorPhysicalReservation::LegacySource
                | LegacyVectorPhysicalReservation::AdoptionBuilding { .. },
            ) => {
                return Err(HelixDbError::MigrationRequired {
                    reason: format!(
                        "legacy vector lane {} reached deletion without a retirement fence",
                        physical_id.get()
                    ),
                });
            }
        };
        let reservation_bytes = reservation_key
            .len()
            .checked_add(reservation_value.as_ref().map_or(0, Bytes::len))
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector reservation input bytes overflowed usize".to_string(),
                )
            })?;
        let row_bytes = row
            .key
            .len()
            .checked_add(row.value.len())
            .and_then(|bytes| bytes.checked_add(reservation_bytes))
            .and_then(|bytes| bytes.checked_add(if delete { row.key.len() } else { 0 }))
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector lane batch bytes overflowed usize".to_string(),
                )
            })?;
        let Some(next_admitted_bytes) = admitted_bytes.checked_add(row_bytes) else {
            return Err(HelixDbError::InvariantViolation(
                "legacy vector lane admitted bytes overflowed usize".to_string(),
            ));
        };
        if next_admitted_bytes > tuning.batch_bytes().get() {
            if processed_rows == 0 {
                return Err(batch_limit_error(
                    physical_id.get(),
                    row_bytes,
                    tuning.batch_bytes().get(),
                ));
            }
            break;
        }
        if delete {
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteBefore)?;
            transaction.delete(row.key.clone())?;
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteAfter)?;
        }
        admitted_bytes = next_admitted_bytes;
        processed_rows = processed_rows.saturating_add(1);
        committed_cursor = MigrationResumeKey::new(row.key.to_vec());
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadAfter)?;
    let Some(resume_after_key) = committed_cursor else {
        return Ok(MigrationBatch::StageComplete);
    };
    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: processed_rows,
        source_bytes: u64::try_from(admitted_bytes).map_err(|_| {
            HelixDbError::InvariantViolation("legacy vector lane bytes do not fit u64".to_string())
        })?,
    })
}

/// Deletes bounded core metadata and guard point reads for fenced reservations.
pub(super) async fn delete_core_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
) -> Result<MigrationBatch> {
    let MigrationJobState::Running {
        resume_after_key, ..
    } = &job.state
    else {
        return Ok(MigrationBatch::StageComplete);
    };
    let prefix = MigrationStage::LegacyVectorCoreRows.prefix(scope);
    let bounds = scan_bounds_for_prefix(&prefix, resume_after_key.as_ref());
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadBefore)?;
    let mut rows = transaction.scan(bounds).await?;
    let mut processed_rows = 0_u64;
    let mut admitted_bytes = 0_usize;
    let mut committed_cursor = None;
    while processed_rows < u64::try_from(tuning.batch_rows().get()).unwrap_or(u64::MAX) {
        let Some(row) = rows.next().await? else {
            break;
        };
        let GlobalKey::LegacyVectorPhysicalReservation(physical_id) =
            GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector core scan returned another global key".to_string(),
            ));
        };
        let reservation = decode_reservation(&row.value)?;
        let retiring = matches!(
            reservation,
            LegacyVectorPhysicalReservation::RetiringSource { .. }
        );
        if matches!(
            reservation,
            LegacyVectorPhysicalReservation::LegacySource
                | LegacyVectorPhysicalReservation::AdoptionBuilding { .. }
        ) {
            return Err(HelixDbError::MigrationRequired {
                reason: "legacy vector core cleanup reached an unfenced source".to_string(),
            });
        }
        let keys = [
            Key::Data {
                scope,
                kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    physical_id.get(),
                ))),
            }
            .to_bytes(),
            Key::Data {
                scope,
                kind: DataKeyKind::Vector(VectorKey::TxnGuard(VectorTxnGuardKey::new(
                    physical_id.get(),
                ))),
            }
            .to_bytes(),
        ];
        let values = if retiring {
            transaction.multi_get(&keys).await?
        } else {
            vec![None, None]
        };
        let row_bytes = row
            .key
            .len()
            .checked_add(row.value.len())
            .and_then(|bytes| {
                keys.iter()
                    .zip(&values)
                    .try_fold(bytes, |total, (key, value)| {
                        total
                            .checked_add(if retiring { key.len() } else { 0 })
                            .and_then(|total| {
                                total.checked_add(value.as_ref().map_or(0, Bytes::len))
                            })
                            .and_then(|total| {
                                total.checked_add(if value.is_some() { key.len() } else { 0 })
                            })
                    })
            })
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector core batch bytes overflowed usize".to_string(),
                )
            })?;
        let Some(next_admitted_bytes) = admitted_bytes.checked_add(row_bytes) else {
            return Err(HelixDbError::InvariantViolation(
                "legacy vector core admitted bytes overflowed usize".to_string(),
            ));
        };
        if next_admitted_bytes > tuning.batch_bytes().get() {
            if processed_rows == 0 {
                return Err(batch_limit_error(
                    physical_id.get(),
                    row_bytes,
                    tuning.batch_bytes().get(),
                ));
            }
            break;
        }
        if retiring {
            for (key, value) in keys.into_iter().zip(values) {
                if value.is_some() {
                    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
                    super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteBefore)?;
                    transaction.delete(key)?;
                    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
                    super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteAfter)?;
                }
            }
        }
        admitted_bytes = next_admitted_bytes;
        processed_rows = processed_rows.saturating_add(1);
        committed_cursor = MigrationResumeKey::new(row.key.to_vec());
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadAfter)?;
    let Some(resume_after_key) = committed_cursor else {
        return Ok(MigrationBatch::StageComplete);
    };
    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: processed_rows,
        source_bytes: u64::try_from(admitted_bytes).map_err(|_| {
            HelixDbError::InvariantViolation("legacy vector core bytes do not fit u64".to_string())
        })?,
    })
}

/// Deletes bounded legacy vector definitions after all source lanes are empty.
pub(super) async fn delete_definitions_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
    catalog: &LegacyVectorRetirementCatalog,
) -> Result<MigrationBatch> {
    let MigrationJobState::Running {
        resume_after_key, ..
    } = &job.state
    else {
        return Ok(MigrationBatch::StageComplete);
    };
    let prefix = MigrationStage::LegacyVectorDefinitions.prefix(scope);
    let bounds = scan_bounds_for_prefix(&prefix, resume_after_key.as_ref());
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadBefore)?;
    let mut rows = transaction.scan(bounds).await?;
    let mut processed_rows = 0_u64;
    let mut admitted_bytes = 0_usize;
    let mut committed_cursor = None;
    while processed_rows < u64::try_from(tuning.batch_rows().get()).unwrap_or(u64::MAX) {
        let Some(row) = rows.next().await? else {
            break;
        };
        let decoded = LegacyDefinitionRow::decode(scope, row.key.clone(), &row.value)?;
        let LegacyDynamicIndexCatalogEntry::Definition(legacy) = decoded.entry else {
            return Err(HelixDbError::MigrationRequired {
                reason: "legacy tombstone survived into vector definition cleanup".to_string(),
            });
        };
        if legacy.key() != decoded.identity {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector definition cleanup found an identity mismatch".to_string(),
            ));
        }
        let LegacyDynamicIndexDefinition::Vector(_) = &legacy else {
            return Err(HelixDbError::MigrationRequired {
                reason: "non-vector definition survived into vector definition cleanup".to_string(),
            });
        };
        let ValidatedDynamicIndexDefinition::Vector(definition) = legacy.into_validated()? else {
            unreachable!("legacy vector definition validates as vector")
        };
        let mut sources = catalog
            .sources
            .iter()
            .filter(|source| source.owner.definition == definition);
        let Some(source) = sources.next() else {
            return Err(HelixDbError::MigrationRequired {
                reason: "legacy vector definition has no exact active cleanup owner".to_string(),
            });
        };
        if sources.next().is_some() {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector definition has multiple cleanup owners".to_string(),
            ));
        }
        catalog.active_owner(source.owner.index_id, source.owner.generation)?;
        let row_bytes = row
            .key
            .len()
            .checked_add(row.value.len())
            .and_then(|bytes| bytes.checked_add(row.key.len()))
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector definition batch bytes overflowed usize".to_string(),
                )
            })?;
        let Some(next_admitted_bytes) = admitted_bytes.checked_add(row_bytes) else {
            return Err(HelixDbError::InvariantViolation(
                "legacy vector definition admitted bytes overflowed usize".to_string(),
            ));
        };
        if next_admitted_bytes > tuning.batch_bytes().get() {
            if processed_rows == 0 {
                return Err(batch_limit_error(
                    source.owner.index_id.get(),
                    row_bytes,
                    tuning.batch_bytes().get(),
                ));
            }
            break;
        }
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteBefore)?;
        transaction.delete(row.key.clone())?;
        #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
        super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteAfter)?;
        admitted_bytes = next_admitted_bytes;
        processed_rows = processed_rows.saturating_add(1);
        committed_cursor = MigrationResumeKey::new(row.key.to_vec());
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadAfter)?;
    let Some(resume_after_key) = committed_cursor else {
        return Ok(MigrationBatch::StageComplete);
    };
    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: processed_rows,
        source_bytes: u64::try_from(admitted_bytes).map_err(|_| {
            HelixDbError::InvariantViolation(
                "legacy vector definition bytes do not fit u64".to_string(),
            )
        })?,
    })
}

/// Releases bounded fenced reservations only after every physical lane probes empty.
pub(super) async fn release_reservations_batch(
    transaction: &DbTransaction,
    scope: DataScope,
    tuning: config::MigrationTuning,
    job: &MigrationJob,
    catalog: &LegacyVectorRetirementCatalog,
) -> Result<MigrationBatch> {
    let MigrationJobState::Running {
        resume_after_key, ..
    } = &job.state
    else {
        return Ok(MigrationBatch::StageComplete);
    };
    let prefix = MigrationStage::ReleaseLegacyVectorReservations.prefix(scope);
    let bounds = scan_bounds_for_prefix(&prefix, resume_after_key.as_ref());
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadBefore)?;
    let mut rows = transaction.scan(bounds).await?;
    let mut processed_rows = 0_u64;
    let mut admitted_bytes = 0_usize;
    let mut committed_cursor = None;
    while processed_rows < u64::try_from(tuning.batch_rows().get()).unwrap_or(u64::MAX) {
        let Some(row) = rows.next().await? else {
            break;
        };
        let GlobalKey::LegacyVectorPhysicalReservation(physical_id) =
            GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "legacy vector reservation release returned another global key".to_string(),
            ));
        };
        let reservation = decode_reservation(&row.value)?;
        let delete = match reservation {
            LegacyVectorPhysicalReservation::RetiringSource {
                index_id,
                generation,
            } => {
                catalog.active_owner(index_id, generation)?;
                true
            }
            LegacyVectorPhysicalReservation::AdoptedActive { .. } => false,
            LegacyVectorPhysicalReservation::LegacySource
            | LegacyVectorPhysicalReservation::AdoptionBuilding { .. } => {
                return Err(HelixDbError::MigrationRequired {
                    reason: "legacy vector reservation release found an unfenced source"
                        .to_string(),
                });
            }
        };
        let mut probe_bytes = 0_usize;
        if delete {
            let metadata_key = Key::Data {
                scope,
                kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    physical_id.get(),
                ))),
            }
            .to_bytes();
            let guard_key = Key::Data {
                scope,
                kind: DataKeyKind::Vector(VectorKey::TxnGuard(VectorTxnGuardKey::new(
                    physical_id.get(),
                ))),
            }
            .to_bytes();
            for key in [&metadata_key, &guard_key] {
                let value = transaction.get(key).await?;
                probe_bytes = probe_bytes
                    .checked_add(key.len())
                    .and_then(|bytes| bytes.checked_add(value.as_ref().map_or(0, Bytes::len)))
                    .ok_or_else(|| {
                        HelixDbError::InvariantViolation(
                            "legacy vector release probe bytes overflowed usize".to_string(),
                        )
                    })?;
                if value.is_some() {
                    return Err(HelixDbError::MigrationRequired {
                        reason: format!(
                            "retiring legacy vector namespace {} retains a core row",
                            physical_id.get()
                        ),
                    });
                }
            }
            for lane in [VectorStorageLane::Hot, VectorStorageLane::Layer0] {
                let lane_prefix =
                    Key::data_prefix(scope, lane.prefix_key(physical_id.get()).to_bytes());
                probe_bytes = probe_bytes.checked_add(lane_prefix.len()).ok_or_else(|| {
                    HelixDbError::InvariantViolation(
                        "legacy vector release lane probe bytes overflowed usize".to_string(),
                    )
                })?;
                let mut residue = transaction.scan_prefix(lane_prefix, ..).await?;
                if residue.next().await?.is_some() {
                    return Err(HelixDbError::MigrationRequired {
                        reason: format!(
                            "retiring legacy vector namespace {} retains a {:?} row",
                            physical_id.get(),
                            lane
                        ),
                    });
                }
            }
        }
        let row_bytes = row
            .key
            .len()
            .checked_add(row.value.len())
            .and_then(|bytes| bytes.checked_add(probe_bytes))
            .and_then(|bytes| bytes.checked_add(if delete { row.key.len() } else { 0 }))
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "legacy vector reservation release bytes overflowed usize".to_string(),
                )
            })?;
        let Some(next_admitted_bytes) = admitted_bytes.checked_add(row_bytes) else {
            return Err(HelixDbError::InvariantViolation(
                "legacy vector reservation release admitted bytes overflowed usize".to_string(),
            ));
        };
        if next_admitted_bytes > tuning.batch_bytes().get() {
            if processed_rows == 0 {
                return Err(batch_limit_error(
                    physical_id.get(),
                    row_bytes,
                    tuning.batch_bytes().get(),
                ));
            }
            break;
        }
        if delete {
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteBefore)?;
            transaction.delete(row.key.clone())?;
            #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
            super::trip_migration_failpoint(super::MigrationFailpoint::BatchWriteAfter)?;
        }
        admitted_bytes = next_admitted_bytes;
        processed_rows = processed_rows.saturating_add(1);
        committed_cursor = MigrationResumeKey::new(row.key.to_vec());
    }
    #[cfg(any(feature = "migration-parity", feature = "production-coverage"))]
    super::trip_migration_failpoint(super::MigrationFailpoint::BatchReadAfter)?;
    let Some(resume_after_key) = committed_cursor else {
        return Ok(MigrationBatch::StageComplete);
    };
    Ok(MigrationBatch::Advanced {
        resume_after_key,
        rows: processed_rows,
        source_bytes: u64::try_from(admitted_bytes).map_err(|_| {
            HelixDbError::InvariantViolation(
                "legacy vector reservation bytes do not fit u64".to_string(),
            )
        })?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::encoding::v1::keys::vectors::{VectorItemKey, VectorSimHashKey};
    use crate::encoding::v1::values::vectors::simhash::encode_simhash;
    use crate::index_lifecycle::IndexElementKind;
    use crate::search::vector::VectorDistanceMetric;

    async fn database(label: &str) -> Db {
        Db::builder(
            format!("legacy-vector-retirement-{label}-{}", uuid::Uuid::new_v4()),
            Arc::new(InMemory::new()),
        )
        .build()
        .await
        .expect("retirement test database opens")
    }

    fn running_job(stage: MigrationStage) -> MigrationJob {
        let mut job = MigrationJob::new(
            super::super::MigrationId::LegacyVectorPhysicalCleanup,
            super::super::MigrationMode::BlockingStartup,
        );
        job.state = MigrationJobState::Running {
            stage,
            resume_after_key: None,
            processed_rows: 0,
        };
        job
    }

    fn vector_definition() -> ValidatedVectorIndexDefinition {
        ValidatedVectorIndexDefinition::try_new(
            IndexElementKind::Node,
            "Document",
            "embedding",
            None::<String>,
            3,
            VectorDistanceMetric::Cosine,
            16,
            32,
            64,
            0.5,
            4,
            0.75,
            false,
            0.25,
        )
        .expect("retirement test vector definition validates")
    }

    fn owner_catalog(
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> LegacyVectorRetirementCatalog {
        LegacyVectorRetirementCatalog {
            active: BTreeMap::from([((index_id, generation), vector_definition())]),
            sources: Vec::new(),
        }
    }

    fn encoded_reservation(reservation: LegacyVectorPhysicalReservation) -> Bytes {
        encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
            reservation,
        ))
    }

    fn core_key(scope: DataScope, physical_id: VectorPhysicalIndexId, metadata: bool) -> Bytes {
        let key = if metadata {
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(physical_id.get()))
        } else {
            VectorKey::TxnGuard(VectorTxnGuardKey::new(physical_id.get()))
        };
        Key::Data {
            scope,
            kind: DataKeyKind::Vector(key),
        }
        .to_bytes()
    }

    fn lane_key(
        scope: DataScope,
        physical_id: VectorPhysicalIndexId,
        lane: VectorStorageLane,
        entity_id: u64,
    ) -> Bytes {
        let key = match lane {
            VectorStorageLane::Hot => {
                VectorKey::SimHash(VectorSimHashKey::new(physical_id.get(), entity_id))
            }
            VectorStorageLane::Layer0 => {
                VectorKey::Vector(VectorItemKey::new(physical_id.get(), entity_id, entity_id))
            }
            VectorStorageLane::Core => panic!("core rows use core_key"),
        };
        Key::Data {
            scope,
            kind: DataKeyKind::Vector(key),
        }
        .to_bytes()
    }

    #[tokio::test]
    async fn dedicated_cleanup_batch_obeys_exact_row_and_combined_byte_limits() {
        let db = database("bounds").await;
        let physical_id = VectorPhysicalIndexId::new(91).unwrap();
        let reservation = LegacyVectorPhysicalReservation::RetiringSource {
            index_id: IndexId::new(7).unwrap(),
            generation: IndexGenerationId::new(3).unwrap(),
        };
        db.put(
            reservation_key(physical_id),
            encode_metadata_value(&IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                reservation,
            )),
        )
        .await
        .unwrap();
        let first_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::Vector(VectorKey::SimHash(VectorSimHashKey::new(
                physical_id.get(),
                1,
            ))),
        }
        .to_bytes();
        let second_key = Key::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::Vector(VectorKey::SimHash(VectorSimHashKey::new(
                physical_id.get(),
                2,
            ))),
        }
        .to_bytes();
        db.put(&first_key, encode_simhash(1)).await.unwrap();
        db.put(&second_key, encode_simhash(2)).await.unwrap();
        let job = running_job(MigrationStage::LegacyVectorHotRows);
        let row_bounded = config::MigrationTuning::default()
            .with_batch_rows(config::MigrationBatchRows::new(1).expect("one row is positive"));
        let measurement = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let MigrationBatch::Advanced {
            rows: 1,
            source_bytes,
            ..
        } = delete_dedicated_lane_batch(
            &measurement,
            DataScope::LegacyUnscoped,
            row_bounded,
            &job,
            VectorStorageLane::Hot,
        )
        .await
        .unwrap()
        else {
            panic!("one cleanup row returns its exact measurement")
        };
        measurement.rollback();

        let below_limit = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let error = delete_dedicated_lane_batch(
            &below_limit,
            DataScope::LegacyUnscoped,
            row_bounded.with_batch_bytes(
                config::MigrationBatchBytes::new(
                    usize::try_from(source_bytes - 1).expect("test bytes fit usize"),
                )
                .expect("measured bytes exceed one"),
            ),
            &job,
            VectorStorageLane::Hot,
        )
        .await
        .expect_err("one byte below the complete delete must fail closed");
        below_limit.rollback();
        assert!(error.to_string().contains("exceeding the"));

        let exact_limit = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let batch = delete_dedicated_lane_batch(
            &exact_limit,
            DataScope::LegacyUnscoped,
            row_bounded.with_batch_bytes(
                config::MigrationBatchBytes::new(
                    usize::try_from(source_bytes).expect("test bytes fit usize"),
                )
                .expect("measured bytes are positive"),
            ),
            &job,
            VectorStorageLane::Hot,
        )
        .await
        .unwrap();
        assert!(matches!(
            batch,
            MigrationBatch::Advanced {
                rows: 1,
                source_bytes: exact,
                ..
            } if exact == source_bytes
        ));
        exact_limit.commit().await.unwrap();
        assert!(db.get(&first_key).await.unwrap().is_none());
        assert!(db.get(&second_key).await.unwrap().is_some());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn inactive_jobs_and_empty_scans_are_exact_stage_completions() {
        let db = database("inactive").await;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let mut job = running_job(MigrationStage::LegacyVectorHotRows);
        job.complete();
        let tuning = config::MigrationTuning::default();
        let catalog = LegacyVectorRetirementCatalog {
            active: BTreeMap::new(),
            sources: Vec::new(),
        };

        assert_eq!(
            fence_sources_batch(
                &transaction,
                DataScope::LegacyUnscoped,
                tuning,
                &job,
                &catalog,
            )
            .await
            .unwrap(),
            MigrationBatch::StageComplete
        );
        for lane in [VectorStorageLane::Hot, VectorStorageLane::Layer0] {
            assert_eq!(
                delete_dedicated_lane_batch(
                    &transaction,
                    DataScope::LegacyUnscoped,
                    tuning,
                    &job,
                    lane,
                )
                .await
                .unwrap(),
                MigrationBatch::StageComplete
            );
        }
        assert_eq!(
            delete_core_batch(&transaction, DataScope::LegacyUnscoped, tuning, &job,)
                .await
                .unwrap(),
            MigrationBatch::StageComplete
        );
        assert_eq!(
            delete_definitions_batch(
                &transaction,
                DataScope::LegacyUnscoped,
                tuning,
                &job,
                &catalog,
            )
            .await
            .unwrap(),
            MigrationBatch::StageComplete
        );
        assert_eq!(
            release_reservations_batch(
                &transaction,
                DataScope::LegacyUnscoped,
                tuning,
                &job,
                &catalog,
            )
            .await
            .unwrap(),
            MigrationBatch::StageComplete
        );

        job = running_job(MigrationStage::LegacyVectorHotRows);
        assert_eq!(
            delete_dedicated_lane_batch(
                &transaction,
                DataScope::LegacyUnscoped,
                tuning,
                &job,
                VectorStorageLane::Hot,
            )
            .await
            .unwrap(),
            MigrationBatch::StageComplete
        );
        transaction.rollback();
        db.close().await.unwrap();
    }

    #[test]
    fn catalog_owner_and_namespace_matching_fail_closed() {
        let index_id = IndexId::new(7).unwrap();
        let generation = IndexGenerationId::new(3).unwrap();
        let definition = vector_definition();
        let exact = LegacyVectorRetirementSource {
            owner: ActiveVectorOwner {
                index_id,
                generation,
                definition: definition.clone(),
            },
            exact_name: Some("exact".to_string()),
            tenant_prefix: None,
        };
        let tenant = LegacyVectorRetirementSource {
            owner: ActiveVectorOwner {
                index_id,
                generation,
                definition: definition.clone(),
            },
            exact_name: None,
            tenant_prefix: Some("tenant:".to_string()),
        };
        let catalog = LegacyVectorRetirementCatalog {
            active: BTreeMap::from([((index_id, generation), definition.clone())]),
            sources: vec![exact, tenant],
        };

        assert_eq!(
            catalog.active_owner(index_id, generation).unwrap(),
            &definition
        );
        assert_eq!(
            catalog.source_for_name("exact").unwrap().owner.index_id,
            index_id
        );
        assert_eq!(
            catalog
                .source_for_name("tenant:one")
                .unwrap()
                .owner
                .generation,
            generation
        );
        assert!(matches!(
            catalog.active_owner(index_id, IndexGenerationId::new(4).unwrap()),
            Err(HelixDbError::MigrationRequired { .. })
        ));
        assert!(matches!(
            catalog.source_for_name("orphan"),
            Err(HelixDbError::MigrationRequired { .. })
        ));

        let ambiguous = LegacyVectorRetirementCatalog {
            active: catalog.active,
            sources: vec![
                LegacyVectorRetirementSource {
                    owner: ActiveVectorOwner {
                        index_id,
                        generation,
                        definition: definition.clone(),
                    },
                    exact_name: Some("tenant:one".to_string()),
                    tenant_prefix: None,
                },
                LegacyVectorRetirementSource {
                    owner: ActiveVectorOwner {
                        index_id,
                        generation,
                        definition,
                    },
                    exact_name: None,
                    tenant_prefix: Some("tenant:".to_string()),
                },
            ],
        };
        assert!(matches!(
            ambiguous.source_for_name("tenant:one"),
            Err(HelixDbError::IndexCatalogCorruption(_))
        ));
        assert_eq!(
            reservation_key(VectorPhysicalIndexId::new(9).unwrap()),
            IndexKey::Global {
                kind: GlobalKey::LegacyVectorPhysicalReservation(
                    VectorPhysicalIndexId::new(9).unwrap()
                ),
            }
            .to_bytes()
        );
        assert!(decode_reservation(&encoded_reservation(
            LegacyVectorPhysicalReservation::LegacySource
        ))
        .is_ok());
        assert!(decode_reservation(&encode_metadata_value(
            &IndexV2MetadataValue::LogicalIndexIdWatermark(
                crate::index_lifecycle::LogicalIndexIdWatermark {
                    next_id: IndexId::new(1).unwrap(),
                },
            ),
        ))
        .is_err());
    }

    #[tokio::test]
    async fn dedicated_lanes_preserve_unowned_rows_and_reject_unfenced_sources() {
        let scope = DataScope::LegacyUnscoped;
        let db = database("dedicated-states").await;
        let index_id = IndexId::new(7).unwrap();
        let generation = IndexGenerationId::new(3).unwrap();
        let states = [
            (31, None, false),
            (
                32,
                Some(LegacyVectorPhysicalReservation::AdoptedActive {
                    index_id,
                    generation,
                }),
                false,
            ),
            (
                33,
                Some(LegacyVectorPhysicalReservation::RetiringSource {
                    index_id,
                    generation,
                }),
                true,
            ),
        ];
        for (raw_id, reservation, deleted) in states {
            let physical_id = VectorPhysicalIndexId::new(raw_id).unwrap();
            let key = lane_key(scope, physical_id, VectorStorageLane::Layer0, raw_id);
            db.put(&key, Bytes::from_static(b"row")).await.unwrap();
            if let Some(reservation) = reservation {
                db.put(
                    reservation_key(physical_id),
                    encoded_reservation(reservation),
                )
                .await
                .unwrap();
            }
            let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
            let batch = delete_dedicated_lane_batch(
                &transaction,
                scope,
                config::MigrationTuning::default(),
                &running_job(MigrationStage::LegacyVectorLayer0Rows),
                VectorStorageLane::Layer0,
            )
            .await
            .unwrap();
            assert!(matches!(batch, MigrationBatch::Advanced { rows: 1, .. }));
            transaction.commit().await.unwrap();
            assert_eq!(db.get(&key).await.unwrap().is_none(), deleted);
            if !deleted {
                db.delete(&key).await.unwrap();
            }
        }

        for (raw_id, reservation) in [
            (41, LegacyVectorPhysicalReservation::LegacySource),
            (
                42,
                LegacyVectorPhysicalReservation::AdoptionBuilding {
                    index_id,
                    generation,
                    operation_id: crate::index_lifecycle::IndexOperationId::new(
                        uuid::Uuid::from_u128(5),
                    )
                    .unwrap(),
                },
            ),
        ] {
            let physical_id = VectorPhysicalIndexId::new(raw_id).unwrap();
            let key = lane_key(scope, physical_id, VectorStorageLane::Hot, raw_id);
            db.put(&key, Bytes::from_static(b"row")).await.unwrap();
            db.put(
                reservation_key(physical_id),
                encoded_reservation(reservation),
            )
            .await
            .unwrap();
            let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
            assert!(matches!(
                delete_dedicated_lane_batch(
                    &transaction,
                    scope,
                    config::MigrationTuning::default(),
                    &running_job(MigrationStage::LegacyVectorHotRows),
                    VectorStorageLane::Hot,
                )
                .await,
                Err(HelixDbError::MigrationRequired { .. })
            ));
            transaction.rollback();
            db.delete(&key).await.unwrap();
            db.delete(reservation_key(physical_id)).await.unwrap();
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn core_cleanup_deletes_only_fenced_owner_rows_and_honors_byte_limits() {
        let scope = DataScope::LegacyUnscoped;
        let db = database("core").await;
        let index_id = IndexId::new(7).unwrap();
        let generation = IndexGenerationId::new(3).unwrap();
        let retiring_id = VectorPhysicalIndexId::new(51).unwrap();
        let adopted_id = VectorPhysicalIndexId::new(52).unwrap();
        for (physical_id, reservation) in [
            (
                retiring_id,
                LegacyVectorPhysicalReservation::RetiringSource {
                    index_id,
                    generation,
                },
            ),
            (
                adopted_id,
                LegacyVectorPhysicalReservation::AdoptedActive {
                    index_id,
                    generation,
                },
            ),
        ] {
            db.put(
                reservation_key(physical_id),
                encoded_reservation(reservation),
            )
            .await
            .unwrap();
            for metadata in [true, false] {
                db.put(
                    core_key(scope, physical_id, metadata),
                    Bytes::from_static(b"core"),
                )
                .await
                .unwrap();
            }
        }
        let one_row = config::MigrationTuning::default()
            .with_batch_rows(config::MigrationBatchRows::new(1).expect("one row is positive"));
        let measure = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let MigrationBatch::Advanced { source_bytes, .. } = delete_core_batch(
            &measure,
            scope,
            one_row,
            &running_job(MigrationStage::LegacyVectorCoreRows),
        )
        .await
        .unwrap() else {
            panic!("one reservation is measured")
        };
        measure.rollback();

        let below = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            delete_core_batch(
                &below,
                scope,
                one_row.with_batch_bytes(
                    config::MigrationBatchBytes::new(source_bytes as usize - 1).unwrap()
                ),
                &running_job(MigrationStage::LegacyVectorCoreRows),
            )
            .await,
            Err(HelixDbError::Config(_))
        ));
        below.rollback();

        let exact = db.begin(IsolationLevel::Snapshot).await.unwrap();
        delete_core_batch(
            &exact,
            scope,
            one_row
                .with_batch_bytes(config::MigrationBatchBytes::new(source_bytes as usize).unwrap()),
            &running_job(MigrationStage::LegacyVectorCoreRows),
        )
        .await
        .unwrap();
        exact.commit().await.unwrap();
        for metadata in [true, false] {
            assert!(db
                .get(core_key(scope, retiring_id, metadata))
                .await
                .unwrap()
                .is_none());
            assert!(db
                .get(core_key(scope, adopted_id, metadata))
                .await
                .unwrap()
                .is_some());
        }

        db.put(
            reservation_key(retiring_id),
            encoded_reservation(LegacyVectorPhysicalReservation::LegacySource),
        )
        .await
        .unwrap();
        let unfenced = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            delete_core_batch(
                &unfenced,
                scope,
                config::MigrationTuning::default(),
                &running_job(MigrationStage::LegacyVectorCoreRows),
            )
            .await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        unfenced.rollback();
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn reservation_release_requires_exact_owner_and_empty_physical_lanes() {
        let scope = DataScope::LegacyUnscoped;
        let db = database("release").await;
        let index_id = IndexId::new(7).unwrap();
        let generation = IndexGenerationId::new(3).unwrap();
        let catalog = owner_catalog(index_id, generation);
        let physical_id = VectorPhysicalIndexId::new(61).unwrap();
        let reservation = LegacyVectorPhysicalReservation::RetiringSource {
            index_id,
            generation,
        };
        db.put(
            reservation_key(physical_id),
            encoded_reservation(reservation),
        )
        .await
        .unwrap();

        for (label, residue) in [
            ("metadata", core_key(scope, physical_id, true)),
            (
                "hot",
                lane_key(scope, physical_id, VectorStorageLane::Hot, 1),
            ),
            (
                "layer-zero",
                lane_key(scope, physical_id, VectorStorageLane::Layer0, 1),
            ),
        ] {
            db.put(&residue, Bytes::copy_from_slice(label.as_bytes()))
                .await
                .unwrap();
            let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
            assert!(matches!(
                release_reservations_batch(
                    &transaction,
                    scope,
                    config::MigrationTuning::default(),
                    &running_job(MigrationStage::ReleaseLegacyVectorReservations),
                    &catalog,
                )
                .await,
                Err(HelixDbError::MigrationRequired { .. })
            ));
            transaction.rollback();
            db.delete(residue).await.unwrap();
        }

        let one_row = config::MigrationTuning::default()
            .with_batch_rows(config::MigrationBatchRows::new(1).expect("one row is positive"));
        let measurement = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let MigrationBatch::Advanced { source_bytes, .. } = release_reservations_batch(
            &measurement,
            scope,
            one_row,
            &running_job(MigrationStage::ReleaseLegacyVectorReservations),
            &catalog,
        )
        .await
        .unwrap() else {
            panic!("empty fenced reservation is measured")
        };
        measurement.rollback();

        let below = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            release_reservations_batch(
                &below,
                scope,
                one_row.with_batch_bytes(
                    config::MigrationBatchBytes::new(source_bytes as usize - 1).unwrap()
                ),
                &running_job(MigrationStage::ReleaseLegacyVectorReservations),
                &catalog,
            )
            .await,
            Err(HelixDbError::Config(_))
        ));
        below.rollback();

        let exact = db.begin(IsolationLevel::Snapshot).await.unwrap();
        release_reservations_batch(
            &exact,
            scope,
            one_row
                .with_batch_bytes(config::MigrationBatchBytes::new(source_bytes as usize).unwrap()),
            &running_job(MigrationStage::ReleaseLegacyVectorReservations),
            &catalog,
        )
        .await
        .unwrap();
        exact.commit().await.unwrap();
        assert!(db
            .get(reservation_key(physical_id))
            .await
            .unwrap()
            .is_none());

        let adopted_id = VectorPhysicalIndexId::new(62).unwrap();
        db.put(
            reservation_key(adopted_id),
            encoded_reservation(LegacyVectorPhysicalReservation::AdoptedActive {
                index_id,
                generation,
            }),
        )
        .await
        .unwrap();
        let adopted = db.begin(IsolationLevel::Snapshot).await.unwrap();
        assert!(matches!(
            release_reservations_batch(
                &adopted,
                scope,
                config::MigrationTuning::default(),
                &running_job(MigrationStage::ReleaseLegacyVectorReservations),
                &catalog,
            )
            .await
            .unwrap(),
            MigrationBatch::Advanced { rows: 1, .. }
        ));
        adopted.commit().await.unwrap();
        assert!(db.get(reservation_key(adopted_id)).await.unwrap().is_some());

        for (raw_id, reservation) in [
            (63, LegacyVectorPhysicalReservation::LegacySource),
            (
                64,
                LegacyVectorPhysicalReservation::RetiringSource {
                    index_id,
                    generation: IndexGenerationId::new(4).unwrap(),
                },
            ),
        ] {
            let physical_id = VectorPhysicalIndexId::new(raw_id).unwrap();
            db.put(
                reservation_key(physical_id),
                encoded_reservation(reservation),
            )
            .await
            .unwrap();
            let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
            assert!(matches!(
                release_reservations_batch(
                    &transaction,
                    scope,
                    config::MigrationTuning::default(),
                    &running_job(MigrationStage::ReleaseLegacyVectorReservations),
                    &catalog,
                )
                .await,
                Err(HelixDbError::MigrationRequired { .. })
            ));
            transaction.rollback();
            db.delete(reservation_key(physical_id)).await.unwrap();
        }
        db.close().await.unwrap();
    }
}
