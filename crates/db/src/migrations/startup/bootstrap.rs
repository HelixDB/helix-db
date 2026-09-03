//! Blocking managed-index and tenant storage bootstrap.

use bytes::Bytes;
use slatedb::{Db, DbTransaction, IsolationLevel};

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey, ScopedKey, GLOBAL_SENTINEL};
use crate::encoding::v2::values::{decode_metadata_value, encode_metadata_value};
use crate::error::{HelixDbError, Result, WriterMigrationRequirement};
use crate::index_lifecycle::{
    IndexId, IndexStorageVersion, IndexV2MetadataValue, LogicalIndexIdWatermark,
    VectorPhysicalIdWatermark, VectorPhysicalIndexId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterBootstrapPlan {
    Initialize,
    MigrateToCurrent,
    CleanupCurrent,
    Ready,
}

/// Migrates tenant-owned keys and converges managed-index storage metadata.
pub(crate) async fn bootstrap_writer(db: &Db) -> Result<()> {
    let plan = preflight_writer_bootstrap(db).await?;
    super::super::tenant::envelope::migrate_all_tenant_keys(db).await?;

    match plan {
        WriterBootstrapPlan::Initialize => initialize_writer_bootstrap(db).await,
        WriterBootstrapPlan::MigrateToCurrent => migrate_to_current(db).await,
        WriterBootstrapPlan::CleanupCurrent => {
            super::super::indexes::equality_bitmap::cleanup_v3_nonunique_equality_rows(db).await
        }
        WriterBootstrapPlan::Ready => Ok(()),
    }
}

async fn migrate_to_current(db: &Db) -> Result<()> {
    let version = read_writer_storage_version(db).await?;
    if version < IndexStorageVersion::EQUALITY_BITMAP {
        return super::super::indexes::equality_bitmap::migrate_v3_to_v4(db).await;
    }
    if version < IndexStorageVersion::CURRENT {
        return super::super::indexes::labels::migrate_hash_labels_to_canonical(db).await;
    }
    Ok(())
}

async fn read_writer_storage_version(db: &Db) -> Result<IndexStorageVersion> {
    let marker = db
        .get(&global_key(GlobalKey::StorageVersion))
        .await?
        .ok_or_else(|| HelixDbError::MigrationRequired {
            reason: "V2 storage marker disappeared after writer preflight".to_string(),
        })?;
    let IndexV2MetadataValue::StorageVersion(version) =
        metadata_or_migration_required(&marker, "storage marker")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 storage marker contains the wrong value kind".to_string(),
        });
    };
    Ok(version)
}

/// Initializes a pristine managed database without entering a migration path.
pub(crate) async fn bootstrap_managed_writer(db: &Db) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let mut rows = transaction.scan(..).await?;
    if rows.next().await?.is_some() {
        transaction.rollback();
        return Err(HelixDbError::WriterMigrationRequired {
            requirement: WriterMigrationRequirement::IncompleteStorageSchema,
        });
    }
    stage_writer_bootstrap_initialization(&transaction)?;
    super::super::stage_current_storage_schema_ready(&transaction)?;
    transaction.commit().await?;
    Ok(())
}

/// Validates current managed storage using point reads only.
pub(crate) async fn require_current_managed_writer(db: &Db) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let marker_key = global_key(GlobalKey::StorageVersion);
    let logical_key = global_key(GlobalKey::LogicalIndexIdWatermark);
    let vector_key = global_key(GlobalKey::VectorPhysicalIdWatermark);
    let marker = transaction.get(&marker_key).await?;
    let logical = transaction.get(&logical_key).await?;
    let vector = transaction.get(&vector_key).await?;
    let cleanup_ready = super::super::index_storage_v4_cleanup_ready(&transaction).await?;
    let tenant_envelope_ready = super::super::tenant_key_envelope_ready(&transaction).await?;
    crate::membership_delta::transaction_write_mode(&transaction).await?;

    let Some(marker) = marker else {
        transaction.rollback();
        if logical.is_some() || vector.is_some() || cleanup_ready || tenant_envelope_ready {
            return Err(HelixDbError::MigrationRequired {
                reason: "managed storage has a partial bootstrap tuple".to_string(),
            });
        }
        return Err(HelixDbError::WriterMigrationRequired {
            requirement: WriterMigrationRequirement::IncompleteStorageSchema,
        });
    };
    let IndexV2MetadataValue::StorageVersion(version) =
        metadata_or_migration_required(&marker, "storage marker")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 storage marker contains the wrong value kind".to_string(),
        });
    };
    validate_writer_bootstrap_values(&marker, logical.as_deref(), vector.as_deref())?;
    if version < IndexStorageVersion::EQUALITY_BITMAP && cleanup_ready {
        transaction.rollback();
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "index storage V4 cleanup is marked complete beside storage version {}",
                version.get()
            ),
        });
    }
    if version < IndexStorageVersion::CURRENT {
        transaction.rollback();
        return Err(HelixDbError::WriterMigrationRequired {
            requirement: WriterMigrationRequirement::StorageVersion {
                found: version.get(),
                target: IndexStorageVersion::CURRENT.get(),
            },
        });
    }
    let progress =
        super::super::storage_schema_progress(&transaction, DataScope::LegacyUnscoped).await?;
    transaction.rollback();
    if cleanup_ready
        && tenant_envelope_ready
        && progress == super::super::StorageSchemaProgress::Complete
    {
        Ok(())
    } else {
        Err(HelixDbError::WriterMigrationRequired {
            requirement: WriterMigrationRequirement::IncompleteStorageSchema,
        })
    }
}

/// Validates all durable bootstrap state before tenant migration may write.
async fn preflight_writer_bootstrap(db: &Db) -> Result<WriterBootstrapPlan> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let marker_key = global_key(GlobalKey::StorageVersion);
    let logical_key = global_key(GlobalKey::LogicalIndexIdWatermark);
    let vector_key = global_key(GlobalKey::VectorPhysicalIdWatermark);
    let marker = transaction.get(&marker_key).await?;
    let logical = transaction.get(&logical_key).await?;
    let vector = transaction.get(&vector_key).await?;
    let cleanup_ready = super::super::index_storage_v4_cleanup_ready(&transaction).await?;
    let tenant_envelope_ready = super::super::tenant_key_envelope_ready(&transaction).await?;
    crate::membership_delta::transaction_write_mode(&transaction).await?;

    let Some(marker) = marker else {
        if logical.is_some() || vector.is_some() || cleanup_ready {
            return Err(HelixDbError::MigrationRequired {
                reason: "V2 storage bootstrap is partial".to_string(),
            });
        }
        let mut rows = transaction.scan(..).await?;
        while let Some(row) = rows.next().await? {
            let is_global_v2 = row.key.starts_with(&GLOBAL_SENTINEL);
            let is_unscoped_v2 = row.key.first().copied() == Some(ScopedKey::key_prefix());
            let is_tenant_v2 = DataScope::strip_tenant_envelope(&row.key)
                .is_some_and(|(_, logical)| ScopedKey::parse_from_slice(logical).is_ok());
            let is_legacy_tenant =
                super::super::tenant::envelope::legacy_key_requires_migration(row.key, row.value)?;
            if tenant_envelope_ready && is_legacy_tenant {
                return Err(HelixDbError::MigrationRequired {
                    reason:
                        "tenant key envelope readiness is inconsistent with a legacy tenant key"
                            .to_string(),
                });
            }
            if is_global_v2 || is_unscoped_v2 || (is_tenant_v2 && !tenant_envelope_ready) {
                return Err(HelixDbError::MigrationRequired {
                    reason: "V2 storage rows exist without the complete bootstrap tuple"
                        .to_string(),
                });
            }
        }
        transaction.rollback();
        return Ok(WriterBootstrapPlan::Initialize);
    };

    let IndexV2MetadataValue::StorageVersion(version) =
        metadata_or_migration_required(&marker, "storage marker")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 storage marker contains the wrong value kind".to_string(),
        });
    };
    validate_writer_bootstrap_values(&marker, logical.as_deref(), vector.as_deref())?;
    if version < IndexStorageVersion::EQUALITY_BITMAP && cleanup_ready {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "index storage V4 cleanup is marked complete beside storage version {}",
                version.get()
            ),
        });
    }
    transaction.rollback();

    Ok(if version < IndexStorageVersion::CURRENT {
        WriterBootstrapPlan::MigrateToCurrent
    } else if cleanup_ready && tenant_envelope_ready {
        WriterBootstrapPlan::Ready
    } else {
        WriterBootstrapPlan::CleanupCurrent
    })
}

async fn initialize_writer_bootstrap(db: &Db) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let marker_key = global_key(GlobalKey::StorageVersion);
    let logical_key = global_key(GlobalKey::LogicalIndexIdWatermark);
    let vector_key = global_key(GlobalKey::VectorPhysicalIdWatermark);
    let marker = transaction.get(&marker_key).await?;
    let logical = transaction.get(&logical_key).await?;
    let vector = transaction.get(&vector_key).await?;
    let cleanup_ready = super::super::index_storage_v4_cleanup_ready(&transaction).await?;
    if marker.is_some() || logical.is_some() || vector.is_some() || cleanup_ready {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 storage bootstrap changed after writer preflight".to_string(),
        });
    }
    stage_writer_bootstrap_initialization(&transaction)?;
    transaction.commit().await?;
    Ok(())
}

fn stage_writer_bootstrap_initialization(transaction: &DbTransaction) -> Result<()> {
    let marker_key = global_key(GlobalKey::StorageVersion);
    let logical_key = global_key(GlobalKey::LogicalIndexIdWatermark);
    let vector_key = global_key(GlobalKey::VectorPhysicalIdWatermark);
    transaction.put(
        marker_key,
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::CURRENT,
        )),
    )?;
    transaction.put(
        logical_key,
        encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
            LogicalIndexIdWatermark {
                next_id: IndexId::initial(),
            },
        )),
    )?;
    transaction.put(
        vector_key,
        encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
            VectorPhysicalIdWatermark {
                next_id: VectorPhysicalIndexId::initial(),
            },
        )),
    )?;
    super::super::stage_index_storage_v4_cleanup_ready(transaction)?;
    Ok(())
}

fn global_key(key: GlobalKey) -> Bytes {
    ManagedIndexKey::Global { kind: key }.to_bytes()
}

fn metadata_or_migration_required(
    value: &[u8],
    role: &'static str,
) -> Result<IndexV2MetadataValue> {
    decode_metadata_value(value).map_err(|error| HelixDbError::MigrationRequired {
        reason: format!("malformed V2 {role}: {error}"),
    })
}

fn validate_writer_bootstrap_values(
    marker: &[u8],
    logical: Option<&[u8]>,
    vector: Option<&[u8]>,
) -> Result<()> {
    let IndexV2MetadataValue::StorageVersion(version) =
        metadata_or_migration_required(marker, "storage marker")?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 storage marker contains the wrong value kind".to_string(),
        });
    };
    let minimum_writer_version =
        IndexStorageVersion::new(0x0002).expect("the V2 storage version is non-zero");
    if version < minimum_writer_version {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "index storage version {} predates migratable version {}; recreate this development database",
                version.get(),
                minimum_writer_version.get()
            ),
        });
    }
    if version > IndexStorageVersion::MAX_SUPPORTED {
        return Err(HelixDbError::UnsupportedIndexStorageVersion {
            found: version.get(),
            supported: IndexStorageVersion::MAX_SUPPORTED.get(),
        });
    }
    let Some(logical) = logical else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 logical index watermark is missing".to_string(),
        });
    };
    let Some(vector) = vector else {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 vector physical watermark is missing".to_string(),
        });
    };
    if !matches!(
        metadata_or_migration_required(logical, "logical index watermark")?,
        IndexV2MetadataValue::LogicalIndexIdWatermark(_)
    ) || !matches!(
        metadata_or_migration_required(vector, "vector physical watermark")?,
        IndexV2MetadataValue::VectorPhysicalIdWatermark(_)
    ) {
        return Err(HelixDbError::MigrationRequired {
            reason: "V2 allocator record contains the wrong value kind".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_bootstrap_tuple_rejects_incomplete_and_cross_typed_shapes() {
        let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::CURRENT,
        ));
        let logical = encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
            LogicalIndexIdWatermark {
                next_id: IndexId::initial(),
            },
        ));
        let vector = encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
            VectorPhysicalIdWatermark {
                next_id: VectorPhysicalIndexId::initial(),
            },
        ));

        for (candidate_logical, candidate_vector) in [
            (None, None),
            (Some(logical.as_ref()), None),
            (None, Some(vector.as_ref())),
            (Some(vector.as_ref()), Some(logical.as_ref())),
        ] {
            assert!(matches!(
                validate_writer_bootstrap_values(&marker, candidate_logical, candidate_vector),
                Err(HelixDbError::MigrationRequired { .. })
            ));
        }
    }
}
