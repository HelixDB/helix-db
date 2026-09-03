//! Persisted activation boundary for V2 graph membership delta operands.

use bytes::Bytes;
use slatedb::{Db, DbReadOps, DbTransaction, IsolationLevel};

use crate::encoding::v2::keys::{GlobalKey, ManagedIndexKey};
use crate::encoding::v2::values::{decode_metadata_value, encode_metadata_value};
use crate::index_lifecycle::{IndexStorageVersion, IndexV2MetadataValue};
use crate::{HelixDbError, MembershipDeltaWriteMode, Result};

fn write_mode_key() -> Bytes {
    ManagedIndexKey::Global {
        kind: GlobalKey::MembershipDeltaWriteMode,
    }
    .to_bytes()
}

fn storage_version_key() -> Bytes {
    ManagedIndexKey::Global {
        kind: GlobalKey::StorageVersion,
    }
    .to_bytes()
}

pub(crate) fn decode_write_mode(
    mode: Option<&[u8]>,
    storage_version: Option<&[u8]>,
) -> Result<MembershipDeltaWriteMode> {
    let mode = mode
        .map(|value| {
            let IndexV2MetadataValue::MembershipDeltaWriteMode(mode) = decode_metadata_value(value)
                .map_err(|error| HelixDbError::MigrationRequired {
                    reason: format!("membership delta write mode is malformed: {error}"),
                })?
            else {
                return Err(HelixDbError::MigrationRequired {
                    reason: "membership delta write mode contains another metadata value"
                        .to_string(),
                });
            };
            Ok(mode)
        })
        .transpose()?
        .unwrap_or_default();
    let storage_version = storage_version
        .map(|value| {
            let IndexV2MetadataValue::StorageVersion(version) = decode_metadata_value(value)
                .map_err(|error| HelixDbError::MigrationRequired {
                    reason: format!("membership delta storage fence is malformed: {error}"),
                })?
            else {
                return Err(HelixDbError::MigrationRequired {
                    reason: "membership delta storage fence contains another metadata value"
                        .to_string(),
                });
            };
            Ok(version)
        })
        .transpose()?;

    match (mode, storage_version) {
        (MembershipDeltaWriteMode::LegacyExclusive, None) => {
            Ok(MembershipDeltaWriteMode::LegacyExclusive)
        }
        (MembershipDeltaWriteMode::LegacyExclusive, Some(version))
            if version <= IndexStorageVersion::CURRENT =>
        {
            Ok(MembershipDeltaWriteMode::LegacyExclusive)
        }
        (MembershipDeltaWriteMode::DisjointV2, Some(version))
            if version >= IndexStorageVersion::DISJOINT_MEMBERSHIP =>
        {
            Ok(MembershipDeltaWriteMode::DisjointV2)
        }
        _ => Err(HelixDbError::MigrationRequired {
            reason: "membership delta write mode and storage format fence disagree".to_string(),
        }),
    }
}

pub(crate) async fn read_write_mode(
    reader: &(impl DbReadOps + Send + Sync),
) -> Result<MembershipDeltaWriteMode> {
    let [mode_key, storage_version_key] = [write_mode_key(), storage_version_key()];
    let values = reader.multi_get(&[mode_key, storage_version_key]).await?;
    let [mode, storage_version] = values.as_slice() else {
        unreachable!("membership delta metadata lookup requests exactly two keys")
    };
    decode_write_mode(mode.as_deref(), storage_version.as_deref())
}

pub(crate) async fn transaction_write_mode(
    transaction: &DbTransaction,
) -> Result<MembershipDeltaWriteMode> {
    read_write_mode(transaction).await
}

pub(crate) async fn activate(db: &Db) -> Result<()> {
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let key = write_mode_key();
    let version_key = storage_version_key();
    let mode = transaction.get(&key).await?;
    let storage_version = transaction.get(&version_key).await?;
    match decode_write_mode(mode.as_deref(), storage_version.as_deref())? {
        MembershipDeltaWriteMode::LegacyExclusive => {
            let Some(storage_version) = storage_version else {
                return Err(HelixDbError::MigrationRequired {
                    reason: "membership delta activation requires a bootstrapped storage marker"
                        .to_string(),
                });
            };
            let IndexV2MetadataValue::StorageVersion(storage_version) =
                decode_metadata_value(&storage_version)?
            else {
                return Err(HelixDbError::MigrationRequired {
                    reason: "membership delta storage fence contains another metadata value"
                        .to_string(),
                });
            };
            if storage_version != IndexStorageVersion::CURRENT {
                return Err(HelixDbError::MigrationRequired {
                    reason: format!(
                        "membership delta activation requires storage version {}, found {}",
                        IndexStorageVersion::CURRENT.get(),
                        storage_version.get()
                    ),
                });
            }
            let no_expiry = slatedb::config::PutOptions {
                ttl: slatedb::config::Ttl::NoExpiry,
            };
            transaction.put_with_options(
                key,
                encode_metadata_value(&IndexV2MetadataValue::MembershipDeltaWriteMode(
                    MembershipDeltaWriteMode::DisjointV2,
                )),
                &no_expiry,
            )?;
        }
        MembershipDeltaWriteMode::DisjointV2 => {}
    }
    transaction.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;

    use super::*;
    use crate::merge_operator::HelixMergeOperator;

    async fn test_db(name: &str) -> Db {
        let db = Db::builder(name, Arc::new(InMemory::new()))
            .with_merge_operator(Arc::new(HelixMergeOperator::new()))
            .build()
            .await
            .unwrap();
        crate::migrations::startup::bootstrap_writer(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn activation_is_persisted_idempotent_and_monotonic() {
        let db = test_db("membership-delta-activation").await;
        assert_eq!(
            read_write_mode(&db).await.unwrap(),
            MembershipDeltaWriteMode::LegacyExclusive
        );
        activate(&db).await.unwrap();
        activate(&db).await.unwrap();
        assert_eq!(
            read_write_mode(&db).await.unwrap(),
            MembershipDeltaWriteMode::DisjointV2
        );
        let version = db.get(storage_version_key()).await.unwrap().unwrap();
        assert_eq!(
            decode_metadata_value(&version).unwrap(),
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT)
        );
    }

    #[tokio::test]
    async fn malformed_activation_marker_fails_closed() {
        let db = test_db("membership-delta-corrupt-activation").await;
        db.put(write_mode_key(), b"invalid").await.unwrap();
        assert!(read_write_mode(&db).await.is_err());
        assert!(activate(&db).await.is_err());
    }

    #[tokio::test]
    async fn mode_and_storage_fence_must_change_together() {
        let db = test_db("membership-delta-inconsistent-activation").await;
        db.put(
            write_mode_key(),
            encode_metadata_value(&IndexV2MetadataValue::MembershipDeltaWriteMode(
                MembershipDeltaWriteMode::DisjointV2,
            )),
        )
        .await
        .unwrap();
        db.put(
            storage_version_key(),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::EQUALITY_BITMAP,
            )),
        )
        .await
        .unwrap();
        assert!(read_write_mode(&db).await.is_err());
        assert!(activate(&db).await.is_err());
    }

    #[tokio::test]
    async fn legacy_storage_versions_remain_migratable_but_cannot_activate_early() {
        let db = test_db("membership-delta-legacy-storage-version").await;
        let legacy_version = IndexStorageVersion::new(0x0003).unwrap();
        db.put(
            storage_version_key(),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(legacy_version)),
        )
        .await
        .unwrap();

        assert_eq!(
            read_write_mode(&db).await.unwrap(),
            MembershipDeltaWriteMode::LegacyExclusive
        );
        assert!(matches!(
            activate(&db).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        assert_eq!(
            read_write_mode(&db).await.unwrap(),
            MembershipDeltaWriteMode::LegacyExclusive
        );
    }

    #[tokio::test]
    async fn activation_conflicts_with_a_writer_that_observed_legacy_mode() {
        let db = test_db("membership-delta-activation-fences-legacy-writer").await;
        let legacy_writer = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        assert_eq!(
            transaction_write_mode(&legacy_writer).await.unwrap(),
            MembershipDeltaWriteMode::LegacyExclusive
        );
        legacy_writer.put(b"legacy-write", b"staged").unwrap();

        activate(&db).await.unwrap();
        assert!(legacy_writer.commit().await.is_err());
        assert_eq!(db.get(b"legacy-write").await.unwrap(), None);
        assert_eq!(
            read_write_mode(&db).await.unwrap(),
            MembershipDeltaWriteMode::DisjointV2
        );
    }
}
