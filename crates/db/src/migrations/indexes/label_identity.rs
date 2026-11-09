//! Startup migration of hash-keyed label index rows to exact label keys.
//!
//! Before storage version 6 every label index row was keyed by the 64-bit
//! SipHash of its label, so two labels with one hash shared a bitmap. This
//! migration moves every node label, global edge label, and edge-label
//! neighbor row of every data scope to the key of its exact label (see
//! [`crate::encoding::indexes::label`]), then publishes
//! [`IndexStorageVersion::LABEL_IDENTITY`].
//!
//! # Contract
//!
//! - It runs in the writer open, after the blocking startup migrations and
//!   before the writer serves any graph write, so no write races a move. It is
//!   a no-op on a store already at the label identity version.
//! - A row's label is the `$label` of its lowest live member's record, checked
//!   against the row's hash; a mismatch fails closed. One member names the
//!   whole row: the migration relies on no two labels of a scope sharing a
//!   hash, which production stores were verified to satisfy before it shipped.
//!   A row with no live member indexes nothing and is deleted.
//! - Edge-label neighbor rows name opposite endpoints, not edges. Their labels
//!   come from the scope's exact global edge label rows, moved first: every
//!   live edge label has one. A neighbor hash no edge label explains fails
//!   closed.
//! - Each row is moved in one commit that merges its members into the exact
//!   row and deletes the hash row, so any snapshot files a label's members
//!   under exactly one of the two keys. Readers of a store before version 6
//!   fall back from an empty exact row to the hash row (see
//!   [`ReaderStorageCompatibility::LegacyLabelHash`]); a reader never observes
//!   a label's rows half-moved.
//! - A crash leaves the unmoved hash rows in place; the next open moves them.
//!   Batches are committed without awaiting durability, and the store is
//!   flushed before the version is published durably.
//!
//! [`ReaderStorageCompatibility::LegacyLabelHash`]:
//!     crate::index_lifecycle::repository::ReaderStorageCompatibility::LegacyLabelHash

use std::collections::BTreeMap;
use std::time::Instant;

use bytes::Bytes;
use roaring::RoaringTreemap;
use slatedb::config::WriteOptions;
use slatedb::{Db, IsolationLevel, WriteBatch};

use crate::encoding::indexes::label::{
    EdgeLabelKey, EdgeLabelNeighborKey, LabelIdentity, LabelScanPrefix, NodeLabelKey,
};
use crate::encoding::indexes::{hash_property_value, PropertyIndexKey, ValueHash};
use crate::encoding::keys::scope::DataScope;
use crate::encoding::keys::{EdgePropertyByIdKey, NodePropertyKey};
use crate::encoding::property::decode_properties;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, GlobalKey, ManagedIndexKey};
use crate::encoding::v2::legacy::label_hash::{
    legacy_edge_label_neighbor_scan_prefix, legacy_edge_label_scan_prefix, LegacyNodeLabelKey,
};
use crate::encoding::v2::values::indexes::{SecondaryEqualityBitmapValue, SecondaryEqualityValue};
use crate::encoding::v2::values::{decode_metadata_value, encode_metadata_value};
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{IndexStorageVersion, IndexV2MetadataValue};
use crate::transaction::Mutation;

/// Hash rows moved per commit. Neighbor rows dominate: one per endpoint,
/// direction, and edge label.
const MOVE_BATCH_ROWS: usize = 8192;
/// Stored hash-row bytes moved per commit, bounding one batch's memory.
const MOVE_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// One family of hash-keyed label rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyLabelRows {
    Node,
    Edge,
    Neighbor,
}

impl LegacyLabelRows {
    fn logical_prefix(self) -> Bytes {
        match self {
            Self::Node => LegacyNodeLabelKey::scan_prefix(),
            Self::Edge => legacy_edge_label_scan_prefix(),
            Self::Neighbor => legacy_edge_label_neighbor_scan_prefix(),
        }
    }
}

/// Rows one migration moved or deleted, for the startup log.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LabelIdentityMigration {
    pub(crate) scopes: usize,
    pub(crate) moved: u64,
    pub(crate) dropped: u64,
}

/// Moves every hash-keyed label row to its exact key and publishes
/// [`IndexStorageVersion::LABEL_IDENTITY`]; see the module contract.
pub(crate) async fn migrate_label_identity(db: &Db) -> Result<LabelIdentityMigration> {
    let version = storage_version(db).await?;
    if version >= IndexStorageVersion::LABEL_IDENTITY {
        return Ok(LabelIdentityMigration::default());
    }
    if version != IndexStorageVersion::OPERATION_QUEUES {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "label identity migration requires storage version {}, found {}",
                IndexStorageVersion::OPERATION_QUEUES.get(),
                version.get()
            ),
        });
    }

    let started = Instant::now();
    let mut migration = LabelIdentityMigration::default();
    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(db).await? {
        let scope_started = Instant::now();
        let mut scope_rows = LabelIdentityMigration::default();
        for rows in [LegacyLabelRows::Node, LegacyLabelRows::Edge] {
            move_rows(db, scope, rows, &BTreeMap::new(), &mut scope_rows).await?;
        }
        let edge_labels = edge_labels_by_hash(db, scope).await?;
        move_rows(
            db,
            scope,
            LegacyLabelRows::Neighbor,
            &edge_labels,
            &mut scope_rows,
        )
        .await?;
        for rows in [
            LegacyLabelRows::Node,
            LegacyLabelRows::Edge,
            LegacyLabelRows::Neighbor,
        ] {
            let mut remaining = db
                .scan_prefix(DataKey::data_prefix(scope, rows.logical_prefix()), ..)
                .await?;
            if remaining.next().await?.is_some() {
                return Err(HelixDbError::MigrationRequired {
                    reason: format!(
                        "{rows:?} hash label rows remained in {scope:?} after the move"
                    ),
                });
            }
        }
        if scope_rows.moved > 0 || scope_rows.dropped > 0 {
            tracing::info!(
                ?scope,
                moved = scope_rows.moved,
                dropped = scope_rows.dropped,
                elapsed_ms = u64::try_from(scope_started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "label identity migration moved a scope's label rows"
            );
        }
        migration.scopes += 1;
        migration.moved += scope_rows.moved;
        migration.dropped += scope_rows.dropped;
    }

    db.flush().await?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    let marker_key = ManagedIndexKey::Global {
        kind: GlobalKey::StorageVersion,
    }
    .to_bytes();
    let marker =
        transaction
            .get(&marker_key)
            .await?
            .ok_or_else(|| HelixDbError::MigrationRequired {
                reason: "storage marker disappeared during the label identity migration"
                    .to_string(),
            })?;
    let IndexV2MetadataValue::StorageVersion(published) = decode_metadata_value(&marker)? else {
        return Err(HelixDbError::MigrationRequired {
            reason: "storage marker holds another value kind".to_string(),
        });
    };
    if published != version {
        transaction.rollback();
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "storage version changed from {} to {} during the label identity migration",
                version.get(),
                published.get()
            ),
        });
    }
    Mutation::put(
        &transaction,
        marker_key,
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::LABEL_IDENTITY,
        )),
    )?;
    transaction.commit().await?;
    tracing::info!(
        scopes = migration.scopes,
        moved = migration.moved,
        dropped = migration.dropped,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "label identity migration published exact label rows"
    );
    Ok(migration)
}

async fn storage_version(db: &Db) -> Result<IndexStorageVersion> {
    let marker = db
        .get(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
        )
        .await?
        .ok_or_else(|| HelixDbError::MigrationRequired {
            reason: "label identity migration requires a V2 storage marker".to_string(),
        })?;
    let IndexV2MetadataValue::StorageVersion(version) = decode_metadata_value(&marker)? else {
        return Err(HelixDbError::MigrationRequired {
            reason: "storage marker holds another value kind".to_string(),
        });
    };
    Ok(version)
}

/// Moves every `rows` row of `scope` to its exact key, in batches.
async fn move_rows(
    db: &Db,
    scope: DataScope,
    rows: LegacyLabelRows,
    edge_labels: &BTreeMap<ValueHash, LabelIdentity>,
    migration: &mut LabelIdentityMigration,
) -> Result<()> {
    let mut legacy = db
        .scan_prefix(DataKey::data_prefix(scope, rows.logical_prefix()), ..)
        .await?;
    let mut batch = WriteBatch::new();
    let mut batch_rows = 0usize;
    let mut batch_bytes = 0usize;
    while let Some(row) = legacy.next().await? {
        let members = SecondaryEqualityValue::decode(&row.value)?.into_ids();
        let target = match rows {
            LegacyLabelRows::Node => node_target(db, scope, &row.key, &members).await?,
            LegacyLabelRows::Edge => edge_target(db, scope, &row.key, &members).await?,
            LegacyLabelRows::Neighbor => Some(neighbor_target(scope, &row.key, edge_labels)?),
        };
        batch.delete(&row.key);
        match target {
            Some(exact) => {
                batch.merge(exact, SecondaryEqualityBitmapValue::new(members).encode());
                migration.moved += 1;
            }
            None => migration.dropped += 1,
        }
        batch_rows += 1;
        batch_bytes = batch_bytes.saturating_add(row.key.len() + row.value.len());
        if batch_rows == MOVE_BATCH_ROWS || batch_bytes >= MOVE_BATCH_BYTES {
            write_batch(db, std::mem::take(&mut batch)).await?;
            batch_rows = 0;
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        write_batch(db, batch).await?;
    }
    Ok(())
}

async fn write_batch(db: &Db, batch: WriteBatch) -> Result<()> {
    db.write_with_options(
        batch,
        &WriteOptions {
            await_durable: false,
            ..WriteOptions::default()
        },
    )
    .await?;
    Ok(())
}

/// The exact node label row for one hash row, or `None` when no member is live.
async fn node_target(
    db: &Db,
    scope: DataScope,
    legacy: &[u8],
    members: &RoaringTreemap,
) -> Result<Option<Bytes>> {
    let DataKey::Data {
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(key)),
        ..
    } = DataKey::parse_from_slice(scope, legacy)?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: format!("{scope:?} holds a non-equality row under the node label prefix"),
        });
    };
    for member in members {
        let record = DataKey::Data {
            scope,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(member)),
        };
        let Some(properties) = db.get(record.to_bytes()).await? else {
            continue;
        };
        let identity = record_label(scope, "node", member, &properties, &key.value_hash)?;
        return Ok(Some(
            DataKey::Data {
                scope,
                kind: DataKeyKind::PropertyIndex(PropertyIndexKey::NodeLabel(NodeLabelKey::new(
                    identity,
                ))),
            }
            .to_bytes(),
        ));
    }
    Ok(None)
}

/// The exact global edge label row for one hash row, or `None` when no member is live.
async fn edge_target(
    db: &Db,
    scope: DataScope,
    legacy: &[u8],
    members: &RoaringTreemap,
) -> Result<Option<Bytes>> {
    let DataKey::Data {
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::LegacyEdgeLabel(key)),
        ..
    } = DataKey::parse_from_slice(scope, legacy)?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: format!("{scope:?} holds a malformed hash edge label row"),
        });
    };
    for member in members {
        let record = DataKey::Data {
            scope,
            kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(member)),
        };
        let Some(properties) = db.get(record.to_bytes()).await? else {
            continue;
        };
        let identity = record_label(scope, "edge", member, &properties, key.label_hash())?;
        return Ok(Some(
            DataKey::Data {
                scope,
                kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(
                    identity,
                ))),
            }
            .to_bytes(),
        ));
    }
    Ok(None)
}

/// The exact neighbor row for one hash row, named by the scope's edge labels.
fn neighbor_target(
    scope: DataScope,
    legacy: &[u8],
    edge_labels: &BTreeMap<ValueHash, LabelIdentity>,
) -> Result<Bytes> {
    let DataKey::Data {
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::LegacyEdgeLabelNeighbor(key)),
        ..
    } = DataKey::parse_from_slice(scope, legacy)?
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: format!("{scope:?} holds a malformed hash edge label neighbor row"),
        });
    };
    let Some(identity) = edge_labels.get(key.label_hash()) else {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "{scope:?} edge label neighbor row of node {} names no edge label of the scope",
                key.node_id()
            ),
        });
    };
    Ok(DataKey::Data {
        scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(key.direction(), key.node_id(), identity.clone()),
        )),
    }
    .to_bytes())
}

/// The `$label` of one record, which must hash to the row it was found in.
fn record_label(
    scope: DataScope,
    kind: &str,
    id: u64,
    properties: &[u8],
    row_hash: &ValueHash,
) -> Result<LabelIdentity> {
    let properties = decode_properties(properties)?;
    let Some(label) = properties
        .iter()
        .find(|property| property.name == "$label")
        .and_then(|property| property.value.as_str())
    else {
        return Err(HelixDbError::MigrationRequired {
            reason: format!("{scope:?} {kind} {id} is indexed by label but has no string $label"),
        });
    };
    if hash_property_value(label) != *row_hash {
        return Err(HelixDbError::MigrationRequired {
            reason: format!(
                "{scope:?} {kind} {id} has a $label that does not hash to the label row it is in"
            ),
        });
    }
    Ok(LabelIdentity::new(label)?)
}

/// Every exact edge label of `scope`, by the legacy hash its neighbor rows use.
async fn edge_labels_by_hash(
    db: &Db,
    scope: DataScope,
) -> Result<BTreeMap<ValueHash, LabelIdentity>> {
    let mut labels = BTreeMap::new();
    let mut rows = db
        .scan_prefix(
            DataKey::data_prefix(scope, LabelScanPrefix::EdgeLabels.to_bytes()),
            ..,
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let DataKey::Data {
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(key)),
            ..
        } = DataKey::parse_from_slice(scope, &row.key)?
        else {
            return Err(HelixDbError::MigrationRequired {
                reason: format!("{scope:?} holds a malformed exact edge label row"),
            });
        };
        let identity = key.identity().clone();
        let hash = hash_property_value(identity.label());
        let Some(existing) = labels.insert(hash, identity) else {
            continue;
        };
        if existing != *key.identity() {
            return Err(HelixDbError::MigrationRequired {
                reason: format!(
                    "{scope:?} edge labels {:?} and {:?} share one hash, so their neighbor rows are ambiguous",
                    existing.label(),
                    key.identity().label()
                ),
            });
        }
    }
    Ok(labels)
}

/// Files every exact label row of every scope back under its hash key and
/// rewinds the marker to [`IndexStorageVersion::OPERATION_QUEUES`]: the store
/// a binary before label identity left. The caller must quiesce every write.
#[cfg(test)]
pub(crate) async fn make_legacy_label_fixture(db: &Db) -> Result<()> {
    use crate::encoding::v2::legacy::label_hash::{LegacyEdgeLabelKey, LegacyEdgeLabelNeighborKey};

    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(db).await? {
        for prefix in [
            LabelScanPrefix::NodeLabels,
            LabelScanPrefix::EdgeLabels,
            LabelScanPrefix::EdgeLabelNeighbors,
        ] {
            let mut rows = db
                .scan_prefix(DataKey::data_prefix(scope, prefix.to_bytes()), ..)
                .await?;
            let mut batch = WriteBatch::new();
            while let Some(row) = rows.next().await? {
                let DataKey::Data {
                    kind: DataKeyKind::PropertyIndex(key),
                    ..
                } = DataKey::parse_from_slice(scope, &row.key)?
                else {
                    unreachable!("label prefixes hold property index rows");
                };
                let legacy = match key {
                    PropertyIndexKey::NodeLabel(key) => PropertyIndexKey::Equality(
                        LegacyNodeLabelKey::for_label(key.identity().label()),
                    ),
                    PropertyIndexKey::EdgeLabel(key) => PropertyIndexKey::LegacyEdgeLabel(
                        LegacyEdgeLabelKey::for_label(key.identity().label()),
                    ),
                    PropertyIndexKey::EdgeLabelNeighbor(key) => {
                        PropertyIndexKey::LegacyEdgeLabelNeighbor(
                            LegacyEdgeLabelNeighborKey::for_label(
                                key.direction(),
                                key.node_id(),
                                key.identity().label(),
                            ),
                        )
                    }
                    other @ (PropertyIndexKey::Equality(_)
                    | PropertyIndexKey::EdgeEquality(_)
                    | PropertyIndexKey::LegacyEdgeLabel(_)
                    | PropertyIndexKey::LegacyEdgeLabelNeighbor(_)
                    | PropertyIndexKey::GlobalEdgeEquality(_)
                    | PropertyIndexKey::Range(_)
                    | PropertyIndexKey::EdgeRange(_)
                    | PropertyIndexKey::GlobalEdgeRange(_)) => {
                        unreachable!("label prefixes hold only label rows, got {other:?}")
                    }
                };
                batch.merge(
                    DataKey::Data {
                        scope,
                        kind: DataKeyKind::PropertyIndex(legacy),
                    }
                    .to_bytes(),
                    SecondaryEqualityBitmapValue::new(
                        SecondaryEqualityValue::decode(&row.value)?.into_ids(),
                    )
                    .encode(),
                );
                batch.delete(&row.key);
            }
            db.write(batch).await?;
        }
    }
    db.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::OPERATION_QUEUES,
        )),
    )
    .await?;
    db.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use helix_ast::batch::{read_batch, write_batch};
    use helix_ast::graph::NodeRef;
    use helix_ast::query::QueryRequest;
    use helix_ast::traversal::g;
    use helix_ast::value::PropertyInput;
    use slatedb::object_store::{memory::InMemory, ObjectStore};

    use super::*;
    use crate::encoding::indexes::EdgeDirection;
    use crate::encoding::keys::scope::TenantId;
    use crate::encoding::v2::legacy::label_hash::{LegacyEdgeLabelKey, LegacyEdgeLabelNeighborKey};
    use crate::index_lifecycle::repository::ReaderStorageCompatibility;

    type Rows = BTreeMap<Bytes, RoaringTreemap>;

    fn scopes() -> [DataScope; 3] {
        [
            DataScope::LegacyUnscoped,
            DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000A").unwrap()),
            DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000B").unwrap()),
        ]
    }

    /// Two people who know each other and both work at one company.
    async fn write_graph(db: &crate::HelixDB, scope: DataScope) {
        let since = || vec![("since", PropertyInput::from(2020i64))];
        db.query_scoped(
            QueryRequest::write(
                write_batch()
                    .var_as(
                        "ada",
                        g().add_n("Person", vec![("name", PropertyInput::from("ada"))]),
                    )
                    .var_as(
                        "bob",
                        g().add_n("Person", vec![("name", PropertyInput::from("bob"))]),
                    )
                    .var_as(
                        "acme",
                        g().add_n("Company", vec![("name", PropertyInput::from("acme"))]),
                    )
                    .var_as(
                        "knows",
                        g().n(NodeRef::var("ada"))
                            .add_e("KNOWS", NodeRef::var("bob"), since()),
                    )
                    .var_as(
                        "ada_works",
                        g().n(NodeRef::var("ada"))
                            .add_e("WORKS_AT", NodeRef::var("acme"), since()),
                    )
                    .var_as(
                        "bob_works",
                        g().n(NodeRef::var("bob"))
                            .add_e("WORKS_AT", NodeRef::var("acme"), since()),
                    )
                    .returning(Vec::<String>::new()),
            ),
            scope,
        )
        .await
        .expect("fixture graph writes");
    }

    /// Every label-served read shape: node and edge label scans and labeled
    /// expansion in both directions.
    async fn label_reads(db: &crate::HelixDB, scope: DataScope) -> serde_json::Value {
        db.query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as("people", g().n_with_label("Person").count())
                    .var_as("companies", g().n_with_label("Company").count())
                    .var_as("knows", g().e_with_label("KNOWS").count())
                    .var_as("works_at", g().e_with_label("WORKS_AT").count())
                    .var_as(
                        "employers",
                        g().n_with_label("Person").out(Some("WORKS_AT")).count(),
                    )
                    .var_as(
                        "employees",
                        g().n_with_label("Company").in_(Some("WORKS_AT")).count(),
                    )
                    .returning([
                        "people",
                        "companies",
                        "knows",
                        "works_at",
                        "employers",
                        "employees",
                    ]),
            ),
            scope,
        )
        .await
        .expect("label reads execute")
    }

    async fn all_label_reads(db: &crate::HelixDB) -> Vec<serde_json::Value> {
        let mut reads = Vec::new();
        for scope in scopes() {
            reads.push(label_reads(db, scope).await);
        }
        reads
    }

    /// Exact and legacy label rows of every scope.
    async fn label_rows(db: &Db) -> (Rows, Rows) {
        let mut exact = Rows::new();
        let mut legacy = Rows::new();
        for scope in crate::index_lifecycle::queue::recovery::discover_scopes(db)
            .await
            .unwrap()
        {
            for (is_exact, prefix) in [
                (true, LabelScanPrefix::NodeLabels.to_bytes()),
                (true, LabelScanPrefix::EdgeLabels.to_bytes()),
                (true, LabelScanPrefix::EdgeLabelNeighbors.to_bytes()),
                (false, LegacyLabelRows::Node.logical_prefix()),
                (false, LegacyLabelRows::Edge.logical_prefix()),
                (false, LegacyLabelRows::Neighbor.logical_prefix()),
            ] {
                let rows = if is_exact { &mut exact } else { &mut legacy };
                let mut scan = db
                    .scan_prefix(DataKey::data_prefix(scope, prefix), ..)
                    .await
                    .unwrap();
                while let Some(row) = scan.next().await.unwrap() {
                    rows.insert(
                        row.key,
                        SecondaryEqualityValue::decode(&row.value)
                            .unwrap()
                            .into_ids(),
                    );
                }
            }
        }
        (exact, legacy)
    }

    async fn put_legacy_row(db: &Db, key: PropertyIndexKey<'static>, ids: &[u64]) {
        db.put(
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::PropertyIndex(key),
            }
            .to_bytes(),
            SecondaryEqualityBitmapValue::new(RoaringTreemap::from_iter(ids.iter().copied()))
                .encode(),
        )
        .await
        .unwrap();
    }

    async fn open_writer(store: &Arc<dyn ObjectStore>) -> crate::HelixDB {
        crate::HelixDB::open_with_object_store_for_tests("label-identity", Arc::clone(store))
            .await
            .expect("writer opens")
    }

    /// A store with the fixture graph in every scope, filed under hash keys
    /// as a binary before label identity left it.
    async fn legacy_store() -> (
        Arc<dyn ObjectStore>,
        crate::HelixDB,
        Vec<serde_json::Value>,
        Rows,
    ) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open_writer(&store).await;
        assert_eq!(
            storage_version(&db.inner_db()).await.unwrap(),
            IndexStorageVersion::LABEL_IDENTITY
        );
        for scope in scopes() {
            write_graph(&db, scope).await;
        }
        let reads = all_label_reads(&db).await;
        for scope_reads in &reads {
            for (read, expected) in [
                ("people", 2),
                ("companies", 1),
                ("knows", 1),
                ("works_at", 2),
                ("employers", 2),
                ("employees", 2),
            ] {
                assert_eq!(scope_reads[read], serde_json::json!(expected), "{read}");
            }
        }
        let (exact, legacy) = label_rows(&db.inner_db()).await;
        assert!(legacy.is_empty());
        // Two node labels, two edge labels, and five neighbor rows per scope.
        assert_eq!(exact.len(), 9 * scopes().len());
        make_legacy_label_fixture(&db.inner_db()).await.unwrap();
        (store, db, reads, exact)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn migration_moves_every_scope_and_reads_stay_exact_at_every_boundary() {
        let (store, db, reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        let (exact, legacy) = label_rows(&inner).await;
        assert!(exact.is_empty());
        assert_eq!(legacy.len(), exact_before.len());
        assert_eq!(
            storage_version(&inner).await.unwrap(),
            IndexStorageVersion::OPERATION_QUEUES
        );

        let reader = crate::HelixDB::open_reader_with_object_store_for_tests(
            "label-identity",
            Arc::clone(&store),
        )
        .await
        .expect("a pre-label store is reader-compatible");
        assert_eq!(
            reader.reader_storage_compatibility_for_tests(),
            ReaderStorageCompatibility::LegacyLabelHash
        );
        assert_eq!(all_label_reads(&reader).await, reads);

        // A crash after node rows moved leaves every other row hash-keyed.
        let mut partial = LabelIdentityMigration::default();
        for scope in scopes() {
            move_rows(
                &inner,
                scope,
                LegacyLabelRows::Node,
                &BTreeMap::new(),
                &mut partial,
            )
            .await
            .unwrap();
        }
        assert_eq!(partial.moved, 2 * scopes().len() as u64);
        inner.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(all_label_reads(&reader).await, reads);

        let migration = migrate_label_identity(&inner).await.unwrap();
        assert_eq!(migration.moved + partial.moved, exact_before.len() as u64);
        assert_eq!(migration.dropped, 0);
        let (exact, legacy) = label_rows(&inner).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        assert_eq!(
            storage_version(&inner).await.unwrap(),
            IndexStorageVersion::LABEL_IDENTITY
        );
        assert_eq!(
            migrate_label_identity(&inner).await.unwrap(),
            LabelIdentityMigration::default()
        );
        assert_eq!(all_label_reads(&db).await, reads);

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert_eq!(all_label_reads(&reader).await, reads);
                if reader.reader_storage_compatibility_for_tests()
                    == ReaderStorageCompatibility::Current
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the reader switches to exact label rows after publication");

        reader.close().await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writer_open_migrates_a_pre_label_store() {
        let (store, db, reads, exact_before) = legacy_store().await;
        db.close().await.unwrap();

        let db = open_writer(&store).await;
        let inner = db.inner_db();
        assert_eq!(
            storage_version(&inner).await.unwrap(),
            IndexStorageVersion::LABEL_IDENTITY
        );
        let (exact, legacy) = label_rows(&inner).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        assert_eq!(all_label_reads(&db).await, reads);
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rows_without_a_live_member_are_dropped() {
        let (_store, db, _reads, exact_before) = legacy_store().await;
        let inner = db.inner_db();
        put_legacy_row(
            &inner,
            PropertyIndexKey::Equality(LegacyNodeLabelKey::for_label("Ghost")),
            &[9_999],
        )
        .await;
        put_legacy_row(
            &inner,
            PropertyIndexKey::LegacyEdgeLabel(LegacyEdgeLabelKey::for_label("HAUNTS")),
            &[9_999],
        )
        .await;

        let migration = migrate_label_identity(&inner).await.unwrap();
        assert_eq!(migration.moved, exact_before.len() as u64);
        assert_eq!(migration.dropped, 2);
        let (exact, legacy) = label_rows(&inner).await;
        assert_eq!(exact, exact_before);
        assert!(legacy.is_empty());
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_member_whose_label_disagrees_with_its_row_fails_closed() {
        let (_store, db, _reads, _exact) = legacy_store().await;
        let inner = db.inner_db();
        let ada = label_rows(&inner)
            .await
            .1
            .values()
            .flat_map(|ids| ids.iter())
            .min()
            .expect("the fixture has members");
        // A row under another label's hash whose lowest live member is a Person.
        put_legacy_row(
            &inner,
            PropertyIndexKey::Equality(LegacyNodeLabelKey::for_label("Ghost")),
            &[ada],
        )
        .await;

        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        assert_eq!(
            storage_version(&inner).await.unwrap(),
            IndexStorageVersion::OPERATION_QUEUES
        );
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_neighbor_row_no_edge_label_explains_fails_closed() {
        let (_store, db, _reads, _exact) = legacy_store().await;
        let inner = db.inner_db();
        put_legacy_row(
            &inner,
            PropertyIndexKey::LegacyEdgeLabelNeighbor(LegacyEdgeLabelNeighborKey::for_label(
                EdgeDirection::Out,
                1,
                "HAUNTS",
            )),
            &[2],
        )
        .await;

        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        assert_eq!(
            storage_version(&inner).await.unwrap(),
            IndexStorageVersion::OPERATION_QUEUES
        );
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn migration_runs_only_from_the_operation_queue_version() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open_writer(&store).await;
        let inner = db.inner_db();
        assert_eq!(
            migrate_label_identity(&inner).await.unwrap(),
            LabelIdentityMigration::default()
        );
        inner
            .put(
                ManagedIndexKey::Global {
                    kind: GlobalKey::StorageVersion,
                }
                .to_bytes(),
                encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                    IndexStorageVersion::EQUALITY_BITMAPS,
                )),
            )
            .await
            .unwrap();
        assert!(matches!(
            migrate_label_identity(&inner).await,
            Err(HelixDbError::MigrationRequired { .. })
        ));
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn labels_longer_than_any_label_key_are_rejected_at_write_time() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open_writer(&store).await;
        let label = "L".repeat(crate::encoding::indexes::label::MAX_LABEL_LEN + 1);
        let error = db
            .query(QueryRequest::write(
                write_batch()
                    .var_as(
                        "node",
                        g().add_n(label.as_str(), vec![("name", PropertyInput::from("long"))]),
                    )
                    .returning(Vec::<String>::new()),
            ))
            .await
            .expect_err("an unkeyable label is rejected");
        assert!(
            error.to_string().contains("label index bound"),
            "unexpected error: {error}"
        );
        db.close().await.unwrap();
    }
}
