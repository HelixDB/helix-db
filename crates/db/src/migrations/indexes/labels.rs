//! Blocking rewrite of hash-only graph `$label` indexes to `CanonicalLabel`.
//!
//! The V4/V5 marker stays authoritative while every scope is rebuilt from
//! graph rows and verified. Publication is one final marker write; hash-only
//! keys are deleted only afterward, so a crash before publication resumes
//! from the previous format.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use bytes::Bytes;
use slatedb::{Db, IsolationLevel};

use crate::encoding::indexes::equality::scans::EqualityScanPrefix;
use crate::encoding::indexes::label::{EdgeLabelNeighborScanPrefix, EdgeLabelScanPrefix};
use crate::encoding::indexes::{
    hash_property_name, EdgeDirection, IndexPrefix, INDEX_PREFIX_LEN, NODE_ID_MAX_LEN,
    PROPERTY_HASH_MAX_LEN, VALUE_HASH_MAX_LEN,
};
use crate::encoding::keys::{KeyPrefix, PREFIX_LEN};
use crate::encoding::property::{decode_properties, Property};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, GlobalKey, ManagedIndexKey};
use crate::encoding::v2::values::encode_metadata_value;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::{IndexElementKind, IndexStorageVersion, IndexV2MetadataValue};
use crate::search;

const MIGRATION_BATCH_SIZE: usize = 256;
const NODE_LABEL_PROPERTY: &str = "$label";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CanonicalLabelMigrationFailpoint {
    RewriteBefore,
    VerificationBefore,
    PublicationBefore,
    PublicationAfter,
    CleanupBefore,
}

impl CanonicalLabelMigrationFailpoint {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RewriteBefore => "rewrite_before",
            Self::VerificationBefore => "verification_before",
            Self::PublicationBefore => "publication_before",
            Self::PublicationAfter => "publication_after",
            Self::CleanupBefore => "cleanup_before",
        }
    }
}

static INJECTED_FAILPOINT: Mutex<Option<CanonicalLabelMigrationFailpoint>> = Mutex::new(None);
static FAILPOINT_TRIGGERED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
pub(crate) fn inject_once(failpoint: CanonicalLabelMigrationFailpoint) -> Result<()> {
    let mut injected = INJECTED_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation(
            "canonical label migration failpoint mutex was poisoned".to_string(),
        )
    })?;
    *injected = Some(failpoint);
    FAILPOINT_TRIGGERED.store(false, Ordering::SeqCst);
    Ok(())
}

#[cfg(test)]
pub(crate) fn was_triggered() -> bool {
    FAILPOINT_TRIGGERED.load(Ordering::SeqCst)
}

fn trip(failpoint: CanonicalLabelMigrationFailpoint) -> Result<()> {
    let mut injected = INJECTED_FAILPOINT.lock().map_err(|_| {
        HelixDbError::InvariantViolation(
            "canonical label migration failpoint mutex was poisoned".to_string(),
        )
    })?;
    if *injected == Some(failpoint) {
        *injected = None;
        FAILPOINT_TRIGGERED.store(true, Ordering::SeqCst);
        return Err(injected_error(failpoint));
    }
    Ok(())
}

fn injected_error(failpoint: CanonicalLabelMigrationFailpoint) -> HelixDbError {
    HelixDbError::InvariantViolation(format!(
        "injected canonical label migration failpoint {}",
        failpoint.as_str()
    ))
}

fn corruption(message: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(message.to_string())
}

/// Rewrites hash-only graph labels from authoritative rows and publishes `0x0006`.
pub(crate) async fn migrate_hash_labels_to_canonical(db: &Db) -> Result<()> {
    rewrite_and_verify_canonical_labels(db).await?;

    trip(CanonicalLabelMigrationFailpoint::PublicationBefore)?;
    let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
    transaction.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::CURRENT,
        )),
    )?;
    transaction.commit().await?;
    trip(CanonicalLabelMigrationFailpoint::PublicationAfter)?;

    cleanup_hash_only_label_keys(db).await
}

/// Rebuilds canonical label indexes from graph rows and verifies membership.
pub(crate) async fn rewrite_and_verify_canonical_labels(db: &Db) -> Result<()> {
    trip(CanonicalLabelMigrationFailpoint::RewriteBefore)?;
    let scopes = discover_graph_scopes(db).await?;
    for scope in &scopes {
        rewrite_scope_labels(db, *scope).await?;
    }
    trip(CanonicalLabelMigrationFailpoint::VerificationBefore)?;
    for scope in &scopes {
        verify_scope_labels(db, *scope).await?;
    }
    Ok(())
}

/// Deletes hash-only `$label` equality, edge-label, and neighbor keys.
pub(crate) async fn cleanup_hash_only_label_keys(db: &Db) -> Result<()> {
    trip(CanonicalLabelMigrationFailpoint::CleanupBefore)?;
    let scopes = discover_graph_scopes(db).await?;
    for scope in scopes {
        delete_matching_keys(db, hash_only_node_label_prefix(scope), |key| {
            is_hash_only_node_label_key(scope, key)
        })
        .await?;
        delete_matching_keys(db, edge_label_prefix(scope), |key| {
            is_hash_only_edge_label_key(scope, key)
        })
        .await?;
        delete_matching_keys(db, edge_label_neighbor_prefix(scope), |key| {
            is_hash_only_edge_neighbor_key(scope, key)
        })
        .await?;
    }
    Ok(())
}

async fn discover_graph_scopes(db: &Db) -> Result<BTreeSet<DataScope>> {
    let mut scopes = BTreeSet::from([DataScope::LegacyUnscoped]);
    let mut rows = db.scan(..).await?;
    while let Some(row) = rows.next().await? {
        if let Some(scope) = graph_property_scope(&row.key) {
            scopes.insert(scope);
        }
    }
    Ok(scopes)
}

fn graph_property_scope(key: &[u8]) -> Option<DataScope> {
    if let Ok(DataKey::Data {
        kind: DataKeyKind::NodeProperty(_) | DataKeyKind::EdgePropertyById(_),
        ..
    }) = DataKey::parse_from_slice(DataScope::LegacyUnscoped, key)
    {
        return Some(DataScope::LegacyUnscoped);
    }
    let (tenant, _) = DataScope::strip_tenant_envelope(key)?;
    let scope = DataScope::Tenant(tenant);
    match DataKey::parse_from_slice(scope, key).ok()? {
        DataKey::Data {
            kind: DataKeyKind::NodeProperty(_) | DataKeyKind::EdgePropertyById(_),
            ..
        } => Some(scope),
        DataKey::Data { .. } | DataKey::Global { .. } => None,
    }
}

async fn rewrite_scope_labels(db: &Db, scope: DataScope) -> Result<()> {
    rewrite_node_labels(db, scope).await?;
    rewrite_edge_labels(db, scope).await
}

async fn rewrite_node_labels(db: &Db, scope: DataScope) -> Result<()> {
    let mut rows = db
        .scan_prefix(
            crate::index_lifecycle::secondary::source_prefix(scope, IndexElementKind::Node),
            ..,
        )
        .await?;
    loop {
        let mut batch = Vec::new();
        while batch.len() < MIGRATION_BATCH_SIZE {
            let Some(row) = rows.next().await? else {
                break;
            };
            let Some(node_id) = crate::index_lifecycle::secondary::source_entity(
                scope,
                IndexElementKind::Node,
                &row.key,
            )?
            else {
                continue;
            };
            let properties = decode_properties(&row.value)?;
            let Some(label) = graph_label(&properties) else {
                continue;
            };
            batch.push((node_id.get(), label.to_string()));
        }
        if batch.is_empty() {
            break;
        }
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        for (node_id, label) in batch {
            search::add_to_equality_index_scoped(
                &transaction,
                NODE_LABEL_PROPERTY,
                &label,
                node_id,
                scope,
            )
            .await?;
        }
        transaction.commit().await?;
    }
    Ok(())
}

async fn rewrite_edge_labels(db: &Db, scope: DataScope) -> Result<()> {
    let mut rows = db
        .scan_prefix(
            crate::index_lifecycle::secondary::source_prefix(scope, IndexElementKind::Edge),
            ..,
        )
        .await?;
    loop {
        let mut batch = Vec::new();
        while batch.len() < MIGRATION_BATCH_SIZE {
            let Some(row) = rows.next().await? else {
                break;
            };
            let Some(edge_id) = crate::index_lifecycle::secondary::source_entity(
                scope,
                IndexElementKind::Edge,
                &row.key,
            )?
            else {
                continue;
            };
            let properties = decode_properties(&row.value)?;
            let Some(label) = graph_label(&properties) else {
                continue;
            };
            let Some((from, to)) =
                search::get_edge_endpoints_scoped(db, edge_id.get(), scope).await?
            else {
                return Err(corruption(
                    "canonical label migration found an edge property row without endpoints",
                ));
            };
            batch.push((edge_id.get(), from, to, label.to_string()));
        }
        if batch.is_empty() {
            break;
        }
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        for (edge_id, from, to, label) in batch {
            search::add_to_edge_label_index_scoped(&transaction, from, to, &label, scope).await?;
            search::add_to_global_edge_label_index_scoped(&transaction, &label, edge_id, scope)
                .await?;
        }
        transaction.commit().await?;
    }
    Ok(())
}

async fn verify_scope_labels(db: &Db, scope: DataScope) -> Result<()> {
    verify_node_labels(db, scope).await?;
    verify_edge_labels(db, scope).await
}

async fn verify_node_labels(db: &Db, scope: DataScope) -> Result<()> {
    let mut rows = db
        .scan_prefix(
            crate::index_lifecycle::secondary::source_prefix(scope, IndexElementKind::Node),
            ..,
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let Some(node_id) = crate::index_lifecycle::secondary::source_entity(
            scope,
            IndexElementKind::Node,
            &row.key,
        )?
        else {
            continue;
        };
        let properties = decode_properties(&row.value)?;
        let Some(label) = graph_label(&properties) else {
            continue;
        };
        let ids =
            search::lookup_equality_index_set_scoped(db, NODE_LABEL_PROPERTY, label, scope).await?;
        if !ids.contains(node_id.get()) {
            return Err(corruption(
                "canonical label migration missing a node after rewrite",
            ));
        }
    }
    Ok(())
}

async fn verify_edge_labels(db: &Db, scope: DataScope) -> Result<()> {
    let mut rows = db
        .scan_prefix(
            crate::index_lifecycle::secondary::source_prefix(scope, IndexElementKind::Edge),
            ..,
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let Some(edge_id) = crate::index_lifecycle::secondary::source_entity(
            scope,
            IndexElementKind::Edge,
            &row.key,
        )?
        else {
            continue;
        };
        let properties = decode_properties(&row.value)?;
        let Some(label) = graph_label(&properties) else {
            continue;
        };
        let Some((from, to)) = search::get_edge_endpoints_scoped(db, edge_id.get(), scope).await?
        else {
            return Err(corruption(
                "canonical label migration found an edge property row without endpoints",
            ));
        };
        if !search::lookup_global_edge_label_index_scoped(db, label, scope)
            .await?
            .contains(edge_id.get())
        {
            return Err(corruption(
                "canonical label migration missing a global edge label after rewrite",
            ));
        }
        if !search::lookup_out_neighbors_by_label_scoped(db, from, label, scope)
            .await?
            .contains(to)
        {
            return Err(corruption(
                "canonical label migration missing an outgoing neighbor after rewrite",
            ));
        }
        if !search::lookup_in_neighbors_by_label_scoped(db, to, label, scope)
            .await?
            .contains(from)
        {
            return Err(corruption(
                "canonical label migration missing an incoming neighbor after rewrite",
            ));
        }
    }
    Ok(())
}

async fn delete_matching_keys(
    db: &Db,
    prefix: Bytes,
    should_delete: impl Fn(&[u8]) -> bool,
) -> Result<()> {
    loop {
        let transaction = db.begin(IsolationLevel::SerializableSnapshot).await?;
        let mut rows = transaction.scan_prefix(&prefix, ..).await?;
        let mut keys = Vec::with_capacity(MIGRATION_BATCH_SIZE);
        let mut scanned = 0;
        while scanned < MIGRATION_BATCH_SIZE {
            let Some(row) = rows.next().await? else {
                break;
            };
            scanned += 1;
            if should_delete(&row.key) {
                keys.push(row.key);
            }
        }
        drop(rows);
        if keys.is_empty() {
            transaction.rollback();
            if scanned < MIGRATION_BATCH_SIZE {
                return Ok(());
            }
            continue;
        }
        for key in keys {
            transaction.delete(key)?;
        }
        transaction.commit().await?;
        if scanned < MIGRATION_BATCH_SIZE {
            return Ok(());
        }
    }
}

fn hash_only_node_label_prefix(scope: DataScope) -> Bytes {
    DataKey::data_prefix(
        scope,
        EqualityScanPrefix::Property {
            property_hash: hash_property_name(NODE_LABEL_PROPERTY),
        }
        .to_bytes(),
    )
}

fn edge_label_prefix(scope: DataScope) -> Bytes {
    DataKey::data_prefix(scope, EdgeLabelScanPrefix::Index.to_bytes())
}

fn edge_label_neighbor_prefix(scope: DataScope) -> Bytes {
    DataKey::data_prefix(scope, EdgeLabelNeighborScanPrefix::Index.to_bytes())
}

fn is_hash_only_node_label_key(scope: DataScope, key: &[u8]) -> bool {
    let Some(logical) = scope.strip_key(key) else {
        return false;
    };
    const HASH_ONLY_NODE_LABEL_LEN: usize =
        PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN;
    if logical.len() != HASH_ONLY_NODE_LABEL_LEN
        || logical[0] != KeyPrefix::PropertyIndex.as_u8()
        || logical[PREFIX_LEN] != IndexPrefix::Equality.as_slice()[0]
    {
        return false;
    }
    logical[PREFIX_LEN + INDEX_PREFIX_LEN..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN]
        == hash_property_name(NODE_LABEL_PROPERTY)
}

fn is_hash_only_edge_label_key(scope: DataScope, key: &[u8]) -> bool {
    let Some(logical) = scope.strip_key(key) else {
        return false;
    };
    logical.len() == PREFIX_LEN + INDEX_PREFIX_LEN + VALUE_HASH_MAX_LEN
}

fn is_hash_only_edge_neighbor_key(scope: DataScope, key: &[u8]) -> bool {
    let Some(logical) = scope.strip_key(key) else {
        return false;
    };
    logical.len()
        == PREFIX_LEN
            + INDEX_PREFIX_LEN
            + core::mem::size_of::<EdgeDirection>()
            + NODE_ID_MAX_LEN
            + VALUE_HASH_MAX_LEN
}

fn graph_label(properties: &[Property]) -> Option<&str> {
    properties
        .iter()
        .find(|property| property.name == NODE_LABEL_PROPERTY)
        .and_then(|property| property.value.as_str())
}

#[cfg(test)]
pub(crate) async fn published_storage_version(db: &Db) -> Result<IndexStorageVersion> {
    let marker = db
        .get(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
        )
        .await?
        .ok_or_else(|| corruption("canonical label tests require a storage marker"))?;
    let IndexV2MetadataValue::StorageVersion(version) =
        crate::encoding::v2::values::decode_metadata_value(&marker)?
    else {
        return Err(corruption(
            "canonical label tests found a non-version storage marker",
        ));
    };
    Ok(version)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use roaring::RoaringTreemap;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;

    use super::*;
    use crate::encoding::indexes::equality::EqualityIndexKey;
    use crate::encoding::indexes::{hash_property_value, IndexPrefix, PropertyIndexKey};
    use crate::encoding::keys::KeyPrefix;
    use crate::encoding::property::{encode_properties, Property};
    use crate::encoding::v2::keys::scope::TenantId;
    use crate::encoding::v2::keys::{EdgePropertyByIdKey, NodePropertyKey};
    use crate::encoding::v2::values::indexes::SecondaryEqualityValue;
    use crate::error::WriterMigrationRequirement;
    use crate::index_lifecycle::{
        IndexId, IndexStorageVersion, LogicalIndexIdWatermark, VectorPhysicalIdWatermark,
        VectorPhysicalIndexId,
    };
    use crate::migrations::startup::bootstrap_writer;

    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn fixture_db(name: &str, store: Arc<dyn ObjectStore>, version: u16) -> Db {
        let db = Db::builder(name, store)
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .unwrap();
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::StorageVersion,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
                IndexStorageVersion::new(version).unwrap(),
            )),
        )
        .await
        .unwrap();
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::LogicalIndexIdWatermark,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::LogicalIndexIdWatermark(
                LogicalIndexIdWatermark {
                    next_id: IndexId::initial(),
                },
            )),
        )
        .await
        .unwrap();
        db.put(
            ManagedIndexKey::Global {
                kind: GlobalKey::VectorPhysicalIdWatermark,
            }
            .to_bytes(),
            encode_metadata_value(&IndexV2MetadataValue::VectorPhysicalIdWatermark(
                VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::initial(),
                },
            )),
        )
        .await
        .unwrap();
        let transaction = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        crate::migrations::stage_reader_compatible_storage_schema_for_tests(&transaction).unwrap();
        crate::migrations::stage_index_storage_v4_cleanup_ready(&transaction).unwrap();
        transaction.commit().await.unwrap();
        db
    }

    fn hash_only_node_label_key(scope: DataScope, label: &str) -> Bytes {
        DataKey::Data {
            scope,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
                hash_property_name(NODE_LABEL_PROPERTY),
                hash_property_value(label),
            ))),
        }
        .to_bytes()
    }

    fn hash_only_edge_label_key(scope: DataScope, label: &str) -> Bytes {
        let mut logical = Vec::with_capacity(PREFIX_LEN + INDEX_PREFIX_LEN + VALUE_HASH_MAX_LEN);
        logical.push(KeyPrefix::PropertyIndex.as_u8());
        logical.extend_from_slice(IndexPrefix::EdgeLabel.as_slice());
        logical.extend_from_slice(&hash_property_value(label));
        DataKey::data_prefix(scope, Bytes::from(logical))
    }

    fn hash_only_neighbor_key(
        scope: DataScope,
        direction: EdgeDirection,
        node: u64,
        label: &str,
    ) -> Bytes {
        let mut logical = Vec::with_capacity(
            PREFIX_LEN
                + INDEX_PREFIX_LEN
                + core::mem::size_of::<EdgeDirection>()
                + NODE_ID_MAX_LEN
                + VALUE_HASH_MAX_LEN,
        );
        logical.push(KeyPrefix::PropertyIndex.as_u8());
        logical.extend_from_slice(IndexPrefix::EdgeLabelNeighbor(direction).as_slice());
        logical.extend_from_slice(&node.to_be_bytes());
        logical.extend_from_slice(&hash_property_value(label));
        DataKey::data_prefix(scope, Bytes::from(logical))
    }

    async fn put_labeled_node(db: &Db, scope: DataScope, node_id: u64, label: &str) {
        db.put(
            DataKey::Data {
                scope,
                kind: DataKeyKind::NodeProperty(NodePropertyKey::new(node_id)),
            }
            .to_bytes(),
            encode_properties(&[Property::string(NODE_LABEL_PROPERTY, label)]),
        )
        .await
        .unwrap();
        db.put(
            hash_only_node_label_key(scope, label),
            SecondaryEqualityValue::encode_ids(&RoaringTreemap::from_iter([node_id])),
        )
        .await
        .unwrap();
    }

    async fn put_labeled_edge(
        db: &Db,
        scope: DataScope,
        edge_id: u64,
        from: u64,
        to: u64,
        label: &str,
    ) {
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                DataKey::Data {
                    scope,
                    kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
                }
                .to_bytes(),
                encode_properties(&[Property::string(NODE_LABEL_PROPERTY, label)]),
            )
            .unwrap();
        search::store_edge_endpoints_scoped(&transaction, edge_id, from, to, scope)
            .await
            .unwrap();
        transaction
            .put(
                hash_only_edge_label_key(scope, label),
                SecondaryEqualityValue::encode_ids(&RoaringTreemap::from_iter([edge_id])),
            )
            .unwrap();
        transaction
            .put(
                hash_only_neighbor_key(scope, EdgeDirection::Out, from, label),
                SecondaryEqualityValue::encode_ids(&RoaringTreemap::from_iter([to])),
            )
            .unwrap();
        transaction
            .put(
                hash_only_neighbor_key(scope, EdgeDirection::In, to, label),
                SecondaryEqualityValue::encode_ids(&RoaringTreemap::from_iter([from])),
            )
            .unwrap();
        transaction.commit().await.unwrap();
    }

    async fn assert_canonical_lookups(db: &Db, scope: DataScope) {
        assert_eq!(
            search::lookup_equality_index_scoped(db, NODE_LABEL_PROPERTY, "User", scope)
                .await
                .unwrap(),
            vec![11]
        );
        assert!(
            search::lookup_global_edge_label_index_scoped(db, "FOLLOWS", scope)
                .await
                .unwrap()
                .contains(99)
        );
        assert!(
            search::lookup_out_neighbors_by_label_scoped(db, 11, "FOLLOWS", scope)
                .await
                .unwrap()
                .contains(17)
        );
        assert!(db
            .get(&hash_only_node_label_key(scope, "User"))
            .await
            .unwrap()
            .is_none());
        assert!(db
            .get(&hash_only_edge_label_key(scope, "FOLLOWS"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn reader_refuses_hash_only_v4_until_writer_rewrites() {
        let _guard = TEST_LOCK.lock().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = "canonical-label-reader-refuses-v4";
        let db = fixture_db(database, Arc::clone(&store), 0x0004).await;
        put_labeled_node(&db, DataScope::LegacyUnscoped, 11, "User").await;
        put_labeled_edge(&db, DataScope::LegacyUnscoped, 99, 11, 17, "FOLLOWS").await;
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let Err(error) =
            crate::HelixDB::open_reader_with_object_store_for_tests(database, Arc::clone(&store))
                .await
        else {
            panic!("hash-only V4 storage is not reader-current");
        };
        assert!(matches!(
            error,
            HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::StorageVersion {
                    found: 0x0004,
                    target: 0x0006,
                },
            }
        ));

        let opened = crate::HelixDB::open_with_object_store(database, store)
            .await
            .expect("writer bootstrap rewrites hash-only labels");
        let storage = opened.inner_db();
        assert_eq!(
            published_storage_version(storage.as_ref()).await.unwrap(),
            IndexStorageVersion::CURRENT
        );
        assert_canonical_lookups(storage.as_ref(), DataScope::LegacyUnscoped).await;
        opened.close().await.unwrap();
    }

    #[tokio::test]
    async fn writer_rewrites_hash_only_labels_in_every_scope() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = "canonical-label-rewrite-v5-scopes";
        let tenant =
            DataScope::Tenant(TenantId::from_ulid_str("01KZ6WZ9QREKZZ87492YXBTFJ3").unwrap());
        let db = fixture_db(database, Arc::clone(&store), 0x0004).await;
        put_labeled_node(&db, DataScope::LegacyUnscoped, 11, "User").await;
        put_labeled_edge(&db, DataScope::LegacyUnscoped, 99, 11, 17, "FOLLOWS").await;
        put_labeled_node(&db, tenant, 11, "User").await;
        put_labeled_edge(&db, tenant, 99, 11, 17, "FOLLOWS").await;
        db.flush().await.unwrap();

        bootstrap_writer(&db)
            .await
            .expect("writer bootstrap rewrites hash-only labels in every scope");
        assert_eq!(
            published_storage_version(&db).await.unwrap(),
            IndexStorageVersion::CURRENT
        );
        assert_canonical_lookups(&db, DataScope::LegacyUnscoped).await;
        assert_canonical_lookups(&db, tenant).await;
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn publication_crash_leaves_previous_marker_and_resumes() {
        let _guard = TEST_LOCK.lock().await;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let database = "canonical-label-rewrite-resumes";
        let db = fixture_db(database, Arc::clone(&store), 0x0004).await;
        put_labeled_node(&db, DataScope::LegacyUnscoped, 11, "User").await;
        put_labeled_edge(&db, DataScope::LegacyUnscoped, 99, 11, 17, "FOLLOWS").await;
        db.flush().await.unwrap();

        inject_once(CanonicalLabelMigrationFailpoint::PublicationBefore).unwrap();
        assert!(bootstrap_writer(&db).await.is_err());
        assert!(was_triggered());
        assert_eq!(published_storage_version(&db).await.unwrap().get(), 0x0004);
        db.flush().await.unwrap();
        db.close().await.unwrap();

        let opened = crate::HelixDB::open_with_object_store(database, store)
            .await
            .expect("writer restart finishes the canonical-label rewrite");
        let storage = opened.inner_db();
        assert_eq!(
            published_storage_version(storage.as_ref()).await.unwrap(),
            IndexStorageVersion::CURRENT
        );
        assert_canonical_lookups(storage.as_ref(), DataScope::LegacyUnscoped).await;
        opened.close().await.unwrap();
    }

    #[tokio::test]
    async fn fresh_writer_bootstrap_publishes_canonical_label_version() {
        let db = Db::builder("canonical-label-fresh-current", Arc::new(InMemory::new()))
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .unwrap();
        bootstrap_writer(&db).await.unwrap();
        assert_eq!(
            published_storage_version(&db).await.unwrap(),
            IndexStorageVersion::CURRENT
        );
        assert_eq!(IndexStorageVersion::CURRENT.get(), 0x0006);
        db.close().await.unwrap();
    }
}
