//! Property index management
//!
//! This module provides secondary indexes for fast property-based queries:
//! - Equality indexes: Fast lookup of vertices by property=value
//! - Range indexes: Fast range scans on property values
//! - Vector indexes: ANN (Approximate Nearest Neighbor) search on vector properties
//! - Text indexes: BM25 full-text search on string properties
//!
//! # Index Storage
//!
//! ## Equality Index
//! Key: `[0x03][0x00][prop_hash:4][value_hash:8]`
//! Value: RoaringTreemap of NodeIds
//!
//! ## Range Index
//! Key: `[0x03][0x01][prop_hash:4][value:var][node_id:8]`
//! Value: empty (presence = membership)
//!
//! ## Vector Index
//! See the `hnsw` module for details on vector index storage layout

pub mod text;
pub mod vector;

use std::borrow::Cow;
#[cfg(feature = "migration-parity")]
use std::collections::BTreeMap;

use bytes::Bytes;
use roaring::RoaringTreemap;
use slatedb::DbTransaction;

use crate::config::{TextElementType, VectorElementType};
use crate::encoding::indexes::equality::{
    scans::{EdgeEqualityScanPrefix, EqualityScanPrefix, GlobalEdgeEqualityScanPrefix},
    EdgeDirection as EdgeEqualityDirection, EdgeEqualityIndexKey, EqualityIndexKey,
};
use crate::encoding::indexes::label::{EdgeLabelKey, EdgeLabelNeighborKey, EdgeLabelScanPrefix};
use crate::encoding::indexes::prefix::exclusive_prefix_end_bound;
use crate::encoding::indexes::range::{
    scans::{
        EdgeRangeScanPrefix, EdgeRangeScanValuePrefix, GlobalEdgeRangeScanPrefix,
        GlobalEdgeRangeScanValuePrefix, RangeScanPrefix, RangeScanValuePrefix,
    },
    EdgeRangeIndexDirection, EdgeRangeIndexKey, GlobalEdgeRangeIndexKey, RangeIndexDirection,
    RangeIndexKey,
};
use crate::encoding::indexes::{
    hash_property_name, hash_property_value, EdgeDirection as EdgeRangeDirection,
};
use crate::encoding::keys::scope::DataScope;
use crate::encoding::keys::{EdgeEndpointsKey, EdgePropertyByIdKey};
use crate::encoding::property::property::Property;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{decode_properties, encode_properties};
use crate::encoding::v2::keys::indexes::PropertyIndexKey;
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::legacy::text::storage_keys::{
    self as key_metadata, LegacyTextMetadataElement as TextMetadataElement,
};
use crate::encoding::v2::values::edge_endpoints::EdgeEndpointsValue;
use crate::encoding::v2::values::indexes::SecondaryEqualityValue;
use crate::encoding::{EdgeId, NodeId};
use crate::error::HelixDbError;
use slatedb::DbReadOps;

fn bounded_prefix_end(prefix: &Bytes) -> Bytes {
    exclusive_prefix_end_bound(prefix)
        .expect("typed index prefixes always have a finite lexicographic successor")
}

/// Build a deterministic vector index name from element type, label, and property.
///
/// This keeps index names compact by hashing label and property values.
pub fn vector_index_name(element_type: VectorElementType, label: &str, property: &str) -> String {
    let label_hash = hash_index_component(label);
    let prop_hash = hash_index_component(property);
    let prefix = match element_type {
        VectorElementType::Node => 'n',
        VectorElementType::Edge => 'e',
    };
    format!("vec:{}:{:016x}:{:016x}", prefix, label_hash, prop_hash)
}

/// Build a deterministic text index name from element type, label, and property.
pub fn text_index_name(element_type: TextElementType, label: &str, property: &str) -> String {
    let label_hash = hash_index_component(label);
    let prop_hash = hash_index_component(property);
    let prefix = match element_type {
        TextElementType::Node => 'n',
        TextElementType::Edge => 'e',
    };
    format!("fts:{}:{:016x}:{:016x}", prefix, label_hash, prop_hash)
}

/// Build a deterministic multitenant vector index name.
pub fn vector_tenant_index_name(
    element_type: VectorElementType,
    label: &str,
    property: &str,
    tenant_property: &str,
    tenant_value: &PropertyValue,
) -> String {
    let label_hash = hash_index_component(label);
    let prop_hash = hash_index_component(property);
    let tenant_prop_hash = hash_index_component(tenant_property);
    let tenant_value_hash = hash_property_value_component(tenant_value);
    let prefix = match element_type {
        VectorElementType::Node => 'n',
        VectorElementType::Edge => 'e',
    };
    format!(
        "vecmt:{}:{:016x}:{:016x}:{:016x}:{:016x}",
        prefix, label_hash, prop_hash, tenant_prop_hash, tenant_value_hash
    )
}

/// Build the deterministic prefix shared by all tenant partitions for a vector definition.
pub fn vector_tenant_index_name_prefix(
    element_type: VectorElementType,
    label: &str,
    property: &str,
    tenant_property: &str,
) -> String {
    let label_hash = hash_index_component(label);
    let prop_hash = hash_index_component(property);
    let tenant_prop_hash = hash_index_component(tenant_property);
    let prefix = match element_type {
        VectorElementType::Node => 'n',
        VectorElementType::Edge => 'e',
    };
    format!(
        "vecmt:{}:{:016x}:{:016x}:{:016x}:",
        prefix, label_hash, prop_hash, tenant_prop_hash
    )
}

/// Build a deterministic multitenant text index name.
pub fn text_tenant_index_name(
    element_type: TextElementType,
    label: &str,
    property: &str,
    tenant_property: &str,
    tenant_value: &PropertyValue,
) -> String {
    let label_hash = hash_index_component(label);
    let prop_hash = hash_index_component(property);
    let tenant_prop_hash = hash_index_component(tenant_property);
    let tenant_value_hash = hash_property_value_component(tenant_value);
    let prefix = match element_type {
        TextElementType::Node => 'n',
        TextElementType::Edge => 'e',
    };
    format!(
        "ftsmt:{}:{:016x}:{:016x}:{:016x}:{:016x}",
        prefix, label_hash, prop_hash, tenant_prop_hash, tenant_value_hash
    )
}

/// Build the deterministic prefix shared by all tenant partitions for a text definition.
///
/// V2 recovery and cleanup use this prefix to prove that a manifest belongs to
/// the catalog definition before retaining its exact full physical name. The
/// final tenant-value hash remains part of the current manifest name and is not
/// reproduced or rewritten by lifecycle records.
pub fn text_tenant_index_name_prefix(
    element_type: TextElementType,
    label: &str,
    property: &str,
    tenant_property: &str,
) -> String {
    let label_hash = hash_index_component(label);
    let prop_hash = hash_index_component(property);
    let tenant_prop_hash = hash_index_component(tenant_property);
    let prefix = match element_type {
        TextElementType::Node => 'n',
        TextElementType::Edge => 'e',
    };
    format!(
        "ftsmt:{}:{:016x}:{:016x}:{:016x}:",
        prefix, label_hash, prop_hash, tenant_prop_hash
    )
}

/// Build the metadata key holding the latest committed text manifest for a physical index.
pub fn make_text_index_manifest_key(index_name: &str) -> Bytes {
    make_text_index_manifest_key_scoped(DataScope::LegacyUnscoped, index_name)
}

pub fn make_text_index_manifest_key_scoped(scope: DataScope, index_name: &str) -> Bytes {
    key_metadata::manifest_key(scope, index_name)
}

/// Build the metadata prefix used for scanning all text manifest rows.
pub fn make_text_index_manifest_scan_prefix() -> Bytes {
    make_text_index_manifest_scan_prefix_scoped(DataScope::LegacyUnscoped)
}

pub fn make_text_index_manifest_scan_prefix_scoped(scope: DataScope) -> Bytes {
    key_metadata::manifest_scan_prefix(scope)
}

/// Build a metadata prefix for scanning text manifest rows for one logical definition.
pub fn make_text_index_manifest_prefix(
    element_type: TextElementType,
    label: &str,
    property: &str,
    tenant_scoped: bool,
) -> Bytes {
    make_text_index_manifest_prefix_scoped(
        DataScope::LegacyUnscoped,
        element_type,
        label,
        property,
        tenant_scoped,
    )
}

pub fn make_text_index_manifest_prefix_scoped(
    scope: DataScope,
    element_type: TextElementType,
    label: &str,
    property: &str,
    tenant_scoped: bool,
) -> Bytes {
    let label_hash = hash_index_component(label);
    let prop_hash = hash_index_component(property);
    key_metadata::manifest_prefix(
        scope,
        text_metadata_element(element_type),
        label_hash,
        prop_hash,
        tenant_scoped,
    )
}

/// Build the SSI guard key for a physical text index partition.
pub fn make_text_index_txn_guard_key(index_name: &str) -> Bytes {
    make_text_index_txn_guard_key_scoped(DataScope::LegacyUnscoped, index_name)
}

pub fn make_text_index_txn_guard_key_scoped(scope: DataScope, index_name: &str) -> Bytes {
    key_metadata::transaction_guard_key(scope, index_name)
}

/// Build the SSI guard key for a logical text index definition.
pub fn make_text_definition_guard_key(
    element_type: TextElementType,
    label: &str,
    property: &str,
) -> Bytes {
    make_text_definition_guard_key_scoped(DataScope::LegacyUnscoped, element_type, label, property)
}

pub fn make_text_definition_guard_key_scoped(
    scope: DataScope,
    element_type: TextElementType,
    label: &str,
    property: &str,
) -> Bytes {
    let label_hash = hash_index_component(label);
    let prop_hash = hash_index_component(property);
    key_metadata::definition_guard_key(
        scope,
        text_metadata_element(element_type),
        label_hash,
        prop_hash,
    )
}

/// Build the metadata key storing the live-state row for one indexed entity.
pub fn make_text_index_live_state_key(index_name: &str, entity_id: u64) -> Bytes {
    make_text_index_live_state_key_scoped(DataScope::LegacyUnscoped, index_name, entity_id)
}

pub fn make_text_index_live_state_key_scoped(
    scope: DataScope,
    index_name: &str,
    entity_id: u64,
) -> Bytes {
    key_metadata::live_state_key(scope, index_name, entity_id)
}

/// Build the metadata prefix for all live-state rows of one physical text index.
pub fn make_text_index_live_state_prefix(index_name: &str) -> Bytes {
    make_text_index_live_state_prefix_scoped(DataScope::LegacyUnscoped, index_name)
}

pub fn make_text_index_live_state_prefix_scoped(scope: DataScope, index_name: &str) -> Bytes {
    key_metadata::live_state_prefix(scope, index_name)
}

/// Build the metadata key holding the latest logical version counter for a physical text index.
pub fn make_text_index_version_counter_key(index_name: &str) -> Bytes {
    make_text_index_version_counter_key_scoped(DataScope::LegacyUnscoped, index_name)
}

pub fn make_text_index_version_counter_key_scoped(scope: DataScope, index_name: &str) -> Bytes {
    key_metadata::version_counter_key(scope, index_name)
}

const fn text_metadata_element(element_type: TextElementType) -> TextMetadataElement {
    match element_type {
        TextElementType::Node => TextMetadataElement::Node,
        TextElementType::Edge => TextMetadataElement::Edge,
    }
}

pub(crate) fn hash_index_component(value: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    // Persisted index names are a storage-format contract. `DefaultHasher`
    // currently delegates to SipHash-1-3 with zero keys, but the standard
    // library does not promise that implementation forever. Pin the algorithm
    // and keys explicitly so compiler upgrades cannot silently rename indexes.
    let mut hasher = siphasher::sip::SipHasher13::new_with_keys(0, 0);
    value.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn hash_property_value_component(value: &PropertyValue) -> u64 {
    use std::hash::{Hash, Hasher};

    // This value is embedded in persisted tenant-scoped text/vector index
    // names. Keep it byte-for-byte compatible with the legacy DefaultHasher
    // output while making the algorithm an explicit storage-format contract.
    let mut hasher = siphasher::sip::SipHasher13::new_with_keys(0, 0);
    match value {
        PropertyValue::Null => {
            0u8.hash(&mut hasher);
        }
        PropertyValue::Bool(v) => {
            1u8.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        PropertyValue::I64(v) => {
            2u8.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        PropertyValue::DateTime(v) => {
            11u8.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        PropertyValue::F64(v) => {
            3u8.hash(&mut hasher);
            v.to_bits().hash(&mut hasher);
        }
        PropertyValue::F32(v) => {
            4u8.hash(&mut hasher);
            v.to_bits().hash(&mut hasher);
        }
        PropertyValue::String(v) => {
            5u8.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        PropertyValue::Bytes(v) => {
            6u8.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        PropertyValue::I64Array(v) => {
            7u8.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        PropertyValue::F64Array(v) => {
            8u8.hash(&mut hasher);
            for item in v {
                item.to_bits().hash(&mut hasher);
            }
        }
        PropertyValue::F32Array(v) => {
            9u8.hash(&mut hasher);
            for item in v {
                item.to_bits().hash(&mut hasher);
            }
        }
        PropertyValue::StringArray(v) => {
            10u8.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        PropertyValue::Array(v) => {
            12u8.hash(&mut hasher);
            for item in v {
                hash_property_value_component(item).hash(&mut hasher);
            }
        }
        PropertyValue::Object(v) => {
            13u8.hash(&mut hasher);
            for (key, item) in v {
                key.hash(&mut hasher);
                hash_property_value_component(item).hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

/// Returns the persisted V1 hash-bearing graph keys used by the cross-repo
/// migration contract. This is deliberately feature-gated: it exposes storage
/// bytes to the parity harness without making them part of the runtime API.
#[cfg(feature = "migration-parity")]
pub fn migration_parity_graph_hash_contract(
    property: &str,
    value: &str,
    label: &str,
    source: NodeId,
    target: NodeId,
    edge_id: EdgeId,
) -> BTreeMap<String, Vec<u8>> {
    let property_hash = hash_property_name(property);
    let value_hash = hash_property_value(value);
    let label_hash = hash_property_value(label);
    let mut rows = BTreeMap::new();
    rows.insert("property_name_hash".to_string(), property_hash.to_vec());
    rows.insert("property_value_hash".to_string(), value_hash.to_vec());
    rows.insert(
        "node_equality_key".to_string(),
        DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
                property_hash,
                value_hash,
            ))),
        }
        .to_bytes()
        .to_vec(),
    );
    rows.insert(
        "node_equality_property_prefix".to_string(),
        EqualityScanPrefix::Property { property_hash }
            .to_bytes()
            .to_vec(),
    );
    for (name, direction) in [
        ("asc", RangeIndexDirection::Asc),
        ("desc", RangeIndexDirection::Desc),
    ] {
        rows.insert(
            format!("node_range_{name}_key"),
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
                    direction,
                    property_hash,
                    Cow::Borrowed(value),
                    source,
                ))),
            }
            .to_bytes()
            .to_vec(),
        );
        rows.insert(
            format!("node_range_{name}_property_prefix"),
            RangeScanPrefix::Property {
                direction,
                property_hash,
            }
            .to_bytes()
            .to_vec(),
        );
        rows.insert(
            format!("node_range_{name}_value_prefix"),
            RangeScanValuePrefix::new(direction, property_hash, value)
                .to_bytes()
                .to_vec(),
        );
    }
    for (name, direction, endpoint) in [
        ("out", EdgeEqualityDirection::Out, source),
        ("in", EdgeEqualityDirection::In, target),
    ] {
        rows.insert(
            format!("edge_equality_{name}_key"),
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeEquality(
                    EdgeEqualityIndexKey::new(direction, endpoint, property_hash, value_hash),
                )),
            }
            .to_bytes()
            .to_vec(),
        );
    }
    rows.insert(
        "global_edge_label_key".to_string(),
        DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(
                label_hash,
            ))),
        }
        .to_bytes()
        .to_vec(),
    );
    for (name, direction, endpoint) in [
        ("out", EdgeRangeDirection::Out, source),
        ("in", EdgeRangeDirection::In, target),
    ] {
        rows.insert(
            format!("edge_label_{name}_key"),
            DataKey::Data {
                scope: DataScope::LegacyUnscoped,
                kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
                    EdgeLabelNeighborKey::new(direction, endpoint, label_hash),
                )),
            }
            .to_bytes()
            .to_vec(),
        );
        for (range_name, range_direction) in [
            ("asc", RangeIndexDirection::Asc),
            ("desc", RangeIndexDirection::Desc),
        ] {
            rows.insert(
                format!("edge_range_{name}_{range_name}_key"),
                DataKey::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: edge_range_key_with_direction(
                        direction,
                        endpoint,
                        property,
                        value,
                        edge_id,
                        range_direction,
                    ),
                }
                .to_bytes()
                .to_vec(),
            );
            rows.insert(
                format!("edge_range_{name}_{range_name}_property_prefix"),
                edge_range_prefix_with_direction(direction, endpoint, property, range_direction)
                    .to_vec(),
            );
            rows.insert(
                format!("edge_range_{name}_{range_name}_value_prefix"),
                edge_range_value_prefix_with_direction(
                    direction,
                    endpoint,
                    property,
                    value,
                    range_direction,
                )
                .to_vec(),
            );
        }
    }
    rows
}

#[cfg(feature = "migration-parity")]
pub fn migration_parity_hash_index_component(value: &str) -> u64 {
    hash_index_component(value)
}

#[cfg(feature = "migration-parity")]
pub fn migration_parity_hash_property_value_component(value: &PropertyValue) -> u64 {
    hash_property_value_component(value)
}

#[cfg(test)]
pub(crate) fn property_value_is_secondary_indexable(value: &PropertyValue) -> bool {
    !matches!(value, PropertyValue::Array(_) | PropertyValue::Object(_))
}

/// Convert a PropertyValue to a string for indexing
#[cfg(test)]
pub fn property_value_to_index_string(value: &PropertyValue) -> String {
    match value {
        PropertyValue::Null => "null".to_string(),
        PropertyValue::Bool(b) => b.to_string(),
        PropertyValue::I64(n) => format!("{:020}", n),
        PropertyValue::DateTime(n) => PropertyValue::DateTime(*n).to_index_string(),
        PropertyValue::F64(n) => format!("{:+024.15e}", n),
        PropertyValue::F32(n) => format!("{:+024.15e}", n),
        PropertyValue::String(s) => s.clone(),
        PropertyValue::Bytes(b) => format!("<bytes:{:?}>", b),
        PropertyValue::I64Array(a) => format!("<i64[{}]>", a.len()),
        PropertyValue::F64Array(a) => format!("<f64[{}]>", a.len()),
        PropertyValue::StringArray(a) => format!("<str[{}]>", a.len()),
        PropertyValue::F32Array(items) => format!("<f32[{}]>", items.len()),
        PropertyValue::Array(items) => format!("<array[{}]>", items.len()),
        PropertyValue::Object(items) => format!("<object[{}]>", items.len()),
    }
}

#[cfg(test)]
fn property_value_type_name(value: &PropertyValue) -> &'static str {
    match value {
        PropertyValue::Null => "Null",
        PropertyValue::Bool(_) => "Bool",
        PropertyValue::I64(_) => "I64",
        PropertyValue::DateTime(_) => "DateTime",
        PropertyValue::F64(_) => "F64",
        PropertyValue::F32(_) => "F32",
        PropertyValue::String(_) => "String",
        PropertyValue::Bytes(_) => "Bytes",
        PropertyValue::I64Array(_) => "I64Array",
        PropertyValue::F64Array(_) => "F64Array",
        PropertyValue::F32Array(_) => "F32Array",
        PropertyValue::StringArray(_) => "StringArray",
        PropertyValue::Array(_) => "Array",
        PropertyValue::Object(_) => "Object",
    }
}

/// Encode a RoaringTreemap to bytes
pub fn encode_roaring_treemap(bitmap: &RoaringTreemap) -> Bytes {
    SecondaryEqualityValue::encode_ids(bitmap)
}

/// Decode a RoaringTreemap from bytes
pub fn decode_roaring_treemap(data: &[u8]) -> Result<RoaringTreemap, HelixDbError> {
    Ok(SecondaryEqualityValue::decode(data)?.into_ids())
}

/// Add a node to an equality index
pub async fn add_to_equality_index(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
) -> Result<(), HelixDbError> {
    add_to_equality_index_scoped(txn, property, value, node_id, DataScope::LegacyUnscoped).await
}

pub async fn add_to_equality_index_scoped(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
            hash_property_name(property),
            hash_property_value(value),
        ))),
    }
    .to_bytes();

    txn.merge_commutative(&key, crate::merge_operator::encode_bitmap_add(node_id))?;
    Ok(())
}

/// Remove a node from an equality index
pub async fn remove_from_equality_index(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
) -> Result<(), HelixDbError> {
    remove_from_equality_index_scoped(txn, property, value, node_id, DataScope::LegacyUnscoped)
        .await
}

pub async fn remove_from_equality_index_scoped(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
            hash_property_name(property),
            hash_property_value(value),
        ))),
    }
    .to_bytes();

    if let Some(data) = txn.get(&key).await? {
        let mut bitmap = decode_roaring_treemap(&data)?;
        bitmap.remove(node_id);

        if bitmap.is_empty() {
            txn.delete(&key)?;
        } else {
            txn.put(&key, encode_roaring_treemap(&bitmap))?;
        }
    }
    Ok(())
}

/// Look up nodes by equality index
pub async fn lookup_equality_index(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    value: &str,
) -> Result<Vec<NodeId>, HelixDbError> {
    lookup_equality_index_scoped(txn, property, value, DataScope::LegacyUnscoped).await
}

pub async fn lookup_equality_index_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    value: &str,
    tenant_scope: DataScope,
) -> Result<Vec<NodeId>, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
            hash_property_name(property),
            hash_property_value(value),
        ))),
    }
    .to_bytes();

    match txn.get(&key).await? {
        Some(data) => {
            let bitmap = decode_roaring_treemap(&data)?;
            Ok(bitmap.iter().collect())
        }
        None => Ok(Vec::new()),
    }
}

/// Look up nodes by equality index (RoaringTreemap)
pub async fn lookup_equality_index_set(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    value: &str,
) -> Result<RoaringTreemap, HelixDbError> {
    lookup_equality_index_set_scoped(txn, property, value, DataScope::LegacyUnscoped).await
}

pub async fn lookup_equality_index_set_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    value: &str,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
            hash_property_name(property),
            hash_property_value(value),
        ))),
    }
    .to_bytes();

    match txn.get(&key).await? {
        Some(data) => decode_roaring_treemap(&data),
        None => Ok(RoaringTreemap::new()),
    }
}

/// Scan all equality-index values for a property, returning up to `limit` nodes.
pub async fn scan_equality_index_property_prefix_limited(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    limit: usize,
) -> Result<RoaringTreemap, HelixDbError> {
    scan_equality_index_property_prefix_limited_filtered(txn, property, limit, None).await
}

/// Scan all equality-index values for a property, returning up to `limit` nodes.
///
/// If `filter` is provided, only nodes contained in the filter set are returned.
pub async fn scan_equality_index_property_prefix_limited_filtered(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    limit: usize,
    filter: Option<&RoaringTreemap>,
) -> Result<RoaringTreemap, HelixDbError> {
    scan_equality_index_property_prefix_limited_filtered_scoped(
        txn,
        property,
        limit,
        filter,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn scan_equality_index_property_prefix_limited_filtered_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    limit: usize,
    filter: Option<&RoaringTreemap>,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    if limit == 0 {
        return Ok(RoaringTreemap::new());
    }

    let prefix = EqualityScanPrefix::Property {
        property_hash: hash_property_name(property),
    }
    .to_bytes();
    let mut iter = txn
        .scan_prefix(DataKey::data_prefix(tenant_scope, prefix), ..)
        .await?;
    let mut results = RoaringTreemap::new();

    while let Some(kv) = iter.next().await? {
        let mut indexed = decode_roaring_treemap(&kv.value)?;
        if let Some(filter) = filter {
            indexed &= filter;
        }
        for node_id in indexed.iter() {
            results.insert(node_id);
            if results.len() as usize >= limit {
                return Ok(results);
            }
        }
    }

    Ok(results)
}

/// Add a node to a range index
pub async fn add_to_range_index(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
) -> Result<(), HelixDbError> {
    add_to_range_index_with_direction(txn, property, value, node_id, RangeIndexDirection::Asc).await
}

/// Add a node to a range index with explicit direction
pub async fn add_to_range_index_with_direction(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
    direction: RangeIndexDirection,
) -> Result<(), HelixDbError> {
    add_to_range_index_with_direction_scoped(
        txn,
        property,
        value,
        node_id,
        direction,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn add_to_range_index_with_direction_scoped(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
    direction: RangeIndexDirection,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            direction,
            hash_property_name(property),
            Cow::Borrowed(value),
            node_id,
        ))),
    }
    .to_bytes();
    txn.put(&key, Bytes::new())?; // Empty value - presence is membership
    Ok(())
}

/// Remove a node from a range index
pub async fn remove_from_range_index(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
) -> Result<(), HelixDbError> {
    remove_from_range_index_with_direction(txn, property, value, node_id, RangeIndexDirection::Asc)
        .await
}

/// Remove a node from a range index with explicit direction
pub async fn remove_from_range_index_with_direction(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
    direction: RangeIndexDirection,
) -> Result<(), HelixDbError> {
    remove_from_range_index_with_direction_scoped(
        txn,
        property,
        value,
        node_id,
        direction,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn remove_from_range_index_with_direction_scoped(
    txn: &DbTransaction,
    property: &str,
    value: &str,
    node_id: NodeId,
    direction: RangeIndexDirection,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Range(RangeIndexKey::new(
            direction,
            hash_property_name(property),
            Cow::Borrowed(value),
            node_id,
        ))),
    }
    .to_bytes();
    txn.delete(&key)?;
    Ok(())
}

/// Scan a range index for all values
pub async fn scan_range_index(
    txn: &(impl DbReadOps + Send + Sync),
    direction: RangeIndexDirection,
    property: &str,
) -> Result<Vec<NodeId>, HelixDbError> {
    scan_range_index_scoped(txn, direction, property, DataScope::LegacyUnscoped).await
}

pub async fn scan_range_index_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    direction: RangeIndexDirection,
    property: &str,
    tenant_scope: DataScope,
) -> Result<Vec<NodeId>, HelixDbError> {
    let property_hash = hash_property_name(property);
    let prefix = RangeScanPrefix::Property {
        direction,
        property_hash,
    }
    .to_bytes();
    let mut iter = txn
        .scan_prefix(DataKey::data_prefix(tenant_scope, prefix), ..)
        .await?;

    let mut results = Vec::new();
    while let Some(kv) = iter.next().await? {
        let Some(key) = tenant_scope.strip_key(&kv.key) else {
            continue;
        };
        let Ok(parsed) = RangeIndexKey::parse_from_slice(key) else {
            continue;
        };
        if parsed.direction() == direction && parsed.property_hash() == &property_hash {
            results.push(parsed.node_id());
        }
    }

    Ok(results)
}

/// Scan a range index in physical direction order with an optional result cap.
pub async fn scan_range_index_with_direction_limited(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    direction: RangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<NodeId>, HelixDbError> {
    scan_range_index_with_direction_limited_scoped(
        txn,
        property,
        direction,
        limit,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn scan_range_index_with_direction_limited_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    direction: RangeIndexDirection,
    limit: Option<usize>,
    tenant_scope: DataScope,
) -> Result<Vec<NodeId>, HelixDbError> {
    let mut results = scan_range_index_scoped(txn, direction, property, tenant_scope).await?;
    if let Some(limit) = limit {
        results.truncate(limit);
    }
    Ok(results)
}

/// Range query type for bounded scans (internal index type)
///
/// Uses string values for lexicographic key comparison in the storage layer.
/// The `PropertyValue::to_index_string()` method should be used to convert
/// typed values before creating a `RangeQuery`.
#[derive(Debug, Clone)]
pub enum RangeQuery<'a> {
    /// Greater than: property > value
    Gt(&'a str),
    /// Greater than or equal: property >= value
    Gte(&'a str),
    /// Less than: property < value
    Lt(&'a str),
    /// Less than or equal: property <= value
    Lte(&'a str),
    /// Between: min <= property <= max (inclusive)
    Between(&'a str, &'a str),
    /// Between with explicit bound inclusivity.
    BetweenBounds {
        /// Lower bound value.
        min: &'a str,
        /// Whether the lower bound is inclusive.
        min_inclusive: bool,
        /// Upper bound value.
        max: &'a str,
        /// Whether the upper bound is inclusive.
        max_inclusive: bool,
    },
}

fn edge_range_index_direction(direction: RangeIndexDirection) -> EdgeRangeIndexDirection {
    match direction {
        RangeIndexDirection::Asc => EdgeRangeIndexDirection::Asc,
        RangeIndexDirection::Desc => EdgeRangeIndexDirection::Desc,
    }
}

fn edge_range_key_with_direction<'a>(
    edge_direction: EdgeRangeDirection,
    node: NodeId,
    property: &str,
    value: &'a str,
    edge_id: EdgeId,
    direction: RangeIndexDirection,
) -> DataKeyKind<'a> {
    DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeRange(EdgeRangeIndexKey::new(
        edge_direction,
        edge_range_index_direction(direction),
        node,
        hash_property_name(property),
        Cow::Borrowed(value),
        edge_id,
    )))
}

fn global_edge_range_key_with_direction<'a>(
    property: &str,
    value: &'a str,
    edge_id: EdgeId,
    direction: RangeIndexDirection,
) -> DataKeyKind<'a> {
    DataKeyKind::PropertyIndex(PropertyIndexKey::GlobalEdgeRange(
        GlobalEdgeRangeIndexKey::new(
            direction,
            hash_property_name(property),
            Cow::Borrowed(value),
            edge_id,
        ),
    ))
}

fn global_edge_range_value_prefix_with_direction(
    property: &str,
    value: &str,
    direction: RangeIndexDirection,
) -> Bytes {
    GlobalEdgeRangeScanValuePrefix::new(direction, hash_property_name(property), value).to_bytes()
}

fn global_edge_range_prefix_with_direction(
    property: &str,
    direction: RangeIndexDirection,
) -> Bytes {
    GlobalEdgeRangeScanPrefix::Property {
        direction,
        property_hash: hash_property_name(property),
    }
    .to_bytes()
}

fn edge_range_value_prefix_with_direction(
    edge_direction: EdgeRangeDirection,
    node: NodeId,
    property: &str,
    value: &str,
    direction: RangeIndexDirection,
) -> Bytes {
    EdgeRangeScanValuePrefix::new(
        edge_direction,
        edge_range_index_direction(direction),
        node,
        hash_property_name(property),
        value,
    )
    .to_bytes()
}

fn edge_range_prefix_with_direction(
    edge_direction: EdgeRangeDirection,
    node: NodeId,
    property: &str,
    direction: RangeIndexDirection,
) -> Bytes {
    EdgeRangeScanPrefix::Property {
        edge_direction,
        range_direction: edge_range_index_direction(direction),
        endpoint: node,
        property_hash: hash_property_name(property),
    }
    .to_bytes()
}

fn inclusive_edge_range_key_upper_bound_with_direction(
    edge_direction: EdgeRangeDirection,
    node: NodeId,
    property: &str,
    value: &str,
    direction: RangeIndexDirection,
) -> Bytes {
    EdgeRangeScanValuePrefix::new(
        edge_direction,
        edge_range_index_direction(direction),
        node,
        hash_property_name(property),
        value,
    )
    .inclusive_end_bound()
}

/// Scan a range index with bounds
///
/// Uses the lexicographic ordering of the range index keys to efficiently
/// scan bounded ranges.
pub async fn scan_range_index_bounded(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    query: RangeQuery<'_>,
) -> Result<Vec<NodeId>, HelixDbError> {
    scan_range_index_bounded_in_direction(txn, property, query, RangeIndexDirection::Asc).await
}

/// Scan a bounded range index in physical direction order with an optional result cap.
pub async fn scan_range_index_bounded_with_direction_limited(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<NodeId>, HelixDbError> {
    scan_range_index_bounded_with_direction_limited_scoped(
        txn,
        property,
        query,
        direction,
        limit,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn scan_range_index_bounded_with_direction_limited_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
    limit: Option<usize>,
    tenant_scope: DataScope,
) -> Result<Vec<NodeId>, HelixDbError> {
    let mut results =
        scan_range_index_bounded_in_direction_scoped(txn, property, query, direction, tenant_scope)
            .await?;
    if let Some(limit) = limit {
        results.truncate(limit);
    }
    Ok(results)
}

async fn scan_range_index_bounded_in_direction(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
) -> Result<Vec<NodeId>, HelixDbError> {
    scan_range_index_bounded_in_direction_scoped(
        txn,
        property,
        query,
        direction,
        DataScope::LegacyUnscoped,
    )
    .await
}

async fn scan_range_index_bounded_in_direction_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
    tenant_scope: DataScope,
) -> Result<Vec<NodeId>, HelixDbError> {
    let property_hash = hash_property_name(property);
    let prefix = RangeScanPrefix::Property {
        direction,
        property_hash,
    }
    .to_bytes();
    let (start, end) = range_scan_bounds_with_direction(&prefix, property_hash, &query, direction);
    let (start, end) = DataKey::data_range(tenant_scope, start, end);
    let mut iter = txn.scan(start..end).await?;
    let mut results = Vec::new();

    while let Some(kv) = iter.next().await? {
        let Some(key) = tenant_scope.strip_key(&kv.key) else {
            continue;
        };
        if !key.starts_with(prefix.as_ref()) {
            continue;
        }
        let Ok(parsed) = RangeIndexKey::parse_from_slice(key) else {
            continue;
        };
        if parsed.direction() != direction || parsed.property_hash() != &property_hash {
            continue;
        }
        if range_query_matches_value(&query, parsed.value().as_bytes()) {
            results.push(parsed.node_id());
        }
    }

    Ok(results)
}

fn range_scan_bounds_with_direction(
    prefix: &Bytes,
    property_hash: [u8; 4],
    query: &RangeQuery<'_>,
    direction: RangeIndexDirection,
) -> (Bytes, Bytes) {
    match query {
        RangeQuery::Gt(value) => match direction {
            RangeIndexDirection::Asc => {
                let start = RangeScanValuePrefix::new(direction, property_hash, value).to_bytes();
                (start, bounded_prefix_end(prefix))
            }
            RangeIndexDirection::Desc => {
                let end = RangeScanValuePrefix::new(direction, property_hash, value).to_bytes();
                (prefix.clone(), end)
            }
        },
        RangeQuery::Gte(value) => match direction {
            RangeIndexDirection::Asc => {
                let start = RangeScanValuePrefix::new(direction, property_hash, value).to_bytes();
                (start, bounded_prefix_end(prefix))
            }
            RangeIndexDirection::Desc => (
                prefix.clone(),
                RangeScanValuePrefix::new(direction, property_hash, value).inclusive_end_bound(),
            ),
        },
        RangeQuery::Lt(value) => match direction {
            RangeIndexDirection::Asc => {
                let end = RangeScanValuePrefix::new(direction, property_hash, value).to_bytes();
                (prefix.clone(), end)
            }
            RangeIndexDirection::Desc => (
                RangeScanValuePrefix::new(direction, property_hash, value).inclusive_end_bound(),
                bounded_prefix_end(prefix),
            ),
        },
        RangeQuery::Lte(value) => match direction {
            RangeIndexDirection::Asc => (
                prefix.clone(),
                RangeScanValuePrefix::new(direction, property_hash, value).inclusive_end_bound(),
            ),
            RangeIndexDirection::Desc => {
                let start = RangeScanValuePrefix::new(direction, property_hash, value).to_bytes();
                (start, bounded_prefix_end(prefix))
            }
        },
        RangeQuery::Between(min, max) => match direction {
            RangeIndexDirection::Asc => (
                RangeScanValuePrefix::new(direction, property_hash, min).to_bytes(),
                RangeScanValuePrefix::new(direction, property_hash, max).inclusive_end_bound(),
            ),
            RangeIndexDirection::Desc => (
                RangeScanValuePrefix::new(direction, property_hash, max).to_bytes(),
                RangeScanValuePrefix::new(direction, property_hash, min).inclusive_end_bound(),
            ),
        },
        RangeQuery::BetweenBounds {
            min,
            min_inclusive,
            max,
            max_inclusive,
        } => match direction {
            RangeIndexDirection::Asc => {
                let start = RangeScanValuePrefix::new(direction, property_hash, min).to_bytes();
                let upper = RangeScanValuePrefix::new(direction, property_hash, max);
                let end = if *max_inclusive {
                    upper.inclusive_end_bound()
                } else {
                    upper.to_bytes()
                };
                (start, end)
            }
            RangeIndexDirection::Desc => {
                let start = RangeScanValuePrefix::new(direction, property_hash, max).to_bytes();
                let lower = RangeScanValuePrefix::new(direction, property_hash, min);
                let end = if *min_inclusive {
                    lower.inclusive_end_bound()
                } else {
                    lower.to_bytes()
                };
                (start, end)
            }
        },
    }
}

fn range_query_matches_value(query: &RangeQuery<'_>, key_value: &[u8]) -> bool {
    match query {
        RangeQuery::Gt(value) => key_value > value.as_bytes(),
        RangeQuery::Gte(value) => key_value >= value.as_bytes(),
        RangeQuery::Lt(value) => key_value < value.as_bytes(),
        RangeQuery::Lte(value) => key_value <= value.as_bytes(),
        RangeQuery::Between(min, max) => key_value >= min.as_bytes() && key_value <= max.as_bytes(),
        RangeQuery::BetweenBounds {
            min,
            min_inclusive,
            max,
            max_inclusive,
        } => {
            let lower_matches = if *min_inclusive {
                key_value >= min.as_bytes()
            } else {
                key_value > min.as_bytes()
            };
            let upper_matches = if *max_inclusive {
                key_value <= max.as_bytes()
            } else {
                key_value < max.as_bytes()
            };
            lower_matches && upper_matches
        }
    }
}

/// Scan a range index with bounds, returning up to `limit` nodes.
///
/// If `filter` is provided, only nodes contained in the filter set are returned.
pub async fn scan_range_index_bounded_limited(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    query: RangeQuery<'_>,
    limit: usize,
    filter: Option<&RoaringTreemap>,
) -> Result<RoaringTreemap, HelixDbError> {
    let mut results = RoaringTreemap::new();
    if limit == 0 {
        return Ok(results);
    }

    for node_id in
        scan_range_index_bounded_in_direction(txn, property, query, RangeIndexDirection::Asc)
            .await?
    {
        if filter.map(|set| set.contains(node_id)).unwrap_or(true) {
            results.insert(node_id);
            if results.len() as usize >= limit {
                break;
            }
        }
    }

    Ok(results)
}

/// Scan a range index prefix (all values), returning up to `limit` nodes.
///
/// If `filter` is provided, only nodes contained in the filter set are returned.
pub async fn scan_range_index_prefix_limited(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    limit: usize,
    filter: Option<&RoaringTreemap>,
) -> Result<RoaringTreemap, HelixDbError> {
    if limit == 0 {
        return Ok(RoaringTreemap::new());
    }

    let property_hash = hash_property_name(property);
    let prefix = RangeScanPrefix::Property {
        direction: RangeIndexDirection::Asc,
        property_hash,
    }
    .to_bytes();
    let mut iter = txn.scan_prefix(prefix, ..).await?;
    let mut results = RoaringTreemap::new();

    while let Some(kv) = iter.next().await? {
        let Ok(parsed) = RangeIndexKey::parse_from_slice(&kv.key) else {
            continue;
        };
        if parsed.property_hash() == &property_hash
            && filter
                .map(|set| set.contains(parsed.node_id()))
                .unwrap_or(true)
        {
            results.insert(parsed.node_id());
            if results.len() as usize >= limit {
                break;
            }
        }
    }

    Ok(results)
}

/// Delete all node equality index rows for a property.
pub async fn delete_equality_index_entries_for_property(
    txn: &DbTransaction,
    property: &str,
) -> Result<(), HelixDbError> {
    let prefix = EqualityScanPrefix::Property {
        property_hash: hash_property_name(property),
    }
    .to_bytes();

    let mut iter = txn.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        txn.delete(&kv.key)?;
    }

    Ok(())
}

/// Delete all node range index rows for a property.
pub async fn delete_range_index_entries_for_property(
    txn: &DbTransaction,
    property: &str,
) -> Result<(), HelixDbError> {
    delete_range_index_entries_for_property_with_direction(txn, property, RangeIndexDirection::Asc)
        .await
}

/// Delete all node range index rows for a property in the direction.
pub async fn delete_range_index_entries_for_property_with_direction(
    txn: &DbTransaction,
    property: &str,
    direction: RangeIndexDirection,
) -> Result<(), HelixDbError> {
    let prefix = RangeScanPrefix::Property {
        direction,
        property_hash: hash_property_name(property),
    }
    .to_bytes();

    let mut iter = txn.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        txn.delete(&kv.key)?;
    }

    Ok(())
}

/// Delete all directional edge equality index rows for a property.
pub async fn delete_edge_equality_index_entries_for_property(
    txn: &DbTransaction,
    property: &str,
) -> Result<(), HelixDbError> {
    let prop_hash = hash_property_name(property);
    let mut iter = txn
        .scan_prefix(EdgeEqualityScanPrefix::Index.to_bytes(), ..)
        .await?;

    while let Some(kv) = iter.next().await? {
        let Ok(parsed) = EdgeEqualityIndexKey::parse_from_slice(&kv.key) else {
            continue;
        };
        if parsed.property_hash() == &prop_hash {
            txn.delete(&kv.key)?;
        }
    }

    Ok(())
}

/// Delete all global edge equality rows in one legacy property-hash lane.
///
/// Distinct legacy properties with the same 32-bit hash share this lane. V1
/// migration must therefore call this only after every current full-string
/// identity has converged to an Active generation.
pub(crate) async fn delete_global_edge_equality_index_entries_for_property(
    txn: &DbTransaction,
    property: &str,
) -> Result<(), HelixDbError> {
    let prefix = GlobalEdgeEqualityScanPrefix::Property {
        property_hash: hash_property_name(property),
    }
    .to_bytes();
    let mut iter = txn.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        txn.delete(&kv.key)?;
    }
    Ok(())
}

/// Delete all directional edge range index rows for a property.
pub async fn delete_edge_range_index_entries_for_property(
    txn: &DbTransaction,
    property: &str,
) -> Result<(), HelixDbError> {
    delete_edge_range_index_entries_for_property_with_direction(
        txn,
        property,
        RangeIndexDirection::Asc,
    )
    .await
}

/// Delete all directional edge range index rows for a property in the direction.
pub async fn delete_edge_range_index_entries_for_property_with_direction(
    txn: &DbTransaction,
    property: &str,
    direction: RangeIndexDirection,
) -> Result<(), HelixDbError> {
    let prop_hash = hash_property_name(property);
    let range_direction = edge_range_index_direction(direction);
    for edge_direction in [EdgeRangeDirection::Out, EdgeRangeDirection::In] {
        let prefix = EdgeRangeScanPrefix::Direction {
            edge_direction,
            range_direction,
        }
        .to_bytes();
        let mut iter = txn.scan_prefix(prefix, ..).await?;

        while let Some(kv) = iter.next().await? {
            let Ok(parsed) = EdgeRangeIndexKey::parse_from_slice(&kv.key) else {
                continue;
            };
            if parsed.property_hash() == &prop_hash {
                txn.delete(&kv.key)?;
            }
        }
    }

    Ok(())
}

/// Delete all global edge range rows in one legacy property-hash lane and
/// physical direction.
///
/// Distinct legacy properties with the same 32-bit hash share this lane. V1
/// migration must therefore call this only after every current full-string
/// identity has converged to an Active generation.
pub(crate) async fn delete_global_edge_range_index_entries_for_property_with_direction(
    txn: &DbTransaction,
    property: &str,
    direction: RangeIndexDirection,
) -> Result<(), HelixDbError> {
    let prefix = GlobalEdgeRangeScanPrefix::Property {
        direction,
        property_hash: hash_property_name(property),
    }
    .to_bytes();
    let mut iter = txn.scan_prefix(prefix, ..).await?;
    while let Some(kv) = iter.next().await? {
        txn.delete(&kv.key)?;
    }
    Ok(())
}

// =============================================================================
// Edge Label Index Functions
// =============================================================================

/// Add an edge to the edge label index
///
/// Updates both the outgoing index (source -> targets with label)
/// and the incoming index (target -> sources with label).
pub async fn add_to_edge_label_index(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    label: &str,
) -> Result<(), HelixDbError> {
    add_to_edge_label_index_scoped(txn, from, to, label, DataScope::LegacyUnscoped).await
}

pub async fn add_to_edge_label_index_scoped(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    label: &str,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let label_hash = hash_property_value(label);

    // Update outgoing index: from + label -> to
    let out_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(EdgeRangeDirection::Out, from, label_hash),
        )),
    }
    .to_bytes();
    txn.merge_commutative(&out_key, crate::merge_operator::encode_bitmap_add(to))?;

    // Update incoming index: to + label -> from
    let in_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(EdgeRangeDirection::In, to, label_hash),
        )),
    }
    .to_bytes();
    txn.merge_commutative(&in_key, crate::merge_operator::encode_bitmap_add(from))?;

    Ok(())
}

/// Remove an edge from the edge label index
pub async fn remove_from_edge_label_index(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    label: &str,
) -> Result<(), HelixDbError> {
    remove_from_edge_label_index_scoped(txn, from, to, label, DataScope::LegacyUnscoped).await
}

pub async fn remove_from_edge_label_index_scoped(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    label: &str,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let label_hash = hash_property_value(label);

    // Update outgoing index
    let out_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(EdgeRangeDirection::Out, from, label_hash),
        )),
    }
    .to_bytes();
    if let Some(data) = txn.get(&out_key).await? {
        let mut bitmap = decode_roaring_treemap(&data)?;
        bitmap.remove(to);
        if bitmap.is_empty() {
            txn.delete(&out_key)?;
        } else {
            txn.put(&out_key, encode_roaring_treemap(&bitmap))?;
        }
    }

    // Update incoming index
    let in_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(EdgeRangeDirection::In, to, label_hash),
        )),
    }
    .to_bytes();
    if let Some(data) = txn.get(&in_key).await? {
        let mut bitmap = decode_roaring_treemap(&data)?;
        bitmap.remove(from);
        if bitmap.is_empty() {
            txn.delete(&in_key)?;
        } else {
            txn.put(&in_key, encode_roaring_treemap(&bitmap))?;
        }
    }

    Ok(())
}

/// Look up outgoing neighbors by edge label
///
/// Returns all target NodeIds reachable from `source` via edges with the given label.
pub async fn lookup_out_neighbors_by_label(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    label: &str,
) -> Result<RoaringTreemap, HelixDbError> {
    lookup_out_neighbors_by_label_scoped(txn, source, label, DataScope::LegacyUnscoped).await
}

pub async fn lookup_out_neighbors_by_label_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    label: &str,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(EdgeRangeDirection::Out, source, hash_property_value(label)),
        )),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => decode_roaring_treemap(&data),
        None => Ok(RoaringTreemap::new()),
    }
}

/// Look up incoming neighbors by edge label
///
/// Returns all source NodeIds that have edges to `target` with the given label.
pub async fn lookup_in_neighbors_by_label(
    txn: &(impl DbReadOps + Send + Sync),
    target: NodeId,
    label: &str,
) -> Result<RoaringTreemap, HelixDbError> {
    lookup_in_neighbors_by_label_scoped(txn, target, label, DataScope::LegacyUnscoped).await
}

pub async fn lookup_in_neighbors_by_label_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    target: NodeId,
    label: &str,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
            EdgeLabelNeighborKey::new(EdgeRangeDirection::In, target, hash_property_value(label)),
        )),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => decode_roaring_treemap(&data),
        None => Ok(RoaringTreemap::new()),
    }
}

// =============================================================================
// Edge Property Index Functions (Equality and Range)
// =============================================================================

/// Add an edge to the equality index (both directions)
pub async fn add_to_edge_equality_index(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
) -> Result<(), HelixDbError> {
    add_to_edge_equality_index_scoped(
        txn,
        from,
        to,
        edge_id,
        property,
        value,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn add_to_edge_equality_index_scoped(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let out_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeEquality(
            EdgeEqualityIndexKey::new(
                EdgeEqualityDirection::Out,
                from,
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    txn.merge_commutative(&out_key, crate::merge_operator::encode_bitmap_add(edge_id))?;

    let in_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeEquality(
            EdgeEqualityIndexKey::new(
                EdgeEqualityDirection::In,
                to,
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    txn.merge_commutative(&in_key, crate::merge_operator::encode_bitmap_add(edge_id))?;

    let global_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::GlobalEdgeEquality(
            crate::encoding::indexes::equality::GlobalEdgeEqualityIndexKey::new(
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    txn.merge_commutative(
        &global_key,
        crate::merge_operator::encode_bitmap_add(edge_id),
    )?;

    Ok(())
}

/// Remove an edge from the equality index (both directions)
pub async fn remove_from_edge_equality_index(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
) -> Result<(), HelixDbError> {
    remove_from_edge_equality_index_scoped(
        txn,
        from,
        to,
        edge_id,
        property,
        value,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn remove_from_edge_equality_index_scoped(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let out_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeEquality(
            EdgeEqualityIndexKey::new(
                EdgeEqualityDirection::Out,
                from,
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    if let Some(data) = txn.get(&out_key).await? {
        let mut bitmap = decode_roaring_treemap(&data)?;
        bitmap.remove(edge_id);
        if bitmap.is_empty() {
            txn.delete(&out_key)?;
        } else {
            txn.put(&out_key, encode_roaring_treemap(&bitmap))?;
        }
    }

    let in_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeEquality(
            EdgeEqualityIndexKey::new(
                EdgeEqualityDirection::In,
                to,
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    if let Some(data) = txn.get(&in_key).await? {
        let mut bitmap = decode_roaring_treemap(&data)?;
        bitmap.remove(edge_id);
        if bitmap.is_empty() {
            txn.delete(&in_key)?;
        } else {
            txn.put(&in_key, encode_roaring_treemap(&bitmap))?;
        }
    }

    let global_key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::GlobalEdgeEquality(
            crate::encoding::indexes::equality::GlobalEdgeEqualityIndexKey::new(
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    if let Some(data) = txn.get(&global_key).await? {
        let mut bitmap = decode_roaring_treemap(&data)?;
        bitmap.remove(edge_id);
        if bitmap.is_empty() {
            txn.delete(&global_key)?;
        } else {
            txn.put(&global_key, encode_roaring_treemap(&bitmap))?;
        }
    }

    Ok(())
}

/// Look up edges by equality (outgoing from source)
pub async fn lookup_edges_out_by_equality(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    property: &str,
    value: &str,
) -> Result<RoaringTreemap, HelixDbError> {
    lookup_edges_out_by_equality_scoped(txn, source, property, value, DataScope::LegacyUnscoped)
        .await
}

pub async fn lookup_edges_out_by_equality_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    property: &str,
    value: &str,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeEquality(
            EdgeEqualityIndexKey::new(
                EdgeEqualityDirection::Out,
                source,
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => decode_roaring_treemap(&data),
        None => Ok(RoaringTreemap::new()),
    }
}

/// Look up edges by equality (incoming to target)
pub async fn lookup_edges_in_by_equality(
    txn: &(impl DbReadOps + Send + Sync),
    target: NodeId,
    property: &str,
    value: &str,
) -> Result<RoaringTreemap, HelixDbError> {
    lookup_edges_in_by_equality_scoped(txn, target, property, value, DataScope::LegacyUnscoped)
        .await
}

pub async fn lookup_edges_in_by_equality_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    target: NodeId,
    property: &str,
    value: &str,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeEquality(
            EdgeEqualityIndexKey::new(
                EdgeEqualityDirection::In,
                target,
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => decode_roaring_treemap(&data),
        None => Ok(RoaringTreemap::new()),
    }
}

/// Look up outgoing edges that have any value for an equality-indexed property.
pub async fn scan_edges_out_by_equality_property_prefix(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    property: &str,
) -> Result<RoaringTreemap, HelixDbError> {
    let prefix = EdgeEqualityScanPrefix::Property {
        direction: EdgeEqualityDirection::Out,
        source,
        property_hash: hash_property_name(property),
    }
    .to_bytes();

    let mut iter = txn.scan_prefix(prefix, ..).await?;
    let mut results = RoaringTreemap::new();
    while let Some(kv) = iter.next().await? {
        results |= &decode_roaring_treemap(&kv.value)?;
    }

    Ok(results)
}

/// Add an edge to the global label -> edge IDs index.
pub async fn add_to_global_edge_label_index(
    txn: &DbTransaction,
    label: &str,
    edge_id: EdgeId,
) -> Result<(), HelixDbError> {
    add_to_global_edge_label_index_scoped(txn, label, edge_id, DataScope::LegacyUnscoped).await
}

pub async fn add_to_global_edge_label_index_scoped(
    txn: &DbTransaction,
    label: &str,
    edge_id: EdgeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(
            hash_property_value(label),
        ))),
    }
    .to_bytes();
    txn.merge_commutative(&key, crate::merge_operator::encode_bitmap_add(edge_id))?;
    Ok(())
}

/// Remove an edge from the global label -> edge IDs index.
pub async fn remove_from_global_edge_label_index(
    txn: &DbTransaction,
    label: &str,
    edge_id: EdgeId,
) -> Result<(), HelixDbError> {
    remove_from_global_edge_label_index_scoped(txn, label, edge_id, DataScope::LegacyUnscoped).await
}

pub async fn remove_from_global_edge_label_index_scoped(
    txn: &DbTransaction,
    label: &str,
    edge_id: EdgeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(
            hash_property_value(label),
        ))),
    }
    .to_bytes();
    if let Some(data) = txn.get(&key).await? {
        let mut bitmap = decode_roaring_treemap(&data)?;
        bitmap.remove(edge_id);
        if bitmap.is_empty() {
            txn.delete(&key)?;
        } else {
            txn.put(&key, encode_roaring_treemap(&bitmap))?;
        }
    }
    Ok(())
}

/// Look up all edge IDs for a label from the global edge-label index.
pub async fn lookup_global_edge_label_index(
    txn: &(impl DbReadOps + Send + Sync),
    label: &str,
) -> Result<RoaringTreemap, HelixDbError> {
    lookup_global_edge_label_index_scoped(txn, label, DataScope::LegacyUnscoped).await
}

pub async fn lookup_global_edge_label_index_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    label: &str,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(
            hash_property_value(label),
        ))),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => decode_roaring_treemap(&data),
        None => Ok(RoaringTreemap::new()),
    }
}

/// Look up all edge IDs for a globally indexed edge property/value pair.
pub async fn lookup_global_edge_equality_index(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    value: &str,
) -> Result<RoaringTreemap, HelixDbError> {
    lookup_global_edge_equality_index_scoped(txn, property, value, DataScope::LegacyUnscoped).await
}

pub async fn lookup_global_edge_equality_index_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    value: &str,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::GlobalEdgeEquality(
            crate::encoding::indexes::equality::GlobalEdgeEqualityIndexKey::new(
                hash_property_name(property),
                hash_property_value(value),
            ),
        )),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => decode_roaring_treemap(&data),
        None => Ok(RoaringTreemap::new()),
    }
}

/// Look up edge IDs by exact `(from, to)` endpoints.
pub async fn lookup_edge_pair_index(
    txn: &(impl DbReadOps + Send + Sync),
    from: NodeId,
    to: NodeId,
) -> Result<RoaringTreemap, HelixDbError> {
    lookup_edge_pair_index_scoped(txn, from, to, DataScope::LegacyUnscoped).await
}

pub async fn lookup_edge_pair_index_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    from: NodeId,
    to: NodeId,
    tenant_scope: DataScope,
) -> Result<RoaringTreemap, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgePairIndex(crate::encoding::keys::EdgePairIndexKey::new(from, to)),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => decode_roaring_treemap(&data),
        None => Ok(RoaringTreemap::new()),
    }
}

/// Add an edge id to the exact `(from, to)` multigraph pair index.
pub async fn add_to_edge_pair_index(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
) -> Result<(), HelixDbError> {
    add_to_edge_pair_index_scoped(txn, from, to, edge_id, DataScope::LegacyUnscoped).await
}

pub async fn add_to_edge_pair_index_scoped(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgePairIndex(crate::encoding::keys::EdgePairIndexKey::new(from, to)),
    }
    .to_bytes();
    txn.merge_commutative(&key, crate::merge_operator::encode_bitmap_add(edge_id))?;
    Ok(())
}

/// Remove an edge id from the exact `(from, to)` multigraph pair index.
pub async fn remove_from_edge_pair_index(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
) -> Result<(), HelixDbError> {
    remove_from_edge_pair_index_scoped(txn, from, to, edge_id, DataScope::LegacyUnscoped).await
}

pub async fn remove_from_edge_pair_index_scoped(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgePairIndex(crate::encoding::keys::EdgePairIndexKey::new(from, to)),
    }
    .to_bytes();
    let Some(data) = txn.get(&key).await? else {
        return Ok(());
    };
    let mut bitmap = decode_roaring_treemap(&data)?;
    bitmap.remove(edge_id);
    if bitmap.is_empty() {
        txn.delete(&key)?;
    } else {
        txn.put(&key, encode_roaring_treemap(&bitmap))?;
    }
    Ok(())
}

/// Delete all node `$label` equality-index entries before a full rebuild.
pub async fn clear_node_label_indexes(txn: &DbTransaction) -> Result<(), HelixDbError> {
    let prefix = EqualityScanPrefix::Property {
        property_hash: hash_property_name("$label"),
    }
    .to_bytes();
    let mut iter = txn.scan_prefix(prefix, ..).await?;
    let mut keys = Vec::new();
    while let Some(kv) = iter.next().await? {
        keys.push(kv.key);
    }
    for key in keys {
        txn.delete(&key)?;
    }
    Ok(())
}

/// Delete all global edge-label index entries before a full rebuild.
pub async fn clear_global_edge_label_indexes(txn: &DbTransaction) -> Result<(), HelixDbError> {
    clear_global_edge_label_indexes_scoped(txn, DataScope::LegacyUnscoped).await
}

pub async fn clear_global_edge_label_indexes_scoped(
    txn: &DbTransaction,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let prefix = DataKey::data_prefix(tenant_scope, EdgeLabelScanPrefix::Index.to_bytes());
    let mut iter = txn.scan_prefix(prefix, ..).await?;
    let mut keys = Vec::new();
    while let Some(kv) = iter.next().await? {
        keys.push(kv.key);
    }
    for key in keys {
        txn.delete(&key)?;
    }
    Ok(())
}

/// Add an edge to the range index (both directions)
pub async fn add_to_edge_range_index(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
) -> Result<(), HelixDbError> {
    add_to_edge_range_index_with_direction(
        txn,
        from,
        to,
        edge_id,
        property,
        value,
        RangeIndexDirection::Asc,
    )
    .await
}

/// Add an edge to the range index with explicit direction (both directions)
pub async fn add_to_edge_range_index_with_direction(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
    direction: RangeIndexDirection,
) -> Result<(), HelixDbError> {
    add_to_edge_range_index_with_direction_scoped(
        txn,
        from,
        to,
        edge_id,
        property,
        value,
        direction,
        DataScope::LegacyUnscoped,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn add_to_edge_range_index_with_direction_scoped(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
    direction: RangeIndexDirection,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let out_key = edge_range_key_with_direction(
        EdgeRangeDirection::Out,
        from,
        property,
        value,
        edge_id,
        direction,
    );
    let out_key = DataKey::Data {
        scope: tenant_scope,
        kind: out_key,
    }
    .to_bytes();
    txn.put(&out_key, Bytes::new())?;

    let in_key = edge_range_key_with_direction(
        EdgeRangeDirection::In,
        to,
        property,
        value,
        edge_id,
        direction,
    );
    let in_key = DataKey::Data {
        scope: tenant_scope,
        kind: in_key,
    }
    .to_bytes();
    txn.put(&in_key, Bytes::new())?;

    let global_key = global_edge_range_key_with_direction(property, value, edge_id, direction);
    let global_key = DataKey::Data {
        scope: tenant_scope,
        kind: global_key,
    }
    .to_bytes();
    txn.put(&global_key, Bytes::new())?;

    Ok(())
}

/// Remove an edge from the range index (both directions)
pub async fn remove_from_edge_range_index(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
) -> Result<(), HelixDbError> {
    remove_from_edge_range_index_with_direction(
        txn,
        from,
        to,
        edge_id,
        property,
        value,
        RangeIndexDirection::Asc,
    )
    .await
}

/// Remove an edge from the range index with explicit direction (both directions)
pub async fn remove_from_edge_range_index_with_direction(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
    direction: RangeIndexDirection,
) -> Result<(), HelixDbError> {
    remove_from_edge_range_index_with_direction_scoped(
        txn,
        from,
        to,
        edge_id,
        property,
        value,
        direction,
        DataScope::LegacyUnscoped,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn remove_from_edge_range_index_with_direction_scoped(
    txn: &DbTransaction,
    from: NodeId,
    to: NodeId,
    edge_id: EdgeId,
    property: &str,
    value: &str,
    direction: RangeIndexDirection,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let out_key = edge_range_key_with_direction(
        EdgeRangeDirection::Out,
        from,
        property,
        value,
        edge_id,
        direction,
    );
    let out_key = DataKey::Data {
        scope: tenant_scope,
        kind: out_key,
    }
    .to_bytes();
    txn.delete(&out_key)?;

    let in_key = edge_range_key_with_direction(
        EdgeRangeDirection::In,
        to,
        property,
        value,
        edge_id,
        direction,
    );
    let in_key = DataKey::Data {
        scope: tenant_scope,
        kind: in_key,
    }
    .to_bytes();
    txn.delete(&in_key)?;

    let global_key = global_edge_range_key_with_direction(property, value, edge_id, direction);
    let global_key = DataKey::Data {
        scope: tenant_scope,
        kind: global_key,
    }
    .to_bytes();
    txn.delete(&global_key)?;

    Ok(())
}

/// Scan edge range index (outgoing from source) with query bounds
pub async fn scan_edge_range_index_out(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    property: &str,
    query: RangeQuery<'_>,
) -> Result<Vec<EdgeId>, HelixDbError> {
    scan_edge_range_index_out_with_direction(txn, source, property, query, RangeIndexDirection::Asc)
        .await
}

/// Scan an outgoing edge range index with explicit direction.
pub async fn scan_edge_range_index_out_with_direction(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
) -> Result<Vec<EdgeId>, HelixDbError> {
    Ok(
        scan_edge_range_index_out_with_direction_counted(txn, source, property, query, direction)
            .await?
            .0,
    )
}

/// Scan an outgoing edge range index with explicit direction and return scanned entry count.
pub(crate) async fn scan_edge_range_index_out_with_direction_counted(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
) -> Result<(Vec<EdgeId>, u64), HelixDbError> {
    let prefix =
        edge_range_prefix_with_direction(EdgeRangeDirection::Out, source, property, direction);
    scan_edge_range_index_impl(txn, prefix, source, property, query, true, direction).await
}

/// Look up outgoing edges that have any value for a range-indexed property.
pub async fn scan_edge_range_index_out_prefix(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    property: &str,
) -> Result<Vec<EdgeId>, HelixDbError> {
    scan_edge_range_index_out_prefix_with_direction(txn, source, property, RangeIndexDirection::Asc)
        .await
}

/// Look up outgoing edges with any range-indexed value in the requested direction.
pub async fn scan_edge_range_index_out_prefix_with_direction(
    txn: &(impl DbReadOps + Send + Sync),
    source: NodeId,
    property: &str,
    direction: RangeIndexDirection,
) -> Result<Vec<EdgeId>, HelixDbError> {
    let prefix =
        edge_range_prefix_with_direction(EdgeRangeDirection::Out, source, property, direction);
    let mut iter = txn.scan_prefix(prefix, ..).await?;
    let mut results = Vec::new();

    while let Some(kv) = iter.next().await? {
        let Ok(parsed) = EdgeRangeIndexKey::parse_from_slice(&kv.key) else {
            continue;
        };
        if parsed.range_direction() == edge_range_index_direction(direction) {
            results.push(parsed.edge_id());
        }
    }

    Ok(results)
}

/// Scan edge range index (incoming to target) with query bounds
pub async fn scan_edge_range_index_in(
    txn: &(impl DbReadOps + Send + Sync),
    target: NodeId,
    property: &str,
    query: RangeQuery<'_>,
) -> Result<Vec<EdgeId>, HelixDbError> {
    scan_edge_range_index_in_with_direction(txn, target, property, query, RangeIndexDirection::Asc)
        .await
}

/// Scan an incoming edge range index with explicit direction.
pub async fn scan_edge_range_index_in_with_direction(
    txn: &(impl DbReadOps + Send + Sync),
    target: NodeId,
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
) -> Result<Vec<EdgeId>, HelixDbError> {
    Ok(
        scan_edge_range_index_in_with_direction_counted(txn, target, property, query, direction)
            .await?
            .0,
    )
}

/// Scan all globally indexed edge range rows for a property.
pub async fn scan_global_edge_range_index_all_with_direction_limited(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    direction: RangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<EdgeId>, HelixDbError> {
    scan_global_edge_range_index_all_with_direction_limited_scoped(
        txn,
        property,
        direction,
        limit,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn scan_global_edge_range_index_all_with_direction_limited_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    direction: RangeIndexDirection,
    limit: Option<usize>,
    tenant_scope: DataScope,
) -> Result<Vec<EdgeId>, HelixDbError> {
    let property_hash = hash_property_name(property);
    let prefix = global_edge_range_prefix_with_direction(property, direction);
    let mut iter = txn
        .scan_prefix(DataKey::data_prefix(tenant_scope, prefix), ..)
        .await?;
    let mut results = Vec::new();

    while let Some(kv) = iter.next().await? {
        let Some(key) = tenant_scope.strip_key(&kv.key) else {
            continue;
        };
        let Ok(parsed) = GlobalEdgeRangeIndexKey::parse_from_slice(key) else {
            continue;
        };
        if parsed.direction() == direction && parsed.property_hash() == &property_hash {
            results.push(parsed.edge_id());
            if limit.is_some_and(|limit| results.len() >= limit) {
                break;
            }
        }
    }

    Ok(results)
}

/// Scan bounded globally indexed edge range rows for a property.
pub async fn scan_global_edge_range_index_with_direction_limited(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
    limit: Option<usize>,
) -> Result<Vec<EdgeId>, HelixDbError> {
    scan_global_edge_range_index_with_direction_limited_scoped(
        txn,
        property,
        query,
        direction,
        limit,
        DataScope::LegacyUnscoped,
    )
    .await
}

pub async fn scan_global_edge_range_index_with_direction_limited_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
    limit: Option<usize>,
    tenant_scope: DataScope,
) -> Result<Vec<EdgeId>, HelixDbError> {
    let property_hash = hash_property_name(property);
    let prefix = global_edge_range_prefix_with_direction(property, direction);
    let (start, end) =
        global_edge_range_scan_bounds_with_direction(&prefix, property, &query, direction);
    let (start, end) = DataKey::data_range(tenant_scope, start, end);

    let mut iter = txn.scan(start..end).await?;
    let mut results = Vec::new();

    while let Some(kv) = iter.next().await? {
        let Some(key) = tenant_scope.strip_key(&kv.key) else {
            continue;
        };
        if !key.starts_with(prefix.as_ref()) {
            continue;
        }
        let Ok(parsed) = GlobalEdgeRangeIndexKey::parse_from_slice(key) else {
            continue;
        };
        if parsed.direction() != direction || parsed.property_hash() != &property_hash {
            continue;
        }
        if range_query_matches_value(&query, parsed.value().as_bytes()) {
            results.push(parsed.edge_id());
            if limit.is_some_and(|limit| results.len() >= limit) {
                break;
            }
        }
    }

    Ok(results)
}

fn global_edge_range_scan_bounds_with_direction(
    prefix: &Bytes,
    property: &str,
    query: &RangeQuery<'_>,
    direction: RangeIndexDirection,
) -> (Bytes, Bytes) {
    match query {
        RangeQuery::Gt(value) => match direction {
            RangeIndexDirection::Asc => {
                let start =
                    global_edge_range_value_prefix_with_direction(property, value, direction);
                (start, bounded_prefix_end(prefix))
            }
            RangeIndexDirection::Desc => {
                let end = global_edge_range_value_prefix_with_direction(property, value, direction);
                (prefix.clone(), end)
            }
        },
        RangeQuery::Gte(value) => match direction {
            RangeIndexDirection::Asc => {
                let start =
                    global_edge_range_value_prefix_with_direction(property, value, direction);
                (start, bounded_prefix_end(prefix))
            }
            RangeIndexDirection::Desc => (
                prefix.clone(),
                GlobalEdgeRangeScanValuePrefix::new(direction, hash_property_name(property), value)
                    .inclusive_end_bound(),
            ),
        },
        RangeQuery::Lt(value) => match direction {
            RangeIndexDirection::Asc => {
                let end = global_edge_range_value_prefix_with_direction(property, value, direction);
                (prefix.clone(), end)
            }
            RangeIndexDirection::Desc => (
                GlobalEdgeRangeScanValuePrefix::new(direction, hash_property_name(property), value)
                    .inclusive_end_bound(),
                bounded_prefix_end(prefix),
            ),
        },
        RangeQuery::Lte(value) => match direction {
            RangeIndexDirection::Asc => (
                prefix.clone(),
                GlobalEdgeRangeScanValuePrefix::new(direction, hash_property_name(property), value)
                    .inclusive_end_bound(),
            ),
            RangeIndexDirection::Desc => {
                let start =
                    global_edge_range_value_prefix_with_direction(property, value, direction);
                (start, bounded_prefix_end(prefix))
            }
        },
        RangeQuery::Between(min, max) => match direction {
            RangeIndexDirection::Asc => {
                let start = global_edge_range_value_prefix_with_direction(property, min, direction);
                (
                    start,
                    GlobalEdgeRangeScanValuePrefix::new(
                        direction,
                        hash_property_name(property),
                        max,
                    )
                    .inclusive_end_bound(),
                )
            }
            RangeIndexDirection::Desc => {
                let start = global_edge_range_value_prefix_with_direction(property, max, direction);
                (
                    start,
                    GlobalEdgeRangeScanValuePrefix::new(
                        direction,
                        hash_property_name(property),
                        min,
                    )
                    .inclusive_end_bound(),
                )
            }
        },
        RangeQuery::BetweenBounds {
            min,
            min_inclusive,
            max,
            max_inclusive,
        } => match direction {
            RangeIndexDirection::Asc => {
                let start = global_edge_range_value_prefix_with_direction(property, min, direction);
                let upper = GlobalEdgeRangeScanValuePrefix::new(
                    direction,
                    hash_property_name(property),
                    max,
                );
                let end = if *max_inclusive {
                    upper.inclusive_end_bound()
                } else {
                    upper.to_bytes()
                };
                (start, end)
            }
            RangeIndexDirection::Desc => {
                let start = global_edge_range_value_prefix_with_direction(property, max, direction);
                let lower = GlobalEdgeRangeScanValuePrefix::new(
                    direction,
                    hash_property_name(property),
                    min,
                );
                let end = if *min_inclusive {
                    lower.inclusive_end_bound()
                } else {
                    lower.to_bytes()
                };
                (start, end)
            }
        },
    }
}

/// Scan an incoming edge range index with explicit direction and return scanned entry count.
pub(crate) async fn scan_edge_range_index_in_with_direction_counted(
    txn: &(impl DbReadOps + Send + Sync),
    target: NodeId,
    property: &str,
    query: RangeQuery<'_>,
    direction: RangeIndexDirection,
) -> Result<(Vec<EdgeId>, u64), HelixDbError> {
    let prefix =
        edge_range_prefix_with_direction(EdgeRangeDirection::In, target, property, direction);
    scan_edge_range_index_impl(txn, prefix, target, property, query, false, direction).await
}

fn edge_range_scan_bounds_with_direction(
    prefix: &Bytes,
    node: NodeId,
    property: &str,
    query: &RangeQuery<'_>,
    outgoing: bool,
    direction: RangeIndexDirection,
) -> (Bytes, Bytes) {
    let edge_direction = if outgoing {
        EdgeRangeDirection::Out
    } else {
        EdgeRangeDirection::In
    };

    match query {
        RangeQuery::Gt(value) => match direction {
            RangeIndexDirection::Asc => {
                let start = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    value,
                    direction,
                );
                (start, bounded_prefix_end(prefix))
            }
            RangeIndexDirection::Desc => {
                let end = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    value,
                    direction,
                );
                (prefix.clone(), end)
            }
        },
        RangeQuery::Gte(value) => match direction {
            RangeIndexDirection::Asc => {
                let start = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    value,
                    direction,
                );
                (start, bounded_prefix_end(prefix))
            }
            RangeIndexDirection::Desc => (
                prefix.clone(),
                inclusive_edge_range_key_upper_bound_with_direction(
                    edge_direction,
                    node,
                    property,
                    value,
                    direction,
                ),
            ),
        },
        RangeQuery::Lt(value) => match direction {
            RangeIndexDirection::Asc => {
                let end = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    value,
                    direction,
                );
                (prefix.clone(), end)
            }
            RangeIndexDirection::Desc => (
                inclusive_edge_range_key_upper_bound_with_direction(
                    edge_direction,
                    node,
                    property,
                    value,
                    direction,
                ),
                bounded_prefix_end(prefix),
            ),
        },
        RangeQuery::Lte(value) => match direction {
            RangeIndexDirection::Asc => (
                prefix.clone(),
                inclusive_edge_range_key_upper_bound_with_direction(
                    edge_direction,
                    node,
                    property,
                    value,
                    direction,
                ),
            ),
            RangeIndexDirection::Desc => {
                let start = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    value,
                    direction,
                );
                (start, bounded_prefix_end(prefix))
            }
        },
        RangeQuery::Between(min, max) => match direction {
            RangeIndexDirection::Asc => {
                let start = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    min,
                    direction,
                );
                (
                    start,
                    inclusive_edge_range_key_upper_bound_with_direction(
                        edge_direction,
                        node,
                        property,
                        max,
                        direction,
                    ),
                )
            }
            RangeIndexDirection::Desc => {
                let start = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    max,
                    direction,
                );
                (
                    start,
                    inclusive_edge_range_key_upper_bound_with_direction(
                        edge_direction,
                        node,
                        property,
                        min,
                        direction,
                    ),
                )
            }
        },
        RangeQuery::BetweenBounds {
            min,
            min_inclusive,
            max,
            max_inclusive,
        } => match direction {
            RangeIndexDirection::Asc => {
                let start = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    min,
                    direction,
                );
                let upper = EdgeRangeScanValuePrefix::new(
                    edge_direction,
                    edge_range_index_direction(direction),
                    node,
                    hash_property_name(property),
                    max,
                );
                let end = if *max_inclusive {
                    upper.inclusive_end_bound()
                } else {
                    upper.to_bytes()
                };
                (start, end)
            }
            RangeIndexDirection::Desc => {
                let start = edge_range_value_prefix_with_direction(
                    edge_direction,
                    node,
                    property,
                    max,
                    direction,
                );
                let lower = EdgeRangeScanValuePrefix::new(
                    edge_direction,
                    edge_range_index_direction(direction),
                    node,
                    hash_property_name(property),
                    min,
                );
                let end = if *min_inclusive {
                    lower.inclusive_end_bound()
                } else {
                    lower.to_bytes()
                };
                (start, end)
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn scan_edge_range_index_impl(
    txn: &(impl DbReadOps + Send + Sync),
    prefix: Bytes,
    node: NodeId,
    property: &str,
    query: RangeQuery<'_>,
    outgoing: bool,
    direction: RangeIndexDirection,
) -> Result<(Vec<EdgeId>, u64), HelixDbError> {
    let (start, end) =
        edge_range_scan_bounds_with_direction(&prefix, node, property, &query, outgoing, direction);

    let mut iter = txn.scan(start..end).await?;
    let mut results = Vec::new();
    let mut entries_scanned = 0u64;

    while let Some(kv) = iter.next().await? {
        entries_scanned += 1;
        if !kv.key.starts_with(prefix.as_ref()) {
            continue;
        }
        let Ok(parsed) = EdgeRangeIndexKey::parse_from_slice(&kv.key) else {
            continue;
        };
        if parsed.range_direction() != edge_range_index_direction(direction) {
            continue;
        }
        if range_query_matches_value(&query, parsed.value().as_bytes()) {
            results.push(parsed.edge_id());
        }
    }

    Ok((results, entries_scanned))
}

/// Store edge endpoints
pub async fn store_edge_endpoints(
    txn: &DbTransaction,
    edge_id: EdgeId,
    from: NodeId,
    to: NodeId,
) -> Result<(), HelixDbError> {
    store_edge_endpoints_scoped(txn, edge_id, from, to, DataScope::LegacyUnscoped).await
}

pub async fn store_edge_endpoints_scoped(
    txn: &DbTransaction,
    edge_id: EdgeId,
    from: NodeId,
    to: NodeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
    }
    .to_bytes();
    txn.put(&key, EdgeEndpointsValue::new(from, to).encode())?;
    Ok(())
}

/// Get edge endpoints
pub async fn get_edge_endpoints(
    txn: &(impl DbReadOps + Send + Sync),
    edge_id: EdgeId,
) -> Result<Option<(NodeId, NodeId)>, HelixDbError> {
    get_edge_endpoints_scoped(txn, edge_id, DataScope::LegacyUnscoped).await
}

pub async fn get_edge_endpoints_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    edge_id: EdgeId,
    tenant_scope: DataScope,
) -> Result<Option<(NodeId, NodeId)>, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => {
            let endpoints = EdgeEndpointsValue::decode(&data)?;
            Ok(Some((endpoints.source(), endpoints.target())))
        }
        None => Ok(None),
    }
}

/// Delete edge endpoints
pub async fn delete_edge_endpoints(
    txn: &DbTransaction,
    edge_id: EdgeId,
) -> Result<(), HelixDbError> {
    delete_edge_endpoints_scoped(txn, edge_id, DataScope::LegacyUnscoped).await
}

pub async fn delete_edge_endpoints_scoped(
    txn: &DbTransaction,
    edge_id: EdgeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(edge_id)),
    }
    .to_bytes();
    txn.delete(&key)?;
    Ok(())
}

/// Store edge properties by edge ID
pub async fn store_edge_properties_by_id(
    txn: &DbTransaction,
    edge_id: EdgeId,
    properties: &[Property],
) -> Result<(), HelixDbError> {
    store_edge_properties_by_id_scoped(txn, edge_id, properties, DataScope::LegacyUnscoped).await
}

pub async fn store_edge_properties_by_id_scoped(
    txn: &DbTransaction,
    edge_id: EdgeId,
    properties: &[Property],
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
    }
    .to_bytes();
    txn.put_bytes(key, encode_properties(properties))?;
    Ok(())
}

/// Get edge properties by edge ID
pub async fn get_edge_properties_by_id(
    txn: &(impl DbReadOps + Send + Sync),
    edge_id: EdgeId,
) -> Result<Vec<Property>, HelixDbError> {
    get_edge_properties_by_id_scoped(txn, edge_id, DataScope::LegacyUnscoped).await
}

pub async fn get_edge_properties_by_id_scoped(
    txn: &(impl DbReadOps + Send + Sync),
    edge_id: EdgeId,
    tenant_scope: DataScope,
) -> Result<Vec<Property>, HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
    }
    .to_bytes();
    match txn.get(&key).await? {
        Some(data) => Ok(decode_properties(&data)?),
        None => Ok(Vec::new()),
    }
}

/// Delete edge properties by edge ID
pub async fn delete_edge_properties_by_id(
    txn: &DbTransaction,
    edge_id: EdgeId,
) -> Result<(), HelixDbError> {
    delete_edge_properties_by_id_scoped(txn, edge_id, DataScope::LegacyUnscoped).await
}

pub async fn delete_edge_properties_by_id_scoped(
    txn: &DbTransaction,
    edge_id: EdgeId,
    tenant_scope: DataScope,
) -> Result<(), HelixDbError> {
    let key = DataKey::Data {
        scope: tenant_scope,
        kind: DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(edge_id)),
    }
    .to_bytes();
    txn.delete(&key)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::scoped_secondary_index_property;
    use proptest::prelude::*;
    use slatedb::IsolationLevel;

    #[test]
    fn test_property_value_to_index_string() {
        assert_eq!(property_value_to_index_string(&PropertyValue::Null), "null");
        assert_eq!(
            property_value_to_index_string(&PropertyValue::Bool(true)),
            "true"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::String("hello".to_string())),
            "hello"
        );

        // I64 should be zero-padded for lexicographic ordering
        let i64_str = property_value_to_index_string(&PropertyValue::I64(42));
        assert!(i64_str.starts_with('0'));
        assert!(i64_str.contains("42"));
    }

    fn current_range_bytes(direction: RangeIndexDirection, value: &PropertyValue) -> Vec<u8> {
        let crate::encoding::v2::values::property::range_index_value::RangeValueProjection::Indexed(
            value,
        ) = crate::encoding::v2::values::property::range_index_value::project_range_value(
            value, direction,
        )
        else {
            panic!("range regression fixture must be indexable");
        };
        value.encoded().to_vec()
    }

    fn current_range_key_bytes(
        direction: RangeIndexDirection,
        value: &str,
        entity_id: u64,
    ) -> Vec<u8> {
        let crate::encoding::v2::values::property::range_index_value::RangeValueProjection::Indexed(
            value,
        ) = crate::encoding::v2::values::property::range_index_value::project_range_value(
            &PropertyValue::String(value.to_string()),
            direction,
        )
        else {
            panic!("range regression fixture must be indexable");
        };
        value.entity_key_suffix(entity_id).to_vec()
    }

    fn assert_distinct_equality_identities(left: PropertyValue, right: PropertyValue) {
        use crate::encoding::v2::values::property::equality_index_value::project_equality_value;

        assert_ne!(
            project_equality_value(&left),
            project_equality_value(&right),
            "unequal typed values must retain distinct canonical identities: {left:?} and {right:?}"
        );
    }

    #[test]
    fn regression_secondary_equality_canonicalization_distinguishes_bool_and_string() {
        assert_distinct_equality_identities(
            PropertyValue::Bool(true),
            PropertyValue::String("true".into()),
        );
    }

    #[test]
    fn regression_secondary_equality_canonicalization_distinguishes_integer_and_string() {
        assert_distinct_equality_identities(
            PropertyValue::I64(42),
            PropertyValue::String("00000000000000000042".into()),
        );
    }

    #[test]
    fn regression_secondary_equality_canonicalization_distinguishes_same_length_arrays() {
        assert_distinct_equality_identities(
            PropertyValue::I64Array(vec![1, 2]),
            PropertyValue::I64Array(vec![8, 9]),
        );
        assert_distinct_equality_identities(
            PropertyValue::StringArray(vec!["a".into(), "b".into()]),
            PropertyValue::StringArray(vec!["x".into(), "y".into()]),
        );
    }

    #[test]
    fn regression_secondary_equality_canonicalization_distinguishes_bytes_and_string() {
        assert_distinct_equality_identities(
            PropertyValue::Bytes(vec![1, 2]),
            PropertyValue::String("<bytes:[1, 2]>".into()),
        );
    }

    #[test]
    fn regression_secondary_equality_canonicalization_distinguishes_null_and_string() {
        assert_distinct_equality_identities(
            PropertyValue::Null,
            PropertyValue::String("null".into()),
        );
    }

    #[test]
    fn regression_secondary_equality_canonicalization_matches_semantic_numeric_equality() {
        use crate::encoding::v2::values::property::equality_index_value::project_equality_value;

        let equal_pairs = [
            (PropertyValue::I64(42), PropertyValue::F64(42.0)),
            (PropertyValue::F64(0.0), PropertyValue::F64(-0.0)),
            (PropertyValue::F32(0.0), PropertyValue::F32(-0.0)),
        ];

        for (left, right) in equal_pairs {
            assert_eq!(
                project_equality_value(&left),
                project_equality_value(&right),
                "semantically equal numeric values must share one canonical identity: {left:?} and {right:?}"
            );
        }
    }

    #[test]
    fn regression_secondary_range_ascending_values_are_prefix_framed_before_entity_ids() {
        let ordered_values = ["", "\0", "a", "a\0", "aa", "aaa"];
        let entity_ids = [
            0x0000_0000_0000_0001,
            0x4100_0000_0000_0001,
            0xFF00_0000_0000_0001,
        ];

        for pair in ordered_values.windows(2) {
            let left = current_range_key_bytes(
                RangeIndexDirection::Asc,
                pair[0],
                *entity_ids.last().unwrap(),
            );
            let right = current_range_key_bytes(RangeIndexDirection::Asc, pair[1], entity_ids[0]);
            assert!(
                left < right,
                "value framing must keep {left:?} before {right:?} regardless of entity ID"
            );
        }

        for pair in entity_ids.windows(2) {
            assert!(
                current_range_key_bytes(RangeIndexDirection::Asc, "a", pair[0])
                    < current_range_key_bytes(RangeIndexDirection::Asc, "a", pair[1]),
                "equal values must use entity ID as the deterministic tie-breaker"
            );
            assert!(
                current_range_key_bytes(RangeIndexDirection::Desc, "a", pair[0])
                    < current_range_key_bytes(RangeIndexDirection::Desc, "a", pair[1]),
                "descending values must retain ascending entity-ID tie-breaking"
            );
        }

        for pair in ordered_values.windows(2) {
            assert!(
                current_range_key_bytes(
                    RangeIndexDirection::Desc,
                    pair[0],
                    *entity_ids.last().unwrap(),
                ) > current_range_key_bytes(RangeIndexDirection::Desc, pair[1], entity_ids[0]),
                "descending encoding must exactly reverse value order"
            );
        }
    }

    #[test]
    fn regression_property_hash_collision_fixture_is_exact() {
        let first = scoped_secondary_index_property("User", "property_16755");
        let second = scoped_secondary_index_property("User", "property_36911");

        assert_eq!(first, "User\u{1f}property_16755");
        assert_eq!(second, "User\u{1f}property_36911");
        assert_ne!(first, second);
        assert_eq!(
            hash_property_name(&first),
            hash_property_name(&second),
            "the fixture must retain the confirmed legacy 32-bit collision"
        );
    }

    #[test]
    fn regression_exact_numeric_semantics_do_not_round_i64_through_f64() {
        let exactly_representable = PropertyValue::I64(9_007_199_254_740_992);
        let next_integer = PropertyValue::I64(9_007_199_254_740_993);
        let float = PropertyValue::F64(9_007_199_254_740_992.0);

        assert!(exactly_representable.eq_value(&float));
        assert!(float.eq_value(&exactly_representable));
        assert!(!next_integer.eq_value(&float));
        assert!(!float.eq_value(&next_integer));
        assert_eq!(
            exactly_representable.compare(&next_integer),
            Some(std::cmp::Ordering::Less)
        );
        assert_eq!(
            next_integer.compare(&float),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(float.compare(&next_integer), Some(std::cmp::Ordering::Less));
    }

    #[test]
    fn regression_secondary_range_encoding_handles_integer_extremes_and_sign_transitions() {
        let ordered = [i64::MIN, -10, -2, -1, 0, 1, 2, 10, i64::MAX];
        for pair in ordered.windows(2) {
            let left = PropertyValue::I64(pair[0]);
            let right = PropertyValue::I64(pair[1]);
            assert!(
                current_range_bytes(RangeIndexDirection::Asc, &left)
                    < current_range_bytes(RangeIndexDirection::Asc, &right),
                "ascending bytes must preserve {left:?} < {right:?}"
            );
            assert!(
                current_range_bytes(RangeIndexDirection::Desc, &left)
                    > current_range_bytes(RangeIndexDirection::Desc, &right),
                "descending bytes must reverse {left:?} < {right:?}"
            );
        }
    }

    #[test]
    fn regression_secondary_range_encoding_handles_float_boundaries() {
        let ordered = [
            f64::NEG_INFINITY,
            -1.0e100,
            -10.0,
            -1.0,
            -f64::MIN_POSITIVE,
            -0.0,
            f64::from_bits(1),
            f64::MIN_POSITIVE,
            1.0,
            10.0,
            1.0e100,
            f64::INFINITY,
        ];
        for pair in ordered.windows(2) {
            let left = PropertyValue::F64(pair[0]);
            let right = PropertyValue::F64(pair[1]);
            assert!(
                left.compare(&right) == Some(std::cmp::Ordering::Less),
                "fixture must be in semantic order: {left:?} < {right:?}"
            );
            assert!(
                current_range_bytes(RangeIndexDirection::Asc, &left)
                    < current_range_bytes(RangeIndexDirection::Asc, &right),
                "ascending bytes must preserve {left:?} < {right:?}"
            );
            assert!(
                current_range_bytes(RangeIndexDirection::Desc, &left)
                    > current_range_bytes(RangeIndexDirection::Desc, &right),
                "descending bytes must reverse {left:?} < {right:?}"
            );
        }
    }

    proptest! {
        #[test]
        fn regression_secondary_range_i64_encoding_is_monotonic(left in any::<i64>(), right in any::<i64>()) {
            let left_value = PropertyValue::I64(left);
            let right_value = PropertyValue::I64(right);
            prop_assert_eq!(
                current_range_bytes(RangeIndexDirection::Asc, &left_value)
                    .cmp(&current_range_bytes(RangeIndexDirection::Asc, &right_value)),
                left.cmp(&right),
            );
            prop_assert_eq!(
                current_range_bytes(RangeIndexDirection::Desc, &left_value)
                    .cmp(&current_range_bytes(RangeIndexDirection::Desc, &right_value)),
                right.cmp(&left),
            );
        }

        #[test]
        fn regression_secondary_range_f64_encoding_is_monotonic(
            left in any::<f64>().prop_filter("range values exclude NaN", |value| !value.is_nan()),
            right in any::<f64>().prop_filter("range values exclude NaN", |value| !value.is_nan()),
        ) {
            let left_value = PropertyValue::F64(left);
            let right_value = PropertyValue::F64(right);
            let semantic = left.partial_cmp(&right).expect("non-NaN values are comparable");
            prop_assert_eq!(
                current_range_bytes(RangeIndexDirection::Asc, &left_value)
                    .cmp(&current_range_bytes(RangeIndexDirection::Asc, &right_value)),
                semantic,
            );
            prop_assert_eq!(
                current_range_bytes(RangeIndexDirection::Desc, &left_value)
                    .cmp(&current_range_bytes(RangeIndexDirection::Desc, &right_value)),
                semantic.reverse(),
            );
        }
    }

    #[test]
    fn test_roaring_treemap_roundtrip() {
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(1);
        bitmap.insert(100);
        bitmap.insert(1000);

        let encoded = encode_roaring_treemap(&bitmap);
        let decoded = decode_roaring_treemap(&encoded).unwrap();

        assert_eq!(bitmap, decoded);
    }

    #[test]
    fn roaring_treemap_decode_rejects_malformed_bytes() {
        assert!(decode_roaring_treemap(b"not a bitmap").is_err());
    }

    #[test]
    fn property_value_index_strings_cover_all_variants() {
        let mut object = std::collections::BTreeMap::new();
        object.insert("nested".to_string(), PropertyValue::Bool(true));

        assert_eq!(property_value_to_index_string(&PropertyValue::Null), "null");
        assert_eq!(
            property_value_to_index_string(&PropertyValue::Bool(false)),
            "false"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::I64(42)),
            "00000000000000000042"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::DateTime(42)),
            PropertyValue::DateTime(42).to_index_string()
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::F64(1.5)),
            "+00001.500000000000000e0"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::F32(1.5)),
            "+00001.500000000000000e0"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::String("hello".to_string())),
            "hello"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::Bytes(vec![1, 2])),
            "<bytes:[1, 2]>"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::I64Array(vec![1, 2])),
            "<i64[2]>"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::F64Array(vec![1.0])),
            "<f64[1]>"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::F32Array(vec![1.0, 2.0])),
            "<f32[2]>"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::StringArray(vec!["a".to_string()])),
            "<str[1]>"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::Array(vec![PropertyValue::Null])),
            "<array[1]>"
        );
        assert_eq!(
            property_value_to_index_string(&PropertyValue::Object(object)),
            "<object[1]>"
        );
    }

    #[test]
    fn property_value_hashes_and_type_names_cover_all_variants() {
        let mut object = std::collections::BTreeMap::new();
        object.insert("nested".to_string(), PropertyValue::I64(7));

        let values = [
            (PropertyValue::Null, "Null"),
            (PropertyValue::Bool(true), "Bool"),
            (PropertyValue::I64(42), "I64"),
            (PropertyValue::DateTime(42), "DateTime"),
            (PropertyValue::F64(1.5), "F64"),
            (PropertyValue::F32(1.5), "F32"),
            (PropertyValue::String("hello".to_string()), "String"),
            (PropertyValue::Bytes(vec![1, 2]), "Bytes"),
            (PropertyValue::I64Array(vec![1, 2]), "I64Array"),
            (PropertyValue::F64Array(vec![1.0, 2.0]), "F64Array"),
            (PropertyValue::F32Array(vec![1.0, 2.0]), "F32Array"),
            (
                PropertyValue::StringArray(vec!["a".to_string(), "b".to_string()]),
                "StringArray",
            ),
            (
                PropertyValue::Array(vec![PropertyValue::Bool(false)]),
                "Array",
            ),
            (PropertyValue::Object(object), "Object"),
        ];

        for (value, type_name) in values {
            assert_eq!(property_value_type_name(&value), type_name);
            assert_eq!(
                hash_property_value_component(&value),
                hash_property_value_component(&value)
            );
        }

        assert_ne!(
            hash_property_value_component(&PropertyValue::Bool(true)),
            hash_property_value_component(&PropertyValue::I64(1))
        );
        assert_ne!(
            hash_property_value_component(&PropertyValue::F64(1.0)),
            hash_property_value_component(&PropertyValue::F32(1.0))
        );
    }

    #[test]
    fn secondary_indexable_values_reject_only_nested_array_and_object() {
        assert!(property_value_is_secondary_indexable(&PropertyValue::Null));
        assert!(property_value_is_secondary_indexable(
            &PropertyValue::StringArray(vec!["a".to_string()])
        ));
        assert!(!property_value_is_secondary_indexable(
            &PropertyValue::Array(vec![PropertyValue::Bool(true)])
        ));
        assert!(!property_value_is_secondary_indexable(
            &PropertyValue::Object(std::collections::BTreeMap::new())
        ));
    }

    #[tokio::test]
    async fn node_equality_index_add_lookup_prefix_filter_and_remove_contracts() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-node-equality-contracts".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");
        let property = scoped_secondary_index_property("User", "status");

        add_to_equality_index(&txn, &property, "active", 2)
            .await
            .expect("add active node 2");
        add_to_equality_index(&txn, &property, "active", 1)
            .await
            .expect("add active node 1");
        add_to_equality_index(&txn, &property, "inactive", 3)
            .await
            .expect("add inactive node");

        assert_eq!(
            lookup_equality_index(&txn, &property, "active")
                .await
                .expect("active lookup succeeds"),
            vec![1, 2]
        );
        assert_eq!(
            lookup_equality_index_set(&txn, &property, "missing")
                .await
                .expect("missing lookup succeeds")
                .len(),
            0
        );
        assert!(
            scan_equality_index_property_prefix_limited(&txn, &property, 0)
                .await
                .expect("zero limit scan succeeds")
                .is_empty()
        );

        let mut filter = RoaringTreemap::new();
        filter.insert(2);
        filter.insert(3);
        let filtered = scan_equality_index_property_prefix_limited_filtered(
            &txn,
            &property,
            10,
            Some(&filter),
        )
        .await
        .expect("filtered prefix scan succeeds");
        assert!(filtered.contains(2));
        assert!(filtered.contains(3));
        assert_eq!(filtered.len(), 2);

        remove_from_equality_index(&txn, &property, "active", 2)
            .await
            .expect("remove active node 2");
        assert_eq!(
            lookup_equality_index(&txn, &property, "active")
                .await
                .expect("active lookup after first remove succeeds"),
            vec![1]
        );
        remove_from_equality_index(&txn, &property, "active", 1)
            .await
            .expect("remove active node 1");
        assert!(lookup_equality_index(&txn, &property, "active")
            .await
            .expect("active lookup after all removes succeeds")
            .is_empty());
    }

    #[tokio::test]
    async fn node_equality_index_is_tenant_scoped_and_reports_malformed_bitmaps() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-node-equality-tenant-and-malformed".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");
        let tenant_a =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000A")
                .expect("valid tenant");
        let tenant_b =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000B")
                .expect("valid tenant");
        let scope_a = DataScope::Tenant(tenant_a);
        let scope_b = DataScope::Tenant(tenant_b);

        add_to_equality_index_scoped(&txn, "status", "active", 10, scope_a)
            .await
            .expect("tenant a equality write succeeds");
        add_to_equality_index_scoped(&txn, "status", "active", 20, scope_b)
            .await
            .expect("tenant b equality write succeeds");

        assert_eq!(
            lookup_equality_index_scoped(&txn, "status", "active", scope_a)
                .await
                .expect("tenant a lookup succeeds"),
            vec![10]
        );
        assert_eq!(
            lookup_equality_index_scoped(&txn, "status", "active", scope_b)
                .await
                .expect("tenant b lookup succeeds"),
            vec![20]
        );
        assert!(lookup_equality_index(&txn, "status", "active")
            .await
            .expect("legacy lookup succeeds")
            .is_empty());

        let malformed_key = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
                hash_property_name("broken"),
                hash_property_value("value"),
            ))),
        }
        .to_bytes();
        txn.put(&malformed_key, Bytes::from_static(b"not a roaring bitmap"))
            .expect("malformed bitmap write succeeds");

        assert!(lookup_equality_index(&txn, "broken", "value")
            .await
            .expect_err("malformed bitmap should fail")
            .to_string()
            .contains("Failed to decode RoaringTreemap"));
    }

    #[tokio::test]
    async fn concurrent_bitmap_inserts_cover_every_family_and_tenant_scope() {
        const INSERTS_PER_TENANT: u64 = 16;

        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-concurrent-bitmap-families".to_string(),
        })
        .await
        .expect("db opens");
        let tenant_a =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000A")
                .expect("valid tenant");
        let tenant_b =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000B")
                .expect("valid tenant");
        let normal_property = scoped_secondary_index_property("User", "status");

        for (scope, node_start, edge_start) in [
            (DataScope::Tenant(tenant_a), 1_000_u64, 10_000_u64),
            (DataScope::Tenant(tenant_b), 2_000_u64, 20_000_u64),
        ] {
            let mut transactions = Vec::new();
            for offset in 0..INSERTS_PER_TENANT {
                let node_id = node_start + offset;
                let edge_id = edge_start + offset;
                let txn = db
                    .inner_db()
                    .begin(IsolationLevel::SerializableSnapshot)
                    .await
                    .expect("transaction starts");

                add_to_equality_index_scoped(&txn, "$label", "User", node_id, scope)
                    .await
                    .expect("node label insert succeeds");
                add_to_equality_index_scoped(&txn, &normal_property, "active", node_id, scope)
                    .await
                    .expect("node equality insert succeeds");
                add_to_edge_label_index_scoped(&txn, 31, node_id + 30_000, "FOLLOWS", scope)
                    .await
                    .expect("outgoing edge-label insert succeeds");
                add_to_edge_label_index_scoped(&txn, node_id + 40_000, 32, "FOLLOWS", scope)
                    .await
                    .expect("incoming edge-label insert succeeds");
                add_to_edge_equality_index_scoped(&txn, 41, 42, edge_id, "status", "active", scope)
                    .await
                    .expect("edge equality insert succeeds");
                add_to_global_edge_label_index_scoped(&txn, "FOLLOWS", edge_id, scope)
                    .await
                    .expect("global edge-label insert succeeds");
                add_to_edge_pair_index_scoped(&txn, 51, 52, edge_id, scope)
                    .await
                    .expect("edge-pair insert succeeds");
                transactions.push(txn);
            }

            for result in
                futures::future::join_all(transactions.into_iter().map(|txn| txn.commit())).await
            {
                result.expect("commutative bitmap transaction commits");
            }
        }

        let read = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("read transaction starts");
        for (scope, node_start, edge_start) in [
            (DataScope::Tenant(tenant_a), 1_000_u64, 10_000_u64),
            (DataScope::Tenant(tenant_b), 2_000_u64, 20_000_u64),
        ] {
            let expected_nodes = (node_start..node_start + INSERTS_PER_TENANT).collect::<Vec<_>>();
            let expected_edges = (edge_start..edge_start + INSERTS_PER_TENANT).collect::<Vec<_>>();

            assert_eq!(
                lookup_equality_index_scoped(&read, "$label", "User", scope)
                    .await
                    .expect("node-label lookup succeeds"),
                expected_nodes,
            );
            assert_eq!(
                lookup_equality_index_scoped(&read, &normal_property, "active", scope)
                    .await
                    .expect("node-equality lookup succeeds"),
                expected_nodes,
            );
            assert_eq!(
                lookup_out_neighbors_by_label_scoped(&read, 31, "FOLLOWS", scope)
                    .await
                    .expect("outgoing edge-label lookup succeeds")
                    .iter()
                    .collect::<Vec<_>>(),
                expected_nodes
                    .iter()
                    .map(|node_id| node_id + 30_000)
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                lookup_in_neighbors_by_label_scoped(&read, 32, "FOLLOWS", scope)
                    .await
                    .expect("incoming edge-label lookup succeeds")
                    .iter()
                    .collect::<Vec<_>>(),
                expected_nodes
                    .iter()
                    .map(|node_id| node_id + 40_000)
                    .collect::<Vec<_>>(),
            );
            assert_eq!(
                lookup_edges_out_by_equality_scoped(&read, 41, "status", "active", scope,)
                    .await
                    .expect("outgoing edge-equality lookup succeeds")
                    .iter()
                    .collect::<Vec<_>>(),
                expected_edges,
            );
            assert_eq!(
                lookup_edges_in_by_equality_scoped(&read, 42, "status", "active", scope)
                    .await
                    .expect("incoming edge-equality lookup succeeds")
                    .iter()
                    .collect::<Vec<_>>(),
                expected_edges,
            );
            assert_eq!(
                lookup_global_edge_equality_index_scoped(&read, "status", "active", scope,)
                    .await
                    .expect("global edge-equality lookup succeeds")
                    .iter()
                    .collect::<Vec<_>>(),
                expected_edges,
            );
            assert_eq!(
                lookup_global_edge_label_index_scoped(&read, "FOLLOWS", scope)
                    .await
                    .expect("global edge-label lookup succeeds")
                    .iter()
                    .collect::<Vec<_>>(),
                expected_edges,
            );
            assert_eq!(
                lookup_edge_pair_index_scoped(&read, 51, 52, scope)
                    .await
                    .expect("edge-pair lookup succeeds")
                    .iter()
                    .collect::<Vec<_>>(),
                expected_edges,
            );
        }
        assert!(
            lookup_equality_index(&read, "$label", "User")
                .await
                .expect("legacy node-label lookup succeeds")
                .is_empty(),
            "tenant writes must not leak into the legacy scope",
        );
    }

    #[tokio::test]
    async fn bitmap_insert_and_remove_races_conflict_in_both_commit_orders() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-bitmap-insert-remove-races".to_string(),
        })
        .await
        .expect("db opens");

        for (value, insert_commits_first) in [("insert-first", true), ("remove-first", false)] {
            let seed = db
                .inner_db()
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("seed transaction starts");
            add_to_equality_index(&seed, "race", value, 1)
                .await
                .expect("seed insert succeeds");
            seed.commit().await.expect("seed transaction commits");

            let insert = db
                .inner_db()
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("insert transaction starts");
            let remove = db
                .inner_db()
                .begin(IsolationLevel::SerializableSnapshot)
                .await
                .expect("remove transaction starts");
            add_to_equality_index(&insert, "race", value, 2)
                .await
                .expect("racing insert buffers");
            remove_from_equality_index(&remove, "race", value, 1)
                .await
                .expect("racing removal buffers");

            if insert_commits_first {
                insert.commit().await.expect("insert commits first");
                assert!(
                    remove.commit().await.is_err(),
                    "read-modify-write removal must conflict with a committed insert",
                );
            } else {
                remove.commit().await.expect("removal commits first");
                assert!(
                    insert.commit().await.is_err(),
                    "insert must conflict with a committed read-modify-write removal",
                );
            }
        }

        let read = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("read transaction starts");
        assert_eq!(
            lookup_equality_index(&read, "race", "insert-first")
                .await
                .expect("insert-first lookup succeeds"),
            vec![1, 2],
        );
        assert!(lookup_equality_index(&read, "race", "remove-first")
            .await
            .expect("remove-first lookup succeeds")
            .is_empty(),);
    }

    #[tokio::test]
    async fn node_range_index_scans_bounds_limits_filters_and_deletes() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-node-range-contracts".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");
        let property = scoped_secondary_index_property("User", "score");

        add_to_range_index(&txn, &property, "001", 1)
            .await
            .expect("add score 1");
        add_to_range_index(&txn, &property, "002", 2)
            .await
            .expect("add score 2");
        add_to_range_index(&txn, &property, "003", 3)
            .await
            .expect("add score 3");

        assert_eq!(
            scan_range_index(&txn, RangeIndexDirection::Asc, &property)
                .await
                .expect("full range scan succeeds"),
            vec![1, 2, 3]
        );
        assert_eq!(
            scan_range_index_with_direction_limited(
                &txn,
                &property,
                RangeIndexDirection::Asc,
                Some(2),
            )
            .await
            .expect("limited range scan succeeds"),
            vec![1, 2]
        );
        assert_eq!(
            scan_range_index_bounded(&txn, &property, RangeQuery::Gt("001"))
                .await
                .expect("gt scan succeeds"),
            vec![2, 3]
        );
        assert_eq!(
            scan_range_index_bounded(&txn, &property, RangeQuery::Gte("002"))
                .await
                .expect("gte scan succeeds"),
            vec![2, 3]
        );
        assert_eq!(
            scan_range_index_bounded(&txn, &property, RangeQuery::Lt("003"))
                .await
                .expect("lt scan succeeds"),
            vec![1, 2]
        );
        assert_eq!(
            scan_range_index_bounded(&txn, &property, RangeQuery::Lte("002"))
                .await
                .expect("lte scan succeeds"),
            vec![1, 2]
        );
        assert_eq!(
            scan_range_index_bounded(
                &txn,
                &property,
                RangeQuery::BetweenBounds {
                    min: "001",
                    min_inclusive: false,
                    max: "003",
                    max_inclusive: false,
                },
            )
            .await
            .expect("exclusive between scan succeeds"),
            vec![2]
        );

        let mut filter = RoaringTreemap::new();
        filter.insert(1);
        filter.insert(3);
        let filtered = scan_range_index_prefix_limited(&txn, &property, 10, Some(&filter))
            .await
            .expect("filtered range prefix scan succeeds");
        assert!(filtered.contains(1));
        assert!(filtered.contains(3));
        assert_eq!(filtered.len(), 2);
        assert!(scan_range_index_prefix_limited(&txn, &property, 0, None)
            .await
            .expect("zero limit prefix scan succeeds")
            .is_empty());
        let bounded_filtered = scan_range_index_bounded_limited(
            &txn,
            &property,
            RangeQuery::Between("001", "003"),
            1,
            Some(&filter),
        )
        .await
        .expect("bounded filtered range scan succeeds");
        assert_eq!(bounded_filtered.len(), 1);
        assert!(bounded_filtered.contains(1));
        assert!(scan_range_index_bounded_limited(
            &txn,
            &property,
            RangeQuery::Between("001", "003"),
            0,
            None
        )
        .await
        .expect("zero limit bounded scan succeeds")
        .is_empty());

        remove_from_range_index(&txn, &property, "002", 2)
            .await
            .expect("remove range row succeeds");
        assert_eq!(
            scan_range_index_bounded(&txn, &property, RangeQuery::Between("001", "003"))
                .await
                .expect("range scan after remove succeeds"),
            vec![1, 3]
        );
        delete_range_index_entries_for_property(&txn, &property)
            .await
            .expect("delete range entries succeeds");
        assert!(scan_range_index(&txn, RangeIndexDirection::Asc, &property)
            .await
            .expect("range scan after delete succeeds")
            .is_empty());
    }

    #[test]
    fn range_bound_helpers_cover_every_query_direction_and_endpoint_shape() {
        let queries = [
            RangeQuery::Gt("001"),
            RangeQuery::Gte("001"),
            RangeQuery::Lt("003"),
            RangeQuery::Lte("003"),
            RangeQuery::Between("001", "003"),
            RangeQuery::BetweenBounds {
                min: "001",
                min_inclusive: false,
                max: "003",
                max_inclusive: false,
            },
            RangeQuery::BetweenBounds {
                min: "001",
                min_inclusive: true,
                max: "003",
                max_inclusive: true,
            },
        ];
        let property = "weight";
        let property_hash = hash_property_name(property);

        for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
            let node_prefix = RangeScanPrefix::Property {
                direction,
                property_hash,
            }
            .to_bytes();
            let global_prefix = global_edge_range_prefix_with_direction(property, direction);
            for query in &queries {
                let (start, end) =
                    range_scan_bounds_with_direction(&node_prefix, property_hash, query, direction);
                assert!(start < end);

                let (start, end) = global_edge_range_scan_bounds_with_direction(
                    &global_prefix,
                    property,
                    query,
                    direction,
                );
                assert!(start < end);

                for outgoing in [true, false] {
                    let edge_direction = if outgoing {
                        EdgeRangeDirection::Out
                    } else {
                        EdgeRangeDirection::In
                    };
                    let edge_prefix =
                        edge_range_prefix_with_direction(edge_direction, 7, property, direction);
                    let (start, end) = edge_range_scan_bounds_with_direction(
                        &edge_prefix,
                        7,
                        property,
                        query,
                        outgoing,
                        direction,
                    );
                    assert!(start < end);
                }
            }
        }
    }

    #[tokio::test]
    async fn node_desc_range_index_scans_bounds_and_tenant_scopes() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-node-desc-range-contracts".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");
        let tenant =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000A")
                .expect("valid tenant");
        let scope = DataScope::Tenant(tenant);

        add_to_range_index_with_direction_scoped(
            &txn,
            "score",
            "001",
            1,
            RangeIndexDirection::Desc,
            scope,
        )
        .await
        .expect("add tenant desc score 1");
        add_to_range_index_with_direction_scoped(
            &txn,
            "score",
            "002",
            2,
            RangeIndexDirection::Desc,
            scope,
        )
        .await
        .expect("add tenant desc score 2");
        add_to_range_index_with_direction_scoped(
            &txn,
            "score",
            "003",
            3,
            RangeIndexDirection::Desc,
            scope,
        )
        .await
        .expect("add tenant desc score 3");

        assert_eq!(
            scan_range_index_scoped(&txn, RangeIndexDirection::Desc, "score", scope)
                .await
                .expect("tenant desc scan succeeds"),
            vec![3, 2, 1]
        );
        assert_eq!(
            scan_range_index_bounded_with_direction_limited_scoped(
                &txn,
                "score",
                RangeQuery::Between("001", "003"),
                RangeIndexDirection::Desc,
                Some(2),
                scope,
            )
            .await
            .expect("bounded tenant desc scan succeeds"),
            vec![3, 2]
        );
        assert_eq!(
            scan_range_index_bounded_with_direction_limited_scoped(
                &txn,
                "score",
                RangeQuery::Gt("001"),
                RangeIndexDirection::Desc,
                None,
                scope,
            )
            .await
            .expect("desc gt scan succeeds"),
            vec![3, 2]
        );
        assert_eq!(
            scan_range_index_bounded_with_direction_limited_scoped(
                &txn,
                "score",
                RangeQuery::Gte("002"),
                RangeIndexDirection::Desc,
                None,
                scope,
            )
            .await
            .expect("desc gte scan succeeds"),
            vec![3, 2]
        );
        assert_eq!(
            scan_range_index_bounded_with_direction_limited_scoped(
                &txn,
                "score",
                RangeQuery::Lt("003"),
                RangeIndexDirection::Desc,
                None,
                scope,
            )
            .await
            .expect("desc lt scan succeeds"),
            vec![2, 1]
        );
        assert_eq!(
            scan_range_index_bounded_with_direction_limited_scoped(
                &txn,
                "score",
                RangeQuery::Lte("002"),
                RangeIndexDirection::Desc,
                None,
                scope,
            )
            .await
            .expect("desc lte scan succeeds"),
            vec![2, 1]
        );
        assert_eq!(
            scan_range_index_bounded_with_direction_limited_scoped(
                &txn,
                "score",
                RangeQuery::BetweenBounds {
                    min: "001",
                    min_inclusive: false,
                    max: "003",
                    max_inclusive: false,
                },
                RangeIndexDirection::Desc,
                None,
                scope,
            )
            .await
            .expect("desc exclusive bounded scan succeeds"),
            vec![2]
        );
        assert!(scan_range_index(&txn, RangeIndexDirection::Desc, "score")
            .await
            .expect("legacy desc scan succeeds")
            .is_empty());

        remove_from_range_index_with_direction_scoped(
            &txn,
            "score",
            "003",
            3,
            RangeIndexDirection::Desc,
            scope,
        )
        .await
        .expect("remove tenant desc score 3");
        assert_eq!(
            scan_range_index_scoped(&txn, RangeIndexDirection::Desc, "score", scope)
                .await
                .expect("tenant desc scan after remove succeeds"),
            vec![2, 1]
        );
    }

    #[tokio::test]
    async fn edge_equality_and_pair_indexes_add_lookup_remove_and_scope() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-edge-equality-contracts".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");
        let property = scoped_secondary_index_property("FOLLOWS", "status");

        add_to_edge_equality_index(&txn, 1, 2, 100, &property, "active")
            .await
            .expect("add edge 100 equality");
        add_to_edge_equality_index(&txn, 1, 3, 101, &property, "active")
            .await
            .expect("add edge 101 equality");
        add_to_edge_equality_index(&txn, 1, 4, 102, &property, "inactive")
            .await
            .expect("add edge 102 equality");

        let active_out = lookup_edges_out_by_equality(&txn, 1, &property, "active")
            .await
            .expect("out equality lookup succeeds");
        assert!(active_out.contains(100));
        assert!(active_out.contains(101));
        assert_eq!(active_out.len(), 2);
        let active_in = lookup_edges_in_by_equality(&txn, 2, &property, "active")
            .await
            .expect("in equality lookup succeeds");
        assert!(active_in.contains(100));
        assert_eq!(active_in.len(), 1);
        let global_active = lookup_global_edge_equality_index(&txn, &property, "active")
            .await
            .expect("global equality lookup succeeds");
        assert!(global_active.contains(100));
        assert!(global_active.contains(101));
        assert_eq!(global_active.len(), 2);
        let any_status = scan_edges_out_by_equality_property_prefix(&txn, 1, &property)
            .await
            .expect("out equality property prefix scan succeeds");
        assert!(any_status.contains(100));
        assert!(any_status.contains(101));
        assert!(any_status.contains(102));
        assert_eq!(any_status.len(), 3);

        remove_from_edge_equality_index(&txn, 1, 2, 100, &property, "active")
            .await
            .expect("remove edge 100 equality");
        assert!(!lookup_edges_out_by_equality(&txn, 1, &property, "active")
            .await
            .expect("out equality after remove succeeds")
            .contains(100));
        assert!(
            !lookup_global_edge_equality_index(&txn, &property, "active")
                .await
                .expect("global equality after remove succeeds")
                .contains(100)
        );

        add_to_edge_pair_index(&txn, 1, 2, 100)
            .await
            .expect("add edge pair 100");
        add_to_edge_pair_index(&txn, 1, 2, 103)
            .await
            .expect("add edge pair 103");
        let pair = lookup_edge_pair_index(&txn, 1, 2)
            .await
            .expect("edge pair lookup succeeds");
        assert!(pair.contains(100));
        assert!(pair.contains(103));
        assert_eq!(pair.len(), 2);
        remove_from_edge_pair_index(&txn, 1, 2, 100)
            .await
            .expect("remove edge pair 100");
        assert!(!lookup_edge_pair_index(&txn, 1, 2)
            .await
            .expect("edge pair after remove succeeds")
            .contains(100));

        let tenant =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000A")
                .expect("valid tenant");
        let scope = DataScope::Tenant(tenant);
        add_to_edge_equality_index_scoped(&txn, 1, 2, 200, "status", "active", scope)
            .await
            .expect("tenant edge equality write succeeds");
        assert!(
            lookup_edges_out_by_equality_scoped(&txn, 1, "status", "active", scope)
                .await
                .expect("tenant edge equality lookup succeeds")
                .contains(200)
        );
        assert!(!lookup_edges_out_by_equality(&txn, 1, "status", "active")
            .await
            .expect("legacy edge equality lookup succeeds")
            .contains(200));
    }

    #[tokio::test]
    async fn edge_range_indexes_scan_bounds_limits_global_and_remove() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-edge-range-contracts".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");
        let property = scoped_secondary_index_property("FOLLOWS", "weight");

        add_to_edge_range_index(&txn, 1, 9, 100, &property, "001")
            .await
            .expect("add edge 100 range");
        add_to_edge_range_index(&txn, 1, 9, 101, &property, "002")
            .await
            .expect("add edge 101 range");
        add_to_edge_range_index(&txn, 1, 9, 102, &property, "003")
            .await
            .expect("add edge 102 range");

        assert_eq!(
            scan_edge_range_index_out(&txn, 1, &property, RangeQuery::Between("001", "003"))
                .await
                .expect("out range scan succeeds"),
            vec![100, 101, 102]
        );
        assert_eq!(
            scan_edge_range_index_in(&txn, 9, &property, RangeQuery::Gte("002"))
                .await
                .expect("in range scan succeeds"),
            vec![101, 102]
        );
        assert_eq!(
            scan_edge_range_index_out_prefix(&txn, 1, &property)
                .await
                .expect("out range prefix succeeds"),
            vec![100, 101, 102]
        );
        assert_eq!(
            scan_global_edge_range_index_all_with_direction_limited(
                &txn,
                &property,
                RangeIndexDirection::Asc,
                Some(2),
            )
            .await
            .expect("global all limited succeeds"),
            vec![100, 101]
        );
        assert_eq!(
            scan_global_edge_range_index_with_direction_limited(
                &txn,
                &property,
                RangeQuery::BetweenBounds {
                    min: "001",
                    min_inclusive: false,
                    max: "003",
                    max_inclusive: false,
                },
                RangeIndexDirection::Asc,
                None,
            )
            .await
            .expect("global bounded exclusive succeeds"),
            vec![101]
        );
        assert_eq!(
            scan_edge_range_index_out_with_direction(
                &txn,
                1,
                &property,
                RangeQuery::Gt("001"),
                RangeIndexDirection::Asc,
            )
            .await
            .unwrap(),
            vec![101, 102]
        );
        assert_eq!(
            scan_edge_range_index_out_with_direction(
                &txn,
                1,
                &property,
                RangeQuery::Lte("002"),
                RangeIndexDirection::Asc,
            )
            .await
            .unwrap(),
            vec![100, 101]
        );
        assert_eq!(
            scan_global_edge_range_index_with_direction_limited(
                &txn,
                &property,
                RangeQuery::Gte("002"),
                RangeIndexDirection::Asc,
                None,
            )
            .await
            .unwrap(),
            vec![101, 102]
        );
        assert_eq!(
            scan_global_edge_range_index_with_direction_limited(
                &txn,
                &property,
                RangeQuery::Lt("003"),
                RangeIndexDirection::Asc,
                None,
            )
            .await
            .unwrap(),
            vec![100, 101]
        );

        add_to_edge_range_index_with_direction(
            &txn,
            1,
            9,
            200,
            &property,
            "001",
            RangeIndexDirection::Desc,
        )
        .await
        .expect("add edge 200 desc range");
        add_to_edge_range_index_with_direction(
            &txn,
            1,
            9,
            201,
            &property,
            "002",
            RangeIndexDirection::Desc,
        )
        .await
        .expect("add edge 201 desc range");
        add_to_edge_range_index_with_direction(
            &txn,
            1,
            9,
            202,
            &property,
            "003",
            RangeIndexDirection::Desc,
        )
        .await
        .expect("add edge 202 desc range");
        assert_eq!(
            scan_edge_range_index_out_with_direction(
                &txn,
                1,
                &property,
                RangeQuery::Between("001", "003"),
                RangeIndexDirection::Desc,
            )
            .await
            .expect("desc out range scan succeeds"),
            vec![202, 201, 200]
        );
        assert_eq!(
            scan_global_edge_range_index_with_direction_limited(
                &txn,
                &property,
                RangeQuery::Lte("002"),
                RangeIndexDirection::Desc,
                Some(1),
            )
            .await
            .expect("desc global limited scan succeeds"),
            vec![201]
        );
        assert_eq!(
            scan_edge_range_index_out_with_direction(
                &txn,
                1,
                &property,
                RangeQuery::Gt("001"),
                RangeIndexDirection::Desc,
            )
            .await
            .unwrap(),
            vec![202, 201]
        );
        assert_eq!(
            scan_edge_range_index_in_with_direction(
                &txn,
                9,
                &property,
                RangeQuery::Gte("002"),
                RangeIndexDirection::Desc,
            )
            .await
            .unwrap(),
            vec![202, 201]
        );
        assert_eq!(
            scan_edge_range_index_out_with_direction(
                &txn,
                1,
                &property,
                RangeQuery::Lt("003"),
                RangeIndexDirection::Desc,
            )
            .await
            .unwrap(),
            vec![201, 200]
        );
        assert_eq!(
            scan_global_edge_range_index_with_direction_limited(
                &txn,
                &property,
                RangeQuery::BetweenBounds {
                    min: "001",
                    min_inclusive: false,
                    max: "003",
                    max_inclusive: false,
                },
                RangeIndexDirection::Desc,
                None,
            )
            .await
            .unwrap(),
            vec![201]
        );

        remove_from_edge_range_index(&txn, 1, 9, 101, &property, "002")
            .await
            .expect("remove asc edge 101 range");
        assert_eq!(
            scan_edge_range_index_out(&txn, 1, &property, RangeQuery::Between("001", "003"))
                .await
                .expect("out range scan after remove succeeds"),
            vec![100, 102]
        );
        assert!(!scan_global_edge_range_index_all_with_direction_limited(
            &txn,
            &property,
            RangeIndexDirection::Asc,
            None,
        )
        .await
        .expect("global all after remove succeeds")
        .contains(&101));
        delete_edge_range_index_entries_for_property(&txn, &property)
            .await
            .expect("delete edge range rows succeeds");
        assert!(
            scan_edge_range_index_out(&txn, 1, &property, RangeQuery::Between("001", "003"))
                .await
                .expect("out range scan after delete succeeds")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn global_edge_secondary_cleanup_preserves_direction_and_unrelated_properties() {
        const ZERO_PREFIX_ID: u64 = 0x0000_0000_0000_0001;
        const ASCII_PREFIX_ID: u64 = 0x4100_0000_0000_0001;
        const FF_PREFIX_ID: u64 = 0xFF00_0000_0000_0001;

        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-global-edge-secondary-cleanup".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");

        for (value, edge_id) in [
            ("", ZERO_PREFIX_ID),
            ("\0", ASCII_PREFIX_ID),
            ("a\0", FF_PREFIX_ID),
        ] {
            add_to_edge_equality_index(&txn, 1, 2, edge_id, "target", value)
                .await
                .expect("target edge equality row stages");
            add_to_edge_range_index_with_direction(
                &txn,
                1,
                2,
                edge_id,
                "target",
                value,
                RangeIndexDirection::Asc,
            )
            .await
            .expect("target ascending edge range row stages");
        }
        add_to_edge_equality_index(&txn, 1, 2, 7, "other", "a\0")
            .await
            .expect("unrelated edge equality row stages");
        add_to_edge_range_index_with_direction(
            &txn,
            1,
            2,
            7,
            "other",
            "a\0",
            RangeIndexDirection::Asc,
        )
        .await
        .expect("unrelated ascending edge range row stages");
        add_to_edge_range_index_with_direction(
            &txn,
            1,
            2,
            FF_PREFIX_ID,
            "target",
            "descending",
            RangeIndexDirection::Desc,
        )
        .await
        .expect("target descending edge range row stages");

        delete_edge_equality_index_entries_for_property(&txn, "target")
            .await
            .expect("directional equality cleanup succeeds");
        assert!(lookup_edges_out_by_equality(&txn, 1, "target", "\0")
            .await
            .expect("directional equality reads")
            .is_empty());
        assert!(
            lookup_global_edge_equality_index(&txn, "target", "\0")
                .await
                .expect("global equality reads")
                .contains(ASCII_PREFIX_ID),
            "directional cleanup must retain its existing global-lane contract"
        );
        delete_global_edge_equality_index_entries_for_property(&txn, "target")
            .await
            .expect("global equality cleanup succeeds");
        for value in ["", "\0", "a\0"] {
            assert!(lookup_global_edge_equality_index(&txn, "target", value)
                .await
                .expect("cleaned global equality reads")
                .is_empty());
        }
        assert!(lookup_global_edge_equality_index(&txn, "other", "a\0")
            .await
            .expect("unrelated global equality reads")
            .contains(7));

        delete_edge_range_index_entries_for_property_with_direction(
            &txn,
            "target",
            RangeIndexDirection::Asc,
        )
        .await
        .expect("directional ascending range cleanup succeeds");
        assert_eq!(
            scan_global_edge_range_index_all_with_direction_limited(
                &txn,
                "target",
                RangeIndexDirection::Asc,
                None,
            )
            .await
            .expect("global ascending range reads")
            .len(),
            3,
            "directional cleanup must retain its existing global-lane contract"
        );
        delete_global_edge_range_index_entries_for_property_with_direction(
            &txn,
            "target",
            RangeIndexDirection::Asc,
        )
        .await
        .expect("global ascending range cleanup succeeds");
        assert!(scan_global_edge_range_index_all_with_direction_limited(
            &txn,
            "target",
            RangeIndexDirection::Asc,
            None,
        )
        .await
        .expect("cleaned global ascending range reads")
        .is_empty());
        assert_eq!(
            scan_global_edge_range_index_all_with_direction_limited(
                &txn,
                "target",
                RangeIndexDirection::Desc,
                None,
            )
            .await
            .expect("opposite global range direction reads"),
            vec![FF_PREFIX_ID]
        );
        assert_eq!(
            scan_global_edge_range_index_all_with_direction_limited(
                &txn,
                "other",
                RangeIndexDirection::Asc,
                None,
            )
            .await
            .expect("unrelated global range reads"),
            vec![7]
        );
    }

    #[tokio::test]
    async fn bulk_index_cleanup_and_global_label_removal_preserve_unrelated_rows() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-bulk-index-cleanup".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");

        add_to_equality_index(&txn, "status", "active", 1)
            .await
            .unwrap();
        add_to_equality_index(&txn, "other", "active", 2)
            .await
            .unwrap();
        delete_equality_index_entries_for_property(&txn, "status")
            .await
            .unwrap();
        assert!(lookup_equality_index(&txn, "status", "active")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            lookup_equality_index(&txn, "other", "active")
                .await
                .unwrap(),
            vec![2]
        );

        add_to_range_index(&txn, "score", "001", 1).await.unwrap();
        add_to_range_index_with_direction(&txn, "score", "002", 2, RangeIndexDirection::Desc)
            .await
            .unwrap();
        delete_range_index_entries_for_property(&txn, "score")
            .await
            .unwrap();
        assert!(scan_range_index(&txn, RangeIndexDirection::Asc, "score")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            scan_range_index(&txn, RangeIndexDirection::Desc, "score")
                .await
                .unwrap(),
            vec![2]
        );
        delete_range_index_entries_for_property_with_direction(
            &txn,
            "score",
            RangeIndexDirection::Desc,
        )
        .await
        .unwrap();
        assert!(scan_range_index(&txn, RangeIndexDirection::Desc, "score")
            .await
            .unwrap()
            .is_empty());

        add_to_edge_equality_index(&txn, 1, 2, 10, "status", "active")
            .await
            .unwrap();
        add_to_edge_equality_index(&txn, 1, 3, 11, "other", "active")
            .await
            .unwrap();
        delete_edge_equality_index_entries_for_property(&txn, "status")
            .await
            .unwrap();
        assert!(lookup_edges_out_by_equality(&txn, 1, "status", "active")
            .await
            .unwrap()
            .is_empty());
        assert!(lookup_global_edge_equality_index(&txn, "status", "active")
            .await
            .unwrap()
            .contains(10));
        assert!(lookup_global_edge_equality_index(&txn, "other", "active")
            .await
            .unwrap()
            .contains(11));

        add_to_edge_label_index(&txn, 1, 2, "FOLLOWS")
            .await
            .unwrap();
        remove_from_edge_label_index(&txn, 1, 2, "FOLLOWS")
            .await
            .unwrap();
        assert!(lookup_out_neighbors_by_label(&txn, 1, "FOLLOWS")
            .await
            .unwrap()
            .is_empty());
        assert!(lookup_in_neighbors_by_label(&txn, 2, "FOLLOWS")
            .await
            .unwrap()
            .is_empty());

        add_to_global_edge_label_index(&txn, "FOLLOWS", 20)
            .await
            .unwrap();
        add_to_global_edge_label_index(&txn, "FOLLOWS", 21)
            .await
            .unwrap();
        remove_from_global_edge_label_index(&txn, "FOLLOWS", 20)
            .await
            .unwrap();
        remove_from_global_edge_label_index(&txn, "FOLLOWS", 99)
            .await
            .unwrap();
        assert_eq!(
            lookup_global_edge_label_index(&txn, "FOLLOWS")
                .await
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![21]
        );

        add_to_equality_index(&txn, "$label", "User", 1)
            .await
            .unwrap();
        clear_node_label_indexes(&txn).await.unwrap();
        assert!(lookup_equality_index(&txn, "$label", "User")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            lookup_equality_index(&txn, "other", "active")
                .await
                .unwrap(),
            vec![2]
        );

        add_to_global_edge_label_index(&txn, "LIKES", 22)
            .await
            .unwrap();
        clear_global_edge_label_indexes(&txn).await.unwrap();
        assert!(lookup_global_edge_label_index(&txn, "FOLLOWS")
            .await
            .unwrap()
            .is_empty());
        assert!(lookup_global_edge_label_index(&txn, "LIKES")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn edge_label_indexes_use_hashed_encoding_keys() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-edge-label-hashed-keys".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");
        let label_hash = hash_property_value("FOLLOWS");

        add_to_edge_label_index(&txn, 1, 2, "FOLLOWS")
            .await
            .expect("neighbor index write succeeds");
        add_to_global_edge_label_index(&txn, "FOLLOWS", 99)
            .await
            .expect("global label index write succeeds");

        let out_key = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
                EdgeLabelNeighborKey::new(EdgeRangeDirection::Out, 1, label_hash),
            )),
        }
        .to_bytes();
        let in_key = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabelNeighbor(
                EdgeLabelNeighborKey::new(EdgeRangeDirection::In, 2, label_hash),
            )),
        }
        .to_bytes();
        let global_key = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(EdgeLabelKey::new(
                label_hash,
            ))),
        }
        .to_bytes();

        let mut expected_out_key = vec![0x03, 0x10, 0x00];
        expected_out_key.extend_from_slice(&1u64.to_be_bytes());
        expected_out_key.extend_from_slice(&label_hash);
        let mut expected_in_key = vec![0x03, 0x10, 0x01];
        expected_in_key.extend_from_slice(&2u64.to_be_bytes());
        expected_in_key.extend_from_slice(&label_hash);
        let mut expected_global_key = vec![0x03, 0x04];
        expected_global_key.extend_from_slice(&label_hash);

        assert_eq!(out_key.as_ref(), expected_out_key.as_slice());
        assert_eq!(in_key.as_ref(), expected_in_key.as_slice());
        assert_eq!(global_key.as_ref(), expected_global_key.as_slice());
        assert!(txn.get(&out_key).await.expect("out get succeeds").is_some());
        assert!(txn.get(&in_key).await.expect("in get succeeds").is_some());
        assert!(txn
            .get(&global_key)
            .await
            .expect("global get succeeds")
            .is_some());
        assert!(lookup_out_neighbors_by_label(&txn, 1, "FOLLOWS")
            .await
            .expect("out lookup succeeds")
            .contains(2));
        assert!(lookup_in_neighbors_by_label(&txn, 2, "FOLLOWS")
            .await
            .expect("in lookup succeeds")
            .contains(1));
        assert!(lookup_global_edge_label_index(&txn, "FOLLOWS")
            .await
            .expect("global lookup succeeds")
            .contains(99));
    }

    #[tokio::test]
    async fn edge_label_indexes_are_tenant_scoped() {
        let db = crate::HelixDB::open(crate::HelixDbSource::InMemory {
            database: "search-edge-label-tenant-scoped".to_string(),
        })
        .await
        .expect("db opens");
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("transaction starts");
        let tenant_a =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000A")
                .expect("valid tenant");
        let tenant_b =
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000B")
                .expect("valid tenant");
        let scope_a = DataScope::Tenant(tenant_a);
        let scope_b = DataScope::Tenant(tenant_b);

        add_to_edge_label_index_scoped(&txn, 1, 2, "FOLLOWS", scope_a)
            .await
            .expect("tenant neighbor index write succeeds");
        add_to_global_edge_label_index_scoped(&txn, "FOLLOWS", 99, scope_a)
            .await
            .expect("tenant global label index write succeeds");

        assert!(
            lookup_out_neighbors_by_label_scoped(&txn, 1, "FOLLOWS", scope_a)
                .await
                .expect("tenant a out lookup succeeds")
                .contains(2)
        );
        assert!(
            lookup_out_neighbors_by_label_scoped(&txn, 1, "FOLLOWS", scope_b)
                .await
                .expect("tenant b out lookup succeeds")
                .is_empty()
        );
        assert!(
            lookup_global_edge_label_index_scoped(&txn, "FOLLOWS", scope_a)
                .await
                .expect("tenant a global lookup succeeds")
                .contains(99)
        );
        assert!(
            lookup_global_edge_label_index_scoped(&txn, "FOLLOWS", scope_b)
                .await
                .expect("tenant b global lookup succeeds")
                .is_empty()
        );

        clear_global_edge_label_indexes_scoped(&txn, scope_b)
            .await
            .expect("tenant b clear succeeds");
        assert!(
            lookup_global_edge_label_index_scoped(&txn, "FOLLOWS", scope_a)
                .await
                .expect("tenant a global lookup succeeds after tenant b clear")
                .contains(99)
        );

        clear_global_edge_label_indexes_scoped(&txn, scope_a)
            .await
            .expect("tenant a clear succeeds");
        assert!(
            lookup_global_edge_label_index_scoped(&txn, "FOLLOWS", scope_a)
                .await
                .expect("tenant a global lookup succeeds after clear")
                .is_empty()
        );
    }
}
