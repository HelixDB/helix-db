//! V1 metadata keys for allocators, manifests, and scoped database state.

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;

use super::{tenant::DataScope, DataKeyKind, Key, KeyPrefix, PREFIX_LEN};

/// Key for next node ID high watermark (for lease-based allocation)
pub const NEXT_NODE_ID: &[u8] = b"next_node_id";
/// Key for next edge ID high watermark (for lease-based allocation)
pub const NEXT_EDGE_ID: &[u8] = b"next_edge_id";
const TEXT_INDEX_MANIFEST_PREFIX: &[u8] = b"text_manifest:";
const DYNAMIC_INDEX_PREFIX: &[u8] = b"dynamic_index:";
const WRITE_RECEIPT_PREFIX: &[u8] = b"write_receipt:";
pub(crate) const WRITE_RECEIPT_ID_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TextMetadataElement {
    Node,
    Edge,
}

impl TextMetadataElement {
    const fn as_prefix(self) -> char {
        match self {
            Self::Node => 'n',
            Self::Edge => 'e',
        }
    }
}

/// Metadata storage key.
///
/// ```text
/// [0xFF][name:var]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataKey<'a> {
    name: &'a [u8],
}

impl<'a> MetadataKey<'a> {
    pub(crate) const fn new(name: &'a [u8]) -> Self {
        Self { name }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::Metadata
    }

    #[inline]
    pub(crate) fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        if slice.len() < PREFIX_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN,
                actual: slice.len(),
            });
        }
        if slice[0] != Self::key_prefix().as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected Metadata key prefix ({:#04x}), got {:#04x}",
                Self::key_prefix().as_u8(),
                slice[0]
            )));
        }

        Ok(Self::new(&slice[PREFIX_LEN..]))
    }

    #[cfg(test)]
    pub(crate) const fn name(&self) -> &'a [u8] {
        self.name
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + self.name.len()
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(self.name);
    }

    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }
}

impl<'a> MetadataKey<'a> {
    /// Key for next node ID high watermark
    #[inline]
    pub fn next_node_id_key() -> Self {
        Self::new(NEXT_NODE_ID)
    }

    /// Key for next edge ID high watermark
    #[inline]
    pub fn next_edge_id_key() -> Self {
        Self::new(NEXT_EDGE_ID)
    }

    /// Prefix of persisted pre-V2 dynamic index definition rows.
    pub(crate) const fn dynamic_index_prefix() -> Self {
        Self::new(DYNAMIC_INDEX_PREFIX)
    }

    /// Returns the JSON-encoded legacy identity from a dynamic catalog row.
    pub(crate) fn dynamic_index_encoded_identity(&self) -> Option<&'a [u8]> {
        self.name.strip_prefix(DYNAMIC_INDEX_PREFIX)
    }
}

/// Complete scoped key for one persisted pre-V2 dynamic index definition.
#[cfg(any(test, feature = "migration-parity", feature = "production-coverage"))]
pub(crate) fn dynamic_index_storage_key_scoped(scope: DataScope, encoded_identity: &[u8]) -> Bytes {
    let mut name = Vec::with_capacity(DYNAMIC_INDEX_PREFIX.len() + encoded_identity.len());
    name.extend_from_slice(DYNAMIC_INDEX_PREFIX);
    name.extend_from_slice(encoded_identity);
    data_metadata_key_scoped(scope, &name)
}

pub(crate) fn text_index_manifest_key_scoped(scope: DataScope, index_name: &str) -> Bytes {
    data_metadata_key_scoped(scope, format!("text_manifest:{index_name}").as_bytes())
}

pub(crate) fn text_index_manifest_scan_prefix_scoped(scope: DataScope) -> Bytes {
    data_metadata_key_scoped(scope, TEXT_INDEX_MANIFEST_PREFIX)
}

pub(crate) fn text_index_manifest_prefix_scoped(
    scope: DataScope,
    element_type: TextMetadataElement,
    label_hash: u64,
    property_hash: u64,
    tenant_scoped: bool,
) -> Bytes {
    let element_prefix = element_type.as_prefix();
    let prefix = if tenant_scoped {
        format!("text_manifest:ftsmt:{element_prefix}:{label_hash:016x}:{property_hash:016x}:")
    } else {
        format!("text_manifest:fts:{element_prefix}:{label_hash:016x}:{property_hash:016x}")
    };
    data_metadata_key_scoped(scope, prefix.as_bytes())
}

pub(crate) fn text_index_txn_guard_key_scoped(scope: DataScope, index_name: &str) -> Bytes {
    data_metadata_key_scoped(scope, format!("text_guard:{index_name}").as_bytes())
}

pub(crate) fn text_definition_guard_key_scoped(
    scope: DataScope,
    element_type: TextMetadataElement,
    label_hash: u64,
    property_hash: u64,
) -> Bytes {
    let element_prefix = element_type.as_prefix();
    data_metadata_key_scoped(
        scope,
        format!("text_def:{element_prefix}:{label_hash:016x}:{property_hash:016x}").as_bytes(),
    )
}

pub(crate) fn text_index_live_state_key_scoped(
    scope: DataScope,
    index_name: &str,
    entity_id: u64,
) -> Bytes {
    data_metadata_key_scoped(
        scope,
        format!("text_live:{index_name}:{entity_id:020}").as_bytes(),
    )
}

pub(crate) fn text_index_live_state_prefix_scoped(scope: DataScope, index_name: &str) -> Bytes {
    data_metadata_key_scoped(scope, format!("text_live:{index_name}:").as_bytes())
}

pub(crate) fn text_index_version_counter_key_scoped(scope: DataScope, index_name: &str) -> Bytes {
    data_metadata_key_scoped(scope, format!("text_version:{index_name}").as_bytes())
}

/// Complete tenant-scoped key for one idempotent write receipt.
///
/// The receipt ID is fixed-width so caller-controlled bytes cannot overlap
/// another metadata family or create ambiguous prefix encodings.
pub(crate) fn write_receipt_key_scoped(
    scope: DataScope,
    request_id: &[u8; WRITE_RECEIPT_ID_LEN],
) -> Bytes {
    let mut name = Vec::with_capacity(WRITE_RECEIPT_PREFIX.len() + WRITE_RECEIPT_ID_LEN);
    name.extend_from_slice(WRITE_RECEIPT_PREFIX);
    name.extend_from_slice(request_id);
    data_metadata_key_scoped(scope, &name)
}

pub(crate) fn write_receipt_scan_prefix_scoped(scope: DataScope) -> Bytes {
    data_metadata_key_scoped(scope, WRITE_RECEIPT_PREFIX)
}

fn data_metadata_key_scoped(scope: DataScope, name: &[u8]) -> Bytes {
    Key::Data {
        scope,
        kind: DataKeyKind::IndexMetadata(MetadataKey::new(name)),
    }
    .to_bytes()
}

impl<'a> From<&MetadataKey<'a>> for KeyPrefix {
    fn from(_: &MetadataKey<'a>) -> KeyPrefix {
        MetadataKey::key_prefix()
    }
}

#[cfg(test)]
mod write_receipt_tests {
    use super::*;
    use crate::encoding::keys::tenant::TenantId;

    #[test]
    fn write_receipt_key_is_fixed_width_and_tenant_scoped() {
        let request_id = [0xA5; WRITE_RECEIPT_ID_LEN];
        let legacy = write_receipt_key_scoped(DataScope::LegacyUnscoped, &request_id);
        assert_eq!(
            legacy.as_ref(),
            [
                &[KeyPrefix::Metadata.as_u8()][..],
                WRITE_RECEIPT_PREFIX,
                &request_id,
            ]
            .concat()
        );

        let tenant = TenantId::from_u128(7);
        let tenant_scope = DataScope::Tenant(tenant);
        let scoped = write_receipt_key_scoped(tenant_scope, &request_id);
        assert_eq!(tenant_scope.strip_key(&scoped), Some(legacy.as_ref()));
        assert_eq!(
            DataScope::Tenant(TenantId::from_u128(8)).strip_key(&scoped),
            None
        );
    }
}
