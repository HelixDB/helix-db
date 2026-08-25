//! Retired text-index metadata keys.

use bytes::Bytes;

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, MetadataKey};

const TEXT_INDEX_MANIFEST_PREFIX: &[u8] = b"text_manifest:";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyTextMetadataElement {
    Node,
    Edge,
}

impl LegacyTextMetadataElement {
    const fn as_prefix(self) -> char {
        match self {
            Self::Node => 'n',
            Self::Edge => 'e',
        }
    }
}

pub(crate) fn manifest_key(scope: DataScope, index_name: &str) -> Bytes {
    data_metadata_key(scope, format!("text_manifest:{index_name}").as_bytes())
}

pub(crate) fn manifest_scan_prefix(scope: DataScope) -> Bytes {
    data_metadata_key(scope, TEXT_INDEX_MANIFEST_PREFIX)
}

pub(crate) fn manifest_prefix(
    scope: DataScope,
    element_type: LegacyTextMetadataElement,
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
    data_metadata_key(scope, prefix.as_bytes())
}

pub(crate) fn transaction_guard_key(scope: DataScope, index_name: &str) -> Bytes {
    data_metadata_key(scope, format!("text_guard:{index_name}").as_bytes())
}

pub(crate) fn definition_guard_key(
    scope: DataScope,
    element_type: LegacyTextMetadataElement,
    label_hash: u64,
    property_hash: u64,
) -> Bytes {
    let element_prefix = element_type.as_prefix();
    data_metadata_key(
        scope,
        format!("text_def:{element_prefix}:{label_hash:016x}:{property_hash:016x}").as_bytes(),
    )
}

pub(crate) fn live_state_key(scope: DataScope, index_name: &str, entity_id: u64) -> Bytes {
    data_metadata_key(
        scope,
        format!("text_live:{index_name}:{entity_id:020}").as_bytes(),
    )
}

pub(crate) fn live_state_prefix(scope: DataScope, index_name: &str) -> Bytes {
    data_metadata_key(scope, format!("text_live:{index_name}:").as_bytes())
}

pub(crate) fn version_counter_key(scope: DataScope, index_name: &str) -> Bytes {
    data_metadata_key(scope, format!("text_version:{index_name}").as_bytes())
}

fn data_metadata_key(scope: DataScope, name: &[u8]) -> Bytes {
    DataKey::Data {
        scope,
        kind: DataKeyKind::IndexMetadata(MetadataKey::new(name)),
    }
    .to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_text_storage_names_are_frozen() {
        let scope = DataScope::LegacyUnscoped;
        assert_eq!(
            manifest_key(scope, "fts:n:Post:body").as_ref(),
            b"\xfftext_manifest:fts:n:Post:body"
        );
        assert_eq!(
            transaction_guard_key(scope, "x").as_ref(),
            b"\xfftext_guard:x"
        );
        assert_eq!(
            definition_guard_key(scope, LegacyTextMetadataElement::Edge, 1, 2).as_ref(),
            b"\xfftext_def:e:0000000000000001:0000000000000002"
        );
        assert_eq!(
            live_state_key(scope, "x", 7).as_ref(),
            b"\xfftext_live:x:00000000000000000007"
        );
        assert_eq!(live_state_prefix(scope, "x").as_ref(), b"\xfftext_live:x:");
        assert_eq!(
            version_counter_key(scope, "x").as_ref(),
            b"\xfftext_version:x"
        );
    }
}
