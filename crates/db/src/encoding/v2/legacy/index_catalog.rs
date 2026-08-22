//! Frozen pre-V2 dynamic-index catalog wire format.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{DataKey, DataKeyKind, MetadataKey};

const DYNAMIC_INDEX_PREFIX: &[u8] = b"dynamic_index:";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LegacySecondaryElementType {
    #[serde(rename = "Node")]
    Node,
    #[serde(rename = "Edge")]
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LegacySecondaryKind {
    #[serde(rename = "Equality")]
    Equality,
    #[serde(rename = "Range")]
    Range,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) enum LegacyRangeDirection {
    #[default]
    #[serde(rename = "Asc")]
    Asc,
    #[serde(rename = "Desc")]
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LegacyVectorElementType {
    #[serde(rename = "Node")]
    Node,
    #[serde(rename = "Edge")]
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LegacyVectorMetric {
    #[serde(rename = "Cosine")]
    Cosine,
    #[serde(rename = "Euclidean")]
    Euclidean,
    #[serde(rename = "Manhattan")]
    Manhattan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LegacyTextElementType {
    #[serde(rename = "Node")]
    Node,
    #[serde(rename = "Edge")]
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LegacyTextAnalyzer {
    #[serde(rename = "Standard")]
    Standard,
    #[serde(rename = "StandardStemEn")]
    StandardStemEn,
    #[serde(rename = "WhitespaceLowercase")]
    WhitespaceLowercase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LegacySecondaryIndexDefinition {
    pub(crate) element_type: LegacySecondaryElementType,
    pub(crate) kind: LegacySecondaryKind,
    pub(crate) label: String,
    pub(crate) property: String,
    #[serde(default)]
    pub(crate) unique: bool,
    #[serde(default)]
    pub(crate) direction: LegacyRangeDirection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct LegacyVectorIndexDefinition {
    pub(crate) element_type: LegacyVectorElementType,
    pub(crate) label: String,
    pub(crate) property: String,
    pub(crate) tenant_property: Option<String>,
    pub(crate) dimension: usize,
    pub(crate) metric: LegacyVectorMetric,
    pub(crate) m: usize,
    pub(crate) m0: usize,
    pub(crate) ef_construction: usize,
    pub(crate) ml: f32,
    pub(crate) simhash_threshold: usize,
    pub(crate) sampling_ratio: f32,
    pub(crate) adaptive_enabled: bool,
    pub(crate) adaptive_failure_prob: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LegacyTextIndexDefinition {
    pub(crate) element_type: LegacyTextElementType,
    pub(crate) label: String,
    pub(crate) property: String,
    pub(crate) tenant_property: Option<String>,
    pub(crate) analyzer: LegacyTextAnalyzer,
    pub(crate) positions_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum LegacyDynamicIndexDefinition {
    #[serde(rename = "Secondary")]
    Secondary(LegacySecondaryIndexDefinition),
    #[serde(rename = "Vector")]
    Vector(LegacyVectorIndexDefinition),
    #[serde(rename = "Text")]
    Text(LegacyTextIndexDefinition),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum LegacyDynamicIndexCatalogEntry {
    Definition(LegacyDynamicIndexDefinition),
    Tombstone { tombstone: bool },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LegacyDynamicIndexKey {
    #[serde(rename = "Secondary")]
    Secondary(LegacySecondaryIndexDefinition),
    #[serde(rename = "Vector")]
    Vector {
        element_type: LegacyVectorElementType,
        label: String,
        property: String,
    },
    #[serde(rename = "Text")]
    Text {
        element_type: LegacyTextElementType,
        label: String,
        property: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LegacyIndexCatalogError {
    #[error("legacy catalog storage key failed: {0}")]
    StorageKey(crate::encoding::error::EncodingError),
    #[error("legacy catalog prefix yielded another key kind")]
    WrongKeyKind,
    #[error("legacy catalog row has no encoded identity")]
    MissingIdentity,
    #[error("legacy catalog identity JSON failed: {0}")]
    IdentityJson(serde_json::Error),
    #[error("legacy catalog row JSON failed: {0}")]
    EntryJson(serde_json::Error),
}

#[derive(Debug)]
pub(crate) struct LegacyDefinitionRow {
    pub(crate) storage_key: Bytes,
    pub(crate) identity: LegacyDynamicIndexKey,
    pub(crate) entry: LegacyDynamicIndexCatalogEntry,
}

impl LegacyDefinitionRow {
    pub(crate) fn decode(
        scope: DataScope,
        storage_key: Bytes,
        value: &[u8],
    ) -> Result<Self, LegacyIndexCatalogError> {
        let DataKey::Data {
            kind: DataKeyKind::IndexMetadata(metadata),
            ..
        } = DataKey::parse_from_slice(scope, &storage_key)
            .map_err(LegacyIndexCatalogError::StorageKey)?
        else {
            return Err(LegacyIndexCatalogError::WrongKeyKind);
        };
        let Some(encoded_identity) = metadata.name().strip_prefix(DYNAMIC_INDEX_PREFIX) else {
            return Err(LegacyIndexCatalogError::MissingIdentity);
        };
        let identity = serde_json::from_slice(encoded_identity)
            .map_err(LegacyIndexCatalogError::IdentityJson)?;
        let entry = serde_json::from_slice(value).map_err(LegacyIndexCatalogError::EntryJson)?;
        Ok(Self {
            storage_key,
            identity,
            entry,
        })
    }
}

pub(crate) fn catalog_scan_prefix(scope: DataScope) -> Bytes {
    data_metadata_key(scope, DYNAMIC_INDEX_PREFIX)
}

#[cfg(any(test, feature = "migration-parity", feature = "production-coverage"))]
pub(crate) fn catalog_storage_key(scope: DataScope, encoded_identity: &[u8]) -> Bytes {
    let mut name = Vec::with_capacity(DYNAMIC_INDEX_PREFIX.len() + encoded_identity.len());
    name.extend_from_slice(DYNAMIC_INDEX_PREFIX);
    name.extend_from_slice(encoded_identity);
    data_metadata_key(scope, &name)
}

#[cfg(any(test, feature = "migration-parity", feature = "production-coverage"))]
pub(crate) fn encode_row_for_contract(
    scope: DataScope,
    identity: &LegacyDynamicIndexKey,
    entry: &LegacyDynamicIndexCatalogEntry,
) -> Result<(Bytes, Bytes), LegacyIndexCatalogError> {
    let identity = serde_json::to_vec(identity).map_err(LegacyIndexCatalogError::IdentityJson)?;
    let key = catalog_storage_key(scope, &identity);
    let value = serde_json::to_vec(entry)
        .map(Bytes::from)
        .map_err(LegacyIndexCatalogError::EntryJson)?;
    Ok((key, value))
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
    use crate::encoding::v2::keys::AdjacencyKey;

    #[test]
    fn deployed_enum_names_are_explicit_and_frozen() {
        assert_eq!(
            serde_json::to_string(&LegacySecondaryElementType::Node).unwrap(),
            "\"Node\""
        );
        assert_eq!(
            serde_json::to_string(&LegacySecondaryElementType::Edge).unwrap(),
            "\"Edge\""
        );
        assert_eq!(
            serde_json::to_string(&LegacySecondaryKind::Equality).unwrap(),
            "\"Equality\""
        );
        assert_eq!(
            serde_json::to_string(&LegacySecondaryKind::Range).unwrap(),
            "\"Range\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyRangeDirection::Asc).unwrap(),
            "\"Asc\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyRangeDirection::Desc).unwrap(),
            "\"Desc\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyVectorElementType::Node).unwrap(),
            "\"Node\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyVectorElementType::Edge).unwrap(),
            "\"Edge\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyVectorMetric::Cosine).unwrap(),
            "\"Cosine\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyVectorMetric::Euclidean).unwrap(),
            "\"Euclidean\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyVectorMetric::Manhattan).unwrap(),
            "\"Manhattan\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyTextElementType::Node).unwrap(),
            "\"Node\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyTextElementType::Edge).unwrap(),
            "\"Edge\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyTextAnalyzer::Standard).unwrap(),
            "\"Standard\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyTextAnalyzer::StandardStemEn).unwrap(),
            "\"StandardStemEn\""
        );
        assert_eq!(
            serde_json::to_string(&LegacyTextAnalyzer::WhitespaceLowercase).unwrap(),
            "\"WhitespaceLowercase\""
        );

        let secondary = LegacyDynamicIndexDefinition::Secondary(LegacySecondaryIndexDefinition {
            element_type: LegacySecondaryElementType::Node,
            kind: LegacySecondaryKind::Equality,
            label: "Post".to_string(),
            property: "slug".to_string(),
            unique: false,
            direction: LegacyRangeDirection::Asc,
        });
        let vector = LegacyDynamicIndexDefinition::Vector(LegacyVectorIndexDefinition {
            element_type: LegacyVectorElementType::Edge,
            label: "LINKS".to_string(),
            property: "embedding".to_string(),
            tenant_property: None,
            dimension: 3,
            metric: LegacyVectorMetric::Cosine,
            m: 16,
            m0: 32,
            ef_construction: 200,
            ml: 0.5,
            simhash_threshold: 43,
            sampling_ratio: 0.8,
            adaptive_enabled: true,
            adaptive_failure_prob: 0.1,
        });
        let text = LegacyDynamicIndexDefinition::Text(LegacyTextIndexDefinition {
            element_type: LegacyTextElementType::Node,
            label: "Post".to_string(),
            property: "body".to_string(),
            tenant_property: None,
            analyzer: LegacyTextAnalyzer::StandardStemEn,
            positions_enabled: true,
        });
        assert!(serde_json::to_value(secondary)
            .unwrap()
            .get("Secondary")
            .is_some());
        assert!(serde_json::to_value(vector)
            .unwrap()
            .get("Vector")
            .is_some());
        assert!(serde_json::to_value(text).unwrap().get("Text").is_some());
    }

    #[test]
    fn catalog_row_round_trips_and_rejects_bad_json() {
        let scope = DataScope::LegacyUnscoped;
        let identity = LegacyDynamicIndexKey::Text {
            element_type: LegacyTextElementType::Node,
            label: "Post".to_string(),
            property: "body".to_string(),
        };
        let entry = LegacyDynamicIndexCatalogEntry::Tombstone { tombstone: true };
        let (key, value) = encode_row_for_contract(scope, &identity, &entry).unwrap();
        let decoded = LegacyDefinitionRow::decode(scope, key, &value).unwrap();
        assert_eq!(decoded.identity, identity);
        assert_eq!(decoded.entry, entry);
        let (_, malformed) = encode_row_for_contract(scope, &identity, &entry).unwrap();
        assert!(LegacyDefinitionRow::decode(
            scope,
            decoded.storage_key,
            &[malformed.as_ref(), b"x"].concat()
        )
        .is_err());

        let wrong_kind = DataKey::Data {
            scope,
            kind: DataKeyKind::Adjacency(AdjacencyKey::new(7)),
        }
        .to_bytes();
        assert!(matches!(
            LegacyDefinitionRow::decode(scope, wrong_kind, &value),
            Err(LegacyIndexCatalogError::WrongKeyKind)
        ));
        assert!(matches!(
            LegacyDefinitionRow::decode(scope, data_metadata_key(scope, b"other"), &value),
            Err(LegacyIndexCatalogError::MissingIdentity)
        ));
        assert!(matches!(
            LegacyDefinitionRow::decode(scope, catalog_storage_key(scope, b"not-json"), &value),
            Err(LegacyIndexCatalogError::IdentityJson(_))
        ));
    }
}
