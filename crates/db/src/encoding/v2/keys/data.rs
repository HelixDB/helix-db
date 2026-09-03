//! Typed graph-data keys and the scoped key framing boundary.

pub(crate) use super::graph::{
    AdjacencyKey, EdgeEndpointsKey, EdgePairIndexKey, EdgePropertyByIdKey, NodePropertyKey,
};
use super::indexes::property::PropertyIndexKey;
use super::indexes::vector::VectorKey;
use super::metadata::MetadataKey;
use super::scope::DataScope;
use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey;

use crate::encoding::error::EncodingError;
use bytes::{BufMut, Bytes};

/// Node identifier type - 64-bit unsigned integer
///
/// Node IDs can use the full range of u64 (0 to 2^64 - 1).
pub type NodeId = u64;

/// Edge identifier type - 64-bit unsigned integer
///
/// Edge IDs live in a separate namespace from NodeIds.
pub type EdgeId = u64;

pub(crate) const PREFIX_LEN: usize = core::mem::size_of::<u8>();
pub(crate) const ID_LEN: usize = core::mem::size_of::<NodeId>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyPrefix {
    Adjacency,
    EdgePropertyPair,
    EdgePropertyById,
    NodeProperty,
    PropertyIndex,
    EdgeEndpoints,
    EdgePairIndex,
    Metadata,
}

impl KeyPrefix {
    #[inline]
    pub(crate) const fn as_u8(&self) -> u8 {
        match self {
            KeyPrefix::Adjacency => 0x00,
            KeyPrefix::EdgePropertyPair => 0x01,
            KeyPrefix::EdgePropertyById => 0x01,
            KeyPrefix::NodeProperty => 0x02,
            KeyPrefix::PropertyIndex => 0x03,
            KeyPrefix::EdgeEndpoints => 0x04,
            KeyPrefix::EdgePairIndex => 0x05,
            KeyPrefix::Metadata => 0xFF,
        }
    }

    #[inline]
    pub(crate) const fn as_slice(&self) -> &[u8] {
        match self {
            KeyPrefix::Adjacency => &[0x00],
            KeyPrefix::EdgePropertyPair => &[0x01],
            KeyPrefix::EdgePropertyById => &[0x01],
            KeyPrefix::NodeProperty => &[0x02],
            KeyPrefix::PropertyIndex => &[0x03],
            KeyPrefix::EdgeEndpoints => &[0x04],
            KeyPrefix::EdgePairIndex => &[0x05],
            KeyPrefix::Metadata => &[0xFF],
        }
    }

    #[inline]
    pub(crate) const fn from_u8(u: u8) -> Result<Self, EncodingError> {
        match u {
            0x00 => Ok(KeyPrefix::Adjacency),
            0x01 => Ok(KeyPrefix::EdgePropertyPair),
            0x02 => Ok(KeyPrefix::NodeProperty),
            0x03 => Ok(KeyPrefix::PropertyIndex),
            0x04 => Ok(KeyPrefix::EdgeEndpoints),
            0x05 => Ok(KeyPrefix::EdgePairIndex),
            0xFF => Ok(KeyPrefix::Metadata),
            _ => Err(EncodingError::InvalidKeyPrefix(u)),
        }
    }

    #[inline]
    pub(crate) const fn from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        if slice.is_empty() {
            return Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN,
                actual: slice.len(),
            });
        }

        Self::from_u8(slice[0])
    }
}

/// High-level keyspace classification for node-id carrying backfill scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(crate) enum KeySpace {
    /// Node property row.
    NodeProperty,
    /// Secondary property-index row.
    PropertyIndex,
    /// Any other keyspace prefix.
    Other(u8),
}

#[cfg(test)]
impl KeySpace {
    pub(crate) const fn prefix(self) -> u8 {
        match self {
            Self::NodeProperty => KeyPrefix::NodeProperty.as_u8(),
            Self::PropertyIndex => KeyPrefix::PropertyIndex.as_u8(),
            Self::Other(prefix) => prefix,
        }
    }
}

/// Parse a node-id carrying key prefix used by backfill scanners.
#[cfg(test)]
pub(crate) fn parse_node_key(key: &[u8]) -> (KeySpace, NodeId) {
    let key_space = match key.first().copied() {
        Some(prefix) if prefix == KeyPrefix::NodeProperty.as_u8() => KeySpace::NodeProperty,
        Some(prefix) if prefix == KeyPrefix::PropertyIndex.as_u8() => KeySpace::PropertyIndex,
        Some(prefix) => KeySpace::Other(prefix),
        None => KeySpace::Other(0),
    };
    let node_id = key
        .get(PREFIX_LEN..PREFIX_LEN + ID_LEN)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_be_bytes)
        .unwrap_or_default();
    (key_space, node_id)
}

/// Tenant-owned logical data key variants, without data scope.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum DataKeyKind<'a> {
    /// `[0x00][node_id:8]``
    Adjacency(AdjacencyKey),

    /// Legacy/simple edge property key: `[0x01][from:8][to:8]`
    EdgePropertyPair(LegacyEdgePropertyPairKey),

    /// Multigraph edge property key: `[0x01][edge_id:8]`
    EdgePropertyById(EdgePropertyByIdKey),

    /// `[0x02][node_id:8]``
    NodeProperty(NodePropertyKey),

    /// `[0x03][index-type...]``
    PropertyIndex(PropertyIndexKey<'a>),

    /// `[0x04][edge_id:8]`
    EdgeEndpoints(EdgeEndpointsKey),

    /// `[0x05][from:8][to:8]` -> RoaringTreemap<EdgeId>
    EdgePairIndex(EdgePairIndexKey),

    /// Vector index keys across the default, hot, and layer-0 keyspaces.
    Vector(VectorKey),

    /// Tenant-owned index catalog and runtime metadata.
    IndexMetadata(MetadataKey<'a>),
}

impl<'a> DataKeyKind<'a> {
    #[inline]
    #[cfg(test)]
    pub(crate) fn prefix(&self) -> KeyPrefix {
        match self {
            DataKeyKind::Adjacency(key) => key.into(),
            DataKeyKind::EdgePropertyPair(key) => key.into(),
            DataKeyKind::EdgePropertyById(key) => key.into(),
            DataKeyKind::NodeProperty(key) => key.into(),
            DataKeyKind::PropertyIndex(_) => KeyPrefix::PropertyIndex,
            DataKeyKind::EdgeEndpoints(key) => key.into(),
            DataKeyKind::EdgePairIndex(key) => key.into(),
            DataKeyKind::Vector(_) => KeyPrefix::PropertyIndex,
            DataKeyKind::IndexMetadata(key) => key.into(),
        }
    }

    #[inline]
    /// Number of ids in the key
    #[cfg(test)]
    pub(crate) const fn id_count(&self) -> usize {
        match self {
            DataKeyKind::EdgePropertyPair(..) | DataKeyKind::EdgePairIndex(..) => 2,
            DataKeyKind::Adjacency(_)
            | DataKeyKind::NodeProperty(_)
            | DataKeyKind::EdgeEndpoints(_)
            | DataKeyKind::EdgePropertyById(_) => 1,
            DataKeyKind::PropertyIndex(_)
            | DataKeyKind::Vector(_)
            | DataKeyKind::IndexMetadata(_) => 0,
        }
    }

    #[inline]
    /// Length of the key in bytes
    fn encoded_len(&self) -> usize {
        match self {
            DataKeyKind::PropertyIndex(index_key) => index_key.encoded_len(),
            DataKeyKind::Adjacency(key) => key.encoded_len(),
            DataKeyKind::EdgePropertyPair(key) => key.encoded_len(),
            DataKeyKind::EdgePropertyById(key) => key.encoded_len(),
            DataKeyKind::NodeProperty(key) => key.encoded_len(),
            DataKeyKind::EdgeEndpoints(key) => key.encoded_len(),
            DataKeyKind::EdgePairIndex(key) => key.encoded_len(),
            DataKeyKind::Vector(key) => key.encoded_len(),
            DataKeyKind::IndexMetadata(key) => key.encoded_len(),
        }
    }

    /// Encode the key into a buffer
    ///
    /// All ids are encoded as big-endian.
    #[inline]
    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        // put_u64 is big-endian
        match self {
            DataKeyKind::Adjacency(key) => key.encode_into(buf),
            DataKeyKind::NodeProperty(key) => key.encode_into(buf),
            DataKeyKind::EdgeEndpoints(key) => key.encode_into(buf),
            DataKeyKind::EdgePropertyPair(key) => key.encode_into(buf),
            DataKeyKind::EdgePairIndex(key) => key.encode_into(buf),
            DataKeyKind::EdgePropertyById(key) => key.encode_into(buf),
            DataKeyKind::PropertyIndex(index_key) => index_key.encode_into(buf),
            DataKeyKind::Vector(vector_key) => vector_key.encode_into(buf),
            DataKeyKind::IndexMetadata(key) => key.encode_into(buf),
        }
    }

    pub(crate) fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        if VectorKey::is_vector_keyspace(slice)
            && let Ok(key) = VectorKey::parse_from_slice(slice)
        {
            return Ok(DataKeyKind::Vector(key));
        }

        let prefix = KeyPrefix::from_slice(slice)?;
        match prefix {
            KeyPrefix::Adjacency => Ok(DataKeyKind::Adjacency(AdjacencyKey::parse_from_slice(
                slice,
            )?)),
            KeyPrefix::EdgePropertyPair | KeyPrefix::EdgePropertyById => match slice.len() {
                len if len == PREFIX_LEN + ID_LEN => Ok(DataKeyKind::EdgePropertyById(
                    EdgePropertyByIdKey::parse_from_slice(slice)?,
                )),
                len if len == PREFIX_LEN + ID_LEN * 2 => Ok(DataKeyKind::EdgePropertyPair(
                    LegacyEdgePropertyPairKey::parse_from_slice(slice)?,
                )),
                actual if actual < PREFIX_LEN + ID_LEN => Err(EncodingError::BufferTooShort {
                    expected: PREFIX_LEN + ID_LEN,
                    actual,
                }),
                actual => Err(EncodingError::InvalidKey(format!(
                    "invalid edge property key length: expected {} or {} bytes, got {actual}",
                    PREFIX_LEN + ID_LEN,
                    PREFIX_LEN + ID_LEN * 2
                ))),
            },
            KeyPrefix::NodeProperty => Ok(DataKeyKind::NodeProperty(
                NodePropertyKey::parse_from_slice(slice)?,
            )),
            KeyPrefix::PropertyIndex => Ok(DataKeyKind::PropertyIndex(
                PropertyIndexKey::parse_from_slice(slice)?,
            )),
            KeyPrefix::EdgeEndpoints => Ok(DataKeyKind::EdgeEndpoints(
                EdgeEndpointsKey::parse_from_slice(slice)?,
            )),
            KeyPrefix::EdgePairIndex => Ok(DataKeyKind::EdgePairIndex(
                EdgePairIndexKey::parse_from_slice(slice)?,
            )),
            KeyPrefix::Metadata => Ok(DataKeyKind::IndexMetadata(MetadataKey::parse_from_slice(
                slice,
            )?)),
        }
    }

    /// Encode the logical key shape into bytes without a tenant prefix.
    #[cfg(test)]
    #[inline]
    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut bytes);
        Bytes::from(bytes)
    }
}

/// DB-wide logical key variants that are never tenant data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GlobalKeyKind<'a> {
    /// `[0xFF]...`
    Metadata(MetadataKey<'a>),
}

impl<'a> GlobalKeyKind<'a> {
    #[inline]
    fn encoded_len(&self) -> usize {
        match self {
            Self::Metadata(key) => key.encoded_len(),
        }
    }

    #[inline]
    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        match self {
            Self::Metadata(key) => key.encode_into(buf),
        }
    }
}

/// Physical storage key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DataKey<'a> {
    /// DB-wide control metadata.
    Global { kind: GlobalKeyKind<'a> },
    /// Tenant-owned data, including the explicit storage namespace.
    Data {
        scope: DataScope,
        kind: DataKeyKind<'a>,
    },
}

impl<'a> DataKey<'a> {
    pub(crate) fn data_prefix(scope: DataScope, logical_prefix: Bytes) -> Bytes {
        match scope {
            DataScope::LegacyUnscoped => logical_prefix,
            scope @ DataScope::Tenant(_) => {
                let mut bytes = Vec::with_capacity(scope.encoded_len() + logical_prefix.len());
                scope.encode_key_prefix(&mut bytes);
                bytes.extend_from_slice(&logical_prefix);
                Bytes::from(bytes)
            }
        }
    }

    pub(crate) fn data_range(scope: DataScope, start: Bytes, end: Bytes) -> (Bytes, Bytes) {
        (
            Self::data_prefix(scope, start),
            Self::data_prefix(scope, end),
        )
    }

    pub(crate) fn parse_from_slice(
        scope: DataScope,
        slice: &'a [u8],
    ) -> Result<Self, EncodingError> {
        let Some(logical) = scope.strip_key(slice) else {
            return Err(EncodingError::InvalidKey(
                "physical key does not match tenant scope".to_string(),
            ));
        };
        Ok(Self::Data {
            scope,
            kind: DataKeyKind::parse_from_slice(logical)?,
        })
    }

    /// Encode the physical key into bytes.
    ///
    /// Tenant-scoped keys serialize the tenant prefix before the logical key
    /// shape. Legacy unscoped keys serialize only the logical key bytes.
    ///
    ///  All ids are encoded as big-endian.
    #[inline]
    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut bytes = match self {
            Self::Global { kind } => Vec::with_capacity(kind.encoded_len()),
            Self::Data { scope, kind } => {
                Vec::with_capacity(scope.encoded_len() + kind.encoded_len())
            }
        };
        match self {
            Self::Global { kind } => kind.encode_into(&mut bytes),
            Self::Data { scope, kind } => {
                scope.encode_key_prefix(&mut bytes);
                kind.encode_into(&mut bytes);
            }
        }
        Bytes::from(bytes)
    }
}

#[cfg(test)]
mod tests {
    use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey as EdgePropertyPairKey;

    use super::*;
    use crate::encoding::{
        indexes::{equality::EqualityIndexKey, label::EdgeLabelKey, PropertyIndexKey},
        keys::{indexes::vector, metadata, scope, scope::TenantId},
    };

    fn fixed_key_bytes(prefix: u8, ids: &[u64]) -> Vec<u8> {
        let mut bytes = vec![prefix];
        for id in ids {
            bytes.extend_from_slice(&id.to_be_bytes());
        }
        bytes
    }

    #[test]
    fn key_prefix_byte_mappings_are_stable() {
        assert_eq!(KeyPrefix::Adjacency.as_u8(), 0x00);
        assert_eq!(KeyPrefix::Adjacency.as_slice(), &[0x00]);
        assert_eq!(KeyPrefix::EdgePropertyPair.as_u8(), 0x01);
        assert_eq!(KeyPrefix::EdgePropertyPair.as_slice(), &[0x01]);
        assert_eq!(KeyPrefix::EdgePropertyById.as_u8(), 0x01);
        assert_eq!(KeyPrefix::EdgePropertyById.as_slice(), &[0x01]);
        assert_eq!(KeyPrefix::NodeProperty.as_u8(), 0x02);
        assert_eq!(KeyPrefix::NodeProperty.as_slice(), &[0x02]);
        assert_eq!(KeyPrefix::PropertyIndex.as_u8(), 0x03);
        assert_eq!(KeyPrefix::PropertyIndex.as_slice(), &[0x03]);
        assert_eq!(KeyPrefix::EdgeEndpoints.as_u8(), 0x04);
        assert_eq!(KeyPrefix::EdgeEndpoints.as_slice(), &[0x04]);
        assert_eq!(KeyPrefix::EdgePairIndex.as_u8(), 0x05);
        assert_eq!(KeyPrefix::EdgePairIndex.as_slice(), &[0x05]);
        assert_eq!(KeyPrefix::Metadata.as_u8(), 0xFF);
        assert_eq!(KeyPrefix::Metadata.as_slice(), &[0xFF]);
        assert!(matches!(
            KeyPrefix::from_u8(0xFE),
            Err(EncodingError::InvalidKeyPrefix(0xFE))
        ));
        assert_eq!(
            KeyPrefix::from_slice(&[0x04]).unwrap(),
            KeyPrefix::EdgeEndpoints
        );
        assert_eq!(
            KeyPrefix::from_slice(&[0x05]).unwrap(),
            KeyPrefix::EdgePairIndex
        );
        assert!(matches!(
            KeyPrefix::from_slice(&[0x06]),
            Err(EncodingError::InvalidKeyPrefix(0x06))
        ));
        assert_eq!(KeyPrefix::from_slice(&[0xFF]).unwrap(), KeyPrefix::Metadata);
        assert!(matches!(
            KeyPrefix::from_slice(&[]),
            Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN,
                actual: 0
            })
        ));
    }

    #[test]
    fn parse_node_key_classifies_backfill_scan_rows() {
        let node_id = 0x0102_0304_0506_0708u64;
        let node_key = fixed_key_bytes(KeyPrefix::NodeProperty.as_u8(), &[node_id]);
        assert_eq!(parse_node_key(&node_key), (KeySpace::NodeProperty, node_id));
        assert_eq!(KeySpace::NodeProperty.prefix(), 0x02);

        let index_key = fixed_key_bytes(KeyPrefix::PropertyIndex.as_u8(), &[node_id]);
        assert_eq!(
            parse_node_key(&index_key),
            (KeySpace::PropertyIndex, node_id)
        );
        assert_eq!(KeySpace::PropertyIndex.prefix(), 0x03);

        assert_eq!(parse_node_key(&[0x77]), (KeySpace::Other(0x77), 0));
        assert_eq!(KeySpace::Other(0x77).prefix(), 0x77);
        assert_eq!(parse_node_key(&[]), (KeySpace::Other(0), 0));
    }

    #[test]
    fn fixed_key_variants_have_exact_wire_layouts() {
        assert_eq!(
            DataKeyKind::Adjacency(AdjacencyKey::new(42))
                .to_bytes()
                .as_ref(),
            fixed_key_bytes(0x00, &[42]).as_slice()
        );
        assert_eq!(
            DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(1, 2))
                .to_bytes()
                .as_ref(),
            fixed_key_bytes(0x01, &[1, 2]).as_slice()
        );
        assert_eq!(
            DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(7))
                .to_bytes()
                .as_ref(),
            fixed_key_bytes(0x01, &[7]).as_slice()
        );
        assert_eq!(
            DataKeyKind::NodeProperty(NodePropertyKey::new(9))
                .to_bytes()
                .as_ref(),
            fixed_key_bytes(0x02, &[9]).as_slice()
        );
        assert_eq!(
            DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(11))
                .to_bytes()
                .as_ref(),
            fixed_key_bytes(0x04, &[11]).as_slice()
        );
        assert_eq!(
            DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(13, 17))
                .to_bytes()
                .as_ref(),
            fixed_key_bytes(0x05, &[13, 17]).as_slice()
        );
    }

    #[test]
    fn property_index_key_uses_actual_index_length() {
        let key = DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
            [1, 2, 3, 4],
            [5, 6, 7, 8, 9, 10, 11, 12],
        )));

        assert_eq!(
            key.to_bytes().as_ref(),
            &[0x03, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        );
    }

    #[test]
    fn parse_round_trips_edge_property_prefix_by_length() {
        let by_id = DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(99)).to_bytes();
        let pair = DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(1, 2)).to_bytes();
        let endpoints = DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(7)).to_bytes();
        let edge_pair = DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(3, 4)).to_bytes();
        let equality_bytes = DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(
            EqualityIndexKey::new([1; 4], [2; 8]),
        ))
        .to_bytes();
        let vector = vector::VectorKey::IndexPrefix(vector::VectorIndexPrefixKey::new(11));
        let vector_bytes = DataKeyKind::Vector(vector).to_bytes();

        assert_eq!(
            DataKeyKind::parse_from_slice(&by_id).unwrap(),
            DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(99))
        );
        assert_eq!(
            DataKeyKind::parse_from_slice(&pair).unwrap(),
            DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(1, 2))
        );
        assert_eq!(
            DataKeyKind::parse_from_slice(&endpoints).unwrap(),
            DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(7))
        );
        assert_eq!(
            DataKeyKind::parse_from_slice(&edge_pair).unwrap(),
            DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(3, 4))
        );
        assert_eq!(
            DataKeyKind::parse_from_slice(&equality_bytes).unwrap(),
            DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(EqualityIndexKey::new(
                [1; 4], [2; 8]
            )))
        );
        assert_eq!(
            DataKeyKind::parse_from_slice(&vector_bytes).unwrap(),
            DataKeyKind::Vector(vector)
        );
        assert_eq!(
            DataKeyKind::parse_from_slice(b"\xFFversion").unwrap(),
            DataKeyKind::IndexMetadata(MetadataKey::new(b"version"))
        );
    }

    #[test]
    fn fixed_key_parsers_accept_exact_wire_layouts() {
        let adjacency = AdjacencyKey::parse_from_slice(&fixed_key_bytes(0x00, &[42])).unwrap();
        assert_eq!(adjacency.node_id(), 42);

        let by_id = EdgePropertyByIdKey::parse_from_slice(&fixed_key_bytes(0x01, &[7])).unwrap();
        assert_eq!(by_id.edge_id(), 7);

        let pair = EdgePropertyPairKey::parse_from_slice(&fixed_key_bytes(0x01, &[1, 2])).unwrap();
        assert_eq!(pair.from(), 1);
        assert_eq!(pair.to(), 2);

        let node = NodePropertyKey::parse_from_slice(&fixed_key_bytes(0x02, &[9])).unwrap();
        assert_eq!(node.node_id(), 9);

        let endpoints = EdgeEndpointsKey::parse_from_slice(&fixed_key_bytes(0x04, &[11])).unwrap();
        assert_eq!(endpoints.edge_id(), 11);

        let edge_pair =
            EdgePairIndexKey::parse_from_slice(&fixed_key_bytes(0x05, &[13, 17])).unwrap();
        assert_eq!(edge_pair.from(), 13);
        assert_eq!(edge_pair.to(), 17);

        let metadata = MetadataKey::parse_from_slice(b"\xFFabc").unwrap();
        assert_eq!(metadata.name(), b"abc");
    }

    #[test]
    fn fixed_key_parsers_reject_short_or_trailing_bytes() {
        assert!(matches!(
            DataKeyKind::parse_from_slice(&[]),
            Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN,
                actual: 0
            })
        ));
        assert!(matches!(
            DataKeyKind::parse_from_slice(&[0x00, 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));
        assert!(matches!(
            DataKeyKind::parse_from_slice(&[0x01]),
            Err(EncodingError::BufferTooShort { expected, actual: 1 })
                if expected == PREFIX_LEN + ID_LEN
        ));
        assert!(matches!(
            DataKeyKind::parse_from_slice(&[KeyPrefix::PropertyIndex.as_u8()]),
            Err(EncodingError::BufferTooShort { .. })
        ));
        assert!(matches!(
            DataKeyKind::parse_from_slice(&[0xFE]),
            Err(EncodingError::InvalidKeyPrefix(0xFE))
        ));

        let mut node_key = DataKeyKind::NodeProperty(NodePropertyKey::new(1))
            .to_bytes()
            .to_vec();
        node_key.push(0);
        assert!(matches!(
            DataKeyKind::parse_from_slice(&node_key),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut invalid_edge_property = fixed_key_bytes(0x01, &[1]);
        invalid_edge_property.extend_from_slice(&[0, 1, 2]);
        assert!(matches!(
            DataKeyKind::parse_from_slice(&invalid_edge_property),
            Err(EncodingError::InvalidKey(_))
        ));
    }

    #[test]
    fn direct_fixed_key_parsers_reject_wrong_prefixes_and_trailing_bytes() {
        let mut adjacency = fixed_key_bytes(0x00, &[1]);
        adjacency.push(0);
        assert!(matches!(
            AdjacencyKey::parse_from_slice(&adjacency),
            Err(EncodingError::InvalidKey(_))
        ));
        assert!(matches!(
            AdjacencyKey::parse_from_slice(&fixed_key_bytes(0x02, &[1])),
            Err(EncodingError::InvalidKey(_))
        ));
        assert!(matches!(
            EdgePropertyByIdKey::parse_from_slice(&fixed_key_bytes(0x04, &[1])),
            Err(EncodingError::InvalidKey(_))
        ));
        assert!(matches!(
            EdgePropertyPairKey::parse_from_slice(&fixed_key_bytes(0x05, &[1, 2])),
            Err(EncodingError::InvalidKey(_))
        ));
        assert!(matches!(
            NodePropertyKey::parse_from_slice(&fixed_key_bytes(0x00, &[1])),
            Err(EncodingError::InvalidKey(_))
        ));
        assert!(matches!(
            EdgeEndpointsKey::parse_from_slice(&fixed_key_bytes(0x00, &[1])),
            Err(EncodingError::InvalidKey(_))
        ));
        assert!(matches!(
            EdgePairIndexKey::parse_from_slice(&fixed_key_bytes(0x01, &[1, 2])),
            Err(EncodingError::InvalidKey(_))
        ));
    }

    #[test]
    fn direct_fixed_key_parsers_reject_short_inputs() {
        assert!(matches!(
            AdjacencyKey::parse_from_slice(&[0x00]),
            Err(EncodingError::BufferTooShort {
                expected: 9,
                actual: 1
            })
        ));
        assert!(matches!(
            EdgePropertyByIdKey::parse_from_slice(&[0x01]),
            Err(EncodingError::BufferTooShort {
                expected: 9,
                actual: 1
            })
        ));
        assert!(matches!(
            NodePropertyKey::parse_from_slice(&[0x02]),
            Err(EncodingError::BufferTooShort {
                expected: 9,
                actual: 1
            })
        ));
        assert!(matches!(
            EdgeEndpointsKey::parse_from_slice(&[0x04]),
            Err(EncodingError::BufferTooShort {
                expected: 9,
                actual: 1
            })
        ));
        assert!(matches!(
            EdgePropertyPairKey::parse_from_slice(&[0x01]),
            Err(EncodingError::BufferTooShort {
                expected: 17,
                actual: 1
            })
        ));
        assert!(matches!(
            EdgePairIndexKey::parse_from_slice(&[0x05]),
            Err(EncodingError::BufferTooShort {
                expected: 17,
                actual: 1
            })
        ));
    }

    #[test]
    fn direct_fixed_key_parsers_reject_trailing_inputs() {
        let mut by_id = fixed_key_bytes(0x01, &[1]);
        by_id.push(0);
        assert!(matches!(
            EdgePropertyByIdKey::parse_from_slice(&by_id),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut pair = fixed_key_bytes(0x01, &[1, 2]);
        pair.push(0);
        assert!(matches!(
            EdgePropertyPairKey::parse_from_slice(&pair),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut node = fixed_key_bytes(0x02, &[1]);
        node.push(0);
        assert!(matches!(
            NodePropertyKey::parse_from_slice(&node),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut endpoints = fixed_key_bytes(0x04, &[1]);
        endpoints.push(0);
        assert!(matches!(
            EdgeEndpointsKey::parse_from_slice(&endpoints),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut edge_pair = fixed_key_bytes(0x05, &[1, 2]);
        edge_pair.push(0);
        assert!(matches!(
            EdgePairIndexKey::parse_from_slice(&edge_pair),
            Err(EncodingError::InvalidKey(_))
        ));
    }

    #[test]
    fn key_variant_contracts_report_prefix_and_id_count() {
        let equality = PropertyIndexKey::Equality(EqualityIndexKey::new([1; 4], [2; 8]));
        let variants = [
            (
                DataKeyKind::Adjacency(AdjacencyKey::new(1)),
                KeyPrefix::Adjacency,
                1,
            ),
            (
                DataKeyKind::EdgePropertyPair(EdgePropertyPairKey::new(1, 2)),
                KeyPrefix::EdgePropertyPair,
                2,
            ),
            (
                DataKeyKind::EdgePropertyById(EdgePropertyByIdKey::new(3)),
                KeyPrefix::EdgePropertyById,
                1,
            ),
            (
                DataKeyKind::NodeProperty(NodePropertyKey::new(4)),
                KeyPrefix::NodeProperty,
                1,
            ),
            (
                DataKeyKind::PropertyIndex(equality),
                KeyPrefix::PropertyIndex,
                0,
            ),
            (
                DataKeyKind::EdgeEndpoints(EdgeEndpointsKey::new(5)),
                KeyPrefix::EdgeEndpoints,
                1,
            ),
            (
                DataKeyKind::EdgePairIndex(EdgePairIndexKey::new(6, 7)),
                KeyPrefix::EdgePairIndex,
                2,
            ),
            (
                DataKeyKind::Vector(vector::VectorKey::IndexPrefix(
                    vector::VectorIndexPrefixKey::new(8),
                )),
                KeyPrefix::PropertyIndex,
                0,
            ),
            (
                DataKeyKind::IndexMetadata(MetadataKey::new(b"catalog")),
                KeyPrefix::Metadata,
                0,
            ),
        ];

        for (key, prefix, id_count) in variants {
            assert_eq!(key.prefix(), prefix);
            assert_eq!(key.id_count(), id_count);
        }
    }

    #[test]
    fn global_metadata_key_serializes_without_data_scope() {
        let encoded = DataKey::Global {
            kind: GlobalKeyKind::Metadata(MetadataKey::new(b"global")),
        }
        .to_bytes();

        assert_eq!(encoded.as_ref(), b"\xFFglobal");
    }

    #[test]
    fn metadata_helpers_have_stable_names_and_wire_layouts() {
        let keys = [
            (MetadataKey::next_node_id_key(), metadata::NEXT_NODE_ID),
            (MetadataKey::next_edge_id_key(), metadata::NEXT_EDGE_ID),
        ];

        for (key, name) in keys {
            let mut expected = vec![0xFF];
            expected.extend_from_slice(name);
            assert_eq!(key.name(), name);
            assert_eq!(key.to_bytes().as_ref(), expected.as_slice());
        }

        assert_eq!(MetadataKey::parse_from_slice(b"\xFF").unwrap().name(), b"");
        assert!(matches!(
            MetadataKey::parse_from_slice(&[]),
            Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN,
                actual: 0
            })
        ));
        assert!(matches!(
            MetadataKey::parse_from_slice(b"\x00bad"),
            Err(EncodingError::InvalidKey(_))
        ));
    }

    #[test]
    fn tenant_key_prefixes_encoded_key() {
        let tenant = TenantId::from_ulid_str("0000000000000000000000000A").expect("valid tenant");
        let encoded = DataKey::Data {
            scope: DataScope::Tenant(tenant),
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(42)),
        }
        .to_bytes();
        let mut expected = vec![scope::TENANT_KEY_PREFIX];
        expected.extend_from_slice(&tenant.as_u128().to_be_bytes());
        expected.extend_from_slice(
            DataKeyKind::NodeProperty(NodePropertyKey::new(42))
                .to_bytes()
                .as_ref(),
        );

        assert_eq!(encoded.as_ref(), expected.as_slice());
    }

    #[test]
    fn physical_key_parser_enforces_scope_and_logical_key_contracts() {
        let tenant_a = TenantId::from_ulid_str("0000000000000000000000000A").expect("valid tenant");
        let tenant_b = TenantId::from_ulid_str("0000000000000000000000000B").expect("valid tenant");
        let legacy = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(42)),
        };
        let tenant = DataKey::Data {
            scope: DataScope::Tenant(tenant_a),
            kind: DataKeyKind::NodeProperty(NodePropertyKey::new(42)),
        };

        assert_eq!(
            DataKey::parse_from_slice(DataScope::LegacyUnscoped, &legacy.to_bytes()).unwrap(),
            legacy
        );
        assert_eq!(
            DataKey::parse_from_slice(DataScope::Tenant(tenant_a), &tenant.to_bytes()).unwrap(),
            tenant
        );
        assert!(matches!(
            DataKey::parse_from_slice(DataScope::Tenant(tenant_b), &tenant.to_bytes()),
            Err(EncodingError::InvalidKey(message))
                if message == "physical key does not match tenant scope"
        ));

        let mut malformed = vec![scope::TENANT_KEY_PREFIX];
        malformed.extend_from_slice(&tenant_a.as_u128().to_be_bytes());
        malformed.push(0xFE);
        assert!(matches!(
            DataKey::parse_from_slice(DataScope::Tenant(tenant_a), &malformed),
            Err(EncodingError::InvalidKeyPrefix(0xFE))
        ));
    }

    #[test]
    fn tenant_key_prefixes_edge_label_index_key() {
        let tenant = TenantId::from_ulid_str("0000000000000000000000000B").expect("valid tenant");
        let encoded = DataKey::Data {
            scope: DataScope::Tenant(tenant),
            kind: DataKeyKind::PropertyIndex(PropertyIndexKey::EdgeLabel(
                EdgeLabelKey::from_label("FOLLOWS")
                    .expect("test labels are valid canonical labels"),
            )),
        }
        .to_bytes();

        let mut expected = vec![scope::TENANT_KEY_PREFIX];
        expected.extend_from_slice(&tenant.as_u128().to_be_bytes());
        expected.extend_from_slice(&[0x03, 0x04]);
        crate::encoding::indexes::CanonicalLabel::from_label("FOLLOWS")
            .unwrap()
            .encode_into(&mut expected);

        assert_eq!(encoded.as_ref(), expected.as_slice());
    }

    #[test]
    fn tenant_data_scope_prefixes_index_metadata_and_vector_keys() {
        let tenant_a = TenantId::from_ulid_str("0000000000000000000000000A").expect("valid tenant");
        let tenant_b = TenantId::from_ulid_str("0000000000000000000000000B").expect("valid tenant");
        let legacy_metadata = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::IndexMetadata(MetadataKey::new(b"text_manifest:idx")),
        }
        .to_bytes();
        let tenant_a_metadata = DataKey::Data {
            scope: DataScope::Tenant(tenant_a),
            kind: DataKeyKind::IndexMetadata(MetadataKey::new(b"text_manifest:idx")),
        }
        .to_bytes();
        let tenant_b_metadata = DataKey::Data {
            scope: DataScope::Tenant(tenant_b),
            kind: DataKeyKind::IndexMetadata(MetadataKey::new(b"text_manifest:idx")),
        }
        .to_bytes();
        let legacy_vector = DataKey::Data {
            scope: DataScope::LegacyUnscoped,
            kind: DataKeyKind::Vector(vector::VectorKey::IndexMetadata(
                vector::VectorIndexMetadataKey::new(7),
            )),
        }
        .to_bytes();
        let tenant_a_vector = DataKey::Data {
            scope: DataScope::Tenant(tenant_a),
            kind: DataKeyKind::Vector(vector::VectorKey::IndexMetadata(
                vector::VectorIndexMetadataKey::new(7),
            )),
        }
        .to_bytes();

        assert_ne!(legacy_metadata, tenant_a_metadata);
        assert_ne!(tenant_a_metadata, tenant_b_metadata);
        assert_eq!(
            DataScope::Tenant(tenant_a)
                .strip_key(&tenant_a_metadata)
                .expect("tenant a metadata strips"),
            legacy_metadata.as_ref()
        );
        assert_eq!(
            DataScope::Tenant(tenant_a)
                .strip_key(&tenant_a_vector)
                .expect("tenant a vector strips"),
            legacy_vector.as_ref()
        );
        assert!(DataScope::Tenant(tenant_b)
            .strip_key(&tenant_a_vector)
            .is_none());
    }
}
