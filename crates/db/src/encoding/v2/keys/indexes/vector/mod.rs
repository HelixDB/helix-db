//! Vector-index key dispatch and shared physical format constants.

use bytes::{BufMut, Bytes};

use crate::encoding::{error::EncodingError, NodeId};

const KEYSPACE_PREFIX_LEN: usize = core::mem::size_of::<u8>();
const INDEX_TYPE_LEN: usize = core::mem::size_of::<u8>();
pub(crate) const INDEX_ID_LEN: usize = core::mem::size_of::<u64>();
const KEY_KIND_LEN: usize = core::mem::size_of::<u8>();
const ORDER_CODE_LEN: usize = core::mem::size_of::<u64>();
const LAYER_LEN: usize = core::mem::size_of::<u16>();
const NODE_ID_LEN: usize = core::mem::size_of::<NodeId>();

pub(crate) const DEFAULT_INDEX_TYPE_OFFSET: usize = KEYSPACE_PREFIX_LEN;
pub(crate) const DEFAULT_INDEX_ID_OFFSET: usize = KEYSPACE_PREFIX_LEN + INDEX_TYPE_LEN;
pub(crate) const DEFAULT_KIND_OFFSET: usize = KEYSPACE_PREFIX_LEN + INDEX_TYPE_LEN + INDEX_ID_LEN;

const VECTOR_INDEX_ID_OFFSET: usize = KEYSPACE_PREFIX_LEN;
const VECTOR_KIND_OFFSET: usize = KEYSPACE_PREFIX_LEN + INDEX_ID_LEN;
const VECTOR_PAYLOAD_OFFSET: usize = KEYSPACE_PREFIX_LEN + INDEX_ID_LEN + KEY_KIND_LEN;

pub(crate) const INDEX_TYPE_VECTOR: u8 = 0x03;
pub(crate) const KEY_SPACE_INDEX: u8 = 0x03;
pub(crate) const VECTOR_HOT_KEYSPACE_PREFIX: u8 = 0xF0;
pub(crate) const VECTOR_L0_KEYSPACE_PREFIX: u8 = 0xF1;

pub(crate) const KEY_KIND_META: u8 = 0x01;
pub(crate) const KEY_KIND_VECTOR: u8 = 0x02;
pub(crate) const KEY_KIND_ENTRY_CAND_SORTED: u8 = 0x04;
pub(crate) const KEY_KIND_ENTRY_CAND_NODE: u8 = 0x05;
pub(crate) const KEY_KIND_TXN_GUARD: u8 = 0x09;
pub(crate) const KEY_KIND_UPPER_NEIGHBORS: u8 = 0x11;
pub(crate) const KEY_KIND_SIMHASH: u8 = 0x12;
pub(crate) const KEY_KIND_UPPER_VECTOR: u8 = 0x13;
pub(crate) const KEY_KIND_REVERSE_EDGE: u8 = 0x15;
pub(crate) const KEY_KIND_LAYER0_VEC_KS: u8 = 0x16;
pub(crate) const KEY_KIND_SIMHASH_DIRECTORY: u8 = 0x17;

pub(crate) const DEFAULT_PREFIX_LEN: usize = KEYSPACE_PREFIX_LEN + INDEX_TYPE_LEN + INDEX_ID_LEN;
pub(crate) const DEFAULT_KEY_LEN: usize = DEFAULT_PREFIX_LEN + KEY_KIND_LEN;
pub(crate) const VECTOR_INDEX_PREFIX_LEN: usize = KEYSPACE_PREFIX_LEN + INDEX_ID_LEN;
pub(crate) const VECTOR_KIND_PREFIX_LEN: usize = VECTOR_INDEX_PREFIX_LEN + KEY_KIND_LEN;
pub(crate) const VECTOR_NODE_KEY_LEN: usize = VECTOR_KIND_PREFIX_LEN + NODE_ID_LEN;
pub(crate) const VECTOR_LAYER_NODE_KEY_LEN: usize =
    VECTOR_KIND_PREFIX_LEN + LAYER_LEN + NODE_ID_LEN;
pub(crate) const VECTOR_ORDERED_KEY_LEN: usize =
    VECTOR_KIND_PREFIX_LEN + ORDER_CODE_LEN + NODE_ID_LEN;
pub(crate) const VECTOR_REVERSE_EDGE_KEY_LEN: usize =
    VECTOR_KIND_PREFIX_LEN + NODE_ID_LEN + LAYER_LEN + NODE_ID_LEN;

/// Exhaustive physical keyspace lanes owned by one vector index ID.
///
/// Collision checks and cleanup must inspect all three lanes because metadata
mod entry_candidates;
mod items;
mod layer0;
pub(crate) mod metadata;
mod reverse_edges;
mod simhash;
mod storage_prefixes;
mod upper_layers;

pub(crate) use entry_candidates::*;
pub(crate) use items::*;
pub(crate) use layer0::*;
pub(crate) use metadata::*;
pub(crate) use reverse_edges::*;
pub(crate) use simhash::*;
pub(crate) use storage_prefixes::*;
pub(crate) use upper_layers::*;

use crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey;
#[cfg(test)]
use crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey as VectorTxnGuardKey;

/// Typed vector-index key shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorKey {
    /// `[0x03][0x03][index_id:8][kind=meta]`
    IndexMetadata(VectorIndexMetadataKey),
    /// `[0x03][0x03][index_id:8]`
    IndexPrefix(VectorIndexPrefixKey),
    /// `[0x03][0x03][index_id:8][kind=txn_guard]`
    TxnGuard(LegacyVectorTxnGuardKey),
    /// `[0xF0][index_id:8][kind=l0_vec_ks][node_id:8]`
    Layer0Neighbors(VectorLayer0NeighborsKey),
    /// `[0xF1][index_id:8][kind=vec]`
    VectorPrefix(VectorItemPrefixKey),
    /// `[0xF1][index_id:8][kind=vec][order_code:8][node_id:8]`
    Vector(VectorItemKey),
    /// `[0xF1][index_id:8][kind=simhash_directory]`
    SimHashDirectoryPrefix(VectorSimHashDirectoryPrefixKey),
    /// `[0xF1][index_id:8][kind=simhash_directory][order_code:8][node_id:8]`
    SimHashDirectory(VectorSimHashDirectoryKey),
    /// `[0xF1][index_id:8][kind=cand_sorted]`
    EntryCandidatePrefix(VectorEntryCandidatePrefixKey),
    /// `[0xF1][index_id:8][kind=cand_sorted][inv_layer:2][node_id:8]`
    EntryCandidateSorted(VectorEntryCandidateKey),
    /// `[0xF1][index_id:8][kind=cand_node][node_id:8]`
    EntryCandidateNode(VectorEntryCandidateNodeKey),
    /// `[0xF0][index_id:8]`
    MemoryPrefix(VectorMemoryPrefixKey),
    /// `[0xF1][index_id:8]`
    L0Prefix(VectorL0PrefixKey),
    /// `[0xF0][index_id:8][kind=upper][layer:2][node_id:8]`
    UpperNeighbors(VectorUpperNeighborsKey),
    /// `[0xF0][index_id:8][kind=simhash][node_id:8]`
    SimHash(VectorSimHashKey),
    /// `[0xF0][index_id:8][kind=upper_vec][node_id:8]`
    UpperVector(VectorUpperVectorKey),
    /// `[0xF1][index_id:8][kind=reverse_edge][target_node_id:8]`
    ReverseEdgePrefix(VectorReverseEdgePrefixKey),
    /// `[0xF1][index_id:8][kind=reverse_edge][target_node_id:8][layer:2][source_node_id:8]`
    ReverseEdge(VectorReverseEdgeKey),
}

impl VectorKey {
    /// Returns the exhaustive physical storage lane for this current key.
    ///
    /// Collision checks and generation cleanup match this method without a
    /// wildcard arm. Adding a new vector key variant therefore requires an
    /// explicit ownership decision before the crate can compile.
    pub(crate) const fn storage_lane(&self) -> VectorStorageLane {
        match self {
            Self::IndexMetadata(_) | Self::IndexPrefix(_) | Self::TxnGuard(_) => {
                VectorStorageLane::Core
            }
            Self::Layer0Neighbors(_)
            | Self::MemoryPrefix(_)
            | Self::UpperNeighbors(_)
            | Self::SimHash(_)
            | Self::UpperVector(_) => VectorStorageLane::Hot,
            Self::VectorPrefix(_)
            | Self::Vector(_)
            | Self::SimHashDirectoryPrefix(_)
            | Self::SimHashDirectory(_)
            | Self::EntryCandidatePrefix(_)
            | Self::EntryCandidateSorted(_)
            | Self::EntryCandidateNode(_)
            | Self::L0Prefix(_)
            | Self::ReverseEdgePrefix(_)
            | Self::ReverseEdge(_) => VectorStorageLane::Layer0,
        }
    }

    /// Returns the existing `u64` physical index ID embedded by this key.
    pub(crate) const fn index_id(&self) -> u64 {
        match self {
            Self::IndexMetadata(key) => key.index_id(),
            Self::IndexPrefix(key) => key.index_id(),
            Self::TxnGuard(key) => key.index_id(),
            Self::Layer0Neighbors(key) => key.index_id(),
            Self::VectorPrefix(key) => key.index_id(),
            Self::Vector(key) => key.index_id(),
            Self::SimHashDirectoryPrefix(key) => key.index_id(),
            Self::SimHashDirectory(key) => key.index_id(),
            Self::EntryCandidatePrefix(key) => key.index_id(),
            Self::EntryCandidateSorted(key) => key.index_id(),
            Self::EntryCandidateNode(key) => key.index_id(),
            Self::MemoryPrefix(key) => key.index_id(),
            Self::L0Prefix(key) => key.index_id(),
            Self::UpperNeighbors(key) => key.index_id(),
            Self::SimHash(key) => key.index_id(),
            Self::UpperVector(key) => key.index_id(),
            Self::ReverseEdgePrefix(key) => key.index_id(),
            Self::ReverseEdge(key) => key.index_id(),
        }
    }

    pub(crate) fn is_vector_keyspace(slice: &[u8]) -> bool {
        matches!(
            slice.first(),
            Some(&VECTOR_HOT_KEYSPACE_PREFIX | &VECTOR_L0_KEYSPACE_PREFIX)
        ) || (slice.len() >= KEYSPACE_PREFIX_LEN + INDEX_TYPE_LEN
            && slice[0] == KEY_SPACE_INDEX
            && slice[KEYSPACE_PREFIX_LEN] == INDEX_TYPE_VECTOR)
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        match self {
            Self::IndexMetadata(key) => key.encoded_len(),
            Self::IndexPrefix(key) => key.encoded_len(),
            Self::TxnGuard(key) => key.encoded_len(),
            Self::Layer0Neighbors(key) => key.encoded_len(),
            Self::VectorPrefix(key) => key.encoded_len(),
            Self::Vector(key) => key.encoded_len(),
            Self::SimHashDirectoryPrefix(key) => key.encoded_len(),
            Self::SimHashDirectory(key) => key.encoded_len(),
            Self::EntryCandidatePrefix(key) => key.encoded_len(),
            Self::EntryCandidateSorted(key) => key.encoded_len(),
            Self::EntryCandidateNode(key) => key.encoded_len(),
            Self::MemoryPrefix(key) => key.encoded_len(),
            Self::L0Prefix(key) => key.encoded_len(),
            Self::UpperNeighbors(key) => key.encoded_len(),
            Self::SimHash(key) => key.encoded_len(),
            Self::UpperVector(key) => key.encoded_len(),
            Self::ReverseEdgePrefix(key) => key.encoded_len(),
            Self::ReverseEdge(key) => key.encoded_len(),
        }
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        match self {
            Self::IndexMetadata(key) => key.encode_into(buf),
            Self::IndexPrefix(key) => key.encode_into(buf),
            Self::TxnGuard(key) => key.encode_into(buf),
            Self::Layer0Neighbors(key) => key.encode_into(buf),
            Self::VectorPrefix(key) => key.encode_into(buf),
            Self::Vector(key) => key.encode_into(buf),
            Self::SimHashDirectoryPrefix(key) => key.encode_into(buf),
            Self::SimHashDirectory(key) => key.encode_into(buf),
            Self::EntryCandidatePrefix(key) => key.encode_into(buf),
            Self::EntryCandidateSorted(key) => key.encode_into(buf),
            Self::EntryCandidateNode(key) => key.encode_into(buf),
            Self::MemoryPrefix(key) => key.encode_into(buf),
            Self::L0Prefix(key) => key.encode_into(buf),
            Self::UpperNeighbors(key) => key.encode_into(buf),
            Self::SimHash(key) => key.encode_into(buf),
            Self::UpperVector(key) => key.encode_into(buf),
            Self::ReverseEdgePrefix(key) => key.encode_into(buf),
            Self::ReverseEdge(key) => key.encode_into(buf),
        }
    }

    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        if slice.is_empty() {
            return Err(EncodingError::BufferTooShort {
                expected: KEYSPACE_PREFIX_LEN,
                actual: 0,
            });
        }

        match slice[0] {
            KEY_SPACE_INDEX => {
                if slice.len() < DEFAULT_INDEX_TYPE_OFFSET + INDEX_TYPE_LEN {
                    return Err(EncodingError::BufferTooShort {
                        expected: DEFAULT_INDEX_TYPE_OFFSET + INDEX_TYPE_LEN,
                        actual: slice.len(),
                    });
                }
                if slice[DEFAULT_INDEX_TYPE_OFFSET] != INDEX_TYPE_VECTOR {
                    return Err(EncodingError::InvalidKey(format!(
                        "expected vector index type ({INDEX_TYPE_VECTOR:#04x}), got {:#04x}",
                        slice[DEFAULT_INDEX_TYPE_OFFSET]
                    )));
                }
                if slice.len() < DEFAULT_PREFIX_LEN {
                    return Err(EncodingError::BufferTooShort {
                        expected: DEFAULT_PREFIX_LEN,
                        actual: slice.len(),
                    });
                }

                match slice.len() {
                    DEFAULT_PREFIX_LEN => Ok(Self::IndexPrefix(
                        VectorIndexPrefixKey::parse_from_slice(slice)?,
                    )),
                    DEFAULT_KEY_LEN => match slice[DEFAULT_KIND_OFFSET] {
                        KEY_KIND_META => Ok(Self::IndexMetadata(
                            VectorIndexMetadataKey::parse_from_slice(slice)?,
                        )),
                        KEY_KIND_TXN_GUARD => {
                            Ok(Self::TxnGuard(LegacyVectorTxnGuardKey::parse_from_slice(slice)?))
                        }
                        kind => Err(EncodingError::InvalidKey(format!(
                            "invalid vector default key kind: {kind:#04x}"
                        ))),
                    },
                    actual => Err(EncodingError::InvalidKey(format!(
                        "invalid vector default key length: expected {DEFAULT_PREFIX_LEN} or {DEFAULT_KEY_LEN} bytes, got {actual}"
                    ))),
                }
            }
            VECTOR_HOT_KEYSPACE_PREFIX => {
                if slice.len() < VECTOR_INDEX_PREFIX_LEN {
                    return Err(EncodingError::BufferTooShort {
                        expected: VECTOR_INDEX_PREFIX_LEN,
                        actual: slice.len(),
                    });
                }
                if slice.len() == VECTOR_INDEX_PREFIX_LEN {
                    return Ok(Self::MemoryPrefix(VectorMemoryPrefixKey::parse_from_slice(
                        slice,
                    )?));
                }

                match (slice[VECTOR_KIND_OFFSET], slice.len()) {
                    (KEY_KIND_LAYER0_VEC_KS, VECTOR_NODE_KEY_LEN) => Ok(Self::Layer0Neighbors(
                        VectorLayer0NeighborsKey::parse_from_slice(slice)?,
                    )),
                    (KEY_KIND_UPPER_NEIGHBORS, VECTOR_LAYER_NODE_KEY_LEN) => Ok(
                        Self::UpperNeighbors(VectorUpperNeighborsKey::parse_from_slice(slice)?),
                    ),
                    (KEY_KIND_SIMHASH, VECTOR_NODE_KEY_LEN) => {
                        Ok(Self::SimHash(VectorSimHashKey::parse_from_slice(slice)?))
                    }
                    (KEY_KIND_UPPER_VECTOR, VECTOR_NODE_KEY_LEN) => Ok(Self::UpperVector(
                        VectorUpperVectorKey::parse_from_slice(slice)?,
                    )),
                    (known, actual)
                        if matches!(
                            known,
                            KEY_KIND_LAYER0_VEC_KS
                                | KEY_KIND_UPPER_NEIGHBORS
                                | KEY_KIND_SIMHASH
                                | KEY_KIND_UPPER_VECTOR
                        ) =>
                    {
                        Err(EncodingError::InvalidKey(format!(
                            "invalid vector-hot key length for kind {known:#04x}: got {actual}"
                        )))
                    }
                    (unknown, _) => Err(EncodingError::InvalidKey(format!(
                        "invalid vector-hot key kind: {unknown:#04x}"
                    ))),
                }
            }
            VECTOR_L0_KEYSPACE_PREFIX => {
                if slice.len() < VECTOR_INDEX_PREFIX_LEN {
                    return Err(EncodingError::BufferTooShort {
                        expected: VECTOR_INDEX_PREFIX_LEN,
                        actual: slice.len(),
                    });
                }
                if slice.len() == VECTOR_INDEX_PREFIX_LEN {
                    return Ok(Self::L0Prefix(VectorL0PrefixKey::parse_from_slice(slice)?));
                }

                match (slice[VECTOR_KIND_OFFSET], slice.len()) {
                    (KEY_KIND_VECTOR, VECTOR_KIND_PREFIX_LEN) => Ok(Self::VectorPrefix(
                        VectorItemPrefixKey::parse_from_slice(slice)?,
                    )),
                    (KEY_KIND_VECTOR, VECTOR_ORDERED_KEY_LEN) => {
                        Ok(Self::Vector(VectorItemKey::parse_from_slice(slice)?))
                    }
                    (KEY_KIND_SIMHASH_DIRECTORY, VECTOR_KIND_PREFIX_LEN) => {
                        Ok(Self::SimHashDirectoryPrefix(
                            VectorSimHashDirectoryPrefixKey::parse_from_slice(slice)?,
                        ))
                    }
                    (KEY_KIND_SIMHASH_DIRECTORY, VECTOR_ORDERED_KEY_LEN) => Ok(
                        Self::SimHashDirectory(VectorSimHashDirectoryKey::parse_from_slice(slice)?),
                    ),
                    (KEY_KIND_ENTRY_CAND_SORTED, VECTOR_KIND_PREFIX_LEN) => {
                        Ok(Self::EntryCandidatePrefix(
                            VectorEntryCandidatePrefixKey::parse_from_slice(slice)?,
                        ))
                    }
                    (KEY_KIND_ENTRY_CAND_SORTED, VECTOR_LAYER_NODE_KEY_LEN) => {
                        Ok(Self::EntryCandidateSorted(
                            VectorEntryCandidateKey::parse_from_slice(slice)?,
                        ))
                    }
                    (KEY_KIND_ENTRY_CAND_NODE, VECTOR_NODE_KEY_LEN) => {
                        Ok(Self::EntryCandidateNode(
                            VectorEntryCandidateNodeKey::parse_from_slice(slice)?,
                        ))
                    }
                    (KEY_KIND_REVERSE_EDGE, VECTOR_NODE_KEY_LEN) => Ok(Self::ReverseEdgePrefix(
                        VectorReverseEdgePrefixKey::parse_from_slice(slice)?,
                    )),
                    (KEY_KIND_REVERSE_EDGE, VECTOR_REVERSE_EDGE_KEY_LEN) => Ok(Self::ReverseEdge(
                        VectorReverseEdgeKey::parse_from_slice(slice)?,
                    )),
                    (known, actual)
                        if matches!(
                            known,
                            KEY_KIND_VECTOR
                                | KEY_KIND_ENTRY_CAND_SORTED
                                | KEY_KIND_ENTRY_CAND_NODE
                                | KEY_KIND_SIMHASH_DIRECTORY
                                | KEY_KIND_REVERSE_EDGE
                        ) =>
                    {
                        Err(EncodingError::InvalidKey(format!(
                            "invalid vector-l0 key length for kind {known:#04x}: got {actual}"
                        )))
                    }
                    (unknown, _) => Err(EncodingError::InvalidKey(format!(
                        "invalid vector-l0 key kind: {unknown:#04x}"
                    ))),
                }
            }
            invalid => Err(EncodingError::InvalidKeyPrefix(invalid)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use crate::encoding::v2::keys::indexes::{
        range::{EdgeRangeIndexDirection, EdgeRangeIndexKey},
        EdgeDirection,
    };

    use super::*;

    const INDEX_ID: u64 = 0x0102_0304_0506_0708;
    const ORDER_CODE: u64 = 0x1112_1314_1516_1718;
    const NODE_ID: NodeId = 0x2122_2324_2526_2728;
    const OTHER_NODE_ID: NodeId = 0x3132_3334_3536_3738;
    const LAYER: u16 = 0x4243;

    fn expected_default_key(kind: Option<u8>) -> Vec<u8> {
        let mut key = vec![KEY_SPACE_INDEX, INDEX_TYPE_VECTOR];
        key.extend_from_slice(&INDEX_ID.to_be_bytes());
        let Some(kind) = kind else {
            return key;
        };
        key.push(kind);
        key
    }

    fn expected_vector_keyspace_prefix(keyspace: u8) -> Vec<u8> {
        let mut key = vec![keyspace];
        key.extend_from_slice(&INDEX_ID.to_be_bytes());
        key
    }

    #[test]
    fn default_keyspace_keys_have_exact_layouts() {
        assert_eq!(
            VectorKey::IndexPrefix(VectorIndexPrefixKey::new(INDEX_ID))
                .to_bytes()
                .as_ref(),
            expected_default_key(None).as_slice()
        );
        assert_eq!(
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID))
                .to_bytes()
                .as_ref(),
            expected_default_key(Some(KEY_KIND_META)).as_slice()
        );
        assert_eq!(
            VectorKey::TxnGuard(VectorTxnGuardKey::new(INDEX_ID))
                .to_bytes()
                .as_ref(),
            expected_default_key(Some(KEY_KIND_TXN_GUARD)).as_slice()
        );
    }

    #[test]
    fn metadata_scan_classifies_colliding_edge_range_rows() {
        let edge_range = EdgeRangeIndexKey::new(
            EdgeDirection::Out,
            EdgeRangeIndexDirection::Asc,
            NODE_ID,
            [1, 2, 3, 4],
            Cow::Borrowed("twenty-byte-value---"),
            OTHER_NODE_ID,
        );
        let mut edge_range_bytes = Vec::new();
        edge_range.encode_into(&mut edge_range_bytes);
        assert_eq!(edge_range_bytes.len(), 45);
        assert_eq!(
            &edge_range_bytes[0..2],
            &[KEY_SPACE_INDEX, INDEX_TYPE_VECTOR]
        );
        assert_eq!(
            VectorMetadataScanPrefix::new()
                .parse_row(&edge_range_bytes)
                .expect("colliding property key is valid"),
            None
        );

        let metadata = VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID));
        assert_eq!(
            VectorMetadataScanPrefix::new()
                .parse_row(&metadata.to_bytes())
                .expect("metadata key is valid"),
            Some(VectorMetadataScanRow::IndexMetadata(
                VectorIndexMetadataKey::new(INDEX_ID)
            ))
        );
        assert_eq!(
            VectorMetadataScanPrefix::new()
                .parse_row(&VectorKey::IndexPrefix(VectorIndexPrefixKey::new(INDEX_ID)).to_bytes())
                .expect("exact graph-prefix collision is valid"),
            None
        );
    }

    #[test]
    fn vector_hot_keys_have_exact_layouts() {
        let mut layer0 = expected_vector_keyspace_prefix(VECTOR_HOT_KEYSPACE_PREFIX);
        layer0.push(KEY_KIND_LAYER0_VEC_KS);
        layer0.extend_from_slice(&NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(INDEX_ID, NODE_ID))
                .to_bytes()
                .as_ref(),
            layer0.as_slice()
        );

        let mut upper = expected_vector_keyspace_prefix(VECTOR_HOT_KEYSPACE_PREFIX);
        upper.push(KEY_KIND_UPPER_NEIGHBORS);
        upper.extend_from_slice(&LAYER.to_be_bytes());
        upper.extend_from_slice(&NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(INDEX_ID, LAYER, NODE_ID))
                .to_bytes()
                .as_ref(),
            upper.as_slice()
        );

        let mut simhash = expected_vector_keyspace_prefix(VECTOR_HOT_KEYSPACE_PREFIX);
        simhash.push(KEY_KIND_SIMHASH);
        simhash.extend_from_slice(&NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::SimHash(VectorSimHashKey::new(INDEX_ID, NODE_ID))
                .to_bytes()
                .as_ref(),
            simhash.as_slice()
        );

        let mut upper_vector = expected_vector_keyspace_prefix(VECTOR_HOT_KEYSPACE_PREFIX);
        upper_vector.push(KEY_KIND_UPPER_VECTOR);
        upper_vector.extend_from_slice(&NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::UpperVector(VectorUpperVectorKey::new(INDEX_ID, NODE_ID))
                .to_bytes()
                .as_ref(),
            upper_vector.as_slice()
        );

        assert_eq!(
            VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(INDEX_ID))
                .to_bytes()
                .as_ref(),
            expected_vector_keyspace_prefix(VECTOR_HOT_KEYSPACE_PREFIX).as_slice()
        );
    }

    #[test]
    fn vector_l0_keys_have_exact_layouts() {
        let mut vector = expected_vector_keyspace_prefix(VECTOR_L0_KEYSPACE_PREFIX);
        vector.push(KEY_KIND_VECTOR);
        assert_eq!(
            VectorKey::VectorPrefix(VectorItemPrefixKey::new(INDEX_ID))
                .to_bytes()
                .as_ref(),
            vector.as_slice()
        );
        vector.extend_from_slice(&ORDER_CODE.to_be_bytes());
        vector.extend_from_slice(&NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::Vector(VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID))
                .to_bytes()
                .as_ref(),
            vector.as_slice()
        );

        let mut directory_prefix = expected_vector_keyspace_prefix(VECTOR_L0_KEYSPACE_PREFIX);
        directory_prefix.push(KEY_KIND_SIMHASH_DIRECTORY);
        assert_eq!(
            VectorKey::SimHashDirectoryPrefix(VectorSimHashDirectoryPrefixKey::new(INDEX_ID))
                .to_bytes()
                .as_ref(),
            directory_prefix.as_slice()
        );

        let mut directory = directory_prefix;
        directory.extend_from_slice(&ORDER_CODE.to_be_bytes());
        directory.extend_from_slice(&NODE_ID.to_be_bytes());
        let directory_key = VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
            INDEX_ID, ORDER_CODE, NODE_ID,
        ));
        assert_eq!(directory_key.to_bytes().as_ref(), directory.as_slice());
        assert_eq!(
            VectorKey::parse_from_slice(&directory).unwrap(),
            directory_key
        );

        let mut entry_prefix = expected_vector_keyspace_prefix(VECTOR_L0_KEYSPACE_PREFIX);
        entry_prefix.push(KEY_KIND_ENTRY_CAND_SORTED);
        assert_eq!(
            VectorKey::EntryCandidatePrefix(VectorEntryCandidatePrefixKey::new(INDEX_ID))
                .to_bytes()
                .as_ref(),
            entry_prefix.as_slice()
        );

        let mut entry_sorted = entry_prefix;
        entry_sorted.extend_from_slice(&(u16::MAX - LAYER).to_be_bytes());
        entry_sorted.extend_from_slice(&NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(INDEX_ID, LAYER, NODE_ID))
                .to_bytes()
                .as_ref(),
            entry_sorted.as_slice()
        );

        let mut entry_node = expected_vector_keyspace_prefix(VECTOR_L0_KEYSPACE_PREFIX);
        entry_node.push(KEY_KIND_ENTRY_CAND_NODE);
        entry_node.extend_from_slice(&NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID))
                .to_bytes()
                .as_ref(),
            entry_node.as_slice()
        );

        assert_eq!(
            VectorKey::L0Prefix(VectorL0PrefixKey::new(INDEX_ID))
                .to_bytes()
                .as_ref(),
            expected_vector_keyspace_prefix(VECTOR_L0_KEYSPACE_PREFIX).as_slice()
        );
    }

    #[test]
    fn reverse_edge_keys_have_exact_layouts() {
        let mut prefix = expected_vector_keyspace_prefix(VECTOR_L0_KEYSPACE_PREFIX);
        prefix.push(KEY_KIND_REVERSE_EDGE);
        prefix.extend_from_slice(&NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(INDEX_ID, NODE_ID))
                .to_bytes()
                .as_ref(),
            prefix.as_slice()
        );

        let mut key = prefix;
        key.extend_from_slice(&LAYER.to_be_bytes());
        key.extend_from_slice(&OTHER_NODE_ID.to_be_bytes());
        assert_eq!(
            VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
                INDEX_ID,
                NODE_ID,
                LAYER,
                OTHER_NODE_ID
            ))
            .to_bytes()
            .as_ref(),
            key.as_slice()
        );
    }

    #[test]
    fn vector_key_encode_into_matches_to_bytes() {
        let cases = [
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID)),
            VectorKey::IndexPrefix(VectorIndexPrefixKey::new(INDEX_ID)),
            VectorKey::TxnGuard(VectorTxnGuardKey::new(INDEX_ID)),
            VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(INDEX_ID, NODE_ID)),
            VectorKey::VectorPrefix(VectorItemPrefixKey::new(INDEX_ID)),
            VectorKey::Vector(VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID)),
            VectorKey::SimHashDirectoryPrefix(VectorSimHashDirectoryPrefixKey::new(INDEX_ID)),
            VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                INDEX_ID, ORDER_CODE, NODE_ID,
            )),
            VectorKey::EntryCandidatePrefix(VectorEntryCandidatePrefixKey::new(INDEX_ID)),
            VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(INDEX_ID, LAYER, NODE_ID)),
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID)),
            VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(INDEX_ID)),
            VectorKey::L0Prefix(VectorL0PrefixKey::new(INDEX_ID)),
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(INDEX_ID, LAYER, NODE_ID)),
            VectorKey::SimHash(VectorSimHashKey::new(INDEX_ID, NODE_ID)),
            VectorKey::UpperVector(VectorUpperVectorKey::new(INDEX_ID, NODE_ID)),
            VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(INDEX_ID, NODE_ID)),
            VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
                INDEX_ID,
                NODE_ID,
                LAYER,
                OTHER_NODE_ID,
            )),
        ];

        for key in cases {
            let expected = key.to_bytes();
            assert_eq!(key.encoded_len(), expected.len());
            let mut encoded = Vec::new();
            key.encode_into(&mut encoded);
            assert_eq!(encoded.as_slice(), expected.as_ref());
        }
    }

    #[test]
    fn typed_vector_key_accessors_cover_every_shape() {
        let metadata = VectorIndexMetadataKey::new(INDEX_ID);
        assert_eq!(metadata.index_id(), INDEX_ID);
        assert_eq!(metadata.encoded_len(), DEFAULT_KEY_LEN);

        let index_prefix = VectorIndexPrefixKey::new(INDEX_ID);
        assert_eq!(index_prefix.index_id(), INDEX_ID);
        assert_eq!(index_prefix.encoded_len(), DEFAULT_PREFIX_LEN);

        let txn_guard = VectorTxnGuardKey::new(INDEX_ID);
        assert_eq!(txn_guard.index_id(), INDEX_ID);
        assert_eq!(txn_guard.encoded_len(), DEFAULT_KEY_LEN);

        let layer0_neighbors = VectorLayer0NeighborsKey::new(INDEX_ID, NODE_ID);
        assert_eq!(layer0_neighbors.index_id(), INDEX_ID);
        assert_eq!(layer0_neighbors.node_id(), NODE_ID);
        assert_eq!(layer0_neighbors.encoded_len(), VECTOR_NODE_KEY_LEN);

        let vector_prefix = VectorItemPrefixKey::new(INDEX_ID);
        assert_eq!(vector_prefix.index_id(), INDEX_ID);
        assert_eq!(vector_prefix.encoded_len(), VECTOR_KIND_PREFIX_LEN);

        let vector = VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID);
        assert_eq!(vector.index_id(), INDEX_ID);
        assert_eq!(vector.order_code(), ORDER_CODE);
        assert_eq!(vector.node_id(), NODE_ID);
        assert_eq!(vector.encoded_len(), VECTOR_ORDERED_KEY_LEN);

        let directory_prefix = VectorSimHashDirectoryPrefixKey::new(INDEX_ID);
        assert_eq!(directory_prefix.index_id(), INDEX_ID);
        assert_eq!(directory_prefix.encoded_len(), VECTOR_KIND_PREFIX_LEN);

        let directory = VectorSimHashDirectoryKey::new(INDEX_ID, ORDER_CODE, NODE_ID);
        assert_eq!(directory.index_id(), INDEX_ID);
        assert_eq!(directory.order_code(), ORDER_CODE);
        assert_eq!(directory.node_id(), NODE_ID);
        assert_eq!(directory.encoded_len(), VECTOR_ORDERED_KEY_LEN);

        let entry_prefix = VectorEntryCandidatePrefixKey::new(INDEX_ID);
        assert_eq!(entry_prefix.index_id(), INDEX_ID);
        assert_eq!(entry_prefix.encoded_len(), VECTOR_KIND_PREFIX_LEN);

        let entry_sorted = VectorEntryCandidateKey::new(INDEX_ID, LAYER, NODE_ID);
        assert_eq!(entry_sorted.index_id(), INDEX_ID);
        assert_eq!(entry_sorted.layer(), LAYER);
        assert_eq!(entry_sorted.node_id(), NODE_ID);
        assert_eq!(entry_sorted.encoded_len(), VECTOR_LAYER_NODE_KEY_LEN);

        let entry_node = VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID);
        assert_eq!(entry_node.index_id(), INDEX_ID);
        assert_eq!(entry_node.node_id(), NODE_ID);
        assert_eq!(entry_node.encoded_len(), VECTOR_NODE_KEY_LEN);

        let memory_prefix = VectorMemoryPrefixKey::new(INDEX_ID);
        assert_eq!(memory_prefix.index_id(), INDEX_ID);
        assert_eq!(memory_prefix.encoded_len(), VECTOR_INDEX_PREFIX_LEN);

        let l0_prefix = VectorL0PrefixKey::new(INDEX_ID);
        assert_eq!(l0_prefix.index_id(), INDEX_ID);
        assert_eq!(l0_prefix.encoded_len(), VECTOR_INDEX_PREFIX_LEN);

        let upper_neighbors = VectorUpperNeighborsKey::new(INDEX_ID, LAYER, NODE_ID);
        assert_eq!(upper_neighbors.index_id(), INDEX_ID);
        assert_eq!(upper_neighbors.layer(), LAYER);
        assert_eq!(upper_neighbors.node_id(), NODE_ID);
        assert_eq!(upper_neighbors.encoded_len(), VECTOR_LAYER_NODE_KEY_LEN);

        let simhash = VectorSimHashKey::new(INDEX_ID, NODE_ID);
        assert_eq!(simhash.index_id(), INDEX_ID);
        assert_eq!(simhash.node_id(), NODE_ID);
        assert_eq!(simhash.encoded_len(), VECTOR_NODE_KEY_LEN);

        let upper_vector = VectorUpperVectorKey::new(INDEX_ID, NODE_ID);
        assert_eq!(upper_vector.index_id(), INDEX_ID);
        assert_eq!(upper_vector.node_id(), NODE_ID);
        assert_eq!(upper_vector.encoded_len(), VECTOR_NODE_KEY_LEN);

        let reverse_prefix = VectorReverseEdgePrefixKey::new(INDEX_ID, NODE_ID);
        assert_eq!(reverse_prefix.index_id(), INDEX_ID);
        assert_eq!(reverse_prefix.target_node_id(), NODE_ID);
        assert_eq!(reverse_prefix.encoded_len(), VECTOR_NODE_KEY_LEN);

        let reverse = VectorReverseEdgeKey::new(INDEX_ID, NODE_ID, LAYER, OTHER_NODE_ID);
        assert_eq!(reverse.index_id(), INDEX_ID);
        assert_eq!(reverse.target_node_id(), NODE_ID);
        assert_eq!(reverse.layer(), LAYER);
        assert_eq!(reverse.source_node_id(), OTHER_NODE_ID);
        assert_eq!(reverse.encoded_len(), VECTOR_REVERSE_EDGE_KEY_LEN);
    }

    #[test]
    fn concrete_key_parsers_round_trip_all_vector_key_shapes() {
        assert_eq!(
            VectorIndexMetadataKey::parse_from_slice(
                &VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID)).to_bytes()
            )
            .unwrap(),
            VectorIndexMetadataKey::new(INDEX_ID)
        );
        assert_eq!(
            VectorIndexPrefixKey::parse_from_slice(
                &VectorKey::IndexPrefix(VectorIndexPrefixKey::new(INDEX_ID)).to_bytes()
            )
            .unwrap(),
            VectorIndexPrefixKey::new(INDEX_ID)
        );
        assert_eq!(
            VectorTxnGuardKey::parse_from_slice(
                &VectorKey::TxnGuard(VectorTxnGuardKey::new(INDEX_ID)).to_bytes()
            )
            .unwrap(),
            VectorTxnGuardKey::new(INDEX_ID)
        );
        assert_eq!(
            VectorLayer0NeighborsKey::parse_from_slice(
                &VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(INDEX_ID, NODE_ID))
                    .to_bytes()
            )
            .unwrap(),
            VectorLayer0NeighborsKey::new(INDEX_ID, NODE_ID)
        );
        assert_eq!(
            VectorItemPrefixKey::parse_from_slice(
                &VectorKey::VectorPrefix(VectorItemPrefixKey::new(INDEX_ID)).to_bytes()
            )
            .unwrap(),
            VectorItemPrefixKey::new(INDEX_ID)
        );
        assert_eq!(
            VectorItemKey::parse_from_slice(
                &VectorKey::Vector(VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID)).to_bytes()
            )
            .unwrap(),
            VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID)
        );
        assert_eq!(
            VectorSimHashDirectoryPrefixKey::parse_from_slice(
                &VectorKey::SimHashDirectoryPrefix(VectorSimHashDirectoryPrefixKey::new(INDEX_ID))
                    .to_bytes()
            )
            .unwrap(),
            VectorSimHashDirectoryPrefixKey::new(INDEX_ID)
        );
        assert_eq!(
            VectorSimHashDirectoryKey::parse_from_slice(
                &VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                    INDEX_ID, ORDER_CODE, NODE_ID
                ))
                .to_bytes()
            )
            .unwrap(),
            VectorSimHashDirectoryKey::new(INDEX_ID, ORDER_CODE, NODE_ID)
        );
        assert_eq!(
            VectorEntryCandidatePrefixKey::parse_from_slice(
                &VectorKey::EntryCandidatePrefix(VectorEntryCandidatePrefixKey::new(INDEX_ID))
                    .to_bytes()
            )
            .unwrap(),
            VectorEntryCandidatePrefixKey::new(INDEX_ID)
        );
        assert_eq!(
            VectorEntryCandidateKey::parse_from_slice(
                &VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(
                    INDEX_ID, LAYER, NODE_ID
                ))
                .to_bytes()
            )
            .unwrap(),
            VectorEntryCandidateKey::new(INDEX_ID, LAYER, NODE_ID)
        );
        assert_eq!(
            VectorEntryCandidateNodeKey::parse_from_slice(
                &VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID))
                    .to_bytes()
            )
            .unwrap(),
            VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID)
        );
        assert_eq!(
            VectorMemoryPrefixKey::parse_from_slice(
                &VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(INDEX_ID)).to_bytes()
            )
            .unwrap(),
            VectorMemoryPrefixKey::new(INDEX_ID)
        );
        assert_eq!(
            VectorL0PrefixKey::parse_from_slice(
                &VectorKey::L0Prefix(VectorL0PrefixKey::new(INDEX_ID)).to_bytes()
            )
            .unwrap(),
            VectorL0PrefixKey::new(INDEX_ID)
        );
        assert_eq!(
            VectorUpperNeighborsKey::parse_from_slice(
                &VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(INDEX_ID, LAYER, NODE_ID))
                    .to_bytes()
            )
            .unwrap(),
            VectorUpperNeighborsKey::new(INDEX_ID, LAYER, NODE_ID)
        );
        assert_eq!(
            VectorSimHashKey::parse_from_slice(
                &VectorKey::SimHash(VectorSimHashKey::new(INDEX_ID, NODE_ID)).to_bytes()
            )
            .unwrap(),
            VectorSimHashKey::new(INDEX_ID, NODE_ID)
        );
        assert_eq!(
            VectorUpperVectorKey::parse_from_slice(
                &VectorKey::UpperVector(VectorUpperVectorKey::new(INDEX_ID, NODE_ID)).to_bytes()
            )
            .unwrap(),
            VectorUpperVectorKey::new(INDEX_ID, NODE_ID)
        );
        assert_eq!(
            VectorReverseEdgePrefixKey::parse_from_slice(
                &VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(INDEX_ID, NODE_ID))
                    .to_bytes()
            )
            .unwrap(),
            VectorReverseEdgePrefixKey::new(INDEX_ID, NODE_ID)
        );
        assert_eq!(
            VectorReverseEdgeKey::parse_from_slice(
                &VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
                    INDEX_ID,
                    NODE_ID,
                    LAYER,
                    OTHER_NODE_ID
                ))
                .to_bytes()
            )
            .unwrap(),
            VectorReverseEdgeKey::new(INDEX_ID, NODE_ID, LAYER, OTHER_NODE_ID)
        );
    }

    #[test]
    fn parse_from_slice_round_trips_all_vector_key_variants() {
        let keys = [
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID)),
            VectorKey::IndexPrefix(VectorIndexPrefixKey::new(INDEX_ID)),
            VectorKey::TxnGuard(VectorTxnGuardKey::new(INDEX_ID)),
            VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(INDEX_ID, NODE_ID)),
            VectorKey::VectorPrefix(VectorItemPrefixKey::new(INDEX_ID)),
            VectorKey::Vector(VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID)),
            VectorKey::SimHashDirectoryPrefix(VectorSimHashDirectoryPrefixKey::new(INDEX_ID)),
            VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                INDEX_ID, ORDER_CODE, NODE_ID,
            )),
            VectorKey::EntryCandidatePrefix(VectorEntryCandidatePrefixKey::new(INDEX_ID)),
            VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(INDEX_ID, LAYER, NODE_ID)),
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID)),
            VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(INDEX_ID)),
            VectorKey::L0Prefix(VectorL0PrefixKey::new(INDEX_ID)),
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(INDEX_ID, LAYER, NODE_ID)),
            VectorKey::SimHash(VectorSimHashKey::new(INDEX_ID, NODE_ID)),
            VectorKey::UpperVector(VectorUpperVectorKey::new(INDEX_ID, NODE_ID)),
            VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(INDEX_ID, NODE_ID)),
            VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
                INDEX_ID,
                NODE_ID,
                LAYER,
                OTHER_NODE_ID,
            )),
        ];

        for key in keys {
            assert_eq!(VectorKey::parse_from_slice(&key.to_bytes()).unwrap(), key);
        }
    }

    #[test]
    fn typed_parser_exposes_entry_candidate_and_reverse_edge_fields() {
        let entry_key =
            VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(INDEX_ID, LAYER, NODE_ID))
                .to_bytes();
        let parsed = VectorKey::parse_from_slice(&entry_key).unwrap();
        let VectorKey::EntryCandidateSorted(parsed) = parsed else {
            panic!("entry candidate key should parse as EntryCandidateSorted");
        };
        assert_eq!(parsed.index_id(), INDEX_ID);
        assert_eq!(parsed.layer(), LAYER);
        assert_eq!(parsed.node_id(), NODE_ID);
        assert!(matches!(
            VectorKey::parse_from_slice(&entry_key[0..VECTOR_LAYER_NODE_KEY_LEN - 1]),
            Err(EncodingError::InvalidKey(_))
        ));

        let reverse_key = VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
            INDEX_ID,
            NODE_ID,
            LAYER,
            OTHER_NODE_ID,
        ))
        .to_bytes();
        let parsed = VectorKey::parse_from_slice(&reverse_key).unwrap();
        let VectorKey::ReverseEdge(parsed) = parsed else {
            panic!("reverse edge key should parse as ReverseEdge");
        };
        assert_eq!(parsed.index_id(), INDEX_ID);
        assert_eq!(parsed.target_node_id(), NODE_ID);
        assert_eq!(parsed.layer(), LAYER);
        assert_eq!(parsed.source_node_id(), OTHER_NODE_ID);
        assert!(matches!(
            VectorKey::parse_from_slice(&reverse_key[0..VECTOR_REVERSE_EDGE_KEY_LEN - 1]),
            Err(EncodingError::InvalidKey(_))
        ));
    }

    #[test]
    fn concrete_parsers_reject_wrong_kind_and_length() {
        let mut wrong_kind =
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID))
                .to_bytes()
                .to_vec();
        wrong_kind[VECTOR_KIND_OFFSET] = KEY_KIND_VECTOR;
        assert!(matches!(
            VectorEntryCandidateNodeKey::parse_from_slice(&wrong_kind),
            Err(EncodingError::InvalidKey(_))
        ));

        let vector_key =
            VectorKey::Vector(VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID)).to_bytes();
        assert!(matches!(
            VectorItemKey::parse_from_slice(&vector_key[0..VECTOR_ORDERED_KEY_LEN - 1]),
            Err(EncodingError::BufferTooShort { .. })
        ));

        let mut trailing = vector_key.to_vec();
        trailing.push(0);
        assert!(matches!(
            VectorItemKey::parse_from_slice(&trailing),
            Err(EncodingError::InvalidKey(_))
        ));
    }

    #[test]
    fn concrete_parsers_reject_all_invalid_shape_boundaries() {
        macro_rules! assert_parser_contract {
            ($parser:path, $bytes:expr, $kind_offset:expr) => {{
                let bytes = $bytes;
                assert!(matches!(
                    $parser(&bytes[0..bytes.len() - 1]),
                    Err(EncodingError::BufferTooShort { .. })
                ));

                let mut trailing = bytes.to_vec();
                trailing.push(0);
                assert!(matches!(
                    $parser(&trailing),
                    Err(EncodingError::InvalidKey(_))
                ));

                let mut wrong_prefix = bytes.to_vec();
                wrong_prefix[0] = 0xAA;
                assert!(matches!(
                    $parser(&wrong_prefix),
                    Err(EncodingError::InvalidKeyPrefix(0xAA))
                ));

                let mut wrong_kind = bytes.to_vec();
                wrong_kind[$kind_offset] = 0xFE;
                assert!(matches!(
                    $parser(&wrong_kind),
                    Err(EncodingError::InvalidKey(_))
                ));
            }};
        }

        macro_rules! assert_prefix_parser_contract {
            ($parser:path, $bytes:expr) => {{
                let bytes = $bytes;
                assert!(matches!(
                    $parser(&bytes[0..bytes.len() - 1]),
                    Err(EncodingError::BufferTooShort { .. })
                ));

                let mut trailing = bytes.to_vec();
                trailing.push(0);
                assert!(matches!(
                    $parser(&trailing),
                    Err(EncodingError::InvalidKey(_))
                ));

                let mut wrong_prefix = bytes.to_vec();
                wrong_prefix[0] = 0xAA;
                assert!(matches!(
                    $parser(&wrong_prefix),
                    Err(EncodingError::InvalidKeyPrefix(0xAA))
                ));
            }};
        }

        assert_parser_contract!(
            VectorIndexMetadataKey::parse_from_slice,
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID)).to_bytes(),
            DEFAULT_KIND_OFFSET
        );
        let mut wrong_metadata_type =
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID))
                .to_bytes()
                .to_vec();
        wrong_metadata_type[DEFAULT_INDEX_TYPE_OFFSET] = 0xFE;
        assert!(matches!(
            VectorIndexMetadataKey::parse_from_slice(&wrong_metadata_type),
            Err(EncodingError::InvalidKey(_))
        ));

        assert_prefix_parser_contract!(
            VectorIndexPrefixKey::parse_from_slice,
            VectorKey::IndexPrefix(VectorIndexPrefixKey::new(INDEX_ID)).to_bytes()
        );
        let mut wrong_index_type = VectorKey::IndexPrefix(VectorIndexPrefixKey::new(INDEX_ID))
            .to_bytes()
            .to_vec();
        wrong_index_type[DEFAULT_INDEX_TYPE_OFFSET] = 0xFE;
        assert!(matches!(
            VectorIndexPrefixKey::parse_from_slice(&wrong_index_type),
            Err(EncodingError::InvalidKey(_))
        ));

        assert_parser_contract!(
            VectorTxnGuardKey::parse_from_slice,
            VectorKey::TxnGuard(VectorTxnGuardKey::new(INDEX_ID)).to_bytes(),
            DEFAULT_KIND_OFFSET
        );
        let mut wrong_guard_type = VectorKey::TxnGuard(VectorTxnGuardKey::new(INDEX_ID))
            .to_bytes()
            .to_vec();
        wrong_guard_type[DEFAULT_INDEX_TYPE_OFFSET] = 0xFE;
        assert!(matches!(
            VectorTxnGuardKey::parse_from_slice(&wrong_guard_type),
            Err(EncodingError::InvalidKey(_))
        ));

        assert_parser_contract!(
            VectorLayer0NeighborsKey::parse_from_slice,
            VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(INDEX_ID, NODE_ID)).to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_parser_contract!(
            VectorItemKey::parse_from_slice,
            VectorKey::Vector(VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID)).to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_parser_contract!(
            VectorEntryCandidatePrefixKey::parse_from_slice,
            VectorKey::EntryCandidatePrefix(VectorEntryCandidatePrefixKey::new(INDEX_ID))
                .to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_parser_contract!(
            VectorEntryCandidateKey::parse_from_slice,
            VectorKey::EntryCandidateSorted(
                VectorEntryCandidateKey::new(INDEX_ID, LAYER, NODE_ID,)
            )
            .to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_parser_contract!(
            VectorEntryCandidateNodeKey::parse_from_slice,
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID))
                .to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_prefix_parser_contract!(
            VectorMemoryPrefixKey::parse_from_slice,
            VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(INDEX_ID)).to_bytes()
        );
        assert_prefix_parser_contract!(
            VectorL0PrefixKey::parse_from_slice,
            VectorKey::L0Prefix(VectorL0PrefixKey::new(INDEX_ID)).to_bytes()
        );
        assert_parser_contract!(
            VectorUpperNeighborsKey::parse_from_slice,
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(INDEX_ID, LAYER, NODE_ID))
                .to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_parser_contract!(
            VectorSimHashKey::parse_from_slice,
            VectorKey::SimHash(VectorSimHashKey::new(INDEX_ID, NODE_ID)).to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_parser_contract!(
            VectorUpperVectorKey::parse_from_slice,
            VectorKey::UpperVector(VectorUpperVectorKey::new(INDEX_ID, NODE_ID)).to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_parser_contract!(
            VectorReverseEdgePrefixKey::parse_from_slice,
            VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(INDEX_ID, NODE_ID))
                .to_bytes(),
            VECTOR_KIND_OFFSET
        );
        assert_parser_contract!(
            VectorReverseEdgeKey::parse_from_slice,
            VectorKey::ReverseEdge(VectorReverseEdgeKey::new(
                INDEX_ID,
                NODE_ID,
                LAYER,
                OTHER_NODE_ID,
            ))
            .to_bytes(),
            VECTOR_KIND_OFFSET
        );
    }

    #[test]
    fn parse_from_slice_rejects_invalid_prefix_kind_and_lengths() {
        assert!(matches!(
            VectorKey::parse_from_slice(&[]),
            Err(EncodingError::BufferTooShort {
                expected: KEYSPACE_PREFIX_LEN,
                actual: 0
            })
        ));
        assert!(matches!(
            VectorKey::parse_from_slice(&[0xAA]),
            Err(EncodingError::InvalidKeyPrefix(0xAA))
        ));
        let Err(EncodingError::BufferTooShort { expected, actual }) =
            VectorKey::parse_from_slice(&[KEY_SPACE_INDEX])
        else {
            panic!("short default key should return BufferTooShort");
        };
        assert_eq!(expected, DEFAULT_INDEX_TYPE_OFFSET + INDEX_TYPE_LEN);
        assert_eq!(actual, 1);
        assert!(matches!(
            VectorKey::parse_from_slice(&[KEY_SPACE_INDEX, 0x04]),
            Err(EncodingError::InvalidKey(_))
        ));
        let mut short_default = vec![KEY_SPACE_INDEX, INDEX_TYPE_VECTOR];
        short_default.extend_from_slice(&INDEX_ID.to_be_bytes()[0..INDEX_ID_LEN - 1]);
        let Err(EncodingError::BufferTooShort { expected, actual }) =
            VectorKey::parse_from_slice(&short_default)
        else {
            panic!("short default prefix should return BufferTooShort");
        };
        assert_eq!(expected, DEFAULT_PREFIX_LEN);
        assert_eq!(actual, DEFAULT_PREFIX_LEN - 1);

        let mut trailing_default = VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID))
            .to_bytes()
            .to_vec();
        trailing_default.push(0);
        assert!(matches!(
            VectorKey::parse_from_slice(&trailing_default),
            Err(EncodingError::InvalidKey(_))
        ));

        let hot_prefix = VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(INDEX_ID)).to_bytes();
        let short_hot = &hot_prefix[0..VECTOR_INDEX_PREFIX_LEN - 1];
        let Err(EncodingError::BufferTooShort { expected, actual }) =
            VectorKey::parse_from_slice(short_hot)
        else {
            panic!("short vector-hot prefix should return BufferTooShort");
        };
        assert_eq!(expected, VECTOR_INDEX_PREFIX_LEN);
        assert_eq!(actual, VECTOR_INDEX_PREFIX_LEN - 1);

        let l0_prefix = VectorKey::L0Prefix(VectorL0PrefixKey::new(INDEX_ID)).to_bytes();
        let short_l0 = &l0_prefix[0..VECTOR_INDEX_PREFIX_LEN - 1];
        let Err(EncodingError::BufferTooShort { expected, actual }) =
            VectorKey::parse_from_slice(short_l0)
        else {
            panic!("short vector-l0 prefix should return BufferTooShort");
        };
        assert_eq!(expected, VECTOR_INDEX_PREFIX_LEN);
        assert_eq!(actual, VECTOR_INDEX_PREFIX_LEN - 1);

        let mut invalid_default_kind =
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(INDEX_ID))
                .to_bytes()
                .to_vec();
        invalid_default_kind[DEFAULT_KIND_OFFSET] = 0xFE;
        assert!(matches!(
            VectorKey::parse_from_slice(&invalid_default_kind),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut invalid_hot_kind =
            VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(INDEX_ID, NODE_ID))
                .to_bytes()
                .to_vec();
        invalid_hot_kind[VECTOR_KIND_OFFSET] = 0xFE;
        assert!(matches!(
            VectorKey::parse_from_slice(&invalid_hot_kind),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut invalid_l0_kind =
            VectorKey::Vector(VectorItemKey::new(INDEX_ID, ORDER_CODE, NODE_ID))
                .to_bytes()
                .to_vec();
        invalid_l0_kind[VECTOR_KIND_OFFSET] = 0xFE;
        assert!(matches!(
            VectorKey::parse_from_slice(&invalid_l0_kind),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut trailing_hot = VectorKey::SimHash(VectorSimHashKey::new(INDEX_ID, NODE_ID))
            .to_bytes()
            .to_vec();
        trailing_hot.push(0);
        assert!(matches!(
            VectorKey::parse_from_slice(&trailing_hot),
            Err(EncodingError::InvalidKey(_))
        ));

        let mut trailing_l0 =
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(INDEX_ID, NODE_ID))
                .to_bytes()
                .to_vec();
        trailing_l0.push(0);
        assert!(matches!(
            VectorKey::parse_from_slice(&trailing_l0),
            Err(EncodingError::InvalidKey(_))
        ));
    }
}
