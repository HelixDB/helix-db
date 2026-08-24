//! Vector keyspace lanes and scan prefixes.

use super::*;
use crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey;

/// can be absent while hot or layer-0 residue remains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorStorageLane {
    /// Default index keyspace containing metadata and the transaction guard.
    Core,
    /// Vector-hot keyspace containing upper rows, layer-0 neighbors, and SimHash.
    Hot,
    /// Vector layer-0 keyspace containing items, candidates, and reverse locators.
    Layer0,
}

/// `[0x03][0x03]` prefix shared by all current core vector rows.
///
/// Legacy ascending edge-range rows share this prefix. Callers must classify
/// every result with [`Self::parse_row`] instead of parsing it directly as a
/// [`VectorKey`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct VectorMetadataScanPrefix;

/// Persisted vector rows that can legally share the metadata scan prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorMetadataScanRow {
    /// One physical vector index's metadata row.
    IndexMetadata(VectorIndexMetadataKey),
    /// One physical vector index's active transaction guard.
    TxnGuard(LegacyVectorTxnGuardKey),
}

impl VectorMetadataScanPrefix {
    /// Constructs the stateless current-format scan prefix.
    pub(crate) const fn new() -> Self {
        Self
    }

    /// Encodes the exact deployed vector-index keyspace prefix.
    pub(crate) fn to_bytes(self) -> Bytes {
        Bytes::copy_from_slice(&[KEY_SPACE_INDEX, INDEX_TYPE_VECTOR])
    }

    /// Classifies one row returned by this deliberately overlapping prefix.
    ///
    /// Persisted current core vector keys have the exact typed key length.
    /// Longer rows and exact prefix-shaped rows belong to overlapping legacy
    /// graph keyspaces and are not vector corruption. Exact-shape vector rows
    /// still receive strict codec validation.
    pub(crate) fn parse_row(
        self,
        slice: &[u8],
    ) -> Result<Option<VectorMetadataScanRow>, EncodingError> {
        if !matches!(slice.len(), DEFAULT_PREFIX_LEN | DEFAULT_KEY_LEN) {
            return Ok(None);
        }
        match VectorKey::parse_from_slice(slice)? {
            VectorKey::IndexPrefix(_) => Ok(None),
            VectorKey::IndexMetadata(key) => Ok(Some(VectorMetadataScanRow::IndexMetadata(key))),
            VectorKey::TxnGuard(key) => Ok(Some(VectorMetadataScanRow::TxnGuard(key))),
            VectorKey::Layer0Neighbors(_)
            | VectorKey::VectorPrefix(_)
            | VectorKey::Vector(_)
            | VectorKey::EntryCandidatePrefix(_)
            | VectorKey::EntryCandidateSorted(_)
            | VectorKey::EntryCandidateNode(_)
            | VectorKey::MemoryPrefix(_)
            | VectorKey::L0Prefix(_)
            | VectorKey::UpperNeighbors(_)
            | VectorKey::SimHash(_)
            | VectorKey::UpperVector(_)
            | VectorKey::SimHashDirectoryPrefix(_)
            | VectorKey::SimHashDirectory(_)
            | VectorKey::ReverseEdgePrefix(_)
            | VectorKey::ReverseEdge(_) => Err(EncodingError::InvalidKey(
                "metadata scan decoded a non-core vector row".to_string(),
            )),
        }
    }
}

impl VectorStorageLane {
    /// Every current physical vector storage lane, in stable probe order.
    pub(crate) const ALL: [Self; 3] = [Self::Core, Self::Hot, Self::Layer0];

    /// Encodes the complete deployed prefix shared by every row in this lane.
    ///
    /// The core prefix deliberately overlaps legacy secondary rows, so callers
    /// scanning it must classify rows with [`VectorMetadataScanPrefix::parse_row`].
    pub(crate) fn scan_prefix(self) -> Bytes {
        match self {
            Self::Core => VectorMetadataScanPrefix::new().to_bytes(),
            Self::Hot => Bytes::copy_from_slice(&[VECTOR_HOT_KEYSPACE_PREFIX]),
            Self::Layer0 => Bytes::copy_from_slice(&[VECTOR_L0_KEYSPACE_PREFIX]),
        }
    }

    /// Constructs the typed prefix for this lane and physical index ID.
    pub(crate) const fn prefix_key(self, index_id: u64) -> VectorKey {
        match self {
            Self::Core => VectorKey::IndexPrefix(VectorIndexPrefixKey::new(index_id)),
            Self::Hot => VectorKey::MemoryPrefix(VectorMemoryPrefixKey::new(index_id)),
            Self::Layer0 => VectorKey::L0Prefix(VectorL0PrefixKey::new(index_id)),
        }
    }
}

/// `[0x03][0x03][index_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorIndexPrefixKey {
    index_id: u64,
}

impl VectorIndexPrefixKey {
    pub(crate) const fn new(index_id: u64) -> Self {
        Self { index_id }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&DEFAULT_PREFIX_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: DEFAULT_PREFIX_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {DEFAULT_PREFIX_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != KEY_SPACE_INDEX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }
        if slice[DEFAULT_INDEX_TYPE_OFFSET] != INDEX_TYPE_VECTOR {
            return Err(EncodingError::InvalidKey(format!(
                "expected vector index type ({INDEX_TYPE_VECTOR:#04x}), got {:#04x}",
                slice[DEFAULT_INDEX_TYPE_OFFSET]
            )));
        }

        Ok(Self::new(u64::from_be_bytes(
            slice[DEFAULT_INDEX_ID_OFFSET..DEFAULT_INDEX_ID_OFFSET + INDEX_ID_LEN]
                .try_into()
                .expect("index id slice is 8 bytes"),
        )))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        DEFAULT_PREFIX_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KEY_SPACE_INDEX);
        buf.put_u8(INDEX_TYPE_VECTOR);
        buf.put_u64(self.index_id);
    }
}

/// `[0xF0][index_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorMemoryPrefixKey {
    index_id: u64,
}

impl VectorMemoryPrefixKey {
    pub(crate) const fn new(index_id: u64) -> Self {
        Self { index_id }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&VECTOR_INDEX_PREFIX_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: VECTOR_INDEX_PREFIX_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {VECTOR_INDEX_PREFIX_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != VECTOR_HOT_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }

        Ok(Self::new(u64::from_be_bytes(
            slice[VECTOR_INDEX_ID_OFFSET..VECTOR_INDEX_ID_OFFSET + INDEX_ID_LEN]
                .try_into()
                .expect("index id slice is 8 bytes"),
        )))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_INDEX_PREFIX_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_HOT_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
    }
}

/// `[0xF1][index_id:8]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorL0PrefixKey {
    index_id: u64,
}

impl VectorL0PrefixKey {
    pub(crate) const fn new(index_id: u64) -> Self {
        Self { index_id }
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice.len().cmp(&VECTOR_INDEX_PREFIX_LEN) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected: VECTOR_INDEX_PREFIX_LEN,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {VECTOR_INDEX_PREFIX_LEN} bytes, got {}",
                    slice.len()
                )));
            }
        }

        if slice[0] != VECTOR_L0_KEYSPACE_PREFIX {
            return Err(EncodingError::InvalidKeyPrefix(slice[0]));
        }

        Ok(Self::new(u64::from_be_bytes(
            slice[VECTOR_INDEX_ID_OFFSET..VECTOR_INDEX_ID_OFFSET + INDEX_ID_LEN]
                .try_into()
                .expect("index id slice is 8 bytes"),
        )))
    }

    pub(crate) const fn index_id(&self) -> u64 {
        self.index_id
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        VECTOR_INDEX_PREFIX_LEN
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(VECTOR_L0_KEYSPACE_PREFIX);
        buf.put_u64(self.index_id);
    }
}
