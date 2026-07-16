//! Typed construction and parsing for the canonical V2 index namespace.
//!
//! Scoped records always begin with logical data prefix `0x06`; tenant bytes
//! are applied only by [`super::Key`]. Database-global records use the exact
//! seventeen-byte `0xFE` sentinel, so no parser guesses scope from arbitrary
//! bytes.

use std::borrow::Cow;
use std::num::NonZeroU64;

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;
use crate::encoding::indexes::range::{RangeIndexDirection, RangeIndexKey};
use crate::encoding::indexes::{PROPERTY_HASH_MAX_LEN, VALUE_HASH_MAX_LEN};
use crate::index_v2::{
    BlobGcRunId, IndexComponent, IndexElementKind, IndexEntityId, IndexGenerationId, IndexId,
    IndexIdentity, IndexIdentityFamily, IndexOperationId, TextUploadIntentId,
};

use super::tenant::DataScope;
use super::KeyPrefix;

const PREFIX_LEN: usize = core::mem::size_of::<u8>();
const KIND_LEN: usize = core::mem::size_of::<u8>();
const INDEX_PREFIX_LEN: usize = core::mem::size_of::<u8>();
const U32_LEN: usize = core::mem::size_of::<u32>();
const U64_LEN: usize = core::mem::size_of::<u64>();
const UUID_LEN: usize = 16;
const HASH_LEN: usize = 32;
const TENANT_ID_LEN: usize = core::mem::size_of::<u128>();
const GLOBAL_SENTINEL_LEN: usize = TENANT_ID_LEN + PREFIX_LEN;

/// Exact V2-only database-global envelope.
pub(crate) const GLOBAL_INDEX_V2_SENTINEL: [u8; GLOBAL_SENTINEL_LEN] = [0xFE; GLOBAL_SENTINEL_LEN];

/// Maximum complete cursor, logical-owner, or global reference key length.
pub(crate) const INDEX_V2_KEY_MAX_LEN: usize = 1024 * 1024;

/// Frozen scoped/value record kinds.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum IndexV2RecordKind {
    IndexRecord = 0x01,
    Operation = 0x02,
    BuildDelta = 0x03,
    AppliedState = 0x04,
    SecondaryEntry = 0x05,
    TextManifestRoot = 0x06,
    TextManifestPage = 0x07,
    TextUploadIntent = 0x08,
    TextBuildArtifact = 0x09,
    BlobGcCandidate = 0x0A,
    BlobGcState = 0x0B,
    TextEntityState = 0x0C,
    ActiveMutationCommitProof = 0x0D,
    BlobReachabilityReference = 0x0E,
    VectorPartitionMapping = 0x0F,
}

impl IndexV2RecordKind {
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }

    pub(crate) fn try_from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(Self::IndexRecord),
            0x02 => Ok(Self::Operation),
            0x03 => Ok(Self::BuildDelta),
            0x04 => Ok(Self::AppliedState),
            0x05 => Ok(Self::SecondaryEntry),
            0x06 => Ok(Self::TextManifestRoot),
            0x07 => Ok(Self::TextManifestPage),
            0x08 => Ok(Self::TextUploadIntent),
            0x09 => Ok(Self::TextBuildArtifact),
            0x0A => Ok(Self::BlobGcCandidate),
            0x0B => Ok(Self::BlobGcState),
            0x0C => Ok(Self::TextEntityState),
            0x0D => Ok(Self::ActiveMutationCommitProof),
            0x0E => Ok(Self::BlobReachabilityReference),
            0x0F => Ok(Self::VectorPartitionMapping),
            unknown => Err(EncodingError::InvalidKey(format!(
                "unknown V2 index record kind {unknown:#04x}"
            ))),
        }
    }
}

/// Entity identity used by build-delta/applied/text-live keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IndexEntity {
    pub(crate) kind: IndexElementKind,
    pub(crate) id: IndexEntityId,
}

/// Full SHA-256 partition identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PartitionFingerprint([u8; HASH_LEN]);

impl PartitionFingerprint {
    pub(crate) const fn new(bytes: [u8; HASH_LEN]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn as_bytes(&self) -> &[u8; HASH_LEN] {
        &self.0
    }
}

/// Full content-addressed blob identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct BlobHash([u8; HASH_LEN]);

impl BlobHash {
    pub(crate) const fn new(bytes: [u8; HASH_LEN]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn as_bytes(&self) -> &[u8; HASH_LEN] {
        &self.0
    }
}

/// Frozen generation-qualified secondary lanes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SecondaryEntryLane {
    NodeEquality = 0x01,
    NodeUniqueEquality = 0x02,
    NodeRangeAscending = 0x03,
    NodeRangeDescending = 0x04,
    EdgeEquality = 0x05,
    EdgeRangeAscending = 0x06,
    EdgeRangeDescending = 0x07,
}

impl SecondaryEntryLane {
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }

    pub(crate) fn try_from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(Self::NodeEquality),
            0x02 => Ok(Self::NodeUniqueEquality),
            0x03 => Ok(Self::NodeRangeAscending),
            0x04 => Ok(Self::NodeRangeDescending),
            0x05 => Ok(Self::EdgeEquality),
            0x06 => Ok(Self::EdgeRangeAscending),
            0x07 => Ok(Self::EdgeRangeDescending),
            unknown => Err(EncodingError::InvalidKey(format!(
                "unknown V2 secondary lane {unknown:#04x}"
            ))),
        }
    }

    pub(crate) const fn is_unique(self) -> bool {
        matches!(self, Self::NodeUniqueEquality)
    }

    pub(crate) const fn is_equality(self) -> bool {
        matches!(
            self,
            Self::NodeEquality | Self::NodeUniqueEquality | Self::EdgeEquality
        )
    }

    /// Returns the canonical ordered-value codec for a range lane.
    pub(crate) const fn range_direction(self) -> Option<RangeIndexDirection> {
        match self {
            Self::NodeRangeAscending | Self::EdgeRangeAscending => Some(RangeIndexDirection::Asc),
            Self::NodeRangeDescending | Self::EdgeRangeDescending => {
                Some(RangeIndexDirection::Desc)
            }
            Self::NodeEquality | Self::NodeUniqueEquality | Self::EdgeEquality => None,
        }
    }
}

/// Canonical secondary value bytes whose shape is fixed by the lane.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum CanonicalSecondaryValue {
    /// Existing fixed `ValueHash` equality representation.
    Equality([u8; VALUE_HASH_MAX_LEN]),
    /// Existing self-delimiting ordered range representation.
    Range(Bytes),
}

impl CanonicalSecondaryValue {
    /// Wraps the existing fixed equality-value hash.
    pub(crate) const fn equality(value_hash: [u8; VALUE_HASH_MAX_LEN]) -> Self {
        Self::Equality(value_hash)
    }

    /// Encodes the exact existing ascending/descending range-value bytes.
    pub(crate) fn range(direction: RangeIndexDirection, value: &str) -> Self {
        let legacy = RangeIndexKey::new(
            direction,
            [0; PROPERTY_HASH_MAX_LEN],
            Cow::Borrowed(value),
            0,
        );
        let mut encoded = Vec::new();
        legacy.encode_into(&mut encoded);
        const VALUE_OFFSET: usize = PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN;
        let value_len = encoded.len() - VALUE_OFFSET - U64_LEN;
        Self::Range(Bytes::copy_from_slice(
            &encoded[VALUE_OFFSET..VALUE_OFFSET + value_len],
        ))
    }

    /// Validates and retains range bytes decoded from a persisted V2 key.
    fn try_encoded_range(
        direction: RangeIndexDirection,
        value: Bytes,
    ) -> Result<Self, EncodingError> {
        crate::encoding::indexes::range::decode_range_value(direction, &value)?;
        Ok(Self::Range(value))
    }

    /// Decodes a canonical range value for exact serving-bound filtering.
    pub(crate) fn decode_range(
        &self,
        direction: RangeIndexDirection,
    ) -> Result<Cow<'_, str>, EncodingError> {
        let Self::Range(value) = self else {
            return Err(EncodingError::InvalidKey(
                "equality secondary value cannot decode as a range".to_string(),
            ));
        };
        crate::encoding::indexes::range::decode_range_value(direction, value)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Equality(value) => value,
            Self::Range(value) => value.as_ref(),
        }
    }
}

/// Candidate record ownership encoded into scoped candidate keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlobGcCandidateKeyOwner {
    GenerationCleanup,
    UploadIntent(TextUploadIntentId),
}

impl BlobGcCandidateKeyOwner {
    const fn as_u8(self) -> u8 {
        match self {
            Self::GenerationCleanup => 0x01,
            Self::UploadIntent(_) => 0x02,
        }
    }
}

/// Canonical index-record key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IndexRecordV2Key {
    pub(crate) identity: IndexIdentity,
}

/// Scoped operation record key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IndexOperationKey {
    pub(crate) operation_id: IndexOperationId,
}

/// Coalesced build delta or builder-applied state key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IndexEntityStateKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) entity: IndexEntity,
}

/// Generation-qualified secondary entry key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SecondaryEntryKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) lane: SecondaryEntryLane,
    pub(crate) value: CanonicalSecondaryValue,
    pub(crate) entity_id: Option<IndexEntityId>,
}

impl SecondaryEntryKey {
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        lane: SecondaryEntryLane,
        value: CanonicalSecondaryValue,
        entity_id: Option<IndexEntityId>,
    ) -> Result<Self, EncodingError> {
        let value_matches_lane = matches!(
            (lane.is_equality(), &value),
            (true, CanonicalSecondaryValue::Equality(_))
                | (false, CanonicalSecondaryValue::Range(_))
        );
        if !value_matches_lane || lane.is_unique() != entity_id.is_none() {
            return Err(EncodingError::InvalidKey(
                "secondary lane/value/entity shape mismatch".to_string(),
            ));
        }
        if let Some(direction) = lane.range_direction() {
            value.decode_range(direction)?;
        }
        let encoded_len = PREFIX_LEN
            + KIND_LEN
            + U64_LEN
            + U64_LEN
            + KIND_LEN
            + value.as_bytes().len()
            + entity_id.map_or(0, |_| U64_LEN);
        if encoded_len > INDEX_V2_KEY_MAX_LEN {
            return Err(EncodingError::InvalidKey(
                "secondary V2 key exceeds 1 MiB".to_string(),
            ));
        }
        Ok(Self {
            index_id,
            generation,
            lane,
            value,
            entity_id,
        })
    }
}

/// Text manifest root key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TextManifestRootKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: PartitionFingerprint,
}

/// Text manifest page key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TextManifestPageKey {
    pub(crate) root: TextManifestRootKey,
    pub(crate) page: u32,
}

/// Text upload intent or active-mutation proof key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TextIntentOwnedKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) intent_id: TextUploadIntentId,
}

/// Text build artifact key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TextBuildArtifactKey {
    pub(crate) root: TextManifestRootKey,
    pub(crate) ordinal: u32,
}

/// Scoped blob-GC candidate key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlobGcCandidateKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) owner: BlobGcCandidateKeyOwner,
    pub(crate) blob_hash: BlobHash,
}

/// Generation-qualified text live-state key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TextEntityStateKey {
    pub(crate) root: TextManifestRootKey,
    pub(crate) entity: IndexEntity,
}

/// Generation-qualified vector tenant-partition mapping key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct VectorPartitionMappingKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: PartitionFingerprint,
}

/// Every legal scoped V2 key shape.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum IndexV2Key {
    IndexRecord(IndexRecordV2Key),
    Operation(IndexOperationKey),
    BuildDelta(IndexEntityStateKey),
    AppliedState(IndexEntityStateKey),
    SecondaryEntry(SecondaryEntryKey),
    TextManifestRoot(TextManifestRootKey),
    TextManifestPage(TextManifestPageKey),
    TextUploadIntent(TextIntentOwnedKey),
    TextBuildArtifact(TextBuildArtifactKey),
    BlobGcCandidate(BlobGcCandidateKey),
    TextEntityState(TextEntityStateKey),
    ActiveMutationCommitProof(TextIntentOwnedKey),
    VectorPartitionMapping(VectorPartitionMappingKey),
}

impl IndexV2Key {
    pub(crate) const fn record_kind(&self) -> IndexV2RecordKind {
        match self {
            Self::IndexRecord(_) => IndexV2RecordKind::IndexRecord,
            Self::Operation(_) => IndexV2RecordKind::Operation,
            Self::BuildDelta(_) => IndexV2RecordKind::BuildDelta,
            Self::AppliedState(_) => IndexV2RecordKind::AppliedState,
            Self::SecondaryEntry(_) => IndexV2RecordKind::SecondaryEntry,
            Self::TextManifestRoot(_) => IndexV2RecordKind::TextManifestRoot,
            Self::TextManifestPage(_) => IndexV2RecordKind::TextManifestPage,
            Self::TextUploadIntent(_) => IndexV2RecordKind::TextUploadIntent,
            Self::TextBuildArtifact(_) => IndexV2RecordKind::TextBuildArtifact,
            Self::BlobGcCandidate(_) => IndexV2RecordKind::BlobGcCandidate,
            Self::TextEntityState(_) => IndexV2RecordKind::TextEntityState,
            Self::ActiveMutationCommitProof(_) => IndexV2RecordKind::ActiveMutationCommitProof,
            Self::VectorPartitionMapping(_) => IndexV2RecordKind::VectorPartitionMapping,
        }
    }

    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::IndexV2
    }

    pub(crate) fn index_record(identity: IndexIdentity) -> Self {
        Self::IndexRecord(IndexRecordV2Key { identity })
    }

    pub(crate) fn operation(operation_id: IndexOperationId) -> Self {
        Self::Operation(IndexOperationKey { operation_id })
    }

    pub(crate) fn logical_prefix(kind: IndexV2RecordKind) -> Bytes {
        Bytes::from(vec![Self::key_prefix().as_u8(), kind.as_u8()])
    }

    /// Returns one exact index-generation prefix for a physical work kind.
    pub(crate) fn generation_prefix(
        kind: IndexV2RecordKind,
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Bytes {
        let mut bytes = Vec::with_capacity(PREFIX_LEN + KIND_LEN + U64_LEN + U64_LEN);
        bytes.put_u8(Self::key_prefix().as_u8());
        bytes.put_u8(kind.as_u8());
        bytes.put_u64(index_id.get());
        bytes.put_u64(generation.get());
        Bytes::from(bytes)
    }

    /// Returns one exact lane prefix inside a secondary generation.
    pub(crate) fn secondary_lane_prefix(
        index_id: IndexId,
        generation: IndexGenerationId,
        lane: SecondaryEntryLane,
    ) -> Bytes {
        let mut bytes =
            Self::generation_prefix(IndexV2RecordKind::SecondaryEntry, index_id, generation)
                .to_vec();
        bytes.put_u8(lane.as_u8());
        Bytes::from(bytes)
    }

    /// Returns the exact operation-owned candidate lane for one generation.
    pub(crate) fn generation_cleanup_candidate_prefix(
        index_id: IndexId,
        generation: IndexGenerationId,
    ) -> Bytes {
        let mut bytes =
            Self::generation_prefix(IndexV2RecordKind::BlobGcCandidate, index_id, generation)
                .to_vec();
        bytes.put_u8(BlobGcCandidateKeyOwner::GenerationCleanup.as_u8());
        Bytes::from(bytes)
    }

    pub(crate) fn encoded_len(&self) -> usize {
        let suffix = match self {
            Self::IndexRecord(key) => identity_encoded_len(&key.identity),
            Self::Operation(_) => UUID_LEN,
            Self::BuildDelta(_) | Self::AppliedState(_) => U64_LEN + U64_LEN + KIND_LEN + U64_LEN,
            Self::SecondaryEntry(key) => {
                U64_LEN
                    + U64_LEN
                    + KIND_LEN
                    + key.value.as_bytes().len()
                    + key.entity_id.map_or(0, |_| U64_LEN)
            }
            Self::TextManifestRoot(_) => U64_LEN + U64_LEN + HASH_LEN,
            Self::TextManifestPage(_) => U64_LEN + U64_LEN + HASH_LEN + U32_LEN,
            Self::TextUploadIntent(_) | Self::ActiveMutationCommitProof(_) => {
                U64_LEN + U64_LEN + UUID_LEN
            }
            Self::TextBuildArtifact(_) => U64_LEN + U64_LEN + HASH_LEN + U32_LEN,
            Self::BlobGcCandidate(key) => {
                U64_LEN
                    + U64_LEN
                    + KIND_LEN
                    + HASH_LEN
                    + match key.owner {
                        BlobGcCandidateKeyOwner::GenerationCleanup => 0,
                        BlobGcCandidateKeyOwner::UploadIntent(_) => UUID_LEN,
                    }
            }
            Self::TextEntityState(_) => U64_LEN + U64_LEN + HASH_LEN + KIND_LEN + U64_LEN,
            Self::VectorPartitionMapping(_) => U64_LEN + U64_LEN + HASH_LEN,
        };
        PREFIX_LEN + KIND_LEN + suffix
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buffer: &mut B) {
        buffer.put_u8(Self::key_prefix().as_u8());
        buffer.put_u8(self.record_kind().as_u8());
        match self {
            Self::IndexRecord(key) => encode_identity(&key.identity, buffer),
            Self::Operation(key) => buffer.put_slice(key.operation_id.as_bytes()),
            Self::BuildDelta(key) | Self::AppliedState(key) => encode_entity_state_key(key, buffer),
            Self::SecondaryEntry(key) => {
                buffer.put_u64(key.index_id.get());
                buffer.put_u64(key.generation.get());
                buffer.put_u8(key.lane.as_u8());
                buffer.put_slice(key.value.as_bytes());
                key.entity_id
                    .iter()
                    .for_each(|entity_id| buffer.put_u64(entity_id.get()));
            }
            Self::TextManifestRoot(key) => encode_text_root(key, buffer),
            Self::TextManifestPage(key) => {
                encode_text_root(&key.root, buffer);
                buffer.put_u32(key.page);
            }
            Self::TextUploadIntent(key) | Self::ActiveMutationCommitProof(key) => {
                encode_intent_owned_key(key, buffer)
            }
            Self::TextBuildArtifact(key) => {
                encode_text_root(&key.root, buffer);
                buffer.put_u32(key.ordinal);
            }
            Self::BlobGcCandidate(key) => {
                buffer.put_u64(key.index_id.get());
                buffer.put_u64(key.generation.get());
                buffer.put_u8(key.owner.as_u8());
                buffer.put_slice(key.blob_hash.as_bytes());
                if let BlobGcCandidateKeyOwner::UploadIntent(intent_id) = key.owner {
                    buffer.put_slice(intent_id.as_bytes());
                }
            }
            Self::TextEntityState(key) => {
                encode_text_root(&key.root, buffer);
                buffer.put_u8(key.entity.kind as u8);
                buffer.put_u64(key.entity.id.get());
            }
            Self::VectorPartitionMapping(key) => {
                buffer.put_u64(key.index_id.get());
                buffer.put_u64(key.generation.get());
                buffer.put_slice(key.partition.as_bytes());
            }
        }
    }

    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut bytes);
        Bytes::from(bytes)
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        const PREFIX_OFFSET: usize = 0;
        const KIND_OFFSET: usize = PREFIX_OFFSET + PREFIX_LEN;
        const SUFFIX_OFFSET: usize = KIND_OFFSET + KIND_LEN;
        if slice.len() < PREFIX_LEN + KIND_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN + KIND_LEN,
                actual: slice.len(),
            });
        }
        if slice[PREFIX_OFFSET] != KeyPrefix::IndexV2.as_u8() {
            return Err(EncodingError::InvalidKeyPrefix(slice[PREFIX_OFFSET]));
        }
        let kind = IndexV2RecordKind::try_from_u8(slice[KIND_OFFSET])?;
        let mut decoder =
            KeyDecoder::new(&slice[SUFFIX_OFFSET..SUFFIX_OFFSET + slice.len() - SUFFIX_OFFSET]);
        let key = match kind {
            IndexV2RecordKind::IndexRecord => Self::IndexRecord(IndexRecordV2Key {
                identity: decode_identity(&mut decoder)?,
            }),
            IndexV2RecordKind::Operation => Self::Operation(IndexOperationKey {
                operation_id: decode_operation_id(&mut decoder)?,
            }),
            IndexV2RecordKind::BuildDelta | IndexV2RecordKind::AppliedState => {
                let state = decode_entity_state_key(&mut decoder)?;
                match kind {
                    IndexV2RecordKind::BuildDelta => Self::BuildDelta(state),
                    IndexV2RecordKind::AppliedState => Self::AppliedState(state),
                    IndexV2RecordKind::IndexRecord
                    | IndexV2RecordKind::Operation
                    | IndexV2RecordKind::SecondaryEntry
                    | IndexV2RecordKind::TextManifestRoot
                    | IndexV2RecordKind::TextManifestPage
                    | IndexV2RecordKind::TextUploadIntent
                    | IndexV2RecordKind::TextBuildArtifact
                    | IndexV2RecordKind::BlobGcCandidate
                    | IndexV2RecordKind::BlobGcState
                    | IndexV2RecordKind::TextEntityState
                    | IndexV2RecordKind::ActiveMutationCommitProof
                    | IndexV2RecordKind::BlobReachabilityReference
                    | IndexV2RecordKind::VectorPartitionMapping => {
                        unreachable!("outer match admits only build/applied state kinds")
                    }
                }
            }
            IndexV2RecordKind::SecondaryEntry => {
                Self::SecondaryEntry(decode_secondary_entry(&mut decoder)?)
            }
            IndexV2RecordKind::TextManifestRoot => {
                Self::TextManifestRoot(decode_text_root(&mut decoder)?)
            }
            IndexV2RecordKind::TextManifestPage => {
                let root = decode_text_root(&mut decoder)?;
                let page = decoder.take_u32()?;
                Self::TextManifestPage(TextManifestPageKey { root, page })
            }
            IndexV2RecordKind::TextUploadIntent => {
                Self::TextUploadIntent(decode_intent_owned_key(&mut decoder)?)
            }
            IndexV2RecordKind::TextBuildArtifact => {
                let root = decode_text_root(&mut decoder)?;
                let ordinal = decoder.take_u32()?;
                Self::TextBuildArtifact(TextBuildArtifactKey { root, ordinal })
            }
            IndexV2RecordKind::BlobGcCandidate => {
                Self::BlobGcCandidate(decode_candidate_key(&mut decoder)?)
            }
            IndexV2RecordKind::TextEntityState => {
                let root = decode_text_root(&mut decoder)?;
                let entity = decode_entity(&mut decoder)?;
                Self::TextEntityState(TextEntityStateKey { root, entity })
            }
            IndexV2RecordKind::ActiveMutationCommitProof => {
                Self::ActiveMutationCommitProof(decode_intent_owned_key(&mut decoder)?)
            }
            IndexV2RecordKind::VectorPartitionMapping => {
                Self::VectorPartitionMapping(VectorPartitionMappingKey {
                    index_id: decode_index_id(&mut decoder)?,
                    generation: decode_generation(&mut decoder)?,
                    partition: PartitionFingerprint::new(decoder.take_array::<HASH_LEN>()?),
                })
            }
            IndexV2RecordKind::BlobGcState | IndexV2RecordKind::BlobReachabilityReference => {
                return Err(EncodingError::InvalidKey(format!(
                    "V2 value kind {:#04x} has no scoped key",
                    kind.as_u8()
                )));
            }
        };
        decoder.finish()?;
        Ok(key)
    }
}

fn identity_encoded_len(identity: &IndexIdentity) -> usize {
    KIND_LEN
        + KIND_LEN
        + U32_LEN
        + identity.label().as_str().len()
        + U32_LEN
        + identity.property().as_str().len()
}

fn encode_identity<B: BufMut>(identity: &IndexIdentity, buffer: &mut B) {
    buffer.put_u8(identity.family() as u8);
    buffer.put_u8(identity.element_kind() as u8);
    put_component(identity.label(), buffer);
    put_component(identity.property(), buffer);
}

fn put_component<B: BufMut>(component: &IndexComponent, buffer: &mut B) {
    let len = u32::try_from(component.as_str().len())
        .expect("validated V2 index components are bounded below u32");
    buffer.put_u32(len);
    buffer.put_slice(component.as_str().as_bytes());
}

fn decode_identity(decoder: &mut KeyDecoder<'_>) -> Result<IndexIdentity, EncodingError> {
    let family = match decoder.take_u8()? {
        0x01 => IndexIdentityFamily::SecondaryEquality,
        0x02 => IndexIdentityFamily::SecondaryRange,
        0x03 => IndexIdentityFamily::Vector,
        0x04 => IndexIdentityFamily::Text,
        unknown => {
            return Err(EncodingError::InvalidKey(format!(
                "unknown V2 identity family {unknown:#04x}"
            )));
        }
    };
    let element_kind = decode_element_kind(decoder.take_u8()?)?;
    let label = decoder.take_component("label")?;
    let property = decoder.take_component("property")?;
    Ok(IndexIdentity::new(family, element_kind, label, property))
}

fn encode_entity_state_key<B: BufMut>(key: &IndexEntityStateKey, buffer: &mut B) {
    buffer.put_u64(key.index_id.get());
    buffer.put_u64(key.generation.get());
    buffer.put_u8(key.entity.kind as u8);
    buffer.put_u64(key.entity.id.get());
}

fn decode_entity_state_key(
    decoder: &mut KeyDecoder<'_>,
) -> Result<IndexEntityStateKey, EncodingError> {
    Ok(IndexEntityStateKey {
        index_id: decode_index_id(decoder)?,
        generation: decode_generation(decoder)?,
        entity: decode_entity(decoder)?,
    })
}

fn decode_secondary_entry(
    decoder: &mut KeyDecoder<'_>,
) -> Result<SecondaryEntryKey, EncodingError> {
    let index_id = decode_index_id(decoder)?;
    let generation = decode_generation(decoder)?;
    let lane = SecondaryEntryLane::try_from_u8(decoder.take_u8()?)?;
    let (value, entity_id) = if lane.is_equality() {
        let value = decoder.take_array::<VALUE_HASH_MAX_LEN>()?;
        let entity_id = if lane.is_unique() {
            None
        } else {
            Some(IndexEntityId::new(decoder.take_u64()?))
        };
        (CanonicalSecondaryValue::Equality(value), entity_id)
    } else {
        if decoder.remaining_len() < U64_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: U64_LEN,
                actual: decoder.remaining_len(),
            });
        }
        let range_len = decoder.remaining_len() - U64_LEN;
        let range = Bytes::copy_from_slice(decoder.take_bytes(range_len)?);
        let Some(direction) = lane.range_direction() else {
            return Err(EncodingError::InvalidKey(
                "non-equality secondary lane has no range direction".to_string(),
            ));
        };
        (
            CanonicalSecondaryValue::try_encoded_range(direction, range)?,
            Some(IndexEntityId::new(decoder.take_u64()?)),
        )
    };
    SecondaryEntryKey::try_new(index_id, generation, lane, value, entity_id)
}

fn encode_text_root<B: BufMut>(key: &TextManifestRootKey, buffer: &mut B) {
    buffer.put_u64(key.index_id.get());
    buffer.put_u64(key.generation.get());
    buffer.put_slice(key.partition.as_bytes());
}

fn decode_text_root(decoder: &mut KeyDecoder<'_>) -> Result<TextManifestRootKey, EncodingError> {
    Ok(TextManifestRootKey {
        index_id: decode_index_id(decoder)?,
        generation: decode_generation(decoder)?,
        partition: PartitionFingerprint::new(decoder.take_array::<HASH_LEN>()?),
    })
}

fn encode_intent_owned_key<B: BufMut>(key: &TextIntentOwnedKey, buffer: &mut B) {
    buffer.put_u64(key.index_id.get());
    buffer.put_u64(key.generation.get());
    buffer.put_slice(key.intent_id.as_bytes());
}

fn decode_intent_owned_key(
    decoder: &mut KeyDecoder<'_>,
) -> Result<TextIntentOwnedKey, EncodingError> {
    Ok(TextIntentOwnedKey {
        index_id: decode_index_id(decoder)?,
        generation: decode_generation(decoder)?,
        intent_id: TextUploadIntentId::from_bytes(decoder.take_array::<UUID_LEN>()?)
            .map_err(model_key_error)?,
    })
}

fn decode_candidate_key(decoder: &mut KeyDecoder<'_>) -> Result<BlobGcCandidateKey, EncodingError> {
    let index_id = decode_index_id(decoder)?;
    let generation = decode_generation(decoder)?;
    let owner_tag = decoder.take_u8()?;
    let blob_hash = BlobHash::new(decoder.take_array::<HASH_LEN>()?);
    let owner = match owner_tag {
        0x01 => BlobGcCandidateKeyOwner::GenerationCleanup,
        0x02 => BlobGcCandidateKeyOwner::UploadIntent(
            TextUploadIntentId::from_bytes(decoder.take_array::<UUID_LEN>()?)
                .map_err(model_key_error)?,
        ),
        unknown => {
            return Err(EncodingError::InvalidKey(format!(
                "unknown GC candidate owner {unknown:#04x}"
            )));
        }
    };
    Ok(BlobGcCandidateKey {
        index_id,
        generation,
        owner,
        blob_hash,
    })
}

fn decode_entity(decoder: &mut KeyDecoder<'_>) -> Result<IndexEntity, EncodingError> {
    Ok(IndexEntity {
        kind: decode_element_kind(decoder.take_u8()?)?,
        id: IndexEntityId::new(decoder.take_u64()?),
    })
}

fn decode_element_kind(value: u8) -> Result<IndexElementKind, EncodingError> {
    match value {
        0x01 => Ok(IndexElementKind::Node),
        0x02 => Ok(IndexElementKind::Edge),
        unknown => Err(EncodingError::InvalidKey(format!(
            "unknown V2 element kind {unknown:#04x}"
        ))),
    }
}

fn decode_index_id(decoder: &mut KeyDecoder<'_>) -> Result<IndexId, EncodingError> {
    IndexId::new(decoder.take_u64()?).map_err(model_key_error)
}

fn decode_generation(decoder: &mut KeyDecoder<'_>) -> Result<IndexGenerationId, EncodingError> {
    IndexGenerationId::new(decoder.take_u64()?).map_err(model_key_error)
}

fn decode_operation_id(decoder: &mut KeyDecoder<'_>) -> Result<IndexOperationId, EncodingError> {
    IndexOperationId::from_bytes(decoder.take_array::<UUID_LEN>()?).map_err(model_key_error)
}

fn model_key_error(error: crate::index_v2::IndexV2ModelError) -> EncodingError {
    EncodingError::InvalidKey(error.to_string())
}

/// Frozen global key kinds after the exact sentinel.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum GlobalIndexV2Kind {
    StorageVersion = 0x01,
    LogicalIndexIdWatermark = 0x02,
    VectorPhysicalIdWatermark = 0x03,
    OperationPointer = 0x04,
    UploadPointer = 0x05,
    BlobGcRunRoot = 0x06,
    BlobGcReachabilityMark = 0x07,
    BlobReachabilityReference = 0x08,
    BlobGcCandidateMember = 0x09,
}

impl GlobalIndexV2Kind {
    const fn as_u8(self) -> u8 {
        self as u8
    }

    fn try_from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(Self::StorageVersion),
            0x02 => Ok(Self::LogicalIndexIdWatermark),
            0x03 => Ok(Self::VectorPhysicalIdWatermark),
            0x04 => Ok(Self::OperationPointer),
            0x05 => Ok(Self::UploadPointer),
            0x06 => Ok(Self::BlobGcRunRoot),
            0x07 => Ok(Self::BlobGcReachabilityMark),
            0x08 => Ok(Self::BlobReachabilityReference),
            0x09 => Ok(Self::BlobGcCandidateMember),
            unknown => Err(EncodingError::InvalidKey(format!(
                "unknown global V2 key kind {unknown:#04x}"
            ))),
        }
    }
}

/// First or second reachability pass encoded in global mark keys.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlobGcPass {
    First = 0x01,
    Second = 0x02,
}

impl BlobGcPass {
    fn try_from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(Self::First),
            0x02 => Ok(Self::Second),
            unknown => Err(EncodingError::InvalidKey(format!(
                "unknown blob GC pass {unknown:#04x}"
            ))),
        }
    }
}

/// Owner kind of a globally discoverable blob reference.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlobReferenceOwnerKind {
    UploadIntent = 0x01,
    ManifestPageSplit = 0x02,
    BuildArtifact = 0x03,
}

impl BlobReferenceOwnerKind {
    fn try_from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(Self::UploadIntent),
            0x02 => Ok(Self::ManifestPageSplit),
            0x03 => Ok(Self::BuildArtifact),
            unknown => Err(EncodingError::InvalidKey(format!(
                "unknown blob reference owner {unknown:#04x}"
            ))),
        }
    }
}

/// Canonical global blob-reference identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BlobReferenceGlobalKey {
    pub(crate) blob_hash: BlobHash,
    pub(crate) owner_kind: BlobReferenceOwnerKind,
    pub(crate) scope: DataScope,
    pub(crate) owner_logical_key: Bytes,
    pub(crate) owner_slot: u32,
}

impl BlobReferenceGlobalKey {
    pub(crate) fn try_new(
        blob_hash: BlobHash,
        owner_kind: BlobReferenceOwnerKind,
        scope: DataScope,
        owner_logical_key: Bytes,
        owner_slot: u32,
    ) -> Result<Self, EncodingError> {
        let parsed_owner = IndexV2Key::parse_from_slice(&owner_logical_key)?;
        let owner_matches_kind = matches!(
            (owner_kind, parsed_owner),
            (
                BlobReferenceOwnerKind::UploadIntent,
                IndexV2Key::TextUploadIntent(_)
            ) | (
                BlobReferenceOwnerKind::ManifestPageSplit,
                IndexV2Key::TextManifestPage(_)
            ) | (
                BlobReferenceOwnerKind::BuildArtifact,
                IndexV2Key::TextBuildArtifact(_)
            )
        );
        if !owner_matches_kind {
            return Err(EncodingError::InvalidKey(
                "blob-reference owner kind disagrees with its complete logical V2 key".to_string(),
            ));
        }
        let scope_len = KIND_LEN
            + if scope.is_unscoped() {
                0
            } else {
                TENANT_ID_LEN
            };
        let total_len = GLOBAL_SENTINEL_LEN
            + KIND_LEN
            + HASH_LEN
            + KIND_LEN
            + scope_len
            + U32_LEN
            + owner_logical_key.len()
            + U32_LEN;
        if total_len > INDEX_V2_KEY_MAX_LEN {
            return Err(EncodingError::InvalidKey(
                "blob-reference key exceeds 1 MiB".to_string(),
            ));
        }
        Ok(Self {
            blob_hash,
            owner_kind,
            scope,
            owner_logical_key,
            owner_slot,
        })
    }
}

/// Every legal database-global V2 key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum GlobalIndexV2Key {
    StorageVersion,
    LogicalIndexIdWatermark,
    VectorPhysicalIdWatermark,
    OperationPointer(IndexOperationId),
    UploadPointer(TextUploadIntentId),
    BlobGcRunRoot(BlobGcRunId),
    BlobGcReachabilityMark {
        run_id: BlobGcRunId,
        pass: BlobGcPass,
        scan_attempt: NonZeroU64,
        blob_hash: BlobHash,
    },
    BlobReachabilityReference(BlobReferenceGlobalKey),
    BlobGcCandidateMember {
        run_id: BlobGcRunId,
        blob_hash: BlobHash,
    },
}

impl GlobalIndexV2Key {
    /// Returns the complete global sentinel plus one frozen lane kind.
    pub(crate) fn logical_prefix(kind: GlobalIndexV2Kind) -> Bytes {
        let mut bytes = Vec::with_capacity(GLOBAL_SENTINEL_LEN + KIND_LEN);
        bytes.put_slice(&GLOBAL_INDEX_V2_SENTINEL);
        bytes.put_u8(kind.as_u8());
        Bytes::from(bytes)
    }

    /// Returns the exact immutable member lane owned by one blob-GC run.
    pub(crate) fn blob_gc_candidate_member_prefix(run_id: BlobGcRunId) -> Bytes {
        let mut bytes = Self::logical_prefix(GlobalIndexV2Kind::BlobGcCandidateMember).to_vec();
        bytes.put_slice(run_id.as_bytes());
        Bytes::from(bytes)
    }

    /// Returns every reachability mark, across passes and attempts, for one run.
    pub(crate) fn blob_gc_reachability_mark_run_prefix(run_id: BlobGcRunId) -> Bytes {
        let mut bytes = Self::logical_prefix(GlobalIndexV2Kind::BlobGcReachabilityMark).to_vec();
        bytes.put_slice(run_id.as_bytes());
        Bytes::from(bytes)
    }

    /// Returns one exact run/pass/attempt reachability-mark prefix.
    pub(crate) fn blob_gc_reachability_mark_prefix(
        run_id: BlobGcRunId,
        pass: BlobGcPass,
        scan_attempt: NonZeroU64,
    ) -> Bytes {
        let mut bytes = Self::logical_prefix(GlobalIndexV2Kind::BlobGcReachabilityMark).to_vec();
        bytes.put_slice(run_id.as_bytes());
        bytes.put_u8(pass as u8);
        bytes.put_u64(scan_attempt.get());
        Bytes::from(bytes)
    }

    /// Returns the complete reference-owner prefix for one content hash.
    pub(crate) fn blob_reachability_reference_prefix(blob_hash: BlobHash) -> Bytes {
        let mut bytes = Self::logical_prefix(GlobalIndexV2Kind::BlobReachabilityReference).to_vec();
        bytes.put_slice(blob_hash.as_bytes());
        Bytes::from(bytes)
    }

    pub(crate) const fn kind(&self) -> GlobalIndexV2Kind {
        match self {
            Self::StorageVersion => GlobalIndexV2Kind::StorageVersion,
            Self::LogicalIndexIdWatermark => GlobalIndexV2Kind::LogicalIndexIdWatermark,
            Self::VectorPhysicalIdWatermark => GlobalIndexV2Kind::VectorPhysicalIdWatermark,
            Self::OperationPointer(_) => GlobalIndexV2Kind::OperationPointer,
            Self::UploadPointer(_) => GlobalIndexV2Kind::UploadPointer,
            Self::BlobGcRunRoot(_) => GlobalIndexV2Kind::BlobGcRunRoot,
            Self::BlobGcReachabilityMark { .. } => GlobalIndexV2Kind::BlobGcReachabilityMark,
            Self::BlobReachabilityReference(_) => GlobalIndexV2Kind::BlobReachabilityReference,
            Self::BlobGcCandidateMember { .. } => GlobalIndexV2Kind::BlobGcCandidateMember,
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        let suffix = match self {
            Self::StorageVersion
            | Self::LogicalIndexIdWatermark
            | Self::VectorPhysicalIdWatermark => 0,
            Self::OperationPointer(_) | Self::UploadPointer(_) | Self::BlobGcRunRoot(_) => UUID_LEN,
            Self::BlobGcReachabilityMark { .. } => UUID_LEN + KIND_LEN + U64_LEN + HASH_LEN,
            Self::BlobReachabilityReference(key) => {
                HASH_LEN
                    + KIND_LEN
                    + KIND_LEN
                    + if key.scope.is_unscoped() {
                        0
                    } else {
                        TENANT_ID_LEN
                    }
                    + U32_LEN
                    + key.owner_logical_key.len()
                    + U32_LEN
            }
            Self::BlobGcCandidateMember { .. } => UUID_LEN + HASH_LEN,
        };
        GLOBAL_SENTINEL_LEN + KIND_LEN + suffix
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buffer: &mut B) {
        buffer.put_slice(&GLOBAL_INDEX_V2_SENTINEL);
        buffer.put_u8(self.kind().as_u8());
        match self {
            Self::StorageVersion
            | Self::LogicalIndexIdWatermark
            | Self::VectorPhysicalIdWatermark => {}
            Self::OperationPointer(id) => buffer.put_slice(id.as_bytes()),
            Self::UploadPointer(id) => buffer.put_slice(id.as_bytes()),
            Self::BlobGcRunRoot(id) => buffer.put_slice(id.as_bytes()),
            Self::BlobGcReachabilityMark {
                run_id,
                pass,
                scan_attempt,
                blob_hash,
            } => {
                buffer.put_slice(run_id.as_bytes());
                buffer.put_u8(*pass as u8);
                buffer.put_u64(scan_attempt.get());
                buffer.put_slice(blob_hash.as_bytes());
            }
            Self::BlobReachabilityReference(key) => {
                buffer.put_slice(key.blob_hash.as_bytes());
                buffer.put_u8(key.owner_kind as u8);
                encode_scope(key.scope, buffer);
                buffer.put_u32(
                    u32::try_from(key.owner_logical_key.len())
                        .expect("blob owner key is bounded below u32"),
                );
                buffer.put_slice(&key.owner_logical_key);
                buffer.put_u32(key.owner_slot);
            }
            Self::BlobGcCandidateMember { run_id, blob_hash } => {
                buffer.put_slice(run_id.as_bytes());
                buffer.put_slice(blob_hash.as_bytes());
            }
        }
    }

    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut bytes);
        Bytes::from(bytes)
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        const SENTINEL_OFFSET: usize = 0;
        const KIND_OFFSET: usize = SENTINEL_OFFSET + GLOBAL_SENTINEL_LEN;
        const SUFFIX_OFFSET: usize = KIND_OFFSET + KIND_LEN;
        if slice.len() < GLOBAL_SENTINEL_LEN + KIND_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: GLOBAL_SENTINEL_LEN + KIND_LEN,
                actual: slice.len(),
            });
        }
        if slice[SENTINEL_OFFSET..SENTINEL_OFFSET + GLOBAL_SENTINEL_LEN] != GLOBAL_INDEX_V2_SENTINEL
        {
            return Err(EncodingError::InvalidKey(
                "global V2 sentinel mismatch".to_string(),
            ));
        }
        let kind = GlobalIndexV2Kind::try_from_u8(slice[KIND_OFFSET])?;
        let mut decoder =
            KeyDecoder::new(&slice[SUFFIX_OFFSET..SUFFIX_OFFSET + slice.len() - SUFFIX_OFFSET]);
        let key = match kind {
            GlobalIndexV2Kind::StorageVersion => Self::StorageVersion,
            GlobalIndexV2Kind::LogicalIndexIdWatermark => Self::LogicalIndexIdWatermark,
            GlobalIndexV2Kind::VectorPhysicalIdWatermark => Self::VectorPhysicalIdWatermark,
            GlobalIndexV2Kind::OperationPointer => {
                Self::OperationPointer(decode_operation_id(&mut decoder)?)
            }
            GlobalIndexV2Kind::UploadPointer => Self::UploadPointer(
                TextUploadIntentId::from_bytes(decoder.take_array::<UUID_LEN>()?)
                    .map_err(model_key_error)?,
            ),
            GlobalIndexV2Kind::BlobGcRunRoot => Self::BlobGcRunRoot(
                BlobGcRunId::from_bytes(decoder.take_array::<UUID_LEN>()?)
                    .map_err(model_key_error)?,
            ),
            GlobalIndexV2Kind::BlobGcReachabilityMark => {
                let run_id = BlobGcRunId::from_bytes(decoder.take_array::<UUID_LEN>()?)
                    .map_err(model_key_error)?;
                let pass = BlobGcPass::try_from_u8(decoder.take_u8()?)?;
                let Some(scan_attempt) = NonZeroU64::new(decoder.take_u64()?) else {
                    return Err(EncodingError::InvalidKey(
                        "blob GC scan attempt must be non-zero".to_string(),
                    ));
                };
                let blob_hash = BlobHash::new(decoder.take_array::<HASH_LEN>()?);
                Self::BlobGcReachabilityMark {
                    run_id,
                    pass,
                    scan_attempt,
                    blob_hash,
                }
            }
            GlobalIndexV2Kind::BlobReachabilityReference => {
                let blob_hash = BlobHash::new(decoder.take_array::<HASH_LEN>()?);
                let owner_kind = BlobReferenceOwnerKind::try_from_u8(decoder.take_u8()?)?;
                let scope = decode_scope(&mut decoder)?;
                let owner_key_len = decoder.take_u32()? as usize;
                let owner_logical_key = Bytes::copy_from_slice(decoder.take_bytes(owner_key_len)?);
                let owner_slot = decoder.take_u32()?;
                Self::BlobReachabilityReference(BlobReferenceGlobalKey::try_new(
                    blob_hash,
                    owner_kind,
                    scope,
                    owner_logical_key,
                    owner_slot,
                )?)
            }
            GlobalIndexV2Kind::BlobGcCandidateMember => {
                let run_id = BlobGcRunId::from_bytes(decoder.take_array::<UUID_LEN>()?)
                    .map_err(model_key_error)?;
                let blob_hash = BlobHash::new(decoder.take_array::<HASH_LEN>()?);
                Self::BlobGcCandidateMember { run_id, blob_hash }
            }
        };
        decoder.finish()?;
        Ok(key)
    }
}

fn encode_scope<B: BufMut>(scope: DataScope, buffer: &mut B) {
    match scope {
        DataScope::LegacyUnscoped => buffer.put_u8(0x00),
        DataScope::Tenant(tenant_id) => {
            buffer.put_u8(0x01);
            buffer.put_u128(tenant_id.as_u128());
        }
    }
}

fn decode_scope(decoder: &mut KeyDecoder<'_>) -> Result<DataScope, EncodingError> {
    match decoder.take_u8()? {
        0x00 => Ok(DataScope::LegacyUnscoped),
        0x01 => Ok(DataScope::Tenant(super::tenant::TenantId::from_u128(
            u128::from_be_bytes(decoder.take_array::<TENANT_ID_LEN>()?),
        ))),
        unknown => Err(EncodingError::InvalidKey(format!(
            "unknown V2 scope kind {unknown:#04x}"
        ))),
    }
}

/// Small bounded decoder shared by the many fixed V2 key suffixes.
struct KeyDecoder<'a> {
    remaining: &'a [u8],
}

impl<'a> KeyDecoder<'a> {
    const fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    const fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    fn take_bytes(&mut self, len: usize) -> Result<&'a [u8], EncodingError> {
        const FIELD_OFFSET: usize = 0;
        if self.remaining.len() < len {
            return Err(EncodingError::BufferTooShort {
                expected: len,
                actual: self.remaining.len(),
            });
        }
        let value = &self.remaining[FIELD_OFFSET..FIELD_OFFSET + len];
        self.remaining = &self.remaining[FIELD_OFFSET + len..FIELD_OFFSET + self.remaining.len()];
        Ok(value)
    }

    fn take_array<const LEN: usize>(&mut self) -> Result<[u8; LEN], EncodingError> {
        Ok(self
            .take_bytes(LEN)?
            .try_into()
            .expect("fixed decoder slice matches requested array length"))
    }

    fn take_u8(&mut self) -> Result<u8, EncodingError> {
        const BYTE_OFFSET: usize = 0;
        Ok(self.take_bytes(KIND_LEN)?[BYTE_OFFSET])
    }

    fn take_u32(&mut self) -> Result<u32, EncodingError> {
        Ok(u32::from_be_bytes(self.take_array::<U32_LEN>()?))
    }

    fn take_u64(&mut self) -> Result<u64, EncodingError> {
        Ok(u64::from_be_bytes(self.take_array::<U64_LEN>()?))
    }

    fn take_component(&mut self, kind: &'static str) -> Result<IndexComponent, EncodingError> {
        let len = self.take_u32()? as usize;
        let bytes = self.take_bytes(len)?;
        let value = std::str::from_utf8(bytes)?;
        IndexComponent::try_new(kind, value).map_err(model_key_error)
    }

    fn finish(self) -> Result<(), EncodingError> {
        if !self.remaining.is_empty() {
            return Err(EncodingError::InvalidKey(format!(
                "V2 key has {} trailing bytes",
                self.remaining.len()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::v1::keys::tenant::TenantId;
    use crate::encoding::v1::keys::{DataKeyKind, GlobalKeyKind, Key};

    fn identity() -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "User").unwrap(),
            IndexComponent::try_new("property", "email").unwrap(),
        )
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn secondary_range_values_preserve_existing_ordered_bytes() {
        const VALUE_OFFSET: usize = PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN;
        for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
            for value in ["", "alpha", "a\0z", "omega"] {
                let legacy = RangeIndexKey::new(
                    direction,
                    [0xAB; PROPERTY_HASH_MAX_LEN],
                    Cow::Borrowed(value),
                    42,
                );
                let mut legacy_bytes = Vec::new();
                legacy.encode_into(&mut legacy_bytes);
                let value_len = legacy_bytes.len() - VALUE_OFFSET - U64_LEN;
                let canonical = CanonicalSecondaryValue::range(direction, value);
                assert_eq!(
                    canonical.as_bytes(),
                    &legacy_bytes[VALUE_OFFSET..VALUE_OFFSET + value_len]
                );
            }

            let alpha = CanonicalSecondaryValue::range(direction, "alpha");
            let omega = CanonicalSecondaryValue::range(direction, "omega");
            match direction {
                RangeIndexDirection::Asc => assert!(alpha.as_bytes() < omega.as_bytes()),
                RangeIndexDirection::Desc => assert!(alpha.as_bytes() > omega.as_bytes()),
            }
        }
    }

    #[test]
    fn generation_qualified_secondary_entries_accept_graph_entity_zero() {
        let key = IndexV2Key::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::initial(),
                IndexGenerationId::initial(),
                SecondaryEntryLane::NodeEquality,
                CanonicalSecondaryValue::equality([0x7A; VALUE_HASH_MAX_LEN]),
                Some(IndexEntityId::initial()),
            )
            .expect("graph entity zero is a valid non-unique owner"),
        );
        let parsed = IndexV2Key::parse_from_slice(&key.to_bytes())
            .expect("zero-valued graph entity round trips");
        assert_eq!(parsed, key);
    }

    #[test]
    fn generation_cleanup_candidate_prefix_is_exact_and_excludes_intent_candidates() {
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let generation_key = IndexV2Key::BlobGcCandidate(BlobGcCandidateKey {
            index_id,
            generation,
            owner: BlobGcCandidateKeyOwner::GenerationCleanup,
            blob_hash: BlobHash::new([1; HASH_LEN]),
        })
        .to_bytes();
        let intent_key = IndexV2Key::BlobGcCandidate(BlobGcCandidateKey {
            index_id,
            generation,
            owner: BlobGcCandidateKeyOwner::UploadIntent(
                TextUploadIntentId::from_bytes([2; UUID_LEN]).unwrap(),
            ),
            blob_hash: BlobHash::new([1; HASH_LEN]),
        })
        .to_bytes();
        let prefix = IndexV2Key::generation_cleanup_candidate_prefix(index_id, generation);

        assert!(generation_key.starts_with(&prefix));
        assert!(!intent_key.starts_with(&prefix));
        assert_eq!(generation_key.len(), prefix.len() + HASH_LEN);
    }

    #[test]
    fn blob_gc_member_prefix_is_exact_and_run_qualified() {
        let run_id = BlobGcRunId::from_bytes([3; UUID_LEN]).unwrap();
        let other_run_id = BlobGcRunId::from_bytes([4; UUID_LEN]).unwrap();
        let member = GlobalIndexV2Key::BlobGcCandidateMember {
            run_id,
            blob_hash: BlobHash::new([5; HASH_LEN]),
        }
        .to_bytes();
        let other_member = GlobalIndexV2Key::BlobGcCandidateMember {
            run_id: other_run_id,
            blob_hash: BlobHash::new([5; HASH_LEN]),
        }
        .to_bytes();
        let prefix = GlobalIndexV2Key::blob_gc_candidate_member_prefix(run_id);

        assert!(member.starts_with(&prefix));
        assert!(!other_member.starts_with(&prefix));
        assert_eq!(member.len(), prefix.len() + HASH_LEN);
    }

    #[test]
    fn every_scoped_and_global_key_shape_has_frozen_bytes() {
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(7),
        };
        let operation_id = IndexOperationId::from_bytes([0x11; UUID_LEN]).unwrap();
        let intent_id = TextUploadIntentId::from_bytes([0x22; UUID_LEN]).unwrap();
        let run_id = BlobGcRunId::from_bytes([0x33; UUID_LEN]).unwrap();
        let partition = PartitionFingerprint::new([0x44; HASH_LEN]);
        let root = TextManifestRootKey {
            index_id,
            generation,
            partition,
        };
        let intent_key = TextIntentOwnedKey {
            index_id,
            generation,
            intent_id,
        };
        let mut scoped = vec![
            ("index_record", IndexV2Key::index_record(identity())),
            ("operation", IndexV2Key::operation(operation_id)),
            (
                "build_delta",
                IndexV2Key::BuildDelta(IndexEntityStateKey {
                    index_id,
                    generation,
                    entity,
                }),
            ),
            (
                "applied_state",
                IndexV2Key::AppliedState(IndexEntityStateKey {
                    index_id,
                    generation,
                    entity,
                }),
            ),
        ];
        let equality = CanonicalSecondaryValue::Equality([0x55; VALUE_HASH_MAX_LEN]);
        let ascending_range = CanonicalSecondaryValue::range(RangeIndexDirection::Asc, "rng");
        let descending_range = CanonicalSecondaryValue::range(RangeIndexDirection::Desc, "rng");
        for (name, lane, value, entity_id) in [
            (
                "secondary_node_equality",
                SecondaryEntryLane::NodeEquality,
                equality.clone(),
                Some(entity.id),
            ),
            (
                "secondary_node_unique_equality",
                SecondaryEntryLane::NodeUniqueEquality,
                equality.clone(),
                None,
            ),
            (
                "secondary_node_range_ascending",
                SecondaryEntryLane::NodeRangeAscending,
                ascending_range.clone(),
                Some(entity.id),
            ),
            (
                "secondary_node_range_descending",
                SecondaryEntryLane::NodeRangeDescending,
                descending_range.clone(),
                Some(entity.id),
            ),
            (
                "secondary_edge_equality",
                SecondaryEntryLane::EdgeEquality,
                equality,
                Some(entity.id),
            ),
            (
                "secondary_edge_range_ascending",
                SecondaryEntryLane::EdgeRangeAscending,
                ascending_range,
                Some(entity.id),
            ),
            (
                "secondary_edge_range_descending",
                SecondaryEntryLane::EdgeRangeDescending,
                descending_range,
                Some(entity.id),
            ),
        ] {
            scoped.push((
                name,
                IndexV2Key::SecondaryEntry(
                    SecondaryEntryKey::try_new(index_id, generation, lane, value, entity_id)
                        .unwrap(),
                ),
            ));
        }
        scoped.extend([
            ("text_manifest_root", IndexV2Key::TextManifestRoot(root)),
            (
                "text_manifest_page",
                IndexV2Key::TextManifestPage(TextManifestPageKey { root, page: 2 }),
            ),
            (
                "text_upload_intent",
                IndexV2Key::TextUploadIntent(intent_key),
            ),
            (
                "text_build_artifact",
                IndexV2Key::TextBuildArtifact(TextBuildArtifactKey { root, ordinal: 3 }),
            ),
            (
                "blob_gc_generation_candidate",
                IndexV2Key::BlobGcCandidate(BlobGcCandidateKey {
                    index_id,
                    generation,
                    owner: BlobGcCandidateKeyOwner::GenerationCleanup,
                    blob_hash: BlobHash::new([0x66; HASH_LEN]),
                }),
            ),
            (
                "blob_gc_intent_candidate",
                IndexV2Key::BlobGcCandidate(BlobGcCandidateKey {
                    index_id,
                    generation,
                    owner: BlobGcCandidateKeyOwner::UploadIntent(intent_id),
                    blob_hash: BlobHash::new([0x66; HASH_LEN]),
                }),
            ),
            (
                "text_entity_state",
                IndexV2Key::TextEntityState(TextEntityStateKey { root, entity }),
            ),
            (
                "active_mutation_proof",
                IndexV2Key::ActiveMutationCommitProof(intent_key),
            ),
            (
                "vector_partition_mapping",
                IndexV2Key::VectorPartitionMapping(VectorPartitionMappingKey {
                    index_id,
                    generation,
                    partition,
                }),
            ),
        ]);

        let tenant = DataScope::Tenant(TenantId::from_u128(0x2A));
        let scoped_goldens = scoped
            .into_iter()
            .map(|(name, key)| {
                let logical = key.to_bytes();
                assert_eq!(IndexV2Key::parse_from_slice(&logical).unwrap(), key);
                let unscoped = Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(key.clone()),
                }
                .to_bytes();
                let tenant = Key::Data {
                    scope: tenant,
                    kind: DataKeyKind::IndexV2(key),
                }
                .to_bytes();
                (name, hex(&logical), hex(&unscoped), hex(&tenant))
            })
            .collect::<Vec<_>>();

        let upload_owner = IndexV2Key::TextUploadIntent(intent_key).to_bytes();
        let manifest_owner =
            IndexV2Key::TextManifestPage(TextManifestPageKey { root, page: 2 }).to_bytes();
        let artifact_owner =
            IndexV2Key::TextBuildArtifact(TextBuildArtifactKey { root, ordinal: 3 }).to_bytes();
        let references = [
            (
                BlobReferenceOwnerKind::UploadIntent,
                DataScope::LegacyUnscoped,
                upload_owner,
            ),
            (
                BlobReferenceOwnerKind::ManifestPageSplit,
                tenant,
                manifest_owner,
            ),
            (
                BlobReferenceOwnerKind::BuildArtifact,
                DataScope::LegacyUnscoped,
                artifact_owner,
            ),
        ];
        let mut global = vec![
            ("storage_version", GlobalIndexV2Key::StorageVersion),
            (
                "logical_id_watermark",
                GlobalIndexV2Key::LogicalIndexIdWatermark,
            ),
            (
                "vector_id_watermark",
                GlobalIndexV2Key::VectorPhysicalIdWatermark,
            ),
            (
                "operation_pointer",
                GlobalIndexV2Key::OperationPointer(operation_id),
            ),
            ("upload_pointer", GlobalIndexV2Key::UploadPointer(intent_id)),
            ("blob_gc_root", GlobalIndexV2Key::BlobGcRunRoot(run_id)),
            (
                "blob_gc_mark_first",
                GlobalIndexV2Key::BlobGcReachabilityMark {
                    run_id,
                    pass: BlobGcPass::First,
                    scan_attempt: NonZeroU64::MIN,
                    blob_hash: BlobHash::new([0x77; HASH_LEN]),
                },
            ),
            (
                "blob_gc_mark_second",
                GlobalIndexV2Key::BlobGcReachabilityMark {
                    run_id,
                    pass: BlobGcPass::Second,
                    scan_attempt: NonZeroU64::new(2).unwrap(),
                    blob_hash: BlobHash::new([0x77; HASH_LEN]),
                },
            ),
            (
                "blob_gc_member",
                GlobalIndexV2Key::BlobGcCandidateMember {
                    run_id,
                    blob_hash: BlobHash::new([0x88; HASH_LEN]),
                },
            ),
        ];
        for (ordinal, (owner_kind, scope, owner_logical_key)) in references.into_iter().enumerate()
        {
            global.push((
                match owner_kind {
                    BlobReferenceOwnerKind::UploadIntent => "blob_reference_upload",
                    BlobReferenceOwnerKind::ManifestPageSplit => "blob_reference_manifest",
                    BlobReferenceOwnerKind::BuildArtifact => "blob_reference_artifact",
                },
                GlobalIndexV2Key::BlobReachabilityReference(
                    BlobReferenceGlobalKey::try_new(
                        BlobHash::new([0x99; HASH_LEN]),
                        owner_kind,
                        scope,
                        owner_logical_key,
                        ordinal as u32,
                    )
                    .unwrap(),
                ),
            ));
        }
        let global_goldens = global
            .into_iter()
            .map(|(name, key)| {
                let bytes = key.to_bytes();
                assert_eq!(GlobalIndexV2Key::parse_from_slice(&bytes).unwrap(), key);
                (name, hex(&bytes))
            })
            .collect::<Vec<_>>();

        insta::assert_debug_snapshot!((scoped_goldens, global_goldens), @r###"
(
    [
        (
            "index_record",
            "06010101000000045573657200000005656d61696c",
            "06010101000000045573657200000005656d61696c",
            "0000000000000000000000000000002a06010101000000045573657200000005656d61696c",
        ),
        (
            "operation",
            "060211111111111111111111111111111111",
            "060211111111111111111111111111111111",
            "0000000000000000000000000000002a060211111111111111111111111111111111",
        ),
        (
            "build_delta",
            "060300000000000000010000000000000001010000000000000007",
            "060300000000000000010000000000000001010000000000000007",
            "0000000000000000000000000000002a060300000000000000010000000000000001010000000000000007",
        ),
        (
            "applied_state",
            "060400000000000000010000000000000001010000000000000007",
            "060400000000000000010000000000000001010000000000000007",
            "0000000000000000000000000000002a060400000000000000010000000000000001010000000000000007",
        ),
        (
            "secondary_node_equality",
            "0605000000000000000100000000000000010155555555555555550000000000000007",
            "0605000000000000000100000000000000010155555555555555550000000000000007",
            "0000000000000000000000000000002a0605000000000000000100000000000000010155555555555555550000000000000007",
        ),
        (
            "secondary_node_unique_equality",
            "060500000000000000010000000000000001025555555555555555",
            "060500000000000000010000000000000001025555555555555555",
            "0000000000000000000000000000002a060500000000000000010000000000000001025555555555555555",
        ),
        (
            "secondary_node_range_ascending",
            "06050000000000000001000000000000000103726e670000000000000007",
            "06050000000000000001000000000000000103726e670000000000000007",
            "0000000000000000000000000000002a06050000000000000001000000000000000103726e670000000000000007",
        ),
        (
            "secondary_node_range_descending",
            "060500000000000000010000000000000001048d9198fffe0000000000000007",
            "060500000000000000010000000000000001048d9198fffe0000000000000007",
            "0000000000000000000000000000002a060500000000000000010000000000000001048d9198fffe0000000000000007",
        ),
        (
            "secondary_edge_equality",
            "0605000000000000000100000000000000010555555555555555550000000000000007",
            "0605000000000000000100000000000000010555555555555555550000000000000007",
            "0000000000000000000000000000002a0605000000000000000100000000000000010555555555555555550000000000000007",
        ),
        (
            "secondary_edge_range_ascending",
            "06050000000000000001000000000000000106726e670000000000000007",
            "06050000000000000001000000000000000106726e670000000000000007",
            "0000000000000000000000000000002a06050000000000000001000000000000000106726e670000000000000007",
        ),
        (
            "secondary_edge_range_descending",
            "060500000000000000010000000000000001078d9198fffe0000000000000007",
            "060500000000000000010000000000000001078d9198fffe0000000000000007",
            "0000000000000000000000000000002a060500000000000000010000000000000001078d9198fffe0000000000000007",
        ),
        (
            "text_manifest_root",
            "0606000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444",
            "0606000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444",
            "0000000000000000000000000000002a0606000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444",
        ),
        (
            "text_manifest_page",
            "060700000000000000010000000000000001444444444444444444444444444444444444444444444444444444444444444400000002",
            "060700000000000000010000000000000001444444444444444444444444444444444444444444444444444444444444444400000002",
            "0000000000000000000000000000002a060700000000000000010000000000000001444444444444444444444444444444444444444444444444444444444444444400000002",
        ),
        (
            "text_upload_intent",
            "06080000000000000001000000000000000122222222222222222222222222222222",
            "06080000000000000001000000000000000122222222222222222222222222222222",
            "0000000000000000000000000000002a06080000000000000001000000000000000122222222222222222222222222222222",
        ),
        (
            "text_build_artifact",
            "060900000000000000010000000000000001444444444444444444444444444444444444444444444444444444444444444400000003",
            "060900000000000000010000000000000001444444444444444444444444444444444444444444444444444444444444444400000003",
            "0000000000000000000000000000002a060900000000000000010000000000000001444444444444444444444444444444444444444444444444444444444444444400000003",
        ),
        (
            "blob_gc_generation_candidate",
            "060a00000000000000010000000000000001016666666666666666666666666666666666666666666666666666666666666666",
            "060a00000000000000010000000000000001016666666666666666666666666666666666666666666666666666666666666666",
            "0000000000000000000000000000002a060a00000000000000010000000000000001016666666666666666666666666666666666666666666666666666666666666666",
        ),
        (
            "blob_gc_intent_candidate",
            "060a0000000000000001000000000000000102666666666666666666666666666666666666666666666666666666666666666622222222222222222222222222222222",
            "060a0000000000000001000000000000000102666666666666666666666666666666666666666666666666666666666666666622222222222222222222222222222222",
            "0000000000000000000000000000002a060a0000000000000001000000000000000102666666666666666666666666666666666666666666666666666666666666666622222222222222222222222222222222",
        ),
        (
            "text_entity_state",
            "060c000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444010000000000000007",
            "060c000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444010000000000000007",
            "0000000000000000000000000000002a060c000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444010000000000000007",
        ),
        (
            "active_mutation_proof",
            "060d0000000000000001000000000000000122222222222222222222222222222222",
            "060d0000000000000001000000000000000122222222222222222222222222222222",
            "0000000000000000000000000000002a060d0000000000000001000000000000000122222222222222222222222222222222",
        ),
        (
            "vector_partition_mapping",
            "060f000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444",
            "060f000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444",
            "0000000000000000000000000000002a060f000000000000000100000000000000014444444444444444444444444444444444444444444444444444444444444444",
        ),
    ],
    [
        (
            "storage_version",
            "fefefefefefefefefefefefefefefefefe01",
        ),
        (
            "logical_id_watermark",
            "fefefefefefefefefefefefefefefefefe02",
        ),
        (
            "vector_id_watermark",
            "fefefefefefefefefefefefefefefefefe03",
        ),
        (
            "operation_pointer",
            "fefefefefefefefefefefefefefefefefe0411111111111111111111111111111111",
        ),
        (
            "upload_pointer",
            "fefefefefefefefefefefefefefefefefe0522222222222222222222222222222222",
        ),
        (
            "blob_gc_root",
            "fefefefefefefefefefefefefefefefefe0633333333333333333333333333333333",
        ),
        (
            "blob_gc_mark_first",
            "fefefefefefefefefefefefefefefefefe07333333333333333333333333333333330100000000000000017777777777777777777777777777777777777777777777777777777777777777",
        ),
        (
            "blob_gc_mark_second",
            "fefefefefefefefefefefefefefefefefe07333333333333333333333333333333330200000000000000027777777777777777777777777777777777777777777777777777777777777777",
        ),
        (
            "blob_gc_member",
            "fefefefefefefefefefefefefefefefefe09333333333333333333333333333333338888888888888888888888888888888888888888888888888888888888888888",
        ),
        (
            "blob_reference_upload",
            "fefefefefefefefefefefefefefefefefe0899999999999999999999999999999999999999999999999999999999999999990100000000220608000000000000000100000000000000012222222222222222222222222222222200000000",
        ),
        (
            "blob_reference_manifest",
            "fefefefefefefefefefefefefefefefefe08999999999999999999999999999999999999999999999999999999999999999902010000000000000000000000000000002a0000003606070000000000000001000000000000000144444444444444444444444444444444444444444444444444444444444444440000000200000001",
        ),
        (
            "blob_reference_artifact",
            "fefefefefefefefefefefefefefefefefe08999999999999999999999999999999999999999999999999999999999999999903000000003606090000000000000001000000000000000144444444444444444444444444444444444444444444444444444444444444440000000300000002",
        ),
    ],
)
"###);
    }

    #[test]
    fn scoped_record_key_has_exact_unscoped_and_tenant_goldens() {
        let logical = IndexV2Key::index_record(identity());
        let expected = b"\x06\x01\x01\x01\x00\x00\x00\x04User\x00\x00\x00\x05email";
        assert_eq!(logical.to_bytes().as_ref(), expected);
        assert_eq!(IndexV2Key::parse_from_slice(expected).unwrap(), logical);

        let tenant = TenantId::from_u128(0x2A);
        let physical = Key::Data {
            scope: DataScope::Tenant(tenant),
            kind: DataKeyKind::IndexV2(logical.clone()),
        }
        .to_bytes();
        assert_eq!(physical.len(), TENANT_ID_LEN + expected.len());
        const TENANT_OFFSET: usize = 0;
        const LOGICAL_OFFSET: usize = TENANT_OFFSET + TENANT_ID_LEN;
        assert_eq!(
            &physical[TENANT_OFFSET..TENANT_OFFSET + TENANT_ID_LEN],
            &0x2Au128.to_be_bytes()
        );
        assert_eq!(
            &physical[LOGICAL_OFFSET..LOGICAL_OFFSET + expected.len()],
            expected
        );
    }

    #[test]
    fn every_global_key_kind_roundtrips_and_uses_exact_sentinel() {
        let operation = IndexOperationId::from_bytes([1; UUID_LEN]).unwrap();
        let intent = TextUploadIntentId::from_bytes([2; UUID_LEN]).unwrap();
        let run = BlobGcRunId::from_bytes([3; UUID_LEN]).unwrap();
        let owner = IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            intent_id: intent,
        })
        .to_bytes();
        let reference = BlobReferenceGlobalKey::try_new(
            BlobHash::new([4; HASH_LEN]),
            BlobReferenceOwnerKind::UploadIntent,
            DataScope::LegacyUnscoped,
            owner,
            0,
        )
        .unwrap();
        let keys = [
            GlobalIndexV2Key::StorageVersion,
            GlobalIndexV2Key::LogicalIndexIdWatermark,
            GlobalIndexV2Key::VectorPhysicalIdWatermark,
            GlobalIndexV2Key::OperationPointer(operation),
            GlobalIndexV2Key::UploadPointer(intent),
            GlobalIndexV2Key::BlobGcRunRoot(run),
            GlobalIndexV2Key::BlobGcReachabilityMark {
                run_id: run,
                pass: BlobGcPass::First,
                scan_attempt: NonZeroU64::MIN,
                blob_hash: BlobHash::new([5; HASH_LEN]),
            },
            GlobalIndexV2Key::BlobReachabilityReference(reference),
            GlobalIndexV2Key::BlobGcCandidateMember {
                run_id: run,
                blob_hash: BlobHash::new([6; HASH_LEN]),
            },
        ];
        for (ordinal, key) in keys.into_iter().enumerate() {
            let bytes = key.to_bytes();
            const SENTINEL_OFFSET: usize = 0;
            const GLOBAL_KIND_OFFSET: usize = SENTINEL_OFFSET + GLOBAL_SENTINEL_LEN;
            assert_eq!(
                &bytes[SENTINEL_OFFSET..SENTINEL_OFFSET + GLOBAL_SENTINEL_LEN],
                &GLOBAL_INDEX_V2_SENTINEL
            );
            assert_eq!(bytes[GLOBAL_KIND_OFFSET], (ordinal + 1) as u8);
            assert_eq!(GlobalIndexV2Key::parse_from_slice(&bytes).unwrap(), key);
            assert_eq!(
                Key::Global {
                    kind: GlobalKeyKind::IndexV2(key.clone())
                }
                .to_bytes(),
                bytes
            );
        }
    }

    #[test]
    fn blob_gc_scan_prefixes_are_exact_typed_key_prefixes() {
        let run_id = BlobGcRunId::from_bytes([7; UUID_LEN]).unwrap();
        let attempt = NonZeroU64::new(9).unwrap();
        let blob_hash = BlobHash::new([8; HASH_LEN]);
        let mark_prefix =
            GlobalIndexV2Key::blob_gc_reachability_mark_prefix(run_id, BlobGcPass::Second, attempt);
        let mark = GlobalIndexV2Key::BlobGcReachabilityMark {
            run_id,
            pass: BlobGcPass::Second,
            scan_attempt: attempt,
            blob_hash,
        }
        .to_bytes();
        const PREFIX_OFFSET: usize = 0;
        let run_prefix = GlobalIndexV2Key::blob_gc_reachability_mark_run_prefix(run_id);
        assert_eq!(
            &mark[PREFIX_OFFSET..PREFIX_OFFSET + run_prefix.len()],
            run_prefix.as_ref(),
            "run prefix excludes pass, attempt, and member hash"
        );
        assert_eq!(
            &mark[PREFIX_OFFSET..PREFIX_OFFSET + mark_prefix.len()],
            mark_prefix.as_ref(),
            "mark prefix excludes only the member hash"
        );

        let reference_prefix = GlobalIndexV2Key::blob_reachability_reference_prefix(blob_hash);
        let reference = BlobReferenceGlobalKey::try_new(
            blob_hash,
            BlobReferenceOwnerKind::UploadIntent,
            DataScope::LegacyUnscoped,
            IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                intent_id: TextUploadIntentId::from_bytes([9; UUID_LEN]).unwrap(),
            })
            .to_bytes(),
            0,
        )
        .unwrap();
        let reference = GlobalIndexV2Key::BlobReachabilityReference(reference).to_bytes();
        assert_eq!(
            &reference[PREFIX_OFFSET..PREFIX_OFFSET + reference_prefix.len()],
            reference_prefix.as_ref(),
            "reference prefix excludes only the typed owner suffix"
        );
    }

    #[test]
    fn sentinel_is_disjoint_from_unscoped_and_all_fe_tenant_data() {
        let logical_prefixes = [
            KeyPrefix::Adjacency,
            KeyPrefix::EdgePropertyPair,
            KeyPrefix::NodeProperty,
            KeyPrefix::PropertyIndex,
            KeyPrefix::EdgeEndpoints,
            KeyPrefix::EdgePairIndex,
            KeyPrefix::IndexV2,
            KeyPrefix::Metadata,
        ];
        for prefix in logical_prefixes {
            let mut tenant_key = vec![0xFE; TENANT_ID_LEN];
            tenant_key.push(prefix.as_u8());
            const SENTINEL_OFFSET: usize = 0;
            assert_ne!(
                &tenant_key[SENTINEL_OFFSET..SENTINEL_OFFSET + GLOBAL_SENTINEL_LEN],
                &GLOBAL_INDEX_V2_SENTINEL
            );
        }
        assert!(GlobalIndexV2Key::parse_from_slice(&[0x06, 0x01]).is_err());
        assert!(IndexV2Key::parse_from_slice(&GLOBAL_INDEX_V2_SENTINEL).is_err());
    }

    #[test]
    fn malformed_zero_unknown_and_trailing_keys_fail_closed() {
        let mut zero_id = IndexV2Key::BuildDelta(IndexEntityStateKey {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            entity: IndexEntity {
                kind: IndexElementKind::Node,
                id: IndexEntityId::initial(),
            },
        })
        .to_bytes()
        .to_vec();
        const INDEX_ID_OFFSET: usize = PREFIX_LEN + KIND_LEN;
        zero_id[INDEX_ID_OFFSET..INDEX_ID_OFFSET + U64_LEN].copy_from_slice(&0u64.to_be_bytes());
        assert!(IndexV2Key::parse_from_slice(&zero_id).is_err());

        assert!(IndexV2Key::parse_from_slice(&[0x06, 0xFF]).is_err());
        let mut trailing =
            IndexV2Key::operation(IndexOperationId::from_bytes([1; UUID_LEN]).unwrap())
                .to_bytes()
                .to_vec();
        trailing.push(0);
        assert!(IndexV2Key::parse_from_slice(&trailing).is_err());

        let run_id = BlobGcRunId::from_bytes([2; UUID_LEN]).unwrap();
        let mut zero_scan_attempt = GlobalIndexV2Key::BlobGcReachabilityMark {
            run_id,
            pass: BlobGcPass::First,
            scan_attempt: NonZeroU64::MIN,
            blob_hash: BlobHash::new([3; HASH_LEN]),
        }
        .to_bytes()
        .to_vec();
        const GLOBAL_KIND_OFFSET: usize = GLOBAL_SENTINEL_LEN;
        const GLOBAL_SUFFIX_OFFSET: usize = GLOBAL_KIND_OFFSET + KIND_LEN;
        const PASS_OFFSET: usize = GLOBAL_SUFFIX_OFFSET + UUID_LEN;
        const SCAN_ATTEMPT_OFFSET: usize = PASS_OFFSET + KIND_LEN;
        zero_scan_attempt[SCAN_ATTEMPT_OFFSET..SCAN_ATTEMPT_OFFSET + U64_LEN]
            .copy_from_slice(&0u64.to_be_bytes());
        assert!(GlobalIndexV2Key::parse_from_slice(&zero_scan_attempt).is_err());

        let mut nil_pointer = GlobalIndexV2Key::OperationPointer(
            IndexOperationId::from_bytes([4; UUID_LEN]).unwrap(),
        )
        .to_bytes()
        .to_vec();
        nil_pointer[GLOBAL_SUFFIX_OFFSET..GLOBAL_SUFFIX_OFFSET + UUID_LEN].fill(0);
        assert!(GlobalIndexV2Key::parse_from_slice(&nil_pointer).is_err());

        let mut global_trailing = GlobalIndexV2Key::StorageVersion.to_bytes().to_vec();
        global_trailing.push(0);
        assert!(GlobalIndexV2Key::parse_from_slice(&global_trailing).is_err());

        let tenant_scope = DataScope::Tenant(TenantId::from_u128(42));
        let tenant_key = Key::Data {
            scope: tenant_scope,
            kind: DataKeyKind::IndexV2(IndexV2Key::index_record(identity())),
        }
        .to_bytes();
        assert!(Key::parse_from_slice(DataScope::LegacyUnscoped, &tenant_key).is_err());

        assert!(BlobReferenceGlobalKey::try_new(
            BlobHash::new([5; HASH_LEN]),
            BlobReferenceOwnerKind::BuildArtifact,
            DataScope::LegacyUnscoped,
            IndexV2Key::TextUploadIntent(TextIntentOwnedKey {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                intent_id: TextUploadIntentId::from_bytes([6; UUID_LEN]).unwrap(),
            })
            .to_bytes(),
            0,
        )
        .is_err());
    }

    #[test]
    fn malformed_secondary_range_value_encodings_fail_closed() {
        const RANGE_VALUE_OFFSET: usize = PREFIX_LEN + KIND_LEN + U64_LEN + U64_LEN + KIND_LEN;
        let mut invalid_utf8 = IndexV2Key::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::initial(),
                IndexGenerationId::initial(),
                SecondaryEntryLane::NodeRangeAscending,
                CanonicalSecondaryValue::range(RangeIndexDirection::Asc, "a"),
                Some(IndexEntityId::new(7)),
            )
            .unwrap(),
        )
        .to_bytes()
        .to_vec();
        invalid_utf8[RANGE_VALUE_OFFSET] = 0xFF;
        assert!(IndexV2Key::parse_from_slice(&invalid_utf8).is_err());

        let mut missing_terminator = IndexV2Key::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::initial(),
                IndexGenerationId::initial(),
                SecondaryEntryLane::NodeRangeDescending,
                CanonicalSecondaryValue::range(RangeIndexDirection::Desc, "a"),
                Some(IndexEntityId::new(7)),
            )
            .unwrap(),
        )
        .to_bytes()
        .to_vec();
        let terminator_last_byte = missing_terminator.len() - U64_LEN - 1;
        missing_terminator[terminator_last_byte] = 0xFD;
        assert!(IndexV2Key::parse_from_slice(&missing_terminator).is_err());

        let mut invalid_escape = IndexV2Key::SecondaryEntry(
            SecondaryEntryKey::try_new(
                IndexId::initial(),
                IndexGenerationId::initial(),
                SecondaryEntryLane::EdgeRangeDescending,
                CanonicalSecondaryValue::range(RangeIndexDirection::Desc, "\0"),
                Some(IndexEntityId::new(8)),
            )
            .unwrap(),
        )
        .to_bytes()
        .to_vec();
        invalid_escape[RANGE_VALUE_OFFSET + 1] = 0x01;
        assert!(IndexV2Key::parse_from_slice(&invalid_escape).is_err());

        assert!(SecondaryEntryKey::try_new(
            IndexId::initial(),
            IndexGenerationId::initial(),
            SecondaryEntryLane::NodeRangeDescending,
            CanonicalSecondaryValue::Range(Bytes::from_static(b"wrong")),
            Some(IndexEntityId::new(9)),
        )
        .is_err());
    }

    #[test]
    fn secondary_lane_shape_cannot_disagree_with_value_or_entity_suffix() {
        assert!(SecondaryEntryKey::try_new(
            IndexId::initial(),
            IndexGenerationId::initial(),
            SecondaryEntryLane::NodeUniqueEquality,
            CanonicalSecondaryValue::Equality([7; VALUE_HASH_MAX_LEN]),
            None,
        )
        .is_ok());
        assert!(SecondaryEntryKey::try_new(
            IndexId::initial(),
            IndexGenerationId::initial(),
            SecondaryEntryLane::NodeUniqueEquality,
            CanonicalSecondaryValue::Range(Bytes::from_static(b"wrong")),
            None,
        )
        .is_err());
    }
}
