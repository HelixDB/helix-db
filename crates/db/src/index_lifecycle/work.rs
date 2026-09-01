//! Typed V2 physical-work and text-manifest values.

use std::num::NonZeroU32;

use bytes::{BufMut, Bytes};
use sha2::{Digest, Sha256};

use crate::encoding::v2::keys::{
    BlobHash, CanonicalSecondaryValue, PartitionFingerprint, SecondaryEntryLane,
};

use super::{
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, TextLogicalVersion,
    TextManifestRevision,
};

const MAX_LENGTH_DELIMITED_FIELD: usize = 16 * 1024 * 1024;
const MAX_COLLECTION_ITEMS: usize = u16::MAX as usize;
const MAX_TEXT_TERM_LEN: usize = u16::MAX as usize - 5;

/// Failure to construct a V2 work value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum IndexWorkModelError {
    #[error("tenant partition value must not be empty")]
    EmptyTenantPartition,
    #[error("a vector partition mapping requires a tenant-value partition")]
    UnpartitionedVectorMapping,
    #[error("field {field} is {actual} bytes; maximum is {maximum}")]
    OversizedField {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("collection {field} has {actual} items; maximum is {maximum}")]
    OversizedCollection {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    #[error("invalid split reference size/offset relationship")]
    InvalidSplitReference,
    #[error("manifest root page count {page_count} disagrees with split count {split_count}")]
    InvalidManifestRootCounts { page_count: u32, split_count: u64 },
    #[error("manifest page must contain at least one split")]
    EmptyManifestPage,
    #[error("manifest page {actual} does not follow expected page {expected}")]
    NonContiguousManifestPage { expected: u32, actual: u32 },
    #[error("text manifest page count is exhausted")]
    ManifestPageCountExhausted,
    #[error("text manifest revision is exhausted")]
    ManifestRevisionExhausted,
    #[error("an empty text corpus cannot retain indexed tokens")]
    EmptyTextCorpusWithTokens,
    #[error("text term document frequency must be non-zero")]
    ZeroTextDocumentFrequency,
    #[error("text statistics terms must be non-empty, sorted, unique, and bounded")]
    InvalidTextStatisticsTerms,
}

/// Canonical text/vector partition identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TextPartition {
    /// Index rows are not partitioned by a tenant property.
    Unpartitioned,
    /// Canonically normalized tenant property bytes.
    TenantValue(Bytes),
}

impl TextPartition {
    /// Validates a normalized tenant value.
    pub(crate) fn try_tenant_value(value: Bytes) -> Result<Self, IndexWorkModelError> {
        if value.is_empty() {
            return Err(IndexWorkModelError::EmptyTenantPartition);
        }
        if value.len() > MAX_LENGTH_DELIMITED_FIELD {
            return Err(IndexWorkModelError::OversizedField {
                field: "tenant partition",
                actual: value.len(),
                maximum: MAX_LENGTH_DELIMITED_FIELD,
            });
        }
        Ok(Self::TenantValue(value))
    }

    /// Encodes the exact bytes hashed into the partition fingerprint.
    pub(crate) fn canonical_bytes(&self) -> Bytes {
        match self {
            Self::Unpartitioned => Bytes::from_static(&[0x01]),
            Self::TenantValue(value) => {
                let mut bytes = Vec::with_capacity(1 + 4 + value.len());
                bytes.put_u8(0x02);
                bytes.put_u32(u32::try_from(value.len()).expect("bounded tenant value fits u32"));
                bytes.put_slice(value);
                Bytes::from(bytes)
            }
        }
    }

    /// Returns the full SHA-256 of the canonical partition encoding.
    pub(crate) fn fingerprint(&self) -> PartitionFingerprint {
        PartitionFingerprint::new(Sha256::digest(self.canonical_bytes()).into())
    }
}

/// Canonical tenant-only partition accepted by vector mapping rows.
///
/// This wrapper excludes [`TextPartition::Unpartitioned`], which is owned
/// directly by [`super::VectorPhysicalLayout::Unpartitioned`] and must never
/// acquire a durable mapping row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct VectorTenantPartition(TextPartition);

impl VectorTenantPartition {
    /// Validates normalized tenant bytes and constructs a mapping partition.
    pub(crate) fn try_new(value: Bytes) -> Result<Self, IndexWorkModelError> {
        TextPartition::try_tenant_value(value).map(Self)
    }

    /// Refines a general canonical partition to its tenant-only variant.
    pub(crate) fn try_from_partition(
        partition: TextPartition,
    ) -> Result<Self, IndexWorkModelError> {
        match partition {
            TextPartition::TenantValue(_) => Ok(Self(partition)),
            TextPartition::Unpartitioned => Err(IndexWorkModelError::UnpartitionedVectorMapping),
        }
    }

    /// Borrows the canonical partition encoding stored in mapping values.
    pub(crate) const fn as_partition(&self) -> &TextPartition {
        &self.0
    }

    /// Returns the full fingerprint stored in the matching mapping key.
    pub(crate) fn fingerprint(&self) -> PartitionFingerprint {
        self.0.fingerprint()
    }
}

/// Content-addressed object reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlobRef {
    pub(crate) hash: BlobHash,
    pub(crate) size: u64,
}

impl BlobRef {
    /// Constructs a content-addressed reference from a full SHA-256 and object size.
    pub const fn new(hash: [u8; 32], size: u64) -> Self {
        Self {
            hash: BlobHash::new(hash),
            size,
        }
    }

    /// Returns the full SHA-256 used as both object identity and checksum.
    pub const fn hash(&self) -> &[u8; 32] {
        self.hash.as_bytes()
    }

    /// Returns the exact object byte size cross-checked during publication.
    pub const fn size(self) -> u64 {
        self.size
    }
}

/// Exact published Tantivy split metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SplitRef {
    blob: BlobRef,
    footer_offset: u64,
    footer_length: u32,
    hot_cache_length: u32,
    total_size: u64,
    pruning: SplitPruning,
}

/// Conservative term summary used to skip immutable text splits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SplitPruning {
    /// The split predates pruning metadata and must always be searched.
    Unavailable,
    /// A 256-bit Bloom filter with three SHA-256-derived bits per analyzed term.
    TermBloom256([u64; SPLIT_PRUNING_BLOOM_WORDS]),
}

pub(crate) const SPLIT_PRUNING_BLOOM_WORDS: usize = 4;

impl SplitPruning {
    /// Builds a conservative Bloom filter from canonical analyzed terms.
    pub(crate) fn from_terms<T: AsRef<[u8]>>(terms: impl IntoIterator<Item = T>) -> Self {
        let bits = terms
            .into_iter()
            .fold([0_u64; SPLIT_PRUNING_BLOOM_WORDS], |mut bits, term| {
                let digest = Sha256::digest(term.as_ref());
                for position in [digest[0], digest[1], digest[2]] {
                    let position = usize::from(position);
                    bits[position / u64::BITS as usize] |= 1_u64 << (position % u64::BITS as usize);
                }
                bits
            });
        Self::TermBloom256(bits)
    }

    /// Returns whether at least one OR query term may occur in this split.
    pub(crate) fn may_match_any<T: AsRef<[u8]>>(self, terms: impl IntoIterator<Item = T>) -> bool {
        match self {
            Self::Unavailable => true,
            Self::TermBloom256(bits) => terms.into_iter().any(|term| {
                let digest = Sha256::digest(term.as_ref());
                [digest[0], digest[1], digest[2]]
                    .into_iter()
                    .all(|position| {
                        let position = usize::from(position);
                        bits[position / u64::BITS as usize]
                            & (1_u64 << (position % u64::BITS as usize))
                            != 0
                    })
            }),
        }
    }

    /// Conservatively combines summaries for a merged split.
    pub(crate) const fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::TermBloom256(left), Self::TermBloom256(right)) => Self::TermBloom256([
                left[0] | right[0],
                left[1] | right[1],
                left[2] | right[2],
                left[3] | right[3],
            ]),
            (Self::Unavailable, _) | (_, Self::Unavailable) => Self::Unavailable,
        }
    }
}

impl SplitRef {
    /// Constructs one non-empty split whose footer and hot-cache regions fit its blob.
    pub(crate) fn try_new(
        blob: BlobRef,
        footer_offset: u64,
        footer_length: u32,
        hot_cache_length: u32,
        total_size: u64,
        pruning: SplitPruning,
    ) -> Result<Self, IndexWorkModelError> {
        let footer_end = footer_offset.checked_add(u64::from(footer_length));
        if total_size == 0
            || total_size != blob.size
            || footer_end.is_none_or(|end| end > total_size)
            || u64::from(hot_cache_length) > total_size
        {
            return Err(IndexWorkModelError::InvalidSplitReference);
        }
        Ok(Self {
            blob,
            footer_offset,
            footer_length,
            hot_cache_length,
            total_size,
            pruning,
        })
    }

    /// Returns the content-addressed object containing this split.
    pub(crate) const fn blob(self) -> BlobRef {
        self.blob
    }

    /// Returns the byte offset at which the serialized footer starts.
    pub(crate) const fn footer_offset(self) -> u64 {
        self.footer_offset
    }

    /// Returns the serialized footer length in bytes.
    pub(crate) const fn footer_length(self) -> u32 {
        self.footer_length
    }

    /// Returns the serialized hot-cache length in bytes.
    pub(crate) const fn hot_cache_length(self) -> u32 {
        self.hot_cache_length
    }

    /// Returns the exact non-zero object size in bytes.
    pub(crate) const fn total_size(self) -> u64 {
        self.total_size
    }

    /// Returns the conservative analyzed-term summary for this split.
    pub(crate) const fn pruning(self) -> SplitPruning {
        self.pruning
    }
}

/// Family-specific recovery state carried by one coalesced build delta.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum CoalescedBuildDeltaState {
    /// The owning family reconciles from durable builder state or authoritative data.
    Marker,
    /// Original secondary value before the first still-pending mutation.
    SecondaryBefore(Option<CanonicalSecondaryValue>),
    /// Original vector partition before the first still-pending mutation.
    VectorBefore(Option<TextPartition>),
}

/// Coalesced build delta body.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CoalescedBuildDeltaValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) entity_kind: IndexElementKind,
    pub(crate) entity_id: IndexEntityId,
    pub(crate) state: CoalescedBuildDeltaState,
}

/// Family-specific authoritative state last applied by a builder.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum AppliedFamilyState {
    Secondary(Option<CanonicalSecondaryValue>),
    Vector(Option<TextPartition>),
    Text(Option<(TextPartition, TextLogicalVersion)>),
}

/// Builder-applied state body.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AppliedEntityStateValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) entity_kind: IndexElementKind,
    pub(crate) entity_id: IndexEntityId,
    pub(crate) state: AppliedFamilyState,
}

/// Generation-qualified secondary entry value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SecondaryEntryValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) lane: SecondaryEntryLane,
    pub(crate) entity_id: IndexEntityId,
}

/// Canonical vector tenant-partition ownership body.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct VectorPartitionMappingValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: VectorTenantPartition,
    pub(crate) physical_index_id: super::VectorPhysicalIndexId,
}

/// Canonical text manifest root.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextManifestRootValue {
    index_id: IndexId,
    generation: IndexGenerationId,
    partition: TextPartition,
    revision: TextManifestRevision,
    page_count: u32,
    split_count: u64,
}

impl TextManifestRootValue {
    /// Constructs a root whose page and split counts can describe contiguous,
    /// non-empty pages or the one canonical empty-partition state.
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        partition: TextPartition,
        revision: TextManifestRevision,
        page_count: u32,
        split_count: u64,
    ) -> Result<Self, IndexWorkModelError> {
        let minimum_splits = u64::from(page_count);
        let maximum_splits = minimum_splits.saturating_mul(MAX_COLLECTION_ITEMS as u64);
        if (page_count == 0) != (split_count == 0)
            || split_count < minimum_splits
            || split_count > maximum_splits
        {
            return Err(IndexWorkModelError::InvalidManifestRootCounts {
                page_count,
                split_count,
            });
        }
        Ok(Self {
            index_id,
            generation,
            partition,
            revision,
            page_count,
            split_count,
        })
    }

    /// Constructs the direct representation of one valid empty partition.
    pub(crate) fn empty(
        index_id: IndexId,
        generation: IndexGenerationId,
        partition: TextPartition,
    ) -> Self {
        Self {
            index_id,
            generation,
            partition,
            revision: TextManifestRevision::initial(),
            page_count: 0,
            split_count: 0,
        }
    }

    /// Returns a revisioned root after appending exactly its next non-empty page.
    pub(crate) fn append_page(
        &self,
        page: u32,
        entry_count: NonZeroU32,
    ) -> Result<Self, IndexWorkModelError> {
        if page != self.page_count {
            return Err(IndexWorkModelError::NonContiguousManifestPage {
                expected: self.page_count,
                actual: page,
            });
        }
        let page_count = self
            .page_count
            .checked_add(1)
            .ok_or(IndexWorkModelError::ManifestPageCountExhausted)?;
        let split_count = self.split_count + u64::from(entry_count.get());
        let revision = self
            .revision
            .checked_next()
            .map_err(|_| IndexWorkModelError::ManifestRevisionExhausted)?;
        Self::try_new(
            self.index_id,
            self.generation,
            self.partition.clone(),
            revision,
            page_count,
            split_count,
        )
    }

    /// Appends one BUILD page while consuming a revision already reserved by
    /// catch-up, or advances when the initial source pass reserved none.
    ///
    /// BUILD catch-up advances roots atomically with entity state before its
    /// immutable artifact reaches manifest preparation. Multiple artifacts can
    /// share one bounded page, so the current revision may remain above the
    /// minimum implied by page count.
    pub(crate) fn append_build_page(
        &self,
        page: u32,
        entry_count: NonZeroU32,
    ) -> Result<Self, IndexWorkModelError> {
        if page != self.page_count {
            return Err(IndexWorkModelError::NonContiguousManifestPage {
                expected: self.page_count,
                actual: page,
            });
        }
        let page_count = self
            .page_count
            .checked_add(1)
            .ok_or(IndexWorkModelError::ManifestPageCountExhausted)?;
        let split_count = self.split_count + u64::from(entry_count.get());
        let minimum_revision = u64::from(page_count).saturating_add(1);
        let revision = if self.revision.get() >= minimum_revision {
            self.revision
        } else {
            self.revision
                .checked_next()
                .map_err(|_| IndexWorkModelError::ManifestRevisionExhausted)?
        };
        Self::try_new(
            self.index_id,
            self.generation,
            self.partition.clone(),
            revision,
            page_count,
            split_count,
        )
    }

    /// Returns the canonical index that owns this manifest root.
    pub(crate) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Returns the exact physical generation that owns this manifest root.
    pub(crate) const fn generation(&self) -> IndexGenerationId {
        self.generation
    }

    /// Returns the canonical partition described by this manifest root.
    pub(crate) fn partition(&self) -> &TextPartition {
        &self.partition
    }

    /// Returns the logical revision advanced by each appended page.
    pub(crate) const fn revision(&self) -> TextManifestRevision {
        self.revision
    }

    /// Returns the number of contiguous pages starting at page zero.
    pub(crate) const fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Returns the total number of splits declared across all pages.
    pub(crate) const fn split_count(&self) -> u64 {
        self.split_count
    }
}

/// Bounded text manifest page.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextManifestPageValue {
    index_id: IndexId,
    generation: IndexGenerationId,
    partition: TextPartition,
    page: u32,
    entries: Vec<SplitRef>,
}

impl TextManifestPageValue {
    /// Maximum entries representable by one frozen manifest-page value.
    pub(crate) const MAX_ENTRIES: usize = MAX_COLLECTION_ITEMS;

    /// Constructs one non-empty bounded page with exact generation ownership.
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        partition: TextPartition,
        page: u32,
        entries: Vec<SplitRef>,
    ) -> Result<Self, IndexWorkModelError> {
        if entries.is_empty() {
            return Err(IndexWorkModelError::EmptyManifestPage);
        }
        if entries.len() > MAX_COLLECTION_ITEMS {
            return Err(IndexWorkModelError::OversizedCollection {
                field: "manifest entries",
                actual: entries.len(),
                maximum: MAX_COLLECTION_ITEMS,
            });
        }
        Ok(Self {
            index_id,
            generation,
            partition,
            page,
            entries,
        })
    }

    /// Returns the canonical index that owns this page.
    pub(crate) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Returns the exact physical generation that owns this page.
    pub(crate) const fn generation(&self) -> IndexGenerationId {
        self.generation
    }

    /// Returns the canonical partition whose root references this page.
    pub(crate) fn partition(&self) -> &TextPartition {
        &self.partition
    }

    /// Returns this page's zero-based position under its root.
    pub(crate) const fn page(&self) -> u32 {
        self.page
    }

    /// Returns the validated non-empty split sequence stored by this page.
    pub(crate) fn entries(&self) -> &[SplitRef] {
        &self.entries
    }
}

/// Durable hidden text build artifact.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextBuildArtifactValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: TextPartition,
    pub(crate) artifact_ordinal: u32,
    pub(crate) split: SplitRef,
}

/// Generation-qualified live text entity state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextEntityStateValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: TextPartition,
    pub(crate) entity_kind: IndexElementKind,
    pub(crate) entity_id: IndexEntityId,
    pub(crate) logical_version: TextLogicalVersion,
    pub(crate) live: bool,
}

/// Exact live corpus totals used by one partition's BM25 provider.
///
/// A zero/zero value explicitly represents an empty live corpus whose
/// immutable manifest may still contain stale versions. That row lets serving
/// distinguish the valid empty corpus from missing V2 accounting.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextCorpusStatisticsValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: TextPartition,
    pub(crate) document_count: u64,
    pub(crate) total_token_count: u64,
}

impl TextCorpusStatisticsValue {
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        partition: TextPartition,
        document_count: u64,
        total_token_count: u64,
    ) -> Result<Self, IndexWorkModelError> {
        if document_count == 0 && total_token_count != 0 {
            return Err(IndexWorkModelError::EmptyTextCorpusWithTokens);
        }
        Ok(Self {
            index_id,
            generation,
            partition,
            document_count,
            total_token_count,
        })
    }
}

/// Exact live document frequency for one analyzed term.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextTermStatisticsValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: TextPartition,
    pub(crate) term: Bytes,
    pub(crate) document_frequency: u64,
}

impl TextTermStatisticsValue {
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        partition: TextPartition,
        term: Bytes,
        document_frequency: u64,
    ) -> Result<Self, IndexWorkModelError> {
        if term.is_empty() || term.len() > MAX_TEXT_TERM_LEN {
            return Err(IndexWorkModelError::InvalidTextStatisticsTerms);
        }
        if document_frequency == 0 {
            return Err(IndexWorkModelError::ZeroTextDocumentFrequency);
        }
        Ok(Self {
            index_id,
            generation,
            partition,
            term,
            document_frequency,
        })
    }
}

/// One entity's exact contribution to live corpus and term statistics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TextStatisticsContribution {
    /// A mutation removed an entity before a concurrent source scan reached it.
    Absent,
    /// The currently accounted document and its sorted unique terms.
    Present {
        partition: TextPartition,
        fingerprint: [u8; 32],
        token_count: u64,
        terms: Vec<Bytes>,
    },
}

impl TextStatisticsContribution {
    pub(crate) fn try_present(
        partition: TextPartition,
        fingerprint: [u8; 32],
        token_count: u64,
        terms: Vec<Bytes>,
    ) -> Result<Self, IndexWorkModelError> {
        let encoded_bytes = terms.iter().try_fold(0_usize, |total, term| {
            if term.is_empty() || term.len() > MAX_TEXT_TERM_LEN {
                return None;
            }
            total
                .checked_add(core::mem::size_of::<u32>())
                .and_then(|total| total.checked_add(term.len()))
        });
        if encoded_bytes.is_none_or(|encoded| encoded > MAX_LENGTH_DELIMITED_FIELD)
            || !terms.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(IndexWorkModelError::InvalidTextStatisticsTerms);
        }
        Ok(Self::Present {
            partition,
            fingerprint,
            token_count,
            terms,
        })
    }
}

/// Exact-once statistics marker for one generation/entity pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextStatisticsEntityValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) entity_kind: IndexElementKind,
    pub(crate) entity_id: IndexEntityId,
    pub(crate) contribution: TextStatisticsContribution,
}

#[cfg(test)]
mod tests {
    use super::{SplitPruning, SPLIT_PRUNING_BLOOM_WORDS};

    #[test]
    fn split_pruning_is_conservative_and_composable() {
        let left = SplitPruning::from_terms([b"alpha".as_slice(), b"beta".as_slice()]);
        let right = SplitPruning::from_terms([b"gamma".as_slice()]);

        assert!(left.may_match_any([b"alpha".as_slice()]));
        assert!(left.may_match_any([b"beta".as_slice()]));
        assert!(!SplitPruning::TermBloom256([0; SPLIT_PRUNING_BLOOM_WORDS])
            .may_match_any([b"alpha".as_slice()]));
        assert!(left.union(right).may_match_any([b"gamma".as_slice()]));
        assert_eq!(
            left.union(SplitPruning::Unavailable),
            SplitPruning::Unavailable
        );
        assert!(SplitPruning::Unavailable.may_match_any([b"absent".as_slice()]));
    }

    #[test]
    fn split_pruning_remains_selective_for_a_modest_vocabulary() {
        let terms = (0..64).map(|term| format!("term-{term}"));
        let pruning = SplitPruning::from_terms(terms);

        assert!(!pruning.may_match_any([b"absent".as_slice()]));
    }
}
