//! Typed V2 physical-work, upload-publication, manifest, and GC values.

use std::num::{NonZeroU32, NonZeroU64};

use bytes::{BufMut, Bytes};
use sha2::{Digest, Sha256};

use crate::encoding::v1::keys::index_v2::{
    BlobHash, BlobReferenceGlobalKey, BlobReferenceOwnerKind, CanonicalSecondaryValue, IndexV2Key,
    PartitionFingerprint, SecondaryEntryLane,
};
use crate::encoding::v1::keys::tenant::DataScope;

use super::{
    BlobGcRunId, BlobGcRunRevision, BlobPublicationPermitId, IndexCursor, IndexElementKind,
    IndexEntityId, IndexGenerationId, IndexId, IndexIdentity, IndexIdentityFamily,
    IndexOperationBlocker, IndexOperationId, IndexOperationRevision, IndexRevision, MutationId,
    OperationClaim, TextIntentRevision, TextLogicalVersion, TextManifestRevision,
    TextUploadIntentId, WriterEpoch,
};

const MAX_LENGTH_DELIMITED_FIELD: usize = 16 * 1024 * 1024;
const MAX_COLLECTION_ITEMS: usize = u16::MAX as usize;
const MAX_LOGICAL_KEY_LEN: usize = 1024 * 1024;

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
    #[error("manifest split location page {page} or slot {slot} is out of bounds")]
    InvalidManifestSplitLocation { page: u32, slot: u32 },
    #[error("upload phase, owner, attachment, and work state disagree")]
    InvalidUploadState,
    #[error("text upload intent revision is exhausted")]
    TextIntentRevisionExhausted,
    #[error("destination owner logical key is not a bounded logical V2 key")]
    InvalidDestinationOwnerKey,
    #[error("GC attempt must be non-zero")]
    ZeroGcAttempt,
    #[error("GC scan attempt is exhausted")]
    GcScanAttemptExhausted,
    #[error("GC candidate count must be non-zero")]
    ZeroCandidateCount,
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
}

impl SplitRef {
    /// Constructs one non-empty split whose footer and hot-cache regions fit its blob.
    pub(crate) fn try_new(
        blob: BlobRef,
        footer_offset: u64,
        footer_length: u32,
        hot_cache_length: u32,
        total_size: u64,
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
}

/// Coalesced build delta body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct CoalescedBuildDeltaValue {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) entity_kind: IndexElementKind,
    pub(crate) entity_id: IndexEntityId,
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

/// Exact bounded manifest page/slot containing one Active split reference.
///
/// Persisting this location in the Active commit proof makes crash recovery an
/// O(1) point-read transition. A worker never scans manifest pages or guesses a
/// slot from logical version, which can also advance for state-only retirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TextManifestSplitLocation {
    page: u32,
    slot: u32,
}

impl TextManifestSplitLocation {
    /// Validates a page index and one bounded zero-based split slot.
    pub(crate) fn try_new(page: u32, slot: u32) -> Result<Self, IndexWorkModelError> {
        if page == u32::MAX
            || usize::try_from(slot)
                .ok()
                .is_none_or(|slot| slot >= TextManifestPageValue::MAX_ENTRIES)
        {
            return Err(IndexWorkModelError::InvalidManifestSplitLocation { page, slot });
        }
        Ok(Self { page, slot })
    }

    /// Returns the zero-based manifest page under the proof's partition root.
    pub(crate) const fn page(self) -> u32 {
        self.page
    }

    /// Returns the zero-based split position within the manifest page.
    pub(crate) const fn slot(self) -> u32 {
        self.slot
    }
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

/// Upload owner and its exact revision/fencing proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TextUploadOwner {
    Build {
        operation_id: IndexOperationId,
        expected_operation_revision: IndexOperationRevision,
    },
    ActiveMutation {
        writer_epoch: WriterEpoch,
        mutation_id: MutationId,
        active_record_revision: IndexRevision,
    },
}

/// Destination attachment prepared by an upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TextUploadAttachment {
    ManifestSplit(SplitRef),
    BuildArtifact {
        artifact_ordinal: u32,
        split: SplitRef,
    },
}

/// Historical authorization retained while releasing a publication permit.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct UploadDestinationAuthorization {
    pub(crate) owner_kind: BlobReferenceOwnerKind,
    pub(crate) owner_logical_key: Bytes,
    pub(crate) owner_slot: u32,
    pub(crate) proof_logical_key: Option<Bytes>,
}

impl UploadDestinationAuthorization {
    /// Validates the destination lane and optional active-mutation proof key.
    pub(crate) fn try_new(
        owner_kind: BlobReferenceOwnerKind,
        owner_logical_key: Bytes,
        owner_slot: u32,
        proof_logical_key: Option<Bytes>,
    ) -> Result<Self, IndexWorkModelError> {
        BlobReferenceGlobalKey::try_new(
            BlobHash::new([0; 32]),
            owner_kind,
            DataScope::LegacyUnscoped,
            owner_logical_key.clone(),
            owner_slot,
        )
        .map_err(|_| IndexWorkModelError::InvalidDestinationOwnerKey)?;
        if owner_logical_key.len() > MAX_LOGICAL_KEY_LEN {
            return Err(IndexWorkModelError::InvalidDestinationOwnerKey);
        }
        if proof_logical_key.as_ref().is_some_and(|proof_logical_key| {
            proof_logical_key.len() > MAX_LOGICAL_KEY_LEN
                || !matches!(
                    IndexV2Key::parse_from_slice(proof_logical_key),
                    Ok(IndexV2Key::ActiveMutationCommitProof(_))
                )
        }) {
            return Err(IndexWorkModelError::InvalidDestinationOwnerKey);
        }
        Ok(Self {
            owner_kind,
            owner_logical_key,
            owner_slot,
            proof_logical_key,
        })
    }
}

/// Reclaim assignment is explicit; absence never means “maybe assigned”.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ReclaimAssignment {
    Unassigned,
    Assigned(BlobGcRunId),
}

/// Monotonic upload publication phase.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TextUploadPhase {
    Prepared,
    Uploaded,
    ReferenceCommitted(UploadDestinationAuthorization),
    Reclaimable(ReclaimAssignment),
    /// Coordinator and object validation proved that publication is terminally absent.
    ///
    /// This durable release-outbox phase remains until the publication permit
    /// is released and the intent-owned reachability row is removed.
    NonPublicationProven,
}

/// Closed phase changes authorized by fenced generation cleanup.
///
/// These transitions are distinct from upload-worker transitions: cleanup
/// owns an exact dropping/aborting generation and a closed blob fence, but it
/// must never manufacture a worker claim merely to reuse a claimed-only API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TextUploadCleanupTransition {
    /// Definitive coordinator success matched the declared blob.
    PublicationSucceeded,
    /// Terminal coordinator state plus fenced object absence was proven.
    NonPublicationProven,
    /// Live intent reachability transferred to intent-qualified reclaim work.
    Reclaimable,
    /// The unassigned reclaim owner joined this exact generation GC run.
    AssignReclaim(BlobGcRunId),
}

/// Runnable upload work state. Completed is represented by record deletion.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TextUploadWorkState {
    Queued { not_before_unix_millis: Option<u64> },
    Claimed(OperationClaim),
    Blocked(IndexOperationBlocker),
}

/// Immutable identity, ownership, and attachment of one text upload.
///
/// This specification is validated before coordinator reservation, so an
/// invalid owner/blob/attachment combination can never consume a publication
/// permit or enter a database transaction.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextUploadSpec {
    index_id: IndexId,
    identity: IndexIdentity,
    generation: IndexGenerationId,
    partition: TextPartition,
    blob: BlobRef,
    owner: TextUploadOwner,
    attachment: TextUploadAttachment,
}

impl TextUploadSpec {
    /// Validates the immutable upload contract before external coordination.
    pub(crate) fn try_new(
        index_id: IndexId,
        identity: IndexIdentity,
        generation: IndexGenerationId,
        partition: TextPartition,
        blob: BlobRef,
        owner: TextUploadOwner,
        attachment: TextUploadAttachment,
    ) -> Result<Self, IndexWorkModelError> {
        if identity.family() != IndexIdentityFamily::Text {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        let active_owner = matches!(owner, TextUploadOwner::ActiveMutation { .. });
        let attachment_split = match attachment {
            TextUploadAttachment::ManifestSplit(split)
            | TextUploadAttachment::BuildArtifact { split, .. } => split,
        };
        if attachment_split.blob != blob
            || (active_owner && !matches!(attachment, TextUploadAttachment::ManifestSplit(_)))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        Ok(Self {
            index_id,
            identity,
            generation,
            partition,
            blob,
            owner,
            attachment,
        })
    }

    /// Returns the exact content-addressed blob reserved for publication.
    pub(crate) const fn blob(&self) -> BlobRef {
        self.blob
    }
}

/// Durable text upload intent.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct TextUploadIntentValue {
    pub(crate) intent_id: TextUploadIntentId,
    pub(crate) revision: TextIntentRevision,
    pub(crate) index_id: IndexId,
    pub(crate) identity: IndexIdentity,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: TextPartition,
    pub(crate) blob: BlobRef,
    pub(crate) publication_permit_id: BlobPublicationPermitId,
    pub(crate) owner: TextUploadOwner,
    pub(crate) attachment: TextUploadAttachment,
    pub(crate) phase: TextUploadPhase,
    pub(crate) attempt: u32,
    pub(crate) work_state: TextUploadWorkState,
}

impl TextUploadIntentValue {
    /// Constructs a text intent only when owner, attachment, phase, and work state agree.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        intent_id: TextUploadIntentId,
        revision: TextIntentRevision,
        index_id: IndexId,
        identity: IndexIdentity,
        generation: IndexGenerationId,
        partition: TextPartition,
        blob: BlobRef,
        publication_permit_id: BlobPublicationPermitId,
        owner: TextUploadOwner,
        attachment: TextUploadAttachment,
        phase: TextUploadPhase,
        attempt: u32,
        work_state: TextUploadWorkState,
    ) -> Result<Self, IndexWorkModelError> {
        let spec = TextUploadSpec::try_new(
            index_id, identity, generation, partition, blob, owner, attachment,
        )?;
        Self::try_from_spec(
            intent_id,
            revision,
            publication_permit_id,
            spec,
            phase,
            attempt,
            work_state,
        )
    }

    /// Combines a prevalidated immutable specification with mutable outbox state.
    pub(crate) fn try_from_spec(
        intent_id: TextUploadIntentId,
        revision: TextIntentRevision,
        publication_permit_id: BlobPublicationPermitId,
        spec: TextUploadSpec,
        phase: TextUploadPhase,
        attempt: u32,
        work_state: TextUploadWorkState,
    ) -> Result<Self, IndexWorkModelError> {
        let TextUploadSpec {
            index_id,
            identity,
            generation,
            partition,
            blob,
            owner,
            attachment,
        } = spec;
        let active_owner = matches!(owner, TextUploadOwner::ActiveMutation { .. });
        let valid_blocked_owner = match (&work_state, owner) {
            (
                TextUploadWorkState::Blocked(IndexOperationBlocker::BlobPublicationMismatch {
                    intent_id: blocked_intent_id,
                }),
                TextUploadOwner::Build { .. },
            ) => *blocked_intent_id == intent_id,
            (TextUploadWorkState::Blocked(_), _) => false,
            (TextUploadWorkState::Queued { .. } | TextUploadWorkState::Claimed(_), _) => true,
        };
        if !valid_blocked_owner {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        let valid_reference_commit = match &phase {
            TextUploadPhase::ReferenceCommitted(authorization) => {
                active_owner == authorization.proof_logical_key.is_some()
                    && matches!(
                        (&attachment, authorization.owner_kind),
                        (
                            TextUploadAttachment::ManifestSplit(_),
                            BlobReferenceOwnerKind::ManifestPageSplit
                        ) | (
                            TextUploadAttachment::BuildArtifact { .. },
                            BlobReferenceOwnerKind::BuildArtifact
                        )
                    )
            }
            TextUploadPhase::Prepared
            | TextUploadPhase::Uploaded
            | TextUploadPhase::NonPublicationProven
            | TextUploadPhase::Reclaimable(_) => true,
        };
        if !valid_reference_commit {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        match &work_state {
            TextUploadWorkState::Blocked(blocker) => blocker
                .validate()
                .map_err(|_| IndexWorkModelError::InvalidUploadState)?,
            TextUploadWorkState::Queued { .. } | TextUploadWorkState::Claimed(_) => {}
        }
        Ok(Self {
            intent_id,
            revision,
            index_id,
            identity,
            generation,
            partition,
            blob,
            publication_permit_id,
            owner,
            attachment,
            phase,
            attempt,
            work_state,
        })
    }

    /// Acquires or replaces a repository-authorized durable worker claim.
    ///
    /// The repository is responsible for proving queued eligibility, writer
    /// fencing, and any supervised same-epoch recovery authority before it
    /// calls this transition.
    pub(crate) fn claim(&self, claim: OperationClaim) -> Result<Self, IndexWorkModelError> {
        if !matches!(
            self.work_state,
            TextUploadWorkState::Queued { .. } | TextUploadWorkState::Claimed(_)
        ) {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            self.phase.clone(),
            self.attempt.saturating_add(1),
            TextUploadWorkState::Claimed(claim),
        )
    }

    /// Releases an exact durable claim into a bounded delayed retry.
    pub(crate) fn transient_failure(
        &self,
        not_before_unix_millis: u64,
    ) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.work_state, TextUploadWorkState::Claimed(_)) {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            self.phase.clone(),
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: Some(not_before_unix_millis),
            },
        )
    }

    /// Records definitive matching publication and releases the worker claim.
    pub(crate) fn publication_succeeded(&self) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Prepared)
            || !matches!(self.work_state, TextUploadWorkState::Claimed(_))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            TextUploadPhase::Uploaded,
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Records publication completed by the in-flight Active request owner.
    ///
    /// Active requests retain exclusive process-local ownership while their
    /// graph transaction is unresolved, so the global worker cannot first
    /// persist a claim. This transition accepts only a queued `Prepared`
    /// Active owner and advances directly to queued `Uploaded`; build owners
    /// and claimed/retried work must use the ordinary claimed transition.
    pub(crate) fn active_request_publication_succeeded(&self) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Prepared)
            || !matches!(self.owner, TextUploadOwner::ActiveMutation { .. })
            || !matches!(
                self.work_state,
                TextUploadWorkState::Queued {
                    not_before_unix_millis: None
                }
            )
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            TextUploadPhase::Uploaded,
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Records the exact Active graph proof without manufacturing a worker claim.
    ///
    /// The request still owns the same-epoch intent while its fresh proof
    /// follow-up transaction removes the intent-owned reachability entry. Only
    /// a queued, immediately runnable `Uploaded` Active intent may cross this
    /// boundary, and the authorization must retain its exact proof key.
    pub(crate) fn active_request_reference_committed(
        &self,
        authorization: UploadDestinationAuthorization,
    ) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Uploaded)
            || !matches!(self.owner, TextUploadOwner::ActiveMutation { .. })
            || authorization.proof_logical_key.is_none()
            || !matches!(
                self.work_state,
                TextUploadWorkState::Queued {
                    not_before_unix_millis: None
                }
            )
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            TextUploadPhase::ReferenceCommitted(authorization),
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Records definitive non-publication before external permit release.
    pub(crate) fn non_publication_proven(&self) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Prepared)
            || !matches!(self.work_state, TextUploadWorkState::Claimed(_))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            TextUploadPhase::NonPublicationProven,
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Transfers an exact shared blob from live intent ownership to GC work.
    pub(crate) fn become_reclaimable(&self) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Prepared)
            || !matches!(self.work_state, TextUploadWorkState::Claimed(_))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned),
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Assigns one claimed reclaimable intent to its atomically created GC root.
    pub(crate) fn assign_reclaim_root(
        &self,
        run_id: BlobGcRunId,
    ) -> Result<Self, IndexWorkModelError> {
        if !matches!(
            self.phase,
            TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned)
        ) || !matches!(self.work_state, TextUploadWorkState::Claimed(_))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            TextUploadPhase::Reclaimable(ReclaimAssignment::Assigned(run_id)),
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Requeues an assigned reclaim intent after its root enters the first pass.
    pub(crate) fn complete_reclaim_owner_normalization(
        &self,
        run_id: BlobGcRunId,
    ) -> Result<Self, IndexWorkModelError> {
        if !matches!(
            self.phase,
            TextUploadPhase::Reclaimable(ReclaimAssignment::Assigned(assigned))
                if assigned == run_id
        ) || !matches!(self.work_state, TextUploadWorkState::Claimed(_))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            self.phase.clone(),
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Records that a fenced/terminal Active graph mutation did not commit.
    ///
    /// Repository code must prove the exact commit-proof key absent while it
    /// atomically transfers intent reachability to the intent-qualified GC
    /// candidate. A build owner or merely prepared publication cannot use this
    /// transition.
    pub(crate) fn active_graph_aborted(&self) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Uploaded)
            || !matches!(self.owner, TextUploadOwner::ActiveMutation { .. })
            || !matches!(self.work_state, TextUploadWorkState::Claimed(_))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned),
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Applies one fenced generation-cleanup transition without a worker claim.
    ///
    /// Cleanup accepts only a queued intent whose exact revision is revalidated
    /// in the same transaction as its pointer and ownership rows. Claimed work
    /// must first be released by the upload lane, and blocked work remains
    /// blocked with its permit retained.
    pub(crate) fn cleanup_transition(
        &self,
        transition: TextUploadCleanupTransition,
    ) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.work_state, TextUploadWorkState::Queued { .. }) {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        let phase = match (&self.phase, transition) {
            (TextUploadPhase::Prepared, TextUploadCleanupTransition::PublicationSucceeded) => {
                TextUploadPhase::Uploaded
            }
            (TextUploadPhase::Prepared, TextUploadCleanupTransition::NonPublicationProven) => {
                TextUploadPhase::NonPublicationProven
            }
            (
                TextUploadPhase::Prepared | TextUploadPhase::Uploaded,
                TextUploadCleanupTransition::Reclaimable,
            ) => TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned),
            (
                TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned),
                TextUploadCleanupTransition::AssignReclaim(run_id),
            ) => TextUploadPhase::Reclaimable(ReclaimAssignment::Assigned(run_id)),
            _ => return Err(IndexWorkModelError::InvalidUploadState),
        };
        self.next_with_state(
            phase,
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Atomically couples a mismatched blob to the operation revision it blocks.
    pub(crate) fn block_for_blob_mismatch(
        &self,
        blocked_operation_revision: IndexOperationRevision,
    ) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Prepared)
            || !matches!(self.work_state, TextUploadWorkState::Claimed(_))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        let TextUploadOwner::Build { operation_id, .. } = self.owner else {
            return Err(IndexWorkModelError::InvalidUploadState);
        };
        self.next_with_owner_and_state(
            TextUploadOwner::Build {
                operation_id,
                expected_operation_revision: blocked_operation_revision,
            },
            self.phase.clone(),
            self.attempt,
            TextUploadWorkState::Blocked(IndexOperationBlocker::BlobPublicationMismatch {
                intent_id: self.intent_id,
            }),
        )
    }

    /// Requeues an exact blocked blob mismatch with the retried operation revision.
    pub(crate) fn retry_blob_mismatch(
        &self,
        retried_operation_revision: IndexOperationRevision,
    ) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Prepared)
            || !matches!(
                self.work_state,
                TextUploadWorkState::Blocked(
                    IndexOperationBlocker::BlobPublicationMismatch { intent_id }
                ) if intent_id == self.intent_id
            )
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        let TextUploadOwner::Build { operation_id, .. } = self.owner else {
            return Err(IndexWorkModelError::InvalidUploadState);
        };
        self.next_with_owner_and_state(
            TextUploadOwner::Build {
                operation_id,
                expected_operation_revision: retried_operation_revision,
            },
            self.phase.clone(),
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Records the exact durable first-reference authorization.
    pub(crate) fn reference_committed(
        &self,
        authorization: UploadDestinationAuthorization,
    ) -> Result<Self, IndexWorkModelError> {
        if !matches!(self.phase, TextUploadPhase::Uploaded)
            || !matches!(self.work_state, TextUploadWorkState::Claimed(_))
        {
            return Err(IndexWorkModelError::InvalidUploadState);
        }
        self.next_with_state(
            TextUploadPhase::ReferenceCommitted(authorization),
            self.attempt,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
    }

    /// Revalidates unchanged upload identity while advancing mutable work state.
    fn next_with_state(
        &self,
        phase: TextUploadPhase,
        attempt: u32,
        work_state: TextUploadWorkState,
    ) -> Result<Self, IndexWorkModelError> {
        self.next_with_owner_and_state(self.owner, phase, attempt, work_state)
    }

    /// Revalidates immutable upload identity while updating its build revision link.
    fn next_with_owner_and_state(
        &self,
        owner: TextUploadOwner,
        phase: TextUploadPhase,
        attempt: u32,
        work_state: TextUploadWorkState,
    ) -> Result<Self, IndexWorkModelError> {
        let revision = self
            .revision
            .checked_next()
            .map_err(|_| IndexWorkModelError::TextIntentRevisionExhausted)?;
        Self::try_new(
            self.intent_id,
            revision,
            self.index_id,
            self.identity.clone(),
            self.generation,
            self.partition.clone(),
            self.blob,
            self.publication_permit_id,
            owner,
            self.attachment,
            phase,
            attempt,
            work_state,
        )
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
    pub(crate) source_intent_id: TextUploadIntentId,
}

/// Scoped blob candidate owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlobGcCandidateOwner {
    GenerationCleanup(IndexOperationId),
    UploadIntent(TextUploadIntentId),
}

/// Scoped candidate value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlobGcCandidateValue {
    pub(crate) owner: BlobGcCandidateOwner,
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) blob: BlobRef,
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

/// Exact proof that an active graph mutation committed its text attachment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ActiveMutationCommitProofValue {
    pub(crate) intent_id: TextUploadIntentId,
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) partition: TextPartition,
    pub(crate) writer_epoch: WriterEpoch,
    pub(crate) mutation_id: MutationId,
    pub(crate) active_record_revision: IndexRevision,
    pub(crate) logical_version: TextLogicalVersion,
    pub(crate) destination: TextManifestSplitLocation,
    pub(crate) split: SplitRef,
}

/// Value cross-check for one globally discoverable blob reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BlobReachabilityReferenceValue {
    pub(crate) blob: BlobRef,
    pub(crate) owner_kind: BlobReferenceOwnerKind,
    pub(crate) scope: DataScope,
    pub(crate) owner_logical_key: Bytes,
    pub(crate) owner_slot: u32,
}

impl BlobReachabilityReferenceValue {
    /// Validates that the redundant value names the same typed owner lane as its global key.
    pub(crate) fn try_new(
        blob: BlobRef,
        owner_kind: BlobReferenceOwnerKind,
        scope: DataScope,
        owner_logical_key: Bytes,
        owner_slot: u32,
    ) -> Result<Self, IndexWorkModelError> {
        BlobReferenceGlobalKey::try_new(
            blob.hash,
            owner_kind,
            scope,
            owner_logical_key.clone(),
            owner_slot,
        )
        .map_err(|_| IndexWorkModelError::InvalidDestinationOwnerKey)?;
        Ok(Self {
            blob,
            owner_kind,
            scope,
            owner_logical_key,
            owner_slot,
        })
    }
}

/// Non-zero GC scan attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct GcScanAttempt(NonZeroU64);

impl GcScanAttempt {
    pub(crate) fn new(value: u64) -> Result<Self, IndexWorkModelError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(IndexWorkModelError::ZeroGcAttempt)
    }

    pub(crate) const fn get(self) -> u64 {
        self.0.get()
    }

    /// Advances a recovery attempt without admitting zero or wraparound.
    pub(crate) fn checked_next(self) -> Result<Self, IndexWorkModelError> {
        self.get()
            .checked_add(1)
            .ok_or(IndexWorkModelError::GcScanAttemptExhausted)
            .and_then(Self::new)
    }
}

/// Persisted GC root ownership route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlobGcRunOwner {
    GenerationCleanup {
        scope: DataScope,
        operation_id: IndexOperationId,
        index_id: IndexId,
        generation: IndexGenerationId,
    },
    UploadReclaim {
        scope: DataScope,
        intent_id: TextUploadIntentId,
        index_id: IndexId,
        generation: IndexGenerationId,
    },
}

/// Delete-phase stale-mark cleanup proof.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum StaleMarkCleanup {
    Pending { mark_cursor: Option<IndexCursor> },
    Complete,
}

/// GC phase owns its exact progress payload.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum BlobGcPhase {
    AwaitDeleteFences {
        member_cursor: Option<IndexCursor>,
    },
    FencesClosed,
    FirstPass {
        writer_epoch: WriterEpoch,
        first_attempt: GcScanAttempt,
        reference_cursor: Option<IndexCursor>,
    },
    SecondPass {
        completed_first_attempt: GcScanAttempt,
        writer_epoch: WriterEpoch,
        second_attempt: GcScanAttempt,
        reference_cursor: Option<IndexCursor>,
    },
    Delete {
        completed_first_attempt: GcScanAttempt,
        completed_second_attempt: GcScanAttempt,
        member_cursor: Option<IndexCursor>,
        stale_mark_cleanup: StaleMarkCleanup,
    },
}

/// Global GC run-root value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BlobGcRunRootValue {
    pub(crate) run_id: BlobGcRunId,
    pub(crate) owner: BlobGcRunOwner,
    pub(crate) revision: BlobGcRunRevision,
    pub(crate) attempt: u32,
    pub(crate) not_before_unix_millis: Option<u64>,
    pub(crate) phase: BlobGcPhase,
    pub(crate) candidate_count: NonZeroU32,
}

impl BlobGcRunRootValue {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        run_id: BlobGcRunId,
        owner: BlobGcRunOwner,
        revision: BlobGcRunRevision,
        attempt: u32,
        not_before_unix_millis: Option<u64>,
        phase: BlobGcPhase,
        candidate_count: u32,
    ) -> Result<Self, IndexWorkModelError> {
        Ok(Self {
            run_id,
            owner,
            revision,
            attempt,
            not_before_unix_millis,
            phase,
            candidate_count: NonZeroU32::new(candidate_count)
                .ok_or(IndexWorkModelError::ZeroCandidateCount)?,
        })
    }
}

/// One immutable reachability observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BlobGcReachabilityMarkValue {
    pub(crate) run_id: BlobGcRunId,
    pub(crate) first_pass: bool,
    pub(crate) scan_attempt: GcScanAttempt,
    pub(crate) blob_hash: BlobHash,
    pub(crate) referenced: bool,
}

/// Final safe disposition of one blob candidate.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlobGcDisposition {
    DeletedOrAbsent = 0x01,
    ReferencedPreserved = 0x02,
}

/// GC member handoff prevents recovery from repeating object deletion.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum BlobGcMemberState {
    PendingDisposition { owner_cursor: Option<IndexCursor> },
    CleanupCommitted(BlobGcDisposition),
}

/// One immutable run member and its terminal handoff state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BlobGcCandidateMemberValue {
    pub(crate) run_id: BlobGcRunId,
    pub(crate) blob: BlobRef,
    pub(crate) state: BlobGcMemberState,
}

/// Closed kind-0x0B value family.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum BlobGcEntryValue {
    RunRoot(BlobGcRunRootValue),
    ReachabilityMark(BlobGcReachabilityMarkValue),
    CandidateMember(BlobGcCandidateMemberValue),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_v2::ClaimSequence;

    fn text_identity() -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            crate::index_v2::IndexComponent::try_new("label", "Doc").unwrap(),
            crate::index_v2::IndexComponent::try_new("property", "body").unwrap(),
        )
    }

    #[test]
    fn partition_fingerprint_hashes_the_exact_canonical_adt_bytes() {
        let unpartitioned = TextPartition::Unpartitioned;
        assert_eq!(unpartitioned.canonical_bytes().as_ref(), &[0x01]);
        assert_eq!(
            unpartitioned.fingerprint(),
            PartitionFingerprint::new(Sha256::digest([0x01]).into())
        );

        let tenant = TextPartition::try_tenant_value(Bytes::from_static(b"acme")).unwrap();
        assert_eq!(
            tenant.canonical_bytes().as_ref(),
            b"\x02\x00\x00\x00\x04acme"
        );
    }

    #[test]
    fn vector_mapping_partition_excludes_unpartitioned_state() {
        assert_eq!(
            VectorTenantPartition::try_from_partition(TextPartition::Unpartitioned),
            Err(IndexWorkModelError::UnpartitionedVectorMapping)
        );
        let partition = VectorTenantPartition::try_new(Bytes::from_static(b"acme")).unwrap();
        assert!(matches!(
            partition.as_partition(),
            TextPartition::TenantValue(value) if value.as_ref() == b"acme"
        ));
        assert_eq!(
            partition.fingerprint(),
            partition.as_partition().fingerprint()
        );
    }

    #[test]
    fn split_reference_rejects_truncation_and_size_disagreement() {
        let blob = BlobRef::new([1; 32], 100);
        assert!(SplitRef::try_new(blob, 80, 20, 10, 100).is_ok());
        assert!(SplitRef::try_new(blob, 81, 20, 10, 100).is_err());
        assert!(SplitRef::try_new(blob, 80, 20, 10, 99).is_err());
        assert!(SplitRef::try_new(BlobRef::new([2; 32], 0), 0, 0, 0, 0).is_err());
    }

    #[test]
    fn manifest_roots_and_pages_encode_only_contiguous_non_empty_page_sets() {
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let partition = TextPartition::Unpartitioned;
        assert!(matches!(
            TextManifestRootValue::try_new(
                index_id,
                generation,
                partition.clone(),
                TextManifestRevision::initial(),
                0,
                1,
            ),
            Err(IndexWorkModelError::InvalidManifestRootCounts { .. })
        ));
        assert!(matches!(
            TextManifestPageValue::try_new(index_id, generation, partition.clone(), 0, Vec::new(),),
            Err(IndexWorkModelError::EmptyManifestPage)
        ));

        let empty = TextManifestRootValue::empty(index_id, generation, partition.clone());
        let first = empty.append_page(0, NonZeroU32::MIN).unwrap();
        assert_eq!(first.page_count(), 1);
        assert_eq!(first.split_count(), 1);
        assert_eq!(first.revision().get(), 2);
        assert!(matches!(
            first.append_page(2, NonZeroU32::MIN),
            Err(IndexWorkModelError::NonContiguousManifestPage {
                expected: 1,
                actual: 2,
            })
        ));

        let exhausted_pages = TextManifestRootValue::try_new(
            index_id,
            generation,
            partition.clone(),
            TextManifestRevision::initial(),
            u32::MAX,
            u64::from(u32::MAX),
        )
        .unwrap();
        assert!(matches!(
            exhausted_pages.append_page(u32::MAX, NonZeroU32::MIN),
            Err(IndexWorkModelError::ManifestPageCountExhausted)
        ));
        let exhausted_revision = TextManifestRootValue::try_new(
            index_id,
            generation,
            partition,
            TextManifestRevision::new(u64::MAX).unwrap(),
            1,
            1,
        )
        .unwrap();
        assert!(matches!(
            exhausted_revision.append_page(1, NonZeroU32::MIN),
            Err(IndexWorkModelError::ManifestRevisionExhausted)
        ));
    }

    #[test]
    fn gc_attempts_and_candidate_counts_are_non_zero_types() {
        assert!(GcScanAttempt::new(0).is_err());
        assert!(GcScanAttempt::new(1).is_ok());
        assert_eq!(
            GcScanAttempt::new(1).unwrap().checked_next().unwrap().get(),
            2
        );
        assert!(matches!(
            GcScanAttempt::new(u64::MAX).unwrap().checked_next(),
            Err(IndexWorkModelError::GcScanAttemptExhausted)
        ));
        assert!(BlobGcRunRootValue::try_new(
            BlobGcRunId::from_bytes([1; 16]).unwrap(),
            BlobGcRunOwner::GenerationCleanup {
                scope: DataScope::LegacyUnscoped,
                operation_id: IndexOperationId::from_bytes([2; 16]).unwrap(),
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
            },
            BlobGcRunRevision::initial(),
            0,
            None,
            BlobGcPhase::FencesClosed,
            0,
        )
        .is_err());
    }

    #[test]
    fn upload_intent_rejects_cross_family_attachment_blob_and_proof_mismatches() {
        let intent_id = TextUploadIntentId::from_bytes([1; 16]).unwrap();
        let operation_id = IndexOperationId::from_bytes([2; 16]).unwrap();
        let split = SplitRef::try_new(BlobRef::new([3; 32], 100), 80, 20, 10, 100).unwrap();
        let owner = TextUploadOwner::Build {
            operation_id,
            expected_operation_revision: IndexOperationRevision::initial(),
        };
        let build = |identity, blob, attachment, phase| {
            TextUploadIntentValue::try_new(
                intent_id,
                TextIntentRevision::initial(),
                IndexId::initial(),
                identity,
                IndexGenerationId::initial(),
                TextPartition::Unpartitioned,
                blob,
                BlobPublicationPermitId::from_bytes([4; 16]).unwrap(),
                owner,
                attachment,
                phase,
                0,
                TextUploadWorkState::Queued {
                    not_before_unix_millis: None,
                },
            )
        };
        let secondary_identity = IndexIdentity::new(
            IndexIdentityFamily::SecondaryEquality,
            IndexElementKind::Node,
            crate::index_v2::IndexComponent::try_new("label", "Doc").unwrap(),
            crate::index_v2::IndexComponent::try_new("property", "body").unwrap(),
        );
        assert!(build(
            secondary_identity,
            split.blob(),
            TextUploadAttachment::ManifestSplit(split),
            TextUploadPhase::Prepared,
        )
        .is_err());
        assert!(build(
            text_identity(),
            BlobRef::new([5; 32], 100),
            TextUploadAttachment::ManifestSplit(split),
            TextUploadPhase::Prepared,
        )
        .is_err());

        let artifact_key = IndexV2Key::TextBuildArtifact(
            crate::encoding::v1::keys::index_v2::TextBuildArtifactKey {
                root: crate::encoding::v1::keys::index_v2::TextManifestRootKey {
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                ordinal: 0,
            },
        )
        .to_bytes();
        let mismatched_authorization = UploadDestinationAuthorization::try_new(
            BlobReferenceOwnerKind::BuildArtifact,
            artifact_key,
            0,
            None,
        )
        .unwrap();
        assert!(build(
            text_identity(),
            split.blob(),
            TextUploadAttachment::ManifestSplit(split),
            TextUploadPhase::ReferenceCommitted(mismatched_authorization),
        )
        .is_err());

        let active = TextUploadOwner::ActiveMutation {
            writer_epoch: WriterEpoch::from_bytes([6; 16]).unwrap(),
            mutation_id: MutationId::from_bytes([7; 16]).unwrap(),
            active_record_revision: IndexRevision::initial(),
        };
        assert!(TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::initial(),
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([4; 16]).unwrap(),
            active,
            TextUploadAttachment::BuildArtifact {
                artifact_ordinal: 0,
                split,
            },
            TextUploadPhase::Prepared,
            0,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .is_err());
        assert!(TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::initial(),
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([4; 16]).unwrap(),
            owner,
            TextUploadAttachment::ManifestSplit(split),
            TextUploadPhase::Prepared,
            0,
            TextUploadWorkState::Blocked(IndexOperationBlocker::InvariantViolation),
        )
        .is_err());
        assert!(TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::initial(),
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([4; 16]).unwrap(),
            active,
            TextUploadAttachment::ManifestSplit(split),
            TextUploadPhase::Prepared,
            0,
            TextUploadWorkState::Blocked(IndexOperationBlocker::BlobPublicationMismatch {
                intent_id,
            }),
        )
        .is_err());
    }

    #[test]
    fn upload_claim_and_retry_transitions_are_closed_and_checked() {
        let intent_id = TextUploadIntentId::from_bytes([21; 16]).unwrap();
        let split = SplitRef::try_new(BlobRef::new([22; 32], 100), 80, 20, 10, 100).unwrap();
        let queued = TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::initial(),
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([23; 16]).unwrap(),
            TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([24; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            TextUploadAttachment::ManifestSplit(split),
            TextUploadPhase::Prepared,
            u32::MAX,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        assert!(matches!(
            queued.transient_failure(10),
            Err(IndexWorkModelError::InvalidUploadState)
        ));

        let cleanup_uploaded = queued
            .cleanup_transition(TextUploadCleanupTransition::PublicationSucceeded)
            .unwrap();
        assert!(matches!(cleanup_uploaded.phase, TextUploadPhase::Uploaded));
        let cleanup_non_publication = queued
            .cleanup_transition(TextUploadCleanupTransition::NonPublicationProven)
            .unwrap();
        assert!(matches!(
            cleanup_non_publication.phase,
            TextUploadPhase::NonPublicationProven
        ));
        let cleanup_reclaimable = cleanup_uploaded
            .cleanup_transition(TextUploadCleanupTransition::Reclaimable)
            .unwrap();
        let cleanup_run = BlobGcRunId::from_bytes([28; 16]).unwrap();
        let cleanup_assigned = cleanup_reclaimable
            .cleanup_transition(TextUploadCleanupTransition::AssignReclaim(cleanup_run))
            .unwrap();
        assert!(matches!(
            cleanup_assigned.phase,
            TextUploadPhase::Reclaimable(ReclaimAssignment::Assigned(run_id))
                if run_id == cleanup_run
        ));
        assert!(matches!(
            cleanup_non_publication.cleanup_transition(TextUploadCleanupTransition::Reclaimable),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        assert!(matches!(
            cleanup_assigned
                .cleanup_transition(TextUploadCleanupTransition::AssignReclaim(cleanup_run,)),
            Err(IndexWorkModelError::InvalidUploadState)
        ));

        let mut active = queued.clone();
        active.owner = TextUploadOwner::ActiveMutation {
            writer_epoch: WriterEpoch::from_bytes([26; 16]).unwrap(),
            mutation_id: MutationId::from_bytes([27; 16]).unwrap(),
            active_record_revision: IndexRevision::initial(),
        };
        let request_uploaded = active.active_request_publication_succeeded().unwrap();
        assert_eq!(request_uploaded.revision.get(), 2);
        assert!(matches!(request_uploaded.phase, TextUploadPhase::Uploaded));
        assert!(matches!(
            request_uploaded.work_state,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert!(matches!(
            queued.active_request_publication_succeeded(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let mut delayed_active = active.clone();
        delayed_active.work_state = TextUploadWorkState::Queued {
            not_before_unix_millis: Some(1),
        };
        assert!(matches!(
            delayed_active.active_request_publication_succeeded(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let manifest_key = IndexV2Key::TextManifestPage(
            crate::encoding::v1::keys::index_v2::TextManifestPageKey {
                root: crate::encoding::v1::keys::index_v2::TextManifestRootKey {
                    index_id: request_uploaded.index_id,
                    generation: request_uploaded.generation,
                    partition: request_uploaded.partition.fingerprint(),
                },
                page: 0,
            },
        )
        .to_bytes();
        let proof_key = IndexV2Key::ActiveMutationCommitProof(
            crate::encoding::v1::keys::index_v2::TextIntentOwnedKey {
                index_id: request_uploaded.index_id,
                generation: request_uploaded.generation,
                intent_id: request_uploaded.intent_id,
            },
        )
        .to_bytes();
        let authorization = UploadDestinationAuthorization::try_new(
            BlobReferenceOwnerKind::ManifestPageSplit,
            manifest_key,
            0,
            Some(proof_key),
        )
        .unwrap();
        let request_referenced = request_uploaded
            .active_request_reference_committed(authorization.clone())
            .unwrap();
        assert_eq!(request_referenced.revision.get(), 3);
        assert!(matches!(
            request_referenced.phase,
            TextUploadPhase::ReferenceCommitted(_)
        ));
        let mut missing_proof = authorization;
        missing_proof.proof_logical_key = None;
        assert!(matches!(
            request_uploaded.active_request_reference_committed(missing_proof),
            Err(IndexWorkModelError::InvalidUploadState)
        ));

        let claimed = queued
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([25; 16]).unwrap(),
                sequence: ClaimSequence::new(1).unwrap(),
            })
            .unwrap();
        assert!(matches!(
            claimed.cleanup_transition(TextUploadCleanupTransition::PublicationSucceeded),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let mut claimed_active = claimed.clone();
        claimed_active.owner = active.owner;
        assert!(matches!(
            claimed_active.active_request_publication_succeeded(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        assert_eq!(claimed.revision.get(), 2);
        assert_eq!(claimed.attempt, u32::MAX);
        let uploaded = claimed.publication_succeeded().unwrap();
        assert_eq!(uploaded.revision.get(), 3);
        assert!(matches!(uploaded.phase, TextUploadPhase::Uploaded));
        assert!(matches!(
            uploaded.work_state,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert!(matches!(
            uploaded.publication_succeeded(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        assert!(matches!(
            queued.publication_succeeded(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let non_publication = claimed.non_publication_proven().unwrap();
        assert_eq!(non_publication.revision.get(), 3);
        assert!(matches!(
            non_publication.phase,
            TextUploadPhase::NonPublicationProven
        ));
        assert!(matches!(
            queued.non_publication_proven(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        assert!(matches!(
            non_publication.non_publication_proven(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let reclaimable = claimed.become_reclaimable().unwrap();
        assert_eq!(reclaimable.revision.get(), 3);
        assert!(matches!(
            reclaimable.phase,
            TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned)
        ));
        assert!(matches!(
            queued.become_reclaimable(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        assert!(matches!(
            reclaimable.become_reclaimable(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let reclaim_run = BlobGcRunId::from_bytes([28; 16]).unwrap();
        assert!(matches!(
            reclaimable.assign_reclaim_root(reclaim_run),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let reclaim_claimed = reclaimable
            .clone()
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([25; 16]).unwrap(),
                sequence: ClaimSequence::new(4).unwrap(),
            })
            .unwrap();
        let reclaim_assigned = reclaim_claimed.assign_reclaim_root(reclaim_run).unwrap();
        assert!(matches!(
            reclaim_assigned.phase,
            TextUploadPhase::Reclaimable(ReclaimAssignment::Assigned(run_id))
                if run_id == reclaim_run
        ));
        assert!(matches!(
            reclaim_assigned.work_state,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert!(matches!(
            reclaim_claimed.complete_reclaim_owner_normalization(reclaim_run),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let reclaim_assigned_claimed = reclaim_assigned
            .clone()
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([25; 16]).unwrap(),
                sequence: ClaimSequence::new(5).unwrap(),
            })
            .unwrap();
        assert!(matches!(
            reclaim_assigned_claimed
                .complete_reclaim_owner_normalization(BlobGcRunId::from_bytes([29; 16]).unwrap()),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let reclaim_normalized = reclaim_assigned_claimed
            .complete_reclaim_owner_normalization(reclaim_run)
            .unwrap();
        assert!(matches!(
            reclaim_normalized.phase,
            TextUploadPhase::Reclaimable(ReclaimAssignment::Assigned(run_id))
                if run_id == reclaim_run
        ));
        assert!(matches!(
            reclaim_normalized.work_state,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        ));
        let blocked_operation_revision = IndexOperationRevision::new(2).unwrap();
        let blocked = claimed
            .block_for_blob_mismatch(blocked_operation_revision)
            .unwrap();
        assert_eq!(blocked.revision.get(), 3);
        assert!(matches!(
            blocked.owner,
            TextUploadOwner::Build {
                expected_operation_revision,
                ..
            } if expected_operation_revision == blocked_operation_revision
        ));
        assert!(matches!(
            blocked.work_state,
            TextUploadWorkState::Blocked(
                IndexOperationBlocker::BlobPublicationMismatch {
                    intent_id: blocked_intent_id,
                }
            ) if blocked_intent_id == intent_id
        ));
        assert!(matches!(
            queued.block_for_blob_mismatch(blocked_operation_revision),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let mut active_claimed = claimed.clone();
        active_claimed.owner = TextUploadOwner::ActiveMutation {
            writer_epoch: WriterEpoch::from_bytes([26; 16]).unwrap(),
            mutation_id: MutationId::from_bytes([27; 16]).unwrap(),
            active_record_revision: IndexRevision::initial(),
        };
        assert!(matches!(
            active_claimed.block_for_blob_mismatch(blocked_operation_revision),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        assert!(matches!(
            active_claimed.active_graph_aborted(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let active_uploaded_claimed = request_uploaded
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([25; 16]).unwrap(),
                sequence: ClaimSequence::new(2).unwrap(),
            })
            .unwrap();
        let active_reclaimable = active_uploaded_claimed.active_graph_aborted().unwrap();
        assert_eq!(active_reclaimable.revision.get(), 4);
        assert!(matches!(
            active_reclaimable.phase,
            TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned)
        ));
        assert!(matches!(
            active_reclaimable.work_state,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        ));
        let build_uploaded_claimed = uploaded
            .clone()
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([25; 16]).unwrap(),
                sequence: ClaimSequence::new(3).unwrap(),
            })
            .unwrap();
        assert!(matches!(
            build_uploaded_claimed.active_graph_aborted(),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let retried_operation_revision = IndexOperationRevision::new(3).unwrap();
        let retried = blocked
            .retry_blob_mismatch(retried_operation_revision)
            .unwrap();
        assert!(matches!(
            retried.owner,
            TextUploadOwner::Build {
                expected_operation_revision,
                ..
            } if expected_operation_revision == retried_operation_revision
        ));
        assert!(matches!(
            retried.work_state,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None
            }
        ));
        assert!(matches!(
            retried.retry_blob_mismatch(retried_operation_revision),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let mut active_blocked = blocked.clone();
        active_blocked.owner = TextUploadOwner::ActiveMutation {
            writer_epoch: WriterEpoch::from_bytes([26; 16]).unwrap(),
            mutation_id: MutationId::from_bytes([27; 16]).unwrap(),
            active_record_revision: IndexRevision::initial(),
        };
        assert!(matches!(
            active_blocked.retry_blob_mismatch(retried_operation_revision),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let uploaded_claimed = uploaded
            .claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([25; 16]).unwrap(),
                sequence: ClaimSequence::new(2).unwrap(),
            })
            .unwrap();
        let manifest_page_key = IndexV2Key::TextManifestPage(
            crate::encoding::v1::keys::index_v2::TextManifestPageKey {
                root: crate::encoding::v1::keys::index_v2::TextManifestRootKey {
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    partition: TextPartition::Unpartitioned.fingerprint(),
                },
                page: 0,
            },
        )
        .to_bytes();
        let authorization = UploadDestinationAuthorization::try_new(
            BlobReferenceOwnerKind::ManifestPageSplit,
            manifest_page_key,
            0,
            None,
        )
        .unwrap();
        let referenced = uploaded_claimed
            .reference_committed(authorization.clone())
            .unwrap();
        assert_eq!(referenced.revision.get(), 5);
        assert!(matches!(
            referenced.phase,
            TextUploadPhase::ReferenceCommitted(_)
        ));
        assert!(matches!(
            uploaded.reference_committed(authorization),
            Err(IndexWorkModelError::InvalidUploadState)
        ));
        let delayed = claimed.transient_failure(10).unwrap();
        assert_eq!(delayed.revision.get(), 3);
        assert!(matches!(
            delayed.work_state,
            TextUploadWorkState::Queued {
                not_before_unix_millis: Some(10)
            }
        ));

        assert!(matches!(
            blocked.claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([25; 16]).unwrap(),
                sequence: ClaimSequence::new(2).unwrap(),
            }),
            Err(IndexWorkModelError::InvalidUploadState)
        ));

        let exhausted = TextUploadIntentValue::try_new(
            intent_id,
            TextIntentRevision::new(u64::MAX).unwrap(),
            IndexId::initial(),
            text_identity(),
            IndexGenerationId::initial(),
            TextPartition::Unpartitioned,
            split.blob(),
            BlobPublicationPermitId::from_bytes([23; 16]).unwrap(),
            TextUploadOwner::Build {
                operation_id: IndexOperationId::from_bytes([24; 16]).unwrap(),
                expected_operation_revision: IndexOperationRevision::initial(),
            },
            TextUploadAttachment::ManifestSplit(split),
            TextUploadPhase::Prepared,
            0,
            TextUploadWorkState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        assert!(matches!(
            exhausted.claim(OperationClaim {
                writer_epoch: WriterEpoch::from_bytes([25; 16]).unwrap(),
                sequence: ClaimSequence::new(3).unwrap(),
            }),
            Err(IndexWorkModelError::TextIntentRevisionExhausted)
        ));
    }
}
