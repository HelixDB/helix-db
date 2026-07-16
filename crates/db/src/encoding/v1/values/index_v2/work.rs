//! Codecs for scoped physical-work, upload, text, reachability, and GC values.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::index_v2::work::*;
use crate::index_v2::{BlobGcRunRevision, IndexEntityId, INDEX_CURSOR_MAX_LEN};

use super::codec::*;

/// Closed dispatch value for record kinds `0x03..=0x0F`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum IndexV2WorkValue {
    CoalescedBuildDelta(CoalescedBuildDeltaValue),
    AppliedEntityState(AppliedEntityStateValue),
    SecondaryEntry(SecondaryEntryValue),
    TextManifestRoot(TextManifestRootValue),
    TextManifestPage(TextManifestPageValue),
    TextUploadIntent(Box<TextUploadIntentValue>),
    TextBuildArtifact(TextBuildArtifactValue),
    BlobGcCandidate(BlobGcCandidateValue),
    BlobGcEntry(BlobGcEntryValue),
    TextEntityState(TextEntityStateValue),
    ActiveMutationCommitProof(ActiveMutationCommitProofValue),
    BlobReachabilityReference(BlobReachabilityReferenceValue),
    VectorPartitionMapping(VectorPartitionMappingValue),
}

impl IndexV2WorkValue {
    const fn record_kind(&self) -> u8 {
        match self {
            Self::CoalescedBuildDelta(_) => 0x03,
            Self::AppliedEntityState(_) => 0x04,
            Self::SecondaryEntry(_) => 0x05,
            Self::TextManifestRoot(_) => 0x06,
            Self::TextManifestPage(_) => 0x07,
            Self::TextUploadIntent(_) => 0x08,
            Self::TextBuildArtifact(_) => 0x09,
            Self::BlobGcCandidate(_) => 0x0A,
            Self::BlobGcEntry(_) => 0x0B,
            Self::TextEntityState(_) => 0x0C,
            Self::ActiveMutationCommitProof(_) => 0x0D,
            Self::BlobReachabilityReference(_) => 0x0E,
            Self::VectorPartitionMapping(_) => 0x0F,
        }
    }
}

pub(crate) fn encode_work_value(value: &IndexV2WorkValue) -> Bytes {
    let mut encoder = ValueEncoder::with_header(value.record_kind());
    match value {
        IndexV2WorkValue::CoalescedBuildDelta(value) => {
            put_index_id(&mut encoder, value.index_id);
            put_generation(&mut encoder, value.generation);
            put_element_kind(&mut encoder, value.entity_kind);
            encoder.put_u64(value.entity_id.get());
        }
        IndexV2WorkValue::AppliedEntityState(value) => {
            put_index_id(&mut encoder, value.index_id);
            put_generation(&mut encoder, value.generation);
            put_element_kind(&mut encoder, value.entity_kind);
            encoder.put_u64(value.entity_id.get());
            match &value.state {
                AppliedFamilyState::Secondary(state) => {
                    encoder.put_u8(0x01);
                    put_option(&mut encoder, state.as_ref(), put_secondary_value);
                }
                AppliedFamilyState::Vector(state) => {
                    encoder.put_u8(0x02);
                    put_option(&mut encoder, state.as_ref(), put_partition);
                }
                AppliedFamilyState::Text(state) => {
                    encoder.put_u8(0x03);
                    put_option(&mut encoder, state.as_ref(), |encoder, state| {
                        put_partition(encoder, &state.0);
                        encoder.put_u64(state.1.get());
                    });
                }
            }
        }
        IndexV2WorkValue::SecondaryEntry(value) => {
            put_index_id(&mut encoder, value.index_id);
            put_generation(&mut encoder, value.generation);
            put_secondary_lane(&mut encoder, value.lane);
            encoder.put_u64(value.entity_id.get());
        }
        IndexV2WorkValue::TextManifestRoot(value) => {
            put_index_id(&mut encoder, value.index_id());
            put_generation(&mut encoder, value.generation());
            put_partition(&mut encoder, value.partition());
            encoder.put_u64(value.revision().get());
            encoder.put_u32(value.page_count());
            encoder.put_u64(value.split_count());
        }
        IndexV2WorkValue::TextManifestPage(value) => {
            put_index_id(&mut encoder, value.index_id());
            put_generation(&mut encoder, value.generation());
            put_partition(&mut encoder, value.partition());
            encoder.put_u32(value.page());
            encoder.put_u32(
                u32::try_from(value.entries().len()).expect("bounded manifest page fits u32"),
            );
            for split in value.entries() {
                put_split_ref(&mut encoder, *split);
            }
        }
        IndexV2WorkValue::TextUploadIntent(value) => put_upload(&mut encoder, value),
        IndexV2WorkValue::TextBuildArtifact(value) => {
            put_index_id(&mut encoder, value.index_id);
            put_generation(&mut encoder, value.generation);
            put_partition(&mut encoder, &value.partition);
            encoder.put_u32(value.artifact_ordinal);
            put_split_ref(&mut encoder, value.split);
            put_intent_id(&mut encoder, value.source_intent_id);
        }
        IndexV2WorkValue::BlobGcCandidate(value) => {
            match value.owner {
                BlobGcCandidateOwner::GenerationCleanup(operation_id) => {
                    encoder.put_u8(0x01);
                    put_operation_id(&mut encoder, operation_id);
                }
                BlobGcCandidateOwner::UploadIntent(intent_id) => {
                    encoder.put_u8(0x02);
                    put_intent_id(&mut encoder, intent_id);
                }
            }
            put_index_id(&mut encoder, value.index_id);
            put_generation(&mut encoder, value.generation);
            put_blob_ref(&mut encoder, value.blob);
        }
        IndexV2WorkValue::BlobGcEntry(value) => put_gc_entry(&mut encoder, value),
        IndexV2WorkValue::TextEntityState(value) => {
            put_index_id(&mut encoder, value.index_id);
            put_generation(&mut encoder, value.generation);
            put_partition(&mut encoder, &value.partition);
            put_element_kind(&mut encoder, value.entity_kind);
            encoder.put_u64(value.entity_id.get());
            encoder.put_u64(value.logical_version.get());
            encoder.put_bool(value.live);
        }
        IndexV2WorkValue::ActiveMutationCommitProof(value) => {
            put_intent_id(&mut encoder, value.intent_id);
            put_index_id(&mut encoder, value.index_id);
            put_generation(&mut encoder, value.generation);
            put_partition(&mut encoder, &value.partition);
            put_writer_epoch(&mut encoder, value.writer_epoch);
            put_mutation_id(&mut encoder, value.mutation_id);
            put_revision(&mut encoder, value.active_record_revision);
            encoder.put_u64(value.logical_version.get());
            encoder.put_u32(value.destination.page());
            encoder.put_u32(value.destination.slot());
            put_split_ref(&mut encoder, value.split);
        }
        IndexV2WorkValue::BlobReachabilityReference(value) => {
            put_blob_ref(&mut encoder, value.blob);
            put_blob_reference_owner(&mut encoder, value.owner_kind);
            put_scope(&mut encoder, value.scope);
            encoder.put_bytes(&value.owner_logical_key);
            encoder.put_u32(value.owner_slot);
        }
        IndexV2WorkValue::VectorPartitionMapping(value) => {
            put_index_id(&mut encoder, value.index_id);
            put_generation(&mut encoder, value.generation);
            put_partition(&mut encoder, value.partition.as_partition());
            encoder.put_u64(value.physical_index_id.get());
        }
    }
    encoder.finish()
}

pub(crate) fn decode_work_value(value: &[u8]) -> Result<IndexV2WorkValue, EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    let decoded = match decoder.kind() {
        0x03 => IndexV2WorkValue::CoalescedBuildDelta(CoalescedBuildDeltaValue {
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            entity_kind: take_element_kind(&mut decoder)?,
            entity_id: IndexEntityId::new(decoder.take_u64()?),
        }),
        0x04 => IndexV2WorkValue::AppliedEntityState(take_applied_state(&mut decoder)?),
        0x05 => IndexV2WorkValue::SecondaryEntry(SecondaryEntryValue {
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            lane: take_secondary_lane(&mut decoder)?,
            entity_id: IndexEntityId::new(decoder.take_u64()?),
        }),
        0x06 => IndexV2WorkValue::TextManifestRoot(
            TextManifestRootValue::try_new(
                take_index_id(&mut decoder)?,
                take_generation(&mut decoder)?,
                take_partition(&mut decoder)?,
                take_manifest_revision(&mut decoder)?,
                decoder.take_u32()?,
                decoder.take_u64()?,
            )
            .map_err(work_model_error)?,
        ),
        0x07 => IndexV2WorkValue::TextManifestPage(take_manifest_page(&mut decoder)?),
        0x08 => IndexV2WorkValue::TextUploadIntent(Box::new(take_upload(&mut decoder)?)),
        0x09 => IndexV2WorkValue::TextBuildArtifact(TextBuildArtifactValue {
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            partition: take_partition(&mut decoder)?,
            artifact_ordinal: decoder.take_u32()?,
            split: take_split_ref(&mut decoder)?,
            source_intent_id: take_intent_id(&mut decoder)?,
        }),
        0x0A => IndexV2WorkValue::BlobGcCandidate(take_candidate(&mut decoder)?),
        0x0B => IndexV2WorkValue::BlobGcEntry(take_gc_entry(&mut decoder)?),
        0x0C => IndexV2WorkValue::TextEntityState(TextEntityStateValue {
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            partition: take_partition(&mut decoder)?,
            entity_kind: take_element_kind(&mut decoder)?,
            entity_id: IndexEntityId::new(decoder.take_u64()?),
            logical_version: take_logical_version(&mut decoder)?,
            live: decoder.take_bool()?,
        }),
        0x0D => IndexV2WorkValue::ActiveMutationCommitProof(ActiveMutationCommitProofValue {
            intent_id: take_intent_id(&mut decoder)?,
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            partition: take_partition(&mut decoder)?,
            writer_epoch: take_writer_epoch(&mut decoder)?,
            mutation_id: take_mutation_id(&mut decoder)?,
            active_record_revision: take_revision(&mut decoder)?,
            logical_version: take_logical_version(&mut decoder)?,
            destination: TextManifestSplitLocation::try_new(
                decoder.take_u32()?,
                decoder.take_u32()?,
            )
            .map_err(work_model_error)?,
            split: take_split_ref(&mut decoder)?,
        }),
        0x0E => {
            let blob = take_blob_ref(&mut decoder)?;
            let owner_kind = take_blob_reference_owner(&mut decoder)?;
            let scope = take_scope(&mut decoder)?;
            let owner_logical_key = decoder.take_bytes(INDEX_CURSOR_MAX_LEN)?;
            IndexV2WorkValue::BlobReachabilityReference(
                BlobReachabilityReferenceValue::try_new(
                    blob,
                    owner_kind,
                    scope,
                    owner_logical_key,
                    decoder.take_u32()?,
                )
                .map_err(work_model_error)?,
            )
        }
        0x0F => IndexV2WorkValue::VectorPartitionMapping(VectorPartitionMappingValue {
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            partition: VectorTenantPartition::try_from_partition(take_partition(&mut decoder)?)
                .map_err(work_model_error)?,
            physical_index_id: crate::index_v2::VectorPhysicalIndexId::new(decoder.take_u64()?)
                .map_err(model_error)?,
        }),
        unknown => return Err(unknown_discriminant("work value kind", unknown)),
    };
    decoder.finish()?;
    Ok(decoded)
}

fn take_applied_state(
    decoder: &mut ValueDecoder<'_>,
) -> Result<AppliedEntityStateValue, EncodingError> {
    let index_id = take_index_id(decoder)?;
    let generation = take_generation(decoder)?;
    let entity_kind = take_element_kind(decoder)?;
    let entity_id = IndexEntityId::new(decoder.take_u64()?);
    let state = match decoder.take_u8()? {
        0x01 => AppliedFamilyState::Secondary(decoder.take_option(take_secondary_value)?),
        0x02 => AppliedFamilyState::Vector(decoder.take_option(take_partition)?),
        0x03 => AppliedFamilyState::Text(decoder.take_option(|decoder| {
            Ok((take_partition(decoder)?, take_logical_version(decoder)?))
        })?),
        unknown => return Err(unknown_discriminant("applied-state family", unknown)),
    };
    Ok(AppliedEntityStateValue {
        index_id,
        generation,
        entity_kind,
        entity_id,
        state,
    })
}

fn take_manifest_page(
    decoder: &mut ValueDecoder<'_>,
) -> Result<TextManifestPageValue, EncodingError> {
    let index_id = take_index_id(decoder)?;
    let generation = take_generation(decoder)?;
    let partition = take_partition(decoder)?;
    let page = decoder.take_u32()?;
    let count = decoder.take_u32()? as usize;
    if count > MAX_COLLECTION_ITEMS {
        return Err(EncodingError::Custom(format!(
            "manifest entry count {count} exceeds maximum {MAX_COLLECTION_ITEMS}"
        )));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(take_split_ref(decoder)?);
    }
    TextManifestPageValue::try_new(index_id, generation, partition, page, entries)
        .map_err(work_model_error)
}

fn put_upload(encoder: &mut ValueEncoder, value: &TextUploadIntentValue) {
    put_intent_id(encoder, value.intent_id);
    put_intent_revision(encoder, value.revision);
    put_index_id(encoder, value.index_id);
    put_identity(encoder, &value.identity);
    put_generation(encoder, value.generation);
    put_partition(encoder, &value.partition);
    put_blob_ref(encoder, value.blob);
    put_publication_permit(encoder, value.publication_permit_id);
    match value.owner {
        TextUploadOwner::Build {
            operation_id,
            expected_operation_revision,
        } => {
            encoder.put_u8(0x01);
            put_operation_id(encoder, operation_id);
            put_operation_revision(encoder, expected_operation_revision);
        }
        TextUploadOwner::ActiveMutation {
            writer_epoch,
            mutation_id,
            active_record_revision,
        } => {
            encoder.put_u8(0x02);
            put_writer_epoch(encoder, writer_epoch);
            put_mutation_id(encoder, mutation_id);
            put_revision(encoder, active_record_revision);
        }
    }
    match value.attachment {
        TextUploadAttachment::ManifestSplit(split) => {
            encoder.put_u8(0x01);
            put_split_ref(encoder, split);
        }
        TextUploadAttachment::BuildArtifact {
            artifact_ordinal,
            split,
        } => {
            encoder.put_u8(0x02);
            encoder.put_u32(artifact_ordinal);
            put_split_ref(encoder, split);
        }
    }
    match &value.phase {
        TextUploadPhase::Prepared => encoder.put_u8(0x01),
        TextUploadPhase::Uploaded => encoder.put_u8(0x02),
        TextUploadPhase::ReferenceCommitted(authorization) => {
            encoder.put_u8(0x03);
            put_blob_reference_owner(encoder, authorization.owner_kind);
            encoder.put_bytes(&authorization.owner_logical_key);
            encoder.put_u32(authorization.owner_slot);
            put_option(
                encoder,
                authorization.proof_logical_key.as_ref(),
                |encoder, key| encoder.put_bytes(key),
            );
        }
        TextUploadPhase::Reclaimable(assignment) => {
            encoder.put_u8(0x04);
            match assignment {
                ReclaimAssignment::Unassigned => encoder.put_u8(0x01),
                ReclaimAssignment::Assigned(run_id) => {
                    encoder.put_u8(0x02);
                    put_run_id(encoder, *run_id);
                }
            }
        }
        TextUploadPhase::NonPublicationProven => encoder.put_u8(0x05),
    }
    encoder.put_u32(value.attempt);
    match &value.work_state {
        TextUploadWorkState::Queued {
            not_before_unix_millis,
        } => {
            encoder.put_u8(0x01);
            put_option(
                encoder,
                not_before_unix_millis.as_ref(),
                |encoder, value| encoder.put_u64(*value),
            );
        }
        TextUploadWorkState::Claimed(claim) => {
            encoder.put_u8(0x02);
            put_claim(encoder, *claim);
        }
        TextUploadWorkState::Blocked(blocker) => {
            encoder.put_u8(0x03);
            put_blocker(encoder, blocker);
        }
    }
}

fn take_upload(decoder: &mut ValueDecoder<'_>) -> Result<TextUploadIntentValue, EncodingError> {
    let intent_id = take_intent_id(decoder)?;
    let revision = take_intent_revision(decoder)?;
    let index_id = take_index_id(decoder)?;
    let identity = take_identity(decoder)?;
    let generation = take_generation(decoder)?;
    let partition = take_partition(decoder)?;
    let blob = take_blob_ref(decoder)?;
    let publication_permit_id = take_publication_permit(decoder)?;
    let owner = match decoder.take_u8()? {
        0x01 => TextUploadOwner::Build {
            operation_id: take_operation_id(decoder)?,
            expected_operation_revision: take_operation_revision(decoder)?,
        },
        0x02 => TextUploadOwner::ActiveMutation {
            writer_epoch: take_writer_epoch(decoder)?,
            mutation_id: take_mutation_id(decoder)?,
            active_record_revision: take_revision(decoder)?,
        },
        unknown => return Err(unknown_discriminant("upload owner", unknown)),
    };
    let attachment = match decoder.take_u8()? {
        0x01 => TextUploadAttachment::ManifestSplit(take_split_ref(decoder)?),
        0x02 => TextUploadAttachment::BuildArtifact {
            artifact_ordinal: decoder.take_u32()?,
            split: take_split_ref(decoder)?,
        },
        unknown => return Err(unknown_discriminant("upload attachment", unknown)),
    };
    let phase = match decoder.take_u8()? {
        0x01 => TextUploadPhase::Prepared,
        0x02 => TextUploadPhase::Uploaded,
        0x03 => {
            let owner_kind = take_blob_reference_owner(decoder)?;
            let owner_logical_key = decoder.take_bytes(1024 * 1024)?;
            let owner_slot = decoder.take_u32()?;
            let proof_logical_key =
                decoder.take_option(|decoder| decoder.take_bytes(1024 * 1024))?;
            TextUploadPhase::ReferenceCommitted(
                UploadDestinationAuthorization::try_new(
                    owner_kind,
                    owner_logical_key,
                    owner_slot,
                    proof_logical_key,
                )
                .map_err(work_model_error)?,
            )
        }
        0x04 => TextUploadPhase::Reclaimable(match decoder.take_u8()? {
            0x01 => ReclaimAssignment::Unassigned,
            0x02 => ReclaimAssignment::Assigned(take_run_id(decoder)?),
            unknown => return Err(unknown_discriminant("reclaim assignment", unknown)),
        }),
        0x05 => TextUploadPhase::NonPublicationProven,
        unknown => return Err(unknown_discriminant("upload phase", unknown)),
    };
    let attempt = decoder.take_u32()?;
    let work_state = match decoder.take_u8()? {
        0x01 => TextUploadWorkState::Queued {
            not_before_unix_millis: decoder.take_option(ValueDecoder::take_u64)?,
        },
        0x02 => TextUploadWorkState::Claimed(take_claim(decoder)?),
        0x03 => TextUploadWorkState::Blocked(take_blocker(decoder)?),
        unknown => return Err(unknown_discriminant("upload work state", unknown)),
    };
    TextUploadIntentValue::try_new(
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
    )
    .map_err(work_model_error)
}

fn take_candidate(decoder: &mut ValueDecoder<'_>) -> Result<BlobGcCandidateValue, EncodingError> {
    let owner = match decoder.take_u8()? {
        0x01 => BlobGcCandidateOwner::GenerationCleanup(take_operation_id(decoder)?),
        0x02 => BlobGcCandidateOwner::UploadIntent(take_intent_id(decoder)?),
        unknown => return Err(unknown_discriminant("GC candidate owner", unknown)),
    };
    Ok(BlobGcCandidateValue {
        owner,
        index_id: take_index_id(decoder)?,
        generation: take_generation(decoder)?,
        blob: take_blob_ref(decoder)?,
    })
}

fn put_gc_entry(encoder: &mut ValueEncoder, value: &BlobGcEntryValue) {
    match value {
        BlobGcEntryValue::RunRoot(root) => {
            encoder.put_u8(0x01);
            put_run_id(encoder, root.run_id);
            put_gc_owner(encoder, root.owner);
            encoder.put_u64(root.revision.get());
            encoder.put_u32(root.attempt);
            put_option(
                encoder,
                root.not_before_unix_millis.as_ref(),
                |encoder, value| encoder.put_u64(*value),
            );
            put_gc_phase(encoder, &root.phase);
            encoder.put_u32(root.candidate_count.get());
        }
        BlobGcEntryValue::ReachabilityMark(mark) => {
            encoder.put_u8(0x02);
            put_run_id(encoder, mark.run_id);
            encoder.put_u8(if mark.first_pass { 0x01 } else { 0x02 });
            encoder.put_u64(mark.scan_attempt.get());
            put_blob_hash(encoder, mark.blob_hash);
            encoder.put_bool(mark.referenced);
        }
        BlobGcEntryValue::CandidateMember(member) => {
            encoder.put_u8(0x03);
            put_run_id(encoder, member.run_id);
            put_blob_ref(encoder, member.blob);
            match &member.state {
                BlobGcMemberState::PendingDisposition { owner_cursor } => {
                    encoder.put_u8(0x01);
                    put_cursor(encoder, owner_cursor.as_ref());
                }
                BlobGcMemberState::CleanupCommitted(disposition) => {
                    encoder.put_u8(0x02);
                    encoder.put_u8(*disposition as u8);
                }
            }
        }
    }
}

fn take_gc_entry(decoder: &mut ValueDecoder<'_>) -> Result<BlobGcEntryValue, EncodingError> {
    match decoder.take_u8()? {
        0x01 => {
            let run_id = take_run_id(decoder)?;
            let owner = take_gc_owner(decoder)?;
            let revision = BlobGcRunRevision::new(decoder.take_u64()?).map_err(model_error)?;
            let attempt = decoder.take_u32()?;
            let not_before_unix_millis = decoder.take_option(ValueDecoder::take_u64)?;
            let phase = take_gc_phase(decoder)?;
            let candidate_count = decoder.take_u32()?;
            Ok(BlobGcEntryValue::RunRoot(
                BlobGcRunRootValue::try_new(
                    run_id,
                    owner,
                    revision,
                    attempt,
                    not_before_unix_millis,
                    phase,
                    candidate_count,
                )
                .map_err(work_model_error)?,
            ))
        }
        0x02 => {
            let run_id = take_run_id(decoder)?;
            let first_pass = match decoder.take_u8()? {
                0x01 => true,
                0x02 => false,
                unknown => return Err(unknown_discriminant("GC pass", unknown)),
            };
            let scan_attempt = GcScanAttempt::new(decoder.take_u64()?).map_err(work_model_error)?;
            let blob_hash = take_blob_hash(decoder)?;
            let referenced = decoder.take_bool()?;
            Ok(BlobGcEntryValue::ReachabilityMark(
                BlobGcReachabilityMarkValue {
                    run_id,
                    first_pass,
                    scan_attempt,
                    blob_hash,
                    referenced,
                },
            ))
        }
        0x03 => {
            let run_id = take_run_id(decoder)?;
            let blob = take_blob_ref(decoder)?;
            let state = match decoder.take_u8()? {
                0x01 => BlobGcMemberState::PendingDisposition {
                    owner_cursor: take_cursor(decoder)?,
                },
                0x02 => BlobGcMemberState::CleanupCommitted(match decoder.take_u8()? {
                    0x01 => BlobGcDisposition::DeletedOrAbsent,
                    0x02 => BlobGcDisposition::ReferencedPreserved,
                    unknown => return Err(unknown_discriminant("GC disposition", unknown)),
                }),
                unknown => return Err(unknown_discriminant("GC member state", unknown)),
            };
            Ok(BlobGcEntryValue::CandidateMember(
                BlobGcCandidateMemberValue {
                    run_id,
                    blob,
                    state,
                },
            ))
        }
        unknown => Err(unknown_discriminant("GC entry", unknown)),
    }
}

fn put_gc_owner(encoder: &mut ValueEncoder, owner: BlobGcRunOwner) {
    match owner {
        BlobGcRunOwner::GenerationCleanup {
            scope,
            operation_id,
            index_id,
            generation,
        } => {
            encoder.put_u8(0x01);
            put_scope(encoder, scope);
            put_operation_id(encoder, operation_id);
            put_index_id(encoder, index_id);
            put_generation(encoder, generation);
        }
        BlobGcRunOwner::UploadReclaim {
            scope,
            intent_id,
            index_id,
            generation,
        } => {
            encoder.put_u8(0x02);
            put_scope(encoder, scope);
            put_intent_id(encoder, intent_id);
            put_index_id(encoder, index_id);
            put_generation(encoder, generation);
        }
    }
}

fn take_gc_owner(decoder: &mut ValueDecoder<'_>) -> Result<BlobGcRunOwner, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(BlobGcRunOwner::GenerationCleanup {
            scope: take_scope(decoder)?,
            operation_id: take_operation_id(decoder)?,
            index_id: take_index_id(decoder)?,
            generation: take_generation(decoder)?,
        }),
        0x02 => Ok(BlobGcRunOwner::UploadReclaim {
            scope: take_scope(decoder)?,
            intent_id: take_intent_id(decoder)?,
            index_id: take_index_id(decoder)?,
            generation: take_generation(decoder)?,
        }),
        unknown => Err(unknown_discriminant("GC run owner", unknown)),
    }
}

fn put_gc_phase(encoder: &mut ValueEncoder, phase: &BlobGcPhase) {
    match phase {
        BlobGcPhase::AwaitDeleteFences { member_cursor } => {
            encoder.put_u8(0x01);
            put_cursor(encoder, member_cursor.as_ref());
        }
        BlobGcPhase::FencesClosed => encoder.put_u8(0x02),
        BlobGcPhase::FirstPass {
            writer_epoch,
            first_attempt,
            reference_cursor,
        } => {
            encoder.put_u8(0x03);
            put_writer_epoch(encoder, *writer_epoch);
            encoder.put_u64(first_attempt.get());
            put_cursor(encoder, reference_cursor.as_ref());
        }
        BlobGcPhase::SecondPass {
            completed_first_attempt,
            writer_epoch,
            second_attempt,
            reference_cursor,
        } => {
            encoder.put_u8(0x04);
            encoder.put_u64(completed_first_attempt.get());
            put_writer_epoch(encoder, *writer_epoch);
            encoder.put_u64(second_attempt.get());
            put_cursor(encoder, reference_cursor.as_ref());
        }
        BlobGcPhase::Delete {
            completed_first_attempt,
            completed_second_attempt,
            member_cursor,
            stale_mark_cleanup,
        } => {
            encoder.put_u8(0x05);
            encoder.put_u64(completed_first_attempt.get());
            encoder.put_u64(completed_second_attempt.get());
            put_cursor(encoder, member_cursor.as_ref());
            match stale_mark_cleanup {
                StaleMarkCleanup::Pending { mark_cursor } => {
                    encoder.put_u8(0x01);
                    put_cursor(encoder, mark_cursor.as_ref());
                }
                StaleMarkCleanup::Complete => encoder.put_u8(0x02),
            }
        }
    }
}

fn take_gc_phase(decoder: &mut ValueDecoder<'_>) -> Result<BlobGcPhase, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(BlobGcPhase::AwaitDeleteFences {
            member_cursor: take_cursor(decoder)?,
        }),
        0x02 => Ok(BlobGcPhase::FencesClosed),
        0x03 => Ok(BlobGcPhase::FirstPass {
            writer_epoch: take_writer_epoch(decoder)?,
            first_attempt: GcScanAttempt::new(decoder.take_u64()?).map_err(work_model_error)?,
            reference_cursor: take_cursor(decoder)?,
        }),
        0x04 => Ok(BlobGcPhase::SecondPass {
            completed_first_attempt: GcScanAttempt::new(decoder.take_u64()?)
                .map_err(work_model_error)?,
            writer_epoch: take_writer_epoch(decoder)?,
            second_attempt: GcScanAttempt::new(decoder.take_u64()?).map_err(work_model_error)?,
            reference_cursor: take_cursor(decoder)?,
        }),
        0x05 => {
            let completed_first_attempt =
                GcScanAttempt::new(decoder.take_u64()?).map_err(work_model_error)?;
            let completed_second_attempt =
                GcScanAttempt::new(decoder.take_u64()?).map_err(work_model_error)?;
            let member_cursor = take_cursor(decoder)?;
            let stale_mark_cleanup = match decoder.take_u8()? {
                0x01 => StaleMarkCleanup::Pending {
                    mark_cursor: take_cursor(decoder)?,
                },
                0x02 => StaleMarkCleanup::Complete,
                unknown => return Err(unknown_discriminant("stale mark cleanup", unknown)),
            };
            Ok(BlobGcPhase::Delete {
                completed_first_attempt,
                completed_second_attempt,
                member_cursor,
                stale_mark_cleanup,
            })
        }
        unknown => Err(unknown_discriminant("GC phase", unknown)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::v1::keys::index_v2::{
        BlobHash, BlobReferenceOwnerKind, CanonicalSecondaryValue, IndexV2Key, SecondaryEntryLane,
        TextBuildArtifactKey, TextIntentOwnedKey, TextManifestPageKey, TextManifestRootKey,
    };
    use crate::encoding::v1::keys::tenant::DataScope;
    use crate::index_v2::{
        BlobGcRunId, BlobPublicationPermitId, ClaimSequence, IndexComponent, IndexElementKind,
        IndexEntityId, IndexGenerationId, IndexId, IndexIdentity, IndexIdentityFamily,
        IndexOperationBlocker, IndexOperationId, IndexOperationRevision, IndexRevision, MutationId,
        OperationClaim, TextIntentRevision, TextLogicalVersion, TextManifestRevision,
        TextUploadIntentId, VectorPhysicalIndexId, WriterEpoch,
    };

    fn split() -> SplitRef {
        SplitRef::try_new(BlobRef::new([1; 32], 100), 80, 20, 10, 100).unwrap()
    }

    fn identity() -> IndexIdentity {
        IndexIdentity::new(
            IndexIdentityFamily::Text,
            IndexElementKind::Node,
            IndexComponent::try_new("label", "Doc").unwrap(),
            IndexComponent::try_new("property", "body").unwrap(),
        )
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn golden_digest(entries: &[(&str, String)]) -> String {
        use sha2::{Digest, Sha256};

        let mut digest = Sha256::new();
        for (name, bytes) in entries {
            digest.update(
                u64::try_from(name.len())
                    .expect("golden fixture name length fits u64")
                    .to_be_bytes(),
            );
            digest.update(name.as_bytes());
            digest.update(
                u64::try_from(bytes.len())
                    .expect("golden fixture byte length fits u64")
                    .to_be_bytes(),
            );
            digest.update(bytes.as_bytes());
        }
        hex(&digest.finalize())
    }

    #[test]
    fn every_work_value_and_nested_discriminant_has_frozen_bytes() {
        let index_id = IndexId::initial();
        let generation = IndexGenerationId::initial();
        let entity_id = IndexEntityId::new(7);
        let intent_id = TextUploadIntentId::from_bytes([0x21; 16]).unwrap();
        let operation_id = IndexOperationId::from_bytes([0x22; 16]).unwrap();
        let run_id = BlobGcRunId::from_bytes([0x23; 16]).unwrap();
        let partition = TextPartition::try_tenant_value(Bytes::from_static(b"acme")).unwrap();
        let split = SplitRef::try_new(BlobRef::new([0x31; 32], 100), 80, 20, 10, 100).unwrap();
        let root_key = TextManifestRootKey {
            index_id,
            generation,
            partition: partition.fingerprint(),
        };
        let intent_key = TextIntentOwnedKey {
            index_id,
            generation,
            intent_id,
        };
        let manifest_owner_key = IndexV2Key::TextManifestPage(TextManifestPageKey {
            root: root_key,
            page: 2,
        })
        .to_bytes();
        let artifact_owner_key = IndexV2Key::TextBuildArtifact(TextBuildArtifactKey {
            root: root_key,
            ordinal: 3,
        })
        .to_bytes();
        let intent_owner_key = IndexV2Key::TextUploadIntent(intent_key).to_bytes();
        let proof_key = IndexV2Key::ActiveMutationCommitProof(intent_key).to_bytes();
        let manifest_authorization = UploadDestinationAuthorization::try_new(
            BlobReferenceOwnerKind::ManifestPageSplit,
            manifest_owner_key.clone(),
            0,
            None,
        )
        .unwrap();
        let artifact_authorization = UploadDestinationAuthorization::try_new(
            BlobReferenceOwnerKind::BuildArtifact,
            artifact_owner_key.clone(),
            0,
            None,
        )
        .unwrap();
        let active_authorization = UploadDestinationAuthorization::try_new(
            BlobReferenceOwnerKind::ManifestPageSplit,
            manifest_owner_key.clone(),
            0,
            Some(proof_key),
        )
        .unwrap();
        let build_owner = TextUploadOwner::Build {
            operation_id,
            expected_operation_revision: IndexOperationRevision::initial(),
        };
        let active_owner = TextUploadOwner::ActiveMutation {
            writer_epoch: WriterEpoch::from_bytes([0x24; 16]).unwrap(),
            mutation_id: MutationId::from_bytes([0x25; 16]).unwrap(),
            active_record_revision: IndexRevision::initial(),
        };
        let manifest_attachment = TextUploadAttachment::ManifestSplit(split);
        let artifact_attachment = TextUploadAttachment::BuildArtifact {
            artifact_ordinal: 3,
            split,
        };
        let queued = TextUploadWorkState::Queued {
            not_before_unix_millis: None,
        };
        let delayed = TextUploadWorkState::Queued {
            not_before_unix_millis: Some(9),
        };
        let claimed = TextUploadWorkState::Claimed(OperationClaim {
            writer_epoch: WriterEpoch::from_bytes([0x26; 16]).unwrap(),
            sequence: ClaimSequence::new(2).unwrap(),
        });
        let blocked =
            TextUploadWorkState::Blocked(IndexOperationBlocker::BlobPublicationMismatch {
                intent_id,
            });
        let upload = |owner, attachment, phase, work_state| {
            TextUploadIntentValue::try_new(
                intent_id,
                TextIntentRevision::initial(),
                index_id,
                identity(),
                generation,
                partition.clone(),
                split.blob(),
                BlobPublicationPermitId::from_bytes([0x27; 16]).unwrap(),
                owner,
                attachment,
                phase,
                4,
                work_state,
            )
            .unwrap()
        };

        let mut values = vec![(
            "coalesced_delta",
            IndexV2WorkValue::CoalescedBuildDelta(CoalescedBuildDeltaValue {
                index_id,
                generation,
                entity_kind: IndexElementKind::Node,
                entity_id,
            }),
        )];
        for (name, state) in [
            (
                "applied_secondary_none",
                AppliedFamilyState::Secondary(None),
            ),
            (
                "applied_secondary_equality",
                AppliedFamilyState::Secondary(Some(CanonicalSecondaryValue::Equality([0x41; 8]))),
            ),
            (
                "applied_secondary_range",
                AppliedFamilyState::Secondary(Some(CanonicalSecondaryValue::Range(
                    Bytes::from_static(b"range"),
                ))),
            ),
            ("applied_vector_none", AppliedFamilyState::Vector(None)),
            (
                "applied_vector_partition",
                AppliedFamilyState::Vector(Some(partition.clone())),
            ),
            ("applied_text_none", AppliedFamilyState::Text(None)),
            (
                "applied_text_partition",
                AppliedFamilyState::Text(Some((partition.clone(), TextLogicalVersion::initial()))),
            ),
        ] {
            values.push((
                name,
                IndexV2WorkValue::AppliedEntityState(AppliedEntityStateValue {
                    index_id,
                    generation,
                    entity_kind: IndexElementKind::Node,
                    entity_id,
                    state,
                }),
            ));
        }
        for (name, lane) in [
            ("entry_node_equality", SecondaryEntryLane::NodeEquality),
            (
                "entry_node_unique_equality",
                SecondaryEntryLane::NodeUniqueEquality,
            ),
            (
                "entry_node_range_ascending",
                SecondaryEntryLane::NodeRangeAscending,
            ),
            (
                "entry_node_range_descending",
                SecondaryEntryLane::NodeRangeDescending,
            ),
            ("entry_edge_equality", SecondaryEntryLane::EdgeEquality),
            (
                "entry_edge_range_ascending",
                SecondaryEntryLane::EdgeRangeAscending,
            ),
            (
                "entry_edge_range_descending",
                SecondaryEntryLane::EdgeRangeDescending,
            ),
        ] {
            values.push((
                name,
                IndexV2WorkValue::SecondaryEntry(SecondaryEntryValue {
                    index_id,
                    generation,
                    lane,
                    entity_id,
                }),
            ));
        }
        values.extend([
            (
                "vector_partition_mapping",
                IndexV2WorkValue::VectorPartitionMapping(VectorPartitionMappingValue {
                    index_id,
                    generation,
                    partition: VectorTenantPartition::try_new(Bytes::from_static(b"acme")).unwrap(),
                    physical_index_id: VectorPhysicalIndexId::initial(),
                }),
            ),
            (
                "manifest_root",
                IndexV2WorkValue::TextManifestRoot(
                    TextManifestRootValue::try_new(
                        index_id,
                        generation,
                        partition.clone(),
                        TextManifestRevision::initial(),
                        1,
                        1,
                    )
                    .unwrap(),
                ),
            ),
            (
                "manifest_page",
                IndexV2WorkValue::TextManifestPage(
                    TextManifestPageValue::try_new(
                        index_id,
                        generation,
                        partition.clone(),
                        2,
                        vec![split],
                    )
                    .unwrap(),
                ),
            ),
            (
                "upload_build_prepared",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    build_owner,
                    manifest_attachment,
                    TextUploadPhase::Prepared,
                    queued.clone(),
                ))),
            ),
            (
                "upload_build_uploaded_claimed",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    build_owner,
                    manifest_attachment,
                    TextUploadPhase::Uploaded,
                    claimed.clone(),
                ))),
            ),
            (
                "upload_build_blocked",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    build_owner,
                    manifest_attachment,
                    TextUploadPhase::Prepared,
                    blocked,
                ))),
            ),
            (
                "upload_build_reference_manifest",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    build_owner,
                    manifest_attachment,
                    TextUploadPhase::ReferenceCommitted(manifest_authorization),
                    delayed.clone(),
                ))),
            ),
            (
                "upload_build_reference_artifact",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    build_owner,
                    artifact_attachment,
                    TextUploadPhase::ReferenceCommitted(artifact_authorization),
                    queued.clone(),
                ))),
            ),
            (
                "upload_build_reclaim_unassigned",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    build_owner,
                    manifest_attachment,
                    TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned),
                    queued.clone(),
                ))),
            ),
            (
                "upload_build_reclaim_assigned",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    build_owner,
                    manifest_attachment,
                    TextUploadPhase::Reclaimable(ReclaimAssignment::Assigned(run_id)),
                    queued.clone(),
                ))),
            ),
            (
                "upload_build_non_publication_proven",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    build_owner,
                    manifest_attachment,
                    TextUploadPhase::NonPublicationProven,
                    queued.clone(),
                ))),
            ),
            (
                "upload_active_prepared",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    active_owner,
                    manifest_attachment,
                    TextUploadPhase::Prepared,
                    queued.clone(),
                ))),
            ),
            (
                "upload_active_uploaded_claimed",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    active_owner,
                    manifest_attachment,
                    TextUploadPhase::Uploaded,
                    claimed,
                ))),
            ),
            (
                "upload_active_reference",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    active_owner,
                    manifest_attachment,
                    TextUploadPhase::ReferenceCommitted(active_authorization),
                    delayed,
                ))),
            ),
            (
                "upload_active_reclaim",
                IndexV2WorkValue::TextUploadIntent(Box::new(upload(
                    active_owner,
                    manifest_attachment,
                    TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned),
                    queued,
                ))),
            ),
            (
                "build_artifact",
                IndexV2WorkValue::TextBuildArtifact(TextBuildArtifactValue {
                    index_id,
                    generation,
                    partition: partition.clone(),
                    artifact_ordinal: 3,
                    split,
                    source_intent_id: intent_id,
                }),
            ),
            (
                "candidate_generation",
                IndexV2WorkValue::BlobGcCandidate(BlobGcCandidateValue {
                    owner: BlobGcCandidateOwner::GenerationCleanup(operation_id),
                    index_id,
                    generation,
                    blob: split.blob(),
                }),
            ),
            (
                "candidate_upload",
                IndexV2WorkValue::BlobGcCandidate(BlobGcCandidateValue {
                    owner: BlobGcCandidateOwner::UploadIntent(intent_id),
                    index_id,
                    generation,
                    blob: split.blob(),
                }),
            ),
        ]);

        let generation_owner = BlobGcRunOwner::GenerationCleanup {
            scope: DataScope::LegacyUnscoped,
            operation_id,
            index_id,
            generation,
        };
        let upload_owner = BlobGcRunOwner::UploadReclaim {
            scope: DataScope::Tenant(crate::encoding::v1::keys::tenant::TenantId::from_u128(42)),
            intent_id,
            index_id,
            generation,
        };
        let gc_cursor = Some(
            crate::index_v2::IndexCursor::try_new(
                IndexV2Key::TextUploadIntent(intent_key).to_bytes(),
            )
            .unwrap(),
        );
        for (name, owner, phase) in [
            (
                "gc_root_await_fences",
                generation_owner,
                BlobGcPhase::AwaitDeleteFences {
                    member_cursor: gc_cursor.clone(),
                },
            ),
            (
                "gc_root_fences_closed",
                upload_owner,
                BlobGcPhase::FencesClosed,
            ),
            (
                "gc_root_first_pass",
                generation_owner,
                BlobGcPhase::FirstPass {
                    writer_epoch: WriterEpoch::from_bytes([0x28; 16]).unwrap(),
                    first_attempt: GcScanAttempt::new(1).unwrap(),
                    reference_cursor: gc_cursor.clone(),
                },
            ),
            (
                "gc_root_second_pass",
                generation_owner,
                BlobGcPhase::SecondPass {
                    completed_first_attempt: GcScanAttempt::new(1).unwrap(),
                    writer_epoch: WriterEpoch::from_bytes([0x28; 16]).unwrap(),
                    second_attempt: GcScanAttempt::new(2).unwrap(),
                    reference_cursor: gc_cursor.clone(),
                },
            ),
            (
                "gc_root_delete_pending",
                generation_owner,
                BlobGcPhase::Delete {
                    completed_first_attempt: GcScanAttempt::new(1).unwrap(),
                    completed_second_attempt: GcScanAttempt::new(2).unwrap(),
                    member_cursor: gc_cursor.clone(),
                    stale_mark_cleanup: StaleMarkCleanup::Pending {
                        mark_cursor: gc_cursor.clone(),
                    },
                },
            ),
            (
                "gc_root_delete_complete",
                generation_owner,
                BlobGcPhase::Delete {
                    completed_first_attempt: GcScanAttempt::new(1).unwrap(),
                    completed_second_attempt: GcScanAttempt::new(2).unwrap(),
                    member_cursor: gc_cursor.clone(),
                    stale_mark_cleanup: StaleMarkCleanup::Complete,
                },
            ),
            (
                "gc_root_upload_owner",
                upload_owner,
                BlobGcPhase::FirstPass {
                    writer_epoch: WriterEpoch::from_bytes([0x28; 16]).unwrap(),
                    first_attempt: GcScanAttempt::new(1).unwrap(),
                    reference_cursor: None,
                },
            ),
        ] {
            values.push((
                name,
                IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::RunRoot(
                    BlobGcRunRootValue::try_new(
                        run_id,
                        owner,
                        BlobGcRunRevision::initial(),
                        3,
                        Some(10),
                        phase,
                        1,
                    )
                    .unwrap(),
                )),
            ));
        }
        values.extend([
            (
                "gc_mark_first",
                IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::ReachabilityMark(
                    BlobGcReachabilityMarkValue {
                        run_id,
                        first_pass: true,
                        scan_attempt: GcScanAttempt::new(1).unwrap(),
                        blob_hash: BlobHash::new([0x32; 32]),
                        referenced: true,
                    },
                )),
            ),
            (
                "gc_mark_second",
                IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::ReachabilityMark(
                    BlobGcReachabilityMarkValue {
                        run_id,
                        first_pass: false,
                        scan_attempt: GcScanAttempt::new(2).unwrap(),
                        blob_hash: BlobHash::new([0x32; 32]),
                        referenced: false,
                    },
                )),
            ),
            (
                "gc_member_pending_none",
                IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::CandidateMember(
                    BlobGcCandidateMemberValue {
                        run_id,
                        blob: split.blob(),
                        state: BlobGcMemberState::PendingDisposition { owner_cursor: None },
                    },
                )),
            ),
            (
                "gc_member_pending_cursor",
                IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::CandidateMember(
                    BlobGcCandidateMemberValue {
                        run_id,
                        blob: split.blob(),
                        state: BlobGcMemberState::PendingDisposition {
                            owner_cursor: gc_cursor,
                        },
                    },
                )),
            ),
            (
                "gc_member_deleted",
                IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::CandidateMember(
                    BlobGcCandidateMemberValue {
                        run_id,
                        blob: split.blob(),
                        state: BlobGcMemberState::CleanupCommitted(
                            BlobGcDisposition::DeletedOrAbsent,
                        ),
                    },
                )),
            ),
            (
                "gc_member_preserved",
                IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::CandidateMember(
                    BlobGcCandidateMemberValue {
                        run_id,
                        blob: split.blob(),
                        state: BlobGcMemberState::CleanupCommitted(
                            BlobGcDisposition::ReferencedPreserved,
                        ),
                    },
                )),
            ),
            (
                "text_entity_live",
                IndexV2WorkValue::TextEntityState(TextEntityStateValue {
                    index_id,
                    generation,
                    partition: partition.clone(),
                    entity_kind: IndexElementKind::Node,
                    entity_id,
                    logical_version: TextLogicalVersion::initial(),
                    live: true,
                }),
            ),
            (
                "text_entity_deleted",
                IndexV2WorkValue::TextEntityState(TextEntityStateValue {
                    index_id,
                    generation,
                    partition: partition.clone(),
                    entity_kind: IndexElementKind::Edge,
                    entity_id,
                    logical_version: TextLogicalVersion::initial(),
                    live: false,
                }),
            ),
            (
                "active_mutation_proof",
                IndexV2WorkValue::ActiveMutationCommitProof(ActiveMutationCommitProofValue {
                    intent_id,
                    index_id,
                    generation,
                    partition: partition.clone(),
                    writer_epoch: WriterEpoch::from_bytes([0x24; 16]).unwrap(),
                    mutation_id: MutationId::from_bytes([0x25; 16]).unwrap(),
                    active_record_revision: IndexRevision::initial(),
                    logical_version: TextLogicalVersion::initial(),
                    destination: TextManifestSplitLocation::try_new(1, 2).unwrap(),
                    split,
                }),
            ),
        ]);
        for (name, owner_kind, scope, owner_key, owner_slot) in [
            (
                "reference_upload",
                BlobReferenceOwnerKind::UploadIntent,
                DataScope::LegacyUnscoped,
                intent_owner_key,
                0,
            ),
            (
                "reference_manifest_tenant",
                BlobReferenceOwnerKind::ManifestPageSplit,
                DataScope::Tenant(crate::encoding::v1::keys::tenant::TenantId::from_u128(42)),
                manifest_owner_key,
                1,
            ),
            (
                "reference_artifact",
                BlobReferenceOwnerKind::BuildArtifact,
                DataScope::LegacyUnscoped,
                artifact_owner_key,
                2,
            ),
        ] {
            values.push((
                name,
                IndexV2WorkValue::BlobReachabilityReference(
                    BlobReachabilityReferenceValue::try_new(
                        split.blob(),
                        owner_kind,
                        scope,
                        owner_key,
                        owner_slot,
                    )
                    .unwrap(),
                ),
            ));
        }

        let goldens = values
            .into_iter()
            .map(|(name, value)| {
                let bytes = encode_work_value(&value);
                assert_eq!(decode_work_value(&bytes).unwrap(), value);
                (name, hex(&bytes))
            })
            .collect::<Vec<_>>();
        assert_eq!(goldens.len(), 52);
        // The length-framed name/value digest freezes every complete encoded
        // byte while keeping this already exhaustive variant matrix readable.
        assert_eq!(
            golden_digest(&goldens),
            "3a3e668b8f2a2e8a4780b0857b3911b54b18b0cec82d27c27f3e947a69d696ab"
        );
    }

    fn all_simple_values() -> Vec<IndexV2WorkValue> {
        vec![
            IndexV2WorkValue::CoalescedBuildDelta(CoalescedBuildDeltaValue {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                entity_kind: IndexElementKind::Node,
                entity_id: IndexEntityId::new(7),
            }),
            IndexV2WorkValue::AppliedEntityState(AppliedEntityStateValue {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                entity_kind: IndexElementKind::Node,
                entity_id: IndexEntityId::new(7),
                state: AppliedFamilyState::Secondary(Some(CanonicalSecondaryValue::Equality(
                    [2; 8],
                ))),
            }),
            IndexV2WorkValue::SecondaryEntry(SecondaryEntryValue {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                lane: SecondaryEntryLane::NodeEquality,
                entity_id: IndexEntityId::new(7),
            }),
            IndexV2WorkValue::TextManifestRoot(
                TextManifestRootValue::try_new(
                    IndexId::initial(),
                    IndexGenerationId::initial(),
                    TextPartition::Unpartitioned,
                    TextManifestRevision::initial(),
                    1,
                    1,
                )
                .unwrap(),
            ),
            IndexV2WorkValue::TextManifestPage(
                TextManifestPageValue::try_new(
                    IndexId::initial(),
                    IndexGenerationId::initial(),
                    TextPartition::Unpartitioned,
                    0,
                    vec![split()],
                )
                .unwrap(),
            ),
            IndexV2WorkValue::TextBuildArtifact(TextBuildArtifactValue {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                partition: TextPartition::Unpartitioned,
                artifact_ordinal: 0,
                split: split(),
                source_intent_id: TextUploadIntentId::from_bytes([3; 16]).unwrap(),
            }),
            IndexV2WorkValue::BlobGcCandidate(BlobGcCandidateValue {
                owner: BlobGcCandidateOwner::GenerationCleanup(
                    IndexOperationId::from_bytes([4; 16]).unwrap(),
                ),
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                blob: BlobRef::new([5; 32], 10),
            }),
            IndexV2WorkValue::TextEntityState(TextEntityStateValue {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                partition: TextPartition::Unpartitioned,
                entity_kind: IndexElementKind::Node,
                entity_id: IndexEntityId::new(7),
                logical_version: TextLogicalVersion::initial(),
                live: true,
            }),
            IndexV2WorkValue::ActiveMutationCommitProof(ActiveMutationCommitProofValue {
                intent_id: TextUploadIntentId::from_bytes([6; 16]).unwrap(),
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                partition: TextPartition::Unpartitioned,
                writer_epoch: WriterEpoch::from_bytes([7; 16]).unwrap(),
                mutation_id: MutationId::from_bytes([8; 16]).unwrap(),
                active_record_revision: IndexRevision::initial(),
                logical_version: TextLogicalVersion::initial(),
                destination: TextManifestSplitLocation::try_new(0, 0).unwrap(),
                split: split(),
            }),
            IndexV2WorkValue::BlobReachabilityReference(
                BlobReachabilityReferenceValue::try_new(
                    BlobRef::new([9; 32], 10),
                    BlobReferenceOwnerKind::BuildArtifact,
                    DataScope::LegacyUnscoped,
                    IndexV2Key::TextBuildArtifact(TextBuildArtifactKey {
                        root: TextManifestRootKey {
                            index_id: IndexId::initial(),
                            generation: IndexGenerationId::initial(),
                            partition: TextPartition::Unpartitioned.fingerprint(),
                        },
                        ordinal: 0,
                    })
                    .to_bytes(),
                    0,
                )
                .unwrap(),
            ),
            IndexV2WorkValue::VectorPartitionMapping(VectorPartitionMappingValue {
                index_id: IndexId::initial(),
                generation: IndexGenerationId::initial(),
                partition: VectorTenantPartition::try_new(Bytes::from_static(b"acme")).unwrap(),
                physical_index_id: VectorPhysicalIndexId::initial(),
            }),
        ]
    }

    #[test]
    fn every_work_value_kind_has_frozen_header_and_roundtrips() {
        for value in all_simple_values() {
            let bytes = encode_work_value(&value);
            const VERSION_OFFSET: usize = 0;
            const KIND_OFFSET: usize = VERSION_OFFSET + 1;
            assert_eq!(bytes[VERSION_OFFSET], 0x01);
            assert_eq!(bytes[KIND_OFFSET], value.record_kind());
            assert_eq!(decode_work_value(&bytes).unwrap(), value);
        }
    }

    #[test]
    fn all_upload_phases_roundtrip_with_owner_specific_proof_rules() {
        let intent_id = TextUploadIntentId::from_bytes([1; 16]).unwrap();
        let phases = [
            TextUploadPhase::Prepared,
            TextUploadPhase::Uploaded,
            TextUploadPhase::Reclaimable(ReclaimAssignment::Unassigned),
            TextUploadPhase::NonPublicationProven,
        ];
        for phase in phases {
            let attachment = split();
            let value = TextUploadIntentValue::try_new(
                intent_id,
                TextIntentRevision::initial(),
                IndexId::initial(),
                identity(),
                IndexGenerationId::initial(),
                TextPartition::Unpartitioned,
                attachment.blob(),
                BlobPublicationPermitId::from_bytes([3; 16]).unwrap(),
                TextUploadOwner::Build {
                    operation_id: IndexOperationId::from_bytes([4; 16]).unwrap(),
                    expected_operation_revision: crate::index_v2::IndexOperationRevision::initial(),
                },
                TextUploadAttachment::ManifestSplit(attachment),
                phase,
                0,
                TextUploadWorkState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .unwrap();
            let wrapped = IndexV2WorkValue::TextUploadIntent(Box::new(value));
            assert_eq!(
                decode_work_value(&encode_work_value(&wrapped)).unwrap(),
                wrapped
            );
        }
    }

    #[test]
    fn every_gc_entry_and_phase_roundtrips() {
        let run_id = BlobGcRunId::from_bytes([1; 16]).unwrap();
        let owner = BlobGcRunOwner::GenerationCleanup {
            scope: DataScope::LegacyUnscoped,
            operation_id: IndexOperationId::from_bytes([2; 16]).unwrap(),
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
        };
        let phases = [
            BlobGcPhase::AwaitDeleteFences {
                member_cursor: None,
            },
            BlobGcPhase::FencesClosed,
            BlobGcPhase::FirstPass {
                writer_epoch: WriterEpoch::from_bytes([3; 16]).unwrap(),
                first_attempt: GcScanAttempt::new(1).unwrap(),
                reference_cursor: None,
            },
            BlobGcPhase::SecondPass {
                completed_first_attempt: GcScanAttempt::new(1).unwrap(),
                writer_epoch: WriterEpoch::from_bytes([3; 16]).unwrap(),
                second_attempt: GcScanAttempt::new(2).unwrap(),
                reference_cursor: None,
            },
            BlobGcPhase::Delete {
                completed_first_attempt: GcScanAttempt::new(1).unwrap(),
                completed_second_attempt: GcScanAttempt::new(2).unwrap(),
                member_cursor: None,
                stale_mark_cleanup: StaleMarkCleanup::Complete,
            },
        ];
        for phase in phases {
            let root = BlobGcRunRootValue::try_new(
                run_id,
                owner,
                BlobGcRunRevision::initial(),
                0,
                None,
                phase,
                1,
            )
            .unwrap();
            let value = IndexV2WorkValue::BlobGcEntry(BlobGcEntryValue::RunRoot(root));
            assert_eq!(
                decode_work_value(&encode_work_value(&value)).unwrap(),
                value
            );
        }

        for entry in [
            BlobGcEntryValue::ReachabilityMark(BlobGcReachabilityMarkValue {
                run_id,
                first_pass: true,
                scan_attempt: GcScanAttempt::new(1).unwrap(),
                blob_hash: BlobHash::new([4; 32]),
                referenced: true,
            }),
            BlobGcEntryValue::CandidateMember(BlobGcCandidateMemberValue {
                run_id,
                blob: BlobRef::new([4; 32], 10),
                state: BlobGcMemberState::CleanupCommitted(BlobGcDisposition::ReferencedPreserved),
            }),
        ] {
            let value = IndexV2WorkValue::BlobGcEntry(entry);
            assert_eq!(
                decode_work_value(&encode_work_value(&value)).unwrap(),
                value
            );
        }
    }

    #[test]
    fn malformed_work_values_reject_unknown_noncanonical_and_trailing_bytes() {
        let valid = encode_work_value(&all_simple_values().remove(0));
        let mut unknown = valid.to_vec();
        const VERSION_OFFSET: usize = 0;
        const KIND_OFFSET: usize = VERSION_OFFSET + 1;
        const BODY_OFFSET: usize = KIND_OFFSET + 1;
        let mut wrong_version = valid.to_vec();
        wrong_version[VERSION_OFFSET] = 0x02;
        assert!(decode_work_value(&wrong_version).is_err());
        unknown[KIND_OFFSET] = 0xFF;
        assert!(decode_work_value(&unknown).is_err());
        let mut zero_index_id = valid.to_vec();
        zero_index_id[BODY_OFFSET..BODY_OFFSET + U64_LEN].copy_from_slice(&0u64.to_be_bytes());
        assert!(decode_work_value(&zero_index_id).is_err());
        let mut trailing = valid.to_vec();
        trailing.push(0);
        assert!(decode_work_value(&trailing).is_err());
        assert!(
            decode_work_value(&valid[VERSION_OFFSET..VERSION_OFFSET + valid.len() - 1]).is_err()
        );

        let live = IndexV2WorkValue::TextEntityState(TextEntityStateValue {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition: TextPartition::Unpartitioned,
            entity_kind: IndexElementKind::Node,
            entity_id: IndexEntityId::initial(),
            logical_version: TextLogicalVersion::initial(),
            live: true,
        });
        let mut noncanonical_bool = encode_work_value(&live).to_vec();
        let live_offset = noncanonical_bool.len() - U8_LEN;
        noncanonical_bool[live_offset] = 0x02;
        assert!(decode_work_value(&noncanonical_bool).is_err());

        let page = IndexV2WorkValue::TextManifestPage(
            TextManifestPageValue::try_new(
                IndexId::initial(),
                IndexGenerationId::initial(),
                TextPartition::Unpartitioned,
                0,
                vec![split()],
            )
            .unwrap(),
        );
        let mut oversized_collection = encode_work_value(&page).to_vec();
        const PARTITION_TAG_LEN: usize = U8_LEN;
        const PAGE_OFFSET: usize = BODY_OFFSET + U64_LEN + U64_LEN + PARTITION_TAG_LEN;
        const COUNT_OFFSET: usize = PAGE_OFFSET + U32_LEN;
        oversized_collection[COUNT_OFFSET..COUNT_OFFSET + U32_LEN]
            .copy_from_slice(&u32::from(u16::MAX).saturating_add(1).to_be_bytes());
        assert!(decode_work_value(&oversized_collection).is_err());
        let mut empty_page = encode_work_value(&page).to_vec();
        empty_page[COUNT_OFFSET..COUNT_OFFSET + U32_LEN].copy_from_slice(&0_u32.to_be_bytes());
        assert!(decode_work_value(&empty_page).is_err());
        let mut empty_split = encode_work_value(&page).to_vec();
        const SPLIT_OFFSET: usize = COUNT_OFFSET + U32_LEN;
        const BLOB_SIZE_OFFSET: usize = SPLIT_OFFSET + HASH_LEN;
        const FOOTER_OFFSET: usize = BLOB_SIZE_OFFSET + U64_LEN;
        const FOOTER_LENGTH_OFFSET: usize = FOOTER_OFFSET + U64_LEN;
        const HOT_CACHE_LENGTH_OFFSET: usize = FOOTER_LENGTH_OFFSET + U32_LEN;
        const TOTAL_SIZE_OFFSET: usize = HOT_CACHE_LENGTH_OFFSET + U32_LEN;
        empty_split[BLOB_SIZE_OFFSET..BLOB_SIZE_OFFSET + U64_LEN]
            .copy_from_slice(&0_u64.to_be_bytes());
        empty_split[FOOTER_OFFSET..FOOTER_OFFSET + U64_LEN].copy_from_slice(&0_u64.to_be_bytes());
        empty_split[FOOTER_LENGTH_OFFSET..FOOTER_LENGTH_OFFSET + U32_LEN]
            .copy_from_slice(&0_u32.to_be_bytes());
        empty_split[HOT_CACHE_LENGTH_OFFSET..HOT_CACHE_LENGTH_OFFSET + U32_LEN]
            .copy_from_slice(&0_u32.to_be_bytes());
        empty_split[TOTAL_SIZE_OFFSET..TOTAL_SIZE_OFFSET + U64_LEN]
            .copy_from_slice(&0_u64.to_be_bytes());
        assert!(decode_work_value(&empty_split).is_err());

        let root = IndexV2WorkValue::TextManifestRoot(
            TextManifestRootValue::try_new(
                IndexId::initial(),
                IndexGenerationId::initial(),
                TextPartition::Unpartitioned,
                TextManifestRevision::initial(),
                1,
                1,
            )
            .unwrap(),
        );
        let mut inconsistent_root = encode_work_value(&root).to_vec();
        const ROOT_REVISION_OFFSET: usize = BODY_OFFSET + U64_LEN + U64_LEN + PARTITION_TAG_LEN;
        const ROOT_PAGE_COUNT_OFFSET: usize = ROOT_REVISION_OFFSET + U64_LEN;
        const ROOT_SPLIT_COUNT_OFFSET: usize = ROOT_PAGE_COUNT_OFFSET + U32_LEN;
        inconsistent_root[ROOT_SPLIT_COUNT_OFFSET..ROOT_SPLIT_COUNT_OFFSET + U64_LEN]
            .copy_from_slice(&0_u64.to_be_bytes());
        assert!(decode_work_value(&inconsistent_root).is_err());

        let proof = IndexV2WorkValue::ActiveMutationCommitProof(ActiveMutationCommitProofValue {
            intent_id: TextUploadIntentId::from_bytes([1; 16]).unwrap(),
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition: TextPartition::Unpartitioned,
            writer_epoch: WriterEpoch::from_bytes([2; 16]).unwrap(),
            mutation_id: MutationId::from_bytes([3; 16]).unwrap(),
            active_record_revision: IndexRevision::initial(),
            logical_version: TextLogicalVersion::initial(),
            destination: TextManifestSplitLocation::try_new(0, 0).unwrap(),
            split: split(),
        });
        let mut nil_intent_id = encode_work_value(&proof).to_vec();
        nil_intent_id[BODY_OFFSET..BODY_OFFSET + UUID_LEN].fill(0);
        assert!(decode_work_value(&nil_intent_id).is_err());
        const PROOF_PARTITION_TAG_LEN: usize = U8_LEN;
        const PROOF_DESTINATION_PAGE_OFFSET: usize = BODY_OFFSET
            + UUID_LEN
            + U64_LEN
            + U64_LEN
            + PROOF_PARTITION_TAG_LEN
            + UUID_LEN
            + UUID_LEN
            + U64_LEN
            + U64_LEN;
        const PROOF_DESTINATION_SLOT_OFFSET: usize = PROOF_DESTINATION_PAGE_OFFSET + U32_LEN;
        let mut exhausted_page = encode_work_value(&proof).to_vec();
        exhausted_page[PROOF_DESTINATION_PAGE_OFFSET..PROOF_DESTINATION_PAGE_OFFSET + U32_LEN]
            .copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_work_value(&exhausted_page).is_err());
        let mut exhausted_slot = encode_work_value(&proof).to_vec();
        exhausted_slot[PROOF_DESTINATION_SLOT_OFFSET..PROOF_DESTINATION_SLOT_OFFSET + U32_LEN]
            .copy_from_slice(&u32::from(u16::MAX).to_be_bytes());
        assert!(decode_work_value(&exhausted_slot).is_err());
    }
}
