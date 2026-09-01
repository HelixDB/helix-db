//! Lifecycle operation-record values.

//! Canonical metadata, logical index-record, and operation codecs.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::index_lifecycle::{
    BuildOperationOutcome, IndexOperationExecutionState, IndexOperationFamily, IndexOperationKind,
    IndexOperationOutcome, IndexOperationProgress, IndexOperationQueueSchedule,
    IndexOperationRecord, LegacyVectorDirectoryValidationProgress, LegacyVectorValidationLane,
    LegacyVectorValidationProgress, NoCursorProgress, PrefixScanProgress, SecondaryBuildProgress,
    SecondaryBuildStage, SecondaryCleanupProgress, SourceScanProgress, TextBuildProgress,
    TextBuildStage, TextCleanupProgress, TextManifestPageValidationProgress,
    TextManifestPartitionValidation, TextManifestValidationProgress, VectorBuildProgress,
    VectorBuildStage, VectorCleanupProgress,
};

#[cfg(test)]
use crate::index_lifecycle::work::{
    AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaState, CoalescedBuildDeltaValue,
};
#[cfg(test)]
use crate::index_lifecycle::IndexRecordV2;

use super::*;

pub(crate) fn encode_operation_record(record: &IndexOperationRecord) -> Bytes {
    let mut encoder = ValueEncoder::with_header(OPERATION_RECORD_KIND);
    put_operation_id(&mut encoder, record.operation_id());
    put_index_id(&mut encoder, record.index_id());
    put_identity(&mut encoder, record.identity());
    put_generation(&mut encoder, record.generation());
    put_revision(&mut encoder, record.index_record_revision());
    put_operation_revision(&mut encoder, record.operation_revision());
    encoder.put_u8(record.kind() as u8);
    encoder.put_u8(record.family() as u8);
    put_operation_progress(&mut encoder, record.progress());
    encoder.put_u32(record.attempt());
    put_execution_state(
        &mut encoder,
        record.kind(),
        record.execution_state(),
        record.queue_schedule(),
    );
    encoder.finish()
}

/// Decodes and cross-validates one durable outbox operation.
pub(crate) fn decode_operation_record(value: &[u8]) -> Result<IndexOperationRecord, EncodingError> {
    decode_operation_record_with_compatibility(value).map(|(record, _legacy)| record)
}

/// Decoded operation plus private compatibility facts that require repair.
struct DecodedOperationRecord {
    record: IndexOperationRecord,
    legacy_reader_coordination_blocker: bool,
}

/// Decodes a durable operation while retaining private legacy repair metadata.
pub(crate) fn decode_operation_record_with_compatibility(
    value: &[u8],
) -> Result<(IndexOperationRecord, bool), EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    if decoder.kind() != OPERATION_RECORD_KIND {
        return Err(EncodingError::UnexpectedValueKind {
            expected: OPERATION_RECORD_KIND,
            actual: decoder.kind(),
        });
    }
    let operation_id = take_operation_id(&mut decoder)?;
    let index_id = take_index_id(&mut decoder)?;
    let identity = take_identity(&mut decoder)?;
    let generation = take_generation(&mut decoder)?;
    let index_record_revision = take_revision(&mut decoder)?;
    let operation_revision = take_operation_revision(&mut decoder)?;
    let kind = match decoder.take_u8()? {
        0x01 => IndexOperationKind::Build,
        0x02 => IndexOperationKind::Drop,
        unknown => return Err(unknown_discriminant("operation kind", unknown)),
    };
    let family = match decoder.take_u8()? {
        0x01 => IndexOperationFamily::Secondary,
        0x02 => IndexOperationFamily::Vector,
        0x03 => IndexOperationFamily::Text,
        unknown => return Err(unknown_discriminant("operation family", unknown)),
    };
    let progress = take_operation_progress(&mut decoder, kind, family)?;
    let attempt = decoder.take_u32()?;
    let DecodedExecutionState {
        state: execution_state,
        queue_schedule,
        legacy_reader_coordination_blocker,
    } = take_execution_state(&mut decoder, kind)?;
    decoder.finish()?;
    let record = IndexOperationRecord::try_new_with_queue_schedule(
        operation_id,
        index_id,
        identity,
        generation,
        index_record_revision,
        operation_revision,
        kind,
        family,
        progress,
        attempt,
        execution_state,
        queue_schedule,
    )
    .map_err(operation_model_error)?;
    let decoded = DecodedOperationRecord {
        record,
        legacy_reader_coordination_blocker,
    };
    Ok((decoded.record, decoded.legacy_reader_coordination_blocker))
}

fn put_operation_progress(encoder: &mut ValueEncoder, progress: &IndexOperationProgress) {
    match progress {
        IndexOperationProgress::SecondaryBuild(progress) => match progress {
            SecondaryBuildProgress::Constructing(stage) => {
                encoder.put_u8(0x01);
                match stage {
                    SecondaryBuildStage::Scan(progress) => {
                        encoder.put_u8(0x01);
                        put_source_scan(encoder, progress);
                    }
                    SecondaryBuildStage::CatchUp(progress) => {
                        encoder.put_u8(0x02);
                        put_prefix_scan(encoder, progress);
                    }
                    SecondaryBuildStage::Validate(progress) => {
                        encoder.put_u8(0x03);
                        put_prefix_scan(encoder, progress);
                    }
                    SecondaryBuildStage::Activate(progress) => {
                        encoder.put_u8(0x04);
                        put_no_cursor(encoder, *progress);
                    }
                }
            }
            SecondaryBuildProgress::Aborting(progress) => {
                encoder.put_u8(0x02);
                put_secondary_cleanup(encoder, progress);
            }
        },
        IndexOperationProgress::VectorBuild(progress) => match progress {
            VectorBuildProgress::Constructing(stage) => {
                encoder.put_u8(0x01);
                match stage {
                    VectorBuildStage::AdoptLegacy(progress) => {
                        encoder.put_u8(0x05);
                        put_legacy_vector_validation(encoder, progress);
                    }
                    VectorBuildStage::ValidateAdoptedDirectory(progress) => {
                        encoder.put_u8(0x06);
                        put_legacy_vector_directory_validation(encoder, progress);
                    }
                    VectorBuildStage::Scan(progress) => {
                        encoder.put_u8(0x01);
                        put_source_scan(encoder, progress);
                    }
                    VectorBuildStage::CatchUp(progress) => {
                        encoder.put_u8(0x02);
                        put_prefix_scan(encoder, progress);
                    }
                    VectorBuildStage::ValidateDescriptor(progress) => {
                        encoder.put_u8(0x03);
                        put_prefix_scan(encoder, progress);
                    }
                    VectorBuildStage::Activate(progress) => {
                        encoder.put_u8(0x04);
                        put_no_cursor(encoder, *progress);
                    }
                }
            }
            VectorBuildProgress::Aborting(progress) => {
                encoder.put_u8(0x02);
                put_vector_cleanup(encoder, progress);
            }
        },
        IndexOperationProgress::TextBuild(progress) => match progress {
            TextBuildProgress::Constructing(stage) => {
                encoder.put_u8(0x01);
                match stage {
                    TextBuildStage::ScanSource(progress) => {
                        encoder.put_u8(0x01);
                        put_source_scan(encoder, progress);
                    }
                    TextBuildStage::ScanPartitions(progress) => {
                        encoder.put_u8(0x02);
                        put_source_scan(encoder, progress);
                    }
                    TextBuildStage::CatchUp(progress) => {
                        encoder.put_u8(0x03);
                        put_prefix_scan(encoder, progress);
                    }
                    TextBuildStage::Compact(progress) => {
                        encoder.put_u8(0x04);
                        put_prefix_scan(encoder, progress);
                    }
                    TextBuildStage::PrepareManifests(progress) => {
                        encoder.put_u8(0x05);
                        put_prefix_scan(encoder, progress);
                    }
                    TextBuildStage::ValidateManifests(progress) => {
                        encoder.put_u8(0x06);
                        put_text_manifest_validation(encoder, progress);
                    }
                    TextBuildStage::Activate(progress) => {
                        encoder.put_u8(0x07);
                        put_no_cursor(encoder, *progress);
                    }
                }
            }
            TextBuildProgress::Aborting(progress) => {
                encoder.put_u8(0x02);
                put_text_cleanup(encoder, progress);
            }
        },
        IndexOperationProgress::SecondaryCleanup(progress) => {
            put_secondary_cleanup(encoder, progress)
        }
        IndexOperationProgress::VectorCleanup(progress) => put_vector_cleanup(encoder, progress),
        IndexOperationProgress::TextCleanup(progress) => put_text_cleanup(encoder, progress),
    }
}

fn put_secondary_cleanup(encoder: &mut ValueEncoder, progress: &SecondaryCleanupProgress) {
    match progress {
        SecondaryCleanupProgress::DeleteEntries(progress) => {
            encoder.put_u8(0x02);
            put_prefix_scan(encoder, progress);
        }
        SecondaryCleanupProgress::DeleteDeltas(progress) => {
            encoder.put_u8(0x03);
            put_prefix_scan(encoder, progress);
        }
        SecondaryCleanupProgress::Finalize(progress) => {
            encoder.put_u8(0x05);
            put_no_cursor(encoder, *progress);
        }
    }
}

fn put_vector_cleanup(encoder: &mut ValueEncoder, progress: &VectorCleanupProgress) {
    match progress {
        VectorCleanupProgress::RetireCache(progress) => {
            encoder.put_u8(0x02);
            put_no_cursor(encoder, *progress);
        }
        VectorCleanupProgress::DeletePhysical(progress) => {
            encoder.put_u8(0x03);
            put_prefix_scan(encoder, progress);
        }
        VectorCleanupProgress::DeleteDeltas(progress) => {
            encoder.put_u8(0x04);
            put_prefix_scan(encoder, progress);
        }
        VectorCleanupProgress::Finalize(progress) => {
            encoder.put_u8(0x06);
            put_no_cursor(encoder, *progress);
        }
    }
}

fn put_text_cleanup(encoder: &mut ValueEncoder, progress: &TextCleanupProgress) {
    match progress {
        TextCleanupProgress::DeleteMetadata(progress) => {
            encoder.put_u8(0x01);
            put_prefix_scan(encoder, progress);
        }
        TextCleanupProgress::Finalize(progress) => {
            encoder.put_u8(0x02);
            put_no_cursor(encoder, *progress);
        }
    }
}

fn take_operation_progress(
    decoder: &mut ValueDecoder<'_>,
    kind: IndexOperationKind,
    family: IndexOperationFamily,
) -> Result<IndexOperationProgress, EncodingError> {
    match (kind, family) {
        (IndexOperationKind::Build, family) => {
            let mode = decoder.take_u8()?;
            match (family, mode) {
                (IndexOperationFamily::Secondary, 0x01) => {
                    Ok(IndexOperationProgress::SecondaryBuild(
                        SecondaryBuildProgress::Constructing(match decoder.take_u8()? {
                            0x01 => SecondaryBuildStage::Scan(take_source_scan(decoder)?),
                            0x02 => SecondaryBuildStage::CatchUp(take_prefix_scan(decoder)?),
                            0x03 => SecondaryBuildStage::Validate(take_prefix_scan(decoder)?),
                            0x04 => SecondaryBuildStage::Activate(take_no_cursor(decoder)?),
                            unknown => {
                                return Err(unknown_discriminant("secondary build stage", unknown));
                            }
                        }),
                    ))
                }
                (IndexOperationFamily::Secondary, 0x02) => {
                    Ok(IndexOperationProgress::SecondaryBuild(
                        SecondaryBuildProgress::Aborting(take_secondary_cleanup(decoder)?),
                    ))
                }
                (IndexOperationFamily::Vector, 0x01) => Ok(IndexOperationProgress::VectorBuild(
                    VectorBuildProgress::Constructing(match decoder.take_u8()? {
                        0x01 => VectorBuildStage::Scan(take_source_scan(decoder)?),
                        0x02 => VectorBuildStage::CatchUp(take_prefix_scan(decoder)?),
                        0x03 => VectorBuildStage::ValidateDescriptor(take_prefix_scan(decoder)?),
                        0x04 => VectorBuildStage::Activate(take_no_cursor(decoder)?),
                        0x05 => {
                            VectorBuildStage::AdoptLegacy(take_legacy_vector_validation(decoder)?)
                        }
                        0x06 => VectorBuildStage::ValidateAdoptedDirectory(
                            take_legacy_vector_directory_validation(decoder)?,
                        ),
                        unknown => {
                            return Err(unknown_discriminant("vector build stage", unknown));
                        }
                    }),
                )),
                (IndexOperationFamily::Vector, 0x02) => Ok(IndexOperationProgress::VectorBuild(
                    VectorBuildProgress::Aborting(take_vector_cleanup(decoder)?),
                )),
                (IndexOperationFamily::Text, 0x01) => Ok(IndexOperationProgress::TextBuild(
                    TextBuildProgress::Constructing(match decoder.take_u8()? {
                        0x01 => TextBuildStage::ScanSource(take_source_scan(decoder)?),
                        0x02 => TextBuildStage::ScanPartitions(take_source_scan(decoder)?),
                        0x03 => TextBuildStage::CatchUp(take_prefix_scan(decoder)?),
                        0x04 => TextBuildStage::Compact(take_prefix_scan(decoder)?),
                        0x05 => TextBuildStage::PrepareManifests(take_prefix_scan(decoder)?),
                        0x06 => TextBuildStage::ValidateManifests(take_text_manifest_validation(
                            decoder,
                        )?),
                        0x07 => TextBuildStage::Activate(take_no_cursor(decoder)?),
                        unknown => {
                            return Err(unknown_discriminant("text build stage", unknown));
                        }
                    }),
                )),
                (IndexOperationFamily::Text, 0x02) => Ok(IndexOperationProgress::TextBuild(
                    TextBuildProgress::Aborting(take_text_cleanup(decoder)?),
                )),
                (_, unknown) => Err(unknown_discriminant("build progress mode", unknown)),
            }
        }
        (IndexOperationKind::Drop, IndexOperationFamily::Secondary) => Ok(
            IndexOperationProgress::SecondaryCleanup(take_secondary_cleanup(decoder)?),
        ),
        (IndexOperationKind::Drop, IndexOperationFamily::Vector) => Ok(
            IndexOperationProgress::VectorCleanup(take_vector_cleanup(decoder)?),
        ),
        (IndexOperationKind::Drop, IndexOperationFamily::Text) => Ok(
            IndexOperationProgress::TextCleanup(take_text_cleanup(decoder)?),
        ),
    }
}

fn take_secondary_cleanup(
    decoder: &mut ValueDecoder<'_>,
) -> Result<SecondaryCleanupProgress, EncodingError> {
    Ok(match decoder.take_u8()? {
        0x01 => SecondaryCleanupProgress::DeleteEntries(PrefixScanProgress {
            cursor: None,
            counters: take_legacy_drain_counters(decoder)?,
        }),
        0x02 => SecondaryCleanupProgress::DeleteEntries(take_prefix_scan(decoder)?),
        0x03 => SecondaryCleanupProgress::DeleteDeltas(take_prefix_scan(decoder)?),
        0x04 => SecondaryCleanupProgress::Finalize(NoCursorProgress {
            counters: take_legacy_drain_counters(decoder)?,
        }),
        0x05 => SecondaryCleanupProgress::Finalize(take_no_cursor(decoder)?),
        unknown => {
            return Err(unknown_discriminant("secondary cleanup stage", unknown));
        }
    })
}

fn take_vector_cleanup(
    decoder: &mut ValueDecoder<'_>,
) -> Result<VectorCleanupProgress, EncodingError> {
    Ok(match decoder.take_u8()? {
        0x01 => VectorCleanupProgress::RetireCache(NoCursorProgress {
            counters: take_legacy_drain_counters(decoder)?,
        }),
        0x02 => VectorCleanupProgress::RetireCache(take_no_cursor(decoder)?),
        0x03 => VectorCleanupProgress::DeletePhysical(take_prefix_scan(decoder)?),
        0x04 => VectorCleanupProgress::DeleteDeltas(take_prefix_scan(decoder)?),
        0x05 => VectorCleanupProgress::Finalize(NoCursorProgress {
            counters: take_legacy_drain_counters(decoder)?,
        }),
        0x06 => VectorCleanupProgress::Finalize(take_no_cursor(decoder)?),
        unknown => {
            return Err(unknown_discriminant("vector cleanup stage", unknown));
        }
    })
}

fn take_text_cleanup(decoder: &mut ValueDecoder<'_>) -> Result<TextCleanupProgress, EncodingError> {
    Ok(match decoder.take_u8()? {
        0x01 => TextCleanupProgress::DeleteMetadata(take_prefix_scan(decoder)?),
        0x02 => TextCleanupProgress::Finalize(take_no_cursor(decoder)?),
        unknown => return Err(unknown_discriminant("text cleanup stage", unknown)),
    })
}

fn put_source_scan(encoder: &mut ValueEncoder, progress: &SourceScanProgress) {
    encoder.put_bytes(progress.inclusive_upper_bound.as_bytes());
    put_cursor(encoder, progress.cursor.as_ref());
    put_counters(encoder, progress.counters);
}

fn take_source_scan(decoder: &mut ValueDecoder<'_>) -> Result<SourceScanProgress, EncodingError> {
    Ok(SourceScanProgress {
        inclusive_upper_bound: crate::index_lifecycle::IndexCursor::try_new(
            decoder.take_bytes(crate::index_lifecycle::INDEX_CURSOR_MAX_LEN)?,
        )
        .map_err(operation_model_error)?,
        cursor: take_cursor(decoder)?,
        counters: take_counters(decoder)?,
    })
}

fn put_prefix_scan(encoder: &mut ValueEncoder, progress: &PrefixScanProgress) {
    put_cursor(encoder, progress.cursor.as_ref());
    put_counters(encoder, progress.counters);
}

fn take_prefix_scan(decoder: &mut ValueDecoder<'_>) -> Result<PrefixScanProgress, EncodingError> {
    Ok(PrefixScanProgress {
        cursor: take_cursor(decoder)?,
        counters: take_counters(decoder)?,
    })
}

fn put_legacy_vector_validation(
    encoder: &mut ValueEncoder,
    progress: &LegacyVectorValidationProgress,
) {
    encoder.put_u8(match progress.lane {
        LegacyVectorValidationLane::Core => 0x01,
        LegacyVectorValidationLane::Hot => 0x02,
        LegacyVectorValidationLane::Layer0 => 0x03,
    });
    put_cursor(encoder, progress.cursor.as_ref());
    put_counters(encoder, progress.counters);
}

fn take_legacy_vector_validation(
    decoder: &mut ValueDecoder<'_>,
) -> Result<LegacyVectorValidationProgress, EncodingError> {
    let lane = match decoder.take_u8()? {
        0x01 => LegacyVectorValidationLane::Core,
        0x02 => LegacyVectorValidationLane::Hot,
        0x03 => LegacyVectorValidationLane::Layer0,
        unknown => {
            return Err(unknown_discriminant(
                "legacy vector validation lane",
                unknown,
            ))
        }
    };
    Ok(LegacyVectorValidationProgress {
        lane,
        cursor: take_cursor(decoder)?,
        counters: take_counters(decoder)?,
    })
}

fn put_legacy_vector_directory_validation(
    encoder: &mut ValueEncoder,
    progress: &LegacyVectorDirectoryValidationProgress,
) {
    put_cursor(encoder, progress.cursor.as_ref());
    encoder.put_u64(progress.expected_markers);
    encoder.put_u64(progress.verified_markers);
    put_counters(encoder, progress.counters);
}

fn take_legacy_vector_directory_validation(
    decoder: &mut ValueDecoder<'_>,
) -> Result<LegacyVectorDirectoryValidationProgress, EncodingError> {
    let progress = LegacyVectorDirectoryValidationProgress {
        cursor: take_cursor(decoder)?,
        expected_markers: decoder.take_u64()?,
        verified_markers: decoder.take_u64()?,
        counters: take_counters(decoder)?,
    };
    if progress.verified_markers > progress.expected_markers {
        return Err(EncodingError::Custom(
            "verified legacy vector directory markers exceed the expected count".to_string(),
        ));
    }
    Ok(progress)
}

fn put_text_manifest_validation(
    encoder: &mut ValueEncoder,
    progress: &TextManifestValidationProgress,
) {
    match progress {
        TextManifestValidationProgress::Pages(progress) => {
            encoder.put_u8(0x01);
            put_cursor(encoder, progress.cursor());
            put_option(encoder, progress.partition(), |encoder, partition| {
                encoder.put_raw(partition.partition_fingerprint());
                encoder.put_u64(partition.root_revision().get());
                encoder.put_u32(partition.page_count());
                encoder.put_u64(partition.split_count());
                encoder.put_u32(partition.next_page());
                encoder.put_u64(partition.observed_split_count());
            });
            put_counters(encoder, progress.counters());
        }
        TextManifestValidationProgress::Roots(progress) => {
            encoder.put_u8(0x02);
            put_prefix_scan(encoder, progress);
        }
        TextManifestValidationProgress::EntityStates(progress) => {
            encoder.put_u8(0x03);
            put_prefix_scan(encoder, progress);
        }
    }
}

fn take_text_manifest_validation(
    decoder: &mut ValueDecoder<'_>,
) -> Result<TextManifestValidationProgress, EncodingError> {
    match decoder.take_u8()? {
        0x01 => {
            let cursor = take_cursor(decoder)?;
            let partition = decoder.take_option(|decoder| {
                TextManifestPartitionValidation::try_new(
                    decoder.take_array::<32>()?,
                    take_manifest_revision(decoder)?,
                    decoder.take_u32()?,
                    decoder.take_u64()?,
                    decoder.take_u32()?,
                    decoder.take_u64()?,
                )
                .map_err(operation_model_error)
            })?;
            let counters = take_counters(decoder)?;
            TextManifestPageValidationProgress::try_new(cursor, partition, counters)
                .map(TextManifestValidationProgress::Pages)
                .map_err(operation_model_error)
        }
        0x02 => take_prefix_scan(decoder).map(TextManifestValidationProgress::Roots),
        0x03 => take_prefix_scan(decoder).map(TextManifestValidationProgress::EntityStates),
        unknown => Err(unknown_discriminant(
            "text manifest validation lane",
            unknown,
        )),
    }
}

fn put_no_cursor(encoder: &mut ValueEncoder, progress: NoCursorProgress) {
    put_counters(encoder, progress.counters);
}

fn take_no_cursor(decoder: &mut ValueDecoder<'_>) -> Result<NoCursorProgress, EncodingError> {
    Ok(NoCursorProgress {
        counters: take_counters(decoder)?,
    })
}

fn take_legacy_drain_counters(
    decoder: &mut ValueDecoder<'_>,
) -> Result<crate::index_lifecycle::OperationCounters, EncodingError> {
    let _legacy_drain_epoch = decoder.take_option(ValueDecoder::take_u64)?;
    take_counters(decoder)
}

fn put_execution_state(
    encoder: &mut ValueEncoder,
    kind: IndexOperationKind,
    state: &IndexOperationExecutionState,
    queue_schedule: Option<IndexOperationQueueSchedule>,
) {
    match state {
        IndexOperationExecutionState::Queued { .. } => {
            encoder.put_u8(0x01);
            match queue_schedule.expect("validated queued operation has one schedule") {
                IndexOperationQueueSchedule::Immediate => encoder.put_u8(0x01),
                IndexOperationQueueSchedule::DelayedAfterProgress {
                    not_before_unix_millis,
                } => {
                    encoder.put_u8(0x02);
                    encoder.put_u64(not_before_unix_millis);
                }
                IndexOperationQueueSchedule::DelayedAfterTransientFailure {
                    not_before_unix_millis,
                    failed_writer_epoch,
                } => {
                    encoder.put_u8(0x03);
                    encoder.put_u64(not_before_unix_millis);
                    put_writer_epoch(encoder, failed_writer_epoch);
                }
            }
        }
        IndexOperationExecutionState::Claimed(claim) => {
            encoder.put_u8(0x02);
            put_claim(encoder, *claim);
        }
        IndexOperationExecutionState::Blocked(blocker) => {
            encoder.put_u8(0x03);
            put_blocker(encoder, blocker);
        }
        IndexOperationExecutionState::Completed(outcome) => {
            encoder.put_u8(0x04);
            match (kind, outcome) {
                (IndexOperationKind::Build, IndexOperationOutcome::Build(outcome)) => {
                    encoder.put_u8(*outcome as u8)
                }
                (IndexOperationKind::Drop, IndexOperationOutcome::DropSucceeded) => {
                    encoder.put_u8(0x01)
                }
                _ => unreachable!("validated operation outcome matches kind"),
            }
        }
    }
}

struct DecodedExecutionState {
    state: IndexOperationExecutionState,
    queue_schedule: Option<IndexOperationQueueSchedule>,
    legacy_reader_coordination_blocker: bool,
}

fn take_execution_state(
    decoder: &mut ValueDecoder<'_>,
    kind: IndexOperationKind,
) -> Result<DecodedExecutionState, EncodingError> {
    match decoder.take_u8()? {
        0x01 => {
            let queue_schedule = match decoder.take_u8()? {
                0x01 => IndexOperationQueueSchedule::Immediate,
                0x02 => IndexOperationQueueSchedule::DelayedAfterProgress {
                    not_before_unix_millis: decoder.take_u64()?,
                },
                0x03 => IndexOperationQueueSchedule::DelayedAfterTransientFailure {
                    not_before_unix_millis: decoder.take_u64()?,
                    failed_writer_epoch: take_writer_epoch(decoder)?,
                },
                unknown => return Err(unknown_discriminant("operation queue schedule", unknown)),
            };
            Ok(DecodedExecutionState {
                state: IndexOperationExecutionState::Queued {
                    not_before_unix_millis: queue_schedule.not_before_unix_millis(),
                },
                queue_schedule: Some(queue_schedule),
                legacy_reader_coordination_blocker: false,
            })
        }
        0x02 => Ok(DecodedExecutionState {
            state: IndexOperationExecutionState::Claimed(take_claim(decoder)?),
            queue_schedule: None,
            legacy_reader_coordination_blocker: false,
        }),
        0x03 => {
            let blocker = take_blocker(decoder)?;
            let (blocker, legacy_reader_coordination_blocker) = match blocker {
                DecodedIndexOperationBlocker::Current(blocker) => (blocker, false),
                DecodedIndexOperationBlocker::LegacyReaderCoordinationUnavailable => (
                    crate::index_lifecycle::IndexOperationBlocker::InvariantViolation,
                    true,
                ),
            };
            Ok(DecodedExecutionState {
                state: IndexOperationExecutionState::Blocked(blocker),
                queue_schedule: None,
                legacy_reader_coordination_blocker,
            })
        }
        0x04 => match (kind, decoder.take_u8()?) {
            (IndexOperationKind::Build, 0x01) => Ok(DecodedExecutionState {
                state: IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Succeeded,
                )),
                queue_schedule: None,
                legacy_reader_coordination_blocker: false,
            }),
            (IndexOperationKind::Build, 0x02) => Ok(DecodedExecutionState {
                state: IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                    BuildOperationOutcome::Aborted,
                )),
                queue_schedule: None,
                legacy_reader_coordination_blocker: false,
            }),
            (IndexOperationKind::Drop, 0x01) => Ok(DecodedExecutionState {
                state: IndexOperationExecutionState::Completed(
                    IndexOperationOutcome::DropSucceeded,
                ),
                queue_schedule: None,
                legacy_reader_coordination_blocker: false,
            }),
            (_, unknown) => Err(unknown_discriminant("operation outcome", unknown)),
        },
        unknown => Err(unknown_discriminant("operation execution state", unknown)),
    }
}

#[cfg(test)]
mod wire_fixtures {
    use std::fmt::Write;

    use bytes::Bytes;

    use super::*;
    use crate::config::SecondaryIndexDefinition;
    use crate::encoding::v2::keys::indexes::CanonicalSecondaryValue;
    use crate::encoding::v2::keys::scope::{DataScope, TenantId};
    use crate::encoding::v2::values::global::{decode_metadata_value, encode_metadata_value};
    use crate::index_lifecycle::{
        IndexCursor, IndexElementKind, IndexGenerationId, IndexId, IndexOperationExecutionState,
        IndexOperationId, IndexOperationRevision, IndexRevision, IndexStorageVersion,
        IndexV2MetadataValue, LegacyVectorPhysicalReservation, LogicalIndexIdWatermark,
        OperationCounters, OperationQueuePointerValue, PhysicalGeneration, SourceScanProgress,
        TextCompactionPointerValue, TextManifestRevision, ValidatedDynamicIndexDefinition,
        VectorPhysicalIdWatermark, VectorPhysicalIndexId,
    };

    fn index_id() -> IndexId {
        IndexId::new(1).unwrap()
    }

    fn generation() -> IndexGenerationId {
        IndexGenerationId::new(2).unwrap()
    }

    fn operation_id() -> IndexOperationId {
        IndexOperationId::from_bytes([0x11; 16]).unwrap()
    }

    fn definition() -> ValidatedDynamicIndexDefinition {
        SecondaryIndexDefinition::node_equality("L", "p")
            .unwrap()
            .try_into()
            .unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
    }

    #[test]
    fn lifecycle_work_value_bytes_are_frozen() {
        let delta = CoalescedBuildDeltaValue {
            index_id: index_id(),
            generation: generation(),
            entity_kind: IndexElementKind::Node,
            entity_id: crate::index_lifecycle::IndexEntityId::new(3),
            state: CoalescedBuildDeltaState::Marker,
        };
        let secondary_delta = CoalescedBuildDeltaValue {
            state: CoalescedBuildDeltaState::SecondaryBefore(Some(
                CanonicalSecondaryValue::equality_string("shared"),
            )),
            ..delta.clone()
        };
        let tenant_partition = crate::index_lifecycle::work::TextPartition::try_tenant_value(
            Bytes::from_static(b"tenant"),
        )
        .unwrap();
        let vector_delta = CoalescedBuildDeltaValue {
            state: CoalescedBuildDeltaState::VectorBefore(Some(tenant_partition.clone())),
            ..delta.clone()
        };
        let applied = AppliedEntityStateValue {
            index_id: index_id(),
            generation: generation(),
            entity_kind: IndexElementKind::Node,
            entity_id: crate::index_lifecycle::IndexEntityId::new(3),
            state: AppliedFamilyState::Secondary(Some(CanonicalSecondaryValue::equality_string(
                "shared",
            ))),
        };
        let applied_vector = AppliedEntityStateValue {
            index_id: index_id(),
            generation: generation(),
            entity_kind: IndexElementKind::Node,
            entity_id: crate::index_lifecycle::IndexEntityId::new(3),
            state: AppliedFamilyState::Vector(Some(tenant_partition.clone())),
        };
        let applied_text = AppliedEntityStateValue {
            index_id: index_id(),
            generation: generation(),
            entity_kind: IndexElementKind::Node,
            entity_id: crate::index_lifecycle::IndexEntityId::new(3),
            state: AppliedFamilyState::Text(Some((
                tenant_partition,
                crate::index_lifecycle::TextLogicalVersion::new(4).unwrap(),
            ))),
        };
        let encoded_delta = encode_build_delta(&delta);
        let encoded_secondary_delta = encode_build_delta(&secondary_delta);
        let encoded_vector_delta = encode_build_delta(&vector_delta);
        let encoded_applied = encode_applied_state(&applied);
        let encoded_vector = encode_applied_state(&applied_vector);
        let encoded_text = encode_applied_state(&applied_text);

        assert_eq!(decode_build_delta(&encoded_delta).unwrap(), delta);
        assert_eq!(
            decode_build_delta(&encoded_secondary_delta).unwrap(),
            secondary_delta
        );
        assert_eq!(
            decode_build_delta(&encoded_vector_delta).unwrap(),
            vector_delta
        );
        assert_eq!(decode_applied_state(&encoded_applied).unwrap(), applied);
        assert_eq!(
            decode_applied_state(&encoded_vector).unwrap(),
            applied_vector
        );
        assert_eq!(decode_applied_state(&encoded_text).unwrap(), applied_text);
        insta::assert_snapshot!(
            format!(
                "build_delta={}\nbuild_delta_secondary={}\nbuild_delta_vector={}\napplied_secondary={}\napplied_vector={}\napplied_text={}\n",
                hex(&encoded_delta),
                hex(&encoded_secondary_delta),
                hex(&encoded_vector_delta),
                hex(&encoded_applied),
                hex(&encoded_vector),
                hex(&encoded_text),
            ),
            @"
build_delta=010300000000000000010000000000000002010000000000000003
build_delta_secondary=010300000000000000010000000000000002010000000000000003010101e9cf50951f33fb140000000b0400000006736861726564
build_delta_vector=0103000000000000000100000000000000020100000000000000030201020000000674656e616e74
applied_secondary=010400000000000000010000000000000002010000000000000003010101e9cf50951f33fb140000000b0400000006736861726564
applied_vector=0104000000000000000100000000000000020100000000000000030201020000000674656e616e74
applied_text=0104000000000000000100000000000000020100000000000000030301020000000674656e616e740000000000000004
"
        );
    }

    #[test]
    fn lifecycle_record_value_bytes_are_frozen() {
        let definition = definition();
        let record = IndexRecordV2::building(
            index_id(),
            definition.clone(),
            IndexRevision::new(3).unwrap(),
            PhysicalGeneration::Secondary {
                generation: generation(),
            },
            operation_id(),
        )
        .unwrap();
        let operation = IndexOperationRecord::try_new(
            operation_id(),
            index_id(),
            definition.identity(),
            generation(),
            IndexRevision::new(3).unwrap(),
            IndexOperationRevision::new(4).unwrap(),
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Scan(SourceScanProgress {
                    inclusive_upper_bound: IndexCursor::try_new(Bytes::from_static(b"upper"))
                        .unwrap(),
                    cursor: Some(IndexCursor::try_new(Bytes::from_static(b"cursor")).unwrap()),
                    counters: OperationCounters {
                        entities: 5,
                        input_bytes: 6,
                        output_operations: 7,
                        output_bytes: 8,
                    },
                }),
            )),
            9,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();

        let encoded_record = encode_index_record(&record);
        let encoded_operation = encode_operation_record(&operation);
        assert_eq!(decode_index_record(&encoded_record).unwrap(), record);
        assert_eq!(
            decode_operation_record(&encoded_operation).unwrap(),
            operation
        );
        insta::assert_snapshot!(
            format!(
                "index_record={}\noperation_record={}\n",
                hex(&encoded_record),
                hex(&encoded_operation)
            ),
            @"
index_record=010100000000000000010101000000014c00000001700101000000014c00000001700000000000000000030101000000000000000211111111111111111111111111111111
operation_record=01021111111111111111111111111111111100000000000000010101000000014c0000000170000000000000000200000000000000030000000000000004010101010000000575707065720100000006637572736f720000000000000005000000000000000600000000000000070000000000000008000000090101
"
        );
    }

    #[test]
    fn global_value_bytes_are_frozen() {
        let values = [
            (
                "storage_version",
                IndexV2MetadataValue::StorageVersion(IndexStorageVersion::new(3).unwrap()),
            ),
            (
                "logical_index_watermark",
                IndexV2MetadataValue::LogicalIndexIdWatermark(LogicalIndexIdWatermark {
                    next_id: index_id(),
                }),
            ),
            (
                "vector_physical_watermark",
                IndexV2MetadataValue::VectorPhysicalIdWatermark(VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::new(4).unwrap(),
                }),
            ),
            (
                "operation_pointer",
                IndexV2MetadataValue::OperationQueuePointer(OperationQueuePointerValue {
                    scope: DataScope::Tenant(TenantId::from_u128(7)),
                    index_id: index_id(),
                    generation: generation(),
                    record_revision: IndexOperationRevision::new(4).unwrap(),
                }),
            ),
            (
                "legacy_vector_source",
                IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    LegacyVectorPhysicalReservation::LegacySource,
                ),
            ),
            (
                "legacy_vector_adoption",
                IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    LegacyVectorPhysicalReservation::AdoptionBuilding {
                        index_id: index_id(),
                        generation: generation(),
                        operation_id: operation_id(),
                    },
                ),
            ),
            (
                "legacy_vector_active",
                IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    LegacyVectorPhysicalReservation::AdoptedActive {
                        index_id: index_id(),
                        generation: generation(),
                    },
                ),
            ),
            (
                "legacy_vector_retiring",
                IndexV2MetadataValue::LegacyVectorPhysicalReservation(
                    LegacyVectorPhysicalReservation::RetiringSource {
                        index_id: index_id(),
                        generation: generation(),
                    },
                ),
            ),
            (
                "text_compaction_pointer",
                IndexV2MetadataValue::TextCompactionPointer(TextCompactionPointerValue {
                    revision: TextManifestRevision::new(4).unwrap(),
                }),
            ),
            (
                "membership_delta_legacy",
                IndexV2MetadataValue::MembershipDeltaWriteMode(
                    crate::MembershipDeltaWriteMode::LegacyExclusive,
                ),
            ),
            (
                "membership_delta_disjoint_v2",
                IndexV2MetadataValue::MembershipDeltaWriteMode(
                    crate::MembershipDeltaWriteMode::DisjointV2,
                ),
            ),
        ];

        let mut rendered = String::new();
        for (name, value) in values {
            let encoded = encode_metadata_value(&value);
            assert_eq!(decode_metadata_value(&encoded).unwrap(), value);
            writeln!(rendered, "{name}={}", hex(&encoded)).expect("writing to String cannot fail");
        }
        insta::assert_snapshot!(rendered, @"
storage_version=01010003
logical_index_watermark=01020000000000000001
vector_physical_watermark=01030000000000000004
operation_pointer=01040100000000000000000000000000000007000000000000000100000000000000020000000000000004
legacy_vector_source=010601
legacy_vector_adoption=0106020000000000000001000000000000000211111111111111111111111111111111
legacy_vector_active=01060300000000000000010000000000000002
legacy_vector_retiring=01060400000000000000010000000000000002
text_compaction_pointer=01070000000000000004
membership_delta_legacy=010800
membership_delta_disjoint_v2=010801
");
    }
}
