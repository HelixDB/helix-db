//! Canonical metadata, logical index-record, and operation codecs.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::index_v2::TEXT_COMPACTION_INPUT_KEY_MAX;
use crate::index_v2::{
    BuildOperationOutcome, DrainProgress, GcProgress, IndexOperationExecutionState,
    IndexOperationFamily, IndexOperationKind, IndexOperationOutcome, IndexOperationProgress,
    IndexOperationRecord, IndexRecordV2, IndexStateV2, LogicalIndexIdWatermark, NoCursorProgress,
    OperationQueuePointerValue, PrefixScanProgress, SecondaryBuildProgress, SecondaryBuildStage,
    SecondaryCleanupProgress, SourceScanProgress, TextBuildProgress, TextBuildStage,
    TextBuildUploadProgress, TextCatchUpUploadProgress, TextCleanupProgress,
    TextCompactionUploadProgress, TextManifestPageValidationProgress,
    TextManifestPartitionValidation, TextManifestValidationProgress, UploadQueuePointerValue,
    VectorBuildProgress, VectorBuildStage, VectorCleanupProgress, VectorPhysicalIdWatermark,
};
use crate::index_v2::{IndexStorageVersion, IndexV2MetadataValue};

use super::codec::*;

const INDEX_RECORD_KIND: u8 = 0x01;
const OPERATION_RECORD_KIND: u8 = 0x02;

/// Encodes the only persisted logical index record.
pub(crate) fn encode_index_record(record: &IndexRecordV2) -> Bytes {
    let mut encoder = ValueEncoder::with_header(INDEX_RECORD_KIND);
    put_index_id(&mut encoder, record.index_id());
    put_identity(&mut encoder, record.identity());
    put_definition(&mut encoder, record.definition());
    put_revision(&mut encoder, record.revision());
    put_index_state(&mut encoder, record.state());
    encoder.finish()
}

/// Decodes and cross-validates a canonical logical index record.
pub(crate) fn decode_index_record(value: &[u8]) -> Result<IndexRecordV2, EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    if decoder.kind() != INDEX_RECORD_KIND {
        return Err(unknown_discriminant("index record kind", decoder.kind()));
    }
    let index_id = take_index_id(&mut decoder)?;
    let identity = take_identity(&mut decoder)?;
    let definition = take_definition(&mut decoder)?;
    let revision = take_revision(&mut decoder)?;
    let state = take_index_state(&mut decoder)?;
    decoder.finish()?;
    IndexRecordV2::try_new(index_id, identity, definition, revision, state).map_err(model_error)
}

fn put_index_state(encoder: &mut ValueEncoder, state: &IndexStateV2) {
    match state {
        IndexStateV2::Building {
            physical,
            build_operation_id,
        } => {
            encoder.put_u8(0x01);
            put_physical_generation(encoder, physical);
            put_operation_id(encoder, *build_operation_id);
        }
        IndexStateV2::Active {
            physical,
            completed_build_operation_id,
        } => {
            encoder.put_u8(0x02);
            put_physical_generation(encoder, physical);
            put_operation_id(encoder, *completed_build_operation_id);
        }
        IndexStateV2::Aborting {
            physical,
            build_operation_id,
        } => {
            encoder.put_u8(0x03);
            put_physical_generation(encoder, physical);
            put_operation_id(encoder, *build_operation_id);
        }
        IndexStateV2::Dropping {
            physical,
            drop_operation_id,
        } => {
            encoder.put_u8(0x04);
            put_physical_generation(encoder, physical);
            put_operation_id(encoder, *drop_operation_id);
        }
        IndexStateV2::Dropped {
            last_generation,
            completed_operation_id,
        } => {
            encoder.put_u8(0x05);
            put_generation(encoder, *last_generation);
            put_operation_id(encoder, *completed_operation_id);
        }
    }
}

fn take_index_state(decoder: &mut ValueDecoder<'_>) -> Result<IndexStateV2, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(IndexStateV2::Building {
            physical: take_physical_generation(decoder)?,
            build_operation_id: take_operation_id(decoder)?,
        }),
        0x02 => Ok(IndexStateV2::Active {
            physical: take_physical_generation(decoder)?,
            completed_build_operation_id: take_operation_id(decoder)?,
        }),
        0x03 => Ok(IndexStateV2::Aborting {
            physical: take_physical_generation(decoder)?,
            build_operation_id: take_operation_id(decoder)?,
        }),
        0x04 => Ok(IndexStateV2::Dropping {
            physical: take_physical_generation(decoder)?,
            drop_operation_id: take_operation_id(decoder)?,
        }),
        0x05 => Ok(IndexStateV2::Dropped {
            last_generation: take_generation(decoder)?,
            completed_operation_id: take_operation_id(decoder)?,
        }),
        unknown => Err(unknown_discriminant("index state", unknown)),
    }
}

/// Encodes a typed global V2 metadata/pointer value.
pub(crate) fn encode_metadata_value(value: &IndexV2MetadataValue) -> Bytes {
    let kind = match value {
        IndexV2MetadataValue::StorageVersion(_) => 0x01,
        IndexV2MetadataValue::LogicalIndexIdWatermark(_) => 0x02,
        IndexV2MetadataValue::VectorPhysicalIdWatermark(_) => 0x03,
        IndexV2MetadataValue::OperationQueuePointer(_) => 0x04,
        IndexV2MetadataValue::UploadQueuePointer(_) => 0x05,
    };
    let mut encoder = ValueEncoder::with_header(kind);
    match value {
        IndexV2MetadataValue::StorageVersion(version) => encoder.put_u16(version.get()),
        IndexV2MetadataValue::LogicalIndexIdWatermark(watermark) => {
            put_index_id(&mut encoder, watermark.next_id)
        }
        IndexV2MetadataValue::VectorPhysicalIdWatermark(watermark) => {
            encoder.put_u64(watermark.next_id.get())
        }
        IndexV2MetadataValue::OperationQueuePointer(pointer) => {
            put_scope(&mut encoder, pointer.scope);
            put_index_id(&mut encoder, pointer.index_id);
            put_generation(&mut encoder, pointer.generation);
            put_operation_revision(&mut encoder, pointer.record_revision);
        }
        IndexV2MetadataValue::UploadQueuePointer(pointer) => {
            put_scope(&mut encoder, pointer.scope);
            put_index_id(&mut encoder, pointer.index_id);
            put_generation(&mut encoder, pointer.generation);
            put_intent_revision(&mut encoder, pointer.record_revision);
        }
    }
    encoder.finish()
}

/// Decodes a global V2 metadata/pointer value under key context.
pub(crate) fn decode_metadata_value(value: &[u8]) -> Result<IndexV2MetadataValue, EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    let decoded = match decoder.kind() {
        0x01 => {
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::new(decoder.take_u16()?)?)
        }
        0x02 => IndexV2MetadataValue::LogicalIndexIdWatermark(LogicalIndexIdWatermark {
            next_id: take_index_id(&mut decoder)?,
        }),
        0x03 => IndexV2MetadataValue::VectorPhysicalIdWatermark(VectorPhysicalIdWatermark {
            next_id: crate::index_v2::VectorPhysicalIndexId::new(decoder.take_u64()?)
                .map_err(model_error)?,
        }),
        0x04 => IndexV2MetadataValue::OperationQueuePointer(OperationQueuePointerValue {
            scope: take_scope(&mut decoder)?,
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            record_revision: take_operation_revision(&mut decoder)?,
        }),
        0x05 => IndexV2MetadataValue::UploadQueuePointer(UploadQueuePointerValue {
            scope: take_scope(&mut decoder)?,
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            record_revision: take_intent_revision(&mut decoder)?,
        }),
        unknown => return Err(unknown_discriminant("metadata value", unknown)),
    };
    decoder.finish()?;
    Ok(decoded)
}

/// Encodes one durable outbox operation.
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
    put_execution_state(&mut encoder, record.kind(), record.execution_state());
    encoder.finish()
}

/// Decodes and cross-validates one durable outbox operation.
pub(crate) fn decode_operation_record(value: &[u8]) -> Result<IndexOperationRecord, EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    if decoder.kind() != OPERATION_RECORD_KIND {
        return Err(unknown_discriminant(
            "operation record kind",
            decoder.kind(),
        ));
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
    let execution_state = take_execution_state(&mut decoder, kind)?;
    decoder.finish()?;
    IndexOperationRecord::try_new(
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
    )
    .map_err(operation_model_error)
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
                        encoder.put_u8(0x07);
                        put_source_scan(encoder, progress);
                    }
                    TextBuildStage::ScanPartitions(progress) => {
                        encoder.put_u8(0x01);
                        put_source_scan(encoder, progress);
                    }
                    TextBuildStage::AwaitUpload(progress) => {
                        encoder.put_u8(0x06);
                        put_text_build_upload(encoder, progress);
                    }
                    TextBuildStage::CatchUp(progress) => {
                        encoder.put_u8(0x02);
                        put_prefix_scan(encoder, progress);
                    }
                    TextBuildStage::AwaitCatchUpUpload(progress) => {
                        encoder.put_u8(0x08);
                        put_text_catch_up_upload(encoder, progress);
                    }
                    TextBuildStage::Compact(progress) => {
                        encoder.put_u8(0x03);
                        put_prefix_scan(encoder, progress);
                    }
                    TextBuildStage::AwaitCompactionUpload(progress) => {
                        encoder.put_u8(0x09);
                        put_text_compaction_upload(encoder, progress);
                    }
                    TextBuildStage::PrepareManifests(progress) => {
                        encoder.put_u8(0x04);
                        put_prefix_scan(encoder, progress);
                    }
                    TextBuildStage::ValidateManifests(progress) => {
                        encoder.put_u8(0x0A);
                        put_text_manifest_validation(encoder, progress);
                    }
                    TextBuildStage::Activate(progress) => {
                        encoder.put_u8(0x05);
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
        SecondaryCleanupProgress::BeginDrain(progress) => {
            encoder.put_u8(0x01);
            put_drain(encoder, *progress);
        }
        SecondaryCleanupProgress::DeleteEntries(progress) => {
            encoder.put_u8(0x02);
            put_prefix_scan(encoder, progress);
        }
        SecondaryCleanupProgress::DeleteDeltas(progress) => {
            encoder.put_u8(0x03);
            put_prefix_scan(encoder, progress);
        }
        SecondaryCleanupProgress::FinishDrain(progress) => {
            encoder.put_u8(0x04);
            put_drain(encoder, *progress);
        }
        SecondaryCleanupProgress::Finalize(progress) => {
            encoder.put_u8(0x05);
            put_no_cursor(encoder, *progress);
        }
    }
}

fn put_vector_cleanup(encoder: &mut ValueEncoder, progress: &VectorCleanupProgress) {
    match progress {
        VectorCleanupProgress::BeginDrain(progress) => {
            encoder.put_u8(0x01);
            put_drain(encoder, *progress);
        }
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
        VectorCleanupProgress::FinishDrain(progress) => {
            encoder.put_u8(0x05);
            put_drain(encoder, *progress);
        }
        VectorCleanupProgress::Finalize(progress) => {
            encoder.put_u8(0x06);
            put_no_cursor(encoder, *progress);
        }
    }
}

fn put_text_cleanup(encoder: &mut ValueEncoder, progress: &TextCleanupProgress) {
    match progress {
        TextCleanupProgress::BeginDrain(progress) => {
            encoder.put_u8(0x01);
            put_drain(encoder, *progress);
        }
        TextCleanupProgress::PrepareCandidates(progress) => {
            encoder.put_u8(0x02);
            put_prefix_scan(encoder, progress);
        }
        TextCleanupProgress::AcquireDeleteFences(progress) => {
            encoder.put_u8(0x03);
            put_gc(encoder, progress);
        }
        TextCleanupProgress::RetireManifest(progress) => {
            encoder.put_u8(0x04);
            put_gc(encoder, progress);
        }
        TextCleanupProgress::RetireArtifacts(progress) => {
            encoder.put_u8(0x05);
            put_gc(encoder, progress);
        }
        TextCleanupProgress::RetireUploadIntents(progress) => {
            encoder.put_u8(0x06);
            put_gc(encoder, progress);
        }
        TextCleanupProgress::MarkReachability(progress) => {
            encoder.put_u8(0x07);
            put_gc(encoder, progress);
        }
        TextCleanupProgress::DeleteBlobs(progress) => {
            encoder.put_u8(0x08);
            put_gc(encoder, progress);
        }
        TextCleanupProgress::DeleteEntityState(progress) => {
            encoder.put_u8(0x09);
            put_prefix_scan(encoder, progress);
        }
        TextCleanupProgress::FinishDrain(progress) => {
            encoder.put_u8(0x0A);
            put_drain(encoder, *progress);
        }
        TextCleanupProgress::Finalize(progress) => {
            encoder.put_u8(0x0B);
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
                        0x01 => TextBuildStage::ScanPartitions(take_source_scan(decoder)?),
                        0x02 => TextBuildStage::CatchUp(take_prefix_scan(decoder)?),
                        0x03 => TextBuildStage::Compact(take_prefix_scan(decoder)?),
                        0x04 => TextBuildStage::PrepareManifests(take_prefix_scan(decoder)?),
                        0x05 => TextBuildStage::Activate(take_no_cursor(decoder)?),
                        0x06 => TextBuildStage::AwaitUpload(take_text_build_upload(decoder)?),
                        0x07 => TextBuildStage::ScanSource(take_source_scan(decoder)?),
                        0x08 => {
                            TextBuildStage::AwaitCatchUpUpload(take_text_catch_up_upload(decoder)?)
                        }
                        0x09 => TextBuildStage::AwaitCompactionUpload(take_text_compaction_upload(
                            decoder,
                        )?),
                        0x0A => TextBuildStage::ValidateManifests(take_text_manifest_validation(
                            decoder,
                        )?),
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
        0x01 => SecondaryCleanupProgress::BeginDrain(take_drain(decoder)?),
        0x02 => SecondaryCleanupProgress::DeleteEntries(take_prefix_scan(decoder)?),
        0x03 => SecondaryCleanupProgress::DeleteDeltas(take_prefix_scan(decoder)?),
        0x04 => SecondaryCleanupProgress::FinishDrain(take_drain(decoder)?),
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
        0x01 => VectorCleanupProgress::BeginDrain(take_drain(decoder)?),
        0x02 => VectorCleanupProgress::RetireCache(take_no_cursor(decoder)?),
        0x03 => VectorCleanupProgress::DeletePhysical(take_prefix_scan(decoder)?),
        0x04 => VectorCleanupProgress::DeleteDeltas(take_prefix_scan(decoder)?),
        0x05 => VectorCleanupProgress::FinishDrain(take_drain(decoder)?),
        0x06 => VectorCleanupProgress::Finalize(take_no_cursor(decoder)?),
        unknown => {
            return Err(unknown_discriminant("vector cleanup stage", unknown));
        }
    })
}

fn take_text_cleanup(decoder: &mut ValueDecoder<'_>) -> Result<TextCleanupProgress, EncodingError> {
    Ok(match decoder.take_u8()? {
        0x01 => TextCleanupProgress::BeginDrain(take_drain(decoder)?),
        0x02 => TextCleanupProgress::PrepareCandidates(take_prefix_scan(decoder)?),
        0x03 => TextCleanupProgress::AcquireDeleteFences(take_gc(decoder)?),
        0x04 => TextCleanupProgress::RetireManifest(take_gc(decoder)?),
        0x05 => TextCleanupProgress::RetireArtifacts(take_gc(decoder)?),
        0x06 => TextCleanupProgress::RetireUploadIntents(take_gc(decoder)?),
        0x07 => TextCleanupProgress::MarkReachability(take_gc(decoder)?),
        0x08 => TextCleanupProgress::DeleteBlobs(take_gc(decoder)?),
        0x09 => TextCleanupProgress::DeleteEntityState(take_prefix_scan(decoder)?),
        0x0A => TextCleanupProgress::FinishDrain(take_drain(decoder)?),
        0x0B => TextCleanupProgress::Finalize(take_no_cursor(decoder)?),
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
        inclusive_upper_bound: crate::index_v2::IndexCursor::try_new(
            decoder.take_bytes(crate::index_v2::INDEX_CURSOR_MAX_LEN)?,
        )
        .map_err(operation_model_error)?,
        cursor: take_cursor(decoder)?,
        counters: take_counters(decoder)?,
    })
}

fn put_text_build_upload(encoder: &mut ValueEncoder, progress: &TextBuildUploadProgress) {
    put_source_scan(encoder, progress.source());
    encoder.put_bytes(progress.completed_cursor().as_bytes());
    put_counters(encoder, progress.completed_counters());
    encoder.put_bytes(progress.artifact_key().as_bytes());
    put_intent_id(encoder, progress.intent_id());
}

fn take_text_build_upload(
    decoder: &mut ValueDecoder<'_>,
) -> Result<TextBuildUploadProgress, EncodingError> {
    let source = take_source_scan(decoder)?;
    let completed_cursor = crate::index_v2::IndexCursor::try_new(
        decoder.take_bytes(crate::index_v2::INDEX_CURSOR_MAX_LEN)?,
    )
    .map_err(operation_model_error)?;
    let completed_counters = take_counters(decoder)?;
    let artifact_key = crate::index_v2::IndexCursor::try_new(
        decoder.take_bytes(crate::index_v2::INDEX_CURSOR_MAX_LEN)?,
    )
    .map_err(operation_model_error)?;
    let intent_id = take_intent_id(decoder)?;
    TextBuildUploadProgress::try_new(
        source,
        completed_cursor,
        completed_counters,
        artifact_key,
        intent_id,
    )
    .map_err(operation_model_error)
}

fn put_text_catch_up_upload(encoder: &mut ValueEncoder, progress: &TextCatchUpUploadProgress) {
    put_prefix_scan(encoder, progress.catch_up());
    encoder.put_bytes(progress.delta_key().as_bytes());
    put_counters(encoder, progress.completed_counters());
    encoder.put_bytes(progress.artifact_key().as_bytes());
    put_intent_id(encoder, progress.intent_id());
}

fn take_text_catch_up_upload(
    decoder: &mut ValueDecoder<'_>,
) -> Result<TextCatchUpUploadProgress, EncodingError> {
    let catch_up = take_prefix_scan(decoder)?;
    let delta_key = crate::index_v2::IndexCursor::try_new(
        decoder.take_bytes(crate::index_v2::INDEX_CURSOR_MAX_LEN)?,
    )
    .map_err(operation_model_error)?;
    let completed_counters = take_counters(decoder)?;
    let artifact_key = crate::index_v2::IndexCursor::try_new(
        decoder.take_bytes(crate::index_v2::INDEX_CURSOR_MAX_LEN)?,
    )
    .map_err(operation_model_error)?;
    let intent_id = take_intent_id(decoder)?;
    TextCatchUpUploadProgress::try_new(
        catch_up,
        delta_key,
        completed_counters,
        artifact_key,
        intent_id,
    )
    .map_err(operation_model_error)
}

fn put_text_compaction_upload(encoder: &mut ValueEncoder, progress: &TextCompactionUploadProgress) {
    put_prefix_scan(encoder, progress.compact());
    encoder.put_u16(
        u16::try_from(progress.input_artifact_keys().len())
            .expect("bounded compaction input count fits u16"),
    );
    for key in progress.input_artifact_keys() {
        encoder.put_bytes(key.as_bytes());
    }
    put_counters(encoder, progress.completed_counters());
    encoder.put_bytes(progress.artifact_key().as_bytes());
    put_intent_id(encoder, progress.intent_id());
}

fn take_text_compaction_upload(
    decoder: &mut ValueDecoder<'_>,
) -> Result<TextCompactionUploadProgress, EncodingError> {
    let compact = take_prefix_scan(decoder)?;
    let input_count = usize::from(decoder.take_u16()?);
    if input_count > TEXT_COMPACTION_INPUT_KEY_MAX {
        return Err(EncodingError::Custom(format!(
            "text compaction input count {input_count} exceeds maximum {TEXT_COMPACTION_INPUT_KEY_MAX}"
        )));
    }
    let mut input_artifact_keys = Vec::with_capacity(input_count);
    for _ in 0..input_count {
        input_artifact_keys.push(
            crate::index_v2::IndexCursor::try_new(
                decoder.take_bytes(crate::index_v2::INDEX_CURSOR_MAX_LEN)?,
            )
            .expect("bounded V2 field cannot exceed the cursor maximum"),
        );
    }
    let completed_counters = take_counters(decoder)?;
    let artifact_key = crate::index_v2::IndexCursor::try_new(
        decoder.take_bytes(crate::index_v2::INDEX_CURSOR_MAX_LEN)?,
    )
    .expect("bounded V2 field cannot exceed the cursor maximum");
    let intent_id = take_intent_id(decoder)?;
    TextCompactionUploadProgress::try_new(
        compact,
        input_artifact_keys,
        completed_counters,
        artifact_key,
        intent_id,
    )
    .map_err(operation_model_error)
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
        TextManifestValidationProgress::UploadIntents(progress) => {
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
        0x03 => take_prefix_scan(decoder).map(TextManifestValidationProgress::UploadIntents),
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

fn put_drain(encoder: &mut ValueEncoder, progress: DrainProgress) {
    put_option(encoder, progress.drain_epoch.as_ref(), |encoder, value| {
        encoder.put_u64(*value)
    });
    put_counters(encoder, progress.counters);
}

fn take_drain(decoder: &mut ValueDecoder<'_>) -> Result<DrainProgress, EncodingError> {
    Ok(DrainProgress {
        drain_epoch: decoder.take_option(ValueDecoder::take_u64)?,
        counters: take_counters(decoder)?,
    })
}

fn put_gc(encoder: &mut ValueEncoder, progress: &GcProgress) {
    put_option(encoder, progress.gc_run_id.as_ref(), |encoder, run_id| {
        put_run_id(encoder, *run_id)
    });
    put_cursor(encoder, progress.candidate_cursor.as_ref());
    put_cursor(encoder, progress.stage_cursor.as_ref());
    put_counters(encoder, progress.counters);
}

fn take_gc(decoder: &mut ValueDecoder<'_>) -> Result<GcProgress, EncodingError> {
    Ok(GcProgress {
        gc_run_id: decoder.take_option(take_run_id)?,
        candidate_cursor: take_cursor(decoder)?,
        stage_cursor: take_cursor(decoder)?,
        counters: take_counters(decoder)?,
    })
}

fn put_execution_state(
    encoder: &mut ValueEncoder,
    kind: IndexOperationKind,
    state: &IndexOperationExecutionState,
) {
    match state {
        IndexOperationExecutionState::Queued {
            not_before_unix_millis,
        } => {
            encoder.put_u8(0x01);
            put_option(
                encoder,
                not_before_unix_millis.as_ref(),
                |encoder, value| encoder.put_u64(*value),
            );
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

fn take_execution_state(
    decoder: &mut ValueDecoder<'_>,
    kind: IndexOperationKind,
) -> Result<IndexOperationExecutionState, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(IndexOperationExecutionState::Queued {
            not_before_unix_millis: decoder.take_option(ValueDecoder::take_u64)?,
        }),
        0x02 => Ok(IndexOperationExecutionState::Claimed(take_claim(decoder)?)),
        0x03 => Ok(IndexOperationExecutionState::Blocked(take_blocker(
            decoder,
        )?)),
        0x04 => match (kind, decoder.take_u8()?) {
            (IndexOperationKind::Build, 0x01) => Ok(IndexOperationExecutionState::Completed(
                IndexOperationOutcome::Build(BuildOperationOutcome::Succeeded),
            )),
            (IndexOperationKind::Build, 0x02) => Ok(IndexOperationExecutionState::Completed(
                IndexOperationOutcome::Build(BuildOperationOutcome::Aborted),
            )),
            (IndexOperationKind::Drop, 0x01) => Ok(IndexOperationExecutionState::Completed(
                IndexOperationOutcome::DropSucceeded,
            )),
            (_, unknown) => Err(unknown_discriminant("operation outcome", unknown)),
        },
        unknown => Err(unknown_discriminant("operation execution state", unknown)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RangeIndexDirection, SecondaryIndexDefinition, TextAnalyzerKind};
    use crate::encoding::v1::keys::index_v2::{
        IndexEntity, IndexEntityStateKey, IndexV2Key, PartitionFingerprint, TextBuildArtifactKey,
        TextManifestRootKey,
    };
    use crate::encoding::v1::keys::tenant::{DataScope, TenantId};
    use crate::encoding::v1::keys::{DataKeyKind, Key};
    use crate::index_v2::{
        BlobGcRunId, BuildOperationOutcome, ClaimSequence, IndexComponent, IndexCursor,
        IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexIdentity,
        IndexIdentityFamily, IndexOperationBlocker, IndexOperationId, IndexOperationOutcome,
        IndexOperationRevision, IndexRevision, OperationClaim, OperationCounters,
        PhysicalGeneration, TextIntentRevision, ValidatedDynamicIndexDefinition,
        ValidatedSecondaryIndexDefinition, ValidatedTextIndexDefinition,
        ValidatedVectorIndexDefinition, VectorGenerationDescriptor, VectorPhysicalIndexId,
        VectorPhysicalLayout, WriterEpoch,
    };
    use crate::search::vector::VectorDistanceMetric;

    fn definition() -> ValidatedDynamicIndexDefinition {
        SecondaryIndexDefinition::node_equality("User", "email")
            .unwrap()
            .try_into()
            .unwrap()
    }

    fn record(state: IndexStateV2) -> IndexRecordV2 {
        let definition = definition();
        IndexRecordV2::try_new(
            IndexId::initial(),
            definition.identity(),
            definition,
            IndexRevision::initial(),
            state,
        )
        .unwrap()
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

    fn dropped_record(definition: ValidatedDynamicIndexDefinition) -> IndexRecordV2 {
        IndexRecordV2::try_new(
            IndexId::initial(),
            definition.identity(),
            definition,
            IndexRevision::initial(),
            IndexStateV2::Dropped {
                last_generation: IndexGenerationId::initial(),
                completed_operation_id: IndexOperationId::from_bytes([0x11; 16]).unwrap(),
            },
        )
        .unwrap()
    }

    #[test]
    fn every_definition_state_physical_and_metadata_variant_has_frozen_bytes() {
        let component = |kind, value| IndexComponent::try_new(kind, value).unwrap();
        let definitions = [
            (
                "secondary_node_equality",
                ValidatedDynamicIndexDefinition::Secondary(
                    ValidatedSecondaryIndexDefinition::NodeEquality {
                        label: component("label", "User"),
                        property: component("property", "email"),
                        unique: true,
                    },
                ),
            ),
            (
                "secondary_node_range",
                ValidatedDynamicIndexDefinition::Secondary(
                    ValidatedSecondaryIndexDefinition::NodeRange {
                        label: component("label", "User"),
                        property: component("property", "age"),
                        direction: RangeIndexDirection::Desc,
                    },
                ),
            ),
            (
                "secondary_edge_equality",
                ValidatedDynamicIndexDefinition::Secondary(
                    ValidatedSecondaryIndexDefinition::EdgeEquality {
                        label: component("label", "Follows"),
                        property: component("property", "since"),
                    },
                ),
            ),
            (
                "secondary_edge_range",
                ValidatedDynamicIndexDefinition::Secondary(
                    ValidatedSecondaryIndexDefinition::EdgeRange {
                        label: component("label", "Follows"),
                        property: component("property", "weight"),
                        direction: RangeIndexDirection::Asc,
                    },
                ),
            ),
        ];
        let mut definition_goldens = definitions
            .into_iter()
            .map(|(name, definition)| {
                let record = dropped_record(definition);
                let bytes = encode_index_record(&record);
                assert_eq!(decode_index_record(&bytes).unwrap(), record);
                (name, hex(&bytes))
            })
            .collect::<Vec<_>>();
        for (metric_name, metric) in [
            ("cosine", VectorDistanceMetric::Cosine),
            ("euclidean", VectorDistanceMetric::Euclidean),
            ("manhattan", VectorDistanceMetric::Manhattan),
        ] {
            let definition = ValidatedDynamicIndexDefinition::Vector(
                ValidatedVectorIndexDefinition::try_new(
                    IndexElementKind::Node,
                    "Doc",
                    "embedding",
                    Some("tenant"),
                    3,
                    metric,
                    16,
                    32,
                    64,
                    0.5,
                    4,
                    0.75,
                    true,
                    0.25,
                )
                .unwrap(),
            );
            let record = dropped_record(definition);
            let bytes = encode_index_record(&record);
            assert_eq!(decode_index_record(&bytes).unwrap(), record);
            definition_goldens.push((metric_name, hex(&bytes)));
        }
        for (analyzer_name, analyzer) in [
            ("text_standard", TextAnalyzerKind::Standard),
            ("text_stem", TextAnalyzerKind::StandardStemEn),
            ("text_whitespace", TextAnalyzerKind::WhitespaceLowercase),
        ] {
            let definition = ValidatedDynamicIndexDefinition::Text(
                ValidatedTextIndexDefinition::try_new(
                    IndexElementKind::Edge,
                    "Comment",
                    "body",
                    Some("tenant"),
                    analyzer,
                    true,
                )
                .unwrap(),
            );
            let record = dropped_record(definition);
            let bytes = encode_index_record(&record);
            assert_eq!(decode_index_record(&bytes).unwrap(), record);
            definition_goldens.push((analyzer_name, hex(&bytes)));
        }

        let operation_id = IndexOperationId::from_bytes([0x22; 16]).unwrap();
        let secondary = PhysicalGeneration::Secondary {
            generation: IndexGenerationId::initial(),
        };
        let states = [
            (
                "building",
                IndexStateV2::Building {
                    physical: secondary.clone(),
                    build_operation_id: operation_id,
                },
            ),
            (
                "active",
                IndexStateV2::Active {
                    physical: secondary.clone(),
                    completed_build_operation_id: operation_id,
                },
            ),
            (
                "aborting",
                IndexStateV2::Aborting {
                    physical: secondary.clone(),
                    build_operation_id: operation_id,
                },
            ),
            (
                "dropping",
                IndexStateV2::Dropping {
                    physical: secondary,
                    drop_operation_id: operation_id,
                },
            ),
            (
                "dropped",
                IndexStateV2::Dropped {
                    last_generation: IndexGenerationId::initial(),
                    completed_operation_id: operation_id,
                },
            ),
        ];
        let mut state_goldens = states
            .into_iter()
            .map(|(name, state)| {
                let record = record(state);
                let bytes = encode_index_record(&record);
                assert_eq!(decode_index_record(&bytes).unwrap(), record);
                (name, hex(&bytes))
            })
            .collect::<Vec<_>>();

        let vector_definition = ValidatedVectorIndexDefinition::try_new(
            IndexElementKind::Node,
            "Doc",
            "embedding",
            None::<String>,
            3,
            VectorDistanceMetric::Cosine,
            16,
            32,
            64,
            0.5,
            4,
            0.75,
            false,
            0.25,
        )
        .unwrap();
        let vector_physical = PhysicalGeneration::Vector {
            generation: IndexGenerationId::initial(),
            layout: VectorPhysicalLayout::Unpartitioned {
                physical_index_id: VectorPhysicalIndexId::initial(),
            },
            descriptor: VectorGenerationDescriptor::for_definition(&vector_definition),
        };
        let vector_definition = ValidatedDynamicIndexDefinition::Vector(vector_definition);
        let vector_record = IndexRecordV2::try_new(
            IndexId::initial(),
            vector_definition.identity(),
            vector_definition,
            IndexRevision::initial(),
            IndexStateV2::Active {
                physical: vector_physical,
                completed_build_operation_id: operation_id,
            },
        )
        .unwrap();
        let bytes = encode_index_record(&vector_record);
        assert_eq!(decode_index_record(&bytes).unwrap(), vector_record);
        state_goldens.push(("active_vector_physical", hex(&bytes)));

        let partitioned_vector_definition = ValidatedVectorIndexDefinition::try_new(
            IndexElementKind::Node,
            "Doc",
            "embedding",
            Some("tenant"),
            3,
            VectorDistanceMetric::Cosine,
            16,
            32,
            64,
            0.5,
            4,
            0.75,
            false,
            0.25,
        )
        .unwrap();
        let partitioned_vector_physical = PhysicalGeneration::Vector {
            generation: IndexGenerationId::initial(),
            layout: VectorPhysicalLayout::Partitioned,
            descriptor: VectorGenerationDescriptor::for_definition(&partitioned_vector_definition),
        };
        let partitioned_vector_definition =
            ValidatedDynamicIndexDefinition::Vector(partitioned_vector_definition);
        let partitioned_vector_record = IndexRecordV2::try_new(
            IndexId::initial(),
            partitioned_vector_definition.identity(),
            partitioned_vector_definition,
            IndexRevision::initial(),
            IndexStateV2::Active {
                physical: partitioned_vector_physical,
                completed_build_operation_id: operation_id,
            },
        )
        .unwrap();
        let bytes = encode_index_record(&partitioned_vector_record);
        assert_eq!(
            decode_index_record(&bytes).unwrap(),
            partitioned_vector_record
        );
        state_goldens.push(("active_vector_partitioned", hex(&bytes)));

        let text_definition = ValidatedDynamicIndexDefinition::Text(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Doc",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let text_record = IndexRecordV2::try_new(
            IndexId::initial(),
            text_definition.identity(),
            text_definition,
            IndexRevision::initial(),
            IndexStateV2::Active {
                physical: PhysicalGeneration::Text {
                    generation: IndexGenerationId::initial(),
                },
                completed_build_operation_id: operation_id,
            },
        )
        .unwrap();
        let bytes = encode_index_record(&text_record);
        assert_eq!(decode_index_record(&bytes).unwrap(), text_record);
        state_goldens.push(("active_text_physical", hex(&bytes)));

        let metadata = [
            (
                "storage_version",
                IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT),
            ),
            (
                "logical_watermark",
                IndexV2MetadataValue::LogicalIndexIdWatermark(LogicalIndexIdWatermark {
                    next_id: IndexId::initial(),
                }),
            ),
            (
                "vector_watermark",
                IndexV2MetadataValue::VectorPhysicalIdWatermark(VectorPhysicalIdWatermark {
                    next_id: VectorPhysicalIndexId::initial(),
                }),
            ),
            (
                "operation_pointer_tenant",
                IndexV2MetadataValue::OperationQueuePointer(OperationQueuePointerValue {
                    scope: DataScope::Tenant(TenantId::from_u128(42)),
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    record_revision: IndexOperationRevision::initial(),
                }),
            ),
            (
                "upload_pointer_unscoped",
                IndexV2MetadataValue::UploadQueuePointer(UploadQueuePointerValue {
                    scope: DataScope::LegacyUnscoped,
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    record_revision: TextIntentRevision::initial(),
                }),
            ),
        ];
        let metadata_goldens = metadata
            .into_iter()
            .map(|(name, value)| {
                let bytes = encode_metadata_value(&value);
                assert_eq!(decode_metadata_value(&bytes).unwrap(), value);
                (name, hex(&bytes))
            })
            .collect::<Vec<_>>();

        insta::assert_debug_snapshot!(
            (definition_goldens, state_goldens, metadata_goldens),
            @r###"
(
    [
        (
            "secondary_node_equality",
            "010100000000000000010101000000045573657200000005656d61696c0101000000045573657200000005656d61696c01000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "secondary_node_range",
            "010100000000000000010201000000045573657200000003616765010200000004557365720000000361676502000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "secondary_edge_equality",
            "01010000000000000001010200000007466f6c6c6f77730000000573696e6365010300000007466f6c6c6f77730000000573696e6365000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "secondary_edge_range",
            "01010000000000000001020200000007466f6c6c6f777300000006776569676874010400000007466f6c6c6f77730000000677656967687401000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "cosine",
            "01010000000000000001030100000003446f6300000009656d62656464696e67020100000003446f6300000009656d62656464696e67010000000674656e616e740000000301010000001000000020000000403f000000000000043f400000013e800000000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "euclidean",
            "01010000000000000001030100000003446f6300000009656d62656464696e67020100000003446f6300000009656d62656464696e67010000000674656e616e740000000302010000001000000020000000403f000000000000043f400000013e800000000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "manhattan",
            "01010000000000000001030100000003446f6300000009656d62656464696e67020100000003446f6300000009656d62656464696e67010000000674656e616e740000000303010000001000000020000000403f000000000000043f400000013e800000000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "text_standard",
            "01010000000000000001040200000007436f6d6d656e7400000004626f6479030200000007436f6d6d656e7400000004626f6479010000000674656e616e740101000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "text_stem",
            "01010000000000000001040200000007436f6d6d656e7400000004626f6479030200000007436f6d6d656e7400000004626f6479010000000674656e616e740201000000000000000105000000000000000111111111111111111111111111111111",
        ),
        (
            "text_whitespace",
            "01010000000000000001040200000007436f6d6d656e7400000004626f6479030200000007436f6d6d656e7400000004626f6479010000000674656e616e740301000000000000000105000000000000000111111111111111111111111111111111",
        ),
    ],
    [
        (
            "building",
            "010100000000000000010101000000045573657200000005656d61696c0101000000045573657200000005656d61696c0000000000000000010101000000000000000122222222222222222222222222222222",
        ),
        (
            "active",
            "010100000000000000010101000000045573657200000005656d61696c0101000000045573657200000005656d61696c0000000000000000010201000000000000000122222222222222222222222222222222",
        ),
        (
            "aborting",
            "010100000000000000010101000000045573657200000005656d61696c0101000000045573657200000005656d61696c0000000000000000010301000000000000000122222222222222222222222222222222",
        ),
        (
            "dropping",
            "010100000000000000010101000000045573657200000005656d61696c0101000000045573657200000005656d61696c0000000000000000010401000000000000000122222222222222222222222222222222",
        ),
        (
            "dropped",
            "010100000000000000010101000000045573657200000005656d61696c0101000000045573657200000005656d61696c00000000000000000105000000000000000122222222222222222222222222222222",
        ),
        (
            "active_vector_physical",
            "01010000000000000001030100000003446f6300000009656d62656464696e67020100000003446f6300000009656d62656464696e67000000000301010000001000000020000000403f000000000000043f400000003e800000000000000000000102020000000000000001010000000000000001000000030101010122222222222222222222222222222222",
        ),
        (
            "active_vector_partitioned",
            "01010000000000000001030100000003446f6300000009656d62656464696e67020100000003446f6300000009656d62656464696e67010000000674656e616e740000000301010000001000000020000000403f000000000000043f400000003e80000000000000000000010202000000000000000102000000030101010122222222222222222222222222222222",
        ),
        (
            "active_text_physical",
            "01010000000000000001040100000003446f6300000004626f6479030100000003446f6300000004626f647900010000000000000000010203000000000000000122222222222222222222222222222222",
        ),
    ],
    [
        (
            "storage_version",
            "01010002",
        ),
        (
            "logical_watermark",
            "01020000000000000001",
        ),
        (
            "vector_watermark",
            "01030000000000000001",
        ),
        (
            "operation_pointer_tenant",
            "0104010000000000000000000000000000002a000000000000000100000000000000010000000000000001",
        ),
        (
            "upload_pointer_unscoped",
            "010500000000000000000100000000000000010000000000000001",
        ),
    ],
)
"###
        );
    }

    #[test]
    fn every_operation_stage_state_blocker_and_outcome_has_frozen_bytes() {
        let cursor = |byte| {
            IndexCursor::try_new(
                IndexV2Key::operation(IndexOperationId::from_bytes([byte; 16]).unwrap()).to_bytes(),
            )
            .unwrap()
        };
        let counters = OperationCounters {
            entities: 1,
            input_bytes: 2,
            output_operations: 3,
            output_bytes: 4,
        };
        let source = SourceScanProgress {
            inclusive_upper_bound: cursor(0x31),
            cursor: Some(cursor(0x32)),
            counters,
        };
        let prefix = PrefixScanProgress {
            cursor: Some(cursor(0x33)),
            counters,
        };
        let no_cursor = NoCursorProgress { counters };
        let drain = DrainProgress {
            drain_epoch: Some(5),
            counters,
        };
        let gc = GcProgress {
            gc_run_id: Some(BlobGcRunId::from_bytes([0x34; 16]).unwrap()),
            candidate_cursor: Some(cursor(0x35)),
            stage_cursor: Some(cursor(0x36)),
            counters,
        };
        let acquire_gc = GcProgress {
            stage_cursor: None,
            ..gc.clone()
        };
        let awaiting_upload = TextBuildUploadProgress::try_new(
            SourceScanProgress {
                inclusive_upper_bound: cursor(0x39),
                cursor: Some(cursor(0x31)),
                counters,
            },
            cursor(0x32),
            OperationCounters {
                entities: 2,
                input_bytes: 3,
                output_operations: 4,
                output_bytes: 5,
            },
            IndexCursor::try_new(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(
                        TextBuildArtifactKey {
                            root: TextManifestRootKey {
                                index_id: IndexId::initial(),
                                generation: IndexGenerationId::initial(),
                                partition: PartitionFingerprint::new([0x37; 32]),
                            },
                            ordinal: 6,
                        },
                    )),
                }
                .to_bytes(),
            )
            .unwrap(),
            crate::index_v2::TextUploadIntentId::from_bytes([0x38; 16]).unwrap(),
        )
        .unwrap();
        let awaiting_catch_up_upload = TextCatchUpUploadProgress::try_new(
            PrefixScanProgress {
                cursor: None,
                counters,
            },
            IndexCursor::try_new(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::BuildDelta(IndexEntityStateKey {
                        index_id: IndexId::initial(),
                        generation: IndexGenerationId::initial(),
                        entity: IndexEntity {
                            kind: IndexElementKind::Node,
                            id: IndexEntityId::new(7),
                        },
                    })),
                }
                .to_bytes(),
            )
            .unwrap(),
            OperationCounters {
                entities: 2,
                input_bytes: 3,
                output_operations: 5,
                output_bytes: 6,
            },
            IndexCursor::try_new(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(
                        TextBuildArtifactKey {
                            root: TextManifestRootKey {
                                index_id: IndexId::initial(),
                                generation: IndexGenerationId::initial(),
                                partition: PartitionFingerprint::new([0x3A; 32]),
                            },
                            ordinal: 8,
                        },
                    )),
                }
                .to_bytes(),
            )
            .unwrap(),
            crate::index_v2::TextUploadIntentId::from_bytes([0x3B; 16]).unwrap(),
        )
        .unwrap();
        let compaction_root = TextManifestRootKey {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            partition: PartitionFingerprint::new([0x3C; 32]),
        };
        let compaction_artifact_cursor = |ordinal| {
            IndexCursor::try_new(
                Key::Data {
                    scope: DataScope::LegacyUnscoped,
                    kind: DataKeyKind::IndexV2(IndexV2Key::TextBuildArtifact(
                        TextBuildArtifactKey {
                            root: compaction_root,
                            ordinal,
                        },
                    )),
                }
                .to_bytes(),
            )
            .unwrap()
        };
        let awaiting_compaction_upload = TextCompactionUploadProgress::try_new(
            PrefixScanProgress {
                cursor: None,
                counters,
            },
            vec![compaction_artifact_cursor(1), compaction_artifact_cursor(2)],
            OperationCounters {
                entities: 3,
                input_bytes: 5,
                output_operations: 6,
                output_bytes: 7,
            },
            compaction_artifact_cursor(3),
            crate::index_v2::TextUploadIntentId::from_bytes([0x3D; 16]).unwrap(),
        )
        .unwrap();
        let validating_pages = TextManifestValidationProgress::Pages(
            TextManifestPageValidationProgress::try_new(
                Some(cursor(0x3E)),
                Some(
                    TextManifestPartitionValidation::try_new(
                        [0x3F; 32],
                        crate::index_v2::TextManifestRevision::new(3).unwrap(),
                        2,
                        3,
                        1,
                        1,
                    )
                    .unwrap(),
                ),
                counters,
            )
            .unwrap(),
        );
        let validating_roots = TextManifestValidationProgress::Roots(PrefixScanProgress {
            cursor: Some(cursor(0x40)),
            counters,
        });
        let validating_intents =
            TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
                cursor: Some(cursor(0x41)),
                counters,
            });
        let progresses = vec![
            (
                "secondary_build_scan",
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Scan(source.clone()),
                )),
            ),
            (
                "secondary_build_aborting_begin_drain",
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                    SecondaryCleanupProgress::BeginDrain(drain),
                )),
            ),
            (
                "secondary_build_catch_up",
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::CatchUp(prefix.clone()),
                )),
            ),
            (
                "secondary_build_validate",
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Validate(prefix.clone()),
                )),
            ),
            (
                "secondary_build_activate",
                IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                    SecondaryBuildStage::Activate(no_cursor),
                )),
            ),
            (
                "vector_build_scan",
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::Scan(source.clone()),
                )),
            ),
            (
                "vector_build_catch_up",
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::CatchUp(prefix.clone()),
                )),
            ),
            (
                "vector_build_validate_descriptor",
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::ValidateDescriptor(prefix.clone()),
                )),
            ),
            (
                "vector_build_activate",
                IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
                    VectorBuildStage::Activate(no_cursor),
                )),
            ),
            (
                "text_build_scan_source",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ScanSource(source.clone()),
                )),
            ),
            (
                "text_build_scan_partitions",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ScanPartitions(source),
                )),
            ),
            (
                "text_build_await_upload",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::AwaitUpload(awaiting_upload),
                )),
            ),
            (
                "text_build_await_catch_up_upload",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::AwaitCatchUpUpload(awaiting_catch_up_upload),
                )),
            ),
            (
                "text_build_await_compaction_upload",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::AwaitCompactionUpload(awaiting_compaction_upload),
                )),
            ),
            (
                "text_build_catch_up",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::CatchUp(prefix.clone()),
                )),
            ),
            (
                "text_build_compact",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::Compact(prefix.clone()),
                )),
            ),
            (
                "text_build_prepare_manifests",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::PrepareManifests(prefix.clone()),
                )),
            ),
            (
                "text_build_validate_manifest_pages",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(validating_pages),
                )),
            ),
            (
                "text_build_validate_manifest_roots",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(validating_roots),
                )),
            ),
            (
                "text_build_validate_upload_intents",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(validating_intents),
                )),
            ),
            (
                "text_build_activate",
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::Activate(no_cursor),
                )),
            ),
            (
                "secondary_cleanup_begin_drain",
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::BeginDrain(
                    drain,
                )),
            ),
            (
                "secondary_cleanup_delete_entries",
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteEntries(
                    prefix.clone(),
                )),
            ),
            (
                "secondary_cleanup_delete_deltas",
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::DeleteDeltas(
                    prefix.clone(),
                )),
            ),
            (
                "secondary_cleanup_finish_drain",
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::FinishDrain(
                    drain,
                )),
            ),
            (
                "secondary_cleanup_finalize",
                IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::Finalize(
                    no_cursor,
                )),
            ),
            (
                "vector_cleanup_begin_drain",
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::BeginDrain(drain)),
            ),
            (
                "vector_cleanup_retire_cache",
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::RetireCache(
                    no_cursor,
                )),
            ),
            (
                "vector_cleanup_delete_physical",
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeletePhysical(
                    prefix.clone(),
                )),
            ),
            (
                "vector_cleanup_delete_deltas",
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::DeleteDeltas(
                    prefix.clone(),
                )),
            ),
            (
                "vector_cleanup_finish_drain",
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::FinishDrain(drain)),
            ),
            (
                "vector_cleanup_finalize",
                IndexOperationProgress::VectorCleanup(VectorCleanupProgress::Finalize(no_cursor)),
            ),
            (
                "text_cleanup_begin_drain",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::BeginDrain(drain)),
            ),
            (
                "text_cleanup_prepare_candidates",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::PrepareCandidates(
                    prefix.clone(),
                )),
            ),
            (
                "text_cleanup_acquire_delete_fences",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::AcquireDeleteFences(
                    acquire_gc,
                )),
            ),
            (
                "text_cleanup_retire_manifest",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::RetireManifest(
                    gc.clone(),
                )),
            ),
            (
                "text_cleanup_retire_artifacts",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::RetireArtifacts(
                    gc.clone(),
                )),
            ),
            (
                "text_cleanup_retire_upload_intents",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::RetireUploadIntents(
                    gc.clone(),
                )),
            ),
            (
                "text_cleanup_mark_reachability",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::MarkReachability(
                    gc.clone(),
                )),
            ),
            (
                "text_cleanup_delete_blobs",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::DeleteBlobs(gc)),
            ),
            (
                "text_cleanup_delete_entity_state",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::DeleteEntityState(prefix)),
            ),
            (
                "text_cleanup_finish_drain",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::FinishDrain(drain)),
            ),
            (
                "text_cleanup_finalize",
                IndexOperationProgress::TextCleanup(TextCleanupProgress::Finalize(no_cursor)),
            ),
        ];

        let identity_for = |family| {
            let identity_family = match family {
                IndexOperationFamily::Secondary => IndexIdentityFamily::SecondaryEquality,
                IndexOperationFamily::Vector => IndexIdentityFamily::Vector,
                IndexOperationFamily::Text => IndexIdentityFamily::Text,
            };
            IndexIdentity::new(
                identity_family,
                IndexElementKind::Node,
                IndexComponent::try_new("label", "Doc").unwrap(),
                IndexComponent::try_new("property", "value").unwrap(),
            )
        };
        let operation = |progress: IndexOperationProgress,
                         execution_state: IndexOperationExecutionState| {
            let kind = progress.kind();
            let family = progress.family();
            IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x21; 16]).unwrap(),
                IndexId::initial(),
                identity_for(family),
                IndexGenerationId::initial(),
                IndexRevision::initial(),
                IndexOperationRevision::initial(),
                kind,
                family,
                progress,
                6,
                execution_state,
            )
            .unwrap()
        };
        let progress_goldens = progresses
            .into_iter()
            .map(|(name, progress)| {
                let record = operation(
                    progress,
                    IndexOperationExecutionState::Queued {
                        not_before_unix_millis: None,
                    },
                );
                let bytes = encode_operation_record(&record);
                assert_eq!(decode_operation_record(&bytes).unwrap(), record);
                (name, hex(&bytes))
            })
            .collect::<Vec<_>>();

        let build_progress = IndexOperationProgress::SecondaryBuild(
            SecondaryBuildProgress::Constructing(SecondaryBuildStage::Activate(no_cursor)),
        );
        let blockers = [
            (
                "blocked_invalid_source",
                IndexOperationBlocker::InvalidSourceData {
                    entity_kind: IndexElementKind::Node,
                    entity_id: IndexEntityId::new(7),
                },
            ),
            (
                "blocked_uniqueness",
                IndexOperationBlocker::UniquenessViolation {
                    first_entity_id: IndexEntityId::new(7),
                    second_entity_id: IndexEntityId::new(8),
                },
            ),
            (
                "blocked_oversized",
                IndexOperationBlocker::OversizedEntity {
                    entity_kind: IndexElementKind::Edge,
                    entity_id: IndexEntityId::new(7),
                    observed: 10,
                    limit: 9,
                },
            ),
            (
                "blocked_manifest_limit",
                IndexOperationBlocker::ManifestLimit {
                    partition: crate::index_v2::TextPartition::Unpartitioned,
                    observed: 10,
                    limit: 9,
                },
            ),
            (
                "blocked_reader_coordination",
                IndexOperationBlocker::ReaderCoordinationUnavailable,
            ),
            (
                "blocked_object_store",
                IndexOperationBlocker::ObjectStoreConfigurationUnavailable,
            ),
            (
                "blocked_invariant",
                IndexOperationBlocker::InvariantViolation,
            ),
            (
                "blocked_publication_coordination",
                IndexOperationBlocker::BlobPublicationCoordinationUnavailable,
            ),
            (
                "blocked_publication_mismatch",
                IndexOperationBlocker::BlobPublicationMismatch {
                    intent_id: crate::index_v2::TextUploadIntentId::from_bytes([0x22; 16]).unwrap(),
                },
            ),
        ];
        let mut state_goldens = vec![
            (
                "queued_none",
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            ),
            (
                "queued_delayed",
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: Some(7),
                },
            ),
            (
                "claimed",
                IndexOperationExecutionState::Claimed(OperationClaim {
                    writer_epoch: WriterEpoch::from_bytes([0x41; 16]).unwrap(),
                    sequence: ClaimSequence::new(8).unwrap(),
                }),
            ),
        ];
        state_goldens.extend(
            blockers
                .into_iter()
                .map(|(name, blocker)| (name, IndexOperationExecutionState::Blocked(blocker))),
        );
        state_goldens.extend([(
            "completed_build_succeeded",
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Succeeded,
            )),
        )]);
        let mut execution_goldens = state_goldens
            .into_iter()
            .map(|(name, state)| {
                let record = operation(build_progress.clone(), state);
                let bytes = encode_operation_record(&record);
                assert_eq!(decode_operation_record(&bytes).unwrap(), record);
                (name, hex(&bytes))
            })
            .collect::<Vec<_>>();
        let aborted_record = operation(
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Aborting(
                SecondaryCleanupProgress::Finalize(no_cursor),
            )),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::Build(
                BuildOperationOutcome::Aborted,
            )),
        );
        let bytes = encode_operation_record(&aborted_record);
        assert_eq!(decode_operation_record(&bytes).unwrap(), aborted_record);
        execution_goldens.push(("completed_build_aborted", hex(&bytes)));
        let drop_record = operation(
            IndexOperationProgress::SecondaryCleanup(SecondaryCleanupProgress::Finalize(no_cursor)),
            IndexOperationExecutionState::Completed(IndexOperationOutcome::DropSucceeded),
        );
        let bytes = encode_operation_record(&drop_record);
        assert_eq!(decode_operation_record(&bytes).unwrap(), drop_record);
        execution_goldens.push(("completed_drop_succeeded", hex(&bytes)));

        // Length-framed names plus complete encoded hex make these SHA-256
        // values compact, collision-resistant byte goldens for the exhaustive
        // matrices above. A stage, payload, field-order, or discriminant change
        // must update the corresponding storage-contract digest deliberately.
        assert_eq!(progress_goldens.len(), 43);
        assert_eq!(execution_goldens.len(), 15);
        assert_eq!(
            golden_digest(&progress_goldens),
            "03256736557853a28cee2290b1d5a8f382f8f813dd143f6be69fc23cf4adbaf0"
        );
        assert_eq!(
            golden_digest(&execution_goldens),
            "c1213c31af7f34f97d5b6900ef8a5f9ee635f3f456f590bf3c4823b7e1715f5f"
        );
    }

    #[test]
    fn every_index_state_roundtrips_with_frozen_tags() {
        let physical = PhysicalGeneration::Secondary {
            generation: IndexGenerationId::initial(),
        };
        let operation = IndexOperationId::from_bytes([1; 16]).unwrap();
        let states = [
            IndexStateV2::Building {
                physical: physical.clone(),
                build_operation_id: operation,
            },
            IndexStateV2::Active {
                physical: physical.clone(),
                completed_build_operation_id: operation,
            },
            IndexStateV2::Aborting {
                physical: physical.clone(),
                build_operation_id: operation,
            },
            IndexStateV2::Dropping {
                physical,
                drop_operation_id: operation,
            },
            IndexStateV2::Dropped {
                last_generation: IndexGenerationId::initial(),
                completed_operation_id: operation,
            },
        ];
        for (ordinal, state) in states.into_iter().enumerate() {
            let record = record(state);
            let bytes = encode_index_record(&record);
            const HEADER_OFFSET: usize = 0;
            const HEADER_LEN: usize = 2;
            assert_eq!(
                bytes[HEADER_OFFSET..HEADER_OFFSET + HEADER_LEN],
                [0x01, 0x01]
            );
            assert_eq!(decode_index_record(&bytes).unwrap(), record);
            assert!(bytes.contains(&((ordinal + 1) as u8)));
        }
    }

    #[test]
    fn persistence_boundary_accepts_only_the_canonical_index_record() {
        let encoder: fn(&IndexRecordV2) -> Bytes = encode_index_record;
        let decoder: fn(&[u8]) -> Result<IndexRecordV2, EncodingError> = decode_index_record;
        let record = record(IndexStateV2::Dropped {
            last_generation: IndexGenerationId::initial(),
            completed_operation_id: IndexOperationId::from_bytes([1; 16]).unwrap(),
        });
        assert_eq!(decoder(&encoder(&record)).unwrap(), record);
    }

    #[test]
    fn metadata_goldens_use_big_endian_and_exact_scope_framing() {
        let marker = encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::CURRENT,
        ));
        assert_eq!(marker.as_ref(), &[0x01, 0x01, 0x00, 0x02]);
        assert_eq!(
            decode_metadata_value(&marker).unwrap(),
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::CURRENT)
        );

        let pointer = IndexV2MetadataValue::OperationQueuePointer(OperationQueuePointerValue {
            scope: DataScope::Tenant(TenantId::from_u128(42)),
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            record_revision: IndexOperationRevision::initial(),
        });
        let bytes = encode_metadata_value(&pointer);
        const POINTER_HEADER_OFFSET: usize = 0;
        const POINTER_HEADER_LEN: usize = 3;
        assert_eq!(
            bytes[POINTER_HEADER_OFFSET..POINTER_HEADER_OFFSET + POINTER_HEADER_LEN],
            [0x01, 0x04, 0x01]
        );
        assert_eq!(decode_metadata_value(&bytes).unwrap(), pointer);
    }

    #[test]
    fn malformed_version_kind_bool_truncation_and_trailing_bytes_fail_closed() {
        let valid = encode_index_record(&record(IndexStateV2::Dropped {
            last_generation: IndexGenerationId::initial(),
            completed_operation_id: IndexOperationId::from_bytes([1; 16]).unwrap(),
        }));
        let mut wrong_version = valid.to_vec();
        const VERSION_OFFSET: usize = 0;
        const KIND_OFFSET: usize = VERSION_OFFSET + 1;
        wrong_version[VERSION_OFFSET] = 2;
        assert!(decode_index_record(&wrong_version).is_err());
        let mut wrong_kind = valid.to_vec();
        wrong_kind[KIND_OFFSET] = 2;
        assert!(decode_index_record(&wrong_kind).is_err());
        const VALUE_OFFSET: usize = 0;
        assert!(decode_index_record(&valid[VALUE_OFFSET..VALUE_OFFSET + valid.len() - 1]).is_err());
        let mut trailing = valid.to_vec();
        trailing.push(0);
        assert!(decode_index_record(&trailing).is_err());
    }

    #[test]
    fn operation_progress_stage_and_state_roundtrip() {
        let definition = definition();
        let operation = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([1; 16]).unwrap(),
            IndexId::initial(),
            definition.identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Secondary,
            IndexOperationProgress::SecondaryBuild(SecondaryBuildProgress::Constructing(
                SecondaryBuildStage::Activate(NoCursorProgress {
                    counters: OperationCounters::default(),
                }),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        let bytes = encode_operation_record(&operation);
        const HEADER_OFFSET: usize = 0;
        const HEADER_LEN: usize = 2;
        assert_eq!(
            bytes[HEADER_OFFSET..HEADER_OFFSET + HEADER_LEN],
            [0x01, 0x02]
        );
        assert_eq!(decode_operation_record(&bytes).unwrap(), operation);
    }

    #[test]
    fn text_build_await_upload_codec_rejects_every_truncation() {
        let text_definition = ValidatedDynamicIndexDefinition::Text(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Doc",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let cursor = |byte| {
            IndexCursor::try_new(
                IndexV2Key::operation(IndexOperationId::from_bytes([byte; 16]).unwrap()).to_bytes(),
            )
            .unwrap()
        };
        let progress = TextBuildUploadProgress::try_new(
            SourceScanProgress {
                inclusive_upper_bound: cursor(0x39),
                cursor: Some(cursor(0x31)),
                counters: OperationCounters {
                    entities: 1,
                    input_bytes: 2,
                    output_operations: 3,
                    output_bytes: 4,
                },
            },
            cursor(0x32),
            OperationCounters {
                entities: 2,
                input_bytes: 3,
                output_operations: 4,
                output_bytes: 5,
            },
            cursor(0x33),
            crate::index_v2::TextUploadIntentId::from_bytes([0x34; 16]).unwrap(),
        )
        .unwrap();
        let operation = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([0x35; 16]).unwrap(),
            IndexId::initial(),
            text_definition.identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::AwaitUpload(progress),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        let bytes = encode_operation_record(&operation);
        const VALUE_OFFSET: usize = 0;

        for truncated_len in VALUE_OFFSET..bytes.len() {
            assert!(
                decode_operation_record(&bytes[VALUE_OFFSET..VALUE_OFFSET + truncated_len],)
                    .is_err(),
                "accepted text AwaitUpload record truncated to {truncated_len} bytes"
            );
        }
        assert_eq!(decode_operation_record(&bytes).unwrap(), operation);
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode_operation_record(&trailing).is_err());
    }

    #[test]
    fn text_build_await_catch_up_upload_codec_rejects_every_truncation() {
        let definition = ValidatedDynamicIndexDefinition::Text(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Doc",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let cursor = |byte| {
            IndexCursor::try_new(
                IndexV2Key::operation(IndexOperationId::from_bytes([byte; 16]).unwrap()).to_bytes(),
            )
            .unwrap()
        };
        let progress = TextCatchUpUploadProgress::try_new(
            PrefixScanProgress {
                cursor: None,
                counters: OperationCounters {
                    entities: 1,
                    input_bytes: 2,
                    output_operations: 3,
                    output_bytes: 4,
                },
            },
            cursor(0x31),
            OperationCounters {
                entities: 2,
                input_bytes: 3,
                output_operations: 5,
                output_bytes: 6,
            },
            cursor(0x32),
            crate::index_v2::TextUploadIntentId::from_bytes([0x33; 16]).unwrap(),
        )
        .unwrap();
        let operation = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([0x34; 16]).unwrap(),
            IndexId::initial(),
            definition.identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::AwaitCatchUpUpload(progress),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        let bytes = encode_operation_record(&operation);
        const VALUE_OFFSET: usize = 0;

        for truncated_len in VALUE_OFFSET..bytes.len() {
            assert!(
                decode_operation_record(&bytes[VALUE_OFFSET..VALUE_OFFSET + truncated_len])
                    .is_err(),
                "accepted text AwaitCatchUpUpload record truncated to {truncated_len} bytes"
            );
        }
        assert_eq!(decode_operation_record(&bytes).unwrap(), operation);
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode_operation_record(&trailing).is_err());
    }

    #[test]
    fn text_build_await_compaction_upload_codec_rejects_every_truncation() {
        let definition = ValidatedDynamicIndexDefinition::Text(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Doc",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let cursor = |byte| {
            IndexCursor::try_new(
                IndexV2Key::operation(IndexOperationId::from_bytes([byte; 16]).unwrap()).to_bytes(),
            )
            .unwrap()
        };
        let progress = TextCompactionUploadProgress::try_new(
            PrefixScanProgress {
                cursor: Some(cursor(0x30)),
                counters: OperationCounters {
                    entities: 1,
                    input_bytes: 2,
                    output_operations: 3,
                    output_bytes: 4,
                },
            },
            vec![cursor(0x31), cursor(0x32)],
            OperationCounters {
                entities: 2,
                input_bytes: 3,
                output_operations: 4,
                output_bytes: 5,
            },
            cursor(0x33),
            crate::index_v2::TextUploadIntentId::from_bytes([0x34; 16]).unwrap(),
        )
        .unwrap();
        let operation = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([0x35; 16]).unwrap(),
            IndexId::initial(),
            definition.identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::AwaitCompactionUpload(progress),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        let bytes = encode_operation_record(&operation);
        const VALUE_OFFSET: usize = 0;

        for truncated_len in VALUE_OFFSET..bytes.len() {
            assert!(
                decode_operation_record(&bytes[VALUE_OFFSET..VALUE_OFFSET + truncated_len])
                    .is_err(),
                "accepted text AwaitCompactionUpload record truncated to {truncated_len} bytes"
            );
        }
        assert_eq!(decode_operation_record(&bytes).unwrap(), operation);
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode_operation_record(&trailing).is_err());
    }

    #[test]
    fn text_build_await_compaction_upload_codec_rejects_oversized_input_count() {
        let cursor = |byte| {
            IndexCursor::try_new(
                IndexV2Key::operation(IndexOperationId::from_bytes([byte; 16]).unwrap()).to_bytes(),
            )
            .unwrap()
        };
        let progress = TextCompactionUploadProgress::try_new(
            PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            },
            vec![cursor(0x41), cursor(0x42)],
            OperationCounters {
                entities: 1,
                input_bytes: 1,
                output_operations: 1,
                output_bytes: 1,
            },
            cursor(0x43),
            crate::index_v2::TextUploadIntentId::from_bytes([0x44; 16]).unwrap(),
        )
        .unwrap();
        let mut encoder = ValueEncoder::with_header(OPERATION_RECORD_KIND);
        put_text_compaction_upload(&mut encoder, &progress);
        let mut bytes = encoder.finish().to_vec();

        const HEADER_LEN: usize = U8_LEN + U8_LEN;
        const EMPTY_CURSOR_LEN: usize = U8_LEN;
        const COUNTER_COUNT: usize = 4;
        const COUNTERS_LEN: usize = COUNTER_COUNT * U64_LEN;
        const INPUT_COUNT_OFFSET: usize = HEADER_LEN + EMPTY_CURSOR_LEN + COUNTERS_LEN;
        let oversized_count = u16::try_from(TEXT_COMPACTION_INPUT_KEY_MAX + 1)
            .expect("the malformed test count fits u16")
            .to_be_bytes();
        bytes[INPUT_COUNT_OFFSET..INPUT_COUNT_OFFSET + U16_LEN].copy_from_slice(&oversized_count);

        let mut decoder = ValueDecoder::new(&bytes).unwrap();
        assert!(take_text_compaction_upload(&mut decoder).is_err());
    }

    #[test]
    fn text_manifest_validation_codec_rejects_every_truncation_and_unknown_lane() {
        let definition = ValidatedDynamicIndexDefinition::Text(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Node,
                "Doc",
                "body",
                None::<String>,
                TextAnalyzerKind::Standard,
                false,
            )
            .unwrap(),
        );
        let cursor = |byte| {
            IndexCursor::try_new(
                IndexV2Key::operation(IndexOperationId::from_bytes([byte; 16]).unwrap()).to_bytes(),
            )
            .unwrap()
        };
        let counters = OperationCounters {
            entities: 1,
            input_bytes: 2,
            output_operations: 3,
            output_bytes: 4,
        };
        let progresses = [
            TextManifestValidationProgress::Pages(
                TextManifestPageValidationProgress::try_new(
                    Some(cursor(0x51)),
                    Some(
                        TextManifestPartitionValidation::try_new(
                            [0x52; 32],
                            crate::index_v2::TextManifestRevision::new(4).unwrap(),
                            3,
                            5,
                            2,
                            3,
                        )
                        .unwrap(),
                    ),
                    counters,
                )
                .unwrap(),
            ),
            TextManifestValidationProgress::Roots(PrefixScanProgress {
                cursor: Some(cursor(0x53)),
                counters,
            }),
            TextManifestValidationProgress::UploadIntents(PrefixScanProgress {
                cursor: Some(cursor(0x54)),
                counters,
            }),
        ];

        for (ordinal, progress) in progresses.into_iter().enumerate() {
            let operation = IndexOperationRecord::try_new(
                IndexOperationId::from_bytes([0x55 + u8::try_from(ordinal).unwrap(); 16]).unwrap(),
                IndexId::initial(),
                definition.identity(),
                IndexGenerationId::initial(),
                IndexRevision::initial(),
                IndexOperationRevision::initial(),
                IndexOperationKind::Build,
                IndexOperationFamily::Text,
                IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                    TextBuildStage::ValidateManifests(progress),
                )),
                0,
                IndexOperationExecutionState::Queued {
                    not_before_unix_millis: None,
                },
            )
            .unwrap();
            let bytes = encode_operation_record(&operation);
            for truncated_len in 0..bytes.len() {
                assert!(
                    decode_operation_record(&bytes[..truncated_len]).is_err(),
                    "accepted validation lane {ordinal} truncated to {truncated_len} bytes"
                );
            }
            assert_eq!(decode_operation_record(&bytes).unwrap(), operation);
            let mut trailing = bytes.to_vec();
            trailing.push(0);
            assert!(decode_operation_record(&trailing).is_err());
        }

        let mut encoder = ValueEncoder::with_header(OPERATION_RECORD_KIND);
        encoder.put_u8(0x7F);
        let bytes = encoder.finish();
        let mut decoder = ValueDecoder::new(&bytes).unwrap();
        assert!(take_text_manifest_validation(&mut decoder).is_err());
    }

    #[test]
    fn text_build_scan_source_codec_rejects_every_truncation() {
        let text_definition = ValidatedDynamicIndexDefinition::Text(
            ValidatedTextIndexDefinition::try_new(
                IndexElementKind::Edge,
                "Doc",
                "body",
                Some("account_id"),
                TextAnalyzerKind::WhitespaceLowercase,
                true,
            )
            .unwrap(),
        );
        let cursor = |byte| {
            IndexCursor::try_new(
                IndexV2Key::operation(IndexOperationId::from_bytes([byte; 16]).unwrap()).to_bytes(),
            )
            .unwrap()
        };
        let operation = IndexOperationRecord::try_new(
            IndexOperationId::from_bytes([0x41; 16]).unwrap(),
            IndexId::initial(),
            text_definition.identity(),
            IndexGenerationId::initial(),
            IndexRevision::initial(),
            IndexOperationRevision::initial(),
            IndexOperationKind::Build,
            IndexOperationFamily::Text,
            IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
                TextBuildStage::ScanSource(SourceScanProgress {
                    inclusive_upper_bound: cursor(0x42),
                    cursor: Some(cursor(0x40)),
                    counters: OperationCounters {
                        entities: 1,
                        input_bytes: 2,
                        output_operations: 3,
                        output_bytes: 4,
                    },
                }),
            )),
            0,
            IndexOperationExecutionState::Queued {
                not_before_unix_millis: None,
            },
        )
        .unwrap();
        let bytes = encode_operation_record(&operation);

        for truncated_len in 0..bytes.len() {
            assert!(
                decode_operation_record(&bytes[..truncated_len]).is_err(),
                "accepted text ScanSource record truncated to {truncated_len} bytes"
            );
        }
        assert_eq!(decode_operation_record(&bytes).unwrap(), operation);
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode_operation_record(&trailing).is_err());
    }
}
