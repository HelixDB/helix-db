//! Stored lifecycle metadata for text indexes.

use bytes::Bytes;

use super::super::{
    put_element_kind, put_generation, put_index_id, put_partition, put_split_ref,
    take_element_kind, take_generation, take_index_id, take_logical_version,
    take_manifest_revision, take_partition, take_split_ref, unknown_discriminant, work_model_error,
    ValueDecoder, ValueEncoder, HASH_LEN, MAX_COLLECTION_ITEMS, MAX_LENGTH_DELIMITED_FIELD,
    U32_LEN,
};
use super::super::{INDEX_V2_VALUE_VERSION, INDEX_V3_SPLIT_VALUE_VERSION};
use crate::encoding::error::EncodingError;
use crate::index_lifecycle::work::{
    TextBuildArtifactValue, TextCorpusStatisticsValue, TextEntityStateValue, TextManifestPageValue,
    TextManifestRootValue, TextStatisticsContribution, TextStatisticsEntityValue,
    TextTermStatisticsValue,
};
use crate::index_lifecycle::IndexEntityId;

const MANIFEST_ROOT_KIND: u8 = 0x06;
const MANIFEST_PAGE_KIND: u8 = 0x07;
const BUILD_ARTIFACT_KIND: u8 = 0x09;
const ENTITY_STATE_KIND: u8 = 0x0C;
const CORPUS_STATISTICS_KIND: u8 = 0x10;
const TERM_STATISTICS_KIND: u8 = 0x11;
const STATISTICS_ENTITY_KIND: u8 = 0x12;

fn decoder(
    value: &[u8],
    expected_kind: u8,
    supports_legacy_split_version: bool,
) -> Result<(ValueDecoder<'_>, bool), EncodingError> {
    let decoder = ValueDecoder::with_supported_versions(
        value,
        &[INDEX_V2_VALUE_VERSION, INDEX_V3_SPLIT_VALUE_VERSION],
    )?;
    if decoder.kind() != expected_kind {
        return Err(EncodingError::UnexpectedValueKind {
            expected: expected_kind,
            actual: decoder.kind(),
        });
    }
    let pruning_version = decoder.version() == INDEX_V3_SPLIT_VALUE_VERSION;
    if pruning_version && !supports_legacy_split_version {
        return Err(EncodingError::Custom(format!(
            "value version {:#04x} is unsupported for text value kind {expected_kind:#04x}",
            decoder.version(),
        )));
    }
    Ok((decoder, pruning_version))
}

pub(crate) fn encode_manifest_root(value: &TextManifestRootValue) -> Bytes {
    let mut encoder = ValueEncoder::with_header(MANIFEST_ROOT_KIND);
    put_index_id(&mut encoder, value.index_id());
    put_generation(&mut encoder, value.generation());
    put_partition(&mut encoder, value.partition());
    encoder.put_u64(value.revision().get());
    encoder.put_u32(value.page_count());
    encoder.put_u64(value.split_count());
    encoder.finish()
}

pub(crate) fn decode_manifest_root(value: &[u8]) -> Result<TextManifestRootValue, EncodingError> {
    let (mut decoder, _) = decoder(value, MANIFEST_ROOT_KIND, false)?;
    let decoded = TextManifestRootValue::try_new(
        take_index_id(&mut decoder)?,
        take_generation(&mut decoder)?,
        take_partition(&mut decoder)?,
        take_manifest_revision(&mut decoder)?,
        decoder.take_u32()?,
        decoder.take_u64()?,
    )
    .map_err(work_model_error)?;
    decoder.finish()?;
    Ok(decoded)
}

pub(crate) fn encode_manifest_page(value: &TextManifestPageValue) -> Bytes {
    let mut encoder =
        ValueEncoder::with_versioned_header(INDEX_V3_SPLIT_VALUE_VERSION, MANIFEST_PAGE_KIND);
    put_index_id(&mut encoder, value.index_id());
    put_generation(&mut encoder, value.generation());
    put_partition(&mut encoder, value.partition());
    encoder.put_u32(value.page());
    encoder.put_u32(u32::try_from(value.entries().len()).expect("bounded manifest page fits u32"));
    for split in value.entries() {
        put_split_ref(&mut encoder, *split);
    }
    encoder.finish()
}

pub(crate) fn decode_manifest_page(value: &[u8]) -> Result<TextManifestPageValue, EncodingError> {
    let (mut decoder, pruning_version) = decoder(value, MANIFEST_PAGE_KIND, true)?;
    let index_id = take_index_id(&mut decoder)?;
    let generation = take_generation(&mut decoder)?;
    let partition = take_partition(&mut decoder)?;
    let page = decoder.take_u32()?;
    let count = decoder.take_u32()? as usize;
    if count > MAX_COLLECTION_ITEMS {
        return Err(EncodingError::Custom(format!(
            "manifest entry count {count} exceeds maximum {MAX_COLLECTION_ITEMS}"
        )));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(take_split_ref(&mut decoder, pruning_version)?);
    }
    decoder.finish()?;
    TextManifestPageValue::try_new(index_id, generation, partition, page, entries)
        .map_err(work_model_error)
}

pub(crate) fn encode_build_artifact(value: &TextBuildArtifactValue) -> Bytes {
    let mut encoder =
        ValueEncoder::with_versioned_header(INDEX_V3_SPLIT_VALUE_VERSION, BUILD_ARTIFACT_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_partition(&mut encoder, &value.partition);
    encoder.put_u32(value.artifact_ordinal);
    put_split_ref(&mut encoder, value.split);
    encoder.finish()
}

pub(crate) fn decode_build_artifact(value: &[u8]) -> Result<TextBuildArtifactValue, EncodingError> {
    let (mut decoder, pruning_version) = decoder(value, BUILD_ARTIFACT_KIND, true)?;
    let decoded = TextBuildArtifactValue {
        index_id: take_index_id(&mut decoder)?,
        generation: take_generation(&mut decoder)?,
        partition: take_partition(&mut decoder)?,
        artifact_ordinal: decoder.take_u32()?,
        split: take_split_ref(&mut decoder, pruning_version)?,
    };
    decoder.finish()?;
    Ok(decoded)
}

pub(crate) fn encode_entity_state(value: &TextEntityStateValue) -> Bytes {
    let mut encoder = ValueEncoder::with_header(ENTITY_STATE_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_partition(&mut encoder, &value.partition);
    put_element_kind(&mut encoder, value.entity_kind);
    encoder.put_u64(value.entity_id.get());
    encoder.put_u64(value.logical_version.get());
    encoder.put_bool(value.live);
    encoder.finish()
}

pub(crate) fn decode_entity_state(value: &[u8]) -> Result<TextEntityStateValue, EncodingError> {
    let (mut decoder, _) = decoder(value, ENTITY_STATE_KIND, false)?;
    let decoded = TextEntityStateValue {
        index_id: take_index_id(&mut decoder)?,
        generation: take_generation(&mut decoder)?,
        partition: take_partition(&mut decoder)?,
        entity_kind: take_element_kind(&mut decoder)?,
        entity_id: IndexEntityId::new(decoder.take_u64()?),
        logical_version: take_logical_version(&mut decoder)?,
        live: decoder.take_bool()?,
    };
    decoder.finish()?;
    Ok(decoded)
}

pub(crate) fn encode_corpus_statistics(value: &TextCorpusStatisticsValue) -> Bytes {
    let mut encoder = ValueEncoder::with_header(CORPUS_STATISTICS_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_partition(&mut encoder, &value.partition);
    encoder.put_u64(value.document_count);
    encoder.put_u64(value.total_token_count);
    encoder.finish()
}

pub(crate) fn decode_corpus_statistics(
    value: &[u8],
) -> Result<TextCorpusStatisticsValue, EncodingError> {
    let (mut decoder, _) = decoder(value, CORPUS_STATISTICS_KIND, false)?;
    let decoded = TextCorpusStatisticsValue::try_new(
        take_index_id(&mut decoder)?,
        take_generation(&mut decoder)?,
        take_partition(&mut decoder)?,
        decoder.take_u64()?,
        decoder.take_u64()?,
    )
    .map_err(work_model_error)?;
    decoder.finish()?;
    Ok(decoded)
}

pub(crate) fn encode_term_statistics(value: &TextTermStatisticsValue) -> Bytes {
    let mut encoder = ValueEncoder::with_header(TERM_STATISTICS_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_partition(&mut encoder, &value.partition);
    encoder.put_bytes(&value.term);
    encoder.put_u64(value.document_frequency);
    encoder.finish()
}

pub(crate) fn decode_term_statistics(
    value: &[u8],
) -> Result<TextTermStatisticsValue, EncodingError> {
    let (mut decoder, _) = decoder(value, TERM_STATISTICS_KIND, false)?;
    let decoded = TextTermStatisticsValue::try_new(
        take_index_id(&mut decoder)?,
        take_generation(&mut decoder)?,
        take_partition(&mut decoder)?,
        decoder.take_bytes(MAX_LENGTH_DELIMITED_FIELD)?,
        decoder.take_u64()?,
    )
    .map_err(work_model_error)?;
    decoder.finish()?;
    Ok(decoded)
}

pub(crate) fn encode_statistics_entity(value: &TextStatisticsEntityValue) -> Bytes {
    let mut encoder = ValueEncoder::with_header(STATISTICS_ENTITY_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_element_kind(&mut encoder, value.entity_kind);
    encoder.put_u64(value.entity_id.get());
    match &value.contribution {
        TextStatisticsContribution::Absent => encoder.put_u8(0x01),
        TextStatisticsContribution::Present {
            partition,
            fingerprint,
            token_count,
            terms,
        } => {
            encoder.put_u8(0x02);
            put_partition(&mut encoder, partition);
            encoder.put_raw(fingerprint);
            encoder.put_u64(*token_count);
            encoder.put_u32(
                u32::try_from(terms.len()).expect("bounded text statistics term count fits u32"),
            );
            for term in terms {
                encoder.put_bytes(term);
            }
        }
    }
    encoder.finish()
}

pub(crate) fn decode_statistics_entity(
    value: &[u8],
) -> Result<TextStatisticsEntityValue, EncodingError> {
    let (mut decoder, _) = decoder(value, STATISTICS_ENTITY_KIND, false)?;
    let index_id = take_index_id(&mut decoder)?;
    let generation = take_generation(&mut decoder)?;
    let entity_kind = take_element_kind(&mut decoder)?;
    let entity_id = IndexEntityId::new(decoder.take_u64()?);
    let contribution = match decoder.take_u8()? {
        0x01 => TextStatisticsContribution::Absent,
        0x02 => {
            let partition = take_partition(&mut decoder)?;
            let fingerprint = decoder.take_array::<HASH_LEN>()?;
            let token_count = decoder.take_u64()?;
            let term_count = decoder.take_u32()? as usize;
            const MAX_TERM_COUNT: usize = MAX_LENGTH_DELIMITED_FIELD / U32_LEN;
            if term_count > MAX_TERM_COUNT {
                return Err(EncodingError::Custom(format!(
                    "text statistics term count {term_count} exceeds maximum {MAX_TERM_COUNT}"
                )));
            }
            let mut terms = Vec::with_capacity(term_count);
            for _ in 0..term_count {
                terms.push(decoder.take_bytes(MAX_LENGTH_DELIMITED_FIELD)?);
            }
            TextStatisticsContribution::try_present(partition, fingerprint, token_count, terms)
                .map_err(work_model_error)?
        }
        unknown => {
            return Err(unknown_discriminant(
                "text statistics contribution",
                unknown,
            ))
        }
    };
    decoder.finish()?;
    Ok(TextStatisticsEntityValue {
        index_id,
        generation,
        entity_kind,
        entity_id,
        contribution,
    })
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;
    use crate::index_lifecycle::work::{BlobRef, SplitPruning, SplitRef, TextPartition};
    use crate::index_lifecycle::{
        IndexElementKind, IndexGenerationId, IndexId, TextLogicalVersion, TextManifestRevision,
    };

    fn index_id() -> IndexId {
        IndexId::new(1).unwrap()
    }

    fn generation() -> IndexGenerationId {
        IndexGenerationId::new(2).unwrap()
    }

    fn partition() -> TextPartition {
        TextPartition::try_tenant_value(Bytes::from_static(b"tenant")).unwrap()
    }

    fn split(pruning: SplitPruning) -> SplitRef {
        SplitRef::try_new(BlobRef::new([7; 32], 100), 80, 20, 0, 100, pruning).unwrap()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
    }

    #[test]
    fn split_versions_round_trip_and_reject_unknown_tags() {
        let page = TextManifestPageValue::try_new(
            index_id(),
            generation(),
            TextPartition::Unpartitioned,
            0,
            vec![
                split(SplitPruning::Unavailable),
                split(SplitPruning::from_terms([b"alpha".as_slice()])),
            ],
        )
        .unwrap();
        let encoded = encode_manifest_page(&page);
        assert_eq!(encoded[0], INDEX_V3_SPLIT_VALUE_VERSION);
        assert_eq!(decode_manifest_page(&encoded).unwrap(), page);

        let legacy_page = TextManifestPageValue::try_new(
            index_id(),
            generation(),
            TextPartition::Unpartitioned,
            0,
            vec![split(SplitPruning::TermBloom256([42, 43, 44, 45]))],
        )
        .unwrap();
        let mut legacy = encode_manifest_page(&legacy_page).to_vec();
        legacy[0] = INDEX_V2_VALUE_VERSION;
        legacy.truncate(
            legacy.len()
                - core::mem::size_of::<u8>()
                - core::mem::size_of::<u64>()
                    * crate::index_lifecycle::work::SPLIT_PRUNING_BLOOM_WORDS,
        );
        assert_eq!(
            decode_manifest_page(&legacy).unwrap().entries()[0].pruning(),
            SplitPruning::Unavailable
        );

        let mut malformed = encode_manifest_page(
            &TextManifestPageValue::try_new(
                index_id(),
                generation(),
                TextPartition::Unpartitioned,
                0,
                vec![split(SplitPruning::Unavailable)],
            )
            .unwrap(),
        )
        .to_vec();
        *malformed.last_mut().unwrap() = 0xFF;
        assert!(decode_manifest_page(&malformed).is_err());
        malformed[0] = INDEX_V3_SPLIT_VALUE_VERSION + 1;
        assert!(decode_manifest_page(&malformed).is_err());
    }

    #[test]
    fn typed_decoder_distinguishes_cross_kind_from_unsupported_version() {
        let artifact = encode_build_artifact(&TextBuildArtifactValue {
            index_id: index_id(),
            generation: generation(),
            partition: TextPartition::Unpartitioned,
            artifact_ordinal: 0,
            split: split(SplitPruning::Unavailable),
        });
        assert!(matches!(
            decode_manifest_root(&artifact),
            Err(EncodingError::UnexpectedValueKind {
                expected: MANIFEST_ROOT_KIND,
                actual: BUILD_ARTIFACT_KIND,
            })
        ));

        let mut unsupported = encode_manifest_root(&TextManifestRootValue::empty(
            index_id(),
            generation(),
            TextPartition::Unpartitioned,
        ))
        .to_vec();
        unsupported[0] = INDEX_V3_SPLIT_VALUE_VERSION;
        assert!(matches!(
            decode_manifest_root(&unsupported),
            Err(EncodingError::Custom(reason)) if reason.contains("unsupported")
        ));
    }

    #[test]
    fn text_value_bytes_are_frozen() {
        let partition = partition();
        let split = split(SplitPruning::TermBloom256([1, 2, 3, 4]));
        let root = TextManifestRootValue::try_new(
            index_id(),
            generation(),
            partition.clone(),
            TextManifestRevision::new(4).unwrap(),
            1,
            1,
        )
        .unwrap();
        let page = TextManifestPageValue::try_new(
            index_id(),
            generation(),
            partition.clone(),
            4,
            vec![split],
        )
        .unwrap();
        let artifact = TextBuildArtifactValue {
            index_id: index_id(),
            generation: generation(),
            partition: partition.clone(),
            artifact_ordinal: 4,
            split,
        };
        let entity = TextEntityStateValue {
            index_id: index_id(),
            generation: generation(),
            partition: partition.clone(),
            entity_kind: IndexElementKind::Node,
            entity_id: IndexEntityId::new(3),
            logical_version: TextLogicalVersion::new(4).unwrap(),
            live: true,
        };
        let corpus =
            TextCorpusStatisticsValue::try_new(index_id(), generation(), partition.clone(), 5, 9)
                .unwrap();
        let term = TextTermStatisticsValue::try_new(
            index_id(),
            generation(),
            partition.clone(),
            Bytes::from_static(b"term"),
            5,
        )
        .unwrap();
        let absent = TextStatisticsEntityValue {
            index_id: index_id(),
            generation: generation(),
            entity_kind: IndexElementKind::Edge,
            entity_id: IndexEntityId::new(3),
            contribution: TextStatisticsContribution::Absent,
        };
        let present = TextStatisticsEntityValue {
            index_id: index_id(),
            generation: generation(),
            entity_kind: IndexElementKind::Node,
            entity_id: IndexEntityId::new(3),
            contribution: TextStatisticsContribution::try_present(
                partition,
                [0x44; 32],
                9,
                vec![Bytes::from_static(b"alpha"), Bytes::from_static(b"beta")],
            )
            .unwrap(),
        };

        let encoded_root = encode_manifest_root(&root);
        let encoded_page = encode_manifest_page(&page);
        let encoded_artifact = encode_build_artifact(&artifact);
        let encoded_entity = encode_entity_state(&entity);
        let encoded_corpus = encode_corpus_statistics(&corpus);
        let encoded_term = encode_term_statistics(&term);
        let encoded_absent = encode_statistics_entity(&absent);
        let encoded_present = encode_statistics_entity(&present);
        assert_eq!(decode_manifest_root(&encoded_root).unwrap(), root);
        assert_eq!(decode_manifest_page(&encoded_page).unwrap(), page);
        assert_eq!(decode_build_artifact(&encoded_artifact).unwrap(), artifact);
        assert_eq!(decode_entity_state(&encoded_entity).unwrap(), entity);
        assert_eq!(decode_corpus_statistics(&encoded_corpus).unwrap(), corpus);
        assert_eq!(decode_term_statistics(&encoded_term).unwrap(), term);
        assert_eq!(decode_statistics_entity(&encoded_absent).unwrap(), absent);
        assert_eq!(decode_statistics_entity(&encoded_present).unwrap(), present);

        insta::assert_snapshot!(
            format!(
                "manifest_root={}\nmanifest_page={}\nbuild_artifact={}\nentity_state={}\ncorpus_statistics={}\nterm_statistics={}\nstatistics_entity_absent={}\nstatistics_entity_present={}\n",
                hex(&encoded_root),
                hex(&encoded_page),
                hex(&encoded_artifact),
                hex(&encoded_entity),
                hex(&encoded_corpus),
                hex(&encoded_term),
                hex(&encoded_absent),
                hex(&encoded_present),
            ),
            @"
manifest_root=010600000000000000010000000000000002020000000674656e616e740000000000000004000000010000000000000001
manifest_page=020700000000000000010000000000000002020000000674656e616e74000000040000000107070707070707070707070707070707070707070707070707070707070707070000000000000064000000000000005000000014000000000000000000000064010000000000000001000000000000000200000000000000030000000000000004
build_artifact=020900000000000000010000000000000002020000000674656e616e740000000407070707070707070707070707070707070707070707070707070707070707070000000000000064000000000000005000000014000000000000000000000064010000000000000001000000000000000200000000000000030000000000000004
entity_state=010c00000000000000010000000000000002020000000674656e616e74010000000000000003000000000000000401
corpus_statistics=011000000000000000010000000000000002020000000674656e616e7400000000000000050000000000000009
term_statistics=011100000000000000010000000000000002020000000674656e616e74000000047465726d0000000000000005
statistics_entity_absent=01120000000000000001000000000000000202000000000000000301
statistics_entity_present=01120000000000000001000000000000000201000000000000000302020000000674656e616e74444444444444444444444444444444444444444444444444444444444444444400000000000000090000000200000005616c7068610000000462657461
"
        );
    }
}
