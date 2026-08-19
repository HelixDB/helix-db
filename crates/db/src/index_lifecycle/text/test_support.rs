use std::num::{NonZeroU64, NonZeroUsize};

use bytes::Bytes;

use crate::config::{SearchIndexBatchLimits, TextBackfillCompactionLimits, TextIndexDefinition};
use crate::encoding::v1::keys::tenant::DataScope;
use crate::encoding::v2::{keys as index_keys, values as index_values};
use crate::index_lifecycle::work;
use crate::index_lifecycle::{
    IndexGenerationId, IndexId, IndexOperationExecutionState, IndexOperationFamily,
    IndexOperationId, IndexOperationKind, IndexOperationProgress, IndexOperationRecord,
    IndexOperationRevision, IndexRevision, OperationCounters, PrefixScanProgress,
    TextBuildProgress, TextBuildStage, ValidatedTextIndexDefinition,
};

pub(super) fn operation() -> IndexOperationRecord {
    let runtime =
        TextIndexDefinition::new_node("Document", "body").expect("text test definition validates");
    let definition = ValidatedTextIndexDefinition::try_from_runtime(&runtime)
        .expect("text test definition has a V2 representation");
    IndexOperationRecord::try_new(
        IndexOperationId::new_v4(),
        IndexId::initial(),
        definition.identity(),
        IndexGenerationId::initial(),
        IndexRevision::initial(),
        IndexOperationRevision::initial(),
        IndexOperationKind::Build,
        IndexOperationFamily::Text,
        IndexOperationProgress::TextBuild(TextBuildProgress::Constructing(
            TextBuildStage::CatchUp(PrefixScanProgress {
                cursor: None,
                counters: OperationCounters::default(),
            }),
        )),
        0,
        IndexOperationExecutionState::Queued {
            not_before_unix_millis: None,
        },
    )
    .expect("text test operation is internally consistent")
}

pub(super) fn batch_limits(
    max_input_bytes: u64,
    max_output_operations: u64,
    max_output_bytes: u64,
) -> SearchIndexBatchLimits {
    SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(8).unwrap(),
        NonZeroU64::new(max_input_bytes).unwrap(),
        NonZeroU64::new(max_output_operations).unwrap(),
        NonZeroU64::new(max_output_bytes).unwrap(),
        NonZeroU64::new(max_output_bytes).unwrap(),
    )
    .expect("text test limits are internally consistent")
}

pub(super) fn compaction_limits(
    max_fan_in: usize,
    max_input_bytes: u64,
    max_temporary_disk_bytes: u64,
    max_output_blob_bytes: u64,
    max_manifest_bytes: u64,
) -> TextBackfillCompactionLimits {
    TextBackfillCompactionLimits::new(
        NonZeroUsize::new(max_fan_in).unwrap(),
        NonZeroU64::new(max_input_bytes).unwrap(),
        NonZeroU64::new(max_temporary_disk_bytes).unwrap(),
        NonZeroU64::new(max_output_blob_bytes).unwrap(),
        NonZeroU64::new(max_manifest_bytes).unwrap(),
    )
}

pub(super) fn split(seed: u8, size: u64) -> work::SplitRef {
    assert_eq!(size, 128, "test split uses the canonical exact layout");
    work::SplitRef::try_new(
        work::BlobRef::new([seed; 32], size),
        80,
        16,
        4,
        size,
        work::SplitPruning::from_terms([format!("term-{seed}")]),
    )
    .expect("text test split is internally consistent")
}

pub(super) fn artifact_row(
    scope: DataScope,
    operation: &IndexOperationRecord,
    partition: work::TextPartition,
    ordinal: u32,
    seed: u8,
) -> (Bytes, Bytes) {
    let key = index_keys::TextBuildArtifactKey {
        root: index_keys::TextManifestRootKey {
            index_id: operation.index_id(),
            generation: operation.generation(),
            partition: partition.fingerprint(),
        },
        ordinal,
    };
    let value = work::TextBuildArtifactValue {
        index_id: operation.index_id(),
        generation: operation.generation(),
        partition,
        artifact_ordinal: ordinal,
        split: split(seed, 128),
    };
    (
        index_keys::Key::Data {
            scope,
            kind: index_keys::ScopedKey::TextBuildArtifact(key),
        }
        .to_bytes(),
        index_values::encode_build_artifact(&value),
    )
}
