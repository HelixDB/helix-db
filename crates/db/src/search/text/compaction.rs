//! Current-format text split inspection and bounded compaction.
//!
//! Hidden lifecycle callers materialize immutable split blobs under explicit
//! fan-in/input/temporary/output limits, inspect logical document versions,
//! and merge only after generation-qualified applied state is resolved. Active
//! compaction retains the same manifest/split codecs. Temporary paths are owned
//! by returned preparation values or function-local guards and are removed on
//! success, error, cancellation, and drop.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IsolationLevel};
use tantivy::directory::MmapDirectory;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::query::{BooleanQuery, Occur, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::{Directory, Index, TantivyDocument, Term};
use uuid::Uuid;

use crate::config::{TextBackfillCompactionLimits, TextIndexDefinition};
use crate::encoding::keys::tenant::DataScope;
use crate::error::HelixDbError;
use crate::search::{
    make_text_index_live_state_prefix_scoped, make_text_index_manifest_key_scoped,
};

use super::materialize_split_ref_to_file;
use super::overlay_directory::OverlayDirectory;
use super::split::{build_split_bundle, open_split_directory_from_file};
use super::{
    decode_live_state_bytes, lookup_schema_fields, register_analyzers, upload_blob, TextBlobRef,
    TextIndexGenerationManifest, TextSchemaFields, TextSplitRef, UnpublishedTextSplit,
    META_JSON_FILE,
};

const MANAGED_JSON_FILE: &str = ".managed.json";
const TEXT_COMPACTION_MAX_INPUT_SPLITS: usize = 8;
const TEXT_COMPACTION_MAX_INPUT_BYTES: u64 = 256 * 1024 * 1024;

/// Materialized bounded build splits awaiting applied-state pruning and merge.
///
/// Preparation downloads each selected current-format split exactly once,
/// reserves input plus maximum output temporary bytes before creating files,
/// and exposes only unique `(entity_id, logical_version)` pairs. The lifecycle
/// caller resolves those pairs against generation-qualified applied state, then
/// consumes this value with [`Self::finish`] to build one unpublished unchanged
/// split. Holding the private temporary directory makes using paths after
/// cleanup unrepresentable.
pub(crate) struct PreparedTextBuildCompaction {
    _temporary_directory: tempfile::TempDir,
    split_paths: Vec<PathBuf>,
    output_dir: PathBuf,
    manifest: TextIndexGenerationManifest,
    document_versions: Vec<(u64, u64)>,
    input_bytes: NonZeroU64,
    max_output_blob_bytes: NonZeroU64,
}

impl PreparedTextBuildCompaction {
    /// Returns unique document identities requiring applied-state resolution.
    pub(crate) fn document_versions(&self) -> &[(u64, u64)] {
        &self.document_versions
    }

    /// Returns exact immutable split bytes materialized by preparation.
    pub(crate) const fn input_bytes(&self) -> NonZeroU64 {
        self.input_bytes
    }

    /// Prunes stale document versions and merges one unpublished unchanged split.
    ///
    /// `live_versions` must come from the same hidden generation and partition
    /// as the selected pages. An entity absent from the map is deleted from the
    /// merge output. `None` means every selected document was stale; callers
    /// must then delete the input pages without creating an output page.
    pub(crate) async fn finish(
        self,
        live_versions: HashMap<u64, u64>,
    ) -> Result<Option<UnpublishedTextSplit>, TextBuildCompactionError> {
        let (merged_split, _) = tokio::task::spawn_blocking({
            let split_paths = self.split_paths.clone();
            let output_dir = self.output_dir.clone();
            let manifest = self.manifest.clone();
            move || {
                merge_local_splits_into_manifest(
                    split_paths,
                    &output_dir,
                    &manifest,
                    &live_versions,
                )
            }
        })
        .await
        .map_err(|error| {
            TextBuildCompactionError::Database(HelixDbError::Config(format!(
                "failed to join hidden text compaction task: {error}"
            )))
        })??;
        let Some(merged_split) = merged_split else {
            return Ok(None);
        };
        let output_bytes = NonZeroU64::new(merged_split.total_size_bytes)
            .ok_or(TextBuildCompactionError::OutputBlobEmpty)?;
        if output_bytes > self.max_output_blob_bytes {
            return Err(TextBuildCompactionError::OutputBlobExceeded {
                required: output_bytes,
                limit: self.max_output_blob_bytes,
            });
        }
        Ok(Some(UnpublishedTextSplit::from_built_split(merged_split)))
    }
}

/// Downloads and inspects one already selected whole-page split set.
///
/// The caller performs whole-page selection; this boundary independently
/// rechecks fan-in, immutable input, and conservative temporary-disk bounds
/// before touching local disk. It writes no database, manifest, live-state, or
/// catalog record or object. [`PreparedTextBuildCompaction::finish`] also
/// performs no object I/O; the caller must first persist an exact upload child
/// before submitting the returned unpublished payload to the coordinator.
pub(crate) async fn prepare_text_build_compaction(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    definition: &TextIndexDefinition,
    physical_index_name: &str,
    split_refs: &[TextSplitRef],
    limits: TextBackfillCompactionLimits,
) -> Result<PreparedTextBuildCompaction, TextBuildCompactionError> {
    if split_refs.len() < 2 {
        return Err(TextBuildCompactionError::TooFewInputSplits);
    }
    if split_refs.len() > limits.max_fan_in().get() {
        return Err(TextBuildCompactionError::FanInExceeded {
            required: NonZeroU64::new(
                u64::try_from(split_refs.len())
                    .map_err(|_| TextBuildCompactionError::MeasurementOverflow)?,
            )
            .expect("a useful split set is non-empty"),
            limit: NonZeroU64::new(
                u64::try_from(limits.max_fan_in().get())
                    .map_err(|_| TextBuildCompactionError::MeasurementOverflow)?,
            )
            .expect("fan-in limits are positive"),
        });
    }
    let input_bytes = split_refs.iter().try_fold(0_u64, |total, split| {
        total
            .checked_add(split.total_size_bytes)
            .ok_or(TextBuildCompactionError::MeasurementOverflow)
    })?;
    let input_bytes =
        NonZeroU64::new(input_bytes).ok_or(TextBuildCompactionError::InputSplitBytesEmpty)?;
    if input_bytes > limits.max_input_bytes() {
        return Err(TextBuildCompactionError::InputBytesExceeded {
            required: input_bytes,
            limit: limits.max_input_bytes(),
        });
    }
    let temporary_disk_reservation = input_bytes
        .get()
        .checked_add(limits.max_output_blob_bytes().get())
        .and_then(NonZeroU64::new)
        .ok_or(TextBuildCompactionError::MeasurementOverflow)?;
    if temporary_disk_reservation > limits.max_temporary_disk_bytes() {
        return Err(TextBuildCompactionError::TemporaryDiskExceeded {
            required: temporary_disk_reservation,
            limit: limits.max_temporary_disk_bytes(),
        });
    }

    let temporary_directory = tempfile::Builder::new()
        .prefix("helix-text-build-compact-")
        .tempdir_in(std::env::temp_dir())
        .map_err(|error| {
            HelixDbError::Config(format!(
                "failed to create hidden text compaction tempdir: {error}"
            ))
        })?;
    let input_dir = temporary_directory.path().join("inputs");
    let output_dir = temporary_directory.path().join("output");
    fs::create_dir_all(&input_dir).map_err(|error| {
        HelixDbError::Config(format!(
            "failed to create hidden text compaction input dir '{}': {error}",
            input_dir.display()
        ))
    })?;
    fs::create_dir_all(&output_dir).map_err(|error| {
        HelixDbError::Config(format!(
            "failed to create hidden text compaction output dir '{}': {error}",
            output_dir.display()
        ))
    })?;
    let mut split_paths = Vec::with_capacity(split_refs.len());
    for (index, split_ref) in split_refs.iter().enumerate() {
        let split_path = input_dir.join(format!("split-{index}.split"));
        materialize_split_ref_to_file(store, db_path, split_ref, &split_path).await?;
        split_paths.push(split_path);
    }
    let first = split_refs
        .first()
        .expect("a useful compaction has at least two splits")
        .clone();
    let mut manifest = TextIndexGenerationManifest::new_split(
        physical_index_name.to_string(),
        "hidden-build-compaction".to_string(),
        definition.analyzer(),
        definition.positions_enabled(),
        first.clone(),
    );
    manifest.split = first;
    manifest.splits = split_refs.to_vec();
    let split_paths_for_inspection = split_paths.clone();
    let output_for_inspection = output_dir.clone();
    let analyzer = definition.analyzer();
    let document_versions = tokio::task::spawn_blocking(move || {
        inspect_local_split_document_versions(
            &split_paths_for_inspection,
            &output_for_inspection,
            analyzer,
        )
    })
    .await
    .map_err(|error| {
        TextBuildCompactionError::Database(HelixDbError::Config(format!(
            "failed to join hidden text inspection task: {error}"
        )))
    })??;
    Ok(PreparedTextBuildCompaction {
        _temporary_directory: temporary_directory,
        split_paths,
        output_dir,
        manifest,
        document_versions,
        input_bytes,
        max_output_blob_bytes: limits.max_output_blob_bytes(),
    })
}

/// Opens selected local splits and returns unique live document-version pairs.
fn inspect_local_split_document_versions(
    split_paths: &[PathBuf],
    output_dir: &Path,
    analyzer: crate::config::TextAnalyzerKind,
) -> Result<Vec<(u64, u64)>, TextBuildCompactionError> {
    let mut input_directories: Vec<Box<dyn Directory>> = Vec::with_capacity(split_paths.len());
    for split_path in split_paths {
        input_directories.push(Box::new(open_split_directory_from_file(split_path)?));
    }
    let synthetic_meta = build_synthetic_meta_json(&input_directories)?;
    let output_directory = MmapDirectory::open(output_dir).map_err(|error| {
        HelixDbError::Config(format!(
            "failed to open hidden text compaction output dir '{}': {error}",
            output_dir.display()
        ))
    })?;
    output_directory
        .atomic_write(Path::new(META_JSON_FILE), &synthetic_meta)
        .map_err(|error| {
            HelixDbError::Config(format!(
                "failed to write hidden text compaction meta '{}': {error}",
                output_dir.display()
            ))
        })?;
    let mut overlay_stack: Vec<Box<dyn Directory>> = vec![Box::new(output_directory)];
    overlay_stack.extend(input_directories);
    let overlay = OverlayDirectory::union_of(overlay_stack);
    let index = open_overlay_index(&overlay, analyzer)?;
    let fields = lookup_schema_fields(&index.schema())?;
    let logical_version_field = fields.logical_version;
    let reader = super::build_reader(&index)?;
    let mut versions = HashSet::new();
    for segment_reader in reader.searcher().segment_readers() {
        let entity_ids = segment_reader
            .fast_fields()
            .u64(super::ENTITY_ID_FIELD_NAME)
            .map_err(|error| {
                HelixDbError::InvariantViolation(format!(
                    "hidden text compaction entity_id fast field is unavailable: {error}"
                ))
            })?;
        let logical_versions = segment_reader
            .fast_fields()
            .u64(super::LOGICAL_VERSION_FIELD_NAME)
            .map_err(|error| {
                HelixDbError::InvariantViolation(format!(
                    "hidden text compaction logical_version fast field is unavailable: {error}"
                ))
            })?;
        let alive_bitset = segment_reader.alive_bitset();
        for document_id in 0..segment_reader.max_doc() {
            if alive_bitset
                .as_ref()
                .is_some_and(|bitset| bitset.is_deleted(document_id))
            {
                continue;
            }
            let entity_id = entity_ids.first(document_id).ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "hidden text compaction document is missing entity_id".to_string(),
                )
            })?;
            let logical_version = logical_versions.first(document_id).ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "hidden text compaction document is missing logical_version".to_string(),
                )
            })?;
            if !versions.insert((entity_id, logical_version)) {
                return Err(TextBuildCompactionError::DuplicateDocumentVersion {
                    entity_id,
                    logical_version,
                });
            }
        }
    }
    let _ = logical_version_field;
    let mut versions = versions.into_iter().collect::<Vec<_>>();
    versions.sort_unstable();
    Ok(versions)
}

/// Hidden build compaction validation, capacity, I/O, or merge failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TextBuildCompactionError {
    /// Useful compaction requires at least two immutable input splits.
    #[error("hidden text compaction requires at least two input splits")]
    TooFewInputSplits,
    /// Input split count exceeded configured fan-in.
    #[error("hidden text compaction needs fan-in {required} but limit is {limit}")]
    FanInExceeded {
        /// Positive required split count.
        required: NonZeroU64,
        /// Configured positive fan-in.
        limit: NonZeroU64,
    },
    /// Selected input declared an invalid zero byte total.
    #[error("hidden text compaction input split bytes must be positive")]
    InputSplitBytesEmpty,
    /// Referenced immutable input exceeded configured bytes.
    #[error("hidden text compaction needs {required} input bytes but limit is {limit}")]
    InputBytesExceeded {
        /// Positive referenced input bytes.
        required: NonZeroU64,
        /// Configured positive input ceiling.
        limit: NonZeroU64,
    },
    /// Conservative input-plus-output reservation exceeded temporary storage.
    #[error("hidden text compaction needs {required} temporary bytes but limit is {limit}")]
    TemporaryDiskExceeded {
        /// Positive temporary reservation.
        required: NonZeroU64,
        /// Configured positive temporary ceiling.
        limit: NonZeroU64,
    },
    /// Merge unexpectedly produced an empty encoded split.
    #[error("hidden text compaction output split bytes must be positive")]
    OutputBlobEmpty,
    /// Produced split exceeded configured immutable output bytes.
    #[error("hidden text compaction needs {required} output bytes but limit is {limit}")]
    OutputBlobExceeded {
        /// Positive produced split bytes.
        required: NonZeroU64,
        /// Configured positive output ceiling.
        limit: NonZeroU64,
    },
    /// Same entity/version appeared more than once across selected splits.
    #[error("duplicate hidden text document {entity_id} at logical version {logical_version}")]
    DuplicateDocumentVersion {
        /// Duplicated entity ID.
        entity_id: u64,
        /// Duplicated logical version.
        logical_version: u64,
    },
    /// Checked count or byte arithmetic overflowed `u64`.
    #[error("hidden text compaction measurement overflowed u64")]
    MeasurementOverflow,
    /// Existing split materialization, Tantivy inspection, or local merge failed.
    #[error(transparent)]
    Database(#[from] HelixDbError),
}

pub(crate) async fn compact_manifest_merge_only_scoped(
    db: &Db,
    scope: DataScope,
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    manifest: &TextIndexGenerationManifest,
) -> Result<bool, HelixDbError> {
    let manifest = manifest.clone();
    if manifest.split_refs().len() <= 1 {
        return Ok(false);
    }
    let Some(selected_split_count) = select_split_prefix_for_compaction(
        &manifest,
        TEXT_COMPACTION_MAX_INPUT_SPLITS,
        TEXT_COMPACTION_MAX_INPUT_BYTES,
    ) else {
        return Ok(false);
    };
    let selected_split_refs = manifest.split_refs()[..selected_split_count].to_vec();
    let remaining_split_refs = manifest.split_refs()[selected_split_count..].to_vec();
    let live_versions = load_live_versions(db, scope, &manifest.physical_index_name).await?;

    let tempdir = tempfile::Builder::new()
        .prefix("helix-text-compact-")
        .tempdir_in(std::env::temp_dir())
        .map_err(|err| {
            HelixDbError::Config(format!("failed to create text compaction tempdir: {err}"))
        })?;
    let input_dir = tempdir.path().join("inputs");
    let output_dir = tempdir.path().join("output");
    fs::create_dir_all(&input_dir).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to create text compaction input dir '{}': {err}",
            input_dir.display()
        ))
    })?;
    fs::create_dir_all(&output_dir).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to create text compaction output dir '{}': {err}",
            output_dir.display()
        ))
    })?;

    let mut split_paths = Vec::with_capacity(selected_split_refs.len());
    for (idx, split_ref) in selected_split_refs.iter().enumerate() {
        let split_path = input_dir.join(format!("split-{idx}.split"));
        materialize_split_ref_to_file(store, db_path, split_ref, &split_path).await?;
        split_paths.push(split_path);
    }

    let (merged_split, _new_manifest) = tokio::task::spawn_blocking({
        let split_paths = split_paths.clone();
        let manifest = manifest.clone();
        let live_versions = live_versions.clone();
        let output_dir = output_dir.clone();
        move || {
            merge_local_splits_into_manifest(split_paths, &output_dir, &manifest, &live_versions)
        }
    })
    .await
    .map_err(|err| HelixDbError::Config(format!("failed to join text compaction task: {err}")))??;

    let manifest_key = make_text_index_manifest_key_scoped(scope, &manifest.physical_index_name);
    let tx = db.begin(IsolationLevel::Snapshot).await?;
    let next_split_refs = if let Some(merged_split) = merged_split {
        let blob = upload_blob(store, db_path, &merged_split.bytes).await?;
        let compacted_split = TextSplitRef {
            blob: TextBlobRef {
                sha256: blob.sha256,
                size_bytes: blob.size_bytes,
            },
            footer_offset: merged_split.footer_offset,
            footer_len: merged_split.footer_len,
            hotcache_len: merged_split.hotcache_len,
            total_size_bytes: merged_split.total_size_bytes,
        };
        let mut split_refs = Vec::with_capacity(1 + remaining_split_refs.len());
        split_refs.push(compacted_split);
        split_refs.extend(remaining_split_refs);
        split_refs
    } else {
        remaining_split_refs
    };

    if next_split_refs.is_empty() {
        tx.delete(&manifest_key)?;
    } else {
        let mut persisted_manifest = manifest.clone();
        persisted_manifest.generation_id = Uuid::new_v4().to_string();
        persisted_manifest.split = next_split_refs
            .first()
            .cloned()
            .expect("compacted text manifest must retain at least one split");
        persisted_manifest.splits = next_split_refs;

        let encoded_manifest = serde_json::to_vec(&persisted_manifest).map_err(|err| {
            HelixDbError::Config(format!("failed to encode compacted text manifest: {err}"))
        })?;
        tx.put(&manifest_key, Bytes::from(encoded_manifest))?;
    }
    tx.commit().await?;
    Ok(true)
}

fn select_split_prefix_for_compaction(
    manifest: &TextIndexGenerationManifest,
    max_input_splits: usize,
    max_input_bytes: u64,
) -> Option<usize> {
    let mut selected = 0usize;
    let mut total_bytes = 0u64;

    for split_ref in manifest.split_refs() {
        if selected >= max_input_splits {
            break;
        }
        if selected > 0 && total_bytes.saturating_add(split_ref.total_size_bytes) > max_input_bytes
        {
            break;
        }
        selected += 1;
        total_bytes = total_bytes.saturating_add(split_ref.total_size_bytes);
    }

    (selected > 1).then_some(selected)
}

async fn load_live_versions(
    db: &Db,
    scope: DataScope,
    index_name: &str,
) -> Result<HashMap<u64, u64>, HelixDbError> {
    let prefix = make_text_index_live_state_prefix_scoped(scope, index_name);
    let prefix_len = prefix.len();
    let tx = db.begin(IsolationLevel::Snapshot).await?;
    let mut iter = tx.scan_prefix(prefix, ..).await?;
    let mut live_versions = HashMap::new();
    while let Some(kv) = iter.next().await? {
        let live_state = decode_live_state_bytes(&kv.value)?;
        if !live_state.live {
            continue;
        }
        let entity_suffix = std::str::from_utf8(&kv.key[prefix_len..]).map_err(|err| {
            HelixDbError::Config(format!(
                "failed to decode text live-state entity id for '{index_name}': {err}"
            ))
        })?;
        let entity_id = entity_suffix.parse::<u64>().map_err(|err| {
            HelixDbError::Config(format!(
                "failed to parse text live-state entity id '{entity_suffix}' for '{index_name}': {err}"
            ))
        })?;
        live_versions.insert(entity_id, live_state.logical_version);
    }
    tx.rollback();
    Ok(live_versions)
}

fn merge_local_splits_into_manifest(
    split_paths: Vec<std::path::PathBuf>,
    output_dir: &Path,
    manifest: &TextIndexGenerationManifest,
    live_versions: &HashMap<u64, u64>,
) -> Result<
    (
        Option<super::split::BuiltTextSplit>,
        Option<TextIndexGenerationManifest>,
    ),
    HelixDbError,
> {
    let mut input_directories: Vec<Box<dyn Directory>> = Vec::with_capacity(split_paths.len());
    for split_path in &split_paths {
        input_directories.push(Box::new(open_split_directory_from_file(split_path)?));
    }

    let synthetic_meta = build_synthetic_meta_json(&input_directories)?;
    let output_directory = MmapDirectory::open(output_dir).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to open text compaction output dir '{}': {err}",
            output_dir.display()
        ))
    })?;
    output_directory
        .atomic_write(Path::new(META_JSON_FILE), &synthetic_meta)
        .map_err(|err| {
            HelixDbError::Config(format!(
                "failed to write synthetic text compaction meta.json '{}': {err}",
                output_dir.display()
            ))
        })?;

    let mut overlay_stack: Vec<Box<dyn Directory>> = vec![Box::new(output_directory.clone())];
    overlay_stack.extend(input_directories);
    let overlay = OverlayDirectory::union_of(overlay_stack);

    let mut index = open_overlay_index(&overlay, manifest.analyzer)?;
    let fields = lookup_schema_fields(&index.schema())?;
    let stale_documents = collect_stale_documents(&index, fields, live_versions)?;
    if !stale_documents.is_empty() {
        apply_stale_document_deletes(&index, fields, &stale_documents)?;
        index = open_overlay_index(&overlay, manifest.analyzer)?;
    }

    let reader = super::build_reader(&index)?;
    if reader.searcher().num_docs() == 0 {
        return Ok((None, None));
    }

    let segment_ids = index
        .searchable_segment_metas()
        .map_err(|err| {
            HelixDbError::Config(format!("failed to inspect text compaction segments: {err}"))
        })?
        .into_iter()
        .map(|segment_meta| segment_meta.id())
        .collect::<Vec<_>>();

    let mut writer: tantivy::IndexWriter<TantivyDocument> = index
        .writer_with_num_threads(1, 15_000_000)
        .map_err(|err| {
            HelixDbError::Config(format!("failed to open text compaction writer: {err}"))
        })?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    writer.merge(&segment_ids).wait().map_err(|err| {
        HelixDbError::Config(format!("failed to merge text compaction segments: {err}"))
    })?;
    drop(writer);

    let merged_index = open_overlay_index(&overlay, manifest.analyzer)?;
    prune_unreferenced_output_files(output_dir, &merged_index)?;

    let built = build_split_bundle(output_dir)?;
    let new_manifest = TextIndexGenerationManifest::new_split(
        manifest.physical_index_name.clone(),
        Uuid::new_v4().to_string(),
        manifest.analyzer,
        manifest.positions_enabled,
        TextSplitRef {
            blob: TextBlobRef {
                sha256: [0u8; 32],
                size_bytes: built.total_size_bytes,
            },
            footer_offset: built.footer_offset,
            footer_len: built.footer_len,
            hotcache_len: built.hotcache_len,
            total_size_bytes: built.total_size_bytes,
        },
    );
    Ok((Some(built), Some(new_manifest)))
}

fn open_overlay_index(
    overlay: &OverlayDirectory,
    analyzer: crate::config::TextAnalyzerKind,
) -> Result<Index, HelixDbError> {
    let index = Index::open(overlay.clone()).map_err(|err| {
        HelixDbError::Config(format!("failed to open text compaction index: {err}"))
    })?;
    register_analyzers(&index, analyzer);
    Ok(index)
}

fn collect_stale_documents(
    index: &Index,
    fields: TextSchemaFields,
    live_versions: &HashMap<u64, u64>,
) -> Result<Vec<(u64, u64)>, HelixDbError> {
    let logical_version_field = fields.logical_version;
    let reader = super::build_reader(index)?;
    let searcher = reader.searcher();
    let mut stale_documents = Vec::new();

    for segment_reader in searcher.segment_readers() {
        let entity_ids = segment_reader
            .fast_fields()
            .u64(super::ENTITY_ID_FIELD_NAME)
            .map_err(|err| {
                HelixDbError::InvariantViolation(format!(
                    "text compaction fast field '{}' is unavailable: {err}",
                    super::ENTITY_ID_FIELD_NAME
                ))
            })?;
        let logical_versions = segment_reader
            .fast_fields()
            .u64(super::LOGICAL_VERSION_FIELD_NAME)
            .map_err(|err| {
                HelixDbError::InvariantViolation(format!(
                    "text compaction fast field '{}' is unavailable: {err}",
                    super::LOGICAL_VERSION_FIELD_NAME
                ))
            })?;
        let alive_bitset = segment_reader.alive_bitset();

        for doc_id in 0..segment_reader.max_doc() {
            if alive_bitset
                .as_ref()
                .map(|bitset| bitset.is_deleted(doc_id))
                .unwrap_or(false)
            {
                continue;
            }
            let entity_id = entity_ids.first(doc_id).ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "text compaction document is missing the entity_id field".into(),
                )
            })?;
            let logical_version = logical_versions.first(doc_id).ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "text compaction document is missing the logical_version field".into(),
                )
            })?;
            if live_versions.get(&entity_id).copied() != Some(logical_version) {
                stale_documents.push((entity_id, logical_version));
            }
        }
    }

    let _ = logical_version_field;
    Ok(stale_documents)
}

fn apply_stale_document_deletes(
    index: &Index,
    fields: TextSchemaFields,
    stale_documents: &[(u64, u64)],
) -> Result<(), HelixDbError> {
    if stale_documents.is_empty() {
        return Ok(());
    }
    let logical_version_field = fields.logical_version;
    let mut writer: tantivy::IndexWriter<TantivyDocument> = index
        .writer_with_num_threads(1, 15_000_000)
        .map_err(|err| {
            HelixDbError::Config(format!(
                "failed to open text compaction delete writer: {err}"
            ))
        })?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for (entity_id, logical_version) in stale_documents {
        let query = BooleanQuery::new(vec![
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_u64(fields.entity_id, *entity_id),
                    IndexRecordOption::Basic,
                )) as Box<dyn tantivy::query::Query>,
            ),
            (
                Occur::Must,
                Box::new(TermQuery::new(
                    Term::from_field_u64(logical_version_field, *logical_version),
                    IndexRecordOption::Basic,
                )) as Box<dyn tantivy::query::Query>,
            ),
        ]);
        writer.delete_query(Box::new(query)).map_err(|err| {
            HelixDbError::Config(format!("failed to queue stale text document delete: {err}"))
        })?;
    }
    writer.commit().map_err(|err| {
        HelixDbError::Config(format!(
            "failed to commit stale text document deletes: {err}"
        ))
    })?;
    Ok(())
}

fn prune_unreferenced_output_files(output_dir: &Path, index: &Index) -> Result<(), HelixDbError> {
    let mut referenced_files = index
        .searchable_segment_metas()
        .map_err(|err| {
            HelixDbError::Config(format!(
                "failed to inspect text compaction output segments: {err}"
            ))
        })?
        .into_iter()
        .flat_map(|segment_meta| segment_meta.list_files())
        .collect::<HashSet<_>>();
    referenced_files.insert(PathBuf::from(META_JSON_FILE));
    referenced_files.insert(PathBuf::from(MANAGED_JSON_FILE));

    for entry in fs::read_dir(output_dir).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to list text compaction output dir '{}': {err}",
            output_dir.display()
        ))
    })? {
        let entry = entry.map_err(|err| {
            HelixDbError::Config(format!(
                "failed to read text compaction output dir entry '{}': {err}",
                output_dir.display()
            ))
        })?;
        let file_type = entry.file_type().map_err(|err| {
            HelixDbError::Config(format!(
                "failed to read text compaction output file type '{}': {err}",
                entry.path().display()
            ))
        })?;
        if !file_type.is_file() {
            continue;
        }
        let relative_path = PathBuf::from(entry.file_name());
        if referenced_files.contains(&relative_path) {
            continue;
        }
        fs::remove_file(entry.path()).map_err(|err| {
            HelixDbError::Config(format!(
                "failed to remove stale text compaction output file '{}': {err}",
                entry.path().display()
            ))
        })?;
    }
    Ok(())
}

fn build_synthetic_meta_json(directories: &[Box<dyn Directory>]) -> Result<Vec<u8>, HelixDbError> {
    if directories.is_empty() {
        return Err(HelixDbError::Config(
            "cannot build synthetic meta.json for zero text splits".into(),
        ));
    }

    let mut synthetic_meta: Option<tantivy::index::IndexMeta> = None;
    let mut seen_segment_ids = std::collections::HashSet::new();
    for directory in directories {
        let index = Index::open(directory.box_clone()).map_err(|err| {
            HelixDbError::Config(format!(
                "failed to open split while building synthetic text compaction meta: {err}"
            ))
        })?;
        let meta = index.load_metas().map_err(|err| {
            HelixDbError::Config(format!(
                "failed to read split metadata while building synthetic text compaction meta: {err}"
            ))
        })?;
        if let Some(existing) = synthetic_meta.as_mut() {
            if existing.schema != meta.schema || existing.index_settings != meta.index_settings {
                return Err(HelixDbError::Config(
                    "cannot compact text splits with mismatched schema or index settings".into(),
                ));
            }
            for segment in meta.segments {
                let segment_id = segment.id();
                if !seen_segment_ids.insert(segment_id) {
                    return Err(HelixDbError::Config(format!(
                        "cannot compact text splits with duplicate segment id {segment_id}"
                    )));
                }
                existing.segments.push(segment);
            }
            existing.opstamp = existing.opstamp.max(meta.opstamp);
            if existing.payload != meta.payload {
                existing.payload = None;
            }
        } else {
            let meta = meta;
            seen_segment_ids.extend(meta.segments.iter().map(|segment| segment.id()));
            synthetic_meta = Some(meta);
        }
    }

    serde_json::to_vec(&synthetic_meta.ok_or_else(|| {
        HelixDbError::Config("failed to assemble synthetic text compaction meta.json".into())
    })?)
    .map_err(|err| {
        HelixDbError::Config(format!(
            "failed to encode synthetic text compaction meta.json: {err}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::config::TextIndexDefinition;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStoreExt;
    use tantivy::directory::RamDirectory;
    use tantivy::schema::{NumericOptions, Schema, TextOptions};

    fn write_split_bundle(
        definition: &TextIndexDefinition,
        documents: &[super::super::TextDocumentInput],
        split_path: &Path,
    ) {
        let index_dir = tempfile::tempdir().expect("index tempdir");
        let (index, fields) = super::super::create_disk_index(index_dir.path(), definition)
            .expect("create disk index");
        super::super::populate_index(&index, fields, documents).expect("populate index");
        let built = build_split_bundle(index_dir.path()).expect("build split bundle");
        fs::write(split_path, built.bytes).expect("write split bundle");
    }

    fn dummy_manifest(definition: &TextIndexDefinition) -> TextIndexGenerationManifest {
        TextIndexGenerationManifest::new_split(
            "fts:n:test:body",
            "gen-test",
            definition.analyzer(),
            definition.positions_enabled(),
            TextSplitRef {
                blob: TextBlobRef {
                    sha256: [0u8; 32],
                    size_bytes: 0,
                },
                footer_offset: 0,
                footer_len: 0,
                hotcache_len: 0,
                total_size_bytes: 0,
            },
        )
    }

    fn combined_manifest(
        left: TextIndexGenerationManifest,
        right: TextIndexGenerationManifest,
    ) -> TextIndexGenerationManifest {
        let mut combined = left;
        combined.splits.push(right.primary_split_ref().clone());
        combined.split = combined.splits[0].clone();
        combined
    }

    fn index_with_fast_field_contract(
        entity_fast: bool,
        logical_version_fast: bool,
        include_entity: bool,
        include_logical_version: bool,
    ) -> (Index, TextSchemaFields) {
        let mut schema_builder = Schema::builder();
        let mut entity_options = NumericOptions::default().set_indexed();
        if entity_fast {
            entity_options = entity_options.set_fast();
        }
        let mut logical_version_options = NumericOptions::default().set_indexed();
        if logical_version_fast {
            logical_version_options = logical_version_options.set_fast();
        }
        let entity_id =
            schema_builder.add_u64_field(super::super::ENTITY_ID_FIELD_NAME, entity_options);
        let logical_version = schema_builder.add_u64_field(
            super::super::LOGICAL_VERSION_FIELD_NAME,
            logical_version_options,
        );
        let body = schema_builder.add_text_field("body", TextOptions::default());
        let index = Index::create_in_ram(schema_builder.build());
        let mut document = TantivyDocument::default();
        if include_entity {
            document.add_u64(entity_id, 1);
        }
        if include_logical_version {
            document.add_u64(logical_version, 1);
        }
        let mut writer = index.writer(15_000_000).unwrap();
        writer.add_document(document).unwrap();
        writer.commit().unwrap();
        drop(writer);
        (
            index,
            TextSchemaFields {
                entity_id,
                logical_version,
                body,
            },
        )
    }

    #[tokio::test]
    async fn hidden_build_preparation_prunes_from_supplied_applied_versions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let old = super::super::persist_documents_as_split(
            &store,
            "db",
            &definition,
            &[
                super::super::TextDocumentInput::new(1, "alice old").with_logical_version(1),
                super::super::TextDocumentInput::new(2, "deleted").with_logical_version(1),
            ],
        )
        .await
        .unwrap()
        .unwrap();
        let current = super::super::persist_documents_as_split(
            &store,
            "db",
            &definition,
            &[super::super::TextDocumentInput::new(1, "alice current").with_logical_version(2)],
        )
        .await
        .unwrap()
        .unwrap();
        let limits = TextBackfillCompactionLimits::new(
            NonZeroUsize::new(4).unwrap(),
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            NonZeroU64::new(128 * 1024 * 1024).unwrap(),
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            NonZeroU64::new(1024 * 1024).unwrap(),
        );
        let prepared = prepare_text_build_compaction(
            &store,
            "db",
            &definition,
            "fts:n:Doc:body",
            &[old.clone(), current.clone()],
            limits,
        )
        .await
        .unwrap();
        assert_eq!(prepared.document_versions(), &[(1, 1), (1, 2), (2, 1)]);
        assert!(prepared.input_bytes().get() > 0);
        let unpublished = prepared
            .finish(HashMap::from([(1, 2)]))
            .await
            .unwrap()
            .unwrap();
        let (payload, output) = unpublished.into_parts();
        assert!(store
            .head(&super::super::blob_object_store_path(
                "db",
                output.blob.sha256,
            ))
            .await
            .is_err());
        assert_eq!(
            upload_blob(&store, "db", &payload).await.unwrap(),
            output.blob
        );
        let inspect = tempfile::tempdir().unwrap();
        let split_path = inspect.path().join("output.split");
        materialize_split_ref_to_file(&store, "db", &output, &split_path)
            .await
            .unwrap();
        let output_dir = inspect.path().join("inspection");
        fs::create_dir_all(&output_dir).unwrap();
        assert_eq!(
            inspect_local_split_document_versions(
                &[split_path],
                &output_dir,
                definition.analyzer(),
            )
            .unwrap(),
            vec![(1, 2)]
        );

        let stale = prepare_text_build_compaction(
            &store,
            "db",
            &definition,
            "fts:n:Doc:body",
            &[old.clone(), current.clone()],
            limits,
        )
        .await
        .unwrap();
        assert!(stale.finish(HashMap::new()).await.unwrap().is_none());

        let tiny_output_limits = TextBackfillCompactionLimits::new(
            limits.max_fan_in(),
            limits.max_input_bytes(),
            limits.max_temporary_disk_bytes(),
            NonZeroU64::MIN,
            limits.max_manifest_bytes(),
        );
        let oversized = prepare_text_build_compaction(
            &store,
            "db",
            &definition,
            "fts:n:Doc:body",
            &[old, current],
            tiny_output_limits,
        )
        .await
        .unwrap();
        assert!(matches!(
            oversized.finish(HashMap::from([(1, 2)])).await,
            Err(TextBuildCompactionError::OutputBlobExceeded { .. })
        ));
    }

    #[tokio::test]
    async fn compact_manifest_persists_live_merge_and_deletes_stale_only_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("text-compaction", Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let index_name = "fts:n:Doc:body";

        let mut over_budget = dummy_manifest(&definition);
        over_budget.splits = vec![
            TextSplitRef {
                total_size_bytes: TEXT_COMPACTION_MAX_INPUT_BYTES,
                ..over_budget.split.clone()
            },
            TextSplitRef {
                total_size_bytes: 1,
                ..over_budget.split.clone()
            },
        ];
        over_budget.split = over_budget.splits[0].clone();
        assert!(!compact_manifest_merge_only_scoped(
            &db,
            DataScope::LegacyUnscoped,
            &store,
            "db",
            &over_budget,
        )
        .await
        .unwrap());

        let old = super::super::persist_documents_as_manifest(
            &store,
            "db",
            &definition,
            index_name,
            &[super::super::TextDocumentInput::new(1, "alice old").with_logical_version(1)],
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!compact_manifest_merge_only_scoped(
            &db,
            DataScope::LegacyUnscoped,
            &store,
            "db",
            &old,
        )
        .await
        .unwrap());
        let current = super::super::persist_documents_as_manifest(
            &store,
            "db",
            &definition,
            index_name,
            &[super::super::TextDocumentInput::new(1, "bob live").with_logical_version(2)],
        )
        .await
        .unwrap()
        .unwrap();
        let manifest = combined_manifest(old, current);

        let live_state =
            super::super::encode_live_state_bytes(&super::super::TextIndexLiveState::live(2))
                .unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            crate::search::make_text_index_live_state_key_scoped(
                DataScope::LegacyUnscoped,
                index_name,
                1,
            ),
            Bytes::from(live_state),
        )
        .unwrap();
        tx.commit().await.unwrap();

        assert!(compact_manifest_merge_only_scoped(
            &db,
            DataScope::LegacyUnscoped,
            &store,
            "db",
            &manifest,
        )
        .await
        .unwrap());
        let manifest_key =
            make_text_index_manifest_key_scoped(DataScope::LegacyUnscoped, index_name);
        let persisted = db.get(&manifest_key).await.unwrap().unwrap();
        let persisted = super::super::decode_manifest_bytes(&persisted).unwrap();
        assert_eq!(persisted.split_refs().len(), 1);
        assert_eq!(
            super::super::search_manifest(&store, "db", &persisted, "bob", 10)
                .await
                .unwrap()
                .into_iter()
                .map(|hit| hit.entity_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert!(
            super::super::search_manifest(&store, "db", &persisted, "alice", 10)
                .await
                .unwrap()
                .is_empty()
        );

        let dead_index_name = "fts:n:Doc:dead";
        let dead_left = super::super::persist_documents_as_manifest(
            &store,
            "db",
            &definition,
            dead_index_name,
            &[super::super::TextDocumentInput::new(2, "left dead")],
        )
        .await
        .unwrap()
        .unwrap();
        let dead_right = super::super::persist_documents_as_manifest(
            &store,
            "db",
            &definition,
            dead_index_name,
            &[super::super::TextDocumentInput::new(3, "right dead")],
        )
        .await
        .unwrap()
        .unwrap();
        let dead_manifest = combined_manifest(dead_left, dead_right);
        let dead_manifest_key =
            make_text_index_manifest_key_scoped(DataScope::LegacyUnscoped, dead_index_name);
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            &dead_manifest_key,
            Bytes::from(serde_json::to_vec(&dead_manifest).unwrap()),
        )
        .unwrap();
        tx.commit().await.unwrap();

        assert!(compact_manifest_merge_only_scoped(
            &db,
            DataScope::LegacyUnscoped,
            &store,
            "db",
            &dead_manifest,
        )
        .await
        .unwrap());
        assert!(db.get(&dead_manifest_key).await.unwrap().is_none());
    }

    #[test]
    fn split_selection_honors_count_and_byte_limits() {
        let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let mut manifest = dummy_manifest(&definition);
        manifest.splits = (0..4)
            .map(|idx| TextSplitRef {
                blob: TextBlobRef {
                    sha256: [idx; 32],
                    size_bytes: 10,
                },
                footer_offset: 0,
                footer_len: 0,
                hotcache_len: 0,
                total_size_bytes: 10,
            })
            .collect();
        manifest.split = manifest.splits[0].clone();

        assert_eq!(
            select_split_prefix_for_compaction(&manifest, 3, 100),
            Some(3)
        );
        assert_eq!(
            select_split_prefix_for_compaction(&manifest, 8, 25),
            Some(2)
        );
        assert_eq!(select_split_prefix_for_compaction(&manifest, 1, 100), None);
        assert_eq!(select_split_prefix_for_compaction(&manifest, 8, 19), None);
        assert_eq!(select_split_prefix_for_compaction(&manifest, 0, 100), None);
    }

    #[tokio::test]
    async fn live_version_loading_filters_dead_rows_and_rejects_bad_suffixes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("text-live-version-loading", store)
            .build()
            .await
            .unwrap();
        let index_name = "fts:n:Doc:body";
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            crate::search::make_text_index_live_state_key_scoped(
                DataScope::LegacyUnscoped,
                index_name,
                1,
            ),
            Bytes::from(
                super::super::encode_live_state_bytes(&super::super::TextIndexLiveState::live(7))
                    .unwrap(),
            ),
        )
        .unwrap();
        tx.put(
            crate::search::make_text_index_live_state_key_scoped(
                DataScope::LegacyUnscoped,
                index_name,
                2,
            ),
            Bytes::from(
                super::super::encode_live_state_bytes(&super::super::TextIndexLiveState::dead(8))
                    .unwrap(),
            ),
        )
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            load_live_versions(&db, DataScope::LegacyUnscoped, index_name)
                .await
                .unwrap(),
            HashMap::from([(1, 7)])
        );

        let mut bad_key =
            make_text_index_live_state_prefix_scoped(DataScope::LegacyUnscoped, index_name)
                .to_vec();
        bad_key.extend_from_slice(b"not-a-number");
        let bad_key = Bytes::from(bad_key);
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            &bad_key,
            Bytes::from(
                super::super::encode_live_state_bytes(&super::super::TextIndexLiveState::live(1))
                    .unwrap(),
            ),
        )
        .unwrap();
        tx.commit().await.unwrap();
        assert!(
            load_live_versions(&db, DataScope::LegacyUnscoped, index_name)
                .await
                .unwrap_err()
                .to_string()
                .contains("failed to parse text live-state entity id")
        );

        let mut invalid_utf8 =
            make_text_index_live_state_prefix_scoped(DataScope::LegacyUnscoped, index_name)
                .to_vec();
        invalid_utf8.push(0xff);
        let invalid_utf8 = Bytes::from(invalid_utf8);
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.delete(&bad_key).unwrap();
        tx.put(
            &invalid_utf8,
            Bytes::from(
                super::super::encode_live_state_bytes(&super::super::TextIndexLiveState::live(1))
                    .unwrap(),
            ),
        )
        .unwrap();
        tx.commit().await.unwrap();
        assert!(
            load_live_versions(&db, DataScope::LegacyUnscoped, index_name)
                .await
                .unwrap_err()
                .to_string()
                .contains("failed to decode text live-state entity id")
        );
    }

    #[test]
    fn stale_document_helpers_accept_empty_deletes_and_reject_invalid_overlays() {
        let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let (index, fields) = super::super::create_ram_index(&definition).unwrap();

        assert!(collect_stale_documents(&index, fields, &HashMap::new())
            .unwrap()
            .is_empty());
        assert!(apply_stale_document_deletes(&index, fields, &[]).is_ok());

        let invalid_overlay = OverlayDirectory::union_of(vec![Box::new(RamDirectory::default())]);
        assert!(open_overlay_index(&invalid_overlay, definition.analyzer())
            .unwrap_err()
            .to_string()
            .contains("failed to open text compaction index"));
    }

    #[test]
    fn stale_document_collection_reports_fast_field_and_missing_value_invariants() {
        let (index, fields) = index_with_fast_field_contract(false, true, true, true);
        assert!(collect_stale_documents(&index, fields, &HashMap::new())
            .unwrap_err()
            .to_string()
            .contains("fast field 'entity_id' is unavailable"));

        let (index, fields) = index_with_fast_field_contract(true, false, true, true);
        assert!(collect_stale_documents(&index, fields, &HashMap::new())
            .unwrap_err()
            .to_string()
            .contains("fast field 'logical_version' is unavailable"));

        let (index, fields) = index_with_fast_field_contract(true, true, false, true);
        assert!(collect_stale_documents(&index, fields, &HashMap::new())
            .unwrap_err()
            .to_string()
            .contains("missing the entity_id field"));

        let (index, fields) = index_with_fast_field_contract(true, true, true, false);
        assert!(collect_stale_documents(&index, fields, &HashMap::new())
            .unwrap_err()
            .to_string()
            .contains("missing the logical_version field"));
    }

    #[test]
    fn synthetic_meta_rejects_empty_invalid_duplicate_and_mismatched_inputs() {
        assert!(build_synthetic_meta_json(&[])
            .unwrap_err()
            .to_string()
            .contains("zero text splits"));
        let invalid: Vec<Box<dyn Directory>> = vec![Box::new(RamDirectory::default())];
        assert!(build_synthetic_meta_json(&invalid)
            .unwrap_err()
            .to_string()
            .contains("failed to open split"));

        let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let tempdir = tempfile::tempdir().unwrap();
        let split_path = tempdir.path().join("split.split");
        write_split_bundle(
            &definition,
            &[super::super::TextDocumentInput::new(1, "duplicate")],
            &split_path,
        );
        let duplicate: Vec<Box<dyn Directory>> = vec![
            Box::new(open_split_directory_from_file(&split_path).unwrap()),
            Box::new(open_split_directory_from_file(&split_path).unwrap()),
        ];
        assert!(build_synthetic_meta_json(&duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate segment id"));

        let positioned = definition.clone().with_positions_enabled(true);
        let positioned_path = tempdir.path().join("positioned.split");
        write_split_bundle(
            &positioned,
            &[super::super::TextDocumentInput::new(2, "positioned")],
            &positioned_path,
        );
        let mismatched: Vec<Box<dyn Directory>> = vec![
            Box::new(open_split_directory_from_file(&split_path).unwrap()),
            Box::new(open_split_directory_from_file(&positioned_path).unwrap()),
        ];
        assert!(build_synthetic_meta_json(&mismatched)
            .unwrap_err()
            .to_string()
            .contains("mismatched schema or index settings"));
    }

    #[test]
    fn prune_output_files_removes_unreferenced_files_and_keeps_directories() {
        let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let output = tempfile::tempdir().unwrap();
        let (index, fields) = super::super::create_disk_index(output.path(), &definition).unwrap();
        super::super::populate_index(
            &index,
            fields,
            &[super::super::TextDocumentInput::new(1, "retained")],
        )
        .unwrap();
        let stale_file = output.path().join("stale.tmp");
        fs::write(&stale_file, b"stale").unwrap();
        let nested = output.path().join("nested");
        fs::create_dir(&nested).unwrap();

        prune_unreferenced_output_files(output.path(), &index).unwrap();
        assert!(!stale_file.exists());
        assert!(nested.exists());
        assert!(output.path().join(META_JSON_FILE).exists());
        assert!(
            prune_unreferenced_output_files(&output.path().join("missing"), &index)
                .unwrap_err()
                .to_string()
                .contains("failed to list text compaction output dir")
        );
    }

    #[test]
    fn merge_local_splits_prunes_stale_and_deleted_docs() {
        let definition =
            TextIndexDefinition::new_node("Doc", "body").expect("test text definition is valid");
        let tempdir = tempfile::tempdir().expect("tempdir");
        let split_one_path = tempdir.path().join("split-1.split");
        let split_two_path = tempdir.path().join("split-2.split");
        let output_dir = tempdir.path().join("output");
        fs::create_dir_all(&output_dir).expect("create output dir");

        write_split_bundle(
            &definition,
            &[
                super::super::TextDocumentInput::new(1, "alice old").with_logical_version(1),
                super::super::TextDocumentInput::new(2, "vector dead").with_logical_version(1),
            ],
            &split_one_path,
        );
        write_split_bundle(
            &definition,
            &[super::super::TextDocumentInput::new(1, "bob live").with_logical_version(2)],
            &split_two_path,
        );

        let live_versions = HashMap::from([(1u64, 2u64)]);
        let (built, manifest) = merge_local_splits_into_manifest(
            vec![split_one_path, split_two_path],
            &output_dir,
            &dummy_manifest(&definition),
            &live_versions,
        )
        .expect("merge local splits");
        let built = built.expect("replacement split should exist");
        let manifest = manifest.expect("replacement manifest should exist");
        let merged_split_path = tempdir.path().join("merged.split");
        fs::write(&merged_split_path, built.bytes).expect("write merged split");

        let index = Index::open(
            open_split_directory_from_file(&merged_split_path).expect("open merged split"),
        )
        .expect("open merged index");
        register_analyzers(&index, manifest.analyzer);
        let fields = lookup_schema_fields(&index.schema()).expect("resolve merged schema");
        let reader = super::super::build_reader(&index).expect("build merged reader");
        assert_eq!(reader.searcher().num_docs(), 1);

        let live_hits =
            super::super::search_reader(&reader, fields, definition.analyzer(), "bob", 10)
                .expect("search live doc");
        assert_eq!(live_hits.len(), 1);
        assert_eq!(live_hits[0].entity_id, 1);

        let stale_hits =
            super::super::search_reader(&reader, fields, definition.analyzer(), "alice", 10)
                .expect("search stale doc");
        assert!(stale_hits.is_empty());

        let deleted_hits =
            super::super::search_reader(&reader, fields, definition.analyzer(), "vector", 10)
                .expect("search deleted doc");
        assert!(deleted_hits.is_empty());
    }

    #[test]
    fn merge_local_splits_returns_none_when_all_docs_are_stale() {
        let definition =
            TextIndexDefinition::new_node("Doc", "body").expect("test text definition is valid");
        let tempdir = tempfile::tempdir().expect("tempdir");
        let split_one_path = tempdir.path().join("split-1.split");
        let split_two_path = tempdir.path().join("split-2.split");
        let output_dir = tempdir.path().join("output");
        fs::create_dir_all(&output_dir).expect("create output dir");

        write_split_bundle(
            &definition,
            &[super::super::TextDocumentInput::new(1, "alice old").with_logical_version(1)],
            &split_one_path,
        );
        write_split_bundle(
            &definition,
            &[super::super::TextDocumentInput::new(1, "bob old").with_logical_version(2)],
            &split_two_path,
        );

        let (built, manifest) = merge_local_splits_into_manifest(
            vec![split_one_path, split_two_path],
            &output_dir,
            &dummy_manifest(&definition),
            &HashMap::new(),
        )
        .expect("merge local stale-only splits");

        assert!(built.is_none());
        assert!(manifest.is_none());
    }
}
