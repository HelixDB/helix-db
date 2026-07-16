//! Tantivy-backed text split construction, loading, search, and cache contracts.
//!
//! V2 lifecycle code publishes immutable split blobs through the coordinated
//! upload boundary and borrows the process-local GC gate exposed here while it
//! atomically attaches or relocates durable references.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{future::try_join_all, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::object_store::{
    path::Path as ObjectStorePath, ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload,
};
use tantivy::collector::TopDocs;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::query::{BooleanQuery, Occur, TermQuery};
use tantivy::schema::{IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{
    Language, LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer, WhitespaceTokenizer,
};
use tantivy::{Index, IndexReader, ReloadPolicy, TantivyDocument, Term};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::config::{TextAnalyzerKind, TextIndexDefinition};
use crate::encoding::keys::tenant::DataScope;
use crate::encoding::property::decode_properties;
use crate::encoding::property::property::Property;
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::{keys, EdgeId, NodeId};
use crate::error::HelixDbError;
use crate::search::{
    get_edge_properties_by_id, make_text_index_live_state_key_scoped,
    make_text_index_manifest_key_scoped, make_text_index_manifest_prefix_scoped,
    make_text_index_manifest_scan_prefix_scoped, text_index_name, text_tenant_index_name,
};
use slatedb::DbReadOps;
#[cfg(test)]
use slatedb::{Db, IsolationLevel};

const BODY_FIELD_NAME: &str = "body";
const ENTITY_ID_FIELD_NAME: &str = "entity_id";
const LOGICAL_VERSION_FIELD_NAME: &str = "logical_version";
const META_JSON_FILE: &str = "meta.json";
pub(crate) const TEXT_INDEX_MANIFEST_FORMAT_V2: u32 = 2;
type TextWarmupFuture = Pin<Box<dyn Future<Output = Result<(), HelixDbError>> + Send>>;

mod blob_gc_gate;
mod bundle_storage;
mod byte_range_cache;
mod caching_directory;
pub(crate) mod compaction;
mod debounced_storage;
mod debug_proxy_directory;
mod hot_directory;
mod overlay_directory;
mod split;
mod storage_directory;
mod storage_with_cache;
mod warmup;

pub(crate) use blob_gc_gate::{BlobDeletionPermit, BlobGcGate, BlobPublicationPermit};
use bundle_storage::ObjectStoreSplitBundleStorage;
use byte_range_cache::ByteRangeCache;
use caching_directory::CachingDirectory;
pub(crate) use compaction::compact_manifest_merge_only_scoped;
use debounced_storage::DebouncedStorage;
use hot_directory::HotDirectory;
pub(crate) use split::{
    build_split_bundle, decode_footer_cache_entry_bytes, split_reference_layout_is_exact,
};
use storage_directory::StorageDirectory;
use storage_with_cache::StorageWithCache;
use warmup::{FastFieldWarmupInfo, WarmupInfo};

fn create_tempdir(context: &str) -> Result<TempDir, HelixDbError> {
    let base = std::env::temp_dir();
    fs::create_dir_all(&base).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to ensure tempdir base '{}' for {context}: {err}",
            base.display()
        ))
    })?;

    tempfile::Builder::new()
        .prefix("helix-text-")
        .tempdir_in(&base)
        .map_err(|err| {
            HelixDbError::Config(format!(
                "failed to create {context} tempdir in '{}': {err}",
                base.display()
            ))
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextBlobRef {
    pub sha256: [u8; 32],
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextSplitRef {
    pub blob: TextBlobRef,
    pub footer_offset: u64,
    pub footer_len: u32,
    pub hotcache_len: u32,
    pub total_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextIndexGenerationManifest {
    pub format_version: u32,
    pub physical_index_name: String,
    pub generation_id: String,
    pub split: TextSplitRef,
    pub splits: Vec<TextSplitRef>,
    pub analyzer: TextAnalyzerKind,
    pub positions_enabled: bool,
}

impl TextIndexGenerationManifest {
    pub fn new_split(
        physical_index_name: impl Into<String>,
        generation_id: impl Into<String>,
        analyzer: TextAnalyzerKind,
        positions_enabled: bool,
        split: TextSplitRef,
    ) -> Self {
        Self {
            format_version: TEXT_INDEX_MANIFEST_FORMAT_V2,
            physical_index_name: physical_index_name.into(),
            generation_id: generation_id.into(),
            split: split.clone(),
            splits: vec![split],
            analyzer,
            positions_enabled,
        }
    }

    pub fn referenced_blob_count(&self) -> usize {
        self.split_refs().len()
    }

    pub fn split_refs(&self) -> &[TextSplitRef] {
        self.splits.as_slice()
    }

    pub fn primary_split_ref(&self) -> &TextSplitRef {
        self.split_refs()
            .first()
            .expect("text manifest must contain at least one split")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TextSearchHit {
    pub entity_id: u64,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TextSearchCandidate {
    pub(crate) entity_id: u64,
    pub(crate) logical_version: u64,
    pub(crate) score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextIndexLiveState {
    pub logical_version: u64,
    pub live: bool,
}

impl TextIndexLiveState {
    pub fn live(logical_version: u64) -> Self {
        Self {
            logical_version,
            live: true,
        }
    }

    pub fn dead(logical_version: u64) -> Self {
        Self {
            logical_version,
            live: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextDocumentInput {
    pub entity_id: u64,
    pub logical_version: u64,
    pub text: String,
}

/// Complete immutable split materialized before any object-store publication.
///
/// The V2 lifecycle uses this value to derive and validate the exact blob
/// identity before reserving a publication permit or writing an upload intent.
/// Keeping the payload and reference together prevents a caller from pairing
/// metadata from one Tantivy bundle with bytes from another. The paired unit
/// tests below exercise construction, empty input, and content hashing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnpublishedTextSplit {
    payload: Bytes,
    split_ref: TextSplitRef,
}

impl UnpublishedTextSplit {
    /// Wraps one locally built split with its deterministic content identity.
    ///
    /// Both ordinary split construction and lifecycle compaction use this
    /// boundary so neither path can persist metadata for bytes it did not hash.
    pub(crate) fn from_built_split(built_split: split::BuiltTextSplit) -> Self {
        let sha256 = Sha256::digest(&built_split.bytes).into();
        let size_bytes = u64::try_from(built_split.bytes.len())
            .expect("usize text split lengths fit the durable u64 size on supported targets");
        let split_ref = TextSplitRef {
            blob: TextBlobRef { sha256, size_bytes },
            footer_offset: built_split.footer_offset,
            footer_len: built_split.footer_len,
            hotcache_len: built_split.hotcache_len,
            total_size_bytes: built_split.total_size_bytes,
        };
        Self {
            payload: Bytes::from(built_split.bytes),
            split_ref,
        }
    }

    /// Transfers the inseparable publication payload and reference to a V2 caller.
    pub(crate) fn into_parts(self) -> (Bytes, TextSplitRef) {
        (self.payload, self.split_ref)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct TextSchemaFields {
    entity_id: tantivy::schema::Field,
    logical_version: tantivy::schema::Field,
    body: tantivy::schema::Field,
}

impl TextDocumentInput {
    pub fn new(entity_id: u64, text: impl Into<String>) -> Self {
        Self {
            entity_id,
            logical_version: 1,
            text: text.into(),
        }
    }

    pub fn with_logical_version(mut self, logical_version: u64) -> Self {
        self.logical_version = logical_version;
        self
    }
}

pub async fn load_manifest(
    reader: &(impl DbReadOps + Send + Sync),
    index_name: &str,
) -> Result<Option<TextIndexGenerationManifest>, HelixDbError> {
    load_manifest_scoped(reader, DataScope::LegacyUnscoped, index_name).await
}

pub async fn load_manifest_scoped(
    reader: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    index_name: &str,
) -> Result<Option<TextIndexGenerationManifest>, HelixDbError> {
    let Some(bytes) = reader
        .get(make_text_index_manifest_key_scoped(scope, index_name))
        .await?
    else {
        return Ok(None);
    };
    decode_manifest_bytes(&bytes).map(Some)
}

pub async fn load_manifests_for_definition(
    reader: &(impl DbReadOps + Send + Sync),
    definition: &TextIndexDefinition,
) -> Result<Vec<(Bytes, TextIndexGenerationManifest)>, HelixDbError> {
    load_manifests_for_definition_scoped(reader, DataScope::LegacyUnscoped, definition).await
}

pub async fn load_manifests_for_definition_scoped(
    reader: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    definition: &TextIndexDefinition,
) -> Result<Vec<(Bytes, TextIndexGenerationManifest)>, HelixDbError> {
    let prefix = make_text_index_manifest_prefix_scoped(
        scope,
        definition.element_type(),
        definition.label(),
        definition.property(),
        definition.tenant_property().is_some(),
    );
    let mut iter = reader.scan_prefix(prefix, ..).await?;
    let mut manifests = Vec::new();
    while let Some(kv) = iter.next().await? {
        let manifest = decode_manifest_bytes(&kv.value)?;
        manifests.push((kv.key, manifest));
    }
    Ok(manifests)
}

pub async fn load_all_manifests(
    reader: &(impl DbReadOps + Send + Sync),
) -> Result<Vec<(Bytes, TextIndexGenerationManifest)>, HelixDbError> {
    load_all_manifests_scoped(reader, DataScope::LegacyUnscoped).await
}

pub async fn load_all_manifests_scoped(
    reader: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<Vec<(Bytes, TextIndexGenerationManifest)>, HelixDbError> {
    let prefix = make_text_index_manifest_scan_prefix_scoped(scope);
    let mut iter = reader.scan_prefix(prefix, ..).await?;
    let mut manifests = Vec::new();
    while let Some(kv) = iter.next().await? {
        let manifest = decode_manifest_bytes(&kv.value)?;
        manifests.push((kv.key, manifest));
    }
    Ok(manifests)
}

fn decode_manifest_bytes(bytes: &[u8]) -> Result<TextIndexGenerationManifest, HelixDbError> {
    let manifest = serde_json::from_slice::<TextIndexGenerationManifest>(bytes)
        .map_err(|err| HelixDbError::Config(format!("failed to decode text manifest: {err}")))?;
    if manifest.format_version != TEXT_INDEX_MANIFEST_FORMAT_V2 {
        return Err(HelixDbError::Config(format!(
            "unsupported text manifest format version {}",
            manifest.format_version
        )));
    }
    if manifest.splits.is_empty() || manifest.splits.first() != Some(&manifest.split) {
        return Err(HelixDbError::Config(
            "text manifest must contain its primary split as the first split".into(),
        ));
    }
    Ok(manifest)
}

pub async fn search_manifest(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    manifest: &TextIndexGenerationManifest,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    if k == 0 {
        return Ok(Vec::new());
    }

    let (_index, fields, reader) = open_manifest_index(store, db_path, manifest).await?;
    warm_searcher(&reader, fields, manifest.analyzer, query).await?;
    search_split_index_bytes(
        &reader,
        fields,
        manifest.analyzer,
        manifest.positions_enabled,
        query,
        k,
    )
}

pub async fn search_manifest_with_live_state(
    reader: &(impl DbReadOps + Send + Sync),
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    manifest: &TextIndexGenerationManifest,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    search_manifest_with_live_state_scoped(
        reader,
        DataScope::LegacyUnscoped,
        store,
        db_path,
        manifest,
        query,
        k,
    )
    .await
}

pub async fn search_manifest_with_live_state_scoped(
    reader: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    manifest: &TextIndexGenerationManifest,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    search_manifest_with_state_source(
        reader,
        store,
        db_path,
        manifest,
        TextLiveStateSource::Legacy {
            scope,
            index_name: &manifest.physical_index_name,
        },
        query,
        k,
    )
    .await
}

/// Searches one bounded V2 manifest page using generation-qualified live state.
///
/// The caller holds the matching reader lease across this entire future, which
/// includes split footer/cache/blob I/O and every candidate state point read.
pub(crate) async fn search_manifest_with_v2_live_state_scoped(
    reader: &(impl DbReadOps + Send + Sync),
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    root: &crate::index_v2::text::serving::ValidatedActiveTextManifestRoot,
    manifest: &TextIndexGenerationManifest,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    search_manifest_with_state_source(
        reader,
        store,
        db_path,
        manifest,
        TextLiveStateSource::V2(root),
        query,
        k,
    )
    .await
}

#[derive(Clone, Copy)]
/// Selects the persisted live-state contract for a split search.
enum TextLiveStateSource<'a> {
    /// Compatibility search against the pre-V2 physical manifest contract.
    Legacy {
        scope: DataScope,
        index_name: &'a str,
    },
    /// Generation-owned state under an exact Active V2 manifest root.
    V2(&'a crate::index_v2::text::serving::ValidatedActiveTextManifestRoot),
}

/// Runs the common split search while keeping live-state ownership explicit.
async fn search_manifest_with_state_source(
    reader: &(impl DbReadOps + Send + Sync),
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    manifest: &TextIndexGenerationManifest,
    state_source: TextLiveStateSource<'_>,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    if k == 0 {
        return Ok(Vec::new());
    }

    let mut live_states = BTreeMap::new();
    let mut hits_by_entity = BTreeMap::new();
    for split_ref in manifest.split_refs() {
        let index = open_remote_split_index(store, db_path, split_ref).await?;
        register_analyzers(&index, manifest.analyzer);
        let fields = lookup_schema_fields(&index.schema())?;
        let index_reader = build_reader(&index)?;
        warm_searcher(&index_reader, fields, manifest.analyzer, query).await?;

        let total_docs = index_reader.searcher().num_docs() as usize;
        if total_docs == 0 {
            continue;
        }

        let mut candidate_limit = k.min(total_docs).max(1);
        let mut split_hits = Vec::new();
        loop {
            split_hits.clear();
            let candidates = search_reader_candidates(
                &index_reader,
                fields,
                manifest.analyzer,
                query,
                candidate_limit,
            )?;
            let candidate_count = candidates.len();
            for candidate in candidates {
                if text_candidate_is_live(reader, state_source, &candidate, &mut live_states)
                    .await?
                {
                    split_hits.push(TextSearchHit {
                        entity_id: candidate.entity_id,
                        score: candidate.score,
                    });
                }
            }

            if split_hits.len() >= k
                || candidate_limit == total_docs
                || candidate_count < candidate_limit
            {
                break;
            }
            candidate_limit = candidate_limit.saturating_mul(2).min(total_docs);
        }

        for hit in split_hits {
            hits_by_entity
                .entry(hit.entity_id)
                .and_modify(|existing: &mut TextSearchHit| {
                    if hit.score > existing.score {
                        existing.score = hit.score;
                    }
                })
                .or_insert(hit);
        }
    }

    let mut hits = hits_by_entity.into_values().collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.entity_id.cmp(&right.entity_id))
    });
    hits.truncate(k);
    Ok(hits)
}

async fn text_candidate_is_live(
    reader: &(impl DbReadOps + Send + Sync),
    state_source: TextLiveStateSource<'_>,
    candidate: &TextSearchCandidate,
    live_states: &mut BTreeMap<u64, Option<TextIndexLiveState>>,
) -> Result<bool, HelixDbError> {
    if let Some(state) = live_states.get(&candidate.entity_id) {
        return Ok(text_candidate_matches_live_state(candidate, state.as_ref()));
    }

    let state = match state_source {
        TextLiveStateSource::Legacy { scope, index_name } => match reader
            .get(make_text_index_live_state_key_scoped(
                scope,
                index_name,
                candidate.entity_id,
            ))
            .await?
        {
            Some(bytes) => Some(decode_live_state_bytes(&bytes)?),
            None => None,
        },
        TextLiveStateSource::V2(root) => {
            let state = crate::index_v2::text::serving::load_active_entity_state(
                reader,
                root,
                candidate.entity_id,
            )
            .await?;
            Some(TextIndexLiveState {
                logical_version: state.logical_version(),
                live: state.is_live(),
            })
        }
    };
    let is_live = text_candidate_matches_live_state(candidate, state.as_ref());
    live_states.insert(candidate.entity_id, state);
    Ok(is_live)
}

fn text_candidate_matches_live_state(
    candidate: &TextSearchCandidate,
    state: Option<&TextIndexLiveState>,
) -> bool {
    state.is_none_or(|state| state.live && state.logical_version == candidate.logical_version)
}

async fn open_manifest_index(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    manifest: &TextIndexGenerationManifest,
) -> Result<(Index, TextSchemaFields, IndexReader), HelixDbError> {
    let split_refs = manifest.split_refs();
    if split_refs.len() != 1 {
        return Err(HelixDbError::Config(format!(
            "multi-split text manifest search is not available yet for '{}'",
            manifest.physical_index_name
        )));
    }

    let split_ref = manifest.primary_split_ref();
    let index = open_remote_split_index(store, db_path, split_ref).await?;
    register_analyzers(&index, manifest.analyzer);
    let fields = lookup_schema_fields(&index.schema())?;
    let reader = build_reader(&index)?;
    Ok((index, fields, reader))
}

async fn open_remote_split_index(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    split_ref: &TextSplitRef,
) -> Result<Index, HelixDbError> {
    let blob_path = blob_object_store_path(db_path, split_ref.blob.sha256);
    let footer_cache_entry = Arc::new(decode_footer_cache_entry_bytes(
        &store
            .get_range(
                &blob_path,
                split_ref.footer_offset..split_ref.total_size_bytes,
            )
            .await?,
        split_ref,
    )?);
    let byte_range_cache = ByteRangeCache::new();
    let bundle_storage: Arc<dyn bundle_storage::SplitStorage> = Arc::new(DebouncedStorage::new(
        Arc::new(ObjectStoreSplitBundleStorage::new(
            Arc::clone(store),
            blob_path,
            footer_cache_entry.footer.clone(),
        )),
    ));
    let directory = StorageDirectory::new(Arc::new(StorageWithCache::new(
        bundle_storage,
        ByteRangeCache::new(),
    )));
    open_split_index_with_hotcache(
        directory,
        footer_cache_entry.hotcache_bytes.as_ref(),
        byte_range_cache,
    )
}

fn open_split_index_with_hotcache(
    directory: impl tantivy::Directory + 'static,
    hotcache_bytes: &[u8],
    byte_range_cache: ByteRangeCache,
) -> Result<Index, HelixDbError> {
    let caching_directory: Arc<dyn tantivy::Directory> =
        Arc::new(CachingDirectory::new(Arc::new(directory), byte_range_cache));
    let hot_directory = HotDirectory::open(caching_directory, hotcache_bytes)?;
    Index::open(hot_directory).map_err(|err| {
        HelixDbError::Config(format!("failed to open split-backed Tantivy index: {err}"))
    })
}

pub fn search_documents(
    definition: &TextIndexDefinition,
    documents: &[TextDocumentInput],
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    if k == 0 || documents.is_empty() {
        return Ok(Vec::new());
    }

    let (index, fields) = create_ram_index(definition)?;
    populate_index(&index, fields, documents)?;
    search_index(&index, fields, definition.analyzer(), query, k)
}

pub async fn persist_documents_as_manifest(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    definition: &TextIndexDefinition,
    index_name: &str,
    documents: &[TextDocumentInput],
) -> Result<Option<TextIndexGenerationManifest>, HelixDbError> {
    let Some(split) = persist_documents_as_split(store, db_path, definition, documents).await?
    else {
        return Ok(None);
    };
    let generation_id = Uuid::new_v4().to_string();

    Ok(Some(TextIndexGenerationManifest::new_split(
        index_name,
        generation_id,
        definition.analyzer(),
        definition.positions_enabled(),
        split,
    )))
}

/// Builds one unchanged current-format immutable text split without object I/O.
///
/// This is the construction half of the V2 publication contract. Callers must
/// reserve and durably persist the exact upload intent derived from
/// [`UnpublishedTextSplit::split_ref`] before submitting
/// [`UnpublishedTextSplit::into_parts`] to the publication coordinator.
pub(crate) fn build_documents_as_split(
    definition: &TextIndexDefinition,
    documents: &[TextDocumentInput],
) -> Result<Option<UnpublishedTextSplit>, HelixDbError> {
    if documents.is_empty() {
        return Ok(None);
    }

    let tempdir = create_tempdir("text-index")?;
    let (index, fields) = create_disk_index(tempdir.path(), definition)?;
    populate_index(&index, fields, documents)?;

    Ok(Some(UnpublishedTextSplit::from_built_split(
        build_split_bundle(tempdir.path())?,
    )))
}

/// Configured-static convenience wrapper that directly uploads one text split.
///
/// V2 lifecycle code must use [`build_documents_as_split`] and route the
/// returned payload through `BlobPublicationCoordinator`; this wrapper remains
/// isolated to configured indexes that do not participate in dynamic DDL.
pub(crate) async fn persist_documents_as_split(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    definition: &TextIndexDefinition,
    documents: &[TextDocumentInput],
) -> Result<Option<TextSplitRef>, HelixDbError> {
    let Some(unpublished) = build_documents_as_split(definition, documents)? else {
        return Ok(None);
    };
    let (payload, split_ref) = unpublished.into_parts();
    let uploaded = upload_blob(store, db_path, &payload).await?;
    assert_eq!(
        uploaded, split_ref.blob,
        "the shared content-addressed constructor must return the precomputed split identity"
    );
    Ok(Some(split_ref))
}

pub async fn collect_node_documents_from_reader(
    reader: &(impl DbReadOps + Send + Sync),
    definition: &TextIndexDefinition,
    tenant_value: Option<&PropertyValue>,
) -> Result<Vec<TextDocumentInput>, HelixDbError> {
    let prefix = keys::KeyPrefix::NodeProperty.as_slice();
    let mut iter = reader.scan_prefix(prefix, ..).await?;
    let mut docs = Vec::new();

    while let Some(kv) = iter.next().await? {
        if kv.key.len() < 9 {
            continue;
        }
        let key = keys::DataKeyKind::parse_from_slice(&kv.key)?;
        if let keys::DataKeyKind::NodeProperty(node_property_key) = key {
            let properties = decode_properties(&kv.value)?;
            if let Some(document) = make_node_document(
                node_property_key.node_id(),
                &properties,
                definition,
                tenant_value,
            )? {
                docs.push(document);
            }
        } else {
            continue;
        }
    }

    Ok(docs)
}

pub async fn collect_edge_documents_from_reader(
    reader: &(impl DbReadOps + Send + Sync),
    definition: &TextIndexDefinition,
    tenant_value: Option<&PropertyValue>,
) -> Result<Vec<TextDocumentInput>, HelixDbError> {
    let prefix = keys::KeyPrefix::EdgeEndpoints.as_slice();
    let mut iter = reader.scan_prefix(prefix, ..).await?;
    let mut docs = Vec::new();

    while let Some(kv) = iter.next().await? {
        if kv.key.len() < 9 {
            continue;
        }
        let edge_id = EdgeId::from_be_bytes(kv.key[1..9].try_into().unwrap());
        let properties = get_edge_properties_by_id(reader, edge_id).await?;
        if let Some(document) = make_edge_document(edge_id, &properties, definition, tenant_value)?
        {
            docs.push(document);
        }
    }

    Ok(docs)
}

pub fn make_node_document(
    node_id: NodeId,
    properties: &[Property],
    definition: &TextIndexDefinition,
    tenant_value: Option<&PropertyValue>,
) -> Result<Option<TextDocumentInput>, HelixDbError> {
    if !label_matches_definition(properties, definition) {
        return Ok(None);
    }

    let Some(value) = property_value(properties, definition.property()) else {
        return Ok(None);
    };
    let Some(text) = normalize_indexed_text_value(value)? else {
        return Ok(None);
    };

    if !tenant_matches_definition(properties, definition, tenant_value)? {
        return Ok(None);
    }

    Ok(Some(TextDocumentInput::new(node_id, text)))
}

pub fn make_edge_document(
    edge_id: EdgeId,
    properties: &[Property],
    definition: &TextIndexDefinition,
    tenant_value: Option<&PropertyValue>,
) -> Result<Option<TextDocumentInput>, HelixDbError> {
    if !label_matches_definition(properties, definition) {
        return Ok(None);
    }

    let Some(value) = property_value(properties, definition.property()) else {
        return Ok(None);
    };
    let Some(text) = normalize_indexed_text_value(value)? else {
        return Ok(None);
    };

    if !tenant_matches_definition(properties, definition, tenant_value)? {
        return Ok(None);
    }

    Ok(Some(TextDocumentInput::new(edge_id, text)))
}

pub fn validate_node_properties_for_definition(
    definition: &TextIndexDefinition,
    properties: &[Property],
) -> Result<(), HelixDbError> {
    if !label_matches_definition(properties, definition) {
        return Ok(());
    }

    let Some(value) = property_value(properties, definition.property()) else {
        return Ok(());
    };
    let _ = normalize_indexed_text_value(value)?.ok_or_else(|| {
        HelixDbError::Query(format!(
            "text index {}:{} only supports string values",
            definition.label(),
            definition.property()
        ))
    })?;

    if let Some(tenant_property) = definition.tenant_property()
        && property_value(properties, tenant_property)
            .and_then(normalize_tenant_value)
            .is_none()
    {
        return Err(HelixDbError::Query(format!(
            "text index {}:{} requires tenant property '{}' when '{}' is present",
            definition.label(),
            definition.property(),
            tenant_property,
            definition.property()
        )));
    }

    Ok(())
}

pub fn validate_edge_properties_for_definition(
    definition: &TextIndexDefinition,
    properties: &[Property],
) -> Result<(), HelixDbError> {
    if !label_matches_definition(properties, definition) {
        return Ok(());
    }

    let Some(value) = property_value(properties, definition.property()) else {
        return Ok(());
    };
    let _ = normalize_indexed_text_value(value)?.ok_or_else(|| {
        HelixDbError::Query(format!(
            "text index {}:{} only supports string values",
            definition.label(),
            definition.property()
        ))
    })?;

    if let Some(tenant_property) = definition.tenant_property()
        && property_value(properties, tenant_property)
            .and_then(normalize_tenant_value)
            .is_none()
    {
        return Err(HelixDbError::Query(format!(
            "text index {}:{} requires tenant property '{}' when '{}' is present",
            definition.label(),
            definition.property(),
            tenant_property,
            definition.property()
        )));
    }

    Ok(())
}

pub fn normalize_indexed_text_value(value: &PropertyValue) -> Result<Option<String>, HelixDbError> {
    match value {
        PropertyValue::String(text) => Ok(Some(text.clone())),
        PropertyValue::StringArray(items) => Ok(Some(items.join("\n"))),
        PropertyValue::Null => Err(HelixDbError::Query(
            "text indexes do not support null property values".into(),
        )),
        PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::I64Array(_)
        | PropertyValue::F64Array(_)
        | PropertyValue::F32Array(_)
        | PropertyValue::Array(_)
        | PropertyValue::Object(_) => Err(HelixDbError::Query(
            "text indexes only support String and StringArray values".into(),
        )),
    }
}

pub fn resolve_physical_index_name(
    definition: &TextIndexDefinition,
    tenant_value: Option<&PropertyValue>,
) -> Result<String, HelixDbError> {
    match (
        definition.tenant_property(),
        tenant_value.and_then(normalize_tenant_value),
    ) {
        (Some(tenant_property), Some(tenant_value)) => Ok(text_tenant_index_name(
            definition.element_type(),
            definition.label(),
            definition.property(),
            tenant_property,
            tenant_value,
        )),
        (Some(tenant_property), None) => Err(HelixDbError::Query(format!(
            "text search for {}:{} requires tenant value for partition property '{}'",
            definition.label(),
            definition.property(),
            tenant_property
        ))),
        (None, Some(_)) => Err(HelixDbError::Query(format!(
            "text search for {}:{} does not support tenant values",
            definition.label(),
            definition.property()
        ))),
        (None, None) => Ok(text_index_name(
            definition.element_type(),
            definition.label(),
            definition.property(),
        )),
    }
}

pub fn normalize_tenant_value(value: &PropertyValue) -> Option<&PropertyValue> {
    match value {
        PropertyValue::Null => None,
        PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::I64Array(_)
        | PropertyValue::F64Array(_)
        | PropertyValue::F32Array(_)
        | PropertyValue::StringArray(_)
        | PropertyValue::Array(_)
        | PropertyValue::Object(_) => Some(value),
    }
}

pub fn manifest_blob_hashes(manifests: &[TextIndexGenerationManifest]) -> BTreeSet<[u8; 32]> {
    manifests
        .iter()
        .flat_map(|manifest| manifest.split_refs().iter().map(|split| split.blob.sha256))
        .collect()
}

pub fn encode_live_state_bytes(state: &TextIndexLiveState) -> Result<Vec<u8>, HelixDbError> {
    serde_json::to_vec(state)
        .map_err(|err| HelixDbError::Config(format!("failed to encode text live-state: {err}")))
}

pub fn decode_live_state_bytes(bytes: &[u8]) -> Result<TextIndexLiveState, HelixDbError> {
    serde_json::from_slice(bytes)
        .map_err(|err| HelixDbError::Config(format!("failed to decode text live-state row: {err}")))
}

pub(crate) async fn materialize_split_ref_to_file(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    split_ref: &TextSplitRef,
    split_path: &Path,
) -> Result<(), HelixDbError> {
    let blob_path = blob_object_store_path(db_path, split_ref.blob.sha256);
    let mut stream = store.get(&blob_path).await?.into_stream();
    let mut file = tokio::fs::File::create(split_path).await.map_err(|err| {
        HelixDbError::Config(format!(
            "failed to create local text split file '{}': {err}",
            split_path.display()
        ))
    })?;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await.map_err(|err| {
            HelixDbError::Config(format!(
                "failed to stream local text split file '{}': {err}",
                split_path.display()
            ))
        })?;
    }
    file.flush().await.map_err(|err| {
        HelixDbError::Config(format!(
            "failed to flush local text split file '{}': {err}",
            split_path.display()
        ))
    })?;
    Ok(())
}

pub async fn copy_blob_set(
    store: &Arc<dyn ObjectStore>,
    source_db_path: &str,
    target_db_path: &str,
    blob_hashes: &BTreeSet<[u8; 32]>,
) -> Result<(), HelixDbError> {
    for sha256 in blob_hashes {
        let source_path = blob_object_store_path(source_db_path, *sha256);
        let target_path = blob_object_store_path(target_db_path, *sha256);
        if source_path == target_path {
            continue;
        }
        let payload = store.get(&source_path).await?.bytes().await?;
        store
            .put(&target_path, PutPayload::from_bytes(payload))
            .await?;
    }
    Ok(())
}

pub async fn list_blob_hashes(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
) -> Result<Vec<(ObjectStorePath, ObjectMeta)>, HelixDbError> {
    let prefix = blob_prefix_object_store_path(db_path);
    let mut stream = store.list(Some(&prefix));
    let mut blobs = Vec::new();
    while let Some(meta) = stream.next().await.transpose()? {
        blobs.push((meta.location.clone(), meta));
    }
    Ok(blobs)
}

pub async fn delete_unreferenced_blobs(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    referenced_hashes: &BTreeSet<[u8; 32]>,
    grace_period: Duration,
) -> Result<usize, HelixDbError> {
    let now = chrono::Utc::now();
    let mut deleted = 0;
    for (location, meta) in list_blob_hashes(store, db_path).await? {
        let Some(sha256) = parse_blob_hash_from_location(&location) else {
            continue;
        };
        if referenced_hashes.contains(&sha256) {
            continue;
        }
        if let Ok(age) = now.signed_duration_since(meta.last_modified).to_std()
            && age < grace_period
        {
            continue;
        }
        store.delete(&location).await?;
        deleted += 1;
    }
    Ok(deleted)
}

fn create_ram_index(
    definition: &TextIndexDefinition,
) -> Result<(Index, TextSchemaFields), HelixDbError> {
    let (schema, fields) = build_schema(definition.analyzer(), definition.positions_enabled());
    let index = Index::create_in_ram(schema);
    register_analyzers(&index, definition.analyzer());
    Ok((index, fields))
}

fn create_disk_index(
    dir: &Path,
    definition: &TextIndexDefinition,
) -> Result<(Index, TextSchemaFields), HelixDbError> {
    let (schema, fields) = build_schema(definition.analyzer(), definition.positions_enabled());
    let index = Index::create_in_dir(dir, schema).map_err(|err| {
        HelixDbError::Config(format!("failed to create Tantivy index directory: {err}"))
    })?;
    register_analyzers(&index, definition.analyzer());
    Ok((index, fields))
}

fn build_schema(analyzer: TextAnalyzerKind, positions_enabled: bool) -> (Schema, TextSchemaFields) {
    let mut schema_builder = Schema::builder();
    let entity_id = schema_builder.add_u64_field(
        ENTITY_ID_FIELD_NAME,
        NumericOptions::default().set_indexed().set_fast(),
    );
    let logical_version = schema_builder.add_u64_field(
        LOGICAL_VERSION_FIELD_NAME,
        NumericOptions::default().set_indexed().set_fast(),
    );
    let body_indexing = TextFieldIndexing::default()
        .set_tokenizer(analyzer.as_str())
        .set_index_option(if positions_enabled {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::WithFreqs
        });
    let body = schema_builder.add_text_field(
        BODY_FIELD_NAME,
        TextOptions::default().set_indexing_options(body_indexing),
    );
    let schema = schema_builder.build();
    (
        schema,
        TextSchemaFields {
            entity_id,
            logical_version,
            body,
        },
    )
}

fn register_analyzers(index: &Index, default_analyzer: TextAnalyzerKind) {
    let tokenizers = index.tokenizers();
    tokenizers.register(
        TextAnalyzerKind::Standard.as_str(),
        build_text_analyzer(TextAnalyzerKind::Standard),
    );
    tokenizers.register(
        TextAnalyzerKind::StandardStemEn.as_str(),
        build_text_analyzer(TextAnalyzerKind::StandardStemEn),
    );
    tokenizers.register(
        TextAnalyzerKind::WhitespaceLowercase.as_str(),
        build_text_analyzer(TextAnalyzerKind::WhitespaceLowercase),
    );
    tokenizers.register(
        default_analyzer.as_str(),
        build_text_analyzer(default_analyzer),
    );
}

fn build_text_analyzer(kind: TextAnalyzerKind) -> TextAnalyzer {
    match kind {
        TextAnalyzerKind::Standard => TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .build(),
        TextAnalyzerKind::StandardStemEn => TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(Stemmer::new(Language::English))
            .build(),
        TextAnalyzerKind::WhitespaceLowercase => {
            TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(LowerCaser)
                .build()
        }
    }
}

fn populate_index(
    index: &Index,
    fields: TextSchemaFields,
    documents: &[TextDocumentInput],
) -> Result<(), HelixDbError> {
    let mut writer = index
        .writer(15_000_000)
        .map_err(|err| HelixDbError::Config(format!("failed to create Tantivy writer: {err}")))?;
    writer.set_merge_policy(Box::new(NoMergePolicy));
    for document in documents {
        let mut tantivy_document = TantivyDocument::default();
        tantivy_document.add_u64(fields.entity_id, document.entity_id);
        tantivy_document.add_u64(fields.logical_version, document.logical_version);
        tantivy_document.add_text(fields.body, document.text.as_str());
        writer.add_document(tantivy_document).map_err(|err| {
            HelixDbError::Config(format!("failed to add Tantivy document: {err}"))
        })?;
    }
    writer
        .commit()
        .map_err(|err| HelixDbError::Config(format!("failed to commit Tantivy writer: {err}")))?;
    drop(writer);
    Ok(())
}

pub(crate) async fn warm_searcher(
    reader: &IndexReader,
    fields: TextSchemaFields,
    analyzer: TextAnalyzerKind,
    query: &str,
) -> Result<(), HelixDbError> {
    let mut warmup_info = query_warmup_info(fields, analyzer, query);
    if warmup_info.terms_grouped_by_field.is_empty()
        && warmup_info.fast_fields.is_empty()
        && !warmup_info.field_norms
        && warmup_info.term_dict_fields.is_empty()
    {
        return Ok(());
    }

    warmup_info.merge(collector_warmup_info());
    warmup_info.simplify();
    execute_warmup(reader, warmup_info).await
}

fn query_warmup_info(
    fields: TextSchemaFields,
    analyzer: TextAnalyzerKind,
    query: &str,
) -> WarmupInfo {
    let terms = analyze_query_terms(analyzer, query);
    let mut warmup = WarmupInfo::default();
    if !terms.is_empty() {
        warmup.terms_grouped_by_field.insert(
            fields.body,
            terms
                .into_iter()
                .map(|term| (Term::from_field_text(fields.body, &term), false))
                .collect(),
        );
    }
    warmup
        .fast_fields
        .insert(FastFieldWarmupInfo::new(ENTITY_ID_FIELD_NAME, false));
    warmup
        .fast_fields
        .insert(FastFieldWarmupInfo::new(LOGICAL_VERSION_FIELD_NAME, false));
    warmup
}

fn collector_warmup_info() -> WarmupInfo {
    WarmupInfo {
        field_norms: true,
        ..WarmupInfo::default()
    }
}

async fn execute_warmup(reader: &IndexReader, warmup_info: WarmupInfo) -> Result<(), HelixDbError> {
    let searcher = reader.searcher();
    let (_, _, _, _, _) = tokio::try_join!(
        warm_up_term_dict_fields(&searcher, &warmup_info),
        warm_up_postings_full(&searcher, &warmup_info),
        warm_up_terms(&searcher, &warmup_info),
        warm_up_fastfields(&searcher, &warmup_info),
        warm_up_fieldnorms(&searcher, &warmup_info),
    )?;
    Ok(())
}

async fn warm_up_term_dict_fields(
    searcher: &tantivy::Searcher,
    warmup_info: &WarmupInfo,
) -> Result<(), HelixDbError> {
    let mut warmup_futures: Vec<TextWarmupFuture> = Vec::new();
    for segment_reader in searcher.segment_readers() {
        for field in &warmup_info.term_dict_fields {
            let inverted_index = segment_reader.inverted_index(*field).map_err(|err| {
                HelixDbError::Config(format!(
                    "failed to open inverted index for term dictionary warmup: {err}"
                ))
            })?;
            let inverted_index = inverted_index.clone();
            warmup_futures.push(Box::pin(async move {
                inverted_index
                    .terms()
                    .warm_up_dictionary()
                    .await
                    .map_err(|err| {
                        HelixDbError::Config(format!("failed to warm text term dictionary: {err}"))
                    })
            }));
        }
    }
    try_join_all(warmup_futures).await.map(|_| ())
}

async fn warm_up_postings_full(
    searcher: &tantivy::Searcher,
    warmup_info: &WarmupInfo,
) -> Result<(), HelixDbError> {
    let mut warmup_futures: Vec<TextWarmupFuture> = Vec::new();
    for segment_reader in searcher.segment_readers() {
        for field in &warmup_info.term_dict_fields {
            let inverted_index = segment_reader.inverted_index(*field).map_err(|err| {
                HelixDbError::Config(format!(
                    "failed to open inverted index for postings-full warmup: {err}"
                ))
            })?;
            let inverted_index = inverted_index.clone();
            warmup_futures.push(Box::pin(async move {
                inverted_index
                    .warm_postings_full(false)
                    .await
                    .map_err(|err| {
                        HelixDbError::Config(format!("failed to warm full text postings: {err}"))
                    })
            }));
        }
    }
    try_join_all(warmup_futures).await.map(|_| ())
}

async fn warm_up_terms(
    searcher: &tantivy::Searcher,
    warmup_info: &WarmupInfo,
) -> Result<(), HelixDbError> {
    let mut warmup_futures: Vec<TextWarmupFuture> = Vec::new();
    for segment_reader in searcher.segment_readers() {
        for (field, terms) in &warmup_info.terms_grouped_by_field {
            let inverted_index = segment_reader.inverted_index(*field).map_err(|err| {
                HelixDbError::Config(format!(
                    "failed to open inverted index for term warmup: {err}"
                ))
            })?;
            for (term, include_positions) in terms {
                let inverted_index = inverted_index.clone();
                let term = term.clone();
                let include_positions = *include_positions;
                warmup_futures.push(Box::pin(async move {
                    inverted_index
                        .warm_postings(&term, include_positions)
                        .await
                        .map(|_| ())
                        .map_err(|err| {
                            HelixDbError::Config(format!("failed to warm text postings: {err}"))
                        })
                }));
            }
        }
    }
    try_join_all(warmup_futures).await.map(|_| ())
}

async fn warm_up_fastfields(
    searcher: &tantivy::Searcher,
    warmup_info: &WarmupInfo,
) -> Result<(), HelixDbError> {
    let mut warmup_futures: Vec<TextWarmupFuture> = Vec::new();
    for segment_reader in searcher.segment_readers() {
        for fast_field in &warmup_info.fast_fields {
            let fast_fields = segment_reader.fast_fields().clone();
            let field_name = fast_field.name.clone();
            warmup_futures.push(Box::pin(async move {
                let handles = fast_fields
                    .list_dynamic_column_handles(&field_name)
                    .await
                    .map_err(|err| {
                        HelixDbError::Config(format!(
                            "failed to list text fast field warmup handles: {err}"
                        ))
                    })?;
                try_join_all(handles.into_iter().map(|handle| async move {
                    handle.file_slice().read_bytes_async().await.map(|_| ())
                }))
                .await
                .map(|_| ())
                .map_err(|err| {
                    HelixDbError::Config(format!(
                        "failed to warm text fast field '{}': {err}",
                        field_name
                    ))
                })
            }));
        }
    }
    try_join_all(warmup_futures).await.map(|_| ())
}

async fn warm_up_fieldnorms(
    searcher: &tantivy::Searcher,
    warmup_info: &WarmupInfo,
) -> Result<(), HelixDbError> {
    if !warmup_info.field_norms {
        return Ok(());
    }

    let mut warmup_futures: Vec<TextWarmupFuture> = Vec::new();
    for segment_reader in searcher.segment_readers() {
        let schema = segment_reader.schema();
        let fieldnorm_file = segment_reader.fieldnorms_readers().get_inner_file();
        for (field, field_entry) in schema.fields() {
            if !field_entry.is_indexed() {
                continue;
            }
            if let Some(fieldnorm_slice) = fieldnorm_file.open_read(field) {
                warmup_futures.push(Box::pin(async move {
                    fieldnorm_slice
                        .read_bytes_async()
                        .await
                        .map(|_| ())
                        .map_err(|err| {
                            HelixDbError::Config(format!("failed to warm text field norms: {err}"))
                        })
                }));
            }
        }
    }
    try_join_all(warmup_futures).await.map(|_| ())
}

fn search_split_index_bytes(
    reader: &IndexReader,
    fields: TextSchemaFields,
    analyzer: TextAnalyzerKind,
    positions_enabled: bool,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    let _ = positions_enabled;
    search_reader(reader, fields, analyzer, query, k)
}

fn search_index(
    index: &Index,
    fields: TextSchemaFields,
    analyzer: TextAnalyzerKind,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    let reader = build_reader(index)?;
    search_reader(&reader, fields, analyzer, query, k)
}

fn build_reader(index: &Index) -> Result<IndexReader, HelixDbError> {
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .map_err(|err| HelixDbError::Config(format!("failed to create Tantivy reader: {err}")))?;
    reader
        .reload()
        .map_err(|err| HelixDbError::Config(format!("failed to reload Tantivy reader: {err}")))?;
    Ok(reader)
}

fn search_reader(
    reader: &IndexReader,
    fields: TextSchemaFields,
    analyzer: TextAnalyzerKind,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchHit>, HelixDbError> {
    Ok(
        search_reader_candidates(reader, fields, analyzer, query, k)?
            .into_iter()
            .map(|candidate| TextSearchHit {
                entity_id: candidate.entity_id,
                score: candidate.score,
            })
            .collect(),
    )
}

pub(crate) fn search_reader_candidates(
    reader: &IndexReader,
    fields: TextSchemaFields,
    analyzer: TextAnalyzerKind,
    query: &str,
    k: usize,
) -> Result<Vec<TextSearchCandidate>, HelixDbError> {
    let terms = analyze_query_terms(analyzer, query);
    if terms.is_empty() || k == 0 {
        return Ok(Vec::new());
    }

    let searcher = reader.searcher();
    let entity_id_columns = searcher
        .segment_readers()
        .iter()
        .map(|segment_reader| {
            segment_reader
                .fast_fields()
                .u64(ENTITY_ID_FIELD_NAME)
                .map_err(|err| {
                    HelixDbError::InvariantViolation(format!(
                        "text index fast field '{ENTITY_ID_FIELD_NAME}' is unavailable: {err}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let logical_version_columns = searcher
        .segment_readers()
        .iter()
        .map(|segment_reader| {
            segment_reader
                .fast_fields()
                .u64(LOGICAL_VERSION_FIELD_NAME)
                .map_err(|err| {
                    HelixDbError::InvariantViolation(format!(
                        "text index fast field '{LOGICAL_VERSION_FIELD_NAME}' is unavailable: {err}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let clauses = terms
        .into_iter()
        .map(|term| {
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_text(fields.body, &term),
                    IndexRecordOption::WithFreqs,
                )) as Box<dyn tantivy::query::Query>,
            )
        })
        .collect::<Vec<_>>();
    let query = BooleanQuery::new(clauses);
    let docs = searcher
        .search(&query, &TopDocs::with_limit(k).order_by_score())
        .map_err(|err| HelixDbError::Config(format!("failed to execute Tantivy search: {err}")))?;

    let mut hits = Vec::with_capacity(docs.len());
    for (score, address) in docs {
        let entity_id = entity_id_columns
            .get(address.segment_ord as usize)
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(format!(
                    "text search hit references missing segment ordinal {}",
                    address.segment_ord
                ))
            })?
            .first(address.doc_id)
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "text index document is missing the entity_id field".into(),
                )
            })?;
        let logical_version = logical_version_columns
            .get(address.segment_ord as usize)
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(format!(
                    "text search hit references missing logical-version segment ordinal {}",
                    address.segment_ord
                ))
            })?
            .first(address.doc_id)
            .ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "text index document is missing the logical_version field".into(),
                )
            })?;
        hits.push(TextSearchCandidate {
            entity_id,
            logical_version,
            score,
        });
    }
    Ok(hits)
}

fn lookup_schema_fields(schema: &Schema) -> Result<TextSchemaFields, HelixDbError> {
    let entity_id = schema.get_field(ENTITY_ID_FIELD_NAME).map_err(|err| {
        HelixDbError::InvariantViolation(format!("text index schema missing entity_id: {err}"))
    })?;
    let logical_version = schema
        .get_field(LOGICAL_VERSION_FIELD_NAME)
        .map_err(|err| {
            HelixDbError::InvariantViolation(format!(
                "text index schema missing logical_version: {err}"
            ))
        })?;
    let body = schema.get_field(BODY_FIELD_NAME).map_err(|err| {
        HelixDbError::InvariantViolation(format!("text index schema missing body: {err}"))
    })?;
    Ok(TextSchemaFields {
        entity_id,
        logical_version,
        body,
    })
}

fn analyze_query_terms(analyzer: TextAnalyzerKind, query: &str) -> Vec<String> {
    let mut terms = BTreeSet::new();
    let mut text_analyzer = build_text_analyzer(analyzer);
    let mut stream = text_analyzer.token_stream(query);
    stream.process(&mut |token| {
        if !token.text.is_empty() {
            terms.insert(token.text.to_string());
        }
    });
    terms.into_iter().collect()
}

fn property_value<'a>(properties: &'a [Property], name: &str) -> Option<&'a PropertyValue> {
    properties
        .iter()
        .find(|property| property.name == name)
        .map(|property| &property.value)
}

fn label_matches_definition(properties: &[Property], definition: &TextIndexDefinition) -> bool {
    extract_label(properties)
        .map(|label| label == definition.label())
        .unwrap_or(false)
}

fn tenant_matches_definition(
    properties: &[Property],
    definition: &TextIndexDefinition,
    tenant_value: Option<&PropertyValue>,
) -> Result<bool, HelixDbError> {
    if let Some(expected_tenant) = tenant_value {
        let Some(tenant_property) = definition.tenant_property() else {
            return Err(HelixDbError::Query(format!(
                "text search for {}:{} does not support tenant values",
                definition.label(),
                definition.property()
            )));
        };
        let current_tenant = property_value(properties, tenant_property)
            .and_then(normalize_tenant_value)
            .ok_or_else(|| {
                HelixDbError::Query(format!(
                    "text index {}:{} requires tenant property '{}' when '{}' is present",
                    definition.label(),
                    definition.property(),
                    tenant_property,
                    definition.property()
                ))
            })?;
        return Ok(current_tenant == expected_tenant);
    }

    if let Some(tenant_property) = definition.tenant_property() {
        let _current_tenant = property_value(properties, tenant_property)
            .and_then(normalize_tenant_value)
            .ok_or_else(|| {
                HelixDbError::Query(format!(
                    "text index {}:{} requires tenant property '{}' when '{}' is present",
                    definition.label(),
                    definition.property(),
                    tenant_property,
                    definition.property()
                ))
            })?;
        return Ok(false);
    }

    Ok(true)
}

fn extract_label(properties: &[Property]) -> Option<&str> {
    property_value(properties, "$label").and_then(PropertyValue::as_str)
}

fn should_persist_file(file_name: &str) -> bool {
    file_name != META_JSON_FILE && !file_name.ends_with(".lock")
}

pub(crate) async fn upload_blob(
    store: &Arc<dyn ObjectStore>,
    db_path: &str,
    payload: &[u8],
) -> Result<TextBlobRef, HelixDbError> {
    let sha256 = Sha256::digest(payload);
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&sha256);
    let location = blob_object_store_path(db_path, hash);
    store
        .put(
            &location,
            PutPayload::from_bytes(Bytes::from(payload.to_vec())),
        )
        .await?;
    Ok(TextBlobRef {
        sha256: hash,
        size_bytes: payload.len() as u64,
    })
}

/// Returns the canonical content-addressed object path for one text blob.
///
/// Lifecycle GC uses the same constructor as upload and reads so deletion
/// cannot drift onto a differently normalized database prefix.
pub(crate) fn blob_object_store_path(db_path: &str, sha256: [u8; 32]) -> ObjectStorePath {
    let base = db_path.trim_matches('/');
    let hex = sha256_hex(sha256);
    if base.is_empty() {
        ObjectStorePath::from(format!("fts/blobs/{hex}"))
    } else {
        ObjectStorePath::from(format!("{base}/fts/blobs/{hex}"))
    }
}

fn blob_prefix_object_store_path(db_path: &str) -> ObjectStorePath {
    let base = db_path.trim_matches('/');
    if base.is_empty() {
        ObjectStorePath::from("fts/blobs")
    } else {
        ObjectStorePath::from(format!("{base}/fts/blobs"))
    }
}

fn parse_blob_hash_from_location(location: &ObjectStorePath) -> Option<[u8; 32]> {
    let file_name = location.filename()?;
    if file_name.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (idx, chunk) in file_name.as_bytes().chunks_exact(2).enumerate() {
        let hex = std::str::from_utf8(chunk).ok()?;
        bytes[idx] = u8::from_str_radix(hex, 16).ok()?;
    }
    Some(bytes)
}

fn sha256_hex(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TextAnalyzerKind, TextIndexDefinition};
    use crate::encoding::keys::tenant::TenantId;
    use crate::encoding::property::encode_properties;
    use slatedb::object_store::memory::InMemory;
    use std::sync::Arc;

    #[test]
    fn manifest_roundtrip_preserves_fields() {
        let manifest = TextIndexGenerationManifest::new_split(
            "fts:n:1:2",
            "gen-1",
            TextAnalyzerKind::Standard,
            false,
            TextSplitRef {
                blob: TextBlobRef {
                    sha256: [7u8; 32],
                    size_bytes: 42,
                },
                footer_offset: 10,
                footer_len: 12,
                hotcache_len: 0,
                total_size_bytes: 42,
            },
        );

        let encoded = serde_json::to_vec(&manifest).expect("encode manifest");
        let decoded = decode_manifest_bytes(&encoded).expect("decode manifest");
        assert_eq!(decoded, manifest);
        assert_eq!(decoded.format_version, TEXT_INDEX_MANIFEST_FORMAT_V2);
        assert_eq!(decoded.splits.len(), 1);
    }

    #[test]
    fn manifest_decoder_rejects_compatibility_and_inconsistent_shapes() {
        let manifest = TextIndexGenerationManifest::new_split(
            "fts:n:1:2",
            "gen-1",
            TextAnalyzerKind::Standard,
            false,
            TextSplitRef {
                blob: TextBlobRef {
                    sha256: [7u8; 32],
                    size_bytes: 42,
                },
                footer_offset: 10,
                footer_len: 12,
                hotcache_len: 0,
                total_size_bytes: 42,
            },
        );

        let mut unsupported = manifest.clone();
        unsupported.format_version = 1;
        assert!(
            decode_manifest_bytes(&serde_json::to_vec(&unsupported).unwrap())
                .unwrap_err()
                .to_string()
                .contains("unsupported text manifest format version")
        );

        let mut empty = manifest.clone();
        empty.splits.clear();
        assert!(decode_manifest_bytes(&serde_json::to_vec(&empty).unwrap()).is_err());

        let mut mismatched = manifest.clone();
        mismatched.splits[0].blob.sha256 = [8; 32];
        assert!(decode_manifest_bytes(&serde_json::to_vec(&mismatched).unwrap()).is_err());

        let mut missing_fields = serde_json::to_value(manifest).unwrap();
        let object = missing_fields.as_object_mut().unwrap();
        object.remove("format_version");
        object.remove("splits");
        assert!(decode_manifest_bytes(&serde_json::to_vec(&missing_fields).unwrap()).is_err());
    }

    #[test]
    fn manifest_blob_hashes_includes_split_artifacts() {
        let left = TextIndexGenerationManifest::new_split(
            "fts:n:1:2",
            "gen-1",
            TextAnalyzerKind::Standard,
            false,
            TextSplitRef {
                blob: TextBlobRef {
                    sha256: [7u8; 32],
                    size_bytes: 42,
                },
                footer_offset: 10,
                footer_len: 12,
                hotcache_len: 0,
                total_size_bytes: 42,
            },
        );
        let right = TextIndexGenerationManifest::new_split(
            "fts:n:1:2",
            "gen-2",
            TextAnalyzerKind::Standard,
            false,
            TextSplitRef {
                blob: TextBlobRef {
                    sha256: [9u8; 32],
                    size_bytes: 99,
                },
                footer_offset: 10,
                footer_len: 20,
                hotcache_len: 0,
                total_size_bytes: 99,
            },
        );

        let hashes = manifest_blob_hashes(&[left, right]);
        assert!(hashes.contains(&[7u8; 32]));
        assert!(hashes.contains(&[9u8; 32]));
    }

    #[test]
    fn live_state_roundtrip_preserves_version_and_liveness() {
        let state = TextIndexLiveState::live(7);
        let encoded = encode_live_state_bytes(&state).expect("encode live state");
        let decoded = decode_live_state_bytes(&encoded).expect("decode live state");
        assert_eq!(decoded, state);
    }

    #[test]
    fn normalize_indexed_text_value_supports_strings_and_arrays() {
        assert_eq!(
            normalize_indexed_text_value(&PropertyValue::String("hello".into())).unwrap(),
            Some("hello".into())
        );
        assert_eq!(
            normalize_indexed_text_value(&PropertyValue::StringArray(vec![
                "hello".into(),
                "world".into(),
            ]))
            .unwrap(),
            Some("hello\nworld".into())
        );
        assert!(normalize_indexed_text_value(&PropertyValue::I64(1)).is_err());
    }

    #[test]
    fn analyzer_selection_changes_query_matching() {
        let definition = TextIndexDefinition::new_node("Doc", "body")
            .expect("test text definition is valid")
            .with_analyzer(TextAnalyzerKind::StandardStemEn);
        let documents = vec![TextDocumentInput::new(1, "running runner".to_string())];
        let hits = search_documents(&definition, &documents, "run", 10).expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entity_id, 1);
    }

    #[test]
    fn standard_analyzer_matches_timestamp_style_suffix_tokens() {
        let definition =
            TextIndexDefinition::new_node("Doc", "body").expect("test text definition is valid");
        let documents = vec![TextDocumentInput::new(
            1,
            "helix staging fts postgamma20260417T154822".to_string(),
        )];

        let hits = search_documents(&definition, &documents, "postgamma20260417T154822", 10)
            .expect("search");

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].entity_id, 1);
    }

    #[test]
    fn unpublished_split_binds_payload_to_its_content_addressed_reference() {
        let definition =
            TextIndexDefinition::new_node("Doc", "body").expect("test text definition is valid");
        let documents = vec![TextDocumentInput::new(7, "bounded lifecycle split")];

        let unpublished = build_documents_as_split(&definition, &documents)
            .expect("build split")
            .expect("non-empty documents produce a split");
        let (payload, split) = unpublished.into_parts();
        let expected_hash: [u8; 32] = Sha256::digest(&payload).into();
        let expected_size = u64::try_from(payload.len()).unwrap();

        assert_eq!(split.blob.sha256, expected_hash);
        assert_eq!(split.blob.size_bytes, expected_size);
        assert_eq!(split.total_size_bytes, expected_size);
        assert!(build_documents_as_split(&definition, &[])
            .expect("empty split build")
            .is_none());
    }

    #[tokio::test]
    async fn configured_split_upload_preserves_the_precomputed_reference() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let definition =
            TextIndexDefinition::new_node("Doc", "body").expect("test text definition is valid");
        let documents = vec![TextDocumentInput::new(7, "bounded lifecycle split")];

        let split = persist_documents_as_split(&store, "db", &definition, &documents)
            .await
            .expect("upload split")
            .expect("non-empty documents produce a split");
        let metadata = store
            .head(&blob_object_store_path("db", split.blob.sha256))
            .await
            .expect("uploaded content-addressed blob exists");

        assert_eq!(metadata.size, split.blob.size_bytes);
        assert_eq!(split.total_size_bytes, split.blob.size_bytes);
        assert!(persist_documents_as_split(&store, "db", &definition, &[])
            .await
            .expect("empty split build")
            .is_none());
    }

    #[tokio::test]
    async fn reopened_persisted_split_preserves_all_document_tokens() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let definition =
            TextIndexDefinition::new_node("Doc", "body").expect("test text definition is valid");
        let documents = vec![
            TextDocumentInput::new(0, "helix staging fts prealpha20260417T170219"),
            TextDocumentInput::new(1, "helix staging fts prebeta20260417T170219"),
            TextDocumentInput::new(1000, "helix staging fts postgamma20260417T170219"),
        ];

        let manifest =
            persist_documents_as_manifest(&store, "db", &definition, "fts:n:Doc:body", &documents)
                .await
                .expect("persist manifest")
                .expect("manifest exists");

        for query in [
            "helix",
            "staging",
            "fts",
            "postgamma20260417T170219",
            "helix staging fts",
        ] {
            let hits = search_manifest(&store, "db", &manifest, query, 10)
                .await
                .unwrap_or_else(|err| panic!("search '{query}' failed: {err}"));
            let ids = hits
                .into_iter()
                .map(|hit| hit.entity_id)
                .collect::<Vec<_>>();
            assert_eq!(
                ids.len(),
                3.min(if query.starts_with("postgamma") { 1 } else { 3 })
            );
            if query.starts_with("postgamma") {
                assert_eq!(ids, vec![1000]);
            } else {
                assert_eq!(
                    ids,
                    vec![0, 1, 1000],
                    "query '{query}' should see every doc"
                );
            }
        }
    }

    #[tokio::test]
    async fn manifest_loaders_isolate_definitions_and_storage_scopes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("text-manifest-loaders", store)
            .build()
            .await
            .unwrap();
        let node_definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let edge_definition = TextIndexDefinition::new_edge("MENTIONS", "body").unwrap();
        let tenant_definition = TextIndexDefinition::new_node("Doc", "body")
            .unwrap()
            .with_tenant_property("firmId")
            .unwrap();
        let node_name = resolve_physical_index_name(&node_definition, None).unwrap();
        let edge_name = resolve_physical_index_name(&edge_definition, None).unwrap();
        let tenant_value = PropertyValue::String("tenant-a".into());
        let tenant_name =
            resolve_physical_index_name(&tenant_definition, Some(&tenant_value)).unwrap();
        let scope =
            DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000A").unwrap());
        let split = TextSplitRef {
            blob: TextBlobRef {
                sha256: [7; 32],
                size_bytes: 42,
            },
            footer_offset: 10,
            footer_len: 12,
            hotcache_len: 0,
            total_size_bytes: 42,
        };
        let node_manifest = TextIndexGenerationManifest::new_split(
            &node_name,
            "node-generation",
            TextAnalyzerKind::Standard,
            false,
            split.clone(),
        );
        let edge_manifest = TextIndexGenerationManifest::new_split(
            &edge_name,
            "edge-generation",
            TextAnalyzerKind::Standard,
            false,
            split.clone(),
        );
        let tenant_manifest = TextIndexGenerationManifest::new_split(
            &tenant_name,
            "tenant-generation",
            TextAnalyzerKind::Standard,
            false,
            split,
        );

        assert!(load_manifest(&db, "missing").await.unwrap().is_none());
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            make_text_index_manifest_key_scoped(DataScope::LegacyUnscoped, &node_name),
            Bytes::from(serde_json::to_vec(&node_manifest).unwrap()),
        )
        .unwrap();
        tx.put(
            make_text_index_manifest_key_scoped(DataScope::LegacyUnscoped, &edge_name),
            Bytes::from(serde_json::to_vec(&edge_manifest).unwrap()),
        )
        .unwrap();
        tx.put(
            make_text_index_manifest_key_scoped(scope, &tenant_name),
            Bytes::from(serde_json::to_vec(&tenant_manifest).unwrap()),
        )
        .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(
            load_manifest(&db, &node_name).await.unwrap(),
            Some(node_manifest)
        );
        assert_eq!(
            load_manifest_scoped(&db, scope, &tenant_name)
                .await
                .unwrap(),
            Some(tenant_manifest)
        );
        assert!(
            load_manifest_scoped(&db, DataScope::LegacyUnscoped, &tenant_name)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            load_manifests_for_definition(&db, &node_definition)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            load_manifests_for_definition_scoped(&db, scope, &tenant_definition)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(load_all_manifests(&db).await.unwrap().len(), 2);
        assert_eq!(
            load_all_manifests_scoped(&db, scope).await.unwrap().len(),
            1
        );

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            make_text_index_manifest_key_scoped(DataScope::LegacyUnscoped, "malformed"),
            Bytes::from_static(b"not-json"),
        )
        .unwrap();
        tx.commit().await.unwrap();
        assert!(load_manifest(&db, "malformed").await.is_err());
        assert!(load_all_manifests(&db).await.is_err());
        assert!(decode_manifest_bytes(b"{").is_err());
    }

    #[tokio::test]
    async fn live_state_search_filters_versions_and_deduplicates_splits() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("text-live-search", Arc::clone(&store))
            .build()
            .await
            .unwrap();
        let definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let index_name = resolve_physical_index_name(&definition, None).unwrap();
        let left = persist_documents_as_manifest(
            &store,
            "live-db",
            &definition,
            &index_name,
            &[
                TextDocumentInput::new(1, "common alpha"),
                TextDocumentInput::new(2, "common alpha").with_logical_version(2),
                TextDocumentInput::new(3, "common alpha"),
            ],
        )
        .await
        .unwrap()
        .unwrap();
        let right = persist_documents_as_manifest(
            &store,
            "live-db",
            &definition,
            &index_name,
            &[
                TextDocumentInput::new(1, "common beta"),
                TextDocumentInput::new(4, "common beta"),
            ],
        )
        .await
        .unwrap()
        .unwrap();
        let mut manifest = left;
        manifest.splits.push(right.primary_split_ref().clone());

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        for (entity_id, state) in [
            (1, TextIndexLiveState::live(1)),
            (2, TextIndexLiveState::live(1)),
            (3, TextIndexLiveState::dead(1)),
        ] {
            tx.put(
                make_text_index_live_state_key_scoped(
                    DataScope::LegacyUnscoped,
                    &index_name,
                    entity_id,
                ),
                Bytes::from(encode_live_state_bytes(&state).unwrap()),
            )
            .unwrap();
        }
        tx.commit().await.unwrap();

        assert!(
            search_manifest_with_live_state(&db, &store, "live-db", &manifest, "common", 0)
                .await
                .unwrap()
                .is_empty()
        );
        let hits = search_manifest_with_live_state(&db, &store, "live-db", &manifest, "common", 10)
            .await
            .unwrap();
        assert_eq!(
            hits.into_iter()
                .map(|hit| hit.entity_id)
                .collect::<Vec<_>>(),
            vec![1, 4]
        );
        assert!(search_manifest(&store, "live-db", &manifest, "common", 10)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn blob_lifecycle_copies_and_collects_content_addressed_objects() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let referenced = upload_blob(&store, "/source/", b"referenced")
            .await
            .unwrap();
        let duplicate = upload_blob(&store, "source", b"referenced").await.unwrap();
        let unreferenced = upload_blob(&store, "source", b"unreferenced")
            .await
            .unwrap();
        assert_eq!(referenced, duplicate);
        assert_eq!(list_blob_hashes(&store, "source").await.unwrap().len(), 2);

        let referenced_hashes = BTreeSet::from([referenced.sha256]);
        copy_blob_set(&store, "source", "source", &referenced_hashes)
            .await
            .unwrap();
        copy_blob_set(&store, "source", "/target/", &referenced_hashes)
            .await
            .unwrap();
        assert_eq!(list_blob_hashes(&store, "target").await.unwrap().len(), 1);

        let malformed = ObjectStorePath::from("source/fts/blobs/not-a-sha256");
        store
            .put(&malformed, PutPayload::from_static(b"ignored"))
            .await
            .unwrap();
        assert_eq!(
            delete_unreferenced_blobs(
                &store,
                "source",
                &referenced_hashes,
                Duration::from_secs(60),
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            delete_unreferenced_blobs(&store, "source", &referenced_hashes, Duration::ZERO,)
                .await
                .unwrap(),
            1
        );
        assert!(store
            .head(&blob_object_store_path("source", referenced.sha256))
            .await
            .is_ok());
        assert!(store
            .head(&blob_object_store_path("source", unreferenced.sha256))
            .await
            .is_err());
        assert!(store.head(&malformed).await.is_ok());

        assert_eq!(
            parse_blob_hash_from_location(&blob_object_store_path("", [0xab; 32])),
            Some([0xab; 32])
        );
        assert_eq!(sha256_hex([0xab; 32]), "ab".repeat(32));
        assert!(parse_blob_hash_from_location(&ObjectStorePath::from("short")).is_none());
        assert!(parse_blob_hash_from_location(&ObjectStorePath::from("z".repeat(64))).is_none());
    }

    #[tokio::test]
    async fn document_collectors_read_typed_node_and_edge_rows() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = Db::builder("text-document-collectors", store)
            .build()
            .await
            .unwrap();
        let node_definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let edge_definition = TextIndexDefinition::new_edge("MENTIONS", "body").unwrap();
        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(Bytes::from_static(&[0x02]), Bytes::new()).unwrap();
        tx.put(
            keys::Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(1)),
            }
            .to_bytes(),
            encode_properties(&[
                Property::string("$label", "Doc"),
                Property::string("body", "alpha node"),
            ]),
        )
        .unwrap();
        tx.put(
            keys::Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(2)),
            }
            .to_bytes(),
            encode_properties(&[
                Property::string("$label", "Other"),
                Property::string("body", "ignored node"),
            ]),
        )
        .unwrap();
        crate::search::store_edge_endpoints(&tx, 10, 1, 2)
            .await
            .unwrap();
        crate::search::store_edge_properties_by_id(
            &tx,
            10,
            &[
                Property::string("$label", "MENTIONS"),
                Property::new(
                    "body",
                    PropertyValue::StringArray(vec!["alpha".into(), "edge".into()]),
                ),
            ],
        )
        .await
        .unwrap();
        crate::search::store_edge_endpoints(&tx, 11, 1, 2)
            .await
            .unwrap();
        tx.put(Bytes::from_static(&[0x04]), Bytes::new()).unwrap();
        tx.commit().await.unwrap();

        assert_eq!(
            collect_node_documents_from_reader(&db, &node_definition, None)
                .await
                .unwrap(),
            vec![TextDocumentInput::new(1, "alpha node")]
        );
        assert_eq!(
            collect_edge_documents_from_reader(&db, &edge_definition, None)
                .await
                .unwrap(),
            vec![TextDocumentInput::new(10, "alpha\nedge")]
        );

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        tx.put(
            keys::Key::Data {
                scope: DataScope::LegacyUnscoped,
                kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(3)),
            }
            .to_bytes(),
            Bytes::from_static(b"invalid-properties"),
        )
        .unwrap();
        tx.commit().await.unwrap();
        assert!(
            collect_node_documents_from_reader(&db, &node_definition, None)
                .await
                .is_err()
        );
    }

    #[test]
    fn document_construction_and_validation_cover_value_and_tenant_contracts() {
        let node_definition = TextIndexDefinition::new_node("Doc", "body").unwrap();
        let edge_definition = TextIndexDefinition::new_edge("MENTIONS", "body").unwrap();
        let tenant_definition = TextIndexDefinition::new_node("Doc", "body")
            .unwrap()
            .with_tenant_property("firmId")
            .unwrap();
        let tenant_a = PropertyValue::String("tenant-a".into());
        let tenant_b = PropertyValue::String("tenant-b".into());
        let node_properties = vec![
            Property::string("$label", "Doc"),
            Property::string("body", "alpha"),
        ];

        assert!(make_node_document(1, &[], &node_definition, None)
            .unwrap()
            .is_none());
        assert!(make_node_document(
            1,
            &[Property::string("$label", "Doc")],
            &node_definition,
            None,
        )
        .unwrap()
        .is_none());
        assert_eq!(
            make_node_document(1, &node_properties, &node_definition, None)
                .unwrap()
                .unwrap(),
            TextDocumentInput::new(1, "alpha")
        );
        assert!(
            make_node_document(1, &node_properties, &node_definition, Some(&tenant_a)).is_err()
        );

        let tenant_properties = vec![
            Property::string("$label", "Doc"),
            Property::string("body", "tenant text"),
            Property::new("firmId", tenant_a.clone()),
        ];
        assert!(
            make_node_document(2, &tenant_properties, &tenant_definition, None)
                .unwrap()
                .is_none()
        );
        assert!(
            make_node_document(2, &tenant_properties, &tenant_definition, Some(&tenant_b))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            make_node_document(2, &tenant_properties, &tenant_definition, Some(&tenant_a))
                .unwrap()
                .unwrap()
                .text,
            "tenant text"
        );
        assert!(
            make_node_document(2, &node_properties, &tenant_definition, Some(&tenant_a)).is_err()
        );

        let wrong_node_label = [Property::string("$label", "Other")];
        let missing_node_body = [Property::string("$label", "Doc")];
        let invalid_node_body = [
            Property::string("$label", "Doc"),
            Property::new("body", PropertyValue::Bool(true)),
        ];
        validate_node_properties_for_definition(&node_definition, &wrong_node_label).unwrap();
        validate_node_properties_for_definition(&node_definition, &missing_node_body).unwrap();
        assert!(
            validate_node_properties_for_definition(&node_definition, &invalid_node_body).is_err()
        );
        assert!(
            validate_node_properties_for_definition(&tenant_definition, &node_properties).is_err()
        );
        validate_node_properties_for_definition(&tenant_definition, &tenant_properties).unwrap();

        let wrong_edge_label = [Property::string("$label", "Other")];
        let missing_edge_body = [Property::string("$label", "MENTIONS")];
        let invalid_edge_body = [
            Property::string("$label", "MENTIONS"),
            Property::new("body", PropertyValue::I64(1)),
        ];
        validate_edge_properties_for_definition(&edge_definition, &wrong_edge_label).unwrap();
        validate_edge_properties_for_definition(&edge_definition, &missing_edge_body).unwrap();
        assert!(
            validate_edge_properties_for_definition(&edge_definition, &invalid_edge_body).is_err()
        );
        assert!(make_edge_document(9, &[], &edge_definition, None)
            .unwrap()
            .is_none());
        assert!(make_edge_document(
            9,
            &[Property::string("$label", "MENTIONS")],
            &edge_definition,
            None,
        )
        .unwrap()
        .is_none());

        assert!(normalize_indexed_text_value(&PropertyValue::Null).is_err());
        assert!(normalize_indexed_text_value(&PropertyValue::Bool(true)).is_err());
        assert!(normalize_tenant_value(&PropertyValue::Null).is_none());
        assert!(normalize_tenant_value(&tenant_a).is_some());
        assert!(resolve_physical_index_name(&node_definition, Some(&tenant_a)).is_err());
    }

    #[tokio::test]
    async fn empty_inputs_and_required_schema_have_explicit_search_contracts() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let definition = TextIndexDefinition::new_node("Doc", "body")
            .unwrap()
            .with_analyzer(TextAnalyzerKind::WhitespaceLowercase)
            .with_positions_enabled(true);
        assert!(search_documents(&definition, &[], "alpha", 10)
            .unwrap()
            .is_empty());
        assert!(search_documents(
            &definition,
            &[TextDocumentInput::new(1, "Alpha")],
            "alpha",
            0,
        )
        .unwrap()
        .is_empty());
        assert!(
            persist_documents_as_manifest(&store, "empty-db", &definition, "empty", &[])
                .await
                .unwrap()
                .is_none()
        );
        assert!(decode_live_state_bytes(b"not-json").is_err());
        assert!(!should_persist_file(META_JSON_FILE));
        assert!(!should_persist_file("writer.lock"));
        assert!(should_persist_file("segment.idx"));

        let empty_schema = Schema::builder().build();
        assert!(lookup_schema_fields(&empty_schema).is_err());
        let mut entity_only = Schema::builder();
        entity_only.add_u64_field(ENTITY_ID_FIELD_NAME, NumericOptions::default());
        assert!(lookup_schema_fields(&entity_only.build()).is_err());
    }

    #[test]
    fn resolve_physical_index_name_requires_tenant_when_configured() {
        let definition = TextIndexDefinition::new_node("Doc", "body")
            .expect("test text definition is valid")
            .with_tenant_property("firmId")
            .expect("test tenant property is valid");
        let tenant = PropertyValue::String("tenant-a".into());
        assert!(resolve_physical_index_name(&definition, Some(&tenant)).is_ok());
        assert!(resolve_physical_index_name(&definition, None).is_err());
    }

    #[test]
    fn make_edge_document_respects_label_and_tenant() {
        let definition = TextIndexDefinition::new_edge("REL", "body")
            .expect("test text definition is valid")
            .with_tenant_property("firmId")
            .expect("test tenant property is valid");
        let properties = vec![
            Property::new("$label", "REL"),
            Property::new("firmId", "tenant-a"),
            Property::new("body", "alice edge"),
        ];
        let tenant = PropertyValue::String("tenant-a".into());
        let doc = make_edge_document(9, &properties, &definition, Some(&tenant)).expect("edge doc");
        assert_eq!(doc.expect("present").entity_id, 9);
    }
}
