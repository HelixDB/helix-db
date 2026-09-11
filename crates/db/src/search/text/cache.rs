//! Bounded split-aware cache for immutable Tantivy text artifacts.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::Read;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use futures::{stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use range_cache::{CacheCapacity, RangeCache};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use slatedb::object_store::{ObjectStore, ObjectStoreExt};
use tantivy::{Index, IndexReader};
use tokio::fs as tokio_fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use crate::config::{self, TextAnalyzerKind};
use crate::error::HelixDbError;

use super::bundle_storage::{CachedSplitStorage, ObjectStoreSplitBundleStorage};
use super::caching_directory::CachingDirectory;
use super::hot_directory::HotDirectory;
use super::storage_directory::StorageDirectory;
use super::{
    blob_object_store_path, build_reader, decode_footer_cache_entry_bytes, lookup_schema_fields,
    open_split_directory_from_file, read_footer_cache_entry_from_file, register_analyzers,
    search_reader_candidates_with_statistics, validate_split_bundle_file, warm_searcher,
    TextSchemaFields, TextSearchCandidate, TextSplitRef,
};

const CACHE_DIR: &str = "fts-split-cache-v2";
const BLOBS_DIR: &str = "blobs";
const METADATA_DIR: &str = "metadata";
const STAGING_DIR: &str = "staging";
const DEMAND_TRACKER_LIMIT: usize = 4096;
const ACCESS_WRITE_INTERVAL: Duration = Duration::from_secs(60);

/// Validated cache tier retained by the FTS runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FtsCacheConfig {
    /// Memory-only split caching.
    Memory(config::FtsMemoryCacheConfig),
    /// Memory plus complete local split artifacts.
    Hybrid(config::FtsHybridCacheConfig),
}

impl FtsCacheConfig {
    const fn memory_bytes(&self) -> u64 {
        match self {
            Self::Memory(config) => config.memory_bytes(),
            Self::Hybrid(config) => config.memory_bytes(),
        }
    }

    const fn disk(&self) -> Option<&config::DiskCacheConfig> {
        match self {
            Self::Memory(_) => None,
            Self::Hybrid(config) => Some(config.disk()),
        }
    }

    const fn warm_concurrency(&self) -> usize {
        match self {
            Self::Memory(config) => config.warm_concurrency(),
            Self::Hybrid(config) => config.warm_concurrency(),
        }
    }

    const fn generation_grace_period(&self) -> Duration {
        match self {
            Self::Memory(config) => config.generation_grace_period(),
            Self::Hybrid(config) => config.generation_grace_period(),
        }
    }
}

/// Exact immutable identity for one text split.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TextSplitCacheKey {
    sha256: [u8; 32],
    blob_size: u64,
    footer_offset: u64,
    footer_len: u32,
    hotcache_len: u32,
    total_size: u64,
}

impl From<&TextSplitRef> for TextSplitCacheKey {
    fn from(split: &TextSplitRef) -> Self {
        Self {
            sha256: split.blob.sha256,
            blob_size: split.blob.size_bytes,
            footer_offset: split.footer_offset,
            footer_len: split.footer_len,
            hotcache_len: split.hotcache_len,
            total_size: split.total_size_bytes,
        }
    }
}

pub(crate) struct OpenedTextSplit {
    index: Index,
    reader: IndexReader,
    fields: TextSchemaFields,
    size_bytes: u64,
    _artifact_lease: Option<DiskArtifactLease>,
}

impl OpenedTextSplit {
    pub(crate) async fn warm(
        &self,
        analyzer: TextAnalyzerKind,
        query: &str,
        fuzzy_distance: u8,
    ) -> Result<Option<super::FuzzyClauses>, HelixDbError> {
        register_analyzers(&self.index, analyzer);
        warm_searcher(&self.reader, self.fields, analyzer, query, fuzzy_distance).await
    }

    pub(crate) const fn fields(&self) -> TextSchemaFields {
        self.fields
    }

    pub(crate) fn total_docs(&self) -> usize {
        self.reader.searcher().num_docs() as usize
    }

    pub(crate) fn search_candidates_with_statistics(
        &self,
        analyzer: TextAnalyzerKind,
        query: &str,
        limit: usize,
        statistics: Option<&crate::index_lifecycle::text::statistics::TextBm25Statistics>,
        scope: &super::TextSearchScope,
        fuzzy: Option<&super::FuzzyClauses>,
    ) -> Result<Vec<TextSearchCandidate>, HelixDbError> {
        register_analyzers(&self.index, analyzer);
        search_reader_candidates_with_statistics(
            &self.reader,
            self.fields,
            analyzer,
            query,
            limit,
            statistics,
            scope,
            fuzzy,
        )
    }
}

struct MemoryEntry {
    split: Arc<OpenedTextSplit>,
    size_bytes: u64,
    access_tick: u64,
}

#[derive(Default)]
struct MemoryState {
    entries: HashMap<TextSplitCacheKey, MemoryEntry>,
    bytes: u64,
    tick: u64,
}

#[derive(Default)]
struct DemandTracker {
    counts: HashMap<TextSplitCacheKey, u8>,
    order: VecDeque<TextSplitCacheKey>,
}

impl DemandTracker {
    fn record_success(&mut self, key: TextSplitCacheKey) -> bool {
        if let Some(count) = self.counts.get_mut(&key) {
            if *count == 1 {
                *count = 2;
                return true;
            }
            return false;
        }
        self.counts.insert(key.clone(), 1);
        self.order.push_back(key);
        while self.order.len() > DEMAND_TRACKER_LIMIT {
            if let Some(oldest) = self.order.pop_front() {
                self.counts.remove(&oldest);
            }
        }
        false
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactMetadata {
    size_bytes: u64,
    last_access_unix_ms: u64,
}

#[derive(Default)]
struct FtsStats {
    memory_hits: AtomicU64,
    memory_misses: AtomicU64,
    memory_evictions: AtomicU64,
    disk_hits: AtomicU64,
    disk_misses: AtomicU64,
    disk_corruptions: AtomicU64,
    disk_evictions: AtomicU64,
    remote_opens: AtomicU64,
    open_failures: AtomicU64,
    singleflight_followers: AtomicU64,
    hydration_attempts: AtomicU64,
    hydration_completions: AtomicU64,
    hydration_failures: AtomicU64,
}

/// Runtime state for the split cache.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FtsCacheStateSnapshot {
    /// Whether this process has an FTS cache instance.
    pub enabled: bool,
    /// Configured resident-memory ceiling.
    pub resolved_memory_budget_bytes: u64,
    /// Configured local-disk ceiling.
    pub resolved_disk_budget_bytes: u64,
    /// Namespaced disk-cache root, when enabled.
    pub disk_root: Option<String>,
    /// Split readers retained in memory.
    pub retained_split_count: u64,
    /// Conservative bytes charged to retained split readers.
    pub retained_split_bytes: u64,
    /// Complete validated split artifacts on local disk.
    pub disk_artifact_count: u64,
    /// Bytes occupied by complete split artifacts on local disk.
    pub disk_artifact_bytes: u64,
    /// Exact-key memory hits.
    pub memory_hits: u64,
    /// Exact-key memory misses.
    pub memory_misses: u64,
    /// LRU memory evictions.
    pub memory_evictions: u64,
    /// Validated local-artifact hits.
    pub disk_hits: u64,
    /// Missing local-artifact lookups.
    pub disk_misses: u64,
    /// Corrupt local artifacts discarded before fallback.
    pub disk_corruptions: u64,
    /// Local artifacts removed to enforce the disk budget.
    pub disk_evictions: u64,
    /// Split readers opened against remote range storage.
    pub remote_opens: u64,
    /// Split open failures across local and remote paths.
    pub open_failures: u64,
    /// Concurrent callers that joined an existing exact-key open.
    pub singleflight_followers: u64,
    /// Full-artifact hydration attempts.
    pub hydration_attempts: u64,
    /// Successful full-artifact hydrations.
    pub hydration_completions: u64,
    /// Failed full-artifact hydrations.
    pub hydration_failures: u64,
}

/// Summary of one explicit/startup FTS warm pass.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FtsWarmSummary {
    /// Active text generations considered.
    pub generation_count: usize,
    /// Unique exact split identities considered.
    pub split_count: usize,
    /// Split readers opened successfully.
    pub opened_splits: usize,
    /// New complete disk artifacts published.
    pub hydrated_splits: usize,
    /// Remote bytes written into newly published artifacts.
    pub hydrated_bytes: u64,
    /// Best-effort open, hydration, cleanup, or lease errors.
    pub warm_errors: u64,
    /// End-to-end elapsed milliseconds for this warm pass.
    pub warm_elapsed_ms: u64,
}

struct DiskArtifactLease {
    sha256: [u8; 32],
    counts: Arc<Mutex<HashMap<[u8; 32], usize>>>,
}

impl DiskArtifactLease {
    fn acquire(sha256: [u8; 32], counts: &Arc<Mutex<HashMap<[u8; 32], usize>>>) -> Self {
        *counts.lock().entry(sha256).or_default() += 1;
        Self {
            sha256,
            counts: Arc::clone(counts),
        }
    }
}

impl Drop for DiskArtifactLease {
    fn drop(&mut self) {
        let mut counts = self.counts.lock();
        let Some(count) = counts.get_mut(&self.sha256) else {
            return;
        };
        *count -= 1;
        if *count == 0 {
            counts.remove(&self.sha256);
        }
    }
}

pub(crate) struct FtsCache {
    db_path: String,
    object_store: Arc<dyn ObjectStore>,
    config: FtsCacheConfig,
    namespace: String,
    memory: Mutex<MemoryState>,
    inflight: DashMap<TextSplitCacheKey, Arc<AsyncMutex<()>>>,
    hydration_inflight: DashMap<[u8; 32], Arc<AsyncMutex<()>>>,
    validated: Mutex<HashSet<TextSplitCacheKey>>,
    artifact_leases: Arc<Mutex<HashMap<[u8; 32], usize>>>,
    access_writes: Mutex<HashMap<[u8; 32], Instant>>,
    demand: Mutex<DemandTracker>,
    tasks: AsyncMutex<Vec<JoinHandle<()>>>,
    stats: FtsStats,
    disk_artifact_count: AtomicU64,
    disk_artifact_bytes: AtomicU64,
}

impl FtsCache {
    pub(crate) fn new(
        db_path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: FtsCacheConfig,
    ) -> Result<Self, HelixDbError> {
        let db_path = db_path.into();
        let namespace = hex_digest(db_path.as_bytes());
        if let Some(disk) = config.disk() {
            for directory in [
                disk.root().join(CACHE_DIR).join(&namespace).join(BLOBS_DIR),
                disk.root()
                    .join(CACHE_DIR)
                    .join(&namespace)
                    .join(METADATA_DIR),
                disk.root()
                    .join(CACHE_DIR)
                    .join(&namespace)
                    .join(STAGING_DIR),
            ] {
                fs::create_dir_all(&directory).map_err(|error| {
                    HelixDbError::Config(format!(
                        "failed to create FTS cache directory '{}': {error}",
                        directory.display()
                    ))
                })?;
            }
        }
        let (disk_artifact_count, disk_artifact_bytes) = disk_usage_sync(
            config
                .disk()
                .map(|disk| disk.root().join(CACHE_DIR).join(&namespace).join(BLOBS_DIR))
                .as_deref(),
        )?;
        Ok(Self {
            db_path,
            object_store,
            config,
            namespace,
            memory: Mutex::new(MemoryState::default()),
            inflight: DashMap::new(),
            hydration_inflight: DashMap::new(),
            validated: Mutex::new(HashSet::new()),
            artifact_leases: Arc::new(Mutex::new(HashMap::new())),
            access_writes: Mutex::new(HashMap::new()),
            demand: Mutex::new(DemandTracker::default()),
            tasks: AsyncMutex::new(Vec::new()),
            stats: FtsStats::default(),
            disk_artifact_count: AtomicU64::new(disk_artifact_count),
            disk_artifact_bytes: AtomicU64::new(disk_artifact_bytes),
        })
    }

    pub(crate) async fn get_or_open_split(
        self: &Arc<Self>,
        split: &TextSplitRef,
    ) -> Result<Arc<OpenedTextSplit>, HelixDbError> {
        let key = TextSplitCacheKey::from(split);
        if let Some(entry) = self.memory_hit(&key) {
            self.stats.memory_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(entry);
        }
        self.stats.memory_misses.fetch_add(1, Ordering::Relaxed);

        let gate = self
            .inflight
            .entry(key.clone())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        if Arc::strong_count(&gate) > 2 {
            self.stats
                .singleflight_followers
                .fetch_add(1, Ordering::Relaxed);
        }
        let guard = gate.lock().await;
        if let Some(entry) = self.memory_hit(&key) {
            drop(guard);
            self.inflight.remove(&key);
            return Ok(entry);
        }

        let opened = match self.try_open_disk(split, &key).await {
            Ok(Some(opened)) => Ok(opened),
            Ok(None) => self.open_remote(split).await,
            Err(error) => {
                self.stats.disk_corruptions.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(%error, "discarding invalid FTS disk artifact");
                self.remove_artifact(key.sha256).await;
                self.open_remote(split).await
            }
        };
        let opened = match opened {
            Ok(opened) => opened,
            Err(error) => {
                drop(guard);
                self.inflight.remove(&key);
                return Err(error);
            }
        };
        let opened = Arc::new(opened);
        self.insert_memory(key.clone(), Arc::clone(&opened));
        drop(guard);
        self.inflight.remove(&key);
        Ok(opened)
    }

    pub(crate) async fn after_successful_search(self: &Arc<Self>, split: TextSplitRef) {
        if self.config.disk().is_none() {
            return;
        }
        let key = TextSplitCacheKey::from(&split);
        if !self.demand.lock().record_success(key) {
            return;
        }
        let cache = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            let Some(cache) = cache.upgrade() else {
                return;
            };
            cache
                .stats
                .hydration_attempts
                .fetch_add(1, Ordering::Relaxed);
            match cache.ensure_artifact(&split).await {
                Ok(_) => {
                    cache
                        .stats
                        .hydration_completions
                        .fetch_add(1, Ordering::Relaxed);
                    if let Err(error) = cache.cleanup_disk().await {
                        tracing::warn!(%error, "FTS disk cleanup failed after demand hydration");
                    }
                }
                Err(error) => {
                    cache
                        .stats
                        .hydration_failures
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(%error, "FTS demand hydration failed");
                }
            }
        });
        let mut handles = self.tasks.lock().await;
        handles.retain(|handle| !handle.is_finished());
        handles.push(handle);
    }

    pub(crate) async fn warm_splits(
        self: &Arc<Self>,
        generation_count: usize,
        mut splits: Vec<TextSplitRef>,
    ) -> FtsWarmSummary {
        let start = Instant::now();
        splits.sort_by_key(|split| TextSplitCacheKey::from(split));
        splits.dedup_by(|left, right| {
            TextSplitCacheKey::from(&*left) == TextSplitCacheKey::from(&*right)
        });
        let split_count = splits.len();
        let opened = Arc::new(AtomicU64::new(0));
        let hydrated = Arc::new(AtomicU64::new(0));
        let hydrated_bytes = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));

        stream::iter(splits)
            .for_each_concurrent(self.config.warm_concurrency(), |split| {
                let cache = Arc::clone(self);
                let opened = Arc::clone(&opened);
                let hydrated = Arc::clone(&hydrated);
                let hydrated_bytes = Arc::clone(&hydrated_bytes);
                let errors = Arc::clone(&errors);
                async move {
                    if cache.config.disk().is_some() {
                        match cache.ensure_artifact(&split).await {
                            Ok(bytes) => {
                                if bytes > 0 {
                                    hydrated.fetch_add(1, Ordering::Relaxed);
                                    hydrated_bytes.fetch_add(bytes, Ordering::Relaxed);
                                }
                            }
                            Err(error) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!(%error, "FTS startup disk hydration failed");
                            }
                        }
                    }
                    match cache.get_or_open_split(&split).await {
                        Ok(_) => {
                            opened.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(%error, "FTS startup split open failed");
                        }
                    }
                }
            })
            .await;
        if let Err(error) = self.cleanup_disk().await {
            errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, "FTS startup disk cleanup failed");
        }

        FtsWarmSummary {
            generation_count,
            split_count,
            opened_splits: opened.load(Ordering::Relaxed) as usize,
            hydrated_splits: hydrated.load(Ordering::Relaxed) as usize,
            hydrated_bytes: hydrated_bytes.load(Ordering::Relaxed),
            warm_errors: errors.load(Ordering::Relaxed),
            warm_elapsed_ms: start.elapsed().as_millis() as u64,
        }
    }

    pub(crate) fn snapshot(&self) -> FtsCacheStateSnapshot {
        let (retained_split_count, retained_split_bytes) = {
            let memory = self.memory.lock();
            (memory.entries.len() as u64, memory.bytes)
        };
        FtsCacheStateSnapshot {
            enabled: true,
            resolved_memory_budget_bytes: self.config.memory_bytes(),
            resolved_disk_budget_bytes: self.config.disk().map_or(0, |disk| disk.bytes() as u64),
            disk_root: self
                .config
                .disk()
                .map(|disk| disk.root().display().to_string()),
            retained_split_count,
            retained_split_bytes,
            disk_artifact_count: self.disk_artifact_count.load(Ordering::Relaxed),
            disk_artifact_bytes: self.disk_artifact_bytes.load(Ordering::Relaxed),
            memory_hits: self.stats.memory_hits.load(Ordering::Relaxed),
            memory_misses: self.stats.memory_misses.load(Ordering::Relaxed),
            memory_evictions: self.stats.memory_evictions.load(Ordering::Relaxed),
            disk_hits: self.stats.disk_hits.load(Ordering::Relaxed),
            disk_misses: self.stats.disk_misses.load(Ordering::Relaxed),
            disk_corruptions: self.stats.disk_corruptions.load(Ordering::Relaxed),
            disk_evictions: self.stats.disk_evictions.load(Ordering::Relaxed),
            remote_opens: self.stats.remote_opens.load(Ordering::Relaxed),
            open_failures: self.stats.open_failures.load(Ordering::Relaxed),
            singleflight_followers: self.stats.singleflight_followers.load(Ordering::Relaxed),
            hydration_attempts: self.stats.hydration_attempts.load(Ordering::Relaxed),
            hydration_completions: self.stats.hydration_completions.load(Ordering::Relaxed),
            hydration_failures: self.stats.hydration_failures.load(Ordering::Relaxed),
        }
    }

    pub(crate) async fn close(&self) {
        let handles = {
            let mut tasks = self.tasks.lock().await;
            core::mem::take(&mut *tasks)
        };
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
    }

    fn memory_hit(&self, key: &TextSplitCacheKey) -> Option<Arc<OpenedTextSplit>> {
        let mut memory = self.memory.lock();
        memory.tick = memory.tick.wrapping_add(1);
        let tick = memory.tick;
        let entry = memory.entries.get_mut(key)?;
        entry.access_tick = tick;
        Some(Arc::clone(&entry.split))
    }

    fn insert_memory(&self, key: TextSplitCacheKey, split: Arc<OpenedTextSplit>) {
        let memory_bytes = self.config.memory_bytes();
        if split.size_bytes > memory_bytes {
            return;
        }
        let mut memory = self.memory.lock();
        memory.tick = memory.tick.wrapping_add(1);
        let tick = memory.tick;
        if memory.entries.contains_key(&key) {
            return;
        }
        memory.bytes = memory.bytes.saturating_add(split.size_bytes);
        memory.entries.insert(
            key,
            MemoryEntry {
                size_bytes: split.size_bytes,
                split,
                access_tick: tick,
            },
        );
        while memory.bytes > memory_bytes {
            let Some(victim) = memory
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.access_tick)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(entry) = memory.entries.remove(&victim) {
                memory.bytes = memory.bytes.saturating_sub(entry.size_bytes);
                self.stats.memory_evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn open_remote(&self, split: &TextSplitRef) -> Result<OpenedTextSplit, HelixDbError> {
        self.stats.remote_opens.fetch_add(1, Ordering::Relaxed);
        let result = async {
            let blob_path = blob_object_store_path(&self.db_path, split.blob.sha256);
            let footer = Arc::new(decode_footer_cache_entry_bytes(
                &self
                    .object_store
                    .get_range(&blob_path, split.footer_offset..split.total_size_bytes)
                    .await?,
                split,
            )?);
            let range_cache = split_range_cache(split.total_size_bytes)?;
            let storage: Arc<dyn super::bundle_storage::SplitStorage> =
                Arc::new(ObjectStoreSplitBundleStorage::new(
                    Arc::clone(&self.object_store),
                    blob_path,
                    Arc::clone(&footer.footer),
                ));
            let directory = StorageDirectory::new(Arc::new(CachedSplitStorage::new(
                storage,
                range_cache.clone(),
            )));
            open_entry_from_directory(
                directory,
                footer.hotcache_bytes.as_ref(),
                split.total_size_bytes,
                range_cache,
                None,
            )
        }
        .await;
        if result.is_err() {
            self.stats.open_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn try_open_disk(
        &self,
        split: &TextSplitRef,
        key: &TextSplitCacheKey,
    ) -> Result<Option<OpenedTextSplit>, HelixDbError> {
        let Some(path) = self.artifact_path(split.blob.sha256) else {
            return Ok(None);
        };
        if !tokio_fs::try_exists(&path).await.unwrap_or(false) {
            self.stats.disk_misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        self.validate_artifact(path.clone(), split.clone(), key.clone())
            .await?;
        let path_for_open = path.clone();
        let split_for_open = split.clone();
        let lease = DiskArtifactLease::acquire(split.blob.sha256, &self.artifact_leases);
        let opened = tokio::task::spawn_blocking(move || {
            let footer = read_footer_cache_entry_from_file(&path_for_open, &split_for_open)?;
            let directory = open_split_directory_from_file(&path_for_open)?;
            let range_cache = split_range_cache(split_for_open.total_size_bytes)?;
            open_entry_from_directory(
                directory,
                footer.hotcache_bytes.as_ref(),
                split_for_open.total_size_bytes,
                range_cache,
                Some(lease),
            )
        })
        .await
        .map_err(|error| HelixDbError::Config(format!("FTS disk open task failed: {error}")))??;
        self.stats.disk_hits.fetch_add(1, Ordering::Relaxed);
        self.note_access(split.blob.sha256, split.blob.size_bytes)
            .await;
        Ok(Some(opened))
    }

    async fn validate_artifact(
        &self,
        path: PathBuf,
        split: TextSplitRef,
        key: TextSplitCacheKey,
    ) -> Result<(), HelixDbError> {
        if self.validated.lock().contains(&key) {
            let size = tokio_fs::metadata(&path)
                .await
                .map_err(|error| HelixDbError::Config(error.to_string()))?
                .len();
            if size == split.total_size_bytes {
                return Ok(());
            }
        }
        tokio::task::spawn_blocking(move || {
            validate_split_bundle_file(&path, &split)?;
            let mut file = fs::File::open(&path).map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to hash FTS artifact '{}': {error}",
                    path.display()
                ))
            })?;
            let mut digest = Sha256::new();
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = file.read(&mut buffer).map_err(|error| {
                    HelixDbError::Config(format!(
                        "failed to hash FTS artifact '{}': {error}",
                        path.display()
                    ))
                })?;
                if read == 0 {
                    break;
                }
                digest.update(&buffer[..read]);
            }
            let actual: [u8; 32] = digest.finalize().into();
            if actual != split.blob.sha256 {
                return Err(HelixDbError::Config(format!(
                    "cached FTS split '{}' hash mismatch",
                    path.display()
                )));
            }
            Ok(())
        })
        .await
        .map_err(|error| HelixDbError::Config(format!("FTS validation task failed: {error}")))??;
        self.validated.lock().insert(key);
        Ok(())
    }

    async fn ensure_artifact(&self, split: &TextSplitRef) -> Result<u64, HelixDbError> {
        if self.config.disk().is_none() {
            return Ok(0);
        }
        let gate = self
            .hydration_inflight
            .entry(split.blob.sha256)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        let guard = gate.lock().await;
        let key = TextSplitCacheKey::from(split);
        if let Some(path) = self.artifact_path(split.blob.sha256)
            && tokio_fs::try_exists(&path).await.unwrap_or(false)
        {
            if self
                .validate_artifact(path.clone(), split.clone(), key.clone())
                .await
                .is_ok()
            {
                self.note_access(split.blob.sha256, split.blob.size_bytes)
                    .await;
                drop(guard);
                self.hydration_inflight.remove(&split.blob.sha256);
                return Ok(0);
            }
            self.remove_artifact(split.blob.sha256).await;
        }

        let final_path = self
            .artifact_path(split.blob.sha256)
            .expect("disk-enabled cache has an artifact path");
        let staging = self
            .staging_dir()
            .expect("disk-enabled cache has a staging directory")
            .join(format!(
                "{}.{}.tmp",
                sha_hex(split.blob.sha256),
                uuid::Uuid::new_v4()
            ));
        let object_path = blob_object_store_path(&self.db_path, split.blob.sha256);
        let result = async {
            let response = self.object_store.get(&object_path).await?;
            let mut stream = response.into_stream();
            let mut file = tokio_fs::File::create(&staging).await.map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to create FTS staging file '{}': {error}",
                    staging.display()
                ))
            })?;
            while let Some(chunk) = stream.try_next().await? {
                file.write_all(&chunk).await.map_err(|error| {
                    HelixDbError::Config(format!(
                        "failed to write FTS staging file '{}': {error}",
                        staging.display()
                    ))
                })?;
            }
            file.sync_all().await.map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to sync FTS staging file '{}': {error}",
                    staging.display()
                ))
            })?;
            drop(file);
            self.validate_artifact(staging.clone(), split.clone(), key)
                .await?;
            let published = match tokio_fs::rename(&staging, &final_path).await {
                Ok(()) => true,
                Err(error) if tokio_fs::try_exists(&final_path).await.unwrap_or(false) => {
                    let _ = tokio_fs::remove_file(&staging).await;
                    tracing::debug!(%error, "FTS artifact won publication race");
                    false
                }
                Err(error) => {
                    return Err(HelixDbError::Config(format!(
                        "failed to publish FTS artifact '{}' -> '{}': {error}",
                        staging.display(),
                        final_path.display()
                    )));
                }
            };
            sync_parent(final_path.clone()).await?;
            if published {
                let size = tokio_fs::metadata(&final_path)
                    .await
                    .map_err(|error| HelixDbError::Config(error.to_string()))?
                    .len();
                self.disk_artifact_count.fetch_add(1, Ordering::Relaxed);
                self.disk_artifact_bytes.fetch_add(size, Ordering::Relaxed);
            }
            self.write_metadata(split.blob.sha256, split.blob.size_bytes)
                .await?;
            Ok(split.blob.size_bytes)
        }
        .await;
        if result.is_err() {
            let _ = tokio_fs::remove_file(&staging).await;
        }
        drop(guard);
        self.hydration_inflight.remove(&split.blob.sha256);
        result
    }

    async fn cleanup_disk(&self) -> Result<(), HelixDbError> {
        let Some(disk) = self.config.disk() else {
            return Ok(());
        };
        let Some(blob_dir) = self.blob_dir() else {
            return Ok(());
        };
        let entries = read_disk_entries(&blob_dir, self.metadata_dir().as_deref()).await?;
        let mut total = entries.iter().map(|entry| entry.size).sum::<u64>();
        if total <= disk.bytes() as u64 {
            return Ok(());
        }
        let now = SystemTime::now();
        let leased = self.artifact_leases.lock().clone();
        for entry in entries {
            if total <= disk.bytes() as u64 {
                break;
            }
            if leased.contains_key(&entry.sha256)
                || now.duration_since(entry.last_access).unwrap_or_default()
                    < self.config.generation_grace_period()
            {
                continue;
            }
            tokio_fs::remove_file(&entry.path).await.map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to evict FTS artifact '{}': {error}",
                    entry.path.display()
                ))
            })?;
            atomic_saturating_sub(&self.disk_artifact_count, 1);
            atomic_saturating_sub(&self.disk_artifact_bytes, entry.size);
            if let Some(metadata) = self.metadata_path(entry.sha256) {
                let _ = tokio_fs::remove_file(metadata).await;
            }
            total = total.saturating_sub(entry.size);
            self.stats.disk_evictions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn note_access(&self, sha256: [u8; 32], size: u64) {
        let should_write = {
            let mut writes = self.access_writes.lock();
            match writes.get(&sha256) {
                Some(last) if last.elapsed() < ACCESS_WRITE_INTERVAL => false,
                _ => {
                    writes.insert(sha256, Instant::now());
                    true
                }
            }
        };
        if should_write && let Err(error) = self.write_metadata(sha256, size).await {
            tracing::warn!(%error, "failed to update FTS artifact access metadata");
        }
    }

    async fn write_metadata(&self, sha256: [u8; 32], size_bytes: u64) -> Result<(), HelixDbError> {
        let Some(path) = self.metadata_path(sha256) else {
            return Ok(());
        };
        let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
        let payload = serde_json::to_vec(&ArtifactMetadata {
            size_bytes,
            last_access_unix_ms: unix_ms(SystemTime::now()),
        })
        .map_err(|error| HelixDbError::Config(error.to_string()))?;
        tokio_fs::write(&temporary, payload)
            .await
            .map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to write FTS metadata '{}': {error}",
                    temporary.display()
                ))
            })?;
        tokio_fs::rename(&temporary, &path).await.map_err(|error| {
            HelixDbError::Config(format!(
                "failed to publish FTS metadata '{}': {error}",
                path.display()
            ))
        })
    }

    async fn remove_artifact(&self, sha256: [u8; 32]) {
        if let Some(path) = self.artifact_path(sha256) {
            let size = tokio_fs::metadata(&path).await.ok().map(|meta| meta.len());
            if tokio_fs::remove_file(path).await.is_ok() {
                atomic_saturating_sub(&self.disk_artifact_count, 1);
                if let Some(size) = size {
                    atomic_saturating_sub(&self.disk_artifact_bytes, size);
                }
            }
        }
        if let Some(path) = self.metadata_path(sha256) {
            let _ = tokio_fs::remove_file(path).await;
        }
        self.validated.lock().retain(|key| key.sha256 != sha256);
    }

    fn namespace_root(&self) -> Option<PathBuf> {
        Some(
            self.config
                .disk()?
                .root()
                .join(CACHE_DIR)
                .join(&self.namespace),
        )
    }

    fn blob_dir(&self) -> Option<PathBuf> {
        Some(self.namespace_root()?.join(BLOBS_DIR))
    }

    fn metadata_dir(&self) -> Option<PathBuf> {
        Some(self.namespace_root()?.join(METADATA_DIR))
    }

    fn staging_dir(&self) -> Option<PathBuf> {
        Some(self.namespace_root()?.join(STAGING_DIR))
    }

    fn artifact_path(&self, sha256: [u8; 32]) -> Option<PathBuf> {
        Some(self.blob_dir()?.join(format!("{}.split", sha_hex(sha256))))
    }

    fn metadata_path(&self, sha256: [u8; 32]) -> Option<PathBuf> {
        Some(
            self.metadata_dir()?
                .join(format!("{}.json", sha_hex(sha256))),
        )
    }
}

fn open_entry_from_directory(
    directory: impl tantivy::Directory + 'static,
    hotcache: &[u8],
    size_bytes: u64,
    range_cache: RangeCache<PathBuf>,
    artifact_lease: Option<DiskArtifactLease>,
) -> Result<OpenedTextSplit, HelixDbError> {
    let cached: Arc<dyn tantivy::Directory> =
        Arc::new(CachingDirectory::new(Arc::new(directory), range_cache));
    let hot = HotDirectory::open(cached, hotcache)?;
    let index = Index::open(hot).map_err(|error| {
        HelixDbError::Config(format!(
            "failed to open split-backed Tantivy index: {error}"
        ))
    })?;
    let fields = lookup_schema_fields(&index.schema())?;
    let reader = build_reader(&index)?;
    Ok(OpenedTextSplit {
        index,
        reader,
        fields,
        size_bytes,
        _artifact_lease: artifact_lease,
    })
}

fn split_range_cache(size_bytes: u64) -> Result<RangeCache<PathBuf>, HelixDbError> {
    let cache_bytes = usize::try_from(size_bytes)
        .map_err(|_| HelixDbError::Config("text split size exceeds platform limits".into()))?;
    let capacity = NonZeroUsize::new(cache_bytes)
        .ok_or_else(|| HelixDbError::Config("text split cache size must be non-zero".into()))?;
    Ok(RangeCache::new(CacheCapacity::Bounded(capacity)))
}

struct DiskEntry {
    sha256: [u8; 32],
    path: PathBuf,
    size: u64,
    last_access: SystemTime,
}

async fn read_disk_entries(
    blob_dir: &Path,
    metadata_dir: Option<&Path>,
) -> Result<Vec<DiskEntry>, HelixDbError> {
    let blob_dir = blob_dir.to_path_buf();
    let metadata_dir = metadata_dir.map(Path::to_path_buf);
    tokio::task::spawn_blocking(move || {
        let mut entries = Vec::new();
        for entry in fs::read_dir(&blob_dir).map_err(|error| {
            HelixDbError::Config(format!(
                "failed to scan FTS cache '{}': {error}",
                blob_dir.display()
            ))
        })? {
            let entry = entry.map_err(|error| HelixDbError::Config(error.to_string()))?;
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Some(sha256) = parse_sha(stem) else {
                continue;
            };
            let size = entry
                .metadata()
                .map_err(|error| HelixDbError::Config(error.to_string()))?
                .len();
            let metadata = metadata_dir
                .as_ref()
                .map(|dir| dir.join(format!("{stem}.json")))
                .and_then(|path| fs::read(path).ok())
                .and_then(|bytes| serde_json::from_slice::<ArtifactMetadata>(&bytes).ok());
            let last_access = metadata
                .map(|metadata| UNIX_EPOCH + Duration::from_millis(metadata.last_access_unix_ms))
                .unwrap_or(UNIX_EPOCH);
            entries.push(DiskEntry {
                sha256,
                path,
                size,
                last_access,
            });
        }
        entries.sort_by_key(|entry| entry.last_access);
        Ok(entries)
    })
    .await
    .map_err(|error| HelixDbError::Config(format!("FTS disk scan task failed: {error}")))?
}

fn disk_usage_sync(root: Option<&Path>) -> Result<(u64, u64), HelixDbError> {
    let Some(root) = root else {
        return Ok((0, 0));
    };
    let mut count = 0_u64;
    let mut bytes = 0_u64;
    for entry in fs::read_dir(root)
        .map_err(|error| HelixDbError::Config(format!("failed to scan FTS cache: {error}")))?
    {
        let entry = entry.map_err(|error| HelixDbError::Config(error.to_string()))?;
        let path = entry.path();
        let valid_name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(parse_sha)
            .is_some();
        if path.extension().and_then(|extension| extension.to_str()) != Some("split") || !valid_name
        {
            continue;
        }
        let metadata = entry
            .metadata()
            .map_err(|error| HelixDbError::Config(error.to_string()))?;
        if metadata.is_file() {
            count = count.saturating_add(1);
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok((count, bytes))
}

fn atomic_saturating_sub(value: &AtomicU64, amount: u64) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(amount))
    });
}

async fn sync_parent(path: PathBuf) -> Result<(), HelixDbError> {
    tokio::task::spawn_blocking(move || {
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                HelixDbError::Config(format!(
                    "failed to sync FTS cache directory '{}': {error}",
                    parent.display()
                ))
            })
    })
    .await
    .map_err(|error| HelixDbError::Config(format!("FTS directory sync task failed: {error}")))?
}

fn sha_hex(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_sha(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut sha = [0_u8; 32];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        sha[index] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(sha)
}

fn hex_digest(bytes: &[u8]) -> String {
    sha_hex(Sha256::digest(bytes).into())
}

fn unix_ms(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::text::{build_split_bundle, TextBlobRef};
    use bytes::Bytes;
    use slatedb::object_store::{memory::InMemory, PutPayload};
    use tantivy::schema::{
        IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
    };
    use tantivy::{doc, Index};

    fn split(seed: u8) -> TextSplitRef {
        TextSplitRef {
            blob: TextBlobRef {
                sha256: [seed; 32],
                size_bytes: 128,
            },
            footer_offset: 80,
            footer_len: 16,
            hotcache_len: 4,
            total_size_bytes: 128,
        }
    }

    fn valid_split(seed: u8) -> (Vec<u8>, TextSplitRef) {
        let directory = tempfile::tempdir().expect("tempdir");
        let mut schema = Schema::builder();
        let entity_id = schema.add_u64_field(
            "entity_id",
            NumericOptions::default().set_indexed().set_fast(),
        );
        let logical_version = schema.add_u64_field(
            "logical_version",
            NumericOptions::default().set_indexed().set_fast(),
        );
        let body = schema.add_text_field(
            "body",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqs),
            ),
        );
        let index = Index::create_in_dir(directory.path(), schema.build()).expect("create index");
        let mut writer = index.writer(15_000_000).expect("writer");
        writer
            .add_document(doc!(
                entity_id => u64::from(seed),
                logical_version => 1_u64,
                body => format!("split {seed}")
            ))
            .expect("add document");
        writer.commit().expect("commit");

        let built = build_split_bundle(directory.path()).expect("build split");
        let sha256 = Sha256::digest(&built.bytes).into();
        let split = TextSplitRef {
            blob: TextBlobRef {
                sha256,
                size_bytes: built.total_size_bytes,
            },
            footer_offset: built.footer_offset,
            footer_len: built.footer_len,
            hotcache_len: built.hotcache_len,
            total_size_bytes: built.total_size_bytes,
        };
        (built.bytes, split)
    }

    fn cache(
        database: &str,
        store: Arc<dyn ObjectStore>,
        disk_root: Option<PathBuf>,
        memory_bytes: u64,
        disk_bytes: u64,
        grace: Duration,
    ) -> Arc<FtsCache> {
        let warm = config::FtsWarmConfig::background(4, None).expect("warm config");
        let config = match disk_root {
            Some(disk_root) => FtsCacheConfig::Hybrid(
                config::FtsHybridCacheConfig::try_new(
                    memory_bytes,
                    disk_root,
                    disk_bytes.try_into().expect("disk budget fits usize"),
                    warm,
                    grace.as_secs(),
                )
                .expect("hybrid cache config"),
            ),
            None => {
                assert_eq!(disk_bytes, 0, "memory cache cannot have a disk budget");
                FtsCacheConfig::Memory(
                    config::FtsMemoryCacheConfig::try_new(memory_bytes, warm, grace.as_secs())
                        .expect("memory cache config"),
                )
            }
        };
        Arc::new(FtsCache::new(database, store, config).expect("cache"))
    }

    async fn put_split(
        store: &Arc<dyn ObjectStore>,
        database: &str,
        bytes: Vec<u8>,
        split: &TextSplitRef,
    ) {
        store
            .put(
                &blob_object_store_path(database, split.blob.sha256),
                PutPayload::from_bytes(Bytes::from(bytes)),
            )
            .await
            .expect("put split");
    }

    #[test]
    fn exact_split_key_includes_layout_and_blob_length() {
        let base = split(1);
        let mut changed = base.clone();
        changed.footer_offset += 1;
        assert_ne!(
            TextSplitCacheKey::from(&base),
            TextSplitCacheKey::from(&changed)
        );

        changed = base.clone();
        changed.blob.size_bytes += 1;
        assert_ne!(
            TextSplitCacheKey::from(&base),
            TextSplitCacheKey::from(&changed)
        );
    }

    #[test]
    fn demand_admission_triggers_only_on_second_success() {
        let key = TextSplitCacheKey::from(&split(2));
        let mut tracker = DemandTracker::default();

        assert!(!tracker.record_success(key.clone()));
        assert!(tracker.record_success(key.clone()));
        assert!(!tracker.record_success(key));
    }

    #[test]
    fn content_hash_file_names_roundtrip() {
        let hash = [0xab; 32];
        assert_eq!(parse_sha(&sha_hex(hash)), Some(hash));
        assert_eq!(parse_sha("not-a-hash"), None);
    }

    #[tokio::test]
    async fn remote_open_is_retained_and_exact_key_hits_memory() {
        let database = "fts-cache-memory-hit";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(3);
        put_split(&store, database, bytes, &split).await;
        let cache = cache(
            database,
            store,
            None,
            split.total_size_bytes,
            0,
            Duration::from_secs(300),
        );

        let first = cache.get_or_open_split(&split).await.expect("remote open");
        let second = cache.get_or_open_split(&split).await.expect("memory hit");
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.total_docs(), 1);
        let state = cache.snapshot();
        assert_eq!(state.remote_opens, 1);
        assert_eq!(state.memory_hits, 1);
        assert_eq!(state.retained_split_count, 1);
    }

    #[tokio::test]
    async fn memory_budget_evicts_the_least_recently_used_split() {
        let database = "fts-cache-memory-eviction";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (first_bytes, first) = valid_split(4);
        let (second_bytes, second) = valid_split(5);
        put_split(&store, database, first_bytes, &first).await;
        put_split(&store, database, second_bytes, &second).await;
        let budget = first.total_size_bytes.max(second.total_size_bytes);
        let cache = cache(database, store, None, budget, 0, Duration::from_secs(300));

        cache.get_or_open_split(&first).await.expect("first open");
        cache.get_or_open_split(&second).await.expect("second open");
        let state = cache.snapshot();
        assert_eq!(state.retained_split_count, 1);
        assert_eq!(state.memory_evictions, 1);
    }

    #[tokio::test]
    async fn corrupt_disk_artifact_is_removed_before_remote_fallback() {
        let database = "fts-cache-corrupt-disk";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(6);
        put_split(&store, database, bytes, &split).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            store,
            Some(disk.path().to_path_buf()),
            split.total_size_bytes,
            split.total_size_bytes * 2,
            Duration::from_secs(1),
        );
        let artifact = cache
            .artifact_path(split.blob.sha256)
            .expect("artifact path");
        tokio_fs::write(&artifact, b"corrupt")
            .await
            .expect("write corrupt artifact");

        let opened = cache
            .get_or_open_split(&split)
            .await
            .expect("remote fallback");
        assert_eq!(opened.total_docs(), 1);
        assert!(!tokio_fs::try_exists(&artifact)
            .await
            .expect("artifact status"));
        let state = cache.snapshot();
        assert_eq!(state.disk_corruptions, 1);
        assert_eq!(state.remote_opens, 1);
    }

    #[tokio::test]
    async fn hydration_publishes_once_and_reuses_the_validated_artifact() {
        let database = "fts-cache-hydration";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(7);
        put_split(&store, database, bytes, &split).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            store,
            Some(disk.path().to_path_buf()),
            split.total_size_bytes,
            split.total_size_bytes * 2,
            Duration::from_secs(1),
        );

        assert_eq!(
            cache
                .ensure_artifact(&split)
                .await
                .expect("first hydration"),
            split.total_size_bytes
        );
        assert_eq!(cache.ensure_artifact(&split).await.expect("reuse"), 0);
        let artifact = cache
            .artifact_path(split.blob.sha256)
            .expect("artifact path");
        assert_eq!(
            tokio_fs::metadata(artifact)
                .await
                .expect("artifact metadata")
                .len(),
            split.total_size_bytes
        );
    }

    #[tokio::test]
    async fn disk_cleanup_preserves_leased_artifacts() {
        let database = "fts-cache-leased-cleanup";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            store,
            Some(disk.path().to_path_buf()),
            1,
            1,
            Duration::from_secs(1),
        );
        let leased_hash = [8; 32];
        let evictable_hash = [9; 32];
        let leased_path = cache.artifact_path(leased_hash).expect("leased path");
        let evictable_path = cache.artifact_path(evictable_hash).expect("evictable path");
        tokio_fs::write(&leased_path, b"aa")
            .await
            .expect("leased artifact");
        tokio_fs::write(&evictable_path, b"bb")
            .await
            .expect("evictable artifact");
        for hash in [leased_hash, evictable_hash] {
            let metadata = serde_json::to_vec(&ArtifactMetadata {
                size_bytes: 2,
                last_access_unix_ms: 0,
            })
            .expect("serialize metadata");
            tokio_fs::write(cache.metadata_path(hash).expect("metadata path"), metadata)
                .await
                .expect("write metadata");
        }
        let lease = DiskArtifactLease::acquire(leased_hash, &cache.artifact_leases);

        cache.cleanup_disk().await.expect("cleanup");
        assert!(tokio_fs::try_exists(&leased_path)
            .await
            .expect("leased status"));
        assert!(!tokio_fs::try_exists(&evictable_path)
            .await
            .expect("evictable status"));
        drop(lease);
    }

    #[tokio::test]
    async fn oversized_memory_entries_remain_usable_without_retention() {
        let database = "fts-cache-oversized-memory";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(10);
        put_split(&store, database, bytes, &split).await;
        let cache = cache(database, store, None, 1, 0, Duration::from_secs(1));

        assert_eq!(
            cache
                .get_or_open_split(&split)
                .await
                .expect("first remote open")
                .total_docs(),
            1
        );
        assert_eq!(
            cache
                .get_or_open_split(&split)
                .await
                .expect("second remote open")
                .total_docs(),
            1
        );
        let state = cache.snapshot();
        assert_eq!(state.retained_split_count, 0);
        assert_eq!(state.remote_opens, 2);
    }

    #[tokio::test]
    async fn concurrent_exact_opens_share_one_remote_reader() {
        let database = "fts-cache-concurrent-open";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(11);
        put_split(&store, database, bytes, &split).await;
        let cache = cache(
            database,
            store,
            None,
            split.total_size_bytes,
            0,
            Duration::from_secs(1),
        );

        let opened = futures::future::join_all((0..8).map(|_| cache.get_or_open_split(&split)))
            .await
            .into_iter()
            .map(|result| result.expect("concurrent open"))
            .collect::<Vec<_>>();
        assert!(opened
            .iter()
            .all(|candidate| Arc::ptr_eq(&opened[0], candidate)));
        assert_eq!(cache.snapshot().remote_opens, 1);
    }

    #[tokio::test]
    async fn concurrent_hydration_publishes_one_complete_artifact() {
        let database = "fts-cache-concurrent-hydration";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(12);
        put_split(&store, database, bytes, &split).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            store,
            Some(disk.path().to_path_buf()),
            split.total_size_bytes,
            split.total_size_bytes * 2,
            Duration::from_secs(1),
        );

        let hydrated = futures::future::join_all((0..8).map(|_| cache.ensure_artifact(&split)))
            .await
            .into_iter()
            .map(|result| result.expect("concurrent hydration"))
            .collect::<Vec<_>>();
        assert_eq!(hydrated.iter().sum::<u64>(), split.total_size_bytes);
        assert_eq!(hydrated.iter().filter(|bytes| **bytes > 0).count(), 1);
        assert_eq!(
            tokio_fs::metadata(
                cache
                    .artifact_path(split.blob.sha256)
                    .expect("artifact path")
            )
            .await
            .expect("artifact metadata")
            .len(),
            split.total_size_bytes
        );
    }

    #[tokio::test]
    async fn second_success_admits_the_complete_disk_artifact() {
        let database = "fts-cache-second-success";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(13);
        put_split(&store, database, bytes, &split).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            store,
            Some(disk.path().to_path_buf()),
            split.total_size_bytes,
            split.total_size_bytes * 2,
            Duration::from_secs(1),
        );
        let artifact = cache
            .artifact_path(split.blob.sha256)
            .expect("artifact path");

        cache.after_successful_search(split.clone()).await;
        assert!(!tokio_fs::try_exists(&artifact)
            .await
            .expect("first-success artifact status"));
        cache.after_successful_search(split).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !tokio_fs::try_exists(&artifact)
                .await
                .expect("artifact status")
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second success hydrates");
        cache.close().await;
    }

    #[tokio::test]
    async fn grace_period_defers_oldest_access_eviction() {
        let database = "fts-cache-grace-eviction";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (first_bytes, first) = valid_split(14);
        let (second_bytes, second) = valid_split(15);
        put_split(&store, database, first_bytes, &first).await;
        put_split(&store, database, second_bytes, &second).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let budget = first.total_size_bytes.max(second.total_size_bytes);
        let protected = cache(
            database,
            Arc::clone(&store),
            Some(disk.path().to_path_buf()),
            budget,
            budget,
            Duration::from_secs(300),
        );
        protected
            .ensure_artifact(&first)
            .await
            .expect("first hydration");
        protected
            .ensure_artifact(&second)
            .await
            .expect("second hydration");
        protected.cleanup_disk().await.expect("protected cleanup");
        assert_eq!(protected.snapshot().disk_artifact_count, 2);
        for split in [&first, &second] {
            let metadata = serde_json::to_vec(&ArtifactMetadata {
                size_bytes: split.blob.size_bytes,
                last_access_unix_ms: 0,
            })
            .expect("serialize metadata");
            tokio_fs::write(
                protected
                    .metadata_path(split.blob.sha256)
                    .expect("metadata path"),
                metadata,
            )
            .await
            .expect("write metadata");
        }

        let evicting = cache(
            database,
            store,
            Some(disk.path().to_path_buf()),
            budget,
            budget,
            Duration::from_secs(1),
        );
        evicting.cleanup_disk().await.expect("unprotected cleanup");
        assert_eq!(evicting.snapshot().disk_artifact_count, 1);
    }
}
