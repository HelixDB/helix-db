//! Bounded split-aware cache for immutable Tantivy text artifacts.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{self, Read};
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
/// Age past which `cleanup_disk` removes a staging file. A download writes
/// its staging file as each chunk arrives and a hydration removes it
/// however it ends, so one untouched this long was left by a killed
/// process, never by a download still in flight in any handle.
const STAGING_ORPHAN_AGE: Duration = Duration::from_secs(60 * 60);

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
    ) -> Result<(), HelixDbError> {
        register_analyzers(&self.index, analyzer);
        warm_searcher(&self.reader, self.fields, analyzer, query).await
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
    /// Successful full-artifact hydrations, each counted after the disk trim
    /// that follows it.
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

/// Removes a hydration's staging file when dropped, which includes an
/// aborted hydration dropped mid-download. After the rename that publishes
/// it, the staging name no longer exists and the removal finds nothing.
/// Only a killed process leaves a staging file, which `cleanup_disk`
/// removes once it is [`STAGING_ORPHAN_AGE`] old.
///
/// The unlink is synchronous because `Drop` cannot await, and one unlink is
/// short enough to run on a runtime thread.
struct StagingFileGuard(PathBuf);

impl Drop for StagingFileGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Whether a cache still runs disk cleanups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskCleanup {
    Open,
    /// [`FtsCache::close`] has waited out every cleanup; none runs again.
    Closed,
}

/// Test hook that `cleanup_disk` calls with an eviction candidate's hash.
#[cfg(test)]
type EvictionHook = Box<dyn FnMut([u8; 32]) + Send>;

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
    /// Held by a disk cleanup until its blocking work ends, which outlives
    /// an abort of the task awaiting it.
    cleanup: Arc<AsyncMutex<DiskCleanup>>,
    stats: FtsStats,
    disk_artifact_count: AtomicU64,
    disk_artifact_bytes: AtomicU64,
    /// Called by `cleanup_disk` with each eviction candidate's hash just
    /// before it checks that candidate's lease, so a test can lease or
    /// remove the artifact at that exact point.
    #[cfg(test)]
    before_eviction: Mutex<Option<EvictionHook>>,
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
            cleanup: Arc::new(AsyncMutex::new(DiskCleanup::Open)),
            stats: FtsStats::default(),
            disk_artifact_count: AtomicU64::new(disk_artifact_count),
            disk_artifact_bytes: AtomicU64::new(disk_artifact_bytes),
            #[cfg(test)]
            before_eviction: Mutex::new(None),
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
        // A split larger than the whole disk tier would only evict the rest.
        if self
            .config
            .disk()
            .is_none_or(|disk| split.blob.size_bytes > disk.bytes() as u64)
        {
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
                    if let Err(error) = cache.cleanup_disk().await {
                        tracing::warn!(%error, "FTS disk cleanup failed after demand hydration");
                    }
                    // Last, so a completion seen in a snapshot means the
                    // task, trim included, has finished.
                    cache
                        .stats
                        .hydration_completions
                        .fetch_add(1, Ordering::Relaxed);
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
                    if cache
                        .config
                        .disk()
                        .is_some_and(|disk| split.blob.size_bytes <= disk.bytes() as u64)
                    {
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
        // An aborted task's blocking cleanup still holds the lock, so every
        // deletion ends before the caller releases the cache directory.
        *self.cleanup.lock().await = DiskCleanup::Closed;
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
        // Leased before the first check, so eviction cannot delete the
        // artifact while it is hashed and then report it corrupt.
        let lease = DiskArtifactLease::acquire(split.blob.sha256, &self.artifact_leases);
        if !tokio_fs::try_exists(&path).await.unwrap_or(false) {
            self.stats.disk_misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }
        self.validate_artifact(path.clone(), split.clone(), key.clone())
            .await?;
        let path_for_open = path.clone();
        let split_for_open = split.clone();
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

    /// Accepts a published artifact whose key this cache has recorded as
    /// hashed in full, if its length still matches, and otherwise verifies
    /// it in full and records the key.
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
        verify_artifact(path, split).await?;
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
        // Held until the artifact has metadata: eviction would otherwise
        // delete it mid-validation, or once published, as never used.
        let _lease = DiskArtifactLease::acquire(split.blob.sha256, &self.artifact_leases);
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
        // No valid copy is published, so an earlier validation of this key,
        // say before another cache evicted it, vouches for nothing.
        self.validated.lock().remove(&key);

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
        let _staging_guard = StagingFileGuard(staging.clone());
        let object_path = blob_object_store_path(&self.db_path, split.blob.sha256);
        let result = async {
            let response = self.object_store.get(&object_path).await?;
            let mut stream = response.into_stream();
            // Created synchronously, with no await between the guard and the
            // file. An abort during an asynchronous create would run the
            // guard first, and the create would then finish on the blocking
            // pool and leave the file. One create is short enough to run on
            // a runtime thread.
            let mut file = fs::File::create(&staging)
                .map(tokio_fs::File::from_std)
                .map_err(|error| {
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
            // Always hashed in full, never vouched for by `validated`: a
            // concurrent open may have recorded this key for another
            // handle's published copy, which says nothing about this file.
            verify_artifact(staging.clone(), split.clone()).await?;
            let published = match tokio_fs::rename(&staging, &final_path).await {
                Ok(()) => true,
                Err(error) if tokio_fs::try_exists(&final_path).await.unwrap_or(false) => {
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
                self.validated.lock().insert(key);
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
        drop(guard);
        self.hydration_inflight.remove(&split.blob.sha256);
        result
    }

    /// Evicts the least recently used disk artifacts down to the disk budget,
    /// skipping leased ones and any used within the grace period. First it
    /// removes staging files older than [`STAGING_ORPHAN_AGE`], which only
    /// a killed process leaves behind.
    ///
    /// Runs on the blocking pool, because each eviction unlinks
    /// synchronously while it holds the lease lock. Aborting the calling task
    /// does not stop that work, so it holds the cleanup lock until it ends:
    /// [`Self::close`] waits for it, and once closed no cleanup runs.
    pub(crate) async fn cleanup_disk(self: &Arc<Self>) -> Result<(), HelixDbError> {
        let (Some(disk), Some(blob_dir), Some(staging_dir)) =
            (self.config.disk(), self.blob_dir(), self.staging_dir())
        else {
            return Ok(());
        };
        let cleanup = Arc::clone(&self.cleanup).lock_owned().await;
        if *cleanup == DiskCleanup::Closed {
            return Ok(());
        }
        let budget = disk.bytes() as u64;
        let cache = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _cleanup = cleanup;
            let now = SystemTime::now();
            fs::read_dir(&staging_dir)
                .into_iter()
                .flatten()
                .flatten()
                .map(|entry| entry.path())
                .filter(|path| {
                    path.extension().is_some_and(|extension| extension == "tmp")
                        && fs::metadata(path)
                            .and_then(|metadata| metadata.modified())
                            .is_ok_and(|modified| {
                                now.duration_since(modified).unwrap_or_default()
                                    >= STAGING_ORPHAN_AGE
                            })
                })
                .for_each(|path| {
                    let _ = fs::remove_file(path);
                });

            let entries = read_disk_entries(&blob_dir, cache.metadata_dir().as_deref())?;
            let mut total = entries.iter().map(|entry| entry.size).sum::<u64>();
            for entry in entries {
                if total <= budget {
                    break;
                }
                if now.duration_since(entry.last_access).unwrap_or_default()
                    < cache.config.generation_grace_period()
                {
                    continue;
                }
                #[cfg(test)]
                {
                    if let Some(hook) = cache.before_eviction.lock().as_mut() {
                        hook(entry.sha256);
                    }
                }
                // The lease check and the unlinks happen under the lease
                // lock, which `DiskArtifactLease::acquire` also takes. A
                // reader that leases this artifact at any point either
                // finds it gone at its first check, a plain miss, or has
                // its lease seen here. A copy of the leases taken earlier
                // would miss a later lease and delete the file while it is
                // hashed or opened.
                let removed = {
                    let leases = cache.artifact_leases.lock();
                    if leases.contains_key(&entry.sha256) {
                        continue;
                    }
                    let removed = fs::remove_file(&entry.path);
                    if removed.is_ok()
                        && let Some(metadata) = cache.metadata_path(entry.sha256)
                    {
                        let _ = fs::remove_file(metadata);
                    }
                    removed
                };
                match removed {
                    Ok(()) => {
                        atomic_saturating_sub(&cache.disk_artifact_count, 1);
                        atomic_saturating_sub(&cache.disk_artifact_bytes, entry.size);
                        cache.stats.disk_evictions.fetch_add(1, Ordering::Relaxed);
                    }
                    // Another trim listed it too and evicted it first, so
                    // that trim counts the eviction; this one carries on.
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(HelixDbError::Config(format!(
                            "failed to evict FTS artifact '{}': {error}",
                            entry.path.display()
                        )));
                    }
                }
                cache
                    .validated
                    .lock()
                    .retain(|key| key.sha256 != entry.sha256);
                total = total.saturating_sub(entry.size);
            }
            Ok(())
        })
        .await
        .map_err(|error| HelixDbError::Config(format!("FTS disk cleanup task failed: {error}")))?
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

/// Checks a split file's bundle layout and its full SHA-256.
async fn verify_artifact(path: PathBuf, split: TextSplitRef) -> Result<(), HelixDbError> {
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
    .map_err(|error| HelixDbError::Config(format!("FTS validation task failed: {error}")))?
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

fn read_disk_entries(
    blob_dir: &Path,
    metadata_dir: Option<&Path>,
) -> Result<Vec<DiskEntry>, HelixDbError> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(blob_dir).map_err(|error| {
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
    use futures::stream::BoxStream;
    use slatedb::object_store::path::Path as ObjectPath;
    use slatedb::object_store::throttle::{ThrottleConfig, ThrottledStore};
    use slatedb::object_store::{
        memory::InMemory, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
        ObjectMeta, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as ObjectStoreResult,
    };
    use std::fmt;
    use tantivy::schema::{
        IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
    };
    use tantivy::{doc, Index};
    use tokio::sync::{Notify, Semaphore};

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

    /// Holds every read until the test adds a permit to `gate`, so a
    /// hydration can be paused mid-download at a known point.
    #[derive(Debug)]
    struct GatedStore {
        inner: InMemory,
        reading: Notify,
        gate: Semaphore,
    }

    impl fmt::Display for GatedStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("gated-memory")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for GatedStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.reading.notify_one();
            let _permit = self.gate.acquire().await.expect("the gate is never closed");
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
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

    /// A restarted cache hashes an artifact on first use. Eviction running
    /// meanwhile, with the artifact over budget and never recorded as used,
    /// must skip it as leased rather than delete it mid-validation.
    #[tokio::test]
    async fn cleanup_spares_artifacts_while_they_are_validated() {
        let database = "fts-cache-validation-lease";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(17);
        put_split(&store, database, bytes, &split).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let restarted = || {
            cache(
                database,
                Arc::clone(&store),
                Some(disk.path().to_path_buf()),
                split.total_size_bytes,
                split.total_size_bytes - 1,
                Duration::from_secs(1),
            )
        };
        let publisher = restarted();
        publisher
            .ensure_artifact(&split)
            .await
            .expect("first hydration");
        let metadata = publisher
            .metadata_path(split.blob.sha256)
            .expect("metadata path");

        tokio_fs::remove_file(&metadata)
            .await
            .expect("forget the first use");
        let opening = restarted();
        let (opened, cleanup) =
            tokio::join!(opening.get_or_open_split(&split), opening.cleanup_disk());
        assert_eq!(opened.expect("disk open").total_docs(), 1);
        cleanup.expect("cleanup");
        let state = opening.snapshot();
        assert_eq!(
            (
                state.disk_hits,
                state.disk_corruptions,
                state.disk_evictions
            ),
            (1, 0, 0)
        );

        tokio_fs::remove_file(&metadata)
            .await
            .expect("forget the disk open");
        let hydrating = restarted();
        let (reused, cleanup) =
            tokio::join!(hydrating.ensure_artifact(&split), hydrating.cleanup_disk());
        assert_eq!(
            reused.expect("reuse"),
            0,
            "the artifact is not downloaded again"
        );
        cleanup.expect("cleanup");
        assert_eq!(hydrating.snapshot().disk_evictions, 0);
        assert!(tokio_fs::try_exists(
            hydrating
                .artifact_path(split.blob.sha256)
                .expect("artifact path")
        )
        .await
        .expect("artifact status"));
    }

    /// Cleanup checks each lease as it unlinks that artifact, so a lease
    /// taken after cleanup listed its victims, here while it evicts an
    /// older one, still keeps the artifact for the open that took it.
    #[tokio::test]
    async fn cleanup_spares_artifacts_leased_after_its_scan() {
        let database = "fts-cache-late-lease";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(20);
        put_split(&store, database, bytes, &split).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            store,
            Some(disk.path().to_path_buf()),
            split.total_size_bytes,
            1,
            Duration::from_secs(1),
        );
        cache
            .ensure_artifact(&split)
            .await
            .expect("first hydration");
        let older = [0xa0; 32];
        tokio_fs::write(cache.artifact_path(older).expect("older path"), b"aa")
            .await
            .expect("older artifact");
        for (last_access_unix_ms, (hash, size_bytes)) in
            [(older, 2), (split.blob.sha256, split.blob.size_bytes)]
                .into_iter()
                .enumerate()
        {
            let metadata = serde_json::to_vec(&ArtifactMetadata {
                size_bytes,
                last_access_unix_ms: last_access_unix_ms as u64,
            })
            .expect("serialize metadata");
            tokio_fs::write(cache.metadata_path(hash).expect("metadata path"), metadata)
                .await
                .expect("write metadata");
        }
        let late_lease = Arc::new(Mutex::new(None));
        *cache.before_eviction.lock() = Some(Box::new({
            let leases = Arc::clone(&cache.artifact_leases);
            let late_lease = Arc::clone(&late_lease);
            move |sha256| {
                if sha256 == split.blob.sha256 {
                    *late_lease.lock() = Some(DiskArtifactLease::acquire(sha256, &leases));
                }
            }
        }));

        cache.cleanup_disk().await.expect("cleanup");
        assert!(
            late_lease.lock().is_some(),
            "cleanup reached the split after evicting the older artifact"
        );
        let opened = cache.get_or_open_split(&split).await.expect("disk open");
        assert_eq!(opened.total_docs(), 1);
        let state = cache.snapshot();
        assert_eq!(
            (
                state.disk_hits,
                state.disk_corruptions,
                state.disk_evictions
            ),
            (1, 0, 1),
            "only the older artifact is evicted"
        );
    }

    /// Concurrent trims, such as the startup trim and one after an
    /// admission, can list the same victim. The one that finds it already
    /// gone neither fails nor counts it, and still trims to the budget.
    #[tokio::test]
    async fn cleanup_continues_past_artifacts_another_trim_evicted() {
        let database = "fts-cache-concurrent-trims";
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            Arc::new(InMemory::new()),
            Some(disk.path().to_path_buf()),
            1,
            2,
            Duration::from_secs(1),
        );
        let [gone, evicted, kept] = [[0xb0; 32], [0xb1; 32], [0xb2; 32]];
        for (last_access_unix_ms, hash) in [gone, evicted, kept].into_iter().enumerate() {
            tokio_fs::write(cache.artifact_path(hash).expect("artifact path"), b"aa")
                .await
                .expect("artifact");
            let metadata = serde_json::to_vec(&ArtifactMetadata {
                size_bytes: 2,
                last_access_unix_ms: last_access_unix_ms as u64,
            })
            .expect("serialize metadata");
            tokio_fs::write(cache.metadata_path(hash).expect("metadata path"), metadata)
                .await
                .expect("write metadata");
        }
        *cache.before_eviction.lock() = Some(Box::new({
            let other_trim = [
                cache.artifact_path(gone).expect("artifact path"),
                cache.metadata_path(gone).expect("metadata path"),
            ];
            move |sha256| {
                if sha256 == gone {
                    for path in &other_trim {
                        fs::remove_file(path).expect("the other trim evicts it first");
                    }
                }
            }
        }));

        cache
            .cleanup_disk()
            .await
            .expect("a victim that is already gone does not fail the trim");
        let remaining = [gone, evicted, kept].map(|hash| {
            cache
                .artifact_path(hash)
                .expect("artifact path")
                .try_exists()
                .expect("artifact status")
        });
        assert_eq!(remaining, [false, false, true]);
        assert_eq!(cache.snapshot().disk_evictions, 1);
    }

    /// Shutdown aborts a trim's task but not the blocking eviction it
    /// started. Close waits for that eviction, so its caller can release the
    /// cache directory, and no trim runs once the cache is closed.
    #[tokio::test]
    async fn close_waits_for_an_aborted_trim_and_stops_later_ones() {
        let database = "fts-cache-close-aborted-trim";
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            Arc::new(InMemory::new()),
            Some(disk.path().to_path_buf()),
            1,
            2,
            Duration::from_secs(1),
        );
        let [evicted, kept, late] = [[0xc0; 32], [0xc1; 32], [0xc2; 32]];
        for (last_access_unix_ms, hash) in [evicted, kept].into_iter().enumerate() {
            tokio_fs::write(cache.artifact_path(hash).expect("artifact path"), b"aa")
                .await
                .expect("artifact");
            let metadata = serde_json::to_vec(&ArtifactMetadata {
                size_bytes: 2,
                last_access_unix_ms: last_access_unix_ms as u64,
            })
            .expect("serialize metadata");
            tokio_fs::write(cache.metadata_path(hash).expect("metadata path"), metadata)
                .await
                .expect("write metadata");
        }
        let (entered, entered_signal) = std::sync::mpsc::channel();
        let (release, release_signal) = std::sync::mpsc::channel::<()>();
        *cache.before_eviction.lock() = Some(Box::new(move |_| {
            let _ = entered.send(());
            let _ = release_signal.recv();
        }));

        let trim = tokio::spawn({
            let cache = Arc::clone(&cache);
            async move { cache.cleanup_disk().await }
        });
        tokio::task::spawn_blocking(move || entered_signal.recv())
            .await
            .expect("wait for the eviction")
            .expect("the trim reached its eviction");
        trim.abort();
        assert!(trim
            .await
            .expect_err("the trim task was aborted")
            .is_cancelled());

        let closing = tokio::spawn({
            let cache = Arc::clone(&cache);
            async move { cache.close().await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let evicted_path = cache.artifact_path(evicted).expect("artifact path");
        assert!(
            !closing.is_finished(),
            "close waits for the blocked eviction"
        );
        assert!(evicted_path.try_exists().expect("artifact status"));

        release.send(()).expect("release the eviction");
        drop(release);
        closing.await.expect("close");
        assert!(
            !evicted_path.try_exists().expect("artifact status"),
            "the eviction ended before close returned"
        );
        assert_eq!(cache.snapshot().disk_evictions, 1);

        let late_path = cache.artifact_path(late).expect("artifact path");
        tokio_fs::write(&late_path, b"aa")
            .await
            .expect("artifact over budget");
        cache.cleanup_disk().await.expect("cleanup after close");
        assert!(late_path.try_exists().expect("artifact status"));
        assert_eq!(cache.snapshot().disk_evictions, 1);
    }

    /// Validation is remembered per key, but only while the validated file
    /// stays published: after an eviction, by this cache or another on the
    /// same directory, the next copy is hashed in full.
    #[tokio::test]
    async fn evicted_splits_are_hashed_in_full_again() {
        let database = "fts-cache-evicted-rehash";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(18);
        let mut corrupt = bytes.clone();
        // Outside the footer, so only the hash can tell.
        corrupt[0] ^= 0xff;
        let disk = tempfile::tempdir().expect("disk cache");
        let open_cache = || {
            cache(
                database,
                Arc::clone(&store),
                Some(disk.path().to_path_buf()),
                split.total_size_bytes,
                split.total_size_bytes - 1,
                Duration::from_secs(1),
            )
        };
        let (hydrating, evicting) = (open_cache(), open_cache());
        let artifact = hydrating
            .artifact_path(split.blob.sha256)
            .expect("artifact path");
        let metadata = hydrating
            .metadata_path(split.blob.sha256)
            .expect("metadata path");

        put_split(&store, database, bytes.clone(), &split).await;
        hydrating
            .ensure_artifact(&split)
            .await
            .expect("first hydration");
        tokio_fs::remove_file(&metadata)
            .await
            .expect("forget the use");
        evicting.cleanup_disk().await.expect("cleanup");
        assert_eq!(evicting.snapshot().disk_evictions, 1);
        put_split(&store, database, corrupt.clone(), &split).await;
        assert!(
            hydrating.ensure_artifact(&split).await.is_err(),
            "a corrupt download is rejected after another cache's eviction"
        );
        assert!(!tokio_fs::try_exists(&artifact)
            .await
            .expect("artifact status"));

        put_split(&store, database, bytes, &split).await;
        hydrating
            .ensure_artifact(&split)
            .await
            .expect("second hydration");
        tokio_fs::remove_file(&metadata)
            .await
            .expect("forget the use");
        hydrating.cleanup_disk().await.expect("cleanup");
        tokio_fs::write(&artifact, corrupt)
            .await
            .expect("corrupt artifact of the same length");
        let opened = hydrating
            .get_or_open_split(&split)
            .await
            .expect("remote fallback");
        assert_eq!(opened.total_docs(), 1);
        let state = hydrating.snapshot();
        assert_eq!(
            (state.disk_evictions, state.disk_corruptions),
            (1, 1),
            "a file reappearing after this cache's own eviction is hashed"
        );
    }

    /// Another handle on the same directory can publish a split while this
    /// cache downloads it, and an open here then records the split's key as
    /// hashed. That record vouches for the other handle's copy only, so the
    /// download is still hashed in full: a corrupt copy of the same length
    /// is rejected and the published copy stays.
    #[tokio::test]
    async fn downloads_are_hashed_in_full_after_a_concurrent_open() {
        let database = "fts-cache-staging-rehash";
        let (bytes, split) = valid_split(21);
        let mut corrupt = bytes.clone();
        // Outside the footer, so only the hash can tell.
        corrupt[0] ^= 0xff;
        let gated = Arc::new(GatedStore {
            inner: InMemory::new(),
            reading: Notify::new(),
            gate: Semaphore::new(0),
        });
        let gated_store: Arc<dyn ObjectStore> = Arc::<GatedStore>::clone(&gated);
        put_split(&gated_store, database, corrupt, &split).await;
        let valid_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_split(&valid_store, database, bytes.clone(), &split).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let open_cache = |store: Arc<dyn ObjectStore>| {
            cache(
                database,
                store,
                Some(disk.path().to_path_buf()),
                split.total_size_bytes,
                split.total_size_bytes * 2,
                Duration::from_secs(1),
            )
        };
        let (downloading, other_handle) = (open_cache(gated_store), open_cache(valid_store));

        let (download, ()) = tokio::join!(downloading.ensure_artifact(&split), async {
            gated.reading.notified().await;
            other_handle
                .ensure_artifact(&split)
                .await
                .expect("the other handle publishes");
            downloading
                .get_or_open_split(&split)
                .await
                .expect("disk open of the published copy");
            gated.gate.add_permits(1);
        });
        assert!(download.is_err(), "the corrupt download is rejected");
        let published = tokio_fs::read(
            downloading
                .artifact_path(split.blob.sha256)
                .expect("artifact path"),
        )
        .await
        .expect("published artifact");
        assert!(published == bytes, "the published copy stays");
        assert_eq!(downloading.snapshot().disk_hits, 1);
    }

    /// Closing the cache aborts a hydration that is still downloading; the
    /// partial download must not stay behind in `staging/`.
    #[tokio::test]
    async fn aborted_hydration_leaves_no_staging_file() {
        let database = "fts-cache-aborted-hydration";
        // Holds each chunk back a second per byte, so the download stalls
        // once its staging file exists.
        let store: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig {
                wait_get_per_byte: Duration::from_secs(1),
                ..ThrottleConfig::default()
            },
        ));
        let (bytes, split) = valid_split(19);
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
        let staging = cache.staging_dir().expect("staging directory");
        let staged = || fs::read_dir(&staging).expect("staging directory").count();

        for _ in 0..2 {
            cache.after_successful_search(split.clone()).await;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while staged() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the download creates its staging file");
        cache.close().await;
        assert_eq!(staged(), 0, "closing removed the partial download");
        let state = cache.snapshot();
        assert_eq!(
            (
                state.hydration_attempts,
                state.hydration_completions,
                state.disk_artifact_count
            ),
            (1, 0, 0)
        );
    }

    /// A process killed mid-download leaves its staging file behind. A trim
    /// removes one nothing has written to for an hour, even with the blobs
    /// under budget, and keeps a younger one a download may still own.
    #[tokio::test]
    async fn cleanup_removes_only_abandoned_staging_files() {
        let database = "fts-cache-staging-sweep";
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            Arc::new(InMemory::new()),
            Some(disk.path().to_path_buf()),
            1,
            1,
            Duration::from_secs(1),
        );
        let staging = cache.staging_dir().expect("staging directory");
        let [abandoned, in_flight] =
            ["abandoned", "in-flight"].map(|name| staging.join(format!("{name}.tmp")));
        for path in [&abandoned, &in_flight] {
            fs::write(path, b"partial").expect("staging file");
        }
        fs::File::options()
            .write(true)
            .open(&abandoned)
            .and_then(|file| {
                file.set_modified(SystemTime::now() - STAGING_ORPHAN_AGE - Duration::from_secs(60))
            })
            .expect("age the abandoned download");

        cache.cleanup_disk().await.expect("cleanup");
        let remaining =
            [&abandoned, &in_flight].map(|path| path.try_exists().expect("staging status"));
        assert_eq!(remaining, [false, true]);
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
    async fn splits_larger_than_the_disk_tier_are_never_admitted() {
        let database = "fts-cache-oversized-disk";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (bytes, split) = valid_split(16);
        put_split(&store, database, bytes, &split).await;
        let disk = tempfile::tempdir().expect("disk cache");
        let cache = cache(
            database,
            store,
            Some(disk.path().to_path_buf()),
            split.total_size_bytes,
            split.blob.size_bytes - 1,
            Duration::from_secs(1),
        );

        for _ in 0..2 {
            cache.after_successful_search(split.clone()).await;
        }
        assert!(
            cache.tasks.lock().await.is_empty(),
            "a second search schedules no hydration"
        );
        let warmed = cache.warm_splits(1, vec![split.clone()]).await;
        assert_eq!(
            (
                warmed.opened_splits,
                warmed.hydrated_splits,
                warmed.warm_errors
            ),
            (1, 0, 0),
            "the warm still opens the split remotely"
        );
        let state = cache.snapshot();
        assert_eq!(state.hydration_attempts, 0);
        assert_eq!(state.disk_artifact_count, 0);
        assert!(!tokio_fs::try_exists(
            cache
                .artifact_path(split.blob.sha256)
                .expect("artifact path")
        )
        .await
        .expect("artifact status"));
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
