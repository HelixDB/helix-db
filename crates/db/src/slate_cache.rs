//! SlateDB block cache owned by one database handle.
//!
//! A hybrid cache runs its Foyer disk engine's background tasks on a runtime
//! the cache owns. Foyer 0.22's block engine never ends those tasks itself:
//! each flusher task holds the block manager, whose reclaimer holds every
//! flusher's queue sender, so no queue ever closes. The tasks therefore keep
//! the engine's device, with one open file per disk block, alive for as long
//! as their runtime runs. Stopping the owned runtime drops them, so the files
//! close when the last handle to the cache drops.

use std::sync::Arc;
use std::time::Duration;

use foyer::{
    BlockEngineConfig, Engine, EngineBuildContext, EngineConfig, HybridCacheProperties, Spawner,
};
use futures::future::BoxFuture;
use slatedb::db_cache::{CachedEntry, CachedKey, DbCache};

use crate::error::{HelixDbError, Result};

/// Longest a close waits for the disk engine's tasks to drop.
///
/// A closed engine's tasks are idle, so this bounds only a stuck shutdown.
const DISK_ENGINE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

type SlateBlockEngineConfig = BlockEngineConfig<CachedKey, CachedEntry, HybridCacheProperties>;
type SlateEngineConfig = dyn EngineConfig<CachedKey, CachedEntry, HybridCacheProperties>;

/// SlateDB's block cache for one database handle.
pub(crate) enum SlateDbCache {
    /// Resident Foyer block and metadata caches.
    Memory(Arc<dyn DbCache>),
    /// Foyer hybrid cache whose disk engine runs on `disk_engine`.
    Hybrid {
        cache: Arc<dyn DbCache>,
        disk_engine: DiskEngineRuntime,
    },
}

impl SlateDbCache {
    /// The cache SlateDB reads through.
    pub(crate) fn cache(&self) -> &Arc<dyn DbCache> {
        match self {
            Self::Memory(cache) | Self::Hybrid { cache, .. } => cache,
        }
    }

    /// Flushes the cache, then stops a hybrid cache's disk engine so dropping
    /// the last handle to the cache closes its disk-block files.
    ///
    /// A failed flush leaves the engine running so the close can be retried.
    pub(crate) async fn close(&self) -> Result<()> {
        self.cache().close().await?;
        let Self::Hybrid { disk_engine, .. } = self else {
            return Ok(());
        };
        disk_engine.shutdown().await;
        Ok(())
    }
}

/// Dedicated runtime for one Foyer disk engine's background tasks.
///
/// It has a single worker: Foyer's default engine runs one flusher and one
/// reclaimer, which await disk I/O the caller's runtime performs. Foyer still
/// runs cache-miss loads, and so object-store reads, on the caller's runtime.
pub(crate) struct DiskEngineRuntime {
    handle: tokio::runtime::Handle,
    /// `None` once shut down.
    runtime: parking_lot::Mutex<Option<tokio::runtime::Runtime>>,
}

impl DiskEngineRuntime {
    /// Starts the runtime's worker thread.
    pub(crate) fn start() -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("helix-foyer-disk")
            .build()
            .map_err(|err| {
                HelixDbError::Config(format!(
                    "failed to start Slate hybrid cache disk engine runtime: {err}"
                ))
            })?;
        Ok(Self {
            handle: runtime.handle().clone(),
            runtime: parking_lot::Mutex::new(Some(runtime)),
        })
    }

    /// Wraps `engine` so Foyer builds it with its tasks on this runtime.
    pub(crate) fn engine_config(&self, engine: SlateBlockEngineConfig) -> Box<SlateEngineConfig> {
        Box::new(DiskEngineConfig {
            engine,
            spawner: Spawner::from(self.handle.clone()),
        })
    }

    /// Stops the runtime and waits for its tasks to drop.
    async fn shutdown(&self) {
        let Some(runtime) = self.runtime.lock().take() else {
            return;
        };
        // Waiting for the worker thread to exit blocks, which an async worker
        // must not do.
        let Err(error) = tokio::task::spawn_blocking(move || {
            runtime.shutdown_timeout(DISK_ENGINE_SHUTDOWN_TIMEOUT);
        })
        .await
        else {
            return;
        };
        tracing::warn!(%error, "Slate hybrid cache disk engine shutdown did not finish");
    }
}

impl Drop for DiskEngineRuntime {
    /// Stops a runtime no close stopped. A drop cannot wait, so the tasks drop
    /// shortly after it returns.
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.get_mut().take() else {
            return;
        };
        runtime.shutdown_background();
    }
}

/// Builds Foyer's block engine with its tasks on a [`DiskEngineRuntime`].
#[derive(Debug)]
struct DiskEngineConfig {
    engine: SlateBlockEngineConfig,
    spawner: Spawner,
}

impl EngineConfig<CachedKey, CachedEntry, HybridCacheProperties> for DiskEngineConfig {
    fn build(
        self: Box<Self>,
        context: EngineBuildContext,
    ) -> BoxFuture<
        'static,
        foyer::Result<Arc<dyn Engine<CachedKey, CachedEntry, HybridCacheProperties>>>,
    > {
        let Self { engine, spawner } = *self;
        EngineConfig::build(Box::new(engine), EngineBuildContext { spawner, ..context })
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::num::NonZeroU64;
    use std::path::Path;
    use std::time::Instant;

    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;

    use super::*;
    use crate::config;
    use crate::encoding::keys::scope::DataScope;
    use crate::error::WriterMigrationRequirement;
    use crate::{HelixDB, HelixDbSource, ManagedWriterOpenIntent, ProcessLocalDatabaseToken};

    /// Time allowed for dropped handles' spawned cleanup to finish.
    const RELEASE_DEADLINE: Duration = Duration::from_secs(10);

    /// Open attempts a test makes, as a caller retrying a failed open would.
    const FAILED_OPEN_ATTEMPTS: usize = 3;

    fn hybrid_slate_cache(root: &Path) -> config::SlateHybridCacheConfig {
        config::SlateHybridCacheConfig::try_new(1024 * 1024, root, 4 * 1024 * 1024)
            .expect("valid Slate hybrid cache")
    }

    fn hybrid_mode(root: &Path) -> config::CacheMode {
        config::CacheMode::Hybrid {
            slate_db: hybrid_slate_cache(root),
            object_store: config::SlateObjectStoreCacheSettings::try_new(
                root.join("object-store"),
                Some(1024 * 1024),
                4096,
                false,
                config::ObjectStoreWarmLevel::Off,
                None,
                1,
            )
            .expect("valid object-store cache"),
            slate_warm: config::SlateWarmConfig::Off,
            fts: None,
        }
    }

    fn hybrid_db_config(root: &Path) -> config::DbConfig {
        config::DbConfig::new().with_cache(config::CacheConfig::new(
            config::VectorMemorySettings::default(),
            hybrid_mode(root),
        ))
    }

    async fn hybrid_cache(root: &Path) -> SlateDbCache {
        crate::build_slate_db_cache(&hybrid_mode(root))
            .await
            .expect("hybrid cache builds")
            .expect("hybrid mode builds a cache")
    }

    fn writer_migration_required(error: &HelixDbError) -> bool {
        matches!(
            error,
            HelixDbError::WriterMigrationRequired {
                requirement: WriterMigrationRequirement::IncompleteStorageSchema,
            }
        )
    }

    /// Descriptors this process holds on Foyer's partition files under
    /// `root`, or `None` without procfs. Filtering by path keeps concurrent
    /// tests' descriptors out of the count.
    fn open_partition_files(root: &Path) -> Option<usize> {
        let entries = std::fs::read_dir("/proc/self/fd").ok()?;
        let root = root.canonicalize().expect("cache root resolves");
        Some(
            entries
                .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
                .filter(|target| target.parent() == Some(root.as_path()))
                .count(),
        )
    }

    /// Partition files still open under `root` once dropped handles' spawned
    /// cleanup finishes, or [`RELEASE_DEADLINE`] passes.
    async fn partition_files_open_after_release(root: &Path) -> usize {
        let started = Instant::now();
        let mut open = open_partition_files(root).expect("procfs is still mounted");
        while open > 0 && started.elapsed() < RELEASE_DEADLINE {
            tokio::time::sleep(Duration::from_millis(10)).await;
            open = open_partition_files(root).expect("procfs is still mounted");
        }
        open
    }

    /// Asserts a hybrid cache holds its partition files until `release` drops
    /// it, and none once the drop's spawned cleanup finishes.
    async fn assert_release_closes_partition_files(release: impl AsyncFnOnce(SlateDbCache)) {
        let root = tempfile::tempdir().expect("temporary cache root");
        let cache = hybrid_cache(root.path()).await;
        let Some(open) = open_partition_files(root.path()) else {
            eprintln!("skipping: /proc/self/fd is unavailable on this platform");
            return;
        };
        let partitions = hybrid_slate_cache(root.path()).disk_partitions();
        assert_eq!(open, partitions, "a running cache holds every partition");

        release(cache).await;
        let open = partition_files_open_after_release(root.path()).await;
        assert_eq!(open, 0, "{open} of {partitions} partition files leaked");
    }

    /// Asserts every one of [`FAILED_OPEN_ATTEMPTS`] calls to `open` fails as
    /// `expected` accepts, after building a hybrid cache under `root`, and
    /// leaves none of that cache's partition files open.
    ///
    /// Callers retry these failures, such as a reader waiting for bootstrap,
    /// so a leak would strand one more set of files on every attempt.
    async fn assert_failed_opens_close_partition_files(
        root: &Path,
        open: impl AsyncFn() -> Result<HelixDB>,
        expected: impl Fn(&HelixDbError) -> bool,
    ) {
        std::fs::create_dir_all(root).expect("cache root");
        if open_partition_files(root).is_none() {
            eprintln!("skipping: /proc/self/fd is unavailable on this platform");
            return;
        }
        let partitions = hybrid_slate_cache(root).disk_partitions();
        for attempt in 0..FAILED_OPEN_ATTEMPTS {
            let Err(error) = open().await else {
                panic!("open attempt {attempt} must fail");
            };
            assert!(
                expected(&error),
                "open attempt {attempt} failed with {error}"
            );
            let built = std::fs::read_dir(root)
                .expect("cache root lists")
                .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
                .filter(|name| name.starts_with(crate::FOYER_PARTITION_FILE_PREFIX))
                .count();
            assert_eq!(built, partitions, "open attempt {attempt} built the cache");
            let open = partition_files_open_after_release(root).await;
            assert_eq!(
                open, 0,
                "open attempt {attempt} leaked {open} of {partitions} partition files"
            );
        }
    }

    #[tokio::test]
    async fn closing_a_hybrid_cache_stops_its_disk_engine_once() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let cache = hybrid_cache(root.path()).await;
        let SlateDbCache::Hybrid { disk_engine, .. } = &cache else {
            panic!("hybrid mode builds a hybrid cache");
        };
        assert!(disk_engine.runtime.lock().is_some());

        cache.close().await.expect("hybrid cache closes");
        assert!(disk_engine.runtime.lock().is_none());
        cache
            .close()
            .await
            .expect("closing a closed hybrid cache is a no-op");
    }

    #[tokio::test]
    async fn a_memory_cache_closes_without_a_disk_engine() {
        let mode = config::CacheMode::Memory {
            slate_db: config::SlateMemoryCacheConfig::try_new(1024 * 1024, 1024 * 1024)
                .expect("valid Slate memory cache"),
            slate_warm: config::SlateWarmConfig::Off,
            fts: None,
        };
        let cache = crate::build_slate_db_cache(&mode)
            .await
            .expect("memory cache builds")
            .expect("memory mode builds a cache");
        assert!(matches!(cache, SlateDbCache::Memory(_)));
        cache.close().await.expect("memory cache closes");
    }

    #[tokio::test]
    async fn dropping_a_closed_hybrid_cache_closes_its_partition_files() {
        assert_release_closes_partition_files(async |cache: SlateDbCache| {
            cache.close().await.expect("hybrid cache closes");
        })
        .await;
    }

    #[tokio::test]
    async fn dropping_an_unclosed_hybrid_cache_closes_its_partition_files() {
        assert_release_closes_partition_files(async |cache: SlateDbCache| drop(cache)).await;
    }

    /// A reader retrying while it waits for a writer to bootstrap the
    /// database fails before SlateDB starts a task.
    #[tokio::test]
    async fn reader_opens_of_a_missing_database_close_their_partition_files() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_failed_opens_close_partition_files(
            root.path(),
            async || {
                HelixDB::open_reader_with_object_store_and_config(
                    "missing",
                    Arc::clone(&object_store),
                    hybrid_db_config(root.path()),
                )
                .await
            },
            |error| {
                matches!(
                    error,
                    HelixDbError::Storage(storage)
                        if storage.code() == Some(slatedb::ErrorCode::DatabaseMissing)
                )
            },
        )
        .await;
    }

    /// A reader retrying while it waits for a writer migration fails after
    /// SlateDB started the reader's manifest poller.
    #[tokio::test]
    async fn reader_opens_awaiting_a_writer_migration_close_their_partition_files() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let token = ProcessLocalDatabaseToken::new("slate-cache-reader-awaiting-migration")
            .expect("valid process-local database");
        let source = || HelixDbSource::InMemoryToken {
            token: token.clone(),
        };
        let writer = HelixDB::open_managed_writer_with_config(
            source(),
            config::DbConfig::new(),
            NonZeroU64::MIN,
            ManagedWriterOpenIntent::Bootstrap,
        )
        .await
        .expect("managed bootstrap succeeds");
        writer
            .inner_db()
            .delete(crate::migrations::storage_schema_complete_key_for_tests(
                DataScope::LegacyUnscoped,
            ))
            .await
            .expect("schema completion marker is removed");
        writer.close().await.expect("writer closes");

        assert_failed_opens_close_partition_files(
            root.path(),
            async || {
                HelixDB::open_reader_with_config(source(), hybrid_db_config(root.path())).await
            },
            writer_migration_required,
        )
        .await;
    }

    /// A managed writer whose storage checks fail after SlateDB started the
    /// writer's tasks.
    #[tokio::test]
    async fn failed_managed_writer_opens_close_their_partition_files() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let token = ProcessLocalDatabaseToken::new("slate-cache-failed-writer-open")
            .expect("valid process-local database");
        let writer_epoch = Cell::new(NonZeroU64::MIN);
        assert_failed_opens_close_partition_files(
            root.path(),
            async || {
                // Every attempt must fence with a newer epoch.
                let epoch = writer_epoch.get();
                writer_epoch.set(epoch.checked_add(1).expect("epoch below u64::MAX"));
                HelixDB::open_managed_writer_with_config(
                    HelixDbSource::InMemoryToken {
                        token: token.clone(),
                    },
                    hybrid_db_config(root.path()),
                    epoch,
                    // Failover never initializes pristine storage.
                    ManagedWriterOpenIntent::Failover,
                )
                .await
            },
            writer_migration_required,
        )
        .await;
    }
}
