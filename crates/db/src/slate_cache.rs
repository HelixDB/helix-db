//! SlateDB's block cache for a database handle.
//!
//! A hybrid cache belongs to the process, not to a handle: every handle whose
//! cache uses the same disk root reads through one Foyer device, built by the
//! first open of that root and kept until the process exits.
//!
//! Foyer 0.22's block engine never ends its background tasks. Each flusher
//! holds the block manager, whose reclaimer holds every flusher's queue sender,
//! so no queue ever closes and the device keeps one open file per disk block.
//! A device per open would strand a full set of files on every reader retry
//! and role handoff, and two devices on one root would overwrite each other's
//! blocks. One device per root bounds a process to one set.
//!
//! The engine's tasks run on [`ENGINE_RUNTIME`], which is never shut down.
//! Dropping a busy engine's tasks can deadlock its block manager: a dropped
//! reclaim schedules the next one while it holds the manager's lock, and a
//! runtime that is shutting down drops that new task in place, which takes the
//! same lock again. Keeping the tasks off the caller's runtime lets the caller
//! shut down at any time. Disk I/O and cache-miss loads still run on the
//! runtime that first opened the root, so that runtime must outlive every
//! handle on the root, as the single runtime of the server, the managed
//! service and the bindings does.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use foyer::DeviceBuilder;
use futures::future::BoxFuture;
use slatedb::db_cache::{foyer_hybrid, CachedEntry, CachedKey, DbCache};

use crate::config;
use crate::error::{HelixDbError, Result};

/// File name prefix of Foyer's partition files, one per disk block.
pub(crate) const FOYER_PARTITION_FILE_PREFIX: &str = "foyer-storage-direct-fs-";

/// Runs the background tasks of every hybrid cache's disk engine.
///
/// Never shut down, so dropping the engine's tasks cannot deadlock it. One
/// worker serves Foyer's default flusher and reclaimer, which await disk I/O
/// the caller's runtime performs.
static ENGINE_RUNTIME: LazyLock<std::io::Result<tokio::runtime::Runtime>> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("helix-foyer-engine")
        .enable_time()
        .build()
});

/// The hybrid cache of each canonical disk root this process has opened.
///
/// A root maps to a cell rather than to a cache, so the map's lock is never
/// held across a build: a slow build cannot delay another root, and a failed
/// build leaves the cell empty for the next open to retry.
static HYBRID_CACHES: LazyLock<
    parking_lot::Mutex<HashMap<PathBuf, Arc<tokio::sync::OnceCell<SharedHybridCache>>>>,
> = LazyLock::new(Default::default);

/// SlateDB's block cache for one database handle.
pub(crate) enum SlateDbCache {
    /// Resident caches the handle owns and closes.
    Memory(Arc<dyn DbCache>),
    /// The process's hybrid cache for a disk root, shared with every other
    /// handle on that root and never closed.
    Hybrid(Arc<dyn DbCache>),
}

impl SlateDbCache {
    /// The cache SlateDB reads through.
    pub(crate) fn cache(&self) -> &Arc<dyn DbCache> {
        match self {
            Self::Memory(cache) | Self::Hybrid(cache) => cache,
        }
    }

    /// Closes a memory cache. A hybrid cache stays open: other handles on its
    /// root may still read through it, and later opens of the root reuse it.
    pub(crate) async fn close(&self) -> Result<()> {
        let Self::Memory(cache) = self else {
            return Ok(());
        };
        cache.close().await?;
        Ok(())
    }
}

/// A built hybrid cache and the settings, with a canonical root, it was
/// built with.
#[derive(Clone)]
struct SharedHybridCache {
    config: config::SlateHybridCacheConfig,
    cache: Arc<dyn DbCache>,
}

/// The process's hybrid cache for `config`'s disk root, built by the first
/// open of the root and shared with every later one.
///
/// Every later open of the root must ask for the same capacities: the root
/// holds one device, so resizing it takes a restart.
pub(crate) async fn shared_hybrid(config: &config::SlateHybridCacheConfig) -> Result<SlateDbCache> {
    let requested_root = config.disk().root();
    // Every spelling of a root, such as through `..` or a symlink, must reach
    // one device, and only an existing directory canonicalizes.
    let root = std::fs::create_dir_all(requested_root)
        .and_then(|()| std::fs::canonicalize(requested_root))
        .map_err(|err| {
            HelixDbError::Config(format!(
                "failed to resolve Slate hybrid cache root '{}': {err}",
                requested_root.display()
            ))
        })?;
    let config = config::SlateHybridCacheConfig::try_new(
        config.memory_bytes(),
        root.clone(),
        config.disk().bytes(),
    )?;
    let cell = Arc::clone(HYBRID_CACHES.lock().entry(root.clone()).or_default());
    // Built on a task of its own, so a cancelled open cannot drop a half-built
    // device whose engine tasks would hold its files beside the next attempt's
    // device.
    let shared = tokio::spawn({
        let config = config.clone();
        async move {
            cell.get_or_try_init(|| build_hybrid(&config))
                .await
                .cloned()
        }
    })
    .await
    .map_err(|err| {
        HelixDbError::Config(format!(
            "Slate hybrid cache build for '{}' did not finish: {err}",
            root.display()
        ))
    })??;
    if shared.config != config {
        return Err(HelixDbError::Config(format!(
            "Slate hybrid cache root '{}' is already open in this process as {:?}, not {config:?}",
            root.display(),
            shared.config,
        )));
    }
    Ok(SlateDbCache::Hybrid(shared.cache))
}

/// Builds the hybrid cache for a canonical `config`, with its disk engine's
/// tasks on [`ENGINE_RUNTIME`].
async fn build_hybrid(config: &config::SlateHybridCacheConfig) -> Result<SharedHybridCache> {
    let root = config.disk().root();
    remove_stale_foyer_partitions(root, config.disk_partitions(), config.disk_block_bytes())
        .map_err(|err| {
            HelixDbError::Config(format!(
                "failed to remove stale Slate hybrid cache partitions in '{}': {err}",
                root.display()
            ))
        })?;
    let engine_runtime = ENGINE_RUNTIME.as_ref().map_err(|err| {
        HelixDbError::Config(format!(
            "failed to start the Slate hybrid cache engine runtime: {err}"
        ))
    })?;
    let device = foyer::FsDeviceBuilder::new(root)
        .with_capacity(config.disk().bytes())
        .build()
        .map_err(|err| {
            HelixDbError::Config(format!("failed to build Slate hybrid cache device: {err}"))
        })?;
    let metrics = foyer_hybrid::FoyerHybridCacheMetrics::new();
    let cache = foyer::HybridCacheBuilder::new()
        .with_name("helix-slate-hybrid")
        .with_metrics_registry(metrics.registry())
        .memory(config.memory_bytes())
        .with_weighter(|_, value: &CachedEntry| value.size())
        .storage()
        .with_io_engine_config(foyer::PsyncIoEngineConfig::new())
        .with_engine_config(Box::new(EngineOnRuntime {
            engine: foyer::BlockEngineConfig::new(device)
                .with_block_size(config.disk_block_bytes()),
            spawner: foyer::Spawner::from(engine_runtime.handle().clone()),
        }) as Box<SlateEngineConfig>)
        .build()
        .await
        .map_err(|err| {
            HelixDbError::Config(format!("failed to build Slate hybrid cache: {err}"))
        })?;
    Ok(SharedHybridCache {
        config: config.clone(),
        cache: Arc::new(foyer_hybrid::FoyerHybridCache::new_with_cache_and_metrics(
            cache, metrics,
        )),
    })
}

/// Removes Foyer partition files that the device about to open would not own.
///
/// Foyer opens partitions `0..partitions`, resizing reused files in place, and
/// never looks at higher indexes. Files left by a larger earlier disk budget
/// would stay on disk forever, and files of another block size would be
/// recovered at the wrong block boundaries. Both are disposable cache data.
fn remove_stale_foyer_partitions(
    root: &Path,
    partitions: usize,
    block_bytes: usize,
) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(root) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        entries => entries?,
    };
    entries.into_iter().try_for_each(|entry| {
        let entry = entry?;
        let Some(index) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix(FOYER_PARTITION_FILE_PREFIX))
            .and_then(|index| index.parse::<usize>().ok())
        else {
            return Ok(());
        };
        if index < partitions && entry.metadata()?.len() == block_bytes as u64 {
            return Ok(());
        }
        std::fs::remove_file(entry.path())
    })
}

type SlateEngineConfig =
    dyn foyer::EngineConfig<CachedKey, CachedEntry, foyer::HybridCacheProperties>;

/// Builds Foyer's block engine with its tasks on [`ENGINE_RUNTIME`].
#[derive(Debug)]
struct EngineOnRuntime {
    engine: foyer::BlockEngineConfig<CachedKey, CachedEntry, foyer::HybridCacheProperties>,
    spawner: foyer::Spawner,
}

impl foyer::EngineConfig<CachedKey, CachedEntry, foyer::HybridCacheProperties> for EngineOnRuntime {
    fn build(
        self: Box<Self>,
        context: foyer::EngineBuildContext,
    ) -> BoxFuture<
        'static,
        foyer::Result<Arc<dyn foyer::Engine<CachedKey, CachedEntry, foyer::HybridCacheProperties>>>,
    > {
        let Self { engine, spawner } = *self;
        foyer::EngineConfig::build(
            Box::new(engine),
            foyer::EngineBuildContext { spawner, ..context },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;

    use super::*;
    use crate::encoding::keys::scope::DataScope;
    use crate::error::WriterMigrationRequirement;
    use crate::{HelixDB, HelixDbSource, ManagedWriterOpenIntent, ProcessLocalDatabaseToken};

    /// Time allowed for a closed handle's tasks to drop their cache handles.
    const RELEASE_DEADLINE: Duration = Duration::from_secs(10);

    /// Open attempts a test makes, as a caller retrying a failed open would.
    const FAILED_OPEN_ATTEMPTS: usize = 3;

    fn hybrid_slate_cache(root: &Path) -> config::SlateHybridCacheConfig {
        config::SlateHybridCacheConfig::try_new(1024 * 1024, root, 4 * 1024 * 1024)
            .expect("valid Slate hybrid cache")
    }

    fn hybrid_db_config(root: &Path) -> config::DbConfig {
        config::DbConfig::new().with_cache(config::CacheConfig::new(
            config::VectorMemorySettings::default(),
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
            },
        ))
    }

    /// The process's hybrid cache for `config`.
    async fn shared_cache(config: &config::SlateHybridCacheConfig) -> Arc<dyn DbCache> {
        let SlateDbCache::Hybrid(cache) = shared_hybrid(config).await.expect("hybrid cache builds")
        else {
            panic!("a hybrid config builds a hybrid cache");
        };
        cache
    }

    /// The cache `db` reads through.
    fn cache_of(db: &HelixDB) -> Arc<dyn DbCache> {
        Arc::clone(
            db.inner
                .caches
                .slate_db
                .as_ref()
                .expect("hybrid mode builds a cache")
                .cache(),
        )
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

    /// Asserts this process holds exactly one device's partition files under
    /// `root`, where procfs can tell.
    fn assert_one_device(root: &Path, context: &str) {
        let Some(open) = open_partition_files(root) else {
            return;
        };
        let partitions = hybrid_slate_cache(root).disk_partitions();
        assert_eq!(
            open, partitions,
            "{context}: {open} partition files are open, not one device's {partitions}"
        );
    }

    /// Handles on `cache` once closed handles' tasks have dropped theirs, or
    /// [`RELEASE_DEADLINE`] passes.
    async fn handles_after_release(cache: &Arc<dyn DbCache>, idle: usize) -> usize {
        let started = Instant::now();
        while Arc::strong_count(cache) > idle && started.elapsed() < RELEASE_DEADLINE {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Arc::strong_count(cache)
    }

    /// Asserts every one of [`FAILED_OPEN_ATTEMPTS`] calls to `open` fails as
    /// `expected` accepts, reads through `root`'s one hybrid cache, and leaves
    /// no SlateDB handle holding it.
    ///
    /// Callers retry these failures, such as a reader waiting for bootstrap,
    /// so an unclosed SlateDB handle would leave one more task polling the
    /// object store on every attempt.
    async fn assert_failed_opens_close_storage(
        root: &Path,
        open: impl AsyncFn() -> Result<HelixDB>,
        expected: impl Fn(&HelixDbError) -> bool,
    ) {
        let cache = shared_cache(&hybrid_slate_cache(root)).await;
        let idle = Arc::strong_count(&cache);
        for attempt in 0..FAILED_OPEN_ATTEMPTS {
            let Err(error) = open().await else {
                panic!("open attempt {attempt} must fail");
            };
            assert!(
                expected(&error),
                "open attempt {attempt} failed with {error}"
            );
            assert_eq!(
                handles_after_release(&cache, idle).await,
                idle,
                "open attempt {attempt} left a SlateDB handle reading through the cache"
            );
            assert_one_device(root, &format!("open attempt {attempt}"));
        }
    }

    #[tokio::test]
    async fn repeated_opens_of_a_root_share_one_cache() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let config = hybrid_slate_cache(root.path());
        let first = shared_cache(&config).await;
        for _ in 0..4 {
            assert!(Arc::ptr_eq(&first, &shared_cache(&config).await));
        }
        assert_one_device(root.path(), "repeated opens");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_opens_of_a_root_build_one_cache() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let config = hybrid_slate_cache(root.path());
        let caches = futures::future::join_all((0..8).map(|_| {
            let config = config.clone();
            tokio::spawn(async move { shared_cache(&config).await })
        }))
        .await
        .into_iter()
        .map(|cache| cache.expect("open task completes"))
        .collect::<Vec<_>>();
        assert!(caches.iter().all(|cache| Arc::ptr_eq(cache, &caches[0])));
        assert_one_device(root.path(), "concurrent opens");
    }

    /// A cancelled open's build finishes on its own task, so the next open
    /// reuses it rather than building a second device over the same files.
    #[tokio::test]
    async fn a_cancelled_open_still_leaves_one_cache() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let config = hybrid_slate_cache(root.path());
        let _ = tokio::time::timeout(Duration::ZERO, shared_hybrid(&config)).await;
        let cache = shared_cache(&config).await;
        assert!(Arc::ptr_eq(&cache, &shared_cache(&config).await));
        assert_one_device(root.path(), "an open after a cancelled open");
    }

    #[tokio::test]
    async fn every_spelling_of_a_root_shares_one_cache() {
        let dir = tempfile::tempdir().expect("temporary cache root");
        let root = dir.path().join("nested").join("foyer");
        let cache = shared_cache(&hybrid_slate_cache(&root)).await;

        let mut aliases = vec![dir
            .path()
            .join("nested")
            .join("..")
            .join("nested")
            .join("foyer")];
        #[cfg(unix)]
        {
            let link = dir.path().join("link");
            std::os::unix::fs::symlink(&root, &link).expect("symlink to the cache root");
            aliases.push(link);
        }
        for alias in aliases {
            assert!(
                Arc::ptr_eq(&cache, &shared_cache(&hybrid_slate_cache(&alias)).await),
                "{} built a second cache over {}",
                alias.display(),
                root.display()
            );
        }
        assert_one_device(&root, "aliased opens");
    }

    #[tokio::test]
    async fn different_roots_get_different_caches() {
        let first = tempfile::tempdir().expect("temporary cache root");
        let second = tempfile::tempdir().expect("temporary cache root");
        assert!(!Arc::ptr_eq(
            &shared_cache(&hybrid_slate_cache(first.path())).await,
            &shared_cache(&hybrid_slate_cache(second.path())).await,
        ));
    }

    #[tokio::test]
    async fn reopening_a_root_with_other_capacities_is_rejected() {
        let root = tempfile::tempdir().expect("temporary cache root");
        shared_cache(&hybrid_slate_cache(root.path())).await;
        for (memory_bytes, disk_bytes) in [
            (2 * 1024 * 1024, 4 * 1024 * 1024),
            (1024 * 1024, 8 * 1024 * 1024),
        ] {
            let config =
                config::SlateHybridCacheConfig::try_new(memory_bytes, root.path(), disk_bytes)
                    .expect("valid Slate hybrid cache");
            let Err(HelixDbError::Config(message)) = shared_hybrid(&config).await else {
                panic!("a differently sized cache must not reuse the root's device");
            };
            assert!(
                message.contains("already open in this process"),
                "{message}"
            );
        }
        assert_one_device(root.path(), "rejected reopens");
    }

    /// Counts the closes that reach it.
    #[derive(Default)]
    struct CloseCountingCache(AtomicUsize);

    #[async_trait::async_trait]
    impl DbCache for CloseCountingCache {
        async fn get_block(
            &self,
            _key: &CachedKey,
        ) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
            Ok(None)
        }

        async fn get_index(
            &self,
            _key: &CachedKey,
        ) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
            Ok(None)
        }

        async fn get_filter(
            &self,
            _key: &CachedKey,
        ) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
            Ok(None)
        }

        async fn get_stats(
            &self,
            _key: &CachedKey,
        ) -> std::result::Result<Option<CachedEntry>, slatedb::Error> {
            Ok(None)
        }

        async fn insert(&self, _key: CachedKey, _value: CachedEntry) {}

        async fn remove(&self, _key: &CachedKey) {}

        fn entry_count(&self) -> u64 {
            0
        }

        async fn close(&self) -> std::result::Result<(), slatedb::Error> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn closing_closes_a_memory_cache_but_not_a_shared_hybrid_cache() {
        let memory = Arc::new(CloseCountingCache::default());
        SlateDbCache::Memory(Arc::clone(&memory) as Arc<dyn DbCache>)
            .close()
            .await
            .expect("memory cache closes");
        assert_eq!(memory.0.load(Ordering::SeqCst), 1);

        let hybrid = Arc::new(CloseCountingCache::default());
        SlateDbCache::Hybrid(Arc::clone(&hybrid) as Arc<dyn DbCache>)
            .close()
            .await
            .expect("hybrid cache close is a no-op");
        assert_eq!(
            hybrid.0.load(Ordering::SeqCst),
            0,
            "a handle closed the process's cache"
        );
    }

    /// A managed reader and writer on one root, as during a role handoff,
    /// read through one cache, and closing both leaves it for the next open.
    #[tokio::test]
    async fn a_writer_and_reader_on_one_root_share_one_cache() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let token = ProcessLocalDatabaseToken::new("slate-cache-shared-writer-reader")
            .expect("valid process-local database");
        let source = || HelixDbSource::InMemoryToken {
            token: token.clone(),
        };
        let writer = HelixDB::open_managed_writer_with_config(
            source(),
            hybrid_db_config(root.path()),
            NonZeroU64::MIN,
            ManagedWriterOpenIntent::Bootstrap,
        )
        .await
        .expect("managed bootstrap succeeds");
        let reader = HelixDB::open_reader_with_config(source(), hybrid_db_config(root.path()))
            .await
            .expect("reader opens");
        let cache = cache_of(&writer);
        assert!(Arc::ptr_eq(&cache, &cache_of(&reader)));
        assert_one_device(root.path(), "a writer and reader");

        writer.close().await.expect("writer closes");
        reader.close().await.expect("reader closes");
        drop((writer, reader));
        let reopened = HelixDB::open_reader_with_config(source(), hybrid_db_config(root.path()))
            .await
            .expect("reader reopens");
        assert!(Arc::ptr_eq(&cache, &cache_of(&reopened)));
        assert_one_device(root.path(), "a reopened reader");
        reopened.close().await.expect("reader closes");
    }

    /// A reader retrying while it waits for a writer to bootstrap the
    /// database fails before SlateDB starts a task.
    #[tokio::test]
    async fn reader_opens_of_a_missing_database_close_storage() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_failed_opens_close_storage(
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
    async fn reader_opens_awaiting_a_writer_migration_close_storage() {
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

        assert_failed_opens_close_storage(
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
    async fn failed_managed_writer_opens_close_storage() {
        let root = tempfile::tempdir().expect("temporary cache root");
        let token = ProcessLocalDatabaseToken::new("slate-cache-failed-writer-open")
            .expect("valid process-local database");
        let writer_epoch = Cell::new(NonZeroU64::MIN);
        assert_failed_opens_close_storage(
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

    #[test]
    fn stale_foyer_partitions_are_indexes_past_the_device_or_another_block_size() {
        const BLOCK: u64 = 64 * 1024;
        let root = tempfile::tempdir().expect("temporary cache root");
        for (name, length) in [
            ("foyer-storage-direct-fs-00000000", BLOCK),
            ("foyer-storage-direct-fs-00000001", 2 * BLOCK),
            ("foyer-storage-direct-fs-00000003", BLOCK),
            ("foyer-storage-direct-fs-00000004", BLOCK),
            ("foyer-storage-direct-fs-not-a-number", BLOCK),
            ("unrelated", 2 * BLOCK),
        ] {
            std::fs::File::create(root.path().join(name))
                .and_then(|file| file.set_len(length))
                .expect("fixture file");
        }

        remove_stale_foyer_partitions(root.path(), 4, BLOCK as usize)
            .expect("stale partitions are removed");
        let mut remaining = std::fs::read_dir(root.path())
            .expect("cache root exists")
            .map(|entry| entry.expect("readable entry").file_name())
            .collect::<Vec<_>>();
        remaining.sort();
        assert_eq!(
            remaining,
            [
                "foyer-storage-direct-fs-00000000",
                "foyer-storage-direct-fs-00000003",
                "foyer-storage-direct-fs-not-a-number",
                "unrelated",
            ]
        );

        remove_stale_foyer_partitions(&root.path().join("missing"), 4, BLOCK as usize)
            .expect("a missing cache directory has nothing to remove");
    }
}
