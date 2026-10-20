//! Reopened hybrid-cache readers must reuse one set of disk-cache files.
//!
//! A hybrid SlateDB cache holds one open file per Foyer disk block for the
//! life of the process, shared by every handle on its disk root. A process
//! that reopens readers must reuse that set rather than open one per reopen,
//! or it exhausts `nofile` after a handful of reopens, and nothing else a
//! closed reader opened may outlive it, apart from SlateDB's object-store
//! cache files (see [`files_closed_readers_must_release`]).
//! Descriptors are counted through `/proc/self/fd`, so the test skips on
//! platforms without procfs. It is its own target so no concurrent test opens
//! or closes descriptors while it counts.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use db::config;
use db::HelixDB;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;

const DATABASE: &str = "hybrid-cache-reuse";
const REOPENS: usize = 8;
/// Time allowed for a closed reader's tasks to drop what they hold.
const RELEASE_DEADLINE: Duration = Duration::from_secs(10);

/// Descriptors this process holds on anything but files under `object_store_root`.
///
/// SlateDB's object-store cache never stops the evictor tasks it starts, and
/// they keep each closed handle's open part files, so a closed reader leaves
/// those behind. That is SlateDB's leak, not the hybrid cache's, and this test
/// does not cover it.
fn files_closed_readers_must_release(object_store_root: &Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("procfs lists this process's descriptors")
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter(|target| !target.starts_with(object_store_root))
        .count()
}

/// Descriptors this process holds on files directly in `dir`, which must be
/// canonical, as procfs reports resolved paths.
fn open_files_in(dir: &Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("procfs lists this process's descriptors")
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter(|target| target.parent() == Some(dir))
        .count()
}

fn hybrid_slate_cache(root: &Path) -> config::SlateHybridCacheConfig {
    config::SlateHybridCacheConfig::try_new(1024 * 1024, root.join("slate"), 4 * 1024 * 1024)
        .expect("valid Slate hybrid cache")
}

fn hybrid_config(root: &Path) -> config::DbConfig {
    config::DbConfig::new().with_cache(config::CacheConfig::new(
        config::VectorMemorySettings::default(),
        config::CacheMode::Hybrid {
            slate_db: hybrid_slate_cache(root),
            object_store: config::SlateObjectStoreCacheSettings::try_new(
                root.join("object-store"),
                Some(4 * 1024 * 1024),
                4096,
                false,
                config::ObjectStoreWarmLevel::Off,
                None,
                1,
            )
            .expect("valid object-store cache"),
            slate_warm: config::SlateWarmConfig::Off,
            fts: Some(
                config::FtsHybridCacheConfig::try_new(
                    1024 * 1024,
                    root.join("fts"),
                    4 * 1024 * 1024,
                    config::FtsWarmConfig::Off,
                    60,
                )
                .expect("valid FTS hybrid cache"),
            ),
        },
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reopened_hybrid_readers_reuse_one_set_of_cache_files() {
    if !Path::new("/proc/self/fd").is_dir() {
        eprintln!("skipping: /proc/self/fd is unavailable on this platform");
        return;
    }
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = HelixDB::open_with_object_store(DATABASE, Arc::clone(&object_store))
        .await
        .expect("writer bootstraps the database");
    writer.close().await.expect("writer closes");
    drop(writer);

    let cache_root = tempfile::tempdir().expect("temporary cache root");
    let root = cache_root
        .path()
        .canonicalize()
        .expect("cache root resolves");
    let slate_root = root.join("slate");
    let object_store_root = root.join("object-store");
    let partitions = hybrid_slate_cache(&root).disk_partitions();
    // Descriptors after the first close; later closes must not leave more.
    let mut settled: Option<usize> = None;
    for reopen in 0..REOPENS {
        let reader = HelixDB::open_reader_with_object_store_and_config(
            DATABASE,
            Arc::clone(&object_store),
            hybrid_config(&root),
        )
        .await
        .expect("hybrid reader opens");
        let held = open_files_in(&slate_root);
        assert_eq!(
            held, partitions,
            "reopen {reopen}: {held} cache files are open, not one device's {partitions}"
        );
        reader.close().await.expect("hybrid reader closes");
        drop(reader);

        let started = Instant::now();
        let mut open = files_closed_readers_must_release(&object_store_root);
        while settled.is_some_and(|settled| open > settled) && started.elapsed() < RELEASE_DEADLINE
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
            open = files_closed_readers_must_release(&object_store_root);
        }
        let held = open_files_in(&slate_root);
        assert_eq!(
            held, partitions,
            "reopen {reopen}: the shared cache holds {held} cache files after a close, not {partitions}"
        );
        let first = *settled.get_or_insert(open);
        assert!(
            open <= first,
            "reopen {reopen}: {open} descriptors are open {:?} after closing a reader, \
             {first} after the first close; reopens leak descriptors",
            started.elapsed(),
        );
    }
}
