//! Closed hybrid-cache readers must release their disk-cache files.
//!
//! A hybrid SlateDB cache holds one open file per Foyer disk block while it
//! runs. A process that reopens readers must return to its baseline descriptor
//! count after each close, or it exhausts `nofile` after a handful of reopens.
//! Descriptors are counted through `/proc/self/fd`, so the test skips on
//! platforms without procfs. It is its own target so no concurrent test opens
//! or closes descriptors while it counts.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use db::HelixDB;
use db::config;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;

const DATABASE: &str = "hybrid-cache-fd-release";
const REOPENS: usize = 8;
/// Time allowed for dropped handles' spawned cleanup to finish.
const RELEASE_DEADLINE: Duration = Duration::from_secs(10);

fn open_files() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("procfs lists this process's descriptors")
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
async fn closed_hybrid_readers_release_their_cache_files() {
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
    let partitions = hybrid_slate_cache(cache_root.path()).disk_partitions();
    let baseline = open_files();
    for reopen in 0..REOPENS {
        let reader = HelixDB::open_reader_with_object_store_and_config(
            DATABASE,
            Arc::clone(&object_store),
            hybrid_config(cache_root.path()),
        )
        .await
        .expect("hybrid reader opens");
        let open = open_files();
        assert!(
            open >= baseline + partitions,
            "reopen {reopen}: an open reader holds its {partitions} cache files, \
             but only {open} descriptors are open over a baseline of {baseline}",
        );
        reader.close().await.expect("hybrid reader closes");
        drop(reader);

        let started = Instant::now();
        let mut open = open_files();
        while open >= baseline + partitions && started.elapsed() < RELEASE_DEADLINE {
            tokio::time::sleep(Duration::from_millis(10)).await;
            open = open_files();
        }
        assert!(
            open < baseline + partitions,
            "reopen {reopen}: {open} descriptors are still open {:?} after closing a \
             reader over a baseline of {baseline}; its {partitions} cache files leaked",
            started.elapsed(),
        );
    }
}
