//! Read-only diagnosis of how this server is set up, served at
//! `GET /v2/diagnostics`.
//!
//! [`report`] runs every check that applies to the configured storage, in a
//! fixed order: storage, cache, resources, indexes, then queries. Each
//! [`Check`] passes, warns, fails, or is skipped with its reason; a warning
//! or failure carries a fix. Checks never change configuration, data, or
//! caches. The only probes that leave the process are described in
//! [`aws`]; the rest read the filesystem, `/proc`, `/sys`, and the
//! database's in-memory counters. Facts that take I/O are reused for
//! [`FACTS_TTL`] ([`FactCache`]), so a client that polls the endpoint
//! repeats neither the outbound probes nor the walk of the disk cache.
//!
//! The JSON shape is the contract `helix doctor` renders:
//!
//! ```json
//! {
//!   "uptime_secs": 3600,
//!   "checks": [{
//!     "id": "cache.device",
//!     "category": "cache",
//!     "status": "warn",
//!     "summary": "The disk cache is on network block storage",
//!     "detail": "/var/cache/helix is on nvme1n1 (Amazon Elastic Block Store) ...",
//!     "fix": "Run on an instance with local NVMe ..."
//!   }]
//! }
//! ```
//!
//! `status` is `pass`, `warn`, `fail`, or `skip`; only `warn` and `fail`
//! have a `fix`. IDs and categories are stable; summaries, details and fixes
//! are prose and may change. A fix may contain `<instance>`, which a client
//! that knows the instance's name substitutes.

mod aws;
mod backing;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use db::query_service::insight_tally::{InsightSnapshot, TalliedInsight};
use helix_planner::catalog::ElementKind;
use helix_planner::diagnostics::SecondaryIndexKind;
use serde::Serialize;

use crate::config::{HybridCache, StorageConfig};
use crate::state::ServerState;

use backing::{Backing, Disk};

/// How long facts gathered with I/O are reused.
const FACTS_TTL: Duration = Duration::from_secs(30);
/// Median S3 round trip above which a bucket is reported as far away.
const S3_LATENCY_WARNING: Duration = Duration::from_millis(50);
/// Share of the memory limit cache budgets may take before the rest is too
/// little for queries, writes, and index builds.
const CACHE_MEMORY_PERCENT_LIMIT: u64 = 70;
/// Upper estimate of the RAM the block cache's disk-tier index takes per
/// byte of `HELIX_DISK_CACHE_BYTES`: 9 MiB per GiB.
const CACHE_INDEX_BYTES_PER_KIB: u64 = 9;
/// Age of the oldest queued index operation above which publication is
/// reported as behind.
const PUBLICATION_LAG_WARNING: Duration = Duration::from_secs(60);
/// Most insights one query check lists.
const LISTED_INSIGHTS: usize = 10;
/// Docs page on index operations the server holds back.
const INDEX_TROUBLESHOOTING: &str =
    "https://docs.helix-db.com/database/helix-db/query-guides/troubleshooting";

/// The diagnostics response.
#[derive(Debug, Serialize)]
pub(crate) struct Report {
    /// Seconds since the server opened its database.
    uptime_secs: u64,
    /// Every check that applies to this server, in report order.
    checks: Vec<Check>,
}

/// One diagnosed aspect of the setup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Check {
    id: CheckId,
    category: Category,
    summary: String,
    #[serde(flatten)]
    outcome: Outcome,
}

impl Check {
    fn new(id: CheckId, summary: impl Into<String>, outcome: Outcome) -> Self {
        Self {
            id,
            category: id.category(),
            summary: summary.into(),
            outcome,
        }
    }
}

/// A check's verdict. Only warnings and failures carry a fix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum Outcome {
    /// The setup is as recommended.
    Pass { detail: String },
    /// The setup works but costs performance or money.
    Warn { detail: String, fix: String },
    /// The setup risks losing data.
    Fail { detail: String, fix: String },
    /// The check could not run; `detail` says why.
    Skip { detail: String },
}

/// Stable check identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub(crate) enum CheckId {
    #[serde(rename = "storage.durability")]
    StorageDurability,
    #[serde(rename = "storage.data_device")]
    DataDevice,
    #[serde(rename = "storage.s3_region")]
    S3Region,
    #[serde(rename = "storage.s3_latency")]
    S3Latency,
    #[serde(rename = "cache.device")]
    CacheDevice,
    #[serde(rename = "cache.space")]
    CacheSpace,
    #[serde(rename = "cache.warm")]
    CacheWarm,
    #[serde(rename = "resources.memory")]
    Memory,
    #[serde(rename = "indexes.publication")]
    IndexPublication,
    #[serde(rename = "queries.missing_indexes")]
    MissingIndexes,
    #[serde(rename = "queries.unbounded_scans")]
    UnboundedScans,
}

impl CheckId {
    const fn category(self) -> Category {
        match self {
            Self::StorageDurability | Self::DataDevice | Self::S3Region | Self::S3Latency => {
                Category::Storage
            }
            Self::CacheDevice | Self::CacheSpace | Self::CacheWarm => Category::Cache,
            Self::Memory => Category::Resources,
            Self::IndexPublication => Category::Indexes,
            Self::MissingIndexes | Self::UnboundedScans => Category::Queries,
        }
    }
}

/// Groups checks are reported under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Category {
    Storage,
    Cache,
    Resources,
    Indexes,
    Queries,
}

/// Where the S3 bucket and this server run, or why that was not looked up.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RegionLookup {
    /// The bucket is behind a custom S3-compatible endpoint.
    CustomEndpoint(String),
    /// The bucket's region from AWS, and this server's from its metadata.
    Looked {
        bucket: Result<String, String>,
        server: Option<String>,
    },
}

/// Facts about the storage that take I/O to gather.
#[derive(Debug, Default)]
pub(crate) struct Facts {
    /// `HELIX_DATA_DIR` and what stores it, for local disk storage.
    data: Option<(PathBuf, Backing)>,
    /// What stores the disk cache and by how much its budget overruns the
    /// filesystem, when there is a disk cache.
    cache: Option<(Backing, Result<Option<u64>, String>)>,
    /// The bucket, its region against this server's, and its round trip,
    /// for S3 storage.
    s3: Option<(String, RegionLookup, aws::Latency)>,
}

/// [`Facts`] shared by every request for [`FACTS_TTL`] after they are
/// gathered. One request gathers while concurrent ones wait for its result.
#[derive(Debug, Default)]
pub(crate) struct FactCache(tokio::sync::Mutex<Option<(tokio::time::Instant, Arc<Facts>)>>);

impl FactCache {
    /// The cached facts, or those `gather` yields once they are older than
    /// [`FACTS_TTL`].
    async fn get(&self, gather: impl std::future::Future<Output = Facts>) -> Arc<Facts> {
        let mut cached = self.0.lock().await;
        let fresh = cached
            .as_ref()
            .filter(|(gathered, _)| gathered.elapsed() < FACTS_TTL)
            .map(|(_, facts)| Arc::clone(facts));
        let Some(facts) = fresh else {
            let facts = Arc::new(gather.await);
            *cached = Some((tokio::time::Instant::now(), Arc::clone(&facts)));
            return facts;
        };
        facts
    }
}

/// Gathers the [`Facts`] for `storage`: filesystem facts on a blocking
/// thread, network probes concurrently, each bounded by its own timeout.
async fn gather(storage: &StorageConfig, db: &db::HelixDB) -> Facts {
    let blocking_storage = storage.clone();
    let filesystem = tokio::task::spawn_blocking(move || {
        let data = match &blocking_storage {
            StorageConfig::Disk { root, .. } => Some((root.clone(), backing::classify(root))),
            StorageConfig::Memory | StorageConfig::S3 { .. } => None,
        };
        let cache = blocking_storage.hybrid_cache().map(|cache| {
            #[cfg(unix)]
            let shortfall = cache.disk_shortfall().map_err(|error| error.to_string());
            #[cfg(not(unix))]
            let shortfall = Err("measuring free space needs Unix".to_owned());
            (backing::classify(cache.root()), shortfall)
        });
        (data, cache)
    });
    let s3 = async {
        let StorageConfig::S3 {
            bucket, endpoint, ..
        } = storage
        else {
            return None;
        };
        let probe = object_store::path::Path::from(format!(
            "{}/.helix-diagnostics-probe",
            db.path().trim_end_matches('/')
        ));
        let regions = async {
            let Some(endpoint) = endpoint else {
                let (bucket, server) = tokio::join!(
                    aws::bucket_region(bucket),
                    aws::server_region(|name| std::env::var(name).ok())
                );
                return RegionLookup::Looked { bucket, server };
            };
            RegionLookup::CustomEndpoint(endpoint.clone())
        };
        let (regions, latency) = tokio::join!(
            regions,
            aws::probe_latency(db.object_store().as_ref(), &probe)
        );
        Some((bucket.clone(), regions, latency))
    };
    let (filesystem, s3) = tokio::join!(filesystem, s3);
    // The filesystem checks report every failure as a skip, so the task
    // only fails if it panics; its checks are then left out.
    let (data, cache) = filesystem.unwrap_or_default();
    Facts { data, cache, s3 }
}

/// Runs every check that applies to `state`'s storage.
pub(crate) async fn report(state: &ServerState) -> Report {
    let storage = state.storage();
    let db = state.db();
    let facts = state.diagnostic_facts().get(gather(storage, db)).await;
    let cache = storage.hybrid_cache().zip(facts.cache.as_ref());
    let cache_stats = db.cache_stats();
    let insights = state.query_service().planner_insights();
    let checks = [
        Some(durability(storage, db.path())),
        facts
            .data
            .as_ref()
            .map(|(root, backing)| data_device(root, backing)),
        facts
            .s3
            .as_ref()
            .map(|(bucket, regions, _)| s3_region(bucket, regions)),
        facts.s3.as_ref().map(|(_, _, latency)| s3_latency(latency)),
        cache.map(|(cache, (backing, _))| cache_device(cache.root(), backing)),
        cache.map(|(cache, (_, shortfall))| cache_space(cache, shortfall, &cache_stats)),
        match storage {
            StorageConfig::S3 { cache, .. } => Some(cache_warm(cache)),
            StorageConfig::Memory | StorageConfig::Disk { .. } => None,
        },
        Some(memory(
            &cache_stats,
            storage.hybrid_cache().map(HybridCache::disk_bytes),
            db::config::memory_ceiling_bytes(),
        )),
        db.is_writer_mode()
            .then(|| index_publication(&db.index_operation_queue_stats())),
        Some(missing_indexes(&insights)),
        Some(unbounded_scans(&insights)),
    ]
    .into_iter()
    .flatten()
    .collect();
    Report {
        uptime_secs: state.uptime().as_secs(),
        checks,
    }
}

fn durability(storage: &StorageConfig, db_path: &str) -> Check {
    match storage {
        StorageConfig::Memory => Check::new(
            CheckId::StorageDurability,
            "Data is kept in memory only",
            Outcome::Warn {
                detail: "In-memory storage loses every write when the server stops.".into(),
                fix: "Set S3_BUCKET (with a local disk cache) or HELIX_DATA_DIR for durable \
                      storage. A local CLI instance persists with `helix start <instance> --disk`."
                    .into(),
            },
        ),
        StorageConfig::Disk { root, .. } => Check::new(
            CheckId::StorageDurability,
            "Data is stored on local disk",
            Outcome::Pass {
                detail: format!("HELIX_DATA_DIR is {}.", root.display()),
            },
        ),
        StorageConfig::S3 {
            bucket, endpoint, ..
        } => Check::new(
            CheckId::StorageDurability,
            "Data is stored in S3",
            Outcome::Pass {
                detail: match endpoint {
                    Some(endpoint) => format!("s3://{bucket}/{db_path} through {endpoint}."),
                    None => format!("s3://{bucket}/{db_path}."),
                },
            },
        ),
    }
}

fn data_device(root: &Path, backing: &Backing) -> Check {
    let root = root.display();
    match backing {
        Backing::Memory { filesystem } => Check::new(
            CheckId::DataDevice,
            "HELIX_DATA_DIR is on a RAM-backed filesystem",
            Outcome::Fail {
                detail: format!("{root} is on {filesystem}: its data is lost when the host restarts."),
                fix: "Point HELIX_DATA_DIR at a persistent volume, or use S3 storage.".into(),
            },
        ),
        Backing::ContainerLayer { filesystem } => Check::new(
            CheckId::DataDevice,
            "HELIX_DATA_DIR is inside the container's writable layer",
            Outcome::Fail {
                detail: format!(
                    "{root} is on {filesystem}: its data is deleted with the container, for \
                     example when the image is upgraded."
                ),
                fix: format!("Mount a persistent volume at {root}, e.g. `docker run -v helix-data:{root} ...`."),
            },
        ),
        Backing::Remote { filesystem } => Check::new(
            CheckId::DataDevice,
            "HELIX_DATA_DIR is on a network filesystem",
            Outcome::Warn {
                detail: format!(
                    "{root} is on {filesystem}: every read and write crosses the network, and \
                     some network filesystems do not support the locks the server takes."
                ),
                fix: "Use a local or block-storage volume, or S3 storage with a disk cache on \
                      local NVMe."
                    .into(),
            },
        ),
        Backing::Block {
            device,
            disk: Disk::InstanceStore { model },
        } => Check::new(
            CheckId::DataDevice,
            "HELIX_DATA_DIR is on ephemeral instance storage",
            Outcome::Fail {
                detail: format!(
                    "{root} is on {device} ({model}): its data is lost when the instance stops \
                     or is replaced."
                ),
                fix: "Keep data on a network volume (EBS, Persistent Disk, a managed disk) or in \
                      S3. Instance storage suits the disk cache (HELIX_DISK_CACHE_DIR)."
                    .into(),
            },
        ),
        Backing::Block {
            device,
            disk: Disk::Rotational,
        } => Check::new(
            CheckId::DataDevice,
            "HELIX_DATA_DIR is on a spinning disk",
            Outcome::Warn {
                detail: format!("{root} is on {device}, a rotational disk: random reads are slow."),
                fix: "Move HELIX_DATA_DIR to an SSD.".into(),
            },
        ),
        Backing::Block {
            device,
            disk: Disk::Network { model },
        } => Check::new(
            CheckId::DataDevice,
            "HELIX_DATA_DIR is on network block storage",
            Outcome::Pass {
                detail: format!("{root} is on {device} ({model})."),
            },
        ),
        Backing::Block {
            device,
            disk: Disk::LocalSsd { model },
        } => Check::new(
            CheckId::DataDevice,
            "HELIX_DATA_DIR is on a local SSD",
            Outcome::Pass {
                detail: format!("{root} is on {device} ({model})."),
            },
        ),
        Backing::Unknown { reason } => Check::new(
            CheckId::DataDevice,
            "Could not tell what stores HELIX_DATA_DIR",
            Outcome::Skip {
                detail: reason.clone(),
            },
        ),
    }
}

fn s3_region(bucket: &str, regions: &RegionLookup) -> Check {
    let summary = "S3 bucket is in this server's region";
    match regions {
        RegionLookup::CustomEndpoint(endpoint) => Check::new(
            CheckId::S3Region,
            summary,
            Outcome::Skip {
                detail: format!(
                    "The bucket is behind the S3-compatible endpoint {endpoint}; regions are \
                     checked for AWS S3 only. The round-trip check still applies."
                ),
            },
        ),
        RegionLookup::Looked {
            bucket: Err(error), ..
        } => Check::new(
            CheckId::S3Region,
            summary,
            Outcome::Skip {
                detail: format!("Could not look up the region of bucket {bucket}: {error}"),
            },
        ),
        RegionLookup::Looked {
            bucket: Ok(bucket_region),
            server: None,
        } => Check::new(
            CheckId::S3Region,
            summary,
            Outcome::Skip {
                detail: format!(
                    "Bucket {bucket} is in {bucket_region}, but this server's region is unknown: \
                     neither EC2 instance metadata nor ECS task metadata answered, so compare \
                     them yourself. A container on EC2 reaches instance metadata only with an \
                     IMDSv2 hop limit of 2; this check alone is no reason to raise it."
                ),
            },
        ),
        RegionLookup::Looked {
            bucket: Ok(bucket_region),
            server: Some(server_region),
        } if bucket_region == server_region => Check::new(
            CheckId::S3Region,
            summary,
            Outcome::Pass {
                detail: format!("Bucket {bucket} and this server are both in {bucket_region}."),
            },
        ),
        RegionLookup::Looked {
            bucket: Ok(bucket_region),
            server: Some(server_region),
        } => Check::new(
            CheckId::S3Region,
            "S3 bucket is in another region",
            Outcome::Warn {
                detail: format!(
                    "Bucket {bucket} is in {bucket_region}, but this server runs in \
                     {server_region}. Every cache miss crosses regions, which adds tens of \
                     milliseconds and inter-region transfer charges."
                ),
                fix: format!(
                    "Run the server in {bucket_region}, or move the database to a bucket in \
                     {server_region}."
                ),
            },
        ),
    }
}

fn s3_latency(latency: &aws::Latency) -> Check {
    match latency {
        aws::Latency::Measured(median) if *median <= S3_LATENCY_WARNING => Check::new(
            CheckId::S3Latency,
            format!("S3 round trips take {}", elapsed(*median)),
            Outcome::Pass {
                detail: format!(
                    "Median of {} HEAD requests from this server.",
                    aws::LATENCY_SAMPLES
                ),
            },
        ),
        aws::Latency::Measured(median) => Check::new(
            CheckId::S3Latency,
            format!("S3 round trips take {}", elapsed(*median)),
            Outcome::Warn {
                detail: format!(
                    "Median of {} HEAD requests from this server; in the bucket's region they \
                     usually take under 30 ms. Each cache miss pays this at least once.",
                    aws::LATENCY_SAMPLES
                ),
                fix: "Run the server in the bucket's region, and on AWS route S3 through a VPC \
                      gateway endpoint rather than a NAT gateway."
                    .into(),
            },
        ),
        aws::Latency::TimedOut => Check::new(
            CheckId::S3Latency,
            "S3 requests time out",
            Outcome::Warn {
                detail: format!(
                    "A HEAD request took longer than {}.",
                    elapsed(aws::LATENCY_TIMEOUT)
                ),
                fix: "Check the network path from this server to S3: its region, proxy, and \
                      NAT or VPC endpoint."
                    .into(),
            },
        ),
        aws::Latency::Failed(error) => Check::new(
            CheckId::S3Latency,
            "Could not time S3 round trips",
            Outcome::Skip {
                detail: format!("A HEAD request failed: {error}"),
            },
        ),
    }
}

fn cache_device(root: &Path, backing: &Backing) -> Check {
    let root = root.display();
    let nvme_fix = format!(
        "Run on an instance with local NVMe (for example the i4i, i8g, m7gd or c7gd families) \
         and mount it at {root}, or point HELIX_DISK_CACHE_DIR at it."
    );
    match backing {
        Backing::Memory { filesystem } => Check::new(
            CheckId::CacheDevice,
            "The disk cache is on a RAM-backed filesystem",
            Outcome::Warn {
                detail: format!(
                    "{root} is on {filesystem}: the cache takes memory from queries and is empty \
                     after every restart."
                ),
                fix: nvme_fix,
            },
        ),
        Backing::ContainerLayer { filesystem } => Check::new(
            CheckId::CacheDevice,
            "The disk cache is inside the container's writable layer",
            Outcome::Warn {
                detail: format!(
                    "{root} is on {filesystem}: the cache is lost whenever the container is \
                     recreated, and writes to it are slower."
                ),
                fix: format!("Mount a volume backed by local NVMe at {root}."),
            },
        ),
        Backing::Remote { filesystem } => Check::new(
            CheckId::CacheDevice,
            "The disk cache is on a network filesystem",
            Outcome::Warn {
                detail: format!(
                    "{root} is on {filesystem}: cache reads cross the network, and its lock may \
                     not hold."
                ),
                fix: nvme_fix,
            },
        ),
        Backing::Block {
            device,
            disk: Disk::InstanceStore { model },
        } => Check::new(
            CheckId::CacheDevice,
            "The disk cache is on local NVMe instance storage",
            Outcome::Pass {
                detail: format!("{root} is on {device} ({model})."),
            },
        ),
        Backing::Block {
            device,
            disk: Disk::LocalSsd { model },
        } => Check::new(
            CheckId::CacheDevice,
            "The disk cache is on a local SSD",
            Outcome::Pass {
                detail: format!("{root} is on {device} ({model})."),
            },
        ),
        Backing::Block {
            device,
            disk: Disk::Network { model },
        } => Check::new(
            CheckId::CacheDevice,
            "The disk cache is on network block storage",
            Outcome::Warn {
                detail: format!(
                    "{root} is on {device} ({model}). Each cache miss pays network-disk latency \
                     and draws on the volume's IOPS limit; local NVMe answers several times \
                     faster."
                ),
                fix: nvme_fix,
            },
        ),
        Backing::Block {
            device,
            disk: Disk::Rotational,
        } => Check::new(
            CheckId::CacheDevice,
            "The disk cache is on a spinning disk",
            Outcome::Warn {
                detail: format!("{root} is on {device}, a rotational disk: random reads are slow."),
                fix: nvme_fix,
            },
        ),
        Backing::Unknown { reason } => Check::new(
            CheckId::CacheDevice,
            "Could not tell what stores the disk cache",
            Outcome::Skip {
                detail: reason.clone(),
            },
        ),
    }
}

fn cache_space(
    cache: &HybridCache,
    shortfall: &Result<Option<u64>, String>,
    stats: &db::DatabaseCacheStats,
) -> Check {
    let budget = cache.disk_bytes() as u64;
    let used = [
        stats.slate_object_store_disk,
        stats.foyer_hybrid_disk,
        stats.fts_disk,
    ]
    .into_iter()
    .filter_map(|tier| match tier.state {
        db::CacheTierState::Ready { used_bytes, .. } => Some(used_bytes),
        db::CacheTierState::Disabled
        | db::CacheTierState::Initializing { .. }
        | db::CacheTierState::Unavailable => None,
    })
    .sum::<u64>();
    match shortfall {
        Ok(None) => Check::new(
            CheckId::CacheSpace,
            "The disk cache budget fits its filesystem",
            Outcome::Pass {
                detail: format!(
                    "HELIX_DISK_CACHE_BYTES is {}; {} is cached.",
                    bytes(budget),
                    bytes(used)
                ),
            },
        ),
        Ok(Some(shortfall)) => Check::new(
            CheckId::CacheSpace,
            "The disk cache budget exceeds its free space",
            Outcome::Warn {
                detail: format!(
                    "HELIX_DISK_CACHE_BYTES is {}, {} more than {} has room for. A full cache \
                     can fill the filesystem, and with HELIX_DATA_DIR on it, fail writes.",
                    bytes(budget),
                    bytes(*shortfall),
                    cache.root().display()
                ),
                fix: format!(
                    "Lower HELIX_DISK_CACHE_BYTES to at most {}, or mount a larger volume at {}.",
                    bytes(budget.saturating_sub(*shortfall)),
                    cache.root().display()
                ),
            },
        ),
        Err(error) => Check::new(
            CheckId::CacheSpace,
            "Could not measure the disk cache's free space",
            Outcome::Skip {
                detail: error.clone(),
            },
        ),
    }
}

fn cache_warm(cache: &HybridCache) -> Check {
    if cache.warms_at_startup() {
        return Check::new(
            CheckId::CacheWarm,
            "The disk cache warms at startup",
            Outcome::Pass {
                detail: "Vector search rows and SST metadata load from S3 in the background \
                         after a restart."
                    .into(),
            },
        );
    }
    Check::new(
        CheckId::CacheWarm,
        "The disk cache does not warm at startup",
        Outcome::Warn {
            detail: "HELIX_DISK_CACHE_WARM=off: after a restart, searches read their index from \
                     S3 until queries fill the cache."
                .into(),
            fix: "Unset HELIX_DISK_CACHE_WARM; it defaults to on.".into(),
        },
    )
}

fn memory(
    stats: &db::DatabaseCacheStats,
    hybrid_disk_bytes: Option<usize>,
    ceiling: Option<u64>,
) -> Check {
    let Some(ceiling) = ceiling else {
        return Check::new(
            CheckId::Memory,
            "Could not read the memory limit",
            Outcome::Skip {
                detail: "Neither a cgroup limit nor the physical memory size is readable.".into(),
            },
        );
    };
    let budgets = [stats.slate_memory, stats.fts_memory, stats.vector_memory]
        .into_iter()
        .filter_map(|tier| match tier.state {
            db::CacheTierState::Ready { capacity_bytes, .. }
            | db::CacheTierState::Initializing { capacity_bytes } => capacity_bytes,
            db::CacheTierState::Disabled | db::CacheTierState::Unavailable => None,
        })
        .sum::<u64>()
        + hybrid_disk_bytes.map_or(0, |disk| disk as u64 / 1024 * CACHE_INDEX_BYTES_PER_KIB);
    let detail = format!(
        "Cache budgets total {} of the {} this process may use",
        bytes(budgets),
        bytes(ceiling)
    );
    if budgets.saturating_mul(100) <= ceiling.saturating_mul(CACHE_MEMORY_PERCENT_LIMIT) {
        return Check::new(
            CheckId::Memory,
            "Memory leaves room beyond the caches",
            Outcome::Pass {
                detail: format!("{detail}."),
            },
        );
    }
    Check::new(
        CheckId::Memory,
        "Caches can take most of the memory limit",
        Outcome::Warn {
            detail: format!(
                "{detail}, leaving {} for queries, writes and index builds; under load the \
                 server can be killed for running out of memory.",
                bytes(ceiling.saturating_sub(budgets))
            ),
            fix: format!(
                "Give the server at least {} of memory, or lower HELIX_DISK_CACHE_MEMORY_BYTES \
                 or HELIX_DISK_CACHE_BYTES.",
                bytes(budgets.saturating_mul(100) / CACHE_MEMORY_PERCENT_LIMIT)
            ),
        },
    )
}

fn index_publication(stats: &db::IndexOperationQueueStats) -> Check {
    let oldest = Duration::from_micros(stats.oldest_pending_micros);
    if stats.blocked_entities > 0 {
        return Check::new(
            CheckId::IndexPublication,
            format!(
                "{} {} held back from vector or text indexes",
                stats.blocked_entities,
                plural(stats.blocked_entities, "entry is", "entries are")
            ),
            Outcome::Warn {
                detail: "Searches leave out the newest writes to each one until it publishes: \
                         one of its operations is too large for the current queue limits, or \
                         applying it fails every time."
                    .into(),
                fix: format!(
                    "The server log names each held-back entry and why. Raise lowered limits \
                     again, and stop writing an entry whose update keeps failing until it is \
                     fixed; see {INDEX_TROUBLESHOOTING}."
                ),
            },
        );
    }
    if oldest > PUBLICATION_LAG_WARNING {
        return Check::new(
            CheckId::IndexPublication,
            "Vector and text index publication is behind",
            Outcome::Warn {
                detail: format!(
                    "{} {} queued; the oldest has waited {}. Eventual searches miss writes \
                     that recent.",
                    stats.pending_operations,
                    plural(stats.pending_operations, "operation is", "operations are"),
                    elapsed(oldest)
                ),
                fix: "Give the server more CPU or lower the write rate: publication runs in the \
                      background and catches up once it outpaces writes."
                    .into(),
            },
        );
    }
    Check::new(
        CheckId::IndexPublication,
        "Vector and text indexes are caught up",
        Outcome::Pass {
            detail: match stats.pending_operations {
                0 => "No index operations are queued.".into(),
                pending => format!(
                    "{pending} {} queued; the oldest has waited {}.",
                    plural(pending, "operation is", "operations are"),
                    elapsed(oldest)
                ),
            },
        },
    )
}

fn missing_indexes(snapshot: &InsightSnapshot) -> Check {
    let missing = snapshot
        .insights
        .iter()
        .filter_map(|count| match &count.insight {
            TalliedInsight::MissingIndex {
                element,
                label,
                property,
                index_kind,
            } => Some((count, *element, label, property, *index_kind)),
            TalliedInsight::UnboundedScan { .. } => None,
        })
        .collect::<Vec<_>>();
    if snapshot.analyzed_queries == 0 {
        return Check::new(
            CheckId::MissingIndexes,
            "Queries use indexes",
            Outcome::Skip {
                detail: "No queries have run since the server started.".into(),
            },
        );
    }
    if missing.is_empty() {
        return Check::new(
            CheckId::MissingIndexes,
            "Queries use indexes",
            Outcome::Pass {
                detail: format!(
                    "No missing index in {} recent {}.",
                    snapshot.analyzed_queries,
                    plural(snapshot.analyzed_queries, "query", "queries")
                ),
            },
        );
    }
    let listed = missing.iter().take(LISTED_INSIGHTS);
    let detail = listed
        .clone()
        .map(|(count, element, label, property, kind)| {
            format!(
                "{label}.{property} ({element} {kind}): {} {}, last seen {} ago",
                count.queries,
                plural(count.queries, "query", "queries"),
                elapsed(count.last_seen_ago)
            )
        })
        .chain(more(missing.len()))
        .chain(evicted(snapshot.evicted_insights))
        .collect::<Vec<_>>()
        .join("\n");
    let fix = std::iter::once(
        "Create each index; it builds in the background and applies once active:".to_owned(),
    )
    .chain(listed.map(|(_, element, label, property, kind)| {
        let spec = match (element, kind) {
            (ElementKind::Node, SecondaryIndexKind::Equality) => "nodeEquality",
            (ElementKind::Node, SecondaryIndexKind::Range) => "nodeRange",
            (ElementKind::Edge, SecondaryIndexKind::Equality) => "edgeEquality",
            (ElementKind::Edge, SecondaryIndexKind::Range) => "edgeRange",
        };
        let expression = format!(
            r#"writeBatch().varAs("index", g().createIndexIfNotExists(IndexSpec.{spec}({}, {}))).returning(["index"])"#,
            json_string(label.as_ref()),
            json_string(property.as_ref())
        );
        format!(
            "helix query <instance> -e '{}'",
            expression.replace('\'', r"'\''")
        )
    }))
    .collect::<Vec<_>>()
    .join("\n");
    Check::new(
        CheckId::MissingIndexes,
        format!(
            "{} {} without an index",
            missing.len(),
            plural(missing.len() as u64, "filter scans", "filters scan")
        ),
        Outcome::Warn { detail, fix },
    )
}

fn unbounded_scans(snapshot: &InsightSnapshot) -> Check {
    let scans = snapshot
        .insights
        .iter()
        .filter_map(|count| match &count.insight {
            TalliedInsight::UnboundedScan {
                element,
                label,
                predicate_properties,
            } => Some((count, *element, label, predicate_properties)),
            TalliedInsight::MissingIndex { .. } => None,
        })
        .collect::<Vec<_>>();
    if snapshot.analyzed_queries == 0 {
        return Check::new(
            CheckId::UnboundedScans,
            "Queries read only what they need",
            Outcome::Skip {
                detail: "No queries have run since the server started.".into(),
            },
        );
    }
    if scans.is_empty() {
        return Check::new(
            CheckId::UnboundedScans,
            "Queries read only what they need",
            Outcome::Pass {
                detail: format!(
                    "No unbounded scan in {} recent {}.",
                    snapshot.analyzed_queries,
                    plural(snapshot.analyzed_queries, "query", "queries")
                ),
            },
        );
    }
    let detail = scans
        .iter()
        .take(LISTED_INSIGHTS)
        .map(|(count, element, label, properties)| {
            let target = match label {
                Some(label) => format!("every {label} {element}"),
                None => format!("every {element}"),
            };
            let filter = if properties.is_empty() {
                String::new()
            } else {
                format!(
                    ", filtering on {}",
                    properties
                        .iter()
                        .map(AsRef::as_ref)
                        .collect::<Vec<&str>>()
                        .join(", ")
                )
            };
            format!(
                "{target}{filter}: {} {}, last seen {} ago",
                count.queries,
                plural(count.queries, "query", "queries"),
                elapsed(count.last_seen_ago)
            )
        })
        .chain(more(scans.len()))
        .chain(evicted(snapshot.evicted_insights))
        .collect::<Vec<_>>()
        .join("\n");
    Check::new(
        CheckId::UnboundedScans,
        format!(
            "{} query {} every element of a label",
            scans.len(),
            plural(scans.len() as u64, "shape reads", "shapes read")
        ),
        Outcome::Warn {
            detail,
            fix: "Start these queries from an indexed property or an ID, or bound them with a \
                  limit: each run reads every element it scans."
                .into(),
        },
    )
}

/// "… and N more" when more insights exist than are listed.
fn more(total: usize) -> Option<String> {
    (total > LISTED_INSIGHTS).then(|| format!("… and {} more", total - LISTED_INSIGHTS))
}

/// A note that the tally dropped insights while full.
fn evicted(evicted: u64) -> Option<String> {
    (evicted > 0).then(|| {
        format!(
            "({evicted} older {} dropped from the tally while it was full)",
            plural(evicted, "insight was", "insights were")
        )
    })
}

/// `one` for a count of one, else `many`.
const fn plural(count: u64, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 {
        one
    } else {
        many
    }
}

/// A string as a quoted JSON (and so JavaScript) literal.
fn json_string(value: &str) -> String {
    serde_json::Value::from(value).to_string()
}

/// A byte count in binary units with one decimal, e.g. `8.0 GiB`.
fn bytes(value: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    let (scaled, unit) = UNITS
        .iter()
        .fold((value as f64, "B"), |(scaled, unit), next| {
            if scaled >= 1024.0 {
                (scaled / 1024.0, *next)
            } else {
                (scaled, unit)
            }
        });
    match unit {
        "B" => format!("{value} B"),
        unit => format!("{scaled:.1} {unit}"),
    }
}

/// A duration at the coarsest whole unit, e.g. `45 s` or `3 h`.
fn elapsed(duration: Duration) -> String {
    match duration.as_secs() {
        0 => format!("{} ms", duration.as_millis()),
        seconds @ 1..60 => format!("{seconds} s"),
        seconds @ 60..3_600 => format!("{} min", seconds / 60),
        seconds @ 3_600..86_400 => format!("{} h", seconds / 3_600),
        seconds => format!("{} d", seconds / 86_400),
    }
}

#[cfg(test)]
mod tests;
