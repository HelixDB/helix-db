use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use db::query_service::insight_tally::InsightCount;
use db::{CacheTierSnapshot, CacheTierState, CacheUsageSemantics, DatabaseCacheStats};
use helix_ast::{batch, expr, graph, query, traversal};
use helix_planner::diagnostics::PredicatePropertySet;
use helix_planner::ir::NonEmptyString;
use tower::ServiceExt;

use super::*;
use crate::config::CacheConfig;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

fn status(check: &Check) -> &'static str {
    match check.outcome {
        Outcome::Pass { .. } => "pass",
        Outcome::Warn { .. } => "warn",
        Outcome::Fail { .. } => "fail",
        Outcome::Skip { .. } => "skip",
    }
}

fn detail(check: &Check) -> &str {
    match &check.outcome {
        Outcome::Pass { detail }
        | Outcome::Warn { detail, .. }
        | Outcome::Fail { detail, .. }
        | Outcome::Skip { detail } => detail,
    }
}

fn fix(check: &Check) -> Option<&str> {
    match &check.outcome {
        Outcome::Warn { fix, .. } | Outcome::Fail { fix, .. } => Some(fix),
        Outcome::Pass { .. } | Outcome::Skip { .. } => None,
    }
}

fn cache(directory: &Path, warm: bool) -> HybridCache {
    let cache = HybridCache::try_new(
        directory.join("cache"),
        NonZeroUsize::new(64 * MIB as usize).unwrap(),
        NonZeroUsize::new(GIB as usize).unwrap(),
    )
    .unwrap();
    if warm {
        cache
    } else {
        cache.without_startup_warm_for_tests()
    }
}

fn tier(state: CacheTierState) -> CacheTierSnapshot {
    CacheTierSnapshot {
        semantics: CacheUsageSemantics::PhysicalFiles,
        state,
    }
}

fn stats(memory: [CacheTierState; 3], disk: [CacheTierState; 3]) -> DatabaseCacheStats {
    DatabaseCacheStats {
        slate_memory: tier(memory[0]),
        fts_memory: tier(memory[1]),
        vector_memory: tier(memory[2]),
        slate_object_store_disk: tier(disk[0]),
        foyer_hybrid_disk: tier(disk[1]),
        fts_disk: tier(disk[2]),
    }
}

fn name(value: &str) -> NonEmptyString {
    NonEmptyString::new(value).unwrap()
}

fn count(insight: TalliedInsight, queries: u64, seconds: u64) -> InsightCount {
    InsightCount {
        insight,
        queries,
        last_seen_ago: Duration::from_secs(seconds),
    }
}

fn missing(
    label: &str,
    property: &str,
    element: ElementKind,
    kind: SecondaryIndexKind,
) -> TalliedInsight {
    TalliedInsight::MissingIndex {
        element,
        label: name(label),
        property: name(property),
        index_kind: kind,
    }
}

/// One NVMe instance-store disk.
fn instance_storage() -> Backing {
    Backing::Block {
        device: "nvme1n1".into(),
        disk: Disk::LocalSsd {
            model: "Amazon EC2 NVMe Instance Storage".into(),
        },
        ephemeral: Some("Amazon EC2 NVMe Instance Storage".into()),
    }
}

/// A RAID0 of EBS and instance storage: as slow as EBS, as ephemeral as
/// instance storage.
fn ebs_and_instance_storage() -> Backing {
    Backing::Block {
        device: "md1".into(),
        disk: Disk::Network {
            model: "Amazon Elastic Block Store".into(),
        },
        ephemeral: Some("Amazon EC2 NVMe Instance Storage".into()),
    }
}

fn scan(label: Option<&str>, properties: &[&str]) -> TalliedInsight {
    TalliedInsight::UnboundedScan {
        element: ElementKind::Node,
        label: label.map(name),
        predicate_properties: PredicatePropertySet::new(properties.iter().copied().map(name)),
    }
}

#[tokio::test(start_paused = true)]
async fn facts_are_gathered_once_per_ttl_even_for_concurrent_requests() {
    let cache = FactCache::default();
    let gathered = std::sync::atomic::AtomicUsize::new(0);
    let gather = || async {
        gathered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Long enough for the second request to arrive while gathering.
        tokio::time::sleep(Duration::from_millis(10)).await;
        Facts::default()
    };
    let (first, second) = tokio::join!(cache.get(gather()), cache.get(gather()));
    assert!(
        Arc::ptr_eq(&first, &second),
        "the waiting request reuses the facts"
    );
    assert_eq!(gathered.load(std::sync::atomic::Ordering::SeqCst), 1);

    tokio::time::advance(FACTS_TTL - Duration::from_millis(1)).await;
    assert!(Arc::ptr_eq(&cache.get(gather()).await, &first));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(!Arc::ptr_eq(&cache.get(gather()).await, &first));
    assert_eq!(gathered.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn checks_serialize_their_stable_id_category_and_status() {
    let warn = serde_json::to_value(Check::new(
        CheckId::CacheDevice,
        "summary",
        Outcome::Warn {
            detail: "detail".into(),
            fix: "fix".into(),
        },
    ))
    .unwrap();
    assert_eq!(
        warn,
        serde_json::json!({
            "id": "cache.device",
            "category": "cache",
            "status": "warn",
            "summary": "summary",
            "detail": "detail",
            "fix": "fix",
        })
    );
    let pass = serde_json::to_value(Check::new(
        CheckId::MissingIndexes,
        "summary",
        Outcome::Pass {
            detail: "detail".into(),
        },
    ))
    .unwrap();
    assert_eq!(pass["status"], "pass");
    assert_eq!(pass["category"], "queries");
    assert!(pass.get("fix").is_none());

    for (id, json, category) in [
        (CheckId::StorageDurability, "storage.durability", "storage"),
        (CheckId::DataDevice, "storage.data_device", "storage"),
        (CheckId::S3Region, "storage.s3_region", "storage"),
        (CheckId::S3Latency, "storage.s3_latency", "storage"),
        (CheckId::CacheDevice, "cache.device", "cache"),
        (CheckId::CacheSpace, "cache.space", "cache"),
        (CheckId::CacheWarm, "cache.warm", "cache"),
        (CheckId::Memory, "resources.memory", "resources"),
        (CheckId::IndexPublication, "indexes.publication", "indexes"),
        (
            CheckId::MissingIndexes,
            "queries.missing_indexes",
            "queries",
        ),
        (
            CheckId::UnboundedScans,
            "queries.unbounded_scans",
            "queries",
        ),
    ] {
        let check = serde_json::to_value(Check::new(
            id,
            "s",
            Outcome::Fail {
                detail: "d".into(),
                fix: "f".into(),
            },
        ))
        .unwrap();
        assert_eq!(check["id"], json);
        assert_eq!(check["category"], category);
        assert_eq!(check["status"], "fail");
    }
    let skip = serde_json::to_value(Check::new(
        CheckId::S3Region,
        "s",
        Outcome::Skip {
            detail: "why".into(),
        },
    ))
    .unwrap();
    assert_eq!(skip["status"], "skip");
    assert_eq!(skip["detail"], "why");
}

#[test]
fn durability_warns_only_for_memory_storage() {
    let directory = tempfile::tempdir().unwrap();
    let memory = durability(&StorageConfig::Memory, "db/");
    assert_eq!(status(&memory), "warn");
    assert!(fix(&memory).unwrap().contains("S3_BUCKET"));

    let disk = durability(
        &StorageConfig::Disk {
            root: PathBuf::from("/var/lib/helix"),
            cache: CacheConfig::Memory,
        },
        "db/",
    );
    assert_eq!(status(&disk), "pass");
    assert!(detail(&disk).contains("/var/lib/helix"));

    let s3 = |endpoint: Option<&str>| {
        durability(
            &StorageConfig::S3 {
                bucket: "launch".into(),
                region: "us-east-1".into(),
                endpoint: endpoint.map(str::to_owned),
                allow_http: false,
                cache: Box::new(cache(directory.path(), true)),
            },
            "tenant/",
        )
    };
    assert_eq!(detail(&s3(None)), "s3://launch/tenant/.");
    assert_eq!(
        detail(&s3(Some("http://seaweedfs:8333"))),
        "s3://launch/tenant/ through http://seaweedfs:8333."
    );
}

#[test]
fn data_on_ephemeral_or_container_storage_fails() {
    let root = Path::new("/var/lib/helix");
    let block = |disk| Backing::Block {
        device: "nvme1n1".into(),
        disk,
        ephemeral: None,
    };
    for (backing, expected, summary) in [
        (
            Backing::Memory {
                filesystem: "tmpfs".into(),
            },
            "fail",
            "HELIX_DATA_DIR is on a RAM-backed filesystem",
        ),
        (
            Backing::ContainerLayer {
                filesystem: "overlay".into(),
            },
            "fail",
            "HELIX_DATA_DIR is inside the container's writable layer",
        ),
        (
            Backing::Remote {
                filesystem: "nfs4".into(),
            },
            "warn",
            "HELIX_DATA_DIR is on a network filesystem",
        ),
        (
            instance_storage(),
            "fail",
            "HELIX_DATA_DIR is on ephemeral instance storage",
        ),
        (
            ebs_and_instance_storage(),
            "fail",
            "HELIX_DATA_DIR is on ephemeral instance storage",
        ),
        (
            block(Disk::Rotational),
            "warn",
            "HELIX_DATA_DIR is on a spinning disk",
        ),
        (
            block(Disk::Network {
                model: "Amazon Elastic Block Store".into(),
            }),
            "pass",
            "HELIX_DATA_DIR is on network block storage",
        ),
        (
            block(Disk::LocalSsd {
                model: "Samsung SSD".into(),
            }),
            "pass",
            "HELIX_DATA_DIR is on a local SSD",
        ),
        (
            Backing::Unknown {
                reason: "no sysfs".into(),
            },
            "skip",
            "Could not tell what stores HELIX_DATA_DIR",
        ),
    ] {
        let check = data_device(root, &backing);
        assert_eq!(status(&check), expected, "{backing:?}");
        assert_eq!(check.summary, summary);

        assert_eq!(check.id, CheckId::DataDevice);
        match &backing {
            Backing::Unknown { reason } => assert_eq!(detail(&check), reason),
            Backing::Memory { .. }
            | Backing::ContainerLayer { .. }
            | Backing::Remote { .. }
            | Backing::Block { .. } => assert!(detail(&check).contains("/var/lib/helix")),
        }
    }
    assert_eq!(
        detail(&data_device(root, &ebs_and_instance_storage())),
        "/var/lib/helix is on md1, which uses Amazon EC2 NVMe Instance Storage: its data is \
         lost when the instance stops or is replaced."
    );
}

#[test]
fn s3_regions_pass_when_equal_and_warn_when_apart() {
    let looked = |bucket: Result<&str, &str>, server: Option<&str>| RegionLookup::Looked {
        bucket: bucket.map(str::to_owned).map_err(str::to_owned),
        server: server.map(str::to_owned),
    };
    let same = s3_region("data", &looked(Ok("us-east-2"), Some("us-east-2")));
    assert_eq!(status(&same), "pass");
    assert!(detail(&same).contains("both in us-east-2"));

    let apart = s3_region("data", &looked(Ok("us-east-1"), Some("eu-west-2")));
    assert_eq!(status(&apart), "warn");
    assert_eq!(apart.summary, "S3 bucket is in another region");
    assert!(detail(&apart).contains("us-east-1") && detail(&apart).contains("eu-west-2"));
    assert!(fix(&apart)
        .unwrap()
        .starts_with("Run the server in us-east-1"));

    let unknown_server = s3_region("data", &looked(Ok("us-east-1"), None));
    assert_eq!(status(&unknown_server), "skip");
    assert!(detail(&unknown_server).contains("hop limit of 2"));

    let unknown_bucket = s3_region("data", &looked(Err("bucket not found"), Some("us-east-1")));
    assert_eq!(status(&unknown_bucket), "skip");
    assert!(detail(&unknown_bucket).contains("bucket not found"));

    let custom = s3_region(
        "data",
        &RegionLookup::CustomEndpoint("https://r2.example.com".into()),
    );
    assert_eq!(status(&custom), "skip");
    assert!(detail(&custom).contains("https://r2.example.com"));
}

#[test]
fn s3_latency_warns_above_the_threshold_or_on_timeout() {
    let fast = s3_latency(&aws::Latency::Measured(S3_LATENCY_WARNING));
    assert_eq!(status(&fast), "pass");
    assert_eq!(fast.summary, "S3 round trips take 50 ms");

    let slow = s3_latency(&aws::Latency::Measured(Duration::from_millis(140)));
    assert_eq!(status(&slow), "warn");
    assert!(fix(&slow).unwrap().contains("VPC gateway endpoint"));

    let timed_out = s3_latency(&aws::Latency::TimedOut);
    assert_eq!(status(&timed_out), "warn");
    assert!(detail(&timed_out).contains("2 s"));

    let failed = s3_latency(&aws::Latency::Failed("connection refused".into()));
    assert_eq!(status(&failed), "skip");
    assert!(detail(&failed).contains("connection refused"));
}

#[test]
fn the_disk_cache_belongs_on_local_ssd() {
    let root = Path::new("/var/cache/helix");
    let block = |disk| Backing::Block {
        device: "nvme1n1".into(),
        disk,
        ephemeral: None,
    };
    for (backing, expected) in [
        (
            Backing::Memory {
                filesystem: "tmpfs".into(),
            },
            "warn",
        ),
        (
            Backing::ContainerLayer {
                filesystem: "overlay".into(),
            },
            "warn",
        ),
        (
            Backing::Remote {
                filesystem: "fuse.s3fs".into(),
            },
            "warn",
        ),
        (instance_storage(), "pass"),
        (ebs_and_instance_storage(), "warn"),
        (
            block(Disk::LocalSsd {
                model: "Samsung SSD".into(),
            }),
            "pass",
        ),
        (
            block(Disk::Network {
                model: "Amazon Elastic Block Store".into(),
            }),
            "warn",
        ),
        (block(Disk::Rotational), "warn"),
        (
            Backing::Unknown {
                reason: "no sysfs".into(),
            },
            "skip",
        ),
    ] {
        let check = cache_device(root, &backing);
        assert_eq!(status(&check), expected, "{backing:?}");
        if expected == "warn" {
            assert!(
                fix(&check).unwrap().contains("/var/cache/helix"),
                "{backing:?}"
            );
        }
    }
    assert_eq!(
        cache_device(root, &instance_storage()).summary,
        "The disk cache is on local NVMe instance storage"
    );
    let ebs = cache_device(
        root,
        &block(Disk::Network {
            model: "Amazon Elastic Block Store".into(),
        }),
    );
    assert_eq!(ebs.summary, "The disk cache is on network block storage");
    assert!(detail(&ebs).contains("nvme1n1 (Amazon Elastic Block Store)"));
}

#[test]
fn cache_space_reports_the_budget_against_the_filesystem() {
    let directory = tempfile::tempdir().unwrap();
    let cache = cache(directory.path(), true);
    let used = stats(
        [CacheTierState::Disabled; 3],
        [
            CacheTierState::Ready {
                used_bytes: 100 * MIB,
                capacity_bytes: Some(512 * MIB),
            },
            CacheTierState::Ready {
                used_bytes: 28 * MIB,
                capacity_bytes: Some(384 * MIB),
            },
            CacheTierState::Initializing {
                capacity_bytes: Some(128 * MIB),
            },
        ],
    );
    let fits = cache_space(&cache, &Ok(None), &used);
    assert_eq!(status(&fits), "pass");
    assert_eq!(fix(&fits), None);
    assert_eq!(
        detail(&fits),
        "HELIX_DISK_CACHE_BYTES is 1.0 GiB; 128.0 MiB is cached."
    );

    let short = cache_space(&cache, &Ok(Some(256 * MIB)), &used);
    assert_eq!(status(&short), "warn");
    assert!(detail(&short).contains("256.0 MiB more than"));
    assert!(fix(&short).unwrap().contains("at most 768.0 MiB"));

    let unmeasured = cache_space(&cache, &Err("permission denied".into()), &used);
    assert_eq!(status(&unmeasured), "skip");
    assert_eq!(detail(&unmeasured), "permission denied");
}

#[test]
fn turning_the_startup_warm_off_warns() {
    let directory = tempfile::tempdir().unwrap();
    assert_eq!(status(&cache_warm(&cache(directory.path(), true))), "pass");
    let off = cache_warm(&cache(directory.path(), false));
    assert_eq!(status(&off), "warn");
    assert!(fix(&off).unwrap().contains("HELIX_DISK_CACHE_WARM"));
}

#[test]
fn memory_warns_when_cache_budgets_take_most_of_the_limit() {
    let budgets = stats(
        [
            CacheTierState::Ready {
                used_bytes: 0,
                capacity_bytes: Some(640 * MIB),
            },
            CacheTierState::Initializing {
                capacity_bytes: Some(64 * MIB),
            },
            CacheTierState::Ready {
                used_bytes: 0,
                capacity_bytes: Some(256 * MIB),
            },
        ],
        [CacheTierState::Disabled; 3],
    );
    // 960 MiB of tiers plus 72 MiB of index for 8 GiB of disk: 1,032 MiB.
    let roomy = memory(&budgets, Some(8 * GIB as usize), Some(4 * GIB));
    assert_eq!(status(&roomy), "pass");
    assert_eq!(
        detail(&roomy),
        "Cache budgets total 1.0 GiB of the 4.0 GiB this process may use."
    );

    let tight = memory(&budgets, Some(8 * GIB as usize), Some(GIB));
    assert_eq!(status(&tight), "warn");
    assert!(detail(&tight).contains("leaving 0 B"), "{}", detail(&tight));
    assert!(fix(&tight).unwrap().contains("at least 1.4 GiB"));
    assert!(
        fix(&tight).unwrap().contains("HELIX_DISK_CACHE_BYTES"),
        "the disk tier's index counts toward the budget too"
    );

    // Exactly 70%: 700 MiB of 1,000 MiB still passes.
    let at_limit = stats(
        [
            CacheTierState::Ready {
                used_bytes: 0,
                capacity_bytes: Some(700 * MIB),
            },
            CacheTierState::Unavailable,
            CacheTierState::Disabled,
        ],
        [CacheTierState::Disabled; 3],
    );
    assert_eq!(status(&memory(&at_limit, None, Some(1_000 * MIB))), "pass");
    assert_eq!(status(&memory(&at_limit, None, Some(999 * MIB))), "warn");
    assert_eq!(status(&memory(&at_limit, None, None)), "skip");
}

#[test]
fn index_publication_warns_on_held_back_entities_or_a_late_backlog() {
    let caught_up = index_publication(&db::IndexOperationQueueStats::default());
    assert_eq!(status(&caught_up), "pass");
    assert_eq!(detail(&caught_up), "No index operations are queued.");

    let draining = index_publication(&db::IndexOperationQueueStats {
        pending_operations: 12,
        oldest_pending_micros: 3_000_000,
        ..db::IndexOperationQueueStats::default()
    });
    assert_eq!(status(&draining), "pass");
    assert_eq!(
        detail(&draining),
        "12 operations are queued; the oldest has waited 3 s."
    );

    let late = index_publication(&db::IndexOperationQueueStats {
        pending_operations: 1,
        oldest_pending_micros: 61_000_000,
        ..db::IndexOperationQueueStats::default()
    });
    assert_eq!(status(&late), "warn");
    assert_eq!(
        detail(&late),
        "1 operation is queued; the oldest has waited 1 min. Eventual searches miss writes \
         that recent."
    );

    let blocked = index_publication(&db::IndexOperationQueueStats {
        blocked_entities: 2,
        oldest_pending_micros: 61_000_000,
        ..db::IndexOperationQueueStats::default()
    });
    assert_eq!(status(&blocked), "warn");
    assert_eq!(
        blocked.summary,
        "2 entries are held back from vector or text indexes"
    );
    assert!(fix(&blocked).unwrap().contains(INDEX_TROUBLESHOOTING));
    assert_eq!(
        index_publication(&db::IndexOperationQueueStats {
            blocked_entities: 1,
            ..db::IndexOperationQueueStats::default()
        })
        .summary,
        "1 entry is held back from vector or text indexes"
    );
}

#[test]
fn missing_indexes_list_each_index_with_a_runnable_fix() {
    let none = InsightSnapshot::default();
    assert_eq!(status(&missing_indexes(&none)), "skip");
    assert_eq!(status(&unbounded_scans(&none)), "skip");

    let clean = InsightSnapshot {
        analyzed_queries: 1,
        ..InsightSnapshot::default()
    };
    let check = missing_indexes(&clean);
    assert_eq!(status(&check), "pass");
    assert_eq!(detail(&check), "No missing index in 1 recent query.");

    let snapshot = InsightSnapshot {
        analyzed_queries: 1_300,
        insights: vec![
            count(
                missing(
                    "User",
                    "email",
                    ElementKind::Node,
                    SecondaryIndexKind::Equality,
                ),
                1_204,
                120,
            ),
            count(scan(Some("Post"), &[]), 9, 5),
            count(
                missing(
                    "Post",
                    "created_at",
                    ElementKind::Node,
                    SecondaryIndexKind::Range,
                ),
                1,
                7_200,
            ),
            count(
                missing(
                    "Follows",
                    "since",
                    ElementKind::Edge,
                    SecondaryIndexKind::Range,
                ),
                3,
                1,
            ),
            count(
                missing(
                    "O'Brien",
                    "kind",
                    ElementKind::Edge,
                    SecondaryIndexKind::Equality,
                ),
                2,
                90_000,
            ),
        ],
        evicted_insights: 0,
        untallied_insights: 0,
    };
    let check = missing_indexes(&snapshot);
    assert_eq!(status(&check), "warn");
    assert_eq!(check.summary, "4 filters scan without an index");
    assert_eq!(
        detail(&check),
        [
            "User.email (node equality): 1204 queries, last seen 2 min ago",
            "Post.created_at (node range): 1 query, last seen 2 h ago",
            "Follows.since (edge range): 3 queries, last seen 1 s ago",
            "O'Brien.kind (edge equality): 2 queries, last seen 1 d ago",
        ]
        .join("\n")
    );
    let fix = fix(&check).unwrap().lines().collect::<Vec<_>>();
    assert_eq!(fix.len(), 5);
    assert_eq!(
        fix[1],
        r#"helix query <instance> -e 'writeBatch().varAs("index", g().createIndexIfNotExists(IndexSpec.nodeEquality("User", "email"))).returning(["index"])'"#
    );
    assert!(fix[2].contains(r#"IndexSpec.nodeRange("Post", "created_at")"#));
    assert!(fix[3].contains(r#"IndexSpec.edgeRange("Follows", "since")"#));
    // A quote in a name closes and reopens the shell string around it.
    assert!(
        fix[4].contains(r#"IndexSpec.edgeEquality("O'\''Brien", "kind")"#),
        "{}",
        fix[4]
    );

    let single = missing_indexes(&InsightSnapshot {
        analyzed_queries: 1,
        insights: vec![count(
            missing(
                "User",
                "email",
                ElementKind::Node,
                SecondaryIndexKind::Equality,
            ),
            1,
            0,
        )],
        evicted_insights: 0,
        untallied_insights: 0,
    });
    assert_eq!(single.summary, "1 filter scans without an index");
    assert!(detail(&single).ends_with("1 query, last seen 0 ms ago"));
}

#[test]
fn long_insight_lists_are_truncated_and_note_evictions() {
    let insights = (0..LISTED_INSIGHTS + 2)
        .map(|index| {
            count(
                missing(
                    "User",
                    &format!("p{index}"),
                    ElementKind::Node,
                    SecondaryIndexKind::Equality,
                ),
                1,
                0,
            )
        })
        .chain(
            (0..LISTED_INSIGHTS + 1)
                .map(|index| count(scan(Some(&format!("L{index}")), &[]), 1, 0)),
        )
        .collect::<Vec<_>>();
    let snapshot = InsightSnapshot {
        analyzed_queries: 50,
        insights,
        evicted_insights: 3,
        untallied_insights: 1,
    };
    let missing = missing_indexes(&snapshot);
    let lines = detail(&missing).lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), LISTED_INSIGHTS + 3);
    assert_eq!(lines[LISTED_INSIGHTS], "… and 2 more");
    assert_eq!(
        lines[LISTED_INSIGHTS + 1],
        "(3 older insights were dropped from the tally while it was full)"
    );
    assert_eq!(
        lines[LISTED_INSIGHTS + 2],
        "(1 insight named a label or property name over 1024 bytes and was not tallied)"
    );
    assert_eq!(fix(&missing).unwrap().lines().count(), LISTED_INSIGHTS + 1);

    let scans = unbounded_scans(&snapshot);
    let lines = detail(&scans).lines().collect::<Vec<_>>();
    assert_eq!(lines[LISTED_INSIGHTS], "… and 1 more");
    assert_eq!(
        scans.summary,
        "11 query shapes read every element of a label"
    );
}

#[test]
fn unbounded_scans_describe_what_each_reads() {
    let clean = InsightSnapshot {
        analyzed_queries: 2,
        ..InsightSnapshot::default()
    };
    let check = unbounded_scans(&clean);
    assert_eq!(status(&check), "pass");
    assert_eq!(detail(&check), "No unbounded scan in 2 recent queries.");

    let check = unbounded_scans(&InsightSnapshot {
        analyzed_queries: 20,
        insights: vec![
            count(scan(Some("User"), &["bio", "name"]), 12, 30),
            count(
                missing(
                    "User",
                    "email",
                    ElementKind::Node,
                    SecondaryIndexKind::Equality,
                ),
                4,
                1,
            ),
            count(scan(None, &[]), 1, 3_600),
        ],
        evicted_insights: 1,
        untallied_insights: 0,
    });
    assert_eq!(status(&check), "warn");
    assert_eq!(
        check.summary,
        "2 query shapes read every element of a label"
    );
    assert_eq!(
        detail(&check),
        [
            "every User node, filtering on bio, name: 12 queries, last seen 30 s ago",
            "every node: 1 query, last seen 1 h ago",
            "(1 older insight was dropped from the tally while it was full)",
        ]
        .join("\n")
    );
    assert_eq!(
        unbounded_scans(&InsightSnapshot {
            analyzed_queries: 1,
            insights: vec![count(scan(Some("User"), &[]), 1, 0)],
            evicted_insights: 0,
            untallied_insights: 0,
        })
        .summary,
        "1 query shape reads every element of a label"
    );
}

#[test]
fn sizes_and_durations_read_in_their_coarsest_unit() {
    assert_eq!(bytes(0), "0 B");
    assert_eq!(bytes(1023), "1023 B");
    assert_eq!(bytes(1024), "1.0 KiB");
    assert_eq!(bytes(640 * MIB), "640.0 MiB");
    assert_eq!(bytes(8 * GIB + GIB / 2), "8.5 GiB");
    assert_eq!(bytes(3 * 1024 * GIB), "3.0 TiB");
    assert_eq!(bytes(5 * 1024 * 1024 * GIB), "5120.0 TiB");

    assert_eq!(elapsed(Duration::from_millis(12)), "12 ms");
    assert_eq!(elapsed(Duration::from_secs(59)), "59 s");
    assert_eq!(elapsed(Duration::from_secs(60)), "1 min");
    assert_eq!(elapsed(Duration::from_secs(3_599)), "59 min");
    assert_eq!(elapsed(Duration::from_secs(3_600)), "1 h");
    assert_eq!(elapsed(Duration::from_secs(86_400 * 3)), "3 d");

    assert_eq!(json_string(r#"a"b\c"#), r#""a\"b\\c""#);
}

async fn get_report(router: axum::Router) -> serde_json::Value {
    let response = router
        .oneshot(
            Request::builder()
                .uri("/v2/diagnostics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&to_bytes(response.into_body(), 1 << 20).await.unwrap()).unwrap()
}

fn ids(report: &serde_json::Value) -> Vec<&str> {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|check| check["id"].as_str().unwrap())
        .collect()
}

fn find<'a>(report: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["id"] == id)
        .expect("the report has the check")
}

#[tokio::test]
async fn memory_storage_reports_its_checks_and_recent_missing_indexes() {
    let db = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemory {
            database: "diagnostics-memory".into(),
        })
        .await
        .unwrap(),
    );
    let state = ServerState::new(Arc::clone(&db), None, StorageConfig::Memory);
    let router = crate::http::router(state.clone());

    let report = get_report(router.clone()).await;
    assert_eq!(
        ids(&report),
        [
            "storage.durability",
            "resources.memory",
            "indexes.publication",
            "queries.missing_indexes",
            "queries.unbounded_scans",
        ]
    );
    assert!(report["uptime_secs"].as_u64().is_some());
    assert_eq!(find(&report, "storage.durability")["status"], "warn");
    assert_eq!(find(&report, "queries.missing_indexes")["status"], "skip");

    state
        .query_service()
        .execute_query(query::QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "users",
                    traversal::g().n_with_label_where(
                        "User",
                        expr::Predicate::eq("email", "ada@example.com"),
                    ),
                )
                .var_as(
                    "count",
                    traversal::g().n(graph::NodeRef::var("users")).count(),
                )
                .returning(["count"]),
        ))
        .await
        .unwrap();
    let report = get_report(router).await;
    let missing = find(&report, "queries.missing_indexes");
    assert_eq!(missing["status"], "warn");
    assert!(
        missing["detail"]
            .as_str()
            .unwrap()
            .starts_with("User.email (node equality): 1 query"),
        "{missing}"
    );
    assert!(
        !report.to_string().contains("ada@example.com"),
        "diagnostics never carry query values"
    );
    // The scan the missing index bounds is reported under that index.
    assert_eq!(find(&report, "queries.unbounded_scans")["status"], "pass");
    db.close().await.unwrap();
}

#[tokio::test]
async fn s3_storage_reports_cache_checks_without_contacting_aws() {
    let directory = tempfile::tempdir().unwrap();
    let db = Arc::new(
        db::HelixDB::open(db::HelixDbSource::InMemory {
            database: "diagnostics-s3".into(),
        })
        .await
        .unwrap(),
    );
    let storage = StorageConfig::S3 {
        bucket: "launch".into(),
        region: "us-east-1".into(),
        // A custom endpoint skips the AWS region lookup; latency is
        // probed through the database's own (in-memory) store.
        endpoint: Some("http://127.0.0.1:9".into()),
        allow_http: true,
        cache: Box::new(cache(directory.path(), false)),
    };
    let report = get_report(crate::http::router(ServerState::new(
        Arc::clone(&db),
        None,
        storage,
    )))
    .await;
    assert_eq!(
        ids(&report),
        [
            "storage.durability",
            "storage.s3_region",
            "storage.s3_latency",
            "cache.device",
            "cache.space",
            "cache.warm",
            "resources.memory",
            "indexes.publication",
            "queries.missing_indexes",
            "queries.unbounded_scans",
        ]
    );
    assert_eq!(find(&report, "storage.s3_region")["status"], "skip");
    assert_eq!(find(&report, "storage.s3_latency")["status"], "pass");
    assert_eq!(find(&report, "cache.warm")["status"], "warn");
    assert_ne!(find(&report, "cache.space")["status"], "fail");
    db.close().await.unwrap();
}

#[tokio::test]
async fn disk_storage_classifies_its_data_directory_and_readers_skip_publication() {
    let directory = tempfile::tempdir().unwrap();
    let source = db::HelixDbSource::Disk {
        root: directory.path().join("data"),
        database: "db/".into(),
    };
    std::fs::create_dir_all(directory.path().join("data")).unwrap();
    let writer = Arc::new(db::HelixDB::open(source.clone()).await.unwrap());
    writer.flush_writer().await.unwrap();
    let reader = Arc::new(db::HelixDB::open_reader(source).await.unwrap());
    let storage = StorageConfig::Disk {
        root: directory.path().join("data"),
        cache: CacheConfig::Memory,
    };
    let report = get_report(crate::http::router(ServerState::new(
        Arc::clone(&reader),
        None,
        storage,
    )))
    .await;
    assert_eq!(
        ids(&report),
        [
            "storage.durability",
            "storage.data_device",
            "resources.memory",
            "queries.missing_indexes",
            "queries.unbounded_scans",
        ]
    );
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}
