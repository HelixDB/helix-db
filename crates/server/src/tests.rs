use std::collections::BTreeMap;
use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use helix_ast::{batch, query, traversal, value};
use tower::ServiceExt;

use super::*;

fn memory_config(name: &str) -> ServerConfig {
    ServerConfig {
        http_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_addr: "127.0.0.1:0".parse().unwrap(),
        db_path: name.to_string(),
        storage: StorageConfig::Memory,
        index_operation_queue: db::config::IndexOperationQueueTuning::default(),
        cypher: crate::CypherEndpoints::Disabled,
    }
}

#[cfg(unix)]
#[tokio::test]
async fn interrupt_completes_process_shutdown_wait() {
    let interrupt_observed = Arc::new(AtomicBool::new(false));
    let observed_by_interrupt = Arc::clone(&interrupt_observed);

    first_shutdown(
        async move {
            observed_by_interrupt.store(true, Ordering::SeqCst);
        },
        std::future::pending::<()>(),
    )
    .await;

    assert!(interrupt_observed.load(Ordering::SeqCst));
}

#[cfg(unix)]
#[tokio::test]
async fn termination_completes_process_shutdown_wait() {
    let termination_observed = Arc::new(AtomicBool::new(false));
    let observed_by_termination = Arc::clone(&termination_observed);

    first_shutdown(std::future::pending::<()>(), async move {
        observed_by_termination.store(true, Ordering::SeqCst);
    })
    .await;

    assert!(termination_observed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn grpc_transport_failure_joins_peer_before_database_close() {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let peer_joined = Arc::new(AtomicBool::new(false));
    let peer_joined_by_task = Arc::clone(&peer_joined);
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while shutdown_rx.changed().await.is_ok() {
            if *shutdown_rx.borrow() {
                break;
            }
        }
        peer_joined_by_task.store(true, Ordering::SeqCst);
        Ok(())
    });
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async {
        Err(
            Box::new(std::io::Error::other("injected gRPC transport failure"))
                as Box<dyn Error + Send + Sync + 'static>,
        )
    });
    let close_called = Arc::new(AtomicBool::new(false));
    let close_called_by_future = Arc::clone(&close_called);
    let peer_joined_before_close = Arc::clone(&peer_joined);

    let error = supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        std::future::pending::<ServerResult<()>>(),
        async move {
            assert!(peer_joined_before_close.load(Ordering::SeqCst));
            close_called_by_future.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .expect_err("transport failure is preserved");

    assert_eq!(error.to_string(), "injected gRPC transport failure");
    assert!(peer_joined.load(Ordering::SeqCst));
    assert!(close_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn grpc_transport_completion_joins_peer_before_database_close() {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let peer_joined = Arc::new(AtomicBool::new(false));
    let peer_joined_by_task = Arc::clone(&peer_joined);
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while !*shutdown_rx.borrow() {
            shutdown_rx.changed().await.unwrap();
        }
        peer_joined_by_task.store(true, Ordering::SeqCst);
        Ok(())
    });
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async { Ok(()) });
    let peer_joined_before_close = Arc::clone(&peer_joined);

    supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        std::future::pending::<ServerResult<()>>(),
        async move {
            assert!(peer_joined_before_close.load(Ordering::SeqCst));
            Ok(())
        },
    )
    .await
    .unwrap();

    assert!(peer_joined.load(Ordering::SeqCst));
}

#[tokio::test]
async fn http_transport_failure_joins_peer_before_database_close() {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let peer_joined = Arc::new(AtomicBool::new(false));
    let peer_joined_by_task = Arc::clone(&peer_joined);
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async {
        Err(
            Box::new(std::io::Error::other("injected HTTP transport failure"))
                as Box<dyn Error + Send + Sync + 'static>,
        )
    });
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while shutdown_rx.changed().await.is_ok() {
            if *shutdown_rx.borrow() {
                break;
            }
        }
        peer_joined_by_task.store(true, Ordering::SeqCst);
        Ok(())
    });
    let close_called = Arc::new(AtomicBool::new(false));
    let close_called_by_future = Arc::clone(&close_called);
    let peer_joined_before_close = Arc::clone(&peer_joined);

    let error = supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        std::future::pending::<ServerResult<()>>(),
        async move {
            assert!(peer_joined_before_close.load(Ordering::SeqCst));
            close_called_by_future.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .expect_err("transport failure is preserved");

    assert_eq!(error.to_string(), "injected HTTP transport failure");
    assert!(peer_joined.load(Ordering::SeqCst));
    assert!(close_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn http_transport_completion_joins_peer_before_database_close() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let peer_joined = Arc::new(AtomicBool::new(false));
    let peer_joined_by_task = Arc::clone(&peer_joined);
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async { Ok(()) });
    let mut grpc_shutdown = shutdown_rx;
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while !*grpc_shutdown.borrow() {
            grpc_shutdown.changed().await.unwrap();
        }
        peer_joined_by_task.store(true, Ordering::SeqCst);
        Ok(())
    });
    let peer_joined_before_close = Arc::clone(&peer_joined);

    supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        std::future::pending::<ServerResult<()>>(),
        async move {
            assert!(peer_joined_before_close.load(Ordering::SeqCst));
            Ok(())
        },
    )
    .await
    .unwrap();

    assert!(peer_joined.load(Ordering::SeqCst));
}

#[tokio::test]
async fn embedding_shutdown_closes_both_transports_and_database() {
    run_with_shutdown(memory_config("server-owned-shutdown"), async {})
        .await
        .unwrap();
}

#[tokio::test]
async fn listener_failure_still_closes_the_database_and_peer_transport() {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap();
    let mut config = memory_config("server-listener-failure");
    config.http_addr = address;
    config.grpc_addr = address;

    assert!(run_with_shutdown(config, std::future::pending())
        .await
        .is_err());
}

#[tokio::test]
async fn grpc_bind_failure_report_keeps_the_os_cause() {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut config = memory_config("server-grpc-bind-failure");
    config.grpc_addr = occupied.local_addr().unwrap();

    let error = run_with_shutdown(config, std::future::pending())
        .await
        .unwrap_err();
    let report = error_report(&*error);
    // tonic's own message is only "transport error"; the cause says why.
    assert!(
        report.contains("\ncaused by: Address already in use"),
        "{report}"
    );
}

#[tokio::test]
async fn ctrl_c_runner_returns_a_failed_database_open_without_waiting() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing");
    let mut config = memory_config("server-ctrl-c-open-failure");
    config.storage = StorageConfig::Disk {
        root: missing.clone(),
        cache: CacheConfig::Memory,
    };

    let error = run_until_ctrl_c(config).await.unwrap_err();
    let report = error_report(&*error);
    assert!(report.contains(&missing.display().to_string()), "{report}");
    assert!(!missing.exists(), "a failed open creates no data directory");
}

#[test]
fn error_report_prints_every_cause_once() {
    #[derive(Debug, thiserror::Error)]
    #[error("server failed to start")]
    struct Startup(#[source] ServerConfigError);

    let error = Startup(ServerConfigError::CacheDirectory {
        path: PathBuf::from("/cache"),
        source: std::io::Error::other("disk on fire"),
    });
    assert_eq!(
        error_report(&error),
        "Error: server failed to start\n\
         caused by: HELIX_DISK_CACHE_DIR: `/cache` is not a writable directory; mount a \
         writable volume there or set HELIX_DISK_CACHE_DIR to a writable directory\n\
         caused by: disk on fire"
    );
}

#[tokio::test]
async fn shutdown_signal_failure_joins_both_transports_before_close() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let joined = Arc::new(AtomicUsize::new(0));
    let http_joined = Arc::clone(&joined);
    let mut http_shutdown = shutdown_rx.clone();
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while !*http_shutdown.borrow() {
            http_shutdown.changed().await.unwrap();
        }
        http_joined.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let grpc_joined = Arc::clone(&joined);
    let mut grpc_shutdown = shutdown_rx;
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while !*grpc_shutdown.borrow() {
            grpc_shutdown.changed().await.unwrap();
        }
        grpc_joined.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });
    let close_joined = Arc::clone(&joined);

    let error = supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        async {
            Err(Box::new(std::io::Error::other("injected signal failure"))
                as Box<dyn Error + Send + Sync + 'static>)
        },
        async move {
            assert_eq!(close_joined.load(Ordering::SeqCst), 2);
            Ok(())
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.to_string(), "injected signal failure");
}

#[tokio::test]
async fn panicked_transport_still_stops_peer_and_closes_database() {
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let peer_joined = Arc::new(AtomicBool::new(false));
    let peer_joined_by_task = Arc::clone(&peer_joined);
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async {
        panic!("injected HTTP panic");
    });
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while !*shutdown_rx.borrow() {
            shutdown_rx.changed().await.unwrap();
        }
        peer_joined_by_task.store(true, Ordering::SeqCst);
        Ok(())
    });
    let close_called = Arc::new(AtomicBool::new(false));
    let close_called_by_future = Arc::clone(&close_called);

    let error = supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        std::future::pending::<ServerResult<()>>(),
        async move {
            close_called_by_future.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("injected HTTP panic"));
    assert!(peer_joined.load(Ordering::SeqCst));
    assert!(close_called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn close_failure_is_returned_after_graceful_transport_shutdown() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut http_shutdown = shutdown_rx.clone();
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while !*http_shutdown.borrow() {
            http_shutdown.changed().await.unwrap();
        }
        Ok(())
    });
    let mut grpc_shutdown = shutdown_rx;
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        while !*grpc_shutdown.borrow() {
            grpc_shutdown.changed().await.unwrap();
        }
        Ok(())
    });

    let error = supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        async { Ok(()) },
        async {
            Err(Box::new(std::io::Error::other("injected close failure"))
                as Box<dyn Error + Send + Sync + 'static>)
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.to_string(), "injected close failure");
}

#[tokio::test]
async fn transport_failure_has_precedence_when_close_also_fails() {
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async {
        Err(Box::new(std::io::Error::other("primary transport failure"))
            as Box<dyn Error + Send + Sync + 'static>)
    });
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async { Ok(()) });

    let error = supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        std::future::pending::<ServerResult<()>>(),
        async {
            Err(Box::new(std::io::Error::other("secondary close failure"))
                as Box<dyn Error + Send + Sync + 'static>)
        },
    )
    .await
    .unwrap_err();

    assert_eq!(error.to_string(), "primary transport failure");
}

#[tokio::test]
async fn simultaneous_transport_completion_is_a_graceful_shutdown_race() {
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async { Ok(()) });
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async { Ok(()) });
    let close_called = Arc::new(AtomicBool::new(false));
    let close_called_by_future = Arc::clone(&close_called);

    supervise_transports_and_close(
        shutdown_tx,
        http_task,
        grpc_task,
        std::future::pending::<ServerResult<()>>(),
        async move {
            close_called_by_future.store(true, Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();

    assert!(close_called.load(Ordering::SeqCst));
}

async fn post_query(router: axum::Router, request: &query::QueryRequest) -> serde_json::Value {
    let is_write = request.request_type() == query::QueryRequestType::Write;
    let response = router
        .oneshot(
            Request::post("/v2/query")
                .header("content-type", "application/json")
                .header("x-helix-await-durable", is_write.to_string())
                .body(Body::from(simd_json::to_vec(request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(
        &to_bytes(response.into_body(), MAX_QUERY_BODY_BYTES)
            .await
            .unwrap(),
    )
    .unwrap()
}

/// Serializes tests that read or change the process-wide open-file limit.
#[cfg(unix)]
static OPEN_FILE_LIMIT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Regular files below `path` and their sizes, recursing through subdirectories.
fn files_below(path: &Path) -> BTreeMap<PathBuf, u64> {
    std::fs::read_dir(path)
        .unwrap()
        .flat_map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                files_below(&path)
            } else {
                BTreeMap::from([(path, entry.metadata().unwrap().len())])
            }
        })
        .collect()
}

/// Runs on the current thread so the final runner's transports only start
/// after its immediate shutdown is sent. On worker threads they could start
/// first or not, and which of their shutdown-wait lines run would vary
/// between runs, breaking the exact server coverage fingerprint.
#[tokio::test]
async fn hybrid_disk_cache_serves_reopened_reads_from_local_disk() {
    const MIB: usize = 1024 * 1024;
    #[cfg(unix)]
    let _limit = OPEN_FILE_LIMIT.lock().await;
    #[cfg(unix)]
    let original_limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    let directory = tempfile::tempdir().unwrap();
    let data_root = directory.path().join("data");
    std::fs::create_dir_all(&data_root).unwrap();
    let cache_root = directory.path().join("cache");
    // The minimum budget keeps the block cache at 384 partition files.
    let cache = HybridCache::try_new(
        &cache_root,
        NonZeroUsize::new(16 * MIB).unwrap(),
        NonZeroUsize::new(64 * MIB).unwrap(),
    )
    .unwrap();
    let config = ServerConfig {
        http_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_addr: "127.0.0.1:0".parse().unwrap(),
        db_path: "server-hybrid-cache".to_string(),
        storage: StorageConfig::Disk {
            root: data_root.clone(),
            cache: CacheConfig::Hybrid(Box::new(cache)),
        },
        index_operation_queue: db::config::IndexOperationQueueTuning::default(),
        cypher: crate::CypherEndpoints::Disabled,
    };
    let write = query::QueryRequest::write(
        batch::write_batch()
            .var_as(
                "created",
                traversal::g().add_n(
                    "CachedUser",
                    vec![("name", value::PropertyInput::from("Ada"))],
                ),
            )
            .returning(["created"]),
    );
    let read = query::QueryRequest::read(
        batch::read_batch()
            .var_as("count", traversal::g().n_with_label("CachedUser").count())
            .returning(["count"]),
    );

    let ServerDatabase { db, cache_lock } = open_database(&config).await.unwrap();
    let stats = db.cache_stats();
    assert!(matches!(
        stats.foyer_hybrid_disk.state,
        db::CacheTierState::Ready { capacity_bytes: Some(bytes), .. } if bytes == 24 * MIB as u64
    ));
    assert!(matches!(
        stats.slate_object_store_disk.state,
        db::CacheTierState::Ready { capacity_bytes: Some(bytes), .. }
            | db::CacheTierState::Initializing { capacity_bytes: Some(bytes) }
            if bytes == 32 * MIB as u64
    ));
    assert!(matches!(
        stats.fts_disk.state,
        db::CacheTierState::Ready { capacity_bytes: Some(bytes), .. } if bytes == 8 * MIB as u64
    ));
    let router = http::router(ServerState::new(
        Arc::clone(&db),
        None,
        config.storage.clone(),
    ));
    post_query(router.clone(), &write).await;
    assert_eq!(post_query(router, &read).await["count"], 1);
    db.close().await.unwrap();
    drop(db);
    drop(cache_lock);

    // 24 MiB of Foyer disk tier in its minimum 64 KiB partitions.
    assert_eq!(files_below(&cache_root.join("slate")).len(), 384);
    assert_eq!(
        files_below(&cache_root.join("object-store")),
        BTreeMap::new(),
        "the SST flushed to HELIX_DATA_DIR on close is not copied into the cache"
    );
    assert!(cache_root.join("fts").is_dir());

    // Reading the flushed SST back caches it on local disk.
    let ServerDatabase {
        db: reader,
        cache_lock,
    } = open_database(&config).await.unwrap();
    let router = http::router(ServerState::new(
        Arc::clone(&reader),
        None,
        config.storage.clone(),
    ));
    assert_eq!(post_query(router, &read).await["count"], 1);
    reader.close().await.unwrap();
    drop(reader);
    drop(cache_lock);
    let cached = files_below(&cache_root.join("object-store"));
    assert!(
        !cached.is_empty(),
        "the SST read back is cached on local disk"
    );

    // With every SST gone from durable storage, the reopened server can
    // only answer from the disk caches.
    let durable_ssts = files_below(&data_root.join("server-hybrid-cache").join("compacted"));
    assert!(!durable_ssts.is_empty());
    durable_ssts
        .keys()
        .for_each(|sst| std::fs::remove_file(sst).unwrap());

    let ServerDatabase {
        db: reopened,
        cache_lock,
    } = open_database(&config).await.unwrap();
    let router = http::router(ServerState::new(
        Arc::clone(&reopened),
        None,
        config.storage.clone(),
    ));
    assert_eq!(post_query(router, &read).await["count"], 1);
    reopened.close().await.unwrap();
    drop(reopened);
    drop(cache_lock);
    let after_restart = files_below(&cache_root.join("object-store"));
    assert!(
        cached
            .iter()
            .all(|(path, size)| after_restart.get(path) == Some(size)),
        "cached files survive the restart unchanged"
    );

    run_with_shutdown(config, async {}).await.unwrap();
    #[cfg(unix)]
    rustix::process::setrlimit(rustix::process::Resource::Nofile, original_limit).unwrap();
}

/// Text queries recurse deeper than a test thread's stack allows in debug
/// builds, so this runs on its own thread with a larger stack.
#[test]
fn hybrid_disk_cache_admits_full_text_splits_only_on_demand() {
    std::thread::Builder::new()
        .name("hybrid-full-text-cache".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(hybrid_disk_cache_admits_full_text_splits_only_on_demand_contract());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn hybrid_disk_cache_admits_full_text_splits_only_on_demand_contract() {
    const MIB: usize = 1024 * 1024;
    #[cfg(unix)]
    let _limit = OPEN_FILE_LIMIT.lock().await;
    #[cfg(unix)]
    let original_limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    let directory = tempfile::tempdir().unwrap();
    let data_root = directory.path().join("data");
    std::fs::create_dir_all(&data_root).unwrap();
    let cache_root = directory.path().join("cache");
    let fts_root = cache_root.join("fts");
    let config = ServerConfig {
        http_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_addr: "127.0.0.1:0".parse().unwrap(),
        db_path: "server-hybrid-text-cache".to_string(),
        storage: StorageConfig::Disk {
            root: data_root,
            cache: CacheConfig::Hybrid(Box::new(
                HybridCache::try_new(
                    &cache_root,
                    NonZeroUsize::new(16 * MIB).unwrap(),
                    NonZeroUsize::new(64 * MIB).unwrap(),
                )
                .unwrap(),
            )),
        },
        index_operation_queue: db::config::IndexOperationQueueTuning::default(),
        cypher: crate::CypherEndpoints::Disabled,
    };
    let search = query::QueryRequest::read(
        batch::read_batch()
            .var_as(
                "ids",
                traversal::g()
                    .text_search_nodes("Document", "body", "alpha", 8, None)
                    .id(),
            )
            .returning(["ids"]),
    );
    let fts_disk_bytes = |db: &HelixDB| {
        let state = db.cache_stats().fts_disk.state;
        let db::CacheTierState::Ready {
            used_bytes,
            capacity_bytes: Some(capacity),
        } = state
        else {
            panic!("the full-text disk tier is ready, got {state:?}");
        };
        assert_eq!(capacity, 8 * MIB as u64, "the full-text tier's share");
        used_bytes
    };

    let ServerDatabase { db, cache_lock } = open_database(&config).await.unwrap();
    let router = http::router(ServerState::new(
        Arc::clone(&db),
        None,
        config.storage.clone(),
    ));
    post_query(
        router.clone(),
        &query::QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "first",
                    traversal::g().add_n(
                        "Document",
                        vec![("body", value::PropertyInput::from("alpha"))],
                    ),
                )
                .var_as(
                    "second",
                    traversal::g().add_n(
                        "Document",
                        vec![("body", value::PropertyInput::from("beta"))],
                    ),
                )
                .returning(Vec::<String>::new()),
        ),
    )
    .await;
    let receipt = post_query(
        router.clone(),
        &query::QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_text_index_nodes("Document", "body", None::<String>),
                )
                .returning(["operation"]),
        ),
    )
    .await;
    let status = query::QueryRequest::read(
        batch::read_batch()
            .var_as(
                "status",
                traversal::g().get_index_operation(
                    receipt["operation"]["operation_id"]
                        .as_str()
                        .expect("the text index build is accepted"),
                ),
            )
            .returning(["status"]),
    );
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while post_query(router.clone(), &status).await["status"]["status"] != "succeeded" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the text index activates");
    db.close().await.unwrap();
    drop(db);
    drop(cache_lock);

    // The reopened server warms nothing into `fts/`, whatever its share.
    let ServerDatabase {
        db: reopened,
        cache_lock,
    } = open_database(&config).await.unwrap();
    reopened.wait_for_startup_cache_warm().await;
    assert_eq!(fts_disk_bytes(&reopened), 0);
    assert!(
        files_below(&fts_root).is_empty(),
        "startup downloads no split into the full-text disk tier"
    );

    // A split searched twice is admitted to disk in the background.
    let router = http::router(ServerState::new(
        Arc::clone(&reopened),
        None,
        config.storage.clone(),
    ));
    for _ in 0..2 {
        assert_eq!(
            post_query(router.clone(), &search).await["ids"],
            serde_json::json!([0])
        );
    }
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        while fts_disk_bytes(&reopened) == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a split searched twice reaches the full-text disk tier");
    assert!(files_below(&fts_root).keys().any(|path| path
        .extension()
        .is_some_and(|extension| extension == "split")));
    reopened.close().await.unwrap();
    drop(reopened);
    drop(cache_lock);
    #[cfg(unix)]
    rustix::process::setrlimit(rustix::process::Resource::Nofile, original_limit).unwrap();
}

#[tokio::test]
async fn a_hybrid_cache_directory_serves_one_server_at_a_time() {
    const MIB: usize = 1024 * 1024;
    #[cfg(unix)]
    let _limit = OPEN_FILE_LIMIT.lock().await;
    #[cfg(unix)]
    let original_limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    let directory = tempfile::tempdir().unwrap();
    let data_root = directory.path().join("data");
    std::fs::create_dir_all(&data_root).unwrap();
    let cache_root = directory.path().join("cache");
    let config = |disk_bytes| ServerConfig {
        http_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_addr: "127.0.0.1:0".parse().unwrap(),
        db_path: "server-cache-lock".to_string(),
        storage: StorageConfig::Disk {
            root: data_root.clone(),
            cache: CacheConfig::Hybrid(Box::new(
                HybridCache::try_new(
                    &cache_root,
                    NonZeroUsize::new(16 * MIB).unwrap(),
                    NonZeroUsize::new(disk_bytes).unwrap(),
                )
                .unwrap(),
            )),
        },
        index_operation_queue: db::config::IndexOperationQueueTuning::default(),
        cypher: crate::CypherEndpoints::Disabled,
    };
    let first = open_database(&config(128 * MIB)).await.unwrap();
    let partitions = files_below(&cache_root.join("slate"));
    assert_eq!(partitions.len(), 768);

    // A second server with a smaller block cache would delete partitions
    // 384..768 while the first still has them open. It stops before that.
    let smaller = config(64 * MIB);
    let Err(error) = open_database(&smaller).await else {
        panic!("a second server opened a cache directory in use");
    };
    assert!(
        matches!(
            error.downcast_ref::<ServerConfigError>(),
            Some(ServerConfigError::CacheDirectoryInUse { path }) if *path == cache_root
        ),
        "{error:?}"
    );
    assert!(error.to_string().starts_with("HELIX_DISK_CACHE_DIR"));
    assert_eq!(files_below(&cache_root.join("slate")), partitions);

    // Closing the first server frees the directory.
    first.db.close().await.unwrap();
    drop(first);
    let ServerDatabase { db, cache_lock } = open_database(&smaller).await.unwrap();
    assert_eq!(files_below(&cache_root.join("slate")).len(), 384);
    db.close().await.unwrap();
    drop(db);
    drop(cache_lock);

    // The server runners hold the lock until storage has closed.
    run_with_shutdown(smaller.clone(), async {}).await.unwrap();
    open_database(&smaller)
        .await
        .unwrap()
        .db
        .close()
        .await
        .unwrap();
    #[cfg(unix)]
    rustix::process::setrlimit(rustix::process::Resource::Nofile, original_limit).unwrap();
}

#[cfg(feature = "async-index-benchmark")]
#[tokio::test]
async fn a_benchmark_writer_holds_its_hybrid_cache_directory_like_the_server() {
    const MIB: usize = 1024 * 1024;
    #[cfg(unix)]
    let _limit = OPEN_FILE_LIMIT.lock().await;
    #[cfg(unix)]
    let original_limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    let directory = tempfile::tempdir().unwrap();
    let data_root = directory.path().join("data");
    std::fs::create_dir_all(&data_root).unwrap();
    let cache_root = directory.path().join("cache");
    let config = ServerConfig {
        http_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_addr: "127.0.0.1:0".parse().unwrap(),
        db_path: "benchmark-cache-lock".to_string(),
        storage: StorageConfig::Disk {
            root: data_root,
            cache: CacheConfig::Hybrid(Box::new(
                HybridCache::try_new(
                    &cache_root,
                    NonZeroUsize::new(16 * MIB).unwrap(),
                    NonZeroUsize::new(128 * MIB).unwrap(),
                )
                .unwrap(),
            )),
        },
        index_operation_queue: db::config::IndexOperationQueueTuning::default(),
        cypher: crate::CypherEndpoints::Disabled,
    };
    let benchmark = crate::benchmark::open_database(
        crate::benchmark::Role::Writer,
        db::config::QueueLayout::Map,
        &config,
    )
    .await
    .unwrap();
    assert!(
        !files_below(&cache_root.join("slate")).is_empty(),
        "the benchmark writer opens storage through the configured disk cache"
    );
    let Err(error) = open_database(&config).await else {
        panic!("a server opened a cache directory the benchmark writer holds");
    };
    assert!(
        matches!(
            error.downcast_ref::<ServerConfigError>(),
            Some(ServerConfigError::CacheDirectoryInUse { path }) if *path == cache_root
        ),
        "{error:?}"
    );

    benchmark.db.close().await.unwrap();
    drop(benchmark);
    open_database(&config)
        .await
        .unwrap()
        .db
        .close()
        .await
        .unwrap();
    #[cfg(unix)]
    rustix::process::setrlimit(rustix::process::Resource::Nofile, original_limit).unwrap();
}

#[cfg(unix)]
#[test]
fn open_file_limit_rises_to_the_hard_limit_or_fails_below_the_minimum() {
    use rustix::process::{getrlimit, setrlimit, Resource, Rlimit};

    let _limit = OPEN_FILE_LIMIT.blocking_lock();
    let original = getrlimit(Resource::Nofile);
    // With no hard limit anywhere there is nothing to raise to.
    let Some(hard) = original
        .maximum
        .into_iter()
        .chain(max_files_per_process())
        .min()
    else {
        return;
    };
    // One below the hard limit runs the raise path without starving tests
    // that share this process.
    setrlimit(
        Resource::Nofile,
        Rlimit {
            current: Some(hard - 1),
            maximum: original.maximum,
        },
    )
    .unwrap();

    ensure_open_file_limit(1).unwrap();
    let raised = getrlimit(Resource::Nofile);
    assert_eq!(
        raised,
        Rlimit {
            current: Some(hard),
            maximum: original.maximum,
        },
        "a minimum far below the hard limit still raises the soft limit to it"
    );
    ensure_open_file_limit(hard).unwrap();
    assert_eq!(getrlimit(Resource::Nofile), raised);

    let error = ensure_open_file_limit(hard + 1).unwrap_err();
    assert!(matches!(
        error,
        ServerConfigError::OpenFileLimit { required, limit } if required == hard + 1 && limit == hard
    ));
    assert!(error.to_string().starts_with("HELIX_DISK_CACHE_BYTES"));
    assert_eq!(getrlimit(Resource::Nofile), raised);
    setrlimit(Resource::Nofile, original).unwrap();
}

#[test]
fn tracing_initialization_is_idempotent() {
    init_tracing_from_env();
    init_tracing_from_env();
}
