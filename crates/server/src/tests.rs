use std::net::TcpListener;
use std::num::NonZeroUsize;
use std::path::Path;
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
                .body(Body::from(sonic_rs::to_vec(request).unwrap()))
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

/// Counts regular files below `path`, recursing through subdirectories.
fn file_count(path: &Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| if path.is_dir() { file_count(&path) } else { 1 })
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_disk_cache_serves_queries_and_keeps_cache_files_across_restart() {
    const MIB: usize = 1024 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let data_root = directory.path().join("data");
    std::fs::create_dir_all(&data_root).unwrap();
    let cache_root = directory.path().join("cache");
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
            root: data_root,
            cache: CacheConfig::Hybrid(cache),
        },
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

    let db = Arc::new(
        HelixDB::open_for_server(config.db_source(), config.db_config())
            .await
            .unwrap(),
    );
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
    let router = http::router(ServerState::new(Arc::clone(&db), None));
    post_query(router.clone(), &write).await;
    assert_eq!(post_query(router, &read).await["count"], 1);
    db.close().await.unwrap();

    // 24 MiB of Foyer disk tier in its minimum 64 KiB partitions.
    assert_eq!(
        std::fs::read_dir(cache_root.join("slate"))
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("foyer-storage-direct-fs-")
            })
            .count(),
        384
    );
    assert!(
        file_count(&cache_root.join("object-store")) > 0,
        "the SST flushed on close is cached on local disk"
    );
    assert!(cache_root.join("fts").is_dir());

    let reopened = Arc::new(
        HelixDB::open_for_server(config.db_source(), config.db_config())
            .await
            .unwrap(),
    );
    let router = http::router(ServerState::new(Arc::clone(&reopened), None));
    assert_eq!(post_query(router, &read).await["count"], 1);
    reopened.close().await.unwrap();

    run_with_shutdown(config, async {}).await.unwrap();
}

#[test]
fn tracing_initialization_is_idempotent() {
    init_tracing_from_env();
    init_tracing_from_env();
}
