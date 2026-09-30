//! Node-level vector cache contracts through the public query path.
//!
//! These scenarios open real writer and reader nodes over one process-local
//! object store, build a managed vector index through DDL, and observe the
//! traversal-scoped search counters. `simhash_row_requests` counts only rows
//! fetched from storage, so zero proves the resident cache served every
//! candidate while the returned ranking proves the answer is still fresh.
//!
//! A graph write only queues its index operations; the writer's queue
//! publisher changes physical vector rows in a later commit of its own. Every
//! writer here opens with automatic publication paused and drains its queue
//! explicitly, so each scenario decides when physical rows change: a queued
//! write fences no cached row and the search overlay serves it, and only a
//! publication commit evicts the rows it rewrote.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use helix_ast::prelude::*;

use crate::config::IndexOperationQueueTuning;
use crate::encoding::v2::values::indexes::operation_queue::QueueFamily;
use crate::index_lifecycle::queue::publication::PublicationOutcome;
use crate::index_lifecycle::{ActiveIndexHandle, VectorPhysicalLayout};
use crate::search::vector::hnsw::restricted::{observe_restricted_search, RestrictedSearchStats};
use crate::search::vector::{ValidatedVectorGenerationHandle, VectorMemoryStore};
use crate::{DatabaseSequence, HelixDB, HelixDbError, HelixDbSource, ProcessLocalDatabaseToken};

/// Scoped fixture documents; `near` is nearest the search query.
const FIXTURE: [(&str, [f32; 3]); 3] = [
    ("far", [0.0, 0.0, 1.0]),
    ("mid", [0.5, 1.0, 0.0]),
    ("near", [1.0, 0.2, 0.0]),
];

/// Opens a writer whose queued index operations wait for [`drain`].
async fn open_paused_writer(token: ProcessLocalDatabaseToken) -> HelixDB {
    let source = HelixDbSource::InMemoryToken { token };
    let config = source
        .embedded_default_config()
        .with_index_operation_queue_tuning(
            IndexOperationQueueTuning::default().with_publication_paused_for_tests(),
        );
    HelixDB::open_with_config(source, config)
        .await
        .expect("writer opens")
}

/// Creates the managed `Doc.embedding` index and waits until it is Active.
async fn create_vector_index(writer: &HelixDB, tenant_property: Option<&str>) {
    let receipts = writer
        .query(QueryRequest::write(
            write_batch()
                .var_as(
                    "vector",
                    g().create_vector_index_nodes(
                        "Doc",
                        "embedding",
                        NonZeroUsize::new(3).expect("fixture dimension is non-zero"),
                        VectorDistanceMetric::Euclidean,
                        tenant_property,
                    ),
                )
                .returning(["vector"]),
        ))
        .await
        .expect("vector index DDL is accepted");
    let operation_id = receipts["vector"]["operation_id"]
        .as_str()
        .expect("vector index DDL returns an operation")
        .to_string();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = writer
                .query(QueryRequest::read(
                    read_batch()
                        .var_as("op", g().get_index_operation(operation_id.clone()))
                        .returning(["op"]),
                ))
                .await
                .expect("vector index operation status loads")
                .to_string();
            if status.contains("succeeded") {
                break;
            }
            assert!(
                !status.contains("blocked") && !status.contains("aborted"),
                "vector index build failed: {status}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("vector index activates");
}

/// Creates the one `Group` whose `HAS` edges scope every search.
async fn create_group(writer: &HelixDB) {
    writer
        .query(QueryRequest::write(write_batch().var_as(
            "group",
            g().add_n("Group", vec![("name", PropertyValue::from("scope"))]),
        )))
        .await
        .expect("group commits");
}

/// Adds one `Doc` reachable from the scoping `Group` and returns its ID.
async fn add_doc(writer: &HelixDB, name: &str, embedding: [f32; 3]) -> u64 {
    let result = writer
        .query(QueryRequest::write(
            write_batch()
                .var_as("group", g().n_with_label("Group"))
                .var_as(
                    "doc",
                    g().add_n(
                        "Doc",
                        vec![
                            ("name", PropertyValue::from(name)),
                            ("embedding", PropertyValue::from(embedding.to_vec())),
                        ],
                    ),
                )
                .var_as(
                    "edge",
                    g().n(NodeRef::var("group")).add_e(
                        "HAS",
                        NodeRef::var("doc"),
                        Vec::<(String, PropertyValue)>::new(),
                    ),
                )
                .returning(["doc"]),
        ))
        .await
        .expect("doc commits");
    result["doc"][0]["$id"]
        .as_u64()
        .expect("the created doc returns its ID")
}

/// Publishes the writer's vector queue until it is empty.
async fn drain(writer: &HelixDB) {
    let target = crate::index_lifecycle::queue::tests::target(writer, QueueFamily::Vector).await;
    let publisher = writer
        .index_queue_publisher()
        .expect("writer runs a queue publisher");
    for _ in 0..1_000 {
        match publisher
            .publish_once(target)
            .await
            .expect("publication succeeds")
        {
            PublicationOutcome::Empty => {
                assert_eq!(writer.index_operation_queue_stats().pending_operations, 0);
                return;
            }
            PublicationOutcome::Published { .. } | PublicationOutcome::Trimmed => {}
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Blocked) => panic!("publication stalled: {outcome:?}"),
        }
    }
    panic!("publication did not drain the vector queue");
}

/// Runs one traversal-scoped vector search and returns ranked names with its counters.
async fn scoped_search(db: &HelixDB) -> (Vec<String>, RestrictedSearchStats) {
    let request = QueryRequest::read(
        read_batch()
            .var_as(
                "r",
                g().n_with_label("Group")
                    .out(Some("HAS"))
                    .vector_search("Doc", "embedding", vec![1.0, 0.0, 0.0], 3, None)
                    .project(vec![PropertyProjection::new("name")]),
            )
            .returning(["r"]),
    );
    let (result, stats) = observe_restricted_search(Box::pin(db.query(request))).await;
    let names = result.expect("scoped vector search succeeds")["r"]
        .as_array()
        .expect("scoped vector search returns rows")
        .iter()
        .map(|row| {
            row["name"]
                .as_str()
                .expect("every doc projects its name")
                .to_string()
        })
        .collect();
    (
        names,
        stats.expect("scoped vector search records restricted counters"),
    )
}

/// Runs one unrestricted vector search and returns ranked names.
async fn unrestricted_search(db: &HelixDB) -> Vec<String> {
    let request = QueryRequest::read(
        read_batch()
            .var_as(
                "r",
                g().vector_search_nodes("Doc", "embedding", vec![1.0, 0.0, 0.0], 3, None)
                    .project(vec![PropertyProjection::new("name")]),
            )
            .returning(["r"]),
    );
    Box::pin(db.query(request))
        .await
        .expect("unrestricted vector search succeeds")["r"]
        .as_array()
        .expect("unrestricted vector search returns rows")
        .iter()
        .map(|row| {
            row["name"]
                .as_str()
                .expect("every doc projects its name")
                .to_string()
        })
        .collect()
}

/// Repeats `scoped_search` until `done` accepts its outcome.
async fn search_until(
    db: &HelixDB,
    done: impl Fn(&[String], &RestrictedSearchStats) -> bool,
) -> (Vec<String>, RestrictedSearchStats) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let (names, stats) = scoped_search(db).await;
            if done(&names, &stats) {
                return (names, stats);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("scoped vector search reaches the expected state")
}

/// Stops the background refresh loop so a scenario controls every refresh.
async fn stop_background_refresh(db: &HelixDB) {
    db.inner
        .caches
        .vector_memory
        .refresh_task
        .lock()
        .await
        .take()
        .expect("the node owns a vector refresh task")
        .stop()
        .await;
}

/// Runs one manual refresh, retrying only a reader poller race.
async fn refresh_until_published(db: &HelixDB) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match db.refresh_vector_memory_cache().await {
                Ok(()) => break,
                Err(HelixDbError::RequestReadViewChanged) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => panic!("vector cache refresh failed: {error}"),
            }
        }
    })
    .await
    .expect("vector cache refresh completes");
}

/// Waits until `reader` applied at least `sequence`.
async fn wait_until_applied(reader: &HelixDB, sequence: DatabaseSequence) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while reader
            .visible_sequence()
            .await
            .expect("reader sequence loads")
            < sequence
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("reader applies the flushed writer state");
}

/// Flushes the writer and waits until `reader` applied everything it flushed.
async fn publish_to_reader(writer: &HelixDB, reader: &HelixDB) {
    let flushed = writer.flush_writer().await.expect("writer flushes");
    wait_until_applied(reader, flushed).await;
}

/// Returns the node's only Active vector generation.
fn active_vector(db: &HelixDB) -> ActiveIndexHandle {
    db.active_index_handles_loaded(crate::encoding::keys::scope::DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| matches!(handle, ActiveIndexHandle::Vector { .. }))
        .expect("the fixture owns one Active vector generation")
}

/// Returns the validated handle of the node's unpartitioned vector generation.
fn unpartitioned_generation(db: &HelixDB) -> ValidatedVectorGenerationHandle {
    let active = active_vector(db);
    let ActiveIndexHandle::Vector {
        layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
        ..
    } = &active
    else {
        panic!("the fixture vector index is unpartitioned");
    };
    ValidatedVectorGenerationHandle::try_from_active_current(&active, *physical_index_id)
        .expect("the Active generation validates")
}

/// Returns the resident store of the node's only Active vector generation.
fn resident_store(db: &HelixDB) -> Arc<VectorMemoryStore> {
    Arc::clone(
        db.vector_cache_registry()
            .resident_guard_for(&unpartitioned_generation(db))
            .expect("the Active generation is hydrated")
            .store(),
    )
}

/// Returns the store a request at the node's latest snapshot attaches, if any.
async fn attached_store(
    db: &HelixDB,
    generation: &ValidatedVectorGenerationHandle,
) -> Option<Arc<VectorMemoryStore>> {
    let sequence = db
        .visible_sequence()
        .await
        .expect("node sequence loads")
        .get();
    db.vector_cache_registry()
        .read_guard_for(generation, sequence)
        .ok()
        .map(|guard| Arc::clone(guard.store()))
}

#[tokio::test]
async fn writer_cache_stays_attached_across_commits_and_evicts_only_vector_rows() {
    let writer =
        open_paused_writer(ProcessLocalDatabaseToken::new("writer-vector-cache").unwrap()).await;
    create_vector_index(&writer, None).await;
    create_group(&writer).await;
    for (name, embedding) in FIXTURE {
        add_doc(&writer, name, embedding).await;
    }
    drain(&writer).await;
    stop_background_refresh(&writer).await;
    refresh_until_published(&writer).await;
    let hydrated = resident_store(&writer);
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["near", "mid", "far"]);
    assert_eq!(stats.simhash_row_requests, 0);

    // A commit that touches no vector row advances the snapshot but keeps the
    // store attached, and the next refresh retains it without rescanning.
    writer
        .query(QueryRequest::write(write_batch().var_as(
            "unrelated",
            g().add_n("Unrelated", vec![("name", PropertyValue::from("other"))]),
        )))
        .await
        .expect("unrelated write commits");
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["near", "mid", "far"]);
    assert_eq!(
        stats.simhash_row_requests, 0,
        "a newer writer snapshot still attaches the commit-fenced store"
    );
    refresh_until_published(&writer).await;
    assert!(Arc::ptr_eq(&hydrated, &resident_store(&writer)));

    // A queued vector write fences no cached row: the overlay serves the
    // pending doc beside cached rows, and the next refresh keeps the store.
    add_doc(&writer, "nearest", [1.0, 0.0, 0.0]).await;
    assert_eq!(writer.index_operation_queue_stats().pending_operations, 1);
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["nearest", "near", "mid"]);
    assert_eq!(
        stats.simhash_row_requests, 0,
        "a queued write leaves every cached row attached"
    );
    refresh_until_published(&writer).await;
    assert!(
        Arc::ptr_eq(&hydrated, &resident_store(&writer)),
        "a queued write forces no rescan"
    );

    // A publication commit evicts only its dirty rows; the store stays
    // attached and the new row falls back to storage until the next refresh.
    drain(&writer).await;
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["nearest", "near", "mid"]);
    assert!(
        (1..4).contains(&stats.simhash_row_requests),
        "only rows the publication changed are read from storage, got {}",
        stats.simhash_row_requests
    );
    let attached = attached_store(&writer, &unpartitioned_generation(&writer))
        .await
        .expect("the publication commit keeps the store attachable");
    assert!(Arc::ptr_eq(&hydrated, &attached));
    drop(attached);
    refresh_until_published(&writer).await;
    assert!(
        !Arc::ptr_eq(&hydrated, &resident_store(&writer)),
        "a resolved publication commit forces a rescan that caches its rows"
    );
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["nearest", "near", "mid"]);
    assert_eq!(stats.simhash_row_requests, 0);

    writer.close().await.expect("writer closes");
}

#[tokio::test]
async fn writer_cache_overlays_queued_updates_and_deletes_until_publication() {
    let writer = open_paused_writer(
        ProcessLocalDatabaseToken::new("writer-vector-cache-update-delete").unwrap(),
    )
    .await;
    create_vector_index(&writer, None).await;
    create_group(&writer).await;
    let mut ids = Vec::new();
    for (name, embedding) in FIXTURE {
        ids.push(add_doc(&writer, name, embedding).await);
    }
    add_doc(&writer, "other", [0.0, 1.0, 1.0]).await;
    let [far, _, near] = ids[..] else {
        unreachable!("the fixture adds three docs")
    };
    drain(&writer).await;
    stop_background_refresh(&writer).await;
    refresh_until_published(&writer).await;
    let generation = unpartitioned_generation(&writer);
    let hydrated = resident_store(&writer);
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["near", "mid", "far"]);
    assert_eq!(stats.simhash_row_requests, 0);
    assert_eq!(unrestricted_search(&writer).await, ["near", "mid", "far"]);

    // Re-embed `far` onto the query and delete `near`; both stay queued.
    writer
        .query(QueryRequest::write(
            write_batch().var_as(
                "updated",
                g().n(NodeRef::from(far))
                    .set_property("embedding", vec![1.0_f32, 0.0, 0.0]),
            ),
        ))
        .await
        .expect("update commits");
    writer
        .query(QueryRequest::write(
            write_batch().var_as("dropped", g().n(NodeRef::from(near)).drop()),
        ))
        .await
        .expect("delete commits");
    assert_eq!(writer.index_operation_queue_stats().pending_operations, 2);

    // `far`'s cached physical row is superseded: the restricted search drops
    // it from its candidates and ranks the pending vector first, while every
    // other candidate is still served from the cache.
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["far", "mid", "other"]);
    assert_eq!(stats.simhash_row_requests, 0);
    // The unrestricted search suppresses both superseded physical rows and
    // widens past them through the attached store.
    assert_eq!(unrestricted_search(&writer).await, ["far", "mid", "other"]);
    let attached = attached_store(&writer, &generation)
        .await
        .expect("queued writes keep the store attachable");
    assert!(Arc::ptr_eq(&hydrated, &attached));
    drop(attached);
    refresh_until_published(&writer).await;
    assert!(
        Arc::ptr_eq(&hydrated, &resident_store(&writer)),
        "queued writes force no rescan"
    );

    // Publication rewrites `far` and removes `near`: only their rows leave
    // the attached store, `far` returns with its new vector, and the deleted
    // doc never reappears.
    drain(&writer).await;
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["far", "mid", "other"]);
    assert!(
        (1..3).contains(&stats.simhash_row_requests),
        "only the rewritten row is read from storage, got {}",
        stats.simhash_row_requests
    );
    assert_eq!(unrestricted_search(&writer).await, ["far", "mid", "other"]);
    let attached = attached_store(&writer, &generation)
        .await
        .expect("the publication commit keeps the store attachable");
    assert!(Arc::ptr_eq(&hydrated, &attached));
    drop(attached);
    refresh_until_published(&writer).await;
    assert!(!Arc::ptr_eq(&hydrated, &resident_store(&writer)));
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["far", "mid", "other"]);
    assert_eq!(stats.simhash_row_requests, 0);
    assert_eq!(unrestricted_search(&writer).await, ["far", "mid", "other"]);

    writer.close().await.expect("writer closes");
}

#[tokio::test]
async fn eventual_search_past_the_suppression_limit_matches_a_search_without_the_cache() {
    /// Returns `(id, distance)` of the `k` docs nearest `query`.
    async fn nearest(
        db: &HelixDB,
        query: [f32; 3],
        k: usize,
        consistency: SearchConsistency,
    ) -> crate::error::Result<Vec<(u64, f64)>> {
        let request = QueryRequest::read(
            read_batch()
                .var_as(
                    "hits",
                    g().vector_search_nodes("Doc", "embedding", query.to_vec(), k, None),
                )
                .returning(["hits"]),
        )
        .with_search_consistency(consistency)
        .expect("read requests accept either consistency");
        Ok(Box::pin(db.query(request)).await?["hits"]
            .as_array()
            .expect("vector search returns hits")
            .iter()
            .map(|hit| {
                (
                    hit["$id"].as_u64().expect("every hit has an ID"),
                    hit["$distance"].as_f64().expect("every hit has a distance"),
                )
            })
            .collect())
    }

    let writer =
        open_paused_writer(ProcessLocalDatabaseToken::new("writer-vector-cache-eventual").unwrap())
            .await;
    create_vector_index(&writer, None).await;
    // 802 docs on a line, written in batches of 100.
    let mut ids = Vec::with_capacity(802);
    for start in (0..802_u16).step_by(100) {
        let positions = start..(start + 100).min(802);
        let batch = positions.clone().fold(write_batch(), |batch, position| {
            batch.var_as(
                &format!("d{position}"),
                g().add_n(
                    "Doc",
                    vec![(
                        "embedding",
                        PropertyValue::from(vec![f32::from(position), 0.0, 0.0]),
                    )],
                ),
            )
        });
        let result = writer
            .query(QueryRequest::write(batch.returning(
                positions.clone().map(|position| format!("d{position}")),
            )))
            .await
            .expect("docs commit");
        ids.extend(positions.map(|position| {
            result[format!("d{position}")][0]["$id"]
                .as_u64()
                .expect("every created doc returns its ID")
        }));
    }
    drain(&writer).await;
    stop_background_refresh(&writer).await;
    refresh_until_published(&writer).await;
    let generation = unpartitioned_generation(&writer);
    let hydrated = resident_store(&writer);

    // 801 queued rewrites move the docs nearest the origin far away, so each
    // one's published result lies ahead of the answer.
    for start in (0..801_u16).step_by(100) {
        let batch = (start..(start + 100).min(801)).fold(write_batch(), |batch, position| {
            batch.var_as(
                &format!("d{position}"),
                g().n(NodeRef::from(ids[usize::from(position)]))
                    .set_property("embedding", vec![10_000.0 + f32::from(position), 0.0, 0.0]),
            )
        });
        writer
            .query(QueryRequest::write(batch))
            .await
            .expect("rewrites commit");
    }
    assert_eq!(writer.index_operation_queue_stats().pending_operations, 801);

    let error = nearest(&writer, [0.0; 3], 1, SearchConsistency::Strong)
        .await
        .expect_err("strong search never skips the limit's worth of superseded results");
    assert!(error.is_index_backpressure(), "unexpected error: {error}");
    // Eventual search overlays the oldest 800 rewrites and serves the newest
    // from its published row, through the attached store. Searches that need
    // no widening still overlay every rewrite.
    let queries = [
        ([0.0, 0.0, 0.0], 1),
        ([10_800.0, 0.0, 0.0], 1),
        ([10_000.0, 0.0, 0.0], 3),
    ];
    let mut cached = Vec::new();
    for (query, k) in queries {
        cached.push(
            nearest(&writer, query, k, SearchConsistency::Eventual)
                .await
                .expect("eventual search degrades instead of failing"),
        );
    }
    assert_eq!(cached[0], [(ids[800], 640_000.0)]);
    assert_eq!(cached[1], [(ids[800], 0.0)]);
    assert_eq!(
        cached[2].iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        ids[..3]
    );
    let attached = attached_store(&writer, &generation)
        .await
        .expect("queued rewrites keep the store attachable");
    assert!(Arc::ptr_eq(&hydrated, &attached));
    drop(attached);

    // Without the cache, every search reads storage and answers the same.
    writer.vector_cache_registry().retire(&generation).await;
    assert!(attached_store(&writer, &generation).await.is_none());
    for ((query, k), cached) in queries.into_iter().zip(cached) {
        assert_eq!(
            nearest(&writer, query, k, SearchConsistency::Eventual)
                .await
                .expect("eventual search degrades instead of failing"),
            cached,
            "{query:?}"
        );
    }

    writer.close().await.expect("writer closes");
}

#[tokio::test]
async fn reader_cache_serves_scoped_search_and_follows_publication_commits() {
    let token = ProcessLocalDatabaseToken::new("reader-vector-cache").unwrap();
    let writer = open_paused_writer(token.clone()).await;
    create_vector_index(&writer, None).await;
    create_group(&writer).await;
    for (name, embedding) in FIXTURE {
        add_doc(&writer, name, embedding).await;
    }
    drain(&writer).await;
    let published = writer.flush_writer().await.expect("writer flushes");

    let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
        .await
        .expect("reader opens");
    reader.wait_for_startup_cache_warm().await;
    wait_until_applied(&reader, published).await;
    let (names, _) = search_until(&reader, |_, stats| stats.simhash_row_requests == 0).await;
    assert_eq!(names, ["near", "mid", "far"]);

    // The background loop republishes after the reader applies the
    // publication commit.
    add_doc(&writer, "nearest", [1.0, 0.0, 0.0]).await;
    drain(&writer).await;
    publish_to_reader(&writer, &reader).await;
    let (names, _) = search_until(&reader, |names, stats| {
        names.first().map(String::as_str) == Some("nearest") && stats.simhash_row_requests == 0
    })
    .await;
    assert_eq!(names, ["nearest", "near", "mid"]);

    // Without a refresh, a newer reader sequence falls back to storage while
    // the overlay serves the queued doc.
    stop_background_refresh(&reader).await;
    add_doc(&writer, "second", [1.0, 0.1, 0.0]).await;
    publish_to_reader(&writer, &reader).await;
    let (names, stats) = scoped_search(&reader).await;
    assert_eq!(names, ["nearest", "second", "near"]);
    assert!(
        stats.simhash_row_requests > 0,
        "an exact-sequence store is never attached to a newer reader snapshot"
    );
    // A store hydrated at the queued write's sequence serves every published
    // row, and the overlay still supplies the queued doc.
    refresh_until_published(&reader).await;
    let (names, stats) = scoped_search(&reader).await;
    assert_eq!(names, ["nearest", "second", "near"]);
    assert_eq!(stats.simhash_row_requests, 0);

    // The publication commit advances the reader past that store: it falls
    // back to storage with the same answer until the next refresh.
    drain(&writer).await;
    publish_to_reader(&writer, &reader).await;
    let (names, stats) = scoped_search(&reader).await;
    assert_eq!(names, ["nearest", "second", "near"]);
    assert!(
        stats.simhash_row_requests > 0,
        "a publication commit leaves the reader store behind its snapshot"
    );
    refresh_until_published(&reader).await;
    let (names, stats) = scoped_search(&reader).await;
    assert_eq!(names, ["nearest", "second", "near"]);
    assert_eq!(stats.simhash_row_requests, 0);

    reader.close().await.expect("reader closes");
    writer.close().await.expect("writer closes");
}

#[tokio::test]
async fn publication_that_reclaims_a_partition_retires_its_cache_store() {
    /// Returns the handle of the generation's only mapped tenant partition
    /// and its mapping row.
    async fn only_partition(
        db: &HelixDB,
    ) -> (ValidatedVectorGenerationHandle, bytes::Bytes, bytes::Bytes) {
        use crate::encoding::v2::keys::{ManagedIndexKey, RecordKind, ScopedKey};

        let active = active_vector(db);
        let ActiveIndexHandle::Vector {
            scope,
            index_id,
            generation,
            layout: VectorPhysicalLayout::Partitioned,
            ..
        } = &active
        else {
            panic!("the fixture vector index is partitioned");
        };
        let prefix = ManagedIndexKey::data_prefix(
            *scope,
            ScopedKey::generation_prefix(
                RecordKind::VectorPartitionMapping,
                *index_id,
                *generation,
            ),
        );
        let storage = db.inner_db();
        let mut rows = storage
            .scan_prefix(&prefix, ..)
            .await
            .expect("partition mappings scan");
        let row = rows
            .next()
            .await
            .expect("partition mapping loads")
            .expect("one tenant partition is mapped");
        assert!(
            rows.next()
                .await
                .expect("partition mappings scan")
                .is_none(),
            "only one tenant partition is mapped"
        );
        let mapping = crate::encoding::v2::values::decode_partition_mapping(&row.value)
            .expect("partition mapping decodes");
        let handle = ValidatedVectorGenerationHandle::try_from_active_current(
            &active,
            mapping.physical_index_id,
        )
        .expect("the partition generation validates");
        (handle, row.key, row.value)
    }

    /// Asserts the source store is still resident, still caches the doc's
    /// row, and is attachable.
    async fn assert_resident(
        db: &HelixDB,
        source: &ValidatedVectorGenerationHandle,
        store: &Arc<VectorMemoryStore>,
        id: u64,
    ) {
        let resident = db
            .vector_cache_registry()
            .resident_guard_for(source)
            .expect("the source store stays resident");
        assert!(Arc::ptr_eq(store, resident.store()));
        drop(resident);
        // The HNSW node ID is the entity ID.
        assert!(
            store.get_simhash(id).is_some(),
            "the doc's row stays cached"
        );
        let attached = attached_store(db, source)
            .await
            .expect("the source store stays attachable");
        assert!(Arc::ptr_eq(store, &attached));
    }

    let writer = open_paused_writer(
        ProcessLocalDatabaseToken::new("writer-vector-cache-partition-retirement").unwrap(),
    )
    .await;
    create_vector_index(&writer, Some("tenant")).await;
    let result = writer
        .query(QueryRequest::write(
            write_batch()
                .var_as(
                    "doc",
                    g().add_n(
                        "Doc",
                        vec![
                            ("embedding", PropertyValue::from(vec![1.0_f32, 0.0, 0.0])),
                            ("tenant", PropertyValue::from("a")),
                        ],
                    ),
                )
                .returning(["doc"]),
        ))
        .await
        .expect("doc commits");
    let id = result["doc"][0]["$id"]
        .as_u64()
        .expect("the created doc returns its ID");
    drain(&writer).await;
    stop_background_refresh(&writer).await;
    refresh_until_published(&writer).await;
    let (source, mapping_key, mapping_value) = only_partition(&writer).await;
    let store = Arc::clone(
        writer
            .vector_cache_registry()
            .resident_guard_for(&source)
            .expect("the tenant partition is hydrated")
            .store(),
    );

    // Moving the partition's only doc to another tenant stays queued.
    writer
        .query(QueryRequest::write(
            write_batch().var_as(
                "moved",
                g().n(NodeRef::from(id))
                    .set_property("tenant", "b".to_string()),
            ),
        ))
        .await
        .expect("tenant move commits");
    assert_resident(&writer, &source, &store, id).await;

    let target = crate::index_lifecycle::queue::tests::target(&writer, QueueFamily::Vector).await;
    let publisher = writer
        .index_queue_publisher()
        .expect("writer runs a queue publisher");
    // A publication that fails before its commit discards the retirement.
    publisher
        .hooks()
        .fail_before_commit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        publisher.publish_once(target).await.unwrap(),
        PublicationOutcome::Retry
    );
    assert_resident(&writer, &source, &store, id).await;

    // A publication that loses a commit conflict on the reclaimed mapping
    // discards the retirement, so the source store stays resident and
    // attachable. The source is retired rather than fenced, so this does not
    // cover fence release on a conflict.
    let (reached_tx, reached) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *publisher.hooks().before_commit.lock() = Some((reached_tx, release_rx));
    let conflicts = publisher
        .metrics()
        .commit_conflicts
        .load(std::sync::atomic::Ordering::Relaxed);
    let (outcome, ()) = tokio::join!(publisher.publish_once(target), async {
        reached.await.expect("the publication stages its effects");
        let competing = writer
            .inner_db()
            .begin(slatedb::IsolationLevel::Snapshot)
            .await
            .unwrap();
        competing.put(&mapping_key, mapping_value).unwrap();
        competing.commit().await.unwrap();
        release.send(()).expect("the publication waits to commit");
    });
    assert_eq!(outcome.unwrap(), PublicationOutcome::Retry);
    assert_eq!(
        publisher
            .metrics()
            .commit_conflicts
            .load(std::sync::atomic::Ordering::Relaxed),
        conflicts + 1,
        "the competing mapping write aborts the publication"
    );
    assert_resident(&writer, &source, &store, id).await;

    // A committed publication reclaims the emptied partition and retires its
    // store, then forgets the tombstone so the identity can hydrate again.
    drain(&writer).await;
    assert!(writer
        .vector_cache_registry()
        .resident_guard_for(&source)
        .is_err());
    assert_eq!(store.estimated_bytes(), 0, "retirement clears the store");
    assert!(
        store.get_simhash(id).is_none(),
        "retirement drops the doc's row"
    );
    let (destination, _, _) = only_partition(&writer).await;
    assert_ne!(
        destination.physical_index_id(),
        source.physical_index_id(),
        "only the destination tenant stays mapped"
    );
    let (_, owns_hydration) = writer.vector_cache_registry().entry_for(&source);
    assert!(
        owns_hydration,
        "the committed retirement forgets its tombstone"
    );

    writer.close().await.expect("writer closes");
}
