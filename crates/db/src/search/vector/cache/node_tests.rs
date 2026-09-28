//! Node-level vector cache contracts through the public query path.
//!
//! These scenarios open real writer and reader nodes over one process-local
//! object store, build a managed vector index through DDL, and observe the
//! traversal-scoped search counters. `simhash_row_requests` counts only rows
//! fetched from storage, so zero proves the resident cache served every
//! candidate while the returned ranking proves the answer is still fresh.

use std::num::NonZeroUsize;
use std::time::Duration;

use helix_ast::prelude::*;

use crate::search::vector::hnsw::restricted::{observe_restricted_search, RestrictedSearchStats};
use crate::{HelixDB, HelixDbError, HelixDbSource, ProcessLocalDatabaseToken};

/// Creates the managed `Doc.embedding` index and waits until it is Active.
async fn create_vector_index(writer: &HelixDB) {
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
                        None::<String>,
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

/// Adds one `Doc` reachable from the scoping `Group`.
async fn add_doc(writer: &HelixDB, name: &str, embedding: [f32; 3]) {
    writer
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
                ),
        ))
        .await
        .expect("doc commits");
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
    let (result, stats) = observe_restricted_search(db.query(request)).await;
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

/// Returns the resident store of the node's only Active vector generation.
fn resident_store(db: &HelixDB) -> std::sync::Arc<crate::search::vector::VectorMemoryStore> {
    let active = db
        .active_index_handles_loaded(crate::encoding::keys::scope::DataScope::LegacyUnscoped)
        .into_iter()
        .find(|handle| {
            matches!(
                handle,
                crate::index_lifecycle::ActiveIndexHandle::Vector { .. }
            )
        })
        .expect("the fixture owns one Active vector generation");
    let crate::index_lifecycle::ActiveIndexHandle::Vector {
        layout: crate::index_lifecycle::VectorPhysicalLayout::Unpartitioned { physical_index_id },
        ..
    } = &active
    else {
        panic!("the fixture vector index is unpartitioned");
    };
    let generation =
        crate::search::vector::ValidatedVectorGenerationHandle::try_from_active_current(
            &active,
            *physical_index_id,
        )
        .expect("the Active generation validates");
    std::sync::Arc::clone(
        db.vector_cache_registry()
            .resident_guard_for(&generation)
            .expect("the Active generation is hydrated")
            .store(),
    )
}

#[tokio::test]
async fn writer_cache_stays_attached_across_commits_and_evicts_only_vector_rows() {
    let token = ProcessLocalDatabaseToken::new("writer-vector-cache").unwrap();
    let writer = HelixDB::open(HelixDbSource::InMemoryToken { token })
        .await
        .expect("writer opens");
    create_vector_index(&writer).await;
    create_group(&writer).await;
    for (name, embedding) in [
        ("far", [0.0, 0.0, 1.0]),
        ("mid", [0.5, 1.0, 0.0]),
        ("near", [1.0, 0.2, 0.0]),
    ] {
        add_doc(&writer, name, embedding).await;
    }
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
    assert!(std::sync::Arc::ptr_eq(&hydrated, &resident_store(&writer)));

    // A vector commit evicts only its dirty rows; the store stays attached and
    // the new row falls back to storage until the next refresh rescans.
    add_doc(&writer, "nearest", [1.0, 0.0, 0.0]).await;
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["nearest", "near", "mid"]);
    assert!(
        (1..4).contains(&stats.simhash_row_requests),
        "only rows the commit changed are read from storage, got {}",
        stats.simhash_row_requests
    );
    refresh_until_published(&writer).await;
    assert!(
        !std::sync::Arc::ptr_eq(&hydrated, &resident_store(&writer)),
        "a resolved vector commit forces a rescan that caches its rows"
    );
    let (names, stats) = scoped_search(&writer).await;
    assert_eq!(names, ["nearest", "near", "mid"]);
    assert_eq!(stats.simhash_row_requests, 0);

    writer.close().await.expect("writer closes");
}

#[tokio::test]
async fn reader_cache_serves_scoped_search_and_follows_writer_commits() {
    let token = ProcessLocalDatabaseToken::new("reader-vector-cache").unwrap();
    let writer = HelixDB::open(HelixDbSource::InMemoryToken {
        token: token.clone(),
    })
    .await
    .expect("writer opens");
    create_vector_index(&writer).await;
    create_group(&writer).await;
    for (name, embedding) in [
        ("far", [0.0, 0.0, 1.0]),
        ("mid", [0.5, 1.0, 0.0]),
        ("near", [1.0, 0.2, 0.0]),
    ] {
        add_doc(&writer, name, embedding).await;
    }
    writer
        .inner_db()
        .flush()
        .await
        .expect("fixture becomes reader-visible");

    let reader = HelixDB::open_reader(HelixDbSource::InMemoryToken { token })
        .await
        .expect("reader opens");
    reader.wait_for_startup_cache_warm().await;
    let (names, _) = search_until(&reader, |_, stats| stats.simhash_row_requests == 0).await;
    assert_eq!(names, ["near", "mid", "far"]);

    // The background loop republishes after the reader applies a writer commit.
    add_doc(&writer, "nearest", [1.0, 0.0, 0.0]).await;
    let (names, _) = search_until(&reader, |names, stats| {
        names.first().map(String::as_str) == Some("nearest") && stats.simhash_row_requests == 0
    })
    .await;
    assert_eq!(names, ["nearest", "near", "mid"]);

    // Without a refresh, a newer reader sequence falls back to storage and stays fresh.
    stop_background_refresh(&reader).await;
    add_doc(&writer, "second", [1.0, 0.1, 0.0]).await;
    let (names, stats) = search_until(&reader, |names, _| {
        names.iter().any(|name| name == "second")
    })
    .await;
    assert_eq!(names, ["nearest", "second", "near"]);
    assert!(
        stats.simhash_row_requests > 0,
        "an exact-sequence store is never attached to a newer reader snapshot"
    );
    refresh_until_published(&reader).await;
    let (names, stats) = scoped_search(&reader).await;
    assert_eq!(names, ["nearest", "second", "near"]);
    assert_eq!(stats.simhash_row_requests, 0);

    reader.close().await.expect("reader closes");
    writer.close().await.expect("writer closes");
}
