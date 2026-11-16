//! Proof, against a real Google Cloud Storage bucket, that GCS keeps the
//! storage contract SlateDB and HelixDB rely on.
//!
//! SlateDB fences an older writer with create-only puts, advances GC boundary
//! files with generation-matched overwrites and revalidates them with
//! conditional GETs, verifies a retried put through custom metadata, and lists
//! strongly consistently. The first tests check each primitive against GCS;
//! the last two open SlateDB and HelixDB on GCS end to end.
//!
//! Every test is ignored by default because it needs a bucket and
//! credentials (see [`db::ObjectStorageBucket::Gcs`]):
//!
//! ```text
//! HELIX_TEST_GCS_BUCKET=<bucket> GOOGLE_SERVICE_ACCOUNT=<key file> \
//!     cargo test -p db --test gcs_object_storage -- --ignored
//! ```
//!
//! Each test writes under its own random prefix and deletes it once it passes.

use std::sync::Arc;

use db::{HelixDB, HelixDbSource, ObjectStorageBucket};
use futures::{StreamExt as _, TryStreamExt as _};
use helix_ast::query::QueryRequest;
use helix_ast::value::PropertyInput;
use helix_ast::{batch, traversal};
use serde_json::json;
use slatedb::object_store::{self, ObjectStore, ObjectStoreExt as _};
use slatedb::{CloseReason, Db, ErrorKind, IsolationLevel};

const BUCKET_VARIABLE: &str = "HELIX_TEST_GCS_BUCKET";
/// GCS multipart uploads need every part but the last to be at least 5 MiB.
const PART_BYTES: usize = 5 * 1024 * 1024;

fn bucket() -> String {
    std::env::var(BUCKET_VARIABLE)
        .unwrap_or_else(|_| panic!("set {BUCKET_VARIABLE} to a GCS bucket"))
}

/// A GCS client for the test bucket and a fresh prefix inside it.
fn scratch() -> (Arc<dyn ObjectStore>, String) {
    let store = object_store::gcp::GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket())
        .build()
        .expect("GCS client builds");
    (
        Arc::new(store),
        format!("helix-gcs-contract/{}", uuid::Uuid::new_v4()),
    )
}

/// Deletes every object under `prefix`.
async fn remove(store: &dyn ObjectStore, prefix: &str) {
    let locations = store
        .list(Some(&object_store::path::Path::from(prefix)))
        .map_ok(|meta| meta.location)
        .boxed();
    store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
        .expect("scratch prefix deletes");
}

/// The put-ID metadata SlateDB stamps on every conditional put.
fn put_id(value: &'static str) -> object_store::Attributes {
    let mut attributes = object_store::Attributes::new();
    attributes.insert(
        object_store::Attribute::Metadata("slatedbputid".into()),
        value.into(),
    );
    attributes
}

/// A create-only put refuses an existing object, an overwrite must match the
/// current generation, and a GET conditioned on the current ETag reports the
/// object unchanged: the writer fence and the GC boundary file both rest on
/// these.
#[tokio::test]
#[ignore = "needs a GCS bucket in HELIX_TEST_GCS_BUCKET"]
async fn conditional_writes_fence_and_compare_and_swap() {
    let (store, prefix) = scratch();
    let path = object_store::path::Path::from(format!("{prefix}/boundary"));

    let created = store
        .put_opts(&path, "1".into(), object_store::PutMode::Create.into())
        .await
        .expect("create-only put of a new object");
    assert!(
        created.version.is_some(),
        "GCS returns the generation an overwrite must match"
    );
    let error = store
        .put_opts(&path, "x".into(), object_store::PutMode::Create.into())
        .await
        .expect_err("create-only put of an existing object");
    assert!(
        matches!(error, object_store::Error::AlreadyExists { .. }),
        "{error:?}"
    );

    let first = object_store::UpdateVersion {
        e_tag: created.e_tag.clone(),
        version: created.version.clone(),
    };
    let updated = store
        .put_opts(
            &path,
            "2".into(),
            object_store::PutMode::Update(first.clone()).into(),
        )
        .await
        .expect("overwrite matching the current generation");
    let error = store
        .put_opts(
            &path,
            "x".into(),
            object_store::PutMode::Update(first).into(),
        )
        .await
        .expect_err("overwrite matching a stale generation");
    assert!(
        matches!(error, object_store::Error::Precondition { .. }),
        "{error:?}"
    );

    let unchanged = store
        .get_opts(
            &path,
            object_store::GetOptions {
                if_none_match: updated.e_tag.clone(),
                ..Default::default()
            },
        )
        .await
        .expect_err("GET conditioned on the current ETag");
    assert!(
        matches!(unchanged, object_store::Error::NotModified { .. }),
        "{unchanged:?}"
    );
    let changed = store
        .get_opts(
            &path,
            object_store::GetOptions {
                if_none_match: created.e_tag,
                ..Default::default()
            },
        )
        .await
        .expect("GET conditioned on a stale ETag");
    assert_eq!(changed.bytes().await.unwrap(), "2");

    remove(store.as_ref(), &prefix).await;
}

/// SlateDB proves a timed-out put landed by reading back its put ID, so the
/// ID must survive both single and multipart uploads.
#[tokio::test]
#[ignore = "needs a GCS bucket in HELIX_TEST_GCS_BUCKET"]
async fn put_metadata_survives_single_and_multipart_uploads() {
    let (store, prefix) = scratch();
    let head = object_store::GetOptions {
        head: true,
        ..Default::default()
    };
    let key = object_store::Attribute::Metadata("slatedbputid".into());

    let single = object_store::path::Path::from(format!("{prefix}/single"));
    store
        .put_opts(
            &single,
            "wal".into(),
            object_store::PutOptions {
                mode: object_store::PutMode::Create,
                attributes: put_id("single-put"),
                ..Default::default()
            },
        )
        .await
        .expect("put with metadata");
    let read = store.get_opts(&single, head.clone()).await.unwrap();
    assert_eq!(
        read.attributes.get(&key).map(AsRef::as_ref),
        Some("single-put")
    );

    let multipart = object_store::path::Path::from(format!("{prefix}/multipart"));
    let mut upload = store
        .put_multipart_opts(
            &multipart,
            object_store::PutMultipartOptions {
                attributes: put_id("multipart-put"),
                ..Default::default()
            },
        )
        .await
        .expect("multipart upload starts");
    upload
        .put_part(vec![1; PART_BYTES].into())
        .await
        .expect("first part uploads");
    upload
        .put_part(vec![2; 1024].into())
        .await
        .expect("last part uploads");
    upload.complete().await.expect("multipart upload completes");
    let read = store.get_opts(&multipart, head).await.unwrap();
    assert_eq!(read.meta.size, (PART_BYTES + 1024) as u64);
    assert_eq!(
        read.attributes.get(&key).map(AsRef::as_ref),
        Some("multipart-put")
    );

    remove(store.as_ref(), &prefix).await;
}

/// A listing sees every object written before it, in key order, and an
/// offset listing starts after the offset key.
#[tokio::test]
#[ignore = "needs a GCS bucket in HELIX_TEST_GCS_BUCKET"]
async fn listings_are_consistent_and_honour_offsets() {
    let (store, prefix) = scratch();
    let names = ["a", "b", "c"];
    for name in names {
        store
            .put(
                &object_store::path::Path::from(format!("{prefix}/{name}")),
                name.into(),
            )
            .await
            .unwrap();
    }
    let listed = |metas: Vec<object_store::ObjectMeta>| {
        let mut names = metas
            .into_iter()
            .map(|meta| meta.location.filename().unwrap().to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    };

    let root = object_store::path::Path::from(prefix.as_str());
    let all = store.list(Some(&root)).try_collect().await.unwrap();
    assert_eq!(listed(all), names);
    let after_a = store
        .list_with_offset(
            Some(&root),
            &object_store::path::Path::from(format!("{prefix}/a")),
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed(after_a), ["b", "c"]);

    remove(store.as_ref(), &prefix).await;
}

/// SlateDB's single-writer epoch holds on GCS: a newer writer's open fences a
/// transaction the older writer already began (see `writer_fence_contract`).
#[tokio::test]
#[ignore = "needs a GCS bucket in HELIX_TEST_GCS_BUCKET"]
#[allow(clippy::disallowed_methods)] // Raw backend writes are the subject here.
async fn newer_slatedb_writer_fences_an_older_one_on_gcs() {
    let (store, prefix) = scratch();

    let old_writer = Db::open(prefix.as_str(), Arc::clone(&store))
        .await
        .expect("old writer opens");
    let old_transaction = old_writer
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("old transaction begins before the fence");
    old_transaction
        .put(b"old-writer", b"must-not-commit")
        .expect("old transaction stages a write");

    let new_writer = Db::open(prefix.as_str(), Arc::clone(&store))
        .await
        .expect("new writer claims its epoch before open returns");
    let new_transaction = new_writer
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("new writer can transact after claiming the epoch");
    new_transaction
        .put(b"new-writer", b"committed")
        .expect("new transaction stages a write");
    new_transaction
        .commit()
        .await
        .expect("new writer commits after its open fence");

    let error = old_transaction
        .commit()
        .await
        .expect_err("the old transaction must not commit after the newer writer opens");
    assert_eq!(error.kind(), ErrorKind::Closed(CloseReason::Fenced));
    assert_eq!(
        new_writer.get(b"new-writer").await.unwrap().as_deref(),
        Some(&b"committed"[..])
    );
    assert_eq!(new_writer.get(b"old-writer").await.unwrap(), None);

    new_writer.close().await.expect("new writer closes cleanly");
    remove(store.as_ref(), &prefix).await;
}

/// HelixDB opened through `ObjectStorageBucket::Gcs`: a second writer fences
/// the first, which can no longer commit, and a reader sees exactly what the
/// surviving writer committed.
#[tokio::test]
#[ignore = "needs a GCS bucket in HELIX_TEST_GCS_BUCKET"]
async fn helix_db_writes_fences_and_reads_through_a_gcs_source() {
    let (store, prefix) = scratch();
    let source = || HelixDbSource::ObjectStorage {
        database: prefix.clone(),
        bucket: ObjectStorageBucket::Gcs { name: bucket() },
    };
    let add = |name: &str| {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "added",
                    traversal::g()
                        .add_n("Item", vec![("name", PropertyInput::from(name))])
                        .count(),
                )
                .returning(["added"]),
        )
    };

    let old = HelixDB::open(source()).await.expect("first writer opens");
    old.query(add("old")).await.expect("first writer commits");
    let new = HelixDB::open(source())
        .await
        .expect("second writer opens and fences the first");
    old.query(add("stale"))
        .await
        .expect_err("a fenced writer cannot commit");
    new.query(add("new")).await.expect("second writer commits");
    new.close().await.expect("second writer closes");
    // The fenced writer may report its fence again while closing.
    let _ = old.close().await;

    let reader = HelixDB::open_reader(source()).await.expect("reader opens");
    let read = reader
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("count", traversal::g().n_with_label("Item").count())
                .var_as(
                    "names",
                    traversal::g()
                        .n_with_label("Item")
                        .value_map(Some(vec!["name"])),
                )
                .returning(["count", "names"]),
        ))
        .await
        .expect("reader reads");
    assert_eq!(read["count"], json!(2), "{read}");
    let mut names = read["names"]
        .as_array()
        .expect("value maps")
        .iter()
        .map(|row| row["name"].as_str().expect("name").to_owned())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, ["new", "old"]);
    reader.close().await.expect("reader closes");

    remove(store.as_ref(), &prefix).await;
}
