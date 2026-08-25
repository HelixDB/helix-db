use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::TryStreamExt;
use object_store_014::path::Path;
use object_store_014::ObjectStore;
use serde::Serialize;

#[derive(Serialize)]
struct ObjectClass {
    objects: u64,
    bytes: u64,
}

#[derive(Serialize)]
struct Audit {
    database: String,
    manifest: target_slatedb::manifest::VersionedManifest,
    object_classes: BTreeMap<String, ObjectClass>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let database = std::env::args()
        .nth(1)
        .context("usage: storage_audit DATABASE_PREFIX [--gc]")?;
    let run_gc = std::env::args().nth(2).as_deref() == Some("--gc");
    let endpoint =
        std::env::var("MINIO_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let bucket =
        std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "helix-migration-parity".to_string());
    let store: Arc<dyn ObjectStore> = Arc::new(
        object_store_014::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(&endpoint)
            .with_access_key_id(
                std::env::var("MINIO_ROOT_USER").unwrap_or_else(|_| "minioadmin".to_string()),
            )
            .with_secret_access_key(
                std::env::var("MINIO_ROOT_PASSWORD").unwrap_or_else(|_| "minioadmin".to_string()),
            )
            .with_allow_http(endpoint.starts_with("http://"))
            .with_virtual_hosted_style_request(false)
            .build()?,
    );
    let admin =
        target_slatedb::admin::AdminBuilder::new(database.as_str(), Arc::clone(&store)).build();
    if run_gc {
        let directory = target_slatedb::config::GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::ZERO,
            dry_run: false,
        };
        admin
            .run_gc_once(target_slatedb::config::GarbageCollectorOptions {
                manifest_options: Some(directory),
                wal_options: Some(directory),
                wal_fence_options: None,
                compacted_options: Some(directory),
                compactions_options: Some(directory),
                detach_options: None,
                metric_level: None,
                boundary_files_enabled: true,
                object_store_max_retries: None,
            })
            .await?;
    }
    let manifest = admin
        .read_manifest(None)
        .await?
        .context("database has no manifest")?;
    let prefix = Path::from(database.as_str());
    let mut objects = store.list(Some(&prefix));
    let mut object_classes = BTreeMap::<String, ObjectClass>::new();
    while let Some(object) = objects.try_next().await? {
        let relative = object
            .location
            .as_ref()
            .strip_prefix(database.as_str())
            .unwrap_or(object.location.as_ref())
            .trim_start_matches('/');
        let class = relative.split('/').next().unwrap_or("unknown").to_string();
        let entry = object_classes.entry(class).or_insert(ObjectClass {
            objects: 0,
            bytes: 0,
        });
        entry.objects = entry.objects.saturating_add(1);
        entry.bytes = entry.bytes.saturating_add(object.size);
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&Audit {
            database,
            manifest,
            object_classes,
        })?
    );
    Ok(())
}
