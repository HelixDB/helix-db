use std::path::PathBuf;
use std::sync::Arc;

use crate::error::HelixError;
use crate::graph::{graph_from_query_response, NativeGraph, NativeGraphError, NativeGraphLoadSpec};
use crate::runtime;

const OBJECT_STORE_PART_SIZE_BYTES: usize = 4 * 1024 * 1024;
const OBJECT_STORE_SCAN_INTERVAL_SECS: u64 = 60 * 60;
const OBJECT_STORE_MAX_OPEN_FILE_HANDLES: usize = 1_000;
const EMBEDDED_MEMORY_SLATE_BLOCK_BYTES: u64 = 48 * 1024 * 1024;
const EMBEDDED_MEMORY_SLATE_METADATA_BYTES: u64 = 16 * 1024 * 1024;

/// Portable cache profile accepted by every embedded SDK.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum EmbeddedCacheMode {
    /// Retain vector indexes in memory and disable SlateDB/object-store caches.
    VectorMemoryOnly,
    /// Retain vector indexes and use SlateDB's default in-memory cache.
    Memory,
    /// Add bounded disk-backed SlateDB and object-store cache tiers.
    Hybrid {
        slate_memory_bytes: u64,
        slate_disk_path: String,
        slate_disk_bytes: u64,
        object_store_disk_path: String,
        object_store_disk_bytes: u64,
    },
}

/// Cache configuration fixed for the lifetime of an embedded handle.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct EmbeddedCacheConfig {
    pub vector_memory_bytes: u64,
    pub mode: EmbeddedCacheMode,
}

impl TryFrom<EmbeddedCacheConfig> for db::config::CacheConfig {
    type Error = HelixError;

    fn try_from(value: EmbeddedCacheConfig) -> Result<Self, Self::Error> {
        let invalid = |msg: String| HelixError::InvalidConfig {
            error: helix_ast::error_code::QueryErrorCode::InvalidConfiguration.to_string(),
            msg,
        };
        let vector_memory = db::config::VectorMemorySettings::try_new(
            db::config::VectorMemoryBudget::bounded(value.vector_memory_bytes)
                .map_err(|error| invalid(error.to_string()))?,
            5,
        )
        .map_err(|error| invalid(error.to_string()))?;
        let mode = match value.mode {
            EmbeddedCacheMode::VectorMemoryOnly => db::config::CacheMode::VectorMemoryOnly,
            EmbeddedCacheMode::Memory => db::config::CacheMode::Memory {
                slate_db: db::config::SlateMemoryCacheConfig::try_new(
                    EMBEDDED_MEMORY_SLATE_BLOCK_BYTES,
                    EMBEDDED_MEMORY_SLATE_METADATA_BYTES,
                )
                .expect("embedded memory-cache capacities are nonzero"),
                slate_warm: Default::default(),
                fts: None,
            },
            EmbeddedCacheMode::Hybrid {
                slate_memory_bytes,
                slate_disk_path,
                slate_disk_bytes,
                object_store_disk_path,
                object_store_disk_bytes,
            } => {
                let to_usize = |name: &str, bytes: u64| {
                    usize::try_from(bytes).map_err(|_| {
                        invalid(format!("{name} exceeds this platform's addressable size"))
                    })
                };
                db::config::CacheMode::Hybrid {
                    slate_db: db::config::SlateHybridCacheConfig::try_new(
                        to_usize("Slate cache memory", slate_memory_bytes)?,
                        slate_disk_path,
                        to_usize("Slate cache disk capacity", slate_disk_bytes)?,
                    )
                    .map_err(|error| invalid(error.to_string()))?,
                    object_store: db::config::SlateObjectStoreCacheSettings::try_new(
                        object_store_disk_path,
                        Some(to_usize(
                            "object-store cache disk capacity",
                            object_store_disk_bytes,
                        )?),
                        OBJECT_STORE_PART_SIZE_BYTES,
                        false,
                        db::config::ObjectStoreWarmLevel::Off,
                        Some(std::time::Duration::from_secs(
                            OBJECT_STORE_SCAN_INTERVAL_SECS,
                        )),
                        OBJECT_STORE_MAX_OPEN_FILE_HANDLES,
                    )
                    .map_err(|error| invalid(error.to_string()))?,
                    slate_warm: Default::default(),
                    fts: None,
                }
            }
        };
        Ok(db::config::CacheConfig::new(vector_memory, mode))
    }
}

/// FFI-safe storage source for opening a HelixDB handle.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum HelixDbSource {
    /// In-memory object storage, scoped by logical database path.
    InMemory { database: String },
    /// Local filesystem object storage rooted at `root`, scoped by database path.
    Disk { root: String, database: String },
    /// S3-compatible object storage.
    ObjectStorage {
        database: String,
        bucket: String,
        region: String,
        endpoint: Option<String>,
        allow_http: bool,
    },
}

impl From<HelixDbSource> for db::HelixDbSource {
    fn from(value: HelixDbSource) -> Self {
        match value {
            HelixDbSource::InMemory { database } => Self::InMemory { database },
            HelixDbSource::Disk { root, database } => Self::Disk {
                root: PathBuf::from(root),
                database,
            },
            HelixDbSource::ObjectStorage {
                database,
                bucket,
                region,
                endpoint,
                allow_http,
            } => Self::ObjectStorage {
                database,
                bucket,
                region,
                endpoint,
                allow_http,
            },
        }
    }
}

/// In-process HelixDB handle.
#[derive(uniffi::Object)]
pub struct HelixDB {
    inner: Arc<db::HelixDB>,
}

impl HelixDB {
    fn new(inner: db::HelixDB) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl HelixDB {
    /// Open a read/write HelixDB handle.
    #[uniffi::constructor]
    pub async fn open(source: HelixDbSource) -> Result<Arc<Self>, HelixError> {
        let inner = runtime::run(db::HelixDB::open(source.into())).await??;
        Ok(Arc::new(Self::new(inner)))
    }

    /// Open a read/write handle with an explicit cache profile.
    #[uniffi::constructor]
    pub async fn open_with_config(
        source: HelixDbSource,
        cache: EmbeddedCacheConfig,
    ) -> Result<Arc<Self>, HelixError> {
        let cache = cache.try_into()?;
        let source = db::HelixDbSource::from(source);
        let config = source.embedded_default_config().with_cache(cache);
        let inner = runtime::run(db::HelixDB::open_with_config(source, config)).await??;
        Ok(Arc::new(Self::new(inner)))
    }

    /// Open a read-only HelixDB handle.
    #[uniffi::constructor]
    pub async fn open_reader(source: HelixDbSource) -> Result<Arc<Self>, HelixError> {
        let inner = runtime::run(db::HelixDB::open_reader(source.into())).await??;
        Ok(Arc::new(Self::new(inner)))
    }

    /// Open a read-only handle with an explicit cache profile.
    #[uniffi::constructor]
    pub async fn open_reader_with_config(
        source: HelixDbSource,
        cache: EmbeddedCacheConfig,
    ) -> Result<Arc<Self>, HelixError> {
        let cache = cache.try_into()?;
        let source = db::HelixDbSource::from(source);
        let config = source.embedded_default_config().with_cache(cache);
        let inner = runtime::run(db::HelixDB::open_reader_with_config(source, config)).await??;
        Ok(Arc::new(Self::new(inner)))
    }

    /// Execute an SDK-built query encoded as JSON bytes.
    pub async fn query_json(&self, request: Vec<u8>) -> Result<Vec<u8>, HelixError> {
        let inner = Arc::clone(&self.inner);
        Ok(runtime::run(async move { inner.query_json(&request).await }).await??)
    }

    /// Execute one ordinary read request and construct a reusable native graph.
    pub async fn graph(
        &self,
        request: Vec<u8>,
        spec: NativeGraphLoadSpec,
    ) -> Result<Arc<NativeGraph>, NativeGraphError> {
        let inner = Arc::clone(&self.inner);
        let response = runtime::run(async move { inner.query_json(&request).await })
            .await
            .map_err(|error| NativeGraphError::Query {
                message: HelixError::from(error).to_string(),
            })?
            .map_err(|error| NativeGraphError::Query {
                message: error.to_string(),
            })?;
        graph_from_query_response(spec, response)
    }

    /// Close the underlying storage handle.
    pub async fn close(&self) -> Result<(), HelixError> {
        let inner = Arc::clone(&self.inner);
        Ok(runtime::run(async move { inner.close().await }).await??)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use helix_ast::batch::read_batch;
    use helix_ast::query::QueryRequest;
    use helix_ast::traversal::g;

    use super::{
        EmbeddedCacheConfig, EmbeddedCacheMode, HelixDB, HelixDbSource,
        EMBEDDED_MEMORY_SLATE_BLOCK_BYTES, EMBEDDED_MEMORY_SLATE_METADATA_BYTES,
    };
    use crate::HelixError;

    #[test]
    fn embedded_cache_profiles_convert_to_validated_db_config() {
        for (mode, expected) in [
            (
                EmbeddedCacheMode::VectorMemoryOnly,
                db::config::CacheMode::VectorMemoryOnly,
            ),
            (
                EmbeddedCacheMode::Memory,
                db::config::CacheMode::Memory {
                    slate_db: db::config::SlateMemoryCacheConfig::try_new(
                        EMBEDDED_MEMORY_SLATE_BLOCK_BYTES,
                        EMBEDDED_MEMORY_SLATE_METADATA_BYTES,
                    )
                    .expect("embedded memory-cache capacities are nonzero"),
                    slate_warm: Default::default(),
                    fts: None,
                },
            ),
        ] {
            let config: db::config::CacheConfig = EmbeddedCacheConfig {
                vector_memory_bytes: 1_024,
                mode,
            }
            .try_into()
            .expect("portable profile should convert");
            assert_eq!(config.vector_memory().budget().bytes(), Some(1_024));
            assert_eq!(config.mode(), &expected);
        }

        let config: db::config::CacheConfig = EmbeddedCacheConfig {
            vector_memory_bytes: 2_048,
            mode: EmbeddedCacheMode::Hybrid {
                slate_memory_bytes: 4_096,
                slate_disk_path: "/tmp/helix-slate-cache".to_string(),
                slate_disk_bytes: 8_192,
                object_store_disk_path: "/tmp/helix-object-cache".to_string(),
                object_store_disk_bytes: 16_384,
            },
        }
        .try_into()
        .expect("hybrid profile should convert");
        let db::config::CacheMode::Hybrid {
            slate_db,
            object_store,
            ..
        } = config.mode()
        else {
            panic!("expected hybrid cache mode");
        };
        assert_eq!(slate_db.memory_bytes(), 4_096);
        assert_eq!(slate_db.disk().bytes(), 8_192);
        assert_eq!(
            object_store.root(),
            std::path::Path::new("/tmp/helix-object-cache")
        );
    }

    #[test]
    fn embedded_cache_profiles_reject_invalid_limits_and_paths() {
        let error = db::config::CacheConfig::try_from(EmbeddedCacheConfig {
            vector_memory_bytes: 0,
            mode: EmbeddedCacheMode::Memory,
        })
        .expect_err("zero vector budget must fail");
        assert!(matches!(error, HelixError::InvalidConfig { .. }));

        for mode in [
            EmbeddedCacheMode::Hybrid {
                slate_memory_bytes: 0,
                slate_disk_path: "/tmp/slate".to_string(),
                slate_disk_bytes: 1,
                object_store_disk_path: "/tmp/object".to_string(),
                object_store_disk_bytes: 1,
            },
            EmbeddedCacheMode::Hybrid {
                slate_memory_bytes: 1,
                slate_disk_path: String::new(),
                slate_disk_bytes: 1,
                object_store_disk_path: "/tmp/object".to_string(),
                object_store_disk_bytes: 1,
            },
            EmbeddedCacheMode::Hybrid {
                slate_memory_bytes: 1,
                slate_disk_path: "/tmp/slate".to_string(),
                slate_disk_bytes: 1,
                object_store_disk_path: String::new(),
                object_store_disk_bytes: 1,
            },
        ] {
            assert!(db::config::CacheConfig::try_from(EmbeddedCacheConfig {
                vector_memory_bytes: 1,
                mode,
            })
            .is_err());
        }
    }

    #[tokio::test]
    async fn configured_writer_and_reader_open_with_every_cache_profile() {
        let temporary = tempfile::tempdir().expect("temporary cache root");
        let modes = [
            EmbeddedCacheMode::VectorMemoryOnly,
            EmbeddedCacheMode::Memory,
            EmbeddedCacheMode::Hybrid {
                slate_memory_bytes: 4 * 1024 * 1024,
                slate_disk_path: temporary.path().join("slate").display().to_string(),
                slate_disk_bytes: 16 * 1024 * 1024,
                object_store_disk_path: temporary.path().join("object").display().to_string(),
                object_store_disk_bytes: 16 * 1024 * 1024,
            },
        ];

        for (index, mode) in modes.into_iter().enumerate() {
            let database_root = temporary.path().join(format!("database-{index}"));
            std::fs::create_dir_all(&database_root).expect("database root should exist");
            let source = HelixDbSource::Disk {
                root: database_root.display().to_string(),
                database: "configured-open".to_string(),
            };
            let cache = EmbeddedCacheConfig {
                vector_memory_bytes: 1024 * 1024,
                mode,
            };
            let writer = HelixDB::open_with_config(source.clone(), cache.clone())
                .await
                .expect("configured writer should open");
            assert_eq!(
                writer
                    .inner
                    .config()
                    .db()
                    .slate()
                    .to_writer_settings(None)
                    .flush_interval,
                Some(std::time::Duration::from_millis(3))
            );
            writer.close().await.expect("writer should close");

            let reader = HelixDB::open_reader_with_config(source, cache)
                .await
                .expect("configured reader should open");
            reader.close().await.expect("reader should close");
        }
    }

    #[tokio::test]
    async fn direct_helixdb_object_opens_queries_and_closes() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "uniffi-direct-query".to_string(),
        })
        .await
        .expect("in-memory DB should open");
        let request = QueryRequest::read(
            read_batch()
                .var_as("users", g().n_with_label("Missing").count())
                .returning(["users"]),
        )
        .to_json_bytes()
        .expect("request should serialize");

        let response = db
            .query_json(request)
            .await
            .expect("query_json should execute");
        let json: BTreeMap<String, u64> =
            sonic_rs::from_slice(&response).expect("response should decode");

        assert_eq!(json.get("users"), Some(&0));
        db.close().await.expect("DB should close");
    }

    #[test]
    fn source_conversion_preserves_all_variants() {
        let db::HelixDbSource::InMemory { database } = HelixDbSource::InMemory {
            database: "mem".to_string(),
        }
        .into() else {
            panic!("expected in-memory source");
        };
        assert_eq!(database, "mem");

        let db::HelixDbSource::Disk { root, database } = HelixDbSource::Disk {
            root: "/tmp/helix".to_string(),
            database: "disk".to_string(),
        }
        .into() else {
            panic!("expected disk source");
        };
        assert_eq!(root, std::path::PathBuf::from("/tmp/helix"));
        assert_eq!(database, "disk");

        let db::HelixDbSource::ObjectStorage {
            database,
            bucket,
            region,
            endpoint,
            allow_http,
        } = HelixDbSource::ObjectStorage {
            database: "os".to_string(),
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            allow_http: true,
        }
        .into()
        else {
            panic!("expected object-storage source");
        };
        assert_eq!(database, "os");
        assert_eq!(bucket, "bucket");
        assert_eq!(region, "region");
        assert_eq!(endpoint.as_deref(), Some("http://localhost:9000"));
        assert!(allow_http);
    }

    #[tokio::test]
    async fn direct_helixdb_query_json_maps_invalid_request_errors() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "uniffi-invalid-query".to_string(),
        })
        .await
        .expect("in-memory DB should open");

        let err = db
            .query_json(b"{".to_vec())
            .await
            .expect_err("invalid query JSON should fail");

        assert!(matches!(
            err,
            HelixError::InvalidRequest { error, msg }
                if error == "invalid_query_json"
                    && msg.starts_with("Query error: invalid query JSON:")
        ));
    }
}
