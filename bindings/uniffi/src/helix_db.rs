use std::path::PathBuf;
use std::sync::Arc;

use crate::error::HelixError;
use crate::graph::{graph_from_query_response, NativeGraph, NativeGraphError, NativeGraphLoadSpec};
use crate::runtime;

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
    inner: db::HelixDB,
}

impl HelixDB {
    fn new(inner: db::HelixDB) -> Self {
        Self { inner }
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl HelixDB {
    /// Open a read/write HelixDB handle.
    #[uniffi::constructor]
    pub async fn open(source: HelixDbSource) -> Result<Arc<Self>, HelixError> {
        let inner = runtime::enter(db::HelixDB::open(source.into()))
            .await
            .map_err(HelixError::from)?;
        Ok(Arc::new(Self::new(inner)))
    }

    /// Open a read-only HelixDB handle.
    #[uniffi::constructor]
    pub async fn open_reader(source: HelixDbSource) -> Result<Arc<Self>, HelixError> {
        let inner = runtime::enter(db::HelixDB::open_reader(source.into()))
            .await
            .map_err(HelixError::from)?;
        Ok(Arc::new(Self::new(inner)))
    }

    /// Execute an SDK-built query encoded as JSON bytes.
    pub async fn query_json(&self, request: Vec<u8>) -> Result<Vec<u8>, HelixError> {
        runtime::enter(self.inner.query_json(&request))
            .await
            .map_err(HelixError::from)
    }

    /// Execute one ordinary read request and construct a reusable native graph.
    pub async fn graph(
        &self,
        request: Vec<u8>,
        spec: NativeGraphLoadSpec,
    ) -> Result<Arc<NativeGraph>, NativeGraphError> {
        let response = runtime::enter(self.inner.query_json(&request))
            .await
            .map_err(|error| NativeGraphError::Query {
                message: error.to_string(),
            })?;
        graph_from_query_response(spec, response)
    }

    /// Close the underlying storage handle.
    pub async fn close(&self) -> Result<(), HelixError> {
        runtime::enter(self.inner.close())
            .await
            .map_err(HelixError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use helix_ast::batch::read_batch;
    use helix_ast::query::QueryRequest;
    use helix_ast::traversal::g;

    use super::{HelixDB, HelixDbSource};
    use crate::HelixError;

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

        assert!(matches!(err, HelixError::InvalidRequest { .. }));
    }
}
