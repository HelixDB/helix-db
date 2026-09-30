use std::sync::Arc;

use db::query_service::HelixQueryService;
use db::{HelixDB, HelixDbMode, IndexRuntimeReadiness};
use helix_metrics::query::transport::OssQueryMetrics;

/// Shared server state used by every transport.
#[derive(Clone)]
pub struct ServerState {
    db: Arc<HelixDB>,
    query_service: HelixQueryService,
    db_mode: HelixDbMode,
    index_readiness: IndexRuntimeReadiness,
    cypher: crate::CypherEndpoints,
}

impl ServerState {
    /// Build state from an opened DB handle.
    pub fn new(db: Arc<HelixDB>, query_metrics: Option<OssQueryMetrics>) -> Self {
        let db_mode = db.mode();
        let index_readiness = db.index_runtime_readiness();
        let query_service = match query_metrics {
            Some(query_metrics) => {
                HelixQueryService::with_query_metrics(Arc::clone(&db), query_metrics)
            }
            None => HelixQueryService::new(Arc::clone(&db)),
        };
        Self {
            db,
            query_service,
            db_mode,
            index_readiness,
            cypher: crate::CypherEndpoints::Disabled,
        }
    }

    /// Route or refuse Cypher requests; new state refuses them.
    pub fn with_cypher_endpoints(self, cypher: crate::CypherEndpoints) -> Self {
        Self { cypher, ..self }
    }

    /// Return whether Cypher requests are routed.
    pub const fn cypher_endpoints(&self) -> crate::CypherEndpoints {
        self.cypher
    }

    /// Borrow the query service.
    pub fn query_service(&self) -> &HelixQueryService {
        &self.query_service
    }

    /// Return the opened DB mode.
    pub const fn db_mode(&self) -> HelixDbMode {
        self.db_mode
    }

    /// Return whether all public index families have their runtime authorities.
    pub const fn index_readiness(&self) -> IndexRuntimeReadiness {
        self.index_readiness
    }

    /// Flush an acknowledged write before a transport acknowledges durability.
    pub async fn flush_writer(&self) -> Result<db::DatabaseSequence, db::error::HelixDbError> {
        self.db.flush_writer().await
    }
}
