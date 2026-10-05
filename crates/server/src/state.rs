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
        }
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

    /// How many entities the writer's index worker holds back: each has a
    /// queued vector/text operation that can never fit a publication under
    /// the current limits. Zero on a reader, and rebuilt after a restart as
    /// publication blocks again. Health responses report only this count, so
    /// unauthenticated probes never learn tenant or entity IDs.
    pub fn blocked_index_entity_count(&self) -> u64 {
        self.db.blocked_index_entity_count()
    }

    /// Flush an acknowledged write before a transport acknowledges durability.
    pub async fn flush_writer(&self) -> Result<db::DatabaseSequence, db::error::HelixDbError> {
        self.db.flush_writer().await
    }
}
