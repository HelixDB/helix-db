use std::sync::Arc;

use db::query_service::HelixQueryService;
use db::{HelixDB, HelixDbMode};

/// Shared server state used by every transport.
#[derive(Clone)]
pub struct ServerState {
    query_service: HelixQueryService,
    db_mode: HelixDbMode,
}

impl ServerState {
    /// Build state from an opened DB handle.
    pub fn new(db: Arc<HelixDB>) -> Self {
        let db_mode = db.mode();
        let query_service = HelixQueryService::new(db);
        Self {
            query_service,
            db_mode,
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
}
