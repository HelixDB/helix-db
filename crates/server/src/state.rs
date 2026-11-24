use std::sync::Arc;
use std::time::{Duration, Instant};

use db::query_service::HelixQueryService;
use db::{HelixDB, HelixDbMode, IndexRuntimeReadiness, StorageState};
use helix_metrics::query::transport::OssQueryMetrics;

use crate::config::StorageConfig;

/// Shared server state used by every transport.
#[derive(Clone)]
pub struct ServerState {
    db: Arc<HelixDB>,
    query_service: HelixQueryService,
    db_mode: HelixDbMode,
    index_readiness: IndexRuntimeReadiness,
    /// The storage `db` was opened on, which diagnostics inspect.
    storage: Arc<StorageConfig>,
    /// Diagnostic facts that take I/O, shared between requests.
    diagnostic_facts: Arc<crate::diagnostics::FactCache>,
    /// The label audit every clone of this state runs one at a time.
    label_audits: Arc<crate::diagnostics::AuditFlight>,
    started: Instant,
}

impl ServerState {
    /// Build state from a DB handle opened on `storage`.
    pub fn new(
        db: Arc<HelixDB>,
        query_metrics: Option<OssQueryMetrics>,
        storage: StorageConfig,
    ) -> Self {
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
            storage: Arc::new(storage),
            diagnostic_facts: Arc::default(),
            label_audits: Arc::default(),
            started: Instant::now(),
        }
    }

    /// Borrow the opened database.
    pub(crate) fn db(&self) -> &HelixDB {
        &self.db
    }

    /// Borrow the storage the database was opened on.
    pub(crate) fn storage(&self) -> &StorageConfig {
        &self.storage
    }

    /// The diagnostic facts every clone of this state shares.
    pub(crate) fn diagnostic_facts(&self) -> &crate::diagnostics::FactCache {
        &self.diagnostic_facts
    }

    /// The label audit every clone of this state shares.
    pub(crate) fn label_audits(&self) -> &crate::diagnostics::AuditFlight {
        &self.label_audits
    }

    /// Time since this state was built, as the server opened its database.
    pub(crate) fn uptime(&self) -> Duration {
        self.started.elapsed()
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

    /// Whether the database's storage still serves.
    pub fn storage_state(&self) -> StorageState {
        self.db.storage_state()
    }

    /// Whether this server should receive traffic: its storage is open and
    /// every index family has its runtime authority. A fenced writer never
    /// is again, so a load balancer moves its traffic to the newer writer.
    pub fn is_ready(&self) -> bool {
        self.storage_state() == StorageState::Open && self.index_readiness.is_ready()
    }

    /// Whether restarting the process could help, which liveness probes
    /// report: not once storage failed or closed. A fenced writer stays live,
    /// as a restarted one would reopen the store as writer and fence the
    /// writer that replaced it, and the two would take turns.
    pub fn is_live(&self) -> bool {
        match self.storage_state() {
            StorageState::Open | StorageState::Fenced => true,
            StorageState::Failed | StorageState::Closed => false,
        }
    }

    /// How many entities the writer's index worker holds back: each has a
    /// queued vector/text operation that can never fit a publication under
    /// the current limits, or whose planning fails deterministically. Zero on
    /// a reader, and rebuilt after a restart as publication blocks again.
    /// Health responses report only this count, so unauthenticated probes
    /// never learn tenant or entity IDs.
    pub fn blocked_index_entity_count(&self) -> u64 {
        self.db.blocked_index_entity_count()
    }
}
