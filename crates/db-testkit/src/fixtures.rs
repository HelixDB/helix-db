//! Storage, coordinator, planner, and query-adapter contract boundaries.

use async_trait::async_trait;

use crate::ids::DatabaseName;
use crate::{Result, TestkitError};

/// Storage backend selected by a workload fixture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    /// On-disk object store used by the initial launch matrix.
    LocalFileSystem,
    /// S3-compatible local object store reserved for a later matrix extension.
    SeaweedFs,
    /// Amazon S3 reserved for a later matrix extension.
    S3,
    /// Adapter-defined backend name.
    Other(String),
}

/// Explicit storage fixture boundary used by every runtime topology.
#[async_trait]
pub trait StorageFixture: Send + Sync {
    /// Returns the backend classification.
    fn backend(&self) -> StorageBackend;

    /// Returns the logical database path.
    fn database(&self) -> &DatabaseName;

    /// Opens a writer directly on the configured storage.
    async fn open_writer(&self, config: db::DbConfig) -> Result<db::HelixDB>;

    /// Opens a reader directly on the configured storage.
    async fn open_reader(&self, config: db::DbConfig) -> Result<db::HelixDB>;
}

/// Public planner boundary used by normalized planner cases.
pub trait PlannerFixture {
    /// Builds the immutable planner context for one case.
    fn planner_context(&self) -> helix_planner::context::PlannerContext;

    /// Plans one public AST batch through the production entrypoint.
    fn plan(
        &self,
        batch: &helix_ast::batch::BatchQuery,
    ) -> Result<helix_planner::exec::ExecutablePlan> {
        helix_planner::planning::plan(batch, &self.planner_context())
            .map_err(|error| TestkitError::Planner(error.to_string()))
    }
}

/// Shared request corpus adapter for embedded, service, and transport parity.
#[async_trait]
pub trait QueryCorpusAdapter: Send {
    /// Executes one public query request and returns its JSON response.
    async fn execute_query(
        &mut self,
        request: helix_ast::query::QueryRequest,
    ) -> Result<serde_json::Value>;

    /// Closes owned runtime resources and joins background work.
    async fn close(&mut self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_backend_keeps_future_adapters_explicit() {
        assert_eq!(
            StorageBackend::LocalFileSystem,
            StorageBackend::LocalFileSystem
        );
        assert_ne!(StorageBackend::SeaweedFs, StorageBackend::S3);
        assert_eq!(
            StorageBackend::Other("fixture".to_string()),
            StorageBackend::Other("fixture".to_string())
        );
    }
}
