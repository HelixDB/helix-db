//! Process-local identity for sharing one in-memory database.
//!
//! Disk and object-store databases do not require a runtime authority. Their
//! writer and reader handles open directly over the configured object store.

#![deny(missing_docs)]

use std::sync::Arc;

use slatedb::object_store::{memory::InMemory, ObjectStore};
use uuid::Uuid;

use crate::error::{HelixDbError, Result};

/// Readiness of graph, secondary, vector, and text index operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexRuntimeReadiness {
    /// SlateDB transactions and the configured object store are available.
    Ready,
}

impl IndexRuntimeReadiness {
    /// Returns whether index operations are ready.
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Returns a stable machine-readable readiness code.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Ready => "ready",
        }
    }
}

struct ProcessLocalDatabaseIdentity {
    database_id: Uuid,
    database: String,
    object_store: Arc<dyn ObjectStore>,
}

/// Reusable identity for handles sharing one in-memory object store.
///
/// Disk, S3, and caller-provided object stores do not use this token.
#[derive(Clone)]
pub struct ProcessLocalDatabaseToken {
    identity: Arc<ProcessLocalDatabaseIdentity>,
}

impl core::fmt::Debug for ProcessLocalDatabaseToken {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ProcessLocalDatabaseToken")
            .field("database_id", &self.identity.database_id)
            .field("database", &self.identity.database)
            .finish_non_exhaustive()
    }
}

impl ProcessLocalDatabaseToken {
    /// Creates a fresh in-memory store with a stable logical database path.
    pub fn new(database: impl Into<String>) -> Result<Self> {
        let database = database.into();
        if database.is_empty() {
            return Err(HelixDbError::Config(
                "process-local database path must not be empty".to_string(),
            ));
        }
        Ok(Self {
            identity: Arc::new(ProcessLocalDatabaseIdentity {
                database_id: Uuid::new_v4(),
                database,
                object_store: Arc::new(InMemory::new()),
            }),
        })
    }

    /// Returns the generated identity shared by every clone.
    pub fn database_id(&self) -> Uuid {
        self.identity.database_id
    }

    pub(crate) fn database(&self) -> &str {
        &self.identity.database
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.identity.object_store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_tokens_do_not_alias() {
        let first = ProcessLocalDatabaseToken::new("same-name").unwrap();
        let second = ProcessLocalDatabaseToken::new("same-name").unwrap();
        assert_ne!(first.database_id(), second.database_id());
        assert!(!Arc::ptr_eq(&first.object_store(), &second.object_store()));
    }
}
