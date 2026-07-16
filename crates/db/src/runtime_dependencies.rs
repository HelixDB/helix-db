//! Trusted, non-serialized runtime dependencies for indexed database access.
//!
//! [`DbConfig`](crate::DbConfig) remains cloneable user policy. This module
//! instead carries runtime authority that must never come from TOML: storage
//! topology, reader-lease coordination, and text blob publication/deletion
//! coordination.
//!
//! Process-local authority is obtainable only from
//! [`ProcessLocalDatabaseToken`]. Cloning that token shares its in-memory
//! object store, generated database identity, and both coordinator instances.
//! Shared disk or object-storage deployments construct
//! [`HelixRuntimeDependencies::shared`] with independently installed or
//! unavailable adapters.
//!
//! ```
//! use db::{
//!     DatabaseAccessTopology, HelixRuntimeDependencies,
//!     ProcessLocalDatabaseToken, SharedBlobPublicationMode,
//!     SharedReaderLeaseMode,
//! };
//!
//! let token = ProcessLocalDatabaseToken::new("runtime-doc-example").unwrap();
//! assert!(token.topology().is_process_local());
//!
//! let shared = HelixRuntimeDependencies::shared(
//!     SharedReaderLeaseMode::Unavailable,
//!     SharedBlobPublicationMode::Unavailable,
//! );
//! assert_eq!(shared.topology(), DatabaseAccessTopology::shared());
//! ```

#![deny(missing_docs)]

use std::sync::Arc;

use slatedb::object_store::{memory::InMemory, ObjectStore};
use uuid::Uuid;

use crate::error::{HelixDbError, Result};
use crate::index_v2::blob_publication::{
    BlobPublicationCoordinator, BlobPublicationTiming, ProcessLocalBlobPublicationCoordinator,
};
use crate::index_v2::reader_lease::{
    IndexLeaseCoordinator, ProcessLocalIndexLeaseCoordinator, ReaderLeaseTiming,
};

/// Trusted storage-sharing topology for one open database identity.
///
/// The representation is private so callers cannot label a disk or object
/// store process-local. Use [`ProcessLocalDatabaseToken`] for process-local
/// opens and [`HelixRuntimeDependencies::shared`] for shared opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseAccessTopology(DatabaseAccessTopologyKind);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatabaseAccessTopologyKind {
    ProcessLocal,
    Shared,
}

impl DatabaseAccessTopology {
    const PROCESS_LOCAL: Self = Self(DatabaseAccessTopologyKind::ProcessLocal);
    const SHARED: Self = Self(DatabaseAccessTopologyKind::Shared);

    /// Returns the only topology callers may pair with disk/object storage.
    pub const fn shared() -> Self {
        Self::SHARED
    }

    /// Returns whether all handles are proven to share one in-process token.
    pub const fn is_process_local(self) -> bool {
        matches!(self.0, DatabaseAccessTopologyKind::ProcessLocal)
    }

    /// Returns whether coordination must be supplied by external shared adapters.
    pub const fn is_shared(self) -> bool {
        matches!(self.0, DatabaseAccessTopologyKind::Shared)
    }
}

/// Reader-lease dependency accepted from a trusted shared-storage embedding.
#[derive(Clone)]
pub enum SharedReaderLeaseMode {
    /// No shared reader coordinator is installed; indexed DDL/reads fail closed.
    Unavailable,
    /// One adapter shared by every runtime that can access the database.
    Installed(Arc<dyn IndexLeaseCoordinator>),
}

impl core::fmt::Debug for SharedReaderLeaseMode {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("Unavailable"),
            Self::Installed(_) => formatter.write_str("Installed(<shared reader coordinator>)"),
        }
    }
}

/// Blob-publication dependency accepted from a trusted shared-storage embedding.
#[derive(Clone)]
pub enum SharedBlobPublicationMode {
    /// No shared blob coordinator is installed; text publication/GC fails closed.
    Unavailable,
    /// One adapter shared by every runtime that can access the object store.
    Installed(Arc<dyn BlobPublicationCoordinator>),
}

impl core::fmt::Debug for SharedBlobPublicationMode {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("Unavailable"),
            Self::Installed(_) => formatter.write_str("Installed(<shared blob coordinator>)"),
        }
    }
}

#[derive(Clone)]
enum ReaderLeaseCoordinatorMode {
    ProcessLocal(Arc<dyn IndexLeaseCoordinator>),
    Shared(Arc<dyn IndexLeaseCoordinator>),
    Unavailable,
}

#[derive(Clone)]
enum BlobPublicationCoordinatorMode {
    ProcessLocal(Arc<dyn BlobPublicationCoordinator>),
    Shared(Arc<dyn BlobPublicationCoordinator>),
    Unavailable,
}

/// Complete trusted runtime authorities supplied at database open.
///
/// Fields are private and the type has no serde implementation, preventing
/// ordinary user configuration from forging process-local topology or adapter
/// installation.
#[derive(Clone)]
pub struct HelixRuntimeDependencies {
    topology: DatabaseAccessTopology,
    reader_leases: ReaderLeaseCoordinatorMode,
    blob_publication: BlobPublicationCoordinatorMode,
}

impl core::fmt::Debug for HelixRuntimeDependencies {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("HelixRuntimeDependencies")
            .field("topology", &self.topology)
            .field(
                "reader_leases",
                &match self.reader_leases {
                    ReaderLeaseCoordinatorMode::ProcessLocal(_) => "ProcessLocal",
                    ReaderLeaseCoordinatorMode::Shared(_) => "Shared",
                    ReaderLeaseCoordinatorMode::Unavailable => "Unavailable",
                },
            )
            .field(
                "blob_publication",
                &match self.blob_publication {
                    BlobPublicationCoordinatorMode::ProcessLocal(_) => "ProcessLocal",
                    BlobPublicationCoordinatorMode::Shared(_) => "Shared",
                    BlobPublicationCoordinatorMode::Unavailable => "Unavailable",
                },
            )
            .finish()
    }
}

impl HelixRuntimeDependencies {
    /// Constructs independently optional adapters for shared storage.
    pub fn shared(
        reader_leases: SharedReaderLeaseMode,
        blob_publication: SharedBlobPublicationMode,
    ) -> Self {
        Self {
            topology: DatabaseAccessTopology::SHARED,
            reader_leases: match reader_leases {
                SharedReaderLeaseMode::Unavailable => ReaderLeaseCoordinatorMode::Unavailable,
                SharedReaderLeaseMode::Installed(coordinator) => {
                    ReaderLeaseCoordinatorMode::Shared(coordinator)
                }
            },
            blob_publication: match blob_publication {
                SharedBlobPublicationMode::Unavailable => {
                    BlobPublicationCoordinatorMode::Unavailable
                }
                SharedBlobPublicationMode::Installed(coordinator) => {
                    BlobPublicationCoordinatorMode::Shared(coordinator)
                }
            },
        }
    }

    /// Safe default for shared storage when no trusted adapters were injected.
    pub fn shared_unavailable() -> Self {
        Self::shared(
            SharedReaderLeaseMode::Unavailable,
            SharedBlobPublicationMode::Unavailable,
        )
    }

    /// Returns the trusted topology bound into these dependencies.
    pub const fn topology(&self) -> DatabaseAccessTopology {
        self.topology
    }

    pub(crate) fn process_local(
        reader_leases: Arc<dyn IndexLeaseCoordinator>,
        blob_publication: Arc<dyn BlobPublicationCoordinator>,
    ) -> Self {
        Self {
            topology: DatabaseAccessTopology::PROCESS_LOCAL,
            reader_leases: ReaderLeaseCoordinatorMode::ProcessLocal(reader_leases),
            blob_publication: BlobPublicationCoordinatorMode::ProcessLocal(blob_publication),
        }
    }

    pub(crate) fn validate_for_topology(&self, topology: DatabaseAccessTopology) -> Result<()> {
        if self.topology != topology {
            return Err(HelixDbError::Config(
                "runtime dependencies do not match the database source topology".to_string(),
            ));
        }
        let reader_matches = matches!(
            (&self.reader_leases, topology.0),
            (
                ReaderLeaseCoordinatorMode::ProcessLocal(_),
                DatabaseAccessTopologyKind::ProcessLocal,
            ) | (
                ReaderLeaseCoordinatorMode::Shared(_) | ReaderLeaseCoordinatorMode::Unavailable,
                DatabaseAccessTopologyKind::Shared,
            )
        );
        let blob_matches = matches!(
            (&self.blob_publication, topology.0),
            (
                BlobPublicationCoordinatorMode::ProcessLocal(_),
                DatabaseAccessTopologyKind::ProcessLocal,
            ) | (
                BlobPublicationCoordinatorMode::Shared(_)
                    | BlobPublicationCoordinatorMode::Unavailable,
                DatabaseAccessTopologyKind::Shared,
            )
        );
        if !reader_matches || !blob_matches {
            return Err(HelixDbError::Config(
                "runtime coordinator modes do not match the database source topology".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn reader_lease_coordinator(&self) -> Option<Arc<dyn IndexLeaseCoordinator>> {
        match &self.reader_leases {
            ReaderLeaseCoordinatorMode::ProcessLocal(coordinator)
            | ReaderLeaseCoordinatorMode::Shared(coordinator) => Some(Arc::clone(coordinator)),
            ReaderLeaseCoordinatorMode::Unavailable => None,
        }
    }

    pub(crate) fn blob_publication_coordinator(
        &self,
    ) -> Option<Arc<dyn BlobPublicationCoordinator>> {
        match &self.blob_publication {
            BlobPublicationCoordinatorMode::ProcessLocal(coordinator)
            | BlobPublicationCoordinatorMode::Shared(coordinator) => Some(Arc::clone(coordinator)),
            BlobPublicationCoordinatorMode::Unavailable => None,
        }
    }
}

struct ProcessLocalDatabaseIdentity {
    database_id: Uuid,
    database: String,
    object_store: Arc<dyn ObjectStore>,
    runtime_dependencies: HelixRuntimeDependencies,
}

/// Non-forgeable shared identity for one in-memory database.
///
/// Every writer or reader handle for this database must be opened from a clone
/// of this token. A token cannot wrap disk, S3, or caller-provided erased object
/// stores.
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
    /// Creates a fresh in-memory store and inseparable runtime authorities.
    pub fn new(database: impl Into<String>) -> Result<Self> {
        let database = database.into();
        if database.is_empty() {
            return Err(HelixDbError::Config(
                "process-local database path must not be empty".to_string(),
            ));
        }
        let database_id = Uuid::new_v4();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let reader_leases: Arc<dyn IndexLeaseCoordinator> = Arc::new(
            ProcessLocalIndexLeaseCoordinator::new(ReaderLeaseTiming::default()),
        );
        let blob_publication: Arc<dyn BlobPublicationCoordinator> =
            Arc::new(ProcessLocalBlobPublicationCoordinator::new(
                Arc::clone(&object_store),
                database.clone(),
                BlobPublicationTiming::default(),
            ));
        Ok(Self {
            identity: Arc::new(ProcessLocalDatabaseIdentity {
                database_id,
                database,
                object_store,
                runtime_dependencies: HelixRuntimeDependencies::process_local(
                    reader_leases,
                    blob_publication,
                ),
            }),
        })
    }

    /// Returns the generated identity shared by every clone.
    pub fn database_id(&self) -> Uuid {
        self.identity.database_id
    }

    /// Returns the only topology a process-local token can represent.
    pub const fn topology(&self) -> DatabaseAccessTopology {
        DatabaseAccessTopology::PROCESS_LOCAL
    }

    pub(crate) fn database(&self) -> &str {
        &self.identity.database
    }

    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.identity.object_store)
    }

    pub(crate) fn runtime_dependencies(&self) -> HelixRuntimeDependencies {
        self.identity.runtime_dependencies.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_local_token_clones_share_every_runtime_identity() {
        let token = ProcessLocalDatabaseToken::new("shared-token").unwrap();
        let clone = token.clone();
        assert_eq!(token.database_id(), clone.database_id());
        assert_eq!(token.database(), clone.database());
        assert!(Arc::ptr_eq(&token.object_store(), &clone.object_store()));

        let first = token.runtime_dependencies();
        let second = clone.runtime_dependencies();
        assert_eq!(first.topology(), DatabaseAccessTopology::PROCESS_LOCAL);
        assert!(Arc::ptr_eq(
            &first.reader_lease_coordinator().unwrap(),
            &second.reader_lease_coordinator().unwrap(),
        ));
        assert!(Arc::ptr_eq(
            &first.blob_publication_coordinator().unwrap(),
            &second.blob_publication_coordinator().unwrap(),
        ));
    }

    #[test]
    fn fresh_tokens_cannot_alias_one_database_identity() {
        let first = ProcessLocalDatabaseToken::new("same-logical-name").unwrap();
        let second = ProcessLocalDatabaseToken::new("same-logical-name").unwrap();
        assert_ne!(first.database_id(), second.database_id());
        assert!(!Arc::ptr_eq(&first.object_store(), &second.object_store()));
        assert!(!Arc::ptr_eq(
            &first
                .runtime_dependencies()
                .reader_lease_coordinator()
                .unwrap(),
            &second
                .runtime_dependencies()
                .reader_lease_coordinator()
                .unwrap(),
        ));
    }

    #[test]
    fn shared_dependencies_keep_coordinators_independently_unavailable() {
        let dependencies = HelixRuntimeDependencies::shared_unavailable();
        assert_eq!(dependencies.topology(), DatabaseAccessTopology::SHARED);
        assert!(dependencies.reader_lease_coordinator().is_none());
        assert!(dependencies.blob_publication_coordinator().is_none());
        dependencies
            .validate_for_topology(DatabaseAccessTopology::SHARED)
            .unwrap();
        assert!(dependencies
            .validate_for_topology(DatabaseAccessTopology::PROCESS_LOCAL)
            .is_err());
    }

    #[test]
    fn process_local_database_path_must_be_nonempty() {
        assert!(ProcessLocalDatabaseToken::new("").is_err());
    }
}
