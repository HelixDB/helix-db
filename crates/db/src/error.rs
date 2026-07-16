//! Error types for Helix database operations

use crate::config::ConfigError;
use crate::encoding::error::EncodingError;
use crate::search::vector::{VectorConfigError, VectorItemDecodeError};
use slatedb::ErrorKind;

/// Index family whose canonical lifecycle authority is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexFamily {
    /// Equality and range secondary indexes.
    Secondary,
    /// HNSW vector indexes.
    Vector,
    /// Tantivy text indexes.
    Text,
    /// All dynamic families when graph mutation maintenance cannot be proven.
    DynamicIndexes,
}

impl core::fmt::Display for IndexFamily {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::Secondary => "secondary",
            Self::Vector => "vector",
            Self::Text => "text",
            Self::DynamicIndexes => "dynamic indexes",
        })
    }
}

/// Typed reason an index operation must fail closed during the V2 cutover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexLifecycleUnavailableReason {
    /// Canonical V2 catalog and physical-generation authority is not installed.
    CanonicalStateUnavailable,
    /// A graph write cannot prove exact same-transaction family maintenance.
    MutationMaintenanceUnavailable,
    /// No trusted blob-publication coordinator is installed for this database.
    BlobPublicationCoordinationUnavailable,
    /// No trusted reader-lease coordinator can authorize physical index reads.
    ReaderCoordinationUnavailable,
}

/// Serialized resource whose Active text-mutation preflight exceeded policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveTextMutationResource {
    /// Exact database key/value bytes read by the plan.
    InputBytes,
    /// Exact database writes staged by the graph transaction.
    OutputOperations,
    /// Exact database key/value bytes staged by the graph transaction.
    OutputBytes,
    /// Immutable text split payload bytes awaiting publication.
    SplitBytes,
    /// Encoded V2 manifest-page value bytes after the append.
    ManifestPageBytes,
}

impl core::fmt::Display for ActiveTextMutationResource {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::InputBytes => "input_bytes",
            Self::OutputOperations => "output_operations",
            Self::OutputBytes => "output_bytes",
            Self::SplitBytes => "split_bytes",
            Self::ManifestPageBytes => "manifest_page_bytes",
        })
    }
}

impl core::fmt::Display for IndexLifecycleUnavailableReason {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::CanonicalStateUnavailable => "canonical V2 state is not installed",
            Self::MutationMaintenanceUnavailable => {
                "same-transaction V2 mutation maintenance is not installed"
            }
            Self::BlobPublicationCoordinationUnavailable => {
                "blob publication coordination is not installed"
            }
            Self::ReaderCoordinationUnavailable => {
                "reader coordination is not installed or unavailable"
            }
        })
    }
}

/// Errors that can occur in Helix database operations
#[derive(Debug, thiserror::Error)]
pub enum HelixDbError {
    /// Error from the underlying SlateDB storage
    #[error("Storage error: {0}")]
    Storage(#[from] slatedb::Error),

    /// Error encoding/decoding graph data
    #[error("Encoding error: {0}")]
    Encoding(#[from] EncodingError),

    /// Transaction conflict during commit
    #[error("Transaction conflict: {0}")]
    TransactionConflict(String),

    /// A standalone reader advanced while one request was executing.
    #[error("Request read view changed during execution; retry the request")]
    RequestReadViewChanged,

    /// Invalid node ID
    #[error("Invalid node ID: {0}")]
    InvalidNodeId(u64),

    /// Node not found
    #[error("Node not found: {0}")]
    NodeNotFound(u64),

    /// Edge not found
    #[error("Edge not found: {from} -> {to}")]
    EdgeNotFound {
        /// Source node ID
        from: u64,
        /// Target node ID
        to: u64,
    },

    /// Database is closed
    #[error("Database is closed")]
    DatabaseClosed,

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Dynamic-index work is disabled until its canonical V2 contract exists.
    #[error("Index lifecycle unavailable for {family}: {reason}")]
    IndexLifecycleUnavailable {
        /// Family whose authority cannot be proven.
        family: IndexFamily,
        /// Missing contract that requires fail-closed behavior.
        reason: IndexLifecycleUnavailableReason,
    },

    /// A request-owned Active text mutation exceeded exact serialized admission.
    #[error("Active text mutation exceeds {resource}: observed {observed}, limit {limit}")]
    ActiveTextMutationLimitExceeded {
        /// Resource rejected before intent creation or object I/O.
        resource: ActiveTextMutationResource,
        /// Exact serialized or operation count measured by preflight.
        observed: u64,
        /// Positive configured ceiling.
        limit: u64,
    },

    /// A trusted text-blob publication coordinator rejected request work.
    #[error("Blob publication error: {0}")]
    BlobPublication(#[from] crate::index_v2::blob_publication::BlobPublicationError),

    /// A value could not satisfy the canonical V2 index model.
    #[error("Invalid V2 index model: {0}")]
    InvalidIndexV2Model(#[from] crate::index_v2::IndexV2ModelError),

    /// Existing storage needs an explicit external migration before V2 opens.
    #[error("Migration required: {reason}")]
    MigrationRequired {
        /// Stable reason the runtime refused to initialize or interpret rows.
        reason: String,
    },

    /// Storage was written by a newer index format than this binary supports.
    #[error("Unsupported index storage version {found}; this binary supports {supported}")]
    UnsupportedIndexStorageVersion {
        /// Durable format encountered at open.
        found: u16,
        /// Current format supported by this binary.
        supported: u16,
    },

    /// A bounded durable numeric namespace has no allocatable IDs remaining.
    #[error("Identifier exhausted: {0}")]
    IdentifierExhausted(&'static str),

    /// Bounded random-ID collision retries could not find an unused identity.
    #[error("Identifier allocation failed for {kind} after {attempts} attempts")]
    IdentifierAllocationFailed {
        /// Durable identity namespace being allocated.
        kind: &'static str,
        /// Checked maximum candidates considered.
        attempts: usize,
    },

    /// Canonical rows could not form one trustworthy scoped runtime catalog.
    #[error("V2 index catalog corruption: {0}")]
    IndexCatalogCorruption(String),

    /// A retained active handle no longer names the canonical active revision.
    #[error(
        "stale index generation: index {index_id}, generation {generation}, revision {record_revision}"
    )]
    StaleIndexGeneration {
        /// Logical index ID retained by the caller.
        index_id: u64,
        /// Physical generation retained by the caller.
        generation: u64,
        /// Canonical record revision retained by the caller.
        record_revision: u64,
    },

    /// A newer writer fenced the request before proof absence became authoritative.
    #[error("writer fencing prevented the Active text commit outcome from being proven")]
    WriterFencedCommitOutcomeUnknown,

    /// Invalid vector index configuration.
    #[error("Invalid vector configuration: {0}")]
    InvalidVectorConfig(#[from] VectorConfigError),

    /// Stored vector row bytes do not satisfy their validated index contract.
    #[error("Invalid vector item: {0}")]
    InvalidVectorItem(#[from] VectorItemDecodeError),

    /// Object store error
    #[error("Object store error: {0}")]
    ObjectStore(#[from] slatedb::object_store::Error),

    /// Query/traversal error
    #[error("Query error: {0}")]
    Query(String),

    /// Operation requires a writer handle.
    #[error("writer mode required, current mode is {actual}")]
    WriterModeRequired {
        /// Current database mode.
        actual: &'static str,
    },

    /// Vector index already exists
    #[error("Vector index already exists: {0}")]
    IndexAlreadyExists(String),

    /// A create request reused a logical index identity with different semantics.
    #[error("Index definition conflicts in fields: {differing_fields}")]
    IndexDefinitionConflict {
        /// Authoritative definition already owning the logical identity.
        existing: Box<crate::index_v2::ValidatedDynamicIndexDefinition>,
        /// Validated definition requested by the caller.
        requested: Box<crate::index_v2::ValidatedDynamicIndexDefinition>,
        /// Canonical, non-empty set of incompatible fields.
        differing_fields: crate::config::NonEmptyDefinitionDifferences,
    },

    /// The logical identity is already changing lifecycle state.
    #[error("Index is busy in lifecycle state {state}")]
    IndexBusy {
        /// Canonical state that prevents this request.
        state: &'static str,
    },

    /// No retained operation with this ID exists in the requested scope.
    #[error("Index operation not found: {operation_id}")]
    IndexOperationNotFound {
        /// Canonical lowercase UUID supplied by the caller.
        operation_id: String,
    },

    /// The retained operation cannot be converted into build-abort cleanup.
    #[error("Index operation {operation_id} is not abortable: {reason}")]
    IndexOperationNotAbortable {
        /// Canonical lowercase UUID supplied by the caller.
        operation_id: String,
        /// Stable diagnostic reason.
        reason: &'static str,
    },

    /// Vector index not found
    #[error("Vector index not found: {0}")]
    IndexNotFound(String),

    /// Unique node equality constraint violation.
    #[error(
        "Unique constraint violated for {label}.{property} on value {value}: existing node {existing_node_id}, attempted node {attempted_node_id}"
    )]
    UniqueConstraintViolation {
        /// Label scope for the unique index.
        label: String,
        /// Indexed property name.
        property: String,
        /// Conflicting value.
        value: String,
        /// Existing node already owning the value.
        existing_node_id: u64,
        /// Node attempting to claim the value.
        attempted_node_id: u64,
    },

    /// Unsupported property type for unique node equality enforcement.
    #[error(
        "Unique node equality index {label}.{property} does not support value type {value_type} on node {node_id}"
    )]
    UnsupportedUniqueIndexValueType {
        /// Label scope for the unique index.
        label: String,
        /// Indexed property name.
        property: String,
        /// Node carrying the unsupported value.
        node_id: u64,
        /// Unsupported property value type.
        value_type: String,
    },

    /// Invalid vector dimension
    #[error("Invalid vector dimension: expected {expected}, got {got}")]
    InvalidDimension {
        /// Expected dimension
        expected: usize,
        /// Actual dimension
        got: usize,
    },

    /// A vector component was NaN or infinite at a public boundary.
    #[error("Invalid vector component at index {index}: value must be finite")]
    InvalidVectorComponent {
        /// Zero-based component offset.
        index: usize,
    },

    /// Cosine distance is undefined for a true zero vector.
    #[error("Invalid cosine vector: norm must be non-zero")]
    ZeroNormCosineVector,

    /// Internal storage invariant violation.
    #[error("Storage invariant violated: {0}")]
    InvariantViolation(String),
}

impl From<ConfigError> for HelixDbError {
    fn from(value: ConfigError) -> Self {
        Self::Config(value.to_string())
    }
}

impl HelixDbError {
    /// Stable public compatibility code when this error belongs to the index
    /// lifecycle API.
    pub fn index_error_code(&self) -> Option<&'static str> {
        match self {
            Self::IndexLifecycleUnavailable {
                reason: IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable,
                ..
            } => Some("reader_coordination_unavailable"),
            Self::IndexLifecycleUnavailable {
                reason: IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
                ..
            } => Some("blob_publication_coordination_unavailable"),
            Self::IndexLifecycleUnavailable { .. } => Some("index_lifecycle_unavailable"),
            Self::ActiveTextMutationLimitExceeded { .. } => {
                Some("active_text_mutation_limit_exceeded")
            }
            Self::BlobPublication(_) => Some("blob_publication_failed"),
            Self::IndexAlreadyExists(_) => Some("index_already_exists"),
            Self::IndexDefinitionConflict { .. } => Some("index_definition_conflict"),
            Self::IndexBusy { .. } => Some("index_busy"),
            Self::IndexNotFound(_) => Some("index_not_found"),
            Self::IndexOperationNotFound { .. } => Some("index_operation_not_found"),
            Self::IndexOperationNotAbortable { .. } => Some("index_operation_not_abortable"),
            Self::IdentifierExhausted("logical index ID") => Some("index_id_exhausted"),
            Self::IdentifierExhausted("vector physical index ID") => {
                Some("vector_physical_id_exhausted")
            }
            Self::InvalidIndexV2Model(
                crate::index_v2::IndexV2ModelError::IdentifierExhausted {
                    kind: "index generation ID",
                },
            ) => Some("index_generation_exhausted"),
            Self::InvalidIndexV2Model(
                crate::index_v2::IndexV2ModelError::IdentifierExhausted {
                    kind: "index revision",
                },
            ) => Some("index_revision_exhausted"),
            Self::InvalidIndexV2Model(
                crate::index_v2::IndexV2ModelError::IdentifierExhausted {
                    kind: "index operation revision",
                },
            ) => Some("index_operation_revision_exhausted"),
            Self::StaleIndexGeneration { .. } => Some("stale_index_generation"),
            Self::WriterFencedCommitOutcomeUnknown => Some("writer_fenced_commit_outcome_unknown"),
            Self::Storage(_)
            | Self::Encoding(_)
            | Self::TransactionConflict(_)
            | Self::RequestReadViewChanged
            | Self::InvalidNodeId(_)
            | Self::NodeNotFound(_)
            | Self::EdgeNotFound { .. }
            | Self::DatabaseClosed
            | Self::Config(_)
            | Self::InvalidIndexV2Model(_)
            | Self::MigrationRequired { .. }
            | Self::UnsupportedIndexStorageVersion { .. }
            | Self::IdentifierExhausted(_)
            | Self::IdentifierAllocationFailed { .. }
            | Self::IndexCatalogCorruption(_)
            | Self::InvalidVectorConfig(_)
            | Self::InvalidVectorItem(_)
            | Self::ObjectStore(_)
            | Self::Query(_)
            | Self::WriterModeRequired { .. }
            | Self::UniqueConstraintViolation { .. }
            | Self::UnsupportedUniqueIndexValueType { .. }
            | Self::InvalidDimension { .. }
            | Self::InvalidVectorComponent { .. }
            | Self::ZeroNormCosineVector
            | Self::InvariantViolation(_) => None,
        }
    }

    /// Returns true when the error represents a retryable transaction conflict.
    #[must_use]
    pub fn is_transaction_conflict(&self) -> bool {
        matches!(
            self,
            Self::TransactionConflict(_) | Self::WriterFencedCommitOutcomeUnknown
        ) || matches!(self, Self::Storage(storage_err) if storage_err.kind() == ErrorKind::Transaction)
    }
}

/// Result type alias for Helix operations
pub type Result<T> = std::result::Result<T, HelixDbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_errors_convert_to_database_errors_with_display_context() {
        let error = HelixDbError::from(ConfigError::new("bad config"));
        assert_eq!(error.to_string(), "Configuration error: bad config");
    }

    #[test]
    fn retryable_conflict_classification_is_explicit() {
        assert!(HelixDbError::TransactionConflict("retry".to_string()).is_transaction_conflict());
        assert!(HelixDbError::WriterFencedCommitOutcomeUnknown.is_transaction_conflict());
        assert!(
            HelixDbError::Storage(slatedb::Error::transaction("retry".to_string()))
                .is_transaction_conflict()
        );
        assert!(
            !HelixDbError::Storage(slatedb::Error::invalid("not retryable".to_string()))
                .is_transaction_conflict()
        );
        assert!(!HelixDbError::Query("not retryable".to_string()).is_transaction_conflict());
    }

    #[test]
    fn rich_error_variants_render_contract_fields() {
        assert_eq!(
            HelixDbError::UniqueConstraintViolation {
                label: "User".to_string(),
                property: "email".to_string(),
                value: "\"a@example.com\"".to_string(),
                existing_node_id: 1,
                attempted_node_id: 2,
            }
            .to_string(),
            "Unique constraint violated for User.email on value \"a@example.com\": existing node 1, attempted node 2"
        );
        assert_eq!(
            HelixDbError::UnsupportedUniqueIndexValueType {
                label: "User".to_string(),
                property: "email".to_string(),
                node_id: 3,
                value_type: "F64".to_string(),
            }
            .to_string(),
            "Unique node equality index User.email does not support value type F64 on node 3"
        );
        assert_eq!(
            HelixDbError::InvalidDimension {
                expected: 3,
                got: 2,
            }
            .to_string(),
            "Invalid vector dimension: expected 3, got 2"
        );
        assert_eq!(
            HelixDbError::IndexLifecycleUnavailable {
                family: IndexFamily::Vector,
                reason: IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
            }
            .to_string(),
            "Index lifecycle unavailable for vector: canonical V2 state is not installed"
        );
    }

    #[test]
    fn lifecycle_errors_expose_frozen_machine_codes() {
        assert_eq!(
            HelixDbError::IndexLifecycleUnavailable {
                family: IndexFamily::Vector,
                reason: IndexLifecycleUnavailableReason::ReaderCoordinationUnavailable,
            }
            .index_error_code(),
            Some("reader_coordination_unavailable")
        );
        assert_eq!(
            HelixDbError::IndexLifecycleUnavailable {
                family: IndexFamily::Text,
                reason: IndexLifecycleUnavailableReason::BlobPublicationCoordinationUnavailable,
            }
            .index_error_code(),
            Some("blob_publication_coordination_unavailable")
        );
        assert_eq!(
            HelixDbError::IndexBusy { state: "building" }.index_error_code(),
            Some("index_busy")
        );
        assert_eq!(
            HelixDbError::IndexOperationNotFound {
                operation_id: "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001".to_string(),
            }
            .index_error_code(),
            Some("index_operation_not_found")
        );
        assert_eq!(
            HelixDbError::InvalidIndexV2Model(
                crate::index_v2::IndexV2ModelError::IdentifierExhausted {
                    kind: "index operation revision",
                },
            )
            .index_error_code(),
            Some("index_operation_revision_exhausted")
        );
        assert_eq!(
            HelixDbError::BlobPublication(
                crate::index_v2::blob_publication::BlobPublicationError::PublicationOutcomeAmbiguous(
                    "retained coordinator outcome".to_string(),
                ),
            )
            .index_error_code(),
            Some("blob_publication_failed")
        );
        assert_eq!(HelixDbError::NodeNotFound(1).index_error_code(), None);
        assert_eq!(
            HelixDbError::WriterFencedCommitOutcomeUnknown.index_error_code(),
            Some("writer_fenced_commit_outcome_unknown")
        );
    }

    #[test]
    fn active_text_limit_resources_render_stable_fields_and_machine_code() {
        let resources = [
            (ActiveTextMutationResource::InputBytes, "input_bytes"),
            (
                ActiveTextMutationResource::OutputOperations,
                "output_operations",
            ),
            (ActiveTextMutationResource::OutputBytes, "output_bytes"),
            (ActiveTextMutationResource::SplitBytes, "split_bytes"),
            (
                ActiveTextMutationResource::ManifestPageBytes,
                "manifest_page_bytes",
            ),
        ];
        for (resource, expected) in resources {
            assert_eq!(resource.to_string(), expected);
        }

        let error = HelixDbError::ActiveTextMutationLimitExceeded {
            resource: ActiveTextMutationResource::OutputBytes,
            observed: 11,
            limit: 10,
        };
        assert_eq!(
            error.to_string(),
            "Active text mutation exceeds output_bytes: observed 11, limit 10"
        );
        assert_eq!(
            error.index_error_code(),
            Some("active_text_mutation_limit_exceeded")
        );
    }
}
