//! Stable UniFFI error categories for database failures.
//!
//! This module is the contract boundary between detailed Rust database errors
//! and the smaller error vocabulary exposed to foreign-language callers.
//! Configuration and request mistakes remain actionable to the caller, while
//! corrupt persisted vector rows and fail-closed lifecycle cutover states are
//! reported as internal failures.

use db::encoding::error::EncodingError;
use db::error::HelixDbError;
use thiserror::Error;

/// Error type returned by the HelixDB UniFFI bindings.
#[derive(Debug, Error, uniffi::Error)]
pub enum HelixError {
    /// Invalid configuration.
    #[error("{message}")]
    InvalidConfig { message: String },

    /// Invalid request body or API usage.
    #[error("{message}")]
    InvalidRequest { message: String },

    /// Query planning failed.
    #[error("{message}")]
    Planner { message: String },

    /// Storage failed.
    #[error("{message}")]
    Storage { message: String },

    /// Transaction failed.
    #[error("{message}")]
    Transaction { message: String },

    /// Internal failure.
    #[error("{message}")]
    Internal { message: String },
}

impl From<HelixDbError> for HelixError {
    /// Classifies a detailed database error without exposing Rust-only payload types.
    ///
    /// The original display message is retained for diagnostics. The category
    /// distinguishes caller-correctable vector configuration/input failures
    /// from invalid stored vector rows, which indicate an internal invariant
    /// violation rather than malformed foreign-language API usage. Retryable
    /// request-view changes remain transaction failures so foreign callers can
    /// apply the same retry policy as a storage transaction conflict.
    fn from(error: HelixDbError) -> Self {
        let message = error.to_string();
        match error {
            HelixDbError::Config(_)
            | HelixDbError::InvalidVectorConfig(_)
            | HelixDbError::IndexDefinitionConflict { .. } => Self::InvalidConfig { message },
            HelixDbError::Query(_)
            | HelixDbError::Encoding(EncodingError::InvalidTenantId(_))
            | HelixDbError::InvalidVectorComponent { .. }
            | HelixDbError::ZeroNormCosineVector
            | HelixDbError::IndexBusy { .. }
            | HelixDbError::IndexOperationNotFound { .. }
            | HelixDbError::IndexOperationNotAbortable { .. }
            | HelixDbError::ActiveTextMutationLimitExceeded { .. } => {
                Self::InvalidRequest { message }
            }
            HelixDbError::WriterModeRequired { .. } => Self::InvalidRequest { message },
            HelixDbError::TransactionConflict(_)
            | HelixDbError::RequestReadViewChanged
            | HelixDbError::StaleIndexGeneration { .. }
            | HelixDbError::WriterFencedCommitOutcomeUnknown => Self::Transaction { message },
            HelixDbError::Storage(_)
            | HelixDbError::ObjectStore(_)
            | HelixDbError::BlobPublication(_)
            | HelixDbError::DatabaseClosed => Self::Storage { message },
            HelixDbError::Encoding(_)
            | HelixDbError::InvalidNodeId(_)
            | HelixDbError::NodeNotFound(_)
            | HelixDbError::EdgeNotFound { .. }
            | HelixDbError::IndexAlreadyExists(_)
            | HelixDbError::IndexNotFound(_)
            | HelixDbError::UniqueConstraintViolation { .. }
            | HelixDbError::UnsupportedUniqueIndexValueType { .. }
            | HelixDbError::InvalidDimension { .. }
            | HelixDbError::InvalidVectorItem(_)
            | HelixDbError::IndexLifecycleUnavailable { .. }
            | HelixDbError::InvalidIndexV2Model(_)
            | HelixDbError::MigrationRequired { .. }
            | HelixDbError::UnsupportedIndexStorageVersion { .. }
            | HelixDbError::IdentifierExhausted(_)
            | HelixDbError::IdentifierAllocationFailed { .. }
            | HelixDbError::IndexCatalogCorruption(_)
            | HelixDbError::InvariantViolation(_) => Self::Internal { message },
        }
    }
}

#[cfg(test)]
mod tests {
    use db::search::vector::{VectorConfigError, VectorItemDecodeError};

    use super::*;

    #[test]
    fn vector_errors_preserve_caller_vs_storage_ownership() {
        assert!(matches!(
            HelixError::from(HelixDbError::InvalidVectorConfig(
                VectorConfigError::EmptyIndexName
            )),
            HelixError::InvalidConfig { .. }
        ));
        assert!(matches!(
            HelixError::from(HelixDbError::InvalidVectorComponent { index: 0 }),
            HelixError::InvalidRequest { .. }
        ));
        assert!(matches!(
            HelixError::from(HelixDbError::ZeroNormCosineVector),
            HelixError::InvalidRequest { .. }
        ));
        assert!(matches!(
            HelixError::from(HelixDbError::InvalidVectorItem(
                VectorItemDecodeError::HeaderMismatch
            )),
            HelixError::Internal { .. }
        ));
    }

    #[test]
    fn request_read_view_changes_are_retryable_transaction_failures() {
        assert!(matches!(
            HelixError::from(HelixDbError::RequestReadViewChanged),
            HelixError::Transaction { .. }
        ));
    }

    #[test]
    fn active_text_resource_limits_are_invalid_requests() {
        assert!(matches!(
            HelixError::from(HelixDbError::ActiveTextMutationLimitExceeded {
                resource: db::error::ActiveTextMutationResource::OutputOperations,
                observed: 2,
                limit: 1,
            }),
            HelixError::InvalidRequest { .. }
        ));
    }

    #[test]
    fn stale_index_handles_are_retryable_transaction_failures() {
        assert!(matches!(
            HelixError::from(HelixDbError::StaleIndexGeneration {
                index_id: 1,
                generation: 2,
                record_revision: 3,
            }),
            HelixError::Transaction { .. }
        ));
    }

    #[test]
    fn fenced_commit_outcomes_are_retryable_transaction_failures() {
        assert!(matches!(
            HelixError::from(HelixDbError::WriterFencedCommitOutcomeUnknown),
            HelixError::Transaction { .. }
        ));
    }

    #[test]
    fn lifecycle_unavailable_failures_remain_internal() {
        assert!(matches!(
            HelixError::from(HelixDbError::IndexLifecycleUnavailable {
                family: db::error::IndexFamily::Text,
                reason: db::error::IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
            }),
            HelixError::Internal { .. }
        ));
    }

    #[test]
    fn blob_publication_failures_remain_storage_failures() {
        assert!(matches!(
            HelixError::from(HelixDbError::BlobPublication(
                db::index_v2::blob_publication::BlobPublicationError::PublicationOutcomeAmbiguous(
                    "retained coordinator outcome".to_string(),
                ),
            )),
            HelixError::Storage { .. }
        ));
    }

    #[test]
    fn lifecycle_control_rejections_remain_caller_actionable() {
        assert!(matches!(
            HelixError::from(HelixDbError::IndexBusy { state: "building" }),
            HelixError::InvalidRequest { .. }
        ));
        assert!(matches!(
            HelixError::from(HelixDbError::IndexOperationNotFound {
                operation_id: "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001".to_string(),
            }),
            HelixError::InvalidRequest { .. }
        ));
        assert!(matches!(
            HelixError::from(HelixDbError::IndexOperationNotAbortable {
                operation_id: "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001".to_string(),
                reason: "successful build",
            }),
            HelixError::InvalidRequest { .. }
        ));
    }
}
