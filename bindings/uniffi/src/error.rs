//! Stable UniFFI error categories for database failures.
//!
//! This module is the contract boundary between detailed Rust database errors
//! and the smaller error vocabulary exposed to foreign-language callers.
//! Configuration and request mistakes remain actionable to the caller, while
//! corrupt persisted vector rows and fail-closed lifecycle cutover states are
//! reported as internal failures.

use db::encoding::error::EncodingError;
use db::error::HelixDbError;
use helix_ast::error_code;
use thiserror::Error;

/// Error type returned by the HelixDB UniFFI bindings.
#[derive(Debug, Error, uniffi::Error)]
pub enum HelixError {
    /// Invalid configuration.
    #[error("{msg}")]
    InvalidConfig { error: String, msg: String },

    /// Invalid request body or API usage.
    #[error("{msg}")]
    InvalidRequest { error: String, msg: String },

    /// Query planning failed.
    #[error("{msg}")]
    Planner { error: String, msg: String },

    /// Storage failed.
    #[error("{msg}")]
    Storage { error: String, msg: String },

    /// Transaction failed.
    #[error("{msg}")]
    Transaction { error: String, msg: String },

    /// Internal failure.
    #[error("{msg}")]
    Internal { error: String, msg: String },
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
        let error_code = error.error_code().to_string();
        let msg = error.to_string();
        if error.is_invalid_vector_input() {
            return Self::InvalidRequest {
                error: error_code,
                msg,
            };
        }
        match error {
            HelixDbError::Config(_)
            | HelixDbError::InvalidVectorConfig(_)
            | HelixDbError::IndexDefinitionConflict { .. } => Self::InvalidConfig {
                error: error_code,
                msg,
            },
            HelixDbError::Query(_)
            | HelixDbError::InvalidQueryJson(_)
            | HelixDbError::Encoding(EncodingError::InvalidTenantId(_))
            | HelixDbError::IndexBusy { .. }
            | HelixDbError::IndexOperationNotFound { .. }
            | HelixDbError::IndexOperationNotAbortable { .. }
            | HelixDbError::ActiveTextMutationLimitExceeded { .. }
            | HelixDbError::InvalidIndexSourceData { .. }
            | HelixDbError::SecondaryIndexValue(_)
            | HelixDbError::SecondaryLifecycleSteppingRequiresDisabledMode => {
                Self::InvalidRequest {
                    error: error_code,
                    msg,
                }
            }
            HelixDbError::WriterModeRequired { .. } | HelixDbError::ReaderModeRequired { .. } => {
                Self::InvalidRequest {
                    error: error_code,
                    msg,
                }
            }
            HelixDbError::TransactionConflict(_)
            | HelixDbError::RequestReadViewChanged
            | HelixDbError::StaleIndexGeneration { .. } => Self::Transaction {
                error: error_code,
                msg,
            },
            HelixDbError::Storage(_)
            | HelixDbError::ObjectStore(_)
            | HelixDbError::DatabaseClosed => Self::Storage {
                error: error_code,
                msg,
            },
            HelixDbError::Encoding(_)
            | HelixDbError::InvalidNodeId(_)
            | HelixDbError::NodeNotFound(_)
            | HelixDbError::EdgeNotFound { .. }
            | HelixDbError::IndexAlreadyExists(_)
            | HelixDbError::IndexNotFound(_)
            | HelixDbError::UniqueConstraintViolation { .. }
            | HelixDbError::UnsupportedUniqueIndexValueType { .. }
            | HelixDbError::InvalidDimension { .. }
            | HelixDbError::InvalidVectorComponent { .. }
            | HelixDbError::VectorComponentMagnitudeExceeded { .. }
            | HelixDbError::ZeroNormCosineVector
            | HelixDbError::InvalidVectorItem(_)
            | HelixDbError::IndexLifecycleUnavailable { .. }
            | HelixDbError::InvalidIndexV2Model(_)
            | HelixDbError::WriterFencedCommitOutcomeUnknown
            | HelixDbError::MigrationRequired { .. }
            | HelixDbError::WriterMigrationRequired { .. }
            | HelixDbError::UnsupportedIndexStorageVersion { .. }
            | HelixDbError::IdentifierExhausted(_)
            | HelixDbError::IdentifierAllocationFailed { .. }
            | HelixDbError::IndexCatalogCorruption(_)
            | HelixDbError::LegacyZeroNormCosineVector { .. }
            | HelixDbError::QueryDeadlineExceeded
            | HelixDbError::QueryCancelledByReaderRetirement
            | HelixDbError::InvariantViolation(_) => Self::Internal {
                error: error_code,
                msg,
            },
        }
    }
}

impl From<tokio::task::JoinError> for HelixError {
    /// Converts a failed binding-runtime task without unwinding across FFI.
    fn from(error: tokio::task::JoinError) -> Self {
        Self::Internal {
            error: error_code::QueryErrorCode::InternalError.to_string(),
            msg: format!("embedded runtime task failed: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use db::error::SecondaryIndexValueError;
    use db::search::vector::{VectorConfigError, VectorDistanceMetric, VectorItemDecodeError};

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
            HelixError::from(HelixDbError::InvalidDimension {
                expected: 3,
                got: 2,
            }),
            HelixError::InvalidRequest { .. }
        ));
        assert!(matches!(
            HelixError::from(HelixDbError::InvalidVectorComponent { index: 0 }),
            HelixError::InvalidRequest { .. }
        ));
        assert!(matches!(
            HelixError::from(HelixDbError::VectorComponentMagnitudeExceeded {
                metric: VectorDistanceMetric::Euclidean,
                dimension: 3,
                component_index: 1,
                observed_magnitude: 4.0,
                inclusive_maximum: 3.0,
            }),
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
    fn query_deadlines_do_not_expand_the_stable_binding_error_contract() {
        assert!(matches!(
            HelixError::from(HelixDbError::QueryDeadlineExceeded),
            HelixError::Internal { error, msg }
                if error == "query_deadline_exceeded" && msg.contains("deadline")
        ));
    }

    #[test]
    fn reader_retirement_does_not_expand_the_stable_binding_error_contract() {
        assert!(matches!(
            HelixError::from(HelixDbError::QueryCancelledByReaderRetirement),
            HelixError::Internal { error, msg }
                if error == "query_cancelled_by_reader_retirement"
                    && msg.contains("reader is retiring")
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
    fn invalid_index_source_data_is_an_invalid_request() {
        assert!(matches!(
            HelixError::from(HelixDbError::InvalidIndexSourceData {
                reason: "indexed document is missing its tenant property".to_string(),
            }),
            HelixError::InvalidRequest { error, msg }
                if error == "invalid_index_source_data"
                    && msg.contains("indexed document is missing its tenant property")
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
    fn fenced_commit_outcomes_are_terminal_internal_failures() {
        assert!(matches!(
            HelixError::from(HelixDbError::WriterFencedCommitOutcomeUnknown),
            HelixError::Internal { .. }
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
    fn reader_mode_requirements_remain_caller_actionable() {
        assert!(matches!(
            HelixError::from(HelixDbError::ReaderModeRequired { actual: "writer" }),
            HelixError::InvalidRequest { .. }
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

    #[tokio::test]
    async fn binding_runtime_task_failure_is_an_internal_error() {
        let error = tokio::spawn(async { panic!("induced binding task failure") })
            .await
            .expect_err("induced task panic returns a join error");

        assert!(matches!(
            HelixError::from(error),
            HelixError::Internal { error, msg }
                if error == "internal_error" && msg.contains("embedded runtime task failed")
        ));
    }

    #[test]
    fn secondary_errors_remain_caller_actionable() {
        assert!(matches!(
            HelixError::from(HelixDbError::SecondaryIndexValue(
                SecondaryIndexValueError::NaNRangeValue
            )),
            HelixError::InvalidRequest { .. }
        ));
        assert!(matches!(
            HelixError::from(HelixDbError::SecondaryLifecycleSteppingRequiresDisabledMode),
            HelixError::InvalidRequest { .. }
        ));
    }
}
