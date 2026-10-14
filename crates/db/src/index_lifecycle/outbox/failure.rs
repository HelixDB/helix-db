//! Retry classification at the index-driver/object-store boundary.

use std::{error, io, iter};

use slatedb::object_store;

/// Rejects permanent object-store failures, including typed wrapped causes.
///
/// Missing immutable data, invalid requests/configuration, denied access and
/// panicked tasks need intervention. Conditional-write races, cancelled tasks
/// and temporary I/O retain backoff. Unknown errors remain retryable: cloud
/// providers can hide both throttling and server failures behind `Generic`,
/// without exposing a public status type. Error messages are not an API.
pub(super) fn is_retryable_object_store(error: &object_store::Error) -> bool {
    !iter::successors(Some(error as &(dyn error::Error + 'static)), |cause| {
        cause.source()
    })
    .any(|cause| {
        cause
            .downcast_ref::<object_store::Error>()
            .is_some_and(|error| {
                matches!(
                    error,
                    object_store::Error::NotFound { .. }
                        | object_store::Error::InvalidPath { .. }
                        | object_store::Error::NotSupported { .. }
                        | object_store::Error::NotImplemented { .. }
                        | object_store::Error::PermissionDenied { .. }
                        | object_store::Error::Unauthenticated { .. }
                        | object_store::Error::UnknownConfigurationKey { .. }
                        | object_store::Error::NotModified { .. }
                )
            })
            || cause.downcast_ref::<io::Error>().is_some_and(|error| {
                matches!(
                    error.kind(),
                    io::ErrorKind::NotFound
                        | io::ErrorKind::PermissionDenied
                        | io::ErrorKind::InvalidInput
                        | io::ErrorKind::InvalidData
                        | io::ErrorKind::Unsupported
                )
            })
            || cause
                .downcast_ref::<tokio::task::JoinError>()
                .is_some_and(tokio::task::JoinError::is_panic)
    })
}

#[cfg(any(test, feature = "production-coverage"))]
#[path = "../../../tests/production_support/index_lifecycle_driver_failure.rs"]
pub(crate) mod contracts;
