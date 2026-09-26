//! Typed driver error contracts shared by unit and production coverage tests.

use super::*;

fn permanent_store_errors_block_directly_and_through_wrappers() {
    let source = || io::Error::other("fixture").into();
    for error in [
        object_store::Error::NotFound {
            path: "fixture".into(),
            source: source(),
        },
        object_store::Error::InvalidPath {
            source: object_store::path::Path::parse("../fixture").unwrap_err(),
        },
        object_store::Error::NotSupported { source: source() },
        object_store::Error::NotImplemented {
            operation: "put".into(),
            implementer: "fixture".into(),
        },
        object_store::Error::PermissionDenied {
            path: "fixture".into(),
            source: source(),
        },
        object_store::Error::Unauthenticated {
            path: "fixture".into(),
            source: source(),
        },
        object_store::Error::UnknownConfigurationKey {
            store: "fixture",
            key: "invalid".into(),
        },
        object_store::Error::NotModified {
            path: "fixture".into(),
            source: source(),
        },
    ] {
        assert!(!is_retryable_object_store(&error), "{error:?}");
        let wrapped = object_store::Error::Generic {
            store: "wrapper",
            source: Box::new(error),
        };
        assert!(!is_retryable_object_store(&wrapped), "{wrapped:?}");
    }
}

fn typed_io_causes_preserve_permanent_and_transient_distinctions() {
    for (kind, retryable) in [
        (io::ErrorKind::NotFound, false),
        (io::ErrorKind::PermissionDenied, false),
        (io::ErrorKind::InvalidInput, false),
        (io::ErrorKind::InvalidData, false),
        (io::ErrorKind::Unsupported, false),
        (io::ErrorKind::TimedOut, true),
        (io::ErrorKind::ConnectionReset, true),
        (io::ErrorKind::ConnectionRefused, true),
        (io::ErrorKind::Interrupted, true),
        (io::ErrorKind::WouldBlock, true),
        (io::ErrorKind::Other, true),
    ] {
        let error = object_store::Error::Generic {
            store: "fixture",
            source: Box::new(io::Error::new(kind, "fixture")),
        };
        assert_eq!(is_retryable_object_store(&error), retryable, "{kind:?}");
        let nested = object_store::Error::Generic {
            store: "wrapper",
            source: Box::new(error),
        };
        assert_eq!(is_retryable_object_store(&nested), retryable, "{kind:?}");
    }
}

fn conditional_races_and_opaque_provider_errors_remain_retryable() {
    for error in [
        object_store::Error::AlreadyExists {
            path: "fixture".into(),
            source: io::Error::other("race").into(),
        },
        object_store::Error::Precondition {
            path: "fixture".into(),
            source: io::Error::other("race").into(),
        },
        object_store::Error::Generic {
            store: "fixture",
            source: "opaque provider failure".into(),
        },
    ] {
        assert!(is_retryable_object_store(&error), "{error:?}");
    }
    for kind in [
        object_store::client::HttpErrorKind::Connect,
        object_store::client::HttpErrorKind::Request,
        object_store::client::HttpErrorKind::Timeout,
        object_store::client::HttpErrorKind::Interrupted,
    ] {
        let error = object_store::Error::Generic {
            store: "fixture",
            source: Box::new(object_store::client::HttpError::new(
                kind,
                io::Error::other("transport failure"),
            )),
        };
        assert!(is_retryable_object_store(&error), "{kind:?}");
    }
}

async fn panicked_tasks_block_but_cancelled_tasks_retry() {
    let panicked = tokio::spawn(async { panic!("fixture panic") })
        .await
        .unwrap_err();
    assert!(!is_retryable_object_store(
        &object_store::Error::JoinError { source: panicked }
    ));
    let task = tokio::spawn(std::future::pending::<()>());
    task.abort();
    let cancelled = task.await.unwrap_err();
    assert!(is_retryable_object_store(&object_store::Error::JoinError {
        source: cancelled,
    }));
}

/// Exercises permanent, temporary, wrapped and task-failure classifications.
pub(crate) async fn run() {
    permanent_store_errors_block_directly_and_through_wrappers();
    typed_io_causes_preserve_permanent_and_transient_distinctions();
    conditional_races_and_opaque_provider_errors_remain_retryable();
    panicked_tasks_block_but_cancelled_tasks_retry().await;
}

#[cfg(test)]
#[tokio::test]
async fn typed_driver_failure_classification_contract() {
    run().await;
}
