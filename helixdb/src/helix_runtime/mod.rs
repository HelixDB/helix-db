pub mod tokio_runtime;

use std::future::Future;
use std::pin::Pin;

/// Trait representing the minimal async runtime capabilities required by HelixDB.
///
/// Production code uses a Tokio-backed implementation while tests can
/// provide deterministic schedulers by implementing this trait.
pub trait AsyncRuntime {
    type JoinHandle<T>: Future<Output = T> + Send + 'static
    where
        T: Send + 'static;

    /// Spawn a future onto the runtime.
    fn spawn<F, T>(&self, fut: F) -> Self::JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static;

    /// Sleep for the specified duration.
    fn sleep(&self, dur: std::time::Duration) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

