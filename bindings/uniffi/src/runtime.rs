//! Dedicated Tokio runtime support for the UniFFI bindings.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};

use tokio::runtime::{Builder, Handle, Runtime};

const RUNTIME_THREADS_ENV: &str = "HELIX_UNIFFI_RUNTIME_THREADS";
const FALLBACK_WORKER_THREADS: usize = 4;

pub(crate) fn enter<F>(future: F) -> impl Future<Output = F::Output>
where
    F: Future,
{
    EnterRuntime {
        handle: runtime().handle().clone(),
        future: Box::pin(future),
    }
}

struct EnterRuntime<F> {
    handle: Handle,
    future: Pin<Box<F>>,
}

impl<F: Future> Future for EnterRuntime<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = this.handle.enter();
        this.future.as_mut().poll(cx)
    }
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .worker_threads(configured_worker_threads(
                std::env::var(RUNTIME_THREADS_ENV).ok().as_deref(),
            ))
            .enable_all()
            .thread_name("helixdb-uniffi-rt")
            .build()
            .expect("failed to build HelixDB UniFFI runtime")
    })
}

fn configured_worker_threads(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(FALLBACK_WORKER_THREADS)
        })
}

#[cfg(test)]
mod tests {
    use tokio::runtime::{Handle, RuntimeFlavor};

    use super::configured_worker_threads;

    #[test]
    fn worker_threads_uses_positive_env_value() {
        assert_eq!(configured_worker_threads(Some("8")), 8);
        assert_eq!(configured_worker_threads(Some(" 3 ")), 3);
    }

    #[test]
    fn worker_threads_falls_back_for_invalid_env_values() {
        let default = configured_worker_threads(None);

        assert_eq!(configured_worker_threads(Some("0")), default);
        assert_eq!(configured_worker_threads(Some("-1")), default);
        assert_eq!(configured_worker_threads(Some("not-a-number")), default);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enter_uses_multi_thread_runtime_context() {
        let flavor = super::enter(async { Handle::current().runtime_flavor() }).await;

        assert_eq!(flavor, RuntimeFlavor::MultiThread);
    }
}
