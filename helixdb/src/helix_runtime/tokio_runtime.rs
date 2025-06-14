use super::AsyncRuntime;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// Tokio based implementation of [`AsyncRuntime`].
#[derive(Clone, Default)]
pub struct TokioRuntime;

/// Wrapper around [`tokio::task::JoinHandle`] that yields the task result
/// directly and panics if the task failed.
pub struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

// SAFETY: `TokioJoinHandle` is only constructed via `TokioRuntime::spawn`,
// which requires `T: Send + 'static`. Therefore it is safe to mark this type
// as `Send` for all `T`.
unsafe impl<T> Send for TokioJoinHandle<T> {}

impl<T> Future for TokioJoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: just projecting the inner JoinHandle
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        match inner.poll(cx) {
            Poll::Ready(Ok(val)) => Poll::Ready(val),
            Poll::Ready(Err(err)) => panic!("Tokio task failed: {}", err),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncRuntime for TokioRuntime {
    type JoinHandle<T> = TokioJoinHandle<T>
    where
        T: Send + 'static;

    fn spawn<F, T>(&self, fut: F) -> Self::JoinHandle<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        TokioJoinHandle(tokio::spawn(fut))
    }

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(tokio::time::sleep(dur))
    }
}
