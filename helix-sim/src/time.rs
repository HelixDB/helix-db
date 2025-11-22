use crate::runtime::{AsyncRuntime, DeterministicRuntime};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum SleepFuture {
    Tokio(tokio::time::Sleep),
    Deterministic {
        wake_time: u64,
        runtime: DeterministicRuntime,
    },
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match unsafe { self.as_mut().get_unchecked_mut() } {
            SleepFuture::Tokio(sleep) => unsafe { Pin::new_unchecked(sleep) }.poll(cx),
            SleepFuture::Deterministic { wake_time, runtime } => {
                if runtime.now() >= *wake_time {
                    Poll::Ready(())
                } else {
                    let _ = cx.waker().clone();
                    Poll::Pending
                }
            }
        }
    }
}
