//! The backend read trait requires a boxed future. Admit that box before its
//! construction, including when the caller never polls it. A rejected call is
//! zero-sized until polled; its error needs no separate boxed future state.

use std::{future::Future, marker::PhantomData, pin::Pin, task};

use crate::query_resources::{Budget, Reservation};

pub(super) type Read<'a, T> = Pin<Box<dyn Future<Output = Result<T, slatedb::Error>> + Send + 'a>>;

pin_project_lite::pin_project! {
    struct Frame<F> {
        #[pin]
        future: F,
        // Field order releases the allocation's contents before its admission.
        _memory: Reservation,
    }
}

impl<F: Future> Future for Frame<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.project().future.poll(context)
    }
}

struct Rejected<T>(PhantomData<fn() -> T>);

impl<T> Future for Rejected<T> {
    type Output = Result<T, slatedb::Error>;

    fn poll(self: Pin<&mut Self>, _: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        task::Poll::Ready(Err(super::storage_error(super::AdmissionFailure)))
    }
}

/// A single allocation whose reservation follows the box, even after a caller
/// manually polls it to completion. Pin projection supports non-Unpin futures
/// without another allocation or handwritten unsafe code.
pub(super) fn admit<'a, T: 'a, F>(budget: Option<&Budget>, future: F) -> Read<'a, T>
where
    F: Future<Output = Result<T, slatedb::Error>> + Send + 'a,
{
    let Some(budget) = budget else {
        return Box::pin(future);
    };
    let Ok(memory) = budget.reserve(size_of::<Frame<F>>()) else {
        return Box::pin(Rejected(PhantomData));
    };
    Box::pin(Frame {
        future,
        _memory: memory,
    })
}

#[cfg(test)]
mod tests;
