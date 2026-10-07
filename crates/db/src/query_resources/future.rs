//! A single admitted box for recursive native execution. Rejection is inline:
//! a caller can return a finite future type without allocating an error future.
use super::{Budget, Reservation};
use crate::error::{HelixDbError, Result};
use std::{future::Future, pin::Pin, task};

pub(crate) struct Admitted<'a, T> {
    state: State<'a, T>,
}
enum State<'a, T> {
    Pending {
        future: Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
        _memory: Option<Reservation>,
    },
    Rejected(Option<HelixDbError>),
    Complete,
}
impl<'a, T> Admitted<'a, T> {
    /// Admit before pinning. Native callers without a query budget preserve
    /// their existing allocation behavior. The guard also covers unpolled drop.
    pub(crate) fn new<F>(budget: Option<&Budget>, future: F) -> Self
    where
        F: Future<Output = Result<T>> + Send + 'a,
    {
        let memory = budget
            .map(|budget| budget.reserve(size_of::<F>()))
            .transpose();
        let state = match memory {
            Ok(memory) => State::Pending {
                future: Box::pin(future),
                _memory: memory,
            },
            Err(error) => State::Rejected(Some(error)),
        };
        Self { state }
    }
}
impl<T> Future for Admitted<'_, T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, context: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let result = match &mut self.state {
            State::Pending { future, .. } => match future.as_mut().poll(context) {
                task::Poll::Pending => return task::Poll::Pending,
                task::Poll::Ready(result) => result,
            },
            State::Rejected(error) => Err(error.take().expect("an unpolled admission error")),
            State::Complete => panic!("completed admitted future polled again"),
        };
        // Release the completed async state before the caller consumes output.
        // Moving this owner never moves a potentially !Unpin boxed future.
        self.state = State::Complete;
        task::Poll::Ready(result)
    }
}

#[cfg(test)]
mod tests;
