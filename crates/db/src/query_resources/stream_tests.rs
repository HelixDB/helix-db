//! Independently observe pinned allocation and ownership through stream states.
use super::*;
use futures::{Stream, StreamExt};
use std::{cell::Cell, marker::PhantomPinned, pin::Pin, task::Poll};

#[derive(Clone, Copy)]
enum Step {
    Pending,
    Value,
    Failure,
    Done,
}
struct Script<'a> {
    payload: [u8; 4096],
    step: Cell<Step>,
    address: Cell<Option<usize>>,
    polls: &'a Cell<usize>,
    _pinned: PhantomPinned,
}
impl Stream for Script<'_> {
    type Item = std::result::Result<u8, &'static str>;
    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let state = self.as_ref().get_ref();
        let address = std::ptr::from_ref(state).addr();
        assert!(
            state
                .address
                .replace(Some(address))
                .is_none_or(|previous| previous == address),
            "the immovable stream was relocated"
        );
        state.polls.set(state.polls.get() + 1);
        match state.step.get() {
            Step::Pending => {
                state.step.set(Step::Value);
                context.waker().wake_by_ref();
                Poll::Pending
            }
            Step::Value => {
                state.step.set(Step::Failure);
                Poll::Ready(Some(Ok(state.payload[0])))
            }
            Step::Failure => {
                state.step.set(Step::Done);
                Poll::Ready(Some(Err("stream failure")))
            }
            Step::Done => Poll::Ready(None),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match self.step.get() {
            Step::Pending | Step::Value => 2,
            Step::Failure => 1,
            Step::Done => 0,
        };
        (remaining, Some(remaining))
    }
}

#[test]
fn stream_admission_precedes_pinning_and_follows_moves_pending_errors_and_exhaustion() {
    let polls = Cell::new(0);
    let make = || Script {
        payload: [37; 4096],
        step: Cell::new(Step::Pending),
        address: Cell::new(None),
        polls: &polls,
        _pinned: PhantomPinned,
    };
    let rejected = Budget::new(1024);
    let (result, allocation) =
        crate::allocation_testing::observe(|| rejected.admitted_stream(make()));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocation.allocations, 0);
    assert_eq!(polls.get(), 0);
    assert_eq!(rejected.available(), 1024);
    for completed_polls in 0..=4 {
        let budget = Budget::new(8192);
        let (result, allocation) =
            crate::allocation_testing::observe(|| budget.admitted_stream(make()));
        let mut original = result.unwrap();
        assert_eq!(allocation.allocations, 1);
        assert_eq!(allocation.bytes, size_of::<Script<'_>>());
        assert_eq!(budget.available(), 8192 - allocation.bytes);
        assert_eq!(original.size_hint(), (2, Some(2)));
        let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
        if completed_polls > 0 {
            assert!(Pin::new(&mut original).poll_next(&mut context).is_pending());
        }
        // Moving the resource owner must keep the !Unpin stream at its address.
        let mut moved = original;
        if completed_polls > 1 {
            assert_eq!(futures::executor::block_on(moved.next()), Some(Ok(37)));
            assert_eq!(moved.size_hint(), (1, Some(1)));
        }
        if completed_polls > 2 {
            assert_eq!(
                futures::executor::block_on(moved.next()),
                Some(Err("stream failure"))
            );
            assert_eq!(moved.size_hint(), (0, Some(0)));
        }
        if completed_polls > 3 {
            assert_eq!(futures::executor::block_on(moved.next()), None);
        }
        assert_eq!(budget.available(), 8192 - allocation.bytes);
        drop(moved);
        assert_eq!(budget.available(), 8192);
    }
}
