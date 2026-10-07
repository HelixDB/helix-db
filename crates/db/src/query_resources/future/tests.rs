use super::*;
use crate::allocation_testing;
use std::{
    cell::Cell,
    marker::PhantomPinned,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Copy)]
enum Finish {
    Value,
    Error,
    Panic,
}
struct Script<'a> {
    payload: [u8; 4096],
    pending: Cell<bool>,
    address: Cell<Option<usize>>,
    polls: &'a AtomicUsize,
    drops: &'a AtomicUsize,
    finish: Finish,
    _pinned: PhantomPinned,
}
impl Future for Script<'_> {
    type Output = Result<u8>;
    fn poll(self: Pin<&mut Self>, context: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let state = self.as_ref().get_ref();
        let address = std::ptr::from_ref(state).addr();
        assert!(state
            .address
            .replace(Some(address))
            .is_none_or(|previous| previous == address));
        state.polls.fetch_add(1, Ordering::Relaxed);
        if state.pending.replace(false) {
            context.waker().wake_by_ref();
            return task::Poll::Pending;
        }
        match state.finish {
            Finish::Value => task::Poll::Ready(Ok(state.payload[0])),
            Finish::Error => task::Poll::Ready(Err(HelixDbError::QueryMemoryLimitExceeded)),
            Finish::Panic => panic!("scripted future panic"),
        }
    }
}
impl Drop for Script<'_> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn recursive_future_admission_precedes_allocation_and_releases_every_state() {
    let polls = AtomicUsize::new(0);
    let drops = AtomicUsize::new(0);
    let make = |finish| Script {
        payload: [37; 4096],
        pending: Cell::new(true),
        address: Cell::new(None),
        polls: &polls,
        drops: &drops,
        finish,
        _pinned: PhantomPinned,
    };
    let rejected = Budget::new(1);
    let (mut future, allocation) =
        allocation_testing::observe(|| Admitted::new(Some(&rejected), make(Finish::Value)));
    assert_eq!(allocation.allocations, 0);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert!(matches!(
        futures::executor::block_on(&mut future),
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(rejected.available(), 1);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || futures::executor::block_on(&mut future)
    ))
    .is_err());
    drop(future);
    drop(Admitted::new(Some(&rejected), make(Finish::Value)));
    assert_eq!(drops.load(Ordering::Relaxed), 2);

    for admitted in [true, false] {
        for polls_before_drop in 0..=2 {
            for finish in [Finish::Value, Finish::Error, Finish::Panic] {
                let before_drops = drops.load(Ordering::Relaxed);
                let budget = Budget::new(8192);
                let (mut original, allocation) = allocation_testing::observe(|| {
                    Admitted::new(admitted.then_some(&budget), make(finish))
                });
                assert_eq!(allocation.allocations, 1);
                assert_eq!(allocation.bytes, size_of::<Script<'_>>());
                let remaining = if admitted {
                    8192 - allocation.bytes
                } else {
                    8192
                };
                assert_eq!(budget.available(), remaining);
                let mut context = task::Context::from_waker(futures::task::noop_waker_ref());
                if polls_before_drop > 0 {
                    assert!(Pin::new(&mut original).poll(&mut context).is_pending());
                }
                let mut moved = original;
                if polls_before_drop > 1 {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        futures::executor::block_on(&mut moved)
                    }));
                    match finish {
                        Finish::Value => assert_eq!(result.unwrap().unwrap(), 37),
                        Finish::Error => assert!(matches!(
                            result.unwrap(),
                            Err(HelixDbError::QueryMemoryLimitExceeded)
                        )),
                        Finish::Panic => assert!(result.is_err()),
                    }
                    assert_eq!(
                        budget.available(),
                        if matches!(finish, Finish::Panic) {
                            remaining
                        } else {
                            8192
                        }
                    );
                }
                drop(moved);
                assert_eq!(budget.available(), 8192);
                assert_eq!(drops.load(Ordering::Relaxed), before_drops + 1);
            }
        }
    }
}

fn recurse(depth: usize, budget: &Budget) -> Admitted<'_, usize> {
    Admitted::new(Some(budget), async move {
        if depth == 0 {
            Ok(0)
        } else {
            Ok(recurse(depth - 1, budget).await? + 1)
        }
    })
}
#[test]
fn recursive_future_exhaustion_unwinds_all_admitted_frames() {
    let budget = Budget::new(1024);
    assert!(matches!(
        futures::executor::block_on(recurse(128, &budget)),
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(budget.available(), 1024);
    assert_eq!(futures::executor::block_on(recurse(2, &budget)).unwrap(), 2);
    assert_eq!(budget.available(), 1024);
}
