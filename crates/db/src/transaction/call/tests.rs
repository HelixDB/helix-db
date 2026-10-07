use super::*;
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
    payload: [u8; 2048],
    pending: Cell<bool>,
    address: Cell<Option<usize>>,
    polls: &'a AtomicUsize,
    drops: &'a AtomicUsize,
    finish: Finish,
    _pinned: PhantomPinned,
}

impl Future for Script<'_> {
    type Output = Result<u8, slatedb::Error>;

    fn poll(self: Pin<&mut Self>, context: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let state = self.as_ref().get_ref();
        let address = std::ptr::from_ref(state).addr();
        assert!(state
            .address
            .replace(Some(address))
            .is_none_or(|old| old == address));
        state.polls.fetch_add(1, Ordering::Relaxed);
        if state.pending.replace(false) {
            context.waker().wake_by_ref();
            return task::Poll::Pending;
        }
        match state.finish {
            Finish::Value => task::Poll::Ready(Ok(state.payload[0])),
            Finish::Error => {
                task::Poll::Ready(Err(slatedb::Error::unavailable("scripted read".into())))
            }
            Finish::Panic => panic!("scripted read panic"),
        }
    }
}

impl Drop for Script<'_> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn one_admitted_box_preserves_pinning_and_retains_its_guard_until_drop() {
    let polls = AtomicUsize::new(0);
    let drops = AtomicUsize::new(0);
    let make = |finish| Script {
        payload: [23; 2048],
        pending: Cell::new(true),
        address: Cell::new(None),
        polls: &polls,
        drops: &drops,
        finish,
        _pinned: PhantomPinned,
    };
    let rejected = Budget::new(1);
    let (future, allocated) =
        crate::allocation_testing::observe(|| admit(Some(&rejected), make(Finish::Value)));
    assert_eq!(allocated.allocations, 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert!(super::super::is_admission_failure(
        &futures::executor::block_on(future).unwrap_err()
    ));
    assert_eq!(rejected.available(), 1);

    for admitted in [false, true] {
        for polls_before_drop in 0..=2 {
            for finish in [Finish::Value, Finish::Error, Finish::Panic] {
                let budget = Budget::new(8192);
                let before_drops = drops.load(Ordering::Relaxed);
                let (mut future, allocated) = crate::allocation_testing::observe(|| {
                    admit(admitted.then_some(&budget), make(finish))
                });
                assert_eq!(allocated.allocations, 1);
                let remaining = 8192 - if admitted { allocated.bytes } else { 0 };
                assert_eq!(budget.available(), remaining);
                if polls_before_drop > 0 {
                    let mut context = task::Context::from_waker(futures::task::noop_waker_ref());
                    assert!(future.as_mut().poll(&mut context).is_pending());
                }
                let mut moved = future;
                if polls_before_drop > 1 {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        futures::executor::block_on(&mut moved)
                    }));
                    match finish {
                        Finish::Value => assert_eq!(result.unwrap().unwrap(), 23),
                        Finish::Error => assert!(result.unwrap().is_err()),
                        Finish::Panic => assert!(result.is_err()),
                    }
                }
                // A completed or panicked future still owns its box until dropped.
                assert_eq!(budget.available(), remaining);
                drop(moved);
                assert_eq!(budget.available(), 8192);
                assert_eq!(drops.load(Ordering::Relaxed), before_drops + 1);
            }
        }
    }
}
