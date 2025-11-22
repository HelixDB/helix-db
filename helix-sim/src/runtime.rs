use crate::time::SleepFuture;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

pub trait AsyncRuntime: Send + Sync + 'static {
    fn spawn_boxed(
        &self,
        future: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Box<dyn std::any::Any>;
    fn sleep(&self, duration: Duration) -> SleepFuture;
    fn now(&self) -> u64;
}

pub struct TokioRuntime;

impl AsyncRuntime for TokioRuntime {
    fn spawn_boxed(
        &self,
        future: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Box<dyn std::any::Any> {
        Box::new(tokio::spawn(async { future.await }))
    }

    fn sleep(&self, duration: Duration) -> SleepFuture {
        SleepFuture::Tokio(tokio::time::sleep(duration))
    }

    fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

pub struct DeterministicRuntime {
    state: Arc<parking_lot::Mutex<RuntimeState>>,
}

struct RuntimeState {
    current_time: u64,
    tasks: Vec<Task>,
    sleepers: std::collections::BinaryHeap<Sleeper>,
}

struct Task {
    _id: u64,
    _future: Pin<Box<dyn Future<Output = ()> + Send>>,
}

#[derive(Debug, Clone)]
struct Sleeper {
    wake_time: u64,
    waker: Option<std::task::Waker>,
}

impl PartialEq for Sleeper {
    fn eq(&self, other: &Self) -> bool {
        self.wake_time == other.wake_time
    }
}

impl Eq for Sleeper {}

impl PartialOrd for Sleeper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other.wake_time.partial_cmp(&self.wake_time)
    }
}

impl Ord for Sleeper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.wake_time.cmp(&self.wake_time)
    }
}

impl DeterministicRuntime {
    pub fn new() -> Self {
        Self {
            state: Arc::new(parking_lot::Mutex::new(RuntimeState {
                current_time: 0,
                tasks: Vec::new(),
                sleepers: std::collections::BinaryHeap::new(),
            })),
        }
    }

    pub fn step_one(&self) -> bool {
        let mut state = self.state.lock();

        if state.tasks.is_empty() {
            return false;
        }

        let _task = state.tasks.remove(0);
        drop(state);

        true
    }

    pub fn advance_to(&self, time: u64) {
        let mut state = self.state.lock();
        state.current_time = time;

        while let Some(sleeper) = state.sleepers.peek() {
            if sleeper.wake_time <= time {
                let sleeper = state.sleepers.pop().unwrap();
                if let Some(waker) = sleeper.waker {
                    waker.wake();
                }
            } else {
                break;
            }
        }
    }
}

impl Clone for DeterministicRuntime {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl AsyncRuntime for DeterministicRuntime {
    fn spawn_boxed(
        &self,
        _future: Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> Box<dyn std::any::Any> {
        Box::new(())
    }

    fn sleep(&self, duration: Duration) -> SleepFuture {
        SleepFuture::Deterministic {
            wake_time: self.now() + duration.as_nanos() as u64,
            runtime: self.clone(),
        }
    }

    fn now(&self) -> u64 {
        self.state.lock().current_time
    }
}
