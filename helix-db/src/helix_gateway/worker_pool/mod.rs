use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use crate::helix_gateway::gateway::CoreSetter;
use crate::protocol::{self, HelixError};
use flume::{Receiver, Sender};
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::trace;

use crate::helix_gateway::router::router::{ContFn, HelixRouter};
use crate::protocol::request::ReqMsg;
use crate::protocol::response::Response;

/// A Thread Pool of workers to execute Database operations
pub struct WorkerPool {
    req_tx: Sender<ReqMsg>,
    _workers: Vec<Worker>,
}

impl WorkerPool {
    pub fn new(
        size: usize,
        core_setter: Option<CoreSetter>,
        graph_access: Arc<HelixGraphEngine>,
        io_rt: Arc<Runtime>,
        router: Arc<HelixRouter>,
    ) -> WorkerPool {
        assert!(
            size > 0,
            "Expected number of threads in thread pool to be more than 0, got {size}"
        );

        let (req_tx, req_rx) = flume::bounded::<ReqMsg>(1000); // TODO: make this configurable

        let workers = (0..size)
            .map(|_| {
                Worker::start(
                    req_rx.clone(),
                    core_setter.clone(),
                    graph_access.clone(),
                    io_rt.clone(),
                    router.clone(),
                )
            })
            .collect::<Vec<_>>();

        WorkerPool {
            req_tx,
            _workers: workers,
        }
    }

    /// Process a request on the Worker Pool
    pub async fn process(&self, req: protocol::request::Request) -> Result<Response, HelixError> {
        let (ret_tx, ret_rx) = oneshot::channel();

        // TODO: add graceful shutdown handling here

        // this read by Worker in start()
        self.req_tx
            .send_async((req, ret_tx))
            .await
            .expect("WorkerPool channel should be open");

        // This is sent by the Worker

        ret_rx
            .await
            .expect("Worker shouldn't drop sender before replying")
    }
}

struct Worker {
    _handle: JoinHandle<()>,
}

#[derive(Clone)]
pub struct TaskContext {
    pub graph_access: Arc<HelixGraphEngine>,
    pub io_rt: Arc<Runtime>,
    pub cont_tx: Sender<ContFn>,
}

impl Worker {
    pub fn start(
        req_rx: Receiver<ReqMsg>,
        core_setter: Option<CoreSetter>,
        graph_access: Arc<HelixGraphEngine>,
        io_rt: Arc<Runtime>,
        router: Arc<HelixRouter>,
    ) -> Worker {
        let handle = std::thread::spawn(move || {
            if let Some(cs) = core_setter {
                cs.set_current();
            }

            trace!("thread started");

            let (cont_tx, cont_rx) = flume::bounded::<ContFn>(1000); // TODO: make this configurable

            let context = TaskContext {
                graph_access,
                io_rt,
                cont_tx,
            };

            loop {
                flume::Selector::new()
                    // Priorities continuations
                    // flume: eventual-fairness should be off
                    .recv(&cont_rx, |v| {
                        if let Ok(cont_func) = v {
                            cont_func();
                        }
                    })
                    .recv(&req_rx, |v| {
                        if let Ok((req, ret_chan)) = v {
                            router.handle(context.clone(), req, ret_chan);
                        }
                    });
            }

            trace!("thread shutting down");
        });
        Worker { _handle: handle }
    }
}
