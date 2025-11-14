use crate::helix_engine::{traversal_core::HelixGraphEngine, types::GraphError};
use crate::helix_gateway::{
    gateway::CoreSetter,
    mcp::mcp::MCPToolInput,
    router::router::{ContChan, ContMsg, HandlerInput, HelixRouter},
};
use crate::protocol::{
    HelixError, Request,
    request::{ReqMsg, RequestType, RetChan},
    response::Response,
};
use flume::{Receiver, Sender};
use std::iter;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::{error, trace};

/// A Thread Pool of workers to execute Database operations
pub struct WorkerPool {
    tx: Sender<ReqMsg>,
    _workers: Vec<Worker>,
}

impl WorkerPool {
    pub fn new(
        workers_core_setter: Arc<CoreSetter>,
        graph_access: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        io_rt: Arc<Runtime>,
    ) -> Self {
        let (req_tx, req_rx) = flume::bounded::<ReqMsg>(1000);
        let (cont_tx, cont_rx) = flume::bounded::<ContMsg>(1000);

        let num_workers = workers_core_setter.num_threads();
        if num_workers < 2 {
            panic!("The number of workers must be at least 2 for parity to act as a select.");
        }
        if !num_workers.is_multiple_of(2) {
            println!("Expected an even number of workers, got {num_workers}");
            panic!("The number of workers should be a multiple of 2 for fairness.");
        }

        let workers = iter::repeat_n(workers_core_setter, num_workers)
            .enumerate()
            .map(|(i, setter)| {
                Worker::start(
                    req_rx.clone(),
                    setter,
                    Arc::clone(&graph_access),
                    Arc::clone(&router),
                    Arc::clone(&io_rt),
                    (cont_tx.clone(), cont_rx.clone()),
                    i % 2 == 0,
                )
            })
            .collect();

        Self {
            tx: req_tx,
            _workers: workers,
        }
    }

    /// Process a request on the Worker Pool
    pub async fn process(&self, req: Request) -> Result<Response, HelixError> {
        let (ret_tx, ret_rx) = oneshot::channel();

        // this read by Worker in start()
        self.tx
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

impl Worker {
    pub fn start(
        rx: Receiver<ReqMsg>,
        core_setter: Arc<CoreSetter>,
        graph_access: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        io_rt: Arc<Runtime>,
        (cont_tx, cont_rx): (ContChan, Receiver<ContMsg>),
        parity: bool,
    ) -> Self {
        let handle = std::thread::spawn(move || {
            core_setter.set_current();

            trace!("thread started");

            // Initialize thread-local metrics buffer
            helix_metrics::init_thread_local();

            // Set thread local context, so we can access the io runtime
            let _io_guard = io_rt.enter();

            // To avoid a select, we try_recv on one channel and then wait on the other.
            // Since we have multiple workers, we use parity to decide which order around,
            // meaning if there's at least 2 worker threads its a fair select.
            match parity {
                true => {
                    loop {
                        // cont_rx.try_recv() then rx.recv()

                        match cont_rx.try_recv() {
                            Ok((ret_chan, cfn)) => {
                                ret_chan.send(cfn().map_err(Into::into)).expect("todo")
                            }
                            Err(flume::TryRecvError::Disconnected) => {
                                error!("Continuation Channel was dropped")
                            }
                            Err(flume::TryRecvError::Empty) => {}
                        }

                        match rx.recv() {
                            Ok((req, ret_chan)) => request_mapper(
                                req,
                                ret_chan,
                                graph_access.clone(),
                                &router,
                                &io_rt,
                                &cont_tx,
                            ),
                            Err(flume::RecvError::Disconnected) => {
                                error!("Request Channel was dropped")
                            }
                        }
                    }
                }
                false => {
                    loop {
                        // rx.try_recv() then cont_rx.recv()

                        match rx.try_recv() {
                            Ok((req, ret_chan)) => request_mapper(
                                req,
                                ret_chan,
                                graph_access.clone(),
                                &router,
                                &io_rt,
                                &cont_tx,
                            ),
                            Err(flume::TryRecvError::Disconnected) => {
                                error!("Request Channel was dropped")
                            }
                            Err(flume::TryRecvError::Empty) => {}
                        }

                        match cont_rx.recv() {
                            Ok((ret_chan, cfn)) => {
                                ret_chan.send(cfn().map_err(Into::into)).expect("todo")
                            }
                            Err(flume::RecvError::Disconnected) => {
                                error!("Continuation Channel was dropped")
                            }
                        }
                    }
                }
            }
        });
        Self { _handle: handle }
    }
}

fn request_mapper(
    request: Request,
    ret_chan: RetChan,
    graph_access: Arc<HelixGraphEngine>,
    router: &HelixRouter,
    io_rt: &Runtime,
    cont_tx: &ContChan,
) {
    let req_name = request.name.clone();
    let req_type = request.req_type;

    let res = match request.req_type {
        RequestType::Query => {
            if let Some(handler) = router.routes.get(&request.name) {
                let input = HandlerInput {
                    request,
                    graph: graph_access,
                };

                match handler(input) {
                    Err(GraphError::IoNeeded(cont_closure)) => {
                        let fut = cont_closure.0(cont_tx.clone(), ret_chan);
                        io_rt.spawn(fut);
                        return;
                    }
                    res => Some(res.map_err(Into::into)),
                }
            } else {
                None
            }
        }
        RequestType::MCP => {
            if let Some(mcp_handler) = router.mcp_routes.get(&request.name) {
                let mut mcp_input = MCPToolInput {
                    request,
                    mcp_backend: Arc::clone(
                        graph_access
                            .mcp_backend
                            .as_ref()
                            .expect("MCP backend not found"),
                    ),
                    mcp_connections: Arc::clone(
                        graph_access
                            .mcp_connections
                            .as_ref()
                            .expect("MCP connections not found"),
                    ),
                    schema: graph_access.storage.storage_config.schema.clone(),
                };
                Some(mcp_handler(&mut mcp_input).map_err(Into::into))
            } else {
                None
            }
        }
    };

    let res = res.unwrap_or(Err(HelixError::NotFound {
        ty: req_type,
        name: req_name,
    }));

    ret_chan
        .send(res)
        .expect("Should always be able to send, as only one worker processes a request")
}
