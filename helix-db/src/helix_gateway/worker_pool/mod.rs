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
    write_tx: Sender<ReqMsg>,
    router: Arc<HelixRouter>,
    _workers: Vec<Worker>,
    _writer_worker: Worker,
}

impl WorkerPool {
    pub fn new(
        workers_core_setter: Arc<CoreSetter>,
        graph_access: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        io_rt: Arc<Runtime>,
    ) -> WorkerPool {
        let (req_tx, req_rx) = flume::bounded::<ReqMsg>(1000);
        let (cont_tx, cont_rx) = flume::bounded::<ContMsg>(1000);

        // Dedicated channel for write operations - single writer thread
        let (write_tx, write_rx) = flume::bounded::<ReqMsg>(1000);

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

        // Create the dedicated writer worker (no core pinning needed for single thread)
        let writer_worker = Worker::start_writer(
            write_rx,
            Arc::clone(&graph_access),
            Arc::clone(&router),
            Arc::clone(&io_rt),
        );

        WorkerPool {
            tx: req_tx,
            write_tx,
            router,
            _workers: workers,
            _writer_worker: writer_worker,
        }
    }

    /// Process a request on the Worker Pool
    /// Write operations are routed to a dedicated writer thread to ensure proper LMDB locking
    pub async fn process(&self, req: Request) -> Result<Response, HelixError> {
        let (ret_tx, ret_rx) = oneshot::channel();
        let req_name = req.name.clone();

        // Route to dedicated writer thread or reader worker pool
        let channel = if self.router.is_write_route(&req.name) {
            &self.write_tx
        } else {
            &self.tx
        };

        channel.send_async((req, ret_tx)).await.map_err(|_| {
            error!("WorkerPool channel closed for request '{req_name}'");
            HelixError::Graph(GraphError::New("Server is shutting down".into()))
        })?;

        // Handle the case where the worker might have dropped the sender
        // (e.g., worker thread panicked or client disconnected)
        ret_rx.await.unwrap_or_else(|_| {
            error!("Worker dropped sender without reply for request '{req_name}'");
            Err(HelixError::Graph(GraphError::New(
                "Internal server error: worker failed to respond".into(),
            )))
        })
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
    ) -> Worker {
        let handle = std::thread::spawn(move || {
            core_setter.set_current();

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
                                let result = cfn().map_err(Into::into);
                                if ret_chan.send(result).is_err() {
                                    trace!(
                                        "Client disconnected before continuation response could be sent"
                                    );
                                }
                            }
                            Err(flume::TryRecvError::Disconnected) => {
                                error!("Continuation Channel was dropped");
                                break;
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
                                error!("Request Channel was dropped");
                                break;
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
                                error!("Request Channel was dropped");
                                break;
                            }
                            Err(flume::TryRecvError::Empty) => {}
                        }

                        match cont_rx.recv() {
                            Ok((ret_chan, cfn)) => {
                                let result = cfn().map_err(Into::into);
                                if ret_chan.send(result).is_err() {
                                    trace!(
                                        "Client disconnected before continuation response could be sent"
                                    );
                                }
                            }
                            Err(flume::RecvError::Disconnected) => {
                                error!("Continuation Channel was dropped");
                                break;
                            }
                        }
                    }
                }
            }
        });
        Worker { _handle: handle }
    }

    /// Start a dedicated writer worker thread
    /// This thread handles all write operations to ensure proper LMDB locking
    /// Note: No core pinning for the writer - let the OS scheduler handle it
    pub fn start_writer(
        rx: Receiver<ReqMsg>,
        graph_access: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        io_rt: Arc<Runtime>,
    ) -> Worker {
        let handle = std::thread::spawn(move || {
            // Initialize thread-local metrics buffer
            helix_metrics::init_thread_local();

            // Set thread local context, so we can access the io runtime
            let _io_guard = io_rt.enter();

            // Single-threaded writer: process one request at a time, waiting for
            // any continuations to complete before moving to the next request.
            loop {
                match rx.recv() {
                    Ok((req, ret_chan)) => {
                        // Create a per-request continuation channel
                        let (cont_tx, cont_rx) = flume::bounded::<ContMsg>(1);

                        // Process the request
                        request_mapper(
                            req,
                            ret_chan,
                            graph_access.clone(),
                            &router,
                            &io_rt,
                            &cont_tx,
                        );

                        // Drop our sender so the channel disconnects when the async future
                        // (which holds a clone) completes.
                        drop(cont_tx);

                        // Poll continuation channel until sender is dropped.
                        while let Ok((ret_chan, cfn)) = cont_rx.recv() {
                            let result = cfn().map_err(Into::into);
                            if ret_chan.send(result).is_err() {
                                trace!(
                                    "Client disconnected before continuation response could be sent"
                                );
                            }
                        }
                    }
                    Err(_) => {
                        trace!("Writer request channel was dropped, shutting down");
                        break;
                    }
                }
            }
        });
        Worker { _handle: handle }
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
        name: req_name.clone(),
    }));

    // Client may have disconnected before we could send the response.
    // This is normal behavior - just log at trace level and continue.
    if ret_chan.send(res).is_err() {
        trace!(
            "Client disconnected before response could be sent for request '{}'",
            req_name
        );
    }
}
