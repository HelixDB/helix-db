use crate::helix_engine::traversal_core::HelixGraphEngine;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::gateway::CoreSetter;
use crate::helix_gateway::mcp::mcp::MCPToolInput;
use crate::protocol::{self, HelixError, Request};
use flume::{Receiver, Selector, Sender};
use std::iter;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tracing::{error, trace};

use crate::helix_gateway::router::router::{ContChan, ContMsg, HandlerInput, HelixRouter};
use crate::protocol::request::{ReqMsg, RequestType, RetChan};
use crate::protocol::response::Response;

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
    ) -> WorkerPool {
        let (req_tx, req_rx) = flume::bounded::<ReqMsg>(1000);
        let (cont_tx, cont_rx) = flume::bounded::<ContMsg>(1000);

        let num_workers = workers_core_setter.num_threads();
        let workers = iter::repeat_n(workers_core_setter, num_workers)
            .map(|setter| {
                Worker::start(
                    req_rx.clone(),
                    setter,
                    Arc::clone(&graph_access),
                    Arc::clone(&router),
                    Arc::clone(&io_rt),
                    (cont_tx.clone(), cont_rx.clone()),
                )
            })
            .collect();

        WorkerPool {
            tx: req_tx,
            _workers: workers,
        }
    }

    /// Process a request on the Worker Pool
    pub async fn process(&self, req: protocol::request::Request) -> Result<Response, HelixError> {
        let (ret_tx, ret_rx) = oneshot::channel();

        // TODO: add graceful shutdown handling here

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
    ) -> Worker {
        let handle = std::thread::spawn(move || {
            core_setter.set_current();

            trace!("thread started");

            // Set thread local context, so we can access the io runtime
            let _io_guard = io_rt.enter();

            loop {
                Selector::new()
                    .recv(&cont_rx, |m| match m {
                        Ok((ret_chan, cfn)) => {
                            ret_chan.send(cfn().map_err(Into::into)).expect("todo")
                        }
                        Err(_) => error!("Continuation Channel was dropped"),
                    })
                    .recv(&rx, |m| match m {
                        Ok((req, ret_chan)) => request_mapper(
                            req,
                            ret_chan,
                            graph_access.clone(),
                            &router,
                            &io_rt,
                            &cont_tx,
                        ),
                        Err(_) => error!("Request Channel was dropped"),
                    })
                    .wait();
            }
            // trace!("thread shutting down");
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
        name: req_name,
    }));

    ret_chan
        .send(res)
        .expect("Should always be able to send, as only one worker processes a request")
}
