use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::router::router::{HelixRouter, RouterError};
use crate::protocol::{request::Request, response::Response};

use flume::{Receiver, Sender};
use http_body_util::combinators::BoxBody;
use http_body_util::Full;
use hyper::{
    body::{Bytes, Incoming},
    service::service_fn,
    Request as HyperRequest, Response as HyperResponse,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct ThreadPool {
    pub receiver: Receiver<tokio::net::TcpStream>,
    pub sender: Sender<tokio::net::TcpStream>,
    pub num_unused_workers: Mutex<usize>,
    pub num_used_workers: Mutex<usize>,
    pub workers: Vec<Worker>,
}

impl ThreadPool {
    pub fn new(
        size: usize,
        graph: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
    ) -> Result<ThreadPool, RouterError> {
        assert!(
            size > 0,
            "Expected number of threads in thread pool to be more than 0, got {}",
            size
        );

        let (tx, rx) = flume::unbounded::<tokio::net::TcpStream>(); // Bounded channel
        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(
                id,
                Arc::clone(&graph),
                Arc::clone(&router),
                rx.clone(),
            ));
        }
        println!("Thread pool initialized with {} workers", workers.len());

        Ok(ThreadPool {
            receiver: rx,
            sender: tx,
            num_unused_workers: Mutex::new(size),
            num_used_workers: Mutex::new(0),
            workers,
        })
    }
}

pub struct Worker {
    pub id: usize,
    pub handle: JoinHandle<()>,
}

impl Worker {
    fn new(
        id: usize,
        graph_access: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        rx: Receiver<tokio::net::TcpStream>,
    ) -> Worker {
        println!("Worker {} initialized {:?}", id, rx);
        let handle = tokio::spawn(async move {
            loop {
                let graph = Arc::clone(&graph_access);
                let router = Arc::clone(&router);
                println!("Worker {} waiting for connection {:?}", id, rx);
                match rx.recv_async().await {
                    Ok(stream) => {
                        println!("Worker {} received connection", id);
                        let io = TokioIo::new(stream);
                        if let Err(err) = Builder::new(TokioExecutor::new())
                            .serve_connection(
                                io,
                                service_fn(move |hyper_req: HyperRequest<Incoming>| {
                                    handle_request(hyper_req, Arc::clone(&graph), Arc::clone(&router))
                                }),
                            )
                            .await
                        {
                            eprintln!("Worker {} error serving connection: {:?}", id, err);
                        }
                    }
                    Err(e) => {
                        println!("Worker {} shutting down: {}", id, e);
                        break;
                    }
                }
            }
        });

        Worker { id, handle }
    }
}

async fn handle_request(
    hyper_req: HyperRequest<Incoming>,
    graph: Arc<HelixGraphEngine>,
    router: Arc<HelixRouter>,
) -> Result<HyperResponse<Full<Bytes>>, Infallible> {
    // TODO: handle request
    let request = Request::from_hyper_request(hyper_req).await.unwrap(); // TODO: handle error

    let mut response = Response::new();
    router.handle(graph, request, &mut response).unwrap();
    // TODO: handle request
    let mut builder = HyperResponse::builder().status(response.status);

    for (key, value) in response.headers {
        builder = builder.header(key.as_str(), value.as_str());
    }

    Ok(builder.body(Full::new(Bytes::from(response.body))).unwrap())
}
