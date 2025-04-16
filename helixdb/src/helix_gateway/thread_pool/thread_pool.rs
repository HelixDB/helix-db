use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use crate::helix_gateway::router::router::{HelixRouter, RouterError};
use crate::protocol::{request::Request, response::Response};

use flume::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct ThreadPool {
    pub sender: Sender<(Request, oneshot::Sender<Response>)>,
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

        let (tx, rx) = flume::bounded::<(Request, oneshot::Sender<Response>)>(size * 2); // Bounded channel
        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&graph), Arc::clone(&router), rx.clone()));
        }
        println!("Thread pool initialized with {} workers", workers.len());

        Ok(ThreadPool {
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
        rx: Receiver<(Request, oneshot::Sender<Response>)>,
    ) -> Worker {
        let handle = tokio::spawn(async move {
            loop {
                let (request, response_tx) = match rx.recv_async().await {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("Worker {} error receiving request: {:?}", id, e);
                        continue;
                    }
                };

                let mut response = Response::new();
                if let Err(e) = router.handle(Arc::clone(&graph_access), request, &mut response) {
                    eprintln!("Worker {} error handling request: {:?}", id, e);
                    response.status = 500;
                    response.body = format!("Error handling request: {:?}", e).into_bytes();
                }

                if let Err(e) = response_tx.send(response) {
                    eprintln!("Worker {} error sending response: {:?}", id, e);
                }
            }
        });

        Worker { id, handle }
    }
}
