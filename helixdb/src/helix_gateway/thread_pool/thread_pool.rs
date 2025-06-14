use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use flume::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use crate::helix_runtime::AsyncRuntime;

use crate::helix_gateway::router::router::{HelixRouter, RouterError};
use crate::protocol::request::Request;
use crate::protocol::response::Response;

use crate::helix_transport::Stream;

pub struct Worker<R: AsyncRuntime, S: Stream> {
    pub id: usize,
    pub handle: <R as AsyncRuntime>::JoinHandle<()>,
    pub runtime: R,
    _marker: std::marker::PhantomData<S>,
}

impl<R: AsyncRuntime + Clone + Send + Sync + 'static, S: Stream + 'static> Worker<R, S> {
    fn new(
        id: usize,
        graph_access: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        rx: Receiver<S>,
        runtime: R,
    ) -> Worker<R, S> {
        let handle = runtime.spawn(async move {
            loop {
                let mut conn = match rx.recv_async().await {
                    Ok(stream) => stream,
                    Err(e) => {
                        eprintln!("Error receiving connection: {:?}", e);
                        continue;
                    }
                };

                let request = match Request::from_stream(&mut conn).await {
                    Ok(request) => request,
                    Err(e) => {
                        eprintln!("Error parsing request: {:?}", e);
                        continue;
                    }
                };

                let mut response = Response::new();
                if let Err(e) = router.handle(Arc::clone(&graph_access), request, &mut response) {
                    eprintln!("Error handling request: {:?}", e);
                    response.status = 500;
                    response.body = format!("\n{:?}", e).into_bytes();
                }

                if let Err(e) = response.send(&mut conn).await {
                    eprintln!("Error sending response: {:?}", e);
                    match e.kind() {
                        std::io::ErrorKind::BrokenPipe => {
                            eprintln!("Client disconnected before response could be sent");
                        }
                        std::io::ErrorKind::ConnectionReset => {
                            eprintln!("Connection was reset by peer");
                        }
                        _ => {
                            eprintln!("Unexpected error type: {:?}", e);
                        }
                    }
                }
            }
        });

        Worker {
            id,
            handle,
            runtime,
            _marker: std::marker::PhantomData,
        }
    }
}

pub struct ThreadPool<R: AsyncRuntime, S: Stream> {
    pub sender: Sender<S>,
    pub num_unused_workers: Mutex<usize>,
    pub num_used_workers: Mutex<usize>,
    pub workers: Vec<Worker<R, S>>,
    pub runtime: R,
}
impl<R: AsyncRuntime + Clone + Send + Sync + 'static, S: Stream + 'static> ThreadPool<R, S> {
    pub fn new(
        size: usize,
        graph: Arc<HelixGraphEngine>,
        router: Arc<HelixRouter>,
        runtime: R,
    ) -> Result<ThreadPool<R, S>, RouterError> {
        assert!(
            size > 0,
            "Expected number of threads in thread pool to be more than 0, got {}",
            size
        );

        let (tx, rx) = flume::bounded::<S>(1000);
        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            workers.push(Worker::new(
                id,
                Arc::clone(&graph),
                Arc::clone(&router),
                rx.clone(),
                runtime.clone(),
            ));
        }
        println!("Thread pool initialized with {} workers", workers.len());

        Ok(ThreadPool {
            sender: tx,
            num_unused_workers: Mutex::new(size),
            num_used_workers: Mutex::new(0),
            runtime: runtime,
            workers,
        })
    }
}