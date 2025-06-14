use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::{router::router::HelixRouter, thread_pool::thread_pool::ThreadPool};
use crate::helix_runtime::AsyncRuntime;
use crate::helix_transport::{Listener, Transport};
use chrono::{DateTime, Utc};
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

pub struct ConnectionHandler<R, T>
where
    R: AsyncRuntime + Clone + Send + Sync + 'static,
    T: Transport,
{
    pub address: String,
    pub active_connections: Arc<Mutex<HashMap<String, ClientConnection>>>,
    pub thread_pool: ThreadPool<R, T::Stream>,
    pub runtime: R,
    transport: T,
}

pub struct ClientConnection {
    pub id: String,
    pub last_active: DateTime<Utc>,
    pub addr: SocketAddr,
}

impl<R, T> ConnectionHandler<R, T>
where
    R: AsyncRuntime + Clone + Send + Sync + 'static,
    T: Transport,
    T::Stream: 'static,
{
    pub fn new(
        address: &str,
        graph: Arc<HelixGraphEngine>,
        size: usize,
        router: HelixRouter,
        runtime: R,
        transport: T,
    ) -> Result<Self, GraphError> {
        Ok(Self {
            address: address.to_string(),
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            thread_pool: ThreadPool::new(size, graph, Arc::new(router), runtime.clone())?,
            runtime,
            transport,
        })
    }

    pub async fn accept_conns(&self) -> Result<<R as AsyncRuntime>::JoinHandle<()>, GraphError> {
        let addr: SocketAddr = self.address.parse().map_err(|e| {
            GraphError::GraphConnectionError(
                "Invalid address".to_string(),
                io::Error::new(io::ErrorKind::InvalidInput, e),
            )
        })?;
        let listener = self.transport.bind(addr).await.map_err(|e| {
            eprintln!("Failed to bind to address {}: {}", self.address, e);
            GraphError::GraphConnectionError("Failed to bind to address".to_string(), e)
        })?;

        let active_connections = Arc::clone(&self.active_connections);
        let thread_pool_sender = self.thread_pool.sender.clone();

        let runtime = self.runtime.clone();
        let handle = runtime.spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let client_id = Uuid::new_v4().to_string();
                        let client = ClientConnection {
                            id: client_id.clone(),
                            last_active: Utc::now(),
                            addr,
                        };

                        active_connections
                            .lock()
                            .unwrap()
                            .insert(client_id.clone(), client);

                        match thread_pool_sender.send_async(stream).await {
                            Ok(_) => (),
                            Err(e) => {
                                eprintln!(
                                    "Error sending connection {} to thread pool: {}",
                                    client_id, e
                                );
                                active_connections.lock().unwrap().remove(&client_id);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error accepting connection: {}", e);
                    }
                }
            }
        });

        Ok(handle)
    }
}
