use crate::helix_engine::graph_core::graph_core::HelixGraphEngine;
use crate::helix_engine::types::GraphError;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use std::{
    net::SocketAddr,
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    net::TcpListener,
    task::JoinHandle,
};

use crate::helix_gateway::{router::router::HelixRouter, thread_pool::thread_pool::ThreadPool};

pub struct ConnectionHandler {
    pub address: String,
    pub active_connections: Arc<Mutex<HashMap<String, ClientConnection>>>,
    pub thread_pool: ThreadPool,
}

pub struct ClientConnection {
    pub id: String,
    pub last_active: DateTime<Utc>,
    pub addr: SocketAddr,
}

impl ConnectionHandler {
    pub fn new(
        address: &str,
        graph: Arc<HelixGraphEngine>,
        size: usize,
        router: HelixRouter,
    ) -> Result<Self, GraphError> {
        Ok(Self {
            address: address.to_string(),
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            thread_pool: ThreadPool::new(size, graph, Arc::new(router))?,
        })
    }

    pub async fn accept_conns(&self) -> Result<JoinHandle<()>, GraphError> {
        // Create a new TcpListener for each accept_conns call
        let listener = TcpListener::bind(&self.address).await.map_err(|e| {
            eprintln!("Failed to bind to address {}: {}", self.address, e);
            GraphError::GraphConnectionError("Failed to bind to address".to_string(), e)
        })?;

        // Log binding success to stderr since stdout might be buffered

        let active_connections = Arc::clone(&self.active_connections);
        let thread_pool_sender = self.thread_pool.sender.clone();


        let handle = tokio::spawn(async move {

            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {

                        // Configure TCP stream
                        if let Err(e) = stream.set_nodelay(true) {
                            eprintln!("Failed to set TCP_NODELAY: {}", e);
                        }

                        // Create a client connection record
                        let client_id = Uuid::new_v4().to_string();
                        let client = ClientConnection {
                            id: client_id.clone(),
                            last_active: Utc::now(),
                            addr,
                        };

                        // Add to active connections
                        active_connections
                            .lock()
                            .unwrap()
                            .insert(client_id.clone(), client);

                        // Send to thread pool
                        match thread_pool_sender.send_async(stream).await {
                            Ok(_) => (),
                            Err(e) => {
                                eprintln!("Error sending connection {} to thread pool: {}", client_id, e);
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
