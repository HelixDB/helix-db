use crate::protocol::{request::Request, response::Response};
use crate::helix_engine::{
    graph_core::graph_core::HelixGraphEngine,
    types::GraphError,
};
use crate::helix_gateway::{
    router::router::{HelixRouter, HandlerFn},
    thread_pool::thread_pool::ThreadPool,
};

use hyper::server::conn::AddrIncoming;
use tokio::sync::oneshot;
use flume::Sender;
use hyper::{
    service::{make_service_fn, Service},
    Body,
    Request as HyperRequest,
    Response as HyperResponse,
    Server,
};
use std::{
    collections::HashMap,
    sync::Arc,
    convert::Infallible,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

pub struct GatewayOpts {}

impl GatewayOpts {
    pub const DEFAULT_POOL_SIZE: usize = 1024;
}

#[derive(Clone)]
struct GatewayService {
    graph: Arc<HelixGraphEngine>,
    router: Arc<HelixRouter>,
    sender: Sender<(Request, oneshot::Sender<Response>)>,
}

impl Service<HyperRequest<Body>> for GatewayService {
    type Response = HyperResponse<hyper::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HyperRequest<Body>) -> Self::Future {
        let graph = Arc::clone(&self.graph);
        let router = Arc::clone(&self.router);
        let sender = self.sender.clone();
        Box::pin(handle_request(req, graph, router, sender))
    }
}

pub struct HelixGateway {
    pub server: Server<AddrIncoming, GatewayService>,
    pub thread_pool: ThreadPool,
}

impl HelixGateway {
    pub async fn new(
        address: &str,
        graph: Arc<HelixGraphEngine>,
        size: usize,
        routes: Option<HashMap<(String, String), HandlerFn>>,
    ) -> Result<HelixGateway, GraphError> {
        let addr: SocketAddr = address.parse().map_err(|e| {
            format!("Invalid address: {}", e)
        })?;

        let router = Arc::new(HelixRouter::new(routes));
        let thread_pool = ThreadPool::new(size, Arc::clone(&graph), Arc::clone(&router))?;
        let sender = thread_pool.sender.clone();

        let service = GatewayService {
            graph: Arc::clone(&graph),
            router: Arc::clone(&router),
            sender,
        };

        //let make_service = make_service_fn(move |_conn| {
        //    let service = service.clone();
        //    async move { Ok::<_, Infallible>(service) }
        //});

        let server = Server::bind(&addr).serve(service);

        println!("Gateway created, listening on {}", address);
        Ok(HelixGateway { server, thread_pool })
    }

    pub async fn run(self) -> Result<(), hyper::Error> {
        self.server.await
    }
}

async fn hyper_to_internal_request(hyper_req: HyperRequest<Body>) -> Result<Request, GraphError> {
    let method = hyper_req.method().to_string();
    let path = hyper_req.uri().path().to_string();
    let headers: HashMap<String, String> = hyper_req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let body_bytes = hyper::body::to_bytes(hyper_req.into_body()).await
        .map_err(|e| GraphError::New(format!("Failed to read request body: {}", e)))?;
    let body = body_bytes.to_vec();

    Ok(Request { method, path, headers, body })
}

fn internal_to_hyper_response(response: Response) -> HyperResponse<Body> {
    let mut builder = HyperResponse::builder()
        .status(response.status);

    for (key, value) in response.headers {
        builder = builder.header(key.as_str(), value.as_str());
    }

    builder.body(Body::from(response.body)).unwrap()
}

async fn handle_request(
    req: HyperRequest<Body>,
    graph: Arc<HelixGraphEngine>,
    router: Arc<HelixRouter>,
    sender: Sender<(Request, oneshot::Sender<Response>)>,
) -> Result<HyperResponse<hyper::body::Body>, Infallible> {
    let internal_req = match hyper_to_internal_request(req).await {
        Ok(req) => req,
        Err(e) => {
            let mut response = Response::new();
            response.status = 500;
            response.body = format!("Error parsing request: {}", e).into_bytes();
            return Ok(internal_to_hyper_response(response));
        }
    };

    let (tx, rx) = oneshot::channel();

    // send request to thread pool
    if let Err(e) = sender.send_async((internal_req, tx)).await {
        let mut response = Response::new();
        response.status = 500;
        response.body = format!("Error sending to thread pool: {}", e).into_bytes();
        return Ok(internal_to_hyper_response(response));
    }

    // wait for response from worker
    let response = match rx.await {
        Ok(response) => response,
        Err(e) => {
            let mut response = Response::new();
            response.status = 500;
            response.body = format!("Error receiving response: {}", e).into_bytes();
            return Ok(internal_to_hyper_response(response));
        }
    };

    Ok(internal_to_hyper_response(response))
}

/*
#[cfg(test)]
mod tests {
    use crate::helix_engine::graph_core::config::Config;
    use crate::helix_gateway::connection::connection::ConnectionHandler;
    use crate::helix_engine::{types::GraphError, graph_core::graph_core::HelixGraphEngineOpts};
    use crate::protocol::{request::Request, response::Response};
    use crate::helix_gateway::router::router::HelixRouter;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::{
        io::{AsyncRead, AsyncWrite},
        net::{TcpListener, TcpStream},

        time::Duration,
    };
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::helix_gateway::thread_pool::thread_pool::ThreadPool;

    use super::*;

    fn setup_temp_db() -> (HelixGraphEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();
        let opts = HelixGraphEngineOpts{
           path: db_path.to_string(),
           config: Config::default(),
        };
        let storage = HelixGraphEngine::new(opts).unwrap();
        (storage, temp_dir)
    }

    async fn create_test_connection() -> std::io::Result<(TcpStream, TcpStream)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let client = TcpStream::connect(addr).await?;
        let server = listener.accept().await?.0;

        Ok((client, server))
    }

    async fn read_with_timeout(stream: &mut TcpStream, timeout: Duration) -> std::io::Result<Vec<u8>> {
        let start = std::time::Instant::now();
        let mut received = Vec::new();
        let mut buffer = [0; 1024];

        while start.elapsed() < timeout {
            match stream.read(&mut buffer).await {
                Ok(0) => break, // If EOF reached
                Ok(n) => received.extend_from_slice(&buffer[..n]),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Ok(received)
    }

    #[tokio::test]
    async fn test_response_creation_and_sending() -> std::io::Result<()> {
        let (mut client, mut server) = create_test_connection().await?;

        let mut response = Response::new();
        response.status = 200;
        response
            .headers
            .insert("Content-Type".to_string(), "text/plain".to_string());
        response.body = b"Hello World".to_vec();

        println!("{:?}", response);
        response.send(&mut server).await?;
        server.flush().await?;

        let received = read_with_timeout(&mut client, Duration::from_millis(100)).await?;
        let response_str = String::from_utf8_lossy(&received);

        println!("{:?}", response_str);
        assert!(response_str.contains("HTTP/1.1 200 OK"));
        assert!(response_str.contains("Content-Type: text/plain"));
        assert!(response_str.contains("Content-Length: 11"));
        assert!(response_str.to_string().contains("Hello World"));

        Ok(())
    }

    #[test]
    fn test_thread_pool_creation() {
        let (storage, _) = setup_temp_db();
        let size = 4;
        let router = Arc::new(HelixRouter::new(None));
        let graph = Arc::new(storage);
        let pool = ThreadPool::new(size, graph, router).unwrap();

        assert_eq!(*pool.num_unused_workers.lock().unwrap(), size);
        assert_eq!(*pool.num_used_workers.lock().unwrap(), 0);
    }

    #[test]
    #[should_panic(expected = "Expected number of threads in thread pool to be more than 0")]
    fn test_thread_pool_zero_size() {
        let (storage, _) = setup_temp_db();
        let router = Arc::new(HelixRouter::new(None));
        let graph = Arc::new(storage);
        ThreadPool::new(0, graph, router).unwrap();
    }

    #[tokio::test]
    async fn test_connection_handler() -> Result<(), GraphError> {
        let (storage, _) = setup_temp_db();
        let address = "127.0.0.1:0";

        let router = HelixRouter::new(None);
        let graph = Arc::new(storage);
        let handler = ConnectionHandler::new(address, graph, 4, router).unwrap();

        let addr = handler.address.clone();
        let _client = TcpStream::connect(addr).await.unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_router_integration() -> std::io::Result<()> {
        let (mut client, mut server) = create_test_connection().await?;
        let (storage, _) = setup_temp_db();
        let mut router = HelixRouter::new(None);
        let graph_storage = Arc::new(storage);

        // Add route
        router.add_route("GET", "/test", |_, response| {
            response.status = 200;
            response.body = b"Success".to_vec();
            response
                .headers
                .insert("Content-Type".to_string(), "text/plain".to_string());
            Ok(())
        });

        // Send test request
        let request_str = "GET /test HTTP/1.1\r\nHost: localhost\r\n\r\n";
        client.write_all(request_str.as_bytes()).await?;
        client.flush().await?;

        // Handle Request
        let request = Request::from_stream(&mut server).await?;
        let mut response = Response::new();
        router
            .handle(graph_storage, request, &mut response)
            .unwrap();
        response.send(&mut server).await?;
        server.flush().await?;

        let received = read_with_timeout(&mut client, Duration::from_millis(100)).await?;
        let response_str = String::from_utf8_lossy(&received);

        println!("{:?}", response_str);
        assert!(response_str.contains("HTTP/1.1 200 OK"));
        assert!(response_str.contains("Content-Type: text/plain"));
        assert!(response_str.to_string().contains("Success"));

        Ok(())
    }
}
*/
