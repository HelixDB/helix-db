use std::{collections::HashMap};
use http_body_util::BodyExt;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader, BufWriter, Result as TokioResult};
use hyper::{
    body::{Bytes, Incoming},
    service::service_fn,
    Request as HyperRequest, Response as HyperResponse,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};

use crate::helix_engine::types::GraphError;
#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub headers: HashMap<String, String>,
    pub path: String,
    pub body: Vec<u8>,
}

impl Request {
    /// Parse a request from a stream
    /// 
    /// # Example
    /// 
    /// ```rust 
    /// use std::io::Cursor;
    /// use helixdb::protocol::request::Request;
    /// 
    /// let request = Request::from_stream(Cursor::new("GET /test HTTP/1.1\r\n\r\n")).unwrap();
    /// assert_eq!(request.method, "GET");
    /// assert_eq!(request.path, "/test");
    /// ```
    pub async fn from_stream<R: AsyncRead + Unpin>(stream: &mut R) -> Result<Request, GraphError> {
        let mut reader = BufReader::new(stream);
        let mut first_line = String::new();
        reader.read_line(&mut first_line).await?;

        // Get method and path
        let mut parts = first_line.trim().split_whitespace();
        let method = parts.next()
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Missing HTTP method: {}", first_line)
            ))?.to_string();
        let path = parts.next()
            .ok_or_else(|| std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Missing path: {}", first_line)
            ))?.to_string();

        // Parse headers
        let mut headers = HashMap::new();
        let mut line = String::new();
        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await?;
            if bytes_read == 0 || line.eq("\r\n") || line.eq("\n") {
                break;
            }
            if let Some((key, value)) = line.trim().split_once(':') {
                headers.insert(
                    key.trim().to_lowercase(),
                    value.trim().to_string()
                );
            }
        }

        // Read body
        let mut body = Vec::new();
        if let Some(length) = headers.get("content-length") {
            if let Ok(length) = length.parse::<usize>() {
                let mut buffer = vec![0; length];
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5), 
                    reader.read_exact(&mut buffer)
                ).await {
                    Ok(Ok(_)) => body = buffer,
                    Ok(Err(e)) => {
                        eprintln!("Error reading body: {}", e);
                        return Err(GraphError::New("Error reading body".to_string()));
                    },
                    Err(_) => {
                        eprintln!("Timeout reading body");
                        return Err(GraphError::New("Timeout reading body".to_string()));
                    }
                }
            }
        }

        Ok(Request {
            method,
            headers,
            path,
            body,
        })
    }

    pub async fn from_hyper_request(req: HyperRequest<Incoming>) -> Result<Request, GraphError> {
        let method = req.method().to_string();
        let path = req.uri().path().to_string();
        let headers: HashMap<String, String> = req
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
    
        let body_bytes = req.into_body().collect().await.map_err(|e| GraphError::New(format!("Failed to read request body: {}", e)))?;
    
        let body = body_bytes.to_bytes().to_vec();

        let request = Request {
            method,
            path,
            headers,
            body,
        };

        Ok(request)
    }
}