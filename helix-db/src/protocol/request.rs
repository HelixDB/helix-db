use axum::{body::Bytes, extract::FromRequest};
use reqwest::{
    StatusCode,
    header::{ACCEPT, CONTENT_TYPE},
};
use tokio::sync::oneshot;
use tracing::error;

use crate::protocol::{Format, HelixError, Response};

pub type RetChan = oneshot::Sender<Result<Response, HelixError>>;

pub type ReqMsg = (Request, RetChan);

#[derive(Debug, Clone)]
pub struct Request {
    pub name: String,
    pub req_type: RequestType,
    /// This contains the input parameters serialized with in_fmt
    pub body: Bytes,
    pub in_fmt: Format,
    pub out_fmt: Format,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RequestType {
    Query,
    MCP,
}

impl<S> FromRequest<S> for Request
where
    S: Send + Sync,
{
    #[doc = " If the extractor fails it\'ll use this \"rejection\" type. A rejection is"]
    #[doc = " a kind of error that can be converted into a response."]
    type Rejection = StatusCode;

    #[doc = " Perform the extraction."]
    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        let path = req.uri().path();

        let (name, req_type) = match path.strip_prefix("/mcp/") {
            Some(n) => (n.to_string(), RequestType::MCP),
            None => (
                path.strip_prefix('/')
                    .expect("paths should start with a '/'")
                    .to_string(),
                RequestType::Query,
            ),
        };

        if name.contains('/') || name.is_empty() {
            // TODO: improve errors
            return Err(StatusCode::BAD_REQUEST);
        }

        let headers = req.headers();
        let in_fmt = match headers.get(CONTENT_TYPE) {
            Some(v) => match v.to_str() {
                Ok(s) => s.parse().map_err(|_| StatusCode::UNSUPPORTED_MEDIA_TYPE)?,
                Err(_) => return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE),
            },
            None => Format::default(),
        };

        let out_fmt = match headers.get(ACCEPT) {
            Some(v) => match v.to_str() {
                Ok(s) => s.parse().unwrap_or_default(),
                Err(_) => return Err(StatusCode::BAD_REQUEST),
            },
            None => Format::default(),
        };

        let body = match Bytes::from_request(req, state).await {
            Ok(b) => b,
            Err(e) => {
                error!(?e, "Error getting bytes");
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };
        let out = Request {
            name,
            req_type,
            body,
            in_fmt,
            out_fmt,
        };

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Request Construction Tests
    // ============================================================================

    #[test]
    fn test_request_construction() {
        let body = Bytes::from("test body");
        let request = Request {
            name: "test_query".to_string(),
            req_type: RequestType::Query,
            body: body.clone(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        assert_eq!(request.name, "test_query");
        assert!(matches!(request.req_type, RequestType::Query));
        assert_eq!(request.body, body);
    }

    #[test]
    fn test_request_clone() {
        let body = Bytes::from("clone test");
        let request = Request {
            name: "original".to_string(),
            req_type: RequestType::MCP,
            body: body.clone(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let cloned = request.clone();
        assert_eq!(cloned.name, request.name);
        assert_eq!(cloned.body, request.body);
    }

    #[test]
    fn test_request_debug() {
        let request = Request {
            name: "debug_test".to_string(),
            req_type: RequestType::Query,
            body: Bytes::from("test"),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let debug_str = format!("{:?}", request);
        assert!(debug_str.contains("debug_test"));
        assert!(debug_str.contains("Query"));
    }

    // ============================================================================
    // RequestType Tests
    // ============================================================================

    #[test]
    fn test_request_type_query() {
        let rt = RequestType::Query;
        assert!(matches!(rt, RequestType::Query));

        let debug_str = format!("{:?}", rt);
        assert!(debug_str.contains("Query"));
    }

    #[test]
    fn test_request_type_mcp() {
        let rt = RequestType::MCP;
        assert!(matches!(rt, RequestType::MCP));

        let debug_str = format!("{:?}", rt);
        assert!(debug_str.contains("MCP"));
    }

    #[test]
    fn test_request_type_copy() {
        let rt1 = RequestType::Query;
        let rt2 = rt1; // Copy trait

        // Both should be usable
        assert!(matches!(rt1, RequestType::Query));
        assert!(matches!(rt2, RequestType::Query));
    }

    #[test]
    fn test_request_type_clone() {
        let rt1 = RequestType::MCP;
        let rt2 = rt1.clone();

        assert!(matches!(rt1, RequestType::MCP));
        assert!(matches!(rt2, RequestType::MCP));
    }

    // ============================================================================
    // Request with Different Content
    // ============================================================================

    #[test]
    fn test_request_empty_body() {
        let request = Request {
            name: "empty_body".to_string(),
            req_type: RequestType::Query,
            body: Bytes::new(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        assert!(request.body.is_empty());
    }

    #[test]
    fn test_request_large_body() {
        let large_data = vec![0u8; 10_000];
        let body = Bytes::from(large_data.clone());

        let request = Request {
            name: "large_body".to_string(),
            req_type: RequestType::Query,
            body: body.clone(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        assert_eq!(request.body.len(), 10_000);
    }

    #[test]
    fn test_request_utf8_name() {
        let request = Request {
            name: "test_世界_query".to_string(),
            req_type: RequestType::Query,
            body: Bytes::from("test"),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        assert!(request.name.contains("世界"));
    }
}
