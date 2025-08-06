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

#[derive(Debug)]
pub struct Request {
    pub name: String,
    pub req_type: RequestType,
    /// This contains the input parameters serialized with in_fmt
    pub body: Bytes,
    pub in_fmt: Format,
    pub out_fmt: Format,
}

#[derive(Debug)]
pub enum RequestType {
    Query,
    MCP,
    GraphVis,
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
