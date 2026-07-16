//! Axum HTTP transport for query execution and server health endpoints.

use std::net::SocketAddr;

use axum::body::{to_bytes, Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use db::query_service::{QueryMode, QueryResponse, QueryServiceError};
use helix_ast::query::{QueryRequest, QueryRequestType};
use serde_json::Value as JsonValue;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::state::ServerState;

const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Serve the HTTP API.
pub async fn serve(
    addr: SocketAddr,
    state: ServerState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "HTTP server listening");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            while shutdown.changed().await.is_ok() {
                if *shutdown.borrow() {
                    break;
                }
            }
        })
        .await
}

fn router(state: ServerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/query", post(execute_query))
        .with_state(state)
}

async fn healthz(State(state): State<ServerState>) -> Response {
    health_response(state)
}

async fn readyz(State(state): State<ServerState>) -> Response {
    health_response(state)
}

fn health_response(state: ServerState) -> Response {
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "ready": true,
            "mode": state.db_mode().as_str(),
        }),
    )
}

async fn execute_query(
    State(state): State<ServerState>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let options = match RequestOptions::from_headers(&headers) {
        Ok(options) => options,
        Err(error) => return error.into_response(),
    };
    let bytes = match read_body(body).await {
        Ok(bytes) => bytes,
        Err(response) => return *response,
    };
    let request = match sonic_rs::from_slice::<QueryRequest>(&bytes) {
        Ok(request) => request,
        Err(error) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("invalid query JSON: {error}"),
            );
        }
    };
    if let Err(error) = options.validate_for_request_type(request.request_type) {
        return error.into_response();
    }

    match state
        .query_service()
        .execute_query_with_mode(request, options.query_mode())
        .await
    {
        Ok(response) => query_response(response),
        Err(error) => service_error_response(error),
    }
}

async fn read_body(body: Body) -> Result<Bytes, Box<Response>> {
    to_bytes(body, MAX_BODY_BYTES).await.map_err(|error| {
        Box::new(error_response(
            StatusCode::BAD_REQUEST,
            format!("failed to read request body: {error}"),
        ))
    })
}

fn query_response(response: QueryResponse) -> Response {
    match response.to_json_bytes() {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response(),
        Err(error) => service_error_response(error),
    }
}

fn service_error_response(error: QueryServiceError) -> Response {
    let status = if error.is_transaction_conflict() {
        StatusCode::CONFLICT
    } else {
        match &error {
            QueryServiceError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            QueryServiceError::Planner(_) => StatusCode::BAD_REQUEST,
            QueryServiceError::Db(_) => StatusCode::INTERNAL_SERVER_ERROR,
            QueryServiceError::JsonSerialize(_) | QueryServiceError::Serialize(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    };
    let message = error.to_string();
    match error.index_error_code() {
        Some(code) => json_response(
            status,
            &serde_json::json!({ "error": message, "code": code }),
        ),
        None => error_response(status, message),
    }
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    json_response(status, &serde_json::json!({ "error": message.into() }))
}

fn json_response(status: StatusCode, body: &JsonValue) -> Response {
    match serde_json::to_vec(body) {
        Ok(body) => (status, [(header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "application/json")],
            format!(r#"{{"error":"failed to serialize response: {error}"}}"#),
        )
            .into_response(),
    }
}

struct RequestOptions {
    warm_only: bool,
    require_writer: bool,
    await_durable: Option<bool>,
}

impl RequestOptions {
    fn from_headers(headers: &HeaderMap) -> Result<Self, HttpError> {
        Ok(Self {
            warm_only: parse_bool_header(headers, "x-helix-warm")?,
            require_writer: parse_bool_header(headers, "x-helix-require-writer")?,
            await_durable: parse_optional_bool_header(headers, "x-helix-await-durable")?,
        })
    }

    fn validate_for_request_type(&self, request_type: QueryRequestType) -> Result<(), HttpError> {
        if self.warm_only && request_type != QueryRequestType::Read {
            return Err(HttpError::new(
                StatusCode::BAD_REQUEST,
                "x-helix-warm is only valid for read requests",
            ));
        }
        if self.await_durable.is_some() && request_type != QueryRequestType::Write {
            return Err(HttpError::new(
                StatusCode::BAD_REQUEST,
                "x-helix-await-durable is only valid for write requests",
            ));
        }
        let _ = self.require_writer;
        Ok(())
    }

    fn query_mode(&self) -> QueryMode {
        if self.warm_only {
            QueryMode::Warm
        } else {
            QueryMode::Execute
        }
    }
}

fn parse_bool_header(headers: &HeaderMap, name: &'static str) -> Result<bool, HttpError> {
    parse_optional_bool_header(headers, name).map(|value| value.unwrap_or(false))
}

fn parse_optional_bool_header(
    headers: &HeaderMap,
    name: &'static str,
) -> Result<Option<bool>, HttpError> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        HttpError::new(
            StatusCode::BAD_REQUEST,
            format!("{name} must be a UTF-8 boolean header"),
        )
    })?;
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(Some(true)),
        "false" | "0" => Ok(Some(false)),
        _ => Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            format!("{name} must be true or false"),
        )),
    }
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn into_response(self) -> Response {
        error_response(self.status, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warm_header_selects_warm_query_mode() {
        let mut headers = HeaderMap::new();
        headers.insert("x-helix-warm", "true".parse().expect("valid header"));

        let options = RequestOptions::from_headers(&headers).expect("valid request options");

        assert_eq!(options.query_mode(), QueryMode::Warm);
    }

    #[tokio::test]
    async fn lifecycle_errors_include_the_stable_machine_code() {
        let response = service_error_response(QueryServiceError::Db(
            db::error::HelixDbError::IndexOperationNotFound {
                operation_id: "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001".to_string(),
            },
        ));
        let body = to_bytes(response.into_body(), 4_096)
            .await
            .expect("lifecycle error body is bounded");
        let json: JsonValue = serde_json::from_slice(&body).expect("error body is JSON");

        assert_eq!(json["code"], "index_operation_not_found");
        assert!(json["error"]
            .as_str()
            .expect("error message is a string")
            .contains("Index operation not found"));
    }
}
