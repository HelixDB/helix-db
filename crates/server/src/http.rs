//! Axum HTTP transport for query execution and server health endpoints.

use std::net::SocketAddr;

use axum::body::{to_bytes, Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use db::query_service::{QueryMode, QueryResponse, QueryServiceError};
use helix_ast::error_code;
use helix_ast::query::{QueryRequest, QueryRequestType};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::state::ServerState;
use crate::MAX_QUERY_BODY_BYTES;

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
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
}

pub(crate) fn router(state: ServerState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v2/query", post(execute_query))
        .with_state(state)
}

async fn healthz(State(state): State<ServerState>) -> Response {
    health_response(StatusCode::OK, state)
}

async fn readyz(State(state): State<ServerState>) -> Response {
    let status = if state.index_readiness().is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    health_response(status, state)
}

fn health_response(status: StatusCode, state: ServerState) -> Response {
    json_response(
        status,
        &serde_json::json!({
            "ready": state.index_readiness().is_ready(),
            "mode": state.db_mode().as_str(),
            "index_runtime": state.index_readiness().code(),
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
                error_code::QueryErrorCode::InvalidQueryJson,
                format!("invalid query JSON: {error}"),
            );
        }
    };
    if let Err(error) = options.validate_for_request_type(request.request_type(), state.db_mode()) {
        return error.into_response();
    }

    match state
        .query_service()
        .execute_query_with_mode_and_metrics_tenant(
            request,
            options.query_mode(),
            options.metrics_tenant_id,
        )
        .await
    {
        Ok(response) => {
            if options.await_durable == Some(true)
                && let Err(error) = state.flush_writer().await
            {
                return service_error_response(error.into());
            }
            query_response(response)
        }
        Err(error) => service_error_response(error),
    }
}

async fn read_body(body: Body) -> Result<Bytes, Box<Response>> {
    to_bytes(body, MAX_QUERY_BODY_BYTES).await.map_err(|error| {
        Box::new(error_response(
            StatusCode::BAD_REQUEST,
            error_code::QueryErrorCode::InvalidRequestBody,
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

pub(super) fn service_error_response(error: QueryServiceError) -> Response {
    let terminal_write_outcome = error.is_commit_outcome_unknown();
    let status = if terminal_write_outcome {
        StatusCode::SERVICE_UNAVAILABLE
    } else if error.is_transaction_conflict() {
        StatusCode::CONFLICT
    } else {
        match &error {
            QueryServiceError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            QueryServiceError::Planner(_) => StatusCode::BAD_REQUEST,
            QueryServiceError::Db(error) if error.is_invalid_vector_input() => {
                StatusCode::BAD_REQUEST
            }
            QueryServiceError::Db(db::error::HelixDbError::WriterModeRequired { .. }) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            QueryServiceError::Db(_) => StatusCode::INTERNAL_SERVER_ERROR,
            QueryServiceError::JsonSerialize(_) | QueryServiceError::Serialize(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    };
    let code = error.error_code();
    let message = error.to_string();
    error_response_with_retryable(
        status,
        code,
        message,
        terminal_write_outcome.then_some(false),
    )
}

#[derive(Serialize)]
struct QueryErrorEnvelope<'a> {
    error: error_code::QueryErrorCode,
    msg: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    retryable: Option<bool>,
}

fn error_response(
    status: StatusCode,
    code: error_code::QueryErrorCode,
    message: impl Into<String>,
) -> Response {
    error_response_with_retryable(status, code, message, None)
}

fn error_response_with_retryable(
    status: StatusCode,
    code: error_code::QueryErrorCode,
    message: impl Into<String>,
    retryable: Option<bool>,
) -> Response {
    let message = message.into();
    json_response(
        status,
        &QueryErrorEnvelope {
            error: code,
            msg: &message,
            retryable,
        },
    )
}

fn json_response(status: StatusCode, body: &(impl Serialize + ?Sized)) -> Response {
    match serde_json::to_vec(body) {
        Ok(body) => (status, [(header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(error) => {
            tracing::error!(%error, "failed to serialize HTTP JSON response");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "application/json")],
                r#"{"error":"response_serialization_error","msg":"failed to serialize response"}"#,
            )
                .into_response()
        }
    }
}

struct RequestOptions {
    warm_only: bool,
    require_writer: bool,
    await_durable: Option<bool>,
    metrics_tenant_id: Option<helix_metrics::query::TenantId>,
}

impl RequestOptions {
    fn from_headers(headers: &HeaderMap) -> Result<Self, HttpError> {
        Ok(Self {
            warm_only: parse_bool_header(headers, "x-helix-warm")?,
            require_writer: parse_bool_header(headers, "x-helix-require-writer")?,
            await_durable: parse_optional_bool_header(headers, "x-helix-await-durable")?,
            metrics_tenant_id: crate::query_metrics_tenant_id(
                headers
                    .get(crate::TENANT_ID_HEADER_NAME)
                    .and_then(|value| value.to_str().ok()),
            ),
        })
    }

    fn validate_for_request_type(
        &self,
        request_type: QueryRequestType,
        db_mode: db::HelixDbMode,
    ) -> Result<(), HttpError> {
        if self.warm_only && request_type != QueryRequestType::Read {
            return Err(HttpError::new(
                StatusCode::BAD_REQUEST,
                error_code::QueryErrorCode::InvalidRequestOption,
                "x-helix-warm is only valid for read requests",
            ));
        }
        if self.await_durable == Some(true) && request_type != QueryRequestType::Write {
            return Err(HttpError::new(
                StatusCode::BAD_REQUEST,
                error_code::QueryErrorCode::InvalidRequestOption,
                "x-helix-await-durable is only valid for write requests",
            ));
        }
        if self.require_writer && db_mode != db::HelixDbMode::Writer {
            return Err(HttpError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                error_code::QueryErrorCode::InvalidRequestOption,
                "request requires a writer but this server is read-only",
            ));
        }
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
            error_code::QueryErrorCode::InvalidRequestOption,
            format!("{name} must be a UTF-8 boolean header"),
        )
    })?;
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(Some(true)),
        "false" | "0" => Ok(Some(false)),
        _ => Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            error_code::QueryErrorCode::InvalidRequestOption,
            format!("{name} must be true or false"),
        )),
    }
}

#[derive(Debug)]
struct HttpError {
    status: StatusCode,
    code: error_code::QueryErrorCode,
    message: String,
}

impl HttpError {
    fn new(
        status: StatusCode,
        code: error_code::QueryErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn into_response(self) -> Response {
        error_response(self.status, self.code, self.message)
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

    #[test]
    fn tenant_metrics_identity_comes_from_the_query_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            crate::TENANT_ID_HEADER_NAME,
            "tenant-1".parse().expect("valid header"),
        );

        let options = RequestOptions::from_headers(&headers).expect("valid request options");

        assert_eq!(
            options
                .metrics_tenant_id
                .as_ref()
                .map(helix_metrics::query::TenantId::as_str),
            Some("tenant-1")
        );

        headers.insert(
            crate::TENANT_ID_HEADER_NAME,
            " tenant-1".parse().expect("valid header bytes"),
        );
        let options =
            RequestOptions::from_headers(&headers).expect("telemetry cannot reject a query");
        assert!(options.metrics_tenant_id.is_none());
    }

    #[tokio::test]
    async fn query_errors_use_the_static_code_and_human_message_envelope() {
        let response = service_error_response(QueryServiceError::Db(
            db::error::HelixDbError::IndexOperationNotFound {
                operation_id: "018f0c58-6bc7-7c56-8d3d-9c5f18a0f001".to_string(),
            },
        ));
        let body = to_bytes(response.into_body(), 4_096)
            .await
            .expect("lifecycle error body is bounded");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("error body is JSON");

        assert_eq!(json["error"], "index_operation_not_found");
        assert!(json["msg"]
            .as_str()
            .expect("error message is a string")
            .contains("Index operation not found"));
        assert_eq!(json.get("code"), None);
        assert_eq!(json.get("retryable"), None);
    }

    #[tokio::test]
    async fn terminal_write_outcomes_are_503_and_explicitly_not_retryable() {
        let response = service_error_response(QueryServiceError::Db(
            db::error::HelixDbError::WriterFencedCommitOutcomeUnknown,
        ));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 4_096)
            .await
            .expect("terminal write error body is bounded");
        let json: serde_json::Value =
            serde_json::from_slice(&body).expect("terminal write error body is JSON");
        assert_eq!(json["error"], "writer_fenced_commit_outcome_unknown");
        assert_eq!(json["retryable"], false);
        assert!(json["msg"].is_string());
    }
}
