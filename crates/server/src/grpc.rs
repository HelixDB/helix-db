use std::net::SocketAddr;

use db::query_service::{QueryMode, QueryServiceError};
use helix_ast::error_code;
use helix_ast::query::{QueryRequest, QueryRequestType};
use tokio::sync::watch;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::state::ServerState;
use crate::MAX_QUERY_BODY_BYTES;

pub(crate) const HELIX_ERROR_CODE_METADATA: &str = "helix-error-code";

pub mod pb {
    tonic::include_proto!("helixdb.server.v1");
}

use pb::helix_db_server_server::{HelixDbServer, HelixDbServerServer};
use pb::{HealthRequest, HealthResponse, QueryJsonRequest, QueryJsonResponse};

/// Serve the gRPC API.
pub async fn serve(
    addr: SocketAddr,
    state: ServerState,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), tonic::transport::Error> {
    tracing::info!(%addr, "gRPC server listening");
    Server::builder()
        .add_service(server_service(state))
        .serve_with_shutdown(addr, async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
}

#[derive(Clone)]
pub(crate) struct GrpcService {
    state: ServerState,
}

impl GrpcService {
    pub(crate) fn new(state: ServerState) -> Self {
        Self { state }
    }
}

pub(crate) fn server_service(state: ServerState) -> HelixDbServerServer<GrpcService> {
    const PROTOBUF_ENVELOPE_ALLOWANCE: usize = 1_024;

    HelixDbServerServer::new(GrpcService::new(state))
        .max_decoding_message_size(MAX_QUERY_BODY_BYTES + PROTOBUF_ENVELOPE_ALLOWANCE)
}

#[tonic::async_trait]
impl HelixDbServer for GrpcService {
    async fn execute_query(
        &self,
        request: Request<QueryJsonRequest>,
    ) -> Result<Response<QueryJsonResponse>, Status> {
        let metrics_tenant_id = crate::query_metrics_tenant_id(
            request
                .metadata()
                .get(crate::TENANT_ID_HEADER_NAME)
                .and_then(|value| value.to_str().ok()),
        );
        let request = request.into_inner();
        if request.body.len() > MAX_QUERY_BODY_BYTES {
            return Err(status_with_error_code(
                tonic::Code::ResourceExhausted,
                error_code::QueryErrorCode::InvalidRequestBody,
                format!("query body exceeds {MAX_QUERY_BODY_BYTES} bytes"),
            ));
        }
        let query = sonic_rs::from_slice::<QueryRequest>(&request.body).map_err(|error| {
            status_with_error_code(
                tonic::Code::InvalidArgument,
                error_code::QueryErrorCode::InvalidQueryJson,
                format!("invalid query JSON: {error}"),
            )
        })?;
        validate_options_for_request_type(
            request.warm_only,
            request.require_writer,
            request.await_durable,
            query.request_type(),
            self.state.db_mode(),
        )?;
        let response = self
            .state
            .query_service()
            .execute_query_with_mode_and_metrics_tenant(
                query,
                query_mode(request.warm_only),
                metrics_tenant_id,
            )
            .await
            .map_err(status_from_service_error)?;
        if request.await_durable {
            self.state
                .flush_writer()
                .await
                .map_err(QueryServiceError::from)
                .map_err(status_from_service_error)?;
        }
        let body = response
            .to_json_bytes()
            .map_err(status_from_service_error)?
            .into();
        Ok(Response::new(QueryJsonResponse { body }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ready: self.state.index_readiness().is_ready(),
            mode: self.state.db_mode().as_str().to_string(),
            index_runtime: self.state.index_readiness().code().to_string(),
        }))
    }
}

fn query_mode(warm_only: bool) -> QueryMode {
    if warm_only {
        QueryMode::Warm
    } else {
        QueryMode::Execute
    }
}

fn validate_options_for_request_type(
    warm_only: bool,
    require_writer: bool,
    await_durable: bool,
    request_type: QueryRequestType,
    db_mode: db::HelixDbMode,
) -> Result<(), Status> {
    if warm_only && request_type != QueryRequestType::Read {
        return Err(status_with_error_code(
            tonic::Code::InvalidArgument,
            error_code::QueryErrorCode::InvalidRequestOption,
            "warm_only is only valid for read requests",
        ));
    }
    if await_durable && request_type != QueryRequestType::Write {
        return Err(status_with_error_code(
            tonic::Code::InvalidArgument,
            error_code::QueryErrorCode::InvalidRequestOption,
            "await_durable is only valid for write requests",
        ));
    }
    if require_writer && db_mode != db::HelixDbMode::Writer {
        return Err(status_with_error_code(
            tonic::Code::Unavailable,
            error_code::QueryErrorCode::InvalidRequestOption,
            "request requires a writer but this server is read-only",
        ));
    }
    Ok(())
}

pub(super) fn status_from_service_error(error: QueryServiceError) -> Status {
    let error_code = error.error_code();
    let message = error.to_string();
    let code = if error.is_transaction_conflict() {
        tonic::Code::Aborted
    } else {
        match error {
            QueryServiceError::InvalidRequest(_) | QueryServiceError::Planner(_) => {
                tonic::Code::InvalidArgument
            }
            QueryServiceError::Db(error) if error.is_invalid_vector_input() => {
                tonic::Code::InvalidArgument
            }
            QueryServiceError::Db(db::error::HelixDbError::WriterModeRequired { .. }) => {
                tonic::Code::FailedPrecondition
            }
            QueryServiceError::Db(_)
            | QueryServiceError::JsonSerialize(_)
            | QueryServiceError::Serialize(_) => tonic::Code::Internal,
        }
    };
    status_with_error_code(code, error_code, message)
}

fn status_with_error_code(
    status_code: tonic::Code,
    error_code: error_code::QueryErrorCode,
    message: impl Into<String>,
) -> Status {
    let mut status = Status::new(status_code, message.into());
    status.metadata_mut().insert(
        HELIX_ERROR_CODE_METADATA,
        tonic::metadata::MetadataValue::from_static(error_code.as_str()),
    );
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_statuses_keep_messages_and_attach_static_codes() {
        let cases = [
            (
                QueryServiceError::InvalidRequest("bad request".to_string()),
                tonic::Code::InvalidArgument,
                "invalid_request",
            ),
            (
                QueryServiceError::Db(db::error::HelixDbError::TransactionConflict(
                    "retry".to_string(),
                )),
                tonic::Code::Aborted,
                "transaction_conflict",
            ),
            (
                QueryServiceError::Db(db::error::HelixDbError::WriterModeRequired {
                    actual: "reader",
                }),
                tonic::Code::FailedPrecondition,
                "writer_mode_required",
            ),
            (
                QueryServiceError::Db(db::error::HelixDbError::IndexNotFound(
                    "documents".to_string(),
                )),
                tonic::Code::Internal,
                "index_not_found",
            ),
        ];

        for (error, expected_status_code, expected_error_code) in cases {
            let expected_message = error.to_string();
            let status = status_from_service_error(error);
            assert_eq!(status.code(), expected_status_code);
            assert_eq!(status.message(), expected_message);
            assert_eq!(
                status
                    .metadata()
                    .get(HELIX_ERROR_CODE_METADATA)
                    .expect("query statuses include an error code")
                    .to_str()
                    .expect("error codes are ASCII"),
                expected_error_code
            );
        }
    }

    #[test]
    fn request_option_statuses_attach_the_request_option_code() {
        let status = validate_options_for_request_type(
            true,
            false,
            false,
            QueryRequestType::Write,
            db::HelixDbMode::Writer,
        )
        .expect_err("warm-only writes are invalid");

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "warm_only is only valid for read requests"
        );
        assert_eq!(
            status
                .metadata()
                .get(HELIX_ERROR_CODE_METADATA)
                .expect("query statuses include an error code")
                .to_str()
                .expect("error codes are ASCII"),
            "invalid_request_option"
        );
    }
}
