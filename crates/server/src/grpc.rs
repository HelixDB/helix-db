use std::net::SocketAddr;

use db::query_service::{QueryMode, QueryServiceError};
use helix_ast::query::{QueryRequest, QueryRequestType};
use tokio::sync::watch;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::state::ServerState;
use crate::MAX_QUERY_BODY_BYTES;

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
            return Err(Status::resource_exhausted(format!(
                "query body exceeds {MAX_QUERY_BODY_BYTES} bytes"
            )));
        }
        let query = sonic_rs::from_slice::<QueryRequest>(&request.body)
            .map_err(|error| Status::invalid_argument(format!("invalid query JSON: {error}")))?;
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
        return Err(Status::invalid_argument(
            "warm_only is only valid for read requests",
        ));
    }
    if await_durable && request_type != QueryRequestType::Write {
        return Err(Status::invalid_argument(
            "await_durable is only valid for write requests",
        ));
    }
    if require_writer && db_mode != db::HelixDbMode::Writer {
        return Err(Status::unavailable(
            "request requires a writer but this server is read-only",
        ));
    }
    Ok(())
}

pub(super) fn status_from_service_error(error: QueryServiceError) -> Status {
    if error.is_transaction_conflict() {
        return Status::aborted(error.to_string());
    }
    let message = error.to_string();
    match error {
        QueryServiceError::InvalidRequest(_) | QueryServiceError::Planner(_) => {
            Status::invalid_argument(message)
        }
        QueryServiceError::Db(error) if error.is_invalid_vector_input() => {
            Status::invalid_argument(message)
        }
        QueryServiceError::Db(db::error::HelixDbError::WriterModeRequired { .. }) => {
            Status::failed_precondition(message)
        }
        QueryServiceError::WriteReceiptConflict => Status::already_exists(message),
        QueryServiceError::WriteReceiptResponseTooLarge { .. } => {
            Status::resource_exhausted(message)
        }
        QueryServiceError::Db(_)
        | QueryServiceError::JsonSerialize(_)
        | QueryServiceError::Serialize(_)
        | QueryServiceError::Deserialize(_) => Status::internal(message),
    }
}
