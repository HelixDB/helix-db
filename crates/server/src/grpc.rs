use std::net::SocketAddr;

use db::query_service::{QueryMode, QueryServiceError};
use helix_ast::query::{QueryRequest, QueryRequestType};
use tokio::sync::watch;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::state::ServerState;

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
        .add_service(HelixDbServerServer::new(GrpcService { state }))
        .serve_with_shutdown(addr, async move {
            while shutdown.changed().await.is_ok() {
                if *shutdown.borrow() {
                    break;
                }
            }
        })
        .await
}

#[derive(Clone)]
struct GrpcService {
    state: ServerState,
}

#[tonic::async_trait]
impl HelixDbServer for GrpcService {
    async fn execute_query(
        &self,
        request: Request<QueryJsonRequest>,
    ) -> Result<Response<QueryJsonResponse>, Status> {
        let request = request.into_inner();
        let query = sonic_rs::from_slice::<QueryRequest>(&request.body)
            .map_err(|error| Status::invalid_argument(format!("invalid query JSON: {error}")))?;
        validate_options_for_request_type(
            request.warm_only,
            request.await_durable,
            query.request_type,
        )?;
        let response = self
            .state
            .query_service()
            .execute_query_with_mode(query, query_mode(request.warm_only))
            .await
            .map_err(status_from_service_error)?;
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
            ready: true,
            mode: self.state.db_mode().as_str().to_string(),
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
    await_durable: bool,
    request_type: QueryRequestType,
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
    Ok(())
}

fn status_from_service_error(error: QueryServiceError) -> Status {
    if error.is_transaction_conflict() {
        return Status::aborted(error.to_string());
    }
    match error {
        QueryServiceError::InvalidRequest(_) | QueryServiceError::Planner(_) => {
            Status::invalid_argument(error.to_string())
        }
        QueryServiceError::Db(_)
        | QueryServiceError::JsonSerialize(_)
        | QueryServiceError::Serialize(_) => Status::internal(error.to_string()),
    }
}
