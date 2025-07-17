use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use sonic_rs::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::{net::SocketAddr, process::Command};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;
use tracing_subscriber;

// Constants for timeouts
//const SOCKET_TIMEOUT: Duration = Duration::from_secs(30);

// make sure build is run in sudo mode

#[derive(Debug, Deserialize, Serialize)]
pub struct HBuildDeployRequest {
    user_id: String,
    instance_id: String,
    version: String,
}

#[derive(Debug, Serialize)]
pub struct DeployResponse {
    success: bool,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl DeployResponse {
    fn success(message: String) -> Self {
        Self {
            success: true,
            message,
            error: None,
        }
    }

    fn error(message: String, error: String) -> Self {
        Self {
            success: false,
            message,
            error: Some(error),
        }
    }
}

// Shared application state
#[derive(Clone)]
pub struct AppState {
    s3_client: Client,
    user_id: String,
    cluster_id: String,
}

// Handler for the health check endpoint
async fn health_handler() -> Json<DeployResponse> {
    Json(DeployResponse::success("Service is healthy".to_string()))
}

// Handler for the /redeploy endpoint
async fn redeploy_handler(State(state): State<AppState>) -> Result<Json<DeployResponse>, StatusCode> {
    tracing::info!("Received redeploy request");

    // Move the deployment logic here
    tokio::spawn(async move {
        if let Err(e) = perform_deployment(&state).await {
            tracing::error!("Deployment failed: {:?}", e);
        }
    });

    Ok(Json(DeployResponse::success(
        "Deployment initiated successfully".to_string(),
    )))
}

async fn perform_deployment(state: &AppState) -> Result<(), AdminError> {
    // rename old binary
    let mv_result = Command::new("mv")
        .arg("helix")
        .arg("helix_old")
        .output()
        .map_err(|e| AdminError::CommandError("Failed to backup old binary".to_string(), e))?;

    if !mv_result.status.success() {
        return Err(AdminError::CommandError(
            "Failed to backup old binary".to_string(),
            std::io::Error::new(std::io::ErrorKind::Other, "mv command failed"),
        ));
    }

    // pull binary from s3
    let response = state
        .s3_client
        .get_object()
        .bucket("helix-build")
        .key(format!(
            "{}/{}/helix/latest",
            state.user_id, state.cluster_id
        ))
        .send()
        .await
        .map_err(|e| AdminError::S3DownloadError("Failed to download binary from S3".to_string(), e))?;

    // create binary file or overwrite if it exists
    let mut file = File::create("helix")
        .map_err(|e| AdminError::FileError("Failed to create new binary file".to_string(), e))?;
    
    let body = response
        .body
        .collect()
        .await
        .map_err(|e| AdminError::FileError("Failed to collect S3 response body".to_string(), 
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?
        .to_vec();
    
    file.write_all(&body)
        .map_err(|e| AdminError::FileError("Failed to write binary file".to_string(), e))?;

    // set permissions
    let chmod_result = Command::new("sudo")
        .arg("chmod")
        .arg("+x")
        .arg("helix")
        .output()
        .map_err(|e| AdminError::CommandError("Failed to set binary permissions".to_string(), e))?;

    if !chmod_result.status.success() {
        return Err(AdminError::CommandError(
            "Failed to set binary permissions".to_string(),
            std::io::Error::new(std::io::ErrorKind::Other, "chmod command failed"),
        ));
    }

    // restart systemd service
    let restart_result = Command::new("sudo")
        .arg("systemctl")
        .arg("restart")
        .arg("helix")
        .output()
        .map_err(|e| AdminError::CommandError("Failed to restart service".to_string(), e))?;

    if !restart_result.status.success() {
        return Err(AdminError::CommandError(
            "Failed to restart service".to_string(),
            std::io::Error::new(std::io::ErrorKind::Other, "systemctl restart failed"),
        ));
    }

    // check if service is running
    let status_result = Command::new("sudo")
        .arg("systemctl")
        .arg("status")
        .arg("helix")
        .output()
        .map_err(|e| AdminError::CommandError("Failed to check service status".to_string(), e))?;

    // if not revert
    if !status_result.status.success() {
        tracing::warn!("Service failed to start, reverting to old binary");
        
        let revert_result = Command::new("mv")
            .arg("helix_old")
            .arg("helix")
            .output();

        if let Err(e) = revert_result {
            tracing::error!("Failed to revert binary: {:?}", e);
        }

        let restart_old_result = Command::new("sudo")
            .arg("systemctl")
            .arg("restart")
            .arg("helix")
            .output();

        if let Err(e) = restart_old_result {
            tracing::error!("Failed to restart with old binary: {:?}", e);
        }

        return Err(AdminError::CommandError(
            "Service failed to start with new binary, reverted".to_string(),
            std::io::Error::new(std::io::ErrorKind::Other, "Service startup failed"),
        ));
    } else {
        // delete old binary
        let rm_result = Command::new("rm")
            .arg("helix_old")
            .output();

        if let Err(e) = rm_result {
            tracing::warn!("Failed to delete old binary: {:?}", e);
        }
        
        tracing::info!("Deployment completed successfully");
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), AdminError> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    tracing::info!("Starting helix build service");
    
    // Initialize AWS SDK with explicit region configuration
    let bucket_region = std::env::var("S3_BUCKET_REGION").unwrap_or("us-west-1".to_string());
    tracing::info!("Using S3 bucket region: {}", bucket_region);

    let config = aws_config::load_defaults(BehaviorVersion::latest())
        .await
        .to_builder()
        .region(aws_config::Region::new(bucket_region.clone()))
        .build();
    let s3_client = Client::new(&config);

    tracing::info!("AWS region configured: {:?}", config.region());

    let user_id = std::env::var("USER_ID").expect("USER_ID is not set");
    let cluster_id = std::env::var("CLUSTER_ID").expect("CLUSTER_ID is not set");
    
    // Create shared application state
    let app_state = AppState {
        s3_client,
        user_id,
        cluster_id,
    };

    // Build the Axum app
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/redeploy", post(redeploy_handler))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
        )
        .with_state(app_state);

    // run server on specified port
    let port = std::env::var("PORT").unwrap_or("6900".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    
    tracing::info!("Server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.map_err(|e| {
        tracing::error!("Failed to bind to address {}: {}", addr, e);
        AdminError::AdminConnectionError("Failed to bind to address".to_string(), e)
    })?;

    axum::serve(listener, app).await.map_err(|e| {
        tracing::error!("Server error: {}", e);
        AdminError::AdminConnectionError("Server error".to_string(), 
            std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    })?;

    Ok(())
}

#[derive(Debug)]
pub enum AdminError {
    AdminConnectionError(String, std::io::Error),
    S3DownloadError(
        String,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    ),
    CommandError(String, std::io::Error),
    FileError(String, std::io::Error),
    InvalidParameter(String),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdminError::AdminConnectionError(msg, err) => {
                write!(f, "Connection error: {}: {}", msg, err)
            }
            AdminError::S3DownloadError(msg, err) => write!(f, "S3 error: {}: {}", msg, err),
            AdminError::CommandError(msg, err) => write!(f, "Command error: {}: {}", msg, err),
            AdminError::FileError(msg, err) => write!(f, "File error: {}: {}", msg, err),
            AdminError::InvalidParameter(msg) => write!(f, "Invalid parameter: {}", msg),
        }
    }
}

impl std::error::Error for AdminError {}
