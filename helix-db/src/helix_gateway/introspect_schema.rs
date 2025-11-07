use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;

use crate::helix_gateway::gateway::AppState;
use axum::response::IntoResponse;

pub async fn introspect_schema_handler(
    State(state): State<Arc<AppState>>,
) -> axum::response::Response {
    match state.schema_json.as_ref() {
        Some(data) => axum::response::Response::builder()
            .header("Content-Type", "application/json")
            .body(Body::from(data.clone().into_bytes()))
            .expect("should be able to make response from string"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "Could not find schema").into_response(),
    }
}

