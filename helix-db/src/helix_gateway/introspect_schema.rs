use std::sync::Arc;

use axum::body::Body;
use axum::extract::State;
#[cfg(feature = "api-key")]
use axum::http::HeaderMap;
use axum::http::StatusCode;

use crate::helix_gateway::gateway::AppState;
use axum::response::IntoResponse;

pub async fn introspect_schema_handler(
    State(state): State<Arc<AppState>>,
    #[cfg(feature = "api-key")] headers: HeaderMap,
) -> axum::response::Response {
    // API key verification when feature is enabled
    #[cfg(feature = "api-key")]
    {
        use crate::helix_gateway::key_verification::verify_key;

        let api_key = match headers.get("x-api-key") {
            Some(v) => match v.to_str() {
                Ok(s) => s,
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "Invalid x-api-key header").into_response();
                }
            },
            None => {
                return (StatusCode::BAD_REQUEST, "Missing x-api-key header").into_response();
            }
        };

        if let Err(e) = verify_key(api_key) {
            return e.into_response(); // Returns 403 Forbidden
        }
    }

    match state.schema_json.as_ref() {
        Some(data) => axum::response::Response::builder()
            .header("Content-Type", "application/json")
            .body(Body::from(data.clone().into_bytes()))
            .expect("should be able to make response from string"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "Could not find schema").into_response(),
    }
}
