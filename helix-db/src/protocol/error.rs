use axum::{body::Body, response::IntoResponse};
use thiserror::Error;

use crate::{
    helix_engine::types::{GraphError, VectorError},
    protocol::request::RequestType,
};

#[derive(Debug, Error)]
pub enum HelixError {
    #[error("{0}")]
    Graph(#[from] GraphError),
    #[error("{0}")]
    Vector(#[from] VectorError),
    #[error("Couldn't find `{name}` of type {ty:?}")]
    NotFound { ty: RequestType, name: String },
    #[error("Invalid API key")]
    InvalidApiKey,
}

impl IntoResponse for HelixError {
    fn into_response(self) -> axum::response::Response {
        let body = self.to_string();
        let code = match &self {
            Self::Graph(_) | Self::Vector(_) => 500,
            Self::NotFound { .. } => 404,
            Self::InvalidApiKey => 403,
        };

        axum::response::Response::builder()
            .status(code)
            .body(Body::from(body))
            .unwrap_or_else(|_| panic!("Should be able to turn HelixError into Response: {self}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // HelixError Variant Tests
    // ============================================================================

    #[test]
    fn test_helix_error_not_found() {
        let error = HelixError::NotFound {
            ty: RequestType::Query,
            name: "test_query".to_string(),
        };

        let error_string = error.to_string();
        assert!(error_string.contains("test_query"));
        assert!(error_string.contains("Couldn't find"));
    }

    #[test]
    fn test_helix_error_not_found_mcp() {
        let error = HelixError::NotFound {
            ty: RequestType::MCP,
            name: "test_mcp".to_string(),
        };

        let error_string = error.to_string();
        assert!(error_string.contains("test_mcp"));
        assert!(error_string.contains("MCP"));
    }

    #[test]
    fn test_helix_error_graph() {
        let graph_err = GraphError::DecodeError("test decode error".to_string());
        let helix_err = HelixError::from(graph_err);

        assert!(matches!(helix_err, HelixError::Graph(_)));
        let error_string = helix_err.to_string();
        assert!(error_string.contains("test decode error"));
    }

    #[test]
    fn test_helix_error_vector() {
        let vector_err = VectorError::InvalidVectorLength;
        let helix_err = HelixError::from(vector_err);

        assert!(matches!(helix_err, HelixError::Vector(_)));
    }

    // ============================================================================
    // IntoResponse Tests (HTTP Status Codes)
    // ============================================================================

    #[test]
    fn test_helix_error_into_response_not_found() {
        let error = HelixError::NotFound {
            ty: RequestType::Query,
            name: "missing".to_string(),
        };

        let response = error.into_response();
        assert_eq!(response.status(), 404);
    }

    #[test]
    fn test_helix_error_into_response_graph_error() {
        let graph_err = GraphError::DecodeError("decode failed".to_string());
        let helix_err = HelixError::from(graph_err);

        let response = helix_err.into_response();
        assert_eq!(response.status(), 500);
    }

    #[test]
    fn test_helix_error_into_response_vector_error() {
        let vector_err = VectorError::InvalidVectorData;
        let helix_err = HelixError::from(vector_err);

        let response = helix_err.into_response();
        assert_eq!(response.status(), 500);
    }

    // ============================================================================
    // Error Trait Tests
    // ============================================================================

    #[test]
    fn test_helix_error_is_error_trait() {
        let error = HelixError::NotFound {
            ty: RequestType::Query,
            name: "test".to_string(),
        };

        // Test that it implements std::error::Error
        fn assert_error<T: std::error::Error>(_: T) {}
        assert_error(error);
    }

    #[test]
    fn test_helix_error_debug() {
        let error = HelixError::NotFound {
            ty: RequestType::Query,
            name: "debug_test".to_string(),
        };

        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("NotFound"));
        assert!(debug_str.contains("debug_test"));
    }

    // ============================================================================
    // InvalidApiKey Tests
    // ============================================================================

    #[test]
    fn test_helix_error_invalid_api_key() {
        let error = HelixError::InvalidApiKey;
        let error_string = error.to_string();
        assert_eq!(error_string, "Invalid API key");
    }

    #[test]
    fn test_helix_error_invalid_api_key_into_response() {
        let error = HelixError::InvalidApiKey;
        let response = error.into_response();
        assert_eq!(response.status(), 403);
    }

    #[test]
    fn test_helix_error_invalid_api_key_debug() {
        let error = HelixError::InvalidApiKey;
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("InvalidApiKey"));
    }
}
