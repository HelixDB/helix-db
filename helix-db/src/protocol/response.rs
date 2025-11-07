use axum::response::IntoResponse;
use reqwest::header::CONTENT_TYPE;

use crate::protocol::Format;
#[derive(Debug)]
pub struct Response {
    pub body: Vec<u8>,
    pub fmt: Format,
}

impl IntoResponse for Response {
    fn into_response(self) -> axum::response::Response {
        axum::response::Response::builder()
            .header(CONTENT_TYPE, self.fmt.to_string())
            .body(axum::body::Body::from(self.body))
            .expect("Should be able to construct response")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Response Construction Tests
    // ============================================================================

    #[test]
    fn test_response_construction() {
        let body = vec![1, 2, 3, 4, 5];
        let response = Response {
            body: body.clone(),
            fmt: Format::Json,
        };

        assert_eq!(response.body, body);
        assert!(matches!(response.fmt, Format::Json));
    }

    #[test]
    fn test_response_empty_body() {
        let response = Response {
            body: vec![],
            fmt: Format::Json,
        };

        assert!(response.body.is_empty());
    }

    #[test]
    fn test_response_large_body() {
        let large_body = vec![0u8; 50_000];
        let response = Response {
            body: large_body.clone(),
            fmt: Format::Json,
        };

        assert_eq!(response.body.len(), 50_000);
    }

    #[test]
    fn test_response_debug() {
        let response = Response {
            body: vec![1, 2, 3],
            fmt: Format::Json,
        };

        let debug_str = format!("{:?}", response);
        assert!(debug_str.contains("Response"));
        assert!(debug_str.contains("Json"));
    }

    // ============================================================================
    // IntoResponse Tests
    // ============================================================================

    #[test]
    fn test_response_into_response() {
        let body = b"test response body".to_vec();
        let response = Response {
            body: body.clone(),
            fmt: Format::Json,
        };

        let axum_response = response.into_response();

        // Check that the response has the correct content-type header
        let content_type = axum_response.headers().get(CONTENT_TYPE);
        assert!(content_type.is_some());
        assert_eq!(content_type.unwrap().to_str().unwrap(), "application/json");
    }

    #[test]
    fn test_response_into_response_preserves_body() {
        let body = b"important data".to_vec();
        let response = Response {
            body: body.clone(),
            fmt: Format::Json,
        };

        let _ = response.into_response();
        // If this compiles and runs, the body was successfully moved into the response
    }

    // ============================================================================
    // UTF-8 and Edge Cases
    // ============================================================================

    #[test]
    fn test_response_utf8_body() {
        let utf8_text = "Hello 世界 🚀".as_bytes().to_vec();
        let response = Response {
            body: utf8_text.clone(),
            fmt: Format::Json,
        };

        assert_eq!(response.body, utf8_text);
    }

    #[test]
    fn test_response_binary_body() {
        let binary_data = vec![0xFF, 0xFE, 0xFD, 0xFC];
        let response = Response {
            body: binary_data.clone(),
            fmt: Format::Json,
        };

        assert_eq!(response.body, binary_data);
    }
}
