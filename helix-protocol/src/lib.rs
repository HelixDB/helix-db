//! Protobuf and gRPC contract for HelixDB.
//!
//! This crate owns the generated Rust types for the public `helix.v1` service.
//! The `.proto` file is included in the repository so other SDKs can generate
//! their native clients from the same source of truth.

/// Default local gRPC port reserved for HelixDB.
pub const DEFAULT_GRPC_PORT: u16 = 6970;

/// Versioned HelixDB gRPC API.
pub mod v1 {
    tonic::include_proto!("helix.v1");
}

#[cfg(test)]
mod tests {
    use super::v1::{
        query_request, DynamicQuery, HealthResponse, HealthStatus, QueryRequest, RequestOptions,
        SearchRequest,
    };

    #[test]
    fn dynamic_query_preserves_json_payload_and_options() {
        let request = QueryRequest {
            target: Some(query_request::Target::Dynamic(DynamicQuery {
                json: br#"{"request_type":"read"}"#.to_vec(),
            })),
            options: Some(RequestOptions {
                require_writer: true,
                warm_only: false,
                await_durability: Some(true),
            }),
        };

        let Some(query_request::Target::Dynamic(dynamic)) = request.target else {
            panic!("expected dynamic query target");
        };
        assert_eq!(dynamic.json.as_slice(), br#"{"request_type":"read"}"#);
        assert_eq!(request.options.unwrap().await_durability, Some(true));
    }

    #[test]
    fn search_request_uses_cross_language_vector_shape() {
        let request = SearchRequest {
            index: "Document".to_string(),
            vector: vec![0.25, -0.5, 0.75],
            limit: 10,
            filter_json: br#"{"tenant":"acme"}"#.to_vec(),
            options: None,
        };

        assert_eq!(request.index, "Document");
        assert_eq!(request.vector, vec![0.25, -0.5, 0.75]);
        assert_eq!(request.limit, 10);
    }

    #[test]
    fn health_status_round_trips_as_enum_value() {
        let response = HealthResponse {
            status: HealthStatus::Serving.into(),
            version: "dev".to_string(),
            message: "ready".to_string(),
        };

        assert_eq!(response.status(), HealthStatus::Serving);
    }
}
