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
}

impl IntoResponse for HelixError {
    fn into_response(self) -> axum::response::Response {
        let body = self.to_string();
        let code = match &self {
            HelixError::Graph(_) | HelixError::Vector(_) => 500,
            HelixError::NotFound { .. } => 404,
        };

        axum::response::Response::builder()
            .status(code)
            .body(Body::from(body))
            .unwrap_or_else(|_| panic!("Should be able to turn HelixError into Response: {self}"))
    }
}