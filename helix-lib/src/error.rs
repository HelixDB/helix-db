use thiserror::Error;
use helix_db::helix_engine::types::GraphError;
use helix_db::protocol::HelixError as ProtocolError;

pub type HelixResult<T> = Result<T, HelixError>;

#[derive(Error, Debug)]
pub enum HelixError {
    #[error("Handler not found: {0}")]
    HandlerNotFound(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    
    #[error("Database error: {0}")]
    Database(#[from] GraphError),
    
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    
    #[error("Storage error: {0}")]
    Storage(String),
}

impl From<serde_json::Error> for HelixError {
    fn from(e: serde_json::Error) -> Self {
        HelixError::Serialization(e.to_string())
    }
}

impl From<heed3::Error> for HelixError {
    fn from(e: heed3::Error) -> Self {
        HelixError::Storage(format!("Storage error: {:?}", e))
    }
}
