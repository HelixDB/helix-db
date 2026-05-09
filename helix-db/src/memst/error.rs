//! Error types for the MemSt integration.
//!
//! Ported from `memst-core/src/error.rs`. Variants requiring external HTTP/LLM
//! crates have been removed - HelixDB does not need the LLM extraction path of
//! the upstream `memst-core` to integrate the memory and session subsystems.

use std::io;
use std::num::ParseIntError;
use thiserror::Error;

use super::objects::ObjectId;

/// Result type alias with the MemSt integration error.
pub type Result<T> = std::result::Result<T, Error>;

/// MemSt integration error types.
#[derive(Debug, Error)]
pub enum Error {
    /// I/O error
    #[error(transparent)]
    Io(#[from] io::Error),

    /// Bincode serialization error
    #[error(transparent)]
    Serialize(#[from] bincode::Error),

    /// JSON serialization error
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Session not found
    #[error("Session not found: {0}")]
    SessionNotFound(uuid::Uuid),

    /// Message not found
    #[error("Message not found: {0}")]
    MessageNotFound(uuid::Uuid),

    /// Lock error
    #[error("Failed to acquire lock: {0}")]
    LockError(String),

    /// Index out of bounds
    #[error("Index out of bounds: {0}")]
    IndexOutOfBounds(String),

    /// Corrupted data
    #[error("Data corruption detected: {0}")]
    Corruption(String),

    /// Version mismatch
    #[error("Version mismatch: expected {expected}, found {found}")]
    VersionMismatch {
        /// Expected version
        expected: String,
        /// Found version
        found: String,
    },

    /// Invalid operation
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// UUID parsing error
    #[error(transparent)]
    UuidParse(#[from] uuid::Error),

    /// Parse integer error
    #[error(transparent)]
    ParseInt(#[from] ParseIntError),

    /// Object not found
    #[error("Object not found: {0}")]
    ObjectNotFound(ObjectId),

    /// Invalid object ID
    #[error("Invalid object ID: {0}")]
    InvalidObjectId(String),

    /// Invalid object format
    #[error("Invalid object format")]
    InvalidObjectFormat,
}
