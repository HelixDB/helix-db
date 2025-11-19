use crate::{
    helix_engine::vector_core::{ItemId, LayerId, key::Key, node_id::NodeMode},
    helix_gateway::router::router::IoContFn,
    helixc::parser::errors::ParserError,
};
use core::fmt;
use heed3::Error as HeedError;
use sonic_rs::Error as SonicError;
use std::{net::AddrParseError, str::Utf8Error, string::FromUtf8Error};

#[derive(Debug)]
pub enum GraphError {
    Io(std::io::Error),
    GraphConnectionError(String, std::io::Error),
    StorageConnectionError(String, std::io::Error),
    StorageError(String),
    TraversalError(String),
    ConversionError(String),
    DecodeError(String),
    EdgeNotFound,
    NodeNotFound,
    LabelNotFound,
    VectorError(String),
    Default,
    New(String),
    Empty,
    MultipleNodesWithSameId,
    MultipleEdgesWithSameId,
    InvalidNode,
    ConfigFileNotFound,
    SliceLengthError,
    ShortestPathNotFound,
    EmbeddingError(String),
    ParamNotFound(&'static str),
    IoNeeded(IoContFn),
    RerankerError(String),
}

impl std::error::Error for GraphError {}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphError::Io(e) => write!(f, "IO error: {e}"),
            GraphError::StorageConnectionError(msg, e) => {
                write!(f, "Error: {msg} {e}")
            }
            GraphError::GraphConnectionError(msg, e) => {
                write!(f, "Error: {msg} {e}")
            }
            GraphError::TraversalError(msg) => write!(f, "Traversal error: {msg}"),
            GraphError::StorageError(msg) => write!(f, "Storage error: {msg}"),
            GraphError::ConversionError(msg) => write!(f, "Conversion error: {msg}"),
            GraphError::DecodeError(msg) => write!(f, "Decode error: {msg}"),
            GraphError::EdgeNotFound => write!(f, "Edge not found"),
            GraphError::NodeNotFound => write!(f, "Node not found"),
            GraphError::LabelNotFound => write!(f, "Label not found"),
            GraphError::New(msg) => write!(f, "Graph error: {msg}"),
            GraphError::Default => write!(f, "Graph error"),
            GraphError::Empty => write!(f, "No Error"),
            GraphError::MultipleNodesWithSameId => write!(f, "Multiple nodes with same id"),
            GraphError::MultipleEdgesWithSameId => write!(f, "Multiple edges with same id"),
            GraphError::InvalidNode => write!(f, "Invalid node"),
            GraphError::ConfigFileNotFound => write!(f, "Config file not found"),
            GraphError::SliceLengthError => write!(f, "Slice length error"),
            GraphError::VectorError(msg) => write!(f, "Vector error: {msg}"),
            GraphError::ShortestPathNotFound => write!(f, "Shortest path not found"),
            GraphError::EmbeddingError(msg) => write!(f, "Error while embedding text: {msg}"),
            GraphError::ParamNotFound(param) => write!(f, "Parameter {param} not found in request"),
            GraphError::IoNeeded(_) => {
                write!(f, "Asyncronous IO is needed to complete the DB operation")
            }
            GraphError::RerankerError(msg) => write!(f, "Reranker error: {msg}"),
        }
    }
}

impl From<HeedError> for GraphError {
    fn from(error: HeedError) -> Self {
        GraphError::StorageError(error.to_string())
    }
}

impl From<std::io::Error> for GraphError {
    fn from(error: std::io::Error) -> Self {
        GraphError::Io(error)
    }
}

impl From<AddrParseError> for GraphError {
    fn from(error: AddrParseError) -> Self {
        GraphError::ConversionError(format!("AddrParseError: {error}"))
    }
}

impl From<SonicError> for GraphError {
    fn from(error: SonicError) -> Self {
        GraphError::ConversionError(format!("sonic error: {error}"))
    }
}

impl From<FromUtf8Error> for GraphError {
    fn from(error: FromUtf8Error) -> Self {
        GraphError::ConversionError(format!("FromUtf8Error: {error}"))
    }
}

impl From<&'static str> for GraphError {
    fn from(error: &'static str) -> Self {
        GraphError::New(error.to_string())
    }
}

impl From<String> for GraphError {
    fn from(error: String) -> Self {
        GraphError::New(error.to_string())
    }
}

impl From<bincode::Error> for GraphError {
    fn from(error: bincode::Error) -> Self {
        GraphError::ConversionError(format!("bincode error: {error}"))
    }
}

impl From<ParserError> for GraphError {
    fn from(error: ParserError) -> Self {
        GraphError::ConversionError(format!("ParserError: {error}"))
    }
}

impl From<Utf8Error> for GraphError {
    fn from(error: Utf8Error) -> Self {
        GraphError::ConversionError(format!("Utf8Error: {error}"))
    }
}

impl From<uuid::Error> for GraphError {
    fn from(error: uuid::Error) -> Self {
        GraphError::ConversionError(format!("uuid error: {error}"))
    }
}

impl From<VectorError> for GraphError {
    fn from(error: VectorError) -> Self {
        GraphError::VectorError(format!("VectorError: {error}"))
    }
}

#[derive(Debug)]
pub enum VectorError {
    VectorNotFound(String),
    VectorDeleted,
    InvalidVectorLength,
    InvalidVectorData,
    EntryPointNotFound,
    ConversionError(String),
    VectorCoreError(String),
    VectorAlreadyDeleted(String),
    InvalidVecDimension {
        expected: usize,
        received: usize,
    },
    MissingKey {
        /// The index that caused the error
        index: u16,
        /// The kind of item that was being queried
        mode: &'static str,
        /// The item ID queried
        item: ItemId,
        /// The item's layer
        layer: LayerId,
    },
    Io(String),
    NeedBuild(u16),
    /// The user is trying to query a database with a distance that is not of the right type.
    UnmatchingDistance {
        /// The expected distance type.
        expected: String,
        /// The distance given by the user.
        received: &'static str,
    },
    MissingMetadata(u16),
    HasNoData,
}

impl std::error::Error for VectorError {}

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VectorError::VectorNotFound(id) => write!(f, "Vector not found: {id}"),
            VectorError::VectorDeleted => write!(f, "Vector deleted"),
            VectorError::InvalidVectorLength => write!(f, "Invalid vector length"),
            VectorError::InvalidVectorData => write!(f, "Invalid vector data"),
            VectorError::EntryPointNotFound => write!(f, "Entry point not found"),
            VectorError::ConversionError(msg) => write!(f, "Conversion error: {msg}"),
            VectorError::VectorCoreError(msg) => write!(f, "Vector core error: {msg}"),
            VectorError::VectorAlreadyDeleted(id) => write!(f, "Vector already deleted: {id}"),
            VectorError::InvalidVecDimension { expected, received } => {
                write!(
                    f,
                    "Invalid vector dimension: expected {expected}, received {received}"
                )
            }
            VectorError::MissingKey {
                index, mode, item, ..
            } => write!(
                f,
                "Internal error: {mode}({item}) is missing in index `{index}`"
            ),
            VectorError::Io(error) => write!(f, "IO error: {error}"),
            VectorError::NeedBuild(idx) => write!(
                f,
                "The graph has not been built after an update on index {idx}"
            ),
            VectorError::UnmatchingDistance { expected, received } => {
                write!(
                    f,
                    "Invalid distance provided. Got {received} but expected {expected}"
                )
            }
            VectorError::MissingMetadata(idx) => write!(
                f,
                "Metadata are missing on index {idx}, You must build your database before attempting to read it"
            ),
            VectorError::HasNoData => write!(f, "Trying to access data where there is none"),
        }
    }
}

impl VectorError {
    pub(crate) fn missing_key(key: Key) -> Self {
        Self::MissingKey {
            index: key.index,
            mode: match key.node.mode {
                NodeMode::Item => "Item",
                NodeMode::Links => "Links",
                NodeMode::Metadata => "Metadata",
                NodeMode::Updated => "Updated",
            },
            item: key.node.item,
            layer: key.node.layer,
        }
    }
}

impl From<HeedError> for VectorError {
    fn from(error: HeedError) -> Self {
        VectorError::VectorCoreError(format!("heed error: {error}"))
    }
}

impl From<FromUtf8Error> for VectorError {
    fn from(error: FromUtf8Error) -> Self {
        VectorError::ConversionError(format!("FromUtf8Error: {error}"))
    }
}

impl From<Utf8Error> for VectorError {
    fn from(error: Utf8Error) -> Self {
        VectorError::ConversionError(format!("Utf8Error: {error}"))
    }
}

impl From<SonicError> for VectorError {
    fn from(error: SonicError) -> Self {
        VectorError::ConversionError(format!("SonicError: {error}"))
    }
}

impl From<bincode::Error> for VectorError {
    fn from(error: bincode::Error) -> Self {
        VectorError::ConversionError(format!("bincode error: {error}"))
    }
}

impl From<std::io::Error> for VectorError {
    fn from(error: std::io::Error) -> Self {
        VectorError::Io(format!("Io Error: {error}"))
    }
}
