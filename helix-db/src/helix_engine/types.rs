use crate::{helix_gateway::router::router::IoContFn, helixc::parser::errors::ParserError};
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
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::StorageConnectionError(msg, e) => {
                write!(f, "Error: {msg} {e}")
            }
            Self::GraphConnectionError(msg, e) => {
                write!(f, "Error: {msg} {e}")
            }
            Self::TraversalError(msg) => write!(f, "Traversal error: {msg}"),
            Self::StorageError(msg) => write!(f, "Storage error: {msg}"),
            Self::ConversionError(msg) => write!(f, "Conversion error: {msg}"),
            Self::DecodeError(msg) => write!(f, "Decode error: {msg}"),
            Self::EdgeNotFound => write!(f, "Edge not found"),
            Self::NodeNotFound => write!(f, "Node not found"),
            Self::LabelNotFound => write!(f, "Label not found"),
            Self::New(msg) => write!(f, "Graph error: {msg}"),
            Self::Default => write!(f, "Graph error"),
            Self::Empty => write!(f, "No Error"),
            Self::MultipleNodesWithSameId => write!(f, "Multiple nodes with same id"),
            Self::MultipleEdgesWithSameId => write!(f, "Multiple edges with same id"),
            Self::InvalidNode => write!(f, "Invalid node"),
            Self::ConfigFileNotFound => write!(f, "Config file not found"),
            Self::SliceLengthError => write!(f, "Slice length error"),
            Self::VectorError(msg) => write!(f, "Vector error: {msg}"),
            Self::ShortestPathNotFound => write!(f, "Shortest path not found"),
            Self::EmbeddingError(msg) => write!(f, "Error while embedding text: {msg}"),
            Self::ParamNotFound(param) => write!(f, "Parameter {param} not found in request"),
            Self::IoNeeded(_) => {
                write!(f, "Asyncronous IO is needed to complete the DB operation")
            }
            Self::RerankerError(msg) => write!(f, "Reranker error: {msg}"),
        }
    }
}

impl From<HeedError> for GraphError {
    fn from(error: HeedError) -> Self {
        Self::StorageError(error.to_string())
    }
}

impl From<std::io::Error> for GraphError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<AddrParseError> for GraphError {
    fn from(error: AddrParseError) -> Self {
        Self::ConversionError(format!("AddrParseError: {error}"))
    }
}

impl From<SonicError> for GraphError {
    fn from(error: SonicError) -> Self {
        Self::ConversionError(format!("sonic error: {error}"))
    }
}

impl From<FromUtf8Error> for GraphError {
    fn from(error: FromUtf8Error) -> Self {
        Self::ConversionError(format!("FromUtf8Error: {error}"))
    }
}

impl From<&'static str> for GraphError {
    fn from(error: &'static str) -> Self {
        Self::New(error.to_string())
    }
}

impl From<String> for GraphError {
    fn from(error: String) -> Self {
        Self::New(error.to_string())
    }
}

impl From<bincode::Error> for GraphError {
    fn from(error: bincode::Error) -> Self {
        Self::ConversionError(format!("bincode error: {error}"))
    }
}

impl From<ParserError> for GraphError {
    fn from(error: ParserError) -> Self {
        Self::ConversionError(format!("ParserError: {error}"))
    }
}

impl From<Utf8Error> for GraphError {
    fn from(error: Utf8Error) -> Self {
        Self::ConversionError(format!("Utf8Error: {error}"))
    }
}

impl From<uuid::Error> for GraphError {
    fn from(error: uuid::Error) -> Self {
        Self::ConversionError(format!("uuid error: {error}"))
    }
}

impl From<VectorError> for GraphError {
    fn from(error: VectorError) -> Self {
        Self::VectorError(format!("VectorError: {error}"))
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
}

impl std::error::Error for VectorError {}

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VectorNotFound(id) => write!(f, "Vector not found: {id}"),
            Self::VectorDeleted => write!(f, "Vector deleted"),
            Self::InvalidVectorLength => write!(f, "Invalid vector length"),
            Self::InvalidVectorData => write!(f, "Invalid vector data"),
            Self::EntryPointNotFound => write!(f, "Entry point not found"),
            Self::ConversionError(msg) => write!(f, "Conversion error: {msg}"),
            Self::VectorCoreError(msg) => write!(f, "Vector core error: {msg}"),
            Self::VectorAlreadyDeleted(id) => write!(f, "Vector already deleted: {id}"),
        }
    }
}

impl From<HeedError> for VectorError {
    fn from(error: HeedError) -> Self {
        Self::VectorCoreError(format!("heed error: {error}"))
    }
}

impl From<FromUtf8Error> for VectorError {
    fn from(error: FromUtf8Error) -> Self {
        Self::ConversionError(format!("FromUtf8Error: {error}"))
    }
}

impl From<Utf8Error> for VectorError {
    fn from(error: Utf8Error) -> Self {
        Self::ConversionError(format!("Utf8Error: {error}"))
    }
}

impl From<SonicError> for VectorError {
    fn from(error: SonicError) -> Self {
        Self::ConversionError(format!("SonicError: {error}"))
    }
}

impl From<bincode::Error> for VectorError {
    fn from(error: bincode::Error) -> Self {
        Self::ConversionError(format!("bincode error: {error}"))
    }
}
