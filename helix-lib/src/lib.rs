pub mod client;
pub mod error;
pub mod handler_metadata;

pub use client::HelixDB;
pub use error::{HelixError, HelixResult};

// Re-export commonly needed types from helix-db
pub use helix_db::helix_engine::traversal_core::config::Config;
pub use helix_db::helix_engine::storage_core::version_info::VersionInfo;
pub use helix_db::helix_gateway::router::router::HandlerInput;
pub use helix_db::protocol::{Request, Response, Format};
pub use helix_db::helix_engine::types::GraphError;

// Re-export handler registration types needed by the macro
pub use helix_db::helix_gateway::router::router::{HandlerSubmission, Handler};

// Re-export macros
pub use helix_macros::handler;

// Include generated handler metadata
include!(concat!(env!("OUT_DIR"), "/handler_metadata.rs"));
