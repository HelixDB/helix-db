//! Helix Cloud: the session-authenticated client, typed resource models, and
//! the resolver that turns optional arguments plus the helix.toml link into
//! concrete workspaces, projects, and databases.

mod client;
pub mod model;
pub mod resolve;

pub(crate) use client::deserialize_i64;
pub use client::{CloudClient, HttpError, SessionCredentials};
