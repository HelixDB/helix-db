#[cfg(feature = "dev-instance")]
pub mod builtin;
pub mod embedding_providers;
pub mod gateway;
pub mod introspect_schema;
pub mod mcp;
pub mod router;
pub mod worker_pool;

#[cfg(test)]
pub mod tests;