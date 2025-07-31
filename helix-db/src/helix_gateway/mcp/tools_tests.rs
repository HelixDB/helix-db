#[cfg(test)]
mod tests {
    use crate::{
        debug_println,
        helix_engine::{
            graph_core::{
                config::Config,
                ops::{source::add_e::EdgeType, tr_val::TraversalVal},
            },
            storage_core::storage_core::HelixGraphStorage,
            types::GraphError,
        },
        helix_gateway::mcp::mcp::{MCPBackend, MCPConnection, MCPHandler},
    };
    use heed3::{Env, EnvOpenOptions, RoTxn};
    use std::{env, path::PathBuf, sync::Arc};
    use tempfile::TempDir;

    /*
       let env = setup_temp_env();
       let mut txn = env.write_txn().unwrap();
       let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
       let mut total_insertion_time = std::time::Duration::from_secs(0);
    */

    fn setup_test_environment() -> () {
        //make a new graph storage
    }

    // Helper to create test data
    fn populate_test_data(backend: &MCPBackend) -> Result<(), GraphError> {
        // Add test nodes and edges here
        // This would depend on your HelixGraphStorage API
        Ok(())
    }

    #[test]
    fn test_mcp_tool_out_step() {}

    #[test]
    fn test_mcp_tool_out_e_step() {}

    #[test]
    fn test_mcp_tool_in_step() {}

    #[test]
    fn test_mcp_tool_in_e_step() {}

    #[test]
    fn test_mcp_tool_n_from_type() {}

    #[test]
    fn test_mcp_tool_e_from_type() {}

    #[test]
    fn test_mcp_tool_filter_items() {}

    // TODO
    #[test]
    fn test_mcp_tool_search_keyword() {
        // - mcp connection
        // - db
    }

    #[test]
    fn test_mcp_tool_search_vector_text() {}
}
