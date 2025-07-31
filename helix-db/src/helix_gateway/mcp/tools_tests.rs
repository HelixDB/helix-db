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

    fn setup_temp_env() -> Env {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        unsafe {
            EnvOpenOptions::new()
                .map_size(1 * 1024 * 1024 * 1024) // 1 GB
                .max_dbs(10)
                .open(path)
                .unwrap()
        }
    }

    fn setup_test_environment() -> (MCPBackend, PathBuf, Arc<HelixGraphStorage>) {
        let temp_dir = env::temp_dir();
        let config = Config::new(
            16,   // m
            128,  // ef_construction
            768,  // ef_search
            2,    // db_max_size_gb
            true, // mcp
            true, // bm25
            None, // schema
            None, // embedding_model
            None, // graphvis_node_label
        );
        let db = Arc::new(HelixGraphStorage::new(&temp_dir.to_string_lossy(), config).unwrap());

        let backend = MCPBackend { db };
        (backend, temp_dir, db)
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
