use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::helix_engine::types::GraphError;

#[derive(Serialize, Deserialize, Debug)]
pub struct VectorConfig {
    // Maximum number of bi-directional links per element
    pub m: Option<usize>,

    // Size of dynamic candidate list for graph construction
    pub ef_construction: Option<usize>,

    // Size of dynamic candidate list for graph search
    pub ef_search: Option<usize>,

    // Database in GB
    pub db_max_size: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GraphConfig {
    pub secondary_indices: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub vector_config: VectorConfig,
    pub graph_config: GraphConfig,
    // // Path to the database
    // pub db_path: String,

    // // Port to run the server on
    // pub port: usize,
}

impl Config {
    pub fn new(m: usize, ef_construction: usize, ef_search: usize, db_max_size: usize) -> Self {
        Self {
            vector_config: VectorConfig {
                m: Some(m),
                ef_construction: Some(ef_construction),
                ef_search: Some(ef_search),
                db_max_size: Some(db_max_size),
            },
            graph_config: GraphConfig {
                secondary_indices: None,
            },
        }
    }

    pub fn from_config_file(input_path: PathBuf) -> Result<Self, GraphError> { // TODO: this isn't
                                                                               // read
        if !input_path.exists() {
            return Err(GraphError::ConfigFileNotFound);
        }
        let config = std::fs::read_to_string(input_path)?;
        let config = sonic_rs::from_str::<Config>(&config)?;

        Ok(config)
    }

    pub fn init_config() -> String {
        r#"
{
    "vector_config": {
        "m": 16,
        "ef_construction": 128,
        "ef_search": 768,
        "db_max_size": 100
    },
    "graph_config": {
        "secondary_indices": []
    }
}
"#
        .to_string()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            vector_config: VectorConfig {
                m: Some(16),
                ef_construction: Some(128),
                ef_search: Some(768),
                db_max_size: Some(10_000),
            },
            graph_config: GraphConfig {
                secondary_indices: None,
            },
        }
    }
}
