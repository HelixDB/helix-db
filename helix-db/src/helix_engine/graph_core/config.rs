use crate::helix_engine::types::GraphError;
use std::{
    path::PathBuf,
    fmt,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct VectorConfig {
    pub m: Option<usize>,
    pub ef_construction: Option<usize>,
    pub ef_search: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GraphConfig {
    pub secondary_indices: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub vector_config: VectorConfig,
    pub graph_config: GraphConfig,
    pub db_max_size_gb: Option<usize>,
    pub mcp: bool,
    pub bm25: bool,
    pub schema: Option<String>,
    pub embedding_model: Option<String>,
    pub graphvis_node_label: Option<String>,
}

impl Config {
    pub fn new(
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        db_max_size_gb: usize,
        mcp: bool,
        bm25: bool,
        schema: Option<String>,
        embedding_model: Option<String>,
        graphvis_node_label: Option<String>,
    ) -> Self {
        Self {
            vector_config: VectorConfig {
                m: Some(m),
                ef_construction: Some(ef_construction),
                ef_search: Some(ef_search),
            },
            graph_config: GraphConfig {
                secondary_indices: None,
            },
            db_max_size_gb: Some(db_max_size_gb),
            mcp,
            bm25,
            schema,
            embedding_model,
            graphvis_node_label,
        }
    }

    pub fn from_files(config_path: PathBuf, schema_path:PathBuf) -> Result<Self, GraphError> {
        if !config_path.exists() {
            println!("no config path!");
            return Err(GraphError::ConfigFileNotFound);
        }

        let config = std::fs::read_to_string(config_path)?;
        let mut config = sonic_rs::from_str::<Config>(&config)?;

        if schema_path.exists() {
            let schema_string = std::fs::read_to_string(schema_path)?;
            config.schema = Some(schema_string);
        } else {
            config.schema = None;
        }

        Ok(config)
    }

    pub fn init_config() -> String {
    r#"
    {
        "vector_config": {
            "m": 16,
            "ef_construction": 128,
            "ef_search": 768
        },
        "graph_config": {
            "secondary_indices": []
        },
        "db_max_size_gb": 10,
        "mcp": true,
        "bm25": true,
        "embedding_model": "text-embedding-ada-002",
        "graphvis_node_label": ""
    }
    "#
    .to_string()
    }

    pub fn to_json(&self) -> String {
        sonic_rs::to_string_pretty(self).unwrap()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            vector_config: VectorConfig {
                m: Some(16),
                ef_construction: Some(128),
                ef_search: Some(768),
            },
            graph_config: GraphConfig {
                secondary_indices: None,
            },
            db_max_size_gb: Some(10),
            mcp: true,
            bm25: true,
            schema: None,
            embedding_model: Some("text-embedding-ada-002".to_string()),
            graphvis_node_label: None,
        }
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Vector config => m: {:?}, ef_construction: {:?}, ef_search: {:?}\n
            Graph config => secondary_indicies: {:?}\n
            db_max_size_gb: {:?}\n
            mcp: {:?}\n
            bm25: {:?}\n
            schema: {:?}\n
            embedding_model: {:?}\n
            graphvis_node_label: {:?}",
            self.vector_config.m,
            self.vector_config.ef_construction,
            self.vector_config.ef_search,
            self.graph_config.secondary_indices,
            self.db_max_size_gb,
            self.mcp,
            self.bm25,
            self.schema,
            self.embedding_model,
            self.graphvis_node_label,
        )
    }
}

