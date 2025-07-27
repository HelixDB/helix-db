use super::storage_methods::DBMethods;
use crate::{
    helix_engine::{
        bm25::bm25::HBM25Config,
        graph_core::config::Config,
        storage_core::storage_methods::StorageMethods,
        types::GraphError,
        vector_core::{
            hnsw::HNSW,
            vector::HVector,
            vector_core::{HNSWConfig, VectorCore},
        },
    },
    protocol::value::Value,
    utils::{
        filterable::Filterable,
        items::{Edge, Node},
        label_hash::hash_label,
    },
};
use heed3::{Database, DatabaseFlags, Env, EnvOpenOptions, RoTxn, RwTxn, byteorder::BE, types::*};
use std::{collections::HashMap, fs, path::Path};

// database names for different stores
const DB_NODES: &str = "nodes"; // for node data (n:)
const DB_EDGES: &str = "edges"; // for edge data (e:)
const DB_OUT_EDGES: &str = "out_edges"; // for outgoing edge indices (o:)
const DB_IN_EDGES: &str = "in_edges"; // for incoming edge indices (i:)

pub type NodeId = u128;
pub type EdgeId = u128;

pub struct StorageConfig {
    pub schema: String,
    pub graphvis_node_label: Option<String>,
    pub embedding_model: Option<String>,
}

pub struct HelixGraphStorage {
    pub graph_env: Env,

    pub nodes_db: Database<U128<BE>, Bytes>,
    pub edges_db: Database<U128<BE>, Bytes>,
    pub out_edges_db: Database<Bytes, Bytes>,
    pub in_edges_db: Database<Bytes, Bytes>,
    pub secondary_indices: HashMap<String, Database<Bytes, U128<BE>>>,
    pub vectors: VectorCore,
    pub bm25: Option<HBM25Config>,

    pub storage_config: StorageConfig,
}

impl HelixGraphStorage {
    pub fn new(path: &str, config: Config) -> Result<HelixGraphStorage, GraphError> {
        fs::create_dir_all(path)?;

        let db_size = if config.db_max_size_gb.unwrap_or(100) >= 9999 {
            9998
        } else {
            config.db_max_size_gb.unwrap_or(100)
        };

        let graph_env = unsafe {
            EnvOpenOptions::new()
                .map_size(db_size * 1024 * 1024 * 1024)
                .max_dbs(20)
                .max_readers(200)
                .open(Path::new(path))?
        };

        let mut wtxn = graph_env.write_txn()?;

        // creates the lmdb databases (tables)
        // Table: [key]->[value]
        //        [size]->[size]

        // Nodes: [node_id]->[bytes array of node data]
        //        [16 bytes]->[dynamic]
        let nodes_db = graph_env
            .database_options()
            .types::<U128<BE>, Bytes>()
            .name(DB_NODES)
            .create(&mut wtxn)?;

        // Edges: [edge_id]->[bytes array of edge data]
        //        [16 bytes]->[dynamic]
        let edges_db = graph_env
            .database_options()
            .types::<U128<BE>, Bytes>()
            .name(DB_EDGES)
            .create(&mut wtxn)?;

        // Out edges: [from_node_id + label]->[edge_id + to_node_id]  (edge first because value is ordered by byte size)
        //                    [20 + 4 bytes]->[16 + 16 bytes]
        //
        // DUP_SORT used to store all values of duplicated keys under a single key. Saves on space and requires a single read to get all values.
        // DUP_FIXED used to ensure all values are the same size meaning 8 byte length header is discarded.
        let out_edges_db: Database<Bytes, Bytes> = graph_env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
            .name(DB_OUT_EDGES)
            .create(&mut wtxn)?;

        // In edges: [to_node_id + label]->[edge_id + from_node_id]  (edge first because value is ordered by byte size)
        //                 [20 + 4 bytes]->[16 + 16 bytes]
        //
        // DUP_SORT used to store all values of duplicated keys under a single key. Saves on space and requires a single read to get all values.
        // DUP_FIXED used to ensure all values are the same size meaning 8 byte length header is discarded.
        let in_edges_db: Database<Bytes, Bytes> = graph_env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
            .name(DB_IN_EDGES)
            .create(&mut wtxn)?;

        let mut secondary_indices = HashMap::new();
        if let Some(indexes) = config.get_graph_config().secondary_indices {
            for index in indexes {
                secondary_indices.insert(
                    index.clone(),
                    graph_env
                        .database_options()
                        .types::<Bytes, U128<BE>>()
                        .flags(DatabaseFlags::DUP_SORT) // DUP_SORT used to store all duplicated node keys under a single key. Saves on space and requires a single read to get all values.
                        .name(&index)
                        .create(&mut wtxn)?,
                );
            }
        }

        let vector_config = config.get_vector_config();
        let vectors = VectorCore::new(
            &graph_env,
            &mut wtxn,
            HNSWConfig::new(
                vector_config.m,
                vector_config.ef_construction,
                vector_config.ef_search,
            ),
        )?;

        let bm25 = config
            .get_bm25()
            .then(|| HBM25Config::new(&graph_env, &mut wtxn))
            .transpose()?;

        let storage_config = StorageConfig::new(
            config.schema.unwrap_or("".to_string()),
            config.graphvis_node_label,
            config.embedding_model,
        );

        wtxn.commit()?;
        Ok(Self {
            graph_env,
            nodes_db,
            edges_db,
            out_edges_db,
            in_edges_db,
            secondary_indices,
            vectors,
            bm25,
            storage_config,
        })
    }

    /// Used because in the case the key changes in the future.
    /// Believed to not introduce any overhead being inline and using a reference.
    #[must_use]
    #[inline(always)]
    pub fn node_key(id: &u128) -> &u128 {
        id
    }

    /// Used because in the case the key changes in the future.
    /// Believed to not introduce any overhead being inline and using a reference.
    #[must_use]
    #[inline(always)]
    pub fn edge_key(id: &u128) -> &u128 {
        id
    }

    /// Out edge key generator. Creates a 20 byte array and copies in the node id and 4 byte label.
    ///
    /// key = `from-node(16)` | `label-id(4)`                 ← 20 B
    ///
    /// The generated out edge key will remain the same for the same from_node_id and label.
    /// To save space, the key is only stored once,
    /// with the values being stored in a sorted sub-tree, with this key being the root.
    #[inline(always)]
    pub fn out_edge_key(from_node_id: &u128, label: &[u8; 4]) -> [u8; 20] {
        let mut key = [0u8; 20];
        key[0..16].copy_from_slice(&from_node_id.to_be_bytes());
        key[16..20].copy_from_slice(label);
        key
    }

    /// In edge key generator. Creates a 20 byte array and copies in the node id and 4 byte label.
    ///
    /// key = `to-node(16)` | `label-id(4)`                 ← 20 B
    ///
    /// The generated in edge key will remain the same for the same to_node_id and label.
    /// To save space, the key is only stored once,
    /// with the values being stored in a sorted sub-tree, with this key being the root.
    #[inline(always)]
    pub fn in_edge_key(to_node_id: &u128, label: &[u8; 4]) -> [u8; 20] {
        let mut key = [0u8; 20];
        key[0..16].copy_from_slice(&to_node_id.to_be_bytes());
        key[16..20].copy_from_slice(label);
        key
    }

    /// Packs the edge data into a 32 byte array.
    ///
    /// data = `edge-id(16)` | `node-id(16)`                 ← 32 B (DUPFIXED)
    #[inline(always)]
    pub fn pack_edge_data(edge_id: &u128, node_id: &u128) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0..16].copy_from_slice(&edge_id.to_be_bytes());
        key[16..32].copy_from_slice(&node_id.to_be_bytes());
        key
    }

    /// Unpacks the 32 byte array into an (edge_id, node_id) tuple of u128s.
    ///
    /// Returns (edge_id, node_id)
    #[inline(always)]
    // Uses Type Aliases for clarity
    pub fn unpack_adj_edge_data(data: &[u8]) -> Result<(EdgeId, NodeId), GraphError> {
        let edge_id = u128::from_be_bytes(
            data[0..16]
                .try_into()
                .map_err(|_| GraphError::SliceLengthError)?,
        );
        let node_id = u128::from_be_bytes(
            data[16..32]
                .try_into()
                .map_err(|_| GraphError::SliceLengthError)?,
        );
        Ok((edge_id, node_id))
    }

    /// Gets a vector from level 0 of HNSW index (because that's where all are stored)
    pub fn get_vector(&self, txn: &RoTxn, id: &u128) -> Result<Option<HVector>, GraphError> {
        let vector = self.vectors.get_vector(txn, *id, 0, true)?;
        if let Ok(is_deleted) = vector.check_property("is_deleted") {
            if let Value::Boolean(is_deleted) = is_deleted.as_ref() {
                if *is_deleted {
                    return Ok(None);
                }
            }
        }
        Ok(Some(vector))
    }
}

impl StorageConfig {
    pub fn new(
        schema: String,
        graphvis_node_label: Option<String>,
        embedding_model: Option<String>,
    ) -> StorageConfig {
        Self {
            schema,
            graphvis_node_label,
            embedding_model,
        }
    }
}

impl DBMethods for HelixGraphStorage {
    /// Creates a secondary index lmdb db (table) for a given index name
    fn create_secondary_index(&mut self, name: &str) -> Result<(), GraphError> {
        let mut wtxn = self.graph_env.write_txn()?;
        let db = self.graph_env.create_database(&mut wtxn, Some(name))?;
        wtxn.commit()?;
        self.secondary_indices.insert(name.to_string(), db);
        Ok(())
    }

    /// Drops a secondary index lmdb db (table) for a given index name
    fn drop_secondary_index(&mut self, name: &str) -> Result<(), GraphError> {
        let mut wtxn = self.graph_env.write_txn()?;
        let db = self
            .secondary_indices
            .get(name)
            .ok_or(GraphError::New(format!("Secondary Index {name} not found")))?;
        db.clear(&mut wtxn)?;
        wtxn.commit()?;
        self.secondary_indices.remove(name);
        Ok(())
    }
}

impl StorageMethods for HelixGraphStorage {
    #[inline(always)]
    fn check_exists(&self, txn: &RoTxn, id: &u128) -> Result<bool, GraphError> {
        Ok(self.nodes_db.get(txn, Self::node_key(id))?.is_some())
    }

    #[inline(always)]
    fn get_node(&self, txn: &RoTxn, id: &u128) -> Result<Node, GraphError> {
        let node = match self.nodes_db.get(txn, Self::node_key(id))? {
            Some(data) => data,
            None => return Err(GraphError::NodeNotFound),
        };
        let node: Node = Node::decode_node(node, *id)?;
        Ok(node)
    }

    #[inline(always)]
    fn get_edge(&self, txn: &RoTxn, id: &u128) -> Result<Edge, GraphError> {
        let edge = match self.edges_db.get(txn, Self::edge_key(id))? {
            Some(data) => data,
            None => return Err(GraphError::EdgeNotFound),
        };
        let edge: Edge = Edge::decode_edge(edge, *id)?;
        Ok(edge)
    }

    fn drop_node(&self, txn: &mut RwTxn, id: &u128) -> Result<(), GraphError> {
        // Get node to get its label
        //let node = self.get_node(txn, id)?;

        // Delete outgoing edges
        let out_edges = {
            let iter = self.out_edges_db.prefix_iter(txn, &id.to_be_bytes())?;
            let capacity = match iter.size_hint() {
                (_, Some(upper)) => upper,
                (lower, None) => lower,
            };
            let mut out_edges = Vec::with_capacity(capacity);

            for result in iter {
                let (key, value) = result?;
                assert_eq!(key.len(), 20);
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (edge_id, _) = Self::unpack_adj_edge_data(value)?;
                out_edges.push((edge_id, label));
            }
            out_edges
        };

        // Delete incoming edges

        let in_edges = {
            let iter = self.in_edges_db.prefix_iter(txn, &id.to_be_bytes())?;
            let capacity = match iter.size_hint() {
                (_, Some(c)) => c,
                (c, None) => c,
            };
            let mut in_edges = Vec::with_capacity(capacity);

            for result in iter {
                let (key, value) = result?;
                assert_eq!(key.len(), 20);
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (edge_id, node_id) = Self::unpack_adj_edge_data(value)?;
                in_edges.push((edge_id, label, node_id));
            }

            in_edges
        };

        // Delete all related data
        for (out_edge_id, label_bytes) in out_edges.iter() {
            // Delete edge data
            self.edges_db.delete(txn, Self::edge_key(out_edge_id))?;
            self.out_edges_db
                .delete(txn, &Self::out_edge_key(id, label_bytes))?;
        }
        for (in_edge_id, label_bytes, other_id) in in_edges.iter() {
            self.edges_db.delete(txn, Self::edge_key(in_edge_id))?;
            self.in_edges_db
                .delete(txn, &Self::in_edge_key(other_id, label_bytes))?;
        }

        // Delete node data and label
        self.nodes_db.delete(txn, Self::node_key(id))?;

        Ok(())
    }

    fn drop_edge(&self, txn: &mut RwTxn, edge_id: &u128) -> Result<(), GraphError> {
        // Get edge data first
        let edge_data = match self.edges_db.get(txn, Self::edge_key(edge_id))? {
            Some(data) => data,
            None => return Err(GraphError::EdgeNotFound),
        };
        let edge: Edge = bincode::deserialize(edge_data)?;
        let label_hash = hash_label(&edge.label, None);
        // Delete all edge-related data
        self.edges_db.delete(txn, Self::edge_key(edge_id))?;
        self.out_edges_db
            .delete(txn, &Self::out_edge_key(&edge.from_node, &label_hash))?;
        self.in_edges_db
            .delete(txn, &Self::in_edge_key(&edge.to_node, &label_hash))?;

        Ok(())
    }

    fn drop_vector(&self, txn: &mut RwTxn, id: &u128) -> Result<(), GraphError> {
        // Delete outgoing edges
        let out_edges = {
            let iter = self.out_edges_db.prefix_iter(txn, &id.to_be_bytes())?;
            let capacity = match iter.size_hint() {
                (_, Some(upper)) => upper,
                (lower, None) => lower,
            };
            let mut out_edges = Vec::with_capacity(capacity);

            for result in iter {
                let (key, value) = result?;
                assert_eq!(key.len(), 20);
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (edge_id, _) = Self::unpack_adj_edge_data(value)?;
                out_edges.push((edge_id, label));
            }
            out_edges
        };

        // Delete incoming edges

        let in_edges = {
            let iter = self.in_edges_db.prefix_iter(txn, &id.to_be_bytes())?;
            let capacity = match iter.size_hint() {
                (_, Some(c)) => c,
                (c, None) => c,
            };
            let mut in_edges = Vec::with_capacity(capacity);

            for result in iter {
                let (key, value) = result?;
                assert_eq!(key.len(), 20);
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (edge_id, node_id) = Self::unpack_adj_edge_data(value)?;
                in_edges.push((edge_id, label, node_id));
            }

            in_edges
        };

        // Delete all related data
        for (out_edge_id, label_bytes) in out_edges.iter() {
            // Delete edge data
            self.edges_db.delete(txn, Self::edge_key(out_edge_id))?;
            self.out_edges_db
                .delete(txn, &Self::out_edge_key(id, label_bytes))?;
        }
        for (in_edge_id, label_bytes, other_id) in in_edges.iter() {
            self.edges_db.delete(txn, Self::edge_key(in_edge_id))?;
            self.in_edges_db
                .delete(txn, &Self::in_edge_key(other_id, label_bytes))?;
        }

        // Delete vector data
        self.vectors.delete(txn, *id)?;

        Ok(())
    }
}
