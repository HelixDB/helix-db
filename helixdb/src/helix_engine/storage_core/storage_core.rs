use crate::{
    helix_engine::{
        graph_core::config::Config,
        storage_core::storage_methods::StorageMethods,
        types::GraphError,
        vector_core::vector_core::{HNSWConfig, VectorCore},
    },
    protocol::{
        filterable::Filterable,
        items::{Edge, Node},
        label_hash::hash_label,
        value::Value,
    },
};

use heed3::byteorder::BE;
use heed3::{types::*, Database, DatabaseFlags, Env, EnvOpenOptions, RoTxn, RwTxn, WithTls};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use super::storage_methods::{BasicStorageMethods, DBMethods};

// Database names for different stores
const DB_NODES: &str = "nodes"; // For node data (n:)
const DB_EDGES: &str = "edges"; // For edge data (e:)
const DB_OUT_EDGES: &str = "out_edges"; // For outgoing edge indices (o:)
const DB_IN_EDGES: &str = "in_edges"; // For incoming edge indices (i:)

// Key prefixes for different types of data

pub struct HelixGraphStorage {
    pub graph_env: Env<WithTls>,
    pub nodes_db: Database<U128<BE>, Bytes>,
    pub edges_db: Database<U128<BE>, Bytes>,
    pub out_edges_db: Database<Bytes, Bytes>,
    pub in_edges_db: Database<Bytes, Bytes>,
    pub secondary_indices: HashMap<String, Database<Bytes, Bytes>>,
    pub vectors: VectorCore,
}

impl HelixGraphStorage {
    pub fn new(path: &str, config: Config) -> Result<HelixGraphStorage, GraphError> {
        fs::create_dir_all(path)?;

        // Configure and open LMDB environment
        let graph_env = unsafe {
            EnvOpenOptions::new()
                .map_size(config.vector_config.db_max_size.unwrap_or(100) * 1024 * 1024 * 1024) // 10GB max
                .max_dbs(20)
                .max_readers(200)
                // .flags(EnvFlags::NO_META_SYNC)
                // .flags(EnvFlags::MAP_ASYNC)
                // .flags(EnvFlags::NO_SYNC)
                .open(Path::new(path))?
        };

        let mut wtxn = graph_env.write_txn()?;

        // Create/open all necessary databases
        let nodes_db = graph_env
            .database_options()
            .types::<U128<BE>, Bytes>()
            //.flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED) // NOTE: commend out because add_n gave error upon inserting
            .name(DB_NODES)
            .create(&mut wtxn)?;
        let edges_db = graph_env
            .database_options()
            .types::<U128<BE>, Bytes>()
            // .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
            .name(DB_EDGES)
            .create(&mut wtxn)?;
        let out_edges_db: Database<Bytes, Bytes> = graph_env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
            .name(DB_OUT_EDGES)
            .create(&mut wtxn)?;
        let in_edges_db: Database<Bytes, Bytes> = graph_env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
            .name(DB_IN_EDGES)
            .create(&mut wtxn)?;
        // Create secondary indices
        let mut secondary_indices = HashMap::new();
        if let Some(indexes) = config.graph_config.secondary_indices {
            for index in indexes {
                secondary_indices.insert(
                    index.clone(),
                    graph_env.create_database(&mut wtxn, Some(&index))?,
                );
            }
        }
        println!("Secondary Indices: {:?}", secondary_indices);

        let vectors = VectorCore::new(
            &graph_env,
            &mut wtxn,
            HNSWConfig::new(
                config.vector_config.m,
                config.vector_config.ef_construction,
                config.vector_config.ef_search,
            ),
        )?;

        wtxn.commit()?;
        Ok(Self {
            graph_env,
            nodes_db,
            edges_db,
            out_edges_db,
            in_edges_db,
            secondary_indices,
            vectors,
        })
    }

    pub fn get_random_node(&self, txn: &RoTxn) -> Result<Node, GraphError> {
        match self.nodes_db.first(&txn)? {
            Some((_, data)) => Ok(bincode::deserialize(data)?),
            None => Err(GraphError::NodeNotFound),
        }
    }

    // todo look into using a shorter hash for space efficiency
    // #[inline(always)]
    // pub fn hash_label(label: &str) -> [u8; 4] {
    //     let mut hash = twox_hash::XxHash32::with_seed(0);
    //     hash.write(label.as_bytes());
    //     hash.finish_32().to_le_bytes()
    // }

    // #[inline(always)]
    // pub fn node_key(id: &u128) -> [u8; 16] {
    //     id.to_be_bytes()
    // }

    // #[inline(always)]
    // pub fn edge_key(id: &u128) -> [u8; 16] {
    //     id.to_be_bytes()
    // }

    #[inline(always)]
    pub fn node_key(id: &u128) -> &u128 {
        id
    }

    #[inline(always)]
    pub fn edge_key(id: &u128) -> &u128 {
        id
    }

    #[inline(always)]
    pub fn node_label_key(label: &[u8; 4], id: &u128) -> [u8; 20] {
        let mut key = [0u8; 20];
        key[0..4].copy_from_slice(label);
        key[4..20].copy_from_slice(&id.to_be_bytes());
        key
    }

    #[inline(always)]
    pub fn edge_label_key(label: &[u8; 4], id: &u128) -> [u8; 20] {
        let mut key = [0u8; 20];
        key[0..4].copy_from_slice(label);
        key[4..20].copy_from_slice(&id.to_be_bytes());
        key
    }

    // key = from-node(16) | label-id(4)                 ← 20 B
    // val = to-node(16)  | edge-id(16)                  ← 32 B (DUPFIXED)
    #[inline(always)]
    pub fn out_edge_key(from_node_id: &u128, label: &[u8; 4]) -> [u8; 20] {
        let mut key = [0u8; 20];
        key[0..16].copy_from_slice(&from_node_id.to_be_bytes());
        key[16..20].copy_from_slice(label);
        key
    }

    #[inline(always)]
    pub fn in_edge_key(to_node_id: &u128, label: &[u8; 4]) -> [u8; 20] {
        let mut key = [0u8; 20];
        key[0..16].copy_from_slice(&to_node_id.to_be_bytes());
        key[16..20].copy_from_slice(label);
        key
    }

    #[inline(always)]
    pub fn pack_edge_data(node_id: &u128, edge_id: &u128) -> [u8; 32] {
        let mut key = [0u8; 32];
        key[0..16].copy_from_slice(&edge_id.to_be_bytes());
        key[16..32].copy_from_slice(&node_id.to_be_bytes());
        key
    }

    #[inline(always)]
    pub fn unpack_adj_edge_data(data: &[u8]) -> Result<(u128, u128), GraphError> {
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
        Ok((node_id, edge_id))
    }

    pub fn get_u128_from_bytes(bytes: &[u8]) -> Result<u128, GraphError> {
        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        let res = u128::from_be_bytes(arr);
        Ok(res)
    }
}

impl DBMethods for HelixGraphStorage {
    fn create_secondary_index(&mut self, name: &str) -> Result<(), GraphError> {
        let mut wtxn = self.graph_env.write_txn()?;
        let db = self.graph_env.create_database(&mut wtxn, Some(name))?;
        wtxn.commit()?;
        self.secondary_indices.insert(name.to_string(), db);
        Ok(())
    }

    fn drop_secondary_index(&mut self, name: &str) -> Result<(), GraphError> {
        let mut wtxn = self.graph_env.write_txn()?;
        let db = self
            .secondary_indices
            .get(name)
            .ok_or(GraphError::New(format!(
                "Secondary Index {} not found",
                name
            )))?;
        db.clear(&mut wtxn)?;
        wtxn.commit()?;
        self.secondary_indices.remove(name);
        Ok(())
    }
}

impl BasicStorageMethods for HelixGraphStorage {
    #[inline(always)]
    fn get_temp_node<'a>(&self, txn: &'a RoTxn, id: &u128) -> Result<&'a [u8], GraphError> {
        match self.nodes_db.get(&txn, Self::node_key(id))? {
            Some(data) => Ok(data),
            None => Err(GraphError::NodeNotFound),
        }
    }

    #[inline(always)]
    fn get_temp_edge<'a>(&self, txn: &'a RoTxn, id: &u128) -> Result<&'a [u8], GraphError> {
        match self.edges_db.get(&txn, Self::edge_key(id))? {
            Some(data) => Ok(data),
            None => Err(GraphError::EdgeNotFound),
        }
    }
}

impl StorageMethods for HelixGraphStorage {
    #[inline(always)]
    fn check_exists(&self, txn: &RoTxn, id: &u128) -> Result<bool, GraphError> {
        // let txn = txn.read_txn();
        let exists = self.nodes_db.get(txn, Self::node_key(id))?.is_some();
        Ok(exists)
    }

    #[inline(always)]
    fn get_node(&self, txn: &RoTxn, id: &u128) -> Result<Node, GraphError> {
        let node = match self.nodes_db.get(txn, Self::node_key(id))? {
            Some(data) => data,
            None => return Err(GraphError::NodeNotFound),
        };
        let node: Node = match Node::decode_node(&node, *id) {
            Ok(node) => node,
            Err(e) => return Err(e),
        };
        Ok(node)
    }

    #[inline(always)]
    fn get_edge(&self, txn: &RoTxn, id: &u128) -> Result<Edge, GraphError> {
        let edge = match self.edges_db.get(txn, Self::edge_key(id))? {
            Some(data) => data,
            None => return Err(GraphError::EdgeNotFound),
        };
        let edge: Edge = match Edge::decode_edge(&edge, *id) {
            Ok(edge) => edge,
            Err(e) => return Err(e),
        };
        Ok(edge)
    }

    // LEAVE FOR NOW
    // fn get_node_by_secondary_index(
    //     &self,
    //     txn: &RoTxn,
    //     index: &str,
    //     key: &Value,
    // ) -> Result<Node, GraphError> {
    //     let db = self
    //         .secondary_indices
    //         .get(index)
    //         .ok_or(GraphError::New(format!(
    //             "Secondary Index {} not found",
    //             index
    //         )))?;
    //     let node_id = db
    //         .get(txn, &bincode::serialize(key)?)?
    //         .ok_or(GraphError::NodeNotFound)?;
    //     let node_id = Self::get_u128_from_bytes(&node_id)?;
    //     self.get_node(txn, &node_id)
    // }

    fn drop_node(&self, txn: &mut RwTxn, id: &u128) -> Result<(), GraphError> {
        // Get node to get its label
        //let node = self.get_node(txn, id)?;

        // Delete outgoing edges
        let out_edges = {
            let iter = self.out_edges_db.get_duplicates(&txn, &id.to_be_bytes())?;
            match iter {
                Some(iter) => {
                    let capacity = match iter.size_hint() {
                        (_, Some(upper)) => upper,
                        (lower, None) => lower,
                    };
                    let mut out_edges = Vec::with_capacity(capacity);

                    for result in iter {
                        let (_, value) = result?;
                        let (edge_id, _) = Self::unpack_adj_edge_data(&value)?;

                        if let Some(edge_data) =
                            &self.edges_db.get(&txn, &Self::edge_key(&edge_id))?
                        {
                            let edge: Edge = bincode::deserialize(edge_data)?;
                            out_edges.push(edge);
                        }
                    }
                    out_edges
                }
                None => {
                    return Ok(());
                }
            }
        };

        // Delete incoming edges

        let in_edges = {
            let iter = self.in_edges_db.get_duplicates(&txn, &id.to_be_bytes())?;
            match iter {
                Some(iter) => {
                    let capacity = match iter.size_hint() {
                        (_, Some(c)) => c,
                        (c, None) => c,
                    };
                    let mut in_edges = Vec::with_capacity(capacity);

                    for result in iter {
                        let (_, value) = result?;
                        let (edge_id, _) = Self::unpack_adj_edge_data(&value)?;

                        if let Some(edge_data) =
                            self.edges_db.get(&txn, &Self::edge_key(&edge_id))?
                        {
                            let edge: Edge = bincode::deserialize(edge_data)?;
                            in_edges.push(edge);
                        }
                    }
                    in_edges
                }
                None => {
                    return Ok(());
                }
            }
        };

        // Delete all related data
        for edge in out_edges.iter().chain(in_edges.iter()) {
            // Delete edge data
            let label_hash = hash_label(&edge.label, None);
            self.edges_db.delete(txn, &Self::edge_key(&edge.id))?;
            self.out_edges_db
                .delete(txn, &Self::out_edge_key(&edge.from_node, &label_hash))?;
            self.in_edges_db
                .delete(txn, &Self::in_edge_key(&edge.to_node, &label_hash))?;
        }

        // Delete node data and label
        self.nodes_db.delete(txn, Self::node_key(id))?;

        Ok(())
    }

    fn drop_edge(&self, txn: &mut RwTxn, edge_id: &u128) -> Result<(), GraphError> {
        // Get edge data first
        let edge_data = match self.edges_db.get(&txn, &Self::edge_key(edge_id))? {
            Some(data) => data,
            None => return Err(GraphError::EdgeNotFound),
        };
        let edge: Edge = bincode::deserialize(edge_data)?;
        let label_hash = hash_label(&edge.label, None);
        // Delete all edge-related data
        self.edges_db.delete(txn, &Self::edge_key(edge_id))?;
        self.out_edges_db
            .delete(txn, &Self::out_edge_key(&edge.from_node, &label_hash))?;
        self.in_edges_db
            .delete(txn, &Self::in_edge_key(&edge.to_node, &label_hash))?;

        Ok(())
    }
}
