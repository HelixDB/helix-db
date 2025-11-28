#[cfg(feature = "lmdb")]
pub mod graph_visualization;
#[cfg(feature = "lmdb")]
pub mod metadata;
pub mod storage_methods;
#[cfg(feature = "lmdb")]
pub mod storage_migration;
pub mod txn;
pub mod version_info;

use crate::helix_engine::storage_core::storage_methods::{DBMethods, StorageMethods};
use crate::{
    helix_engine::{
        bm25::HBM25Config,
        storage_core::version_info::VersionInfo,
        traversal_core::config::Config,
        types::GraphError,
        vector_core::{HNSW, HNSWConfig, VectorCore},
    },
    utils::{
        items::{Edge, Node},
        label_hash::hash_label,
    },
};

use std::{
    collections::{HashMap, HashSet},
    fs,
};

#[cfg(feature = "lmdb")]
pub use lmdb::*;
#[cfg(feature = "rocks")]
pub use rocks::*;
#[cfg(feature = "slate")]
pub use slate::*;

pub type NodeId = u128;
pub type EdgeId = u128;

pub struct StorageConfig {
    pub schema: Option<String>,
    pub graphvis_node_label: Option<String>,
    pub embedding_model: Option<String>,
}

impl StorageConfig {
    pub fn new(
        schema: Option<String>,
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
#[cfg(feature = "lmdb")]
pub mod lmdb {

    use super::*;
    use heed3::{
        Database, DatabaseFlags, Env, EnvOpenOptions, RoTxn, RwTxn, byteorder::BE, types::*,
    };
    pub struct HelixGraphStorage {
        pub graph_env: Env,

        pub nodes_db: Database<U128<BE>, Bytes>,
        pub edges_db: Database<U128<BE>, Bytes>,
        pub out_edges_db: Database<Bytes, Bytes>,
        pub in_edges_db: Database<Bytes, Bytes>,
        pub secondary_indices: HashMap<String, Database<Bytes, U128<BE>>>,
        pub vectors: VectorCore,
        pub bm25: Option<HBM25Config>,
        pub metadata_db: Database<Bytes, Bytes>,
        pub version_info: VersionInfo,

        pub storage_config: StorageConfig,
    }

    pub type Txn<'db> = heed3::RoTxn<'db>;

    impl HelixGraphStorage {
        // database names for different stores
        const DB_NODES: &str = "nodes"; // for node data (n:)
        const DB_EDGES: &str = "edges"; // for edge data (e:)
        const DB_OUT_EDGES: &str = "out_edges"; // for outgoing edge indices (o:)
        const DB_IN_EDGES: &str = "in_edges"; // for incoming edge indices (i:)
        const DB_STORAGE_METADATA: &str = "storage_metadata"; // for storage metadata key/value pairs

        pub fn new(
            path: &str,
            config: Config,
            version_info: VersionInfo,
        ) -> Result<HelixGraphStorage, GraphError> {
            fs::create_dir_all(path)?;

            let db_size = if config.db_max_size_gb.unwrap_or(100) >= 9999 {
                9998
            } else {
                config.db_max_size_gb.unwrap_or(100)
            };

            let graph_env = unsafe {
                EnvOpenOptions::new()
                    .map_size(db_size * 1024 * 1024 * 1024)
                    .max_dbs(200)
                    .max_readers(200)
                    .open(std::path::Path::new(path))?
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
                .name(Self::DB_NODES)
                .create(&mut wtxn)?;

            // Edges: [edge_id]->[bytes array of edge data]
            //        [16 bytes]->[dynamic]
            let edges_db = graph_env
                .database_options()
                .types::<U128<BE>, Bytes>()
                .name(Self::DB_EDGES)
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
                .name(Self::DB_OUT_EDGES)
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
                .name(Self::DB_IN_EDGES)
                .create(&mut wtxn)?;

            let metadata_db: Database<Bytes, Bytes> = graph_env
                .database_options()
                .types::<Bytes, Bytes>()
                .name(Self::DB_STORAGE_METADATA)
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
                config.schema,
                config.graphvis_node_label,
                config.embedding_model,
            );

            wtxn.commit()?;

            let mut storage = Self {
                graph_env,
                nodes_db,
                edges_db,
                out_edges_db,
                in_edges_db,
                secondary_indices,
                vectors,
                bm25,
                metadata_db,
                storage_config,
                version_info,
            };

            storage_migration::migrate(&mut storage)?;

            Ok(storage)
        }

        /// Used because in the case the key changes in the future.
        /// Believed to not introduce any overhead being inline and using a reference.
        #[must_use]
        #[inline(always)]
        pub fn node_key(id: u128) -> u128 {
            id
        }

        /// Used because in the case the key changes in the future.
        /// Believed to not introduce any overhead being inline and using a reference.
        #[must_use]
        #[inline(always)]
        pub fn edge_key(id: u128) -> u128 {
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
        pub fn out_edge_key(from_node_id: u128, label: &[u8; 4]) -> [u8; 20] {
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
        pub fn in_edge_key(to_node_id: u128, label: &[u8; 4]) -> [u8; 20] {
            let mut key = [0u8; 20];
            key[0..16].copy_from_slice(&to_node_id.to_be_bytes());
            key[16..20].copy_from_slice(label);
            key
        }

        /// Packs the edge data into a 32 byte array.
        ///
        /// data = `edge-id(16)` | `node-id(16)`                 ← 32 B (DUPFIXED)
        #[inline(always)]
        pub fn pack_edge_data(edge_id: u128, node_id: u128) -> [u8; 32] {
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
        #[inline]
        fn get_node<'arena>(
            &self,
            txn: &RoTxn,
            id: u128,
            arena: &'arena bumpalo::Bump,
        ) -> Result<Node<'arena>, GraphError> {
            let node = match self.nodes_db.get(txn, &Self::node_key(id))? {
                Some(data) => data,
                None => return Err(GraphError::NodeNotFound),
            };
            let node: Node = Node::from_bincode_bytes(id, node, arena)?;
            let node = self.version_info.upgrade_to_node_latest(node);
            Ok(node)
        }

        #[inline]
        fn get_edge<'arena>(
            &self,
            txn: &RoTxn,
            id: u128,
            arena: &'arena bumpalo::Bump,
        ) -> Result<Edge<'arena>, GraphError> {
            let edge = match self.edges_db.get(txn, &Self::edge_key(id))? {
                Some(data) => data,
                None => return Err(GraphError::EdgeNotFound),
            };
            let edge: Edge = Edge::from_bincode_bytes(id, edge, arena)?;
            Ok(self.version_info.upgrade_to_edge_latest(edge))
        }

        fn drop_node(&self, txn: &mut RwTxn, id: u128) -> Result<(), GraphError> {
            let arena = bumpalo::Bump::new();
            // Get node to get its label
            //let node = self.get_node(txn, id)?;
            let mut edges = HashSet::new();
            let mut out_edges = HashSet::new();
            let mut in_edges = HashSet::new();

            let mut other_out_edges = Vec::new();
            let mut other_in_edges = Vec::new();
            // Delete outgoing edges

            let iter = self.out_edges_db.prefix_iter(txn, &id.to_be_bytes())?;

            for result in iter {
                let (key, value) = result?;
                assert_eq!(key.len(), 20);
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (edge_id, to_node_id) = Self::unpack_adj_edge_data(value)?;
                edges.insert(edge_id);
                out_edges.insert(label);
                other_in_edges.push((to_node_id, label, edge_id));
            }

            // Delete incoming edges

            let iter = self.in_edges_db.prefix_iter(txn, &id.to_be_bytes())?;

            for result in iter {
                let (key, value) = result?;
                assert_eq!(key.len(), 20);
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (edge_id, from_node_id) = Self::unpack_adj_edge_data(value)?;
                in_edges.insert(label);
                edges.insert(edge_id);
                other_out_edges.push((from_node_id, label, edge_id));
            }

            // println!("In edges: {}", in_edges.len());

            // println!("Deleting edges: {}", );
            // Delete all related data
            for edge in edges {
                self.edges_db.delete(txn, &Self::edge_key(edge))?;
            }
            for label_bytes in out_edges.iter() {
                self.out_edges_db
                    .delete(txn, &Self::out_edge_key(id, label_bytes))?;
            }
            for label_bytes in in_edges.iter() {
                self.in_edges_db
                    .delete(txn, &Self::in_edge_key(id, label_bytes))?;
            }

            for (other_node_id, label_bytes, edge_id) in other_out_edges.iter() {
                self.out_edges_db.delete_one_duplicate(
                    txn,
                    &Self::out_edge_key(*other_node_id, label_bytes),
                    &Self::pack_edge_data(*edge_id, id),
                )?;
            }
            for (other_node_id, label_bytes, edge_id) in other_in_edges.iter() {
                self.in_edges_db.delete_one_duplicate(
                    txn,
                    &Self::in_edge_key(*other_node_id, label_bytes),
                    &Self::pack_edge_data(*edge_id, id),
                )?;
            }

            // delete secondary indices
            let node = self.get_node(txn, id, &arena)?;
            for (index_name, db) in &self.secondary_indices {
                // Use get_property like we do when adding, to handle id, label, and regular properties consistently
                match node.get_property(index_name) {
                    Some(value) => match bincode::serialize(value) {
                        Ok(serialized) => {
                            if let Err(e) = db.delete_one_duplicate(txn, &serialized, &node.id) {
                                return Err(GraphError::from(e));
                            }
                        }
                        Err(e) => return Err(GraphError::from(e)),
                    },
                    None => {
                        // Property not found - this is expected for some indices
                        // Continue to next index
                    }
                }
            }

            // Delete node data and label
            self.nodes_db.delete(txn, &Self::node_key(id))?;

            Ok(())
        }

        fn drop_edge(&self, txn: &mut RwTxn, edge_id: u128) -> Result<(), GraphError> {
            let arena = bumpalo::Bump::new();
            // Get edge data first
            let edge_data = match self.edges_db.get(txn, &Self::edge_key(edge_id))? {
                Some(data) => data,
                None => return Err(GraphError::EdgeNotFound),
            };
            let edge: Edge = Edge::from_bincode_bytes(edge_id, edge_data, &arena)?;
            let label_hash = hash_label(edge.label, None);
            let out_edge_value = Self::pack_edge_data(edge_id, edge.to_node);
            let in_edge_value = Self::pack_edge_data(edge_id, edge.from_node);
            // Delete all edge-related data
            self.edges_db.delete(txn, &Self::edge_key(edge_id))?;
            self.out_edges_db.delete_one_duplicate(
                txn,
                &Self::out_edge_key(edge.from_node, &label_hash),
                &out_edge_value,
            )?;
            self.in_edges_db.delete_one_duplicate(
                txn,
                &Self::in_edge_key(edge.to_node, &label_hash),
                &in_edge_value,
            )?;

            Ok(())
        }

        fn drop_vector(&self, txn: &mut RwTxn, id: u128) -> Result<(), GraphError> {
            let arena = bumpalo::Bump::new();
            let mut edges = HashSet::new();
            let mut out_edges = HashSet::new();
            let mut in_edges = HashSet::new();

            let mut other_out_edges = Vec::new();
            let mut other_in_edges = Vec::new();
            // Delete outgoing edges

            let iter = self.out_edges_db.prefix_iter(txn, &id.to_be_bytes())?;

            for result in iter {
                let (key, value) = result?;
                assert_eq!(key.len(), 20);
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (edge_id, to_node_id) = Self::unpack_adj_edge_data(value)?;
                edges.insert(edge_id);
                out_edges.insert(label);
                other_in_edges.push((to_node_id, label, edge_id));
            }

            // Delete incoming edges

            let iter = self.in_edges_db.prefix_iter(txn, &id.to_be_bytes())?;

            for result in iter {
                let (key, value) = result?;
                assert_eq!(key.len(), 20);
                let mut label = [0u8; 4];
                label.copy_from_slice(&key[16..20]);
                let (edge_id, from_node_id) = Self::unpack_adj_edge_data(value)?;
                in_edges.insert(label);
                edges.insert(edge_id);
                other_out_edges.push((from_node_id, label, edge_id));
            }

            // println!("In edges: {}", in_edges.len());

            // println!("Deleting edges: {}", );
            // Delete all related data
            for edge in edges {
                self.edges_db.delete(txn, &Self::edge_key(edge))?;
            }
            for label_bytes in out_edges.iter() {
                self.out_edges_db
                    .delete(txn, &Self::out_edge_key(id, label_bytes))?;
            }
            for label_bytes in in_edges.iter() {
                self.in_edges_db
                    .delete(txn, &Self::in_edge_key(id, label_bytes))?;
            }

            for (other_node_id, label_bytes, edge_id) in other_out_edges.iter() {
                self.out_edges_db.delete_one_duplicate(
                    txn,
                    &Self::out_edge_key(*other_node_id, label_bytes),
                    &Self::pack_edge_data(*edge_id, id),
                )?;
            }
            for (other_node_id, label_bytes, edge_id) in other_in_edges.iter() {
                self.in_edges_db.delete_one_duplicate(
                    txn,
                    &Self::in_edge_key(*other_node_id, label_bytes),
                    &Self::pack_edge_data(*edge_id, id),
                )?;
            }

            // Delete vector data
            self.vectors.delete(txn, id, &arena)?;

            Ok(())
        }
    }
}

#[cfg(feature = "rocks")]
pub mod rocks {

    use super::*;
    use std::sync::Arc;
    pub struct HelixGraphStorage {
        pub graph_env: Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>>,
        pub secondary_indices: HashMap<String, String>, // Store CF names instead of handles
        pub vectors: VectorCore,
        pub bm25: Option<HBM25Config>,
        pub version_info: VersionInfo,
        pub storage_config: StorageConfig,
    }

    pub type Txn<'db> = rocksdb::Transaction<'db, rocksdb::TransactionDB>;

    pub fn default_helix_rocksdb_options() -> rocksdb::Options {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Optimize for concurrent writes
        db_opts.set_max_background_jobs(6);
        db_opts.set_write_buffer_size(128 * 1024 * 1024); // 128MB
        db_opts.set_max_write_buffer_number(4);
        db_opts.set_allow_concurrent_memtable_write(true);
        db_opts.set_enable_write_thread_adaptive_yield(true);
        db_opts.increase_parallelism(num_cpus::get() as i32);

        // Compression
        db_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        db_opts
    }

    #[cfg(feature = "rocks")]
    impl HelixGraphStorage {
        // Helper methods to get column family handles on-demand
        #[inline(always)]
        pub fn cf_nodes(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
            self.graph_env.cf_handle("nodes").unwrap()
        }

        #[inline(always)]
        pub fn cf_edges(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
            self.graph_env.cf_handle("edges").unwrap()
        }

        #[inline(always)]
        pub fn cf_out_edges(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
            self.graph_env.cf_handle("out_edges").unwrap()
        }

        #[inline(always)]
        pub fn cf_in_edges(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
            self.graph_env.cf_handle("in_edges").unwrap()
        }

        #[inline(always)]
        pub fn cf_metadata(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
            self.graph_env.cf_handle("metadata").unwrap()
        }

        /// Create a read transaction (snapshot)
        pub fn read_txn(
            &self,
        ) -> Result<rocksdb::Transaction<'_, rocksdb::TransactionDB>, GraphError> {
            Ok(self.graph_env.transaction())
        }

        /// Create a write transaction
        pub fn write_txn(
            &self,
        ) -> Result<rocksdb::Transaction<'_, rocksdb::TransactionDB>, GraphError> {
            Ok(self.graph_env.transaction())
        }

        pub fn new(
            path: &str,
            config: Config,
            version_info: VersionInfo,
        ) -> Result<HelixGraphStorage, GraphError> {
            use std::sync::Arc;

            fs::create_dir_all(path)?;

            // Base options
            let db_opts = default_helix_rocksdb_options();

            // Set up column families
            let mut cf_descriptors = vec![
                rocksdb::ColumnFamilyDescriptor::new("nodes", Self::nodes_cf_options()),
                rocksdb::ColumnFamilyDescriptor::new("edges", Self::edges_cf_options()),
                rocksdb::ColumnFamilyDescriptor::new("out_edges", Self::edges_index_cf_options()),
                rocksdb::ColumnFamilyDescriptor::new("in_edges", Self::edges_index_cf_options()),
                rocksdb::ColumnFamilyDescriptor::new("metadata", rocksdb::Options::default()),
            ];

            let vector_cf_descriptors = vec![
                rocksdb::ColumnFamilyDescriptor::new("vectors", VectorCore::vector_cf_options()),
                rocksdb::ColumnFamilyDescriptor::new(
                    "vector_data",
                    VectorCore::vector_properties_cf_options(),
                ),
                rocksdb::ColumnFamilyDescriptor::new(
                    "hnsw_edges",
                    VectorCore::vector_edges_cf_options(),
                ),
                rocksdb::ColumnFamilyDescriptor::new("ep", rocksdb::Options::default()),
            ];
            cf_descriptors.extend(vector_cf_descriptors);

            let bm25_cf_descriptors = vec![
                rocksdb::ColumnFamilyDescriptor::new("inverted_index", rocksdb::Options::default()),
                rocksdb::ColumnFamilyDescriptor::new("doc_lengths", rocksdb::Options::default()),
                rocksdb::ColumnFamilyDescriptor::new(
                    "term_frequencies",
                    rocksdb::Options::default(),
                ),
                rocksdb::ColumnFamilyDescriptor::new("bm25_metadata", rocksdb::Options::default()),
            ];
            cf_descriptors.extend(bm25_cf_descriptors);

            // Store secondary index names (not handles)
            let mut secondary_indices = HashMap::new();
            if let Some(indexes) = config.get_graph_config().secondary_indices.as_ref() {
                for index in indexes {
                    // let cf_name = format!("idx_{}", index);
                    secondary_indices.insert(index.to_string(), index.to_string());
                }
            }
            cf_descriptors.extend(
                secondary_indices
                    .values()
                    .map(|cf_name| {
                        rocksdb::ColumnFamilyDescriptor::new(cf_name, rocksdb::Options::default())
                    })
                    .collect::<Vec<_>>(),
            );
            // TODO: TransactionDB tuning
            let txn_db_opts = rocksdb::TransactionDBOptions::new();

            // Open database with optimistic transactions
            let db = Arc::new(
                rocksdb::TransactionDB::<rocksdb::MultiThreaded>::open_cf_descriptors(
                    &db_opts,
                    &txn_db_opts,
                    path,
                    cf_descriptors,
                )
                .unwrap(),
            );

            // Initialize vector storage
            let vector_config = config.get_vector_config();
            let vectors = VectorCore::new(
                Arc::clone(&db),
                HNSWConfig::new(
                    vector_config.m,
                    vector_config.ef_construction,
                    vector_config.ef_search,
                ),
            )?;

            let bm25 = config
                .get_bm25()
                .then(|| HBM25Config::new(Arc::clone(&db)))
                .transpose()?;

            let storage_config = StorageConfig::new(
                config.schema,
                config.graphvis_node_label,
                config.embedding_model,
            );

            let storage = Self {
                graph_env: db,
                secondary_indices,
                vectors,
                bm25,
                storage_config,
                version_info,
            };

            // TODO: Implement RocksDB-specific migration if needed
            // storage_migration is LMDB-specific for now

            Ok(storage)
        }

        pub fn nodes_cf_options() -> rocksdb::Options {
            let mut opts = rocksdb::Options::default();
            opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(16)); // u128 = 16 bytes
            opts
        }

        pub fn edges_cf_options() -> rocksdb::Options {
            let mut opts = rocksdb::Options::default();
            opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(16)); // u128 = 16 bytes
            opts
        }

        pub fn edges_index_cf_options() -> rocksdb::Options {
            let mut opts = rocksdb::Options::default();
            // For DUP_SORT replacement: use prefix for node_id+label (24 bytes)
            opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(20));
            opts
        }

        // TODO CHANGE THIS
        pub fn secondary_index_cf_options() -> rocksdb::Options {
            let opts = rocksdb::Options::default();
            // opts.set_merge_operator_associative("append", Self::merge_append);
            opts
        }

        // // Merge operator for secondary indices (replaces DUP_SORT)
        // fn merge_append(
        //     _key: &[u8],
        //     existing: Option<&[u8]>,
        //     operands: &rocksdb::MergeOperands,
        // ) -> Option<Vec<u8>> {
        //     let mut result = existing.map(|v| v.to_vec()).unwrap_or_default();
        //     for op in operands {
        //         result.extend_from_slice(op);
        //     }
        //     Some(result)
        // }

        pub fn get_secondary_index_cf_handle(
            &self,
            name: &str,
        ) -> Option<Arc<rocksdb::BoundColumnFamily<'_>>> {
            self.graph_env.cf_handle(name)
        }

        /// Used because in the case the key changes in the future.
        /// Believed to not introduce any overhead being inline and using a reference.
        #[must_use]
        #[inline(always)]
        pub fn node_key(id: u128) -> [u8; 16] {
            id.to_be_bytes()
        }

        /// Used because in the case the key changes in the future.
        /// Believed to not introduce any overhead being inline and using a reference.
        #[must_use]
        #[inline(always)]
        pub fn edge_key(id: u128) -> [u8; 16] {
            id.to_be_bytes()
        }

        /// Out edge key generator. Creates a 20 byte array and copies in the node id and 4 byte label.
        ///
        /// key = `from-node(16)` | `label-id(4)`                 ← 20 B
        ///
        /// The generated out edge key will remain the same for the same from_node_id and label.
        /// To save space, the key is only stored once,
        /// with the values being stored in a sorted sub-tree, with this key being the root.
        #[inline(always)]
        pub fn out_edge_key(
            from_node_id: u128,
            label: &[u8; 4],
            to_node_id: u128,
            edge_id: u128,
        ) -> [u8; 52] {
            let mut key = [0u8; 52];
            key[0..16].copy_from_slice(&from_node_id.to_be_bytes());
            key[16..20].copy_from_slice(label);
            key[20..36].copy_from_slice(&to_node_id.to_be_bytes());
            key[36..52].copy_from_slice(&edge_id.to_be_bytes());
            key
        }

        #[inline(always)]
        pub fn out_edge_key_prefix(from_node_id: u128, label: &[u8; 4]) -> [u8; 20] {
            let mut key = [0u8; 20];
            key[0..16].copy_from_slice(&from_node_id.to_be_bytes());
            key[16..20].copy_from_slice(label);
            key
        }

        /// In edge key prefix generator. Creates a 20 byte array with the to_node_id and label.
        /// Used for prefix iteration in RocksDB.
        ///
        /// key = `to-node(16)` | `label-id(4)`                 ← 20 B
        #[inline(always)]
        pub fn in_edge_key_prefix(to_node_id: u128, label: &[u8; 4]) -> [u8; 20] {
            let mut key = [0u8; 20];
            key[0..16].copy_from_slice(&to_node_id.to_be_bytes());
            key[16..20].copy_from_slice(label);
            key
        }

        /// In edge key generator. Creates a 36 byte array with to_node, label, and from_node.
        ///
        /// key = `to-node(16)` | `label-id(4)` | `from-node(16)`    ← 36 B
        ///
        /// The generated in edge key will be unique for each edge.
        #[inline(always)]
        pub fn in_edge_key(
            to_node_id: u128,
            label: &[u8; 4],
            from_node_id: u128,
            edge_id: u128,
        ) -> [u8; 52] {
            let mut key = [0u8; 52];
            key[0..16].copy_from_slice(&to_node_id.to_be_bytes());
            key[16..20].copy_from_slice(label);
            key[20..36].copy_from_slice(&from_node_id.to_be_bytes());
            key[36..52].copy_from_slice(&edge_id.to_be_bytes());
            key
        }

        /// Packs the edge data into a 32 byte array.x
        ///
        /// data = `edge-id(16)` | `node-id(16)`                 ← 32 B (DUPFIXED)
        #[inline(always)]
        pub fn pack_edge_data(node_id: u128) -> [u8; 16] {
            let mut key = [0u8; 16];
            key[0..16].copy_from_slice(&node_id.to_be_bytes());
            key
        }

        /// Unpacks the 32 byte array into an (edge_id, node_id) tuple of u128s.
        ///
        /// Returns (edge_id, node_id)
        #[inline(always)]
        // Uses Type Aliases for clarity
        pub fn unpack_adj_edge_data(data: &[u8]) -> Result<NodeId, GraphError> {
            let node_id = u128::from_be_bytes(
                data[0..16]
                    .try_into()
                    .map_err(|_| GraphError::SliceLengthError)?,
            );
            Ok(node_id)
        }

        #[inline(always)]
        pub fn unpack_adj_edge_key(
            data: &[u8],
        ) -> Result<(NodeId, [u8; 4], NodeId, EdgeId), GraphError> {
            let node_id = u128::from_be_bytes(
                data[0..16]
                    .try_into()
                    .map_err(|_| GraphError::SliceLengthError)?,
            );
            let label = data[16..20]
                .try_into()
                .map_err(|_| GraphError::SliceLengthError)?;
            let node_id2 = u128::from_be_bytes(
                data[20..36]
                    .try_into()
                    .map_err(|_| GraphError::SliceLengthError)?,
            );
            let edge_id = EdgeId::from_be_bytes(
                data[36..52]
                    .try_into()
                    .map_err(|_| GraphError::SliceLengthError)?,
            );
            Ok((node_id, label, node_id2, edge_id))
        }

        /// clears buffer then writes secondary index key
        #[inline(always)]
        pub fn secondary_index_key<'a>(
            buf: &'a mut bumpalo::collections::Vec<u8>,
            key: &[u8],
            node_id: u128,
        ) -> &'a mut [u8] {
            buf.clear();
            buf.extend_from_slice(key);
            buf.extend_from_slice(&node_id.to_be_bytes());
            buf
        }
    }

    impl DBMethods for HelixGraphStorage {
        /// Creates a secondary index lmdb db (table) for a given index name
        fn create_secondary_index(&mut self, _name: &str) -> Result<(), GraphError> {
            unimplemented!(
                "cannot be implemented for rocks db due to table having to be declared at creation time"
            )
        }

        /// Drops a secondary index lmdb db (table) for a given index name
        fn drop_secondary_index(&mut self, name: &str) -> Result<(), GraphError> {
            self.graph_env.drop_cf(name)?;
            self.secondary_indices.remove(name);
            Ok(())
        }
    }

    impl StorageMethods for HelixGraphStorage {
        #[inline]
        fn get_node<'db, 'arena>(
            &self,
            txn: &Txn<'db>,
            id: u128,
            arena: &'arena bumpalo::Bump,
        ) -> Result<Node<'arena>, GraphError> {
            let cf = self.cf_nodes();
            let node = match txn.get_pinned_cf(&cf, Self::node_key(id)).unwrap() {
                Some(data) => data,
                None => return Err(GraphError::NodeNotFound),
            };
            let node: Node = Node::from_bincode_bytes(id, &node, arena)?;
            let node = self.version_info.upgrade_to_node_latest(node);
            Ok(node)
        }

        #[inline]
        fn get_edge<'db, 'arena>(
            &self,
            txn: &Txn<'db>,
            id: u128,
            arena: &'arena bumpalo::Bump,
        ) -> Result<Edge<'arena>, GraphError> {
            let cf = self.cf_edges();
            let edge = match txn.get_pinned_cf(&cf, Self::edge_key(id)).unwrap() {
                Some(data) => data,
                None => return Err(GraphError::EdgeNotFound),
            };
            let edge: Edge = Edge::from_bincode_bytes(id, &edge, arena)?;
            Ok(self.version_info.upgrade_to_edge_latest(edge))
        }

        fn drop_node<'db>(&self, txn: &Txn<'db>, id: u128) -> Result<(), GraphError> {
            use crate::helix_engine::rocks_utils::RocksUtils;

            let arena = bumpalo::Bump::new();
            let mut edges = HashSet::new();
            let mut out_edges = HashSet::new();
            let mut in_edges = HashSet::new();

            let mut other_out_edges = Vec::new();
            let mut other_in_edges = Vec::new();

            let cf_out_edges = self.cf_out_edges();
            let cf_in_edges = self.cf_in_edges();
            let cf_edges = self.cf_edges();

            // Delete outgoing edges
            let mut iter = txn.raw_prefix_iter(&cf_out_edges, &id.to_be_bytes());

            while let Some(key) = iter.key() {
                assert_eq!(key.len(), 52);
                let (_, label, to_node_id, edge_id) = Self::unpack_adj_edge_key(key)?;
                edges.insert(edge_id);
                out_edges.insert((label, to_node_id, edge_id));
                other_in_edges.push((to_node_id, label, edge_id));
                iter.next();
            }
            iter.status().map_err(GraphError::from)?;

            // Delete incoming edges
            let mut iter = txn.raw_prefix_iter(&cf_in_edges, &id.to_be_bytes());

            while let Some(key) = iter.key() {
                assert_eq!(key.len(), 52);
                let (_, label, from_node_id, edge_id) = Self::unpack_adj_edge_key(key)?;
                edges.insert(edge_id);
                in_edges.insert((label, from_node_id, edge_id));
                other_out_edges.push((from_node_id, label, edge_id));
                iter.next();
            }
            iter.status().map_err(GraphError::from)?;

            // Delete all related data
            for edge in edges {
                txn.delete_cf(&cf_edges, Self::edge_key(edge))?;
            }
            for (label_bytes, to_node_id, edge_id) in out_edges.iter() {
                txn.delete_cf(
                    &cf_out_edges,
                    Self::out_edge_key(id, label_bytes, *to_node_id, *edge_id),
                )?;
            }
            for (label_bytes, from_node_id, edge_id) in in_edges.iter() {
                txn.delete_cf(
                    &cf_in_edges,
                    Self::in_edge_key(id, label_bytes, *from_node_id, *edge_id),
                )?;
            }

            for (other_node_id, label_bytes, edge_id) in other_out_edges.iter() {
                txn.delete_cf(
                    &cf_out_edges,
                    Self::out_edge_key(*other_node_id, label_bytes, id, *edge_id),
                )?;
            }
            for (other_node_id, label_bytes, edge_id) in other_in_edges.iter() {
                txn.delete_cf(
                    &cf_in_edges,
                    Self::in_edge_key(*other_node_id, label_bytes, id, *edge_id),
                )?;
            }

            // delete secondary indices
            let node = self.get_node(txn, id, &arena)?;

            for (index_name, cf_name) in &self.secondary_indices {
                let cf = self.graph_env.cf_handle(cf_name).unwrap();
                let mut buf = bumpalo::collections::Vec::new_in(&arena);
                match node.get_property(index_name) {
                    Some(value) => match bincode::serialize(value) {
                        Ok(serialized) => {
                            txn.delete_cf(
                                &cf,
                                Self::secondary_index_key(&mut buf, &serialized, node.id),
                            )?;
                        }
                        Err(e) => return Err(GraphError::from(e)),
                    },
                    None => {
                        // Property not found - this is expected for some indices
                    }
                }
            }

            // Delete node data
            let cf_nodes = self.cf_nodes();
            txn.delete_cf(&cf_nodes, Self::node_key(id))
                .map_err(GraphError::from)
        }

        fn drop_edge<'db>(&self, txn: &Txn<'db>, edge_id: u128) -> Result<(), GraphError> {
            let arena = bumpalo::Bump::new();
            let edge = self.get_edge(txn, edge_id, &arena)?;
            let label_hash = hash_label(edge.label, None);
            let out_edge_key =
                Self::out_edge_key(edge.from_node, &label_hash, edge.to_node, edge_id);
            let in_edge_key = Self::in_edge_key(edge.to_node, &label_hash, edge.from_node, edge_id);

            // Get column family handles
            let cf_edges = self.cf_edges();
            let cf_out_edges = self.cf_out_edges();
            let cf_in_edges = self.cf_in_edges();

            // Delete all edge-related data
            txn.delete_cf(&cf_edges, Self::edge_key(edge_id))?;
            txn.delete_cf(&cf_out_edges, out_edge_key)?;
            txn.delete_cf(&cf_in_edges, in_edge_key)?;
            Ok(())
        }

        fn drop_vector<'db>(&self, txn: &Txn<'db>, id: u128) -> Result<(), GraphError> {
            use crate::helix_engine::rocks_utils::RocksUtils;

            let arena = bumpalo::Bump::new();
            let mut edges = HashSet::new();
            let mut out_edges = HashSet::new();
            let mut in_edges = HashSet::new();

            let mut other_out_edges = Vec::new();
            let mut other_in_edges = Vec::new();

            let cf_out_edges = self.cf_out_edges();
            let cf_in_edges = self.cf_in_edges();
            let cf_edges = self.cf_edges();

            // Delete outgoing edges
            let mut iter = txn.raw_prefix_iter(&cf_out_edges, &id.to_be_bytes());

            while let Some(key) = iter.key() {
                assert_eq!(key.len(), 52);
                let (_, label, to_node_id, edge_id) = Self::unpack_adj_edge_key(key)?;
                edges.insert(edge_id);
                out_edges.insert((label, to_node_id, edge_id));
                other_in_edges.push((to_node_id, label, edge_id));
                iter.next();
            }
            iter.status().map_err(GraphError::from)?;

            // Delete incoming edges
            let mut iter = txn.raw_prefix_iter(&cf_in_edges, &id.to_be_bytes());

            while let Some(key) = iter.key() {
                assert_eq!(key.len(), 52);
                let (_, label, from_node_id, edge_id) = Self::unpack_adj_edge_key(key)?;
                edges.insert(edge_id);
                in_edges.insert((label, from_node_id, edge_id));
                other_out_edges.push((from_node_id, label, edge_id));
                iter.next();
            }
            iter.status().map_err(GraphError::from)?;

            // Delete all related data
            for edge in edges {
                txn.delete_cf(&cf_edges, Self::edge_key(edge))?;
            }
            for (label_bytes, to_node_id, edge_id) in out_edges.iter() {
                txn.delete_cf(
                    &cf_out_edges,
                    Self::out_edge_key(id, label_bytes, *to_node_id, *edge_id),
                )?;
            }
            for (label_bytes, from_node_id, edge_id) in in_edges.iter() {
                txn.delete_cf(
                    &cf_in_edges,
                    Self::in_edge_key(id, label_bytes, *from_node_id, *edge_id),
                )?;
            }

            for (other_node_id, label_bytes, edge_id) in other_out_edges.iter() {
                txn.delete_cf(
                    &cf_out_edges,
                    Self::out_edge_key(*other_node_id, label_bytes, id, *edge_id),
                )?;
            }
            for (other_node_id, label_bytes, edge_id) in other_in_edges.iter() {
                txn.delete_cf(
                    &cf_in_edges,
                    Self::in_edge_key(*other_node_id, label_bytes, id, *edge_id),
                )?;
            }

            // Delete vector data
            self.vectors.delete(txn, id, &arena)?;

            Ok(())
        }
    }
}

#[cfg(feature = "slate")]
pub mod slate {

    use crate::helix_engine::slate_utils::{Entries, SlateUtils, table_prefix_delete};

    use super::*;

    use async_trait::async_trait;
    use futures::TryFutureExt;
    use helix_macros::{Len, index};
    use serde::{Deserialize, Serialize};
    use slatedb::object_store::{ObjectStore, aws::*};
    use slatedb::{Db, Error, WriteBatch};
    use std::ops::RangeBounds;
    use std::sync::Arc;

    pub type Herd = bumpalo_herd::Herd;
    pub type Arena<'db> = bumpalo_herd::Member<'db>;

    pub struct Txn<'db> {
        pub txn: slatedb::DBTransaction,
        _phantom: std::marker::PhantomData<&'db ()>,
    }

    // impl<'db> Txn<'db> {
    //     pub async fn commit(self) -> Result<(), Error> {
    //         self.txn.commit().await
    //     }
    // }

    impl<'db> std::ops::Deref for Txn<'db> {
        type Target = slatedb::DBTransaction;

        fn deref(&self) -> &Self::Target {
            &self.txn
        }
    }

    impl<'db> std::ops::DerefMut for Txn<'db> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.txn
        }
    }

    pub struct HelixGraphStorage {
        pub graph_env: Arc<slatedb::Db>,
        pub secondary_indices: HashMap<String, u16>, // Store CF names instead of handles
        pub vectors: VectorCore,
        pub bm25: Option<HBM25Config>,
        pub version_info: VersionInfo,
        pub storage_config: StorageConfig,
    }

    #[derive(Clone, Copy, Serialize, Deserialize, Len)]
    #[index(u16)]
    pub enum TableIndex {
        Nodes = 0,
        Edges = 1,
        OutEdges = 2,
        InEdges = 3,
        Metadata = 4,
    }

    const NODE_KEY_LEN: usize = 18;
    const EDGE_KEY_LEN: usize = 18;
    const VECTOR_KEY_LEN: usize = 18;
    const DIRECTION_KEY_LEN: usize = 54;

    impl HelixGraphStorage {
        /// Create a read transaction (snapshot)
        pub async fn read_txn(&self) -> Result<slatedb::DBTransaction, GraphError> {
            self.graph_env
                .begin(slatedb::IsolationLevel::Snapshot)
                .await
                .map_err(GraphError::from)
        }

        /// Create a write transaction
        pub async fn write_txn(&self) -> Result<slatedb::DBTransaction, GraphError> {
            self.graph_env
                .begin(slatedb::IsolationLevel::Snapshot)
                .await
                .map_err(GraphError::from)
        }

        pub async fn new(
            path: &str,
            config: Config,
            version_info: VersionInfo,
        ) -> Result<HelixGraphStorage, GraphError> {
            use std::sync::Arc;

            fs::create_dir_all(path)?;

            let bucket_name =
                std::env::var("AWS_S3_BUCKET_NAME").expect("AWS_S3_BUCKET_NAME not set");

            // TODO: configure properly

            let object_store: Arc<dyn ObjectStore> = Arc::new(
                AmazonS3Builder::new()
                    .with_bucket_name(bucket_name)
                    .build()?,
            );
            // Open database with optimistic transactions
            let db = Arc::new(slatedb::Db::open(path, object_store).await?);

            let mut secondary_indices =
                config
                    .get_graph_config()
                    .secondary_indices
                    .map_or(HashMap::new(), |indices| {
                        indices
                            .iter()
                            .enumerate()
                            .map(|(i, k)| (k.clone(), (i + TableIndex::len()) as u16))
                            .collect::<HashMap<_, _>>()
                    });

            // Initialize vector storage
            let vector_config = config.get_vector_config();
            let vectors = VectorCore::new(
                Arc::clone(&db),
                HNSWConfig::new(
                    vector_config.m,
                    vector_config.ef_construction,
                    vector_config.ef_search,
                ),
            )?;

            let bm25 = config
                .get_bm25()
                .then(|| HBM25Config::new(Arc::clone(&db)))
                .transpose()?;

            let storage_config = StorageConfig::new(
                config.schema,
                config.graphvis_node_label,
                config.embedding_model,
            );

            let storage = Self {
                graph_env: db,
                secondary_indices,
                vectors,
                bm25,
                storage_config,
                version_info,
            };

            // TODO: Implement RocksDB-specific migration if needed
            // storage_migration is LMDB-specific for now

            Ok(storage)
        }

        // // Merge operator for secondary indices (replaces DUP_SORT)
        // fn merge_append(
        //     _key: &[u8],
        //     existing: Option<&[u8]>,
        //     operands: &rocksdb::MergeOperands,
        // ) -> Option<Vec<u8>> {
        //     let mut result = existing.map(|v| v.to_vec()).unwrap_or_default();
        //     for op in operands {
        //         result.extend_from_slice(op);
        //     }
        //     Some(result)
        // }

        /// Used because in the case the key changes in the future.
        /// Believed to not introduce any overhead being inline and using a reference.
        #[must_use]
        #[inline(always)]
        pub fn node_key(id: u128) -> [u8; 18] {
            let mut key = [0u8; 18];
            key[0..2].copy_from_slice(TableIndex::Nodes.as_bytes());
            key[2..18].copy_from_slice(&id.to_be_bytes());
            key
        }

        /// Used because in the case the key changes in the future.
        /// Believed to not introduce any overhead being inline and using a reference.
        #[must_use]
        #[inline(always)]
        pub fn edge_key(id: u128) -> [u8; 18] {
            let mut key = [0u8; 18];
            key[0..2].copy_from_slice(TableIndex::Edges.as_bytes());
            key[2..18].copy_from_slice(&id.to_be_bytes());
            key
        }

        /// Out edge key generator. Creates a 20 byte array and copies in the node id and 4 byte label.
        ///
        /// key = `from-node(16)` | `label-id(4)`                 ← 20 B
        ///
        /// The generated out edge key will remain the same for the same from_node_id and label.
        /// To save space, the key is only stored once,
        /// with the values being stored in a sorted sub-tree, with this key being the root.
        #[inline(always)]
        pub fn out_edge_key(
            from_node_id: u128,
            label: &[u8; 4],
            to_node_id: u128,
            edge_id: u128,
        ) -> [u8; 54] {
            let mut key = [0u8; 54];
            key[0..2].copy_from_slice(TableIndex::OutEdges.as_bytes());
            key[2..18].copy_from_slice(&to_node_id.to_be_bytes());
            key[18..22].copy_from_slice(label);
            key[22..38].copy_from_slice(&from_node_id.to_be_bytes());
            key[38..54].copy_from_slice(&edge_id.to_be_bytes());
            key
        }

        #[inline(always)]
        pub fn out_edge_key_prefix(from_node_id: u128, label: &[u8; 4]) -> [u8; 22] {
            let mut key = [0u8; 22];
            key[0..2].copy_from_slice(TableIndex::OutEdges.as_bytes());
            key[2..18].copy_from_slice(&from_node_id.to_be_bytes());
            key[18..22].copy_from_slice(label);
            key
        }

        /// In edge key prefix generator. Creates a 20 byte array with the to_node_id and label.
        /// Used for prefix iteration in RocksDB.
        ///
        /// key = `to-node(16)` | `label-id(4)`                 ← 20 B
        #[inline(always)]
        pub fn in_edge_key_prefix(to_node_id: u128, label: &[u8; 4]) -> [u8; 22] {
            let mut key = [0u8; 22];
            key[0..2].copy_from_slice(TableIndex::InEdges.as_bytes());
            key[2..18].copy_from_slice(&to_node_id.to_be_bytes());
            key[18..22].copy_from_slice(label);
            key
        }

        /// In edge key generator. Creates a 36 byte array with to_node, label, and from_node.
        ///
        /// key = `to-node(16)` | `label-id(4)` | `from-node(16)`    ← 36 B
        ///
        /// The generated in edge key will be unique for each edge.
        #[inline(always)]
        pub fn in_edge_key(
            to_node_id: u128,
            label: &[u8; 4],
            from_node_id: u128,
            edge_id: u128,
        ) -> [u8; 54] {
            let mut key = [0u8; 54];
            key[0..2].copy_from_slice(TableIndex::InEdges.as_bytes());
            key[2..18].copy_from_slice(&to_node_id.to_be_bytes());
            key[18..22].copy_from_slice(label);
            key[22..38].copy_from_slice(&from_node_id.to_be_bytes());
            key[38..54].copy_from_slice(&edge_id.to_be_bytes());
            key
        }

        #[inline(always)]
        pub fn unpack_adj_edge_key(
            data: &[u8],
        ) -> Result<(NodeId, [u8; 4], NodeId, EdgeId), GraphError> {
            let _index = TableIndex::from(
                &data[0..2]
                    .try_into()
                    .map_err(|_| GraphError::from("invalid index bytes"))?,
            );
            let node_id = u128::from_be_bytes(
                data[2..18]
                    .try_into()
                    .map_err(|_| GraphError::SliceLengthError)?,
            );
            let label = data[18..22]
                .try_into()
                .map_err(|_| GraphError::SliceLengthError)?;
            let node_id2 = u128::from_be_bytes(
                data[22..38]
                    .try_into()
                    .map_err(|_| GraphError::SliceLengthError)?,
            );
            let edge_id = EdgeId::from_be_bytes(
                data[38..54]
                    .try_into()
                    .map_err(|_| GraphError::SliceLengthError)?,
            );
            Ok((node_id, label, node_id2, edge_id))
        }

        /// clears buffer then writes secondary index key
        #[inline(always)]
        pub fn secondary_index_key<'a>(
            buf: &'a mut bumpalo::collections::Vec<u8>,
            table_index: u16,
            key: &[u8],
            node_id: u128,
        ) -> &'a mut [u8] {
            buf.clear();
            buf.extend_from_slice(&table_index.to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&node_id.to_be_bytes());
            buf
        }

        /// Creates a secondary index lmdb db (table) for a given index name
        fn create_secondary_index(&mut self, _name: &str) -> Result<(), GraphError> {
            unimplemented!(
                "cannot be implemented for rocks db due to table having to be declared at creation time"
            )
        }

        /// Drops a secondary index lmdb db (table) for a given index name
        /// left as sync
        fn drop_secondary_index(&mut self, name: &str) -> Result<(), GraphError> {
            let si_prefix = self
                .secondary_indices
                .remove(name)
                .ok_or(GraphError::from("secondary index not found"))?;
            let _ = futures::executor::block_on(async {
                let txn = self
                    .graph_env
                    .begin(slatedb::IsolationLevel::Snapshot)
                    .await
                    .unwrap();

                let mut batch = WriteBatch::new();
                let mut iterator = txn.secondary_index_iter(si_prefix).await.unwrap();
                while let Some(kv) = iterator.next().await.unwrap() {
                    batch.delete(kv.key);
                }
            });
            Ok(())
        }

        #[inline]
        pub async fn get_node<'db, 'arena>(
            &self,
            txn: &Txn<'db>,
            id: u128,
            arena: &'arena Herd,
        ) -> Result<Node<'arena>, GraphError> {
            let node = match txn.get(Self::node_key(id)).await.unwrap() {
                Some(data) => data,
                None => return Err(GraphError::NodeNotFound),
            };
            let node: Node = Node::from_bincode_bytes(id, &node, arena.get())?;
            let node = self.version_info.upgrade_to_node_latest(node);
            Ok(node)
        }

        #[inline]
        pub async fn get_edge<'db, 'arena>(
            &self,
            txn: &Txn<'db>,
            id: u128,
            arena: &'arena Herd,
        ) -> Result<Edge<'arena>, GraphError> {
            let edge = match txn.get(Self::edge_key(id)).await.unwrap() {
                Some(data) => data,
                None => return Err(GraphError::EdgeNotFound),
            };
            let edge: Edge = Edge::from_bincode_bytes(id, &edge, arena.get())?;
            Ok(self.version_info.upgrade_to_edge_latest(edge))
        }

        pub async fn drop_node<'db>(
            &self,
            txn: &Txn<'db>,
            id: u128,
            batch: &mut slatedb::WriteBatch,
            arena: &bumpalo_herd::Herd,
        ) -> Result<(), GraphError> {
            let mut edges = HashSet::new();
            let mut out_edges = HashSet::new();
            let mut in_edges = HashSet::new();

            let mut other_out_edges = Vec::new();
            let mut other_in_edges = Vec::new();

            // Delete outgoing edges
            let mut iter = txn
                .table_prefix_iter::<DIRECTION_KEY_LEN>(TableIndex::OutEdges, &id.to_be_bytes())
                .await?;

            while let Some(key) = iter.key().await? {
                assert_eq!(key.len(), 52);
                let (_, label, to_node_id, edge_id) = Self::unpack_adj_edge_key(&key)?;
                edges.insert(edge_id);
                out_edges.insert((label, to_node_id, edge_id));
                other_in_edges.push((to_node_id, label, edge_id));
            }

            // Delete incoming edges
            let mut iter = txn
                .table_prefix_iter::<DIRECTION_KEY_LEN>(TableIndex::InEdges, &id.to_be_bytes())
                .await?;

            while let Some(key) = iter.key().await? {
                assert_eq!(key.len(), 52);
                let (_, label, from_node_id, edge_id) = Self::unpack_adj_edge_key(&key)?;
                edges.insert(edge_id);
                in_edges.insert((label, from_node_id, edge_id));
                other_out_edges.push((from_node_id, label, edge_id));
            }

            // Delete all related data
            for edge in edges {
                batch.delete(Self::edge_key(edge));
            }
            for (label_bytes, to_node_id, edge_id) in out_edges.iter() {
                batch.delete(Self::out_edge_key(id, label_bytes, *to_node_id, *edge_id));
            }
            for (label_bytes, from_node_id, edge_id) in in_edges.iter() {
                batch.delete(Self::in_edge_key(id, label_bytes, *from_node_id, *edge_id));
            }

            for (other_node_id, label_bytes, edge_id) in other_out_edges.iter() {
                batch.delete(Self::out_edge_key(
                    *other_node_id,
                    label_bytes,
                    id,
                    *edge_id,
                ));
            }
            for (other_node_id, label_bytes, edge_id) in other_in_edges.iter() {
                batch.delete(Self::in_edge_key(*other_node_id, label_bytes, id, *edge_id));
            }

            // delete secondary indices
            let node = self.get_node(txn, id, &arena).await?;

            let member = arena.get();
            for (index_name, si_index) in &self.secondary_indices {
                let mut buf = bumpalo::collections::Vec::new_in(member.as_bump());
                match node.get_property(index_name) {
                    Some(value) => match bincode::serialize(value) {
                        Ok(serialized) => {
                            batch.delete(Self::secondary_index_key(
                                &mut buf,
                                *si_index,
                                &serialized,
                                node.id,
                            ));
                        }
                        Err(e) => return Err(GraphError::from(e)),
                    },
                    None => {
                        // Property not found - this is expected for some indices
                    }
                }
            }

            // Delete node data
            batch.delete(Self::node_key(id));
            Ok(())
        }

        async fn drop_edge<'db>(
            &self,
            txn: &Txn<'db>,
            edge_id: u128,
            batch: &mut slatedb::WriteBatch,
            arena: &bumpalo_herd::Herd,
        ) -> Result<(), GraphError> {
            let edge = self.get_edge(txn, edge_id, arena).await?;
            let label_hash = hash_label(edge.label, None);
            let out_edge_key =
                Self::out_edge_key(edge.from_node, &label_hash, edge.to_node, edge_id);
            let in_edge_key = Self::in_edge_key(edge.to_node, &label_hash, edge.from_node, edge_id);

            // Delete all edge-related data
            batch.delete(Self::edge_key(edge_id));
            batch.delete(out_edge_key);
            batch.delete(in_edge_key);
            Ok(())
        }

        async fn drop_vector<'db>(
            &self,
            txn: &Txn<'db>,
            id: u128,
            batch: &mut slatedb::WriteBatch,
            arena: &bumpalo_herd::Herd,
        ) -> Result<(), GraphError> {
            let mut edges = HashSet::new();
            let mut out_edges = HashSet::new();
            let mut in_edges = HashSet::new();

            // this is okay as allocated vecs are tied to member lifetime
            let member = arena.get();
            let mut other_out_edges = bumpalo::collections::Vec::new_in(member.as_bump());
            let mut other_in_edges = bumpalo::collections::Vec::new_in(member.as_bump());

            // Delete outgoing edges
            let mut iter = txn
                .table_prefix_iter::<DIRECTION_KEY_LEN>(TableIndex::OutEdges, &id.to_be_bytes())
                .await?;

            while let Some(key) = iter.key().await? {
                assert_eq!(key.len(), 52);
                let (_, label, to_node_id, edge_id) = Self::unpack_adj_edge_key(&key)?;
                edges.insert(edge_id);
                out_edges.insert((label, to_node_id, edge_id));
                other_in_edges.push((to_node_id, label, edge_id));
            }

            // Delete incoming edges
            let mut iter = txn
                .table_prefix_iter::<DIRECTION_KEY_LEN>(TableIndex::InEdges, &id.to_be_bytes())
                .await?;

            while let Some(key) = iter.key().await? {
                assert_eq!(key.len(), 52);
                let (_, label, from_node_id, edge_id) = Self::unpack_adj_edge_key(&key)?;
                edges.insert(edge_id);
                in_edges.insert((label, from_node_id, edge_id));
                other_out_edges.push((from_node_id, label, edge_id));
            }

            // Delete all related data
            for edge in edges {
                batch.delete(Self::edge_key(edge));
            }
            for (label_bytes, to_node_id, edge_id) in out_edges.iter() {
                batch.delete(Self::out_edge_key(id, label_bytes, *to_node_id, *edge_id));
            }
            for (label_bytes, from_node_id, edge_id) in in_edges.iter() {
                batch.delete(Self::in_edge_key(id, label_bytes, *from_node_id, *edge_id));
            }

            for (other_node_id, label_bytes, edge_id) in other_out_edges.iter() {
                batch.delete(Self::out_edge_key(
                    *other_node_id,
                    label_bytes,
                    id,
                    *edge_id,
                ));
            }
            for (other_node_id, label_bytes, edge_id) in other_in_edges.iter() {
                batch.delete(Self::in_edge_key(*other_node_id, label_bytes, id, *edge_id));
            }

            // Delete vector data
            todo!("implement deleting vectors");
            self.vectors.delete(txn, id, &arena)?;

            Ok(())
        }
    }
}
