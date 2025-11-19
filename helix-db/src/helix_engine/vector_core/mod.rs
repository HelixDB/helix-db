use std::{
    borrow::Cow,
    cell::RefCell,
    cmp::Ordering,
    hash::Hash,
    sync::{
        RwLock,
        atomic::{self, AtomicU16, AtomicU32, AtomicUsize},
    },
};

use bincode::Options;
use byteorder::BE;
use hashbrown::HashMap;
use heed3::{
    Database, Env, Error as LmdbError, RoTxn, RwTxn,
    types::{Bytes, U128},
};
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};

use crate::{
    helix_engine::{
        types::VectorError,
        vector_core::{
            distance::{Cosine, Distance, DistanceValue},
            key::{Key, KeyCodec},
            node::{Item, NodeCodec},
            node_id::NodeMode,
            reader::{Reader, Searched},
            unaligned_vector::UnalignedVector,
            writer::Writer,
        },
    },
    protocol::{
        custom_serde::vector_serde::{VectoWithoutDataDeSeed, VectorDeSeed},
        value::Value,
    },
    utils::{id::v6_uuid, properties::ImmutablePropertiesMap},
};

pub mod distance;
pub mod hnsw;
pub mod item_iter;
pub mod key;
pub mod metadata;
pub mod node;
pub mod node_id;
pub mod ordered_float;
pub mod parallel;
pub mod reader;
pub mod spaces;
pub mod stats;
pub mod unaligned_vector;
pub mod version;
pub mod writer;

const DB_VECTORS: &str = "vectors"; // for vector data (v:)
const DB_VECTOR_DATA: &str = "vector_data"; // for vector's properties

pub type ItemId = u32;

pub type LayerId = u8;

pub type VectorCoreResult<T> = std::result::Result<T, VectorError>;

pub type LmdbResult<T, E = LmdbError> = std::result::Result<T, E>;

pub type CoreDatabase<D> = heed3::Database<KeyCodec, NodeCodec<D>>;

#[derive(Debug, Serialize, Clone)]
pub struct HVector<'arena> {
    pub id: u128,
    pub distance: Option<f32>,
    pub label: &'arena str,
    pub deleted: bool,
    pub version: u8,
    pub properties: Option<ImmutablePropertiesMap<'arena>>,
    pub data: Option<Item<'arena, Cosine>>,
}

impl<'arena> HVector<'arena> {
    pub fn data_borrowed(&self) -> &[f32] {
        bytemuck::cast_slice(self.data.as_ref().unwrap().vector.as_bytes())
    }

    pub fn from_vec(label: &'arena str, data: bumpalo::collections::Vec<'arena, f32>) -> Self {
        let id = v6_uuid();
        HVector {
            id,
            version: 1,
            label,
            data: Some(Item::<Cosine>::new(data)),
            distance: None,
            properties: None,
            deleted: false,
        }
    }

    pub fn score(&self) -> f32 {
        self.distance.unwrap_or(2.0)
    }

    /// Converts HVector's data to a vec of bytes by accessing the data field directly
    /// and converting each f64 to a byte slice
    #[inline(always)]
    pub fn vector_data_to_bytes(&self) -> VectorCoreResult<&[u8]> {
        Ok(self.data.as_ref().unwrap().vector.as_ref().as_bytes())
    }

    /// Deserializes bytes into an vector using a custom deserializer that allocates into the provided arena
    ///
    /// Both the properties bytes (if present) and the raw vector data are combined to generate the final vector struct
    ///
    /// NOTE: in this method, fixint encoding is used
    #[inline]
    pub fn from_bincode_bytes<'txn>(
        arena: &'arena bumpalo::Bump,
        properties: Option<&'txn [u8]>,
        raw_vector_data: &'txn [u8],
        id: u128,
        get_data: bool,
    ) -> Result<Self, VectorError> {
        if get_data {
            bincode::options()
                .with_fixint_encoding()
                .allow_trailing_bytes()
                .deserialize_seed(
                    VectorDeSeed {
                        arena,
                        id,
                        raw_vector_data,
                    },
                    properties.unwrap_or(&[]),
                )
                .map_err(|e| {
                    VectorError::ConversionError(format!("Error deserializing vector: {e}"))
                })
        } else {
            bincode::options()
                .with_fixint_encoding()
                .allow_trailing_bytes()
                .deserialize_seed(
                    VectoWithoutDataDeSeed { arena, id },
                    properties.unwrap_or(&[]),
                )
                .map_err(|e| {
                    VectorError::ConversionError(format!("Error deserializing vector: {e}"))
                })
        }
    }

    #[inline(always)]
    pub fn to_bincode_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    pub fn distance_to(&self, rhs: &HVector<'arena>) -> VectorCoreResult<f64> {
        todo!()
    }

    pub fn set_distance(&mut self, distance: f32) {
        self.distance = Some(distance);
    }

    pub fn get_distance(&self) -> f32 {
        self.distance.unwrap()
    }

    pub fn len(&self) -> usize {
        self.data.as_ref().unwrap().vector.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.as_ref().unwrap().vector.is_empty()
    }

    #[inline(always)]
    pub fn get_property(&self, key: &str) -> Option<&'arena Value> {
        self.properties.as_ref().and_then(|value| value.get(key))
    }

    pub fn raw_vector_data_to_vec<'txn>(
        raw_vector_data: &'txn [u8],
        arena: &'arena bumpalo::Bump,
    ) -> bumpalo::collections::Vec<'arena, f32> {
        let mut bump_vec = bumpalo::collections::Vec::<'arena, f32>::new_in(arena);
        bump_vec.extend_from_slice(bytemuck::cast_slice(raw_vector_data));
        bump_vec
    }

    pub fn from_raw_vector_data<'txn>(
        arena: &'arena bumpalo::Bump,
        raw_vector_data: &'txn [u8],
        label: &'arena str,
        id: u128,
    ) -> Result<Self, VectorError> {
        todo!()
    }
}

impl PartialEq for HVector<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for HVector<'_> {}
impl PartialOrd for HVector<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HVector<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWConfig {
    pub m: usize,             // max num of bi-directional links per element
    pub m_max_0: usize,       // max num of links for lower layers
    pub ef_construct: usize,  // size of the dynamic candidate list for construction
    pub m_l: f64,             // level generation factor
    pub ef: usize,            // search param, num of cands to search
    pub min_neighbors: usize, // for get_neighbors, always 512
}

impl HNSWConfig {
    /// Constructor for the configs of the HNSW vector similarity search algorithm
    /// - m (5 <= m <= 48): max num of bi-directional links per element
    /// - m_max_0 (2 * m): max num of links for level 0 (level that stores all vecs)
    /// - ef_construct (40 <= ef_construct <= 512): size of the dynamic candidate list
    ///   for construction
    /// - m_l (ln(1/m)): level generation factor (multiplied by a random number)
    /// - ef (10 <= ef <= 512): num of candidates to search
    pub fn new(m: Option<usize>, ef_construct: Option<usize>, ef: Option<usize>) -> Self {
        let m = m.unwrap_or(16).clamp(5, 48);
        let ef_construct = ef_construct.unwrap_or(128).clamp(40, 512);
        let ef = ef.unwrap_or(768).clamp(10, 512);

        Self {
            m,
            m_max_0: 2 * m,
            ef_construct,
            m_l: 1.0 / (m as f64).ln(),
            ef,
            min_neighbors: 512,
        }
    }
}

pub struct VectorCoreStats {
    pub num_vectors: AtomicUsize,
}

// TODO: Properties filters
// TODO: Support different distances for each database
pub struct VectorCore {
    pub hsnw_index: CoreDatabase<Cosine>,
    pub stats: VectorCoreStats,
    pub vector_properties_db: Database<U128<BE>, Bytes>,
    pub config: HNSWConfig,

    /// Map labels to a different index and dimension
    pub label_to_index: RwLock<HashMap<String, (u16, usize)>>,
    /// Track the last index
    curr_index: AtomicU16,

    /// Maps global id (u128) to internal id (u32) and label
    pub global_to_local_id: RwLock<HashMap<u128, (u32, String)>>,
    pub local_to_global_id: RwLock<HashMap<u32, u128>>,
    curr_id: AtomicU32,
}

impl VectorCore {
    pub fn new(env: &Env, txn: &mut RwTxn, config: HNSWConfig) -> VectorCoreResult<Self> {
        let vectors_db: CoreDatabase<Cosine> = env.create_database(txn, Some(DB_VECTORS))?;
        let vector_properties_db = env
            .database_options()
            .types::<U128<BE>, Bytes>()
            .name(DB_VECTOR_DATA)
            .create(txn)?;

        Ok(Self {
            hsnw_index: vectors_db,
            stats: VectorCoreStats {
                num_vectors: AtomicUsize::new(0),
            },
            vector_properties_db,
            config,
            label_to_index: RwLock::new(HashMap::new()),
            curr_index: AtomicU16::new(0),
            global_to_local_id: RwLock::new(HashMap::new()),
            curr_id: AtomicU32::new(0),
            local_to_global_id: RwLock::new(HashMap::new()),
        })
    }

    pub fn search<'arena>(
        &self,
        txn: &RoTxn,
        query: Vec<f32>,
        k: usize,
        label: &'arena str,
        _should_trickle: bool,
        arena: &'arena bumpalo::Bump,
    ) -> VectorCoreResult<Searched<'arena>> {
        match self.label_to_index.read().unwrap().get(label) {
            Some(&(index, dimension)) => {
                if dimension != query.len() {
                    return Err(VectorError::InvalidVectorLength);
                }

                let reader = Reader::open(txn, index, self.hsnw_index)?;
                reader.nns(k).by_vector(txn, query.as_slice(), arena)
            }
            None => Ok(Searched::new(bumpalo::vec![in &arena])),
        }
    }

    /// Get a writer based on label. If it doesn't exist build a new index
    /// and return a writer to it
    fn get_writer_or_create_index(
        &self,
        label: &str,
        dimension: usize,
        txn: &mut RwTxn,
    ) -> VectorCoreResult<Writer<Cosine>> {
        if let Some(&(idx, dimension)) = self.label_to_index.read().unwrap().get(label) {
            Ok(Writer::new(self.hsnw_index, idx, dimension))
        } else {
            // Index do not exist, we should build it
            let idx = self.curr_index.fetch_add(1, atomic::Ordering::SeqCst);
            self.label_to_index
                .write()
                .unwrap()
                .insert(label.to_string(), (idx, dimension));
            let writer = Writer::new(self.hsnw_index, idx, dimension);
            let mut rng = StdRng::from_os_rng();
            let mut builder = writer.builder(&mut rng);

            builder
                .ef_construction(self.config.ef_construct)
                .build(txn)?;
            Ok(writer)
        }
    }

    pub fn insert<'arena>(
        &self,
        txn: &mut RwTxn,
        label: &'arena str,
        data: &'arena [f32],
        properties: Option<ImmutablePropertiesMap<'arena>>,
        arena: &'arena bumpalo::Bump,
    ) -> VectorCoreResult<HVector<'arena>> {
        // index hasn't been built yet
        let writer = self.get_writer_or_create_index(label, data.len(), txn)?;

        let mut bump_vec = bumpalo::collections::Vec::new_in(arena);
        bump_vec.extend_from_slice(data);
        let hvector = HVector::from_vec(label, bump_vec);

        let idx = self.curr_id.fetch_add(1, atomic::Ordering::SeqCst);
        self.global_to_local_id
            .write()
            .unwrap()
            .insert(hvector.id, (idx, label.to_string()));
        self.local_to_global_id
            .write()
            .unwrap()
            .insert(idx, hvector.id);
        self.stats
            .num_vectors
            .fetch_add(1, atomic::Ordering::SeqCst);

        writer.add_item(txn, idx, data)?;

        Ok(hvector)
    }

    pub fn delete(&self, txn: &mut RwTxn, id: u128) -> VectorCoreResult<()> {
        match self.global_to_local_id.read().unwrap().get(&id) {
            Some(&(idx, ref label)) => {
                let &(index, dimension) = self
                    .label_to_index
                    .read()
                    .unwrap()
                    .get(label)
                    .expect("if index exist label should also exist");
                let writer = Writer::new(self.hsnw_index, index, dimension);
                writer.del_item(txn, idx)?;
                self.stats
                    .num_vectors
                    .fetch_add(1, atomic::Ordering::SeqCst);
                Ok(())
            }
            None => Err(VectorError::VectorNotFound(format!(
                "vector {} doesn't exist",
                id
            ))),
        }
    }

    pub fn nns_to_hvectors<'arena>(
        &self,
        nns: bumpalo::collections::Vec<'arena, (ItemId, f32)>,
        with_data: bool,
        arena: &bumpalo::Bump,
    ) -> bumpalo::collections::Vec<'arena, HVector<'arena>> {
        todo!()
    }

    pub fn get_full_vector<'arena>(
        &self,
        txn: &RoTxn,
        id: u128,
        arena: &bumpalo::Bump,
    ) -> VectorCoreResult<HVector<'arena>> {
        todo!()
    }

    pub fn get_vector_properties<'arena>(
        &self,
        txn: &RoTxn,
        id: u128,
        arena: &bumpalo::Bump,
    ) -> VectorCoreResult<Option<HVector<'arena>>> {
        todo!()
    }

    pub fn num_inserted_vectors(&self) -> usize {
        self.stats.num_vectors.load(atomic::Ordering::SeqCst)
    }
}
