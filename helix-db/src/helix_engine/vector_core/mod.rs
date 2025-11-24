use std::{
    cmp::Ordering,
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
    types::{Bytes, U32, U128},
};
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};

use crate::{
    helix_engine::{
        types::VectorError,
        vector_core::{
            distance::{Cosine, Distance},
            key::KeyCodec,
            node::{Item, NodeCodec},
            reader::{Reader, Searched, get_item},
            writer::Writer,
        },
    },
    protocol::{
        custom_serde::vector_serde::{VectoWithoutDataDeSeed, VectorDeSeed},
        value::Value,
    },
    utils::{
        id::{uuid_str_from_buf, v6_uuid},
        properties::ImmutablePropertiesMap,
    },
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
const DB_ID_MAP: &str = "id_map"; // for map ids

pub type ItemId = u32;

pub type LayerId = u8;

pub type VectorCoreResult<T> = std::result::Result<T, VectorError>;

pub type LmdbResult<T, E = LmdbError> = std::result::Result<T, E>;

pub type CoreDatabase<D> = heed3::Database<KeyCodec, NodeCodec<D>>;

#[derive(Debug, Clone)]
pub struct HVector<'arena> {
    pub id: u128,
    pub distance: Option<f32>,
    // TODO: String Interning. We do a lot of unnecessary string allocations
    // for the same set of labels.
    pub label: &'arena str,
    pub deleted: bool,
    pub level: Option<usize>,
    pub version: u8,
    pub properties: Option<ImmutablePropertiesMap<'arena>>,
    pub data: Option<Item<'arena, Cosine>>,
}

impl<'arena> serde::Serialize for HVector<'arena> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        // Check if this is a human-readable format (like JSON)
        if serializer.is_human_readable() {
            // Include id for JSON serialization
            let mut buffer = [0u8; 36];
            let mut state = serializer.serialize_map(Some(
                5 + self.properties.as_ref().map(|p| p.len()).unwrap_or(0),
            ))?;
            state.serialize_entry("id", uuid_str_from_buf(self.id, &mut buffer))?;
            state.serialize_entry("label", &self.label)?;
            state.serialize_entry("version", &self.version)?;
            state.serialize_entry("deleted", &self.deleted)?;
            if let Some(properties) = &self.properties {
                for (key, value) in properties.iter() {
                    state.serialize_entry(key, value)?;
                }
            }
            state.end()
        } else {
            // Skip id, level, distance, and data for bincode serialization
            let mut state = serializer.serialize_struct("HVector", 4)?;
            state.serialize_field("label", &self.label)?;
            state.serialize_field("version", &self.version)?;
            state.serialize_field("deleted", &self.deleted)?;
            state.serialize_field("properties", &self.properties)?;
            state.end()
        }
    }
}

impl<'arena> HVector<'arena> {
    pub fn data_borrowed(&self) -> &[f32] {
        bytemuck::cast_slice(self.data.as_ref().unwrap().vector.as_bytes())
    }

    pub fn from_vec(label: &'arena str, data: bumpalo::collections::Vec<'arena, f32>) -> Self {
        let id = v6_uuid();
        HVector {
            id,
            label,
            version: 1,
            data: Some(Item::<Cosine>::from_vec(data)),
            distance: None,
            properties: None,
            deleted: false,
            level: None,
        }
    }

    pub fn score(&self) -> f32 {
        self.distance.unwrap_or(2.0)
    }

    /// Converts HVector's data to a vec of bytes by accessing the data field directly
    /// and converting each f32 to a byte slice
    #[inline(always)]
    pub fn vector_data_to_bytes(&self) -> VectorCoreResult<&[u8]> {
        Ok(self
            .data
            .as_ref()
            .ok_or(VectorError::HasNoData)?
            .vector
            .as_ref()
            .as_bytes())
    }

    /// Deserializes bytes into an vector using a custom deserializer that allocates into the provided arena
    ///
    /// Both the properties bytes (if present) and the raw vector data are combined to generate the final vector struct
    ///
    /// NOTE: in this method, fixint encoding is used
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

    pub fn distance_to(&self, rhs: &HVector<'arena>) -> VectorCoreResult<f32> {
        match (self.data.as_ref(), rhs.data.as_ref()) {
            (None, _) | (_, None) => Err(VectorError::HasNoData),
            (Some(a), Some(b)) => Ok(Cosine::distance(a, b)),
        }
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
        id: u128,
        label: &'arena str,
        raw_vector_data: &'txn [u8],
    ) -> VectorCoreResult<HVector<'txn>>
    where
        'arena: 'txn,
    {
        Ok(HVector {
            id,
            label,
            data: Some(Item::<Cosine>::from_raw_slice(raw_vector_data)),
            properties: None,
            distance: None,
            deleted: false,
            level: Some(0),
            version: 1,
        })
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
    /// max num of bi-directional links per element
    pub m: usize,
    /// max num of links for lower layers
    pub m_max_0: usize,
    /// size of the dynamic candidate list for construction
    pub ef_construct: usize,
    /// level generation factor
    pub m_l: f64,
    /// search param, num of cands to search
    pub ef: usize,
    /// for get_neighbors, always 512
    pub min_neighbors: usize,
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

pub struct HnswIndex {
    pub id: u16,
    pub dimension: usize,
    pub num_vectors: AtomicUsize,
}

// TODO: Properties filters
// TODO: Support different distances for each database
pub struct VectorCore {
    pub hsnw: CoreDatabase<Cosine>,
    pub vector_properties_db: Database<U128<BE>, Bytes>,
    pub config: HNSWConfig,

    /// Map labels to a different [HnswIndex]
    pub label_to_index: RwLock<HashMap<String, HnswIndex>>,
    /// Track the last index
    curr_index: AtomicU16,

    /// Maps global id (u128) to internal id (u32) and label
    pub global_to_local_id: RwLock<HashMap<u128, (u32, String)>>,
    pub local_to_global_id: Database<U32<BE>, U128<BE>>,
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

        let local_to_global_id = env
            .database_options()
            .types::<U32<BE>, U128<BE>>()
            .name(DB_ID_MAP)
            .create(txn)?;

        Ok(Self {
            hsnw: vectors_db,
            vector_properties_db,
            config,
            local_to_global_id,
            label_to_index: RwLock::new(HashMap::new()),
            curr_index: AtomicU16::new(0),
            global_to_local_id: RwLock::new(HashMap::new()),
            curr_id: AtomicU32::new(0),
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
            Some(index) => {
                if index.dimension != query.len() {
                    return Err(VectorError::InvalidVectorLength);
                }

                let reader = Reader::open(txn, index.id, self.hsnw)?;
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
        if let Some(index) = self.label_to_index.read().unwrap().get(label) {
            Ok(Writer::new(self.hsnw, index.id, dimension))
        } else {
            // Index do not exist, we should build it
            let idx = self.curr_index.fetch_add(1, atomic::Ordering::SeqCst);
            self.label_to_index.write().unwrap().insert(
                label.to_string(),
                HnswIndex {
                    id: idx,
                    dimension,
                    num_vectors: AtomicUsize::new(0),
                },
            );
            let writer = Writer::new(self.hsnw, idx, dimension);
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
        _properties: Option<ImmutablePropertiesMap<'arena>>,
        arena: &'arena bumpalo::Bump,
    ) -> VectorCoreResult<HVector<'arena>> {
        let writer = self.get_writer_or_create_index(label, data.len(), txn)?;

        let idx = self.curr_id.fetch_add(1, atomic::Ordering::SeqCst);
        writer.add_item(txn, idx, data).inspect_err(|_| {
            self.curr_id.fetch_sub(1, atomic::Ordering::SeqCst);
        })?;

        let mut bump_vec = bumpalo::collections::Vec::new_in(arena);
        bump_vec.extend_from_slice(data);
        let hvector = HVector::from_vec(label, bump_vec);

        self.global_to_local_id
            .write()
            .unwrap()
            .insert(hvector.id, (idx, label.to_string()));
        self.local_to_global_id.put(txn, &idx, &hvector.id)?;

        self.label_to_index
            .read()
            .unwrap()
            .get(label)
            .unwrap()
            .num_vectors
            .fetch_add(1, atomic::Ordering::SeqCst);

        let mut rng = StdRng::from_os_rng();
        let mut builder = writer.builder(&mut rng);

        // FIXME: We shouldn't rebuild on every insertion
        builder
            .ef_construction(self.config.ef_construct)
            .build(txn)?;

        Ok(hvector)
    }

    pub fn delete(&self, txn: &mut RwTxn, id: u128) -> VectorCoreResult<()> {
        match self.global_to_local_id.read().unwrap().get(&id) {
            Some(&(idx, ref label)) => {
                let label_to_index = self.label_to_index.read().unwrap();
                let index = label_to_index
                    .get(label)
                    .expect("if index exist label should also exist");
                let writer = Writer::new(self.hsnw, index.id, index.dimension);
                writer.del_item(txn, idx)?;

                // TODO: do we actually need to delete here?
                self.local_to_global_id.delete(txn, &idx)?;

                index.num_vectors.fetch_sub(1, atomic::Ordering::SeqCst);
                Ok(())
            }
            None => Err(VectorError::VectorNotFound(format!(
                "vector {} doesn't exist",
                id
            ))),
        }
    }

    pub fn nns_to_hvectors<'arena, 'txn>(
        &self,
        txn: &'txn RoTxn,
        nns: bumpalo::collections::Vec<'arena, (ItemId, f32)>,
        with_data: bool,
        arena: &'arena bumpalo::Bump,
    ) -> VectorCoreResult<bumpalo::collections::Vec<'arena, HVector<'arena>>>
    where
        'txn: 'arena,
    {
        let mut results = bumpalo::collections::Vec::<'arena, HVector<'arena>>::with_capacity_in(
            nns.len(),
            arena,
        );

        let label_to_index = self.label_to_index.read().unwrap();
        let global_to_local_id = self.global_to_local_id.read().unwrap();

        let (item_id, _) = nns.first().unwrap();
        let global_id = self
            .local_to_global_id
            .get(txn, &item_id)?
            .ok_or_else(|| VectorError::VectorNotFound("Vector not found".to_string()))?;
        let (_, label) = global_to_local_id.get(&global_id).unwrap();
        let index = label_to_index.get(label).unwrap();
        let label = arena.alloc_str(label);

        if with_data {
            for (item_id, distance) in nns.into_iter() {
                let global_id = self
                    .local_to_global_id
                    .get(txn, &item_id)?
                    .ok_or_else(|| VectorError::VectorNotFound("Vector not found".to_string()))?;

                results.push(HVector {
                    id: global_id,
                    distance: Some(distance),
                    label,
                    deleted: false,
                    level: None,
                    version: 0,
                    properties: None,
                    data: get_item(self.hsnw, index.id, txn, item_id).unwrap(),
                });
            }
        } else {
            for (item_id, distance) in nns.into_iter() {
                let global_id = self
                    .local_to_global_id
                    .get(txn, &item_id)?
                    .ok_or_else(|| VectorError::VectorNotFound("Vector not found".to_string()))?;

                results.push(HVector {
                    id: global_id,
                    distance: Some(distance),
                    label,
                    deleted: false,
                    version: 0,
                    properties: None,
                    level: None,
                    data: None,
                });
            }
        }

        Ok(results)
    }

    pub fn get_full_vector<'arena>(
        &self,
        txn: &RoTxn,
        id: u128,
        arena: &'arena bumpalo::Bump,
    ) -> VectorCoreResult<HVector<'arena>> {
        let label_to_index = self.label_to_index.read().unwrap();
        let global_to_local_id = self.global_to_local_id.read().unwrap();

        let (item_id, label) = global_to_local_id
            .get(&id)
            .ok_or_else(|| VectorError::VectorNotFound(format!("Vector {id} not found")))?;

        let index = label_to_index.get(label).unwrap();
        let label = arena.alloc_str(label);

        let item = get_item(self.hsnw, index.id, txn, *item_id)?.map(|i| i.clone_in(arena));

        Ok(HVector {
            id,
            distance: None,
            label,
            deleted: false,
            version: 0,
            level: None,
            properties: None,
            data: item.clone(),
        })
    }

    pub fn get_vector_properties<'arena>(
        &self,
        _txn: &RoTxn,
        id: u128,
        arena: &'arena bumpalo::Bump,
    ) -> VectorCoreResult<Option<HVector<'arena>>> {
        let global_to_local_id = self.global_to_local_id.read().unwrap();
        let (_, label) = global_to_local_id.get(&id).unwrap();

        // todo: actually take properties
        Ok(Some(HVector {
            id,
            distance: None,
            label: arena.alloc_str(label.as_str()),
            deleted: false,
            version: 0,
            level: None,
            properties: None,
            data: None,
        }))
    }

    pub fn num_inserted_vectors(&self) -> usize {
        self.label_to_index
            .read()
            .unwrap()
            .iter()
            .map(|(_, i)| i.num_vectors.load(atomic::Ordering::SeqCst))
            .sum()
    }

    pub fn get_all_vectors_by_label<'arena>(
        &self,
        txn: &RoTxn,
        label: &'arena str,
        get_vector_data: bool,
        arena: &'arena bumpalo::Bump,
    ) -> VectorCoreResult<bumpalo::collections::Vec<'arena, HVector<'arena>>> {
        let mut result = bumpalo::collections::Vec::new_in(arena);
        let label_to_index = self.label_to_index.read().unwrap();
        let index = label_to_index.get(label).unwrap();

        let reader = Reader::open(txn, index.id, self.hsnw)?;
        let mut iter = reader.iter(txn)?;

        if get_vector_data {
            while let Some((key, item)) = iter.next().transpose()? {
                let id = self
                    .local_to_global_id
                    .get(txn, &key.item)?
                    .ok_or_else(|| VectorError::VectorNotFound("Vector not found".to_string()))?;

                result.push(HVector {
                    id,
                    label,
                    distance: None,
                    deleted: false,
                    level: Some(key.layer as usize),
                    version: 0,
                    properties: None,
                    data: Some(item.clone_in(arena)),
                });
            }
        } else {
            while let Some(key) = iter.next_id().transpose()? {
                let id = self
                    .local_to_global_id
                    .get(txn, &key.item)?
                    .ok_or_else(|| VectorError::VectorNotFound("Vector not found".to_string()))?;

                result.push(HVector {
                    id,
                    label,
                    distance: None,
                    deleted: false,
                    level: Some(key.layer as usize),
                    version: 0,
                    properties: None,
                    data: None,
                });
            }
        }

        Ok(result)
    }

    pub fn get_all_vectors<'arena>(
        &self,
        txn: &RoTxn,
        get_vector_data: bool,
        arena: &'arena bumpalo::Bump,
    ) -> VectorCoreResult<bumpalo::collections::Vec<'_, HVector<'arena>>> {
        let label_to_index = self.label_to_index.read().unwrap();
        let mut result = bumpalo::collections::Vec::new_in(arena);

        for (label, index) in label_to_index.iter() {
            let reader = Reader::open(txn, index.id, self.hsnw)?;
            let mut iter = reader.iter(txn)?;

            if get_vector_data {
                while let Some((key, item)) = iter.next().transpose()? {
                    let id = self
                        .local_to_global_id
                        .get(txn, &key.item)?
                        .ok_or_else(|| {
                            VectorError::VectorNotFound("Vector not found".to_string())
                        })?;

                    result.push(HVector {
                        id,
                        label: arena.alloc_str(label),
                        distance: None,
                        deleted: false,
                        level: Some(key.layer as usize),
                        version: 0,
                        properties: None,
                        data: Some(item.clone_in(arena)),
                    });
                }
            } else {
                while let Some(key) = iter.next_id().transpose()? {
                    let id = self
                        .local_to_global_id
                        .get(txn, &key.item)?
                        .ok_or_else(|| {
                            VectorError::VectorNotFound("Vector not found".to_string())
                        })?;

                    result.push(HVector {
                        id,
                        label: arena.alloc_str(label),
                        distance: None,
                        deleted: false,
                        level: Some(key.layer as usize),
                        version: 0,
                        properties: None,
                        data: None,
                    });
                }
            }
        }

        Ok(result)
    }

    pub fn into_global_id(
        &self,
        txn: &RoTxn,
        searched: &Searched,
    ) -> VectorCoreResult<Vec<(u128, f32)>> {
        let mut result = Vec::new();
        for &(id, distance) in searched.nns.iter() {
            result.push((
                self.local_to_global_id
                    .get(txn, &id)?
                    .ok_or_else(|| VectorError::VectorNotFound("Vector not found".to_string()))?,
                distance,
            ))
        }

        Ok(result)
    }
}
