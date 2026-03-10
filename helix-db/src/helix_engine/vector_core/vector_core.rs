use super::binary_heap::BinaryHeap;
use crate::{
    debug_println,
    helix_engine::{
        types::VectorError,
        vector_core::{
            hnsw::HNSW,
            utils::{Candidate, HeapOps, VectorFilter},
            vector::HVector,
            vector_distance::distance_from_stored_bytes,
            vector_without_data::VectorWithoutData,
        },
    },
    utils::{id::uuid_str, properties::ImmutablePropertiesMap},
};
use heed3::{
    Database, Env, RoTxn, RwTxn,
    byteorder::BE,
    types::{Bytes, U128, Unit},
};
use rand::prelude::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const DB_VECTORS: &str = "vectors"; // for vector data (v:)
const DB_VECTOR_DATA: &str = "vector_data"; // for vector data (v:)
const DB_HNSW_EDGES: &str = "hnsw_out_nodes"; // for hnsw out node data
const VECTOR_PREFIX: &[u8] = b"v:";
pub const ENTRY_POINT_KEY: &[u8] = b"entry_point";
const ENTRY_POINT_BYTES_LEN: usize = 24;

#[derive(Clone, Copy, Debug)]
struct InsertEntryPoint {
    id: u128,
    level: usize,
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

pub struct VectorCore {
    pub vectors_db: Database<Bytes, Bytes>,
    pub vector_properties_db: Database<U128<BE>, Bytes>,
    pub edges_db: Database<Bytes, Unit>,
    pub config: HNSWConfig,
}

impl VectorCore {
    pub fn new(env: &Env, txn: &mut RwTxn, config: HNSWConfig) -> Result<Self, VectorError> {
        let vectors_db = env.create_database(txn, Some(DB_VECTORS))?;
        let vector_properties_db = env
            .database_options()
            .types::<U128<BE>, Bytes>()
            .name(DB_VECTOR_DATA)
            .create(txn)?;
        let edges_db = env.create_database(txn, Some(DB_HNSW_EDGES))?;

        Ok(Self {
            vectors_db,
            vector_properties_db,
            edges_db,
            config,
        })
    }

    /// Vector key: [v, id, ]
    #[inline(always)]
    pub fn vector_key(id: u128, level: usize) -> Vec<u8> {
        [VECTOR_PREFIX, &id.to_be_bytes(), &level.to_be_bytes()].concat()
    }

    #[inline(always)]
    pub fn out_edges_key(source_id: u128, level: usize, sink_id: Option<u128>) -> Vec<u8> {
        match sink_id {
            Some(sink_id) => [
                source_id.to_be_bytes().as_slice(),
                level.to_be_bytes().as_slice(),
                sink_id.to_be_bytes().as_slice(),
            ]
            .concat()
            .to_vec(),
            None => [
                source_id.to_be_bytes().as_slice(),
                level.to_be_bytes().as_slice(),
            ]
            .concat()
            .to_vec(),
        }
    }

    #[inline]
    fn get_new_level(&self) -> usize {
        let mut rng = rand::rng();
        let r: f64 = rng.random::<f64>();
        (-r.ln() * self.config.m_l).floor() as usize
    }

    #[inline]
    fn decode_entry_point(bytes: &[u8]) -> Result<InsertEntryPoint, VectorError> {
        if bytes.len() >= ENTRY_POINT_BYTES_LEN {
            let mut id = [0u8; 16];
            id.copy_from_slice(&bytes[..16]);

            let mut level = [0u8; 8];
            level.copy_from_slice(&bytes[16..ENTRY_POINT_BYTES_LEN]);

            return Ok(InsertEntryPoint {
                id: u128::from_be_bytes(id),
                level: u64::from_be_bytes(level) as usize,
            });
        }

        if bytes.len() >= 16 {
            let mut id = [0u8; 16];
            id.copy_from_slice(&bytes[..16]);
            return Ok(InsertEntryPoint {
                id: u128::from_be_bytes(id),
                level: 0,
            });
        }

        Err(VectorError::EntryPointNotFound)
    }

    #[inline]
    fn encode_entry_point(entry: &HVector) -> [u8; ENTRY_POINT_BYTES_LEN] {
        let mut bytes = [0u8; ENTRY_POINT_BYTES_LEN];
        bytes[..16].copy_from_slice(&entry.id.to_be_bytes());
        bytes[16..].copy_from_slice(&(entry.level as u64).to_be_bytes());
        bytes
    }

    #[inline]
    fn get_entry_point<'db: 'arena, 'arena: 'txn, 'txn>(
        &self,
        txn: &'txn RoTxn<'db>,
        label: &'arena str,
        arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError> {
        let ep_id = self.vectors_db.get(txn, ENTRY_POINT_KEY)?;
        if let Some(ep_id) = ep_id {
            let entry = Self::decode_entry_point(ep_id)?;
            let mut vector = self.get_raw_vector_data(txn, entry.id, label, arena)?;
            vector.level = entry.level;
            Ok(vector)
        } else {
            Err(VectorError::EntryPointNotFound)
        }
    }

    #[inline]
    fn set_entry_point(&self, txn: &mut RwTxn, entry: &HVector) -> Result<(), VectorError> {
        let encoded = Self::encode_entry_point(entry);
        self.vectors_db
            .put(txn, ENTRY_POINT_KEY, &encoded)
            .map_err(VectorError::from)?;
        Ok(())
    }

    #[inline]
    fn get_entry_point_for_insert<'db: 'txn, 'txn>(
        &self,
        txn: &'txn RoTxn<'db>,
    ) -> Result<InsertEntryPoint, VectorError> {
        let ep_id = self.vectors_db.get(txn, ENTRY_POINT_KEY)?;
        if let Some(ep_id) = ep_id {
            Self::decode_entry_point(ep_id)
        } else {
            Err(VectorError::EntryPointNotFound)
        }
    }

    #[inline(always)]
    fn get_vector_bytes<'db: 'txn, 'txn>(
        &self,
        txn: &'txn RoTxn<'db>,
        id: u128,
    ) -> Result<&'txn [u8], VectorError> {
        self.vectors_db
            .get(txn, &Self::vector_key(id, 0))?
            .ok_or_else(|| VectorError::VectorNotFound(id.to_string()))
    }

    #[inline(always)]
    fn get_or_compute_distance<'db: 'txn, 'txn>(
        &self,
        txn: &'txn RoTxn<'db>,
        id: u128,
        query: &[f64],
        distance_cache: &mut HashMap<u128, f64>,
    ) -> Result<f64, VectorError> {
        if let Some(distance) = distance_cache.get(&id) {
            return Ok(*distance);
        }

        let distance = distance_from_stored_bytes(query, self.get_vector_bytes(txn, id)?)?;
        distance_cache.insert(id, distance);
        Ok(distance)
    }

    #[inline(always)]
    fn for_each_neighbor_id<'db: 'txn, 'txn, C>(
        &self,
        txn: &'txn RoTxn<'db>,
        id: u128,
        level: usize,
        mut callback: C,
    ) -> Result<(), VectorError>
    where
        C: FnMut(u128) -> Result<(), VectorError>,
    {
        let out_key = Self::out_edges_key(id, level, None);
        let prefix_len = out_key.len();
        let iter = self
            .edges_db
            .lazily_decode_data()
            .prefix_iter(txn, &out_key)?;

        for result in iter {
            let (key, _) = result?;
            if key.len() < prefix_len + 16 {
                continue;
            }

            let mut arr = [0u8; 16];
            arr.copy_from_slice(&key[prefix_len..prefix_len + 16]);
            let neighbor_id = u128::from_be_bytes(arr);

            if neighbor_id == id {
                continue;
            }

            callback(neighbor_id)?;
        }

        Ok(())
    }

    #[inline(always)]
    pub(crate) fn put_vector_metadata<S: Serialize + ?Sized>(
        &self,
        txn: &mut RwTxn,
        id: u128,
        value: &S,
    ) -> Result<(), VectorError> {
        self.vector_properties_db
            .put(txn, &id, bincode::serialize(value)?.as_ref())?;
        Ok(())
    }

    #[inline(always)]
    pub fn put_vector<'arena>(
        &self,
        txn: &mut RwTxn,
        vector: &HVector<'arena>,
    ) -> Result<(), VectorError> {
        self.vectors_db
            .put(
                txn,
                &Self::vector_key(vector.id, 0),
                vector.vector_data_to_bytes()?,
            )
            .map_err(VectorError::from)?;
        self.put_vector_metadata(txn, vector.id, vector)?;
        Ok(())
    }

    #[inline(always)]
    fn get_neighbors<'db: 'arena, 'arena: 'txn, 'txn, F>(
        &self,
        txn: &'txn RoTxn<'db>,
        label: &'arena str,
        id: u128,
        level: usize,
        filter: Option<&[F]>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &RoTxn<'db>) -> bool,
    {
        let out_key = Self::out_edges_key(id, level, None);
        let mut neighbors = bumpalo::collections::Vec::with_capacity_in(
            self.config.m_max_0.min(self.config.min_neighbors),
            arena,
        );

        let iter = self
            .edges_db
            .lazily_decode_data()
            .prefix_iter(txn, &out_key)?;

        let prefix_len = out_key.len();

        for result in iter {
            let (key, _) = result?;

            let mut arr = [0u8; 16];
            arr[..16].copy_from_slice(&key[prefix_len..(prefix_len + 16)]);
            let neighbor_id = u128::from_be_bytes(arr);

            if neighbor_id == id {
                continue;
            }
            let vector = self.get_raw_vector_data(txn, neighbor_id, label, arena)?;

            let passes_filters = match filter {
                Some(filter_slice) => filter_slice.iter().all(|f| f(&vector, txn)),
                None => true,
            };

            if passes_filters {
                neighbors.push(vector);
            }
        }
        neighbors.shrink_to_fit();

        Ok(neighbors)
    }

    #[inline(always)]
    fn set_neighbours_from_ids<'db, 'txn, I>(
        &'db self,
        txn: &'txn mut RwTxn<'db>,
        id: u128,
        neighbors: I,
        level: usize,
    ) -> Result<(), VectorError>
    where
        I: IntoIterator<Item = u128>,
    {
        let prefix = Self::out_edges_key(id, level, None);

        let mut keys_to_delete: HashSet<Vec<u8>> = self
            .edges_db
            .prefix_iter(txn, prefix.as_ref())?
            .filter_map(|result| result.ok().map(|(key, _)| key.to_vec()))
            .collect();

        neighbors.into_iter().try_for_each(|neighbor_id| -> Result<(), VectorError> {
                if neighbor_id == id {
                    return Ok(());
                }

                let out_key = Self::out_edges_key(id, level, Some(neighbor_id));
                keys_to_delete.remove(&out_key);
                self.edges_db.put(txn, &out_key, &())?;

                let in_key = Self::out_edges_key(neighbor_id, level, Some(id));
                keys_to_delete.remove(&in_key);
                self.edges_db.put(txn, &in_key, &())?;

                Ok(())
            })?;

        for key in keys_to_delete {
            self.edges_db.delete(txn, &key)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn select_neighbors<'db: 'arena, 'arena: 'txn, 'txn, 's, F>(
        &'db self,
        txn: &'txn RoTxn<'db>,
        label: &'arena str,
        query: &'s HVector<'arena>,
        mut cands: BinaryHeap<'arena, HVector<'arena>>,
        level: usize,
        should_extend: bool,
        filter: Option<&[F]>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<BinaryHeap<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &RoTxn<'db>) -> bool,
    {
        let m = self.config.m;

        if !should_extend {
            return Ok(cands.take_inord(m));
        }

        let mut visited: HashSet<u128> = HashSet::new();
        let mut result = BinaryHeap::with_capacity(arena, m * cands.len());
        for candidate in cands.iter() {
            for mut neighbor in
                self.get_neighbors(txn, label, candidate.id, level, filter, arena)?
            {
                if !visited.insert(neighbor.id) {
                    continue;
                }

                neighbor.set_distance(neighbor.distance_to(query)?);

                /*
                let passes_filters = match filter {
                    Some(filter_slice) => filter_slice.iter().all(|f| f(&neighbor, txn)),
                    None => true,
                };

                if passes_filters {
                    result.push(neighbor);
                }
                */

                if filter.is_none() || filter.unwrap().iter().all(|f| f(&neighbor, txn)) {
                    result.push(neighbor);
                }
            }
        }

        result.extend(cands);
        Ok(result.take_inord(m))
    }

    fn get_neighbor_candidates_for_insert<'db: 'txn, 'txn, 'scratch>(
        &self,
        txn: &'txn RoTxn<'db>,
        id: u128,
        level: usize,
        query: &[f64],
        distance_cache: &mut HashMap<u128, f64>,
        arena: &'scratch bumpalo::Bump,
    ) -> Result<BinaryHeap<'scratch, Candidate>, VectorError> {
        let mut neighbors = BinaryHeap::with_capacity(
            arena,
            self.config.m_max_0.min(self.config.min_neighbors),
        );

        self.for_each_neighbor_id(txn, id, level, |neighbor_id| {
            let distance = self.get_or_compute_distance(txn, neighbor_id, query, distance_cache)?;
            neighbors.push(Candidate {
                id: neighbor_id,
                distance,
            });
            Ok(())
        })?;

        Ok(neighbors)
    }

    fn select_neighbors_for_insert<'db: 'txn, 'txn, 'scratch>(
        &self,
        txn: &'txn RoTxn<'db>,
        query: &[f64],
        mut cands: BinaryHeap<'scratch, Candidate>,
        level: usize,
        should_extend: bool,
        distance_cache: &mut HashMap<u128, f64>,
        arena: &'scratch bumpalo::Bump,
    ) -> Result<BinaryHeap<'scratch, Candidate>, VectorError> {
        let m = self.config.m;

        if !should_extend {
            return Ok(cands.take_inord(m));
        }

        let mut seen: HashSet<u128> = cands.iter().map(|candidate| candidate.id).collect();
        let mut result = BinaryHeap::with_capacity(arena, m * cands.len());

        for candidate in cands.iter() {
            self.for_each_neighbor_id(txn, candidate.id, level, |neighbor_id| {
                if !seen.insert(neighbor_id) {
                    return Ok(());
                }

                let distance = self.get_or_compute_distance(txn, neighbor_id, query, distance_cache)?;
                result.push(Candidate {
                    id: neighbor_id,
                    distance,
                });
                Ok(())
            })?;
        }

        result.extend(cands);
        Ok(result.take_inord(m))
    }

    fn search_level<'db: 'arena, 'arena: 'txn, 'txn, 'q, F>(
        &self,
        txn: &'txn RoTxn<'db>,
        label: &'arena str,
        query: &'q HVector<'arena>,
        entry_point: &'q mut HVector<'arena>,
        ef: usize,
        level: usize,
        filter: Option<&[F]>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<BinaryHeap<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &RoTxn<'db>) -> bool,
    {
        let mut visited: HashSet<u128> = HashSet::new();
        let mut candidates: BinaryHeap<'arena, Candidate> =
            BinaryHeap::with_capacity(arena, self.config.ef_construct);
        let mut results: BinaryHeap<'arena, HVector<'arena>> = BinaryHeap::new(arena);

        entry_point.set_distance(entry_point.distance_to(query)?);
        candidates.push(Candidate {
            id: entry_point.id,
            distance: entry_point.get_distance(),
        });
        results.push(*entry_point);
        visited.insert(entry_point.id);

        while let Some(curr_cand) = candidates.pop() {
            if results.len() >= ef
                && results
                    .get_max()
                    .is_none_or(|f| curr_cand.distance > f.get_distance())
            {
                break;
            }

            let max_distance = if results.len() >= ef {
                results.get_max().map(|f| f.get_distance())
            } else {
                None
            };

            self.get_neighbors(txn, label, curr_cand.id, level, filter, arena)?
                .into_iter()
                .filter(|neighbor| visited.insert(neighbor.id))
                .filter_map(|mut neighbor| {
                    let distance = neighbor.distance_to(query).ok()?;

                    if max_distance.is_none_or(|max| distance < max) {
                        neighbor.set_distance(distance);
                        Some((neighbor, distance))
                    } else {
                        None
                    }
                })
                .for_each(|(neighbor, distance)| {
                    candidates.push(Candidate {
                        id: neighbor.id,
                        distance,
                    });

                    results.push(neighbor);

                    if results.len() > ef {
                        results = results.take_inord(ef);
                    }
                });
        }
        Ok(results)
    }

    fn search_level_for_insert<'db: 'txn, 'txn, 'scratch>(
        &self,
        txn: &'txn RoTxn<'db>,
        query: &[f64],
        entry_point: Candidate,
        ef: usize,
        level: usize,
        distance_cache: &mut HashMap<u128, f64>,
        arena: &'scratch bumpalo::Bump,
    ) -> Result<BinaryHeap<'scratch, Candidate>, VectorError> {
        let mut visited: HashSet<u128> = HashSet::new();
        let mut candidates: BinaryHeap<'scratch, Candidate> =
            BinaryHeap::with_capacity(arena, self.config.ef_construct);
        let mut results: BinaryHeap<'scratch, Candidate> = BinaryHeap::new(arena);

        candidates.push(entry_point);
        results.push(entry_point);
        visited.insert(entry_point.id);

        while let Some(curr_cand) = candidates.pop() {
            if results.len() >= ef
                && results
                    .get_max()
                    .is_none_or(|f| curr_cand.distance > f.distance)
            {
                break;
            }

            let max_distance = if results.len() >= ef {
                results.get_max().map(|f| f.distance)
            } else {
                None
            };

            self.for_each_neighbor_id(txn, curr_cand.id, level, |neighbor_id| {
                if !visited.insert(neighbor_id) {
                    return Ok(());
                }

                let distance = self.get_or_compute_distance(txn, neighbor_id, query, distance_cache)?;
                if max_distance.is_none_or(|max| distance < max) {
                    let neighbor = Candidate {
                        id: neighbor_id,
                        distance,
                    };
                    candidates.push(neighbor);
                    results.push(neighbor);

                    if results.len() > ef {
                        results = results.take_inord(ef);
                    }
                }

                Ok(())
            })?;
        }

        Ok(results)
    }

    pub fn num_inserted_vectors(&self, txn: &RoTxn) -> Result<u64, VectorError> {
        Ok(self.vectors_db.len(txn)?)
    }

    #[inline]
    pub fn get_vector_properties<'db: 'arena, 'arena: 'txn, 'txn>(
        &self,
        txn: &'txn RoTxn<'db>,
        id: u128,
        arena: &'arena bumpalo::Bump,
    ) -> Result<Option<VectorWithoutData<'arena>>, VectorError> {
        let vector: Option<VectorWithoutData<'arena>> =
            match self.vector_properties_db.get(txn, &id)? {
                Some(bytes) => Some(VectorWithoutData::from_bincode_bytes(arena, bytes, id)?),
                None => None,
            };

        if let Some(vector) = vector
            && vector.deleted
        {
            return Err(VectorError::VectorDeleted);
        }

        Ok(vector)
    }

    #[inline(always)]
    pub fn get_full_vector<'arena>(
        &self,
        txn: &RoTxn,
        id: u128,
        arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError> {
        let vector_data_bytes = self
            .vectors_db
            .get(txn, &Self::vector_key(id, 0))?
            .ok_or(VectorError::VectorNotFound(uuid_str(id, arena).to_string()))?;

        let properties_bytes = self.vector_properties_db.get(txn, &id)?;

        let vector = HVector::from_bincode_bytes(arena, properties_bytes, vector_data_bytes, id)?;
        if vector.deleted {
            return Err(VectorError::VectorDeleted);
        }
        Ok(vector)
    }

    #[inline(always)]
    pub fn get_raw_vector_data<'db: 'arena, 'arena: 'txn, 'txn>(
        &self,
        txn: &'txn RoTxn<'db>,
        id: u128,
        label: &'arena str,
        arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError> {
        let vector_data_bytes = self
            .vectors_db
            .get(txn, &Self::vector_key(id, 0))?
            .ok_or(VectorError::EntryPointNotFound)?;
        HVector::from_raw_vector_data(arena, vector_data_bytes, label, id)
    }

    /// Get all vectors from the database, optionally filtered by level
    pub fn get_all_vectors<'db: 'arena, 'arena: 'txn, 'txn>(
        &self,
        txn: &'txn RoTxn<'db>,
        level: Option<usize>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError> {
        let mut vectors = bumpalo::collections::Vec::new_in(arena);

        if level.is_some_and(|level| level > 0) {
            return Ok(vectors);
        }

        for result in self.vector_properties_db.iter(txn)? {
            let (id, _) = result?;

            if let Ok(vector) = self.get_full_vector(txn, id, arena) {
                vectors.push(vector);
            }
        }

        Ok(vectors)
    }

    pub(crate) fn clear_hnsw_index(&self, txn: &mut RwTxn) -> Result<(), VectorError> {
        self.edges_db.clear(txn)?;
        self.vectors_db.delete(txn, ENTRY_POINT_KEY)?;

        let level_offset = VECTOR_PREFIX.len() + 16;
        let mut nonzero_level_keys = Vec::new();

        for result in self.vectors_db.iter(txn)? {
            let (key, _) = result?;
            if !key.starts_with(VECTOR_PREFIX)
                || key.len() != level_offset + std::mem::size_of::<usize>()
            {
                continue;
            }

            let mut level_bytes = [0u8; std::mem::size_of::<usize>()];
            level_bytes.copy_from_slice(&key[level_offset..]);
            if usize::from_be_bytes(level_bytes) > 0 {
                nonzero_level_keys.push(key.to_vec());
            }
        }

        for key in nonzero_level_keys {
            self.vectors_db.delete(txn, &key)?;
        }

        Ok(())
    }

    fn index_existing_vector<'db, 'arena, 'txn>(
        &'db self,
        txn: &'txn mut RwTxn<'db>,
        query: &mut HVector<'arena>,
    ) -> Result<(), VectorError>
    where
        'db: 'arena,
        'arena: 'txn,
    {
        let entry_point = match self.get_entry_point_for_insert(txn) {
            Ok(ep) => ep,
            Err(_) => {
                self.set_entry_point(txn, query)?;
                query.set_distance(0.0);
                return Ok(());
            }
        };

        let insert_arena = bumpalo::Bump::new();
        let mut distance_cache = HashMap::new();
        let l = entry_point.level;
        let mut curr_ep = Candidate {
            id: entry_point.id,
            distance: self.get_or_compute_distance(
                txn,
                entry_point.id,
                query.data,
                &mut distance_cache,
            )?,
        };

        for level in (query.level + 1..=l).rev() {
            let mut nearest = self.search_level_for_insert(
                txn,
                query.data,
                curr_ep,
                1,
                level,
                &mut distance_cache,
                &insert_arena,
            )?;
            curr_ep = nearest.pop().ok_or(VectorError::VectorCoreError(
                "emtpy search result".to_string(),
            ))?;
        }

        for level in (0..=l.min(query.level)).rev() {
            let nearest = self.search_level_for_insert(
                txn,
                query.data,
                curr_ep,
                self.config.ef_construct,
                level,
                &mut distance_cache,
                &insert_arena,
            )?;
            curr_ep = *nearest.peek().ok_or(VectorError::VectorCoreError(
                "emtpy search result".to_string(),
            ))?;

            let neighbors = self.select_neighbors_for_insert(
                txn,
                query.data,
                nearest,
                level,
                true,
                &mut distance_cache,
                &insert_arena,
            )?;
            self.set_neighbours_from_ids(
                txn,
                query.id,
                neighbors.iter().map(|candidate| candidate.id),
                level,
            )?;

            for neighbor in neighbors {
                let id = neighbor.id;
                let e_conns = self.get_neighbor_candidates_for_insert(
                    txn,
                    id,
                    level,
                    query.data,
                    &mut distance_cache,
                    &insert_arena,
                )?;
                let e_new_conn = self.select_neighbors_for_insert(
                    txn,
                    query.data,
                    e_conns,
                    level,
                    true,
                    &mut distance_cache,
                    &insert_arena,
                )?;
                self.set_neighbours_from_ids(
                    txn,
                    id,
                    e_new_conn.iter().map(|candidate| candidate.id),
                    level,
                )?;
            }
        }

        if query.level > l {
            self.set_entry_point(txn, query)?;
        }

        Ok(())
    }

    pub(crate) fn rebuild_hnsw_index<'db>(
        &'db self,
        txn: &mut RwTxn<'db>,
    ) -> Result<(), VectorError> {
        let ids = self
            .vector_properties_db
            .iter(txn)?
            .map(|result| result.map(|(id, _)| id))
            .collect::<Result<Vec<_>, _>>()?;

        self.clear_hnsw_index(txn)?;

        for id in ids {
            let arena = bumpalo::Bump::new();
            let properties_bytes = match self.vector_properties_db.get(txn, &id)? {
                Some(bytes) => bytes.to_vec(),
                None => continue,
            };

            let metadata = VectorWithoutData::from_bincode_bytes(&arena, &properties_bytes, id)?;
            if metadata.deleted {
                continue;
            }

            let raw_bytes = self
                .vectors_db
                .get(txn, &Self::vector_key(id, 0))?
                .ok_or_else(|| VectorError::VectorNotFound(id.to_string()))?
                .to_vec();

            let mut vector = HVector::from_bincode_bytes(
                &arena,
                Some(&properties_bytes),
                &raw_bytes,
                id,
            )?;
            vector.level = self.get_new_level();
            self.index_existing_vector(txn, &mut vector)?;
        }

        Ok(())
    }
}

impl HNSW for VectorCore {
    fn search<'db, 'arena, 'txn, F>(
        &self,
        txn: &'txn RoTxn<'db>,
        query: &'arena [f64],
        k: usize,
        label: &'arena str,
        filter: Option<&'arena [F]>,
        should_trickle: bool,
        arena: &'arena bumpalo::Bump,
    ) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &RoTxn<'db>) -> bool,
        'db: 'arena,
        'arena: 'txn,
    {
        let query = HVector::from_slice(label, 0, query);
        // let temp_arena = bumpalo::Bump::new();

        let mut entry_point = self.get_entry_point(txn, label, arena)?;

        let ef = self.config.ef;
        let curr_level = entry_point.level;
        // println!("curr_level: {curr_level}");
        for level in (1..=curr_level).rev() {
            let mut nearest = self.search_level(
                txn,
                label,
                &query,
                &mut entry_point,
                ef,
                level,
                match should_trickle {
                    true => filter,
                    false => None,
                },
                arena,
            )?;
            if let Some(closest) = nearest.pop() {
                entry_point = closest;
            }
        }
        // println!("entry_point: {entry_point:?}");
        let candidates = self.search_level(
            txn,
            label,
            &query,
            &mut entry_point,
            ef,
            0,
            match should_trickle {
                true => filter,
                false => None,
            },
            arena,
        )?;
        // println!("candidates");
        let results = candidates.to_vec_with_filter::<F, true>(
            k,
            filter,
            label,
            txn,
            self.vector_properties_db,
            arena,
        )?;

        debug_println!("vector search found {} results", results.len());
        Ok(results)
    }

    fn insert<'db, 'arena, 'txn, F>(
        &'db self,
        txn: &'txn mut RwTxn<'db>,
        label: &'arena str,
        data: &'arena [f64],
        properties: Option<ImmutablePropertiesMap<'arena>>,
        _arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError>
    where
        F: Fn(&HVector<'arena>, &RoTxn<'db>) -> bool,
        'db: 'arena,
        'arena: 'txn,
    {
        let new_level = self.get_new_level();

        let mut query = HVector::from_slice(label, new_level, data);
        query.properties = properties;
        self.put_vector(txn, &query)?;
        self.index_existing_vector(txn, &mut query)?;

        debug_println!("vector inserted with id {}", query.id);
        Ok(query)
    }

    fn delete(&self, txn: &mut RwTxn, id: u128, arena: &bumpalo::Bump) -> Result<(), VectorError> {
        match self.get_vector_properties(txn, id, arena)? {
            Some(mut properties) => {
                debug_println!("properties: {properties:?}");
                if properties.deleted {
                    return Err(VectorError::VectorAlreadyDeleted(id.to_string()));
                }

                properties.deleted = true;
                self.put_vector_metadata(txn, id, &properties)?;
                debug_println!("vector deleted with id {}", &id);
                Ok(())
            }
            None => Err(VectorError::VectorNotFound(id.to_string())),
        }
    }
}
