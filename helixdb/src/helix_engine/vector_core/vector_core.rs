use crate::helix_engine::{
    types::VectorError,
    vector_core::{
        vector::HVector,
        hnsw::HNSW,
    },
};
use std::{
    cmp::Ordering,
    collections::{
        BinaryHeap,
        HashSet,
        HashMap,
    },
};
use heed3::{
    types::{Bytes, Unit},
    Database, Env, RoTxn, RwTxn,
};
use crate::protocol::value::Value;
use itertools::Itertools;
use rand::prelude::Rng;
use serde::{Deserialize, Serialize};
//use rayon::prelude::*;

const DB_VECTORS: &str = "vectors"; // for vector data (v:)
const DB_VECTOR_DATA: &str = "vector_data"; // for vector data (v:)

const DB_HNSW_OUT_EDGES: &str = "hnsw_out_nodes"; // for hnsw out node data
const VECTOR_PREFIX: &[u8] = b"v:";
const ENTRY_POINT_KEY: &str = "entry_point";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWConfig {
    pub m: usize,            // max num of bi-directional links per element
    pub m_max_0: usize,      // max num of links for lower layers
    pub ef_construct: usize, // size of the dynamic candidate list for construction
    pub m_l: f64,            // level generation factor
    pub ef: usize,           // search param, num of cands to search
}

impl HNSWConfig {
    pub fn new(m: Option<usize>, ef_construct: Option<usize>, ef: Option<usize>) -> Self {
        let m = m.unwrap_or(16);
        Self {
            m,
            m_max_0: 2 * m,
            ef_construct: ef_construct.unwrap_or(128),
            m_l: 1.0 / (m as f64).ln(),
            ef: ef.unwrap_or(768),
        }
    }
}

#[derive(PartialEq)]
struct Candidate {
    id: u128,
    distance: f64,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.distance.partial_cmp(&self.distance)
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

pub trait HeapOps<T> {
    /// Extend the heap with another heap
    /// Used because using `.extend()` does not keep the order
    fn extend_inord(&mut self, other: BinaryHeap<T>)
    where
        T: Ord;

    /// Take the top k elements from the heap
    /// Used because using `.iter()` does not keep the order
    fn take_inord(&mut self, k: usize) -> BinaryHeap<T>
    where
        T: Ord;

    /// Take the top k elements from the heap and return a vector
    fn to_vec(&mut self, k: usize) -> Vec<T>
    where
        T: Ord;

    /// Get the maximum element from the heap
    fn get_max(&self) -> Option<&T>
    where
        T: Ord;

    fn to_vec_with_filter<F>(&mut self, k: usize, filter: Option<&[F]>) -> Vec<T>
    where
        T: Ord,
        F: Fn(&T) -> bool;
}

impl<T> HeapOps<T> for BinaryHeap<T> {
    #[inline(always)]
    fn extend_inord(&mut self, mut other: BinaryHeap<T>)
    where
        T: Ord,
    {
        self.reserve(other.len());
        for item in other.drain() {
            self.push(item);
        }
    }

    #[inline(always)]
    fn take_inord(&mut self, k: usize) -> BinaryHeap<T>
    where
        T: Ord,
    {
        let mut result = BinaryHeap::with_capacity(k);
        for _ in 0..k {
            if let Some(item) = self.pop() {
                result.push(item);
            } else {
                break;
            }
        }
        result
    }

    #[inline(always)]
    fn to_vec(&mut self, k: usize) -> Vec<T>
    where
        T: Ord,
    {
        let mut result = Vec::with_capacity(k);
        for _ in 0..k {
            if let Some(item) = self.pop() {
                result.push(item);
            } else {
                break;
            }
        }
        result
    }

    #[inline(always)]
    fn get_max(&self) -> Option<&T>
    where
        T: Ord,
    {
        self.iter().max()
    }

    #[inline(always)]
    fn to_vec_with_filter<F>(&mut self, k: usize, filter: Option<&[F]>) -> Vec<T>
    where
        T: Ord,
        F: Fn(&T) -> bool,
    {
        let mut result = Vec::with_capacity(k);
        for _ in 0..k {
            // while pop check filters and pop until one passes
            while let Some(item) = self.pop() {
                if filter.is_none() || filter.unwrap().iter().all(|f| f(&item)) {
                    result.push(item);
                    break;
                }
            }
        }
        result
    }
}

pub struct VectorCore {
    pub vectors_db: Database<Bytes, Bytes>,
    pub vector_data_db: Database<Bytes, Bytes>,
    pub out_edges_db: Database<Bytes, Unit>,
    pub config: HNSWConfig,
}

impl VectorCore {
    pub fn new(env: &Env, txn: &mut RwTxn, config: HNSWConfig) -> Result<Self, VectorError> {
        let vectors_db = env.create_database(txn, Some(DB_VECTORS))?;
        let vector_data_db = env.create_database(txn, Some(DB_VECTOR_DATA))?;
        let out_edges_db = env.create_database(txn, Some(DB_HNSW_OUT_EDGES))?;

        Ok(Self {
            vectors_db,
            vector_data_db,
            out_edges_db,
            config,
        })
    }

    #[inline(always)]
    fn vector_key(id: u128, level: usize) -> Vec<u8> {
        [VECTOR_PREFIX, &id.to_be_bytes(), &level.to_be_bytes()].concat()
    }

    #[inline(always)]
    fn out_edges_key(source_id: u128, level: usize, sink_id: Option<u128>) -> Vec<u8> {
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
        // TODO: look at using the XOR shift algorithm for random number generation
        // Storing global rng will not be threadsafe or possible as thread rng needs to be mutable
        // Should instead using an atomic mutable seed and the XOR shift algorithm
        let mut rng = rand::rng();
        let r: f64 = rng.random::<f64>();
        let level = (-r.ln() * self.config.m_l).floor() as usize;
        level
    }

    #[inline]
    fn get_entry_point(&self, txn: &RoTxn) -> Result<HVector, VectorError> {
        let ep_id = self.vectors_db.get(txn, ENTRY_POINT_KEY.as_bytes())?;
        if let Some(ep_id) = ep_id {
            let mut arr = [0u8; 16];
            let len = std::cmp::min(ep_id.len(), 16);
            arr[..len].copy_from_slice(&ep_id[..len]);

            let ep = self
                .get_vector(txn, u128::from_be_bytes(arr), 0, true)
                .map_err(|_| VectorError::EntryPointNotFound)?;
            Ok(ep)
        } else {
            Err(VectorError::EntryPointNotFound)
        }
    }

    #[inline]
    fn set_entry_point(&self, txn: &mut RwTxn, entry: &HVector) -> Result<(), VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        self.vectors_db
            .put(txn, &entry_key, &entry.get_id().to_be_bytes())
            .map_err(VectorError::from)?;

        Ok(())
    }

    // #[inline(always)]
    // fn get_vector_(&self, txn: &RoTxn, id: u128) -> Result<Vec<f64>, VectorError> {
    #[inline(always)]
    fn put_vector(&self, txn: &mut RwTxn, vector: &HVector) -> Result<(), VectorError> {
        self.vectors_db
            .put(
                txn,
                &Self::vector_key(vector.get_id(), vector.get_level()),
                vector.to_bytes().as_ref(),
            )
            .map_err(VectorError::from)?;
        Ok(())
    }

    #[inline(always)]
    fn get_neighbors<F>(
        &self,
        txn: &RoTxn,
        id: u128,
        level: usize,
        filter: Option<&[F]>,
    ) -> Result<Vec<HVector>, VectorError>
    where
        F: Fn(&HVector) -> bool,
    {
        let out_key = Self::out_edges_key(id, level, None);
        let mut neighbors = Vec::with_capacity(self.config.m_max_0.min(512)); // TODO: why 512?

        let iter = self
            .out_edges_db
            .lazily_decode_data()
            .prefix_iter(txn, &out_key)?;

        let prefix_len = out_key.len();

        for result in iter {
            if let Ok((key, _)) = result { // TODO: fix here because not working at all
                let mut arr = [0u8; 16];
                let len = std::cmp::min(key.len(), 16);
                arr[..len].copy_from_slice(&key[prefix_len..(prefix_len+len)]);
                let neighbor_id = u128::from_be_bytes(arr);

                if neighbor_id != id {
                    if let Ok(vector) = self.get_vector(txn, neighbor_id, level, true) {
                        // TODO: look at implementing a macro that actually just runs each function rather than iterating through
                        if filter.is_none() || filter.unwrap().iter().all(|f| f(&vector)) {
                            neighbors.push(vector);
                        }
                    }
                }
            }
        }
        // neighbors.shrink_to_fit();

        Ok(neighbors)
    }

    #[inline(always)]
    fn set_neighbours<'a>(
        &self,
        txn: &mut RwTxn,
        id: u128,
        neighbors: &'a BinaryHeap<HVector>,
        level: usize,
    ) -> Result<(), VectorError> {
        let prefix = Self::out_edges_key(id, level, None);

        let mut keys_to_delete: HashSet<Vec<u8>> = self
            .out_edges_db
            .prefix_iter(txn, prefix.as_ref())?
            .filter_map(|result| result.ok().map(|(key, _)| key.to_vec()))
            .collect();

        neighbors
            .iter()
            .try_for_each(|neighbor| -> Result<(), VectorError> {
                let neighbor_id = neighbor.get_id();
                if neighbor_id == id {
                    return Ok(());
                }
                let out_key = Self::out_edges_key(id, level, Some(neighbor_id));
                keys_to_delete.remove(&out_key);
                self.out_edges_db.put(txn, &out_key, &())?;

                let in_key = Self::out_edges_key(neighbor_id, level, Some(id));
                keys_to_delete.remove(&in_key);
                self.out_edges_db.put(txn, &in_key, &())?;

                Ok(())
            })?;

        for key in keys_to_delete {
            self.out_edges_db.delete(txn, &key)?;
        }

        Ok(())
    }

    fn select_neighbors<'a, F>(
        &'a self,
        txn: &RoTxn,
        query: &'a HVector,
        mut cands: BinaryHeap<HVector>,
        level: usize,
        should_extend: bool,
        filter: Option<&[F]>,
    ) -> Result<BinaryHeap<HVector>, VectorError>
    where
        F: Fn(&HVector) -> bool,
    {
        let m: usize = if level == 0 {
            self.config.m
        } else {
            self.config.m_max_0
        };
        let mut visited: HashSet<String> = HashSet::new();
        if should_extend {
            let mut result = BinaryHeap::with_capacity(m * cands.len());
            for candidate in cands.iter() {
                for mut neighbor in self.get_neighbors(txn, candidate.get_id(), level, filter)? {
                    if visited.insert(neighbor.get_id().to_string()) { // TODO: NOT TO_STRING()
                        neighbor.set_distance(neighbor.distance_to(query)?);
                        if filter.is_none() || filter.unwrap().iter().all(|f| f(&neighbor)) {
                            result.push(neighbor);
                        }
                    }
                }
            }
            result.extend_inord(cands);
            Ok(result.take_inord(m))
        } else {
            Ok(cands.take_inord(m))
        }
    }

    fn search_level<'a, F>(
        &'a self,
        txn: &RoTxn,
        query: &'a HVector,
        entry_point: &'a mut HVector,
        ef: usize,
        level: usize,
        filter: Option<&[F]>,
    ) -> Result<BinaryHeap<HVector>, VectorError>
    where
        F: Fn(&HVector) -> bool,
    {
        let mut visited: HashSet<u128> = HashSet::new();
        let mut candidates: BinaryHeap<Candidate> = BinaryHeap::new();
        let mut results: BinaryHeap<HVector> = BinaryHeap::new();

        entry_point.set_distance(entry_point.distance_to(query)?);
        candidates.push(Candidate {
            id: entry_point.get_id(),
            distance: entry_point.get_distance(),
        });
        results.push(entry_point.clone());
        visited.insert(entry_point.get_id());

        while let Some(curr_cand) = candidates.pop() {
            if results.len() >= ef
                && results
                    .get_max()
                    .map_or(false, |f| curr_cand.distance > f.get_distance())
            {
                break;
            }

            let max_distance = if results.len() >= ef {
                results.get_max().map(|f| f.get_distance())
            } else {
                None
            };

            self.get_neighbors(txn, curr_cand.id, level, filter)?.into_iter()
                .filter(|neighbor| visited.insert(neighbor.get_id()))
                .filter_map(|mut neighbor| {
                    let distance = neighbor.distance_to(query).ok()?;
                    if max_distance.map_or(true, |max| distance < max) {
                        neighbor.set_distance(distance);
                        Some((neighbor, distance))
                    } else {
                        None
                    }
                })
            .for_each(|(neighbor, distance)| {
                candidates.push(Candidate {
                    id: neighbor.get_id(),
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
}

impl HNSW for VectorCore {
    #[inline(always)]
    fn get_vector(
        &self,
        txn: &RoTxn,
        id: u128,
        level: usize,
        with_data: bool,
    ) -> Result<HVector, VectorError> {
        let key = Self::vector_key(id, level);
        match self.vectors_db.get(txn, key.as_ref())? {
            Some(bytes) => {
                let vector = match with_data {
                    true => HVector::from_bytes(id, level, &bytes),
                    false => Ok(HVector::from_slice(level, vec![])),
                }?;
                Ok(vector)
            }
            None if level > 0 => self.get_vector(txn, id, 0, with_data),
            None => Err(VectorError::VectorNotFound(id.to_string())),
        }
    }

    fn search<F>(
        &self,
        txn: &RoTxn,
        query: &[f64],
        k: usize,
        filter: Option<&[F]>,
        should_trickle: bool,
    ) -> Result<Vec<HVector>, VectorError>
    where
        F: Fn(&HVector) -> bool,
    {
        let query = HVector::from_slice(0, query.to_vec());

        let mut entry_point = self.get_entry_point(txn)?;

        let ef = self.config.ef;
        let curr_level = entry_point.get_level();

        for level in (1..=curr_level).rev() {
            let mut nearest = self.search_level(
                txn,
                &query,
                &mut entry_point,
                1,
                level,
                match should_trickle {
                    true => filter,
                    false => None,
                },
            )?;
            if let Some(closest) = nearest.pop() {
                entry_point = closest;
            }
        }

        let mut candidates = self.search_level(
            txn,
            &query,
            &mut entry_point,
            ef,
            0,
            match should_trickle {
                true => filter,
                false => None,
            },
        )?;

        Ok(candidates.to_vec_with_filter(k, filter))
    }

    fn insert<F>(
        &self,
        txn: &mut RwTxn,
        data: &[f64],
        fields: Option<HashMap<String, Value>>,
    ) -> Result<HVector, VectorError>
    where
        F: Fn(&HVector) -> bool,
    {
        let new_level = self.get_new_level();

        let mut query = HVector::from_slice(0, data.to_vec());
        self.put_vector(txn, &query)?;

        query.level = new_level;
        if new_level > 0 {
            self.put_vector(txn, &query)?;
        }

        let entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                self.set_entry_point(txn, &query)?;
                query.set_distance(0.0);
                return Ok(query);
            }
        };

        let l = entry_point.get_level();
        let mut curr_ep = entry_point;
        for level in (new_level + 1..=l).rev() {
            let nearest = self.search_level::<F>(txn, &query, &mut curr_ep, 1, level, None)?;
            curr_ep = nearest.peek().unwrap().clone();
        }

        for level in (0..=l.min(new_level)).rev() {
            let nearest = self.search_level::<F>(
                txn,
                &query,
                &mut curr_ep,
                self.config.ef_construct,
                level,
                None,
            )?;

            curr_ep = nearest.peek().unwrap().clone();

            let neighbors = self.select_neighbors::<F>(txn, &query, nearest, level, true, None)?;

            self.set_neighbours(txn, query.get_id(), &neighbors, level)?;

            for e in neighbors {
                let id = e.get_id();
                let e_conns = self.get_neighbors::<F>(txn, id, level, None)?;
                if e_conns.len()
                    > if level == 0 {
                        self.config.m_max_0
                    } else {
                        self.config.m_max_0
                    }
                {
                    let e_conns = BinaryHeap::from(e_conns);
                    let e_new_conn =
                        self.select_neighbors::<F>(txn, &query, e_conns, level, true, None)?;
                    self.set_neighbours(txn, id, &e_new_conn, level)?;
                }
            }
        }

        if new_level > l {
            self.set_entry_point(txn, &query)?;
        }

        if let Some(fields) = fields {
            self.vector_data_db.put(
                txn,
                &query.get_id().to_be_bytes(),
                &bincode::serialize(&fields)?,
            )?;
        }
        Ok(query)
    }

    // TODO: unused used
    fn get_all_vectors(&self, txn: &RoTxn, level: Option<usize>) -> Result<Vec<HVector>, VectorError> {
        self.vectors_db
            .prefix_iter(txn, VECTOR_PREFIX)?
            .map(|result| {
                result.map_err(VectorError::from)
                    .and_then(|(_, value)| {
                        bincode::deserialize(&value)
                            .map_err(VectorError::from)
                    })
            })
            .filter_ok(|vector: &HVector| level.map_or(true, |l| vector.level == l))
            .collect()
    }
}

