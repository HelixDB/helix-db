use bincode::{deserialize, serialize};
use indexmap::IndexMap;
use rand::prelude::Rng;
use serde::{Deserialize, Serialize};
use heed3::{types::{Bytes, Unit}, Database, Env, RoTxn, RwTxn};
use std::{cmp::Ordering, collections::{BinaryHeap, HashSet}};
use crate::helix_engine::vector_core::{vector::HVector, pca::PCA};
use crate::helix_engine::{storage_core::storage_core::OUT_EDGES_PREFIX, types::VectorError};

const DB_VECTORS: &str = "vectors"; // for vector data (v:)
const DB_HNSW_OUT_EDGES: &str = "hnsw_out_nodes"; // for hnsw out node data

const VECTOR_PREFIX: &[u8] = b"v:";
const ENTRY_POINT_KEY: &str = "entry_point";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWConfig {
    pub m: usize,                 // max num of bi-directional links per element
    pub m_max: usize,             // max num of links for upper layers
    pub ef_construction: usize,   // size of the dynamic candidate list for construction
    pub ef_c: usize,              // ef_search factor (usually 10 so that 10*k)
    pub max_elements: usize,      // maximum number of elements in the index
    pub m_l: f64,                 // level generation factor
    pub max_level: usize,         // max number of levels in index
}

impl Default for HNSWConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max: 32,
            ef_construction: 512,
            ef_c: 10,
            max_elements: 1_000_000,
            m_l: 0.36,
            max_level: 5,
        }
    }
}

impl HNSWConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn optimized(n: usize) -> Self {
        let d = (10.0 + 20.0 * (10_000.0_f64.log10()/(n as f64).log10())).floor() as usize;
        let o_m = 5.max(48.min(d));
        //let o_m = (2.0 * (n as f64).ln().ceil()) as usize;
        Self {
            m: o_m,
            m_max: 2*o_m,
            ef_construction: 512,
            ef_c: 10,
            max_elements: n,
            m_l: 1.0/(o_m as f64).log10(),
            max_level: ((n as f64).log10()/(o_m as f64).log10()).floor() as usize,
        }
    }
}

#[derive(PartialEq)]
struct Candidate {
    id: String,
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

pub struct VectorCore {
    vectors_db: Database<Bytes, Bytes>,
    out_edges_db: Database<Bytes, Unit>,
    hnsw_config: HNSWConfig,
    dimensions: usize,
    pca: Option<PCA>,
}

impl VectorCore {
    pub fn new(
        env: &Env,
        txn: &mut RwTxn,
        config: Option<HNSWConfig>,
        dimensions: usize,
        pca: Option<PCA>,
    ) -> Result<Self, VectorError> {
        let vectors_db = env.create_database(txn, Some(DB_VECTORS))?;
        let out_edges_db = env.create_database(txn, Some(DB_HNSW_OUT_EDGES))?;
        let hnsw_config = config.unwrap_or_default();
        Ok(Self {
            vectors_db,
            out_edges_db,
            hnsw_config,
            dimensions,
            pca,
        })
    }

    #[inline(always)]
    fn vector_key(id: &str, level: usize) -> Vec<u8> {
        [
            VECTOR_PREFIX,
            id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
        ]
        .concat()
    }

    #[inline(always)]
    fn out_edges_key(source_id: &str, sink_id: &str, level: usize) -> Vec<u8> {
        [
            OUT_EDGES_PREFIX,
            source_id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
            b":",
            sink_id.as_bytes(),
        ]
        .concat()
    }

    #[inline]
    pub fn get_new_level(&self) -> usize {
        let mut rng = rand::rng();
        let level = (-rng.random::<f64>().ln()).floor() as usize;
        level.min(self.hnsw_config.max_level)
    }

    #[inline]
    fn get_entry_point(&self, txn: &RoTxn) -> Result<HVector, VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        let entry_point_bytes = match self.vectors_db.get(txn, entry_key.as_ref())? {
            Some(bytes) => bytes,
            None => return Err(VectorError::EntryPointNotFound),
        };

        let vector: HVector = deserialize(entry_point_bytes)
            .map_err(|_| VectorError::InvalidEntryPoint)?;

        Ok(vector)
    }

    #[inline]
    fn set_entry_point(&self, txn: &mut RwTxn, entry: &HVector) -> Result<(), VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        self.vectors_db
            .put(txn, &entry_key, &serialize(entry)?)
            .map_err(VectorError::from)?;

        Ok(())
    }

    #[inline(always)]
    fn get_vector(&self, txn: &RoTxn, id: &str, level: usize) -> Result<HVector, VectorError> {
        let key = Self::vector_key(id, level);
        match self.vectors_db.get(txn, key.as_ref())? {
            Some(bytes) => deserialize(&bytes).map_err(VectorError::from),
            None if level > 0 => self.get_vector(txn, id, 0),
            None => Err(VectorError::VectorNotFound(id.to_string())),
        }
    }

    #[inline(always)]
    fn put_vector(&self, txn: &mut RwTxn, vector: &HVector) -> Result<(), VectorError> {
        self.vectors_db
            .put(
                txn,
                &Self::vector_key(vector.get_id(), vector.get_level()),
                &serialize(vector)?,
            )
            .map_err(VectorError::from)
    }

    /// fit pca to downsize all vecs
    pub fn pca_fit(&mut self) -> Result<(), VectorError> {
        if self.pca.is_none() {
            return Err(VectorError::ConversionError("no pca set".to_string()));
        }

        Ok(())
    }

    /// transform all data stored
    pub fn pca_transform(&mut self, txn: &mut RwTxn) -> Result<(), VectorError> {
        if self.pca.is_none() {
            return Err(VectorError::ConversionError("no pca set".to_string()));
        }

        Ok(())
    }

    #[inline(always)]
    fn get_neighbors(&self, txn: &RoTxn, id: &str, level: usize) -> Result<Vec<HVector>, VectorError> {
        let out_key = Self::out_edges_key(id, "", level);

        let iter = self
            .out_edges_db
            .lazily_decode_data()
            .prefix_iter(&txn, &out_key)?;

        let mut neighbors = Vec::with_capacity(512);
        let prefix_len = OUT_EDGES_PREFIX.len() + id.len() + 1 + level.to_string().len() + 1;

        for result in iter {
            // key = source_id:sink_id
            let (key, _) = result?;
            let neighbor_id = std::str::from_utf8(&key[prefix_len..])?;
            if neighbor_id == id {
                continue;
            }
            neighbors.push(self.get_vector(txn, neighbor_id, level)?); // TODO: can this be better
        }

        Ok(neighbors)
    }

    #[inline(always)]
    fn set_neighbours(&self, txn: &mut RwTxn, id: &str, neighbors: &BinaryHeap<HVector>, level: usize) -> Result<(), VectorError> {
        neighbors
            .iter()
            .try_for_each(|neighbor| -> Result<(), VectorError> {
                let neighbor_id = neighbor.get_id();
                if neighbor_id == id {
                    return Ok(());
                }
                let out_key = Self::out_edges_key(id, neighbor_id, level);
                let in_key = Self::out_edges_key(neighbor_id, id, level);

                self.out_edges_db.put(txn, &out_key, &())?;
                self.out_edges_db.put(txn, &in_key, &())?;
                Ok(())
            })?;
        Ok(())
    }

    fn select_neighbors(
        &self,
        txn: &RoTxn,
        candidates: &BinaryHeap<HVector>,
        level: usize,
        extend_cands: bool,
        _keep_prund_cands: bool, // TODO: we remove this option for clarity (but in paper is there)
    ) -> Result<BinaryHeap<HVector>, VectorError> {
        let m = if level == 0 { self.hnsw_config.m } else { self.hnsw_config.m_max };

        let mut all_candidates = IndexMap::new();
        for candidate in candidates {
            all_candidates.insert(candidate.get_id().to_string(), candidate.clone());
            if extend_cands {
                for neighbor in self.get_neighbors(txn, candidate.get_id(), level)? {
                    all_candidates.entry(neighbor.get_id().to_string()).or_insert(neighbor);
                }
            }
        }

        let mut sorted_candidates: Vec<_> = all_candidates.into_iter().map(|(_id, vec)| vec).collect();
        sorted_candidates.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap_or(Ordering::Equal));

        let selected = sorted_candidates.into_iter().take(m);
        let mut neighbor_heap = BinaryHeap::with_capacity(m);
        for candidate in selected {
            neighbor_heap.push(candidate);
        }

        Ok(neighbor_heap)
    }

    fn search_level(
        &self,
        txn: &RoTxn,
        query: &HVector,
        entry_point: &HVector,
        ef: usize,
        level: usize,
    ) -> Result<BinaryHeap<HVector>, VectorError> {
        let mut visited: HashSet<String> = HashSet::new();
        let mut candidates: BinaryHeap<Candidate> = BinaryHeap::new();
        let mut results: BinaryHeap<HVector> = BinaryHeap::new();

        candidates.push(Candidate {
            id: entry_point.get_id().to_string(),
            distance: entry_point.distance,
        });
        results.push(entry_point.clone());
        visited.insert(entry_point.get_id().to_string());

        while !candidates.is_empty() {
            let curr_cand = candidates.pop().unwrap();

            if results.len() >= ef
                && results
                    .peek()
                    .map_or(false, |f| curr_cand.distance > f.distance)
            {
                break;
            }

            let neighbors = self.get_neighbors(txn, &curr_cand.id, level)?;

            for mut neighbor in neighbors {
                if !visited.contains(neighbor.get_id()) {
                    visited.insert(neighbor.get_id().to_string());

                    let distance = neighbor.distance_to(query);

                    candidates.push(Candidate {
                        id: neighbor.get_id().to_string(),
                        distance,
                    });
                    if results.len() < ef || distance < results.peek().unwrap().distance {
                        neighbor.distance = distance;
                        results.push(neighbor.clone());

                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn search(&self, txn: &RoTxn, query: &[f64], k: usize) -> Result<Vec<HVector>, VectorError> {
        // TODO: run through pca here if exists
        let query = HVector::from_slice("".to_string(), 0, query.to_vec());

        let mut entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                return Err(VectorError::EntryPointNotFound);
            }
        };

        let ef_search = self.hnsw_config.ef_c * k;
        let curr_level = entry_point.get_level();

        for level in (1..=curr_level).rev() {
            let nearest = self.search_level(txn, &query, &mut entry_point, ef_search, level)?;
            if !nearest.is_empty() {
                std::mem::replace(&mut entry_point, nearest.peek().unwrap().clone()); // TODO: do better (no clone)
            }
        }

        let candidates = self.search_level(txn, &query, &mut entry_point, ef_search, 0)?; // TODO: if we get nothing, add a change in precision mechanism for ef

        let mut results: Vec<HVector> = Vec::with_capacity(candidates.len());
        candidates.iter().for_each(|c| {
            let mut n_c = c.clone();
            n_c.distance = n_c.distance_to(&query);
            results.push(n_c);
        });

        results.truncate(k);
        Ok(results)
    }

    // paper: https://arxiv.org/pdf/1603.09320
    pub fn insert(&self, txn: &mut RwTxn, data: &[f64]) -> Result<HVector, VectorError> {
        let id = uuid::Uuid::new_v4().as_simple().to_string();
        let new_level = self.get_new_level();

        let mut data_query = HVector::from_slice(id.clone(), 0, data.to_vec());
        //if self.pca.is_none() {
        //    self.pca.transform()
        //}

        self.put_vector(txn, &data_query)?;
        data_query.level = new_level;
        self.put_vector(txn, &data_query)?;

        let entry_point = match self.get_entry_point(txn) {
            Ok(ep) => ep,
            Err(_) => {
                let mut entry_point = HVector::from_slice(id.to_string(), new_level, data.to_vec());
                entry_point.distance = 0.0;
                self.set_entry_point(txn, &entry_point)?;
                entry_point
            }
        };

        let l = entry_point.get_level();
        let mut curr_ep = entry_point;
        for level in (new_level + 1..=l).rev() {
            let nearest = self.search_level(txn, &data_query, &curr_ep, 1, level)?;
            curr_ep = nearest.peek().unwrap().clone();
        }

        for level in (0..=l.min(new_level)).rev() {
            let nearest = self.search_level(txn, &data_query, &curr_ep, self.hnsw_config.ef_construction, level)?;
            let neighbors = self.select_neighbors(txn, &nearest, level, true, true)?;

            self.set_neighbours(txn, &data_query.get_id(), &neighbors, level)?;
            for e in neighbors {
                let e_conn = BinaryHeap::from(self.get_neighbors(txn, e.get_id(), level)?);
                if e_conn.len() > self.hnsw_config.m_max {
                    let e_new_conn = self.select_neighbors(txn, &e_conn, level, true, true)?;
                    self.set_neighbours(txn, e.get_id(), &e_new_conn, level)?;
                }
            }
        }

        if new_level > l {
            self.set_entry_point(txn, &data_query)?;
        }

        Ok(data_query)
    }

    pub fn get_all_vectors(&self, txn: &RoTxn) -> Result<Vec<HVector>, VectorError> {
        let mut vectors = Vec::new();

        let prefix_iter = self.vectors_db.prefix_iter(txn, VECTOR_PREFIX)?;
        for result in prefix_iter {
            let (_, value) = result?;
            let vector: HVector = deserialize(&value)?;
            vectors.push(vector);
        }
        Ok(vectors)
    }

    pub fn load_data(&mut self, txn: &mut RwTxn, vectors: Vec<(String, Vec<f64>)>) -> Result<(), VectorError> {
        for (_, data) in vectors.iter() {
            self.insert(txn, data).unwrap();
        }
        Ok(())
    }
}
