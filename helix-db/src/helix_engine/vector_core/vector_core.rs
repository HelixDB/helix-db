use crate::{
    debug_println,
    helix_engine::{
        types::VectorError,
        vector_core::{
            hnsw::HNSW,
            txn::VecTxn,
            utils::{Candidate, HeapOps, VectorFilter},
            vector::HVector,
        },
    },
    protocol::value::Value,
};
use heed3::{
    Database, Env, RoTxn, RwTxn,
    types::{Bytes, Unit},
};
use itertools::Itertools;
use rand::prelude::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    rc::Rc,
};

const DB_VECTORS: &str = "vectors"; // for vector data (v:)
const DB_VECTOR_DATA: &str = "vector_data"; // for vector data (v:)
const DB_HNSW_EDGES: &str = "hnsw_out_nodes"; // for hnsw out node data
const VECTOR_PREFIX: &[u8] = b"v:";
const ENTRY_POINT_KEY: &str = "entry_point";

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
    pub vector_data_db: Database<Bytes, Bytes>,
    pub edges_db: Database<Bytes, Unit>,
    pub config: HNSWConfig,
}

impl VectorCore {
    pub fn new(env: &Env, txn: &mut RwTxn, config: HNSWConfig) -> Result<Self, VectorError> {
        let vectors_db = env.create_database(txn, Some(DB_VECTORS))?;
        let vector_data_db = env.create_database(txn, Some(DB_VECTOR_DATA))?;
        let edges_db = env.create_database(txn, Some(DB_HNSW_EDGES))?;

        Ok(Self {
            vectors_db,
            vector_data_db,
            edges_db,
            config,
        })
    }

    #[inline(always)]
    fn vector_key(id: u128, level: usize) -> Vec<u8> {
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
        // TODO: look at using the XOR shift algorithm for random number generation
        // Should instead using an atomic mutable seed and the XOR shift algorithm
        let mut rng = rand::rng();
        let r: f64 = rng.random::<f64>();
        (-r.ln() * self.config.m_l).floor() as usize
    }

    #[inline]
    fn get_entry_point_rc(&self, txn: &RoTxn) -> Result<Rc<HVector>, VectorError> {
        let ep_id = self.vectors_db.get(txn, ENTRY_POINT_KEY.as_bytes())?;
        if let Some(ep_id) = ep_id {
            let mut arr = [0u8; 16];
            let len = std::cmp::min(ep_id.len(), 16);
            arr[..len].copy_from_slice(&ep_id[..len]);

            let ep = self
                .get_vector(txn, u128::from_be_bytes(arr), 0, true)
                .map_err(|_| VectorError::EntryPointNotFound)?;
            Ok(Rc::new(ep))
        } else {
            Err(VectorError::EntryPointNotFound)
        }
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

    #[inline]
    fn set_entry_point_with_rc(&self, txn: &mut RwTxn, entry: Rc<HVector>) -> Result<(), VectorError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        self.vectors_db
            .put(txn, &entry_key, &entry.get_id().to_be_bytes())?;
        Ok(())
    }

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

    #[inline]
    fn _get_neighbors_with_vec_txn<F>(
        &self,
        txn: &mut VecTxn,
        id: u128,
        level: usize,
        filter: Option<&[F]>,
    ) -> Result<Vec<Rc<HVector>>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        if let Some(neighbors) = txn.get_neighbors(id, level) {
            return Ok(neighbors);
        }

        let out_key = Self::out_edges_key(id, level, None);
        let mut neighbors = Vec::with_capacity(self.config.m_max_0.min(self.config.min_neighbors));

        let iter = self
            .edges_db
            .lazily_decode_data()
            .prefix_iter(txn, &out_key)?;

        let prefix_len = out_key.len();

        for result in iter {
            let (key, _) = result?;

            let mut arr = [0u8; 16];
            let len = std::cmp::min(key.len(), 16);
            arr[..len].copy_from_slice(&key[prefix_len..(prefix_len + len)]);
            let neighbor_id = u128::from_be_bytes(arr);

            if neighbor_id == id {
                continue;
            }

            let vector = self.get_vector(txn, neighbor_id, level, false)?;

            let passes_filters = match filter {
                Some(filter_slice) => filter_slice.iter().all(|f| f(&vector, txn)),
                None => true,
            };

            if passes_filters {
                neighbors.push(Rc::new(vector));
            }
        }
        neighbors.shrink_to_fit();

        txn.insert_neighbors(id, level, &neighbors);

        Ok(neighbors)
    }

    #[inline(always)]
    fn _get_neighbors_with_lmdb_txn<F>(
        &self,
        txn: &RoTxn,
        id: u128,
        level: usize,
        filter: Option<&[F]>,
    ) -> Result<Vec<HVector>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let out_key = Self::out_edges_key(id, level, None);
        let mut neighbors = Vec::with_capacity(self.config.m_max_0.min(self.config.min_neighbors));

        let iter = self
            .edges_db
            .lazily_decode_data()
            .prefix_iter(txn, &out_key)?;

        let prefix_len = out_key.len();

        for result in iter {
            let (key, _) = result?;

            // TODO: fix here because not working at all
            let mut arr = [0u8; 16];
            let len = std::cmp::min(key.len(), 16);
            arr[..len].copy_from_slice(&key[prefix_len..(prefix_len + len)]);
            let neighbor_id = u128::from_be_bytes(arr);

            if neighbor_id == id {
                continue;
            }

            let vector = self.get_vector(txn, neighbor_id, level, false)?;

            let passes_filters = match filter {
                // TODO: look at implementing a macro that actually just runs each function rather than iterating through
                Some(filter_slice) => filter_slice.iter().all(|f| f(&vector, txn)),
                None => true,
            };

            if passes_filters {
                neighbors.push(vector);
            }

            //if let Ok(vector) = self.get_vector(txn, neighbor_id, level, true) {
            //    if filter.is_none() || filter.unwrap().iter().all(|f| f(&vector, txn)) {
            //        neighbors.push(vector);
            //    }
            //}
        }
        neighbors.shrink_to_fit();

        Ok(neighbors)
    }

    #[inline(always)]
    fn set_neighbours_with_vec_txn(
        &self,
        txn: &mut VecTxn,
        curr_vec: Rc<HVector>,
        neighbors: &BinaryHeap<Rc<HVector>>,
        level: usize,
    ) -> Result<(), VectorError> {
        txn.set_neighbors(curr_vec, level, neighbors);
        Ok(())
    }

    #[inline(always)]
    fn set_neighbours_with_lmdb_txn(
        &self,
        txn: &mut RwTxn,
        id: u128,
        neighbors: &BinaryHeap<HVector>,
        level: usize,
    ) -> Result<(), VectorError> {
        let prefix = Self::out_edges_key(id, level, None);

        let mut keys_to_delete: HashSet<Vec<u8>> = self
            .edges_db
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

    fn _select_neighbors_with_vec_txn<'a, F>(
        &'a self,
        txn: &'a mut VecTxn,
        query: &'a HVector,
        mut cands: BinaryHeap<Rc<HVector>>,
        level: usize,
        should_extend: bool,
        filter: Option<&[F]>,
    ) -> Result<BinaryHeap<Rc<HVector>>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let m = self.config.m;

        if !should_extend {
            return Ok(cands.take_inord(m));
        }

        /* 
        let start = std::time::Instant::now();

        
        let new = cands
            .iter()
            .map(|candidate| {
                let start = std::time::Instant::now();
                let neighbors = self
                    ._get_neighbors_with_vec_txn(txn, candidate.get_id(), level, filter)
                    .unwrap();
                println!("time taken get_neighbors: {:?}", start.elapsed());
                let ns = neighbors
                    .into_par_iter()
                    .filter_map(|mut neighbor| {
                        let distance = neighbor.distance_to(query).unwrap();
                        Arc::make_mut(&mut neighbor).set_distance(distance);
                        Some(neighbor)
                    })
                    .collect::<Vec<_>>();

                ns
            })
            .flatten()
            .collect::<Vec<_>>();

        println!("time taken calc_neighbors: {:?}", start.elapsed());

        let mut result = BinaryHeap::with_capacity(m * cands.len());
        let mut visited: std::collections::HashSet<u128> = std::collections::HashSet::new();
        for neighbor in new {
            if !visited.insert(neighbor.get_id()) {
                continue;
            }
            if filter.map_or(true, |fs| fs.iter().all(|f| f(&neighbor, &txn.txn))) {
                result.push(neighbor);
            }
        }
         */
        let mut visited: HashSet<u128> = HashSet::new();
        let mut result = BinaryHeap::with_capacity(m * cands.len());
        for candidate in cands.iter() {
            for mut neighbor in
                self._get_neighbors_with_vec_txn(txn, candidate.get_id(), level, filter)?
            {
                if !visited.insert(neighbor.get_id()) {
                    continue;
                }
                let distance = neighbor.distance_to(query)?;
                Rc::make_mut(&mut neighbor).set_distance(distance);
                

                if filter.is_none() || filter.unwrap().iter().all(|f| f(&neighbor, &txn.txn)) {
                    result.push(neighbor);
                }
            }
        }

        result.extend_inord(cands);
        Ok(result.take_inord(m))
    }

    fn _select_neighbors_with_lmdb_txn<'a, F>(
        &'a self,
        txn: &'a RoTxn,
        query: &'a HVector,
        mut cands: BinaryHeap<HVector>,
        level: usize,
        should_extend: bool,
        filter: Option<&[F]>,
    ) -> Result<BinaryHeap<HVector>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let m = self.config.m;

        if !should_extend {
            return Ok(cands.take_inord(m));
        }

        let mut visited: HashSet<u128> = HashSet::new();
        let mut result = BinaryHeap::with_capacity(m * cands.len());
        for candidate in cands.iter() {
            for mut neighbor in
                self._get_neighbors_with_lmdb_txn(txn, candidate.get_id(), level, filter)?
            {
                if !visited.insert(neighbor.get_id()) {
                    continue;
                }
                neighbor.set_distance(neighbor.distance_to(query)?);

                if filter.is_none() || filter.unwrap().iter().all(|f| f(&neighbor, &txn)) {
                    result.push(neighbor);
                }
            }
        }

        result.extend_inord(cands);
        Ok(result.take_inord(m))
    }

    fn _search_level_with_lmdb_txn<'a, F>(
        &'a self,
        txn: &'a RoTxn,
        query: &'a HVector,
        entry_point: &'a mut HVector,
        ef: usize,
        level: usize,
        filter: Option<&[F]>,
    ) -> Result<BinaryHeap<HVector>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
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
                    .is_none_or(|f| curr_cand.distance > f.get_distance())
            {
                break;
            }

            let max_distance = if results.len() >= ef {
                results.get_max().map(|f| f.get_distance())
            } else {
                None
            };

            self._get_neighbors_with_lmdb_txn(txn, curr_cand.id, level, filter)?
                .into_iter()
                .filter(|neighbor| visited.insert(neighbor.get_id()))
                .filter_map(|mut neighbor| {
                    let distance = neighbor.distance_to(query).ok()?;

                    if max_distance.is_none_or(|max| distance < max) {
                        neighbor.set_distance(distance);
                        // neighbor.set_distance(distance);
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

    fn _search_level_with_vec_txn<'a, F>(
        &'a self,
        txn: &'a mut VecTxn,
        query: &'a HVector,
        entry_point: &'a mut Rc<HVector>,
        ef: usize,
        level: usize,
        filter: Option<&[F]>,
    ) -> Result<BinaryHeap<Rc<HVector>>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let mut visited: HashSet<u128> = HashSet::new();
        let mut candidates: BinaryHeap<Candidate> = BinaryHeap::new();
        let mut results: BinaryHeap<Rc<HVector>> = BinaryHeap::new();

        let ep_distance = entry_point.distance_to(query)?;
        Rc::get_mut(entry_point).unwrap().set_distance(ep_distance);
        candidates.push(Candidate {
            id: entry_point.get_id(),
            distance: ep_distance,
        });
        results.push(entry_point.clone());
        visited.insert(entry_point.get_id());

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

            self._get_neighbors_with_vec_txn(txn, curr_cand.id, level, filter)?
                .into_iter()
                .filter(|neighbor| visited.insert(neighbor.get_id()))
                .filter_map(|neighbor| {
                    let distance = neighbor.distance_to(query).ok()?;

                    if max_distance.is_none_or(|max| distance < max) {
                        let mut neighbor = Rc::unwrap_or_clone(neighbor);
                        neighbor.set_distance(distance);
                        // neighbor.set_distance(distance);
                        Some((Rc::new(neighbor), distance))
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

    pub fn num_inserted_vectors(&self, txn: &RoTxn) -> Result<u64, VectorError> {
        Ok(self.vectors_db.len(txn)?)
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
                let mut vector = HVector::from_bytes(id, level, bytes)?;
                match with_data {
                    true => {
                        let properties: Option<HashMap<String, Value>> =
                            match self.vector_data_db.get(txn, &id.to_be_bytes())? {
                                Some(bytes) => {
                                    Some(bincode::deserialize(bytes).map_err(VectorError::from)?)
                                }
                                None => None,
                            };
                        vector.properties = properties;
                        Ok(vector)
                    }
                    false => Ok(vector),
                }
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
        label: &str,
        filter: Option<&[F]>,
        should_trickle: bool,
    ) -> Result<Vec<HVector>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let query = HVector::from_slice(0, query.to_vec());

        let mut entry_point = self.get_entry_point(txn)?;

        let ef = self.config.ef;
        let curr_level = entry_point.get_level();

        for level in (1..=curr_level).rev() {
            let mut nearest = self._search_level_with_lmdb_txn(
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

        let mut candidates = self._search_level_with_lmdb_txn(
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

        let results =
            candidates.to_vec_with_filter::<F, true>(k, filter, label, txn, self.vector_data_db)?;

        debug_println!("vector search found {} results", results.len());
        Ok(results)
    }

    fn search_with_vec_txn<F>(
        &self,
        txn: &mut VecTxn,
        query: &[f64],
        k: usize,
        label: &str,
        filter: Option<&[F]>,
        should_trickle: bool,
    ) -> Result<Vec<Rc<HVector>>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let query = HVector::from_slice(0, query.to_vec());

        let mut entry_point = self.get_entry_point_rc(txn.get_rtxn())?;

        let ef = self.config.ef;
        let curr_level = entry_point.get_level();

        for level in (1..=curr_level).rev() {
            let mut nearest = self._search_level_with_vec_txn(
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

        let mut candidates = self._search_level_with_vec_txn(
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

        let results = candidates.to_rc_vec_with_filter::<F, true>(
            k,
            filter,
            label,
            txn,
            self.vector_data_db,
        )?;

        debug_println!("vector search found {} results", results.len());
        Ok(results)
    }

    fn insert_with_vec_txn<F>(
        &self,
        txn: &mut VecTxn,
        data: &[f64],
        fields: Option<Vec<(String, Value)>>,
    ) -> Result<Rc<HVector>, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let new_level = self.get_new_level();

        let mut query = HVector::from_slice(0, data.to_vec());
        self.put_vector(txn.get_wtxn(), &query)?;
        query.level = new_level;
        if new_level > 0 {
            self.put_vector(txn.get_wtxn(), &query)?;
        }

        let entry_point = match self.get_entry_point_rc(txn.get_wtxn()) {
            Ok(ep) => ep,
            Err(_) => {
                self.set_entry_point(txn.get_wtxn(), &query)?;
                query.set_distance(0.0);

                if let Some(fields) = fields {
                    self.vector_data_db.put(
                        txn.get_wtxn(),
                        &query.get_id().to_be_bytes(),
                        &bincode::serialize(&fields)?,
                    )?;
                }
                return Ok(Rc::new(query));
            }
        };

        let l = entry_point.get_level();
        let mut curr_ep = entry_point;
        for level in (new_level + 1..=l).rev() {
            let nearest =
                self._search_level_with_vec_txn::<F>(txn, &query, &mut curr_ep, 1, level, None)?;
            curr_ep = nearest
                .peek()
                .ok_or(VectorError::VectorCoreError(
                    "emtpy search result".to_string(),
                ))?
                .clone();
        }

        let query = Rc::new(query);
        for level in (0..=l.min(new_level)).rev() {
            let nearest = self._search_level_with_vec_txn::<F>(
                txn,
                &query,
                &mut curr_ep,
                self.config.ef_construct,
                level,
                None,
            )?;
            curr_ep = nearest.peek().unwrap().clone();

            let neighbors =
                self._select_neighbors_with_vec_txn::<F>(txn, &query, nearest, level, true, None)?;
            self.set_neighbours_with_vec_txn(txn, Rc::clone(&query), &neighbors, level)?;
            for e in neighbors {
                let id = e.get_id();
                let e_conns =
                    BinaryHeap::from(self._get_neighbors_with_vec_txn::<F>(txn, id, level, None)?);
                let e_new_conn = self
                    ._select_neighbors_with_vec_txn::<F>(txn, &query, e_conns, level, true, None)?;
                // neighbor_updates.push((id, e_new_conn));
                self.set_neighbours_with_vec_txn(txn, e, &e_new_conn, level)?;
            }
        }

        if new_level > l {
            self.set_entry_point_with_rc(txn.get_wtxn(), Rc::clone(&query))?;
        }

        if let Some(fields) = fields {
            self.vector_data_db.put(
                txn.get_wtxn(),
                &query.get_id().to_be_bytes(),
                &bincode::serialize(&fields)?,
            )?;
        }

        debug_println!("vector inserted with id {}", query.get_id());
        Ok(query)
    }

    fn insert_with_lmdb_txn<F>(
        &self,
        txn: &mut RwTxn,
        data: &[f64],
        fields: Option<Vec<(String, Value)>>,
    ) -> Result<HVector, VectorError>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
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

                if let Some(fields) = fields {
                    self.vector_data_db.put(
                        txn,
                        &query.get_id().to_be_bytes(),
                        &bincode::serialize(&fields)?,
                    )?;
                }
                return Ok(query);
            }
        };

        let l = entry_point.get_level();
        let mut curr_ep = entry_point;
        for level in (new_level + 1..=l).rev() {
            let nearest =
                self._search_level_with_lmdb_txn::<F>(txn, &query, &mut curr_ep, 1, level, None)?;
            curr_ep = nearest
                .peek()
                .ok_or(VectorError::VectorCoreError(
                    "emtpy search result".to_string(),
                ))?
                .clone();
        }

        for level in (0..=l.min(new_level)).rev() {
            let nearest = self._search_level_with_lmdb_txn::<F>(
                txn,
                &query,
                &mut curr_ep,
                self.config.ef_construct,
                level,
                None,
            )?;
            curr_ep = nearest.peek().unwrap().clone();

            let neighbors =
                self._select_neighbors_with_lmdb_txn::<F>(txn, &query, nearest, level, true, None)?;
            self.set_neighbours_with_lmdb_txn(txn, query.get_id(), &neighbors, level)?;
            for e in &neighbors {
                let id = e.get_id();
                let e_conns =
                    BinaryHeap::from(self._get_neighbors_with_lmdb_txn::<F>(txn, id, level, None)?);
                let e_new_conn = self._select_neighbors_with_lmdb_txn::<F>(
                    txn, &query, e_conns, level, true, None,
                )?;
                // neighbor_updates.push((id, e_new_conn));
                self.set_neighbours_with_lmdb_txn(txn, id, &e_new_conn, level)?;
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

        debug_println!("vector inserted with id {}", query.get_id());
        Ok(query)
    }

    fn delete(&self, txn: &mut RwTxn, id: u128) -> Result<(), VectorError> {
        let properties: Option<HashMap<String, Value>> =
            match self.vector_data_db.get(txn, &id.to_be_bytes())? {
                Some(bytes) => Some(bincode::deserialize(bytes).map_err(VectorError::from)?),
                None => None,
            };

        debug_println!("properties: {properties:?}");
        if let Some(mut properties) = properties {
            if let Some(Value::Boolean(is_deleted)) = properties.get("is_deleted")
                && *is_deleted
            {
                return Err(VectorError::VectorAlreadyDeleted(id.to_string()));
            }

            properties.insert("is_deleted".to_string(), Value::Boolean(true));
            debug_println!("properties: {properties:?}");

            self.vector_data_db
                .put(txn, &id.to_be_bytes(), &bincode::serialize(&properties)?)?;
        } else {
            let mut n_properties: HashMap<String, Value> = HashMap::new();
            n_properties.insert("is_deleted".to_string(), Value::Boolean(true));
            debug_println!("properties: {n_properties:?}");

            self.vector_data_db
                .put(txn, &id.to_be_bytes(), &bincode::serialize(&n_properties)?)?;
        }

        debug_println!("vector deleted with id {}", &id);
        Ok(())
    }

    fn get_all_vectors(
        &self,
        txn: &RoTxn,
        level: Option<usize>,
    ) -> Result<Vec<HVector>, VectorError> {
        self.vectors_db
            .prefix_iter(txn, VECTOR_PREFIX)?
            .map(|result| {
                result
                    .map_err(VectorError::from)
                    .and_then(|(_, value)| bincode::deserialize(value).map_err(VectorError::from))
            })
            .filter_ok(|vector: &HVector| level.is_none_or(|l| vector.level == l))
            .collect()
    }
}
