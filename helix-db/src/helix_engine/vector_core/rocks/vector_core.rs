use super::binary_heap::BinaryHeap;
use crate::{
    debug_println,
    helix_engine::{
        storage_core::Txn,
        types::VectorError,
        vector_core::{
            rocks::{
                hnsw::HNSW,
                utils::{Candidate, HeapOps, VectorFilter},
            },
            vector::HVector,
            vector_without_data::VectorWithoutData,
        },
    },
    utils::properties::ImmutablePropertiesMap,
};
use rand::prelude::Rng;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashSet, sync::Arc};

pub const ENTRY_POINT_KEY: &[u8] = b"entry_point";
const EDGE_LENGTH: usize = 17;

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
    pub db: Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>>,
    pub config: HNSWConfig,
}

#[repr(u8)]
enum EdgeOp {
    Add,
    Remove,
}

impl EdgeOp {
    fn encode(kind: EdgeOp, bytes: &[u8]) -> [u8; 18] {
        let mut buf = [0u8; 18];
        buf[0] = kind as u8;
        buf[1..18].copy_from_slice(bytes);
        buf
    }

    fn decode(bytes: &[u8]) -> Option<(Self, [u8; 17])> {
        if bytes.len() != 18 {
            return None;
        }
        let kind = match bytes[0] {
            0 => Self::Add,
            1 => Self::Remove,
            _ => return None,
        };
        Some((kind, bytes[1..18].try_into().unwrap()))
    }
}

// TODO: use something similar to immutable map with SIMD keys. Is fine for now
fn remove(bytes: &mut Vec<u8>, target: [u8; 17]) {
    let step = target.len();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index..index + step] == target {
            bytes.drain(index..index + step);
        }
        index += step;
    }
}

fn insert(bytes: &mut Vec<u8>, target: [u8; 17]) {
    let step = target.len();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index..index + step] == target {
            return;
        }
        index += step;
    }
    bytes.extend_from_slice(&target);
}

fn hnsw_edges_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    let mut new_edges = Vec::with_capacity(existing.map(|e| (e.len() / 17) * 2).unwrap_or(0));
    new_edges.clear();
    new_edges.extend_from_slice(existing.unwrap_or(&[]));
    for op in operands {
        if let Some((kind, bytes)) = EdgeOp::decode(op) {
            match kind {
                EdgeOp::Add => insert(&mut new_edges, bytes),
                EdgeOp::Remove => remove(&mut new_edges, bytes),
            }
        }
    }
    Some(new_edges)
}

impl VectorCore {
    // Helper methods to get column family handles on-demand
    #[inline(always)]
    pub fn cf_vectors(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.cf_handle("vectors").unwrap()
    }

    #[inline(always)]
    pub fn cf_vector_properties(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.cf_handle("vector_data").unwrap()
    }

    #[inline(always)]
    pub fn cf_edges(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.cf_handle("hnsw_edges").unwrap()
    }

    #[inline(always)]
    pub fn cf_ep(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.cf_handle("ep").unwrap()
    }

    pub fn new(
        db: Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>>,
        config: HNSWConfig,
    ) -> Result<Self, VectorError> {
        Ok(Self { db, config })
    }

    /// VECTOR KEY STRUCTURE
    ///
    /// [u128 uuid] -> [<f64/f32/f16/binary>; dimension]
    pub(crate) fn vector_cf_options() -> rocksdb::Options {
        let mut options = rocksdb::Options::default();
        options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(16));
        options
    }

    /// VECTOR PROPERTY KEY STRUCTURE
    ///
    /// [u128 uuid] -> [<f64/f32/f16/binary>; dimension]
    pub(crate) fn vector_properties_cf_options() -> rocksdb::Options {
        let mut options = rocksdb::Options::default();
        options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(16));
        options
    }

    /// VECTOR EDGE KEY STRUCTURE
    ///
    /// [u128 uuid : level u8] -> [u128 uuid, level u8]
    pub(crate) fn vector_edges_cf_options() -> rocksdb::Options {
        let mut options = rocksdb::Options::default();
        options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(17));
        options.set_merge_operator_associative("hnsw_edges", hnsw_edges_merge);
        options
    }

    /// Vector key: [v, id, ]
    #[inline(always)]
    pub fn vector_key(id: u128) -> [u8; 16] {
        id.to_be_bytes()
    }

    /// edges key: [u128 uuid : level u8] -> [level u8, u128 uuid]
    #[inline(always)]
    pub fn edges_key(source_id: u128, level: u8) -> [u8; 17] {
        let mut key = [0u8; 17];
        key[..16].copy_from_slice(&source_id.to_be_bytes());
        key[16] = level;
        key
    }

    #[inline]
    fn get_new_level(&self) -> u8 {
        let mut rng = rand::rng();
        let r: f64 = rng.random::<f64>();
        (-r.ln() * self.config.m_l).floor() as u8
    }

    #[inline]
    fn get_entry_point<'db, 'arena: 'txn, 'txn>(
        &self,
        txn: &'txn Txn<'db>,
        label: &'arena str,
        arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError> {
        let cf = self.cf_ep();
        let ep_id = txn.get_pinned_cf(&cf, ENTRY_POINT_KEY)?;
        if let Some(ep_id) = ep_id {
            let mut arr = [0u8; 16];
            let len = std::cmp::min(ep_id.len(), 16);
            arr[..len].copy_from_slice(&ep_id[..len]);

            let ep = self
                .get_raw_vector_data(txn, u128::from_be_bytes(arr), label, arena)
                .map_err(|_| VectorError::EntryPointNotFound)?;
            Ok(ep)
        } else {
            Err(VectorError::EntryPointNotFound)
        }
    }

    #[inline]
    fn set_entry_point<'db>(&self, txn: &Txn<'db>, entry: &HVector) -> Result<(), VectorError> {
        let cf = self.cf_ep();
        txn.put_cf(&cf, ENTRY_POINT_KEY, entry.id.to_be_bytes())
            .map_err(VectorError::from)?;
        Ok(())
    }

    #[inline(always)]
    pub fn put_vector<'db, 'arena>(
        &self,
        txn: &Txn<'db>,
        vector: &HVector<'arena>,
    ) -> Result<(), VectorError> {
        let cf_vectors = self.cf_vectors();
        let cf_props = self.cf_vector_properties();
        txn.put_cf(
            &cf_vectors,
            vector.id.to_be_bytes(),
            vector.vector_data_to_bytes()?,
        )?;
        txn.put_cf(
            &cf_props,
            vector.id.to_be_bytes(),
            &bincode::serialize(&vector)?,
        )?;
        Ok(())
    }

    #[inline(always)]
    fn get_neighbors<'db, 'arena: 'txn, 'txn, F>(
        &self,
        txn: &'txn Txn<'db>,
        label: &'arena str,
        id: u128,
        level: u8,
        filter: Option<&[F]>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &Txn<'db>) -> bool,
    {
        let out_key = Self::edges_key(id, level);
        let mut neighbors = bumpalo::collections::Vec::with_capacity_in(
            self.config.m_max_0.min(self.config.min_neighbors),
            arena,
        );

        let cf_edges = self.cf_edges();
        let edges = txn.get_pinned_cf(&cf_edges, out_key)?;

        if let Some(value) = edges {
            let edges = Self::decode_edges(&value);
            for edge_entry in edges {
                let neighbor_id = u128::from_be_bytes(edge_entry[..16].try_into().unwrap());
                if neighbor_id == id {
                    continue;
                }

                let level = edge_entry[16];
                let mut vector = self.get_raw_vector_data(txn, neighbor_id, label, arena)?;
                vector.level = level as usize; // TODO modify vector to take level.
                let passes_filters = match filter {
                    Some(filter_slice) => filter_slice.iter().all(|f| f(&vector, txn)),
                    None => true,
                };

                if passes_filters {
                    neighbors.push(vector);
                }
            }
        }

        neighbors.shrink_to_fit();

        Ok(neighbors)
    }

    #[inline]
    fn edge_entry(id: u128, level: u8) -> [u8; EDGE_LENGTH] {
        let mut buf = [0u8; EDGE_LENGTH];
        buf[..16].copy_from_slice(&id.to_be_bytes());
        buf[16] = level;
        buf
    }

    fn decode_edges(bytes: &[u8]) -> Vec<[u8; EDGE_LENGTH]> {
        bytes
            .chunks_exact(EDGE_LENGTH)
            .map(|chunk| {
                let mut entry = [0u8; EDGE_LENGTH];
                entry.copy_from_slice(chunk);
                entry
            })
            .collect()
    }
    #[inline(always)]
    fn set_neighbours<'db, 'arena: 'txn, 'txn>(
        &self,
        txn: &'txn Txn<'db>,
        id: u128,
        neighbors: &BinaryHeap<'arena, HVector<'arena>>,
        level: u8,
    ) -> Result<(), VectorError> {
        let key = Self::edges_key(id, level);
        let mut desired = Vec::with_capacity(neighbors.len());

        // get desired neighbors
        for neighbor in neighbors.iter() {
            if neighbor.id == id {
                continue;
            }
            // Store the neighbor id + whichever level you want to persist.
            desired.push(Self::edge_entry(neighbor.id, neighbor.level as u8));
        }
        desired.sort_unstable();
        desired.dedup();

        let cf_edges = self.cf_edges();

        // then determine the changes needed
        let mut existing = txn
            .get_pinned_cf(&cf_edges, key)?
            .map(|buf| Self::decode_edges(buf.as_ref()))
            .unwrap_or_default();
        existing.sort_unstable();

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < existing.len() && j < desired.len() {
            match existing[i].cmp(&desired[j]) {
                Ordering::Less => {
                    removes.push(existing[i]);
                    i += 1;
                }
                Ordering::Greater => {
                    adds.push(desired[j]);
                    j += 1;
                }
                Ordering::Equal => {
                    i += 1;
                    j += 1;
                }
            }
        }
        removes.extend_from_slice(&existing[i..]);
        adds.extend_from_slice(&desired[j..]);

        let reciprocal = Self::edge_entry(id, level);

        for entry in removes {
            let operand = EdgeOp::encode(EdgeOp::Remove, &entry);
            let neighbor_key = Self::edges_key(
                u128::from_be_bytes(entry[..16].try_into().unwrap()),
                entry[16],
            );
            let reciprocal_operand = EdgeOp::encode(EdgeOp::Remove, &reciprocal);
            Self::merge_edge_pair(
                txn,
                &cf_edges,
                key,
                operand,
                neighbor_key,
                reciprocal_operand,
            )?;
        }

        for entry in adds {
            let operand = EdgeOp::encode(EdgeOp::Add, &entry);
            let neighbor_key = Self::edges_key(
                u128::from_be_bytes(entry[..16].try_into().unwrap()),
                entry[16],
            );
            let reciprocal_operand = EdgeOp::encode(EdgeOp::Add, &reciprocal);
            Self::merge_edge_pair(
                txn,
                &cf_edges,
                key,
                operand,
                neighbor_key,
                reciprocal_operand,
            )?;
        }

        Ok(())
    }

    #[inline(always)]
    fn merge_edge_pair(
        txn: &Txn<'_>,
        cf_edges: &Arc<rocksdb::BoundColumnFamily<'_>>,
        self_key: [u8; EDGE_LENGTH],
        self_operand: [u8; 18],
        neighbor_key: [u8; EDGE_LENGTH],
        neighbor_operand: [u8; 18],
    ) -> Result<(), rocksdb::Error> {
        if self_key <= neighbor_key {
            txn.merge_cf(cf_edges, self_key, self_operand)?;
            txn.merge_cf(cf_edges, neighbor_key, neighbor_operand)?;
        } else {
            txn.merge_cf(cf_edges, neighbor_key, neighbor_operand)?;
            txn.merge_cf(cf_edges, self_key, self_operand)?;
        }
        Ok(())
    }

    fn select_neighbors<'db, 'arena: 'txn, 'txn, 's, F>(
        &self,
        txn: &'txn Txn<'db>,
        label: &'arena str,
        query: &'s HVector<'arena>,
        mut cands: BinaryHeap<'arena, HVector<'arena>>,
        level: u8,
        should_extend: bool,
        filter: Option<&[F]>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<BinaryHeap<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &Txn<'db>) -> bool,
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

    fn search_level<'db, 'arena: 'txn, 'txn, 'q, F>(
        &self,
        txn: &'txn Txn<'db>,
        label: &'arena str,
        query: &'q HVector<'arena>,
        entry_point: &'q mut HVector<'arena>,
        ef: usize,
        level: u8,
        filter: Option<&[F]>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<BinaryHeap<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &Txn<'db>) -> bool,
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

    // Not possible to implement in RocksDB unless iterating over all keys
    pub fn num_inserted_vectors<'db>(&self, _txn: &Txn<'db>) -> Result<u64, VectorError> {
        unimplemented!()
    }

    #[inline]
    pub fn get_vector_properties<'db, 'arena: 'txn, 'txn>(
        &self,
        txn: &'txn Txn<'db>,
        id: u128,
        arena: &'arena bumpalo::Bump,
    ) -> Result<Option<VectorWithoutData<'arena>>, VectorError> {
        let cf = self.cf_vector_properties();
        let vector: Option<VectorWithoutData<'arena>> =
            match txn.get_pinned_cf(&cf, id.to_be_bytes())? {
                Some(bytes) => Some(VectorWithoutData::from_bincode_bytes(arena, &bytes, id)?),
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
    pub fn get_full_vector<'db, 'arena>(
        &self,
        txn: &Txn<'db>,
        id: u128,
        arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError> {
        let key = Self::vector_key(id);
        let cf_vectors = self.cf_vectors();
        let cf_props = self.cf_vector_properties();
        let vector_data_bytes =
            txn.get_pinned_cf(&cf_vectors, key)?
                .ok_or(VectorError::VectorNotFound(
                    uuid::Uuid::from_u128(id).to_string(),
                ))?;

        let properties_bytes = txn.get_pinned_cf(&cf_props, key)?;

        let vector = HVector::from_bincode_bytes(
            arena,
            properties_bytes.as_deref(),
            &vector_data_bytes,
            id,
        )?;
        if vector.deleted {
            return Err(VectorError::VectorDeleted);
        }
        Ok(vector)
    }

    #[inline(always)]
    pub fn get_raw_vector_data<'db, 'arena: 'txn, 'txn>(
        &self,
        txn: &'txn Txn<'db>,
        id: u128,
        label: &'arena str,
        arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError> {
        let cf = self.cf_vectors();
        let vector_data_bytes =
            txn.get_pinned_cf(&cf, Self::vector_key(id))?
                .ok_or(VectorError::VectorNotFound(
                    uuid::Uuid::from_u128(id).to_string(),
                ))?;

        HVector::from_raw_vector_data(arena, &vector_data_bytes, label, id)
    }
}

impl HNSW for VectorCore {
    fn search<'db, 'arena, 'txn, F>(
        &self,
        txn: &'txn Txn<'db>,
        query: &'arena [f64],
        k: usize,
        label: &'arena str,
        filter: Option<&'arena [F]>,
        should_trickle: bool,
        arena: &'arena bumpalo::Bump,
    ) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &Txn<'db>) -> bool,
        'db: 'arena,
        'arena: 'txn,
    {
        let query = HVector::from_slice(label, 0, query);
        // let temp_arena = bumpalo::Bump::new();

        let mut entry_point = self.get_entry_point(txn, label, arena)?;

        let ef = self.config.ef;
        let curr_level = entry_point.level as u8;
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
            Arc::clone(&self.cf_vector_properties()),
            arena,
        )?;

        debug_println!("vector search found {} results", results.len());
        Ok(results)
    }

    fn insert<'db, 'arena, 'txn, F>(
        &self,
        txn: &'txn Txn<'db>,
        label: &'arena str,
        data: &'arena [f64],
        properties: Option<ImmutablePropertiesMap<'arena>>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<HVector<'arena>, VectorError>
    where
        F: Fn(&HVector<'arena>, &Txn<'db>) -> bool,
        'db: 'arena,
        'arena: 'txn,
    {
        let new_level = self.get_new_level();

        let mut query = HVector::from_slice(label, 0, data);
        query.properties = properties;
        self.put_vector(txn, &query).map_err(|err| {
            VectorError::VectorCoreError(format!("Failed to put vector: {} {}", line!(), err))
        })?;

        query.level = new_level as usize; // TODO: change vector to take level as u8

        let entry_point = match self.get_entry_point(txn, label, arena) {
            Ok(ep) => ep,
            Err(_) => {
                // TODO: use proper error handling
                self.set_entry_point(txn, &query).map_err(|err| {
                    VectorError::VectorCoreError(format!(
                        "Failed to set entry point: {} {}",
                        line!(),
                        err
                    ))
                })?;
                query.set_distance(0.0);

                return Ok(query);
            }
        };

        let l = entry_point.level as u8; // TODO Change
        let mut curr_ep = entry_point;
        for level in (new_level + 1..=l).rev() {
            let mut nearest =
                self.search_level::<F>(txn, label, &query, &mut curr_ep, 1, level, None, arena)?;
            curr_ep = nearest.pop().ok_or(VectorError::VectorCoreError(
                "emtpy search result".to_string(),
            ))?;
        }

        for level in (0..=l.min(new_level)).rev() {
            let nearest = self.search_level::<F>(
                txn,
                label,
                &query,
                &mut curr_ep,
                self.config.ef_construct,
                level,
                None,
                arena,
            )?;
            curr_ep = *nearest.peek().ok_or(VectorError::VectorCoreError(
                "emtpy search result".to_string(),
            ))?;

            let neighbors =
                self.select_neighbors::<F>(txn, label, &query, nearest, level, true, None, arena)?;

            self.set_neighbours(txn, query.id, &neighbors, level)
                .map_err(|err| {
                    VectorError::VectorCoreError(format!(
                        "Failed to set neighbors: {} {}",
                        line!(),
                        err
                    ))
                })?;

            for e in neighbors {
                let id = e.id;
                let e_conns = BinaryHeap::from(
                    arena,
                    self.get_neighbors::<F>(txn, label, id, level, None, arena)?,
                );
                let e_new_conn = self
                    .select_neighbors::<F>(txn, label, &query, e_conns, level, true, None, arena)?;
                self.set_neighbours(txn, id, &e_new_conn, level)
                    .map_err(|err| {
                        VectorError::VectorCoreError(format!(
                            "Failed to set neighbors: {} {}",
                            line!(),
                            err
                        ))
                    })?;
            }
        }

        if new_level > l {
            self.set_entry_point(txn, &query).map_err(|err| {
                VectorError::VectorCoreError(format!(
                    "Failed to set entry point: {} {}",
                    line!(),
                    err
                ))
            })?;
        }

        debug_println!("vector inserted with id {}", query.id);
        Ok(query)
    }

    fn delete<'db>(
        &self,
        txn: &Txn<'db>,
        id: u128,
        arena: &bumpalo::Bump,
    ) -> Result<(), VectorError> {
        match self.get_vector_properties(txn, id, arena)? {
            Some(mut properties) => {
                debug_println!("properties: {properties:?}");
                if properties.deleted {
                    return Err(VectorError::VectorAlreadyDeleted(id.to_string()));
                }
                properties.deleted = true;
                txn.put_cf(
                    &self.cf_vector_properties(),
                    id.to_be_bytes(),
                    &bincode::serialize(&properties)?,
                )?;
                debug_println!("vector deleted with id {}", &id);
                Ok(())
            }
            None => Err(VectorError::VectorNotFound(id.to_string())),
        }
    }
}
