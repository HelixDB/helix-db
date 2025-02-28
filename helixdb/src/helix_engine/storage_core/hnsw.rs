use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
};

use bincode::{deserialize, serialize};
use heed3::{types::{Bytes, Unit}, Database, Env, RoTxn, RwTxn};
use rand::{rngs::ThreadRng, Rng};
use serde::{Deserialize, Serialize};

use super::storage_core::{IN_EDGES_PREFIX, OUT_EDGES_PREFIX};
use super::vectors::HVector;
use crate::{decode_str, decode_string, helix_engine::types::GraphError};

const HNSW_VECTORS: &str = "hnsw_vectors";
const HNSW_IN_EDGES: &str = "hnsw_in_edges";
const HNSW_OUT_EDGES: &str = "hnsw_out_edges";
const ENTRY_POINT_KEY: &str = "entry_point";

const VECTOR_PREFIX: &[u8] = b"v:";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HNSWConfig {
    pub m: usize,                 // Maximum number of connections per element
    pub m_max: usize,             // Maximum number of connections for upper layers
    pub ef_construction: usize,   // Size of the dynamic candidate list for construction
    pub max_elements: usize,      // Maximum number of elements in the index
    pub ml_factor: f64,           // Level generation factor
    pub distance_multiplier: f64, // Distance multiplier for pruning
}

impl Default for HNSWConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max: 32,
            ef_construction: 200,
            max_elements: 1_000_000,
            ml_factor: 1.0 / std::f64::consts::LN_2,
            distance_multiplier: 1.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryPoint {
    id: String,
    level: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DistancedId {
    id: String,
    distance: f64,
}

impl Eq for DistancedId {}

impl PartialOrd for DistancedId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.distance.partial_cmp(&self.distance)
    }
}

impl Ord for DistancedId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

pub struct HNSW {
    vectors_db: Database<Bytes, Bytes>,
    in_edges_db: Database<Bytes, Unit>,
    out_edges_db: Database<Bytes, Unit>,
    entry_point: Option<EntryPoint>,
    rng: ThreadRng,
    config: HNSWConfig,
}

impl HNSW {
    pub fn new(env: &Env, txn: &mut RwTxn, config: Option<HNSWConfig>) -> Result<Self, GraphError> {
        let vectors_db = env.create_database(txn, Some(HNSW_VECTORS))?;
        let in_edges_db = env.create_database(txn, Some(HNSW_IN_EDGES))?;
        let out_edges_db = env.create_database(txn, Some(HNSW_OUT_EDGES))?;

        let config = config.unwrap_or_default();

        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();

        let entry_point = match vectors_db.get(txn, entry_key.as_ref())? {
            Some(bytes) => Some(deserialize(bytes).map_err(|_| GraphError::Default)?),
            None => None,
        };

        // let entry_point = sm

        Ok(Self {
            vectors_db,
            in_edges_db,
            out_edges_db,
            entry_point,
            rng: ThreadRng::default(),
            config,
        })
    }

    #[inline]
    pub fn vector_key(id: &str, level: usize) -> Vec<u8> {
        [
            VECTOR_PREFIX,
            id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
        ]
        .concat()
    }

    #[inline]
    pub fn in_edges_key(source_id: &str, sink_id: &str, level: usize) -> Vec<u8> {
        [
            IN_EDGES_PREFIX,
            source_id.as_bytes(),
            b":",
            sink_id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
        ]
        .concat()
    }

    #[inline]
    pub fn out_edges_key(source_id: &str, sink_id: &str, level: usize) -> Vec<u8> {
        [
            OUT_EDGES_PREFIX,
            source_id.as_bytes(),
            b":",
            sink_id.as_bytes(),
            b":",
            &level.to_string().into_bytes(),
        ]
        .concat()
    }

    #[inline]
    pub fn get_random_level(&mut self) -> usize {
        let r = self.rng.random_range(0.0..1.0);
        (-r * self.config.ml_factor).floor() as usize
    }

    #[inline]
    pub fn get_entry_point(&self) -> Result<&Option<EntryPoint>, GraphError> {
        Ok(&self.entry_point)
    }

    #[inline]
    pub fn set_entry_point(
        &mut self,
        txn: &mut RwTxn,
        entry: &EntryPoint,
    ) -> Result<(), GraphError> {
        let entry_key = ENTRY_POINT_KEY.as_bytes().to_vec();
        self.vectors_db.put(txn, &entry_key, &serialize(entry)?)?;
        self.entry_point = Some(entry.clone());
        Ok(())
    }

    fn get_neighbors(
        &self,
        txn: &RoTxn,
        id: &str,
        level: usize,
    ) -> Result<Vec<String>, GraphError> {
        let out_key = Self::out_edges_key(id, "", level);

        let iter = self.out_edges_db.lazily_decode_data().prefix_iter(&txn, &out_key)?;

        let mut neighbors = Vec::with_capacity(512);

        for result in iter {
            let (key, _) = result?;
            let neighbor_id = String::from_utf8(key[id.len() + 1..].to_vec())?;
            neighbors.push(neighbor_id);
        }

        Ok(neighbors)
    }

    fn set_neighbors(
        &self,
        txn: &mut RwTxn,
        id: &str,
        level: usize,
        neighbors: &[String],
    ) -> Result<(), GraphError> {
        neighbors.iter().try_for_each(|neighbor_id| -> Result<(), GraphError> {
            let out_key = Self::out_edges_key(id, neighbor_id, level);
            let in_key = Self::in_edges_key(neighbor_id, id, level);

            self.out_edges_db.put(txn, &out_key, &())?;
            self.in_edges_db.put(txn, &in_key, &())?;
            Ok(())
        })?;
        Ok(())
    }

    #[inline]
    fn get_vector(&self, txn: &RoTxn, id: &str, level: usize) -> Result<HVector, GraphError> {
        match self.vectors_db.get(txn, &Self::vector_key(id, level))? {
            Some(bytes) => deserialize(&bytes).map_err(|_| GraphError::Default), // do properly
            None => Err(GraphError::Default),
        }
    }

    #[inline]
    fn put_vector(&self, txn: &mut RwTxn, id: &str, vector: &HVector) -> Result<(), GraphError> {
        self.vectors_db.put(
            txn,
            &Self::vector_key(id, vector.level),
            &serialize(&vector)?,
        )?;
        Ok(())
    }

    pub fn search_layer(
        &self,
        txn: &RoTxn,
        query: &HVector,
        entry_id: &str,
        ef: usize,
        level: usize,
    ) -> Result<BinaryHeap<DistancedId>, GraphError> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        let entry_vector = match self.get_vector(txn, entry_id, level) {
            Ok(v) => v,
            Err(_) => return Ok(BinaryHeap::new()),
        };

        let distance = entry_vector.distance_to(query);

        candidates.push(DistancedId {
            id: entry_id.to_string(),
            distance,
        });

        results.push(DistancedId {
            id: entry_id.to_string(),
            distance,
        });

        visited.insert(entry_id.to_string());

        while !candidates.is_empty() {
            let current = candidates.pop().unwrap();

            if let Some(furthest) = results.peek() {
                if current.distance > furthest.distance && results.len() >= ef {
                    continue;
                }
            }

            let neighbors = self.get_neighbors(txn, &current.id, level)?;

            for neighbor_id in neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }

                visited.insert(neighbor_id.clone());

                let neighbor_vector = match self.get_vector(txn, &neighbor_id, level) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let distance = neighbor_vector.distance_to(query);

                if results.len() < ef || distance < results.peek().unwrap().distance {
                    candidates.push(DistancedId {
                        id: neighbor_id.clone(),
                        distance,
                    });

                    results.push(DistancedId {
                        id: neighbor_id,
                        distance,
                    });

                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        Ok(results)
    }

    fn select_neighbors(
        &self,
        txn: &RoTxn,
        query: &HVector,
        candidates: &BinaryHeap<DistancedId>,
        m: usize,
        level: usize,
    ) -> Result<Vec<String>, GraphError> {
        if candidates.len() <= m {
            return Ok(candidates.iter().map(|c| c.id.clone()).collect());
        }

        let mut selected = Vec::with_capacity(m);
        let mut remaining: Vec<_> = candidates.iter().collect();

        remaining.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });

        while selected.len() < m && !remaining.is_empty() {
            let next = remaining.remove(0);
            selected.push(next.id.clone());

            remaining = remaining
                .into_iter()
                .filter(|candidate| {
                    for selected_id in &selected {
                        if &candidate.id == selected_id {
                            continue;
                        }

                        let selected_vector = match self.get_vector(txn, selected_id, level) {
                            Ok(v) => v,
                            Err(_) => return true,
                        };

                        let candidate_vector = match self.get_vector(txn, &candidate.id, level) {
                            Ok(v) => v,
                            Err(_) => return true,
                        };

                        let distance = selected_vector.distance_to(&candidate_vector);

                        if distance < candidate.distance * self.config.distance_multiplier {
                            return false;
                        }
                    }
                    true
                })
                .collect();
        }

        Ok(selected)
    }

    pub fn search(
        &self,
        txn: &RoTxn,
        query: &HVector,
        k: usize,
    ) -> Result<Vec<(String, f64)>, GraphError> {
        let entry_point = match &self.entry_point {
            Some(ep) => ep,
            None => return Ok(Vec::new()),
        };

        let max_level = entry_point.level;
        let curr_id = entry_point.id.clone();

        let curr_dist = match self.get_vector(txn, &curr_id, entry_point.level) {
            Ok(v) => v.distance_to(query),
            Err(_) => return Ok(Vec::new()),
        };

        let mut curr_dist = curr_dist;
        let mut curr_id = curr_id;

        for l in (1..=max_level).rev() {
            let mut changed = true;

            while changed {
                changed = false;

                let neighbors = self.get_neighbors(txn, &curr_id, l)?;

                for neighbor_id in neighbors {
                    let neighbor_vector = match self.get_vector(txn, &neighbor_id, l) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let dist = neighbor_vector.distance_to(query);

                    if dist < curr_dist {
                        curr_dist = dist;
                        curr_id = neighbor_id;
                        changed = true;
                    }
                }
            }
        }

        let ef = k.max(self.config.ef_construction);
        let candidates = self.search_layer(txn, query, &curr_id, ef, 0)?;

        let mut results = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            results.push((candidate.id, candidate.distance));
        }

        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));

        if results.len() > k {
            results.truncate(k);
        }

        Ok(results)
    }

    pub fn insert(&mut self, txn: &mut RwTxn, id: &str, data: &[f64]) -> Result<(), GraphError> {
        let random_level = self.get_random_level();
        let vector = HVector::from_slice(id.to_string(), random_level, data.to_vec());

        self.put_vector(txn, id, &vector)?;

        if self.entry_point.is_none() {
            let entry_point = EntryPoint {
                id: id.to_string(),
                level: random_level,
            };
            self.set_entry_point(txn, &entry_point)?;
            return Ok(());
        }

        let current_ep = match &self.entry_point {
            Some(ep) => ep.clone(),
            None => return Err(GraphError::from("No entry point found")),
        };

        let curr_id = current_ep.id.clone();
        let mut curr_level = current_ep.level;

        if random_level > curr_level {
            let new_ep = EntryPoint {
                id: id.to_string(),
                level: random_level,
            };
            self.set_entry_point(txn, &new_ep)?;
            curr_level = random_level;
        }

        let mut ep_id = curr_id;

        for level in (0..=random_level).rev() {
            if level <= curr_level {
                let nearest =
                    self.search_layer(txn, &vector, &ep_id, self.config.ef_construction, level)?;

                if nearest.is_empty() {
                    continue;
                }

                let m = if level == 0 {
                    self.config.m
                } else {
                    self.config.m_max
                };
                let neighbors = self.select_neighbors(txn, &vector, &nearest, m, level)?;

                self.set_neighbors(txn, id, level, &neighbors)?;

                for neighbor_id in &neighbors {
                    let mut neighbor_neighbors = self.get_neighbors(txn, neighbor_id, level)?;
                    neighbor_neighbors.push(id.to_string());

                    if neighbor_neighbors.len() > m {
                        let neighbor_vector = match self.get_vector(txn, neighbor_id, level) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        let candidates: BinaryHeap<_> = neighbor_neighbors
                            .iter()
                            .filter_map(|n_id| match self.get_vector(txn, n_id, level) {
                                Ok(n_vector) => {
                                    let dist = neighbor_vector.distance_to(&n_vector);
                                    Some(DistancedId {
                                        id: n_id.clone(),
                                        distance: dist,
                                    })
                                }
                                Err(_) => None,
                            })
                            .collect();

                        let pruned =
                            self.select_neighbors(txn, &neighbor_vector, &candidates, m, level)?;
                        self.set_neighbors(txn, neighbor_id, level, &pruned)?;
                    } else {
                        self.set_neighbors(txn, neighbor_id, level, &neighbor_neighbors)?;
                    }
                }

                if !nearest.is_empty() {
                    ep_id = nearest.peek().unwrap().id.clone();
                }
            }
        }

        Ok(())
    }
}
