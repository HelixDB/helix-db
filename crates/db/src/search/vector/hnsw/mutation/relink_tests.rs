//! Byte identity of hydrated deletion relinking with the per-candidate relink
//! it replaced.
//!
//! [`reference_delete_from_layer`] keeps the previous implementation, which
//! read each (source, candidate) item through the mutation cache and fully
//! sorted every candidate list. Random graphs with tied distances, dangling
//! neighbor IDs, and itemless sources must stage identical rows through both.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use proptest::prelude::*;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use slatedb::object_store::memory::InMemory;
use slatedb::{DbReadOps, IsolationLevel};

use super::*;
use crate::search::vector::distance::{Cosine, Euclidean};
use crate::search::vector::VectorIndexConfig;

/// First ID of nodes that appear in rows but have no vector.
const GHOST: NodeId = 1_000;
const GHOSTS: NodeId = 4;
/// Highest layer a scenario inserts into or rewires.
const TOP_LAYER: u16 = 2;

/// Previous `delete_from_layer`, kept verbatim as the parity oracle.
async fn reference_delete_from_layer<D: Distance>(
    index: &VectorIndex<D>,
    txn: &MeasuredVectorTransaction<'_>,
    node_id: NodeId,
    layer: u16,
    maximum_neighbors: usize,
    extra_sources: &[NodeId],
    mutation_cache: &mut MutationOpCache<D>,
) -> Result<Vec<NodeId>, HelixDbError> {
    let outgoing_neighbors = index
        .load_neighbors_for_mutation(txn, layer, node_id, mutation_cache)
        .await?;
    let mandatory_relink = outgoing_neighbors
        .iter()
        .copied()
        .filter(|neighbor_id| *neighbor_id != node_id)
        .collect::<BTreeSet<_>>();
    let mut affected_sources = mandatory_relink.clone();
    affected_sources.extend(
        extra_sources
            .iter()
            .copied()
            .filter(|source_id| *source_id != node_id),
    );
    if affected_sources.is_empty() {
        return Ok(outgoing_neighbors);
    }

    let mut relink_sources = mandatory_relink;
    for neighbor_id in affected_sources {
        if index
            .remove_edge_from_neighbor(txn, layer, neighbor_id, node_id, mutation_cache)
            .await?
        {
            relink_sources.insert(neighbor_id);
        }
    }
    if relink_sources.is_empty() {
        return Ok(outgoing_neighbors);
    }
    let relink_sources = relink_sources.into_iter().collect::<Vec<_>>();
    let mut candidates = relink_sources
        .iter()
        .copied()
        .filter(|candidate| *candidate != node_id)
        .collect::<HashSet<_>>();
    for &neighbor_id in &relink_sources {
        let neighbors = index
            .load_neighbors_for_mutation(txn, layer, neighbor_id, mutation_cache)
            .await?;
        candidates.extend(
            neighbors
                .into_iter()
                .filter(|candidate| *candidate != node_id && *candidate != neighbor_id),
        );
    }
    for &neighbor_id in &relink_sources {
        reference_relink_neighbor(
            index,
            txn,
            layer,
            neighbor_id,
            &candidates,
            maximum_neighbors,
            mutation_cache,
        )
        .await?;
    }
    Ok(outgoing_neighbors)
}

/// Previous `relink_neighbor`, kept verbatim as the parity oracle.
async fn reference_relink_neighbor<D: Distance>(
    index: &VectorIndex<D>,
    txn: &MeasuredVectorTransaction<'_>,
    layer: u16,
    neighbor_id: NodeId,
    candidates: &HashSet<NodeId>,
    maximum_neighbors: usize,
    mutation_cache: &mut MutationOpCache<D>,
) -> Result<(), HelixDbError> {
    let Some(neighbor_item) = index
        .get_item_for_layer_cached(txn, layer, neighbor_id, mutation_cache)
        .await?
    else {
        return Ok(());
    };
    let old_neighbors = index
        .load_neighbors_for_mutation(txn, layer, neighbor_id, mutation_cache)
        .await?;
    let mut current_neighbors = old_neighbors.clone();
    let mut candidate_distances = Vec::new();
    for &candidate_id in candidates {
        if candidate_id == neighbor_id {
            continue;
        }
        let Some(candidate_item) = index
            .get_item_for_layer_cached(txn, layer, candidate_id, mutation_cache)
            .await?
        else {
            continue;
        };
        candidate_distances.push(Candidate::try_new(
            candidate_id,
            D::distance(neighbor_item.as_ref(), candidate_item.as_ref()),
        )?);
    }
    candidate_distances.sort();
    for candidate in candidate_distances.iter().take(maximum_neighbors) {
        if !current_neighbors.contains(&candidate.node_id) {
            current_neighbors.push(candidate.node_id);
        }
    }

    if current_neighbors.len() > maximum_neighbors {
        let mut distances = Vec::new();
        let mut items = HashMap::<NodeId, Arc<Item<'static, D>>>::new();
        for &node_id in &current_neighbors {
            let Some(item) = index
                .get_item_for_layer_cached(txn, layer, node_id, mutation_cache)
                .await?
            else {
                continue;
            };
            distances.push(Candidate::try_new(
                node_id,
                D::distance(neighbor_item.as_ref(), item.as_ref()),
            )?);
            items.insert(node_id, item);
        }
        distances.sort();
        current_neighbors = select_diverse(
            neighbor_item.as_ref(),
            &distances,
            &|node_id| items.get(&node_id).map(|item| item.as_ref()),
            maximum_neighbors,
        )?;
    }

    index
        .stage_neighbors_for_mutation(txn, layer, neighbor_id, &current_neighbors, mutation_cache)
        .await?;
    for &new_neighbor_id in &current_neighbors {
        if old_neighbors.contains(&new_neighbor_id) {
            continue;
        }
        let mut reverse_neighbors = index
            .load_neighbors_for_mutation(txn, layer, new_neighbor_id, mutation_cache)
            .await?;
        if reverse_neighbors.contains(&neighbor_id) {
            continue;
        }
        reverse_neighbors.push(neighbor_id);
        if reverse_neighbors.len() > maximum_neighbors {
            let Some(reverse_item) = index
                .get_item_for_layer_cached(txn, layer, new_neighbor_id, mutation_cache)
                .await?
            else {
                index
                    .stage_neighbors_vec_for_mutation(
                        txn,
                        layer,
                        new_neighbor_id,
                        reverse_neighbors,
                        mutation_cache,
                    )
                    .await?;
                continue;
            };
            let mut reverse_distances = Vec::new();
            let mut items = HashMap::<NodeId, Arc<Item<'static, D>>>::new();
            for &node_id in &reverse_neighbors {
                let Some(item) = index
                    .get_item_for_layer_cached(txn, layer, node_id, mutation_cache)
                    .await?
                else {
                    continue;
                };
                reverse_distances.push(Candidate::try_new(
                    node_id,
                    D::distance(reverse_item.as_ref(), item.as_ref()),
                )?);
                items.insert(node_id, item);
            }
            reverse_distances.sort();
            reverse_neighbors = select_diverse(
                reverse_item.as_ref(),
                &reverse_distances,
                &|node_id| items.get(&node_id).map(|item| item.as_ref()),
                maximum_neighbors,
            )?;
        }
        index
            .stage_neighbors_vec_for_mutation(
                txn,
                layer,
                new_neighbor_id,
                reverse_neighbors,
                mutation_cache,
            )
            .await?;
    }
    Ok(())
}

async fn open_db(name: &str) -> slatedb::Db {
    slatedb::Db::open(name, Arc::new(InMemory::new()))
        .await
        .unwrap()
}

async fn all_rows(read: &(impl DbReadOps + Send + Sync)) -> Vec<(Bytes, Bytes)> {
    let mut rows = read.scan(..).await.unwrap();
    let mut collected = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        collected.push((row.key, row.value));
    }
    collected
}

/// Returns `count` distinct values drawn from `pool`.
fn sample(rng: &mut StdRng, pool: &[NodeId], count: usize) -> Vec<NodeId> {
    let mut pool = pool.to_vec();
    (0..count.min(pool.len()))
        .map(|_| pool.swap_remove(rng.random_range(0..pool.len())))
        .collect()
}

/// Builds a random graph, then deletes a few nodes layer by layer through the
/// hydrated relink and the reference in two transactions over the same
/// snapshot, and requires identical outgoing lists and identical rows.
///
/// Vectors use components in `{-1, 0, 1, 2}`, so equal vectors and equal
/// cosine directions tie distances and node IDs decide the order. Rewired
/// rows add asymmetric edges, residue above a node's layer, dangling IDs with
/// no vector (batch-load misses), and itemless upper-layer sources.
async fn assert_relink_parity<D: Distance>(seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let m = rng.random_range(2..=4_usize);
    let layer0_limit = 2 * m;
    let dimension = rng.random_range(2..=3_usize);
    let node_count = rng.random_range(12..=48_u64);
    let real = (1..=node_count).collect::<Vec<_>>();
    let ghosts = (GHOST..GHOST + GHOSTS).collect::<Vec<_>>();
    let everyone = real.iter().chain(&ghosts).copied().collect::<Vec<_>>();
    let limit = |layer: u16| if layer == 0 { layer0_limit } else { m };

    let db = open_db(&format!("relink-parity-{seed}")).await;
    let index = VectorIndex::<D>::new("relink-parity");
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .create(
            &txn,
            VectorIndexConfig::new(index.name(), "embedding", dimension)
                .with_m(m)
                .with_m0(layer0_limit)
                .with_ef_construction(8),
        )
        .await
        .unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    for &node_id in &real {
        let mut vector = (0..dimension)
            .map(|_| rng.random_range(-1..=2_i8) as f32)
            .collect::<Vec<_>>();
        if vector.iter().all(|component| *component == 0.0) {
            vector[0] = 1.0;
        }
        let layer = match rng.random_range(0..10) {
            0 => 2,
            1..=2 => 1,
            _ => 0,
        };
        index
            .insert_with_measured_transaction(
                &measured,
                node_id,
                &vector,
                VectorInsertContract::Upsert,
                Some(layer),
            )
            .await
            .unwrap();
    }
    if rng.random_range(0..2) == 0 {
        let rows = VectorWriteRows::new(&measured, index.row_keyspace());
        for layer in 0..=TOP_LAYER {
            // A layer-0 row without a SimHash is corrupt, so ghosts own rows
            // only above layer 0.
            let owners = if layer == 0 { &real } else { &everyone };
            for &owner in owners {
                if rng.random_range(0..4) != 0 {
                    continue;
                }
                let others = everyone
                    .iter()
                    .copied()
                    .filter(|node_id| *node_id != owner)
                    .collect::<Vec<_>>();
                let degree = rng.random_range(0..=limit(layer));
                let row = sample(&mut rng, &others, degree);
                if layer == 0 {
                    rows.put_layer0_neighbors(owner, &row).unwrap();
                } else {
                    rows.put_upper_neighbors(layer, owner, &row).unwrap();
                }
            }
        }
    }
    txn.commit().await.unwrap();

    let hydrated_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let reference_txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let hydrated = MeasuredVectorTransaction::new(&hydrated_txn);
    let reference = MeasuredVectorTransaction::new(&reference_txn);
    let mut hydrated_cache = MutationOpCache::<D>::with_degree_limits(layer0_limit, m).unwrap();
    let mut reference_cache = MutationOpCache::<D>::with_degree_limits(layer0_limit, m).unwrap();
    let deletions = rng.random_range(1..=3);
    for target in sample(&mut rng, &real, deletions) {
        for layer in (0..=TOP_LAYER).rev() {
            let extra_count = rng.random_range(0..=4);
            let extra_sources = sample(&mut rng, &everyone, extra_count);
            let expected = reference_delete_from_layer(
                &index,
                &reference,
                target,
                layer,
                limit(layer),
                &extra_sources,
                &mut reference_cache,
            )
            .await
            .unwrap();
            let actual = index
                .delete_from_layer(
                    &hydrated,
                    target,
                    layer,
                    limit(layer),
                    &extra_sources,
                    &mut hydrated_cache,
                )
                .await
                .unwrap();
            assert_eq!(
                actual, expected,
                "seed {seed}: outgoing of {target}@{layer}"
            );
        }
    }
    index
        .flush_mutation_cache(&reference, &mut reference_cache)
        .await
        .unwrap();
    index
        .flush_mutation_cache(&hydrated, &mut hydrated_cache)
        .await
        .unwrap();
    let actual = all_rows(&hydrated_txn).await;
    let expected = all_rows(&reference_txn).await;
    let first_difference = actual
        .iter()
        .zip(&expected)
        .position(|(actual, expected)| actual != expected);
    assert!(
        actual == expected,
        "seed {seed}: rows differ first at {first_difference:?} ({} rows against {})",
        actual.len(),
        expected.len()
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn hydrated_relink_matches_the_per_candidate_reference(seed in any::<u64>()) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                assert_relink_parity::<Cosine>(seed).await;
                assert_relink_parity::<Euclidean>(seed).await;
            });
    }
}

/// Inserts `nodes` at layer 1 and then overwrites their layer-1 rows.
async fn upper_layer_fixture(
    name: &str,
    nodes: &[(NodeId, [f32; 2])],
    rows: &[(NodeId, &[NodeId])],
) -> (slatedb::Db, VectorIndex<Cosine>) {
    let db = open_db(name).await;
    let index = VectorIndex::<Cosine>::new(name);
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    index
        .create(
            &txn,
            VectorIndexConfig::new(index.name(), "embedding", 2)
                .with_m(2)
                .with_m0(4)
                .with_ef_construction(8),
        )
        .await
        .unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    for (node_id, vector) in nodes {
        index
            .insert_with_measured_transaction(
                &measured,
                *node_id,
                vector,
                VectorInsertContract::Upsert,
                Some(1),
            )
            .await
            .unwrap();
    }
    let writer = VectorWriteRows::new(&measured, index.row_keyspace());
    for (owner, row) in rows {
        writer.put_upper_neighbors(1, *owner, row).unwrap();
    }
    txn.commit().await.unwrap();
    (db, index)
}

#[tokio::test]
async fn relink_reads_no_candidate_when_no_source_has_an_item() {
    // Node 1's only layer-1 neighbor is the itemless node 50, whose row also
    // holds node 3.
    let (db, index) = upper_layer_fixture(
        "relink-itemless-sources",
        &[(1, [1.0, 0.0]), (3, [0.8, 0.2])],
        &[(1, &[50]), (50, &[1, 3])],
    )
    .await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let mut cache = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();

    let outgoing = index
        .delete_from_layer(&measured, 1, 1, 2, &[], &mut cache)
        .await
        .unwrap();

    assert_eq!(outgoing, vec![50]);
    assert!(cache.item_is_known_absent(1, 50));
    assert!(!cache.items.contains_key(&(1, 3)));
    index
        .flush_mutation_cache(&measured, &mut cache)
        .await
        .unwrap();
    assert_eq!(
        index.load_upper_neighbors(&txn, 1, 50).await.unwrap(),
        Some(vec![3])
    );
}

#[tokio::test]
async fn relink_skips_candidates_without_an_item() {
    // Deleting node 1 relinks nodes 2 and 3 against each other and the
    // dangling node 60.
    let (db, index) = upper_layer_fixture(
        "relink-dangling-candidate",
        &[(1, [1.0, 0.0]), (2, [0.9, 0.1]), (3, [0.0, 1.0])],
        &[(1, &[2, 3]), (2, &[1, 60]), (3, &[1])],
    )
    .await;
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let mut cache = MutationOpCache::<Cosine>::with_degree_limits(4, 2).unwrap();

    let candidates = index
        .load_relink_candidates(&measured, 1, &HashSet::from([2, 3, 60]), &mut cache)
        .await
        .unwrap();
    assert_eq!(candidates.layer, 1);
    assert!(candidates.items[&2].is_some());
    assert!(candidates.items[&3].is_some());
    assert!(candidates.items[&60].is_none());

    index
        .delete_from_layer(&measured, 1, 1, 2, &[], &mut cache)
        .await
        .unwrap();
    index
        .flush_mutation_cache(&measured, &mut cache)
        .await
        .unwrap();
    // Node 2 keeps its surviving dangling edge but links only node 3 anew.
    assert_eq!(
        index.load_upper_neighbors(&txn, 1, 2).await.unwrap(),
        Some(vec![3, 60])
    );
    assert_eq!(
        index.load_upper_neighbors(&txn, 1, 3).await.unwrap(),
        Some(vec![2])
    );
}
