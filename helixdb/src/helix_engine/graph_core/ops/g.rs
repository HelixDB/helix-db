use super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::{RoTraversalIterator, RwTraversalIterator},
        storage_core::storage_core::HelixGraphStorage,
        types::GraphError,
    },
    protocol::{
        items::{Edge, SerializedEdge},
        label_hash::hash_label,
    },
};
use std::{collections::HashMap, sync::Arc};
use heed3::{PutFlags, RoTxn, RwTxn};

pub struct G {
    iter: std::iter::Once<Result<TraversalVal, GraphError>>,
}

// implementing iterator for OutIterator
impl Iterator for G {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl G {
    pub fn new<'a>(
        storage: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        Self: Sized,
    {
        let iter = std::iter::once(Ok(TraversalVal::Empty));
        RoTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }

    pub fn new_mut<'a, 'b>(
        storage: Arc<HelixGraphStorage>,
        txn: &'b mut RwTxn<'a>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        Self: Sized,
    {
        let iter = std::iter::once(Ok(TraversalVal::Empty));
        RwTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }

    pub fn new_from<'a>(
        storage: Arc<HelixGraphStorage>,
        txn: &'a RoTxn<'a>,
        vals: Vec<TraversalVal>,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        RoTraversalIterator {
            inner: vals.into_iter().map(|val| Ok(val)),
            storage,
            txn,
        }
    }

    pub fn bulk_add_e(
        storage: Arc<HelixGraphStorage>,
        mut edges: Vec<(u128, u128, u128)>,
        should_check_nodes: bool,
        chunk_size: usize,
    ) -> Result<(), GraphError> {
        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);
        // sort by id
        edges.sort_unstable_by(|(_, _, id), (_, _, id_)| id.cmp(id_));

        let mut count = 0;
        println!("Adding edges");
        // EDGES
        let chunks = edges.chunks_mut(chunk_size);
        for chunk in chunks {
            let mut txn = storage.graph_env.write_txn().unwrap();
            for (e_from, e_to, e_id) in chunk.iter() {
                if should_check_nodes
                    && (storage
                        .nodes_db
                        .get(&txn, &HelixGraphStorage::node_key(&e_from))
                        .map_or(false, |node| node.is_none())
                        || storage
                            .nodes_db
                            .get(&txn, &HelixGraphStorage::node_key(&e_to))
                            .map_or(false, |node| node.is_none()))
                {
                    return Err(GraphError::NodeNotFound);
                }
                match SerializedEdge::encode_edge(&Edge {
                    id: *e_id,
                    label: "knows".to_string(),
                    properties: HashMap::new(),
                    from_node: *e_from,
                    to_node: *e_to,
                }) {
                    Ok(bytes) => {
                        if let Err(e) = storage.edges_db.put_with_flags(
                            &mut txn,
                            PutFlags::APPEND,
                            &HelixGraphStorage::edge_key(&e_id),
                            &bytes,
                        ) {
                            println!("error adding edge: {:?}", e);
                            return Err(GraphError::from(e));
                        }
                    }
                    Err(e) => {
                        println!("error serializing edge: {:?}", e);
                        return Err(GraphError::from(e));
                    }
                }

                count += 1;
                if count % 1000000 == 0 {
                    println!("Processed {} chunks", count);
                }
            }
            txn.commit()?;
        }

        count = 0;
        println!("Adding out edges");
        // OUT EDGES
        let mut prev_out = None;

        edges.sort_unstable_by(|(from, to, id), (from_, to_, id_)| {
            if from == from_ {
                id.cmp(id_)
            } else {
                from.cmp(from_)
            }
        });

        let chunks = edges.chunks_mut(chunk_size);
        for chunk in chunks {
            let mut txn = storage.graph_env.write_txn().unwrap();
            for (from_node, to_node, id) in chunk.iter() {
                // OUT EDGES
                let out_flag = if Some(from_node) == prev_out {
                    PutFlags::APPEND_DUP
                } else {
                    prev_out = Some(from_node);
                    PutFlags::APPEND
                };

                match storage.out_edges_db.put_with_flags(
                    &mut txn,
                    out_flag,
                    &HelixGraphStorage::out_edge_key(&from_node, &hash_label("knows", None)),
                    &HelixGraphStorage::pack_edge_data(&to_node, &id),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error adding out edge: {:?}", e);
                        return Err(GraphError::from(e));
                    }
                }
                count += 1;
                if count % 1000000 == 0 {
                    println!("Processed {} chunks", count);
                }
            }
            txn.commit()?;
        }

        count = 0;
        println!("Adding in edges");
        // IN EDGES
        edges.sort_unstable_by(
            |(from, to, id), (from_, to_, id_)| {
                if to == to_ {
                    id.cmp(id_)
                } else {
                    to.cmp(to_)
                }
            },
        );
        let mut prev_in = None;
        let chunks = edges.chunks_mut(chunk_size);
        for chunk in chunks {
            let mut txn = storage.graph_env.write_txn().unwrap();
            for (from_node, to_node, id) in chunk.iter() {
                // IN EDGES
                let in_flag = if Some(to_node) == prev_in {
                    PutFlags::APPEND_DUP
                } else {
                    prev_in = Some(to_node);
                    PutFlags::APPEND
                };

                match storage.in_edges_db.put_with_flags(
                    &mut txn,
                    in_flag,
                    &HelixGraphStorage::in_edge_key(&to_node, &hash_label("knows", None)),
                    &HelixGraphStorage::pack_edge_data(&from_node, &id),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        println!("error adding in edge: {:?}", e);
                        return Err(GraphError::from(e));
                    }
                }
                count += 1;
                if count % 1000000 == 0 {
                    println!("Processed {} chunks", count);
                }
            }
            txn.commit()?;
        }
        Ok(())
    }
}
