use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RwTraversalIterator},
        storage_core::storage_core::HelixGraphStorage,
        types::GraphError,
    },
    protocol::{items::Edge, label_hash::hash_label},
};
use heed3::PutFlags;

pub struct BulkAddE {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for BulkAddE {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait BulkAddEAdapter<'a, 'b>: Iterator<Item = Result<TraversalVal, GraphError>> {
    ///
    #[deprecated(note = "only used for testing when larger than ram use for loop of addE instead")]
    fn bulk_add_e(
        self,
        edges: Vec<(u128, u128, u128)>,
        should_check_nodes: bool,
        chunk_size: usize,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>>> BulkAddEAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn bulk_add_e(
        self,
        mut edges: Vec<(u128, u128, u128)>,
        should_check_nodes: bool,
        _chunk_size: usize,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);
        // sort by id
        edges.sort_unstable_by(|(_, _, id), (_, _, id_)| id.cmp(id_));

        let mut count = 0;
        println!("Adding edges");
        // EDGES
        for (e_from, e_to, e_id) in edges.iter() {
            if should_check_nodes
                && (self
                    .storage
                    .nodes_db
                    .get(self.txn, &HelixGraphStorage::node_key(&e_from))
                    .map_or(false, |node| node.is_none())
                    || self
                        .storage
                        .nodes_db
                        .get(self.txn, &HelixGraphStorage::node_key(&e_to))
                        .map_or(false, |node| node.is_none()))
            {
                result = Err(GraphError::NodeNotFound);
            }
            match {
                Edge {
                    id: *e_id,
                    label: "knows".to_string(),
                    properties: None,
                    from_node: *e_from,
                    to_node: *e_to,
                }
                .encode_edge()
            } {
                Ok(bytes) => {
                    if let Err(e) = self.storage.edges_db.put_with_flags(
                        self.txn,
                        PutFlags::APPEND,
                        &HelixGraphStorage::edge_key(&e_id),
                        &bytes,
                    ) {
                        println!("error adding edge: {:?}", e);
                        result = Err(GraphError::from(e));
                    }
                }
                Err(e) => {
                    println!("error serializing edge: {:?}", e);
                    result = Err(GraphError::from(e));
                }
            }

            count += 1;
            if count % 1000000 == 0 {
                println!("Processed {} chunks", count);
            }
        }

        count = 0;
        println!("Adding out edges");
        // OUT EDGES
        let mut prev_out = None;

        edges.sort_unstable_by(|(from, _, id), (from_, _, id_)| {
            if from == from_ {
                id.cmp(id_)
            } else {
                from.cmp(from_)
            }
        });

        for (from_node, to_node, id) in edges.iter() {
            // OUT EDGES
            let out_flag = if Some(from_node) == prev_out {
                PutFlags::APPEND_DUP
            } else {
                prev_out = Some(from_node);
                PutFlags::APPEND
            };

            match self.storage.out_edges_db.put_with_flags(
                self.txn,
                out_flag,
                &HelixGraphStorage::out_edge_key(&from_node, &hash_label("knows", None)),
                &HelixGraphStorage::pack_edge_data(&to_node, &id),
            ) {
                Ok(_) => {}
                Err(e) => {
                    println!("error adding out edge: {:?}", e);
                    result = Err(GraphError::from(e));
                }
            }
            count += 1;
            if count % 1000000 == 0 {
                println!("Processed {} chunks", count);
            }
        }

        count = 0;
        println!("Adding in edges");
        // IN EDGES
        edges.sort_unstable_by(
            |(_, to, id), (_, to_, id_)| {
                if to == to_ {
                    id.cmp(id_)
                } else {
                    to.cmp(to_)
                }
            },
        );
        let mut prev_in = None;
        for (from_node, to_node, id) in edges.iter() {
            // IN EDGES
            let in_flag = if Some(to_node) == prev_in {
                PutFlags::APPEND_DUP
            } else {
                prev_in = Some(to_node);
                PutFlags::APPEND
            };

            match self.storage.in_edges_db.put_with_flags(
                self.txn,
                in_flag,
                &HelixGraphStorage::in_edge_key(&to_node, &hash_label("knows", None)),
                &HelixGraphStorage::pack_edge_data(&from_node, &id),
            ) {
                Ok(_) => {}
                Err(e) => {
                    println!("error adding in edge: {:?}", e);
                    result = Err(GraphError::from(e));
                }
            }
            count += 1;
            if count % 1000000 == 0 {
                println!("Processed {} chunks", count);
            }
        }

        RwTraversalIterator {
            inner: std::iter::once(result), // TODO: change to support adding multiple edges
            storage: self.storage,
            txn: self.txn,
        }
    }
}
