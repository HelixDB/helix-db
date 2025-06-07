use crate::{
    helix_engine::{graph_core::traversal_iter::RwTraversalIterator, types::GraphError},
    protocol::items::Node,
};
use heed3::PutFlags;
use super::super::tr_val::TraversalVal;

pub struct BulkAddN {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for BulkAddN {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait BulkAddNAdapter<'a, 'b>: Iterator<Item = Result<TraversalVal, GraphError>> {
    ///
    #[deprecated(note = "only used for testing when larger than ram use for loop of addN instead")]
    fn bulk_add_n(
        self,
        nodes: &mut [u128],
        chunk_size: usize,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>>> BulkAddNAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn bulk_add_n(
        self,
        nodes: &mut [u128],
        chunk_size: usize,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);
        nodes.sort_unstable_by_key(|node| *node);
        let chunks = nodes.chunks_mut(chunk_size);
        let mut count = 0;
        for chunk in chunks {
            for node in chunk {
                let node = Node {
                    id: *node,
                    label: "user".to_string(),
                    properties: None,
                };

                let id = node.id;
                // insert node

                match node.encode_node() {
                    Ok(bytes) => {
                        if let Err(e) = self.storage.nodes_db.put_with_flags(
                            self.txn,
                            PutFlags::APPEND,
                            &id,
                            &bytes,
                        ) {
                            result = Err(GraphError::from(e));
                        }
                    }
                    Err(e) => result = Err(GraphError::from(e)),
                }
                count += 1;

                // for index in &secondary_indices {
                //     match self.storage.secondary_indices.get(index.as_str()) {
                //         Some(db) => {
                //             let key = match node.check_property(&index) {
                //                 Some(value) => value,
                //                 None => {
                //                     result = Err(GraphError::New(format!(
                //                         "Secondary Index {} not found",
                //                         index
                //                     )));
                //                     continue;
                //                 }
                //             };
                //             match bincode::serialize(&key) {
                //                 Ok(serialized) => {
                //                     if let Err(e) = db.put_with_flags(
                //                         self.txn,
                //                         PutFlags::APPEND,
                //                         &serialized,
                //                         &node.id.to_be_bytes(),
                //                     ) {
                //                         result = Err(GraphError::from(e));
                //                     }
                //                 }
                //                 Err(e) => result = Err(GraphError::from(e)),
                //             }
                //         }
                //         None => {
                //             result = Err(GraphError::New(format!(
                //                 "Secondary Index {} not found",
                //                 index
                //             )));
                //         }
                //     }
                // }
            }

            if count % 1000000 == 0 {
                println!("processed: {:?}", count);
            }
        }
        RwTraversalIterator {
            inner: std::iter::once(result), // TODO: change to support adding multiple edges
            storage: self.storage,
            txn: self.txn,
        }
    }
}
