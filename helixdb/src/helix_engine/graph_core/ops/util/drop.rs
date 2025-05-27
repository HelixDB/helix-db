use crate::helix_engine::{
    graph_core::ops::tr_val::TraversalVal,
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use heed3::RwTxn;
use std::sync::Arc;

pub struct Drop<I> {
    pub iter: I,
}

impl<'a> Drop<Vec<Result<TraversalVal, GraphError>>> {
    pub fn drop_traversal(
        iter: Vec<Result<TraversalVal, GraphError>>,
        storage: Arc<HelixGraphStorage>,
        txn: &mut RwTxn,
    ) -> Result<(), GraphError> {
        iter.into_iter()
            .try_for_each(|item| -> Result<(), GraphError> {
                match item {
                    Ok(item) => match item {
                        TraversalVal::Node(node) => match storage.drop_node(txn, &node.id) {
                            Ok(_) => Ok(()),
                            Err(e) => return Err(e),
                        },
                        TraversalVal::Edge(edge) => match storage.drop_edge(txn, &edge.id) {
                            Ok(_) => Ok(()),
                            Err(e) => return Err(e),
                        },
                        _ => {
                            return Err(GraphError::ConversionError(format!(
                                "Incorrect Type: {:?}",
                                item
                            )));
                        }
                    },
                    Err(e) => return Err(e),
                }
            })
    }
}
