use crate::helix_engine::{
    bm25::bm25::BM25,
    storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
    traversal_core::traversal_value::TraversalValue,
    types::GraphError,
};
use heed3::RwTxn;

pub struct Drop<I> {
    pub iter: I,
}

impl<'db, 'arena, 'txn, I> Drop<I>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    pub fn drop_traversal(
        iter: I,
        storage: &'db HelixGraphStorage,
        txn: &'txn mut RwTxn<'db>,
    ) -> Result<(), GraphError> {
        iter.into_iter().filter_map(|item| item.ok()).try_for_each(
            |item| -> Result<(), GraphError> {
                match item {
                    TraversalValue::Node(node) => match storage.drop_node(txn, &node.id) {
                        Ok(_) => {
                            if let Some(bm25) = &storage.bm25
                                && let Err(e) = bm25.delete_doc(txn, node.id)
                            {
                                println!("failed to delete doc from bm25: {e}");
                            }
                            println!("Dropped node: {:?}", node.id);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    },
                    TraversalValue::Edge(edge) => match storage.drop_edge(txn, &edge.id) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    },
                    TraversalValue::Vector(vector) => match storage.drop_vector(txn, &vector.id) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    },
                    TraversalValue::VectorNodeWithoutVectorData(vector) => {
                        match storage.drop_vector(txn, &vector.id) {
                            Ok(_) => Ok(()),
                            Err(e) => Err(e),
                        }
                    }
                    TraversalValue::Empty => Ok(()),
                    _ => Err(GraphError::ConversionError(format!(
                        "Incorrect Type: {item:?}"
                    ))),
                }
            },
        )
    }
}
