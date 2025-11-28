#[cfg(not(feature = "slate"))]
use crate::helix_engine::{
    bm25::BM25,
    storage_core::HelixGraphStorage,
    storage_core::storage_methods::StorageMethods,
    traversal_core::{WTxn, traversal_value::TraversalValue},
    types::GraphError,
};

#[cfg(feature = "slate")]
use crate::helix_engine::{traversal_core::traversal_value::TraversalValue, types::GraphError};

pub struct Drop<I> {
    pub iter: I,
}

#[cfg(not(feature = "slate"))]
impl<'db, 'arena, 'txn, I> Drop<I>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    pub fn drop_traversal(
        iter: I,
        storage: &'db HelixGraphStorage,
        txn: &'txn mut WTxn<'db>,
    ) -> Result<(), GraphError> {
        iter.into_iter().filter_map(|item| item.ok()).try_for_each(
            |item| -> Result<(), GraphError> {
                match item {
                    TraversalValue::Node(node) => match storage.drop_node(txn, node.id) {
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
                    TraversalValue::Edge(edge) => match storage.drop_edge(txn, edge.id) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    },
                    TraversalValue::Vector(vector) => match storage.drop_vector(txn, vector.id) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    },
                    TraversalValue::VectorNodeWithoutVectorData(vector) => {
                        match storage.drop_vector(txn, vector.id) {
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

#[cfg(feature = "slate")]
use crate::helix_engine::traversal_core::traversal_iter::AsyncRoTraversalIterator;
#[cfg(feature = "slate")]
use futures::Stream;

pub trait AsyncDropAdapter<'db, 'arena, 'txn> {
    /// Dedup returns an iterator that will return unique items when collected
    fn drop(
        self,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, S: Stream<Item = Result<TraversalValue<'arena>, GraphError>>>
    AsyncDropAdapter<'db, 'arena, 'txn> for AsyncRoTraversalIterator<'db, 'arena, 'txn, S>
{
    fn drop(
        self,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        use futures::StreamExt;
        let stream = async_stream::try_stream! {
            let mut inner = Box::pin(self.inner);
            while let Some(item) = inner.next().await {
                let item = item?;
                match item {
                    TraversalValue::Node(node) => {
                        self.storage.drop_node(self.txn, node.id, self.arena).await?;
                        if let Some(bm25) = &self.storage.bm25
                            && let Err(e) = bm25.delete_doc(self.txn, node.id).await
                        {
                            println!("failed to delete doc from bm25: {e}");
                        }
                        println!("Dropped node: {:?}", node.id);
                        yield TraversalValue::Empty;
                    },
                    TraversalValue::Edge(edge) => {
                        self.storage.drop_edge(self.txn, edge.id, self.arena).await?;
                        yield TraversalValue::Empty;
                    },
                    TraversalValue::Vector(vector) => {
                        self.storage.drop_vector(self.txn, vector.id, self.arena).await?;
                        yield TraversalValue::Empty;
                    },
                    TraversalValue::VectorNodeWithoutVectorData(vector) => {
                        self.storage.drop_vector(self.txn, vector.id, self.arena).await?;
                        yield TraversalValue::Empty;
                    }
                    TraversalValue::Empty => yield TraversalValue::Empty,
                    _ => {
                        Err(GraphError::ConversionError(format!(
                            "Incorrect Type: {item:?}"
                        )))?;
                    }
                }
            }
        };
        AsyncRoTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: stream,
        }
    }
}
