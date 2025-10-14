use heed3::RoTxn;

use crate::{
    helix_engine::{
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
        vector_core::{VectorData, hnsw::HNSW, vector::HVector},
    },
    protocol::value::Value,
};

pub struct InsertVIterator {
    inner: std::iter::Once<Result<TraversalValue, GraphError>>,
}

impl Iterator for InsertVIterator {
    type Item = Result<TraversalValue, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait InsertVAdapter<'a, 'b>: Iterator<Item = Result<TraversalValue, GraphError>> {
    fn insert_v<F>(
        self,
        query: VectorData,
        label: &str,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalValue, GraphError>>> InsertVAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn insert_v<F>(
        self,
        query: VectorData,
        label: &str,
        fields: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        F: Fn(&HVector, &RoTxn) -> bool,
    {
        let fields = match fields {
            Some(mut fields) => {
                fields.push((String::from("label"), Value::String(label.to_string())));
                fields.push((String::from("is_deleted"), Value::Boolean(false)));
                Some(fields)
            }
            None => Some(vec![
                (String::from("label"), Value::String(label.to_string())),
                (String::from("is_deleted"), Value::Boolean(false)),
            ]),
        };
        let vector =
            self.storage
                .vectors
                .insert::<F>(self.txn, query, fields);

        let result = match vector {
            Ok(vector) => Ok(TraversalValue::Vector(vector)),
            Err(e) => Err(GraphError::from(e)),
        };

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
