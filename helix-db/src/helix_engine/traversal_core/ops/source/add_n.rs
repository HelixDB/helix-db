use crate::{
    helix_engine::{
        bm25::bm25::{BM25, BM25Flatten},
        storage_core::HelixGraphStorage,
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    protocol::value::Value,
    utils::{filterable::Filterable, id::v6_uuid, items::Node},
};
use heed3::{PutFlags, RwTxn};

pub struct AddNIterator<'db, 'arena, 'txn>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub storage: &'db HelixGraphStorage,
    pub arena: &'arena bumpalo::Bump,
    pub txn: &'txn RwTxn<'db>,
    inner: std::iter::Once<Result<TraversalValue<'arena>, GraphError>>,
}

impl<'db, 'arena, 'txn> Iterator for AddNIterator<'db, 'arena, 'txn> {
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait AddNAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    fn add_n(
        self,
        label: &'s str,
        properties: Option<Vec<(String, Value)>>,
        secondary_indices: Option<&'s [&str]>,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    AddNAdapter<'db, 'arena, 'txn, 's> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    fn add_n(
        self,
        label: &'s str,
        properties: Option<Vec<(String, Value)>>,
        secondary_indices: Option<&'s [&str]>,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let node = Node {
            id: v6_uuid(),
            label: label, // TODO: just &str or Cow<'arena, str>
            version: 1,
            properties: properties.map(|props| props.into_iter().collect()),
            _phantom: std::marker::PhantomData,
        };
        let secondary_indices = secondary_indices.unwrap_or(&[]).to_vec();
        let mut result: Result<TraversalValue, GraphError> = Ok(TraversalValue::Empty);

        match node.encode_node() {
            Ok(bytes) => {
                if let Err(e) = self.storage.nodes_db.put_with_flags(
                    self.txn,
                    PutFlags::APPEND,
                    &node.id,
                    &bytes,
                ) {
                    result = Err(GraphError::from(e));
                }
            }
            Err(e) => result = Err(e),
        }

        for index in secondary_indices {
            match self.storage.secondary_indices.get(index) {
                Some(db) => {
                    let key = match node.check_property(index) {
                        Ok(value) => value,
                        Err(e) => {
                            result = Err(e);
                            continue;
                        }
                    };
                    // look into if there is a way to serialize to a slice
                    match bincode::serialize(&key) {
                        Ok(serialized) => {
                            // possibly append dup

                            if let Err(e) = db.put(self.txn, &serialized, &node.id) {
                                println!(
                                    "{} Error adding node to secondary index: {:?}",
                                    line!(),
                                    e
                                );
                                result = Err(GraphError::from(e));
                            }
                        }
                        Err(e) => result = Err(GraphError::from(e)),
                    }
                }
                None => {
                    result = Err(GraphError::New(format!(
                        "Secondary Index {index} not found"
                    )));
                }
            }
        }

        if let Some(bm25) = &self.storage.bm25
            && let Some(props) = node.properties.as_ref()
        {
            let mut data = props.flatten_bm25();
            data.push_str(&node.label);
            if let Err(e) = bm25.insert_doc(self.txn, node.id, &data) {
                result = Err(e);
            }
        }

        if result.is_ok() {
            result = Ok(TraversalValue::Node(node.clone()));
        } else {
            result = Err(GraphError::New(
                "Failed to add node to secondary indices".to_string(),
            ));
        }

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }
}
