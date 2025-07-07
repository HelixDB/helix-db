use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        bm25::bm25::{BM25Flatten, BM25},
        graph_core::traversal_iter::RwTraversalIterator,
        types::GraphError,
    },
    protocol::{
        filterable::Filterable,
        id::v6_uuid, items::Node,
        value::Value,
    },
};
use heed3::PutFlags;

pub struct AddNIterator {
    inner: std::iter::Once<Result<TraversalVal, GraphError>>,
}

impl Iterator for AddNIterator {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait AddNAdapter<'a, 'b>: Iterator<Item = Result<TraversalVal, GraphError>> {
    fn add_n(
        self,
        label: &'a str,
        properties: Option<Vec<(String, Value)>>,
        secondary_indices: Option<&'a [&str]>,
    ) -> RwTraversalIterator<'a, 'b, std::iter::Once<Result<TraversalVal, GraphError>>>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>>> AddNAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn add_n(
        self,
        label: &'a str,
        properties: Option<Vec<(String, Value)>>,
        secondary_indices: Option<&'a [&str]>,
    ) -> RwTraversalIterator<'a, 'b, std::iter::Once<Result<TraversalVal, GraphError>>> {
        let node = Node {
            id: v6_uuid(),
            label: label.to_string(), // TODO: just &str or Cow<'a, str>
            properties: properties.map(|props| props.into_iter().collect()),
        };
        println!("node created");
        let secondary_indices = secondary_indices.unwrap_or(&[]).to_vec();
        let mut result: Result<TraversalVal, GraphError> = Ok(TraversalVal::Empty);

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
            Err(e) => result = Err(GraphError::from(e)),
        }

        for index in secondary_indices {
            match self.storage.secondary_indices.get(index) {
                Some(db) => {
                    let key = match node.check_property(&index) {
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
                                println!("{} Error adding node to secondary index: {:?}", line!(), e);
                                result = Err(GraphError::from(e));
                            }
                        }
                        Err(e) => result = Err(GraphError::from(e)),
                    }
                }
                None => {
                    result = Err(GraphError::New(format!(
                        "Secondary Index {} not found",
                        index
                    )));
                }
            }
        }

        // auto inserts to bm25 if should_add_to_bm25 is true
        if node.properties.is_some() {
            let mut data = node
                .properties
                .as_ref()
                .map(|props| props.flatten_bm25())
                .unwrap_or_default();
            data.push_str(&node.label);
            match self.storage.bm25.insert_doc(self.txn, node.id, &data) {
                Ok(_) => {}
                Err(e) => {
                    result = Err(GraphError::from(e));
                }
            }
        }

        if result.is_ok() {
            result = Ok(TraversalVal::Node(node.clone()));
        } else {
            result = Err(GraphError::New(format!(
                "Failed to add node to secondary indices"
            )));
        }

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
