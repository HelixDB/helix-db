use crate::{
    helix_engine::{
        graph_core::traversal_iter::RwTraversalIterator,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::value::Value,
};

use super::super::tr_val::TraversalVal;

pub struct Update<I> {
    iter: I,
}

impl<I> Iterator for Update<I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait UpdateAdapter<'scope, 'env>: Iterator  {
    fn update(
        self,
        props: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'scope, 'env, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'scope, 'env, I: Iterator<Item = Result<TraversalVal, GraphError>>> UpdateAdapter<'scope, 'env>
    for RwTraversalIterator<'scope, 'env, I>
{
    fn update(
        self,
        props: Option<Vec<(String, Value)>>,
    ) -> RwTraversalIterator<'scope, 'env, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    {
        let storage = self.storage.clone();

        let capacity = match self.inner.size_hint() {
            (_, Some(upper)) => upper,
            (lower, None) => lower,
        };
        let mut vec = Vec::with_capacity(capacity);

        for item in self.inner {
            match item {
                Ok(TraversalVal::Node(node)) => match storage.get_node(self.txn, &node.id) {
                    Ok(mut old_node) => {
                        if let Some(mut properties) = old_node.properties {
                            if let Some(ref props) = props {
                                for (k, v) in props.iter() {
                                    properties.insert(k.clone(), v.clone());
                                }
                            }
                            for (key, v) in properties.iter() {
                                if let Some(db) = storage.secondary_indices.get(key) {
                                    match bincode::serialize(v) {
                                        Ok(serialized) => {
                                            if let Err(e) = db.put(
                                                self.txn,
                                                &serialized,
                                                &node.id,
                                            ) {
                                                vec.push(Err(GraphError::from(e)));
                                            }
                                        }
                                        Err(e) => vec.push(Err(GraphError::from(e))),
                                    }
                                }
                            }
                            old_node.properties = Some(properties);
                        }
                        match bincode::serialize(&node) {
                            Ok(serialized) => {
                                match storage.nodes_db.put(
                                    self.txn,
                                    &HelixGraphStorage::node_key(&node.id),
                                    &serialized,
                                ) {
                                    Ok(_) => vec.push(Ok(TraversalVal::Node(old_node))),
                                    Err(e) => vec.push(Err(GraphError::from(e))),
                                }
                            }
                            Err(e) => vec.push(Err(GraphError::from(e))),
                        }
                    }
                    Err(e) => vec.push(Err(e)),
                },
                Ok(TraversalVal::Edge(edge)) => match storage.get_edge(self.txn, &edge.id) {
                    Ok(old_edge) => {
                        let mut old_edge = old_edge.clone();
                        if let Some(mut properties) = old_edge.properties.clone() {
                            if let Some(ref props) = props {
                                for (k, v) in props.iter() {
                                    properties.insert(k.clone(), v.clone());
                                }
                                old_edge.properties = Some(properties);
                            }
                        }
                        match bincode::serialize(&edge) {
                            Ok(serialized) => {
                                match storage.nodes_db.put(
                                    self.txn,
                                    &HelixGraphStorage::edge_key(&edge.id),
                                    &serialized,
                                ) {
                                    Ok(_) => vec.push(Ok(TraversalVal::Edge(old_edge))),
                                    Err(e) => vec.push(Err(GraphError::from(e))),
                                }
                            }
                            Err(e) => vec.push(Err(GraphError::from(e))),
                        }
                    }
                    Err(e) => vec.push(Err(e)),
                },
                _ => vec.push(Err(GraphError::New("Unsupported value type".to_string()))),
            }
        }
        RwTraversalIterator {
            inner: Update {
                iter: vec.into_iter(),
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
