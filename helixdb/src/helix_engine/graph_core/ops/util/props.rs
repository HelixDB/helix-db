use crate::helix_engine::{
    graph_core::{
        ops::tr_val::TraversalVal,
        traversal_iter::{RoTraversalIterator, RwTraversalIterator},
    },
    types::GraphError,
};

pub struct PropsIterator<'a, I> {
    iter: I,
    prop: &'a str,
}

// TODO: get rid of clones in return values
impl<'a, I> Iterator for PropsIterator<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(Ok(TraversalVal::Node(node))) => match node.properties {
                Some(prop) => {
                    let prop = prop.get(self.prop);
                    match prop {
                        Some(prop) => Some(Ok(TraversalVal::Value(prop.clone()))),
                        None => None,
                    }
                }
                None => None,
            },
            Some(Ok(TraversalVal::Edge(edge))) => match edge.properties {
                Some(prop) => {
                    let prop = prop.get(self.prop);
                    match prop {
                        Some(prop) => Some(Ok(TraversalVal::Value(prop.clone()))),
                        None => None,
                    }
                }
                None => None,
            },
            Some(Ok(TraversalVal::Vector(vec))) => match vec.properties {
                Some(prop) => {
                    let prop = prop.get(self.prop);
                    match prop {
                        Some(prop) => Some(Ok(TraversalVal::Value(prop.clone()))),
                        None => None,
                    }
                }
                None => None,
            },
            _ => None,
        }
    }
}
pub trait PropsAdapter<'a, I>: Iterator<Item = Result<TraversalVal, GraphError>> {
    fn check_property(
        self,
        prop: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I> PropsAdapter<'a, I> for RoTraversalIterator<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    #[inline]
    fn check_property(
        self,
        prop: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        RoTraversalIterator {
            inner: PropsIterator {
                iter: self.inner,
                prop,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}

impl<'a, 'b, I> PropsAdapter<'a, I> for RwTraversalIterator<'a, 'b, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    'b: 'a,
{
    #[inline]
    fn check_property(
        self,
        prop: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        RoTraversalIterator {
            inner: PropsIterator {
                iter: self.inner,
                prop,
            },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
