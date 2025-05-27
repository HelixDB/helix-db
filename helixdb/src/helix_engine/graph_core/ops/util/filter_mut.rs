use heed3::RwTxn;

use crate::helix_engine::{
    graph_core::ops::tr_val::TraversalVal,
    types::GraphError,
};

pub struct FilterMut<'a, I, F> {
    iter: I,
    txn: &'a mut RwTxn<'a>,
    f: F,
}

impl<'a, I, F> Iterator for FilterMut<'a, I, F>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
    F: FnMut(&mut I::Item, &mut RwTxn) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(mut item) => match (self.f)(&mut item, &mut self.txn) {
                true => Some(item),
                false => None,
            },
            None => None,
        }
    }
}