use heed3::RwTxn;

use crate::helix_engine::{traversal_core::traversal_value::TraversalValue, types::GraphError};

pub struct FilterMut<'db, 'txn, I, F> {
    iter: I,
    txn: &'txn mut RwTxn<'db>,
    f: F,
}

impl<'db, 'arena, 'txn, I, F> Iterator for FilterMut<'db, 'txn, I, F>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    F: FnMut(&mut I::Item, &mut RwTxn) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(mut item) => match (self.f)(&mut item, self.txn) {
                true => Some(item),
                false => None,
            },
            None => None,
        }
    }
}
