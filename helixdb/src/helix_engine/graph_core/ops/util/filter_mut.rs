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

// implementing iterator for filter ref
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

// pub trait FilterMutAdapter<'a, 'b>: Iterator + Sized {
//     /// FilterMut filters the iterator by taking a mutable
//     /// reference to each item and a transaction.
//     fn filter_mut<F>(
//         self,
//         f: F,
//     ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
//     where
//         F: FnMut(&mut Result<TraversalVal, GraphError>, &mut RwTxn) -> bool,
//         'b: 'a;
// }

// impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> FilterMutAdapter<'a, 'b>
//     for RwTraversalIterator<'a, 'b, I>
// {
//     fn filter_mut<F>(
//         self,
//         f: F,
//     ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>
//     where
//         F: FnMut(&mut Result<TraversalVal, GraphError>, &mut RwTxn) -> bool,
//         'b: 'a,
//     {
//         RwTraversalIterator {
//             inner: FilterMut {
//                 iter: self.inner,
//                 txn: self.txn,
//                 f,
//             },
//             storage: self.storage,
//             txn: self.txn,
//         }
//     }
// }
