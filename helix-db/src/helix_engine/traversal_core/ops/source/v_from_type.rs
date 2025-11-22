use crate::helix_engine::{
    traversal_core::{
        traversal_iter::RoTraversalIterator,
        traversal_value::TraversalValue,
    },
    types::GraphError,
};

pub trait VFromTypeAdapter<'db, 'arena, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the vector with the given label.
    ///
    /// Note that the `label` cannot be empty and must be a valid, existing vector label.
    fn v_from_type(
        self,
        label: &'arena str,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    VFromTypeAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn v_from_type(
        self,
        label: &'arena str,
        _get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let _label_bytes = label.as_bytes();
        let iter = self
            .storage
            .vectors
            .vector_properties_db
            .iter(self.txn)
            .unwrap()
            .filter_map(move |_item| todo!());

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}
