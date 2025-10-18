use crate::helix_engine::{
    storage_core::HelixGraphStorage,
    traversal_core::{
        traversal_iter::{RoTraversalIterator, RwTraversalIterator},
        traversal_value::{IntoTraversalValues, TraversalValue, Variable},
    },
    types::GraphError,
};
use heed3::{RoTxn, RwTxn};
use std::borrow::Cow;

pub struct G {}

impl G {
    /// Starts a new empty traversal
    ///
    /// # Arguments
    ///
    /// * `storage` - An owned Arc of the storage for the traversal
    /// * `txn` - A reference to the transaction for the traversal
    ///
    /// # Example
    ///
    /// ```rust
    /// let storage = Arc::new(HelixGraphStorage::new());
    /// let txn = storage.graph_env.read_txn().unwrap();
    /// let traversal = G::new(storage, &txn);
    /// ```
    #[inline]
    pub fn new<'db, 'arena, 'txn>(
        storage: &'db HelixGraphStorage,
        txn: &'txn RoTxn<'db>,
        arena: &'arena bumpalo::Bump,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<Variable<'arena>, GraphError>>,
    >
    where
        Self: Sized,
    {
        RoTraversalIterator {
            storage,
            txn,
            arena,
            inner: std::iter::once(Ok(Cow::Owned(TraversalValue::Empty))),
        }
    }

    /// Starts a new traversal from a vector of traversal values
    ///
    /// # Arguments
    ///
    /// * `storage` - An owned Arc of the storage for the traversal
    /// * `txn` - A reference to the transaction for the traversal
    /// * `items` - A vector of traversal values to start the traversal from
    ///
    /// # Example
    ///
    /// ```rust
    /// let storage = Arc::new(HelixGraphStorage::new());
    /// let txn = storage.graph_env.read_txn().unwrap();
    /// let traversal = G::from_iter(storage, &txn, vec![TraversalValue::Node(Node { id: 1, label: "Person".to_string(), properties: None })]);
    /// ```
    pub fn from_iter<'db, 'arena, 'txn>(
        storage: &'db HelixGraphStorage,
        txn: &'txn RoTxn<'db>,
        items: impl Iterator<Item = Cow<'arena, TraversalValue<'arena>>>,
        arena: &'arena bumpalo::Bump,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<Variable<'arena>, GraphError>>,
    > {
        RoTraversalIterator {
            inner: items.map(Ok),
            storage,
            txn,
            arena,
        }
    }

    /// Starts a new mutable traversal
    ///
    /// # Arguments
    ///
    /// * `storage` - An owned Arc of the storage for the traversal
    /// * `txn` - A reference to the transaction for the traversal
    /// * `items` - A vector of traversal values to start the traversal from
    ///
    /// # Example
    ///
    /// ```rust
    /// let storage = Arc::new(HelixGraphStorage::new());
    /// let txn = storage.graph_env.write_txn().unwrap();
    /// let traversal = G::new_mut(storage, &mut txn);
    /// ```
    pub fn new_mut<'db, 'arena, 'txn>(
        storage: &'db HelixGraphStorage,
        arena: &'arena bumpalo::Bump,
        txn: &'txn mut RwTxn<'db>,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        Self: Sized,
    {
        RwTraversalIterator {
            storage,
            arena,
            txn,
            inner: std::iter::once(Ok(TraversalValue::Empty)),
        }
    }

    /// Starts a new mutable traversal from a vector of traversal values
    ///
    /// # Arguments
    ///
    /// * `storage` - An owned Arc of the storage for the traversal
    /// * `txn` - A reference to the transaction for the traversal
    /// * `items` - A vector of traversal values to start the traversal from
    ///
    /// # Example
    ///
    /// ```rust
    /// let storage = Arc::new(HelixGraphStorage::new());
    /// let txn = storage.graph_env.write_txn().unwrap();
    /// let traversal = G::new_mut_from(storage, &mut txn, vec![TraversalValue::Node(Node { id: 1, label: "Person".to_string(), properties: None })]);
    /// ```
    pub fn new_mut_from<'db, 'arena, 'txn, T: IntoTraversalValues<'arena>>(
        storage: &'db HelixGraphStorage,
        arena: &'arena bumpalo::Bump,
        txn: &'txn mut RwTxn<'db>,
        items: impl Iterator<Item = Cow<'arena, TraversalValue<'arena>>>,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<Variable<'arena>, GraphError>>,
    > {
        RwTraversalIterator {
            inner: items.map(Ok),
            storage,
            txn,
            arena,
        }
    }
}
