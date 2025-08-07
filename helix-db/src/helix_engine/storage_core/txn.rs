use heed3::RoTxn;
use std::ops::Deref;

/// A wrapper around RoTxn that implements Sync for safe concurrent access.
/// This allows read-only transactions to be safely shared across threads.
/// 
/// # Safety
/// 
/// LMDB read-only transactions are thread-safe for concurrent reads.
/// RoTxn is immutable and LMDB guarantees that read transactions can be 
/// safely shared across threads as they provide a consistent view of the 
/// database at the time the transaction was created.
/// 
/// From LMDB documentation: "Readers don't lock anything. Many readers can 
/// have active transactions at the same time, all of them can access the 
/// same version of data."
/// 
/// This implementation assumes:
/// 1. The transaction is only used for read operations
/// 2. The environment was opened with read_txn_without_tls() for thread safety
/// 3. No cursor modifications are performed (read-only operations only)
pub struct HelixTxnRead<'a>(pub RoTxn<'a>);

impl<'a> HelixTxnRead<'a> {
    /// Create a new HelixTxnRead from a RoTxn
    pub fn new(txn: RoTxn<'a>) -> Self {
        Self(txn)
    }
    
    /// Get the inner RoTxn
    pub fn inner(&self) -> &RoTxn<'a> {
        &self.0
    }
    
    /// Convert into the inner RoTxn
    pub fn into_inner(self) -> RoTxn<'a> {
        self.0
    }
}

impl<'a> Deref for HelixTxnRead<'a> {
    type Target = RoTxn<'a>;
    
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// SAFETY: LMDB read-only transactions are thread-safe for concurrent reads.
/// RoTxn is immutable and LMDB guarantees that read transactions can be 
/// safely shared across threads as they provide a consistent view of the 
/// database at the time the transaction was created.
unsafe impl<'a> Sync for HelixTxnRead<'a> {}

/// SAFETY: LMDB read-only transactions are Send as they can be transferred
/// between threads safely since they are immutable snapshots.
unsafe impl<'a> Send for HelixTxnRead<'a> {}


