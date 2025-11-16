/// Transaction provider traits for abstracting over LMDB and RocksDB transaction creation
use crate::helix_engine::{
    traversal_core::{RTxn, WTxn},
    types::GraphError,
};

/// Trait for types that can create read transactions
pub trait ReadTransaction {
    fn read_txn(&self) -> Result<RTxn, GraphError>;
}

/// Trait for types that can create write transactions
pub trait WriteTransaction {
    fn write_txn(&self) -> Result<WTxn, GraphError>;
}

// ==================== RocksDB Implementation ====================

#[cfg(feature = "rocks")]
use std::sync::Arc;

#[cfg(feature = "rocks")]
impl ReadTransaction for Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>> {
    fn read_txn(&self) -> Result<RTxn, GraphError> {
        Ok(self.transaction())
    }
}

#[cfg(feature = "rocks")]
impl WriteTransaction for Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>> {
    fn write_txn(&self) -> Result<WTxn, GraphError> {
        Ok(self.transaction())
    }
}
