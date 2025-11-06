use crate::helix_engine::types::GraphError;

pub struct RTxn<'db> {
    #[cfg(feature = "lmdb")]
    pub txn: heed3::RoTxn<'db>,
    #[cfg(feature = "rocks")]
    pub txn: rocksdb::Transaction<'db, rocksdb::TransactionDB>,
}

/// Rocks implementation of txn
#[cfg(feature = "rocks")]
impl<'db> RTxn<'db> {
    pub fn new(env: &'db rocksdb::TransactionDB) -> rocksdb::Transaction<'db, rocksdb::TransactionDB> {
        env.transaction()
    }

    pub fn commit(self) -> Result<(), GraphError> {
        self.txn.commit().map_err(|_| GraphError::Default)
    }
}

pub struct Wtxn<'db> {
    #[cfg(feature = "lmdb")]
    pub txn: heed3::RwTxn<'db>,
    #[cfg(feature = "rocks")]
    pub txn: rocksdb::Transaction<'db, rocksdb::TransactionDB>,
}

/// Rocks implementation of txn
#[cfg(feature = "rocks")]
impl<'db> Wtxn<'db> {
    pub fn new(env: &'db rocksdb::TransactionDB) -> rocksdb::Transaction<'db, rocksdb::TransactionDB> {
        env.transaction()
    }

    pub fn commit(self) -> Result<(), GraphError> {
        self.txn.commit().map_err(|_| GraphError::Default)
    }
}
