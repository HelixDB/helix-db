pub(super) trait RocksUtils<'db> {
    fn raw_prefix_iter(
        &self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        prefix: &[u8],
    ) -> rocksdb::DBRawIteratorWithThreadMode<'_, rocksdb::Transaction<'_, rocksdb::TransactionDB>>;
}

impl<'db> RocksUtils<'db> for rocksdb::Transaction<'db, rocksdb::TransactionDB> {
    fn raw_prefix_iter(
        &self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        prefix: &[u8],
    ) -> rocksdb::DBRawIteratorWithThreadMode<'_, rocksdb::Transaction<'_, rocksdb::TransactionDB>>
    {
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_iterate_range(rocksdb::PrefixRange(prefix));
        let mut iterator = self.raw_iterator_cf_opt(cf_handle, ro);
        iterator.seek(prefix);
        iterator
    }
}
