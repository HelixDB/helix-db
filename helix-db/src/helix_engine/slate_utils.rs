use std::{
    io::Read,
    ops::{Bound, Range, RangeBounds},
    slice::Iter,
};

use slatedb::{DbIterator, object_store::prefix};

use crate::helix_engine::storage_core::TableIndex;
use async_trait::async_trait;

#[async_trait]
pub trait SlateUtils {
    async fn prefix_iter<const N: usize>(
        &self,
        prefix: &[u8],
    ) -> Result<DbIterator, slatedb::Error>;

    async fn table_iter(&self, table: TableIndex) -> Result<DbIterator, slatedb::Error>;

    async fn table_prefix_iter<const N: usize>(
        &self,
        table: TableIndex,
        prefix: &[u8],
    ) -> Result<DbIterator, slatedb::Error>;

    async fn secondary_index_iter(&self, index: u16) -> Result<DbIterator, slatedb::Error>;
}

#[async_trait]
impl SlateUtils for slatedb::DBTransaction {
    async fn prefix_iter<const N: usize>(
        &self,
        prefix: &[u8],
    ) -> Result<DbIterator, slatedb::Error> {
        let options = slatedb::config::ScanOptions::default();
        self.scan_with_options(prefix_bound::<N>(prefix), &options)
            .await
    }

    async fn table_iter(&self, table: TableIndex) -> Result<DbIterator, slatedb::Error> {
        let options = slatedb::config::ScanOptions::default();
        match table.next_index_as_bytes() {
            Some(next_table_index) => {
                self.scan_with_options(table.as_bytes()..next_table_index, &options)
                    .await
            }
            None => self.scan_with_options(table.as_bytes().., &options).await,
        }
    }

    async fn table_prefix_iter<const N: usize>(
        &self,
        table: TableIndex,
        prefix: &[u8],
    ) -> Result<DbIterator, slatedb::Error> {
        let options = slatedb::config::ScanOptions::default();
        self.scan_with_options(table_prefix_bound::<N>(table, prefix), &options)
            .await
    }

    async fn secondary_index_iter(&self, index: u16) -> Result<DbIterator, slatedb::Error> {
        let options = slatedb::config::ScanOptions::default();
        self.scan_with_options(index.to_be_bytes()..(index + 1).to_be_bytes(), &options)
            .await
    }
}

fn prefix_bound<const N: usize>(prefix_slice: &[u8]) -> (Bound<[u8; N]>, Bound<[u8; N]>) {
    let mut prefix = [0u8; N];
    prefix.copy_from_slice(prefix_slice);

    let mut end_buf = prefix;

    let start = Bound::Included(prefix);
    let end =
        prefix_successor_bytes(&mut end_buf).map_or(Bound::Unbounded, |_| Bound::Excluded(end_buf));

    (start, end)
}

fn table_prefix_bound<const N: usize>(
    table: TableIndex,
    prefix_slice: &[u8],
) -> (Bound<[u8; N]>, Bound<[u8; N]>) {
    assert_eq!(N, prefix_slice.len() + 2); // might not be needed
    let mut prefix = [0u8; N];
    prefix[0..2].copy_from_slice(table.as_bytes());
    prefix[2..prefix_slice.len() + 2].copy_from_slice(prefix_slice);

    let mut end_buf = prefix;
    let start = Bound::Included(prefix);
    let end =
        prefix_successor_bytes(&mut end_buf).map_or(Bound::Unbounded, |_| Bound::Excluded(end_buf));

    (start, end)
}

/// computes the smallest byte sequence that is greater than all keys starting with the prefix
fn prefix_successor_bytes(prefix: &mut [u8]) -> Option<()> {
    for i in (0..prefix.len()).rev() {
        if prefix[i] < 0xFF {
            prefix[i] += 1;
            prefix[i + 1..].fill(0);
            return Some(());
        }
    }
    None
}

#[async_trait]
pub trait Entries {
    type Key;
    type Value;

    async fn entry(&mut self) -> Result<Option<(Self::Key, Self::Value)>, slatedb::Error>;
    async fn key(&mut self) -> Result<Option<Self::Key>, slatedb::Error>;
    async fn value(&mut self) -> Result<Option<Self::Value>, slatedb::Error>;
}

#[async_trait]
impl Entries for slatedb::DbIterator {
    type Key = bytes::Bytes;
    type Value = bytes::Bytes;

    async fn entry(&mut self) -> Result<Option<(Self::Key, Self::Value)>, slatedb::Error> {
        self.next()
            .await
            .map(|entry| entry.map(|kv| (kv.key, kv.value)))
    }

    async fn key(&mut self) -> Result<Option<Self::Key>, slatedb::Error> {
        self.next().await.map(|entry| entry.map(|kv| kv.key))
    }

    async fn value(&mut self) -> Result<Option<Self::Value>, slatedb::Error> {
        self.next().await.map(|entry| entry.map(|kv| kv.value))
    }
}

pub async fn table_prefix_delete<const N: usize>(
    txn: &slatedb::DBTransaction,
    batch: &mut slatedb::WriteBatch,
    table: TableIndex,
    prefix: &[u8],
) -> Result<(), slatedb::Error> {
    let options = slatedb::config::ScanOptions::new()
        .with_max_fetch_tasks(8)
        .with_read_ahead_bytes(64 * 1024);
    let mut iter = txn
        .scan_with_options(table_prefix_bound::<N>(table, prefix), &options)
        .await?;
    while let Some(key) = iter.next().await?.map(|entry| entry.key) {
        batch.delete(key);
    }
    Ok(())
}

pub async fn table_scan_delete<const N: usize>(
    txn: &slatedb::DBTransaction,
    batch: &mut slatedb::WriteBatch,
    table: TableIndex,
    prefix: &[u8],
) -> Result<(), slatedb::Error> {
    let options = slatedb::config::ScanOptions::new()
        .with_max_fetch_tasks(8)
        .with_read_ahead_bytes(64 * 1024);
    let mut iter = txn
        .scan_with_options(table_prefix_bound::<N>(table, prefix), &options)
        .await?;
    while let Some(key) = iter.next().await?.map(|entry| entry.key) {
        batch.delete(key);
    }
    Ok(())
}
