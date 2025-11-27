use std::{
    io::Read,
    ops::{Range, RangeBounds},
    slice::Iter,
};

use slatedb::DbIterator;

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
        self.scan_with_options(Prefix::<N>::new(prefix), &options)
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
        self.scan_with_options(Prefix::<N>::new_with_table(table, prefix), &options)
            .await
    }

    async fn secondary_index_iter(&self, index: u16) -> Result<DbIterator, slatedb::Error> {
        let options = slatedb::config::ScanOptions::default();
        self.scan_with_options(index.to_be_bytes()..(index + 1).to_be_bytes(), &options)
            .await
    }
}

pub(crate) struct Prefix<const N: usize> {
    // pub table: TableIndexes,
    prefix: [u8; N],
    end: Option<[u8; N]>,
}

impl<const N: usize> Prefix<N> {
    fn new(prefix: &[u8]) -> Self {
        let mut new = [0u8; N];
        new.copy_from_slice(prefix);
        Self {
            prefix: new,
            end: prefix_successor(new),
        }
    }

    fn new_with_table(table: TableIndex, prefix: &[u8]) -> Self {
        assert_eq!(N, prefix.len() + 2);
        let mut new = [0u8; N];
        new[0..2].copy_from_slice(table.as_bytes());
        new[0..prefix.len()].copy_from_slice(prefix);
        Self {
            prefix: new,
            end: prefix_successor(new),
        }
    }
}

impl<const N: usize> RangeBounds<[u8; N]> for Prefix<N> {
    fn start_bound(&self) -> std::ops::Bound<&[u8; N]> {
        std::ops::Bound::Included(&self.prefix)
    }

    fn end_bound(&self) -> std::ops::Bound<&[u8; N]> {
        match self.end {
            Some(ref end) => std::ops::Bound::Excluded(end),
            None => std::ops::Bound::Unbounded,
        }
    }
}

/// computes the smallest byte sequence that is greater than all keys starting with the prefix
const fn prefix_successor<const N: usize>(prefix: [u8; N]) -> Option<[u8; N]> {
    let mut i = N;
    while i > 0 {
        i -= 1;
        if prefix[i] < 0xFF {
            let mut end = prefix;
            end[i] += 1;
            let mut j = i + 1;
            while j < N {
                end[j] = 0;
                j += 1;
            }
            return Some(end);
        }
    }
    None
}
