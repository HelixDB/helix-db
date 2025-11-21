use super::binary_heap::BinaryHeap;
use crate::helix_engine::{
    traversal_core::LMDB_STRING_HEADER_LENGTH,
    types::VectorError,
    vector_core::{vector::HVector, vector_without_data::VectorWithoutData},
};
use heed3::{
    Database, RoTxn,
    byteorder::BE,
    types::{Bytes, U128},
};
use std::cmp::Ordering;

#[derive(PartialEq)]
pub(super) struct Candidate {
    pub id: u128,
    pub distance: f64,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

pub(super) trait HeapOps<'a, T> {
    /// Take the top k elements from the heap
    /// Used because using `.iter()` does not keep the order
    fn take_inord(&mut self, k: usize) -> BinaryHeap<'a, T>
    where
        T: Ord;

    /// Get the maximum element from the heap
    fn get_max<'q>(&'q self) -> Option<&'a T>
    where
        T: Ord,
        'q: 'a;
}

impl<'a, T> HeapOps<'a, T> for BinaryHeap<'a, T> {
    #[inline(always)]
    fn take_inord(&mut self, k: usize) -> BinaryHeap<'a, T>
    where
        T: Ord,
    {
        let mut result = BinaryHeap::with_capacity(self.arena, k);
        for _ in 0..k {
            if let Some(item) = self.pop() {
                result.push(item);
            } else {
                break;
            }
        }
        result
    }

    #[inline(always)]
    fn get_max<'q>(&'q self) -> Option<&'a T>
    where
        T: Ord,
        'q: 'a,
    {
        self.iter().max()
    }
}

pub trait VectorFilter<'db, 'arena, 'txn, 'q> {
    fn to_vec_with_filter<F, const SHOULD_CHECK_DELETED: bool>(
        self,
        k: usize,
        filter: Option<&'arena [F]>,
        label: &'arena str,
        txn: &'txn RoTxn<'db>,
        db: Database<U128<BE>, Bytes>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &'txn RoTxn<'db>) -> bool;
}

impl<'db, 'arena, 'txn, 'q> VectorFilter<'db, 'arena, 'txn, 'q>
    for BinaryHeap<'arena, HVector<'arena>>
{
    #[inline(always)]
    fn to_vec_with_filter<F, const SHOULD_CHECK_DELETED: bool>(
        mut self,
        k: usize,
        filter: Option<&'arena [F]>,
        label: &'arena str,
        txn: &'txn RoTxn<'db>,
        db: Database<U128<BE>, Bytes>,
        arena: &'arena bumpalo::Bump,
    ) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError>
    where
        F: Fn(&HVector<'arena>, &'txn RoTxn<'db>) -> bool,
    {
        let mut result = bumpalo::collections::Vec::with_capacity_in(k, arena);
        for _ in 0..k {
            // while pop check filters and pop until one passes
            while let Some(mut item) = self.pop() {
                let properties = match db.get(txn, &item.id)? {
                    Some(bytes) => {
                        // println!("decoding");
                        let res = Some(VectorWithoutData::from_bincode_bytes(
                            arena, bytes, item.id,
                        )?);
                        // println!("decoded: {res:?}");
                        res
                    }
                    None => None, // TODO: maybe should be an error?
                };

                if let Some(properties) = properties
                    && SHOULD_CHECK_DELETED
                    && properties.deleted
                {
                    continue;
                }

                if item.label() == label
                    && (filter.is_none() || filter.unwrap().iter().all(|f| f(&item, txn)))
                {
                    assert!(
                        properties.is_some(),
                        "properties should be some, otherwise there has been an error on vector insertion as properties are always inserted"
                    );
                    item.expand_from_vector_without_data(properties.unwrap());
                    result.push(item);
                    break;
                }
            }
        }

        Ok(result)
    }
}

pub fn check_deleted(data: &[u8]) -> bool {
    assert!(
        data.len() >= LMDB_STRING_HEADER_LENGTH,
        "value length does not contain header which means the `label` field was missing from the node on insertion"
    );
    let length_of_label_in_lmdb =
        u64::from_le_bytes(data[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap()) as usize;

    let length_of_version_in_lmdb = 1;

    let deleted_index =
        LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb + length_of_version_in_lmdb;

    assert!(
        data.len() >= deleted_index,
        "data length is not at least the deleted index plus the length of the deleted field meaning there has been a corruption on node insertion"
    );
    data[deleted_index] == 1
}
