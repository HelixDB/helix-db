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

                        // println!("decoded: {res:?}");
                        Some(VectorWithoutData::from_bincode_bytes(
                            arena, bytes, item.id,
                        )?)
                    }
                    None => None, // TODO: maybe should be an error?
                };

                let Some(properties) = properties else {
                    continue;
                };

                if SHOULD_CHECK_DELETED && properties.deleted {
                    continue;
                }

                if properties.label == label
                    && (filter.is_none() || filter.unwrap().iter().all(|f| f(&item, txn)))
                {
                    item.expand_from_vector_without_data(properties);
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

#[cfg(test)]
mod tests {
    use super::*;
    use bumpalo::Bump;

    // ============================================================================
    // Candidate Ord/PartialOrd Tests
    // ============================================================================

    #[test]
    fn test_candidate_ord_by_distance() {
        // Candidate uses reverse ordering (smaller distance = greater in ordering)
        // This is for min-heap behavior in a max-heap
        let c1 = Candidate {
            id: 1,
            distance: 0.5,
        };
        let c2 = Candidate {
            id: 2,
            distance: 1.0,
        };
        let c3 = Candidate {
            id: 3,
            distance: 0.2,
        };

        // c3 has smallest distance, so it should be "greatest" in ordering
        assert!(c3 > c1);
        assert!(c3 > c2);
        assert!(c1 > c2);

        // Verify the reverse: larger distance = smaller in ordering
        assert!(c2 < c1);
        assert!(c2 < c3);
    }

    #[test]
    fn test_candidate_partial_ord_consistency() {
        let c1 = Candidate {
            id: 1,
            distance: 0.5,
        };
        let c2 = Candidate {
            id: 2,
            distance: 0.5,
        };

        // Same distance should be equal in ordering
        assert_eq!(c1.cmp(&c2), Ordering::Equal);
        assert_eq!(c1.partial_cmp(&c2), Some(Ordering::Equal));
    }

    #[test]
    fn test_candidate_equality() {
        let c1 = Candidate {
            id: 1,
            distance: 0.5,
        };
        let c2 = Candidate {
            id: 1,
            distance: 0.5,
        };
        let c3 = Candidate {
            id: 2,
            distance: 0.5,
        };

        assert!(c1 == c2);
        // Different id but same distance - not equal
        assert!(c1 != c3);
    }

    // ============================================================================
    // HeapOps Tests
    // ============================================================================

    #[test]
    fn test_heap_ops_take_inord() {
        let arena = Bump::new();
        let mut heap: BinaryHeap<i32> = BinaryHeap::new(&arena);

        // Push elements in random order
        heap.push(5);
        heap.push(1);
        heap.push(8);
        heap.push(3);
        heap.push(9);

        // Take top 3 elements
        let result = heap.take_inord(3);

        // Result should be a new heap with 3 elements
        assert_eq!(result.len(), 3);

        // Original heap should have remaining elements
        assert_eq!(heap.len(), 2);
    }

    #[test]
    fn test_heap_ops_take_inord_more_than_available() {
        let arena = Bump::new();
        let mut heap: BinaryHeap<i32> = BinaryHeap::new(&arena);

        heap.push(5);
        heap.push(1);

        // Try to take more than available
        let result = heap.take_inord(10);

        // Should only take what's available
        assert_eq!(result.len(), 2);
        assert_eq!(heap.len(), 0);
    }

    #[test]
    fn test_heap_ops_get_max() {
        let arena = Bump::new();
        let mut heap: BinaryHeap<i32> = BinaryHeap::new(&arena);

        heap.push(5);
        heap.push(1);
        heap.push(8);
        heap.push(3);

        // Get max without removal
        let max = heap.get_max();
        assert_eq!(max, Some(&8));

        // Heap should still have all elements
        assert_eq!(heap.len(), 4);
    }

    #[test]
    fn test_heap_ops_get_max_empty() {
        let arena = Bump::new();
        let heap: BinaryHeap<i32> = BinaryHeap::new(&arena);

        let max = heap.get_max();
        assert_eq!(max, None);
    }

    // ============================================================================
    // check_deleted Tests
    // ============================================================================

    #[test]
    fn test_check_deleted_returns_false() {
        // Construct data with: 8-byte header (label length) + label + 1-byte version + deleted flag
        let label = "test";
        let label_len = label.len() as u64;
        let mut data = Vec::new();

        // 8-byte length header (little-endian)
        data.extend_from_slice(&label_len.to_le_bytes());
        // Label bytes
        data.extend_from_slice(label.as_bytes());
        // Version byte
        data.push(0);
        // Deleted flag (0 = not deleted)
        data.push(0);

        assert!(!check_deleted(&data));
    }

    #[test]
    fn test_check_deleted_returns_true() {
        // Construct data with deleted flag = 1
        let label = "test";
        let label_len = label.len() as u64;
        let mut data = Vec::new();

        // 8-byte length header (little-endian)
        data.extend_from_slice(&label_len.to_le_bytes());
        // Label bytes
        data.extend_from_slice(label.as_bytes());
        // Version byte
        data.push(0);
        // Deleted flag (1 = deleted)
        data.push(1);

        assert!(check_deleted(&data));
    }
}
