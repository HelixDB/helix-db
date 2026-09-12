//! Allocation bounds shared by scalar values and graph/row execution. These
//! describe owned storage, not serialized sizes or process RSS.

/// Bound the nodes retained by a standard-library B-tree, excluding separately
/// allocated key/value payloads and the inline map/set header.
///
/// Rust's B-tree has eleven entries per node and at least five entries in each
/// non-root node. Include one potentially empty retained root, child pointers,
/// and padding. The bound covers arbitrary insertion/removal histories; it does
/// not assume compact bulk construction. Allocation-observer tests validate the
/// supported toolchain against actual sparse, dense, cloned and pruned trees.
/// See <https://doc.rust-lang.org/src/alloc/collections/btree/node.rs.html>.
///
/// ```
/// use helix_planner::relational::allocation;
/// let small = allocation::btree_bytes::<String, i64>(1);
/// assert!(small >= 11 * (size_of::<String>() + size_of::<i64>()));
/// assert!(allocation::btree_bytes::<String, i64>(1_000) > small);
/// assert_eq!(allocation::btree_bytes::<String, i64>(usize::MAX), usize::MAX);
/// ```
pub fn btree_bytes<K, V>(entries: usize) -> usize {
    let alignment = align_of::<K>()
        .max(align_of::<V>())
        .max(align_of::<usize>());
    let node = size_of::<K>()
        .saturating_add(size_of::<V>())
        .saturating_mul(11)
        .saturating_add(16 * size_of::<usize>())
        .saturating_add(alignment.saturating_mul(4));
    let nodes = entries
        .saturating_sub(1)
        .saturating_div(5)
        .saturating_add(1);
    nodes.saturating_mul(node)
}

/// Bound a standard-library hash table grown by single-entry insertions, with
/// no removals or oversized explicit reservations. Payloads owned by keys and
/// values are additional. Reserve both old and new tables during rehashing.
///
/// The supported SwissTable layout rounds the seven-eighths load bound to a
/// power of two. One full 16-byte control group also covers sparse tables;
/// tuple alignment covers padding before control bytes. Independent allocator
/// observations check these assumptions against the supported Rust toolchain.
///
/// ```
/// use helix_planner::relational::allocation;
/// assert_eq!(allocation::hash_table_bytes::<u64, u64>(0), 0);
/// assert!(allocation::hash_table_bytes::<u64, u64>(1) >= 4 * 16);
/// assert_eq!(allocation::hash_table_bytes::<u64, u64>(usize::MAX), usize::MAX);
/// ```
pub fn hash_table_bytes<K, V>(entries: usize) -> usize {
    hash_table_retained_bytes::<K, V>(entries).saturating_mul(2)
}

/// Bound one retained hash table under the insertion contract of
/// [`hash_table_bytes`]. A growth operation must additionally reserve its old
/// table until rehashing finishes. Separating these lifetimes avoids retaining
/// a growth allowance beside later hydration and probe batches.
///
/// ```
/// use helix_planner::relational::allocation;
/// assert_eq!(allocation::hash_table_retained_bytes::<u64, u64>(0), 0);
/// assert_eq!(allocation::hash_table_bytes::<u64, u64>(128),
///     2 * allocation::hash_table_retained_bytes::<u64, u64>(128));
/// ```
pub fn hash_table_retained_bytes<K, V>(entries: usize) -> usize {
    if entries == 0 {
        return 0;
    }
    let Some(buckets) = entries
        .checked_mul(8)
        .map(|entries| entries / 7)
        .and_then(usize::checked_next_power_of_two)
    else {
        return usize::MAX;
    };
    buckets
        .max(16)
        .saturating_mul(size_of::<(K, V)>().saturating_add(1))
        .saturating_add(align_of::<(K, V)>().max(16))
}
