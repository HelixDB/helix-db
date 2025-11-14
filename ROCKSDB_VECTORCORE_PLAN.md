# RocksDB VectorCore Implementation Plan

## Executive Summary

This document outlines the plan to implement RocksDB storage functions for VectorCore, enabling vector operations (including `from_v`) to work with the RocksDB backend. This follows the existing dual-implementation pattern used in BM25 (lmdb_bm25.rs / rocks_bm25.rs).

## Background

### Current State
- VectorCore currently only has LMDB implementation
- The `from_v` traversal operation depends on VectorCore methods:
  - `get_full_vector()` - loads complete vectors with data
  - `get_vector_properties()` - loads metadata without vector data
- HNSW (Hierarchical Navigable Small World) graph structure is stored in three databases

### Goal
Implement RocksDB versions of VectorCore storage functions to:
1. Enable `from_v` operations with RocksDB backend
2. Support vector CRUD operations
3. Maintain HNSW graph structure in RocksDB
4. Keep feature-flag based compilation working (`#[cfg(feature = "rocks")]` / `#[cfg(feature = "lmdb")]`)

---

## File Structure Changes

### Current Structure
```
helix-db/src/helix_engine/vector_core/
├── mod.rs
├── vector_core.rs       (LMDB only)
├── vector.rs
├── vector_without_data.rs
└── ... (other files)
```

### Proposed Structure
```
helix-db/src/helix_engine/vector_core/
├── mod.rs               (updated with feature flags)
├── lmdb_vector_core.rs  (moved from vector_core.rs)
├── rocks_vector_core.rs (NEW - RocksDB implementation)
├── vector.rs
├── vector_without_data.rs
└── ... (other files)
```

### mod.rs Changes
```rust
// Separate implementation files
pub mod lmdb_vector_core;
#[cfg(feature = "rocks")]
pub mod rocks_vector_core;

// Conditional exports
#[cfg(feature = "lmdb")]
pub use lmdb_vector_core::VectorCore;
#[cfg(feature = "rocks")]
pub use rocks_vector_core::VectorCore;
```

---

## RocksDB VectorCore Architecture

### Struct Definition

```rust
#[cfg(feature = "rocks")]
pub struct VectorCore<'db> {
    pub graph_env: Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>>,
    pub vectors_db: Arc<rocksdb::BoundColumnFamily<'db>>,
    pub vector_properties_db: Arc<rocksdb::BoundColumnFamily<'db>>,
    pub edges_db: Arc<rocksdb::BoundColumnFamily<'db>>,
    pub config: HNSWConfig,
}
```

### Database Mapping

| LMDB Database | RocksDB Column Family | Purpose |
|---------------|----------------------|---------|
| `vectors_db` | `vectors` | Raw vector data (f64 arrays) |
| `vector_properties_db` | `vector_data` | Vector metadata (label, version, properties) |
| `edges_db` | `hnsw_out_nodes` | HNSW graph edges |

These column families are already created in `storage_core/mod.rs` lines 607-611.

### Key Structures

**Vector Data Key Format:**
```
Key: b"v:" + id.to_be_bytes() (16 bytes) + level.to_be_bytes() (16 bytes)
Value: Raw f64 array as bytes (bytemuck)
```

**Vector Properties Key Format:**
```
Key: id.to_be_bytes() (16 bytes)
Value: Bincode-serialized VectorWithoutData
```

**HNSW Edge Key Format:**
```
Key: source_id.to_be_bytes() (16) + level.to_be_bytes() (16) + sink_id.to_be_bytes() (16)
Value: () [Unit type - presence indicates edge exists]
```

---

## Implementation Phases

## Phase 1: Core Storage Functions (CRITICAL)

These methods are required for basic vector operations and `from_v` functionality.

### 1.1 Constructor: `new()`

**Purpose:** Initialize VectorCore with RocksDB column family handles

**Signature:**
```rust
pub fn new(
    graph_env: Arc<rocksdb::TransactionDB<rocksdb::MultiThreaded>>,
    config: HNSWConfig,
) -> Result<Self, VectorError>
```

**Implementation:**
- Get column family handles via `graph_env.cf_handle("vectors")` etc.
- Wrap in `Arc` for shared ownership
- Return constructed VectorCore

**Reference:** storage_core/mod.rs lines 549-687 (RocksDB HelixGraphStorage constructor)

---

### 1.2 Method: `get_vector_properties()`

**Purpose:** Load vector metadata without the f64 data array (used by from_v for filtering)

**Current LMDB Implementation:** vector_core.rs line 392

**Signature:**
```rust
pub fn get_vector_properties<'db: 'arena, 'arena: 'txn, 'txn>(
    &self,
    txn: &'txn RTxn<'db>,
    id: u128,
    arena: &'arena bumpalo::Bump,
) -> Result<Option<VectorWithoutData<'arena>>, VectorError>
```

**RocksDB Implementation Steps:**
1. Access database: `txn.txn.get_pinned_cf(&self.vector_properties_db, id.to_be_bytes())`
2. Handle `None` case (vector not found)
3. Deserialize: `VectorWithoutData::from_bincode_bytes(data, arena)?`
4. Check deleted flag: `if vector.deleted { return Err(...) }`
5. Return `Ok(Some(vector))`

**Key Differences from LMDB:**
- LMDB: `self.vector_properties_db.get(txn, &id)?`
- RocksDB: `txn.txn.get_pinned_cf(&self.vector_properties_db, &id.to_be_bytes())?`
- Manual byte conversion for keys

---

### 1.3 Method: `get_full_vector()`

**Purpose:** Load complete vector with f64 data array (used by from_v when vector data is needed)

**Current LMDB Implementation:** vector_core.rs line 414

**Signature:**
```rust
pub fn get_full_vector<'arena>(
    &self,
    txn: &RTxn,
    id: u128,
    arena: &'arena bumpalo::Bump,
) -> Result<HVector<'arena>, VectorError>
```

**RocksDB Implementation Steps:**
1. Construct key: `let key = vector_key(id, 0);` (level=0 for base vectors)
2. Get raw data: `txn.txn.get_pinned_cf(&self.vectors_db, &key)?`
3. Get properties: `txn.txn.get_pinned_cf(&self.vector_properties_db, &id.to_be_bytes())?`
4. Deserialize both: `HVector::from_bincode_bytes(vector_data, props_data, arena)?`
5. Check deleted flag
6. Return complete vector

**Key Differences from LMDB:**
- Need two separate get_cf calls (one for data, one for properties)
- LMDB uses typed keys (U128<BE>), RocksDB uses byte arrays

---

### 1.4 Method: `put_vector()`

**Purpose:** Write vector to storage (both data and properties)

**Current LMDB Implementation:** vector_core.rs line 152

**Signature:**
```rust
pub fn put_vector<'arena>(
    &self,
    txn: &mut WTxn,
    vector: &HVector<'arena>,
) -> Result<(), VectorError>
```

**RocksDB Implementation Steps:**
1. Serialize vector data: `let data_bytes = vector.vector_data_to_bytes()?;`
2. Construct key: `let key = vector_key(vector.id, vector.level);`
3. Write data: `txn.txn.put_cf(&self.vectors_db, &key, &data_bytes)?;`
4. Serialize properties: `let props = bincode::serialize(&vector)?;`
5. Write properties: `txn.txn.put_cf(&self.vector_properties_db, &vector.id.to_be_bytes(), &props)?;`

**Key Differences from LMDB:**
- LMDB: Single put operation per database
- RocksDB: Need explicit put_cf calls with column family handles

---

### 1.5 Method: `get_raw_vector_data()`

**Purpose:** Performance optimization - get only f64 array without deserializing properties

**Current LMDB Implementation:** vector_core.rs line 436

**Signature:**
```rust
pub fn get_raw_vector_data<'db: 'arena, 'arena: 'txn, 'txn>(
    &self,
    txn: &'txn RTxn<'db>,
    id: u128,
    label: &'arena str,
    arena: &'arena bumpalo::Bump,
) -> Result<HVector<'arena>, VectorError>
```

**RocksDB Implementation Steps:**
1. Construct key: `let key = vector_key(id, 0);`
2. Get data: `txn.txn.get_pinned_cf(&self.vectors_db, &key)?`
3. Convert bytes to f64 slice: `bytemuck::cast_slice(data)`
4. Create minimal HVector with provided label and default metadata
5. Return vector

**Usage:** Called by `get_neighbors()` when traversing HNSW graph

---

## Phase 2: HNSW Graph Storage (HIGH PRIORITY)

These methods manage the HNSW graph structure for nearest neighbor search.

### 2.1 Methods: Entry Point Management

**Purpose:** Track the entry point of the HNSW graph (highest-level node)

**Current LMDB Implementation:** vector_core.rs lines 122-149

#### `get_entry_point()`

**Signature:**
```rust
pub fn get_entry_point(&self, txn: &RTxn) -> Result<Option<u128>, VectorError>
```

**RocksDB Implementation:**
1. Read special key: `txn.txn.get_cf(&self.vectors_db, ENTRY_POINT_KEY)?`
   - `ENTRY_POINT_KEY = b"entry_point"`
2. If found, deserialize as u128: `u128::from_be_bytes(data.try_into()?)`
3. Return `Ok(Some(id))` or `Ok(None)`

#### `set_entry_point()`

**Signature:**
```rust
pub fn set_entry_point(&self, txn: &mut WTxn, id: u128) -> Result<(), VectorError>
```

**RocksDB Implementation:**
1. Serialize id: `id.to_be_bytes()`
2. Write: `txn.txn.put_cf(&self.vectors_db, ENTRY_POINT_KEY, &bytes)?`

---

### 2.2 Method: `get_neighbors()`

**Purpose:** Retrieve all neighbors of a node at a specific HNSW level

**Current LMDB Implementation:** vector_core.rs line 170

**Signature:**
```rust
fn get_neighbors<'db: 'arena, 'arena: 'txn, 'txn, F>(
    &self,
    txn: &'txn RTxn<'db>,
    label: &'arena str,
    id: u128,
    level: usize,
    filter: Option<&[F]>,
    arena: &'arena bumpalo::Bump,
) -> Result<bumpalo::collections::Vec<'arena, HVector<'arena>>, VectorError>
where
    F: Fn(&HVector) -> bool,
```

**RocksDB Implementation Steps:**
1. Construct prefix key: `let prefix = out_edges_key(id, level, None);`
   - Key format: `[id(16)][level(16)]` = 32 bytes prefix
2. Create prefix iterator: `txn.txn.prefix_iterator_cf(&self.edges_db, &prefix)`
3. For each key in iterator:
   - Parse neighbor ID from key suffix (bytes 32-48)
   - Convert to u128: `u128::from_be_bytes(...)`
   - Load vector: `self.get_raw_vector_data(txn, neighbor_id, label, arena)?`
4. Apply optional filters
5. Collect into arena-allocated Vec
6. Return neighbors

**Key Differences from LMDB:**
- LMDB: `db.prefix_iter()` returns typed (key, value) pairs
- RocksDB: `prefix_iterator_cf()` returns raw bytes, need manual parsing

---

### 2.3 Method: `set_neighbours()`

**Purpose:** Update the neighbor set for a node (add/remove edges)

**Current LMDB Implementation:** vector_core.rs line 222

**Signature:**
```rust
fn set_neighbours<'db: 'arena, 'arena: 'txn, 'txn, 's>(
    &'db self,
    txn: &'txn mut WTxn<'db>,
    id: u128,
    neighbors: &BinaryHeap<'arena, HVector<'arena>>,
    level: usize,
) -> Result<(), VectorError>
```

**RocksDB Implementation Steps:**
1. Get current neighbors: `self.get_neighbors(txn, ...)?`
2. Build set of current neighbor IDs
3. Build set of new neighbor IDs from `neighbors` param
4. **Add new edges:**
   - For each new neighbor not in current:
     - Add outgoing edge: `txn.txn.put_cf(&self.edges_db, out_key, &[])?`
     - Add incoming edge: `txn.txn.put_cf(&self.edges_db, in_key, &[])?`
5. **Remove old edges:**
   - For each current neighbor not in new:
     - Delete outgoing: `txn.txn.delete_cf(&self.edges_db, out_key)?`
     - Delete incoming: `txn.txn.delete_cf(&self.edges_db, in_key)?`

**Key Details:**
- Bidirectional edges: Both `id→neighbor` and `neighbor→id` must be maintained
- Edge keys use `out_edges_key(source, level, Some(sink))`
- Empty value `&[]` (Unit type serialized)

---

## Phase 3: Supporting Methods (MEDIUM PRIORITY)

### 3.1 Method: `num_inserted_vectors()`

**Purpose:** Return count of vectors in storage

**Current LMDB Implementation:** vector_core.rs line 387

**Signature:**
```rust
pub fn num_inserted_vectors(&self, txn: &RTxn) -> Result<usize, VectorError>
```

**RocksDB Implementation Options:**

**Option A: Iteration (Accurate but slower)**
```rust
let mut count = 0;
let iter = txn.txn.iterator_cf(&self.vector_properties_db, rocksdb::IteratorMode::Start);
for _ in iter {
    count += 1;
}
Ok(count)
```

**Option B: Cached counter (Fast but requires maintenance)**
- Store counter as special key in metadata
- Increment on insert, decrement on delete
- Trade-off: Faster reads, more complex writes

**Recommendation:** Start with Option A for correctness, optimize later if needed.

---

### 3.2 Helper Functions

These functions are **identical** between LMDB and RocksDB versions:

#### `vector_key(id: u128, level: usize) -> Vec<u8>`
```rust
pub fn vector_key(id: u128, level: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(VECTOR_PREFIX.len() + 16 + 16);
    key.extend_from_slice(VECTOR_PREFIX); // b"v:"
    key.extend_from_slice(&id.to_be_bytes());
    key.extend_from_slice(&level.to_be_bytes());
    key
}
```

#### `out_edges_key(source_id: u128, level: usize, sink_id: Option<u128>) -> Vec<u8>`
```rust
pub fn out_edges_key(source_id: u128, level: usize, sink_id: Option<u128>) -> Vec<u8> {
    let capacity = 16 + 16 + if sink_id.is_some() { 16 } else { 0 };
    let mut key = Vec::with_capacity(capacity);
    key.extend_from_slice(&source_id.to_be_bytes());
    key.extend_from_slice(&level.to_be_bytes());
    if let Some(sink) = sink_id {
        key.extend_from_slice(&sink.to_be_bytes());
    }
    key
}
```

---

## Implementation Details & Patterns

### Transaction Access Pattern

**Type Definitions** (from traversal_core/mod.rs):
```rust
#[cfg(feature = "rocks")]
pub type WTxn<'db> = rocksdb::Transaction<'db, rocksdb::TransactionDB>;
pub type RTxn<'db> = rocksdb::Transaction<'db, rocksdb::TransactionDB>;
```

**Access Pattern:**
- LMDB: `db.get(txn, key)` - direct access
- RocksDB: `txn.txn.get_cf(&cf_handle, key)` - nested `.txn` field

**Nested Structure:**
The RocksDB transaction wrapper has an inner `.txn` field that is the actual rocksdb transaction:
```rust
txn.txn.get_cf(...)     // txn is WTxn/RTxn, .txn is rocksdb::Transaction
```

### Key Encoding

**LMDB:**
```rust
Database<U128<BE>, Bytes>  // Typed key
nodes_db.get(txn, &id)     // id is u128
```

**RocksDB:**
```rust
Arc<BoundColumnFamily<'db>>  // Byte-based
txn.get_cf(&cf, &id.to_be_bytes())  // Manual conversion
```

**Always use big-endian for consistent ordering:**
- `id.to_be_bytes()` for u128 → [u8; 16]
- `level.to_be_bytes()` for usize → [u8; 16]

### Serialization

**Format Compatibility:**
- Keep identical serialization between LMDB and RocksDB
- Bincode for structured data (VectorWithoutData, properties)
- Bytemuck for f64 arrays (zero-copy)

**Example:**
```rust
// Serialize
let data = bincode::serialize(&vector)?;

// Deserialize
let vector = VectorWithoutData::from_bincode_bytes(data, arena)?;
```

### Arena Allocation

**Preserve all lifetime parameters:**
```rust
pub fn get_full_vector<'arena>(
    &self,
    txn: &RTxn,
    id: u128,
    arena: &'arena bumpalo::Bump,
) -> Result<HVector<'arena>, VectorError>
```

**Why:**
- `bumpalo::Bump` provides fast arena allocation
- Strings and vectors are borrowed from arena ('arena lifetime)
- Avoids heap allocations during hot path (HNSW search)

**Usage:**
```rust
let data_slice = arena.alloc_slice_copy(raw_bytes);
let label = arena.alloc_str(label_str);
```

### Error Handling

**Keep same error types:**
```rust
pub enum VectorError {
    VectorNotFound(String),
    SerializationError(String),
    DatabaseError(String),
    // ... etc
}
```

**Conversion from RocksDB errors:**
```rust
impl From<rocksdb::Error> for VectorError {
    fn from(err: rocksdb::Error) -> Self {
        VectorError::DatabaseError(err.to_string())
    }
}
```

---

## Reference Implementations

### BM25 RocksDB Implementation
**File:** `helix-db/src/helix_engine/bm25/rocks_bm25.rs`

**Key patterns to follow:**
- Constructor gets column family handles (lines 28-52)
- Transaction access via `txn.get_cf()` / `txn.put_cf()`
- Prefix iteration for range queries
- Bincode serialization for structured data

### Storage Core RocksDB Implementation
**File:** `helix-db/src/helix_engine/storage_core/mod.rs` (lines 549-1062)

**Key patterns:**
- Struct definition with Arc<BoundColumnFamily> (line 549)
- get_node() implementation (line 745) - shows get_pinned_cf usage
- Edge iteration patterns (lines 867-931)
- Key construction with to_be_bytes()

### LMDB VectorCore (Reference for Logic)
**File:** `helix-db/src/helix_engine/vector_core/vector_core.rs`

**Don't change the logic, only the database access:**
- HNSW algorithm logic stays identical
- Distance calculations unchanged
- Neighbor selection unchanged
- Only database read/write operations change

---

## Testing Strategy

### Unit Tests

Create `rocks_vector_core_tests.rs` with:

1. **Basic CRUD:**
   - Test `put_vector()` then `get_full_vector()`
   - Test `get_vector_properties()` returns correct metadata
   - Test deleted vectors return error

2. **Entry Point:**
   - Test `set_entry_point()` then `get_entry_point()`
   - Test None case when no entry point set

3. **Neighbors:**
   - Test `set_neighbours()` creates bidirectional edges
   - Test `get_neighbors()` returns correct set
   - Test updating neighbors (add/remove edges)

4. **Prefix Iteration:**
   - Test multiple neighbors at same level
   - Test neighbors at different levels don't interfere
   - Test empty neighbor set

### Integration Tests

1. **from_v operation:**
   - Create graph with nodes and edges
   - Add vectors to nodes
   - Execute `g.V().outE().fromV()` traversal
   - Verify correct vectors returned

2. **HNSW Search:**
   - Insert multiple vectors
   - Perform nearest neighbor search
   - Verify graph structure maintained

### Compilation Tests

Verify feature flags work:
```bash
# LMDB version
cargo build --features lmdb

# RocksDB version
cargo build --features rocks
```

---

## Migration Checklist

### Pre-Implementation
- [ ] Review BM25 RocksDB implementation (rocks_bm25.rs)
- [ ] Review storage_core RocksDB patterns (mod.rs lines 549-1062)
- [ ] Understand HNSW algorithm (no changes needed to logic)
- [ ] Set up RocksDB test environment

### Phase 1: Core Storage
- [ ] Create `rocks_vector_core.rs` file
- [ ] Implement `VectorCore<'db>` struct
- [ ] Implement `new()` constructor
- [ ] Implement `get_vector_properties()`
- [ ] Implement `get_full_vector()`
- [ ] Implement `put_vector()`
- [ ] Implement `get_raw_vector_data()`
- [ ] Write unit tests for Phase 1
- [ ] Verify from_v operation works

### Phase 2: HNSW Graph
- [ ] Implement `get_entry_point()` / `set_entry_point()`
- [ ] Implement `get_neighbors()` with prefix iteration
- [ ] Implement `set_neighbours()` with bidirectional edges
- [ ] Write unit tests for Phase 2
- [ ] Test HNSW graph construction

### Phase 3: Supporting
- [ ] Implement `num_inserted_vectors()`
- [ ] Implement any missing helper methods
- [ ] Add comprehensive error handling

### Refactoring
- [ ] Move current vector_core.rs to lmdb_vector_core.rs
- [ ] Update mod.rs with feature flags
- [ ] Verify both features compile independently
- [ ] Run full test suite with both backends

### Documentation
- [ ] Add rustdoc comments to new methods
- [ ] Update README if needed
- [ ] Document any RocksDB-specific gotchas

---

## Potential Challenges & Solutions

### Challenge 1: Prefix Iteration Key Parsing

**Issue:** RocksDB returns raw bytes, need to extract IDs from keys

**Solution:**
```rust
// Key format: [source_id(16)][level(16)][sink_id(16)]
let prefix_len = 32; // source + level
for item in iterator {
    let (key, _value) = item?;
    if key.len() >= prefix_len + 16 {
        let sink_bytes: [u8; 16] = key[prefix_len..prefix_len+16].try_into()?;
        let sink_id = u128::from_be_bytes(sink_bytes);
        // Process sink_id...
    }
}
```

### Challenge 2: Transaction Lifetime Management

**Issue:** Complex lifetime relationships ('db: 'arena: 'txn)

**Solution:**
- Keep exact same signatures as LMDB version
- Let Rust's borrow checker guide you
- Reference storage_core RocksDB implementation for patterns

### Challenge 3: Bidirectional Edge Management

**Issue:** Must maintain both directions consistently

**Solution:**
```rust
// Always do both operations together
fn add_bidirectional_edge(txn, id, neighbor_id, level) {
    let out_key = out_edges_key(id, level, Some(neighbor_id));
    let in_key = out_edges_key(neighbor_id, level, Some(id));
    txn.put_cf(&edges_db, &out_key, &[])?;
    txn.put_cf(&edges_db, &in_key, &[])?;
}
```

### Challenge 4: Empty Value Encoding

**Issue:** LMDB Unit type vs RocksDB empty bytes

**Solution:**
- Use `&[]` (empty slice) as value for edges
- RocksDB allows empty values (just stores the key)
- No need to serialize Unit type

---

## Performance Considerations

### Optimizations
1. **Use get_pinned_cf():** Avoids copying for large values
2. **Batch operations:** Group multiple puts in same transaction
3. **Arena allocation:** Already optimized, keep using bumpalo::Bump
4. **Key caching:** Reuse vector_key() / out_edges_key() results when possible

### Benchmarks to Track
- Vector insertion throughput (puts/sec)
- Vector retrieval latency (get_full_vector time)
- Neighbor traversal speed (get_neighbors iteration)
- HNSW search latency (end-to-end)

### RocksDB Tuning
- **Block cache size:** Adjust for working set
- **Compression:** May help for large vector data
- **Write buffer size:** Tune for batch insert workloads

---

## Success Criteria

### Functional Requirements
- [ ] from_v operation returns correct vectors with RocksDB backend
- [ ] Vector CRUD operations work correctly
- [ ] HNSW graph structure maintained accurately
- [ ] Bidirectional edges consistent
- [ ] Feature flags allow LMDB/RocksDB switching

### Non-Functional Requirements
- [ ] No data corruption or inconsistencies
- [ ] Performance within 2x of LMDB version (acceptable trade-off)
- [ ] All tests pass with both backends
- [ ] Code follows existing patterns and style
- [ ] No unsafe code unless absolutely necessary

---

## Timeline Estimate

- **Phase 1 (Core Storage):** 2-3 days
  - Critical path, enables from_v
  - Most important to get right

- **Phase 2 (HNSW Graph):** 2-3 days
  - Prefix iteration can be tricky
  - Bidirectional edge management needs care

- **Phase 3 (Supporting):** 1 day
  - Straightforward implementations

- **Testing & Debugging:** 2-3 days
  - Integration tests
  - Edge case handling

- **Refactoring & Documentation:** 1 day

**Total:** 8-10 days

---

## Conclusion

This plan provides a phased approach to implementing RocksDB support for VectorCore. By following the existing BM25 and storage_core patterns, the implementation should be straightforward. The critical path is Phase 1, which enables from_v operations. Once that works, the HNSW graph storage can be completed to enable full vector search functionality.

The key is to keep the HNSW algorithm logic identical and only change the database access layer. This ensures correctness while maintaining the feature-flag based compilation model.
