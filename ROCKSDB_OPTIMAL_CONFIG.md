# Optimal RocksDB Configuration for HelixDB

This document provides a production-ready RocksDB configuration optimized for HelixDB's workload characteristics:
- High write throughput (overcoming LMDB's single-writer bottleneck)
- Fast read performance (graph traversals and vector searches)
- Large dataset support (100GB+)
- Concurrent read/write operations

## Table of Contents

1. [Core RocksDB Configuration](#1-core-rocksdb-configuration)
2. [Column Family Specific Configurations](#2-column-family-specific-configurations)
3. [TransactionDB Configuration](#3-transactiondb-configuration)
4. [Performance Tuning Guide](#4-performance-tuning-guide)
5. [Trade-off Analysis](#5-trade-off-analysis)
6. [Monitoring and Statistics](#6-monitoring-and-statistics)
7. [Integration Example](#7-integration-example)
8. [Benchmarking Recommendations](#8-benchmarking-recommendations)

---

## 1. Core RocksDB Configuration

```rust
use rocksdb::{
    Options, DBCompressionType, Cache, BlockBasedOptions,
    TransactionDB, TransactionDBOptions, WriteOptions, ReadOptions,
    SliceTransform, ColumnFamilyDescriptor
};

/// Create optimized RocksDB options for HelixDB workload
pub fn create_base_options(db_size_gb: u64) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // ============ MEMORY CONFIGURATION ============
    // Critical for balancing reads and writes

    // Block cache: 50% of budget for read performance
    // This caches decompressed data blocks - essential for graph traversals
    let total_memory_bytes = db_size_gb * 1024 * 1024 * 1024;
    let block_cache_size = total_memory_bytes / 2;
    let block_cache = Cache::new_lru_cache(block_cache_size);
    opts.set_block_cache(&block_cache);

    // Write buffers: Balance write throughput vs memory
    // Multiple buffers enable concurrent writes without blocking
    opts.set_write_buffer_size(256 * 1024 * 1024); // 256 MB per memtable
    opts.set_max_write_buffer_number(4);           // 4 memtables = 1GB total
    opts.set_min_write_buffer_number_to_merge(2);  // Merge 2 before flushing

    // Total memory: 50% cache + ~25% write buffers + 25% OS/overhead

    // ============ WRITE THROUGHPUT OPTIMIZATION ============

    // Background jobs for concurrent operations
    // Higher values = better write throughput (more parallel compaction/flush)
    opts.set_max_background_jobs(8);      // Total background threads
    opts.set_max_background_flushes(4);   // Concurrent memtable flushes
    opts.set_max_background_compactions(4); // Concurrent compactions
    opts.set_max_subcompactions(4);       // Parallel subcompaction threads

    // Allow more Level 0 files before slowing writes
    // Higher = better write throughput, but slower reads if too high
    opts.set_level_zero_file_num_compaction_trigger(4);
    opts.set_level_zero_slowdown_writes_trigger(20);
    opts.set_level_zero_stop_writes_trigger(36);

    // ============ COMPACTION STRATEGY ============

    // Universal compaction for write-heavy workloads
    // Better write amplification than level compaction
    // Trade-off: Slightly more read amplification, but worth it for your needs
    opts.set_compaction_style(rocksdb::DBCompactionStyle::Universal);

    // Universal compaction tuning
    let mut universal_opts = rocksdb::UniversalCompactOptions::default();
    universal_opts.set_size_ratio(1);           // Aggressive compaction
    universal_opts.set_min_merge_width(2);      // Merge at least 2 files
    universal_opts.set_max_merge_width(5);      // Merge up to 5 files
    universal_opts.set_compression_size_percent(80); // Compress older data
    opts.set_universal_compaction_options(&universal_opts);

    // Enable dynamic leveling for better space efficiency
    opts.set_level_compaction_dynamic_level_bytes(true);

    // ============ READ PERFORMANCE OPTIMIZATION ============

    // Keep many files open for faster access
    opts.set_max_open_files(2000);

    // Disable direct I/O to leverage OS page cache
    // This works well with your read patterns
    opts.set_use_direct_reads(false);
    opts.set_use_direct_io_for_flush_and_compaction(false);

    // Hint that access pattern is random (graph traversals)
    opts.set_advise_random_on_open(true);

    // ============ COMPRESSION STRATEGY ============

    // LZ4 for hot data (fast compression/decompression)
    opts.set_compression_type(DBCompressionType::Lz4);

    // Zstd for cold data (better ratio, worth the CPU on infrequent reads)
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);
    opts.set_bottommost_compression_options(&rocksdb::BottommostLevelCompaction::Force);

    // Per-level compression (increasingly aggressive)
    opts.set_compression_per_level(&[
        DBCompressionType::None,  // L0: No compression (about to be compacted)
        DBCompressionType::None,  // L1: No compression (hot data)
        DBCompressionType::Lz4,   // L2: Fast compression
        DBCompressionType::Lz4,   // L3: Fast compression
        DBCompressionType::Lz4,   // L4: Fast compression
        DBCompressionType::Zstd,  // L5: Strong compression
        DBCompressionType::Zstd,  // L6: Strong compression (cold data)
    ]);

    // ============ DURABILITY & WAL ============

    // Keep WAL for durability, but tune for performance
    opts.set_manual_wal_flush(false);
    opts.set_max_total_wal_size(1024 * 1024 * 1024); // 1 GB max WAL
    opts.set_wal_size_limit_mb(0); // No size limit per file
    opts.set_wal_ttl_seconds(0);   // No time-based deletion

    // Recycle WAL files for better performance
    opts.set_recycle_log_file_num(4);

    // ============ STATISTICS & MONITORING ============

    opts.enable_statistics();
    opts.set_stats_dump_period_sec(300); // Log stats every 5 minutes
    opts.set_stats_persist_period_sec(600); // Persist stats every 10 minutes

    opts
}
```

---

## 2. Column Family Specific Configurations

### Adjacency List Configuration

These column families (`out_edges_db`, `in_edges_db`) are **CRITICAL** for graph traversal performance.

```rust
/// Configuration for adjacency list column families (out_edges_db, in_edges_db)
/// These are CRITICAL for graph traversal performance
pub fn adjacency_list_cf_options(base_cache: &Cache) -> Options {
    let mut opts = Options::default();

    // ============ PREFIX OPTIMIZATION ============
    // Keys are: node_id(16 bytes) + label_hash(4 bytes) = 20 bytes
    // Enable prefix bloom filters for fast prefix scans
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(20));

    // Enable prefix bloom in memtable (helps with recent writes)
    opts.set_memtable_prefix_bloom_ratio(0.2);
    opts.set_memtable_whole_key_filtering(false); // Only prefix matters

    // ============ BLOOM FILTERS ============
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(base_cache); // Share cache

    // Whole key bloom filter (for point lookups)
    block_opts.set_bloom_filter(10.0, false); // 10 bits per key

    // Partition filters for better memory efficiency
    block_opts.set_partition_filters(true);
    block_opts.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);

    // Pin index/filter blocks in cache (critical for hot data)
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_pin_top_level_index_and_filter(true);

    // Optimize for sequential scans (prefix iteration)
    block_opts.set_block_size(32 * 1024); // 32 KB blocks

    opts.set_block_based_table_factory(&block_opts);

    // ============ COMPRESSION ============
    // Fixed-size values (32 bytes: edge_id + node_id)
    // IDs are random, don't compress well, waste CPU
    opts.set_compression_type(DBCompressionType::None);
    opts.set_bottommost_compression_type(DBCompressionType::Lz4); // Try on old data

    // ============ WRITE OPTIMIZATION ============
    // Slightly smaller write buffers (these CFs get many small writes)
    opts.set_write_buffer_size(128 * 1024 * 1024); // 128 MB
    opts.set_max_write_buffer_number(4);

    // ============ COMPACTION ============
    // These CFs have many range deletes (when dropping nodes)
    // Enable DeleteRange optimization
    opts.set_optimize_filters_for_hits(true);

    opts
}
```

### Entity Storage Configuration

For `nodes_db` and `edges_db` column families.

```rust
/// Configuration for node and edge storage CFs
pub fn entity_storage_cf_options(base_cache: &Cache) -> Options {
    let mut opts = Options::default();

    // ============ POINT LOOKUP OPTIMIZATION ============
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(base_cache); // Share cache

    // Smaller blocks for point lookups (better cache utilization)
    block_opts.set_block_size(16 * 1024); // 16 KB blocks

    // Strong bloom filters (existence checks are common)
    block_opts.set_bloom_filter(10.0, false); // 10 bits per key

    // Ribbon filters (newer, more memory-efficient than bloom)
    // 30% less memory for same false positive rate
    block_opts.set_ribbon_filter(10.0);

    // Pin critical data in cache
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

    opts.set_block_based_table_factory(&block_opts);

    // ============ COMPRESSION ============
    // Node/edge data is compressible (strings, properties)
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);

    // ============ WRITE BUFFERS ============
    opts.set_write_buffer_size(256 * 1024 * 1024); // 256 MB
    opts.set_max_write_buffer_number(4);

    opts
}
```

### Vector Storage Configuration

For `vectors_db` column family storing large embeddings (1-6 KB).

```rust
/// Configuration for vector storage CF (large embeddings)
pub fn vector_storage_cf_options(base_cache: &Cache) -> Options {
    let mut opts = Options::default();

    // ============ BLOB FILE CONFIGURATION ============
    // Store large values (embeddings: 1-6 KB) separately from LSM
    // Reduces write amplification and improves scan performance

    opts.set_enable_blob_files(true);
    opts.set_min_blob_size(1024); // 1 KB threshold

    // Blob file settings
    opts.set_blob_file_size(512 * 1024 * 1024); // 512 MB per blob file
    opts.set_blob_compression_type(DBCompressionType::Zstd); // Good ratio on vectors

    // Blob garbage collection
    opts.set_enable_blob_gc(true);
    opts.set_blob_gc_age_cutoff(0.25); // GC when 25% garbage
    opts.set_blob_gc_force_threshold(0.5); // Force GC at 50% garbage

    // ============ LSM CONFIGURATION ============
    // LSM tree only stores keys and blob references (small)
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(base_cache);
    block_opts.set_block_size(32 * 1024); // 32 KB
    block_opts.set_bloom_filter(10.0, false);

    opts.set_block_based_table_factory(&block_opts);

    // ============ COMPRESSION ============
    // Vectors compress moderately well
    opts.set_compression_type(DBCompressionType::Zstd);

    // ============ WRITE BUFFERS ============
    // Larger buffers (large values)
    opts.set_write_buffer_size(512 * 1024 * 1024); // 512 MB
    opts.set_max_write_buffer_number(3);

    opts
}
```

### Metadata Configuration

For `metadata_db` and other small, infrequently accessed column families.

```rust
/// Configuration for metadata and small CFs
pub fn metadata_cf_options(base_cache: &Cache) -> Options {
    let mut opts = Options::default();

    // Lightweight configuration (small, infrequent access)
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_cache(base_cache);
    block_opts.set_block_size(4 * 1024); // 4 KB blocks

    opts.set_block_based_table_factory(&block_opts);

    // Minimal write buffers
    opts.set_write_buffer_size(16 * 1024 * 1024); // 16 MB
    opts.set_max_write_buffer_number(2);

    // No compression (small data)
    opts.set_compression_type(DBCompressionType::None);

    opts
}
```

---

## 3. TransactionDB Configuration

```rust
use rocksdb::{TransactionDB, TransactionDBOptions, TransactionOptions, WriteOptions};

/// Create TransactionDB with optimized settings
pub fn create_transaction_db(path: &str, db_size_gb: u64) -> Result<TransactionDB> {
    let base_opts = create_base_options(db_size_gb);
    let base_cache = Cache::new_lru_cache((db_size_gb * 1024 * 1024 * 1024) / 2);

    // ============ TRANSACTION DB OPTIONS ============
    let mut txn_db_opts = TransactionDBOptions::default();

    // Lock management for concurrent writes
    // Higher values = better write concurrency
    txn_db_opts.set_max_num_locks(100_000);      // 100K locks
    txn_db_opts.set_max_num_stripes(256);         // Lock striping (power of 2)
    txn_db_opts.set_transaction_lock_timeout(1000); // 1 second timeout
    txn_db_opts.set_default_lock_timeout(1000);    // 1 second default

    // ============ COLUMN FAMILIES ============
    let cfs = vec![
        ColumnFamilyDescriptor::new("nodes_db", entity_storage_cf_options(&base_cache)),
        ColumnFamilyDescriptor::new("edges_db", entity_storage_cf_options(&base_cache)),
        ColumnFamilyDescriptor::new("out_edges_db", adjacency_list_cf_options(&base_cache)),
        ColumnFamilyDescriptor::new("in_edges_db", adjacency_list_cf_options(&base_cache)),
        ColumnFamilyDescriptor::new("metadata_db", metadata_cf_options(&base_cache)),
        ColumnFamilyDescriptor::new("vectors_db", vector_storage_cf_options(&base_cache)),
        // Add more CFs as needed: hnsw_edges_db, vector_properties_db, etc.
    ];

    let db = TransactionDB::open_cf_descriptors(&base_opts, &txn_db_opts, path, cfs)?;
    Ok(db)
}

/// Default write options for transactions
pub fn transaction_write_options() -> WriteOptions {
    let mut opts = WriteOptions::default();

    // Async writes for better throughput
    // WAL still protects against crashes
    opts.set_sync(false);

    // Don't disable WAL (need durability)
    opts.disable_wal(false);

    // Don't ignore missing column families (fail fast)
    opts.set_ignore_missing_column_families(false);

    opts
}

/// Transaction options for MVCC snapshot isolation
pub fn create_transaction_options() -> TransactionOptions {
    let mut opts = TransactionOptions::default();

    // Enable snapshot isolation
    opts.set_set_snapshot(true);

    // Deadlock detection
    opts.set_deadlock_detect(true);
    opts.set_deadlock_detect_depth(50);

    // Lock timeout
    opts.set_lock_timeout(1000); // 1 second

    opts
}
```

---

## 4. Performance Tuning Guide

### Memory Allocation Strategy

```
Total Memory Budget (e.g., 10 GB):
├─ 50% Block Cache (5 GB)       - Read performance
├─ 25% Write Buffers (~2.5 GB)  - Write throughput
│   └─ 4 memtables × 256 MB each per major CF
├─ 10% RocksDB overhead (1 GB)  - Internal structures
└─ 15% OS page cache (1.5 GB)   - File system cache
```

### Write Throughput Optimization

For **VERY write-heavy** workloads:

```rust
// Adjust these settings:
opts.set_max_background_jobs(12);            // More parallel work
opts.set_write_buffer_size(512 * 1024 * 1024); // Larger memtables
opts.set_level_zero_slowdown_writes_trigger(30); // Tolerate more L0 files

// Trade-off: Higher memory usage, potentially slower reads if L0 gets too large
```

### Read Performance Optimization

For **read-heavy** workloads (if writes are acceptable):

```rust
opts.set_block_cache(&Cache::new_lru_cache(8 * 1024 * 1024 * 1024)); // 8 GB cache
opts.set_write_buffer_size(128 * 1024 * 1024); // Smaller memtables
opts.set_compaction_style(rocksdb::DBCompactionStyle::Level); // Better read amp

// Trade-off: Lower write throughput, more compaction overhead
```

### Large Dataset Optimization (100GB+)

```rust
// Enable tiered storage (future: hot SSD, cold HDD)
opts.set_compaction_readahead_size(2 * 1024 * 1024); // 2 MB readahead

// More aggressive compaction for space efficiency
opts.set_max_bytes_for_level_base(512 * 1024 * 1024); // 512 MB
opts.set_max_bytes_for_level_multiplier(8.0); // 8x multiplier

// Enable file deletion via archive (safer for large files)
opts.set_delete_obsolete_files_period_micros(6 * 60 * 60 * 1000000); // 6 hours
```

---

## 5. Trade-off Analysis

### RocksDB vs LMDB Comparison

| Aspect | RocksDB (with this config) | LMDB (current) | Winner |
|--------|---------------------------|----------------|---------|
| **Write Throughput** | ✅ High (concurrent writes, async) | ❌ Single writer bottleneck | **RocksDB** |
| **Point Lookup Latency** | ~1-2 μs (with cache) | ~0.5-1 μs | LMDB (slight) |
| **Range Scan (prefix_iter)** | ~5-10 μs + scan time | ~2-5 μs + scan time | LMDB (slight) |
| **Memory Efficiency** | Moderate (overhead from LSM) | High (B+ tree) | LMDB |
| **Large Dataset (>RAM)** | ✅ Excellent (tiered storage) | ⚠️ Slower on large DBs | **RocksDB** |
| **Concurrent Reads** | ✅ Excellent (MVCC snapshots) | ✅ Excellent (MVCC) | Tie |
| **Space Amplification** | ~1.3-1.5x (with compression) | ~1.1x | LMDB |
| **Write Amplification** | 10-20x (universal compaction) | ~2x (B+ tree updates) | LMDB |
| **Operational Complexity** | Higher (tuning required) | Lower (simple config) | LMDB |

### Expected Performance Characteristics

With this configuration on typical hardware (16 GB RAM, NVMe SSD, 100 GB dataset):

```
Expected Metrics:
├─ Point Lookup: 0.5-2 μs (95% cache hit)
├─ Prefix Scan: 10-50 μs (depending on result size)
├─ Write Throughput: 50K-200K ops/sec (concurrent)
├─ Read Throughput: 100K-500K ops/sec (concurrent)
├─ Graph Traversal: 1-3 ms per 2-hop path
└─ Vector Search: 3-10 ms (10K vectors, 3-way)

Compaction Impact:
├─ Background CPU: 20-40% sustained
├─ Periodic I/O spikes: 100-500 MB/s
└─ User-facing latency: Minimal (async)
```

### Write Amplification Deep Dive

```
Universal Compaction (your config):
  Physical Writes / Logical Writes = 10-15x

Why it's acceptable:
1. Modern SSDs handle write endurance well (PB lifespan)
2. Write throughput is HIGH despite amplification
3. Read performance doesn't suffer
4. Better than Level compaction for write-heavy (20-30x)

If write amplification becomes an issue:
- Consider Leveled compaction (better read/write balance)
- Enable TTL-based compaction for time-series data
- Use blob files more aggressively (reduces amp for large values)
```

### Compression Trade-offs

```rust
// Current config: Hybrid (Lz4 + Zstd)
Compression Ratio: 2-3x (depending on data)
CPU Impact: Low (Lz4 is fast, ~500 MB/s compression)
Read Latency: +10-20% (decompression overhead)

// Alternative: No compression
opts.set_compression_type(DBCompressionType::None);
Benefit: -10-20% read latency
Cost: 2-3x more storage space
When: If storage is cheap and latency is critical

// Alternative: All Zstd
opts.set_compression_type(DBCompressionType::Zstd);
Benefit: 3-5x compression ratio
Cost: +50-100% CPU usage, +20-40% read latency
When: If storage is expensive and CPU is cheap
```

---

## 6. Monitoring and Statistics

### Performance Statistics

```rust
use rocksdb::{DB, statistics::Histogram};

/// Print performance statistics
pub fn print_rocksdb_stats(db: &TransactionDB) {
    if let Some(stats) = db.property_value("rocksdb.stats") {
        println!("RocksDB Statistics:\n{}", stats);
    }

    // Key metrics to monitor:
    // 1. Block cache hit rate (should be >95%)
    if let Some(hit_rate) = db.property_value("rocksdb.block-cache-hit-rate") {
        println!("Block Cache Hit Rate: {}", hit_rate);
    }

    // 2. Write stall time (should be near 0)
    if let Some(stall) = db.property_value("rocksdb.write-stall-micros") {
        println!("Write Stall Time: {} μs", stall);
    }

    // 3. Compaction pending (should be stable)
    if let Some(pending) = db.property_value("rocksdb.compaction-pending") {
        println!("Compaction Pending: {}", pending);
    }

    // 4. Memory usage
    if let Some(mem) = db.property_value("rocksdb.estimate-table-readers-mem") {
        println!("Table Readers Memory: {} bytes", mem);
    }
}

/// Health check - detect performance issues
pub fn check_rocksdb_health(db: &TransactionDB) -> Vec<String> {
    let mut warnings = Vec::new();

    // Check L0 files (too many = read slowdown)
    if let Some(l0_files) = db.property_value("rocksdb.num-files-at-level0") {
        if let Ok(count) = l0_files.parse::<i32>() {
            if count > 10 {
                warnings.push(format!("High L0 file count: {} (target: <10)", count));
            }
        }
    }

    // Check write stalls
    if let Some(stall_pct) = db.property_value("rocksdb.actual-delayed-write-rate") {
        if stall_pct != "0" {
            warnings.push(format!("Write stalls detected: {}", stall_pct));
        }
    }

    // Check memtable sizes
    if let Some(mem_size) = db.property_value("rocksdb.size-all-mem-tables") {
        if let Ok(size) = mem_size.parse::<usize>() {
            if size > 4 * 1024 * 1024 * 1024 { // 4 GB
                warnings.push(format!("Large memtable size: {} GB", size / (1024*1024*1024)));
            }
        }
    }

    warnings
}
```

### Critical Metrics to Monitor

| Metric | Target | Action if Out of Range |
|--------|--------|------------------------|
| **Block Cache Hit Rate** | >95% | Increase block cache size |
| **Write Stall %** | <1% | Increase background jobs or L0 trigger thresholds |
| **L0 File Count** | <10 files | Decrease compaction trigger or increase background compactions |
| **Compaction CPU** | 20-40% | If higher: reduce compaction threads or tune settings |
| **Space Amplification** | 1.3-1.5x | If higher: compaction not keeping up, check settings |

---

## 7. Integration Example

### Basic Integration

```rust
// In storage_core/mod.rs

impl HelixStorage {
    pub fn open_rocks(path: &str, config: &StorageConfig) -> Result<Self> {
        let db_size_gb = config.db_max_size_gb.unwrap_or(10);

        // Create TransactionDB with optimized config
        let db = create_transaction_db(path, db_size_gb)?;

        // Warm up cache (optional: preload hot keys)
        // Self::warmup_cache(&db)?;

        // Start background health monitoring
        // std::thread::spawn(move || {
        //     loop {
        //         std::thread::sleep(Duration::from_secs(60));
        //         let warnings = check_rocksdb_health(&db);
        //         for warning in warnings {
        //             eprintln!("RocksDB Warning: {}", warning);
        //         }
        //     }
        // });

        Ok(Self::Rocks(db))
    }
}
```

---

## 8. Benchmarking Recommendations

Before deploying to production, benchmark these scenarios:

### Critical Benchmarks

1. **Concurrent Write Throughput**
   ```rust
   // Spawn 8 threads, each writing 10K nodes
   // Target: >50K writes/sec aggregate
   ```

2. **Read Latency Under Write Load**
   ```rust
   // Write in background, measure read p99 latency
   // Target: p99 < 5ms for get_node()
   ```

3. **Graph Traversal Performance**
   ```rust
   // 2-hop traversal with 10 edges per node
   // Target: <5ms per traversal
   ```

4. **Prefix Scan Performance**
   ```rust
   // Iterate 100 edges via prefix_iter
   // Target: <1ms per scan
   ```

5. **Large Dataset Behavior**
   ```rust
   // Load 100GB dataset, measure cache hit rate
   // Target: >90% cache hits after warmup
   ```

6. **Recovery Time**
   ```rust
   // Crash simulation, measure restart time
   // Target: <30 seconds for 100GB DB
   ```

---

## Quick Start Checklist

### Setup Steps

- [x] **Add dependency to Cargo.toml** (already done)
  ```toml
  rocksdb = { version = "0.24.0", features = ["multi-threaded-cf"] }
  ```

- [ ] **Copy configuration functions to codebase**
  - `create_base_options()`
  - `adjacency_list_cf_options()`
  - `entity_storage_cf_options()`
  - `vector_storage_cf_options()`
  - `create_transaction_db()`

- [ ] **Integrate into storage_core/mod.rs**
  - Use `create_transaction_db()` to open database
  - Apply appropriate CF options to each column family

- [ ] **Run benchmarks**
  ```bash
  cargo bench --bench hnsw_benches
  cargo test --release integration_stress_tests
  ```

- [ ] **Monitor in production**
  - Track block cache hit rate
  - Watch write stall percentage
  - Monitor L0 file count

---

## When to Adjust Configuration

### If writes are slower than expected:

- Increase `max_background_jobs` to 12
- Increase `level_zero_slowdown_writes_trigger` to 30
- Consider switching to leveled compaction if universal is bottleneck

### If reads are slower than LMDB:

- Increase block cache to 60-70% of memory
- Reduce write buffer size to free up memory
- Enable more aggressive bloom filters (12-15 bits per key)

### If storage space is too high:

- Switch to all-Zstd compression
- Enable more aggressive compaction
- Reduce blob file threshold

### If compaction uses too much CPU:

- Reduce `max_background_compactions` to 2
- Increase `level_zero_file_num_compaction_trigger` to 6
- Consider lighter compression (Lz4 everywhere)

---

## Summary

This configuration represents a **balanced starting point** optimized for HelixDB's mixed workload:
- High write throughput via universal compaction and concurrent writes
- Fast reads via large block cache and prefix bloom filters
- Large dataset support via blob files and tiered compression

**Expected Outcome:**
- **3-5x better write throughput** than LMDB
- **Within 2x of LMDB's read latency** (still sub-millisecond for cached reads)
- **Excellent scaling** for datasets larger than RAM

The key advantage is that RocksDB excels at **concurrent writes** and **large datasets**, which addresses LMDB's single-writer bottleneck while maintaining competitive read performance.

---

## Additional Resources

- [RocksDB Tuning Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
- [RocksDB FAQ](https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ)
- [Universal vs Level Compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction)
- [Bloom Filter Performance](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter)
