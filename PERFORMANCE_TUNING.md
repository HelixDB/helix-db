# Helix DB Performance Tuning Guide

## Environment Variables for Performance Optimization

The following environment variables have been added to allow fine-tuning of Helix DB performance:

### Thread Pool Configuration

- **`HELIX_WORKER_THREADS`**: Number of worker threads for processing database operations
  - Default: Number of CPU cores
  - Example: `HELIX_WORKER_THREADS=16`

- **`HELIX_IO_THREADS`**: Number of IO threads for handling network requests
  - Default: Number of CPU cores / 4 (minimum 2)
  - Example: `HELIX_IO_THREADS=4`

### Request Handling

- **`HELIX_MAX_REQUEST_SIZE`**: Maximum allowed request body size in bytes
  - Default: 104857600 (100MB)
  - Example: `HELIX_MAX_REQUEST_SIZE=1073741824` (1GB)

- **`HELIX_REQUEST_TIMEOUT`**: Request timeout in seconds
  - Default: 60
  - Example: `HELIX_REQUEST_TIMEOUT=300` (5 minutes)

### Channel Configuration

- **`HELIX_CHANNEL_CAPACITY`**: Capacity of internal message channels between threads
  - Default: 10000
  - Example: `HELIX_CHANNEL_CAPACITY=50000`

### Database Configuration

- **`HELIX_MAX_READERS`**: Maximum number of concurrent LMDB readers
  - Default: 1000
  - Example: `HELIX_MAX_READERS=2000`

### Batch Processing Configuration

- **`HELIX_BATCH_SIZE`**: Maximum number of items per batch operation
  - Default: 1000
  - Example: `HELIX_BATCH_SIZE=5000`

- **`HELIX_BATCH_AUTO_COMMIT`**: Whether to auto-commit after each batch
  - Default: true
  - Example: `HELIX_BATCH_AUTO_COMMIT=false`

- **`HELIX_BATCH_VALIDATE`**: Whether to validate items before batch insertion
  - Default: true
  - Example: `HELIX_BATCH_VALIDATE=false`

- **`HELIX_BATCH_COMMIT_SIZE`**: Number of operations before auto-committing transaction
  - Default: 10000
  - Example: `HELIX_BATCH_COMMIT_SIZE=50000`

## Quick Start Configuration Examples

### For High-Throughput Ingestion
```bash
export HELIX_WORKER_THREADS=32
export HELIX_IO_THREADS=8
export HELIX_MAX_REQUEST_SIZE=1073741824  # 1GB
export HELIX_REQUEST_TIMEOUT=300           # 5 minutes
export HELIX_CHANNEL_CAPACITY=50000
export HELIX_MAX_READERS=2000
export HELIX_BATCH_SIZE=5000
export HELIX_BATCH_AUTO_COMMIT=true
export HELIX_BATCH_VALIDATE=false          # Skip validation for speed
```

### For Low-Latency Queries
```bash
export HELIX_WORKER_THREADS=16
export HELIX_IO_THREADS=8
export HELIX_MAX_REQUEST_SIZE=10485760    # 10MB
export HELIX_REQUEST_TIMEOUT=30           # 30 seconds
export HELIX_CHANNEL_CAPACITY=5000
export HELIX_MAX_READERS=500
```

### For Resource-Constrained Environments
```bash
export HELIX_WORKER_THREADS=4
export HELIX_IO_THREADS=2
export HELIX_MAX_REQUEST_SIZE=52428800    # 50MB
export HELIX_REQUEST_TIMEOUT=60           # 60 seconds
export HELIX_CHANNEL_CAPACITY=1000
export HELIX_MAX_READERS=200
```

## Performance Improvements Implemented

1. **Request Size Limits**: Prevents memory exhaustion from oversized requests
2. **Configurable Thread Pools**: Allows scaling based on available CPU cores
3. **HTTP/2 Support**: Enabled for better connection multiplexing
4. **Compression**: Automatic gzip/brotli compression for responses
5. **TCP Optimizations**: 
   - TCP_NODELAY enabled for low latency
   - 1MB send/receive buffers
6. **Increased LMDB Readers**: Better concurrent read performance
7. **Larger Channel Capacity**: Reduced backpressure under high load
8. **Batch Insert APIs**: Optimized batch operations for nodes and edges with configurable batch sizes

## Monitoring Recommendations

Monitor these metrics to tune the configuration:
- CPU utilization per thread pool
- Channel queue depths
- Request latency percentiles (p50, p99)
- Memory usage patterns
- LMDB reader slot usage

## Next Steps

The following optimizations are planned for future releases:
- Transaction batching with configurable commit sizes
- Asynchronous index updates
- Zero-copy serialization
- Connection pooling
- Streaming response support