# HelixDB Contributors Guide

## Overview
HelixDB is a high-performance graph-vector database built in Rust, optimized for RAG and AI applications. It combines graph traversals, vector similarity search, and full-text search in a single database.

## Project Structure

### Core Components

#### `/helixdb/` - Main Database Library
The heart of HelixDB containing all database functionality.

- **`helix_engine/`** - Database engine implementation
  - `bm25/` - Full-text search using BM25 algorithm
  - `graph_core/` - Graph database operations (nodes, edges, traversals)
  - `storage_core/` - LMDB-based storage backend via heed3
  - `vector_core/` - Vector storage and HNSW similarity search

- **`helix_gateway/`** - Network layer
  - `connection/` - TCP connection handling
  - `router/` - Request routing to handlers
  - `thread_pool/` - Concurrent request processing
  - `mcp/` - Model Context Protocol support

- **`helixc/`** - Query compiler
  - `parser/` - Pest-based parser for `.hx` files
  - `analyzer/` - Type checking and validation
  - `generator/` - Query plan generation

- **`ingestion_engine/`** - External data ingestion
  - PostgreSQL support (with pgvector)
  - SQLite support

- **`protocol/`** - Wire protocol and data types

#### `/helix-container/` - Runtime Container
The server process that hosts compiled queries and handles requests.

**Files:**
- `main.rs` - Initializes graph engine and HTTP gateway
- `queries.rs` - Generated code placeholder (populated during build)

**Architecture:**
- Loads compiled queries via inventory crate route discovery
- Creates HelixGraphEngine with LMDB storage backend
- Starts HelixGateway on configured port (default: 6969)
- Routes HTTP requests to registered handlers

**Environment Variables:**
- `HELIX_DATA_DIR` - Database storage location
- `HELIX_PORT` - Server port

#### `/helix-cli/` - Command-Line Interface
User-facing CLI for managing HelixDB instances.

**Files:**
- `main.rs` - Command implementations
- `args.rs` - CLI argument definitions (clap)
- `instance_manager.rs` - Instance lifecycle management
- `types.rs` - Error types and version handling
- `utils.rs` - File handling, port management, templates

**Commands:**
- `helix install` - Clone and setup HelixDB repository
- `helix init` - Create new project with template files
- `helix check` - Validate schema and query syntax
- `helix deploy` - Compile queries and start new instance
- `helix redeploy` - Update existing instance (local/remote)
- `helix instances` - List all running instances
- `helix start/stop` - Control instance lifecycle
- `helix delete` - Remove instance and data
- `helix save` - Export instance data

**Deploy Flow:**
1. Read `.hx` files (schema.hx, queries.hx)
2. Parse and analyze using helixc
3. Generate Rust code with handler functions
4. Write to container/src/queries.rs
5. Build release binary with optimizations
6. Start instance with unique ID and port

### Supporting Libraries

#### `/debug_trace/` - Debug Macro
Procedural macro for function tracing (`#[debug_trace]`).

#### `/get_routes/` - Route Registration
Procedural macro for HTTP handler registration (`#[handler]`).

#### `/hbuild_redploy/` - Deployment Service
Hot-swapping service for production deployments via AWS S3.

## Key Concepts

### Query Language
HelixDB uses a custom query language defined in `.hx` files:
```
QUERY addUser(name: String, age: I64) =>
   user <- AddN<User({name: name, age: age})
   RETURN user
```

### Data Model
- **Nodes** (N::) - Graph vertices with properties
- **Edges** (E::) - Relationships between nodes
- **Vectors** (V::) - High-dimensional embeddings

### Operations
- **Graph traversals**: `In`, `Out`, `InE`, `OutE`
- **Vector search**: HNSW-based similarity search
- **Text search**: BM25 full-text search
- **CRUD**: `AddN`, `AddE`, `Update`, `Drop`

## Architecture Flow

1. **Definition**: Write queries in `.hx` files
2. **Compilation**: `helix check` parses and validates
3. **Deployment**: `helix deploy` loads into container
4. **Execution**: Gateway routes requests to compiled handlers
5. **Storage**: LMDB handles persistence with ACID guarantees

## Development Guidelines

### Code Style
- Prefer functional patterns (pattern matching, iterators, closures)
- Document code inline - no separate docs needed
- Minimize dependencies
- Use asserts liberally in production code

### Testing
- Write benchmarks before optimizing
- DST (Deterministic Simulation Testing) coming soon

### Performance
- Currently 1000x faster than Neo4j for graph operations
- On par with Qdrant for vector search
- LMDB provides memory-mapped performance

## Getting Started

1. Install CLI: `curl -sSL "https://install.helix-db.com" | bash`
2. Install Helix: `helix install`
3. Initialize project: `helix init --path <path>`
4. Write queries in `.hx` files
5. Deploy: `helix deploy`

## License
AGPL (Affero General Public License)

For commercial support: founders@helix-db.com
message.txt
5 KB
