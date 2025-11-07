# Contributing to HelixDB

## Overview
HelixDB is a high-performance graph-vector database built in Rust, optimized for RAG and AI applications. It combines graph traversals, vector similarity search, and full-text search in a single database.

We welcome contributions from the community! This guide will help you get started with contributing to HelixDB.

## How to Contribute

### Reporting Issues
- Check existing [GitHub Issues](https://github.com/HelixDB/helix-db/issues) to avoid duplicates
- Use a clear, descriptive title
- Include steps to reproduce for bugs
- Provide system information (OS, Rust version, HelixDB version)
- Add relevant logs or error messages

### Contribution Workflow
1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/helix-db.git
   cd helix-db
   ```
3. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
4. **Make your changes** following our coding guidelines
5. **Commit your changes** with clear, descriptive commit messages:
   ```bash
   git commit -m "feat: add new feature description"
   ```
6. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```
7. **Open a Pull Request** against the `main` branch
8. **Respond to feedback** from reviewers

### Pull Request Guidelines
- Link related issues in the PR description
- Ensure all tests pass
- Add tests for new features
- Update documentation if needed
- Keep PRs focused on a single feature or fix
- Write clear commit messages following conventional commits format

## Prerequisites and Development Setup

### Required Tools
- **Rust**: 1.75.0 or later (install via [rustup](https://rustup.rs/))
- **Cargo**: Comes with Rust
- **Git**: For version control

### Optional Tools
- **cargo-watch**: For development auto-reloading
- **cargo-nextest**: Faster test runner
- **rust-analyzer**: IDE support

### Building the Project
1. **Clone the repository**:
   ```bash
   git clone https://github.com/HelixDB/helix-db.git
   cd helix-db
   ```

2. **Build all components**:
   ```bash
   cargo build
   ```

3. **Build in release mode** (optimized):
   ```bash
   cargo build --release
   ```

### Building Specific Components
- **CLI only**: `cargo build -p helix-cli`
- **Core database**: `cargo build -p helix-db`
- **Container**: `cargo build -p helix-container`

### Running HelixDB Locally
1. Install the CLI (development version):
   ```bash
   cargo install --path helix-cli
   ```

2. Initialize a test project:
   ```bash
   mkdir test-project && cd test-project
   helix init
   ```

3. Deploy locally:
   ```bash
   helix push dev
   ```

## Project Structure

### Core Components

#### `/helix-db/` - Main Database Library
The heart of HelixDB containing all database functionality.

- **`helix_engine/`** - Database engine implementation
  - `bm25/` - Full-text search using BM25 algorithm
  - `storage_core/` - LMDB-based storage backend via heed3
  - `traversal_core/` - Graph traversal operations and query execution
  - `vector_core/` - Vector storage and HNSW similarity search
  - `tests/` - Integration and unit tests
  - `types.rs` - Core type definitions
  - `macros.rs` - Helper macros

- **`helix_gateway/`** - Network layer
  - `builtin/` - Built-in query handlers (node_by_id, all_nodes_and_edges, node_connections, nodes_by_label)
  - `embedding_providers/` - Integration with embedding services
  - `router/` - Request routing to handlers
  - `worker_pool/` - Concurrent request processing (formerly thread_pool)
  - `mcp/` - Model Context Protocol support
  - `gateway.rs` - Main gateway implementation
  - `introspect_schema.rs` - Schema introspection utilities

- **`helixc/`** - Query compiler
  - `parser/` - Parser for `.hx` files (using Pest grammar)
  - `analyzer/` - Type checking, validation, and diagnostics
  - `generator/` - Rust code generation from parsed queries

- **`grammar.pest`** - 295-line Pest grammar defining HQL syntax

- **`protocol/`** - Wire protocol and data types

- **`utils/`** - Shared utilities across the codebase

#### `/helix-container/` - Runtime Container
The server process that hosts compiled queries and handles requests.

**Files:**
- `main.rs` - Initializes graph engine and HTTP gateway
- `queries.rs` - Generated code placeholder (populated during build)
- `docker-compose.yml` - Container orchestration configuration
- `Dockerfile` - Development container image

**Architecture:**
- Loads compiled queries via inventory crate route discovery
- Creates HelixGraphEngine with LMDB storage backend
- Starts HelixGateway on configured port (default: 6969)
- Routes HTTP requests to registered handlers

**Environment Variables:**
- `HELIX_DATA_DIR` - Database storage location
- `HELIX_PORT` - Server port

#### `/helix-cli/` - Command-Line Interface
User-facing CLI for managing HelixDB instances and deployments.

**Directory Structure:**
```
helix-cli/
├── src/
│   ├── commands/           # CLI command implementations
│   │   ├── integrations/   # Cloud deployment integrations
│   │   │   ├── docker_hub.rs
│   │   │   ├── ecr.rs      # AWS ECR
│   │   │   ├── fly.rs      # Fly.io
│   │   │   ├── ghcr.rs     # GitHub Container Registry
│   │   │   └── helix.rs    # Helix Cloud
│   │   ├── add.rs         # Add dependencies
│   │   ├── auth.rs        # Authentication (login/logout/create-key)
│   │   ├── build.rs       # Build queries
│   │   ├── check.rs       # Validate schema and queries
│   │   ├── compile.rs     # Compile queries
│   │   ├── delete.rs      # Delete instances
│   │   ├── init.rs        # Initialize new projects
│   │   ├── metrics.rs     # Metrics configuration
│   │   ├── migrate.rs     # Database migrations
│   │   ├── prune.rs       # Cleanup unused resources
│   │   ├── pull.rs        # Pull from cloud deployments
│   │   ├── push.rs        # Push to cloud deployments
│   │   ├── start.rs       # Start instances
│   │   ├── status.rs      # Instance status
│   │   ├── stop.rs        # Stop instances
│   │   └── update.rs      # Update CLI
│   ├── tests/             # CLI tests
│   ├── config.rs          # Configuration management
│   ├── docker.rs          # Docker integration
│   ├── errors.rs          # Error handling
│   ├── lib.rs             # Library interface
│   ├── main.rs            # Entry point
│   ├── metrics_sender.rs  # Metrics collection
│   ├── project.rs         # Project management
│   ├── update.rs          # Self-update functionality
│   └── utils.rs           # Utilities
```

**Available Commands:**
- `helix add` - Add dependencies to project
- `helix auth` - Authentication management (login/logout/create-key)
- `helix build` - Build queries without deploying
- `helix check` - Validate schema and query syntax
- `helix compile` - Compile queries to Rust code
- `helix delete` - Remove instance and data
- `helix init` - Create new project with template files
- `helix metrics` - Configure metrics collection (full/basic/off/status)
- `helix migrate` - Run database migrations
- `helix prune` - Clean up unused resources
- `helix pull` - Pull deployment from cloud
- `helix push` - Push deployment to cloud (dev/staging/prod)
- `helix start` - Start stopped instances
- `helix status` - Show instance status
- `helix stop` - Stop running instances
- `helix update` - Update CLI to latest version

**Deployment Integrations:**
- Helix Cloud (managed hosting)
- AWS ECR (Elastic Container Registry)
- Fly.io
- Docker Hub
- GitHub Container Registry (GHCR)
- Local deployment

**Build & Deploy Flow:**
1. Read `.hx` files (schema.hx, queries.hx)
2. Parse and analyze using helixc
3. Generate Rust code with handler functions
4. Write to container/src/queries.rs
5. Build release binary with optimizations
6. Push to target deployment (cloud or local)

### Supporting Components

#### `/helix-macros/` - Procedural Macros
Procedural macros for HelixDB including route registration and code generation utilities.

#### `/hql-tests/` - HQL Test Suite
Test files for the Helix Query Language (HQL).

#### `/metrics/` - Performance Metrics
Performance benchmarking and metrics collection.

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

### Linting

Run Clippy to check code quality:
```bash
./clippy_check.sh
```

The `clippy_check.sh` script at the repository root runs clippy with project-specific rules:
- Treats warnings as errors
- Excludes `hql-tests` crate
- Can run in dashboard mode with additional features

### Testing

HelixDB has a comprehensive test suite organized across multiple levels:

#### Test Structure

**Unit Tests** (within `src/` directories)
- `/helix-db/src/helix_engine/tests/` - Engine unit tests
- `/helix-db/src/helix_gateway/tests/` - Gateway unit tests
- Inline `#[cfg(test)]` modules throughout the codebase

**Integration Tests**
- `/helix-db/tests/` - Database integration tests

**CLI Tests**
- `/helix-cli/src/tests/` - Command-line interface tests
  - `check_tests.rs` - Validation testing
  - `compile_tests.rs` - Compilation testing
  - `init_tests.rs` - Project initialization
  - `project_tests.rs` - Project management

**HQL End-to-End Tests**
- `/hql-tests/tests/` - 54+ test directories covering:
  - Graph operations (add_n, add_e, traversals)
  - Vector search (search_v_with_embed)
  - Text search (search_bm25)
  - Aggregations and counting
  - Migrations
  - Cloud queries
  - Rerankers
  - Knowledge graphs
  - Benchmarks

**Benchmark Tests**
- `/helix-db/benches/bm25_benches.rs` - Full-text search performance
- `/helix-db/benches/hnsw_benches.rs` - Vector search performance

#### Running Tests

```bash
# Run all tests
cargo test --workspace

# Run specific crate tests
cargo test -p helix-db
cargo test -p helix-cli

# Run HQL tests
cd hql-tests
./test.sh

# Run benchmarks
cargo test --benches
```

#### Testing Guidelines
- Write tests for all new features
- Include both positive and negative test cases
- Add benchmarks before optimizing performance-critical code
- Ensure tests pass locally before opening PR
- DST (Deterministic Simulation Testing) coming soon

### Performance
- Currently 1000x faster than Neo4j for graph operations
- On par with Qdrant for vector search
- LMDB provides memory-mapped performance

## Communication Channels

### Getting Help
- **Discord**: Join our [Discord community](https://discord.gg/2stgMPr5BD) for real-time discussions, questions, and support
- **GitHub Issues**: Report bugs or request features at [github.com/HelixDB/helix-db/issues](https://github.com/HelixDB/helix-db/issues)
- **Documentation**: Check [docs.helix-db.com](https://docs.helix-db.com) for comprehensive guides
- **Twitter/X**: Follow [@hlx_db](https://x.com/hlx_db) for updates and announcements

### Before You Ask
- Search existing GitHub issues and Discord for similar questions
- Check the documentation for relevant guides
- Try to create a minimal reproducible example
- Include error messages, logs, and system information

### Community Guidelines
- Be respectful and constructive
- Help others when you can
- Share your use cases and learnings
- Follow our [Code of Conduct](CODE_OF_CONDUCT.md)

## Code Review Process

### What Reviewers Look For
- **Correctness**: Does the code work as intended?
- **Tests**: Are there adequate tests? Do they pass?
- **Code style**: Does it follow Rust and HelixDB conventions?
- **Performance**: Are there obvious performance issues?
- **Documentation**: Are complex parts explained?
- **Scope**: Is the PR focused on a single feature/fix?

### Common Reasons PRs Get Rejected
- Failing tests or CI checks
- No tests for new functionality
- Breaks existing functionality
- Code style violations
- Too broad in scope (mixing multiple unrelated changes)
- Missing documentation for complex features
- Performance regressions without justification

### How to Respond to Feedback
- Address all reviewer comments
- Ask for clarification if feedback is unclear
- Make requested changes in new commits (don't force push during review)
- Mark conversations as resolved after addressing them
- Be patient and respectful - reviewers are volunteers

### Review Timeline
- Initial response: Usually within 2-3 days
- Follow-up reviews: 1-2 days after updates
- Complex PRs may take longer
- Feel free to ping on Discord if your PR hasn't been reviewed after a week

## Getting Started

1. Install CLI: `curl -sSL "https://install.helix-db.com" | bash`
2. Install Helix: `helix install`
3. Initialize project: `helix init --path <path>`
4. Write queries in `.hx` files
5. Deploy: `helix deploy`

## License
AGPL (Affero General Public License)

For commercial support: founders@helix-db.com