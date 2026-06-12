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

The Cargo workspace members are `helix-cli`, `metrics`, and `sdks/rust`:
- **CLI**: `cargo build -p helix-cli`
- **Metrics**: `cargo build -p helix-metrics`
- **Rust DSL SDK**: `cargo build -p helix-db` (the SDK crate in `sdks/rust`; docs at [docs.rs/helix-db](https://docs.rs/helix-db))

The TypeScript SDK (`sdks/typescript`) and Go SDK (`sdks/go`) build with their own toolchains (`npm`, `go`).

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

3. Start a local instance (Docker/Podman container):
   ```bash
   helix start dev
   ```

4. Send a query:
   ```bash
   helix query dev --file examples/request.json
   ```

## Project Structure

This repository contains the user-facing tooling for HelixDB: the CLI, the client SDKs, and metrics. The database engine itself runs inside the `enterprise-dev` container image that the CLI pulls and manages — it is not built from this repo. The root holds `helix-cli/`, `sdks/`, `metrics/`, and `assets/`.

### Core Components

#### `/helix-cli/` - Command-Line Interface
User-facing CLI for managing HelixDB instances and deployments.

**Directory Structure:**
```
helix-cli/
├── src/
│   ├── commands/           # CLI command implementations (one module per subcommand)
│   │   ├── logs/          # Local + Enterprise Cloud log viewing
│   │   ├── add.rs         # Add a local or Enterprise Cloud instance
│   │   ├── auth.rs        # Enterprise Cloud auth (login/logout/create-key)
│   │   ├── chef.rs        # Bootstrap a first Helix app for a coding agent
│   │   ├── config.rs      # workspace/project/cluster config (hidden parent)
│   │   ├── dashboard.rs   # Launch the Helix Dashboard
│   │   ├── delete.rs      # Delete an instance and its local state
│   │   ├── enterprise_deploy.rs # Enterprise Cloud deploy helpers
│   │   ├── feedback.rs    # Send feedback to the Helix team
│   │   ├── init.rs        # Initialize a v2 project
│   │   ├── metrics.rs     # Metrics configuration
│   │   ├── prune.rs       # Prune local containers/workspaces
│   │   ├── push.rs        # Deploy an Enterprise Cloud instance
│   │   ├── query.rs       # Send a dynamic query to POST /v1/query
│   │   ├── restart.rs     # Restart a background local instance
│   │   ├── start.rs       # Start a local instance (alias: run)
│   │   ├── status.rs      # Instance status
│   │   ├── stop.rs        # Stop a background local instance
│   │   ├── sync.rs        # Sync Enterprise Cloud metadata into helix.toml
│   │   └── update.rs      # Self-update the CLI
│   ├── config.rs          # helix.toml + ~/.helix config management
│   ├── enterprise_cloud.rs # Enterprise Cloud REST types and fetchers
│   ├── errors.rs          # Error handling
│   ├── lib.rs             # Library interface + subcommand enums
│   ├── local_runtime.rs   # Docker/Podman container lifecycle
│   ├── main.rs            # Entry point + clap command definitions
│   ├── metrics_sender.rs  # Metrics collection
│   ├── output.rs          # Terminal output / verbosity helpers
│   ├── port.rs            # Port availability helpers
│   ├── project.rs         # Project context + helix.toml discovery
│   ├── prompts.rs         # Interactive cliclack prompts
│   ├── setup.rs           # Shared init/chef setup helpers
│   ├── sse_client.rs      # Server-sent-event client (auth/deploy/logs)
│   ├── ts_query.rs        # TypeScript DSL query evaluation
│   ├── update.rs          # Self-update functionality
│   └── utils.rs           # Utilities
```

**Available Commands:**
- `helix init` - Initialize a v2 Helix project
- `helix chef` (alias `cook`) - Bootstrap a first Helix app for a coding agent
- `helix add` - Add a local or Enterprise Cloud instance to a project
- `helix start` (alias `run`) - Start a local instance in the background
- `helix stop` - Stop a background local instance
- `helix restart` - Restart a background local instance
- `helix status` - Show local and Enterprise Cloud instance status
- `helix logs` - View logs for a local or Enterprise Cloud instance
- `helix query` - Send a dynamic query to `POST /v1/query`
- `helix push` - Deploy an Enterprise Cloud instance
- `helix auth` - Enterprise Cloud authentication (login/logout/create-key)
- `helix workspace` - Manage the active Enterprise Cloud workspace
- `helix project` - Manage the linked Enterprise Cloud project
- `helix cluster` - List and inspect Enterprise Cloud clusters
- `helix sync` - Sync Enterprise Cloud metadata into `helix.toml`
- `helix prune` - Prune local containers/workspaces
- `helix delete` - Delete an instance from `helix.toml` and local state
- `helix metrics` - Configure metrics collection (full/basic/off/status)
- `helix dashboard` - Launch the Helix Dashboard (start/stop/status)
- `helix update` - Update the CLI to the latest version
- `helix feedback` - Send feedback to the Helix team

**Deployment Targets:**
- Local Docker/Podman containers (`helix start`) — image `ghcr.io/helixdb/enterprise-dev`
- Helix Cloud (managed Enterprise hosting) via `helix push`

**Build & Deploy Flow:**

The v3 CLI is a runtime orchestrator — there is no `helix compile`/`helix check` step and no `.hx` query files.

1. Scaffold a project with `helix init` (writes `helix.toml` and a `.helix/` workspace).
2. Start a local instance with `helix start` — a Docker/Podman container running the `enterprise-dev` image (in-memory by default, on-disk with `--disk`).
3. Author queries with the Rust, TypeScript, Go, or Python DSL; they serialize to JSON "dynamic queries".
4. Send queries to a running instance via `POST /v1/query` (`helix query`); validation happens server-side.
5. For production, deploy an Enterprise Cloud instance with `helix push`, managing auth/metadata via `helix auth`, `helix sync`, and the `workspace`/`project`/`cluster` commands.

### Supporting Components

#### `/sdks/` - Client SDKs
Client libraries that build HelixDB queries and send them to a running instance.
- `rust/` - Rust DSL builder (crate `helix-db`), with the `helix-dsl-macros` procedural-macro crate
- `typescript/` - TypeScript DSL (`@helix-db/helix-db`)
- `go/` - Go client and DSL
- `python/` - Python client and DSL (`helix-db`, imported as `helixdb`)
- `tests/` - Cross-SDK parity tests and metadata registration tests

#### `/metrics/` - Metrics
The `helix-metrics` crate used by the CLI for telemetry collection.

#### `/assets/` - Brand Assets
Logos and images used in the README and docs.

## Key Concepts

### Query Language
Queries are authored with the Rust, TypeScript, Go, or Python DSL (in `sdks/`) and serialized to JSON "dynamic queries" sent to a running instance. The legacy HelixQL `.hx` form below is still supported for reference and translation:
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

1. **Definition**: Author queries with a Rust, TypeScript, Go, or Python DSL
2. **Serialization**: The DSL produces a JSON dynamic-query AST (`POST /v1/query` body)
3. **Execution**: Send to a running instance with `helix query`; the gateway validates and runs it server-side
4. **Storage**: LMDB handles persistence with ACID guarantees

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

The `clippy_check.sh` script at the repository root runs `cargo clippy --workspace -- -D warnings`, treating all warnings as errors across every workspace crate.

### Testing

Tests live alongside the code in each crate and SDK:

#### Test Structure

**CLI Tests** (`helix-cli`)
- Inline `#[cfg(test)]` modules throughout `helix-cli/src/`
- clap argument-parsing tests in `src/main.rs` (every command/flag combo)
- Config (de)serialization and backward-compat defaults in `src/config.rs`
- `chef` prompt rendering, agent-priority, and stream-json parsing in `src/commands/chef.rs`

**SDK Tests** (`sdks/`)
- `sdks/rust/` - Rust DSL unit tests (`cargo test -p helix-db`)
- `sdks/typescript/` - TypeScript DSL tests (run with `npm test`)
- `sdks/go/` - Go DSL tests (`go test ./...`)
- `sdks/tests/` - Cross-SDK parity tests and metadata registration tests

#### Running Tests

```bash
# Run all Rust workspace tests
cargo test --workspace

# Run specific crate tests
cargo test -p helix-cli
cargo test -p helix-db      # Rust SDK in sdks/rust

# TypeScript SDK
cd sdks/typescript && npm test

# Go SDK
cd sdks/go && go test ./...
```

Format and lint before opening a PR: `cargo fmt` and `./clippy_check.sh`.

#### Testing Guidelines
- Write tests for all new features
- Include both positive and negative test cases
- Ensure tests pass locally before opening PR

### Performance
- Currently 1000x faster than Neo4j for graph operations
- On par with Qdrant for vector search
- LMDB provides memory-mapped performance

## Communication Channels

### Getting Help
- **Discord**: Join our [Discord community](https://discord.gg/2stgMPr5BD) for real-time discussions, questions, and support
- **GitHub Issues**: Report bugs or request features at [github.com/HelixDB/helix-db/issues](https://github.com/HelixDB/helix-db/issues)
- **Documentation**: Check [docs.helix-db.com](https://docs.helix-db.com) for comprehensive guides
- **Twitter/X**: Follow [@helixdb](https://x.com/helixdb) for updates and announcements

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
2. Initialize project: `helix init --path <path>`
3. Start a local instance: `helix start dev`
4. Author queries with the Rust or TypeScript DSL (see `sdks/`)
5. Send a query: `helix query dev --file examples/request.json`

## License
AGPL (Affero General Public License)

For commercial support: founders@helix-db.com
