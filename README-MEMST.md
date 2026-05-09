# MemSt Integration

`helix_db::memst` brings the core of the [memst](https://github.com/yfyang86/memst) crate
into HelixDB: a Git-like memory architecture for LLM session management.

It adds two capabilities that complement HelixDB's graph + vector engine:

1. **Tiered memory management** — `Working` / `ShortTerm` / `LongTerm` tiers with
   automatic promotion, demotion, token-budget tracking, and lossless compaction
   checkpoints.
2. **File and session management** — an on-disk session layout for chat
   messages, operation logs, and per-tier memory persistence, with a global
   manifest index and an exclusive process lock.

A content-addressable object store (BLAKE3-hashed `Blob` / `Tree` / `Commit` /
`Tag`) is also included as the persistent backbone for snapshots and history.

> **Status.** This is a focused port of `memst-core`. Knowledge-graph, search,
> vector, hybrid retrieval, and LLM-extraction modules from upstream `memst`
> are intentionally **not** included — HelixDB already provides graph and
> vector primitives, so they would conflict.

---

## Why this is here

HelixDB stores graph and vector data extremely well, but a typical AI agent
also needs:

- A **conversation log** that grows append-only and can be read back in order.
- An **operation log** of tool calls, web searches, retrievals, etc.
- A pool of **distilled memories** at multiple lifetimes (hot / warm / cold).
- A **budget** so the working set never explodes past the model's context.
- A **commit history** so memories can be rewound when consolidation goes wrong.

`memst` provides exactly that, with a small, dependency-light surface area.
Merging it into HelixDB lets a downstream agent persist its full session
state alongside the graph/vector data it queries — no second database.

---

## User story

> *As an AI-agent backend, I want to remember the right things at the right
> level of fidelity, with bounded context cost, and never lose history when
> compacting.*

A concrete walkthrough:

1. **Alice**, a Rust developer, starts a session. The agent appends each
   conversation turn to `messages.bin` and indexes it in `messages.idx`.
2. The agent extracts a semantic memory ("user prefers Tokio") and stores it
   in **working** memory with a token estimate.
3. Each time Alice asks something Tokio-related, the agent calls
   `access_memory`, bumping the access count.
4. Once the access count crosses the configured threshold, the lifecycle's
   `should_promote` returns `TransitionTrigger::AccessCount`, and the agent
   moves the memory to **long-term**.
5. As context fills up, `is_budget_exceeded(MemoryTier::Working)` flips to
   `true`. The agent calls `select_for_compaction`, which returns the
   lowest-importance / oldest items first.
6. Before rewriting them, the agent opens a `CompactionCheckpoint` capturing
   the pre-commit OID and original token count. Compaction summarises the
   chosen memories into a single short-term entry; the checkpoint is
   `finalize`d with the post-commit OID. If anything looks wrong later,
   `restore_checkpoint` recovers the exact pre-state.
7. Meanwhile **Bob** and **Carol** are running in their own sessions — the
   per-session `memory/` directory keeps everyone isolated, and the manifest
   tracks all of them centrally.
8. Tomorrow Alice reopens her session: `SessionStore::open` loads the
   manifest, the messages stream is replayed, and her long-term memories are
   right where she left them.

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    helix_db::memst                         │
├────────────────────────────────────────────────────────────┤
│  store.rs        SessionStore                              │
│                  ├── manifest.json     (global index)      │
│                  ├── store.lock        (fs2 exclusive)     │
│                  ├── sessions/<uuid>/                      │
│                  │   ├── metadata.json                     │
│                  │   ├── messages.bin  (append-only)       │
│                  │   ├── messages.idx  (text index)        │
│                  │   ├── operations.log (JSONL)            │
│                  │   ├── memory/working.bin                │
│                  │   ├── memory/short.bin                  │
│                  │   └── memory/long.bin                   │
│                  └── attachments/                          │
│                                                            │
│  memory.rs       MemoryLifecycle                           │
│                  ├── HashMap<id, MemoryState>              │
│                  ├── Vec<Transition>     (history)         │
│                  ├── Vec<CompactionCheckpoint>             │
│                  └── LifecycleConfig (budgets, TTLs)       │
│                                                            │
│  objects.rs      ObjectStore (content-addressable)         │
│                  ├── Blob/Tree/Commit/Tag                  │
│                  ├── BLAKE3-hashed paths                   │
│                  └── automatic deduplication               │
│                                                            │
│  types.rs        Message · MemoryItem · Manifest · ...     │
│  error.rs        Error / Result                            │
└────────────────────────────────────────────────────────────┘
```

---

## Quick start

Add nothing — the module is part of the `helix-db` crate. From application
code:

```rust
use helix_db::memst::{
    MemoryItem, MemoryQuery, MemoryTier, MemoryType, Message,
    SessionMetadata, SessionStore,
};

let dir = tempfile::TempDir::new()?;
let store = SessionStore::init(dir.path())?;

let session_id = store.create_session(SessionMetadata::new("alice", "gpt-4"))?;

store.append_message(session_id, Message::user("Hello!"))?;
store.append_message(session_id, Message::assistant("Hi there."))?;

let mem = MemoryItem::new("user prefers Tokio", "extraction")
    .with_memory_type(MemoryType::Semantic)
    .with_tag("rust")
    .with_token_estimate(7);
store.add_memory(session_id, MemoryTier::Working, mem)?;

let hits = store.retrieve_memories(
    session_id,
    MemoryQuery::new().with_keyword("tokio").with_tag("rust"),
)?;
assert_eq!(hits.len(), 1);
```

### Tiered lifecycle

```rust
use helix_db::memst::{LifecycleConfig, MemoryLifecycle, MemoryTier, TransitionTrigger};

let mut lifecycle = MemoryLifecycle::new(LifecycleConfig::default());
lifecycle.register(&mem.id.to_string(), MemoryTier::Working);

// Decide if a memory should move tier:
if let Some(trigger) = lifecycle.should_promote(&mem) {
    match trigger {
        TransitionTrigger::AccessCount(n)        => println!("hot: {n} accesses"),
        TransitionTrigger::ImportanceThreshold(i) => println!("important: {i:.2}"),
        _ => {}
    }
}

// Detect budget pressure:
if lifecycle.is_budget_exceeded(&memories, MemoryTier::Working) {
    let to_compact = lifecycle.select_for_compaction(&memories, MemoryTier::Working, 10);
    // ... summarise, write a Commit, move to ShortTerm ...
}
```

### Compaction checkpoint

```rust
let pre = ObjectId::from_content(b"working tree before compaction");
let cp = lifecycle.create_checkpoint(
    Some(MemoryScope::Session(session_id.to_string())),
    MemoryTier::Working,
    MemoryTier::ShortTerm,
    memory_ids,
    pre,
    /* original_tokens = */ 1_000,
);

// ... do the compaction work ...

let post = ObjectId::from_content(b"compacted state");
lifecycle.finalize_checkpoint(&cp.id, post, /* compacted_tokens = */ 250)?;

// Lossless rewind, anytime later:
let restored = lifecycle.restore_checkpoint(&cp.id);
```

---

## Public API surface

Re-exported from `helix_db::memst`:

| Category | Types |
|---|---|
| Errors | `Error`, `Result` |
| Messages | `Message`, `Role`, `Content`, `ContentPart` |
| Sessions | `SessionStore`, `SessionMetadata`, `SessionSummary`, `Manifest`, `MessageIndexEntry`, `SessionId` |
| Memories | `MemoryItem`, `MemoryTier`, `MemoryType`, `MemoryQuery`, `MemoryId` |
| Operations | `Operation`, `OperationType`, `OperationQuery` |
| Lifecycle | `MemoryLifecycle`, `LifecycleConfig`, `MemoryState`, `Transition`, `TransitionTrigger`, `CompactionCheckpoint` |
| Token counters | `TokenCounter` (trait), `SimpleTokenCounter`, `ConfiguredTokenCounter` |
| Object store | `ObjectStore`, `ObjectId`, `Blob`, `Tree`, `TreeEntry`, `Commit`, `CommitMetadata`, `CommitSource`, `Author`, `Tag`, `MemoryScope` |

Submodules (`error`, `types`, `memory`, `objects`, `store`) remain `pub` so
internal helpers like `ObjectHeader` are reachable when needed.

---

## On-disk layout

```
<base>/
├── manifest.json          // global session index
├── schema_version         // "1.0.0"
├── store.lock             // fs2 exclusive lock
├── attachments/           // out-of-band binary blobs
└── sessions/
    └── <session-uuid>/
        ├── metadata.json   // SessionMetadata
        ├── messages.bin    // bincode-packed Message stream
        ├── messages.idx    // text index: id offset length ts role
        ├── operations.log  // JSONL Operation entries
        ├── memory/
        │   ├── working.bin
        │   ├── short.bin
        │   └── long.bin
        └── extractions/    // reserved for downstream extractors
```

The lock file ensures only one writer per store directory. Schema version
bumps cause `SessionStore::open` to error out with `Error::VersionMismatch`.

---

## Testing

The integration ships with three layers of coverage:

| Layer | Location | Count |
|---|---|---|
| Module-level unit tests (happy path) | inline `#[cfg(test)] mod tests` in each `memst/*.rs` | ~15 |
| Fine-grained unit tests (edge cases, builders, serde, errors) | `helix-db/tests/memst_unit.rs` | ~40 |
| User Acceptance Tests (BDD-style end-to-end scenarios) | `helix-db/tests/memst_uat.rs`, `helix-db/tests/memst_uat_multisession.rs` | ~27 |

Run all of them:

```bash
cargo test -p helix-db --tests
```

Run only the memst test files:

```bash
cargo test -p helix-db --test memst_unit
cargo test -p helix-db --test memst_uat
cargo test -p helix-db --test memst_uat_multisession
```

There's also a runnable demo that simulates three concurrent sessions:

```bash
cargo run -p helix-db --example memst_multi_session
```

---

## Limitations and non-goals

- **No knowledge-graph types.** Upstream `memst-core::types` exposes
  `Entity`/`Relationship` plus a KG-evolution machinery; HelixDB has its own
  graph engine, so those are not ported.
- **No search/vector backends.** `memst-core::search`, `vector`, and `hybrid`
  are dropped; use HelixDB's built-in `helix_engine` instead.
- **No LLM extraction.** `memst-core::llm` and `extract` are dropped — feed
  whatever extractor you like and call `add_memory` with the result.
- **Single-process locking.** The `store.lock` file uses `fs2`'s POSIX/Windows
  exclusive lock. It does not coordinate across machines.
- **Bincode 1.x format.** Tied to HelixDB's existing bincode dependency. The
  on-disk format will need a migration if HelixDB upgrades to bincode 2.

---

## File map

```
helix-db/
├── src/memst/
│   ├── mod.rs        re-exports
│   ├── error.rs
│   ├── types.rs      Message, MemoryItem, MemoryTier, ...
│   ├── objects.rs    ObjectId, Blob/Tree/Commit/Tag, ObjectStore
│   ├── memory.rs     MemoryLifecycle, checkpoints, token counters
│   └── store.rs      SessionStore (file/session management)
├── examples/
│   └── memst_multi_session.rs   runnable demo
└── tests/
    ├── memst_unit.rs
    ├── memst_uat.rs
    └── memst_uat_multisession.rs
```
