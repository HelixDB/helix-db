//! Runnable demo: three concurrent sessions sharing one MemSt store.
//!
//! Each user has their own conversation, extracts a few memories at the
//! `Working` tier, and the agent is configured with a small token budget so
//! we can watch the lifecycle decide which memories to compact.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p helix-db --example memst_multi_session
//! ```
//!
//! The demo writes everything to a temporary directory and prints a summary
//! at the end; nothing is left on disk after the process exits.

use helix_db::memst::{
    ConfiguredTokenCounter, LifecycleConfig, MemoryItem, MemoryLifecycle, MemoryQuery, MemoryState,
    MemoryTier, MemoryType, Message, Operation, OperationType, SessionMetadata, SessionStore,
    TokenCounter, TransitionTrigger,
};
use std::error::Error;
use tempfile::TempDir;
use uuid::Uuid;

fn main() -> Result<(), Box<dyn Error>> {
    println!("=== MemSt multi-session demo ===\n");

    // 1. Initialize a fresh store in a temporary directory.
    let dir = TempDir::new()?;
    let store = SessionStore::init(dir.path())?;
    println!("store rooted at {}", dir.path().display());

    // 2. Three users, three sessions, each on a different model.
    let alice = create_session(&store, "alice", "claude-sonnet-4")?;
    let bob = create_session(&store, "bob", "gpt-4o")?;
    let carol = create_session(&store, "carol", "llama-3-70b")?;
    println!("\ncreated 3 sessions:");
    for s in store.list_sessions()? {
        println!("  - {} ({}) -> {}", s.name, s.model, s.id);
    }

    // 3. Use a small working-memory budget so compaction triggers visibly.
    let cfg = LifecycleConfig {
        working_memory_max_tokens: 60,
        promotion_access_threshold: 2,
        ..LifecycleConfig::default()
    };
    let mut lifecycle = MemoryLifecycle::new(cfg);
    let counter = ConfiguredTokenCounter::default();

    // 4. Drive each session's conversation + memory extraction.
    drive_alice(&store, &mut lifecycle, &counter, alice)?;
    drive_bob(&store, &mut lifecycle, &counter, bob)?;
    drive_carol(&store, &mut lifecycle, &counter, carol)?;

    // 5. Show what each session has on disk.
    println!("\n--- per-session summary ---");
    for s in store.list_sessions()? {
        let meta = store.get_session(s.id)?.unwrap();
        let working = store.get_tier(s.id, MemoryTier::Working)?;
        let short = store.get_tier(s.id, MemoryTier::ShortTerm)?;
        let long = store.get_tier(s.id, MemoryTier::LongTerm)?;
        let ops = store.get_operations(s.id)?;
        println!(
            "  {} : {} msg / {} tok | mem  W={}  S={}  L={} | ops={}",
            meta.name,
            meta.message_count,
            meta.token_count,
            working.len(),
            short.len(),
            long.len(),
            ops.len()
        );
    }

    // 6. Demonstrate budget pressure and lifecycle decisions for Alice.
    let alice_working = store.get_tier(alice, MemoryTier::Working)?;
    let usage = lifecycle.calculate_token_usage(&alice_working);
    println!(
        "\nalice working tokens: {}/{}",
        usage.get(&MemoryTier::Working).copied().unwrap_or(0),
        60
    );
    if lifecycle.is_budget_exceeded(&alice_working, MemoryTier::Working) {
        println!("budget exceeded -> selecting compaction candidates");
        let picked = lifecycle.select_for_compaction(&alice_working, MemoryTier::Working, 2);
        for m in &picked {
            println!(
                "    candidate: \"{}\" (importance={:.2}, tokens={})",
                truncate(&m.content, 40),
                m.importance,
                m.token_estimate.unwrap_or(0)
            );
        }
        // Move the picked items down a tier and update the lifecycle state.
        for m in picked {
            store.promote_memory(alice, m.id, MemoryTier::ShortTerm)?;
            lifecycle
                .transition(
                    &m.id.to_string(),
                    MemoryState::ShortTerm,
                    TransitionTrigger::TokenBudgetExceeded,
                    -(m.token_estimate.unwrap_or(0) as i32),
                )
                .ok();
        }
        println!(
            "    -> alice now has {} working / {} short-term",
            store.get_tier(alice, MemoryTier::Working)?.len(),
            store.get_tier(alice, MemoryTier::ShortTerm)?.len(),
        );
    }

    // 7. Cross-session sanity: a query against alice never sees bob or carol.
    println!("\n--- isolation check ---");
    for (label, id) in [("alice", alice), ("bob", bob), ("carol", carol)] {
        let hits = store.retrieve_memories(
            id,
            MemoryQuery::new().with_keyword("python"), // bob-specific keyword
        )?;
        println!(
            "  {label}: {} memories matching 'python' (expected: only bob)",
            hits.len()
        );
    }

    // 8. Persistence check: drop the store and re-open from disk.
    drop(store);
    let store = SessionStore::open(dir.path())?;
    let reopened = store.list_sessions()?;
    println!(
        "\nre-opened store sees {} sessions ({:?})",
        reopened.len(),
        reopened.iter().map(|s| s.name.as_str()).collect::<Vec<_>>()
    );

    println!("\n=== done ===");
    Ok(())
}

// -----------------------------------------------------------------------------
// per-user scripts
// -----------------------------------------------------------------------------

fn drive_alice<C: TokenCounter>(
    store: &SessionStore,
    lifecycle: &mut MemoryLifecycle,
    counter: &C,
    id: Uuid,
) -> Result<(), Box<dyn Error>> {
    let turns = [
        Message::system("You help with Rust async."),
        Message::user("How does Tokio's runtime schedule tasks?").with_token_count(8),
        Message::assistant("Tokio uses a multi-threaded work-stealing scheduler...").with_token_count(60),
        Message::user("Is it OK to mix tokio and async-std?").with_token_count(9),
        Message::assistant("Generally avoid mixing runtimes...").with_token_count(40),
    ];
    append_turns(store, id, turns)?;

    // Three working memories, each with a token estimate.
    for content in [
        "user prefers Tokio over async-std for async runtimes",
        "user is comfortable with work-stealing schedulers",
        "user wants idiomatic Rust async patterns",
    ] {
        let mem = MemoryItem::new(content, "extraction")
            .with_memory_type(MemoryType::Semantic)
            .with_tag("rust")
            .with_token_estimate(counter.count(content));
        lifecycle.register(&mem.id.to_string(), MemoryTier::Working);
        store.add_memory(id, MemoryTier::Working, mem)?;
    }

    // Touch the Tokio memory twice -> crosses the access-count threshold.
    let working = store.get_tier(id, MemoryTier::Working)?;
    if let Some(tokio_mem) = working.iter().find(|m| m.content.contains("Tokio")) {
        store.access_memory(id, tokio_mem.id)?;
        store.access_memory(id, tokio_mem.id)?;
        if let Some(updated) = store.get_memory(id, tokio_mem.id)? {
            if let Some(trigger) = lifecycle.should_promote(&updated) {
                println!(
                    "alice: tokio memory should promote ({:?})",
                    trigger
                );
                store.promote_memory(id, tokio_mem.id, MemoryTier::LongTerm)?;
                lifecycle
                    .transition(
                        &tokio_mem.id.to_string(),
                        MemoryState::LongTerm,
                        trigger,
                        0,
                    )
                    .ok();
            }
        }
    }

    log_op(store, id, OperationType::MemoryRetrieval, "tokio", 4)?;
    Ok(())
}

fn drive_bob<C: TokenCounter>(
    store: &SessionStore,
    lifecycle: &mut MemoryLifecycle,
    counter: &C,
    id: Uuid,
) -> Result<(), Box<dyn Error>> {
    let turns = [
        Message::system("You help with ML in Python."),
        Message::user("How do I avoid GIL contention in PyTorch?").with_token_count(11),
        Message::assistant("Use multiprocessing or move tensors to GPU...").with_token_count(35),
    ];
    append_turns(store, id, turns)?;

    let content = "user uses Python and PyTorch; cares about GIL implications";
    let mem = MemoryItem::new(content, "extraction")
        .with_memory_type(MemoryType::Semantic)
        .with_tag("python")
        .with_tag("ml")
        .with_token_estimate(counter.count(content));
    lifecycle.register(&mem.id.to_string(), MemoryTier::Working);
    store.add_memory(id, MemoryTier::Working, mem)?;

    log_op(store, id, OperationType::WebSearch, "GIL pytorch", 12)?;
    Ok(())
}

fn drive_carol<C: TokenCounter>(
    store: &SessionStore,
    lifecycle: &mut MemoryLifecycle,
    counter: &C,
    id: Uuid,
) -> Result<(), Box<dyn Error>> {
    let turns = [
        Message::system("You help with Go infrastructure."),
        Message::user("How do I tune GOMAXPROCS in Kubernetes?").with_token_count(12),
        Message::assistant("Use the automaxprocs library or set it to the cgroup CPU limit.").with_token_count(45),
    ];
    append_turns(store, id, turns)?;

    let content = "user runs Go services on Kubernetes; tuning GOMAXPROCS is recurring";
    let mem = MemoryItem::new(content, "extraction")
        .with_memory_type(MemoryType::Procedural)
        .with_tag("go")
        .with_tag("k8s")
        .with_token_estimate(counter.count(content));
    lifecycle.register(&mem.id.to_string(), MemoryTier::Working);
    store.add_memory(id, MemoryTier::Working, mem)?;

    log_op(store, id, OperationType::ToolCall { name: "kubectl".into() }, "describe pod", 30)?;
    Ok(())
}

// -----------------------------------------------------------------------------
// small helpers
// -----------------------------------------------------------------------------

fn create_session(
    store: &SessionStore,
    name: &str,
    model: &str,
) -> Result<Uuid, Box<dyn Error>> {
    Ok(store.create_session(SessionMetadata::new(name, model).with_tag("demo"))?)
}

fn append_turns<I: IntoIterator<Item = Message>>(
    store: &SessionStore,
    id: Uuid,
    turns: I,
) -> Result<(), Box<dyn Error>> {
    for m in turns {
        store.append_message(id, m)?;
    }
    Ok(())
}

fn log_op(
    store: &SessionStore,
    id: Uuid,
    op_type: OperationType,
    detail: &str,
    duration_ms: u64,
) -> Result<(), Box<dyn Error>> {
    store.append_operation(
        id,
        Operation::new(op_type, serde_json::json!({ "detail": detail }), duration_ms),
    )?;
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max).collect();
        format!("{head}...")
    }
}
