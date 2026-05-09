//! Multi-session UAT mirroring the `memst_multi_session` example.
//!
//! These tests assert the same invariants the demo exercises: per-session
//! isolation, per-tier accounting, lifecycle decisions under a tight token
//! budget, and persistence across reopen. They are meant to ride along with
//! the demo - if you change the demo's behaviour, update these accordingly.

use helix_db::memst::{
    ConfiguredTokenCounter, LifecycleConfig, MemoryItem, MemoryLifecycle, MemoryQuery, MemoryState,
    MemoryTier, MemoryType, Message, Operation, OperationType, SessionMetadata, SessionStore,
    TokenCounter, TransitionTrigger,
};
use tempfile::TempDir;
use uuid::Uuid;

// -----------------------------------------------------------------------------
// scenario setup helpers (mirror examples/memst_multi_session.rs)
// -----------------------------------------------------------------------------

struct UserScript {
    name: &'static str,
    model: &'static str,
    messages: Vec<Message>,
    memories: Vec<(&'static str, MemoryType, &'static [&'static str])>,
}

fn alice() -> UserScript {
    UserScript {
        name: "alice",
        model: "claude-sonnet-4",
        messages: vec![
            Message::system("You help with Rust async."),
            Message::user("How does Tokio's runtime schedule tasks?").with_token_count(8),
            Message::assistant("Tokio uses a multi-threaded work-stealing scheduler.")
                .with_token_count(60),
            Message::user("Is it OK to mix tokio and async-std?").with_token_count(9),
            Message::assistant("Generally avoid mixing runtimes.").with_token_count(40),
        ],
        memories: vec![
            (
                "user prefers Tokio over async-std for async runtimes",
                MemoryType::Semantic,
                &["rust"],
            ),
            (
                "user is comfortable with work-stealing schedulers",
                MemoryType::Semantic,
                &["rust"],
            ),
            (
                "user wants idiomatic Rust async patterns",
                MemoryType::Semantic,
                &["rust"],
            ),
        ],
    }
}

fn bob() -> UserScript {
    UserScript {
        name: "bob",
        model: "gpt-4o",
        messages: vec![
            Message::system("You help with ML in Python."),
            Message::user("How do I avoid GIL contention in PyTorch?").with_token_count(11),
            Message::assistant("Use multiprocessing or move tensors to GPU.").with_token_count(35),
        ],
        memories: vec![(
            "user uses Python and PyTorch; cares about GIL implications",
            MemoryType::Semantic,
            &["python", "ml"],
        )],
    }
}

fn carol() -> UserScript {
    UserScript {
        name: "carol",
        model: "llama-3-70b",
        messages: vec![
            Message::system("You help with Go infrastructure."),
            Message::user("How do I tune GOMAXPROCS in Kubernetes?").with_token_count(12),
            Message::assistant("Use the automaxprocs library.").with_token_count(45),
        ],
        memories: vec![(
            "user runs Go services on Kubernetes; tuning GOMAXPROCS is recurring",
            MemoryType::Procedural,
            &["go", "k8s"],
        )],
    }
}

fn run_script<C: TokenCounter>(
    store: &SessionStore,
    lifecycle: &mut MemoryLifecycle,
    counter: &C,
    script: UserScript,
) -> Uuid {
    let id = store
        .create_session(SessionMetadata::new(script.name, script.model).with_tag("uat"))
        .unwrap();
    for m in script.messages {
        store.append_message(id, m).unwrap();
    }
    for (content, ty, tags) in script.memories {
        let mut mem = MemoryItem::new(content, "extraction")
            .with_memory_type(ty)
            .with_token_estimate(counter.count(content));
        for t in tags {
            mem = mem.with_tag(*t);
        }
        lifecycle.register(&mem.id.to_string(), MemoryTier::Working);
        store.add_memory(id, MemoryTier::Working, mem).unwrap();
    }
    id
}

// -----------------------------------------------------------------------------
// UAT-MS-01: each session gets its own state and they don't bleed
// -----------------------------------------------------------------------------

#[test]
fn uat_ms_01_three_sessions_remain_isolated() {
    let dir = TempDir::new().unwrap();
    let store = SessionStore::init(dir.path()).unwrap();
    let mut lc = MemoryLifecycle::new(LifecycleConfig::default());
    let counter = ConfiguredTokenCounter::default();

    let alice_id = run_script(&store, &mut lc, &counter, alice());
    let bob_id = run_script(&store, &mut lc, &counter, bob());
    let carol_id = run_script(&store, &mut lc, &counter, carol());

    // Three sessions are listed.
    let sessions = store.list_sessions().unwrap();
    assert_eq!(sessions.len(), 3);
    let names: Vec<_> = sessions.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"alice"));
    assert!(names.contains(&"bob"));
    assert!(names.contains(&"carol"));

    // Per-session memory counts match the scripts.
    assert_eq!(
        store.get_tier(alice_id, MemoryTier::Working).unwrap().len(),
        3
    );
    assert_eq!(
        store.get_tier(bob_id, MemoryTier::Working).unwrap().len(),
        1
    );
    assert_eq!(
        store
            .get_tier(carol_id, MemoryTier::Working)
            .unwrap()
            .len(),
        1
    );

    // A query for "python" only matches Bob's memories.
    for (id, expected) in [(alice_id, 0), (bob_id, 1), (carol_id, 0)] {
        let hits = store
            .retrieve_memories(id, MemoryQuery::new().with_keyword("python"))
            .unwrap();
        assert_eq!(
            hits.len(),
            expected,
            "session {id} should have {expected} python hits"
        );
    }

    // A query for "go" only matches Carol's memories.
    let hits = store
        .retrieve_memories(carol_id, MemoryQuery::new().with_tag("go"))
        .unwrap();
    assert_eq!(hits.len(), 1);
}

// -----------------------------------------------------------------------------
// UAT-MS-02: per-session message + token accounting
// -----------------------------------------------------------------------------

#[test]
fn uat_ms_02_message_and_token_counts_are_per_session() {
    let dir = TempDir::new().unwrap();
    let store = SessionStore::init(dir.path()).unwrap();
    let mut lc = MemoryLifecycle::new(LifecycleConfig::default());
    let counter = ConfiguredTokenCounter::default();

    let alice_id = run_script(&store, &mut lc, &counter, alice());
    let bob_id = run_script(&store, &mut lc, &counter, bob());
    let carol_id = run_script(&store, &mut lc, &counter, carol());

    // alice: 5 messages, token sum = 8 + 60 + 9 + 40 = 117 (system has none).
    let alice_meta = store.get_session(alice_id).unwrap().unwrap();
    assert_eq!(alice_meta.message_count, 5);
    assert_eq!(alice_meta.token_count, 117);

    // bob: 3 messages, token sum = 11 + 35 = 46.
    let bob_meta = store.get_session(bob_id).unwrap().unwrap();
    assert_eq!(bob_meta.message_count, 3);
    assert_eq!(bob_meta.token_count, 46);

    // carol: 3 messages, token sum = 12 + 45 = 57.
    let carol_meta = store.get_session(carol_id).unwrap().unwrap();
    assert_eq!(carol_meta.message_count, 3);
    assert_eq!(carol_meta.token_count, 57);
}

// -----------------------------------------------------------------------------
// UAT-MS-03: tight budget triggers compaction selection
// -----------------------------------------------------------------------------

#[test]
fn uat_ms_03_budget_pressure_triggers_compaction_candidates() {
    let dir = TempDir::new().unwrap();
    let store = SessionStore::init(dir.path()).unwrap();

    // A small working budget so even Alice's three semantic memories overflow.
    let cfg = LifecycleConfig {
        working_memory_max_tokens: 60,
        promotion_access_threshold: 2,
        ..LifecycleConfig::default()
    };
    let mut lc = MemoryLifecycle::new(cfg);
    let counter = ConfiguredTokenCounter::default();
    let alice_id = run_script(&store, &mut lc, &counter, alice());

    let working = store.get_tier(alice_id, MemoryTier::Working).unwrap();
    let total: u32 = working.iter().filter_map(|m| m.token_estimate).sum();
    assert!(
        total > 60,
        "alice's three memories should add up to more than the 60-token budget; got {total}"
    );
    assert!(lc.is_budget_exceeded(&working, MemoryTier::Working));

    // select_for_compaction should pick at most 2 lowest-importance items.
    let picked = lc.select_for_compaction(&working, MemoryTier::Working, 2);
    assert_eq!(picked.len(), 2);

    // Move them to ShortTerm and update the lifecycle.
    for m in picked {
        store
            .promote_memory(alice_id, m.id, MemoryTier::ShortTerm)
            .unwrap();
        lc.transition(
            &m.id.to_string(),
            MemoryState::ShortTerm,
            TransitionTrigger::TokenBudgetExceeded,
            -(m.token_estimate.unwrap_or(0) as i32),
        )
        .unwrap();
    }

    let working_after = store.get_tier(alice_id, MemoryTier::Working).unwrap();
    let short_after = store.get_tier(alice_id, MemoryTier::ShortTerm).unwrap();
    assert_eq!(working_after.len(), 1);
    assert_eq!(short_after.len(), 2);
}

// -----------------------------------------------------------------------------
// UAT-MS-04: hot memory crosses access threshold and promotes to long-term
// -----------------------------------------------------------------------------

#[test]
fn uat_ms_04_repeated_access_promotes_to_long_term() {
    let dir = TempDir::new().unwrap();
    let store = SessionStore::init(dir.path()).unwrap();
    let cfg = LifecycleConfig {
        promotion_access_threshold: 2,
        ..LifecycleConfig::default()
    };
    let mut lc = MemoryLifecycle::new(cfg);
    let counter = ConfiguredTokenCounter::default();
    let alice_id = run_script(&store, &mut lc, &counter, alice());

    // Find the Tokio memory and access it twice.
    let working = store.get_tier(alice_id, MemoryTier::Working).unwrap();
    let tokio_mem = working
        .iter()
        .find(|m| m.content.contains("Tokio"))
        .expect("alice has a Tokio memory")
        .clone();

    for _ in 0..2 {
        assert!(store.access_memory(alice_id, tokio_mem.id).unwrap());
    }
    let updated = store.get_memory(alice_id, tokio_mem.id).unwrap().unwrap();
    assert_eq!(updated.access_count, 2);

    // The lifecycle should now flag promotion.
    let trigger = lc.should_promote(&updated).expect("should fire");
    assert!(matches!(
        trigger,
        TransitionTrigger::AccessCount(n) if n >= 2
    ));

    // Apply the promotion.
    store
        .promote_memory(alice_id, tokio_mem.id, MemoryTier::LongTerm)
        .unwrap();
    lc.transition(
        &tokio_mem.id.to_string(),
        MemoryState::LongTerm,
        trigger,
        0,
    )
    .unwrap();

    // The Tokio memory now lives only in long-term.
    let long = store.get_tier(alice_id, MemoryTier::LongTerm).unwrap();
    assert!(long.iter().any(|m| m.id == tokio_mem.id));
    let working_after = store.get_tier(alice_id, MemoryTier::Working).unwrap();
    assert!(working_after.iter().all(|m| m.id != tokio_mem.id));
    assert_eq!(lc.get_state(&tokio_mem.id.to_string()), Some(MemoryState::LongTerm));
}

// -----------------------------------------------------------------------------
// UAT-MS-05: operation logs are per-session and surviving reopen
// -----------------------------------------------------------------------------

#[test]
fn uat_ms_05_operations_isolated_and_persistent() {
    let dir = TempDir::new().unwrap();
    let alice_id;
    let bob_id;

    {
        let store = SessionStore::init(dir.path()).unwrap();
        let mut lc = MemoryLifecycle::new(LifecycleConfig::default());
        let counter = ConfiguredTokenCounter::default();
        alice_id = run_script(&store, &mut lc, &counter, alice());
        bob_id = run_script(&store, &mut lc, &counter, bob());

        store
            .append_operation(
                alice_id,
                Operation::new(
                    OperationType::MemoryRetrieval,
                    serde_json::json!({"q": "tokio"}),
                    4,
                ),
            )
            .unwrap();
        store
            .append_operation(
                bob_id,
                Operation::new(OperationType::WebSearch, serde_json::json!({"q": "GIL"}), 12),
            )
            .unwrap();
    }

    // Re-open and verify everything is still there and isolated.
    let store = SessionStore::open(dir.path()).unwrap();
    let alice_ops = store.get_operations(alice_id).unwrap();
    let bob_ops = store.get_operations(bob_id).unwrap();
    assert_eq!(alice_ops.len(), 1);
    assert_eq!(bob_ops.len(), 1);
    assert!(matches!(alice_ops[0].op_type, OperationType::MemoryRetrieval));
    assert!(matches!(bob_ops[0].op_type, OperationType::WebSearch));
}

// -----------------------------------------------------------------------------
// UAT-MS-06: full round-trip - 3 sessions, persistence, retrieval all intact
// -----------------------------------------------------------------------------

#[test]
fn uat_ms_06_full_multi_session_persistence() {
    let dir = TempDir::new().unwrap();

    let (alice_id, bob_id, carol_id) = {
        let store = SessionStore::init(dir.path()).unwrap();
        let mut lc = MemoryLifecycle::new(LifecycleConfig::default());
        let counter = ConfiguredTokenCounter::default();
        let a = run_script(&store, &mut lc, &counter, alice());
        let b = run_script(&store, &mut lc, &counter, bob());
        let c = run_script(&store, &mut lc, &counter, carol());
        (a, b, c)
    };

    let store = SessionStore::open(dir.path()).unwrap();
    let summaries = store.list_sessions().unwrap();
    assert_eq!(summaries.len(), 3);

    let messages_total: u32 = summaries.iter().map(|s| s.message_count).sum();
    assert_eq!(messages_total, 5 + 3 + 3);

    // Spot-check that each user's first memory survived.
    let alice_w = store.get_tier(alice_id, MemoryTier::Working).unwrap();
    assert!(alice_w
        .iter()
        .any(|m| m.content.contains("Tokio") || m.content.contains("Rust")));

    let bob_w = store.get_tier(bob_id, MemoryTier::Working).unwrap();
    assert!(bob_w.iter().any(|m| m.content.contains("Python")));

    let carol_w = store.get_tier(carol_id, MemoryTier::Working).unwrap();
    assert!(carol_w.iter().any(|m| m.content.contains("Kubernetes")));
}
