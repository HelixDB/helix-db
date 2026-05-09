//! User Acceptance Tests for the MemSt integration in HelixDB.
//!
//! These exercise end-to-end scenarios that a downstream user of
//! `helix_db::memst` is expected to rely on. They are written in
//! Given/When/Then style and prefer the public API surface (re-exports
//! from [`helix_db::memst`]) over module internals.
//!
//! Coverage map:
//! - UAT-01..03  Session + message lifecycle (file/session management).
//! - UAT-04..07  Memory tiers, querying, promotion (memory management).
//! - UAT-08..09  Lifecycle promotion / demotion triggers.
//! - UAT-10..12  Token-budget aware compaction.
//! - UAT-13..14  Operation log filtering.
//! - UAT-15..16  Content-addressable object store.
//! - UAT-17..18  Persistence and isolation invariants.
//! - UAT-19..21  Composed end-to-end workflows.

use chrono::{Duration, Utc};
use helix_db::memst::{
    Author, Blob, Commit, CommitMetadata, CommitSource, ConfiguredTokenCounter, LifecycleConfig,
    MemoryItem, MemoryLifecycle, MemoryQuery, MemoryScope, MemoryState, MemoryTier, MemoryType,
    Message, ObjectId, ObjectStore, Operation, OperationQuery, OperationType, Role,
    SessionMetadata, SessionStore, Tag, TokenCounter, TransitionTrigger, Tree, TreeEntry,
};
use tempfile::TempDir;
use uuid::Uuid;

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn store() -> (TempDir, SessionStore) {
    let dir = TempDir::new().unwrap();
    let store = SessionStore::init(dir.path()).unwrap();
    (dir, store)
}

fn session(store: &SessionStore, name: &str) -> Uuid {
    store
        .create_session(SessionMetadata::new(name, "gpt-4"))
        .unwrap()
}

fn working_memory(content: &str, tag: &str, tokens: u32) -> MemoryItem {
    MemoryItem::new(content, "uat")
        .with_tag(tag)
        .with_token_estimate(tokens)
        .with_memory_type(MemoryType::Semantic)
}

// -----------------------------------------------------------------------------
// UAT-01..03: Session + message lifecycle
// -----------------------------------------------------------------------------

#[test]
fn uat_01_session_lifecycle_create_list_get_delete() {
    // Given a freshly initialized store
    let (_dir, store) = store();

    // When a session is created with metadata
    let id = store
        .create_session(SessionMetadata::new("UAT", "gpt-4").with_tag("test"))
        .unwrap();

    // Then it shows up in list_sessions
    let sessions = store.list_sessions().unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].id, id);
    assert_eq!(sessions[0].name, "UAT");
    assert_eq!(sessions[0].tags, vec!["test".to_string()]);

    // And get_session returns matching metadata
    let meta = store.get_session(id).unwrap().expect("session exists");
    assert_eq!(meta.model, "gpt-4");
    assert_eq!(meta.message_count, 0);
    assert_eq!(meta.token_count, 0);

    // When deleted
    store.delete_session(id).unwrap();

    // Then list is empty and the session can no longer be retrieved
    assert_eq!(store.list_sessions().unwrap().len(), 0);
    assert!(store.get_session(id).unwrap().is_none());
}

#[test]
fn uat_02_message_conversation_round_trip() {
    // Given a session
    let (_dir, store) = store();
    let id = session(&store, "chat");

    // When a 3-turn conversation is appended
    store
        .append_message(id, Message::system("You are a helpful assistant."))
        .unwrap();
    store
        .append_message(id, Message::user("hello").with_token_count(2))
        .unwrap();
    store
        .append_message(
            id,
            Message::assistant("hi! how can I help?").with_token_count(7),
        )
        .unwrap();

    // Then messages come back in order
    let msgs = store.get_messages(id).unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs[0].role, Role::System);
    assert_eq!(msgs[1].role, Role::User);
    assert_eq!(msgs[2].role, Role::Assistant);

    // And metadata reflects message + token counts
    let meta = store.get_session(id).unwrap().unwrap();
    assert_eq!(meta.message_count, 3);
    assert_eq!(meta.token_count, 9);

    // And a range query returns the correct slice
    let middle = store.get_messages_range(id, 1, 2).unwrap();
    assert_eq!(middle.len(), 1);
    assert_eq!(middle[0].role, Role::User);
}

#[test]
fn uat_03_message_index_offsets_match_binary_layout() {
    // Given a session with three messages
    let (_dir, store) = store();
    let id = session(&store, "index");
    for i in 0..3 {
        store
            .append_message(id, Message::user(format!("msg {}", i)))
            .unwrap();
    }

    // When the index is read
    let idx = store.read_message_index(id).unwrap();

    // Then there is one entry per message and offsets are strictly increasing
    assert_eq!(idx.len(), 3);
    let mut prev = 0u64;
    for entry in &idx {
        assert!(entry.byte_offset >= prev);
        prev = entry.byte_offset + entry.byte_length;
        assert_eq!(entry.role, Role::User);
    }
}

// -----------------------------------------------------------------------------
// UAT-04..07: Memory tiers, querying, retrieval ordering
// -----------------------------------------------------------------------------

#[test]
fn uat_04_tiered_memory_promotion_working_to_long_term() {
    // Given a session with a working-tier memory
    let (_dir, store) = store();
    let id = session(&store, "tiers");
    let mem = working_memory("user prefers tokio for async", "rust", 7);
    store.add_memory(id, MemoryTier::Working, mem.clone()).unwrap();
    assert_eq!(store.get_tier(id, MemoryTier::Working).unwrap().len(), 1);
    assert_eq!(store.get_tier(id, MemoryTier::LongTerm).unwrap().len(), 0);

    // When the memory is promoted to LongTerm
    let promoted = store.promote_memory(id, mem.id, MemoryTier::LongTerm).unwrap();
    assert!(promoted);

    // Then it lives only in LongTerm
    assert!(store.get_tier(id, MemoryTier::Working).unwrap().is_empty());
    let long = store.get_tier(id, MemoryTier::LongTerm).unwrap();
    assert_eq!(long.len(), 1);
    assert_eq!(long[0].id, mem.id);

    // And get_memory finds it across tiers
    let found = store.get_memory(id, mem.id).unwrap().unwrap();
    assert_eq!(found.id, mem.id);
}

#[test]
fn uat_05_memory_query_filters_by_keyword_and_tag() {
    // Given a session with memories at different tiers and tags
    let (_dir, store) = store();
    let id = session(&store, "query");
    store
        .add_memory(id, MemoryTier::Working, working_memory("Rust uses Tokio for async", "rust", 5))
        .unwrap();
    store
        .add_memory(id, MemoryTier::ShortTerm, working_memory("Go uses goroutines", "go", 4))
        .unwrap();
    store
        .add_memory(id, MemoryTier::LongTerm, working_memory("User likes async/await", "rust", 3))
        .unwrap();

    // When querying for keyword "async" and tag "rust"
    let q = MemoryQuery::new()
        .with_keyword("async")
        .with_tag("rust");
    let hits = store.retrieve_memories(id, q).unwrap();

    // Then only the rust-tagged "async" memories match
    assert_eq!(hits.len(), 2);
    for hit in hits {
        assert!(hit.tags.contains(&"rust".to_string()));
        assert!(hit.content.to_lowercase().contains("async"));
    }
}

#[test]
fn uat_06_memory_query_respects_min_confidence() {
    let (_dir, store) = store();
    let id = session(&store, "conf");
    store
        .add_memory(
            id,
            MemoryTier::Working,
            working_memory("low conf", "x", 1).with_confidence(0.2),
        )
        .unwrap();
    store
        .add_memory(
            id,
            MemoryTier::Working,
            working_memory("high conf", "x", 1).with_confidence(0.9),
        )
        .unwrap();

    let hits = store
        .retrieve_memories(id, MemoryQuery::new().with_min_confidence(0.5))
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].content, "high conf");
}

#[test]
fn uat_07_memory_query_limits_results() {
    let (_dir, store) = store();
    let id = session(&store, "limit");
    for i in 0..10 {
        store
            .add_memory(
                id,
                MemoryTier::Working,
                working_memory(&format!("note {}", i), "x", 1),
            )
            .unwrap();
    }

    let hits = store
        .retrieve_memories(id, MemoryQuery::new().with_limit(3))
        .unwrap();
    assert_eq!(hits.len(), 3);
}

// -----------------------------------------------------------------------------
// UAT-08..09: Lifecycle promotion / demotion triggers
// -----------------------------------------------------------------------------

#[test]
fn uat_08_lifecycle_promotion_triggered_by_access_count() {
    let lifecycle = MemoryLifecycle::new(LifecycleConfig::default());

    let mut item = MemoryItem::new("cool fact", "uat").with_confidence(0.4);
    assert!(lifecycle.should_promote(&item).is_none());

    // After enough accesses, the access-count threshold fires.
    for _ in 0..3 {
        item.record_access();
    }
    let trigger = lifecycle.should_promote(&item).expect("should fire");
    assert!(matches!(trigger, TransitionTrigger::AccessCount(n) if n >= 3));
}

#[test]
fn uat_09_lifecycle_demotion_triggered_by_ttl() {
    let lifecycle = MemoryLifecycle::new(LifecycleConfig::default());

    let mut item = MemoryItem::new("stale", "uat");
    item.last_accessed = Utc::now() - Duration::hours(48);

    // Working memory expires after 1h by default; 48h is way past TTL.
    assert!(matches!(
        lifecycle.should_demote(&item, MemoryTier::Working),
        Some(TransitionTrigger::TtlExpired)
    ));
    // ShortTerm expires after 24h by default.
    assert!(matches!(
        lifecycle.should_demote(&item, MemoryTier::ShortTerm),
        Some(TransitionTrigger::TtlExpired)
    ));
    // LongTerm never demotes by TTL.
    assert!(lifecycle.should_demote(&item, MemoryTier::LongTerm).is_none());
}

// -----------------------------------------------------------------------------
// UAT-10..12: Token-budget aware compaction
// -----------------------------------------------------------------------------

#[test]
fn uat_10_compaction_checkpoint_create_finalize_restore() {
    let mut lifecycle = MemoryLifecycle::new(LifecycleConfig::default());
    let pre = ObjectId::from_content(b"pre-commit");
    let post = ObjectId::from_content(b"post-commit");

    let cp = lifecycle.create_checkpoint(
        Some(MemoryScope::Session("s1".into())),
        MemoryTier::Working,
        MemoryTier::ShortTerm,
        vec!["m1".into(), "m2".into()],
        pre,
        1_000,
    );
    let cp_id = cp.id.clone();
    assert!(cp_id.starts_with("chk-"));
    assert!(cp.post_commit.is_none());

    // Finalize updates the checkpoint with post-commit and compacted tokens.
    lifecycle
        .finalize_checkpoint(&cp_id, post, 250)
        .expect("finalize should succeed");

    // Restore looks up the checkpoint by id.
    let restored = lifecycle.restore_checkpoint(&cp_id).unwrap();
    assert_eq!(restored.post_commit, Some(post));
    assert_eq!(restored.compacted_tokens, 250);

    // Listing returns at least the one checkpoint we created.
    assert!(!lifecycle.list_checkpoints().is_empty());
}

#[test]
fn uat_11_token_budget_exceeded_when_over_limit() {
    // Working budget defaults to 4096 tokens.
    let mut lifecycle = MemoryLifecycle::new(LifecycleConfig::default());

    let mut memories = Vec::new();
    for _ in 0..10 {
        let m = MemoryItem::new("x", "uat").with_token_estimate(500);
        lifecycle.register(&m.id.to_string(), MemoryTier::Working);
        memories.push(m);
    }

    // 10 * 500 = 5000 > 4096, budget should be exceeded.
    assert!(lifecycle.is_budget_exceeded(&memories, MemoryTier::Working));
    // ShortTerm has a higher budget and we registered everything to Working,
    // so ShortTerm usage is 0 and not exceeded.
    assert!(!lifecycle.is_budget_exceeded(&memories, MemoryTier::ShortTerm));
}

#[test]
fn uat_12_select_for_compaction_picks_low_importance_oldest_first() {
    let mut lifecycle = MemoryLifecycle::new(LifecycleConfig::default());

    let mut high = MemoryItem::new("important", "uat");
    high.importance = 0.9;
    high.last_accessed = Utc::now();
    lifecycle.register(&high.id.to_string(), MemoryTier::Working);

    let mut low_recent = MemoryItem::new("low recent", "uat");
    low_recent.importance = 0.1;
    low_recent.last_accessed = Utc::now();
    lifecycle.register(&low_recent.id.to_string(), MemoryTier::Working);

    let mut low_old = MemoryItem::new("low old", "uat");
    low_old.importance = 0.1;
    low_old.last_accessed = Utc::now() - Duration::days(7);
    lifecycle.register(&low_old.id.to_string(), MemoryTier::Working);

    let memories = vec![high.clone(), low_recent.clone(), low_old.clone()];
    let picked = lifecycle.select_for_compaction(&memories, MemoryTier::Working, 2);

    // The two lowest-importance items are picked, with the older one first.
    assert_eq!(picked.len(), 2);
    assert_eq!(picked[0].id, low_old.id);
    assert_eq!(picked[1].id, low_recent.id);
}

// -----------------------------------------------------------------------------
// UAT-13..14: Operation log filtering
// -----------------------------------------------------------------------------

#[test]
fn uat_13_operations_log_filter_by_type() {
    let (_dir, store) = store();
    let id = session(&store, "ops");
    store
        .append_operation(id, Operation::tool_call("search", serde_json::json!({}), 10))
        .unwrap();
    store
        .append_operation(
            id,
            Operation::new(OperationType::WebSearch, serde_json::json!({}), 20),
        )
        .unwrap();
    store
        .append_operation(id, Operation::tool_call("fetch", serde_json::json!({}), 5))
        .unwrap();

    let only_web = store
        .get_operations_by_type(id, OperationType::WebSearch)
        .unwrap();
    assert_eq!(only_web.len(), 1);
    assert!(matches!(only_web[0].op_type, OperationType::WebSearch));
}

#[test]
fn uat_14_operations_log_filter_by_time_range_and_limit() {
    let (_dir, store) = store();
    let id = session(&store, "ops-range");
    let t0 = Utc::now() - Duration::hours(2);
    let t1 = Utc::now() - Duration::hours(1);
    let t2 = Utc::now() + Duration::hours(1);

    let mut op_old = Operation::new(OperationType::WebSearch, serde_json::json!({}), 1);
    op_old.timestamp = t0 - Duration::minutes(1);
    let mut op_mid = Operation::new(OperationType::WebSearch, serde_json::json!({}), 1);
    op_mid.timestamp = t0 + Duration::minutes(30);
    let mut op_new = Operation::new(OperationType::WebSearch, serde_json::json!({}), 1);
    op_new.timestamp = t1 + Duration::minutes(30);
    for op in [op_old, op_mid, op_new] {
        store.append_operation(id, op).unwrap();
    }

    let q = OperationQuery::new()
        .with_time_range(t0, t2)
        .with_limit(2);
    let hits = store.query_operations(id, q).unwrap();
    assert!(hits.len() <= 2);
    for op in &hits {
        assert!(op.timestamp >= t0);
        assert!(op.timestamp <= t2);
    }
}

// -----------------------------------------------------------------------------
// UAT-15..16: Object store
// -----------------------------------------------------------------------------

#[test]
fn uat_15_object_store_commit_chain_traversal() {
    let dir = TempDir::new().unwrap();
    let mut os = ObjectStore::new(&dir.path().to_path_buf()).unwrap();

    // Build a parent commit
    let mut tree = Tree::new();
    let blob_oid = os.write_blob(&Blob::new(b"data v1")).unwrap();
    tree.add_entry(TreeEntry::new(TreeEntry::MODE_FILE, blob_oid, "file.txt"));
    let tree1 = os.write_tree(&tree).unwrap();
    let author = Author::new("uat", "uat@example.com");
    let parent = Commit::with_metadata(
        tree1,
        author.clone(),
        "init",
        CommitMetadata {
            token_delta: 100,
            confidence: 1.0,
            source: CommitSource::UserExplicit,
            scope: None,
        },
    );
    let parent_oid = os.write_commit(&parent).unwrap();

    // Build a child commit pointing at parent
    let blob2 = os.write_blob(&Blob::new(b"data v2")).unwrap();
    let mut tree2 = Tree::new();
    tree2.add_entry(TreeEntry::new(TreeEntry::MODE_FILE, blob2, "file.txt"));
    let tree2_oid = os.write_tree(&tree2).unwrap();
    let mut child = Commit::new(tree2_oid, author.clone(), "update");
    child.add_parent(parent_oid);
    let child_oid = os.write_commit(&child).unwrap();

    // Read back and verify the parent linkage
    let read = os.read_commit(&child_oid).unwrap();
    assert_eq!(read.first_parent(), Some(parent_oid));
    assert!(!read.is_merge());

    // Tagging the parent and reading it back works
    let tag = Tag::new(parent_oid, "v1.0", author, "first release");
    let tag_oid = os.write_tag(&tag).unwrap();
    let read_tag = os.read_tag(&tag_oid).unwrap();
    assert_eq!(read_tag.target_oid, parent_oid);
    assert_eq!(read_tag.name, "v1.0");
}

#[test]
fn uat_16_object_store_dedupes_identical_blobs() {
    let dir = TempDir::new().unwrap();
    let mut os = ObjectStore::new(&dir.path().to_path_buf()).unwrap();

    let oid1 = os.write_blob(&Blob::new(b"same content")).unwrap();
    let oid2 = os.write_blob(&Blob::new(b"same content")).unwrap();
    let oid3 = os.write_blob(&Blob::new(b"different")).unwrap();

    assert_eq!(oid1, oid2);
    assert_ne!(oid1, oid3);
    // Dedup means the store contains only 2 distinct objects.
    assert_eq!(os.count().unwrap(), 2);
}

// -----------------------------------------------------------------------------
// UAT-17..18: Persistence and isolation
// -----------------------------------------------------------------------------

#[test]
fn uat_17_multi_session_isolation_no_cross_contamination() {
    let (_dir, store) = store();
    let a = session(&store, "alice");
    let b = session(&store, "bob");

    store
        .add_memory(a, MemoryTier::Working, working_memory("alice secret", "private", 1))
        .unwrap();
    store
        .add_memory(b, MemoryTier::Working, working_memory("bob fact", "public", 1))
        .unwrap();

    let alice = store.get_tier(a, MemoryTier::Working).unwrap();
    let bob = store.get_tier(b, MemoryTier::Working).unwrap();
    assert_eq!(alice.len(), 1);
    assert_eq!(bob.len(), 1);
    assert_ne!(alice[0].content, bob[0].content);
}

#[test]
fn uat_18_session_persistence_across_reopen() {
    let dir = TempDir::new().unwrap();

    // First session: write data, then drop the store (releases lock).
    let id = {
        let store = SessionStore::init(dir.path()).unwrap();
        let id = session(&store, "persistent");
        store
            .append_message(id, Message::user("first message"))
            .unwrap();
        store
            .add_memory(id, MemoryTier::LongTerm, working_memory("learned fact", "x", 5))
            .unwrap();
        id
    };

    // Re-open and verify everything is intact.
    let store = SessionStore::open(dir.path()).unwrap();
    let summaries = store.list_sessions().unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id, id);

    let msgs = store.get_messages(id).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].role, Role::User);

    let lt = store.get_tier(id, MemoryTier::LongTerm).unwrap();
    assert_eq!(lt.len(), 1);
    assert_eq!(lt[0].content, "learned fact");
}

// -----------------------------------------------------------------------------
// UAT-19..21: End-to-end composed workflows
// -----------------------------------------------------------------------------

#[test]
fn uat_19_full_chat_extract_store_retrieve_workflow() {
    // A realistic workflow: chat -> extract memories -> persist -> retrieve.
    let (_dir, store) = store();
    let id = session(&store, "e2e");

    // 1. Chat turns are appended.
    let turns = [
        Message::system("You are helpful."),
        Message::user("I prefer Rust over Python for systems work."),
        Message::assistant("Got it - I'll keep that in mind."),
    ];
    for m in turns {
        store.append_message(id, m).unwrap();
    }

    // 2. The agent extracts a semantic memory and tags it.
    let counter = ConfiguredTokenCounter::new();
    let extracted = MemoryItem::new(
        "User prefers Rust over Python for systems-programming work",
        "extraction",
    )
    .with_memory_type(MemoryType::Semantic)
    .with_tag("preference")
    .with_tag("language");
    let token_estimate = counter.count(&extracted.content);
    let extracted = extracted.with_token_estimate(token_estimate);
    store
        .add_memory(id, MemoryTier::ShortTerm, extracted.clone())
        .unwrap();

    // 3. The retrieval API surfaces it again on a relevant query.
    let q = MemoryQuery::new()
        .with_keyword("rust")
        .with_tag("preference");
    let hits = store.retrieve_memories(id, q).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, extracted.id);

    // 4. Operation log records the retrieval.
    store
        .append_operation(
            id,
            Operation::new(
                OperationType::MemoryRetrieval,
                serde_json::json!({ "query": "rust preference" }),
                3,
            ),
        )
        .unwrap();
    let ops = store
        .get_operations_by_type(id, OperationType::MemoryRetrieval)
        .unwrap();
    assert_eq!(ops.len(), 1);
}

#[test]
fn uat_20_memory_access_increments_count_and_recency() {
    let (_dir, store) = store();
    let id = session(&store, "access");
    let mem = working_memory("hot fact", "x", 1);
    store.add_memory(id, MemoryTier::Working, mem.clone()).unwrap();

    let before = store.get_memory(id, mem.id).unwrap().unwrap();
    assert_eq!(before.access_count, 0);

    let touched = store.access_memory(id, mem.id).unwrap();
    assert!(touched);

    let after = store.get_memory(id, mem.id).unwrap().unwrap();
    assert_eq!(after.access_count, 1);
    assert!(after.last_accessed >= before.last_accessed);
    // Importance is recomputed on access.
    assert!(after.importance > 0.0);

    // Accessing a non-existent memory id returns false, not an error.
    let missing = store.access_memory(id, Uuid::new_v4()).unwrap();
    assert!(!missing);
}

#[test]
fn uat_21_state_transition_history_is_preserved() {
    let mut lifecycle = MemoryLifecycle::new(LifecycleConfig::default());
    lifecycle.register("m1", MemoryTier::Working);

    lifecycle
        .transition(
            "m1",
            MemoryState::ShortTerm,
            TransitionTrigger::TtlExpired,
            -50,
        )
        .unwrap();
    lifecycle
        .transition(
            "m1",
            MemoryState::LongTerm,
            TransitionTrigger::ImportanceThreshold(0.8),
            20,
        )
        .unwrap();

    let history = lifecycle.transitions();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].to, MemoryState::ShortTerm);
    assert_eq!(history[1].from, MemoryState::ShortTerm);
    assert_eq!(history[1].to, MemoryState::LongTerm);

    // Final state matches the last transition.
    assert_eq!(lifecycle.get_state("m1"), Some(MemoryState::LongTerm));
}
