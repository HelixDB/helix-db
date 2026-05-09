//! Fine-grained unit tests for the MemSt integration.
//!
//! Module-level inline tests cover the primary happy paths; these tests focus
//! on edge cases and small contracts: hex / serialization round-trips, default
//! impls, builder methods, error branches, and helpers that don't need a full
//! filesystem store.

use chrono::{Duration, Utc};
use helix_db::memst::{
    Author, Blob, Commit, CommitMetadata, CommitSource, ConfiguredTokenCounter, Content,
    ContentPart, LifecycleConfig, Manifest, MemoryItem, MemoryLifecycle, MemoryQuery, MemoryScope,
    MemoryState, MemoryTier, MemoryType, Message, ObjectId, ObjectStore, Operation,
    OperationQuery, OperationType, Role, SessionMetadata, SessionSummary, SimpleTokenCounter,
    Tag, TokenCounter, TransitionTrigger, Tree, TreeEntry,
};
use tempfile::TempDir;
use uuid::Uuid;

// -----------------------------------------------------------------------------
// ObjectId
// -----------------------------------------------------------------------------

#[test]
fn object_id_hex_round_trip() {
    let oid = ObjectId::from_content(b"hello world");
    let hex = oid.to_hex();
    assert_eq!(hex.len(), 64);
    let parsed = ObjectId::from_hex(&hex).unwrap();
    assert_eq!(oid, parsed);
}

#[test]
fn object_id_blake3_is_deterministic() {
    let a = ObjectId::from_content(b"abc");
    let b = ObjectId::from_content(b"abc");
    let c = ObjectId::from_content(b"abd");
    assert_eq!(a, b);
    assert_ne!(a, c);
}

#[test]
fn object_id_nil_and_abbreviate() {
    let nil = ObjectId::nil();
    assert!(nil.is_nil());
    assert_eq!(nil.to_hex(), "0".repeat(64));

    let oid = ObjectId::from_content(b"x");
    assert!(!oid.is_nil());
    assert_eq!(oid.abbreviate().len(), 7);
    assert_eq!(oid.abbreviate(), &oid.to_hex()[..7]);
}

#[test]
fn object_id_from_hex_rejects_wrong_length() {
    assert!(ObjectId::from_hex("abc").is_err());
    assert!(ObjectId::from_hex(&"f".repeat(63)).is_err());
    assert!(ObjectId::from_hex(&"f".repeat(65)).is_err());
}

#[test]
fn object_id_display_uses_hex() {
    let oid = ObjectId::from_content(b"x");
    assert_eq!(format!("{}", oid), oid.to_hex());
    assert_eq!(format!("{:?}", oid), oid.to_hex());
}

// -----------------------------------------------------------------------------
// Tree / TreeEntry / Author / Tag
// -----------------------------------------------------------------------------

#[test]
fn tree_default_is_empty() {
    let t = Tree::default();
    assert!(t.entries.is_empty());
    assert!(t.get_entry("anything").is_none());
}

#[test]
fn tree_add_get_remove_entries() {
    let mut t = Tree::new();
    let oid = ObjectId::from_content(b"x");
    t.add_entry(TreeEntry::new(TreeEntry::MODE_FILE, oid, "a.txt"));
    t.add_entry(TreeEntry::new(TreeEntry::MODE_DIR, oid, "subdir"));

    assert!(t.get_entry("a.txt").is_some());
    let removed = t.remove_entry("a.txt").unwrap();
    assert_eq!(removed.name, "a.txt");
    assert!(t.get_entry("a.txt").is_none());
    assert_eq!(t.entries.len(), 1);
}

#[test]
fn tree_entry_modes_are_octal() {
    assert_eq!(TreeEntry::MODE_FILE, 0o100644);
    assert_eq!(TreeEntry::MODE_DIR, 0o040000);
    assert_eq!(TreeEntry::MODE_EXECUTABLE, 0o100755);
}

#[test]
fn author_with_explicit_timestamp() {
    let ts = Utc::now() - Duration::days(30);
    let a = Author::with_timestamp("alice", "alice@example.com", ts);
    assert_eq!(a.name, "alice");
    assert_eq!(a.email, "alice@example.com");
    assert_eq!(a.timestamp, ts);
}

#[test]
fn tag_lightweight_has_default_tagger_and_no_signature() {
    let oid = ObjectId::from_content(b"x");
    let tag = Tag::lightweight(oid, "v1");
    assert!(tag.is_lightweight);
    assert!(tag.signature.is_none());
    assert_eq!(tag.message, "");
    assert_eq!(tag.tagger.name, "memst");
}

#[test]
fn tag_annotated_has_message_and_no_signature_initially() {
    let oid = ObjectId::from_content(b"x");
    let tag = Tag::new(oid, "v2", Author::new("a", "a@x"), "release notes");
    assert!(!tag.is_lightweight);
    assert_eq!(tag.message, "release notes");
    assert!(tag.signature.is_none());
    assert_eq!(tag.target_oid, oid);
}

// -----------------------------------------------------------------------------
// Commit
// -----------------------------------------------------------------------------

#[test]
fn commit_is_merge_when_multiple_parents() {
    let tree = ObjectId::from_content(b"tree");
    let mut c = Commit::new(tree, Author::new("a", "a@x"), "msg");
    assert!(!c.is_merge());

    c.add_parent(ObjectId::from_content(b"p1"));
    assert!(!c.is_merge());

    c.add_parent(ObjectId::from_content(b"p2"));
    assert!(c.is_merge());
    assert_eq!(c.first_parent(), Some(ObjectId::from_content(b"p1")));
}

#[test]
fn commit_metadata_default_values() {
    let m = CommitMetadata::default();
    assert_eq!(m.token_delta, 0);
    assert_eq!(m.confidence, 1.0);
    assert!(matches!(m.source, CommitSource::UserExplicit));
    assert!(m.scope.is_none());
}

#[test]
fn commit_with_metadata_round_trips_via_object_store() {
    let dir = TempDir::new().unwrap();
    let mut os = ObjectStore::new(&dir.path().to_path_buf()).unwrap();
    let tree = os.write_tree(&Tree::new()).unwrap();
    let metadata = CommitMetadata {
        token_delta: -5,
        confidence: 0.42,
        source: CommitSource::SleepConsolidation,
        scope: Some(MemoryScope::User("alice".into())),
    };
    let commit = Commit::with_metadata(tree, Author::new("a", "a@x"), "test", metadata);
    let oid = os.write_commit(&commit).unwrap();
    let read = os.read_commit(&oid).unwrap();
    assert_eq!(read.metadata.token_delta, -5);
    assert!((read.metadata.confidence - 0.42).abs() < 1e-6);
    assert!(matches!(read.metadata.source, CommitSource::SleepConsolidation));
    assert!(matches!(read.metadata.scope, Some(MemoryScope::User(ref s)) if s == "alice"));
}

// -----------------------------------------------------------------------------
// Manifest
// -----------------------------------------------------------------------------

fn summary(name: &str) -> SessionSummary {
    SessionSummary {
        id: Uuid::new_v4(),
        name: name.into(),
        model: "gpt-4".into(),
        tags: vec![],
        created_at: Utc::now(),
        last_activity: Utc::now(),
        message_count: 0,
    }
}

#[test]
fn manifest_upsert_replaces_existing() {
    let mut m = Manifest::new();
    let mut s = summary("first");
    let id = s.id;
    m.upsert_session(s.clone());
    assert_eq!(m.sessions.len(), 1);

    s.name = "renamed".into();
    m.upsert_session(s);
    assert_eq!(m.sessions.len(), 1);
    assert_eq!(m.get_session(&id).unwrap().name, "renamed");
}

#[test]
fn manifest_remove_session() {
    let mut m = Manifest::new();
    let s = summary("only");
    let id = s.id;
    m.upsert_session(s);
    m.remove_session(&id);
    assert!(m.get_session(&id).is_none());
    assert_eq!(m.sessions.len(), 0);
}

#[test]
fn manifest_default_schema_version() {
    let m = Manifest::new();
    assert_eq!(m.schema_version, "1.0.0");
    assert!(m.last_compaction.is_none());
}

// -----------------------------------------------------------------------------
// MemoryItem
// -----------------------------------------------------------------------------

#[test]
fn memory_item_record_access_increments_and_updates_recency() {
    let mut m = MemoryItem::new("x", "test");
    assert_eq!(m.access_count, 0);
    let before = m.last_accessed;
    std::thread::sleep(std::time::Duration::from_millis(2));
    m.record_access();
    assert_eq!(m.access_count, 1);
    assert!(m.last_accessed >= before);
    assert!(m.importance > 0.0);
}

#[test]
fn memory_item_with_confidence_clamps() {
    let m = MemoryItem::new("x", "test").with_confidence(2.5);
    assert_eq!(m.confidence, 1.0);
    let m2 = MemoryItem::new("x", "test").with_confidence(-0.5);
    assert_eq!(m2.confidence, 0.0);
}

#[test]
fn memory_item_builders_set_fields() {
    let m = MemoryItem::new("hello", "src")
        .with_tag("a")
        .with_tag("b")
        .with_tags(["c", "d"])
        .with_embedding(vec![1.0, 2.0])
        .with_memory_type(MemoryType::Procedural)
        .with_token_estimate(42);
    // with_tags replaces, not appends.
    assert_eq!(m.tags, vec!["c".to_string(), "d".to_string()]);
    assert_eq!(m.embedding.as_ref().unwrap().len(), 2);
    assert!(matches!(m.memory_type, MemoryType::Procedural));
    assert_eq!(m.token_estimate, Some(42));
}

// -----------------------------------------------------------------------------
// Content / Message / Role
// -----------------------------------------------------------------------------

#[test]
fn content_from_string_and_str() {
    let from_str: Content = "hi".into();
    assert!(matches!(from_str, Content::Text(ref s) if s == "hi"));

    let owned = String::from("hello");
    let from_string: Content = owned.into();
    assert!(matches!(from_string, Content::Text(ref s) if s == "hello"));
}

#[test]
fn content_default_is_empty_text() {
    match Content::default() {
        Content::Text(s) => assert!(s.is_empty()),
        _ => panic!("expected text default"),
    }
}

#[test]
fn content_multipart_holds_mixed_parts() {
    let c = Content::MultiPart(vec![
        ContentPart::Text("hello".into()),
        ContentPart::Image {
            data: vec![0xff, 0xd8],
            mime_type: "image/jpeg".into(),
        },
        ContentPart::File {
            hash: "abc".into(),
            filename: "x.txt".into(),
        },
    ]);
    if let Content::MultiPart(parts) = c {
        assert_eq!(parts.len(), 3);
    } else {
        panic!("expected multipart");
    }
}

#[test]
fn message_user_assistant_system_helpers() {
    let u = Message::user("hi");
    assert_eq!(u.role, Role::User);
    let a = Message::assistant("hello");
    assert_eq!(a.role, Role::Assistant);
    let s = Message::system("be nice");
    assert_eq!(s.role, Role::System);
}

#[test]
fn message_with_metadata_and_tokens() {
    let m = Message::user("hi")
        .with_token_count(10)
        .with_metadata("model", "gpt-4")
        .with_metadata("temperature", 0.5);
    assert_eq!(m.token_count, Some(10));
    assert_eq!(m.metadata.len(), 2);
    assert_eq!(m.metadata.get("model").unwrap(), "gpt-4");
}

#[test]
fn role_display_is_lowercase() {
    assert_eq!(Role::System.to_string(), "system");
    assert_eq!(Role::User.to_string(), "user");
    assert_eq!(Role::Assistant.to_string(), "assistant");
    assert_eq!(Role::Tool.to_string(), "tool");
}

#[test]
fn memory_tier_display() {
    assert_eq!(MemoryTier::Working.to_string(), "working");
    assert_eq!(MemoryTier::ShortTerm.to_string(), "short_term");
    assert_eq!(MemoryTier::LongTerm.to_string(), "long_term");
}

// -----------------------------------------------------------------------------
// Operations
// -----------------------------------------------------------------------------

#[test]
fn operation_type_display_includes_name() {
    let t = OperationType::ToolCall {
        name: "search".into(),
    };
    assert_eq!(t.to_string(), "tool_call:search");
    assert_eq!(OperationType::WebSearch.to_string(), "web_search");
    assert_eq!(
        OperationType::Custom("xyz".into()).to_string(),
        "custom:xyz"
    );
}

#[test]
fn operation_builders_attach_output_and_tokens() {
    let op = Operation::tool_call("read", serde_json::json!({"path": "/x"}), 5)
        .with_output(serde_json::json!({"ok": true}))
        .with_tokens(15);
    assert!(op.output.is_some());
    assert_eq!(op.tokens_used, Some(15));
}

#[test]
fn operation_query_default_is_empty() {
    let q = OperationQuery::default();
    assert!(q.op_types.is_empty());
    assert_eq!(q.limit, 0);
}

#[test]
fn operation_serialization_round_trip() {
    let op = Operation::new(
        OperationType::KnowledgeGraphQuery,
        serde_json::json!({"q": 1}),
        12,
    )
    .with_tokens(7);
    let s = serde_json::to_string(&op).unwrap();
    let back: Operation = serde_json::from_str(&s).unwrap();
    assert_eq!(back.id, op.id);
    assert_eq!(back.duration_ms, 12);
    assert_eq!(back.tokens_used, Some(7));
}

// -----------------------------------------------------------------------------
// Token counters
// -----------------------------------------------------------------------------

#[test]
fn simple_token_counter_counts_whitespace_separated() {
    let c = SimpleTokenCounter;
    assert_eq!(c.count(""), 0);
    assert_eq!(c.count("one"), 1);
    assert_eq!(c.count("a b c"), 3);
}

#[test]
fn configured_token_counter_default_includes_overhead() {
    let c = ConfiguredTokenCounter::default();
    let n = c.count("a b c d");
    // 4 words * 1.3 = 5.2 truncated to 5, + 3 overhead = 8.
    assert_eq!(n, 8);
}

#[test]
fn configured_token_counter_with_ratio_is_configurable() {
    let c = ConfiguredTokenCounter::with_ratio(2.0, 0);
    assert_eq!(c.count("one two"), 4);
}

#[test]
fn token_counter_count_memory_default_uses_content() {
    let item = MemoryItem::new("hello world from memst", "uat");
    let c = SimpleTokenCounter;
    assert_eq!(c.count_memory(&item), 4);
}

// -----------------------------------------------------------------------------
// Lifecycle: edge cases
// -----------------------------------------------------------------------------

#[test]
fn lifecycle_register_long_term_starts_in_long_term_state() {
    let mut lc = MemoryLifecycle::new(LifecycleConfig::default());
    assert_eq!(lc.register("m", MemoryTier::LongTerm), MemoryState::LongTerm);
    assert_eq!(lc.get_state("m"), Some(MemoryState::LongTerm));
}

#[test]
fn lifecycle_should_not_promote_below_threshold() {
    let lc = MemoryLifecycle::new(LifecycleConfig::default());
    let item = MemoryItem::new("x", "uat").with_confidence(0.1);
    assert!(lc.should_promote(&item).is_none());
}

#[test]
fn lifecycle_should_not_demote_long_term_by_ttl() {
    let lc = MemoryLifecycle::new(LifecycleConfig::default());
    let mut item = MemoryItem::new("x", "uat");
    item.last_accessed = Utc::now() - Duration::days(365);
    assert!(lc.should_demote(&item, MemoryTier::LongTerm).is_none());
}

#[test]
fn lifecycle_finalize_unknown_checkpoint_errors() {
    let mut lc = MemoryLifecycle::new(LifecycleConfig::default());
    let result = lc.finalize_checkpoint("does-not-exist", ObjectId::nil(), 0);
    assert!(result.is_err());
}

#[test]
fn lifecycle_default_config_values() {
    let cfg = LifecycleConfig::default();
    assert_eq!(cfg.working_memory_max_tokens, 4096);
    assert_eq!(cfg.short_term_max_tokens, 16_384);
    assert_eq!(cfg.long_term_max_tokens, 1_048_576);
    assert_eq!(cfg.promotion_access_threshold, 3);
}

#[test]
fn lifecycle_calculate_token_usage_groups_by_state() {
    let mut lc = MemoryLifecycle::new(LifecycleConfig::default());
    let working = MemoryItem::new("w", "uat").with_token_estimate(100);
    let short = MemoryItem::new("s", "uat").with_token_estimate(200);
    let long = MemoryItem::new("l", "uat").with_token_estimate(400);
    lc.register(&working.id.to_string(), MemoryTier::Working);
    lc.register(&short.id.to_string(), MemoryTier::ShortTerm);
    lc.register(&long.id.to_string(), MemoryTier::LongTerm);

    let usage = lc.calculate_token_usage(&[working, short, long]);
    assert_eq!(usage.get(&MemoryTier::Working), Some(&100));
    assert_eq!(usage.get(&MemoryTier::ShortTerm), Some(&200));
    assert_eq!(usage.get(&MemoryTier::LongTerm), Some(&400));
}

// -----------------------------------------------------------------------------
// SessionMetadata helpers
// -----------------------------------------------------------------------------

#[test]
fn session_metadata_with_tag_appends() {
    let m = SessionMetadata::new("s", "gpt-4")
        .with_tag("a")
        .with_tag("b");
    assert_eq!(m.tags, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn session_metadata_with_tags_replaces() {
    let m = SessionMetadata::new("s", "gpt-4")
        .with_tag("old")
        .with_tags(["x", "y"]);
    assert_eq!(m.tags, vec!["x".to_string(), "y".to_string()]);
}

// -----------------------------------------------------------------------------
// MemoryQuery builder
// -----------------------------------------------------------------------------

#[test]
fn memory_query_builder_sets_fields() {
    let q = MemoryQuery::new()
        .with_keyword("foo")
        .with_keyword("bar")
        .with_tag("t1")
        .with_min_confidence(0.5)
        .with_limit(7);
    assert_eq!(q.keywords, vec!["foo", "bar"]);
    assert_eq!(q.tags, vec!["t1"]);
    assert_eq!(q.min_confidence, Some(0.5));
    assert_eq!(q.limit, 7);
}

// -----------------------------------------------------------------------------
// Object store: error path
// -----------------------------------------------------------------------------

#[test]
fn object_store_read_missing_object_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let os = ObjectStore::new(&dir.path().to_path_buf()).unwrap();
    let unknown = ObjectId::from_content(b"never written");
    let err = os.read_blob(&unknown).err().expect("expected error");
    let msg = err.to_string();
    assert!(msg.contains("Object not found"), "msg={msg}");
}

#[test]
fn object_store_blob_to_text_handles_utf8_and_binary() {
    let dir = TempDir::new().unwrap();
    let mut os = ObjectStore::new(&dir.path().to_path_buf()).unwrap();
    let text_oid = os.write_blob(&Blob::new(b"hello")).unwrap();
    let text = os.read_blob(&text_oid).unwrap();
    assert_eq!(text.to_text(), Some("hello"));

    let bin_oid = os.write_blob(&Blob::new(&[0xff, 0xfe, 0x00, 0x80])).unwrap();
    let bin = os.read_blob(&bin_oid).unwrap();
    assert!(bin.to_text().is_none());
    assert_eq!(bin.size(), 4);
}

// -----------------------------------------------------------------------------
// Sanity: TransitionTrigger pattern matching
// -----------------------------------------------------------------------------

#[test]
fn transition_trigger_variants_round_trip_via_serde() {
    for trig in [
        TransitionTrigger::TokenBudgetExceeded,
        TransitionTrigger::TtlExpired,
        TransitionTrigger::AccessCount(7),
        TransitionTrigger::ImportanceThreshold(0.9),
        TransitionTrigger::Manual,
        TransitionTrigger::SleepConsolidation,
        TransitionTrigger::ConflictDetected,
    ] {
        let s = serde_json::to_string(&trig).unwrap();
        let back: TransitionTrigger = serde_json::from_str(&s).unwrap();
        // pattern equality (the type doesn't derive PartialEq).
        match (trig, back) {
            (TransitionTrigger::TokenBudgetExceeded, TransitionTrigger::TokenBudgetExceeded) => {}
            (TransitionTrigger::TtlExpired, TransitionTrigger::TtlExpired) => {}
            (TransitionTrigger::AccessCount(a), TransitionTrigger::AccessCount(b)) => {
                assert_eq!(a, b);
            }
            (
                TransitionTrigger::ImportanceThreshold(a),
                TransitionTrigger::ImportanceThreshold(b),
            ) => {
                assert!((a - b).abs() < 1e-6);
            }
            (TransitionTrigger::Manual, TransitionTrigger::Manual) => {}
            (TransitionTrigger::SleepConsolidation, TransitionTrigger::SleepConsolidation) => {}
            (TransitionTrigger::ConflictDetected, TransitionTrigger::ConflictDetected) => {}
            (a, b) => panic!("variant mismatch: {:?} vs {:?}", a, b),
        }
    }
}
