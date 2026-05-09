//! Core data types for MemSt session and memory management.
//!
//! This is a focused port of `memst-core/src/types/mod.rs` covering only the
//! types required for the [`super::memory`] and [`super::store`] modules:
//! messages, sessions, operations, and memory items. Knowledge-graph types
//! (entities/relationships) and KG-evolution types live in upstream `memst`
//! and are not part of the HelixDB integration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Represents the role of a message in a conversation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    /// System prompt that defines the assistant's behavior
    System,
    /// User's input message
    User,
    /// Assistant's response message
    Assistant,
    /// Message from a tool execution
    Tool,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::System => write!(f, "system"),
            Role::User => write!(f, "user"),
            Role::Assistant => write!(f, "assistant"),
            Role::Tool => write!(f, "tool"),
        }
    }
}

/// Content part types for multipart messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentPart {
    /// Plain text content
    Text(String),
    /// Image content with data and mime type
    Image {
        /// Image binary data
        data: Vec<u8>,
        /// MIME type (e.g., "image/png")
        mime_type: String,
    },
    /// File reference
    File {
        /// Content hash of the file
        hash: String,
        /// Original filename
        filename: String,
    },
}

/// Message content - either plain text or multipart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Content {
    /// Plain text message
    Text(String),
    /// Multipart message with multiple content parts
    MultiPart(Vec<ContentPart>),
}

impl Default for Content {
    fn default() -> Self {
        Content::Text(String::new())
    }
}

impl From<String> for Content {
    fn from(s: String) -> Self {
        Content::Text(s)
    }
}

impl From<&str> for Content {
    fn from(s: &str) -> Self {
        Content::Text(s.to_string())
    }
}

/// A single message in a conversation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    /// Unique message identifier
    pub id: Uuid,
    /// Role of the message sender
    pub role: Role,
    /// Message content
    pub content: Content,
    /// Timestamp when the message was created
    pub timestamp: DateTime<Utc>,
    /// Optional metadata (model name, token usage, etc.)
    pub metadata: HashMap<String, serde_json::Value>,
    /// Optional token count for this message
    pub token_count: Option<u32>,
}

impl Message {
    /// Create a new message with the given role and content.
    pub fn new(role: Role, content: impl Into<Content>) -> Self {
        Self {
            id: Uuid::new_v4(),
            role,
            content: content.into(),
            timestamp: Utc::now(),
            metadata: HashMap::new(),
            token_count: None,
        }
    }

    /// Create a user message.
    pub fn user(content: impl Into<Content>) -> Self {
        Self::new(Role::User, content)
    }

    /// Create an assistant message.
    pub fn assistant(content: impl Into<Content>) -> Self {
        Self::new(Role::Assistant, content)
    }

    /// Create a system message.
    pub fn system(content: impl Into<Content>) -> Self {
        Self::new(Role::System, content)
    }

    /// Set the token count for this message.
    pub fn with_token_count(mut self, count: u32) -> Self {
        self.token_count = Some(count);
        self
    }

    /// Add metadata to this message.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
        self.metadata
            .insert(key.into(), serde_json::to_value(value).unwrap());
        self
    }
}

impl Default for Message {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            role: Role::User,
            content: Content::Text(String::new()),
            timestamp: Utc::now(),
            metadata: HashMap::new(),
            token_count: None,
        }
    }
}

/// Session metadata stored in `metadata.json`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionMetadata {
    /// Human-readable session name
    pub name: String,
    /// Model used for this session
    pub model: String,
    /// Tags for categorization
    pub tags: Vec<String>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last activity timestamp
    pub last_activity: DateTime<Utc>,
    /// Total message count
    pub message_count: u32,
    /// Total token count
    pub token_count: u64,
}

impl SessionMetadata {
    /// Create new session metadata with default values.
    pub fn new(name: impl Into<String>, model: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            name: name.into(),
            model: model.into(),
            tags: Vec::new(),
            created_at: now,
            last_activity: now,
            message_count: 0,
            token_count: 0,
        }
    }

    /// Add a tag to this session.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set multiple tags on this session.
    pub fn with_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags = tags.into_iter().map(|t| t.into()).collect();
        self
    }
}

/// Summary information for session listing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSummary {
    /// Session ID
    pub id: Uuid,
    /// Human-readable name
    pub name: String,
    /// Model name
    pub model: String,
    /// Tags
    pub tags: Vec<String>,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last activity timestamp
    pub last_activity: DateTime<Utc>,
    /// Message count
    pub message_count: u32,
}

/// Global store manifest containing the session index.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Store version
    pub version: String,
    /// Schema version
    pub schema_version: String,
    /// Last compaction timestamp
    pub last_compaction: Option<DateTime<Utc>>,
    /// Session index: session_id -> session summary
    pub sessions: indexmap::IndexMap<Uuid, SessionSummary>,
}

impl Manifest {
    /// Create a new manifest.
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            schema_version: "1.0.0".to_string(),
            last_compaction: None,
            sessions: indexmap::IndexMap::new(),
        }
    }

    /// Insert or update a session summary.
    pub fn upsert_session(&mut self, summary: SessionSummary) {
        self.sessions.insert(summary.id, summary);
    }

    /// Remove a session from the index.
    pub fn remove_session(&mut self, id: &Uuid) {
        self.sessions.swap_remove(id);
    }

    /// Look up a session summary by id.
    pub fn get_session(&self, id: &Uuid) -> Option<&SessionSummary> {
        self.sessions.get(id)
    }
}

/// Types of operations that can be logged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    /// A tool call (web_search, file_read, etc.)
    ToolCall {
        /// Name of the tool
        name: String,
    },
    /// A function call
    FunctionCall {
        /// Name of the function
        name: String,
    },
    /// Web search operation
    WebSearch,
    /// Reasoning/thinking step
    ThinkingStep,
    /// Memory retrieval operation
    MemoryRetrieval,
    /// Knowledge graph query
    KnowledgeGraphQuery,
    /// Custom operation type
    Custom(String),
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::ToolCall { name } => write!(f, "tool_call:{}", name),
            OperationType::FunctionCall { name } => write!(f, "function_call:{}", name),
            OperationType::WebSearch => write!(f, "web_search"),
            OperationType::ThinkingStep => write!(f, "thinking_step"),
            OperationType::MemoryRetrieval => write!(f, "memory_retrieval"),
            OperationType::KnowledgeGraphQuery => write!(f, "knowledge_graph_query"),
            OperationType::Custom(name) => write!(f, "custom:{}", name),
        }
    }
}

/// An operation log entry for tracking tool calls, searches, etc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    /// Unique operation ID
    pub id: Uuid,
    /// Timestamp of the operation
    pub timestamp: DateTime<Utc>,
    /// Type of operation
    pub op_type: OperationType,
    /// Input parameters (JSON)
    pub input: serde_json::Value,
    /// Output result (JSON)
    pub output: Option<serde_json::Value>,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Token usage (optional)
    pub tokens_used: Option<u32>,
}

impl Operation {
    /// Create a new operation entry.
    pub fn new(op_type: OperationType, input: serde_json::Value, duration_ms: u64) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            op_type,
            input,
            output: None,
            duration_ms,
            tokens_used: None,
        }
    }

    /// Helper to construct a tool-call operation.
    pub fn tool_call(name: impl Into<String>, input: serde_json::Value, duration_ms: u64) -> Self {
        Self::new(
            OperationType::ToolCall { name: name.into() },
            input,
            duration_ms,
        )
    }

    /// Attach output payload.
    pub fn with_output(mut self, output: serde_json::Value) -> Self {
        self.output = Some(output);
        self
    }

    /// Attach token usage information.
    pub fn with_tokens(mut self, tokens: u32) -> Self {
        self.tokens_used = Some(tokens);
        self
    }
}

/// Query options for filtering operations.
#[derive(Debug, Clone, Default)]
pub struct OperationQuery {
    /// Filter by operation types (empty = all)
    pub op_types: Vec<OperationType>,
    /// Start time filter (inclusive)
    pub from: Option<DateTime<Utc>>,
    /// End time filter (inclusive)
    pub to: Option<DateTime<Utc>>,
    /// Maximum results (0 = unlimited)
    pub limit: usize,
}

impl OperationQuery {
    /// Create an empty query.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by operation type.
    pub fn by_type(mut self, op_type: OperationType) -> Self {
        self.op_types.push(op_type);
        self
    }

    /// Restrict to a time range.
    pub fn with_time_range(mut self, from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        self.from = Some(from);
        self.to = Some(to);
        self
    }

    /// Cap maximum results.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// Memory categories following MIRIX taxonomy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryType {
    /// "What happened" - conversation turns, event summaries
    Episodic,
    /// "What is true" - facts, preferences, world knowledge
    Semantic,
    /// "How to do X" - learned workflows, tool usage patterns
    Procedural,
    /// File paths, URLs, external references
    Resource,
    /// Agent's beliefs about its own knowledge quality
    MetaCognitive,
}

/// Memory tier levels for automatic promotion/demotion.
///
/// This is the *different levels of memory management* surface area requested
/// when integrating MemSt into HelixDB:
/// - [`MemoryTier::Working`] - hot, in-context memories.
/// - [`MemoryTier::ShortTerm`] - retained for moderate periods.
/// - [`MemoryTier::LongTerm`] - persistent important memories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MemoryTier {
    /// Working memory - most recent / frequently accessed.
    Working,
    /// Short-term memory - retained for moderate periods.
    ShortTerm,
    /// Long-term memory - persistent important memories.
    LongTerm,
}

impl std::fmt::Display for MemoryTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryTier::Working => write!(f, "working"),
            MemoryTier::ShortTerm => write!(f, "short_term"),
            MemoryTier::LongTerm => write!(f, "long_term"),
        }
    }
}

/// A memory item stored in memory tiers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryItem {
    /// Unique memory ID
    pub id: Uuid,
    /// Memory content
    pub content: String,
    /// Source (e.g., message_id, extraction)
    pub source: String,
    /// When the memory was created
    pub created_at: DateTime<Utc>,
    /// When the memory was last accessed
    pub last_accessed: DateTime<Utc>,
    /// Number of times this memory was accessed
    pub access_count: u32,
    /// Optional embedding vector for similarity search
    pub embedding: Option<Vec<f32>>,
    /// Tags for categorization
    pub tags: Vec<String>,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    /// Importance score (derived from access count and confidence)
    pub importance: f32,
    /// Memory type
    pub memory_type: MemoryType,
    /// Token estimate
    pub token_estimate: Option<u32>,
    /// Superseded memory ID (if this memory replaces another)
    pub supersedes: Option<Uuid>,
}

impl MemoryItem {
    /// Create a new memory item.
    pub fn new(content: impl Into<String>, source: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            content: content.into(),
            source: source.into(),
            created_at: now,
            last_accessed: now,
            access_count: 0,
            embedding: None,
            tags: Vec::new(),
            confidence: 1.0,
            importance: 0.0,
            memory_type: MemoryType::Semantic,
            token_estimate: None,
            supersedes: None,
        }
    }

    /// Add a tag.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set tags.
    pub fn with_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tags = tags.into_iter().map(|t| t.into()).collect();
        self
    }

    /// Set the confidence score, clamped into [0, 1].
    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = confidence.clamp(0.0, 1.0);
        self
    }

    /// Attach an embedding vector.
    pub fn with_embedding(mut self, embedding: Vec<f32>) -> Self {
        self.embedding = Some(embedding);
        self
    }

    /// Override the memory type.
    pub fn with_memory_type(mut self, memory_type: MemoryType) -> Self {
        self.memory_type = memory_type;
        self
    }

    /// Set a token estimate.
    pub fn with_token_estimate(mut self, tokens: u32) -> Self {
        self.token_estimate = Some(tokens);
        self
    }

    /// Record an access (bumps `access_count`, updates recency, recomputes importance).
    pub fn record_access(&mut self) {
        self.access_count += 1;
        self.last_accessed = Utc::now();
        self.recalculate_importance();
    }

    fn recalculate_importance(&mut self) {
        let access_factor = (self.access_count as f32 + 1.0).ln();
        let recency_factor =
            (Utc::now() - self.last_accessed).num_seconds() as f32 / 86400.0; // days
        self.importance = 0.5 * self.confidence
            + 0.3 * access_factor.min(3.0) / 3.0
            + 0.2 * (-recency_factor).exp();
    }
}

/// Query for memory retrieval.
#[derive(Debug, Clone, Default)]
pub struct MemoryQuery {
    /// Keywords to search for (case-insensitive substring match)
    pub keywords: Vec<String>,
    /// Required tags
    pub tags: Vec<String>,
    /// Minimum confidence
    pub min_confidence: Option<f32>,
    /// Maximum results (0 = unlimited)
    pub limit: usize,
    /// Include embeddings in returned results
    pub include_embeddings: bool,
}

impl MemoryQuery {
    /// Create an empty query.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a keyword filter.
    pub fn with_keyword(mut self, keyword: impl Into<String>) -> Self {
        self.keywords.push(keyword.into());
        self
    }

    /// Add a tag filter.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Set minimum confidence.
    pub fn with_min_confidence(mut self, confidence: f32) -> Self {
        self.min_confidence = Some(confidence);
        self
    }

    /// Cap maximum results.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

/// Convenience alias for memory IDs.
pub type MemoryId = Uuid;
/// Convenience alias for session IDs.
pub type SessionId = Uuid;
