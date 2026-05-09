//! File and session management.
//!
//! Port of the search-independent core of `memst-core/src/store/mod.rs`. The
//! upstream module has both a path-based session/messages/memory layout and a
//! search-index integration; here we keep only the layout and file APIs since
//! HelixDB already provides its own indexing/search via its query engine.
//!
//! On-disk layout managed by [`SessionStore`]:
//!
//! ```text
//! <base>/
//!   manifest.json
//!   schema_version
//!   store.lock
//!   sessions/
//!     <session-uuid>/
//!       metadata.json
//!       messages.bin
//!       messages.idx
//!       operations.log
//!       memory/
//!         working.bin
//!         short.bin
//!         long.bin
//!       extractions/
//!   attachments/
//! ```

use bincode::Options;
use chrono::Utc;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::error::{Error, Result};
use super::types::{
    Manifest, MemoryItem, MemoryQuery, MemoryTier, Message, Operation, OperationQuery,
    OperationType, Role, SessionMetadata, SessionSummary,
};

const SCHEMA_VERSION: &str = "1.0.0";

/// Index entry for message lookup.
///
/// Each entry corresponds to a line in `messages.idx` and points at a record
/// inside `messages.bin`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageIndexEntry {
    /// Message UUID
    pub message_id: Uuid,
    /// Byte offset in `messages.bin`
    pub byte_offset: u64,
    /// Byte length of the message record
    pub byte_length: u64,
    /// Timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Role for quick filtering
    pub role: Role,
}

impl MessageIndexEntry {
    /// Create a new index entry.
    pub fn new(
        message_id: Uuid,
        byte_offset: u64,
        byte_length: u64,
        timestamp: chrono::DateTime<chrono::Utc>,
        role: Role,
    ) -> Self {
        Self {
            message_id,
            byte_offset,
            byte_length,
            timestamp,
            role,
        }
    }
}

/// Session storage with the file-based layout described in the module docs.
///
/// Holds an exclusive file lock on `store.lock` for the lifetime of the
/// instance, ensuring only one process mutates a given store at a time.
pub struct SessionStore {
    base_path: PathBuf,
    manifest_path: PathBuf,
    sessions_dir: PathBuf,
    attachments_dir: PathBuf,
    _lock_file: File,
}

impl SessionStore {
    /// Initialize a fresh session store at `base_path`.
    pub fn init(base_path: &Path) -> Result<Self> {
        fs::create_dir_all(base_path)?;
        fs::create_dir_all(base_path.join("sessions"))?;
        fs::create_dir_all(base_path.join("attachments"))?;

        fs::write(base_path.join("schema_version"), SCHEMA_VERSION)?;

        let manifest = Manifest::new();
        let manifest_path = base_path.join("manifest.json");
        fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)?;

        let lock_path = base_path.join("store.lock");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        // Use UFCS so this resolves to fs2's trait method even on Rust >=1.89
        // where `std::fs::File::lock_exclusive` is also stable.
        FileExt::lock_exclusive(&lock_file)
            .map_err(|e| Error::LockError(e.to_string()))?;

        Ok(Self {
            base_path: base_path.to_path_buf(),
            manifest_path,
            sessions_dir: base_path.join("sessions"),
            attachments_dir: base_path.join("attachments"),
            _lock_file: lock_file,
        })
    }

    /// Open an existing session store.
    pub fn open(base_path: &Path) -> Result<Self> {
        let schema_version = fs::read_to_string(base_path.join("schema_version"))?;
        if schema_version.trim() != SCHEMA_VERSION {
            return Err(Error::VersionMismatch {
                expected: SCHEMA_VERSION.to_string(),
                found: schema_version,
            });
        }

        let lock_path = base_path.join("store.lock");
        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&lock_path)?;
        FileExt::lock_exclusive(&lock_file)
            .map_err(|e| Error::LockError(e.to_string()))?;

        Ok(Self {
            base_path: base_path.to_path_buf(),
            manifest_path: base_path.join("manifest.json"),
            sessions_dir: base_path.join("sessions"),
            attachments_dir: base_path.join("attachments"),
            _lock_file: lock_file,
        })
    }

    /// The store's base directory.
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// Directory housing attachments.
    pub fn attachments_dir(&self) -> &Path {
        &self.attachments_dir
    }

    /// Path of a session directory (does not check existence).
    pub fn session_path(&self, session_id: Uuid) -> PathBuf {
        self.sessions_dir.join(session_id.to_string())
    }

    fn read_manifest(&self) -> Result<Manifest> {
        let content = fs::read_to_string(&self.manifest_path)?;
        Ok(serde_json::from_str(&content)?)
    }

    fn write_manifest(&self, manifest: &Manifest) -> Result<()> {
        fs::write(&self.manifest_path, serde_json::to_string_pretty(manifest)?)?;
        Ok(())
    }

    /// Create a new session and return its id.
    pub fn create_session(&self, metadata: SessionMetadata) -> Result<Uuid> {
        let session_id = Uuid::new_v4();
        let session_path = self.session_path(session_id);

        fs::create_dir_all(&session_path)?;
        fs::write(
            session_path.join("metadata.json"),
            serde_json::to_string_pretty(&metadata)?,
        )?;
        fs::write(session_path.join("messages.bin"), "")?;
        fs::write(
            session_path.join("messages.idx"),
            "# message_id byte_offset byte_length timestamp role\n",
        )?;
        fs::write(session_path.join("operations.log"), "")?;
        fs::create_dir_all(session_path.join("memory"))?;
        fs::create_dir_all(session_path.join("extractions"))?;

        let mut manifest = self.read_manifest()?;
        manifest.upsert_session(SessionSummary {
            id: session_id,
            name: metadata.name.clone(),
            model: metadata.model.clone(),
            tags: metadata.tags.clone(),
            created_at: metadata.created_at,
            last_activity: metadata.last_activity,
            message_count: 0,
        });
        self.write_manifest(&manifest)?;

        Ok(session_id)
    }

    /// Load metadata for a session.
    pub fn get_session(&self, session_id: Uuid) -> Result<Option<SessionMetadata>> {
        let metadata_path = self.session_path(session_id).join("metadata.json");
        if !metadata_path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&metadata_path)?;
        Ok(Some(serde_json::from_str(&content)?))
    }

    /// List every known session summary.
    pub fn list_sessions(&self) -> Result<Vec<SessionSummary>> {
        let manifest = self.read_manifest()?;
        Ok(manifest.sessions.values().cloned().collect())
    }

    /// Delete a session and remove it from the manifest.
    pub fn delete_session(&self, session_id: Uuid) -> Result<()> {
        let session_path = self.session_path(session_id);
        if !session_path.exists() {
            return Err(Error::SessionNotFound(session_id));
        }
        fs::remove_dir_all(&session_path)?;
        let mut manifest = self.read_manifest()?;
        manifest.remove_session(&session_id);
        self.write_manifest(&manifest)?;
        Ok(())
    }

    /// Append a message to a session, updating both the binary log and the
    /// text index.
    pub fn append_message(&self, session_id: Uuid, message: Message) -> Result<()> {
        let session_path = self.session_path(session_id);
        let messages_path = session_path.join("messages.bin");
        let index_path = session_path.join("messages.idx");

        let byte_offset = fs::metadata(&messages_path)?.len();

        let options = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes();
        let encoded = options.serialize(&message)?;

        let mut file = OpenOptions::new().append(true).open(&messages_path)?;
        file.write_all(&encoded)?;
        file.flush()?;
        drop(file);

        let index_entry = format!(
            "{} {} {} {} {}\n",
            message.id,
            byte_offset,
            encoded.len(),
            message.timestamp.format("%Y-%m-%dT%H:%M:%SZ"),
            message.role
        );
        let mut index_file = OpenOptions::new().append(true).open(&index_path)?;
        index_file.write_all(index_entry.as_bytes())?;
        index_file.flush()?;
        drop(index_file);

        let mut metadata = self
            .get_session(session_id)?
            .ok_or(Error::SessionNotFound(session_id))?;
        metadata.message_count += 1;
        metadata.last_activity = Utc::now();
        if let Some(count) = message.token_count {
            metadata.token_count += count as u64;
        }
        fs::write(
            session_path.join("metadata.json"),
            serde_json::to_string_pretty(&metadata)?,
        )?;

        let mut manifest = self.read_manifest()?;
        if let Some(summary) = manifest.sessions.get_mut(&session_id) {
            summary.message_count = metadata.message_count;
            summary.last_activity = metadata.last_activity;
        }
        self.write_manifest(&manifest)?;

        Ok(())
    }

    /// Read every message in a session (in append order).
    pub fn get_messages(&self, session_id: Uuid) -> Result<Vec<Message>> {
        let messages_path = self.session_path(session_id).join("messages.bin");
        if !messages_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&messages_path)?;
        let mut reader = BufReader::new(file);
        let options = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes();

        let mut messages = Vec::new();
        loop {
            match options.deserialize_from(&mut reader) {
                Ok(msg) => messages.push(msg),
                Err(e) => {
                    let err_kind: &bincode::ErrorKind = Box::as_ref(&e);
                    if let bincode::ErrorKind::Io(io_err) = err_kind {
                        if io_err.kind() == io::ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(Error::from(e));
                }
            }
        }
        Ok(messages)
    }

    /// Read messages in the half-open range `[start, end)`.
    pub fn get_messages_range(
        &self,
        session_id: Uuid,
        start: usize,
        end: usize,
    ) -> Result<Vec<Message>> {
        let all = self.get_messages(session_id)?;
        Ok(all
            .into_iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .collect())
    }

    /// Read the message index for a session.
    pub fn read_message_index(&self, session_id: Uuid) -> Result<Vec<MessageIndexEntry>> {
        let index_path = self.session_path(session_id).join("messages.idx");
        if !index_path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&index_path)?;
        let mut entries = Vec::new();

        for line in content.lines().skip(1) {
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 5 {
                let entry = MessageIndexEntry {
                    message_id: Uuid::parse_str(parts[0])?,
                    byte_offset: parts[1].parse::<u64>()?,
                    byte_length: parts[2].parse::<u64>()?,
                    timestamp: chrono::DateTime::parse_from_rfc3339(parts[3])
                        .map(|d| d.with_timezone(&chrono::Utc))
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    role: match parts[4] {
                        "system" => Role::System,
                        "user" => Role::User,
                        "assistant" => Role::Assistant,
                        "tool" => Role::Tool,
                        _ => Role::User,
                    },
                };
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Append an operation entry to `operations.log`.
    pub fn append_operation(&self, session_id: Uuid, operation: Operation) -> Result<()> {
        let ops_path = self.session_path(session_id).join("operations.log");
        let json_line = serde_json::to_string(&operation)?;
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&ops_path)?
            .write_all(format!("{}\n", json_line).as_bytes())?;
        Ok(())
    }

    /// Read every operation logged for a session.
    pub fn get_operations(&self, session_id: Uuid) -> Result<Vec<Operation>> {
        let ops_path = self.session_path(session_id).join("operations.log");
        if !ops_path.exists() {
            return Ok(Vec::new());
        }
        let content = fs::read_to_string(&ops_path)?;
        let mut operations = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            operations.push(serde_json::from_str(line)?);
        }
        Ok(operations)
    }

    /// Filter operations using `query`.
    pub fn query_operations(
        &self,
        session_id: Uuid,
        query: OperationQuery,
    ) -> Result<Vec<Operation>> {
        let mut filtered: Vec<_> = self
            .get_operations(session_id)?
            .into_iter()
            .filter(|op| {
                if !query.op_types.is_empty() && !query.op_types.contains(&op.op_type) {
                    return false;
                }
                if let Some(from) = query.from {
                    if op.timestamp < from {
                        return false;
                    }
                }
                if let Some(to) = query.to {
                    if op.timestamp > to {
                        return false;
                    }
                }
                true
            })
            .collect();

        if query.limit > 0 && filtered.len() > query.limit {
            filtered.truncate(query.limit);
        }
        Ok(filtered)
    }

    /// Convenience helper - filter operations by type.
    pub fn get_operations_by_type(
        &self,
        session_id: Uuid,
        op_type: OperationType,
    ) -> Result<Vec<Operation>> {
        self.query_operations(session_id, OperationQuery::new().by_type(op_type))
    }

    fn memory_tier_path(&self, session_id: Uuid, tier: MemoryTier) -> PathBuf {
        let tier_name = match tier {
            MemoryTier::Working => "working.bin",
            MemoryTier::ShortTerm => "short.bin",
            MemoryTier::LongTerm => "long.bin",
        };
        self.session_path(session_id).join("memory").join(tier_name)
    }

    /// Add a memory item to a tier file.
    pub fn add_memory(
        &self,
        session_id: Uuid,
        tier: MemoryTier,
        item: MemoryItem,
    ) -> Result<MemoryItem> {
        let tier_path = self.memory_tier_path(session_id, tier);
        if let Some(parent) = tier_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut memories = self.load_memories(&tier_path)?;
        memories.push(item.clone());
        self.save_memories(&tier_path, &memories)?;
        Ok(item)
    }

    /// Load every memory in a tier file.
    pub fn get_tier(
        &self,
        session_id: Uuid,
        tier: MemoryTier,
    ) -> Result<Vec<MemoryItem>> {
        self.load_memories(&self.memory_tier_path(session_id, tier))
    }

    /// Find a memory by id, searching every tier.
    pub fn get_memory(
        &self,
        session_id: Uuid,
        memory_id: Uuid,
    ) -> Result<Option<MemoryItem>> {
        for tier in [MemoryTier::Working, MemoryTier::ShortTerm, MemoryTier::LongTerm] {
            if let Some(memory) = self
                .get_tier(session_id, tier)?
                .into_iter()
                .find(|m| m.id == memory_id)
            {
                return Ok(Some(memory));
            }
        }
        Ok(None)
    }

    /// Record an access to a memory and persist the updated counters.
    pub fn access_memory(&self, session_id: Uuid, memory_id: Uuid) -> Result<bool> {
        for tier in [MemoryTier::Working, MemoryTier::ShortTerm, MemoryTier::LongTerm] {
            let tier_path = self.memory_tier_path(session_id, tier);
            if !tier_path.exists() {
                continue;
            }
            let mut memories = self.load_memories(&tier_path)?;
            if let Some(pos) = memories.iter().position(|m| m.id == memory_id) {
                memories[pos].record_access();
                self.save_memories(&tier_path, &memories)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Retrieve memories matching `query`. Searches across all three tiers.
    pub fn retrieve_memories(
        &self,
        session_id: Uuid,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryItem>> {
        let mut results = Vec::new();
        for tier in [MemoryTier::Working, MemoryTier::ShortTerm, MemoryTier::LongTerm] {
            for memory in self.get_tier(session_id, tier)? {
                if !query.keywords.is_empty() {
                    let lower = memory.content.to_lowercase();
                    if !query.keywords.iter().all(|kw| lower.contains(&kw.to_lowercase())) {
                        continue;
                    }
                }
                if !query.tags.is_empty()
                    && !query.tags.iter().all(|tag| memory.tags.contains(tag))
                {
                    continue;
                }
                if let Some(min_conf) = query.min_confidence {
                    if memory.confidence < min_conf {
                        continue;
                    }
                }
                results.push(memory);
            }
        }
        results.sort_by(|a, b| {
            b.importance
                .partial_cmp(&a.importance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if query.limit > 0 && results.len() > query.limit {
            results.truncate(query.limit);
        }
        Ok(results)
    }

    /// Move a memory from its current tier into `to_tier`.
    pub fn promote_memory(
        &self,
        session_id: Uuid,
        memory_id: Uuid,
        to_tier: MemoryTier,
    ) -> Result<bool> {
        for tier in [MemoryTier::Working, MemoryTier::ShortTerm, MemoryTier::LongTerm] {
            let tier_path = self.memory_tier_path(session_id, tier);
            if !tier_path.exists() {
                continue;
            }
            let mut memories = self.load_memories(&tier_path)?;
            if let Some(pos) = memories.iter().position(|m| m.id == memory_id) {
                let memory = memories.remove(pos);
                self.save_memories(&tier_path, &memories)?;
                self.add_memory(session_id, to_tier, memory)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn load_memories(&self, path: &Path) -> Result<Vec<MemoryItem>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let options = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes();

        let mut memories = Vec::new();
        loop {
            match options.deserialize_from(&mut reader) {
                Ok(memory) => memories.push(memory),
                Err(e) => {
                    let err_kind: &bincode::ErrorKind = Box::as_ref(&e);
                    if let bincode::ErrorKind::Io(io_err) = err_kind {
                        if io_err.kind() == io::ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(Error::from(e));
                }
            }
        }
        Ok(memories)
    }

    fn save_memories(&self, path: &Path, memories: &[MemoryItem]) -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let mut writer = BufWriter::new(file);
        let options = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes();
        for memory in memories {
            options.serialize_into(&mut writer, memory)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memst::types::{Content, MemoryType};

    #[test]
    fn init_and_open_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        {
            let _store = SessionStore::init(dir.path()).unwrap();
        }
        let _store = SessionStore::open(dir.path()).unwrap();
    }

    #[test]
    fn create_and_list_sessions() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = SessionStore::init(dir.path()).unwrap();
        let id = store
            .create_session(SessionMetadata::new("s1", "gpt-4"))
            .unwrap();
        assert!(store.get_session(id).unwrap().is_some());
        assert_eq!(store.list_sessions().unwrap().len(), 1);
    }

    #[test]
    fn append_and_read_messages() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = SessionStore::init(dir.path()).unwrap();
        let id = store
            .create_session(SessionMetadata::new("s1", "gpt-4"))
            .unwrap();

        store
            .append_message(id, Message::new(Role::User, Content::Text("hi".into())))
            .unwrap();
        store
            .append_message(
                id,
                Message::new(Role::Assistant, Content::Text("hello".into())),
            )
            .unwrap();

        let msgs = store.get_messages(id).unwrap();
        assert_eq!(msgs.len(), 2);
        let idx = store.read_message_index(id).unwrap();
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn memory_tiers_and_retrieval() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = SessionStore::init(dir.path()).unwrap();
        let id = store
            .create_session(SessionMetadata::new("s1", "gpt-4"))
            .unwrap();

        let m = MemoryItem::new("user prefers tokio", "test")
            .with_memory_type(MemoryType::Semantic)
            .with_tag("rust");
        store.add_memory(id, MemoryTier::Working, m.clone()).unwrap();

        let q = MemoryQuery::new().with_keyword("tokio").with_tag("rust");
        let hits = store.retrieve_memories(id, q).unwrap();
        assert_eq!(hits.len(), 1);

        assert!(store.promote_memory(id, m.id, MemoryTier::LongTerm).unwrap());
        assert!(store.get_tier(id, MemoryTier::Working).unwrap().is_empty());
        assert_eq!(store.get_tier(id, MemoryTier::LongTerm).unwrap().len(), 1);
    }

    #[test]
    fn operations_log_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = SessionStore::init(dir.path()).unwrap();
        let id = store
            .create_session(SessionMetadata::new("s1", "gpt-4"))
            .unwrap();
        store
            .append_operation(
                id,
                Operation::tool_call("search", serde_json::json!({"q": "x"}), 50),
            )
            .unwrap();
        let ops = store.get_operations(id).unwrap();
        assert_eq!(ops.len(), 1);
    }
}
