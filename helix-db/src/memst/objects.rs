//! Content-addressable object storage (Git-like).
//!
//! Focused port of `memst-core/src/objects/mod.rs` covering the building blocks
//! used by HelixDB's MemSt integration:
//! - [`ObjectId`] - BLAKE3 content hashes.
//! - [`Blob`] / [`Tree`] / [`Commit`] / [`Tag`] - object kinds.
//! - [`Author`], [`CommitMetadata`], [`CommitSource`], [`MemoryScope`].
//! - [`ObjectStore`] - on-disk read/write of these objects.
//!
//! Skill, ContextFile, Entity and Relation upstream types are intentionally
//! omitted: they belong to the knowledge-graph slice of `memst-core`, which is
//! not part of the memory/session merge.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

use super::error::{Error, Result};

/// BLAKE3 content hash (32 bytes, 64 hex characters).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectId(pub [u8; 32]);

impl ObjectId {
    /// Create an `ObjectId` from raw bytes.
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self(*bytes)
    }

    /// Create an `ObjectId` from a hex string.
    pub fn from_hex(hex: &str) -> Result<Self> {
        let bytes = hex_decode(hex)?;
        Ok(Self::from_bytes(&bytes))
    }

    /// Compute an `ObjectId` from content using BLAKE3.
    pub fn from_content(content: &[u8]) -> Self {
        let hash = blake3::hash(content);
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(hash.as_bytes());
        Self(bytes)
    }

    /// Render as a 64-character lowercase hex string.
    pub fn to_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// Abbreviated hash (first 7 hex chars), as in `git log --oneline`.
    pub fn abbreviate(&self) -> String {
        self.to_hex()[..7].to_string()
    }

    /// Whether this is the zero/nil OID.
    pub fn is_nil(&self) -> bool {
        self.0.iter().all(|&b| b == 0)
    }

    /// The nil OID constant.
    pub const fn nil() -> Self {
        Self([0u8; 32])
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Object kinds in the content-addressable store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectType {
    /// Raw data (messages, memories)
    Blob,
    /// Directory structure
    Tree,
    /// Snapshot with metadata
    Commit,
    /// Annotated tag
    Tag,
}

impl ObjectType {
    fn as_str(&self) -> &'static str {
        match self {
            ObjectType::Blob => "blob",
            ObjectType::Tree => "tree",
            ObjectType::Commit => "commit",
            ObjectType::Tag => "tag",
        }
    }
}

impl std::str::FromStr for ObjectType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "blob" => Ok(ObjectType::Blob),
            "tree" => Ok(ObjectType::Tree),
            "commit" => Ok(ObjectType::Commit),
            "tag" => Ok(ObjectType::Tag),
            _ => Err(Error::InvalidObjectFormat),
        }
    }
}

/// On-disk header preceding object content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectHeader {
    /// Object kind
    pub object_type: ObjectType,
    /// Content size in bytes
    pub size: u64,
}

impl ObjectHeader {
    /// Serialize header and content together (`<type> <size>\0<content>`).
    pub fn serialize(&self, content: &[u8]) -> Result<Vec<u8>> {
        let header_str = format!("{} {}\0", self.object_type.as_str(), self.size);
        let mut result = Vec::with_capacity(header_str.len() + content.len());
        result.extend_from_slice(header_str.as_bytes());
        result.extend_from_slice(content);
        Ok(result)
    }

    /// Parse an object from on-disk bytes, returning the header and content slice.
    pub fn deserialize(data: &[u8]) -> Result<(Self, &[u8])> {
        let null_pos = data
            .iter()
            .position(|&b| b == 0)
            .ok_or(Error::InvalidObjectFormat)?;
        let header_str =
            std::str::from_utf8(&data[..null_pos]).map_err(|_| Error::InvalidObjectFormat)?;
        let parts: Vec<&str> = header_str.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return Err(Error::InvalidObjectFormat);
        }
        let object_type: ObjectType = parts[0].parse()?;
        let size = parts[1].parse().map_err(|_| Error::InvalidObjectFormat)?;
        Ok((Self { object_type, size }, &data[null_pos + 1..]))
    }
}

/// Blob object - raw content storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blob {
    /// Raw content bytes
    pub content: Vec<u8>,
}

impl Blob {
    /// Wrap a byte slice as a blob.
    pub fn new(content: &[u8]) -> Self {
        Self {
            content: content.to_vec(),
        }
    }

    /// Length in bytes.
    pub fn size(&self) -> u64 {
        self.content.len() as u64
    }

    /// Try to interpret content as UTF-8 text.
    pub fn to_text(&self) -> Option<&str> {
        std::str::from_utf8(&self.content).ok()
    }
}

/// Tree entry - a reference to a child object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeEntry {
    /// POSIX mode (100644 for file, 040000 for subtree)
    pub mode: u32,
    /// Object id of the child
    pub oid: ObjectId,
    /// Name of the entry
    pub name: String,
}

impl TreeEntry {
    /// Create a tree entry.
    pub fn new(mode: u32, oid: ObjectId, name: &str) -> Self {
        Self {
            mode,
            oid,
            name: name.to_string(),
        }
    }

    /// File mode for blobs.
    pub const MODE_FILE: u32 = 0o100644;
    /// Directory mode for subtrees.
    pub const MODE_DIR: u32 = 0o040000;
    /// Executable mode.
    pub const MODE_EXECUTABLE: u32 = 0o100755;
}

/// Tree object - directory structure.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tree {
    /// Entries in the tree
    pub entries: Vec<TreeEntry>,
}

impl Tree {
    /// Create a new empty tree.
    pub fn new() -> Self {
        Self::default()
    }

    /// Push an entry.
    pub fn add_entry(&mut self, entry: TreeEntry) {
        self.entries.push(entry);
    }

    /// Lookup an entry by name.
    pub fn get_entry(&self, name: &str) -> Option<&TreeEntry> {
        self.entries.iter().find(|e| e.name == name)
    }

    /// Remove an entry by name.
    pub fn remove_entry(&mut self, name: &str) -> Option<TreeEntry> {
        self.entries
            .iter()
            .position(|e| e.name == name)
            .map(|i| self.entries.remove(i))
    }
}

/// Author / committer information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Author {
    /// Human-readable name.
    pub name: String,
    /// Email address.
    pub email: String,
    /// Timestamp associated with the author/committer.
    pub timestamp: DateTime<Utc>,
}

impl Author {
    /// Create an author with `Utc::now()` timestamp.
    pub fn new(name: &str, email: &str) -> Self {
        Self {
            name: name.to_string(),
            email: email.to_string(),
            timestamp: Utc::now(),
        }
    }

    /// Create an author with an explicit timestamp.
    pub fn with_timestamp(name: &str, email: &str, timestamp: DateTime<Utc>) -> Self {
        Self {
            name: name.to_string(),
            email: email.to_string(),
            timestamp,
        }
    }
}

/// Commit source classification (where the commit originated).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitSource {
    /// Manually triggered by a user
    UserExplicit,
    /// Written inline by the agent
    AgentInline,
    /// Async consolidation job
    SleepConsolidation,
    /// Skill extracted from conversation
    SkillLearning,
    /// Imported from external format
    ImportExternal,
    /// Three-way semantic merge
    Merge,
}

/// Memory scope for multi-agent isolation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemoryScope {
    /// User-scoped memory
    User(String),
    /// Project-scoped memory
    Project(String),
    /// Organization-scoped memory
    Org(String),
    /// Session-scoped memory
    Session(String),
    /// Agent-scoped memory
    Agent(String),
}

/// Commit metadata extensions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitMetadata {
    /// Token budget change introduced by this commit
    pub token_delta: i32,
    /// Confidence score in `[0.0, 1.0]`
    pub confidence: f32,
    /// Source classification
    pub source: CommitSource,
    /// Optional scope of the memory
    pub scope: Option<MemoryScope>,
}

impl Default for CommitMetadata {
    fn default() -> Self {
        Self {
            token_delta: 0,
            confidence: 1.0,
            source: CommitSource::UserExplicit,
            scope: None,
        }
    }
}

/// Commit object - snapshot with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    /// Tree object id
    pub tree_oid: ObjectId,
    /// Parent commit ids
    pub parent_oids: Vec<ObjectId>,
    /// Author information
    pub author: Author,
    /// Committer information
    pub committer: Author,
    /// Commit message
    pub message: String,
    /// Optional signature (PGP/GPG)
    pub signature: Option<String>,
    /// MemSt metadata extensions
    pub metadata: CommitMetadata,
}

impl Commit {
    /// Create a new commit (committer defaults to the author).
    pub fn new(tree_oid: ObjectId, author: Author, message: &str) -> Self {
        Self {
            tree_oid,
            parent_oids: Vec::new(),
            author: author.clone(),
            committer: author,
            message: message.to_string(),
            signature: None,
            metadata: CommitMetadata::default(),
        }
    }

    /// Create a commit with explicit metadata.
    pub fn with_metadata(
        tree_oid: ObjectId,
        author: Author,
        message: &str,
        metadata: CommitMetadata,
    ) -> Self {
        Self {
            tree_oid,
            parent_oids: Vec::new(),
            author: author.clone(),
            committer: author,
            message: message.to_string(),
            signature: None,
            metadata,
        }
    }

    /// Add a parent commit.
    pub fn add_parent(&mut self, parent_oid: ObjectId) {
        self.parent_oids.push(parent_oid);
    }

    /// Whether this commit has more than one parent.
    pub fn is_merge(&self) -> bool {
        self.parent_oids.len() > 1
    }

    /// First parent (used for fast-forward detection).
    pub fn first_parent(&self) -> Option<ObjectId> {
        self.parent_oids.first().copied()
    }
}

/// Tag object - annotated tag for marking commits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    /// Tagged object oid
    pub target_oid: ObjectId,
    /// Tag name
    pub name: String,
    /// Tagger information
    pub tagger: Author,
    /// Tag message
    pub message: String,
    /// Optional signature
    pub signature: Option<String>,
    /// Whether this is a lightweight tag
    pub is_lightweight: bool,
}

impl Tag {
    /// Create an annotated tag.
    pub fn new(target_oid: ObjectId, name: &str, tagger: Author, message: &str) -> Self {
        Self {
            target_oid,
            name: name.to_string(),
            tagger,
            message: message.to_string(),
            signature: None,
            is_lightweight: false,
        }
    }

    /// Create a lightweight tag (no tagger metadata, no message).
    pub fn lightweight(target_oid: ObjectId, name: &str) -> Self {
        Self {
            target_oid,
            name: name.to_string(),
            tagger: Author::new("memst", "memst@local"),
            message: String::new(),
            signature: None,
            is_lightweight: true,
        }
    }
}

/// Object store - on-disk content-addressable storage.
pub struct ObjectStore {
    base_path: PathBuf,
}

impl ObjectStore {
    /// Open or create an object store at `base_path`.
    pub fn new(base_path: &PathBuf) -> Result<Self> {
        std::fs::create_dir_all(base_path)?;
        Ok(Self {
            base_path: base_path.clone(),
        })
    }

    fn object_path(&self, oid: &ObjectId) -> PathBuf {
        let hex = oid.to_hex();
        let dir = &hex[..2];
        let file = &hex[2..];
        self.base_path.join("objects").join(dir).join(file)
    }

    /// Write a blob, returning its content hash.
    pub fn write_blob(&mut self, blob: &Blob) -> Result<ObjectId> {
        let content = bincode::serialize(blob)?;
        let oid = ObjectId::from_content(&content);
        if self.object_path(&oid).exists() {
            return Ok(oid);
        }
        self.write_object(oid, ObjectType::Blob, &content)
    }

    /// Write a tree.
    pub fn write_tree(&mut self, tree: &Tree) -> Result<ObjectId> {
        let content = bincode::serialize(tree)?;
        let oid = ObjectId::from_content(&content);
        if self.object_path(&oid).exists() {
            return Ok(oid);
        }
        self.write_object(oid, ObjectType::Tree, &content)
    }

    /// Write a commit.
    pub fn write_commit(&mut self, commit: &Commit) -> Result<ObjectId> {
        let content = bincode::serialize(commit)?;
        let oid = ObjectId::from_content(&content);
        if self.object_path(&oid).exists() {
            return Ok(oid);
        }
        self.write_object(oid, ObjectType::Commit, &content)
    }

    /// Write an annotated/lightweight tag.
    pub fn write_tag(&mut self, tag: &Tag) -> Result<ObjectId> {
        let content = bincode::serialize(tag)?;
        let oid = ObjectId::from_content(&content);
        if self.object_path(&oid).exists() {
            return Ok(oid);
        }
        self.write_object(oid, ObjectType::Tag, &content)
    }

    fn write_object(
        &self,
        oid: ObjectId,
        object_type: ObjectType,
        data: &[u8],
    ) -> Result<ObjectId> {
        let path = self.object_path(&oid);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let header = ObjectHeader {
            object_type,
            size: data.len() as u64,
        };
        let serialized = header.serialize(data)?;
        let temp_path = path.with_extension("tmp");
        std::fs::write(&temp_path, &serialized)?;
        std::fs::rename(&temp_path, &path)?;
        Ok(oid)
    }

    fn read_object_data(&self, oid: &ObjectId, expected_type: ObjectType) -> Result<Vec<u8>> {
        let path = self.object_path(oid);
        if !path.exists() {
            return Err(Error::ObjectNotFound(*oid));
        }
        let content = std::fs::read(&path)?;
        let (header, data) = ObjectHeader::deserialize(&content)?;
        if header.object_type != expected_type {
            return Err(Error::InvalidObjectFormat);
        }
        Ok(data.to_vec())
    }

    /// Read a blob by id.
    pub fn read_blob(&self, oid: &ObjectId) -> Result<Blob> {
        let data = self.read_object_data(oid, ObjectType::Blob)?;
        Ok(bincode::deserialize(&data)?)
    }

    /// Read a tree by id.
    pub fn read_tree(&self, oid: &ObjectId) -> Result<Tree> {
        let data = self.read_object_data(oid, ObjectType::Tree)?;
        Ok(bincode::deserialize(&data)?)
    }

    /// Read a commit by id.
    pub fn read_commit(&self, oid: &ObjectId) -> Result<Commit> {
        let data = self.read_object_data(oid, ObjectType::Commit)?;
        Ok(bincode::deserialize(&data)?)
    }

    /// Read a tag by id.
    pub fn read_tag(&self, oid: &ObjectId) -> Result<Tag> {
        let data = self.read_object_data(oid, ObjectType::Tag)?;
        Ok(bincode::deserialize(&data)?)
    }

    /// Whether the store contains the given object.
    pub fn exists(&self, oid: &ObjectId) -> bool {
        self.object_path(oid).exists()
    }

    /// List every object id present in the store.
    pub fn list_objects(&self) -> Result<Vec<ObjectId>> {
        let mut oids = Vec::new();
        let objects_dir = self.base_path.join("objects");
        if !objects_dir.exists() {
            return Ok(oids);
        }
        for entry in std::fs::read_dir(&objects_dir)? {
            let dir = entry?;
            for subentry in std::fs::read_dir(dir.path())? {
                let file = subentry?;
                let file_name = file.file_name().to_string_lossy().to_string();
                let hex = format!("{}{}", dir.file_name().to_string_lossy(), file_name);
                if let Ok(oid) = ObjectId::from_hex(&hex) {
                    oids.push(oid);
                }
            }
        }
        Ok(oids)
    }

    /// Number of objects in the store.
    pub fn count(&self) -> Result<usize> {
        Ok(self.list_objects()?.len())
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(hex: &str) -> Result<[u8; 32]> {
    if hex.len() != 64 {
        return Err(Error::InvalidObjectId(format!(
            "Expected 64 hex chars, got {}",
            hex.len()
        )));
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk)
            .map_err(|_| Error::InvalidObjectId(format!("Invalid hex at position {}", i)))?;
        bytes[i] = u8::from_str_radix(s, 16)
            .map_err(|_| Error::InvalidObjectId(format!("Invalid hex at position {}", i)))?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_id_blake3() {
        let oid = ObjectId::from_content(b"hello");
        assert_eq!(oid.to_hex().len(), 64);
        assert_eq!(oid.abbreviate().len(), 7);
    }

    #[test]
    fn blob_roundtrip() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut store = ObjectStore::new(&dir.path().to_path_buf()).unwrap();
        let oid = store.write_blob(&Blob::new(b"abc")).unwrap();
        let blob = store.read_blob(&oid).unwrap();
        assert_eq!(blob.content, b"abc");
    }

    #[test]
    fn commit_with_metadata() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut store = ObjectStore::new(&dir.path().to_path_buf()).unwrap();
        let tree_oid = store.write_tree(&Tree::new()).unwrap();
        let author = Author::new("test", "test@example.com");
        let metadata = CommitMetadata {
            token_delta: 42,
            confidence: 0.9,
            source: CommitSource::AgentInline,
            scope: Some(MemoryScope::Session("abc".to_string())),
        };
        let commit = Commit::with_metadata(tree_oid, author, "msg", metadata);
        let oid = store.write_commit(&commit).unwrap();
        let read = store.read_commit(&oid).unwrap();
        assert_eq!(read.metadata.token_delta, 42);
        assert!(matches!(read.metadata.source, CommitSource::AgentInline));
    }
}
