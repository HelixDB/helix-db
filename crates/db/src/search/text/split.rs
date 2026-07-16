//! Immutable Tantivy split construction and local split-directory adapters.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::MmapDirectory;
use tantivy::directory::{DirectoryLock, FileHandle, FileSlice, OwnedBytes, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen};

use crate::error::HelixDbError;

use super::hot_directory::write_hotcache;
use super::{should_persist_file, TextSplitRef, META_JSON_FILE};

const SPLIT_TRAILER_MAGIC: [u8; 8] = *b"HLXSPT01";
const SPLIT_TRAILER_VERSION: u32 = 1;
const SPLIT_TRAILER_LEN: usize =
    SPLIT_TRAILER_MAGIC.len() + std::mem::size_of::<u32>() + std::mem::size_of::<u64>();

/// Returns whether persisted split offsets and lengths exactly cover the blob.
///
/// The footer and hot-cache payloads are each followed by their encoded
/// `u32` length. The final trailer contains the fixed magic, version, and
/// footer offset. Keeping this equation beside the writer constants prevents
/// lifecycle validation from silently accepting a shorter, imaginary trailer.
pub(crate) fn split_reference_layout_is_exact(
    footer_offset: u64,
    footer_len: u32,
    hotcache_len: u32,
    total_size: u64,
) -> bool {
    footer_offset
        .checked_add(u64::from(footer_len))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<u32>() as u64))
        .and_then(|bytes| bytes.checked_add(u64::from(hotcache_len)))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<u32>() as u64))
        .and_then(|bytes| bytes.checked_add(SPLIT_TRAILER_LEN as u64))
        == Some(total_size)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TextSplitFooterEntry {
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TextSplitFooterData {
    pub(crate) version: u32,
    pub(crate) files: BTreeMap<String, TextSplitFooterEntry>,
}

#[derive(Clone)]
pub(crate) struct TextSplitFooterCacheEntry {
    pub(crate) footer: Arc<TextSplitFooterData>,
    pub(crate) hotcache_bytes: Arc<[u8]>,
}

pub(crate) struct BuiltTextSplit {
    pub bytes: Vec<u8>,
    pub footer_offset: u64,
    pub footer_len: u32,
    pub hotcache_len: u32,
    pub total_size_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct TextSplitDirectory {
    body: FileSlice,
    footer: Arc<TextSplitFooterData>,
}

impl fmt::Debug for TextSplitDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TextSplitDirectory")
    }
}

pub(crate) fn build_split_bundle(dir: &Path) -> Result<BuiltTextSplit, HelixDbError> {
    let mut file_names = fs::read_dir(dir)
        .map_err(|err| {
            HelixDbError::Config(format!(
                "failed to list Tantivy files for split bundle '{}': {err}",
                dir.display()
            ))
        })?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if file_name == META_JSON_FILE || should_persist_file(&file_name) {
                Some(file_name)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    file_names.sort();

    let mut split_bytes = Vec::new();
    let mut files = BTreeMap::new();

    for file_name in file_names {
        let payload = fs::read(dir.join(&file_name)).map_err(|err| {
            HelixDbError::Config(format!(
                "failed to read Tantivy file '{}' for split bundle: {err}",
                file_name
            ))
        })?;
        let start = split_bytes.len() as u64;
        split_bytes.extend_from_slice(&payload);
        let end = split_bytes.len() as u64;
        files.insert(
            file_name,
            TextSplitFooterEntry {
                start,
                end,
                size_bytes: payload.len() as u64,
            },
        );
    }

    let footer_bytes = serde_json::to_vec(&TextSplitFooterData {
        version: SPLIT_TRAILER_VERSION,
        files,
    })
    .map_err(|err| HelixDbError::Config(format!("failed to encode text split footer: {err}")))?;
    let footer_len = u32::try_from(footer_bytes.len()).map_err(|_| {
        HelixDbError::Config("text split footer exceeds 32-bit length limit".into())
    })?;
    let mut hotcache_bytes = Vec::new();
    write_hotcache(
        MmapDirectory::open(dir).map_err(|err| {
            HelixDbError::Config(format!(
                "failed to open Tantivy directory '{}' for hotcache build: {err}",
                dir.display()
            ))
        })?,
        &mut hotcache_bytes,
    )?;
    let hotcache_len = u32::try_from(hotcache_bytes.len()).map_err(|_| {
        HelixDbError::Config("text split hotcache exceeds 32-bit length limit".into())
    })?;
    let footer_offset = split_bytes.len() as u64;

    split_bytes.extend_from_slice(&footer_bytes);
    split_bytes.extend_from_slice(&footer_len.to_le_bytes());
    split_bytes.extend_from_slice(&hotcache_bytes);
    split_bytes.extend_from_slice(&hotcache_len.to_le_bytes());
    split_bytes.extend_from_slice(&SPLIT_TRAILER_MAGIC);
    split_bytes.extend_from_slice(&SPLIT_TRAILER_VERSION.to_le_bytes());
    split_bytes.extend_from_slice(&footer_offset.to_le_bytes());

    Ok(BuiltTextSplit {
        total_size_bytes: split_bytes.len() as u64,
        bytes: split_bytes,
        footer_offset,
        footer_len,
        hotcache_len,
    })
}

pub(crate) fn decode_split_footer_bytes(bytes: &[u8]) -> Result<TextSplitFooterData, HelixDbError> {
    let footer: TextSplitFooterData = serde_json::from_slice(bytes).map_err(|err| {
        HelixDbError::Config(format!("failed to decode text split footer: {err}"))
    })?;
    if footer.version != SPLIT_TRAILER_VERSION {
        return Err(HelixDbError::Config(format!(
            "unsupported text split footer version {}",
            footer.version
        )));
    }
    Ok(footer)
}

pub(crate) fn open_split_directory_from_file(
    path: &Path,
) -> Result<TextSplitDirectory, HelixDbError> {
    let file = File::open(path).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to open text split bundle '{}': {err}",
            path.display()
        ))
    })?;
    let file_slice = FileSlice::new(Arc::new(LocalSplitFileHandle::new(file).map_err(
        |err| {
            HelixDbError::Config(format!(
                "failed to read metadata for text split bundle '{}': {err}",
                path.display()
            ))
        },
    )?));
    open_split_directory(file_slice)
}

#[cfg(test)]
pub(crate) fn open_split_directory_from_bytes(
    bytes: Vec<u8>,
) -> Result<TextSplitDirectory, HelixDbError> {
    open_split_directory(FileSlice::from(Arc::<[u8]>::from(bytes.into_boxed_slice())))
}

fn open_split_directory(file: FileSlice) -> Result<TextSplitDirectory, HelixDbError> {
    let parsed = parse_split_metadata(file)?;
    Ok(TextSplitDirectory {
        body: parsed.body,
        footer: Arc::new(parsed.footer),
    })
}

struct ParsedSplitMetadata {
    body: FileSlice,
    footer: TextSplitFooterData,
    _hotcache_bytes: Arc<[u8]>,
}

fn parse_split_metadata(file: FileSlice) -> Result<ParsedSplitMetadata, HelixDbError> {
    let total_size = file.len();
    if total_size < SPLIT_TRAILER_LEN + std::mem::size_of::<u32>() * 2 {
        return Err(HelixDbError::Config(
            "text split bundle is too small to contain a footer".into(),
        ));
    }

    let trailer = file
        .slice_from_end(SPLIT_TRAILER_LEN)
        .read_bytes()
        .map_err(|err| HelixDbError::Config(format!("failed to read text split trailer: {err}")))?;

    if trailer[..SPLIT_TRAILER_MAGIC.len()] != SPLIT_TRAILER_MAGIC[..] {
        return Err(HelixDbError::Config(
            "text split bundle trailer magic mismatch".into(),
        ));
    }

    let version_start = SPLIT_TRAILER_MAGIC.len();
    let version_end = version_start + std::mem::size_of::<u32>();
    let version = u32::from_le_bytes(
        trailer[version_start..version_end]
            .try_into()
            .expect("version slice length"),
    );
    if version != SPLIT_TRAILER_VERSION {
        return Err(HelixDbError::Config(format!(
            "unsupported text split trailer version {version}"
        )));
    }

    let footer_offset = u64::from_le_bytes(
        trailer[version_end..]
            .try_into()
            .expect("footer offset slice length"),
    );

    let trailer_start = total_size - SPLIT_TRAILER_LEN;
    let hotcache_len_end = trailer_start;
    let hotcache_len_start = hotcache_len_end - std::mem::size_of::<u32>();
    let hotcache_len = u32::from_le_bytes(
        file.slice(hotcache_len_start..hotcache_len_end)
            .read_bytes()
            .map_err(|err| {
                HelixDbError::Config(format!("failed to read text split hotcache length: {err}"))
            })?
            .as_slice()
            .try_into()
            .expect("hotcache length slice length"),
    );

    let hotcache_start = hotcache_len_start
        .checked_sub(hotcache_len as usize)
        .ok_or_else(|| HelixDbError::Config("text split hotcache exceeds file size".into()))?;
    let footer_len_end = hotcache_start;
    let footer_len_start = footer_len_end - std::mem::size_of::<u32>();
    let footer_len = u32::from_le_bytes(
        file.slice(footer_len_start..footer_len_end)
            .read_bytes()
            .map_err(|err| {
                HelixDbError::Config(format!("failed to read text split footer length: {err}"))
            })?
            .as_slice()
            .try_into()
            .expect("footer length slice length"),
    );

    let footer_start = footer_len_start
        .checked_sub(footer_len as usize)
        .ok_or_else(|| HelixDbError::Config("text split footer exceeds file size".into()))?;
    if footer_start as u64 != footer_offset {
        return Err(HelixDbError::Config(format!(
            "text split trailer footer offset {} does not match computed footer start {}",
            footer_offset, footer_start
        )));
    }

    let footer_bytes = file
        .slice(footer_start..footer_len_start)
        .read_bytes()
        .map_err(|err| HelixDbError::Config(format!("failed to read text split footer: {err}")))?;
    let footer = decode_split_footer_bytes(footer_bytes.as_slice())?;
    let hotcache_bytes = file
        .slice(hotcache_start..hotcache_len_start)
        .read_bytes()
        .map_err(|err| {
            HelixDbError::Config(format!("failed to read text split hotcache: {err}"))
        })?;

    Ok(ParsedSplitMetadata {
        body: file.slice_to(footer_start),
        footer,
        _hotcache_bytes: Arc::<[u8]>::from(hotcache_bytes.as_slice().to_vec().into_boxed_slice()),
    })
}

pub(crate) fn decode_footer_cache_entry_bytes(
    payload: &[u8],
    split_ref: &TextSplitRef,
) -> Result<TextSplitFooterCacheEntry, HelixDbError> {
    let footer_len = split_ref.footer_len as usize;
    let hotcache_len = split_ref.hotcache_len as usize;
    let expected_payload_len = footer_len
        + std::mem::size_of::<u32>()
        + hotcache_len
        + std::mem::size_of::<u32>()
        + SPLIT_TRAILER_LEN;
    if payload.len() != expected_payload_len {
        return Err(HelixDbError::Config(format!(
            "text split footer payload has length {} but expected {}",
            payload.len(),
            expected_payload_len
        )));
    }

    let footer_bytes = &payload[..footer_len];
    let stored_footer_len = u32::from_le_bytes(
        payload[footer_len..footer_len + std::mem::size_of::<u32>()]
            .try_into()
            .expect("footer len slice"),
    );
    if stored_footer_len != split_ref.footer_len {
        return Err(HelixDbError::Config(format!(
            "text split footer payload stored footer length {} but expected {}",
            stored_footer_len, split_ref.footer_len
        )));
    }

    let hotcache_start = footer_len + std::mem::size_of::<u32>();
    let hotcache_end = hotcache_start + hotcache_len;
    let stored_hotcache_len = u32::from_le_bytes(
        payload[hotcache_end..hotcache_end + std::mem::size_of::<u32>()]
            .try_into()
            .expect("hotcache len slice"),
    );
    if stored_hotcache_len != split_ref.hotcache_len {
        return Err(HelixDbError::Config(format!(
            "text split footer payload stored hotcache length {} but expected {}",
            stored_hotcache_len, split_ref.hotcache_len
        )));
    }

    let trailer = &payload[hotcache_end + std::mem::size_of::<u32>()..];
    if trailer[..SPLIT_TRAILER_MAGIC.len()] != SPLIT_TRAILER_MAGIC[..] {
        return Err(HelixDbError::Config(
            "text split footer payload trailer magic mismatch".into(),
        ));
    }

    let version_start = SPLIT_TRAILER_MAGIC.len();
    let version_end = version_start + std::mem::size_of::<u32>();
    let version = u32::from_le_bytes(
        trailer[version_start..version_end]
            .try_into()
            .expect("trailer version slice"),
    );
    if version != SPLIT_TRAILER_VERSION {
        return Err(HelixDbError::Config(format!(
            "unsupported text split trailer version {version}"
        )));
    }
    let trailer_footer_offset = u64::from_le_bytes(
        trailer[version_end..]
            .try_into()
            .expect("trailer footer offset slice"),
    );
    if trailer_footer_offset != split_ref.footer_offset {
        return Err(HelixDbError::Config(format!(
            "text split footer payload trailer footer offset {} but expected {}",
            trailer_footer_offset, split_ref.footer_offset
        )));
    }

    Ok(TextSplitFooterCacheEntry {
        footer: Arc::new(decode_split_footer_bytes(footer_bytes)?),
        hotcache_bytes: Arc::<[u8]>::from(
            payload[hotcache_start..hotcache_end]
                .to_vec()
                .into_boxed_slice(),
        ),
    })
}

fn path_key(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn read_local_range(file: &File, range: Range<usize>) -> io::Result<Vec<u8>> {
    if range.is_empty() {
        return Ok(Vec::new());
    }

    let mut buffer = vec![0; range.end.saturating_sub(range.start)];

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut buffer, range.start as u64)?;
    }

    #[cfg(not(unix))]
    {
        use std::io::{Read, Seek};

        let mut cloned = file.try_clone()?;
        cloned.seek(io::SeekFrom::Start(range.start as u64))?;
        cloned.read_exact(&mut buffer)?;
    }

    Ok(buffer)
}

#[derive(Debug)]
struct LocalSplitFileHandle {
    file: File,
    len: usize,
}

impl LocalSplitFileHandle {
    fn new(file: File) -> io::Result<Self> {
        let len = file.metadata()?.len() as usize;
        Ok(Self { file, len })
    }
}

#[async_trait]
impl FileHandle for LocalSplitFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.end > self.len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "requested range {}..{} exceeds local split length {}",
                    range.start, range.end, self.len
                ),
            ));
        }
        Ok(OwnedBytes::new(read_local_range(&self.file, range)?))
    }

    async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.read_bytes(range)
    }
}

impl HasLen for LocalSplitFileHandle {
    fn len(&self) -> usize {
        self.len
    }
}

impl Directory for TextSplitDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(self.open_read(path)?))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let path_key = path_key(path);
        let entry = self
            .footer
            .files
            .get(&path_key)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        Ok(self.body.slice(entry.start as usize..entry.end as usize))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        let payload = file_slice
            .read_bytes()
            .map_err(|err| OpenReadError::wrap_io_error(err, path.to_path_buf()))?;
        Ok(payload.to_vec())
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.footer.files.contains_key(&path_key(path)))
    }

    fn delete(&self, _path: &Path) -> Result<(), DeleteError> {
        unimplemented!("read-only")
    }

    fn open_write(&self, _path: &Path) -> Result<WritePtr, OpenWriteError> {
        unimplemented!("read-only")
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        unimplemented!("read-only")
    }

    fn sync_directory(&self) -> io::Result<()> {
        unimplemented!("read-only")
    }

    fn watch(
        &self,
        _watch_callback: tantivy::directory::WatchCallback,
    ) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }

    fn acquire_lock(&self, _lock: &tantivy::directory::Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(|| {})))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_reference_layout_counts_the_complete_frozen_trailer() {
        assert!(split_reference_layout_is_exact(80, 16, 4, 128));
        assert!(!split_reference_layout_is_exact(80, 16, 4, 127));
        assert!(!split_reference_layout_is_exact(80, 16, 4, 116));
        assert!(!split_reference_layout_is_exact(
            u64::MAX,
            u32::MAX,
            u32::MAX,
            u64::MAX,
        ));
    }
    use crate::search::text::{TextBlobRef, TextSplitRef};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use tantivy::schema::{
        IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
    };
    use tantivy::{doc, Index};

    fn synthetic_split_bytes(
        body: &[u8],
        footer: TextSplitFooterData,
        hotcache: &[u8],
    ) -> (Vec<u8>, TextSplitRef) {
        let footer_bytes = serde_json::to_vec(&footer).expect("encode footer");
        let footer_len = u32::try_from(footer_bytes.len()).expect("footer fits");
        let hotcache_len = u32::try_from(hotcache.len()).expect("hotcache fits");
        let footer_offset = body.len() as u64;
        let mut bytes = body.to_vec();
        bytes.extend_from_slice(&footer_bytes);
        bytes.extend_from_slice(&footer_len.to_le_bytes());
        bytes.extend_from_slice(hotcache);
        bytes.extend_from_slice(&hotcache_len.to_le_bytes());
        bytes.extend_from_slice(&SPLIT_TRAILER_MAGIC);
        bytes.extend_from_slice(&SPLIT_TRAILER_VERSION.to_le_bytes());
        bytes.extend_from_slice(&footer_offset.to_le_bytes());

        let total_size_bytes = bytes.len() as u64;
        (
            bytes,
            TextSplitRef {
                blob: TextBlobRef {
                    sha256: [7u8; 32],
                    size_bytes: total_size_bytes,
                },
                footer_offset,
                footer_len,
                hotcache_len,
                total_size_bytes,
            },
        )
    }

    fn synthetic_footer() -> TextSplitFooterData {
        TextSplitFooterData {
            version: SPLIT_TRAILER_VERSION,
            files: BTreeMap::from([
                (
                    META_JSON_FILE.to_string(),
                    TextSplitFooterEntry {
                        start: 0,
                        end: 4,
                        size_bytes: 4,
                    },
                ),
                (
                    "segment.term".to_string(),
                    TextSplitFooterEntry {
                        start: 4,
                        end: 8,
                        size_bytes: 4,
                    },
                ),
            ]),
        }
    }

    fn build_valid_tantivy_dir(dir: &Path) {
        let mut schema_builder = Schema::builder();
        let entity_id =
            schema_builder.add_u64_field("entity_id", NumericOptions::default().set_fast());
        let body = schema_builder.add_text_field(
            "body",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("default")
                    .set_index_option(IndexRecordOption::WithFreqs),
            ),
        );
        let schema = schema_builder.build();
        let index = Index::create_in_dir(dir, schema).expect("create index");
        let mut writer = index.writer(15_000_000).expect("writer");
        writer
            .add_document(doc!(entity_id => 1u64, body => "hello split"))
            .expect("add doc");
        writer.commit().expect("commit");
    }

    #[test]
    fn split_bundle_roundtrips_internal_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        build_valid_tantivy_dir(dir.path());

        let built = build_split_bundle(dir.path()).expect("build split");
        let split = open_split_directory_from_bytes(built.bytes).expect("open split dir");

        assert!(split
            .exists(Path::new(META_JSON_FILE))
            .expect("meta exists"));
        assert!(!split
            .atomic_read(Path::new(META_JSON_FILE))
            .expect("read meta")
            .is_empty());
    }

    #[test]
    fn split_bundle_build_reports_directory_file_and_index_errors() {
        let missing = tempfile::tempdir().unwrap().path().join("missing");
        assert!(build_split_bundle(&missing)
            .err()
            .expect("missing directory fails")
            .to_string()
            .contains("failed to list Tantivy files"));

        let invalid_file = tempfile::NamedTempFile::new().unwrap();
        assert!(build_split_bundle(invalid_file.path())
            .err()
            .expect("file path fails")
            .to_string()
            .contains("failed to list Tantivy files"));

        let unreadable = tempfile::tempdir().unwrap();
        std::fs::write(unreadable.path().join(META_JSON_FILE), b"{}").unwrap();
        std::fs::create_dir(unreadable.path().join("invalid.term")).unwrap();
        assert!(build_split_bundle(unreadable.path())
            .err()
            .expect("directory entry fails")
            .to_string()
            .contains("failed to read Tantivy file 'invalid.term'"));

        let not_an_index = tempfile::tempdir().unwrap();
        std::fs::write(not_an_index.path().join(META_JSON_FILE), b"{}").unwrap();
        assert!(build_split_bundle(not_an_index.path())
            .err()
            .expect("non-index directory fails")
            .to_string()
            .contains("failed to open Tantivy index"));
    }

    #[test]
    fn split_bundle_file_roundtrips_internal_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        build_valid_tantivy_dir(dir.path());

        assert!(open_split_directory_from_file(&dir.path().join("missing"))
            .expect_err("missing split fails")
            .to_string()
            .contains("failed to open text split bundle"));

        let built = build_split_bundle(dir.path()).expect("build split");
        let split_path = dir.path().join("generation.split");
        std::fs::write(&split_path, built.bytes).expect("write split file");

        let split = open_split_directory_from_file(&split_path).expect("open split dir");
        assert!(!split
            .atomic_read(Path::new(META_JSON_FILE))
            .expect("read meta")
            .is_empty());
    }

    #[test]
    fn decode_split_footer_rejects_malformed_and_unsupported_payloads() {
        let malformed = decode_split_footer_bytes(b"{").expect_err("malformed footer");
        assert!(malformed
            .to_string()
            .contains("failed to decode text split footer"));

        let unsupported = serde_json::to_vec(&TextSplitFooterData {
            version: SPLIT_TRAILER_VERSION + 1,
            files: BTreeMap::new(),
        })
        .expect("encode footer");
        let err = decode_split_footer_bytes(&unsupported).expect_err("unsupported version");
        assert!(err
            .to_string()
            .contains("unsupported text split footer version"));
    }

    #[test]
    fn split_metadata_rejects_invalid_trailers_and_offsets() {
        let (bytes, _) = synthetic_split_bytes(b"metaterm", synthetic_footer(), b"hot");

        let too_small =
            open_split_directory_from_bytes(vec![0; SPLIT_TRAILER_LEN]).expect_err("too small");
        assert!(too_small.to_string().contains("too small"));

        let mut bad_magic = bytes.clone();
        let magic_start = bad_magic.len() - SPLIT_TRAILER_LEN;
        bad_magic[magic_start] = b'X';
        let err = open_split_directory_from_bytes(bad_magic).expect_err("bad magic");
        assert!(err.to_string().contains("trailer magic mismatch"));

        let mut bad_version = bytes.clone();
        let version_start = bad_version.len() - SPLIT_TRAILER_LEN + SPLIT_TRAILER_MAGIC.len();
        bad_version[version_start..version_start + std::mem::size_of::<u32>()]
            .copy_from_slice(&(SPLIT_TRAILER_VERSION + 1).to_le_bytes());
        let err = open_split_directory_from_bytes(bad_version).expect_err("bad version");
        assert!(err
            .to_string()
            .contains("unsupported text split trailer version"));

        let mut bad_footer_offset = bytes;
        let footer_offset_start = bad_footer_offset.len() - std::mem::size_of::<u64>();
        bad_footer_offset[footer_offset_start..footer_offset_start + std::mem::size_of::<u64>()]
            .copy_from_slice(&99u64.to_le_bytes());
        let err =
            open_split_directory_from_bytes(bad_footer_offset).expect_err("bad footer offset");
        assert!(err
            .to_string()
            .contains("does not match computed footer start"));

        let (bytes, _) = synthetic_split_bytes(b"metaterm", synthetic_footer(), b"hot");
        let mut excessive_hotcache = bytes.clone();
        let hotcache_len_start =
            excessive_hotcache.len() - SPLIT_TRAILER_LEN - std::mem::size_of::<u32>();
        excessive_hotcache[hotcache_len_start..hotcache_len_start + std::mem::size_of::<u32>()]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(open_split_directory_from_bytes(excessive_hotcache)
            .unwrap_err()
            .to_string()
            .contains("hotcache exceeds file size"));

        let mut excessive_footer = bytes;
        let hotcache_len_start =
            excessive_footer.len() - SPLIT_TRAILER_LEN - std::mem::size_of::<u32>();
        let footer_len_start = hotcache_len_start - 3 - std::mem::size_of::<u32>();
        excessive_footer[footer_len_start..footer_len_start + std::mem::size_of::<u32>()]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(open_split_directory_from_bytes(excessive_footer)
            .unwrap_err()
            .to_string()
            .contains("footer exceeds file size"));
    }

    #[test]
    fn footer_cache_entry_decodes_and_validates_payload_metadata() {
        let (bytes, split_ref) = synthetic_split_bytes(b"metaterm", synthetic_footer(), b"hot");
        let payload = &bytes[split_ref.footer_offset as usize..split_ref.total_size_bytes as usize];
        let decoded =
            decode_footer_cache_entry_bytes(payload, &split_ref).expect("decode footer cache");

        assert_eq!(decoded.hotcache_bytes.as_ref(), b"hot");
        assert!(decoded.footer.files.contains_key(META_JSON_FILE));

        let wrong_len_ref = TextSplitRef {
            footer_len: split_ref.footer_len + 1,
            ..split_ref.clone()
        };
        let err = decode_footer_cache_entry_bytes(payload, &wrong_len_ref)
            .err()
            .expect("wrong payload length");
        assert!(err.to_string().contains("payload has length"));

        let mut wrong_stored_footer_len = payload.to_vec();
        wrong_stored_footer_len[split_ref.footer_len as usize
            ..split_ref.footer_len as usize + std::mem::size_of::<u32>()]
            .copy_from_slice(&(split_ref.footer_len + 1).to_le_bytes());
        let err = decode_footer_cache_entry_bytes(&wrong_stored_footer_len, &split_ref)
            .err()
            .expect("wrong stored footer length");
        assert!(err.to_string().contains("stored footer length"));

        let mut wrong_trailer_offset = payload.to_vec();
        let offset_start = wrong_trailer_offset.len() - std::mem::size_of::<u64>();
        wrong_trailer_offset[offset_start..offset_start + std::mem::size_of::<u64>()]
            .copy_from_slice(&(split_ref.footer_offset + 1).to_le_bytes());
        let err = decode_footer_cache_entry_bytes(&wrong_trailer_offset, &split_ref)
            .err()
            .expect("wrong trailer footer offset");
        assert!(err.to_string().contains("trailer footer offset"));

        let hotcache_start = split_ref.footer_len as usize + std::mem::size_of::<u32>();
        let hotcache_end = hotcache_start + split_ref.hotcache_len as usize;
        let mut wrong_stored_hotcache_len = payload.to_vec();
        wrong_stored_hotcache_len[hotcache_end..hotcache_end + std::mem::size_of::<u32>()]
            .copy_from_slice(&(split_ref.hotcache_len + 1).to_le_bytes());
        assert!(
            decode_footer_cache_entry_bytes(&wrong_stored_hotcache_len, &split_ref)
                .err()
                .expect("wrong stored hotcache length fails")
                .to_string()
                .contains("stored hotcache length")
        );

        let trailer_start = hotcache_end + std::mem::size_of::<u32>();
        let mut wrong_magic = payload.to_vec();
        wrong_magic[trailer_start] = b'X';
        assert!(decode_footer_cache_entry_bytes(&wrong_magic, &split_ref)
            .err()
            .expect("wrong footer cache magic fails")
            .to_string()
            .contains("trailer magic mismatch"));

        let mut wrong_version = payload.to_vec();
        let version_start = trailer_start + SPLIT_TRAILER_MAGIC.len();
        wrong_version[version_start..version_start + std::mem::size_of::<u32>()]
            .copy_from_slice(&(SPLIT_TRAILER_VERSION + 1).to_le_bytes());
        assert!(decode_footer_cache_entry_bytes(&wrong_version, &split_ref)
            .err()
            .expect("wrong footer cache version fails")
            .to_string()
            .contains("unsupported text split trailer version"));
    }

    #[tokio::test]
    async fn local_split_file_handle_enforces_range_bounds() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"abcd").unwrap();
        let file = File::open(file.path()).unwrap();

        assert_eq!(read_local_range(&file, 2..2).unwrap(), Vec::<u8>::new());
        assert_eq!(read_local_range(&file, 1..3).unwrap(), b"bc");
        assert!(read_local_range(&file, 0..5).is_err());

        let handle = LocalSplitFileHandle::new(file).unwrap();
        assert_eq!(handle.len(), 4);
        assert_eq!(handle.read_bytes(1..3).unwrap().as_slice(), b"bc");
        assert_eq!(
            handle.read_bytes_async(2..4).await.unwrap().as_slice(),
            b"cd"
        );
        assert_eq!(
            handle.read_bytes(0..5).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn text_split_directory_exposes_files_and_read_only_methods() {
        let (bytes, _) = synthetic_split_bytes(b"metaterm", synthetic_footer(), b"hot");
        let split = open_split_directory_from_bytes(bytes).expect("open split");

        assert_eq!(format!("{split:?}"), "TextSplitDirectory");
        assert!(split.exists(Path::new(META_JSON_FILE)).expect("exists"));
        assert!(!split.exists(Path::new("missing")).expect("missing"));
        assert_eq!(
            split
                .atomic_read(Path::new("segment.term"))
                .expect("read segment"),
            b"term"
        );
        assert!(matches!(
            split.open_read(Path::new("missing")),
            Err(OpenReadError::FileDoesNotExist(_))
        ));
        assert_eq!(
            split
                .get_file_handle(Path::new("segment.term"))
                .unwrap()
                .read_bytes(1..3)
                .unwrap()
                .as_slice(),
            b"er"
        );

        let _watch = split
            .watch(tantivy::directory::WatchCallback::new(|| {}))
            .expect("watch");
        let lock = tantivy::directory::Lock {
            filepath: Path::new("split.lock").to_path_buf(),
            is_blocking: false,
        };
        let _guard = split.acquire_lock(&lock).expect("lock");

        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = split.delete(Path::new("segment.term"));
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = split.open_write(Path::new("segment.term"));
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = split.atomic_write(Path::new("segment.term"), b"new");
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = split.sync_directory();
        }))
        .is_err());
    }
}
