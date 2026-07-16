use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryLock, FileHandle, FileSlice, OwnedBytes, WatchHandle, WritePtr};
use tantivy::{Directory, HasLen, Index, IndexReader, ReloadPolicy, TantivyError};

use crate::error::HelixDbError;

use super::caching_directory::CachingDirectory;
use super::debug_proxy_directory::DebugProxyDirectory;

#[derive(Serialize, Deserialize)]
struct HotDirectoryMeta {
    file_lengths: HashMap<PathBuf, u64>,
    slice_offsets: Vec<(PathBuf, u64)>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SliceCacheIndexEntry {
    start: usize,
    stop: usize,
    addr: usize,
}

impl SliceCacheIndexEntry {
    fn len(&self) -> usize {
        self.stop - self.start
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct SliceCacheIndex {
    total_len: u64,
    slices: Vec<SliceCacheIndexEntry>,
}

impl SliceCacheIndex {
    fn is_complete(&self) -> bool {
        self.slices.len() == 1 && self.slices[0].len() as u64 == self.total_len
    }

    fn get(&self, byte_range: Range<usize>) -> Option<usize> {
        let entry_idx = match self
            .slices
            .binary_search_by_key(&byte_range.start, |entry| entry.start)
        {
            Ok(idx) => idx,
            Err(0) => return None,
            Err(idx_after) => idx_after - 1,
        };
        let entry = &self.slices[entry_idx];
        if entry.start > byte_range.start || entry.stop < byte_range.end {
            return None;
        }
        Some(entry.addr + byte_range.start - entry.start)
    }
}

#[derive(Default)]
struct StaticDirectoryCacheBuilder {
    file_builders: HashMap<PathBuf, StaticSliceCacheBuilder>,
    file_lengths: HashMap<PathBuf, u64>,
}

impl StaticDirectoryCacheBuilder {
    fn add_file(&mut self, path: &Path, file_len: u64) -> &mut StaticSliceCacheBuilder {
        self.file_lengths.insert(path.to_path_buf(), file_len);
        self.file_builders
            .entry(path.to_path_buf())
            .or_insert_with(|| StaticSliceCacheBuilder::new(file_len))
    }

    fn write(self, output: &mut dyn Write) -> Result<(), HelixDbError> {
        let mut data_buffer = Vec::new();
        let mut data_index = Vec::new();
        let mut offset = 0u64;

        for (path, cache) in self.file_builders {
            let bytes = cache.flush()?;
            data_index.push((path, offset));
            offset += bytes.len() as u64;
            data_buffer.extend_from_slice(&bytes);
        }

        let meta = HotDirectoryMeta {
            file_lengths: self.file_lengths,
            slice_offsets: data_index,
        };
        let meta_bytes = bincode::serialize(&meta).map_err(|err| {
            HelixDbError::Config(format!("failed to encode hotcache meta: {err}"))
        })?;
        output
            .write_all(&(meta_bytes.len() as u32).to_le_bytes())
            .map_err(|err| {
                HelixDbError::Config(format!("failed to write hotcache meta len: {err}"))
            })?;
        output
            .write_all(&meta_bytes)
            .map_err(|err| HelixDbError::Config(format!("failed to write hotcache meta: {err}")))?;
        output.write_all(&data_buffer).map_err(|err| {
            HelixDbError::Config(format!("failed to write hotcache payload: {err}"))
        })?;
        output.flush().map_err(|err| {
            HelixDbError::Config(format!("failed to flush hotcache bytes: {err}"))
        })?;
        Ok(())
    }
}

#[derive(Debug)]
struct StaticDirectoryCache {
    file_lengths: HashMap<PathBuf, u64>,
    slices: HashMap<PathBuf, Arc<StaticSliceCache>>,
}

impl StaticDirectoryCache {
    fn open(bytes: OwnedBytes) -> Result<Self, HelixDbError> {
        if bytes.len() < std::mem::size_of::<u32>() {
            return Err(HelixDbError::Config("hotcache payload is too short".into()));
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&bytes.as_slice()[..4]);
        let meta_len = u32::from_le_bytes(len_bytes) as usize;
        if bytes.len() < 4 + meta_len {
            return Err(HelixDbError::Config(
                "hotcache metadata exceeds payload size".into(),
            ));
        }
        let meta: HotDirectoryMeta = bincode::deserialize(&bytes.as_slice()[4..4 + meta_len])
            .map_err(|err| {
                HelixDbError::Config(format!("failed to decode hotcache meta: {err}"))
            })?;
        let payload = bytes.slice(4 + meta_len..bytes.len());

        let mut slice_offsets = meta.slice_offsets.clone();
        slice_offsets.push((PathBuf::new(), payload.len() as u64));
        let slices = slice_offsets
            .windows(2)
            .map(|window| {
                let path = window[0].0.clone();
                let start = window[0].1 as usize;
                let end = window[1].1 as usize;
                StaticSliceCache::open(payload.slice(start..end))
                    .map(|cache| (path, Arc::new(cache)))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        Ok(Self {
            file_lengths: meta.file_lengths,
            slices,
        })
    }

    fn get_slice(&self, path: &Path) -> Arc<StaticSliceCache> {
        self.slices.get(path).cloned().unwrap_or_default()
    }

    fn file_length(&self, path: &Path) -> Option<u64> {
        self.file_lengths.get(path).copied()
    }
}

#[derive(Default)]
struct StaticSliceCacheBuilder {
    bytes: Vec<u8>,
    slices: Vec<SliceCacheIndexEntry>,
    offset: u64,
    total_len: u64,
}

impl StaticSliceCacheBuilder {
    fn new(total_len: u64) -> Self {
        Self {
            bytes: Vec::new(),
            slices: Vec::new(),
            offset: 0,
            total_len,
        }
    }

    fn add_bytes(&mut self, bytes: &[u8], start: usize) {
        self.bytes.extend_from_slice(bytes);
        self.slices.push(SliceCacheIndexEntry {
            start,
            stop: start + bytes.len(),
            addr: self.offset as usize,
        });
        self.offset += bytes.len() as u64;
    }

    fn flush(mut self) -> Result<Vec<u8>, HelixDbError> {
        if !self.slices.is_empty() {
            self.slices.sort_unstable_by_key(|entry| entry.start);
            let mut merged = Vec::with_capacity(self.slices.len());
            let mut last = self.slices[0].clone();
            for entry in &self.slices[1..] {
                if entry.start < last.stop {
                    return Err(HelixDbError::Config(format!(
                        "hotcache contains overlapping slices at byte {}",
                        entry.start
                    )));
                }
                if last.stop == entry.start && last.addr + last.len() == entry.addr {
                    last.stop += entry.len();
                } else {
                    merged.push(last);
                    last = entry.clone();
                }
            }
            merged.push(last);
            self.slices = merged;
        }

        let index = SliceCacheIndex {
            total_len: self.total_len,
            slices: self.slices,
        };
        let index_bytes = bincode::serialize(&index).map_err(|err| {
            HelixDbError::Config(format!("failed to encode hotcache index: {err}"))
        })?;
        self.bytes.extend_from_slice(&index_bytes);
        self.bytes.extend_from_slice(&self.offset.to_le_bytes());
        Ok(self.bytes)
    }
}

#[derive(Debug)]
struct StaticSliceCache {
    bytes: OwnedBytes,
    index: SliceCacheIndex,
}

impl Default for StaticSliceCache {
    fn default() -> Self {
        Self {
            bytes: OwnedBytes::empty(),
            index: SliceCacheIndex::default(),
        }
    }
}

impl StaticSliceCache {
    fn open(bytes: OwnedBytes) -> Result<Self, HelixDbError> {
        if bytes.len() < std::mem::size_of::<u64>() {
            return Err(HelixDbError::Config(
                "hotcache slice payload is too short".into(),
            ));
        }
        let payload_len_start = bytes.len() - std::mem::size_of::<u64>();
        let mut payload_len_bytes = [0u8; 8];
        payload_len_bytes.copy_from_slice(&bytes.as_slice()[payload_len_start..]);
        let payload_len = u64::from_le_bytes(payload_len_bytes) as usize;
        if payload_len > payload_len_start {
            return Err(HelixDbError::Config(
                "hotcache slice payload length is invalid".into(),
            ));
        }
        let payload = bytes.slice(0..payload_len);
        let index: SliceCacheIndex =
            bincode::deserialize(&bytes.as_slice()[payload_len..payload_len_start]).map_err(
                |err| HelixDbError::Config(format!("failed to decode hotcache slice index: {err}")),
            )?;
        Ok(Self {
            bytes: payload,
            index,
        })
    }

    fn try_read_all(&self) -> Option<OwnedBytes> {
        if self.index.is_complete() {
            Some(self.bytes.clone())
        } else {
            None
        }
    }

    fn try_read_bytes(&self, byte_range: Range<usize>) -> Option<OwnedBytes> {
        if byte_range.is_empty() {
            return Some(OwnedBytes::empty());
        }
        let start = self.index.get(byte_range.clone())?;
        Some(self.bytes.slice(start..start + byte_range.len()))
    }
}

#[derive(Clone)]
pub(crate) struct HotDirectory {
    inner: Arc<InnerHotDirectory>,
}

struct InnerHotDirectory {
    underlying: Arc<dyn Directory>,
    cache: Arc<StaticDirectoryCache>,
}

impl HotDirectory {
    pub(crate) fn open(
        underlying: Arc<dyn Directory>,
        hot_cache_bytes: &[u8],
    ) -> Result<Self, HelixDbError> {
        let cache = StaticDirectoryCache::open(OwnedBytes::new(hot_cache_bytes.to_vec()))?;
        Ok(Self {
            inner: Arc::new(InnerHotDirectory {
                underlying,
                cache: Arc::new(cache),
            }),
        })
    }
}

impl fmt::Debug for HotDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HotDirectory")
    }
}

struct FileSliceWithCache {
    underlying: FileSlice,
    static_cache: Arc<StaticSliceCache>,
    file_length: u64,
}

impl fmt::Debug for FileSliceWithCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileSliceWithCache")
    }
}

#[async_trait]
impl FileHandle for FileSliceWithCache {
    fn read_bytes(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.static_cache.try_read_bytes(byte_range.clone()) {
            return Ok(bytes);
        }
        self.underlying.read_bytes_slice(byte_range)
    }

    async fn read_bytes_async(&self, byte_range: Range<usize>) -> io::Result<OwnedBytes> {
        if let Some(bytes) = self.static_cache.try_read_bytes(byte_range.clone()) {
            return Ok(bytes);
        }
        self.underlying.read_bytes_slice_async(byte_range).await
    }
}

impl HasLen for FileSliceWithCache {
    fn len(&self) -> usize {
        self.file_length as usize
    }
}

impl Directory for HotDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_length = self
            .inner
            .cache
            .file_length(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
        let underlying_file_handle = self.inner.underlying.get_file_handle(path)?;
        let underlying =
            FileSlice::new_with_num_bytes(underlying_file_handle, file_length as usize);
        Ok(Arc::new(FileSliceWithCache {
            underlying,
            static_cache: self.inner.cache.get_slice(path),
            file_length,
        }))
    }

    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        let file_handle = self.get_file_handle(path)?;
        Ok(FileSlice::new_with_num_bytes(
            file_handle.clone(),
            file_handle.len(),
        ))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if let Some(bytes) = self.inner.cache.get_slice(path).try_read_all() {
            return Ok(bytes.as_slice().to_vec());
        }
        self.inner.underlying.atomic_read(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.inner.cache.file_length(path).is_some())
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

fn list_index_files(index: &Index) -> tantivy::Result<HashSet<PathBuf>> {
    let index_meta = index.load_metas()?;
    let mut files: HashSet<PathBuf> = index_meta
        .segments
        .into_iter()
        .flat_map(|segment_meta| segment_meta.list_files())
        .collect();
    files.insert(Path::new("meta.json").to_path_buf());
    files.insert(Path::new(".managed.json").to_path_buf());
    Ok(files)
}

pub(crate) fn write_hotcache<D: Directory>(
    directory: D,
    output: &mut dyn Write,
) -> Result<(), HelixDbError> {
    let caching_directory = CachingDirectory::new(
        Arc::new(directory),
        super::byte_range_cache::ByteRangeCache::new(),
    );
    let debug_proxy_directory = DebugProxyDirectory::wrap(caching_directory);
    let index = Index::open(debug_proxy_directory.clone()).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to open Tantivy index for hotcache build: {err}"
        ))
    })?;
    let schema = index.schema();
    let reader: IndexReader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .map_err(|err: TantivyError| {
            HelixDbError::Config(format!(
                "failed to open Tantivy reader for hotcache build: {err}"
            ))
        })?;
    let searcher = reader.searcher();
    for (field, field_entry) in schema.fields() {
        if !field_entry.is_indexed() {
            continue;
        }
        for segment_reader in searcher.segment_readers() {
            let _ = segment_reader.inverted_index(field).map_err(|err| {
                HelixDbError::Config(format!(
                    "failed to read inverted index for hotcache build: {err}"
                ))
            })?;
        }
    }

    let mut cache_builder = StaticDirectoryCacheBuilder::default();
    let mut per_file_slices: HashMap<PathBuf, HashSet<Range<usize>>> = HashMap::new();
    for operation in debug_proxy_directory.drain_read_operations() {
        per_file_slices
            .entry(operation.path)
            .or_default()
            .insert(operation.offset..operation.offset + operation.num_bytes);
    }

    for file_path in list_index_files(&index).map_err(|err| {
        HelixDbError::Config(format!(
            "failed to list index files for hotcache build: {err}"
        ))
    })? {
        let file_slice = match debug_proxy_directory.open_read(&file_path) {
            Ok(file_slice) => file_slice,
            Err(OpenReadError::FileDoesNotExist(_)) => continue,
            Err(err) => {
                return Err(HelixDbError::Config(format!(
                    "failed to open '{}' for hotcache build: {err}",
                    file_path.display()
                )));
            }
        };

        let file_builder = cache_builder.add_file(&file_path, file_slice.len() as u64);
        if let Some(intervals) = per_file_slices.get(&file_path) {
            for byte_range in intervals {
                let file_name = file_path.to_string_lossy();
                if file_name.ends_with("store")
                    || file_name.ends_with("term")
                    || byte_range.len() < 10_000_000
                {
                    let bytes = file_slice
                        .read_bytes_slice(byte_range.clone())
                        .map_err(|err| {
                            HelixDbError::Config(format!(
                                "failed to read '{}' slice {}..{} for hotcache build: {err}",
                                file_path.display(),
                                byte_range.start,
                                byte_range.end
                            ))
                        })?;
                    file_builder.add_bytes(bytes.as_slice(), byte_range.start);
                }
            }
        }
    }

    cache_builder.write(output)
}

#[cfg(test)]
mod tests {
    use super::{
        write_hotcache, HotDirectory, HotDirectoryMeta, SliceCacheIndex, SliceCacheIndexEntry,
        StaticDirectoryCache, StaticDirectoryCacheBuilder, StaticSliceCache,
        StaticSliceCacheBuilder,
    };
    use crate::error::HelixDbError;
    use std::collections::HashMap;
    use std::io::{self, Write};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tantivy::directory::{MmapDirectory, OwnedBytes, RamDirectory, WatchCallback};
    use tantivy::schema::{
        IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions,
    };
    use tantivy::{doc, Directory, Index};

    struct FailingWriter {
        writes_before_error: usize,
        fail_flush: bool,
    }

    impl Write for FailingWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.writes_before_error == 0 {
                return Err(io::Error::other("injected write failure"));
            }
            self.writes_before_error -= 1;
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            if self.fail_flush {
                Err(io::Error::other("injected flush failure"))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn static_slice_cache_supports_subranges() -> Result<(), HelixDbError> {
        let mut builder = StaticSliceCacheBuilder::new(10);
        builder.add_bytes(b"abc", 2);
        builder.add_bytes(b"def", 5);
        let cache = super::StaticSliceCache::open(OwnedBytes::new(builder.flush()?))?;
        assert_eq!(cache.try_read_bytes(3..7).unwrap().as_slice(), b"bcde");
        Ok(())
    }

    #[test]
    fn hot_directory_uses_cached_file_bytes() -> Result<(), HelixDbError> {
        let directory = RamDirectory::default();
        directory
            .atomic_write(Path::new("meta.json"), b"meta")
            .map_err(|err| HelixDbError::Config(err.to_string()))?;
        let mut cache_builder = super::StaticDirectoryCacheBuilder::default();
        cache_builder
            .add_file(Path::new("meta.json"), 4)
            .add_bytes(b"meta", 0);
        let mut hotcache = Vec::new();
        cache_builder.write(&mut hotcache)?;
        let hot_directory = HotDirectory::open(Arc::new(directory), &hotcache)?;
        assert_eq!(
            hot_directory.atomic_read(Path::new("meta.json")).unwrap(),
            b"meta"
        );
        Ok(())
    }

    #[test]
    fn hotcache_open_and_builder_reject_malformed_payloads() {
        assert!(
            super::StaticDirectoryCache::open(OwnedBytes::new(vec![1, 2, 3]))
                .expect_err("short directory payload")
                .to_string()
                .contains("hotcache payload is too short")
        );
        assert!(
            super::StaticSliceCache::open(OwnedBytes::new(vec![1, 2, 3]))
                .expect_err("short slice payload")
                .to_string()
                .contains("hotcache slice payload is too short")
        );

        let mut builder = StaticSliceCacheBuilder::new(10);
        builder.add_bytes(b"abc", 0);
        builder.add_bytes(b"def", 2);
        assert!(builder
            .flush()
            .expect_err("overlapping slices fail")
            .to_string()
            .contains("overlapping slices"));

        assert!(
            StaticDirectoryCache::open(OwnedBytes::new(vec![1, 0, 0, 0]))
                .expect_err("metadata beyond payload fails")
                .to_string()
                .contains("metadata exceeds payload size")
        );
        assert!(
            StaticDirectoryCache::open(OwnedBytes::new(vec![1, 0, 0, 0, 0xff]))
                .expect_err("invalid metadata fails")
                .to_string()
                .contains("failed to decode hotcache meta")
        );

        let mut invalid_payload_len = vec![0; std::mem::size_of::<u64>()];
        invalid_payload_len.copy_from_slice(&1_u64.to_le_bytes());
        assert!(StaticSliceCache::open(OwnedBytes::new(invalid_payload_len))
            .expect_err("invalid payload length fails")
            .to_string()
            .contains("payload length is invalid"));
        let mut invalid_index = vec![0xff];
        invalid_index.extend_from_slice(&0_u64.to_le_bytes());
        assert!(StaticSliceCache::open(OwnedBytes::new(invalid_index))
            .expect_err("invalid index fails")
            .to_string()
            .contains("failed to decode hotcache slice index"));

        let meta = HotDirectoryMeta {
            file_lengths: HashMap::from([(PathBuf::from("segment.term"), 3)]),
            slice_offsets: vec![(PathBuf::from("segment.term"), 0)],
        };
        let meta_bytes = bincode::serialize(&meta).unwrap();
        let mut invalid_nested_cache = (meta_bytes.len() as u32).to_le_bytes().to_vec();
        invalid_nested_cache.extend_from_slice(&meta_bytes);
        invalid_nested_cache.extend_from_slice(&[1, 2, 3]);
        assert!(
            StaticDirectoryCache::open(OwnedBytes::new(invalid_nested_cache))
                .expect_err("invalid nested slice fails")
                .to_string()
                .contains("hotcache slice payload is too short")
        );
    }

    #[test]
    fn static_cache_indexes_cover_empty_missing_and_disjoint_ranges() {
        let index = SliceCacheIndex {
            total_len: 12,
            slices: vec![
                SliceCacheIndexEntry {
                    start: 2,
                    stop: 5,
                    addr: 0,
                },
                SliceCacheIndexEntry {
                    start: 8,
                    stop: 10,
                    addr: 3,
                },
            ],
        };
        assert!(!index.is_complete());
        assert_eq!(index.get(2..5), Some(0));
        assert_eq!(index.get(3..5), Some(1));
        assert_eq!(index.get(0..1), None);
        assert_eq!(index.get(4..6), None);

        let mut builder = StaticSliceCacheBuilder::new(12);
        builder.add_bytes(b"abc", 2);
        builder.add_bytes(b"de", 8);
        let cache = StaticSliceCache::open(OwnedBytes::new(builder.flush().unwrap())).unwrap();
        assert_eq!(cache.try_read_bytes(4..4).unwrap().as_slice(), b"");
        assert!(cache.try_read_bytes(5..8).is_none());
        assert!(cache.try_read_all().is_none());

        let directory_cache = StaticDirectoryCache {
            file_lengths: HashMap::new(),
            slices: HashMap::new(),
        };
        assert_eq!(
            directory_cache
                .get_slice(Path::new("missing"))
                .try_read_bytes(0..0)
                .unwrap()
                .as_slice(),
            b""
        );
    }

    #[test]
    fn hotcache_builder_maps_each_output_failure() {
        for (writes_before_error, expected) in [
            (0, "failed to write hotcache meta len"),
            (1, "failed to write hotcache meta"),
            (2, "failed to write hotcache payload"),
        ] {
            let mut builder = StaticDirectoryCacheBuilder::default();
            builder
                .add_file(Path::new("segment.term"), 3)
                .add_bytes(b"abc", 0);
            let mut writer = FailingWriter {
                writes_before_error,
                fail_flush: false,
            };
            assert!(builder
                .write(&mut writer)
                .unwrap_err()
                .to_string()
                .contains(expected));
        }

        let mut builder = StaticDirectoryCacheBuilder::default();
        builder
            .add_file(Path::new("segment.term"), 3)
            .add_bytes(b"abc", 0);
        let mut writer = FailingWriter {
            writes_before_error: 3,
            fail_flush: true,
        };
        assert!(builder
            .write(&mut writer)
            .unwrap_err()
            .to_string()
            .contains("failed to flush hotcache bytes"));
    }

    #[test]
    fn write_hotcache_roundtrips_a_real_tantivy_index() {
        let dir = tempfile::tempdir().unwrap();
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
        let index = Index::create_in_dir(dir.path(), schema_builder.build()).unwrap();
        let mut writer = index.writer(15_000_000).unwrap();
        writer
            .add_document(doc!(entity_id => 1_u64, body => "hello hotcache"))
            .unwrap();
        writer.commit().unwrap();
        drop(writer);
        drop(index);

        let directory = MmapDirectory::open(dir.path()).unwrap();
        let mut hotcache = Vec::new();
        write_hotcache(directory.clone(), &mut hotcache).unwrap();
        assert!(!hotcache.is_empty());

        let hot_directory = HotDirectory::open(Arc::new(directory), &hotcache).unwrap();
        let index = Index::open(hot_directory).unwrap();
        let reader = index.reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 1);
    }

    #[test]
    fn write_hotcache_rejects_a_non_index_directory() {
        let error = write_hotcache(RamDirectory::default(), &mut Vec::new()).unwrap_err();
        assert!(error.to_string().contains("failed to open Tantivy index"));
    }

    #[tokio::test]
    async fn hot_directory_falls_back_for_uncached_ranges_and_supports_directory_methods(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let directory = RamDirectory::default();
        directory.atomic_write(Path::new("segment.term"), b"abcdefghij")?;
        let mut cache_builder = super::StaticDirectoryCacheBuilder::default();
        cache_builder
            .add_file(Path::new("segment.term"), 10)
            .add_bytes(b"cde", 2);
        let mut hotcache = Vec::new();
        cache_builder.write(&mut hotcache)?;
        let hot_directory = HotDirectory::open(Arc::new(directory), &hotcache)?;

        assert_eq!(format!("{hot_directory:?}"), "HotDirectory");
        assert!(hot_directory.exists(Path::new("segment.term"))?);
        assert!(!hot_directory.exists(Path::new("missing"))?);
        assert!(matches!(
            hot_directory.get_file_handle(Path::new("missing")),
            Err(tantivy::directory::error::OpenReadError::FileDoesNotExist(
                _
            ))
        ));

        let handle = hot_directory.get_file_handle(Path::new("segment.term"))?;
        assert_eq!(handle.len(), 10);
        assert!(format!("{handle:?}").contains("FileSliceWithCache"));
        assert_eq!(handle.read_bytes(2..5)?.as_slice(), b"cde");
        assert_eq!(handle.read_bytes(4..4)?.as_slice(), b"");
        assert_eq!(handle.read_bytes(0..2)?.as_slice(), b"ab");
        assert_eq!(handle.read_bytes_async(4..4).await?.as_slice(), b"");
        assert_eq!(handle.read_bytes_async(5..8).await?.as_slice(), b"fgh");
        assert_eq!(
            hot_directory
                .open_read(Path::new("segment.term"))?
                .read_bytes()?
                .as_slice(),
            b"abcdefghij"
        );
        assert_eq!(
            hot_directory
                .atomic_read(Path::new("segment.term"))?
                .as_slice(),
            b"abcdefghij"
        );

        let _watch = hot_directory
            .watch(WatchCallback::new(|| {}))
            .expect("watch succeeds");
        let lock = tantivy::directory::Lock {
            filepath: Path::new("hot.lock").to_path_buf(),
            is_blocking: false,
        };
        let _guard = hot_directory.acquire_lock(&lock).expect("lock succeeds");

        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = hot_directory.delete(Path::new("segment.term"));
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = hot_directory.open_write(Path::new("segment.term"));
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = hot_directory.atomic_write(Path::new("segment.term"), b"new");
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = hot_directory.sync_directory();
        }))
        .is_err());
        Ok(())
    }
}
