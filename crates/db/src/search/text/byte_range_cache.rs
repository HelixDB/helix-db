use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tantivy::directory::OwnedBytes;

#[derive(PartialOrd, Ord, PartialEq, Eq)]
struct CacheKey<'a, T: ToOwned + ?Sized> {
    tag: Cow<'a, T>,
    range_start: usize,
}

impl<T: ToOwned + ?Sized> Clone for CacheKey<'_, T>
where
    T::Owned: Clone,
{
    fn clone(&self) -> Self {
        Self {
            tag: self.tag.clone(),
            range_start: self.range_start,
        }
    }
}

impl<T: ToOwned + ?Sized> CacheKey<'static, T> {
    fn from_owned(tag: T::Owned, range_start: usize) -> Self {
        Self {
            tag: Cow::Owned(tag),
            range_start,
        }
    }
}

impl<'a, T: ToOwned + ?Sized> CacheKey<'a, T> {
    fn from_borrowed(tag: &'a T, range_start: usize) -> Self {
        Self {
            tag: Cow::Borrowed(tag),
            range_start,
        }
    }
}

struct CacheValue {
    range_end: usize,
    bytes: OwnedBytes,
}

struct InnerByteRangeCache<T: 'static + ToOwned + ?Sized> {
    cache: BTreeMap<CacheKey<'static, T>, CacheValue>,
}

impl<T: 'static + ToOwned + ?Sized + Ord> InnerByteRangeCache<T>
where
    T::Owned: Clone,
{
    fn new() -> Self {
        Self {
            cache: BTreeMap::new(),
        }
    }

    fn get_slice(&mut self, tag: &T, byte_range: Range<usize>) -> Option<OwnedBytes> {
        if byte_range.is_empty() {
            return Some(OwnedBytes::empty());
        }

        let key = CacheKey::from_borrowed(tag, byte_range.start);
        let (cache_key, cache_value) = if let Some(found) = self.get_block(&key, byte_range.end) {
            found
        } else {
            self.merge_ranges(&key, byte_range.end)?
        };

        let start = byte_range.start - cache_key.range_start;
        let end = byte_range.end - cache_key.range_start;
        Some(cache_value.bytes.slice(start..end))
    }

    fn missing_ranges(&mut self, tag: &T, byte_range: Range<usize>) -> Vec<Range<usize>> {
        if byte_range.is_empty() {
            return Vec::new();
        }

        let query = CacheKey::from_borrowed(tag, byte_range.start);
        let mut covered = Vec::new();

        if let Some((key, value)) = self
            .cache
            .range(..=query.clone())
            .next_back()
            .filter(|(key, value)| key.tag == query.tag && value.range_end > byte_range.start)
        {
            covered.push(key.range_start..value.range_end.min(byte_range.end));
        }

        covered.extend(
            self.cache
                .range(query..)
                .take_while(|(key, _)| key.tag.as_ref() == tag && key.range_start < byte_range.end)
                .map(|(key, value)| {
                    key.range_start.max(byte_range.start)..value.range_end.min(byte_range.end)
                }),
        );

        if covered.is_empty() {
            return vec![byte_range];
        }

        covered.sort_unstable_by_key(|range| range.start);
        let mut merged_covered = Vec::with_capacity(covered.len());
        let mut current = covered[0].clone();
        for next in covered.into_iter().skip(1) {
            if next.start <= current.end {
                current.end = current.end.max(next.end);
            } else {
                merged_covered.push(current);
                current = next;
            }
        }
        merged_covered.push(current);

        let mut missing = Vec::new();
        let mut cursor = byte_range.start;
        for covered in merged_covered {
            if cursor < covered.start {
                missing.push(cursor..covered.start);
            }
            cursor = cursor.max(covered.end);
        }
        if cursor < byte_range.end {
            missing.push(cursor..byte_range.end);
        }
        missing
    }

    fn put_slice(&mut self, tag: T::Owned, byte_range: Range<usize>, bytes: OwnedBytes) {
        let len = byte_range.end.saturating_sub(byte_range.start);
        if len == 0 {
            return;
        }
        if len != bytes.len() {
            return;
        }

        let start_key = CacheKey::from_borrowed(tag.borrow(), byte_range.start);
        let first_matching_block = self
            .get_block(&start_key, byte_range.start.saturating_add(1))
            .map(|(key, _)| key);

        let end_key = CacheKey::from_borrowed(tag.borrow(), byte_range.end.saturating_sub(1));
        let last_matching_block = self.get_block(&end_key, byte_range.end).map(|(key, _)| key);

        if first_matching_block.is_some() && first_matching_block == last_matching_block {
            return;
        }

        let first_matching_block = first_matching_block.unwrap_or(&start_key);
        let last_matching_block = last_matching_block.unwrap_or(&end_key);
        let overlapping = self
            .cache
            .range(first_matching_block..=last_matching_block)
            .map(|(key, value)| key.range_start..value.range_end)
            .collect::<Vec<_>>();

        let can_drop_first = overlapping
            .first()
            .map(|range| byte_range.start <= range.start)
            .unwrap_or(true);
        let can_drop_last = overlapping
            .last()
            .map(|range| byte_range.end >= range.end)
            .unwrap_or(true);

        let (final_range, final_bytes) = if can_drop_first && can_drop_last {
            (byte_range, bytes)
        } else {
            let start = if can_drop_first {
                byte_range.start
            } else {
                overlapping.first().expect("overlap start").start
            };
            let end = if can_drop_last {
                byte_range.end
            } else {
                overlapping.last().expect("overlap end").end
            };
            let mut buffer = Vec::with_capacity(end.saturating_sub(start));

            if !can_drop_first {
                let first_range = overlapping.first().expect("first overlap");
                let key = CacheKey::from_borrowed(tag.borrow(), first_range.start);
                let block = self.cache.get(&key).expect("first overlap block");
                let prefix_len = first_range.end.min(byte_range.start) - first_range.start;
                buffer.extend_from_slice(&block.bytes[..prefix_len]);
            }

            buffer.extend_from_slice(bytes.as_slice());

            if !can_drop_last {
                let last_range = overlapping.last().expect("last overlap");
                let key = CacheKey::from_borrowed(tag.borrow(), last_range.start);
                let block = self.cache.get(&key).expect("last overlap block");
                let suffix_start = last_range.start.max(byte_range.end) - last_range.start;
                buffer.extend_from_slice(&block.bytes[suffix_start..]);
            }

            (start..end, OwnedBytes::new(buffer))
        };

        let mut key = CacheKey::from_owned(tag, 0);
        for range in overlapping {
            key.range_start = range.start;
            self.cache.remove(&key);
        }

        key.range_start = final_range.start;
        self.cache.insert(
            key,
            CacheValue {
                range_end: final_range.end,
                bytes: final_bytes,
            },
        );
    }

    fn get_block<'a>(
        &self,
        query: &CacheKey<'a, T>,
        range_end: usize,
    ) -> Option<(&CacheKey<'a, T>, &CacheValue)> {
        self.cache
            .range(..=query)
            .next_back()
            .filter(|(key, value)| key.tag == query.tag && range_end <= value.range_end)
    }

    fn merge_ranges<'a>(
        &mut self,
        start: &CacheKey<'a, T>,
        range_end: usize,
    ) -> Option<(&CacheKey<'a, T>, &CacheValue)> {
        let own_key = |key: &CacheKey<T>| {
            CacheKey::from_owned(T::borrow(&key.tag).to_owned(), key.range_start)
        };

        let first_block = self.get_block(start, start.range_start)?;
        let overlapping_blocks = self
            .cache
            .range(first_block.0..)
            .take_while(|(key, _)| key.tag == start.tag && key.range_start <= range_end);

        let mut last_block = first_block;
        for (key, value) in overlapping_blocks.clone().skip(1) {
            if key.range_start != last_block.1.range_end {
                return None;
            }
            last_block = (key, value);
        }
        if last_block.1.range_end < range_end {
            return None;
        }

        let mut merged = Vec::with_capacity(last_block.1.range_end - first_block.0.range_start);
        for (_, value) in overlapping_blocks {
            merged.extend_from_slice(value.bytes.as_slice());
        }

        let first_owned_key = own_key(first_block.0);
        for (key, _) in self
            .cache
            .range(first_block.0..=last_block.0)
            .map(|(key, value)| (own_key(key), value.range_end))
            .collect::<Vec<_>>()
        {
            self.cache.remove(&key);
        }

        let merged_len = merged.len();
        self.cache.insert(
            first_owned_key.clone(),
            CacheValue {
                range_end: first_owned_key.range_start + merged_len,
                bytes: OwnedBytes::new(merged),
            },
        );

        self.cache.get_key_value(&first_owned_key)
    }
}

#[derive(Clone)]
pub(crate) struct ByteRangeCache {
    inner: Arc<Mutex<InnerByteRangeCache<Path>>>,
}

impl ByteRangeCache {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InnerByteRangeCache::new())),
        }
    }

    pub(crate) fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        self.inner
            .lock()
            .expect("byte range cache poisoned")
            .get_slice(path, byte_range)
    }

    pub(crate) fn put_slice(&self, path: PathBuf, byte_range: Range<usize>, bytes: OwnedBytes) {
        self.inner
            .lock()
            .expect("byte range cache poisoned")
            .put_slice(path, byte_range, bytes);
    }

    pub(crate) fn missing_ranges(
        &self,
        path: &Path,
        byte_range: Range<usize>,
    ) -> Vec<Range<usize>> {
        self.inner
            .lock()
            .expect("byte range cache poisoned")
            .missing_ranges(path, byte_range)
    }
}

impl Default for ByteRangeCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ByteRangeCache;
    use std::path::Path;
    use tantivy::directory::OwnedBytes;

    #[test]
    fn cache_serves_subranges_after_merge() {
        let cache = ByteRangeCache::new();
        let path = Path::new("segment.term");
        cache.put_slice(
            path.to_path_buf(),
            10..14,
            OwnedBytes::new(b"abcd".to_vec()),
        );
        cache.put_slice(
            path.to_path_buf(),
            14..18,
            OwnedBytes::new(b"efgh".to_vec()),
        );

        let bytes = cache.get_slice(path, 11..17).expect("merged bytes");
        assert_eq!(bytes.as_slice(), b"bcdefg");
    }

    #[test]
    fn cache_reports_only_missing_subranges() {
        let cache = ByteRangeCache::new();
        let path = Path::new("segment.term");
        cache.put_slice(path.to_path_buf(), 0..4, OwnedBytes::new(b"abcd".to_vec()));
        cache.put_slice(path.to_path_buf(), 8..12, OwnedBytes::new(b"ijkl".to_vec()));

        let missing = cache.missing_ranges(path, 2..10);
        assert_eq!(missing, vec![4..8]);
    }

    #[test]
    fn cache_treats_empty_ranges_as_already_available() {
        let cache = ByteRangeCache::default();
        let path = Path::new("segment.term");

        let bytes = cache.get_slice(path, 4..4).expect("empty hit");
        assert!(bytes.as_slice().is_empty());
        assert!(cache.missing_ranges(path, 4..4).is_empty());
    }

    #[test]
    fn cache_ignores_empty_and_mismatched_inserts() {
        let cache = ByteRangeCache::new();
        let path = Path::new("segment.term");

        cache.put_slice(path.to_path_buf(), 2..2, OwnedBytes::new(Vec::new()));
        cache.put_slice(
            path.to_path_buf(),
            2..5,
            OwnedBytes::new(b"too long".to_vec()),
        );

        assert!(cache.get_slice(path, 2..5).is_none());
        assert_eq!(cache.missing_ranges(path, 2..5), vec![2..5]);
    }

    #[test]
    fn cache_keeps_paths_isolated() {
        let cache = ByteRangeCache::new();
        let first_path = Path::new("segment-one.term");
        let second_path = Path::new("segment-two.term");
        cache.put_slice(
            first_path.to_path_buf(),
            0..4,
            OwnedBytes::new(b"abcd".to_vec()),
        );

        assert!(cache.get_slice(second_path, 0..4).is_none());
        assert_eq!(cache.missing_ranges(second_path, 0..4), vec![0..4]);
    }

    #[test]
    fn cache_does_not_merge_across_gaps() {
        let cache = ByteRangeCache::new();
        let path = Path::new("segment.term");
        cache.put_slice(path.to_path_buf(), 0..4, OwnedBytes::new(b"abcd".to_vec()));
        cache.put_slice(path.to_path_buf(), 6..10, OwnedBytes::new(b"ghij".to_vec()));

        assert!(cache.get_slice(path, 2..8).is_none());
        assert_eq!(cache.missing_ranges(path, 2..8), vec![4..6]);
    }

    #[test]
    fn cache_put_slice_preserves_existing_prefix_when_extending_right() {
        let cache = ByteRangeCache::new();
        let path = Path::new("segment.term");
        cache.put_slice(
            path.to_path_buf(),
            0..6,
            OwnedBytes::new(b"abcdef".to_vec()),
        );
        cache.put_slice(
            path.to_path_buf(),
            4..10,
            OwnedBytes::new(b"EFGHIJ".to_vec()),
        );

        let bytes = cache.get_slice(path, 0..10).expect("merged bytes");
        assert_eq!(bytes.as_slice(), b"abcdEFGHIJ");
    }

    #[test]
    fn cache_put_slice_preserves_existing_suffix_when_extending_left() {
        let cache = ByteRangeCache::new();
        let path = Path::new("segment.term");
        cache.put_slice(path.to_path_buf(), 6..10, OwnedBytes::new(b"ghij".to_vec()));
        cache.put_slice(
            path.to_path_buf(),
            0..8,
            OwnedBytes::new(b"ABCDEFGH".to_vec()),
        );

        let bytes = cache.get_slice(path, 0..10).expect("merged bytes");
        assert_eq!(bytes.as_slice(), b"ABCDEFGHij");
    }

    #[test]
    fn cache_put_slice_replaces_middle_and_bridges_neighbors() {
        let cache = ByteRangeCache::new();
        let path = Path::new("segment.term");
        cache.put_slice(path.to_path_buf(), 0..4, OwnedBytes::new(b"abcd".to_vec()));
        cache.put_slice(path.to_path_buf(), 8..12, OwnedBytes::new(b"ijkl".to_vec()));
        cache.put_slice(
            path.to_path_buf(),
            2..10,
            OwnedBytes::new(b"CDEFGHIJ".to_vec()),
        );

        let bytes = cache.get_slice(path, 0..12).expect("merged bytes");
        assert_eq!(bytes.as_slice(), b"abCDEFGHIJkl");
    }

    #[test]
    fn cache_put_slice_ignores_insert_already_inside_existing_block() {
        let cache = ByteRangeCache::new();
        let path = Path::new("segment.term");
        cache.put_slice(
            path.to_path_buf(),
            0..8,
            OwnedBytes::new(b"abcdefgh".to_vec()),
        );
        cache.put_slice(path.to_path_buf(), 2..4, OwnedBytes::new(b"XY".to_vec()));

        let bytes = cache.get_slice(path, 0..8).expect("original bytes");
        assert_eq!(bytes.as_slice(), b"abcdefgh");
    }
}
