//! Production-format benchmark fixture for exact traversal-scoped FTS.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};

use super::{
    analyze_text, persist_documents_as_split, RestrictedTextCandidates, SplitSearchReader,
    TextDocumentInput, TextSearchCandidate, TextSearchRuntime, TextSearchScope, TextSplitRef,
};
use crate::config::TextIndexDefinition;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::text::statistics::TextBm25Statistics;

const DB_PATH: &str = "fts-prefilter-benchmark";
const LABEL: &str = "FtsPrefilterDocument";
const PROPERTY: &str = "body";
const QUERY_TERMS: [&str; 3] = ["rareterm", "mediumterm", "commonterm"];
const CANDIDATE_PERMUTATION_MULTIPLIER: u128 = 65_537;

/// Physical corpus shape measured before or after production-format compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtsPrefilterBenchmarkLayout {
    MultiSplit,
    Compacted,
}

impl FtsPrefilterBenchmarkLayout {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MultiSplit => "multi_split",
            Self::Compacted => "compacted",
        }
    }
}

/// Restricted collector or unrestricted baseline selected for one sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtsPrefilterBenchmarkStrategy {
    Collector,
    Unrestricted,
}

impl FtsPrefilterBenchmarkStrategy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Collector => "collector",
            Self::Unrestricted => "unrestricted",
        }
    }
}

/// One validated point in the FTS prefilter benchmark matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FtsPrefilterBenchmarkCase {
    pub layout: FtsPrefilterBenchmarkLayout,
    pub strategy: FtsPrefilterBenchmarkStrategy,
    pub candidate_count: usize,
    pub query: &'static str,
    pub k: usize,
}

impl FtsPrefilterBenchmarkCase {
    pub fn try_new(
        layout: FtsPrefilterBenchmarkLayout,
        strategy: FtsPrefilterBenchmarkStrategy,
        candidate_count: usize,
        query: &'static str,
        k: usize,
        document_count: usize,
    ) -> Result<Self> {
        if candidate_count == 0 || candidate_count > document_count {
            return Err(HelixDbError::Config(format!(
                "FTS prefilter benchmark candidate count must be in 1..={document_count}"
            )));
        }
        if !QUERY_TERMS.contains(&query) {
            return Err(HelixDbError::Config(format!(
                "FTS prefilter benchmark query must be one of {QUERY_TERMS:?}"
            )));
        }
        if k == 0 {
            return Err(HelixDbError::Config(
                "FTS prefilter benchmark k must be positive".to_string(),
            ));
        }
        Ok(Self {
            layout,
            strategy,
            candidate_count,
            query,
            k,
        })
    }
}

/// Non-allocator measurements produced by one exact search sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsPrefilterBenchmarkSample {
    pub split_count: usize,
    pub candidate_count: usize,
    pub object_store_reads: u64,
    pub object_store_bytes: u64,
    pub result_count: usize,
    pub result_digest: String,
}

#[derive(Debug)]
struct MeasuredReadObjectStore {
    inner: Arc<dyn ObjectStore>,
    reads: AtomicU64,
    bytes: AtomicU64,
}

impl MeasuredReadObjectStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            reads: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.reads.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.reads.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

impl fmt::Display for MeasuredReadObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FTS prefilter benchmark local store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for MeasuredReadObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let result = self.inner.get_opts(location, options).await?;
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(
            result.range.end.saturating_sub(result.range.start),
            Ordering::Relaxed,
        );
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Reusable 100k-document production split fixture for the full benchmark matrix.
pub struct FtsPrefilterBenchmarkFixture {
    _temporary: tempfile::TempDir,
    store: Arc<MeasuredReadObjectStore>,
    _object_store: Arc<dyn ObjectStore>,
    definition: TextIndexDefinition,
    document_count: usize,
    multi_split: Vec<TextSplitRef>,
    compacted: Vec<TextSplitRef>,
    multi_split_readers: Vec<SplitSearchReader>,
    compacted_readers: Vec<SplitSearchReader>,
    statistics: BTreeMap<&'static str, TextBm25Statistics>,
    exhaustive: BTreeMap<&'static str, Vec<TextSearchCandidate>>,
}

impl FtsPrefilterBenchmarkFixture {
    /// Builds the same immutable split format and BM25 statistics used by production serving.
    pub async fn try_new(document_count: usize, split_count: usize) -> Result<Self> {
        if document_count == 0 || split_count == 0 || split_count > document_count {
            return Err(HelixDbError::Config(
                "FTS prefilter benchmark requires documents >= splits > 0".to_string(),
            ));
        }
        let temporary = tempfile::tempdir().map_err(|error| {
            HelixDbError::Config(format!(
                "failed to create FTS prefilter benchmark directory: {error}"
            ))
        })?;
        let local: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temporary.path())?);
        let store = Arc::new(MeasuredReadObjectStore::new(local));
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let definition = TextIndexDefinition::new_node(LABEL, PROPERTY)?;
        let documents = (0..document_count)
            .map(|entity_id| {
                TextDocumentInput::new(
                    u64::try_from(entity_id).expect("benchmark entity ID fits u64"),
                    benchmark_document(entity_id),
                )
            })
            .collect::<Vec<_>>();

        let chunk_size = document_count.div_ceil(split_count);
        let mut multi_split = Vec::with_capacity(split_count);
        for chunk in documents.chunks(chunk_size) {
            multi_split.push(
                persist_documents_as_split(&object_store, DB_PATH, &definition, chunk)
                    .await?
                    .expect("non-empty benchmark chunks always create splits"),
            );
        }
        let compacted =
            vec![
                persist_documents_as_split(&object_store, DB_PATH, &definition, &documents)
                    .await?
                    .expect("non-empty benchmark corpus always creates a compacted split"),
            ];

        let mut total_token_count = 0_u64;
        let mut document_frequencies = BTreeMap::<Bytes, u64>::new();
        for document in &documents {
            let analyzed = analyze_text(definition.analyzer(), &document.text);
            total_token_count = total_token_count.saturating_add(analyzed.token_count);
            for term in analyzed.unique_terms {
                document_frequencies
                    .entry(term)
                    .and_modify(|frequency| *frequency = frequency.saturating_add(1))
                    .or_insert(1);
            }
        }
        let statistics = QUERY_TERMS
            .into_iter()
            .map(|query| {
                (
                    query,
                    TextBm25Statistics::for_benchmark(
                        u64::try_from(document_count).expect("benchmark document count fits u64"),
                        total_token_count,
                        document_frequencies.clone(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let runtime = TextSearchRuntime::new(&object_store, DB_PATH, None);
        let multi_split_readers = futures::stream::iter(multi_split.iter())
            .map(|split| SplitSearchReader::open(&runtime, split))
            .buffered(8)
            .try_collect::<Vec<_>>()
            .await?;
        let compacted_readers = futures::stream::iter(compacted.iter())
            .map(|split| SplitSearchReader::open(&runtime, split))
            .buffered(8)
            .try_collect::<Vec<_>>()
            .await?;
        let mut fixture = Self {
            _temporary: temporary,
            store,
            _object_store: object_store,
            definition,
            document_count,
            multi_split,
            compacted,
            multi_split_readers,
            compacted_readers,
            statistics,
            exhaustive: BTreeMap::new(),
        };
        for query in QUERY_TERMS {
            let scope = TextSearchScope::Unrestricted;
            let hits = fixture
                .search_layout(
                    FtsPrefilterBenchmarkLayout::Compacted,
                    &scope,
                    query,
                    document_count,
                )
                .await?;
            fixture.exhaustive.insert(query, hits);
        }
        fixture.store.reset();
        Ok(fixture)
    }

    pub const fn document_count(&self) -> usize {
        self.document_count
    }

    /// Executes one cold production-format sample and verifies its exact oracle first.
    pub async fn run_case(
        &self,
        case: FtsPrefilterBenchmarkCase,
    ) -> Result<FtsPrefilterBenchmarkSample> {
        let candidates = match case.strategy {
            FtsPrefilterBenchmarkStrategy::Collector => {
                Some(Arc::new(RestrictedTextCandidates::from_ids(
                    benchmark_candidate_ids(self.document_count, case.candidate_count),
                )?))
            }
            FtsPrefilterBenchmarkStrategy::Unrestricted => None,
        };
        let scope = match &candidates {
            Some(candidates) => TextSearchScope::restricted(Arc::clone(candidates)),
            None => TextSearchScope::Unrestricted,
        };

        self.store.reset();
        let hits = self
            .search_layout(case.layout, &scope, case.query, case.k)
            .await?;
        let (object_store_reads, object_store_bytes) = self.store.snapshot();
        self.assert_exact(case, candidates.as_deref(), &hits)?;
        Ok(FtsPrefilterBenchmarkSample {
            split_count: self.splits(case.layout).len(),
            candidate_count: case.candidate_count,
            object_store_reads,
            object_store_bytes,
            result_count: hits.len(),
            result_digest: digest(&hits),
        })
    }

    fn assert_exact(
        &self,
        case: FtsPrefilterBenchmarkCase,
        candidates: Option<&RestrictedTextCandidates>,
        actual: &[TextSearchCandidate],
    ) -> Result<()> {
        let exhaustive = self
            .exhaustive
            .get(case.query)
            .expect("validated benchmark query has an exhaustive result");
        let expected = exhaustive
            .iter()
            .filter(|hit| candidates.is_none_or(|candidates| candidates.contains(hit.entity_id)))
            .take(case.k)
            .map(|hit| (hit.entity_id, hit.score.to_bits()))
            .collect::<Vec<_>>();
        let actual = actual
            .iter()
            .map(|hit| (hit.entity_id, hit.score.to_bits()))
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(HelixDbError::InvariantViolation(format!(
                "FTS prefilter benchmark result differs from exhaustive filtered BM25: expected {expected:?}, actual {actual:?}"
            )));
        }
        Ok(())
    }

    async fn search_layout(
        &self,
        layout: FtsPrefilterBenchmarkLayout,
        scope: &TextSearchScope,
        query: &str,
        k: usize,
    ) -> Result<Vec<TextSearchCandidate>> {
        let statistics = self
            .statistics
            .get(query)
            .expect("validated benchmark query has exact statistics");
        let split_hits = futures::stream::iter(self.readers(layout).iter())
            .map(|reader| async {
                reader.warm(self.definition.analyzer(), query).await?;
                reader.search_candidates(
                    self.definition.analyzer(),
                    query,
                    k,
                    Some(statistics),
                    scope,
                    0,
                )
            })
            .buffered(8)
            .try_collect::<Vec<_>>()
            .await?;
        let mut hits = split_hits.into_iter().flatten().collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.entity_id.cmp(&right.entity_id))
        });
        let mut seen = BTreeSet::new();
        hits.retain(|hit| seen.insert(hit.entity_id));
        hits.truncate(k);
        Ok(hits)
    }

    fn splits(&self, layout: FtsPrefilterBenchmarkLayout) -> &[TextSplitRef] {
        match layout {
            FtsPrefilterBenchmarkLayout::MultiSplit => &self.multi_split,
            FtsPrefilterBenchmarkLayout::Compacted => &self.compacted,
        }
    }

    fn readers(&self, layout: FtsPrefilterBenchmarkLayout) -> &[SplitSearchReader] {
        match layout {
            FtsPrefilterBenchmarkLayout::MultiSplit => &self.multi_split_readers,
            FtsPrefilterBenchmarkLayout::Compacted => &self.compacted_readers,
        }
    }
}

fn benchmark_candidate_ids(
    document_count: usize,
    candidate_count: usize,
) -> impl Iterator<Item = u64> {
    let document_count = document_count as u128;
    (0..candidate_count).map(move |ordinal| {
        let permuted = (ordinal as u128 * CANDIDATE_PERMUTATION_MULTIPLIER) % document_count;
        u64::try_from(permuted).expect("benchmark entity ID fits u64")
    })
}

fn benchmark_document(entity_id: usize) -> String {
    let mut terms = Vec::with_capacity(12);
    if entity_id.is_multiple_of(1_000) {
        terms.extend(std::iter::repeat_n("rareterm", 1 + entity_id % 3));
    }
    if entity_id.is_multiple_of(10) {
        terms.extend(std::iter::repeat_n("mediumterm", 1 + entity_id % 4));
    }
    if !entity_id.is_multiple_of(5) {
        terms.extend(std::iter::repeat_n("commonterm", 1 + entity_id % 5));
    }
    if terms.is_empty() {
        terms.push("backgroundterm");
    }
    terms.extend(std::iter::repeat_n("padding", entity_id % 7));
    terms.join(" ")
}

fn digest(hits: &[TextSearchCandidate]) -> String {
    let mut digest = Sha256::new();
    for hit in hits {
        digest.update(hit.entity_id.to_be_bytes());
        digest.update(hit.score.to_bits().to_be_bytes());
    }
    digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut encoded, byte| {
            write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
            encoded
        })
}
