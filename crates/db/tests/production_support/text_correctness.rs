use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use db::config::TextAnalyzerKind;
use futures::stream::BoxStream;
use futures::StreamExt;
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, TermQuery};
use tantivy::schema::{IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{
    Language, LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer, WhitespaceTokenizer,
};
use tantivy::{Index, ReloadPolicy, TantivyDocument, Term};
use tokio::sync::Notify;

const ORACLE_BODY_FIELD: &str = "body";
const ORACLE_ENTITY_ID_FIELD: &str = "entity_id";
pub const ORACLE_MAX_TOKEN_LEN: usize = u16::MAX as usize - 5;

#[derive(Debug, Clone, Copy)]
pub struct OracleDocument<'a> {
    pub entity_id: u64,
    pub text: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleHit {
    pub entity_id: u64,
    pub score_bits: u32,
}

pub fn analyze_text(kind: TextAnalyzerKind, text: &str) -> Vec<String> {
    let mut analyzer = oracle_analyzer(kind);
    let mut stream = analyzer.token_stream(text);
    let mut terms = Vec::new();
    stream.process(&mut |token| {
        if token.text.len() <= ORACLE_MAX_TOKEN_LEN {
            terms.push(token.text.clone());
        }
    });
    terms
}

pub fn search_live_corpus(
    analyzer: TextAnalyzerKind,
    documents: &[OracleDocument<'_>],
    query: &str,
    limit: usize,
) -> Vec<OracleHit> {
    if documents.is_empty() || limit == 0 {
        return Vec::new();
    }

    let mut schema = Schema::builder();
    let entity_id = schema.add_u64_field(
        ORACLE_ENTITY_ID_FIELD,
        NumericOptions::default().set_indexed().set_fast(),
    );
    let body = schema.add_text_field(
        ORACLE_BODY_FIELD,
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(analyzer.as_str())
                .set_index_option(IndexRecordOption::WithFreqs),
        ),
    );
    let index = Index::create_in_ram(schema.build());
    index
        .tokenizers()
        .register(analyzer.as_str(), oracle_analyzer(analyzer));
    let mut writer = index.writer(15_000_000).expect("oracle writer opens");
    for document in documents {
        let mut tantivy_document = TantivyDocument::default();
        tantivy_document.add_u64(entity_id, document.entity_id);
        tantivy_document.add_text(body, document.text);
        writer
            .add_document(tantivy_document)
            .expect("oracle document is admitted");
    }
    writer.commit().expect("oracle corpus commits");

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .expect("oracle reader opens");
    reader.reload().expect("oracle reader reloads");
    let searcher = reader.searcher();
    let terms = analyze_text(analyzer, query)
        .into_iter()
        .collect::<BTreeSet<_>>();
    if terms.is_empty() {
        return Vec::new();
    }
    let query = BooleanQuery::new(
        terms
            .into_iter()
            .map(|term| {
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, &term),
                        IndexRecordOption::WithFreqs,
                    )) as Box<dyn tantivy::query::Query>,
                )
            })
            .collect(),
    );
    let entity_ids = searcher
        .segment_readers()
        .iter()
        .map(|segment| {
            segment
                .fast_fields()
                .u64(ORACLE_ENTITY_ID_FIELD)
                .expect("oracle entity ID fast field exists")
        })
        .collect::<Vec<_>>();
    let mut hits = searcher
        .search(
            &query,
            &TopDocs::with_limit(documents.len()).order_by_score(),
        )
        .expect("oracle query executes")
        .into_iter()
        .map(|(score, address)| OracleHit {
            entity_id: entity_ids[address.segment_ord as usize]
                .first(address.doc_id)
                .expect("oracle hit has an entity ID"),
            score_bits: score.to_bits(),
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        f32::from_bits(right.score_bits)
            .total_cmp(&f32::from_bits(left.score_bits))
            .then_with(|| left.entity_id.cmp(&right.entity_id))
    });
    hits.truncate(limit);
    hits
}

fn oracle_analyzer(kind: TextAnalyzerKind) -> TextAnalyzer {
    match kind {
        TextAnalyzerKind::Standard => TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .build(),
        TextAnalyzerKind::StandardStemEn => TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(Stemmer::new(Language::English))
            .build(),
        TextAnalyzerKind::WhitespaceLowercase => {
            TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(LowerCaser)
                .build()
        }
    }
}

#[derive(Debug, Default)]
struct BarrierState {
    path: Option<Path>,
    entered: bool,
    released: bool,
}

#[derive(Debug, Default)]
pub struct BarrierObjectStore {
    inner: InMemory,
    barrier: Mutex<BarrierState>,
    changed: Notify,
    deleted: Arc<Mutex<Vec<Path>>>,
    fail_text_puts: AtomicUsize,
    text_puts: AtomicUsize,
}

impl BarrierObjectStore {
    pub fn arm_read(&self, path: Path) {
        *self.barrier.lock().expect("barrier mutex is healthy") = BarrierState {
            path: Some(path),
            entered: false,
            released: false,
        };
    }

    pub async fn wait_until_read_is_blocked(&self) {
        loop {
            let notified = self.changed.notified();
            if self
                .barrier
                .lock()
                .expect("barrier mutex is healthy")
                .entered
            {
                return;
            }
            notified.await;
        }
    }

    pub fn release_read(&self) {
        self.barrier
            .lock()
            .expect("barrier mutex is healthy")
            .released = true;
        self.changed.notify_waiters();
    }

    pub fn deleted_paths(&self) -> Vec<Path> {
        self.deleted
            .lock()
            .expect("deleted-path mutex is healthy")
            .clone()
    }

    pub fn fail_next_text_put(&self) {
        self.fail_text_puts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn text_put_count(&self) -> usize {
        self.text_puts.load(Ordering::Relaxed)
    }

    pub async fn text_blob_paths(&self) -> Vec<Path> {
        let mut paths = self
            .inner
            .list(None)
            .filter_map(|result| async {
                result
                    .ok()
                    .map(|meta| meta.location)
                    .filter(|path| path.to_string().contains("/fts/blobs/"))
            })
            .collect::<Vec<_>>()
            .await;
        paths.sort();
        paths
    }

    async fn pause_if_armed(&self, location: &Path) {
        let should_pause = {
            let mut barrier = self.barrier.lock().expect("barrier mutex is healthy");
            if barrier.path.as_ref() != Some(location) || barrier.released {
                false
            } else {
                barrier.entered = true;
                true
            }
        };
        if !should_pause {
            return;
        }
        self.changed.notify_waiters();
        loop {
            let notified = self.changed.notified();
            if self
                .barrier
                .lock()
                .expect("barrier mutex is healthy")
                .released
            {
                return;
            }
            notified.await;
        }
    }
}

impl fmt::Display for BarrierObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("barrier-memory")
    }
}

#[async_trait::async_trait]
impl ObjectStore for BarrierObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if location.to_string().contains("/fts/blobs/") {
            self.text_puts.fetch_add(1, Ordering::Relaxed);
            if self
                .fail_text_puts
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(slatedb::object_store::Error::Generic {
                    store: "barrier-memory",
                    source: Box::new(std::io::Error::other(
                        "injected content-addressed text upload failure",
                    )),
                });
            }
        }
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
        self.pause_if_armed(location).await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let deleted = Arc::clone(&self.deleted);
        let locations = locations
            .map(move |result| {
                if let Ok(path) = &result {
                    deleted
                        .lock()
                        .expect("deleted-path mutex is healthy")
                        .push(path.clone());
                }
                result
            })
            .boxed();
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
