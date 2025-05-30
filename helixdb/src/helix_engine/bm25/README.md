# BM25 Implementation for HelixDB

This module provides a complete BM25 (Best Matching 25) implementation for full-text search in your graph vector database. BM25 is a probabilistic ranking function used by search engines to estimate the relevance of documents to a given search query.

## Features

- **Disk-based storage**: Uses LMDB for persistent inverted index storage
- **Full BM25 scoring**: Implements the complete BM25 algorithm with configurable parameters
- **CRUD operations**: Support for inserting, updating, and deleting documents
- **Hybrid search ready**: Designed to work alongside vector similarity search
- **Efficient tokenization**: Includes text preprocessing and tokenization
- **Scalable**: Handles large document collections efficiently

## Architecture

The BM25 implementation uses four LMDB databases:

1. **Inverted Index** (`bm25_inverted_index`): Maps terms to posting lists containing document IDs and term frequencies
2. **Document Lengths** (`bm25_doc_lengths`): Stores the length of each indexed document
3. **Term Frequencies** (`bm25_term_frequencies`): Stores document frequency for each term
4. **Metadata** (`bm25_metadata`): Stores global statistics like total documents and average document length

## Usage

### Basic Text Search

```rust
use helixdb::helix_engine::{
    bm25::BM25,
    storage_core::storage_core::HelixGraphStorage,
};

// Assuming you have a HelixGraphStorage instance
let storage = HelixGraphStorage::new(db_path, config)?;

// Index a document
let doc_id = node.id;
let text = "The quick brown fox jumps over the lazy dog";
storage.insert_doc(doc_id, text)?;

// Search for documents
let results = storage.search("quick fox", 10)?;
for (doc_id, score) in results {
    println!("Document {}: Score {:.4}", doc_id, score);
}
```

### Document Management

```rust
// Update a document (deletes old and re-indexes)
storage.update_doc(doc_id, "Updated text content")?;

// Delete a document from the index
storage.delete_doc(doc_id)?;
```

### Hybrid Search (BM25 + Vector Similarity)

```rust
use helixdb::helix_engine::bm25::HybridSearch;

// Combine BM25 text search with vector similarity
let query_text = "machine learning";
let query_vector = Some(&[0.1, 0.2, 0.3, ...]); // Your query vector
let alpha = 0.7; // Weight for BM25 vs vector similarity (0.7 = 70% BM25, 30% vector)
let limit = 10;

let results = storage.hybrid_search(query_text, query_vector, alpha, limit)?;
```

### Automatic Node Indexing

The implementation automatically extracts text from nodes by combining:
- Node label
- All property keys and values

```rust
// This node will be indexed as: "Person name John Doe age 30"
let node = Node {
    id: uuid,
    label: "Person".to_string(),
    properties: Some(hashmap!{
        "name".to_string() => Value::String("John Doe".to_string()),
        "age".to_string() => Value::Integer(30),
    }),
};
```

## BM25 Algorithm Details

The implementation uses the standard BM25 formula:

```
score(D,Q) = Σ IDF(qi) * (f(qi,D) * (k1 + 1)) / (f(qi,D) + k1 * (1 - b + b * |D| / avgdl))
```

Where:
- `D` is a document
- `Q` is a query
- `qi` is the i-th query term
- `f(qi,D)` is the term frequency of qi in document D
- `|D|` is the length of document D
- `avgdl` is the average document length
- `k1` and `b` are tuning parameters (default: k1=1.2, b=0.75)
- `IDF(qi)` is the inverse document frequency of qi

## Configuration

Default BM25 parameters:
- `k1 = 1.2`: Controls term frequency saturation
- `b = 0.75`: Controls length normalization

These can be adjusted based on your specific use case:
- Higher `k1` values give more weight to term frequency
- Higher `b` values give more weight to document length normalization

## Performance Considerations

1. **Indexing**: O(n) where n is the number of unique terms in the document
2. **Search**: O(m * k) where m is the number of query terms and k is the average posting list length
3. **Storage**: Efficient disk-based storage with LMDB's memory-mapped files
4. **Memory**: Minimal memory usage as data is stored on disk

## Integration with Vector Search

The BM25 implementation is designed to work seamlessly with your existing vector similarity search:

1. **Complementary**: BM25 handles exact term matching while vectors handle semantic similarity
2. **Hybrid scoring**: Combine scores using weighted averages
3. **Fallback**: Use BM25 when vector search returns insufficient results
4. **Filtering**: Use BM25 to pre-filter candidates for vector search

## Example Use Cases

1. **Document Search**: Full-text search across node properties
2. **Hybrid Retrieval**: Combine keyword and semantic search
3. **Query Expansion**: Use BM25 to find related terms for vector queries
4. **Faceted Search**: Filter by text criteria before vector similarity
5. **Autocomplete**: Fast prefix matching for search suggestions

## Error Handling

The implementation provides comprehensive error handling:
- Database connection errors
- Serialization/deserialization errors
- Missing document errors
- Invalid query errors

All errors are wrapped in the `GraphError` type for consistent error handling across the system. 