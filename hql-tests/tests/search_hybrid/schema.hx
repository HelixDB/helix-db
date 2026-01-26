// Schema for SearchHybrid tests
// SearchHybrid combines vector similarity search with BM25 keyword search

// Vector schema for documents with embeddings
V::Document {
    content: String,
    title: String,
}

// Node schema for articles (used in some hybrid search scenarios)
N::Article {
    title: String,
    content: String,
    category: String,
}

// Edge connecting articles to their document embeddings
E::HasEmbedding {
    From: Article,
    To: Document,
}
