QUERY SearchRecentDocuments (vector: [F32], limit: I64, cutoff_date: Date) =>
    documents <- SearchV<Document>(vector, limit)::WHERE(_::{created_at}::GTE(cutoff_date))
    RETURN documents

QUERY InsertVector (vector: [F32], content: String, created_at: Date) =>
    document <- AddV<Document>(vector, { content: content, created_at: created_at })
    doc <- document::{content, created_at}
    RETURN document
