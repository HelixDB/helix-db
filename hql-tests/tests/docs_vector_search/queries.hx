// Vector search documentation examples

// Example 1: Basic vector search
QUERY SearchVector (vector: [F64], limit: I64) =>
    documents <- SearchV<Document>(vector, limit)
    RETURN documents

// Example 2: Vector search with postfiltering
QUERY SearchRecentDocuments (vector: [F64], limit: I64, cutoff_date: Date) =>
    documents <- SearchV<Document>(vector, limit)::WHERE(_::{created_at}::GTE(cutoff_date))
    RETURN documents

// Example 3: Using the built in Embed function
QUERY SearchWithText (text: String, limit: I64) =>
    documents <- SearchV<Document>(Embed(text), limit)
    RETURN documents

// Helper queries
QUERY InsertVector (vector: [F64], content: String, created_at: Date) =>
    document <- AddV<Document>(vector, { content: content, created_at: created_at })
    RETURN document

QUERY InsertTextAsVector (content: String, created_at: Date) =>
    document <- AddV<Document>(Embed(content), { content: content, created_at: created_at })
    RETURN document
