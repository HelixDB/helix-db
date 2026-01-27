// SelectV documentation examples

// Example 1: Selecting a vector by ID
QUERY GetDocumentVector (vector_id: ID) =>
    doc_vector <- V<Document>(vector_id)
    RETURN doc_vector

// Helper query to create document vectors
QUERY CreateDocumentVector (vector: [F64], content: String) =>
    doc_vector <- AddV<Document>(vector, {
        content: content
    })
    RETURN doc_vector
