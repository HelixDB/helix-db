V::DocumentVector {
    content: String,
    source: String,
    created_at: Date
}

QUERY semantic_search(query_embedding: [F64], top_k: I32) =>
    candidates <- SearchV<DocumentVector>(query_embedding, top_k)
    reranked <- candidates::RerankMMR(lambda: 0.7)
    RETURN reranked::{id, label, score}

QUERY semantic_search_literal() =>
    candidates <- SearchV<DocumentVector>([0.1,0.2,0.3,0.4], 10)
    RETURN candidates::{id, score}
