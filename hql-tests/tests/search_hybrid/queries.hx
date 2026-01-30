// Test schema for hybrid search (vector + BM25)
V::Document {
    content: String,
}

N::Article {
    title: String,
    content: String,
}

// Test 1: Basic SearchHybrid with vector and text query
QUERY testHybridBasic(query_vec: [F64]) =>
    results <- SearchHybrid<Document>(query_vec, "search query", 10)
    RETURN results

// Test 2: SearchHybrid with Embed for vector
QUERY testHybridWithEmbed(query_text: String) =>
    results <- SearchHybrid<Document>(Embed(query_text), query_text, 10)
    RETURN results

// Test 3: SearchHybrid with variable k
QUERY testHybridVariableK(query_vec: [F64], search_text: String, k: I32) =>
    results <- SearchHybrid<Document>(query_vec, search_text, k)
    RETURN results

// Test 4: SearchHybrid with RerankRRF (main use case)
QUERY testHybridWithRRF(query_vec: [F64]) =>
    results <- SearchHybrid<Document>(query_vec, "search query", 10)
        ::RerankRRF
    RETURN results

// Test 5: SearchHybrid with RerankRRF custom k
QUERY testHybridWithRRFCustomK(query_vec: [F64], rrf_k: F64) =>
    results <- SearchHybrid<Document>(query_vec, "search query", 20)
        ::RerankRRF(k: rrf_k)
        ::RANGE(0, 10)
    RETURN results

// Test 6: SearchHybrid with RerankMMR for diversity
QUERY testHybridWithMMR(query_vec: [F64]) =>
    results <- SearchHybrid<Document>(query_vec, "search query", 20)
        ::RerankMMR(lambda: 0.7)
        ::RANGE(0, 10)
    RETURN results

// Test 7: SearchHybrid with identifier for query text
QUERY testHybridWithIdentifierQuery(query_vec: [F64], search_text: String) =>
    results <- SearchHybrid<Document>(query_vec, search_text, 10)
        ::RerankRRF
    RETURN results

// Test 8: SearchHybrid with chained rerankers
QUERY testHybridChainedRerankers(query_vec: [F64]) =>
    results <- SearchHybrid<Document>(query_vec, "search query", 50)
        ::RerankRRF(k: 60)
        ::RerankMMR(lambda: 0.5)
        ::RANGE(0, 10)
    RETURN results

// Test 9: SearchHybrid with Embed and variable query
QUERY testHybridEmbedVariable(query_text: String) =>
    results <- SearchHybrid<Document>(Embed(query_text), query_text, 15)
        ::RerankRRF
        ::RANGE(0, 5)
    RETURN results
