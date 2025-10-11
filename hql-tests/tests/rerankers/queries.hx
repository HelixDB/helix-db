// Test schema for reranker functionality
V::Document {
    content: String,
}

N::Article {
    title: String,
    content: String,
}

// Test 1: RerankRRF with default k
QUERY testRRFDefault(query_vec: [F64]) =>
    results <- SearchV<Document>(query_vec, 100)
        ::RerankRRF
        ::RANGE(0, 10)
    RETURN results

// Test 2: RerankRRF with custom k parameter
QUERY testRRFCustomK(query_vec: [F64], k_val: F64) =>
    results <- SearchV<Document>(query_vec, 100)
        ::RerankRRF(k: k_val)
        ::RANGE(0, 10)
    RETURN results

// Test 3: RerankMMR with default distance (cosine)
QUERY testMMRDefault(query_vec: [F64]) =>
    results <- SearchV<Document>(query_vec, 100)
        ::RerankMMR(lambda: 0.7)
        ::RANGE(0, 10)
    RETURN results

// Test 4: RerankMMR with euclidean distance
QUERY testMMREuclidean(query_vec: [F64]) =>
    results <- SearchV<Document>(query_vec, 100)
        ::RerankMMR(lambda: 0.5, distance: "euclidean")
        ::RANGE(0, 10)
    RETURN results

// Test 5: RerankMMR with dot product distance
QUERY testMMRDotProduct(query_vec: [F64]) =>
    results <- SearchV<Document>(query_vec, 100)
        ::RerankMMR(lambda: 0.6, distance: "dotproduct")
        ::RANGE(0, 10)
    RETURN results

// Test 6: Chained rerankers (RRF then MMR)
QUERY testChainedRerankers(query_vec: [F64]) =>
    results <- SearchV<Document>(query_vec, 100)
        ::RerankRRF(k: 60)
        ::RerankMMR(lambda: 0.7)
        ::RANGE(0, 10)
    RETURN results

// Test 7: MMR with variable lambda
QUERY testMMRVariableLambda(query_vec: [F64], lambda_val: F64) =>
    results <- SearchV<Document>(query_vec, 100)
        ::RerankMMR(lambda: lambda_val)
        ::RANGE(0, 10)
    RETURN results

// Test 8: Multiple chained MMR rerankers
QUERY testMultipleMMR(query_vec: [F64]) =>
    results <- SearchV<Document>(query_vec, 100)
        ::RerankMMR(lambda: 0.9)
        ::RerankMMR(lambda: 0.5)
        ::RANGE(0, 10)
    RETURN results
