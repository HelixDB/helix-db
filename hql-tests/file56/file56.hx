QUERY CreateUserBioEmbedding(userId: String, bioText: String, lastUpdated: String) =>
    embedding <- AddV<UserEmbedding>(Embed(bioText), {
        userId: userId,
        dataType: "bio",
        metadata: "{}",
        lastUpdated: lastUpdated
    })
    RETURN embedding

QUERY SearchSimilarUsers(queryText: String, k: I64, dataType: String) =>
    search_results <- SearchV<UserEmbedding>(Embed(queryText), k)
    RETURN search_results

V::UserEmbedding {
    userId: String,
    dataType: String,
    metadata: String DEFAULT "{}",
    lastUpdated: String,
    createdAt: Date DEFAULT NOW
}