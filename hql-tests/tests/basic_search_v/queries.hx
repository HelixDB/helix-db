N::User {
    name: String,
    age: I32,
}

V::UserVec {
    content: String,
}

E::EdgeUser {
    From: User,
    To: UserVec,
}


QUERY user(vec: [F64]) =>
    vecs <- SearchV<UserVec>(vec, 10)
    // pre_filter <- SearchV<File7Vec>(vec, 10)::PREFILTER(_::{content}::EQ("hello"))
    RETURN "hello"


QUERY user_with_embed(text: String) =>
    vecs <- SearchV<UserVec>(Embed(text), 10)
    RETURN vecs


V::Document {
    content: String,
    created_at: I64
}

QUERY SearchText(query: String, limit: I64) =>
    // Search for documents that are similar to the query
    results <- SearchV<Document>(Embed(query), limit)
    RETURN results