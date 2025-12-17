N::Post {
    subreddit: String,
    title: String,
    content: String,
    url: String,
    score: I32,
}

V::Content {
    chunk: String
}

E::EmbeddingOf {
    From: Post,
    To: Content,
}

N::Comment {
    content: String,
    score: I32,
}

E::CommentOf {
    From: Post,
    To: Comment,
}

QUERY get_all_posts() =>
    posts <- N<Post>
    RETURN posts

QUERY search_posts_vec(query: [F32], k: I32) =>
    vecs <- SearchV<Content>(query, k)
    posts <- vecs::In<EmbeddingOf>
    RETURN posts::{subreddit, title, content, url}
