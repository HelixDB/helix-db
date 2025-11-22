N::User {
    INDEX name: String,
}

V::Embedding {
    content: String,
}

E::EmbeddingOf {
    From: User,
    To: Embedding,
    Properties: {
        category: String
    }
}

QUERY add(vec: [F32]) =>
    user <- AddN<User>({
        name: "John Doe"
    })
    embedding <- AddV<Embedding>(vec,{
        content: "Hello, world!"
    })
    AddE<EmbeddingOf>({category: "test"})::From(user)::To(embedding)
    RETURN user

QUERY to_v(query: [F32], k: I32, data: String) =>
    user <- N<User>({name: "John Doe"})
    edges <- user::OutE<EmbeddingOf>
    filtered <- edges::WHERE(_::{category}::EQ(data))
    vectors <- filtered::ToV
    searched <- vectors::SearchV<Embedding>(query, k)
    RETURN user, edges, filtered, vectors, searched
