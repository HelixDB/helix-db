N::User {
    username: String,
    email: String,
}

V::Embedding {
    created_at: Date DEFAULT NOW,
}

E::Connection {
    From: User,
    To: Embedding,
}

E::Friend {
    From: User,
    To: User,
}

QUERY search_vector(query: [f32], k: I64) =>
    result <- N<User>::Out<Connection>::SearchV<Embedding>(query, k)
    RETURN result
