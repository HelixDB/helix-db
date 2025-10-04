N::File9 {
    INDEX name: String,
    INDEX age: I32,
    INDEX count: F32,
}

E::EFile9 {
    From: File9,
    To: File9,
}


QUERY file9(other_id: ID, id: ID) =>
    path1 <- N<File9>(id)::ShortestPath<EFile9>::To(other_id)
    path2 <- N<File9>(id)::ShortestPath<EFile9>::From(other_id)
    RETURN path1, path2