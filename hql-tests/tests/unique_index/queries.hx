N::File9 {
    UNIQUE INDEX name: String,
    INDEX age: I32,
    INDEX count: F32 DEFAULT 0.0,
    other_field: String,
}

E::UniqueEdge UNIQUE {
    From: File9,
    To: File9,
}


QUERY file9(name: String, id: ID) =>
    user <- AddN<File9>({name: name, age: 10, other_field: "other_field"})
    user_by_id <- N<File9>(id)
    node <- N<File9>({name: name})
    node_by_name <- N<File9>({count: 24.5})
    e <- AddE<UniqueEdge>::From(user)::To(node)
    RETURN user, node, node_by_name