// Traversal from nodes documentation examples

N::User {
    name: String,
    handle: String,
}

E::Follows {
    From: User,
    To: User,
    Properties: {
        since: Date
    }
}
