// Traversal from edges documentation examples

N::User {
    name: String,
    email: String,
}

V::Document {
    content: String
}

E::Creates {
    From: User,
    To: Document
}

E::MentionsUser {
    From: Document,
    To: User
}

E::Follows {
    From: User,
    To: User
}
