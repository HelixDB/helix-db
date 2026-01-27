// AddV documentation examples

N::User {
    name: String,
    age: U8,
    email: String,
}

V::Document {
    content: String,
    created_at: Date
}

E::User_to_Document_Embedding {
    From: User,
    To: Document,
}
