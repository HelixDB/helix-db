// SelectE documentation examples

N::User {
    name: String,
    age: U8,
    email: String,
}

E::Follows {
    From: User,
    To: User,
}
