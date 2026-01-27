// AddE documentation examples

N::User {
    name: String,
    age: U8,
    email: String,
}

E::Follows {
    From: User,
    To: User,
}

E::Friends {
    From: User,
    To: User,
    Properties: {
        since: Date,
        strength: F64
    }
}
