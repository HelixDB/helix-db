schema::1 {
    N::User {
        name: String,
        age: U32,
        active: Boolean
    }
}

schema::2 {
    N::User {
        full_name: String,
        age: U32,
        active: Boolean,
        created_at: Date
    }
}

schema::3 {
    N::User {
        full_name: String,
        age: U32,
        active: Boolean,
        created_at: Date,
        score: F64
    }
}

MIGRATION schema::1 => schema::2 {
    N::User => _:: {
        full_name: name,
        age: age,
        active: active,
        created_at: "2024-01-01T00:00:00Z"
    }
}

MIGRATION schema::2 => schema::3 {
    N::User => _:: {
        full_name: full_name,
        age: age,
        active: active,
        created_at: created_at,
        score: 0.0
    }
}
