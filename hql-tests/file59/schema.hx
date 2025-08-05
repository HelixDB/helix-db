schema::1 {
    N::User {
        name: String,
        age: I32
    }
}

schema::2 {
    N::User {
        username: String,
        age: U32,
        post_count: U32
    }
}


MIGRATION schema::1 => schema::2 {
    N::User => _::{
        username: name,
        age: age AS U32,
        post_count: 0
    }
}