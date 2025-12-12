MIGRATION schema::1 => schema::2 {
    N::User => _::{
        username: name,
        given_age: age AS U32,
        ..
    }

    E::Knows => _::{
        Properties: {
            created_at: since,
            updated_at: since,
            ..
        }
    }

    N::OtherUser => _::{
        username: name,
        age: age AS U32,
        post_count: 0,
        ..
    }
}
