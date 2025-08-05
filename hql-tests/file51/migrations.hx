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
        }
    }
}