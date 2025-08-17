schema::1 {
    N::User {
        name: String,
        age: I32,
        bio: String,
    }

    N::OtherUser {
        name: String,
        age: I32
    }

    E::Knows {
        From: User,
        To: User,
        Properties: {
            since: I32,
            fact: String,
        }
    }
}

schema::2 {
    N::User {
        username: String,
        given_age: U32,
        bio: String,
    }

    E::Knows {
        From: User,
        To: User,
        Properties: {
            created_at: I32,
            updated_at: I32,
        }
    }

    N::OtherUser {
        username: String,
        age: U32,
        post_count: U32
    }
}





