N::User {
    boolean: Boolean,
}

QUERY GetUser(user_id: ID) => 
    user <- N<User>::MATCH|_::{boolean}|{
        true => "true",
        false => "false",
    }
    RETURN user

