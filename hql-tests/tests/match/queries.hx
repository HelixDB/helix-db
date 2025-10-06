N::User {
    boolean: Boolean,
}

QUERY GetUser(user_id: ID) => 
    user <- N<User>::MATCH|_::{boolean}|{
        true => _,
        false => NONE,
    }
    RETURN user
    
QUERY GetUserOther(user_id: ID) => 
    user <- N<User>::MATCH|_|{
        N::User => _::{boolean},
    }
    RETURN user
