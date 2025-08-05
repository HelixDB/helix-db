N::User {
    user_field: String,
    user_type: UserType,
}

Enum::UserType {
    User,
    Admin,
}

QUERY GetAdmins() => 
    users <- N<User>::MATCH|_::{user_type}|{
        UserType::User => _::{user_field},
        UserType::Admin => _::{user_field},
    }
    RETURN users

QUERY Search() => 
    nodes <- SearchV(queryText)::MATCH|_|{
        N::User(user) => user::{user_field},
        ? => SKIP,
    }
    RETURN nodes