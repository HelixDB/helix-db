N::User {
    name: String,
    age: U8,
    email: String
}

QUERY GetUserCount () =>
    user_count <- N<User>::COUNT
    RETURN user_count

QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email
    })
    RETURN user


