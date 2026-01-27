// AddN documentation examples

// Example 1: Adding an empty user node
QUERY CreateUsers () =>
    empty_user <- AddN<User>
    RETURN empty_user

// Example 2: Adding a user with parameters
QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email
    })
    RETURN user

// Example 3: Adding a user with predefined properties
QUERY CreatePredefinedUser () =>
    predefined_user <- AddN<User>({
        name: "Alice Johnson",
        age: 30,
        email: "alice@example.com"
    })
    RETURN predefined_user
