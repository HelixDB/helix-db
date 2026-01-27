// SelectN documentation examples

// Example 1: Selecting a user by ID
QUERY GetUser (user_id: ID) =>
    user <- N<User>(user_id)
    RETURN user

// Example 2: Selecting all users
QUERY GetAllUsers () =>
    users <- N<User>
    RETURN users

// Helper query to create users
QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email
    })
    RETURN user
