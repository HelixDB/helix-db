// AddE documentation examples

// Example 1: Creating a simple follows relationship
QUERY CreateRelationships (user1_id: ID, user2_id: ID) =>
    follows <- AddE<Follows>::From(user1_id)::To(user2_id)
    RETURN follows

// Example 2: Creating a detailed friendship with properties
QUERY CreateFriendship (user1_id: ID, user2_id: ID) =>
    friendship <- AddE<Friends>({
        since: "2024-01-15",
        strength: 0.85
    })::From(user1_id)::To(user2_id)
    RETURN friendship

// Example 3: Traversal example - finding user by name
QUERY CreateRelationshipsByName (user1_id: ID, user2_name: String) =>
    user2 <- N<User>::WHERE(_::{name}::EQ(user2_name))
    follows <- AddE<Follows>::From(user1_id)::To(user2)
    RETURN follows

// Helper query to create users
QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email
    })
    RETURN user
