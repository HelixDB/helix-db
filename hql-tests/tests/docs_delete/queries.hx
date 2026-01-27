// Delete documentation examples

// Example 1: Removing a user node by ID
QUERY DeleteUserNode (user_id: ID) =>
    DROP N<User>(user_id)
    RETURN "Removed user node"

// Example 2: Removing outgoing neighbors
QUERY DeleteOutgoingNeighbors (user_id: ID) =>
    DROP N<User>(user_id)::Out<Follows>
    RETURN "Removed outgoing neighbors"

// Example 3: Removing incoming neighbors
QUERY DeleteIncomingNeighbors (user_id: ID) =>
    DROP N<User>(user_id)::In<Follows>
    RETURN "Removed incoming neighbors"

// Example 4: Removing outgoing edges only
QUERY DeleteOutgoingEdges (user_id: ID) =>
    DROP N<User>(user_id)::OutE<Follows>
    RETURN "Removed outgoing edges"

// Example 5: Removing incoming edges only
QUERY DeleteIncomingEdges (user_id: ID) =>
    DROP N<User>(user_id)::InE<Follows>
    RETURN "Removed incoming edges"

// Helper queries
QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({ name: name, age: age, email: email })
    RETURN user

QUERY CreateRelationships (user1_id: ID, user2_id: ID) =>
    follows <- AddE<Follows>::From(user1_id)::To(user2_id)
    RETURN follows
