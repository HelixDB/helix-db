// SelectE documentation examples

// Example 1: Selecting a follows edge by ID
QUERY GetFollowEdge (edge_id: ID) =>
    follow_edge <- E<Follows>(edge_id)
    RETURN follow_edge

// Example 2: Selecting all follows edges
QUERY GetAllFollows () =>
    follows <- E<Follows>
    RETURN follows

// Helper query to create users
QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email
    })
    RETURN user

// Helper query to create relationships
QUERY CreateRelationships (user1_id: ID, user2_id: ID) =>
    follows <- AddE<Follows>::From(user1_id)::To(user2_id)
    RETURN follows
