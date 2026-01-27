// Traversal from nodes documentation examples

// ::Out - Outgoing Nodes
// Example: Listing who a user follows
QUERY GetUserFollowing (user_id: ID) =>
    following <- N<User>(user_id)::Out<Follows>
    RETURN following

// ::In - Incoming Nodes
// Example: Listing who follows a user
QUERY GetUserFollowers (user_id: ID) =>
    followers <- N<User>(user_id)::In<Follows>
    RETURN followers

// ::OutE - Outgoing Edges
// Example: Inspecting follow relationships
QUERY GetFollowingEdges (user_id: ID) =>
    follow_edges <- N<User>(user_id)::OutE<Follows>
    RETURN follow_edges

// ::InE - Incoming Edges
// Example: Inspecting who followed a user
QUERY GetFollowerEdges (user_id: ID) =>
    follow_edges <- N<User>(user_id)::InE<Follows>
    RETURN follow_edges

// Helper queries
QUERY CreateUser (name: String, handle: String) =>
    user <- AddN<User>({
        name: name,
        handle: handle,
    })
    RETURN user

QUERY FollowUser (follower_id: ID, followed_id: ID, since: Date) =>
    follow_edge <- AddE<Follows>({
        since: since
    })::From(follower_id)::To(followed_id)
    RETURN follow_edge
