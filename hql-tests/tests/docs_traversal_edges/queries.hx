// Traversal from edges documentation examples

// ::FromN - Source Node
// Example: Getting the user from a document creation edge
QUERY GetCreatorFromEdge (creation_id: ID) =>
    creator <- E<Creates>(creation_id)::FromN
    RETURN creator

// ::FromV - Source Vector
// Example: Getting the source document from a mentions edge
QUERY GetMentionSource (mention_id: ID) =>
    source_doc <- E<MentionsUser>(mention_id)::FromV
    RETURN source_doc

// ::ToN - Destination Node
// Example: Getting the followed user from a follow edge
QUERY GetFollowedUser (follow_id: ID) =>
    followed_user <- E<Follows>(follow_id)::ToN
    RETURN followed_user

// ::ToV - Destination Vector
// Example: Inspecting the document vector
QUERY GetDocumentVector (creation_id: ID) =>
    document_vector <- E<Creates>(creation_id)::ToV
    RETURN document_vector

// Helper queries
QUERY CreateUser (name: String, email: String) =>
    user <- AddN<User>({
        name: name,
        email: email,
    })
    RETURN user

QUERY CreateDocument (user_id: ID, content: String, vector: [F64]) =>
    document <- AddV<Document>(vector, {
        content: content
    })
    creation_edge <- AddE<Creates>::From(user_id)::To(document)
    RETURN creation_edge

QUERY CreateDocumentSimple (content: String, vector: [F64]) =>
    document <- AddV<Document>(vector, {
        content: content
    })
    RETURN document

QUERY LinkMention (document_id: ID, user_id: ID) =>
    mention_edge <- AddE<MentionsUser>::From(document_id)::To(user_id)
    RETURN mention_edge

QUERY FollowUser (follower_id: ID, followed_id: ID) =>
    follow_edge <- AddE<Follows>::From(follower_id)::To(followed_id)
    RETURN follow_edge
