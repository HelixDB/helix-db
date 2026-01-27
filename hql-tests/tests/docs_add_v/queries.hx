// AddV documentation examples

// Example 1: Creating a vector with no properties
QUERY InsertVectorSimple (vector: [F64]) =>
    vector_node <- AddV<Document>(vector)
    RETURN vector_node

// Example 2: Creating a vector with properties
QUERY InsertVector (vector: [F64], content: String, created_at: Date) =>
    vector_node <- AddV<Document>(vector, { content: content, created_at: created_at })
    RETURN vector_node

// Example 3: Creating a vector and connecting it to a node
QUERY InsertVectorWithEdge (user_id: ID, vector: [F64], content: String, created_at: Date) =>
    vector_node <- AddV<Document>(vector, { content: content, created_at: created_at })
    edge <- AddE<User_to_Document_Embedding>::From(user_id)::To(vector_node)
    RETURN "Success"

// Example 4: Using the built in Embed function
QUERY InsertVectorEmbed (content: String, created_at: Date) =>
    vector_node <- AddV<Document>(Embed(content), { content: content, created_at: created_at })
    RETURN vector_node

// Helper query to create users
QUERY CreateUser (name: String, age: U8, email: String) =>
    user <- AddN<User>({
        name: name,
        age: age,
        email: email
    })
    RETURN user
