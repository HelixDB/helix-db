// Test queries for test_consumer

// Get all users from the database
QUERY GetUsers() =>
    users <- N<User>
    RETURN users

// Get all memories
QUERY GetMemories() =>
    memories <- N<Memory>
    RETURN memories

// Create a new user
QUERY CreateUser(name: String, email: String) =>
    user <- AddN<User>({name: name, email: email})
    RETURN user

// Create a memory for a user
QUERY CreateMemory(user_id: ID, content: String, title: String) =>
    user <- N<User>(user_id)
    memory <- AddN<Memory>({
        user_id: user_id,
        content: content,
        title: title,
        content_type: "text",
        original_input: content,
        url: "",
        metadata: "{}",
        chunk_count: 0
    })
    AddE<Owns>::From(user)::To(memory)
    RETURN memory

// Get memories for a specific user
QUERY GetUserMemories(user_id: ID) =>
    memories <- N<User>(user_id)::Out<Owns>
    RETURN memories
