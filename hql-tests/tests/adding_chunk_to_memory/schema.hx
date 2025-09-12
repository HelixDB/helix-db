// User node - represents users of the memory system with email indexing
N::User {
    email?: String,
    name: String,
    created_at: String,
    updated_at: String,
}

// Memory node - always chunked content storage with multimodal support
N::Memory {
    user_id: ID,               // Reference to user - changed from String to ID
    content: String,           // Full original processed content
    original_input: String,    // Original URL/text user provided
    title: String,
    content_type: String,      // "page", "tweet", "document", "notion", "note"
    url: String,              // Canonical URL (if applicable)
    metadata: String,         // JSON metadata specific to content type
    chunk_count: U32,         // Number of chunks (always > 0)
    created_at: String,
    updated_at: String,
}

// Chunk vector - individual searchable text segments with embeddings
V::Chunk {
    memory_id: ID,             // Reference to parent memory
    text_content: String,      // Chunk text content
    order_in_document: U32,    // Position in original document (0-based)
    metadata: String,          // JSON metadata specific to chunk
    created_at: String,
}

// Space node - organizational container for memories  
N::Space {
    user_id: ID,
    name: String,
    description: String,
    created_at: String,
    updated_at: String,
}

// Edges for relationships in chunked memory system

// User owns memories
E::Owns {
    From: User,
    To: Memory,
    Properties: {
        created_at: String,
    }
}

// Memory has chunks (replaces direct embedding relationship)
E::HasChunk {
    From: Memory,
    To: Chunk,
    Properties: {
        similarity_score: F32 DEFAULT 0.0,  // For search result ranking
        created_at: String,
    }
}

// User owns spaces
E::HasSpace {
    From: User,
    To: Space,
    Properties: {
        role: String,              // owner, collaborator, viewer
        created_at: String,
    }
}

// Memory belongs to space
E::BelongsTo {
    From: Memory,
    To: Space,
    Properties: {
        added_at: String,
    }
}

// Memory relates to memory (for semantic connections via shared chunks)
E::RelatedTo {
    From: Memory,
    To: Memory,
    Properties: {
        similarity_score: F32,
        relationship_type: String, // semantic, contextual, temporal
        shared_chunks: U32,        // Number of similar chunks
        created_at: String,
    }
}