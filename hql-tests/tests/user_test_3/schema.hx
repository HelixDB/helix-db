N::Document {
    INDEX filename: String,
    upload_date: String,
    filetype: String,
    total_elements: I64
}

N::Chunk {
    INDEX element_id: String,
    text: String,
    element_type: String,
    page_number: I64,
    parent_id: String,
    category_depth: I64,
    metadata_json: String,
    order: I64
}

V::ChunkEmbedding {
    chunk_id: String
}

E::HAS_CHUNK {
    From: Document,
    To: Chunk
}

E::HAS_PARENT {
    From: Chunk,
    To: Chunk
}

E::HAS_EMBEDDING {
    From: Chunk,
    To: ChunkEmbedding
}
