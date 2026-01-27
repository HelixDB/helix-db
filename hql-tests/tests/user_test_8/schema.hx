// KidKazz RAG Schema
// Defines nodes and edges for hierarchical document chunking with vector embeddings

// Document node: Top-level container for chunks
N::Document {
    INDEX doc_id: String,
    title: String,
    tags: String,           // JSON array of document tags
    created_at: I64,
    chunk_count: U32,
}

// Chunk node: Text content with metadata
N::Chunk {
    INDEX chunk_id: String,
    content: String,
    INDEX level: U32,             // 0=doc, 1=section, 2=leaf
    token_count: U32,
    word_count: U32,
    INDEX document_id: String,
    INDEX semantic_type: String,  // definition, example, procedure, theorem, narrative
    topic_tags: String,     // JSON array
    section_path: String,   // JSON array
    source_section: String,
    sequence_position: U32,
    INDEX parent_id: String,
    child_ids: String,      // JSON array
    sibling_ids: String,    // JSON array
    prev_id: String,
    next_id: String,
    has_table: U32,
    has_code: U32,
    has_math: U32,
    has_list: U32,
}

// ChunkVector: Embedding vector for similarity search
// Note: Vector data is stored implicitly, metadata fields only
V::ChunkVector {
    model_name: String,
    embedding_dim: U32,
}

// Document contains chunks
E::HasChunk {
    From: Document,
    To: Chunk,
}

// Chunk hierarchy (L1 -> L2)
E::ParentOf {
    From: Chunk,
    To: Chunk,
}

// Sequential order
E::NextSibling {
    From: Chunk,
    To: Chunk,
}

E::PrevSibling {
    From: Chunk,
    To: Chunk,
}

// Same parent chunks
E::SiblingOf {
    From: Chunk,
    To: Chunk,
}

// Chunk to embedding link
E::HasEmbedding {
    From: Chunk,
    To: ChunkVector,
}


// ========== CONCEPT GRAPH ==========

// Concept node: Extracted entity from textbooks
N::Concept {
    INDEX concept_id: String,      // Slugified: "cost-of-goods-sold"
    INDEX name: String,            // Display: "Cost of Goods Sold"
    definition: String,            // Brief definition
    concept_type: String,          // term, method, principle, formula, account
    source_documents: String,      // JSON array of doc_ids
    aliases: String,               // JSON array: ["COGS", "cost of sales"]
}

// Chunk defines a concept (contains definition)
E::DefinesConcept {
    From: Chunk,
    To: Concept,
}

// Chunk mentions a concept (references without defining)
E::MentionsConcept {
    From: Chunk,
    To: Concept,
}

// Concept relationship edges - typed for semantic meaning
// Generic relation (fallback)
E::RelatesTo {
    From: Concept,
    To: Concept,
}

// Specific typed relations
E::Uses {
    From: Concept,
    To: Concept,
}

E::Requires {
    From: Concept,
    To: Concept,
}

E::CalculatedFrom {
    From: Concept,
    To: Concept,
}

E::ComponentOf {
    From: Concept,
    To: Concept,
}

E::RecordedIn {
    From: Concept,
    To: Concept,
}

E::Supersedes {
    From: Concept,
    To: Concept,
}


// ========== TABLE STORAGE ==========

// Table node: Parsed markdown table with metadata
N::Table {
    INDEX table_id: String,           // "table_{source_chunk_id}"
    raw_markdown: String,             // Original markdown for LLM synthesis
    summary_text: String,             // LLM-generated description
    column_names: String,             // JSON array
    column_types: String,             // JSON array: ["text", "numeric", "date"]
    row_count: U32,
    column_count: U32,
    rows: String,                     // JSON array of arrays
    has_header_row: U32,              // Boolean as U32
    surrounding_context: String,      // Text before table
    source_chunk_id: String,          // Link to source chunk
    INDEX document_id: String,        // For document filtering
    key_columns: String,              // JSON array (from summary)
    key_values: String,               // JSON array (from summary)
}

// TableVector: Embedding for table summary similarity search
V::TableVector {
    model_name: String,
    embedding_dim: U32,
}

// Document owns tables
E::HasTable {
    From: Document,
    To: Table,
}

// Chunk contains table
E::ChunkHasTable {
    From: Chunk,
    To: Table,
}

// Table relates to concept (graph traversal)
E::TableRelatedToConcept {
    From: Table,
    To: Concept,
}

// Table has embedding
E::TableHasEmbedding {
    From: Table,
    To: TableVector,
}


// ========== DOCUMENT SUMMARIZATION ==========

// Summary node: Hierarchical document summaries
N::Summary {
    INDEX summary_id: String,      // "summary_{source_id}_{level}"
    content: String,               // Summary text
    INDEX level: String,           // "document", "chapter", "section"
    INDEX source_id: String,       // doc_id or chunk_id
    INDEX document_id: String,     // For document filtering
    parent_summary_id: String,     // Hierarchy navigation (empty if top-level)
    key_points: String,            // JSON array of key takeaways
    word_count: U32,
    created_at: I64,
}

// SummaryVector: Embedding for summary semantic search
V::SummaryVector {
    model_name: String,
    embedding_dim: U32,
}

// Document has summary (document-level)
E::DocumentHasSummary {
    From: Document,
    To: Summary,
}

// Chunk has summary (chapter/section level)
E::ChunkHasSummary {
    From: Chunk,
    To: Summary,
}

// Summary hierarchy (document -> chapter -> section)
E::SummaryHasChild {
    From: Summary,
    To: Summary,
}

// Summary has embedding
E::SummaryHasEmbedding {
    From: Summary,
    To: SummaryVector,
}