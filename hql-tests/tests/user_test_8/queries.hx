// KidKazz RAG Queries

// ========== MUTATION QUERIES ==========

// Add a new document
QUERY AddDocument(doc_id: String, title: String, tags: String, chunk_count: U32, created_at: I64) =>
    doc <- AddN<Document>({
        doc_id: doc_id,
        title: title,
        tags: tags,
        created_at: created_at,
        chunk_count: chunk_count
    })
    RETURN doc

// Add a chunk
QUERY AddChunk(
    chunk_id: String,
    content: String,
    level: U32,
    token_count: U32,
    word_count: U32,
    document_id: String,
    semantic_type: String,
    topic_tags: String,
    section_path: String,
    source_section: String,
    sequence_position: U32,
    parent_id: String,
    child_ids: String,
    sibling_ids: String,
    prev_id: String,
    next_id: String,
    has_table: U32,
    has_code: U32,
    has_math: U32,
    has_list: U32
) =>
    chunk <- AddN<Chunk>({
        chunk_id: chunk_id,
        content: content,
        level: level,
        token_count: token_count,
        word_count: word_count,
        document_id: document_id,
        semantic_type: semantic_type,
        topic_tags: topic_tags,
        section_path: section_path,
        source_section: source_section,
        sequence_position: sequence_position,
        parent_id: parent_id,
        child_ids: child_ids,
        sibling_ids: sibling_ids,
        prev_id: prev_id,
        next_id: next_id,
        has_table: has_table,
        has_code: has_code,
        has_math: has_math,
        has_list: has_list
    })
    RETURN chunk

// Link document to chunk
QUERY LinkDocumentChunk(doc_id: ID, chunk_id: ID) =>
    doc <- N<Document>(doc_id)
    chunk <- N<Chunk>(chunk_id)
    AddE<HasChunk>::From(doc)::To(chunk)
    RETURN doc

// Add parent-child relationship
QUERY AddParentChild(parent_id: ID, child_id: ID) =>
    parent <- N<Chunk>(parent_id)
    child <- N<Chunk>(child_id)
    AddE<ParentOf>::From(parent)::To(child)
    RETURN parent

// Add next sibling relationship
QUERY AddNextSibling(chunk_id: ID, next_id: ID) =>
    chunk <- N<Chunk>(chunk_id)
    next <- N<Chunk>(next_id)
    AddE<NextSibling>::From(chunk)::To(next)
    RETURN chunk

// ========== READ QUERIES (String-based lookups) ==========

// List all documents
QUERY ListDocuments() =>
    docs <- N<Document>
    RETURN docs

// Get document by user-facing doc_id string
QUERY GetDocumentByDocId(doc_id: String) =>
    docs <- N<Document>::WHERE(_::{doc_id}::EQ(doc_id))
    RETURN docs

// Get chunk by user-facing chunk_id string
QUERY GetChunkByChunkId(chunk_id: String) =>
    chunks <- N<Chunk>::WHERE(_::{chunk_id}::EQ(chunk_id))
    RETURN chunks

// Get all chunks for a document by document_id string
QUERY GetChunksByDocumentId(document_id: String) =>
    chunks <- N<Chunk>::WHERE(_::{document_id}::EQ(document_id))
    RETURN chunks

// Get chunks by level
QUERY GetChunksByLevel(level: U32) =>
    chunks <- N<Chunk>::WHERE(_::{level}::EQ(level))
    RETURN chunks

// Get chunks by document AND level
QUERY GetChunksByDocAndLevel(document_id: String, level: U32) =>
    chunks <- N<Chunk>::WHERE(
        AND(
            _::{document_id}::EQ(document_id),
            _::{level}::EQ(level)
        )
    )
    RETURN chunks

// Get chunks by parent_id (for finding children)
QUERY GetChunksByParentId(parent_id: String) =>
    chunks <- N<Chunk>::WHERE(_::{parent_id}::EQ(parent_id))
    RETURN chunks

// Delete document by doc_id string (returns doc for client to then DROP)
QUERY DeleteDocumentByDocId(doc_id: String) =>
    doc <- N<Document>::WHERE(_::{doc_id}::EQ(doc_id))
    RETURN doc

// ========== DELETE QUERIES (using internal IDs) ==========

// Delete a chunk by internal ID (also removes connected edges and vectors)
QUERY DropChunk(chunk_id: ID) =>
    DROP N<Chunk>(chunk_id)
    RETURN "Removed chunk"

// Delete a document by internal ID
QUERY DropDocument(doc_id: ID) =>
    DROP N<Document>(doc_id)
    RETURN "Removed document"

// Delete HasChunk edges from document (useful before dropping document)
QUERY DropDocumentChunkEdges(doc_id: ID) =>
    DROP N<Document>(doc_id)::OutE<HasChunk>
    RETURN "Removed document chunk edges"

// Delete HasEmbedding edge from chunk
QUERY DropChunkEmbeddingEdge(chunk_id: ID) =>
    DROP N<Chunk>(chunk_id)::OutE<HasEmbedding>
    RETURN "Removed chunk embedding edge"

// ========== UPDATE QUERIES ==========

// Update chunk content
QUERY UpdateChunkContent(chunk_id: ID, content: String, word_count: U32) =>
    updated <- N<Chunk>(chunk_id)::UPDATE({
        content: content,
        word_count: word_count
    })
    RETURN updated

// ========== READ QUERIES (Internal ID-based - for edge traversals) ==========

// Get a document by internal ID
QUERY GetDocument(doc_id: ID) =>
    doc <- N<Document>(doc_id)
    RETURN doc

// Get a chunk by internal ID
QUERY GetChunk(chunk_id: ID) =>
    chunk <- N<Chunk>(chunk_id)
    RETURN chunk

// Get all chunks for a document via edge traversal
QUERY GetDocumentChunks(doc_id: ID) =>
    chunks <- N<Document>(doc_id)::Out<HasChunk>
    RETURN chunks

// Get parent chunk via edge
QUERY GetParentChunk(chunk_id: ID) =>
    parent <- N<Chunk>(chunk_id)::In<ParentOf>
    RETURN parent

// Get child chunks via edge
QUERY GetChildChunks(chunk_id: ID) =>
    children <- N<Chunk>(chunk_id)::Out<ParentOf>
    RETURN children

// Get next chunk in sequence via edge
QUERY GetNextChunk(chunk_id: ID) =>
    next <- N<Chunk>(chunk_id)::Out<NextSibling>
    RETURN next

// Get previous chunk via edge
QUERY GetPrevChunk(chunk_id: ID) =>
    prev <- N<Chunk>(chunk_id)::In<NextSibling>
    RETURN prev

// Get sibling chunks via edge
QUERY GetSiblingChunks(chunk_id: ID) =>
    siblings <- N<Chunk>(chunk_id)::Out<SiblingOf>
    RETURN siblings

// ========== VECTOR QUERIES ==========

// Add vector embedding for a chunk
// Note: AddV expects (vector_data, {properties}) - vector first, then metadata
QUERY AddChunkVector(embedding: [F64], model_name: String, embedding_dim: U32) =>
    vec <- AddV<ChunkVector>(embedding, {
        model_name: model_name,
        embedding_dim: embedding_dim
    })
    RETURN vec

// Link chunk to its embedding
QUERY LinkChunkVector(chunk_id: ID, vector_id: ID) =>
    chunk <- N<Chunk>(chunk_id)
    vec <- V<ChunkVector>(vector_id)
    AddE<HasEmbedding>::From(chunk)::To(vec)
    RETURN chunk

// ========== SEARCH QUERIES ==========

// Vector similarity search (filtering done in Python post-processing)
QUERY SearchSimilar(query_vec: [F64], top_k: U32) =>
    results <- SearchV<ChunkVector>(query_vec, top_k)
    RETURN results

// Vector search with MMR reranking
QUERY SearchSimilarMMR(query_vec: [F64], top_k: U32, lambda: F64) =>
    results <- SearchV<ChunkVector>(query_vec, top_k)::RerankMMR(lambda: lambda)
    RETURN results

// BM25 keyword search
QUERY SearchKeywordBM25(keyword: String, limit: U32) =>
    results <- SearchBM25<Chunk>(keyword, limit)
    RETURN results


// ========== CONCEPT MUTATIONS ==========

// Add a new concept
QUERY AddConcept(
    concept_id: String,
    name: String,
    definition: String,
    concept_type: String,
    source_documents: String,
    aliases: String
) =>
    concept <- AddN<Concept>({
        concept_id: concept_id,
        name: name,
        definition: definition,
        concept_type: concept_type,
        source_documents: source_documents,
        aliases: aliases
    })
    RETURN concept

// Link chunk to concept it defines
QUERY LinkChunkDefinesConcept(chunk_id: ID, concept_id: ID) =>
    chunk <- N<Chunk>(chunk_id)
    concept <- N<Concept>(concept_id)
    AddE<DefinesConcept>::From(chunk)::To(concept)
    RETURN chunk

// Link chunk to concept it mentions
QUERY LinkChunkMentionsConcept(chunk_id: ID, concept_id: ID) =>
    chunk <- N<Chunk>(chunk_id)
    concept <- N<Concept>(concept_id)
    AddE<MentionsConcept>::From(chunk)::To(concept)
    RETURN chunk

// Link two concepts with a generic relationship (fallback)
QUERY LinkConceptRelatesTo(from_id: ID, to_id: ID) =>
    from_concept <- N<Concept>(from_id)
    to_concept <- N<Concept>(to_id)
    AddE<RelatesTo>::From(from_concept)::To(to_concept)
    RETURN from_concept

// Typed relationship queries
QUERY LinkConceptUses(from_id: ID, to_id: ID) =>
    from_concept <- N<Concept>(from_id)
    to_concept <- N<Concept>(to_id)
    AddE<Uses>::From(from_concept)::To(to_concept)
    RETURN from_concept

QUERY LinkConceptRequires(from_id: ID, to_id: ID) =>
    from_concept <- N<Concept>(from_id)
    to_concept <- N<Concept>(to_id)
    AddE<Requires>::From(from_concept)::To(to_concept)
    RETURN from_concept

QUERY LinkConceptCalculatedFrom(from_id: ID, to_id: ID) =>
    from_concept <- N<Concept>(from_id)
    to_concept <- N<Concept>(to_id)
    AddE<CalculatedFrom>::From(from_concept)::To(to_concept)
    RETURN from_concept

QUERY LinkConceptComponentOf(from_id: ID, to_id: ID) =>
    from_concept <- N<Concept>(from_id)
    to_concept <- N<Concept>(to_id)
    AddE<ComponentOf>::From(from_concept)::To(to_concept)
    RETURN from_concept

QUERY LinkConceptRecordedIn(from_id: ID, to_id: ID) =>
    from_concept <- N<Concept>(from_id)
    to_concept <- N<Concept>(to_id)
    AddE<RecordedIn>::From(from_concept)::To(to_concept)
    RETURN from_concept

QUERY LinkConceptSupersedes(from_id: ID, to_id: ID) =>
    from_concept <- N<Concept>(from_id)
    to_concept <- N<Concept>(to_id)
    AddE<Supersedes>::From(from_concept)::To(to_concept)
    RETURN from_concept

// ========== CONCEPT QUERIES ==========

// Get concept by name
QUERY GetConceptByName(name: String) =>
    concepts <- N<Concept>::WHERE(_::{name}::EQ(name))
    RETURN concepts

// Get concept by concept_id
QUERY GetConceptById(concept_id: String) =>
    concepts <- N<Concept>::WHERE(_::{concept_id}::EQ(concept_id))
    RETURN concepts

// List all concepts
QUERY ListConcepts() =>
    concepts <- N<Concept>
    RETURN concepts

// List concepts from a specific document
QUERY ListDocumentConcepts(document_id: String) =>
    chunks <- N<Chunk>::WHERE(_::{document_id}::EQ(document_id))
    concepts <- chunks::Out<DefinesConcept>
    RETURN concepts

// ========== CONCEPT TRAVERSALS ==========

// Get chunks that define a concept (for citations)
QUERY GetConceptDefinitionChunks(concept_id: ID) =>
    chunks <- N<Concept>(concept_id)::In<DefinesConcept>
    RETURN chunks

// Get chunks that mention a concept
QUERY GetConceptMentionChunks(concept_id: ID) =>
    chunks <- N<Concept>(concept_id)::In<MentionsConcept>
    RETURN chunks

// Get related concepts via generic RelatesTo edge
QUERY GetRelatedConcepts(concept_id: ID) =>
    related <- N<Concept>(concept_id)::Out<RelatesTo>
    RETURN related

// Get related concepts via typed edges
QUERY GetConceptsUses(concept_id: ID) =>
    related <- N<Concept>(concept_id)::Out<Uses>
    RETURN related

QUERY GetConceptsRequires(concept_id: ID) =>
    related <- N<Concept>(concept_id)::Out<Requires>
    RETURN related

QUERY GetConceptsCalculatedFrom(concept_id: ID) =>
    related <- N<Concept>(concept_id)::Out<CalculatedFrom>
    RETURN related

QUERY GetConceptsComponentOf(concept_id: ID) =>
    related <- N<Concept>(concept_id)::Out<ComponentOf>
    RETURN related

QUERY GetConceptsRecordedIn(concept_id: ID) =>
    related <- N<Concept>(concept_id)::Out<RecordedIn>
    RETURN related

QUERY GetConceptsSupersedes(concept_id: ID) =>
    related <- N<Concept>(concept_id)::Out<Supersedes>
    RETURN related

// Get concepts that relate TO this one (reverse)
QUERY GetConceptDependents(concept_id: ID) =>
    dependents <- N<Concept>(concept_id)::In<RelatesTo>
    RETURN dependents

QUERY GetConceptUsedBy(concept_id: ID) =>
    dependents <- N<Concept>(concept_id)::In<Uses>
    RETURN dependents

QUERY GetConceptRequiredBy(concept_id: ID) =>
    dependents <- N<Concept>(concept_id)::In<Requires>
    RETURN dependents

// ========== CONCEPT UPDATE ==========

// Update concept source_documents and aliases (for cross-document merging)
QUERY UpdateConcept(concept_id: String, source_documents: String, aliases: String) =>
    concept <- N<Concept>::WHERE(_::{concept_id}::EQ(concept_id))::UPDATE({
        source_documents: source_documents,
        aliases: aliases
    })
    RETURN concept

// Update concept with definition
QUERY UpdateConceptWithDefinition(concept_id: String, source_documents: String, aliases: String, definition: String) =>
    concept <- N<Concept>::WHERE(_::{concept_id}::EQ(concept_id))::UPDATE({
        source_documents: source_documents,
        aliases: aliases,
        definition: definition
    })
    RETURN concept

// ========== CONCEPT DELETION ==========

// Delete a concept
QUERY DropConcept(concept_id: ID) =>
    DROP N<Concept>(concept_id)
    RETURN "Removed concept"


// ========== TABLE MUTATIONS ==========

// Add a new table
QUERY AddTable(
    table_id: String,
    raw_markdown: String,
    summary_text: String,
    column_names: String,
    column_types: String,
    row_count: U32,
    column_count: U32,
    rows: String,
    has_header_row: U32,
    surrounding_context: String,
    source_chunk_id: String,
    document_id: String,
    key_columns: String,
    key_values: String
) =>
    table <- AddN<Table>({
        table_id: table_id,
        raw_markdown: raw_markdown,
        summary_text: summary_text,
        column_names: column_names,
        column_types: column_types,
        row_count: row_count,
        column_count: column_count,
        rows: rows,
        has_header_row: has_header_row,
        surrounding_context: surrounding_context,
        source_chunk_id: source_chunk_id,
        document_id: document_id,
        key_columns: key_columns,
        key_values: key_values
    })
    RETURN table

// Add vector embedding for a table summary
QUERY AddTableVector(embedding: [F64], model_name: String, embedding_dim: U32) =>
    vec <- AddV<TableVector>(embedding, {
        model_name: model_name,
        embedding_dim: embedding_dim
    })
    RETURN vec

// Link document to table
QUERY LinkDocumentTable(doc_id: ID, table_id: ID) =>
    doc <- N<Document>(doc_id)
    table <- N<Table>(table_id)
    AddE<HasTable>::From(doc)::To(table)
    RETURN doc

// Link chunk to table
QUERY LinkChunkTable(chunk_id: ID, table_id: ID) =>
    chunk <- N<Chunk>(chunk_id)
    table <- N<Table>(table_id)
    AddE<ChunkHasTable>::From(chunk)::To(table)
    RETURN chunk

// Link table to concept
QUERY LinkTableConcept(table_id: ID, concept_id: ID) =>
    table <- N<Table>(table_id)
    concept <- N<Concept>(concept_id)
    AddE<TableRelatedToConcept>::From(table)::To(concept)
    RETURN table

// Link table to its embedding
QUERY LinkTableVector(table_id: ID, vector_id: ID) =>
    table <- N<Table>(table_id)
    vec <- V<TableVector>(vector_id)
    AddE<TableHasEmbedding>::From(table)::To(vec)
    RETURN table

// ========== TABLE QUERIES ==========

// Get table by table_id string
QUERY GetTableById(table_id: String) =>
    tables <- N<Table>::WHERE(_::{table_id}::EQ(table_id))
    RETURN tables

// Get tables by document_id
QUERY GetTablesByDocumentId(document_id: String) =>
    tables <- N<Table>::WHERE(_::{document_id}::EQ(document_id))
    RETURN tables

// List all tables
QUERY ListTables() =>
    tables <- N<Table>
    RETURN tables

// ========== TABLE TRAVERSALS ==========

// Get tables from document via edge
QUERY GetDocumentTables(doc_id: ID) =>
    tables <- N<Document>(doc_id)::Out<HasTable>
    RETURN tables

// Get table from chunk via edge
QUERY GetChunkTable(chunk_id: ID) =>
    tables <- N<Chunk>(chunk_id)::Out<ChunkHasTable>
    RETURN tables

// Get tables for concept via edge (reverse)
QUERY GetTablesForConcept(concept_id: ID) =>
    tables <- N<Concept>(concept_id)::In<TableRelatedToConcept>
    RETURN tables

// Get concepts related to table via edge
QUERY GetTableConcepts(table_id: ID) =>
    concepts <- N<Table>(table_id)::Out<TableRelatedToConcept>
    RETURN concepts

// ========== TABLE VECTOR SEARCH ==========

// Vector similarity search on table summaries
QUERY SearchSimilarTables(query_vec: [F64], top_k: U32) =>
    results <- SearchV<TableVector>(query_vec, top_k)
    RETURN results

// ========== TABLE DELETION ==========

// Delete a table (cascades to connected edges)
QUERY DropTable(table_id: ID) =>
    DROP N<Table>(table_id)
    RETURN "Removed table"

// Delete table embedding edge
QUERY DropTableEmbeddingEdge(table_id: ID) =>
    DROP N<Table>(table_id)::OutE<TableHasEmbedding>
    RETURN "Removed table embedding edge"


// ========== SUMMARY MUTATIONS ==========

// Add a new summary
QUERY AddSummary(
    summary_id: String,
    content: String,
    level: String,
    source_id: String,
    document_id: String,
    parent_summary_id: String,
    key_points: String,
    word_count: U32,
    created_at: I64
) =>
    summary <- AddN<Summary>({
        summary_id: summary_id,
        content: content,
        level: level,
        source_id: source_id,
        document_id: document_id,
        parent_summary_id: parent_summary_id,
        key_points: key_points,
        word_count: word_count,
        created_at: created_at
    })
    RETURN summary

// Add vector embedding for a summary
QUERY AddSummaryVector(embedding: [F64], model_name: String, embedding_dim: U32) =>
    vec <- AddV<SummaryVector>(embedding, {
        model_name: model_name,
        embedding_dim: embedding_dim
    })
    RETURN vec

// Link document to summary
QUERY LinkDocumentSummary(doc_id: ID, summary_id: ID) =>
    doc <- N<Document>(doc_id)
    summary <- N<Summary>(summary_id)
    AddE<DocumentHasSummary>::From(doc)::To(summary)
    RETURN doc

// Link chunk to summary
QUERY LinkChunkSummary(chunk_id: ID, summary_id: ID) =>
    chunk <- N<Chunk>(chunk_id)
    summary <- N<Summary>(summary_id)
    AddE<ChunkHasSummary>::From(chunk)::To(summary)
    RETURN chunk

// Link parent summary to child summary
QUERY LinkSummaryParent(parent_id: ID, child_id: ID) =>
    parent <- N<Summary>(parent_id)
    child <- N<Summary>(child_id)
    AddE<SummaryHasChild>::From(parent)::To(child)
    RETURN parent

// Link summary to its embedding
QUERY LinkSummaryVector(summary_id: ID, vector_id: ID) =>
    summary <- N<Summary>(summary_id)
    vec <- V<SummaryVector>(vector_id)
    AddE<SummaryHasEmbedding>::From(summary)::To(vec)
    RETURN summary

// ========== SUMMARY QUERIES ==========

// Get summary by summary_id string (maps to Python GetSummary)
QUERY GetSummary(summary_id: String) =>
    summaries <- N<Summary>::WHERE(_::{summary_id}::EQ(summary_id))
    RETURN summaries

// Get summaries by document_id property (maps to Python GetDocumentSummaries)
QUERY GetSummariesByDocumentId(document_id: String) =>
    summaries <- N<Summary>::WHERE(_::{document_id}::EQ(document_id))
    RETURN summaries

// Get summaries by document_id and level (maps to Python GetDocumentSummaries with level filter)
QUERY GetDocumentSummariesByLevel(document_id: String, level: String) =>
    summaries <- N<Summary>::WHERE(
        AND(
            _::{document_id}::EQ(document_id),
            _::{level}::EQ(level)
        )
    )
    RETURN summaries

// Get summaries by source_id (chunk or document)
QUERY GetSummariesBySourceId(source_id: String) =>
    summaries <- N<Summary>::WHERE(_::{source_id}::EQ(source_id))
    RETURN summaries

// Get summaries by level only
QUERY GetSummariesByLevel(level: String) =>
    summaries <- N<Summary>::WHERE(_::{level}::EQ(level))
    RETURN summaries

// List all summaries
QUERY ListSummaries() =>
    summaries <- N<Summary>
    RETURN summaries

// List summarized documents (returns all Summary nodes; client deduplicates by document_id)
QUERY ListSummarizedDocuments() =>
    summaries <- N<Summary>
    RETURN summaries

// ========== SUMMARY TRAVERSALS ==========

// Get summaries from document via edge
QUERY GetDocumentSummaries(doc_id: ID) =>
    summaries <- N<Document>(doc_id)::Out<DocumentHasSummary>
    RETURN summaries

// Get summary from chunk via edge
QUERY GetChunkSummary(chunk_id: ID) =>
    summaries <- N<Chunk>(chunk_id)::Out<ChunkHasSummary>
    RETURN summaries

// Get child summaries via edge
QUERY GetSummaryChildren(summary_id: ID) =>
    children <- N<Summary>(summary_id)::Out<SummaryHasChild>
    RETURN children

// Get parent summary via edge (reverse)
QUERY GetSummaryParent(summary_id: ID) =>
    parent <- N<Summary>(summary_id)::In<SummaryHasChild>
    RETURN parent

// ========== SUMMARY VECTOR SEARCH ==========

// Vector similarity search on summaries
QUERY SearchSimilarSummaries(query_vec: [F64], top_k: U32) =>
    results <- SearchV<SummaryVector>(query_vec, top_k)
    RETURN results

// ========== SUMMARY DELETION ==========

// Delete a summary (cascades to connected edges) - maps to Python DeleteSummary
QUERY DeleteSummary(summary_id: ID) =>
    DROP N<Summary>(summary_id)
    RETURN "Removed summary"

// Delete summary embedding edge
QUERY DropSummaryEmbeddingEdge(summary_id: ID) =>
    DROP N<Summary>(summary_id)::OutE<SummaryHasEmbedding>
    RETURN "Removed summary embedding edge"

// Delete all summaries for a document - maps to Python DeleteDocumentSummaries
// Note: This returns summaries for client to drop individually since HelixQL
// doesn't support DELETE WHERE in a single query
QUERY DeleteDocumentSummaries(document_id: String) =>
    summaries <- N<Summary>::WHERE(_::{document_id}::EQ(document_id))
    RETURN summaries