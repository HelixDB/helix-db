QUERY add_document(filename: String, upload_date: String, filetype: String, total_elements: I64) =>
  doc <- AddN<Document>({filename: filename, upload_date: upload_date, filetype: filetype, total_elements: total_elements})
  RETURN doc

QUERY add_chunk_with_metadata(
  doc_filename: String,
  element_id: String,
  text: String,
  element_type: String,
  page_number: I64,
  parent_id: String,
  category_depth: I64,
  metadata_json: String,
  order: I64,
  upload_date: String
) =>
  doc <- N<Document>({filename: doc_filename})
  chunk <- AddN<Chunk>({
    element_id: element_id,
    text: text,
    element_type: element_type,
    page_number: page_number,
    parent_id: parent_id,
    category_depth: category_depth,
    metadata_json: metadata_json,
    order: order
  })
  edge <- AddE<HAS_CHUNK>::From(doc)::To(chunk)
  RETURN chunk

QUERY add_embedding(chunk_element_id: String, vec: [F32]) =>
  chunk <- N<Chunk>({element_id: chunk_element_id})
  embedding <- AddV<ChunkEmbedding>(vec, {chunk_id: chunk_element_id})
  edge <- AddE<HAS_EMBEDDING>::From(chunk)::To(embedding)
  RETURN embedding

QUERY get_all_documents() =>
  docs <- N<Document>
  RETURN docs

QUERY get_document_chunks(doc_filename: String) =>
  doc <- N<Document>({filename: doc_filename})
  chunks <- doc::Out<HAS_CHUNK>
  RETURN chunks

QUERY search_similar_chunks(query_vec: [F32], limit: I64) =>
  embeddings <- SearchV<ChunkEmbedding>(query_vec, limit)
  chunks <- embeddings::In<HAS_EMBEDDING>
  RETURN chunks

QUERY get_chunk_with_context(element_id: String) =>
  chunk <- N<Chunk>({element_id: element_id})
  parent <- chunk::Out<HAS_PARENT>
  doc <- chunk::In<HAS_CHUNK>
  RETURN {chunk: chunk, parent: parent, document: doc}
