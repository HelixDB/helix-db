QUERY addEmbedding(vec: [F32]) =>
    doc <- AddN<Doc>({content: "Hello, content!", number: 1})
    embedding <- AddV<Embedding>(vec, {chunk: "Hello, chunk!", chunk_id: 1, number: 1, reference: "Hello, reference!"})
    AddE<EmbeddingOf>::From(doc)::To(embedding)
    RETURN embedding

QUERY getAllEmbedding() =>
    c <- N<Doc>({number: 1})
    embeddings <- c::Out<EmbeddingOf>
    RETURN embeddings


QUERY searchEmbedding(query: [F32]) =>
    c <- N<Doc>({number: 1})
    embedding_search <- SearchV<Embedding>(query, 10)
    RETURN embedding_search::{
        chunk,
        chunk_id,
        number,
        reference
    }
