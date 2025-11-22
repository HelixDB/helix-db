QUERY loaddocs_rag(chapters: [{ id: I64, subchapters: [{ title: String, content: String, chunks: [{chunk: String, vector: [F32]}]}] }]) =>
    FOR {id, subchapters} IN chapters {
        chapter_node <- AddN<Chapter>({ chapter_index: id })
        FOR {title, content, chunks} IN subchapters {
            subchapter_node <- AddN<SubChapter>({ title: title, content: content })
            AddE<Contains>::From(chapter_node)::To(subchapter_node)
            FOR {chunk, vector} IN chunks {
                vec <- AddV<Embedding>(vector)
                AddE<EmbeddingOf>({chunk: chunk})::From(subchapter_node)::To(vec)
            }
        }
    }
    RETURN "Success"

QUERY searchdocs_rag(query: [F32], k: I32) =>
    vecs <- SearchV<Embedding>(query, k)
    subchapters <- vecs::In<EmbeddingOf>
    RETURN subchapters::{title, content}

QUERY edge_node(id: ID) =>
    e <- N<Chapter>::OutE<Contains>
    RETURN e
