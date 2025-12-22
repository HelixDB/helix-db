// create a vector and connect to chunk node for transcript embeddings
#[model("gemini:gemini-embedding-001:RETRIEVAL_DOCUMENT")]
QUERY CreateTranscriptEmbeddings (chunk_id: String, content: String) =>
    chunk <- N<Chunk>({chunk_id: chunk_id})
    transcript_embeddings <- AddV<TranscriptEmbeddings>(Embed(content), {chunk_id: chunk_id, content: content})
    edge <- AddE<HasTranscriptEmbeddings>::From(chunk)::To(transcript_embeddings)
    RETURN "Success"


