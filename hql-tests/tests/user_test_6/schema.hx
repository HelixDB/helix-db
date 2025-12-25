N::Video {
    INDEX video_id: String,
    no_of_chunks: U8
}

N::Chunk {
    INDEX video_id: String,
    INDEX chunk_id: String,
    start_time: I16,
    end_time: I16,
    transcript: String, 
}

E::Has{
    From: Video, 
    To: Chunk
}

E::HasTranscriptEmbeddings {
    From: Chunk, 
    To: TranscriptEmbeddings
}

E::HasFrameSummaryEmbeddings {
    From: Chunk, 
    To: FrameSummaryEmbeddings
}

V::TranscriptEmbeddings {
    chunk_id: String,
    content: String
}

V::FrameSummaryEmbeddings {
    chunk_id: String,
    content: String 
}