V::Embedding1<F16> {
    content: String
}

V::Embedding2<F32> {
    content: String
}

V::Embedding3<F64> {
    content: String
}

V::Embedding4 {
    content: String
}

QUERY add_embedding(content: String, vector: [F16]) =>
    embedding <- AddV<Embedding1>(vector, {content: content})
    RETURN embedding

QUERY search_embedding(content: String, vector: [F16]) =>
    embedding <- SearchV<Embedding1>(vector, 10)
    RETURN embedding


