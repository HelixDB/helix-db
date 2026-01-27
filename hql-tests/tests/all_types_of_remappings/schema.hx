// N::Doc {
//     content: String
// }
//     
// V::Embedding {
//     chunk: String
// }
// 
// N::Chunk {
//     content: String
// }
// 
// E::EmbeddingOf {
//     From: Doc,
//     To: Embedding, 
//     Properties: {
//     }
// }

// N::User {
//     name: String,
//     age: I32
// }
// 
// E::Knows {
//     From: User,
//     To: User,
//     Properties: {
//         since: I32,
//     }
// }

N::File {
    name: String,
    extension: String,
    text: String,
    extracted_at: Date DEFAULT NOW
}

E::FileEdge {
    From: File,
    To: File,
    Properties: {
        since: I32,
    }
}