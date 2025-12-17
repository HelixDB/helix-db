N::File8 {
    name: String,
    age: I32,
}

V::File8Vec {
    content: String,
}

E::EdgeFile8 {
    From: File8,
    To: File8,
}


QUERY file8(vec: [F32]) =>
    new_vec <- AddV<File8Vec>(vec)
    AddV<File8Vec>(vec)
    RETURN new_vec
