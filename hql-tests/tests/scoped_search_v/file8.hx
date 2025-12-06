N::User {
    name: String,
    age: I32,
}

V::Memory {
    content: String,
}

E::StoredMemory {
    From: User,
    To: Memory,
}

QUERY vectorSearch(user_id: ID, query: [F64], limit: I64) => 
     memories <- N<User>(user_id)::Out<StoredMemory>
     RETURN memories