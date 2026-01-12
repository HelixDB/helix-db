N::Person { name: String, age: U32 }

E::Knows { From: Person, To: Person }

V::Document { content: String, embedding: [F32] }
