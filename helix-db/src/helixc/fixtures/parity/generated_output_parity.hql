N::User { INDEX email: String, name: String, age: U32 }
N::Company { name: String }
E::WorksAt { From: User, To: Company, Properties: { role: String } }
V::Document { title: String, content: String }

QUERY generatedParity(id: ID) =>
    user <- N<User>(id)
    company <- user::Out<WorksAt>
    profile <- user::{name, email}
    RETURN { profile: profile, company: company }

QUERY vectorParity(q: String) =>
    docs <- SearchV<Document>(Embed(q), 3)
    RETURN docs
