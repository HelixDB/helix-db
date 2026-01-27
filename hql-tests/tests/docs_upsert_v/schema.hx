// UpsertV documentation schema

V::Document {
    content: String,
}

N::Person {
    INDEX name: String,
    age: U32,
}

N::Company {
    INDEX name: String,
}

V::Resume {
    content: String,
}

E::WorksAt {
    From: Person,
    To: Company,
    Properties: {
        position: String,
    }
}
