// UpsertE documentation schema

N::Person {
    INDEX name: String,
    age: U32,
}

E::Knows {
    From: Person,
    To: Person,
    Properties: {
        since: String,
    }
}

E::Friendship {
    From: Person,
    To: Person,
    Properties: {
        since: String,
        strength: F32,
    }
}
