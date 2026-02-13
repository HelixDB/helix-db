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
