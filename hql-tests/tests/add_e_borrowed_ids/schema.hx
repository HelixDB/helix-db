N::Person {
    name: String
}

E::Knows {
    From: Person,
    To: Person,
    Properties: {
        since: String
    }
}
