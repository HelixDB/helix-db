// UpsertE documentation examples

// Example 1: Basic edge upsert with properties
QUERY UpsertFriendship(id1: ID, id2: ID, since: String) =>
    person1 <- N<Person>(id1)
    person2 <- N<Person>(id2)
    existing <- E<Knows>
    edge <- existing::UpsertE({since: since})::From(person1)::To(person2)
    RETURN edge

// Example 2: Upsert edge with multiple properties
QUERY UpsertFriendshipWithProps(id1: ID, id2: ID, since: String, strength: F32) =>
    person1 <- N<Person>(id1)
    person2 <- N<Person>(id2)
    existing <- E<Friendship>
    edge <- existing::UpsertE({since: since, strength: strength})::From(person1)::To(person2)
    RETURN edge

// Example 3: Flexible From/To ordering
QUERY UpsertEdgeToFrom(id1: ID, id2: ID, since: String) =>
    person1 <- N<Person>(id1)
    person2 <- N<Person>(id2)
    existing <- E<Knows>
    edge <- existing::UpsertE({since: since})::To(person2)::From(person1)
    RETURN edge

// Helper query to create persons
QUERY CreatePerson(name: String, age: U32) =>
    person <- AddN<Person>({name: name, age: age})
    RETURN person

// Helper query to get all edges
QUERY GetAllKnows() =>
    edges <- E<Knows>
    RETURN edges

QUERY GetAllFriendships() =>
    edges <- E<Friendship>
    RETURN edges
