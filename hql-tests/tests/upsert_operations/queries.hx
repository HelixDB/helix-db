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

V::Document {
    content: String,
}


// =============================================================================
// Legacy UPSERT syntax - now using UpsertN
// =============================================================================

// Chained UpsertN syntax - updates existing if found, creates new if not
QUERY updateOrCreatePerson(name: String, new_age: U32) =>
    existing <- N<Person>::WHERE(_::{name}::EQ(name))
    person <- existing::UpsertN({name: name, age: new_age})
    RETURN person


// =============================================================================
// UpsertN - Node upsert operations
// =============================================================================

// Basic node upsert - updates existing if found, creates new if not
QUERY upsertPersonBasic(name: String, age: U32) =>
    existing <- N<Person>::WHERE(_::{name}::EQ(name))
    person <- existing::UpsertN({name: name, age: age})
    RETURN person

// Node upsert from a pre-fetched variable
QUERY upsertPersonFromVar(id: ID, new_age: U32) =>
    existing <- N<Person>(id)
    person <- existing::UpsertN({age: new_age})
    RETURN person

QUERY getNode(id: ID) => 
    node <- N<Person>(id)
    RETURN node


// =============================================================================
// UpsertE - Edge upsert operations (with From/To)
// =============================================================================

// Basic edge upsert with From/To
QUERY upsertFriendship(id1: ID, id2: ID, since: String) =>
    person1 <- N<Person>(id1)
    person2 <- N<Person>(id2)
    existing <- E<Knows>
    edge <- existing::UpsertE({since: since})::From(person1)::To(person2)
    RETURN edge

// Edge upsert from pre-filtered edges
QUERY upsertFilteredEdge(id1: ID, id2: ID, since: String) =>
    person1 <- N<Person>(id1)
    person2 <- N<Person>(id2)
    existing <- E<Knows>::WHERE(_::{since}::EQ(since))
    edge <- existing::UpsertE({since: since})::From(person1)::To(person2)
    RETURN edge

QUERY getEdge(id: ID) => 
    edge <- E<Knows>(id)
    RETURN edge


// =============================================================================
// UpsertV - Vector upsert operations
// =============================================================================

// Vector upsert with Embed function
QUERY upsertDocEmbed(text: String) =>
    existing <- V<Document>::WHERE(_::{content}::EQ(text))
    doc <- existing::UpsertV(Embed(text), {content: text})
    RETURN doc

// Vector upsert with vector literal identifier
QUERY upsertDocLiteral(vec: [F64], content: String) =>
    existing <- V<Document>::WHERE(_::{content}::EQ(content))
    doc <- existing::UpsertV(vec, {content: content})
    RETURN doc

QUERY getDoc(id: ID) => 
    doc <- V<Document>(id)
    RETURN doc