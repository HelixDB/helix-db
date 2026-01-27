// UpsertN documentation examples

// Example 1: Basic node upsert with properties
QUERY UpsertPerson(name: String, age: U32) =>
    existing <- N<Person>::WHERE(_::{name}::EQ(name))
    person <- existing::UpsertN({name: name, age: age})
    RETURN person

// Example 2: Upsert from a pre-fetched node by ID
QUERY UpsertPersonById(id: ID, new_age: U32) =>
    existing <- N<Person>(id)
    person <- existing::UpsertN({age: new_age})
    RETURN person

// Example 3: Upsert with WHERE filter
QUERY UpdateOrCreatePerson(name: String, new_age: U32) =>
    existing <- N<Person>::WHERE(_::{name}::EQ(name))
    person <- existing::UpsertN({name: name, age: new_age})
    RETURN person

// Helper query to create persons for testing
QUERY CreatePerson(name: String, age: U32) =>
    person <- AddN<Person>({name: name, age: age})
    RETURN person

// Helper query to get all persons
QUERY GetAllPersons() =>
    persons <- N<Person>
    RETURN persons
