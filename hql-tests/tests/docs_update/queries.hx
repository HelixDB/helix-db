// Update documentation examples

// Example: Updating a person's profile
QUERY UpdateUser (user_id: ID, new_name: String, new_age: U32) =>
    updated <- N<Person>(user_id)::UPDATE({
        name: new_name,
        age: new_age
    })
    RETURN updated

// Helper query to create persons
QUERY CreatePerson (name: String, age: U32) =>
    person <- AddN<Person>({
        name: name,
        age: age,
    })
    RETURN person
