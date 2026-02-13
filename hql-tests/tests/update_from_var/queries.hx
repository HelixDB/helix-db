// Update a node from a variable
QUERY UpdateNodeFromVar(person_id: ID, new_name: String) =>
    person <- N<Person>(person_id)
    updated <- person::UPDATE({ name: new_name })
    RETURN updated

// Update an edge from a variable (the original reported bug pattern)
QUERY UpdateEdgeFromVar(person1Id: ID, person2Id: ID, since: String) =>
    edges <- N<Person>(person1Id)::OutE<Knows>::WHERE(_::ToN::ID::EQ(person2Id))
    updated_edge <- edges::UPDATE({ since: since })
    RETURN updated_edge

// Inline UPDATE still works (regression check)
QUERY UpdateNodeInline(person_id: ID, new_name: String) =>
    updated <- N<Person>(person_id)::UPDATE({ name: new_name })
    RETURN updated
