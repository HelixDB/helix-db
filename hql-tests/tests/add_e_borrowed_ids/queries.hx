QUERY AddEdges(edges: [{from_id: ID, to_id: ID, since: String}]) =>
    FOR {from_id, to_id, since} IN edges {
        AddE<Knows>({since: since})::From(from_id)::To(to_id)
    }
    RETURN "Edges added successfully"
