V::CogneeVector {
    collection_name: String,
    data_point_id: String,
    payload: String, // json.dumps(DataPoint) eg. (id as string, created_at, updated_at, ontology_valid, version, topological_rank, type)
    content: String,
    created_at: Date DEFAULT NOW,
    updated_at: Date DEFAULT NOW,
}

N::CogneeNode {
    INDEX node_id: String,
    name: String,
    node_type: String,
    properties: String, // (JSON string containing additional DataPoint fields like ontology_valid, version, topological_rank, metadata)
    created_at: Date DEFAULT NOW,
    updated_at: Date DEFAULT NOW,
}

E::CogneeEdge {
    From: CogneeNode,
    To: CogneeNode,
    Properties: {
        relationship_name: String,
        properties: String, // (JSON string containing additional DataPoint fields like ontology_valid, version, topological_rank, metadata)
    }
}