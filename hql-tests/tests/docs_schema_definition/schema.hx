// Schema definition examples from docs

// Node schema
N::NodeType {
    field1: String,
    field2: U32
}

// Edge schema
E::EdgeType {
    From: NodeType,
    To: NodeType,
    Properties: {
        field1: String,
        field2: U32
    }
}

// Vector schema
V::VectorType {
    field1: String
}
