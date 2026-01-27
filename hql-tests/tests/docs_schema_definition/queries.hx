// Basic queries for schema definition test

QUERY CreateNode (field1: String, field2: U32) =>
    node <- AddN<NodeType>({
        field1: field1,
        field2: field2
    })
    RETURN node

QUERY GetNodes () =>
    nodes <- N<NodeType>
    RETURN nodes
