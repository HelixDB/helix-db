N::MyNode { 
    field: String,
}


QUERY GetNodes (fields: [String]) =>
	node <- N<MyNode>::WHERE(_::{field}::IS_IN(fields))
    RETURN node


QUERY GetNodesByID (node_ids: [ID]) =>
	node <- N<MyNode>::WHERE(_::{id}::IS_IN(node_ids))
    RETURN node