////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  Cognee Vector Queries
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Checks if a collection exists
QUERY CogneeHasCollection (collection_name: String) =>
  collection <- V<CogneeVector>::WHERE(_::{collection_name}::EQ(collection_name))
  RETURN {collection: collection}

// Add multiple vectors to a collection with a given data points
QUERY CogneeCreateDataPoints (collection_name: String, data_points: [{vector: [F64], dp_id: String, payload: String, content: String}]) =>
	FOR {vector, dp_id, payload, content} IN data_points {
		AddV<CogneeVector>(vector, {collection_name: collection_name, data_point_id: dp_id, payload: payload, content: content})
	}
	RETURN NONE

// Retrieve data points from a collection using their IDs.
QUERY CogneeRetrieve (collection_name: String, dp_ids: [String]) =>
	documents <- V<CogneeVector>::WHERE(AND(_::{collection_name}::EQ(collection_name), _::{data_point_id}::IS_IN(dp_ids)))
	RETURN {documents: documents}

// Perform a search in the specified collection using a vector.
QUERY CogneeSearch (collection_name: String, vector: [F64], limit: I64) =>
	result <- SearchV<CogneeVector>(vector, limit)::WHERE(_::{collection_name}::EQ(collection_name))
	RETURN {result: result}

// Delete specified data points from a collection.
QUERY CogneeDeleteDataPoints (collection_name: String, dp_ids: [String]) =>
	DROP V<CogneeVector>::WHERE(AND(_::{collection_name}::EQ(collection_name), _::{data_point_id}::IS_IN(dp_ids)))
	RETURN NONE

// Delete all collections in the vector database.
QUERY CogneePruneCollections () =>
	DROP V<CogneeVector>
	RETURN NONE



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Cognee Graph Queries
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Get a single node by ID
QUERY CogneeGetNode (node_id: String) =>
	node <- N<CogneeNode>({node_id: node_id})
	RETURN node

// Get multiple nodes by list of IDs
QUERY CogneeGetNodes (node_ids: [String]) =>
	nodes <- N<CogneeNode>::WHERE(_::{node_id}::IS_IN(node_ids))
	RETURN {nodes: nodes}

// Get all neighboring nodes
QUERY CogneeGetNeighbors (node_id: String) =>
	node <- N<CogneeNode>({node_id: node_id})
	incoming <- node::In<CogneeEdge>
	outgoing <- node::Out<CogneeEdge>
	RETURN {incoming: incoming, outgoing: outgoing}

// Add a single node with properties (DataPoint as jason string)
QUERY CogneeAddNode (node_id: String, name: String, node_type: String, properties: String) =>
	node <- AddN<CogneeNode>({
		node_id: node_id,
		name: name,
		node_type: node_type,
		properties: properties
	})
	RETURN {node: node}

// Add multiple nodes
QUERY CogneeAddNodes (nodes: [{node_id: String, name: String, node_type: String, properties: String}]) =>
	FOR {node_id, name, node_type, properties} IN nodes {
		AddN<CogneeNode>({
			node_id: node_id,
			name: name,
			node_type: node_type,
			properties: properties
		})
	}
	RETURN NONE

// Delete a single node by ID
QUERY CogneeDeleteNode (node_id: String) =>
	DROP N<CogneeNode>({node_id: node_id})
	RETURN NONE

// Delete all nodes from a list of IDs
QUERY CogneeDeleteNodes (node_ids: [String]) =>
	DROP N<CogneeNode>::WHERE(_::{node_id}::IS_IN(node_ids))
	RETURN NONE

// Get edge by relationship between source and target nodes.
QUERY CogneeHasRelationship (from_node_id: String, to_node_id: String, relationship_name: String) =>
	from_node <- N<CogneeNode>({node_id: from_node_id})
	out_nodes <- from_node::OutE<CogneeEdge>::WHERE(_::{relationship_name}::EQ(relationship_name))::ToN
	result <- out_nodes::WHERE(_::{node_id}::EQ(to_node_id))
	RETURN {result: result}

// Add a single edge between nodes
QUERY CogneeAddEdge (from_node_id: String, to_node_id: String, relationship_name: String, properties: String) =>
	from_node <- N<CogneeNode>({node_id: from_node_id})
	to_node <- N<CogneeNode>({node_id: to_node_id})
  edge <- AddE<CogneeEdge>({
		relationship_name: relationship_name,
		properties: properties
	})::From(from_node)::To(to_node)
	RETURN {edge: edge}

// Add multiple edges between nodes
QUERY CogneeAddEdges (edges: [{from_node_id: String, to_node_id: String, relationship_name: String, properties: String}]) =>
	FOR {from_node_id, to_node_id, relationship_name, properties} IN edges {
		from_node <- N<CogneeNode>({node_id: from_node_id})
		to_node <- N<CogneeNode>({node_id: to_node_id})
		AddE<CogneeEdge>({
			relationship_name: relationship_name,
			properties: properties
		})::From(from_node)::To(to_node)
	}
	RETURN NONE

// Get all nodes and edges in the graph
QUERY CogneeGetGraphData () =>
	nodes <- N<CogneeNode>
	edges <- E<CogneeEdge>
	RETURN {nodes: nodes, edges: edges}

// Delete the entire graph
QUERY CogneeDeleteGraph () =>
	DROP N<CogneeNode>
	DROP E<CogneeEdge>
	RETURN NONE

// Get the target node and its entire neighborhood
QUERY CogneeGetConnections (node_id: String) =>
	main_node <- N<CogneeNode>({node_id: node_id})
	
	in_nodes <- main_node::In<CogneeEdge>
	in_edges <- main_node::InE<CogneeEdge>

	out_nodes <- main_node::Out<CogneeEdge>
	out_edges <- main_node::OutE<CogneeEdge>

	RETURN {main_node: main_node, in_nodes: in_nodes, in_edges: in_edges, out_nodes: out_nodes, out_edges: out_edges}

QUERY CogneeGetNodesetSubgraph (node_type: String, node_name: String) =>
	main_node <- N<CogneeNode>::WHERE(AND(_::{node_type}::EQ(node_type), _::{name}::EQ(node_name)))
	in_nodes <- main_node::In<CogneeEdge>
	in_edges <- main_node::InE<CogneeEdge>

	out_nodes <- main_node::Out<CogneeEdge>
	out_edges <- main_node::OutE<CogneeEdge>

	RETURN {main_node: main_node, in_nodes: in_nodes, in_edges: in_edges, out_nodes: out_nodes, out_edges: out_edges}
