// HelixQL Schema Definition
// Based on the provided Rust structs

// Node: UserRecord
// Note: 'id' field is implicit in HelixQL and not declared in schema
N::User {
    country: U8
}

// Vector: ItemRecord
// Note: 'id' field is implicit and 'embedding' is implicit for vectors
V::Item {
    category: U16
}

// Edge: EdgeRecord
// Connects User nodes to Item nodes
E::Interacted {
    From: User,
    To: Item
}

// HelixQL Query Definitions
// Insertion queries for the three types

// Query 1: Insert a User node
// Creates a new User record with the provided country code
QUERY InsertUser(country: U8) =>
    user <- AddN<User>({
        country: country
    })
    RETURN user::{id}

// Query 2: Insert an Item vector node
// Creates a new Item record with embedding and category
// The embedding parameter is explicit as an array of F32 values
QUERY InsertItem(embedding: [F32], category: U16) =>
    item <- AddV<Item>(embedding, {
        category: category
    })
    RETURN item::{id}

// Query 3: Insert a UserItem edge
// Creates an edge connecting a User to an Item using their IDs
QUERY InsertInteractedEdge(user_id: ID, item_id: ID) =>
    e <- AddE<Interacted>::From(user_id)::To(item_id)
    RETURN NONE

N::Metadata {
    INDEX key: String,
    value: String
}

QUERY CreateDatasetId(dataset_id: String) =>
    metadata <- AddN<Metadata>({
        key: "dataset_id",
        value: dataset_id
    })
    RETURN metadata

QUERY UpdateDatasetId(dataset_id: String) =>
    DROP N<Metadata>({ key: "dataset_id" })
    metadata <- AddN<Metadata>({
        key: "dataset_id",
        value: dataset_id
    })
    RETURN metadata

// Query to get dataset_id
QUERY GetDatasetId() =>
    dataset_id <- N<Metadata>({ key: "dataset_id" })
    RETURN dataset_id::{value}

// --------------------------
// Benchmarks
//

QUERY PointGet(item_id: ID) =>
    item <- V<Item>(item_id)
    RETURN item::{id, category}

QUERY OneHop(user_id: ID) =>
    items <- N<User>(user_id)::Out<Interacted>
    RETURN items::{id, category}

QUERY OneHopFilter(user_id: ID, category: U16) =>
    items <- N<User>(user_id)::Out<Interacted>::WHERE(_::{category}::EQ(category))
    RETURN items::{id, category}

QUERY Vector(vector: [F32], top_k: I64) =>
    items <- SearchV<Item>(vector, top_k)
    RETURN items::{id, score, category}

QUERY VectorHopFilter(vector: [F32], top_k: I64, country: U8) =>
    items <- SearchV<Item>(vector, top_k)::WHERE(EXISTS(_::In<Interacted>::WHERE(_::{country}::EQ(country))))
    RETURN items::{id, category}
