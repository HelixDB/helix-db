use super::{
    source_steps::{AddE, AddN, EFromID, EFromType, NFromID, NFromType},
    traversal_steps::{In, InE, OrderBy, Out, OutE, Range, Traversal, Where},
};

pub trait GeneratorOutput {
    fn generate_headers(&self) -> String;
    fn generate_source(&self) -> String;
    fn generate_query(&self) -> String;
    fn generate_traversal(&self, traversal: Traversal) -> String;

    // ============================================================
    // terminals
    // ============================================================

    // ------------------------------------------------------------
    // creates
    // ------------------------------------------------------------
    /// Used to insert a node into the graph
    ///
    /// **Inputs:**
    /// - label: `&'a str`
    /// - properties: `Vec<(String, Value)>`
    /// - secondary_indices: `Option<&'a [String]>`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new_mut
    /// add_n(&label, properties, Some(idxs))
    /// ```
    ///
    ///
    /// ## HQL Example :
    /// ```
    /// QUERY addN({field1: value1, field2: value2, ...})
    ///     n <- AddN<NodeType>({field1: value1, field2: value2, ...})
    /// ```
    ///
    /// ## HQL Rules:
    /// - nothing can come before `add_n`
    fn generate_add_n(&self, add_n: AddN) -> String;

    /// Used to insert an edge into the graph
    ///
    /// **Inputs:**
    /// - label: `&'a str`
    /// - properties: `Vec<(String, Value)>`
    /// - from: `&'a str`
    /// - to: `&'a str`
    /// - secondary_indices: `Option<&'a [String]>`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new_mut
    /// add_e(&label, properties, from, to, Some(idxs))
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY addE({field1: value1, field2: value2, ...})
    ///     e <- AddE<EdgeType>({field1: value1, field2: value2, ...})::From(from_node_id)::To(to_node_id)
    /// ```
    ///
    /// ## HQL Rules:
    /// - nothing can come before `add_e`
    fn generate_add_e(&self, add_e: AddE) -> String;

    /// Used to insert a vector into the graph
    ///
    /// **Inputs:**
    /// - query: `Vec<f64>` // to change to `[f32 | f64; {const}]` where `{const}` is the number of dimensions
    /// - vec_label: `&'a str`
    /// - fields: `Option<HashMap<String, Value>>`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new_mut
    /// add_v(&query, &vec_label, Some(fields))
    /// ```
    ///
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY insertV(vec: [F64], value1: String, value2: String)
    ///     vec <- AddV<VectorType>(vec, {field1: value1, field2: value2, ...})
    /// ```
    ///
    /// ## HQL Rules:
    /// - nothing can come before `add_v`
    fn generate_add_v(&self) -> String;

    // deletes
    /// Used to delete a node from the graph
    ///
    /// **Inputs:**
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new_mut
    /// drop()
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY dropItems(node_id) =>
    ///     DROP N<NodeType>(node_id)
    /// ```
    ///
    /// ## HQL Rules:
    /// - drop is not a step
    fn generate_drop(&self) -> String;

    // ------------------------------------------------------------
    // source steps
    // ------------------------------------------------------------
    /// Used to get a node from its id
    ///
    /// **Inputs:**
    /// - id: `u128`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// n_from_id(id)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY n_from_id(node_id) =>
    ///     nodes <- N<Type>(node_id)
    /// ```
    ///
    /// ## HQL Rules:
    /// - Type must exist in schema
    /// - the ID type is a UUID String in HQL
    ///     - This will get converted to a u128 inside the query
    fn generate_n_from_id(&self, n_from_id: NFromID) -> String;

    /// Used to get a node from its types
    ///
    /// **Inputs:**
    /// - types: `&'a str`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// n_from_type(&label)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY n_from_type(NodeType) =>
    ///     nodes <- N<NodeType>
    /// ```
    ///
    /// ## HQL Rules:
    /// - Type must exist in schema
    fn generate_n_from_type(&self, n_from_type: NFromType) -> String;

    /// Used to get an edge from its id
    ///
    /// **Inputs:**
    /// - id: `u128`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// e_from_id(id)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY e_from_id(edge_id) =>
    ///     edges <- E<EdgeType>(edge_id)
    /// ```
    ///
    /// ## HQL Rules:
    /// - Type must exist in schema
    /// - the ID type is a UUID String in HQL
    ///     - This will get converted to a u128 inside the query
    fn generate_e_from_id(&self, e_from_id: EFromID) -> String;

    /// Used to get an edge from its type
    ///
    /// **Inputs:**
    /// - type: `&'a str`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// e_from_type(&label)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY e_from_type(EdgeType) =>
    ///     edges <- E<EdgeType>
    /// ```
    ///
    /// ## HQL Rules:
    /// - Type must exist in schema
    fn generate_e_from_type(&self, e_from_type: EFromType) -> String;

    /// Used to search for a vector in the graph
    ///
    /// **Inputs:**
    /// -query: Vec<f64>
    /// - k: `usize`
    /// - filter: `Option<&[Fn(&HVector) -> bool]>`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// search_v(&query, k, filter)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY search_v(query: [F64], k: Int, doc_type: String) =>
    ///     vectors <- SearchV<VectorType>(query, k)::PREFILTER(_::{doc_type}::EQ(docType))
    /// ```
    ///
    /// ## HQL Rules:
    /// - the prefilter must be an EXISTS traversal or an anonymous traversal that evaluates to a boolean
    /// - the prefilter acts exactly like a WHERE clause
    /// - the k must be an integer
    /// - the query must be a vector
    ///  
    fn generate_search_v(&self) -> String;

    // ------------------------------------------------------------
    // traversal steps
    // ------------------------------------------------------------

    /// Used to get the outgoing edges of a node
    ///
    /// **Inputs:**
    /// - edge_label: `&'a str`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// out(edge_label)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY out(EdgeLabel) =>
    ///     nodes <- N<Type>::Out<EdgeLabel>
    /// ```
    ///
    /// ## HQL Rules:
    /// - the edge label must exist in the schema
    fn generate_out(&self, out: Out) -> String;

    /// Used to get the incoming edges of a node
    ///
    /// **Inputs:**
    /// - edge_label: `&'a str`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// in(&edge_label)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY in(EdgeLabel) =>
    ///     nodes <- N<Type>::In<EdgeLabel>
    /// ```
    ///
    /// ## HQL Rules:
    /// - the edge label must exist in the schema
    fn generate_in(&self, in_: In) -> String;

    /// Used to get the outgoing edges of a node
    ///
    /// **Inputs:**
    /// - edge_label: `&'a str`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// out_e(&edge_label)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY out_e(EdgeLabel) =>
    ///     edges <- E<EdgeType>::OutE<EdgeLabel>
    /// ```
    ///
    /// ## HQL Rules:
    /// - the edge label must exist in the schema
    fn generate_out_e(&self, out_e: OutE) -> String;

    /// Used to get the incoming edges of a node
    ///
    /// **Inputs:**
    /// - edge_label: `&'a str`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// in_e(edge_label)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY in_e(EdgeLabel) =>
    ///     edges <- E<EdgeType>::InE<EdgeLabel>
    /// ```
    ///
    /// ## HQL Rules:
    /// - the edge label must exist in the schema
    fn generate_in_e(&self, in_e: InE) -> String;

    /// Used to get the nodes connected to a node
    ///
    /// **Inputs:**
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// from_n()
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY from_n() =>
    ///     nodes <- N<Type>::OutE<EdgeLabel>::FromN
    /// ```
    ///
    fn generate_from_n(&self) -> String;

    /// Used to get the nodes connected to a node
    ///
    /// **Inputs:**
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// to_n()
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY to_n() =>
    ///     nodes <- N<Type>::InE<EdgeLabel>::ToN
    /// ```
    ///
    /// ## HQL Rules:
    /// - the edge label must exist in the schema
    fn generate_to_n(&self) -> String;

    // ------------------------------------------------------------
    // utils
    // ------------------------------------------------------------
    /// Used to filter the results of a query
    ///
    /// **Inputs:**
    /// - f: `Fn(&Result<TraversalVal, GraphError>, &RoTxn) -> Result<bool, GraphError>`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// .filter_ref(|val: TraversalVal, txn: &'a RoTxn<'a>| -> Result<bool, GraphError> {
    ///     // return true if val should be included
    ///     // return false if val should be excluded
    ///     // e.g. the following filters out all nodes with a name that is not "John"
    ///     if let Ok(TraversalVal::Node(node)) = val {
    ///         if let Some(value) = node.check_property("name") {
    ///             match value {
    ///                 Value::String(name) => return Ok(name == "John"),
    ///                 _ => return Ok(false),
    ///             }
    ///         }
    ///     }
    ///     false
    /// })
    /// ```
    /// - Note that the Result<bool, GraphError> is used because lmdb gets can be used and may fail thus returning an error.
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY filter_ref() =>
    ///     nodes <- N<Type>::WHERE(_::{name}::EQ("John"))
    /// ```
    ///
    /// ## HQL Rules:
    /// - anything that returns a collection of traversal items (nodes, edges, vectors) can be filtered
    /// - only an anonymous or an EXISTS traversal that evaluates to a boolean can be used in the WHERE clause
    fn generate_filter(&self, where_: Where) -> String;

    /// Used to get the range of a node
    ///
    /// **Inputs:**
    /// - start: `u128`
    /// - end: `u128`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// range(start, end)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY range(start, end) =>
    ///     nodes <- N<Type>::Range(start, end)
    /// ```
    ///  
    fn generate_range(&self, range: Range) -> String;

    /// Used to order the results of a query
    ///     
    /// **Inputs:**
    /// - value: `&'a Value`
    /// - orderType: `OrderType`
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// order_by(value, orderType)
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY order_by(order_by) =>             // |------| This is optional
    ///     nodes <- N<Type>::OrderBy(_::{property})::ASC
    ///                                             ::DESC
    /// ```
    ///
    fn generate_order_by(&self, order_by: OrderBy) -> String;

    /// Used to deduplicate the results of a query
    ///
    /// **Inputs:**
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// dedup()
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY dedup() =>
    ///     nodes <- N<Type>::Dedup
    /// ```
    ///
    /// ## HQL Rules:
    /// - anything that returns a collection of traversal items (nodes, edges, vectors) can be deduped
    /// - it deduplicates in place
    /// - for explicitness
    /// - only object access can come after Dedup for explicitness (each source item is pushed through the entire traversal pipeline so the results are deduplicated instead any intermediate results)
    ///     - only objects because don't want to implicitly only deduplicate at the end if dedup is used in the middle of a traversal.
    fn generate_dedup(&self) -> String;

    /// Used to count the number of results of a query
    ///
    /// **Inputs:**
    ///
    /// ## Internal API:
    /// ```
    /// // needs G::new
    /// count()
    /// ```
    ///
    /// ## HQL Example:
    /// ```
    /// QUERY count() =>
    ///     count <- N<Type>::Count
    /// ```
    ///
    fn generate_count(&self) -> String;

    // TODO:
    /// Used to iterate over the results of a query
    ///
    /// **Inputs:**
    ///
    /// ## Internal API:
    /// ```
    fn generate_for_each(&self) -> String;
}
