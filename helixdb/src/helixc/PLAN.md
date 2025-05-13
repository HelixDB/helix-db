## db native functions

## `add_e`

---

#### deps

```rs
self
label: &'a str
properties: Vec<(String, Value)>
from: &'a str
to: &'a str
secondary_indices: Option<&'a [String]>

add_e(&label, properties, from, to, Some(idxs))
```

#### query

```
QUERY addE({field1: value1, field2: value2, ...})
    e <- AddE<Type>({field1: value1, field2: value2, ...})::From(from_node_id)::To(to_node_id)
```

#### HQL rules

- nothing can come before `AddE`

## `add_n`

---

#### deps

```rs
self
label: &'a str
properties: Vec<(String, Value)>
secondary_indices: Option<&'a [String]>
// needs G::new_mut
add_n(&label, properties, Some(idxs))
```

#### query

```
QUERY addN({field1: value1, field2: value2, ...})
    n <- AddN<Type>({field1: value1, field2: value2, ...})
```

#### HQL rules

- nothing can come before or after `AddN`

## `add_v`

---

#### deps

```rs
query: Vec<f64>
vec_label: String
fields: Option<HashMap<String, Value>> (HashMap::from(props! { ... }))
// needs G::new_mut
add_v(&query, &label, Some(fields))
```

#### query

```
QUERY insertV(vec: [F64], label: String)
    vec <- AddV<Type>(vec, {field1: value1, field2: value2, ...})
```

#### HQL rules

- nothing can come before or after `AddV`

## `drop`

---

#### deps

```rs
query: Vec<f64>
vec_label: String
fields: Option<HashMap<String, Value>> (HashMap::from(props! { ... }))
// needs G::new_mut
add_v(&query, &label, Some(fields))
```

#### query

```
QUERY insertV(vec: [F64], label: String)
    vec <- AddV<Type>(vec, {field1: value1, field2: value2, ...})
```

#### HQL rules

- nothing can come before or after `AddV`

## `dedup`

---

#### deps

```rs
none

.dedup()
```

#### query

```
QUERY dedup() =>
    nodes <- N<Type>::Out<EdgeType>::Dedup()
```

#### HQL rules

- anything that returns a collection of traversal items (nodes, edges, vectors) can be deduped
- it deduplicates in place
- for explicitness
- only object access can come after `Dedup` for explicitness (each source item is pushed through the entire traversal pipeline so the results are deduplicated instead any intermediate results)
  - only objects because don't want to implicitly only deduplicate at the end if dedup is used in the middle of a traversal.

## `filter_ref`

---

#### deps

```rs
f: Fn(&Result<TraversalVal, GraphError>, &RoTxn) -> Result<bool, GraphError>

.filter_ref(|val: TraversalVal, txn: &'a RoTxn<'a>| -> Result<bool, GraphError> {
    // return true if val should be included
    // return false if val should be excluded
    // e.g. the following filters out all nodes with a name that is not "John"
    if let Ok(TraversalVal::Node(node)) = val {
        if let Some(value) = node.check_property("name") {
            match value {
                Value::String(name) => return Ok(name == "John"),
                _ => return Ok(false),
            }
        }
    }
    false
})
```

- Note that the Result<bool, GraphError> is used because lmdb gets can be used and may fail thus returning an error.

#### query

```
QUERY filter_ref() =>
    nodes <- N<Type>::WHERE(_::{name}::EQ("John"))
```

#### HQL rules

- anything that returns a collection of traversal items (nodes, edges, vectors) can be filtered
- only an anonymous or an `EXISTS` traversal that evaluates to a boolean can be used in the `WHERE` clause

## `for ... in ...`

---

#### deps

> instead of using a for loop as we are currently doing
> could use a `.iter()` that takes iterates through the parameter vec

> QUESTION: do we allow the iterated value to be the result of a traversal?
> e.g. `FOR node in N<Type>::Out<EdgeType> {...}`

```rs
for data in data.nodes {
    // do something with data
}
```

#### query

```
QUERY for_in(nodes: [Type]) =>
    FOR node IN nodes {
        // do something with node
    }
```

#### HQL rules

- the iterated parameter must be a collection of items (nodes, edges, vectors)
- TODO: the iterated value can be the result of a traversal
- you can have nested for loops

## `range`

---

#### deps

```rs
start: i32
end: i32

.range(start, end)
```

#### query

```
QUERY range(start: Int, end: Int) =>
    nodes <- N<Type>::Range(start, end)
```

#### HQL rules

- the start and end must be integers
- the start must be less than the end
- the start and end must be positive
- if the start is greater than the length of the collection, it will return an empty collection
- if the end is greater than the length of the collection, it will return the collection from the start index to the end of the collection

## `update`

---

#### deps

```rs
// needs G::new_mut
.update(props: Vec<(String, Value)>)
```

#### query

```
QUERY update(node_id: ID, newNode: NodeType) =>
    N<NodeType>(node_id)::Update(newNode)
    // assuming the node type has the fields `field1` and `field2`
    N<NodeType>(node_id)::Update({field1: value1, field2: value2, ...})
```

#### HQL rules

- the value passed into Update must be of the corresponding node type
- or it can be a partial object that is on the node type

## `out`

---

#### deps

```rs
edge_label: &'a str

.out(edge_label: &'a str)
```

#### query

```
QUERY out() =>
    nodes <- N<Type>::Out<EdgeLabel>
```

## `out_e`

---

#### deps

```rs
edge_label: &'a str

.out_e(edge_label: &'a str)
```

#### query

```
QUERY out_e() =>
    edges <- N<Type>::OutE<EdgeLabel>
```

## `in_`

---

#### deps

```rs
edge_label: &'a str

.in(edge_label: &'a str)
```

#### query

```
QUERY in() =>
    nodes <- N<Type>::In<EdgeLabel>
```

## `in_e`

---

#### deps

```rs
edge_label: &'a str

.in_e(edge_label: &'a str)
```

#### query

```
QUERY in_e() =>
    edges <- N<Type>::InE<EdgeLabel>
```

## `from_n`

---

#### deps

```rs
.from_n()
```

#### query

```
QUERY from_n(edge_id: ID) =>
    nodes <- E<Type>(edge_id)::FromN()
```

## `to_n`

---

#### deps

```rs
.to_n()
```

#### query

```
QUERY to_n(edge_id: ID) =>
    nodes <- E<Type>(edge_id)::ToN()
```


## `search_v`

---

#### deps

```rs
query: Vec<f64>
k: usize
filter: Option<&[Fn(&HVector) -> bool]>

.search_v(query: Vec<f64>, k: usize, filter: Option<&[Fn(&HVector) -> bool]>)
```

#### query

```
QUERY search_v(query: [F64], k: Int, doc_type: String) =>
    vectors <- SearchV<VectorType>(query, k)::PREFILTER(_::{doc_type}::EQ(docType))
```

#### HQL rules

- the prefilter must be an `EXISTS` traversal or an anonymous traversal that evaluates to a boolean
- the prefilter acts exactly like a `WHERE` clause
- the k must be an integer
- the query must be a vector

## `e_from_id`

---

#### deps

```rs
edge_id: &u128

.e_from_id(edge_id: &u128)
```

#### query

```
QUERY e_from_id(edge_id: ID) =>
    edges <- E<Type>(edge_id)
```

#### HQL rules
- Type must exist in schema
- the ID type is a UUID String in HQL
  - This will get converted to a u128 inside the query

## `e_from_types`

---

#### deps

```rs
edge_label: &'a str

.e_from_types(edge_label: &'a str)
```

#### query

```
QUERY e_from_types() =>
    edges <- E<EdgeLabel>
```

#### HQL rules
- Type must exist in schema

## `n_from_id`

---

#### deps

```rs
node_id: &u128

.n_from_id(node_id: &u128)
```

#### query

```
QUERY n_from_id(node_id: ID) =>
    nodes <- N<Type>(node_id)
```

#### HQL rules
- Type must exist in schema
- the ID type is a UUID String in HQL
  - This will get converted to a u128 inside the query

## `n_from_types`

---

#### deps

```rs
edge_label: &'a str

.n_from_types(edge_label: &'a str)
```

#### query

```
QUERY n_from_types() =>
    nodes <- N<Type>
```

#### HQL rules
- Type must exist in schema

# TODO

## `bulk_add_e`

## `bulk_add_n`

## `insert_vs`

## `filter_mut`

---

#### deps

```rs
self

.filter_mut(filter)
```
