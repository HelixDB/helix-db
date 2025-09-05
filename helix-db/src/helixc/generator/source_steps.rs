use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::utils::{write_properties, write_secondary_indices, VecData};

use super::{
    bool_op::BoExp,
    utils::{GenRef, GeneratedValue},
};

#[derive(Clone)]
pub enum SourceStep {
    /// Traversal starts from an identifier
    Identifier(GenRef<String>),
    /// Add a node
    AddN(AddN),
    /// Add an edge
    AddE(AddE),
    /// Insert a vector
    AddV(AddV),
    /// Lookup a node by ID
    NFromID(NFromID),
    /// Lookup a node by index
    NFromIndex(NFromIndex),
    /// Lookup a node by type
    NFromType(NFromType),
    /// Lookup an edge by ID
    EFromID(EFromID),
    /// Lookup an edge by type
    EFromType(EFromType),
    /// Search for vectors
    SearchVector(SearchVector),
    /// Search for vectors using BM25
    SearchBM25(SearchBM25),
    /// Traversal starts from an anonymous node
    Anonymous,
    Empty,
}

#[derive(Clone)]
pub struct AddN {
    /// Label of node
    pub label: GenRef<String>,
    /// Properties of node
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// Names of properties to index on
    pub secondary_indices: Option<Vec<String>>,
}
impl Display for AddN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = write_properties(&self.properties);
        let secondary_indices = write_secondary_indices(&self.secondary_indices);
        write!(
            f,
            "add_n({}, {}, {})",
            self.label, properties, secondary_indices
        )
    }
}

#[derive(Clone)]
pub struct AddE {
    /// Label of edge
    pub label: GenRef<String>,
    /// Properties of edge
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// From node ID
    pub from: GeneratedValue,
    /// To node ID
    pub to: GeneratedValue,
}
impl Display for AddE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "add_e({}, {}, {}, {}, true, EdgeType::Node)",
            self.label,
            write_properties(&self.properties),
            self.from,
            self.to
        )
    }
}
#[derive(Clone)]
pub struct AddV {
    /// Vector to add
    pub vec: VecData,
    /// Label of vector
    pub label: GenRef<String>,
    /// Properties of vector
    pub properties: Option<Vec<(String, GeneratedValue)>>,
}
impl Display for AddV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "insert_v::<fn(&HVector, &RoTxn) -> bool>({}, {}, {})",
            self.vec,
            self.label,
            write_properties(&self.properties)
        )
    }
}


#[derive(Clone)]
pub struct NFromID {
    /// ID of node
    pub id: GenRef<String>,
    /// Label of node 
    /// 
    /// - unused currently but kept in the case ID lookups need to be from specific table based on type
    pub label: GenRef<String>, 
}
impl Display for NFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_id({})", self.id)
    }
}

#[derive(Clone)]
pub struct NFromType {
    /// Label of nodes to lookup
    pub label: GenRef<String>,
}
impl Display for NFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_type({})", self.label)
    }
}

#[derive(Clone)]
pub struct EFromID {
    /// ID of edge
    pub id: GenRef<String>,
    /// Label of edge 
    /// 
    /// - unused currently but kept in the case ID lookups need to be from specific table based on type
    pub label: GenRef<String>, 
}
impl Display for EFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_id({})", self.id)
    }
}

#[derive(Clone)]
pub struct EFromType {
    /// Label of edges to lookup
    pub label: GenRef<String>,
}
impl Display for EFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_type({})", self.label)
    }
}

#[derive(Clone)]
pub struct SearchBM25 {
    /// Type of node to search for
    pub type_arg: GenRef<String>,
    /// Query to search for
    pub query: GeneratedValue,
    /// Number of results to return
    pub k: GeneratedValue,
}

impl Display for SearchBM25 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "search_bm25({}, {}, {})?", self.type_arg, self.query, self.k)
    }
}

impl Display for SourceStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceStep::Identifier(_) => write!(f, ""),
            SourceStep::AddN(add_n) => write!(f, "{add_n}"),
            SourceStep::AddE(add_e) => write!(f, "{add_e}"),
            SourceStep::AddV(add_v) => write!(f, "{add_v}"),
            SourceStep::NFromID(n_from_id) => write!(f, "{n_from_id}"),
            SourceStep::NFromIndex(n_from_index) => write!(f, "{n_from_index}"),
            SourceStep::NFromType(n_from_type) => write!(f, "{n_from_type}"),
            SourceStep::EFromID(e_from_id) => write!(f, "{e_from_id}"),
            SourceStep::EFromType(e_from_type) => write!(f, "{e_from_type}"),
            SourceStep::SearchVector(search_vector) => write!(f, "{search_vector}"),
            SourceStep::SearchBM25(search_bm25) => write!(f, "{search_bm25}"),
            SourceStep::Anonymous => write!(f, ""),
            SourceStep::Empty => panic!("Should not be empty"),
        }
    }
}

#[derive(Clone)]
pub struct SearchVector {
    /// Label of vector to search for
    pub label: GenRef<String>,
    /// Vector to search for
    pub vec: VecData,
    /// Number of results to return
    pub k: GeneratedValue,
    /// Pre-filter to apply to the search - currently not implemented in grammar
    pub pre_filter: Option<Vec<BoExp>>,
}

impl Display for SearchVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.pre_filter {
            Some(pre_filter) => write!(
                f,
                "search_v::<fn(&HVector, &RoTxn) -> bool, _>({}, {}, {}, Some(&[{}]))",
                self.vec,
                self.k,
                self.label,
                pre_filter
                    .iter()
                    .map(|f| format!("|v: &HVector, txn: &RoTxn| {f}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            None => write!(
                f,
                "search_v::<fn(&HVector, &RoTxn) -> bool, _>({}, {}, {}, None)",
                self.vec,
                self.k,
                self.label,
            ),
        }
    }
}

#[derive(Clone)]
pub struct NFromIndex {
    /// Index to search against
    pub index: GenRef<String>,
    /// Key to search for in the index
    pub key: GeneratedValue,
    /// Label of nodes to lookup - used for post filtering
    pub label: GenRef<String>,
}

impl Display for NFromIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_index({}, {}, {})", self.label, self.index, self.key)
    }
}

