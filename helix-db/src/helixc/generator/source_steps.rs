use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::utils::{
    VecData, write_properties, write_properties_slice, write_secondary_indices,
};

use super::{
    bool_ops::BoExp,
    utils::{GenRef, GeneratedValue},
};

#[derive(Clone, Debug)]
pub enum SourceStep {
    /// Traversal starts from an identifier
    Identifier(GenRef<String>),
    /// Add a node
    AddN(AddN),
    /// Add an edge
    AddE(AddE),
    /// Insert a vector
    AddV(AddV),
    /// Upsert a node
    UpsertN(UpsertN),
    /// Upsert an edge
    UpsertE(UpsertE),
    /// Upsert a vector
    UpsertV(UpsertV),
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
    /// Lookup a vector by ID
    VFromID(VFromID),
    /// Lookup a vector by type
    VFromType(VFromType),
    /// Search for vectors
    SearchVector(SearchVector),
    /// Search for vectors using BM25
    SearchBM25(SearchBM25),
    Upsert(Upsert),
    /// Traversal starts from an anonymous node
    Anonymous,
    Empty,
}

#[derive(Clone, Debug)]
pub struct Upsert {
    /// Properties of node
    pub properties: Option<Vec<(String, GeneratedValue)>>,

    /// Names of properties to index on
    pub secondary_indices: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct AddE {
    /// Label of edge
    pub label: GenRef<String>,
    /// Properties of edge
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// From node ID
    pub from: GeneratedValue,
    /// To node ID
    pub to: GeneratedValue,
    /// Whether from is a plural variable (needs iteration)
    pub from_is_plural: bool,
    /// Whether to is a plural variable (needs iteration)
    pub to_is_plural: bool,
    pub is_unique: bool,
}
impl Display for AddE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If either from or to is plural, we need to generate iteration code
        match (self.from_is_plural, self.to_is_plural) {
            (false, false) => {
                // Both singular - from and to already have .id() appended
                write!(
                    f,
                    "add_edge({}, {}, {}, {}, false, {})",
                    self.label,
                    write_properties(&self.properties),
                    self.from,
                    self.to,
                    self.is_unique
                )
            }
            (true, false) => {
                // From is plural - iterate over from, to already has .id()
                write!(
                    f,
                    "{}.iter().map(|from_val| {{\n        G::new_mut(&db, &arena, &mut txn)\n        .add_edge({}, {}, from_val.id(), {}, false, {})\n        .collect_to_obj()\n    }}).collect::<Result<Vec<_>,_>>()?",
                    self.from,
                    self.label,
                    write_properties(&self.properties),
                    self.to,
                    self.is_unique
                )
            }
            (false, true) => {
                // To is plural - iterate over to, from already has .id()
                write!(
                    f,
                    "{}.iter().map(|to_val| {{\n        G::new_mut(&db, &arena, &mut txn)\n        .add_edge({}, {}, {}, to_val.id(), false, {})\n        .collect_to_obj()\n    }}).collect::<Result<Vec<_>,_>>()?",
                    self.to,
                    self.label,
                    write_properties(&self.properties),
                    self.from,
                    self.is_unique
                )
            }
            (true, true) => {
                // Both plural - nested iteration
                write!(
                    f,
                    "{}.iter().flat_map(|from_val| {{\n        {}.iter().map(move |to_val| {{\n            G::new_mut(&db, &arena, &mut txn)\n            .add_edge({}, {}, from_val.id(), to_val.id(), false, {})\n            .collect_to_obj()\n        }})\n    }}).collect::<Result<Vec<_>,_>>()?",
                    self.from,
                    self.to,
                    self.label,
                    write_properties(&self.properties),
                    self.is_unique,
                )
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct UpsertN {
    /// Label of node
    pub label: GenRef<String>,
    /// Properties of node
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// Names of properties to index on
    pub secondary_indices: Option<Vec<String>>,
}

impl Display for UpsertN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = if self.properties.is_some() {
            &self.properties
        } else {
            &Some(Vec::new())
        };
        write!(
            f,
            "upsert_n({}, {})",
            self.label,
            write_properties_slice(properties)
        )
    }
}

#[derive(Clone, Debug)]
pub struct UpsertE {
    /// Label of edge
    pub label: GenRef<String>,
    /// Properties of edge
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// From node ID
    pub from: GeneratedValue,
    /// To node ID
    pub to: GeneratedValue,
    /// Whether from is a plural variable (needs iteration)
    pub from_is_plural: bool,
    /// Whether to is a plural variable (needs iteration)
    pub to_is_plural: bool,
}
impl Display for UpsertE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = if self.properties.is_some() {
            &self.properties
        } else {
            &Some(Vec::new())
        };
        // If either from or to is plural, we need to generate iteration code
        match (self.from_is_plural, self.to_is_plural) {
            (false, false) => {
                // Both singular - from and to already have .id() appended
                write!(
                    f,
                    "upsert_e({}, {}, {}, {})",
                    self.label,
                    self.from,
                    self.to,
                    write_properties_slice(properties),
                )
            }
            (true, false) => {
                // From is plural - iterate over from, to already has .id()
                write!(
                    f,
                    "{}.iter().map(|from_val| {{\n        G::new_mut(&db, &arena, &mut txn)\n        .upsert_e({}, from_val.id(), {}, {})\n        .collect_to_obj()\n    }}).collect::<Result<Vec<_>,_>>()?",
                    self.from,
                    self.label,
                    self.to,
                    write_properties_slice(properties),
                )
            }
            (false, true) => {
                // To is plural - iterate over to, from already has .id()
                write!(
                    f,
                    "{}.iter().map(|to_val| {{\n        G::new_mut(&db, &arena, &mut txn)\n        .upsert_e({}, {}, to_val.id(), {})\n        .collect_to_obj()\n    }}).collect::<Result<Vec<_>,_>>()?",
                    self.to,
                    self.label,
                    self.from,
                    write_properties_slice(properties),
                )
            }
            (true, true) => {
                // Both plural - nested iteration
                write!(
                    f,
                    "{}.iter().flat_map(|from_val| {{\n        {}.iter().map(move |to_val| {{\n            G::new_mut(&db, &arena, &mut txn)\n            .upsert_e({}, from_val.id(), to_val.id(), {})\n            .collect_to_obj()\n        }})\n    }}).collect::<Result<Vec<_>,_>>()?",
                    self.from,
                    self.to,
                    self.label,
                    write_properties_slice(properties),
                )
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct UpsertV {
    /// Vector to upsert
    pub vec: VecData,
    /// Label of vector
    pub label: GenRef<String>,
    /// Properties of vector
    pub properties: Option<Vec<(String, GeneratedValue)>>,
}
impl Display for UpsertV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = if self.properties.is_some() {
            &self.properties
        } else {
            &Some(Vec::new())
        };
        write!(
            f,
            "upsert_v({}, {}, {})",
            self.vec,
            self.label,
            write_properties_slice(properties)
        )
    }
}

#[derive(Clone, Debug)]
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
            "insert_v({}, {}, {})",
            self.vec,
            self.label,
            write_properties(&self.properties)
        )
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct NFromType {
    /// Label of nodes to lookup
    pub label: GenRef<String>,
}
impl Display for NFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_type({})", self.label)
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct EFromType {
    /// Label of edges to lookup
    pub label: GenRef<String>,
}
impl Display for EFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_type({})", self.label)
    }
}

#[derive(Clone, Debug)]
pub struct VFromID {
    /// ID of vector
    pub id: GenRef<String>,
    /// Label of vector
    pub label: GenRef<String>,

    /// Whether to get the vector data
    pub get_vector_data: bool,
}

impl Display for VFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v_from_id({}, {})", self.id, self.get_vector_data)
    }
}

#[derive(Clone, Debug)]
pub struct VFromType {
    /// Label of vectors to lookup
    pub label: GenRef<String>,
    /// Whether to get the vector data
    pub get_vector_data: bool,
}

impl Display for VFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v_from_type({}, {})", self.label, self.get_vector_data)
    }
}

#[derive(Clone, Debug)]
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
        write!(
            f,
            "search_bm25({}, {}, {})?",
            self.type_arg, self.query, self.k
        )
    }
}

impl Display for SourceStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceStep::Identifier(_) => write!(f, ""),
            SourceStep::AddN(add_n) => write!(f, "{add_n}"),
            SourceStep::AddE(add_e) => write!(f, "{add_e}"),
            SourceStep::AddV(add_v) => write!(f, "{add_v}"),
            SourceStep::UpsertN(upsert_n) => write!(f, "{upsert_n}"),
            SourceStep::UpsertE(upsert_e) => write!(f, "{upsert_e}"),
            SourceStep::UpsertV(upsert_v) => write!(f, "{upsert_v}"),
            SourceStep::NFromID(n_from_id) => write!(f, "{n_from_id}"),
            SourceStep::NFromIndex(n_from_index) => write!(f, "{n_from_index}"),
            SourceStep::NFromType(n_from_type) => write!(f, "{n_from_type}"),
            SourceStep::EFromID(e_from_id) => write!(f, "{e_from_id}"),
            SourceStep::EFromType(e_from_type) => write!(f, "{e_from_type}"),
            SourceStep::SearchVector(search_vector) => write!(f, "{search_vector}"),
            SourceStep::SearchBM25(search_bm25) => write!(f, "{search_bm25}"),
            SourceStep::Upsert(upsert) => write!(f, "upsert({:?})", upsert),
            SourceStep::Anonymous => write!(f, ""),
            SourceStep::Empty => panic!("Should not be empty"),
            SourceStep::VFromID(v_from_id) => write!(f, "{v_from_id}"),
            SourceStep::VFromType(v_from_type) => write!(f, "{v_from_type}"),
        }
    }
}

#[derive(Clone, Debug)]
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
                self.vec, self.k, self.label,
            ),
        }
    }
}

#[derive(Clone, Debug)]
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
        write!(
            f,
            "n_from_index({}, {}, {})",
            self.label, self.index, self.key
        )
    }
}
