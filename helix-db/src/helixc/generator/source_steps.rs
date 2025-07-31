use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::utils::{write_properties, write_secondary_indices, VecData};

use super::{
    generator_types::BoExp,
    utils::{GenRef, GeneratedValue},
};

#[derive(Clone)]
pub enum SourceStep {
    Identifier(GenRef<String>),
    AddN(AddN),
    AddE(AddE),
    AddV(AddV),
    NFromID(NFromID),
    NFromIndex(NFromIndex),
    NFromType(NFromType),
    EFromID(EFromID),
    EFromType(EFromType),
    SearchVector(SearchVector),
    SearchBM25(SearchBM25),
    Anonymous,
    Empty,
}

#[derive(Clone)]
pub struct AddN {
    pub label: GenRef<String>,
    pub properties: Option<Vec<(String, GeneratedValue)>>,
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
    pub label: GenRef<String>,
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    pub from: GeneratedValue,
    pub to: GeneratedValue,
    // pub secondary_indices: Option<Vec<String>>,
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
    pub vec: VecData,
    pub label: GenRef<String>,
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
    pub id: GenRef<String>,
    pub label: GenRef<String>, // possible not needed, do we do runtime label checking?
}
impl Display for NFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: possibly add label for runtime label checking?
        write!(f, "n_from_id({})", self.id)
    }
}

#[derive(Clone)]
pub struct NFromType {
    pub label: GenRef<String>,
}
impl Display for NFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_type({})", self.label)
    }
}

#[derive(Clone)]
pub struct EFromID {
    pub id: GenRef<String>,
    pub label: GenRef<String>, // possible not needed, do we do runtime label checking?
}
impl Display for EFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_id({})", self.id)
    }
}

#[derive(Clone)]
pub struct EFromType {
    pub label: GenRef<String>,
}
impl Display for EFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_type({})", self.label)
    }
}

#[derive(Clone)]
pub struct SearchBM25 {
    pub type_arg: GenRef<String>,
    pub query: GeneratedValue,
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
    pub label: GenRef<String>,
    pub vec: VecData,
    pub k: GeneratedValue,
    pub pre_filter: Option<Vec<BoExp>>,
}

impl Display for SearchVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.pre_filter {
            Some(pre_filter) => write!(
                f,
                "search_v::<fn(&HVector, &RoTxn) -> bool, _>({}, {}, {}, Some(&[{}]))",
                self.label,
                self.vec,
                self.k,
                pre_filter
                    .iter()
                    .map(|f| format!("|v: &HVector, txn: &RoTxn| {f}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            None => write!(
                f,
                "search_v::<fn(&HVector, &RoTxn) -> bool, _>({}, {}, {}, None)",
                self.label,
                self.vec,
                self.k
            ),
        }
    }
}

#[derive(Clone)]
pub struct NFromIndex {
    pub index: GenRef<String>,
    pub key: GeneratedValue,
}

impl Display for NFromIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_index({}, {})", self.index, self.key)
    }
}

