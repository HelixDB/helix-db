use super::{
    generator_output::GeneratorOutput,
    generator_types::GeneratedValue,
    source_steps::{AddE, AddN, EFromID, EFromType, NFromID, NFromType},
    traversal_steps::{In, InE, OrderBy, Out, OutE, Range, Traversal, Where},
};

pub struct NewGenerator {}


pub fn write_properties(properties: &Vec<(String, GeneratedValue)>) -> String {
    format!(
        "props! {{ {} }}",
        properties
            .iter()
            .fold(String::new(), |mut acc, (name, value)| {
                acc.push_str(&format!("{} => {}, ", name, value));
                acc
            }),
    )
}

pub fn write_secondary_indices(secondary_indices: &Option<Vec<String>>) -> String {
    match secondary_indices {
        Some(indices) => format!(
            "Some(&[{}]",
            indices.iter().fold(String::new(), |mut acc, idx| {
                acc.push_str(&format!("{}, ", idx));
                acc
            })
        ),
        None => "None".to_string(),
    }
}
