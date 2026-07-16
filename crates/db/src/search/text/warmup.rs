//! Query-derived text split warmup requirements.
//!
//! Search derives this compact description from the exact Tantivy query and
//! collector it is about to execute. Merging preserves the strongest postings
//! requirement for each term while keeping fast-field subcolumn coverage
//! explicit; execution remains owned by the parent text-search module.

use std::collections::{HashMap, HashSet};

use tantivy::schema::Field;
use tantivy::Term;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FastFieldWarmupInfo {
    pub(crate) name: String,
    pub(crate) with_subfields: bool,
}

impl FastFieldWarmupInfo {
    pub(crate) fn new(name: impl Into<String>, with_subfields: bool) -> Self {
        Self {
            name: name.into(),
            with_subfields,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct WarmupInfo {
    pub(crate) term_dict_fields: HashSet<Field>,
    pub(crate) fast_fields: HashSet<FastFieldWarmupInfo>,
    pub(crate) field_norms: bool,
    pub(crate) terms_grouped_by_field: HashMap<Field, HashMap<Term, bool>>,
}

impl WarmupInfo {
    pub(crate) fn merge(&mut self, other: Self) {
        self.term_dict_fields.extend(other.term_dict_fields);
        self.field_norms |= other.field_norms;

        for (field, terms) in other.terms_grouped_by_field {
            let entry = self.terms_grouped_by_field.entry(field).or_default();
            for (term, include_position) in terms {
                entry
                    .entry(term)
                    .and_modify(|current| *current |= include_position)
                    .or_insert(include_position);
            }
        }

        for fast_field in other.fast_fields {
            let covered_by_subfields = !fast_field.with_subfields
                && self
                    .fast_fields
                    .iter()
                    .any(|current| current.name == fast_field.name && current.with_subfields);
            if !covered_by_subfields {
                self.fast_fields.insert(fast_field);
            }
        }
    }

    pub(crate) fn simplify(&mut self) {
        for field in &self.term_dict_fields {
            if let Some(terms) = self.terms_grouped_by_field.get_mut(field) {
                terms.retain(|_, include_position| *include_position);
            }
        }

        self.terms_grouped_by_field
            .retain(|_, terms| !terms.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use tantivy::schema::Field;
    use tantivy::Term;

    use super::{FastFieldWarmupInfo, WarmupInfo};

    #[test]
    fn merge_ors_term_position_requirement() {
        let field = Field::from_field_id(1);
        let term = Term::from_field_text(field, "alice");
        let mut left = WarmupInfo {
            terms_grouped_by_field: HashMap::from([(
                field,
                HashMap::from([(term.clone(), false)]),
            )]),
            ..WarmupInfo::default()
        };
        left.merge(WarmupInfo {
            terms_grouped_by_field: HashMap::from([(field, HashMap::from([(term, true)]))]),
            ..WarmupInfo::default()
        });

        assert_eq!(
            left.terms_grouped_by_field[&field]
                .values()
                .copied()
                .collect::<Vec<_>>(),
            vec![true]
        );
    }

    #[test]
    fn merge_preserves_fast_field_subfield_asymmetry() {
        let mut left = WarmupInfo {
            fast_fields: HashSet::from([FastFieldWarmupInfo::new("entity_id", false)]),
            ..WarmupInfo::default()
        };
        left.merge(WarmupInfo {
            fast_fields: HashSet::from([FastFieldWarmupInfo::new("entity_id", true)]),
            ..WarmupInfo::default()
        });
        assert_eq!(left.fast_fields.len(), 2);

        let mut left = WarmupInfo {
            fast_fields: HashSet::from([FastFieldWarmupInfo::new("entity_id", true)]),
            ..WarmupInfo::default()
        };
        left.merge(WarmupInfo {
            fast_fields: HashSet::from([FastFieldWarmupInfo::new("entity_id", false)]),
            ..WarmupInfo::default()
        });
        assert_eq!(
            left.fast_fields,
            HashSet::from([FastFieldWarmupInfo::new("entity_id", true)])
        );
    }

    #[test]
    fn simplify_drops_only_non_position_terms_covered_by_term_dictionary() {
        let covered_field = Field::from_field_id(2);
        let retained_field = Field::from_field_id(3);
        let mut info = WarmupInfo {
            term_dict_fields: HashSet::from([covered_field]),
            terms_grouped_by_field: HashMap::from([
                (
                    covered_field,
                    HashMap::from([(Term::from_field_text(covered_field, "covered"), false)]),
                ),
                (
                    retained_field,
                    HashMap::from([(Term::from_field_text(retained_field, "retained"), false)]),
                ),
            ]),
            ..WarmupInfo::default()
        };

        info.simplify();

        assert!(!info.terms_grouped_by_field.contains_key(&covered_field));
        assert_eq!(info.terms_grouped_by_field[&retained_field].len(), 1);
    }
}
