//! Access-index property validation.

use crate::ir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AccessIndexProperty {
    Indexable(ir::NonEmptyString),
    NotIndexable(AccessIndexPropertyRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AccessIndexPropertyRejection {
    LabelScope,
    Nested,
    Empty,
}

pub(super) fn access_index_property(property: String) -> AccessIndexProperty {
    if property == "$label" {
        return AccessIndexProperty::NotIndexable(AccessIndexPropertyRejection::LabelScope);
    }
    if property.contains('.') {
        return AccessIndexProperty::NotIndexable(AccessIndexPropertyRejection::Nested);
    }
    match ir::NonEmptyString::new(property) {
        Some(property) => AccessIndexProperty::Indexable(property),
        None => AccessIndexProperty::NotIndexable(AccessIndexPropertyRejection::Empty),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_index_properties_reject_label_scope_dotted_and_empty_names() {
        assert!(matches!(
            access_index_property("age".to_owned()),
            AccessIndexProperty::Indexable(property) if property.as_ref() == "age"
        ));
        assert_eq!(
            access_index_property("$label".to_owned()),
            AccessIndexProperty::NotIndexable(AccessIndexPropertyRejection::LabelScope)
        );
        assert_eq!(
            access_index_property("profile.age".to_owned()),
            AccessIndexProperty::NotIndexable(AccessIndexPropertyRejection::Nested)
        );
        assert_eq!(
            access_index_property(String::new()),
            AccessIndexProperty::NotIndexable(AccessIndexPropertyRejection::Empty)
        );
    }
}
