use super::super::helpers::{label_property_name, properties_to_object};
use super::*;

#[test]
fn property_helpers_preserve_labels_and_object_fields() {
    assert_eq!(label_property_name().as_ref(), "$label");
    assert_eq!(
        properties_to_object(vec![
            Property {
                name: "name".to_string(),
                value: DbPropertyValue::String("ada".to_string()),
            },
            Property {
                name: "score".to_string(),
                value: DbPropertyValue::I64(7),
            },
        ]),
        BTreeMap::from([
            (
                "name".to_string(),
                DbPropertyValue::String("ada".to_string())
            ),
            ("score".to_string(), DbPropertyValue::I64(7)),
        ])
    );
}
