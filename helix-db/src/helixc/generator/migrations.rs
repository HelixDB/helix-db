use crate::{
    helixc::generator::utils::{GeneratedValue, Separator},
    protocol::value::casting::CastType,
};

#[derive(Debug)]
pub struct GeneratedMigration {
    pub from_version: String,
    pub to_version: String,
    pub body: Vec<GeneratedMigrationItemMapping>,
}
#[derive(Debug)]
pub struct GeneratedMigrationItemMapping {
    pub from_item: String,
    pub to_item: String,
    pub remappings: Vec<Separator<GeneratedMigrationPropertyMapping>>,
    pub should_spread: bool,
}

#[derive(Debug)]
pub enum GeneratedMigrationPropertyMapping {
    FieldAdditionFromOldField {
        old_field: GeneratedValue,
        new_field: GeneratedValue,
    },
    FieldAdditionFromValue {
        new_field: GeneratedValue,
        value: GeneratedValue,
    },
    FieldTypeCast {
        field: GeneratedValue,
        cast: CastType,
    },
}

impl std::fmt::Display for GeneratedMigration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for item in self.body.iter() {
            writeln!(
                f,
                "#[migration({}, {} -> {})]",
                item.from_item, self.from_version, self.to_version
            )?;
            writeln!(
                f,
                "pub fn migration_{}_{}_{}(mut props: HashMap<String, Value>) -> HashMap<String, Value> {{",
                item.from_item.to_ascii_lowercase(),
                self.from_version,
                self.to_version
            )?;
            writeln!(f, "let mut new_props = HashMap::new();")?;
            for remapping in item.remappings.iter() {
                writeln!(f, "{remapping}")?;
            }
            writeln!(f, "new_props")?;
            writeln!(f, "}}")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for GeneratedMigrationPropertyMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneratedMigrationPropertyMapping::FieldAdditionFromOldField {
                old_field,
                new_field,
            } => write!(
                f,
                "field_addition_from_old_field!(&mut props, &mut new_props, {new_field}, {old_field})"
            ),
            GeneratedMigrationPropertyMapping::FieldAdditionFromValue { new_field, value } => {
                write!(
                    f,
                    "field_addition_from_value!(&mut new_props, {new_field}, {value})"
                )
            }
            GeneratedMigrationPropertyMapping::FieldTypeCast { field, cast } => {
                write!(
                    f,
                    "field_type_cast!(&mut props, &mut new_props, {field}, {cast})"
                )
            }
        }
    }
}
