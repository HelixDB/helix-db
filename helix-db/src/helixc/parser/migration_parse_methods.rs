use crate::helixc::parser::{
    HelixParser, ParserError, Rule,
    location::HasLoc,
    types::{Migration, MigrationItem, MigrationItemMapping, MigrationPropertyMapping, ValueCast},
    utils::{PairTools, PairsTools},
};
use pest::iterators::Pair;

impl HelixParser {
    pub(super) fn parse_migration_def(
        &self,
        pair: Pair<Rule>,
        filepath: &str,
    ) -> Result<Migration, ParserError> {
        let loc = pair.loc_with_filepath(filepath);
        let mut pairs = pair.into_inner();
        let from_version = pairs.try_next_inner()?.try_next()?;
        let to_version = pairs.try_next_inner()?.try_next()?;

        let body = pairs
            .try_next_inner()?
            .map(|p| self.parse_migration_item_mapping(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Migration {
            from_version: (
                from_version.loc(),
                from_version.as_str().parse::<usize>().map_err(|e| {
                    ParserError::from(format!(
                        "Invalid schema version number '{}': {e}",
                        from_version.as_str()
                    ))
                })?,
            ),
            to_version: (
                to_version.loc(),
                to_version.as_str().parse::<usize>().map_err(|e| {
                    ParserError::from(format!(
                        "Invalid schema version number '{}': {e}",
                        to_version.as_str()
                    ))
                })?,
            ),
            body,
            loc,
        })
    }

    pub(super) fn parse_migration_item_mapping(
        &self,
        pair: Pair<Rule>,
    ) -> Result<MigrationItemMapping, ParserError> {
        let loc = pair.loc();
        let pair_rule = pair.as_rule();
        let mut pairs = pair.into_inner();
        let from_item_type = match pairs.next() {
            Some(item_def) => match item_def.into_inner().next() {
                Some(item_decl) => match item_decl.as_rule() {
                    Rule::node_decl => (
                        item_decl.loc(),
                        MigrationItem::Node(item_decl.try_inner_next()?.as_str().to_string()),
                    ),
                    Rule::edge_decl => (
                        item_decl.loc(),
                        MigrationItem::Edge(item_decl.try_inner_next()?.as_str().to_string()),
                    ),
                    Rule::vec_decl => (
                        item_decl.loc(),
                        MigrationItem::Vector(item_decl.try_inner_next()?.as_str().to_string()),
                    ),
                    _ => {
                        return Err(ParserError::from(format!(
                            "Expected item declaration, got {:?}",
                            item_decl.as_rule()
                        )));
                    }
                },
                None => {
                    return Err(ParserError::from(format!(
                        "Expected item declaration, got {:?}",
                        pair_rule
                    )));
                }
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Expected item declaration, got {:?}",
                    pair_rule
                )));
            }
        };

        let to_item_type = match pairs.next() {
            Some(pair) => match pair.as_rule() {
                Rule::item_def => match pair.into_inner().next() {
                    Some(item_decl) => match item_decl.as_rule() {
                        Rule::node_decl => (
                            item_decl.loc(),
                            MigrationItem::Node(item_decl.try_inner_next()?.as_str().to_string()),
                        ),
                        Rule::edge_decl => (
                            item_decl.loc(),
                            MigrationItem::Edge(item_decl.try_inner_next()?.as_str().to_string()),
                        ),
                        Rule::vec_decl => (
                            item_decl.loc(),
                            MigrationItem::Vector(item_decl.try_inner_next()?.as_str().to_string()),
                        ),
                        _ => {
                            return Err(ParserError::from(format!(
                                "Expected item declaration, got {:?}",
                                item_decl.as_rule()
                            )));
                        }
                    },
                    None => {
                        return Err(ParserError::from(format!(
                            "Expected item, got {:?}",
                            pairs.peek()
                        )));
                    }
                },
                Rule::anon_decl => from_item_type.clone(),
                _ => {
                    return Err(ParserError::from(format!(
                        "Invalid item declaration, got {:?}",
                        pair.as_rule()
                    )));
                }
            },
            None => {
                return Err(ParserError::from(format!(
                    "Expected item_def, got {:?}",
                    pairs.peek()
                )));
            }
        };

        let remappings = match pairs.next() {
            Some(p) => match p.as_rule() {
                Rule::node_migration => p
                    .try_inner_next()?
                    .into_inner()
                    .map(|p| self.parse_field_migration(p))
                    .collect::<Result<Vec<_>, _>>()?,
                Rule::edge_migration => p
                    .try_inner_next()?
                    .into_inner()
                    .map(|p| self.parse_field_migration(p))
                    .collect::<Result<Vec<_>, _>>()?,
                _ => {
                    return Err(ParserError::from(
                        "Expected node_migration or edge_migration",
                    ));
                }
            },
            None => {
                return Err(ParserError::from(
                    "Expected node_migration or edge_migration",
                ));
            }
        };

        Ok(MigrationItemMapping {
            from_item: from_item_type,
            to_item: to_item_type,
            remappings,
            loc,
        })
    }

    pub(super) fn parse_cast(&self, pair: Pair<Rule>) -> Result<Option<ValueCast>, ParserError> {
        match pair.as_rule() {
            Rule::cast => Ok(Some(ValueCast {
                loc: pair.loc(),
                cast_to: self.parse_field_type(pair.try_inner_next()?, None)?,
            })),
            _ => Ok(None),
        }
    }

    pub(super) fn parse_field_migration(
        &self,
        pair: Pair<Rule>,
    ) -> Result<MigrationPropertyMapping, ParserError> {
        let loc = pair.loc();
        let mut pairs = pair.into_inner();
        let property_name = pairs.try_next()?;
        let property_value = pairs.try_next()?;
        let cast = if let Some(cast_pair) = pairs.next() {
            self.parse_cast(cast_pair)?
        } else {
            None
        };

        Ok(MigrationPropertyMapping {
            property_name: (property_name.loc(), property_name.as_str().to_string()),
            property_value: self.parse_field_value(property_value)?,
            default: None,
            cast,
            loc,
        })
    }
}
