//! Borrowed graph hydration boundary for scalar evaluation.
use super::{Entity, Result, Value};
use std::collections::{btree_map, BTreeMap};

/// Prepared user properties. A stored value outside the language's value domain
/// retains its error until that particular value is evaluated. Reading another
/// property, or just enumerating keys, does not evaluate dormant errors.
pub type GraphProperties = BTreeMap<String, Result<Value>>;

/// Batch-local graph values. Returned references borrow the prepared batch so
/// the evaluator can admit owned output before cloning strings or containers.
/// Implementations return an error for an unavailable entity rather than
/// treating an unprepared entity as an empty property map.
pub trait GraphValues: Sync {
    fn properties(&self, entity: Entity) -> Result<&GraphProperties>;
    fn label(&self, entity: Entity) -> Result<Option<&str>>;

    fn property(&self, entity: Entity, key: &str) -> Result<&Value> {
        self.properties(entity)?
            .get(key)
            .map(|value| value.as_ref().map_err(Clone::clone))
            .unwrap_or(Ok(&Value::Null))
    }

    fn keys(&self, entity: Entity) -> Result<btree_map::Keys<'_, String, Result<Value>>> {
        Ok(self.properties(entity)?.keys())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::QueryError;

    struct Graph(GraphProperties);
    impl GraphValues for Graph {
        fn properties(&self, entity: Entity) -> Result<&GraphProperties> {
            match entity {
                Entity::Node(1) => Ok(&self.0),
                _ => Err(QueryError::runtime(
                    "EntityNotFound",
                    "Missing",
                    "not prepared",
                )),
            }
        }
        fn label(&self, entity: Entity) -> Result<Option<&str>> {
            self.properties(entity).map(|_| Some("N"))
        }
    }

    #[test]
    fn hydration_is_borrowed_and_errors_are_evaluated_only_on_access() {
        let unsupported = QueryError::unsupported("StoredValue");
        let graph = Graph(BTreeMap::from([
            ("large".into(), Ok(Value::String("x".repeat(2048)))),
            ("unsupported".into(), Err(unsupported.clone())),
        ]));
        let entity = Entity::Node(1);
        assert!(std::ptr::eq(
            graph.property(entity, "large").unwrap(),
            graph.0["large"].as_ref().unwrap(),
        ));
        assert_eq!(graph.property(entity, "absent").unwrap(), &Value::Null);
        assert_eq!(
            graph
                .keys(entity)
                .unwrap()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["large", "unsupported"]
        );
        assert_eq!(
            graph.property(entity, "unsupported").unwrap_err(),
            unsupported
        );
        assert_eq!(graph.label(entity).unwrap(), Some("N"));
        assert!(graph.property(Entity::Node(2), "large").is_err());
        assert!(graph.keys(Entity::Node(2)).is_err());
        assert!(graph.label(Entity::Node(2)).is_err());
    }
}
