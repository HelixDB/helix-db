#![allow(clippy::unwrap_used)]

use std::io::Write;

use crate::helixc::parser::helix_parser::HxFile;

use super::helix_parser::{Content, FieldType, HelixParser, Source};

#[test]
fn test_parse_node_schema() {
    let input = r#"
        N::User {
            Name: String,
            Age: I32
        }
        "#;

    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.node_schemas.len(), 1);
    let schema = &result.node_schemas[0];
    assert_eq!(schema.name.1, "User");
    assert_eq!(schema.fields.len(), 2);
}

#[test]
fn test_parse_edge_schema() {
    let input = r#"

        E::Follows {
            From: User,
            To: User,
            Properties: {
                Since: F64
            }
        }
        "#;

    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.edge_schemas.len(), 1);
    let schema = &result.edge_schemas[0];
    assert_eq!(schema.name.1, "Follows");
    assert_eq!(schema.from.1, "User");
    assert_eq!(schema.to.1, "User");
    assert!(schema.properties.is_some());
    let properties = schema.properties.as_ref().unwrap();
    assert_eq!(properties.len(), 1);
    assert_eq!(properties[0].name, "Since");
    matches!(properties[0].field_type, FieldType::F64);
}

#[test]
fn test_parse_edge_schema_no_props() {
    let input = r#"

        E::Follows {
            From: User,
            To: User,
            Properties: {
            }
        }
        "#;
    let input = Content {
        content: input.to_string(),
        files: vec![],
        source: Source::default(),
    };
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.edge_schemas.len(), 1);
    let schema = &result.edge_schemas[0];
    assert_eq!(schema.name.1, "Follows");
    assert_eq!(schema.from.1, "User");
    assert_eq!(schema.to.1, "User");
    assert!(schema.properties.is_some());
    let properties = schema.properties.as_ref().unwrap();
    assert_eq!(properties.len(), 0);
}

#[test]
fn test_parse_query() {
    let input = r#"
        QUERY FindUser(userName : String) =>
            user <- N<User>
            RETURN user
        "#;

    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.queries.len(), 1);
    let query = &result.queries[0];
    assert_eq!(query.name, "FindUser");
    assert_eq!(query.parameters.len(), 1);
    assert_eq!(query.parameters[0].name.1, "userName");
    assert_eq!(query.statements.len(), 1);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_query_with_parameters() {
    let input = r#"
        QUERY fetchUsers(name: String, age: I32) =>
            user <- N<USER>("123")
            nameField <- user::{Name}
            ageField <- user::{Age}
            RETURN nameField, ageField
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.queries.len(), 1);
    let query = &result.queries[0];
    assert_eq!(query.name, "fetchUsers");
    assert_eq!(query.parameters.len(), 2);
    assert_eq!(query.parameters[0].name.1, "name");
    assert!(matches!(
        query.parameters[0].param_type.1,
        FieldType::String
    ));
    assert_eq!(query.parameters[1].name.1, "age");
    assert!(matches!(query.parameters[1].param_type.1, FieldType::I32));
    assert_eq!(query.statements.len(), 3);
    assert_eq!(query.return_values.len(), 2);
}

#[test]
fn test_node_definition() {
    let input = r#"
        N::USER {
            ID: String,
            Name: String,
            Age: I32
        }
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.node_schemas.len(), 1);
    let schema = &result.node_schemas[0];
    assert_eq!(schema.name.1, "USER");
    assert_eq!(schema.fields.len(), 3);
}

#[test]
fn test_edge_with_properties() {
    let input = r#"
        E::FRIENDSHIP {
            From: USER,
            To: USER,
            Properties: {
                Since: String,
                Strength: I32
            }
        }
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.edge_schemas.len(), 1);
    let schema = &result.edge_schemas[0];
    assert_eq!(schema.name.1, "FRIENDSHIP");
    assert_eq!(schema.from.1, "USER");
    assert_eq!(schema.to.1, "USER");
    let props = schema.properties.as_ref().unwrap();
    assert_eq!(props.len(), 2);
}

#[test]
fn test_multiple_schemas() {
    let input = r#"
        N::USER {
            ID: String,
            Name: String,
            Email: String
        }
        N::POST {
            ID: String,
            Content: String
        }
        E::LIKES {
            From: USER,
            To: POST,
            Properties: {
                Timestamp: String
            }
        }
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.node_schemas.len(), 2);
    assert_eq!(result.edge_schemas.len(), 1);
}

/// THESE FAIL
///
///
///

#[test]
fn test_logical_operations() {
    let input = r#"
    QUERY logicalOps(id : String) =>
        user <- N<USER>(id)
        condition <- user::{name}::EQ("Alice")
        condition2 <- user::{age}::GT(20)
        RETURN condition
    "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.name, "logicalOps");
    assert_eq!(query.statements.len(), 3);
}

#[test]
fn test_anonymous_traversal() {
    let input = r#"
    QUERY anonymousTraversal() =>
        result <- N::OutE<FRIENDSHIP>::InN::{Age}
        RETURN result
    "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.name, "anonymousTraversal");
    assert_eq!(query.statements.len(), 1);
}

#[test]
fn test_edge_traversal() {
    let input = r#"
    QUERY getEdgeInfo() =>
        edge <- E<FRIENDSHIP>("999")
        fromUser <- edge::OutE
        toUser <- edge::OutN
        RETURN fromUser, toUser

    "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 3);
    assert_eq!(query.return_values.len(), 2);
}

#[test]
fn test_exists_query() {
    let input = r#"
        QUERY userExists(id : String) =>
            user <- N<User>(id)
            result <- EXISTS(user::OutE::InN<User>)
            RETURN result
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.queries.len(), 1);
    let query = &result.queries[0];
    assert_eq!(query.name, "userExists");
    assert_eq!(query.parameters.len(), 1);
    assert_eq!(query.statements.len(), 2);
}

#[test]
fn test_multiple_return_values() {
    let input = r#"
    N::USER {
        Name: String,
        Age: Int
    }

    QUERY returnMultipleValues() =>
        user <- N<USER>("999")
        name <- user::{Name}
        age <- user::{Age}
        RETURN name, age
    "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 3);
    assert_eq!(query.return_values.len(), 2);
}

#[test]
fn test_add_fields() {
    let input = r#"
    QUERY enrichUserData() =>
        user <- N<USER>("123")
        enriched <- user::{Name: "name", Follows: _::Out<Follows>::{Age}}
        RETURN enriched
    "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
}

#[test]
fn test_query_with_count() {
    let input = r#"
    QUERY analyzeNetwork() =>
        user <- N<USER>("999")
        friends <- user::Out<FRIENDSHIP>::InN::WHERE(_::Out::COUNT::GT(0))
        friendCount <- activeFriends::COUNT
        RETURN friendCount
    "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 3);
}

#[test]
fn test_add_node_query() {
    let input = r#"
    QUERY analyzeNetwork() =>
        user <- AddN<User>({Name: "Alice"})
        RETURN user
    "#;
    let input = write_to_temp_file(vec![input]);
    let result = match HelixParser::parse_source(&input) {
        Ok(result) => result,
        Err(e) => {
            println!("{e:?}");
            panic!();
        }
    };
    let query = &result.queries[0];
    // println!("{:?}", query);
    assert_eq!(query.statements.len(), 1);
}

#[test]
fn test_add_edge_query() {
    let input = r#"
    QUERY analyzeNetwork() =>
        edge <- AddE<Rating>({Rating: 5})::To("123")::From("456")
        edge <- AddE<Rating>({Rating: 5, Date: "2025-01-01"})::To("123")::From("456")
        RETURN edge
    "#;
    let input = write_to_temp_file(vec![input]);
    let result = match HelixParser::parse_source(&input) {
        Ok(result) => result,
        Err(e) => {
            println!("{e:?}");
            panic!();
        }
    };
    let query = &result.queries[0];
    // println!("{:?}", query);
    assert_eq!(query.statements.len(), 2);
}

#[test]
fn test_adding_with_identifiers() {
    let input = r#"
    QUERY addUsers() =>
        user1 <- AddN<User>({Name: "Alice", Age: 30})
        user2 <- AddN<User>({Name: "Bob", Age: 25})
        AddE<Follows>({Since: "1.0"})::From(user1)::To(user2)
        RETURN user1, user2
    "#;
    let input = write_to_temp_file(vec![input]);
    let result = match HelixParser::parse_source(&input) {
        Ok(result) => result,
        Err(e) => {
            println!("{e:?}");
            panic!();
        }
    };
    // println!("{:?}", result);
    let query = &result.queries[0];
    // println!("{:?}", query);
    assert_eq!(query.statements.len(), 3);
}

#[test]
fn test_where_with_props() {
    let input = r#"
    QUERY getFollows() =>
        user <- N<User>::WHERE(_::{Age}::GT(2))
        user <- N<User>::WHERE(_::GT(2))
        RETURN user, follows
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = match HelixParser::parse_source(&input) {
        Ok(result) => result,
        Err(_e) => {
            panic!();
        }
    };
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
}

#[test]
fn test_drop_operation() {
    let input = r#"
        QUERY deleteUser(id: String) =>
            user <- N<USER>(id)
            DROP user
            DROP user::OutE
            DROP N::OutE
            RETURN user
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.name, "deleteUser");
    assert_eq!(query.parameters.len(), 1);
    assert_eq!(query.statements.len(), 4);
}

#[test]
fn test_update_operation() {
    let input = r#"
        QUERY updateUser(id: String) =>
            user <- N<USER>(id)
            x <- user::UPDATE({Name: "NewName"})
            l <- user::UPDATE({Name: "NewName", Age: 30})
            RETURN user
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.name, "updateUser");
    assert_eq!(query.parameters.len(), 1);
    assert_eq!(query.statements.len(), 3);
}

#[test]
fn test_complex_traversal_combinations() {
    let input = r#"
        QUERY complexTraversal() =>
            result1 <- N<User>::OutE<Follows>::InN<User>::{name}
            result2 <- N::WHERE(AND(
                _::{age}::GT(20),
                OR(_::{name}::EQ("Alice"), _::{name}::EQ("Bob"))
            ))
            result3 <- N<User>::{
                friends: _::Out<Follows>::InN::{name},
                avgFriendAge: _::Out<Follows>::InN::{age}::GT(25)
            }
            RETURN result1, result2, result3
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.name, "complexTraversal");
    assert_eq!(query.statements.len(), 3);
    assert_eq!(query.return_values.len(), 3);
}

#[test]
fn test_nested_property_operations() {
    let input = r#"
        QUERY nestedProps() =>
            user <- N<User>("123")
            // Test nested property operations
            result <- user::{
                basic: {
                    name: _::{name},
                    age: _::{age}
                },
                social: {
                    friends: _::Out<Follows>::COUNT,
                    groups: _::Out<BelongsTo>::InN<Group>::{name}
                }
            }
            RETURN result
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
}

#[test]
fn test_complex_edge_operations() {
    let input = r#"
        QUERY edgeOperations() =>
            edge1 <- AddE<Follows>({since: "2024-01-01", weight: 0.8})::From("user1")::To("user2")
            edge2 <- E<Follows>::WHERE(_::{weight}::GT(0.5))
            edge3 <- edge2::UPDATE({weight: 1.0, updated: "2024-03-01"})
            RETURN edge1, edge2, edge3
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 3);
    assert_eq!(query.return_values.len(), 3);
}

#[test]
fn test_mixed_type_operations() {
    let input = r#"
        QUERY mixedTypes() =>
            v1 <- AddN<User>({
                name: "Alice",
                age: 25,
                active: true,
                score: 4.5
            })
            result <- N<User>::WHERE(OR(
                _::{age}::GT(20),
                _::{score}::LT(5.0)
            ))
            RETURN v1, result
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
    assert_eq!(query.return_values.len(), 2);
}

#[test]
fn test_error_cases() {
    // Test missing return statement
    let missing_return = r#"
        QUERY noReturn() =>
            result <- N<User>()
        "#;
    let input = write_to_temp_file(vec![missing_return]);
    let result = HelixParser::parse_source(&input);
    assert!(result.is_err());

    // Test invalid property access
    let invalid_props = r#"
        QUERY invalidProps() =>
            result <- N<User>::{}
            RETURN result
        "#;
    let input = write_to_temp_file(vec![invalid_props]);
    let result = HelixParser::parse_source(&input);
    assert!(result.is_err());
}

#[test]
fn test_complex_schema_definitions() {
    let input = r#"
        N::ComplexUser {
            ID: String,
            Name: String,
            Age: I32,
            Score: F64,
            Active: Boolean
        }
        E::ComplexRelation {
            From: ComplexUser,
            To: ComplexUser,
            Properties: {
                StartDate: String,
                EndDate: String,
                Weight: F64,
                Valid: Boolean,
                Count: I32
            }
        }
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    assert_eq!(result.node_schemas.len(), 1);
    assert_eq!(result.edge_schemas.len(), 1);

    let node = &result.node_schemas[0];
    assert_eq!(node.fields.len(), 5);

    let edge = &result.edge_schemas[0];
    let props = edge.properties.as_ref().unwrap();
    assert_eq!(props.len(), 5);
}

#[test]
fn test_query_chaining() {
    let input = r#"
        QUERY chainedOperations() =>
            result <- N<User>("123")
                ::OutE<Follows>
                ::InN<User>
                ::{name}
                ::EQ("Alice")
            filtered <- N<User>::WHERE(
                _::Out<Follows>
                    ::InN<User>
                    ::{age}
                    ::GT(25)
            )
            updated <- filtered
                ::UPDATE({status: "active"})
            has_updated <- updated::{status}
                ::EQ("active")
            RETURN result, filtered, updated, has_updated
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 4);
    assert_eq!(query.return_values.len(), 4);
}

#[test]
fn test_property_assignments() {
    let input = r#"
        QUERY testProperties(age: I32) =>
            user <- AddN<User>({
                name: "Alice",
                age: age
            })
            RETURN user
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.parameters.len(), 1);
}

#[test]
fn test_map_operation() {
    let input = r#"
        QUERY mapOperation() =>
            user <- N<User>("123")
            mapped <- user::{name: "name", age: "age"}
            RETURN mapped
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_map_in_return() {
    let input = r#"
        QUERY mapInReturn() =>
            user <- N<User>("123")
            RETURN user::{
                name,
                age
            }
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 1);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_complex_object_operations() {
    let input = r#"
        QUERY complexObjects() =>
            user <- N<User>("123")
            result <- user::{
                basic: {
                    name,
                    age
                },
                friends: _::Out<Follows>::InN::{
                    name,
                    mutualFriends: _::Out<Follows>::COUNT
                }
            }
            RETURN result
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_exclude_fields() {
    let input = r#"
        QUERY excludeFields() =>
            user <- N<User>("123")
            filtered <- user::!{password, secretKey}
            RETURN filtered
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_spread_operator() {
    let input = r#"
        QUERY spreadFields() =>
            user <- N<User>("123")
            result <- user::{
                newField: "value",
                ..
            }
            RETURN result
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_complex_update_operations() {
    let input = r#"
        QUERY updateUser() =>
            user <- N<User>("123")
            updated <- user::UPDATE({
                name: "New Name",
                age: 30,
                lastUpdated: "2024-03-01",
                friendCount: _::Out<Follows>::COUNT
            })
            RETURN updated
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 2);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_nested_traversals() {
    let input = r#"
        QUERY nestedTraversals() =>
            start <- N<User>("123")
            result <- start::Out<Follows>::InN<User>::Out<Likes>::InN<Post>::{title}
            filtered <- result::WHERE(_::{likes}::GT(10))
            RETURN filtered
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 3);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_combined_operations() {
    let input = r#"
        QUERY combinedOps() =>
            // Test combination of different operations
            user <- N<User>("123")
            friends <- user::Out<Follows>::InN<User>
            active <- friends::WHERE(_::{active}::EQ(true))
            result <- active::{
                name,
                posts: _::Out<Created>::InN<Post>::!{deleted}::{
                    title: title,
                    likes: _::In<Likes>::COUNT
                }
            }
            RETURN result
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 4);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_closure() {
    let input = r#"
        QUERY multipleLayers() =>
            result <- N<User>::|user|{
                posts: _::Out<Created>::{
                    user_id: user::ID
                }
            }
            RETURN result
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    // println!("\n\nresult: {:?}\n\n", result);
    let query = &result.queries[0];
    assert_eq!(query.statements.len(), 1);
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_complex_return_traversal() {
    let input = r#"
        QUERY returnTraversal() =>
            RETURN N<User>::|user|{
                posts: _::Out<Created>::{
                    user_id: user::ID
                }
            }::!{createdAt, lastUpdated}::{username: name, ..}
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_array_as_param_type() {
    let input = r#"
        QUERY trWithArrayParam(ids: [String], names:[String], ages: [I32], createdAt: String) =>
            AddN<User>({Name: "test"})
            RETURN "SUCCESS"
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.return_values.len(), 1);

    assert!(
        query
            .parameters
            .iter()
            .any(|param| matches!(param.param_type.1, FieldType::String))
    );
    assert!(
        query
            .parameters
            .iter()
            .any(|param| match param.param_type.1 {
                FieldType::Array(ref field) => match &**field {
                    FieldType::String => param.name.1 == "names" || param.name.1 == "ids",
                    _ => false,
                },
                _ => false,
            })
    );
    assert!(
        query
            .parameters
            .iter()
            .any(|param| match param.param_type.1 {
                FieldType::Array(ref field) => match &**field {
                    FieldType::I32 => param.name.1 == "ages",
                    _ => false,
                },
                _ => false,
            })
    )
}

#[test]
fn test_schema_obj_as_param_type() {
    let input = r#"
        N::User {
            Name: String
        }

        QUERY trWithArrayParam(user: User) =>
            AddN<User>({Name: "test"})
            RETURN "SUCCESS"
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.return_values.len(), 1);

    // println!("{:?}", query.parameters);
    let mut param_type = "";
    assert!(
        query
            .parameters
            .iter()
            .any(|param| match param.param_type.1 {
                FieldType::Identifier(ref id) => match id.as_str() {
                    "User" => true,
                    _ => {
                        param_type = id;
                        false
                    }
                },
                _ => false,
            }),
        "Param of type {param_type} was not found"
    );
}

#[test]
fn test_add_vector() {
    let input = r#"
        V::User

        QUERY addVector(vector: [F64]) =>
            RETURN AddV<User>(vector)
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_bulk_insert() {
    let input = r#"
        QUERY bulkInsert(vectors: [[F64]]) =>
            BatchAddV<User>(vectors)
            RETURN "SUCCESS"
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.return_values.len(), 1);
}

#[test]
fn test_search_vector() {
    let input = r#"
        V::User

        QUERY searchVector(vector: [F64], k: I32) =>
            RETURN SearchV<User>(vector, k)
        "#;
    let input = write_to_temp_file(vec![input]);
    let result = HelixParser::parse_source(&input).unwrap();
    let query = &result.queries[0];
    assert_eq!(query.return_values.len(), 1);
}

pub fn write_to_temp_file(content: Vec<&str>) -> Content {
    let mut files = Vec::new();
    for c in content {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(c.as_bytes()).unwrap();
        let path = file.path().to_string_lossy().into_owned();
        files.push(HxFile {
            name: path,
            content: c.to_string(),
        });
    }
    Content {
        content: String::new(),
        files,
        source: Source::default(),
    }
}
