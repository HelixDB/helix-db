#[cfg(test)]
mod upsert_integration_tests {
    use helix_db::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // UpsertN Integration Tests
    // ============================================================================

    #[test]
    fn test_upsert_node_basic_compilation() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY upsertPerson(name: String, age: U32) =>
                person <- UpsertN<Person>({name: name, age: age})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok(), "UpsertN should parse successfully");
    }

    #[test]
    fn test_upsert_node_empty_fields() {
        let source = r#"
            N::Person { name: String }

            QUERY upsertPersonEmpty() =>
                person <- UpsertN<Person>()
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertN with empty fields should parse successfully"
        );
    }

    #[test]
    fn test_upsert_node_with_literals() {
        let source = r#"
            N::Person { name: String, active: Boolean }

            QUERY upsertPersonLiteral() =>
                person <- UpsertN<Person>({name: "Default", active: true})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertN with literal values should parse successfully"
        );
    }

    // ============================================================================
    // UpsertE Integration Tests
    // ============================================================================

    #[test]
    fn test_upsert_edge_basic() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY upsertFriendship(id1: ID, id2: ID) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                UpsertE<Knows>::From(person1)::To(person2)
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok(), "UpsertE should parse successfully");
    }

    #[test]
    fn test_upsert_edge_with_properties() {
        let source = r#"
            N::Person { name: String }
            E::Friendship { From: Person, To: Person, Properties: { since: String, strength: F32 } }

            QUERY upsertFriendshipWithProps(id1: ID, id2: ID, since: String, strength: F32) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                UpsertE<Friendship>({since: since, strength: strength})::From(person1)::To(person2)
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertE with properties should parse successfully"
        );
    }

    #[test]
    fn test_upsert_edge_to_from_order() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY upsertEdgeToFromOrder(id1: ID, id2: ID) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                UpsertE<Knows>::To(person2)::From(person1)
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertE with To::From order should parse successfully"
        );
    }

    // ============================================================================
    // UpsertV Integration Tests
    // ============================================================================

    #[test]
    fn test_upsert_vector_basic() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY upsertDoc(vector: [F32], content: String) =>
                doc <- UpsertV<Document>(vector, {content: content})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok(), "UpsertV should parse successfully");
    }

    #[test]
    fn test_upsert_vector_with_embed() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY upsertDocEmbed(text: String) =>
                doc <- UpsertV<Document>(Embed(text), {content: text})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertV with Embed should parse successfully"
        );
    }

    #[test]
    fn test_upsert_vector_with_string_embed() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY upsertDocStringEmbed() =>
                doc <- UpsertV<Document>(Embed("test document"), {content: "test document"})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertV with string Embed should parse successfully"
        );
    }

    #[test]
    fn test_upsert_vector_multiple_fields() {
        let source = r#"
            V::Document { content: String, title: String, author: String, embedding: [F32] }

            QUERY upsertDocMultiField(vec: [F32], content: String, title: String, author: String) =>
                doc <- UpsertV<Document>(vec, {content: content, title: title, author: author})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertV with multiple fields should parse successfully"
        );
    }

    // ============================================================================
    // Complex Upsert Scenarios
    // ============================================================================

    #[test]
    fn test_upsert_mixed_operations() {
        let source = r#"
            N::Person { name: String, age: U32 }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company, Properties: { position: String } }
            V::Resume { content: String, embedding: [F32] }

            QUERY complexUpsertOperation(
                person_name: String,
                person_age: U32,
                company_name: String,
                position: String,
                resume_content: String
            ) =>
                person <- UpsertN<Person>({name: person_name, age: person_age})
                company <- UpsertN<Company>({name: company_name})
                UpsertE<WorksAt>({position: position})::From(person)::To(company)
                resume <- UpsertV<Resume>(Embed(resume_content), {content: resume_content})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "Complex upsert scenario should parse successfully"
        );
    }

    #[test]
    fn test_upsert_in_traversal() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY upsertInTraversal(person_id: ID, friend_name: String) =>
                person <- N<Person>(person_id)
                friend <- UpsertN<Person>({name: friend_name})
                edge <- person::UpsertE<Knows>::To(friend)
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        if let Err(ref e) = result {
            println!("Debug: UpsertE in traversal failed with error: {:?}", e);
        }
        assert!(
            result.is_ok(),
            "Upsert in traversal should parse successfully: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_upsert_with_defaults() {
        let source = r#"
            N::Person {
                name: String,
                age: U32 DEFAULT 25,
                active: Boolean DEFAULT true
            }

            QUERY upsertWithDefaults(name: String) =>
                person <- UpsertN<Person>({name: name})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "Upsert with schema defaults should parse successfully"
        );
    }

    // ============================================================================
    // Error Cases
    // ============================================================================

    #[test]
    fn test_upsert_invalid_type() {
        let source = r#"
            N::Person { name: String }

            QUERY upsertInvalidType() =>
                item <- UpsertN<NonExistentType>({name: "test"})
                RETURN item
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        // This should parse but fail during analysis
        assert!(
            result.is_ok(),
            "Parser should handle unknown types gracefully"
        );
    }

    #[test]
    fn test_upsert_syntax_variations() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY testSyntaxVariations() =>
                // Basic upsert
                p1 <- UpsertN<Person>({name: "Alice"})

                // Upsert with multiple fields
                p2 <- UpsertN<Person>({name: "Bob", age: 30})

                // Empty upsert
                p3 <- UpsertN<Person>()

                RETURN p1
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "Various upsert syntax patterns should parse successfully"
        );
    }

    // ============================================================================
    // Property Validation Tests
    // ============================================================================

    #[test]
    fn test_upsert_with_comprehensive_properties() {
        let source = r#"
            N::Person {
                name: String,
                age: U32,
                email: String,
                active: Boolean DEFAULT true,
                INDEX phone: String
            }

            QUERY upsertPersonWithProperties(name: String, age: U32, email: String, phone: String) =>
                person <- UpsertN<Person>({
                    name: name,
                    age: age,
                    email: email,
                    phone: phone
                })
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertN with comprehensive properties should parse successfully"
        );
    }

    #[test]
    fn test_upsert_edge_with_complex_properties() {
        let source = r#"
            N::Person { name: String }
            E::Relationship {
                From: Person,
                To: Person,
                Properties: {
                    type: String,
                    strength: F32,
                    since: String,
                    active: Boolean DEFAULT true
                }
            }

            QUERY upsertRelationshipWithProps(
                id1: ID,
                id2: ID,
                rel_type: String,
                strength: F32,
                since: String
            ) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                rel <- UpsertE<Relationship>({
                    type: rel_type,
                    strength: strength,
                    since: since
                })::From(person1)::To(person2)
                RETURN rel
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertE with complex properties should parse successfully"
        );
    }

    #[test]
    fn test_upsert_vector_with_metadata_properties() {
        let source = r#"
            V::Document {
                content: String,
                title: String,
                author: String,
                published: Boolean DEFAULT false,
                rating: F32 DEFAULT 0.0,
                embedding: [F32]
            }

            QUERY upsertDocumentWithMetadata(
                vector: [F32],
                content: String,
                title: String,
                author: String,
                published: Boolean,
                rating: F32
            ) =>
                doc <- UpsertV<Document>(vector, {
                    content: content,
                    title: title,
                    author: author,
                    published: published,
                    rating: rating
                })
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertV with metadata properties should parse successfully"
        );
    }

    #[test]
    fn test_upsert_with_only_defaults() {
        let source = r#"
            N::Settings {
                theme: String DEFAULT "dark",
                notifications: Boolean DEFAULT true,
                volume: F32 DEFAULT 0.5
            }

            QUERY upsertDefaultSettings() =>
                settings <- UpsertN<Settings>()
                RETURN settings
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertN with only default values should parse successfully"
        );
    }

    #[test]
    fn test_upsert_mixed_literals_and_variables() {
        let source = r#"
            N::Product {
                name: String,
                price: F32,
                category: String,
                in_stock: Boolean
            }

            QUERY upsertProductMixed(name: String, price: F32) =>
                product <- UpsertN<Product>({
                    name: name,
                    price: price,
                    category: "electronics",
                    in_stock: true
                })
                RETURN product
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertN with mixed literals and variables should parse successfully"
        );
    }

    #[test]
    fn test_upsert_with_indexed_fields() {
        let source = r#"
            N::User {
                INDEX username: String,
                INDEX email: String,
                password_hash: String,
                created_at: String
            }

            QUERY upsertUser(username: String, email: String, password: String, timestamp: String) =>
                user <- UpsertN<User>({
                    username: username,
                    email: email,
                    password_hash: password,
                    created_at: timestamp
                })
                RETURN user
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertN with indexed fields should parse successfully"
        );
    }

    #[test]
    fn test_upsert_vector_embed_with_properties() {
        let source = r#"
            V::Article {
                title: String,
                content: String,
                tags: String,
                word_count: U32,
                embedding: [F32]
            }

            QUERY upsertArticleEmbed(title: String, content: String, tags: String, words: U32) =>
                article <- UpsertV<Article>(Embed(content), {
                    title: title,
                    content: content,
                    tags: tags,
                    word_count: words
                })
                RETURN article
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(
            result.is_ok(),
            "UpsertV with Embed and properties should parse successfully"
        );
    }
}
