use crate::helixc::parser::helix_parser::{
    AddEdge, AddNode, AddVector, Assignment, BatchAddVector, BooleanOp, EdgeConnection, EdgeSchema, EvaluatesToNumber, Expression, Field, FieldAddition, FieldType, FieldValue, GraphStep, IdType, NodeSchema, Parameter, Query, SearchVector, Source, StartNode::{Anonymous, Edge, Node, Variable}, Statement, Step, Traversal, ValueType, VectorData
};
use crate::helixc::parser::helix_parser::{Exclude, Object, StartNode};
use crate::protocol::value::Value;
use std::{collections::HashMap, vec};

pub struct CodeGenerator {
    indent_level: usize,
    current_variables: HashMap<String, String>,
}

impl CodeGenerator {
    pub fn new() -> Self {
        Self {
            indent_level: 0,
            current_variables: HashMap::new(),
        }
    }

    pub fn generate_headers(&mut self) -> String {
        let mut output = String::new();
        output.push_str("use std::collections::{HashMap, HashSet};\n");
        output.push_str("use std::cell::RefCell;\n");
        output.push_str("use std::sync::Arc;\n");
        output.push_str("use std::time::Instant;\n\n");
        output.push_str("use get_routes::handler;\n");
        output.push_str("use helixdb::{\n");
        output.push_str("    node_matches,\n");
        output.push_str("    props,\n");
        output.push_str("    helix_engine::graph_core::traversal::TraversalBuilder,\n");
        output.push_str("    helix_engine::graph_core::traversal_steps::{\n");
        output.push_str("        SourceTraversalSteps, TraversalBuilderMethods, TraversalSteps, TraversalMethods,\n");
        output.push_str("        TraversalSearchMethods, VectorTraversalSteps\n");
        output.push_str("    },\n");
        output.push_str("    helix_engine::types::GraphError,\n");
        output.push_str("    helix_gateway::router::router::HandlerInput,\n");
        output.push_str("    protocol::count::Count,\n");
        output.push_str("    protocol::response::Response,\n");
        output.push_str("    protocol::traversal_value::TraversalValue,\n");
        output.push_str("    protocol::remapping::ResponseRemapping,\n");
        output.push_str(
            "    protocol::{filterable::Filterable, value::Value, return_values::ReturnValue, remapping::Remapping},\n",
        );
        output.push_str("};\n");
        output.push_str("use sonic_rs::{Deserialize, Serialize};\n\n");
        output
    }

    fn indent(&self) -> String {
        "    ".repeat(self.indent_level)
    }

    fn generate_props_macro(&mut self, props: &HashMap<String, ValueType>) -> String {
        let props_str = props
            .iter()
            .map(|(k, v)| format!("\"{}\".to_string() => {}", k, self.value_type_to_rust(v)))
            .collect::<Vec<_>>()
            .join(", ");
        format!("props!{{ {} }}", props_str)
    }

    pub fn generate_source(&mut self, source: &Source) -> String {
        let mut output = String::new();

        // Generate node schema definitions
        for node_schema in &source.node_schemas {
            output.push_str(&mut self.generate_node_schema(node_schema));
            output.push_str("\n");
        }

        // Generate edge schema definitions
        for edge_schema in &source.edge_schemas {
            output.push_str(&mut self.generate_edge_schema(edge_schema));
            output.push_str("\n");
        }

        // Generate query implementations
        for query in &source.queries {
            output.push_str(&mut self.generate_query(query));
            output.push_str("\n");
        }

        output
    }

    fn generate_node_schema(&mut self, schema: &NodeSchema) -> String {
        let mut output = String::new();
        output.push_str(&format!("// Node Schema: {}\n", schema.name));
        output.push_str("#[derive(Serialize, Deserialize)]\n");
        output.push_str("struct ");
        output.push_str(&schema.name);
        output.push_str(" {\n");

        for field in &schema.fields {
            output.push_str(&format!(
                "    {}: {},\n",
                to_snake_case(&field.name),
                self.field_type_to_rust(&field.field_type)
            ));
        }

        output.push_str("}\n");
        output
    }

    fn generate_edge_schema(&mut self, schema: &EdgeSchema) -> String {
        let mut output = String::new();
        output.push_str(&format!("// Edge Schema: {}\n", schema.name));
        output.push_str("#[derive(Serialize, Deserialize)]\n");
        output.push_str("struct ");
        output.push_str(&schema.name);
        output.push_str(" {\n");

        for field in schema.properties.as_ref().unwrap_or(&vec![]) {
            output.push_str(&format!(
                "    {}: {},\n",
                to_snake_case(&field.name),
                self.field_type_to_rust(&field.field_type)
            ));
        }

        output.push_str("}\n");
        output
    }

    fn field_type_to_rust(&self, field_type: &FieldType) -> String {
        match field_type {
            FieldType::String => "String".to_string(),
            FieldType::Integer => "i32".to_string(),
            FieldType::Float => "f64".to_string(),
            FieldType::Boolean => "bool".to_string(),
            FieldType::Array(field) => format!("Vec<{}>", &Self::field_type_to_rust(&self, field)),
            FieldType::Identifier(id) => format!("{}", id),
            _ => "".to_string(),
        }
    }

    pub fn generate_query(&mut self, query: &Query) -> String {
        self.current_variables.clear();
        let mut output = String::new();

        // Generate function signature
        output.push_str("#[handler]\n");
        output.push_str(&format!("pub fn {}(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {{\n", to_snake_case(&query.name)));
        self.indent_level += 1;

        // Generate input struct if there are parameters
        if !query.parameters.is_empty() {
            output.push_str(&mut self.indent());
            output.push_str("#[derive(Serialize, Deserialize)]\n");
            output.push_str(&mut self.indent());
            output.push_str(&format!("struct {}Data {{\n", query.name));
            self.indent_level += 1;

            for param in &query.parameters {
                output.push_str(&mut self.indent());
                output.push_str(&format!(
                    "{}: {},\n",
                    to_snake_case(&param.name),
                    self.field_type_to_rust(&param.param_type)
                ));
            }

            self.indent_level -= 1;
            output.push_str(&mut self.indent());
            output.push_str("}\n\n");

            // Deserialize input data
            output.push_str(&mut self.indent());
            output.push_str(&format!(
                "let data: {}Data = match sonic_rs::from_slice(&input.request.body) {{\n",
                query.name
            ));
            output.push_str(&mut self.indent());
            output.push_str("    Ok(data) => data,\n");
            output.push_str(&mut self.indent());
            output.push_str("    Err(err) => return Err(GraphError::from(err)),\n");
            output.push_str(&mut self.indent());
            output.push_str("};\n\n");
        }

        //
        output.push_str(&mut self.indent());
        output.push_str("let mut remapping_vals: RefCell<HashMap<String, ResponseRemapping>> = RefCell::new(HashMap::new());\n");

        // Setup database transaction
        output.push_str(&mut self.indent());
        output.push_str("let db = Arc::clone(&input.graph.storage);\n");
        output.push_str(&mut self.indent());
        if query.statements.iter().any(|s| {
            matches!(s, Statement::AddNode(_))
                || matches!(s, Statement::AddEdge(_))
                || matches!(s, Statement::Drop(_))
                || matches!(s, Statement::AddVector(_))
                || matches!(s, Statement::BatchAddVector(_))
                || {
                    if let Statement::Assignment(assignment) = s {
                        matches!(assignment.value, Expression::AddNode(_))
                            || matches!(assignment.value, Expression::AddEdge(_))
                            || matches!(assignment.value, Expression::AddVector(_))
                            || matches!(assignment.value, Expression::BatchAddVector(_))
                            || {
                                let steps = match &assignment.value {
                                    Expression::Traversal(traversal) => &traversal.steps,
                                    _ => return false,
                                };
                                steps.iter().any(|step| matches!(step, Step::Update(_)))
                            }
                    } else {
                        false
                    }
                }
        }) {
            output.push_str("let mut txn = db.graph_env.write_txn().unwrap();\n\n");
        } else {
            output.push_str("let txn = db.graph_env.read_txn().unwrap();\n\n");
        }

        // Generate return values map if needed
        if !query.return_values.is_empty() {
            output.push_str(&mut self.indent());
            output.push_str(&format!("let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity({});\n\n", query.return_values.len()));
        }

        // Generate statements
        for statement in &query.statements {
            output.push_str(&mut self.generate_statement(statement, &query));
        }

        // Generate return statement
        if !query.return_values.is_empty() {
            output.push_str(&mut self.generate_return_values(&query.return_values, &query));
        }

        if query.statements.iter().any(|s| {
            matches!(s, Statement::AddNode(_))
                || matches!(s, Statement::AddEdge(_))
                || matches!(s, Statement::Drop(_))
                || matches!(s, Statement::AddVector(_))
                || matches!(s, Statement::BatchAddVector(_))
                || {
                    if let Statement::Assignment(assignment) = s {
                        matches!(assignment.value, Expression::AddNode(_))
                            || matches!(assignment.value, Expression::AddEdge(_))
                            || matches!(assignment.value, Expression::AddVector(_))
                            || matches!(assignment.value, Expression::BatchAddVector(_))
                    } else {
                        false
                    }
                }
        }) {
            output.push_str(&mut self.indent());
            output.push_str("txn.commit()?;\n");
        }

        // Close function
        output.push_str(&mut self.indent());
        output.push_str("Ok(())\n");
        self.indent_level -= 1;
        output.push_str("}\n");

        output
    }

    fn generate_statement(&mut self, statement: &Statement, query: &Query) -> String {
        match statement {
            Statement::Assignment(assignment) => self.generate_assignment(assignment, query),
            Statement::AddNode(add_vertex) => self.generate_add_vertex(add_vertex, None),
            Statement::AddEdge(add_edge) => self.generate_add_edge(add_edge),
            Statement::Drop(expr) => self.generate_drop(expr, query),
            Statement::AddVector(add_vector) => self.generate_add_vector(add_vector),
            Statement::SearchVector(search_vector) => self.generate_search_vector(search_vector),
            Statement::BatchAddVector(batch_add_vector) => self.generate_batch_add_vector(batch_add_vector),
        }
    }

    fn generate_add_vector(&mut self, add_vector: &AddVector) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );
        match &add_vector.data {
            Some(VectorData::Vector(vec)) => {
                output.push_str(&format!("tr.insert_vector(&mut txn, &{:?});\n", vec));
            }
            Some(VectorData::Identifier(id)) => {
                output.push_str(&format!("tr.insert_vector(&mut txn, &data.{});\n", id));
            }
            None => (),
        };

        output
    }

    fn generate_batch_add_vector(&mut self, batch_add_vector: &BatchAddVector) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );
        //iterate over the vectors and insert them
        output.push_str(&mut self.indent());
        match &batch_add_vector.vec_identifier {
            Some(id) => output.push_str(&format!("for vec in data.{} {{\n", id)),
            None => (),
        };

        output.push_str(&mut self.indent());
        output.push_str("    tr.insert_vector(&mut txn, &vec);\n");
        output.push_str(&mut self.indent());
        output.push_str("}\n");
        output
    }

    fn generate_search_vector(&mut self, vec: &SearchVector) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );
        let k = match &vec.k {
            Some(EvaluatesToNumber::Integer(k)) => k.to_string(),
            Some(EvaluatesToNumber::Float(k)) => k.to_string(),
            Some(EvaluatesToNumber::Identifier(id)) => format!("data.{} as usize", id),
            None => "10".to_string(),
        };
        match &vec.data {
            Some(VectorData::Vector(v)) => {
                output.push_str(&format!("tr.vector_search(&txn, &{:?}, {});\n", v, k));
            }
            Some(VectorData::Identifier(id)) => {
                output.push_str(&format!("tr.vector_search(&txn, &data.{}, {});\n", id, k));
            }
            None => panic!("No vector data provided for search vector, {:?}", vec),
        };
        output
    }

    fn generate_assignment(&mut self, assignment: &Assignment, query: &Query) -> String {
        let mut output = String::new();
        let var_name = &assignment.variable;

        output.push_str(&mut self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );

        output.push_str(&mut self.generate_expression(&assignment.value, query));

        // Store variable for later use
        self.current_variables
            .insert(var_name.clone(), var_name.clone());

        output.push_str(&mut self.indent());

        match assignment.value {
            _ => output.push_str(&format!(
                "let {} = tr.finish()?;\n\n",
                to_snake_case(var_name)
            )),
        }

        output
    }

    fn generate_expression(&mut self, expr: &Expression, query: &Query) -> String {
        let mut output = String::new();

        match expr {
            Expression::Traversal(traversal) => {
                output.push_str(&mut self.generate_traversal(traversal, query));
            }
            Expression::Identifier(id) => {
                if let Some(var_name) = self.current_variables.get(id) {
                    output.push_str(&mut self.indent());
                    output.push_str(&format!(
                        "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from({}.clone()));",
                        to_snake_case(var_name)
                    ));
                }
            }
            Expression::StringLiteral(s) => {
                output.push_str(&mut self.indent());
                output.push_str(&format!("\"{}\"", s));
            }
            Expression::IntegerLiteral(i) => {
                output.push_str(&mut self.indent());
                output.push_str(&i.to_string());
            }
            Expression::FloatLiteral(f) => {
                output.push_str(&mut self.indent());
                output.push_str(&f.to_string());
            }
            Expression::BooleanLiteral(b) => {
                output.push_str(&mut self.indent());
                output.push_str(&b.to_string());
            }
            Expression::AddNode(add_vertex) => {
                output.push_str(&mut self.generate_add_vertex(add_vertex, None));
            }
            Expression::AddEdge(add_edge) => {
                output.push_str(&mut self.generate_add_edge(add_edge));
            }
            Expression::BatchAddVector(batch_add_vector) => {
                output.push_str(&mut self.generate_batch_add_vector(batch_add_vector));
            }
            Expression::AddVector(add_vector) => {
                output.push_str(&mut self.generate_add_vector(add_vector));
            }
            Expression::SearchVector(search_vector) => {
                output.push_str(&mut self.generate_search_vector(search_vector));
            }
            Expression::Exists(traversal) => {
                output.push_str(&mut self.generate_exists_check(traversal, query));
            }
            _ => {}
        }

        output
    }

    fn generate_traversal(&mut self, traversal: &Traversal, query: &Query) -> String {
        let mut output = String::new();

        // Generate start node
        match &traversal.start {
            Node { types, ids } => {
                if let Some(ids) = ids {
                    output.push_str(&mut self.indent());
                    if let Some(var_name) = self.current_variables.get(&ids[0]) {
                        output.push_str(&format!("tr.v_from_id(&txn, {});\n", var_name));
                    } else {
                        output.push_str(&format!(
                            "tr.v_from_id(&txn, &data.{});\n",
                            to_snake_case(&ids[0])
                        ));
                    }
                } else if let Some(types) = types {
                    output.push_str(&mut self.indent());
                    output.push_str(&format!(
                        "tr.v_from_types(&txn, &[{}]);\n",
                        types
                            .iter()
                            .map(|t| format!("\"{}\"", t))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                } else {
                    output.push_str(&mut self.indent());
                    output.push_str("tr.v(&txn);\n");
                }
            }
            Edge { types, ids } => {
                if let Some(ids) = ids {
                    output.push_str(&mut self.indent());
                    if let Some(var_name) = self.current_variables.get(&ids[0]) {
                        output.push_str(&format!("tr.e_from_id(&txn, {});\n", var_name));
                    } else {
                        output.push_str(&format!(
                            "tr.e_from_id(&txn, &data.{});\n",
                            to_snake_case(&ids[0])
                        ));
                    }
                } else if let Some(types) = types {
                    output.push_str(&mut self.indent());
                    output.push_str("tr.e(&txn);\n");
                } else {
                    output.push_str(&mut self.indent());
                    output.push_str("tr.e(&txn);\n");
                }
            }
            Variable(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    output.push_str(&mut self.indent());
                    output.push_str(&format!(
                        "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from({}.clone()));\n",
                        to_snake_case(var_name)
                    ));
                }
            }
            Anonymous => {}
        }

        // Generate steps
        let mut skip_next = false;
        for (i, step) in traversal.steps.iter().enumerate() {
            if skip_next {
                skip_next = false;
                continue;
            }

            match step {
                // Step::Object(_) => {
                //     // If this is part of a count comparison, skip property checks
                //     if i < traversal.steps.len() - 2
                //         && matches!(traversal.steps[i + 1], Step::BooleanOperation(_))
                //         && matches!(traversal.steps[i + 2], Step::Count)
                //     {
                //         skip_next = true;
                //         continue;
                //     }
                //     output.push_str(&mut self.generate_step(step, query));
                // }
                Step::BooleanOperation(_) => {
                    // Skip boolean operations if they're part of a count comparison
                    if i < traversal.steps.len() - 1
                        && matches!(traversal.steps[i + 1], Step::Count)
                    {
                        continue;
                    }
                    output.push_str(&mut self.generate_step(step, query));
                }
                Step::Node(graph_step) => match graph_step {
                    GraphStep::Out(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!("tr.out(&txn, \"{}\");\n", types[0]));
                        } else {
                            output.push_str("tr.out(&txn, \"\");\n");
                        }
                    }
                    GraphStep::In(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!("tr.in_(&txn, \"{}\");\n", types[0]));
                        } else {
                            output.push_str("tr.in_(&txn, \"\");\n");
                        }
                    }
                    GraphStep::OutE(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!("tr.out_e(&txn, \"{}\");\n", types[0]));
                        } else {
                            output.push_str("tr.out_e(&txn, \"\");\n");
                        }
                    }
                    GraphStep::InE(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!("tr.in_e(&txn, \"{}\");\n", types[0]));
                        } else {
                            output.push_str("tr.in_e(&txn, \"\");\n");
                        }
                    }
                    GraphStep::Both(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!("tr.both(&txn, \"{}\");\n", types[0]));
                        } else {
                            output.push_str("tr.both(&txn, \"\");\n");
                        }
                    }
                    GraphStep::BothE(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!("tr.both_e(&txn, \"{}\");\n", types[0]));
                        } else {
                            output.push_str("tr.both_e(&txn, \"\");\n");
                        }
                    }
                    _ => output.push_str(&mut self.generate_step(step, query)),
                },
                Step::Edge(graph_step) => match graph_step {
                    GraphStep::InN => output.push_str("tr.in_v(&txn);\n"),
                    GraphStep::OutN => output.push_str("tr.out_v(&txn);\n"),
                    GraphStep::BothN => output.push_str("tr.both_v(&txn);\n"),
                    _ => output.push_str(&mut self.generate_step(step, query)),
                },
                _ => output.push_str(&mut self.generate_step(step, query)),
            }
        }

        output
    }
    fn generate_boolean_operation(&mut self, bool_op: &BooleanOp) -> String {
        let mut output = String::new();
        match bool_op {
            BooleanOp::Equal(value) => match &**value {
                Expression::BooleanLiteral(b) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Boolean(val) if *val == {})));\n", b));
                }
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if *val == {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if *val == {})));\n", f));
                }
                Expression::StringLiteral(s) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val == \"{}\")));\n", s));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val == data.{} )));\n", id));
                }
                _ => output.push_str(&format!("// Unhandled value type in EQ\n {:?}", value)),
            },
            BooleanOp::GreaterThan(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val > {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val > {})));\n", f));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val > data.{} )));\n", id));
                }
                _ => output.push_str("// Unhandled value type in GT\n"),
            },
            BooleanOp::GreaterThanOrEqual(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val >= {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val >= {})));\n", f));
                }
                Expression::StringLiteral(s) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::String(val) if val >= \"{}\")));\n", s));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val >= data.{} )));\n", id));
                }
                _ => output.push_str("// Unhandled value type in GTE\n"),
            },
            BooleanOp::LessThan(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val < {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val < {})));\n", f));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val < data.{} )));\n", id));
                }
                _ => output.push_str("// Unhandled value type in LT\n"),
            },
            BooleanOp::LessThanOrEqual(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val <= {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val <= {})));\n", f));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val <= data.{} )));\n", id));
                }
                _ => output.push_str("// Unhandled value type in LTE\n"),
            },
            BooleanOp::NotEqual(value) => match &**value {
                Expression::Identifier(id) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val != data.{} )));\n", id));
                }
                Expression::StringLiteral(s) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val != \"{}\"))", s));
                }
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if *val != {}))", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if *val != {}))", f));
                }
                Expression::BooleanLiteral(b) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Boolean(val) if *val != {}))", b));
                }
                _ => output.push_str(&format!("// Unhandled value type in NEQ\n {:?}", value)),
            },
            _ => output.push_str(&format!("// Unhandled boolean operation {:?}\n", bool_op)),
        }
        output
    }

    fn generate_step(&mut self, step: &Step, query: &Query) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());

        match step {
            // Step::Object(obj) => {
            //     output.push_str(&format!(
            //         "tr.get_properties(&txn, &vec![{}]);\n",
            //         obj.fields
            //             .iter()
            //             .map(|p| format!(
            //                 "\"{}\".to_string()",
            //                 match &p.1 {
            //                     FieldValue::Literal(Value::String(s)) => s.clone(),
            //                     _ => unreachable!(),
            //                 }
            //             ))
            //             .collect::<Vec<_>>()
            //             .join(", ")
            //     ));
            //     // Return the property name for the next step
            //     output.push_str(&format!(
            //         "let current_prop = \"{}\";\n",
            //         obj.fields[0].0.clone()
            //     ));
            // }
            Step::BooleanOperation(bool_op) => {
                output.push_str(&mut self.generate_boolean_operation(bool_op));
            }
            Step::Node(graph_step) => match graph_step {
                GraphStep::Out(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!("tr.out(&txn, \"{}\");\n", types[0]));
                    } else {
                        output.push_str("tr.out(&txn, \"\");\n");
                    }
                }
                GraphStep::In(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!("tr.in_(&txn, \"{}\");\n", types[0]));
                    } else {
                        output.push_str("tr.in_(&txn, \"\");\n");
                    }
                }
                GraphStep::OutE(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!("tr.out_e(&txn, \"{}\");\n", types[0]));
                    } else {
                        output.push_str("tr.out_e(&txn, \"\");\n");
                    }
                }
                GraphStep::InE(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!("tr.in_e(&txn, \"{}\");\n", types[0]));
                    } else {
                        output.push_str("tr.in_e(&txn, \"\");\n");
                    }
                }
                GraphStep::OutN => output.push_str("tr.out_v(&txn);\n"),
                GraphStep::InN => output.push_str("tr.in_v(&txn);\n"),
                GraphStep::BothN => output.push_str("tr.both_v(&txn);\n"),
                GraphStep::BothE(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!("tr.both_e(&txn, \"{}\");\n", types[0]));
                    } else {
                        output.push_str("tr.both_e(&txn, \"\");\n");
                    }
                }
                GraphStep::Both(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!("tr.both(&txn, \"{}\");\n", types[0]));
                    } else {
                        output.push_str("tr.both(&txn, \"\");\n");
                    }
                }
            },
            Step::Range((start, end)) => {
                let start = match start {
                    Expression::IntegerLiteral(val) => format!("{}", val),
                    Expression::Identifier(id) => format!("data.{}", to_snake_case(id)),
                    _ => unreachable!(),
                };
                let end = match end {
                    Expression::IntegerLiteral(val) => format!("{}", val),
                    Expression::Identifier(id) => format!("data.{}", to_snake_case(id)),
                    _ => unreachable!(),
                };

                output.push_str(&format!("tr.range({}, {});\n", start, end));
            }
            Step::Where(expr) => {
                match &**expr {
                    Expression::BooleanLiteral(b) => {
                        output.push_str(&format!("tr.filter_nodes(&txn, |_| Ok({}));\n", b));
                    }
                    Expression::Exists(traversal) => {
                        output.push_str(&mut self.generate_exists_check(traversal, query));
                    }
                    Expression::And(exprs) => {
                        output.push_str("tr.filter_nodes(&txn, |node| {\n");
                        output.push_str(&mut self.indent());
                        output.push_str("    Ok(");
                        for (i, expr) in exprs.iter().enumerate() {
                            if i > 0 {
                                output.push_str(" && ");
                            }
                            output.push_str(&mut self.generate_filter_condition(expr, query));
                        }
                        output.push_str(")\n");
                        output.push_str(&mut self.indent());
                        output.push_str("});\n");
                    }
                    Expression::Or(exprs) => {
                        output.push_str("tr.filter_nodes(&txn, |node| {\n");
                        output.push_str(&mut self.indent());
                        output.push_str("    Ok(");
                        for (i, expr) in exprs.iter().enumerate() {
                            if i > 0 {
                                output.push_str(" || ");
                            }
                            output.push_str(&mut self.generate_filter_condition(expr, query));
                        }
                        output.push_str(")\n");
                        output.push_str(&mut self.indent());
                        output.push_str("});\n");
                    }
                    Expression::Traversal(_) => {
                        // For traversal-based conditions
                        output.push_str("tr.filter_nodes(&txn, |node| {\n");
                        output.push_str(&mut self.indent());
                        output.push_str("    Ok(");
                        output.push_str(&mut self.generate_filter_condition(expr, query));
                        output.push_str("    )\n");
                        output.push_str(&mut self.indent());
                        output.push_str("});\n");
                        // output.push_str("    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));\n");
                        // output.push_str(&mut self.generate_traversal(traversal));
                        // output.push_str(&mut self.indent());
                        // output.push_str("    tr.count();\n");
                        // output.push_str(&mut self.indent());
                        // output.push_str("    let count = tr.finish()?.as_count().unwrap();\n");
                        // output.push_str(&mut self.indent());
                        // output.push_str("    Ok(count > 0)\n");
                        // output.push_str(&mut self.indent());
                        // output.push_str("});\n");
                    }
                    _ => {
                        output.push_str(&format!("// Unhandled where condition: {:?}\n", expr));
                    }
                }
            }
            Step::Count => {
                output.push_str("tr.count();\n");
            }
            // Step::ID => {
            //     // output.push_str("tr.id();\n");
            // }
            Step::Update(update) => {
                let props = update
                    .fields
                    .iter()
                    .map(|f| {
                        format!(
                            "\"{}\".to_string() => {}",
                            f.name,
                            self.generate_field_addition(&f.value, &query.parameters)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                output.push_str(&format!(
                    "tr.update_props(&mut txn, props!{{ {} }});\n",
                    props
                ));
            }
            Step::Object(obj) => {
                // Assume the current variable (e.g. from an earlier assignment) is named "current_var"
                output.push_str(&self.generate_object_remapping(true, None, obj, query));
            }
            Step::Closure(closure) => {
                output.push_str(&self.generate_object_remapping(
                    true,
                    Some(&closure.identifier),
                    &closure.object,
                    query,
                ));
            }
            Step::Exclude(exclude) => {
                output.push_str(&self.generate_exclude_remapping(true, None, exclude));
            }
            _ => {}
        }

        output
    }

    fn generate_filter_condition(&mut self, expr: &Expression, query: &Query) -> String {
        match expr {
            Expression::BooleanLiteral(b) => b.to_string(),
            Expression::Exists(traversal) => {
                format!("{{ let mut inner_tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone())); {} inner_tr.count(); let count = inner_tr.finish()?.as_count().unwrap(); count > 0 }}", 
                    self.generate_traversal(traversal, query))
            }
            Expression::Traversal(traversal) => {
                // For traversals that check properties with boolean operations
                let mut output = String::new();

                // match traversal.start {
                //     StartNode::Anonymous => {
                //         output.push_str("{");
                //         output.push_str("let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));");
                //         output.push_str(&process_steps(&traversal.steps));
                //         output.push_str("}");
                //     }
                //     _ => {
                //         output.push_str(&process_steps(&traversal.steps));
                //     },
                // }
                let mut inner_traversal = false;
                for (i, step) in traversal.steps.iter().enumerate() {
                    match step {
                        Step::Object(obj) => {
                            let prop_name = &obj.fields[0].0.clone();
                            if let Some(Step::BooleanOperation(bool_op)) =
                                traversal.steps.get(i + 1)
                            {
                                match bool_op {
                                    BooleanOp::Equal(value) => match &**value {
                                        Expression::BooleanLiteral(b) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Boolean(val) if *val == {}))", prop_name, b));
                                        }
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val == {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Float(val) if *val == {}))", prop_name, f));
                                        }
                                        Expression::StringLiteral(s) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::String(val) if *val == \"{}\"))", prop_name, s));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::String(val) if *val == data.{} )));\n", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in EQ */"),
                                    },
                                    BooleanOp::GreaterThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val > {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Float(val) if *val > {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val > data.{} )));\n", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GT */"),
                                    },
                                    BooleanOp::GreaterThanOrEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val >= {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Float(val) if *val >= {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val >= data.{} )));\n", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GTE */"),
                                    },
                                    BooleanOp::LessThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val < {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Float(val) if *val < {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val < data.{} )));\n", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LT */"),
                                    },
                                    BooleanOp::LessThanOrEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val <= {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Float(val) if *val <= {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val <= data.{} )));\n", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LTE */"),
                                    },
                                    BooleanOp::NotEqual(value) => match &**value {
                                        Expression::StringLiteral(s) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::String(val) if *val != \"{}\"))", prop_name, s));
                                        }
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val != {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Float(val) if *val != {}))", prop_name, f));
                                        }
                                        Expression::BooleanLiteral(b) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Boolean(val) if *val != {}))", prop_name, b));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::String(val) if *val != data.{} )));\n", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in NEQ */"),
                                    },
                                    _ => output.push_str(&format!(
                                        "/* Unhandled boolean operation {:?} */",
                                        bool_op
                                    )),
                                }
                            } else {
                                output.push_str(&format!(
                                    "node.check_property(\"{}\").is_some()",
                                    prop_name
                                ));
                            }
                            if inner_traversal {
                                output.push_str("}");
                            }

                            return output;
                        }
                        Step::Count => {
                            output.push_str("tr.count();\n");
                            output.push_str("let count = tr.finish()?.as_count().unwrap();\n");
                            if let Some(Step::BooleanOperation(bool_op)) =
                                traversal.steps.get(i + 1)
                            {
                                match bool_op {
                                    BooleanOp::Equal(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count == {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count == {}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in EQ */"),
                                    },
                                    BooleanOp::GreaterThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count > {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count > {}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GT */"),
                                    },
                                    BooleanOp::LessThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count < {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count < {}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LT */"),
                                    },
                                    BooleanOp::GreaterThanOrEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count >= {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count >= {}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GTE */"),
                                    },
                                    BooleanOp::LessThanOrEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count <= {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count <= {}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LTE */"),
                                    },
                                    BooleanOp::NotEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count != {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count != {}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in NEQ */"),
                                    },
                                    _ => output.push_str(&format!(
                                        "/* Unhandled boolean operation {:?} */",
                                        bool_op
                                    )),
                                }
                            } else {
                                output.push_str("count > 0");
                            }
                            if inner_traversal {
                                output.push_str("}");
                            }
                            return output;
                        }
                        Step::BooleanOperation(bo) => match bo {
                            BooleanOp::Equal(value) => match &**value {
                                Expression::BooleanLiteral(b) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Boolean(val) if *val == {}))\n", b));
                                }
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if *val == {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if *val == {}))\n", f));
                                }
                                Expression::StringLiteral(s) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val == \"{}\"))\n", s));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val == {}))\n", id));
                                }
                                _ => output.push_str(&format!(
                                    "// Unhandled value type in EQ\n {:?}",
                                    value
                                )),
                            },
                            BooleanOp::GreaterThan(value) => match &**value {
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val > {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val > {}))\n", f));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val > {}))\n", id));
                                }
                                _ => output.push_str("// Unhandled value type in GT\n"),
                            },
                            BooleanOp::GreaterThanOrEqual(value) => match &**value {
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val >= {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val >= {}))\n", f));
                                }
                                Expression::StringLiteral(s) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::String(val) if val >= \"{}\"))\n", s));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val >= {}))\n", id));
                                }
                                _ => output.push_str("// Unhandled value type in GTE\n"),
                            },
                            BooleanOp::LessThan(value) => match &**value {
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val < {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val < {}))\n", f));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val < {}))\n", id));
                                }
                                _ => output.push_str("// Unhandled value type in LT\n"),
                            },
                            BooleanOp::LessThanOrEqual(value) => match &**value {
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val <= {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val <= {}))\n", f));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val <= {}))\n", id));
                                }
                                _ => output.push_str("// Unhandled value type in LTE\n"),
                            },
                            BooleanOp::NotEqual(value) => match &**value {
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val != \"{}\")", id));
                                }
                                Expression::StringLiteral(s) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val != \"{}\")", s));
                                }
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if *val != {})", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if *val != {})", f));
                                }
                                Expression::BooleanLiteral(b) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::Boolean(val) if *val != {})", b));
                                }
                                _ => output.push_str(&format!(
                                    "// Unhandled value type in NEQ\n {:?}",
                                    value
                                )),
                            },
                            _ => output
                                .push_str(&format!("// Unhandled boolean operation {:?}\n", bo)),
                        },
                        step => {
                            println!("STEP NOT mATCHED: {:?}", step);
                            inner_traversal = true;
                            if i == 0 {
                                output.push_str("{");
                                output.push_str("let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));");
                                output.push_str(&mut self.generate_step(step, query));
                            } else {
                                output.push_str(&mut self.generate_step(step, query));
                            }
                        }
                    }
                }
                output
            }

            Expression::And(exprs) => {
                let conditions = exprs
                    .iter()
                    .map(|e| self.generate_filter_condition(e, query))
                    .collect::<Vec<_>>();
                format!("({})", conditions.join(" && "))
            }
            Expression::Or(exprs) => {
                let conditions = exprs
                    .iter()
                    .map(|e| self.generate_filter_condition(e, query))
                    .collect::<Vec<_>>();
                format!("({})", conditions.join(" || "))
            }
            _ => format!("/* Unhandled filter condition: {:?} */", expr),
        }
    }

    fn generate_field_addition(
        &mut self,
        field_addition: &FieldValue,
        parameters: &Vec<Parameter>,
    ) -> String {
        let mut output = String::new();
        match field_addition {
            FieldValue::Literal(value) => {
                output.push_str(&self.value_to_rust(value));
            }
            FieldValue::Empty => {
                output.push_str(&self.value_to_rust(&Value::Empty));
            }
            FieldValue::Expression(expr) => match expr {
                Expression::StringLiteral(s) => {
                    output.push_str(&format!("\"{}\"", s));
                }
                Expression::Identifier(id) => {
                    // println!("ID: {:?} {:?}", id, parameters);
                    parameters
                        .iter()
                        .find(|param| param.name == *id)
                        .map(|value| {
                            output.push_str(&format!("data.{}", &to_snake_case(&value.name)));
                        });
                }
                _ => {
                    println!("Unhandled field addition EXPR: {:?}", field_addition);
                    unreachable!()
                }
            },
            _ => {
                println!("Unhandled field addition FV: {:?}", field_addition);
                unreachable!()
            }
        }
        output
    }

    fn generate_add_vertex(&mut self, add_vertex: &AddNode, var_name: Option<&str>) -> String {
        let mut output = String::new();

        output.push_str(&mut self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );

        let vertex_type = add_vertex
            .vertex_type
            .as_ref()
            .map_or("".to_string(), |t| t.clone());
        
        let (props, possible_id) = if let Some(fields) = &add_vertex.fields {
            let possible_id = match fields.get("id") {
                Some(ValueType::Literal(Value::String(s))) => Some(s.clone()),
                Some(ValueType::Identifier(identifier)) => Some(format!("data.{}.clone()", identifier)),
                _ => None,
            };
            (self.generate_props_macro(&fields), possible_id)
        } else {
            ("props!{}".to_string(), None)
        };



        output.push_str(&mut self.indent());
        match possible_id {
            Some(id) => {
                output.push_str(&format!(
                    "tr.add_v(&mut txn, \"{}\", {}, None, Some({}));\n",
                    vertex_type, props, id
                ));
            }
            None => {
                output.push_str(&format!(
                    "tr.add_v(&mut txn, \"{}\", {}, None, None);\n",
                    vertex_type, props
                ));
            }
        }

        if let Some(name) = var_name {
            output.push_str(&mut self.indent());
            output.push_str(&format!("let {} = tr.result(txn)?;\n", name));
            self.current_variables
                .insert(name.to_string(), name.to_string());
        }

        output
    }

    fn generate_add_edge(&mut self, add_edge: &AddEdge) -> String {
        let mut output = String::new();

        output.push_str(&mut self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );

        let edge_type = add_edge
            .edge_type
            .as_ref()
            .map_or("".to_string(), |t| t.clone());
        let props = if let Some(fields) = &add_edge.fields {
            self.generate_props_macro(fields)
        } else {
            "props!{}".to_string()
        };

        // TODO: change
        let from_id = match &add_edge.connection.from_id.as_ref().unwrap() {
            IdType::Literal(id) => format!("\"{}\"", id),
            IdType::Identifier(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    format!("&{}.get_id()?", to_snake_case(var_name))
                } else {
                    format!("\"{}\"", var)
                }
            }
        };

        let to_id = match &add_edge.connection.to_id.as_ref().unwrap() {
            IdType::Literal(id) => format!("\"{}\"", id),
            IdType::Identifier(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    format!("&{}.get_id()?", to_snake_case(var_name))
                } else {
                    format!("\"{}\"", var)
                }
            }
        };

        output.push_str(&mut self.indent());
        output.push_str(&format!(
            "tr.add_e(&mut txn, \"{}\", {}, {}, {});\n",
            edge_type, from_id, to_id, props
        ));
        // output.push_str(&format!("tr.result()?;\n"));

        output
    }

    fn generate_drop(&mut self, expr: &Expression, query: &Query) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );
        match expr {
            Expression::Traversal(traversal) => {
                output.push_str(&mut self.generate_traversal(traversal, query));
            }
            _ => {
                output.push_str("tr.drop(&mut txn);\n");
            }
        }
        output.push_str(&mut self.indent());
        output.push_str("tr.drop(&mut txn);\n");
        output
    }

    fn generate_exists_check(&mut self, traversal: &Traversal, query: &Query) -> String {
        let mut output = String::new();
        output.push_str("tr.filter_nodes(&txn, |node| {\n");
        output.push_str(&mut self.indent());
        output.push_str("let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));\n");
        output.push_str(&mut self.indent());
        output.push_str(&mut self.generate_traversal(traversal, query));
        output.push_str(&mut self.indent());
        output.push_str("tr.count();\n");
        output.push_str(&mut self.indent());
        output.push_str("let count = tr.finish()?.as_count().unwrap();\n");
        output.push_str(&mut self.indent());
        output.push_str("Ok(count > 0)\n");
        output.push_str(&mut self.indent());
        output.push_str("});\n");
        output
    }

    fn generate_return_values(&mut self, return_values: &[Expression], query: &Query) -> String {
        let mut output = String::new();
        // println!("return_values: {:?}", return_values);
        for (i, expr) in return_values.iter().enumerate() {
            output.push_str(&mut self.indent());
            // output.push_str(&self.expression_to_return_value(expr));
            // println!("expr: {:?}", expr);
            match expr {
                Expression::Identifier(id) => {
                    output.push_str(&format!(
                        "return_vals.insert(\"{}\".to_string(), ReturnValue::from_traversal_value_array_with_mixin({}, remapping_vals.borrow_mut()));\n",
                        id, to_snake_case(id)
                    ));
                }
                Expression::StringLiteral(value) => {
                    output.push_str(&format!(
                        "return_vals.insert(\"message\".to_string(), ReturnValue::from(\"{}\"));\n",
                        value,
                    ));
                }
                Expression::None => {
                    output.push_str(&format!(
                        "return_vals.insert(\"message\".to_string(), ReturnValue::Empty);\n",
                    ));
                }
                Expression::Traversal(traversal) => {
                    output.push_str(&mut self.generate_traversal(traversal, query));
                    output.push_str(&mut self.indent());
                    output.push_str("let return_val = tr.finish()?;\n");
                    output.push_str(&mut self.indent());
                    if let Variable(var_name) = &traversal.start {
                        output.push_str(&format!(
                            "return_vals.insert(\"{}\".to_string(), ReturnValue::from_traversal_value_array_with_mixin(return_val, remapping_vals.borrow_mut()));\n", 
                            var_name,
                        ));
                    } else {
                        println!("Unhandled return value: {:?}", expr);
                        unreachable!()
                    }
                }

                _ => {
                    println!("Unhandled return value: {:?}", expr);
                    unreachable!()
                }
            }
        }

        output.push_str(&mut self.indent());
        output.push_str("response.body = sonic_rs::to_vec(&return_vals).unwrap();\n\n");

        output
    }

    fn expression_to_return_value(&mut self, expr: &Expression) -> String {
        match expr {
            Expression::Identifier(id) => {
                if let Some(var_name) = self.current_variables.get(id) {
                    var_name.clone()
                } else {
                    format!("\"{}\"", id)
                }
            }
            Expression::Traversal(traversal) => {
                format!("tr.finish()?")
            }
            _ => String::new(),
        }
    }

    fn value_type_to_rust(&mut self, value: &ValueType) -> String {
        println!("value: {:?}", value);
        match value {
            ValueType::Literal(value) => self.value_to_rust(value),
            ValueType::Identifier(identifier) => format!("data.{}", to_snake_case(identifier)),
            _ => unreachable!(),
        }
    }

    fn value_to_rust(&mut self, value: &Value) -> String {
        match value {
            Value::String(s) => format!("\"{}\"", s),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Array(arr) => format!(
                "vec![{}]",
                arr.iter()
                    .map(|v| self.value_to_rust(v))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            _ => unreachable!(),
        }
    }

    fn expression_to_value(&mut self, expr: &Expression) -> String {
        match expr {
            Expression::StringLiteral(s) => format!("\"{}\"", s),
            Expression::IntegerLiteral(i) => i.to_string(),
            Expression::FloatLiteral(f) => f.to_string(),
            Expression::BooleanLiteral(b) => b.to_string(),
            Expression::Identifier(id) => {
                if let Some(var_name) = self.current_variables.get(id) {
                    format!("&{}", var_name)
                } else {
                    format!("\"{}\"", id)
                }
            }
            _ => String::new(),
        }
    }

    fn generate_exclude_remapping(
        &mut self,
        is_node: bool,
        var_name: Option<&str>,
        exclude: &Exclude,
    ) -> String {
        let var_name = match var_name {
            Some(var) => var,
            None => "item",
        };
        let item_type = match is_node {
            true => "node",
            false => "edge",
        };

        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str(&format!(
            "tr.for_each_{}(&txn, |{}, txn| {{\n",
            item_type, var_name
        ));
        for field in exclude.fields.iter() {
            output.push_str(&format!(
                "let {}_remapping = Remapping::new(true, Some(\"{}\".to_string()), None);\n",
                to_snake_case(field).trim_end_matches("_"),
                field
            ));
        }

        output.push_str(&mut self.indent());
        output.push_str("remapping_vals.borrow_mut().insert(\n");
        output.push_str(&self.indent());
        output.push_str(&format!("{}.id.clone(),\n", var_name));
        output.push_str(&self.indent());
        output.push_str("ResponseRemapping::new(\n");
        output.push_str(&self.indent());
        output.push_str(&format!("HashMap::from([\n",));
        for field in exclude.fields.iter() {
            output.push_str(&format!(
                "(\"{}\".to_string(), {}_remapping),\n",
                field,
                to_snake_case(field).trim_end_matches("_")
            ));
        }
        output.push_str(&self.indent());
        output.push_str("]),");
        output.push_str(&self.indent());
        output.push_str(&format!("{}", false));
        output.push_str(&self.indent());
        output.push_str("),");
        output.push_str(&self.indent());
        output.push_str(");");
        output.push_str(&self.indent());
        output.push_str("Ok(())");
        output.push_str("});\n");
        output
    }

    fn generate_object_remapping(
        &mut self,
        is_node: bool,
        var_name: Option<&str>,
        object: &Object,
        query: &Query,
    ) -> String {
        /*
        tr.for_each_node(&txn, |node, txn| {
            // generate traversal if there is one

            // generate for that traversal if there is one

            // generate the remapping
            let remapping = Remapping::new(false, <new_name>, <return_value>);
            remapping_vals.borrow_mut().insert(<key>, remapping);
        });
         */

        let mut output = String::new();

        let var_name = match var_name {
            Some(var) => var,
            None => "item",
        };
        let item_type = match is_node {
            true => "node",
            false => "edge",
        };
        output.push_str(&mut self.indent());
        output.push_str(&format!(
            "tr.for_each_{}(&txn, |{}, txn| {{\n",
            item_type, var_name
        ));
        output.push_str(&mut self.indent());
        for (key, field) in object.fields.iter() {
            output.push_str(&mut self.indent());
            match field {
                FieldValue::Traversal(traversal) => {
                    output.push_str(&format!("let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from({}.clone()));\n", to_snake_case(var_name)));
                    output.push_str(&mut self.indent());
                    output.push_str(&mut self.generate_traversal(traversal, query));
                    output.push_str(&mut self.indent());
                    println!("traversal: {:?}", traversal);
                    match traversal.steps.last() {
                        Some(Step::Object(obj)) => {
                            if let Some((field_name, _)) = obj.fields.first() {
                                if field_name.as_str() == "id" {
                                    output.push_str(&format!(
                                        "let {} = tr.finish()?.get_id()?;\n",
                                        to_snake_case(key)
                                    ));
                                }
                            }
                        }
                        _ => {
                            output
                                .push_str(&format!("let {} = tr.finish()?;\n", to_snake_case(key)));
                        }
                    }
                }
                FieldValue::Expression(expr) => {
                    output.push_str(&format!("let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from({}.clone()));\n", to_snake_case(var_name)));
                    output.push_str(&mut self.indent());
                    output.push_str(&mut self.generate_expression(expr, query));
                    output.push_str(&mut self.indent());
                    match expr {
                        Expression::Traversal(traversal) => match traversal.steps.last().unwrap() {
                            Step::Object(obj) => {
                                if let Some((field_name, _)) = obj.fields.first() {
                                    if field_name.as_str() == "id" {
                                        output.push_str(&format!(
                                            "let {} = tr.finish()?.get_id()?;\n",
                                            to_snake_case(key)
                                        ));
                                    }
                                }
                            }
                            _ => {
                                output.push_str(&format!(
                                    "let {} = tr.finish()?;\n",
                                    to_snake_case(key)
                                ));
                            }
                        },

                        _ => {
                            output
                                .push_str(&format!("let {} = tr.finish()?;\n", to_snake_case(key)));
                        }
                    }
                }
                FieldValue::Literal(value) => {
                    output.push_str(&format!(
                        "let {} = {}.check_property({});\n",
                        to_snake_case(key),
                        var_name,
                        self.value_to_rust(value)
                    ));
                }
                FieldValue::Empty => {}
                _ => {
                    println!("unhandled field type: {:?}", field);
                    panic!("unhandled field type");
                }
            }
            output.push_str(&mut self.indent());

            // generate remapping
            output.push_str(&self.generate_remapping(key, field, query));
        }
        output.push_str("remapping_vals.borrow_mut().insert(\n");
        output.push_str(&self.indent());
        output.push_str(&format!("{}.id.clone(),\n", var_name));
        output.push_str(&self.indent());
        output.push_str("ResponseRemapping::new(\n");
        output.push_str(&self.indent());
        output.push_str("HashMap::from([\n");

        for (key, field) in object.fields.iter() {
            output.push_str(&format!(
                "(\"{}\".to_string(), {}_remapping),\n",
                key,
                to_snake_case(key)
            ));
        }
        output.push_str(&self.indent());
        output.push_str("]),");
        output.push_str(&self.indent());
        output.push_str(&format!("{}", object.should_spread));
        output.push_str(&self.indent());
        output.push_str("),");
        output.push_str(&self.indent());
        output.push_str(");");
        output.push_str(&self.indent());
        output.push_str("Ok(())");
        output.push_str("});\n");
        output
    }

    fn generate_remapping(&mut self, key: &String, field: &FieldValue, query: &Query) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        let opt_key = match key.as_str() {
            "" => "None".to_string(),
            _ => format!("Some(\"{}\".to_string())\n", key),
        };

        match field {
            FieldValue::Traversal(_) | FieldValue::Expression(_) => {
                output.push_str(&format!(
                    "let {}_remapping = Remapping::new(false, {}, Some({}));\n",
                    to_snake_case(key),
                    opt_key,
                    self.generate_return_value(key, field, query)
                ));
            }
            FieldValue::Literal(value) => {
                output.push_str(&format!(
                    r#"let {}_remapping = Remapping::new(false, None, Some(
                        match {} {{
                            Some(value) => ReturnValue::from(value.clone()),
                            None => return Err(GraphError::ConversionError(
                                "Property not found on {}".to_string(),
                            )),
                        }}
                    ));"#,
                    to_snake_case(key),
                    to_snake_case(key),
                    to_snake_case(key),
                ));
            }
            FieldValue::Empty => {
                output.push_str(&format!(
                    "let {}_remapping = Remapping::new(false, None, None);\n",
                    to_snake_case(key)
                ));
            }
            _ => {
                println!("unhandled field type: {:?}", field);
                panic!("unhandled field type");
            }
        }
        output
    }

    fn generate_return_value(&mut self, key: &String, field: &FieldValue, query: &Query) -> String {
        let mut output = String::new();

        // if last step of traversal or traversal in expression is id, ReturnValue::from({key})

        match field {
            FieldValue::Traversal(tr) => match tr.steps.last() {
                Some(Step::Object(obj)) => {
                    if let Some((field_name, _)) = obj.fields.first() {
                        if field_name.as_str() == "id" {
                            output
                                .push_str(&format!("ReturnValue::from({})\n", to_snake_case(key)));
                        } else {
                            output.push_str(&format!(
                                r#"ReturnValue::from(
                                    match item.check_property("{}") {{
                                        Some(value) => value,
                                        None => return Err(GraphError::ConversionError(
                                            "Property not found on {}".to_string(),
                                        )),
                                    }}
                                )
                                "#,
                                field_name, field_name
                            ));
                        }
                    }
                }
                _ => {
                    output.push_str("ReturnValue::from_traversal_value_array_with_mixin(\n");
                    output.push_str(&self.indent());
                    output.push_str(&format!("{},\n", to_snake_case(key)));
                    output.push_str(&self.indent());
                    output.push_str("remapping_vals.borrow_mut(),\n");
                    output.push_str(&self.indent());
                    output.push_str(")\n");
                }
            },
            FieldValue::Expression(expr) => match expr {
                Expression::Traversal(tr) => match tr.steps.last().unwrap() {
                    Step::Object(obj) => {
                        println!("obj: {:?}", obj);
                        if let Some((field_name, _)) = obj.fields.first() {
                            if field_name.as_str() == "id" {
                                output.push_str(&format!(
                                    "ReturnValue::from({}.get_id()?)\n",
                                    to_snake_case(key)
                                ));
                            } else {
                                output.push_str(&format!(
                                    r#"ReturnValue::from(
                                        match item.check_property("{}") {{
                                            Some(value) => value,
                                            None => return Err(GraphError::ConversionError(
                                                "Property not found on {}".to_string(),
                                            )),
                                        }}
                                    )
                                    "#,
                                    field_name, field_name
                                ));
                            }
                        }
                    }
                    _ => {
                        output.push_str("ReturnValue::from_traversal_value_array_with_mixin(\n");
                        output.push_str(&self.indent());
                        output.push_str(&format!("{},\n", to_snake_case(key)));
                        output.push_str(&self.indent());
                        output.push_str("remapping_vals.borrow_mut(),\n");
                        output.push_str(&self.indent());
                        output.push_str(")\n");
                    }
                },
                Expression::None => {
                    output.push_str(&format!("ReturnValue::Empty\n"));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!(
                        "ReturnValue::from({}.get_id()?)\n",
                        to_snake_case(key)
                    ));
                }
                _ => {
                    output.push_str(&format!(
                        "ReturnValue::from(item.check_property(\"{}\"))\n",
                        key
                    ));
                }
            },
            FieldValue::Literal(_) => {
                /// to rust value
                output.push_str(&format!("ReturnValue::from(\"{}\")\n", key));
            }
            _ => {
                println!("unhandled field type: {:?}", field);
                panic!("unhandled field type");
            }
        }

        output
    }
}

/// thoughts are:
/// - create a hashmap for the remappings for each var name
/// - insert at the end of the function before the return
///

fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    let mut prev_is_upper = false;

    while let Some(c) = chars.next() {
        if c.is_uppercase() {
            // Add underscore if:
            // 1. Not at start of string and previous char wasn't uppercase (camelCase -> camel_case)
            // 2. Previous char was uppercase but next char is lowercase (UserIDs -> user_ids)
            if !result.is_empty()
                && (!prev_is_upper || chars.peek().map_or(false, |next| next.is_lowercase()))
            {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
            prev_is_upper = true;
        } else {
            result.push(c);
            prev_is_upper = false;
        }
    }

    if result == "type" {
        result = "type_".to_string();
    }

    result
}

fn tr_is_object_remapping(tr: &Traversal) -> bool {
    match tr.steps.last() {
        Some(Step::Object(_)) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::parser::helix_parser::HelixParser;
    use pest::Parser;

    #[test]
    fn test_basic_query_generation() {
        let input = r#"
        QUERY GetUser(id: String) =>
            user <- V("id")
            RETURN user
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        let mut generator = CodeGenerator::new();
        let output = generator.generate_source(&source);
        println!("{}", output);
        assert!(output.contains("pub fn get_user"));
        assert!(output.contains("struct GetUserData"));
        assert!(output.contains("id: String"));
    }

    #[test]
    fn test_add_vertex_generation() {
        let input = r#"
        QUERY CreateUser(name: String, age: Integer) =>
            user <- AddV<User>({Name: "name", Age: "age"})
            RETURN user
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        let mut generator = CodeGenerator::new();
        let output = generator.generate_source(&source);

        assert!(output.contains("tr.add_v"));
        assert!(output.contains("props!"));
        assert!(output.contains("Name"));
        assert!(output.contains("Age"));
    }

    #[test]
    fn test_where_simple_condition() {
        let input = r#"
        QUERY FindActiveUsers() =>
            users <- V<User>::WHERE(_::Props(is_enabled)::EQ(true))
            RETURN users
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        let mut generator = CodeGenerator::new();
        let generated = generator.generate_source(&source);
        println!("Generated code:\n{}", generated);
        assert!(generated.contains("tr.filter_nodes"));
        assert!(generated.contains("is_enabled"));
        assert!(generated.contains("=="));
    }

    #[test]
    fn test_where_exists_condition() {
        let input = r#"
        QUERY FindUsersWithPosts() =>
            users <- V<User>::WHERE(EXISTS(_::OutE<Authored>))
            RETURN users
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        println!("Source:\n{:?}", source);
        let mut generator = CodeGenerator::new();
        let generated = generator.generate_source(&source);
        println!("Generated code:\n{}", generated);

        assert!(generated.contains("tr.filter_nodes"));
        assert!(generated.contains("out_e"));
        assert!(generated.contains("count"));
        assert!(generated.contains("count > 0"));
    }

    #[test]
    fn test_where_and_condition() {
        let input = r#"
        QUERY FindVerifiedActiveUsers() =>
            users <- V<User>::WHERE(AND(
                _::Props(verified)::EQ(true),
                _::Props(is_enabled)::EQ(true)
            ))
            RETURN users
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        println!("Source:\n{:?}", source);
        let mut generator = CodeGenerator::new();
        let generated = generator.generate_source(&source);
        println!("Generated code:\n{}", generated);
        assert!(generated.contains("tr.filter_nodes"));
        assert!(generated.contains("&&"));
        assert!(generated.contains("verified"));
        assert!(generated.contains("is_enabled"));
    }

    #[test]
    fn test_where_or_condition() {
        let input = r#"
        QUERY FindSpecialUsers() =>
            users <- V<User>::WHERE(OR(
                _::Props(verified)::EQ(true),
                _::Props(followers_count)::GT(1000)
            ))
            RETURN users
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        let mut generator = CodeGenerator::new();
        let generated = generator.generate_source(&source);

        assert!(generated.contains("tr.filter_nodes"));
        assert!(generated.contains("||"));
        assert!(generated.contains("verified"));
        assert!(generated.contains("followers_count"));
    }

    #[test]
    fn test_where_complex_traversal() {
        let input = r#"
        QUERY FindInfluentialUsers() =>
            users <- V<User>::WHERE(
                _::Out<Follows>::COUNT::GT(100)
            )::WHERE(
                _::In<Follows>::COUNT::GT(1000)
            )
            RETURN users
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        let mut generator = CodeGenerator::new();
        let generated = generator.generate_source(&source);
        println!("Generated code:\n{}", generated);

        assert!(generated.contains("tr.filter_nodes"));
        assert!(generated.contains("out"));
        assert!(generated.contains("in_"));
        assert!(generated.contains("count"));
        assert!(generated.contains(">"));
    }

    #[test]
    fn test_where_with_nested_conditions() {
        let input = r#"
        QUERY FindComplexUsers() =>
            users <- V<User>::WHERE(AND(
                OR(
                    _::Props(verified)::EQ(true),
                    _::Props(followers_count)::GT(5000)
                ),
                _::Out<Authored>::COUNT::GT(10)
            ))
            RETURN users
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        let mut generator = CodeGenerator::new();
        println!("Source:\n{:?}", source);
        let generated = generator.generate_source(&source);
        println!("Generated code:\n{}", generated);
        assert!(generated.contains("tr.filter_nodes"));
        assert!(generated.contains("&&"));
        assert!(generated.contains("||"));
        assert!(generated.contains("verified"));
        assert!(generated.contains("followers_count"));
        assert!(generated.contains("out"));
        assert!(generated.contains("count"));
    }

    #[test]
    fn test_boolean_operations() {
        let input = r#"
        QUERY FindUsersWithSpecificProperty(property_name: String, value: String) =>
            users <- V<User>::WHERE(_::Props(property_name)::EQ(value))
            RETURN users
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        let mut generator = CodeGenerator::new();
        let generated = generator.generate_source(&source);
        println!("Generated code:\n{}", generated);

        assert!(generated.contains("tr.filter_nodes"));
        assert!(generated.contains("property_name"));
        assert!(generated.contains("=="));
        assert!(generated.contains("value"));
        assert!(generated.contains("node.check_property"));
    }

    #[test]
    fn test_boolean_operations_with_multiple_properties() {
        let input = r#"
        QUERY FindUsersWithSpecificProperties(property1: String, value1: String, property2: String, value2: String, property3: String, value3: String) =>
            users <- V<User>::WHERE(AND(
                _::Props(property1)::EQ(value1),
                _::Props(property2)::EQ(value2),
                _::Props(property3)::EQ(value3)
            ))
            RETURN users
        "#;

        let source = HelixParser::parse_source(input).unwrap();
        let mut generator = CodeGenerator::new();
        let generated = generator.generate_source(&source);
        println!("Generated code:\n{}", generated);

        assert!(generated.contains("tr.filter_nodes"));
        assert!(generated.contains("&&"));
        assert!(generated.contains("property1"));
        assert!(generated.contains("property2"));
        assert!(generated.contains("property3"));
        assert!(generated.contains("value1"));
        assert!(generated.contains("value2"));
        assert!(generated.contains("value3"));
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("camelCase"), "camel_case");
        assert_eq!(to_snake_case("UserIDs"), "user_ids");
        assert_eq!(to_snake_case("SimpleXMLParser"), "simple_xml_parser");
        assert_eq!(to_snake_case("type"), "type_");
        assert_eq!(to_snake_case("ID"), "id");
        assert_eq!(to_snake_case("UserID"), "user_id");
        assert_eq!(to_snake_case("XMLHttpRequest"), "xml_http_request");
        assert_eq!(to_snake_case("iOS"), "i_os");
    }
}
