use crate::helix_engine::types::GraphError;
use crate::helixc::parser::helix_parser::StartNode;
use crate::helixc::parser::helix_parser::{
    AddEdge, AddVertex, Assignment, BooleanOp, EdgeConnection, Expression, Field, FieldAddition,
    FieldType, FieldValue, GraphStep, IdType, NodeSchema, Query, Source,
    StartNode::{Anonymous, Edge, Variable, Vertex},
    Statement, Step, Traversal, ValueType,
};
use crate::protocol::response::Response;
use crate::protocol::return_values::ReturnValue;
use crate::protocol::traversal_value::TraversalValue;
use crate::protocol::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

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

    pub fn generate_headers(&self) -> String {
        let mut output = String::new();
        output.push_str("use std::collections::{HashMap, HashSet};\n");
        output.push_str("use std::sync::Arc;\n");
        output.push_str("use std::time::Instant;\n\n");
        output.push_str("use get_routes::handler;\n");
        output.push_str("use helixdb::{\n");
        output.push_str("    node_matches,\n");
        output.push_str("    props,\n");
        output.push_str("    helix_engine::graph_core::traversal::TraversalBuilder,\n");
        output.push_str("    helix_engine::graph_core::traversal_steps::{\n");
        output.push_str("        RSourceTraversalSteps, RTraversalBuilderMethods, RTraversalSteps, TraversalMethods,\n");
        output.push_str("        TraversalSearchMethods, WSourceTraversalSteps, WTraversalBuilderMethods, WTraversalSteps,\n");
        output.push_str("    },\n");
        output.push_str("    helix_engine::types::GraphError,\n");
        output.push_str("    helix_gateway::router::router::HandlerInput,\n");
        output.push_str("    protocol::count::Count,\n");
        output.push_str("    protocol::response::Response,\n");
        output.push_str("    protocol::traversal_value::TraversalValue,\n");
        output.push_str(
            "    protocol::{filterable::Filterable, value::Value, return_values::ReturnValue},\n",
        );
        output.push_str("};\n");
        output.push_str("use sonic_rs::{Deserialize, Serialize};\n\n");
        output
    }

    fn indent(&self) -> String {
        "    ".repeat(self.indent_level)
    }

    fn generate_props_macro(&self, props: &[(String, ValueType)]) -> String {
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
            output.push_str(&self.generate_node_schema(node_schema));
            output.push_str("\n");
        }

        // Generate query implementations
        for query in &source.queries {
            output.push_str(&self.generate_query(query));
            output.push_str("\n");
        }

        output
    }

    fn generate_node_schema(&self, schema: &NodeSchema) -> String {
        let mut output = String::new();
        output.push_str(&format!("// Node Schema: {}\n", schema.name));
        output.push_str("struct ");
        output.push_str(&schema.name);
        output.push_str(" {\n");

        for field in &schema.fields {
            output.push_str(&format!(
                "    {}: {},\n",
                field.name,
                self.field_type_to_rust(&field.field_type)
            ));
        }

        output.push_str("}\n");
        output
    }

    fn field_type_to_rust(&self, field_type: &FieldType) -> &str {
        match field_type {
            FieldType::String => "String",
            FieldType::Integer => "i32",
            FieldType::Float => "f64",
            FieldType::Boolean => "bool",
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
            output.push_str(&self.indent());
            output.push_str("#[derive(Serialize, Deserialize)]\n");
            output.push_str(&self.indent());
            output.push_str(&format!("struct {}Data {{\n", query.name));
            self.indent_level += 1;

            for param in &query.parameters {
                output.push_str(&self.indent());
                output.push_str(&format!(
                    "{}: {},\n",
                    to_snake_case(&param.name),
                    self.field_type_to_rust(&param.param_type)
                ));
            }

            self.indent_level -= 1;
            output.push_str(&self.indent());
            output.push_str("}\n\n");

            // Deserialize input data
            output.push_str(&self.indent());
            output.push_str(&format!(
                "let data: {}Data = sonic_rs::from_slice(&input.request.body).unwrap();\n\n",
                query.name
            ));
        }

        // Setup database transaction
        output.push_str(&self.indent());
        output.push_str("let db = Arc::clone(&input.graph.storage);\n");
        output.push_str(&self.indent());
        if query.statements.iter().any(|s| {
            matches!(s, Statement::AddVertex(_))
                || matches!(s, Statement::AddEdge(_))
                || matches!(s, Statement::Drop(_))
                || { 
                    if let Statement::Assignment(assignment) = s {
                        matches!(assignment.value, Expression::AddVertex(_)) || matches!(assignment.value, Expression::AddEdge(_))
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
            output.push_str(&self.indent());
            output.push_str(&format!("let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity({});\n\n", query.return_values.len()));
        }

        // Generate statements
        for statement in &query.statements {
            output.push_str(&self.generate_statement(statement));
        }

        if query.statements.iter().any(|s| {
            matches!(s, Statement::AddVertex(_))
                || matches!(s, Statement::AddEdge(_))
                || matches!(s, Statement::Drop(_))
                || { 
                    if let Statement::Assignment(assignment) = s {
                        matches!(assignment.value, Expression::AddVertex(_)) || matches!(assignment.value, Expression::AddEdge(_))
                    } else {
                        false
                    }
                }
        }) {
            output.push_str(&self.indent());
            output.push_str("txn.commit()?;\n");
        }
        // Generate return statement
        if !query.return_values.is_empty() {
            output.push_str(&self.generate_return_values(&query.return_values));
        }
       
        // Close function
        output.push_str(&self.indent());
        output.push_str("Ok(())\n");
        self.indent_level -= 1;
        output.push_str("}\n");

        

        output
    }

    fn generate_statement(&mut self, statement: &Statement) -> String {
        match statement {
            Statement::Assignment(assignment) => self.generate_assignment(assignment),
            Statement::AddVertex(add_vertex) => self.generate_add_vertex(add_vertex, None),
            Statement::AddEdge(add_edge) => self.generate_add_edge(add_edge),
            Statement::Drop(expr) => self.generate_drop(expr),
        }
    }

    fn generate_assignment(&mut self, assignment: &Assignment) -> String {
        let mut output = String::new();
        let var_name = &assignment.variable;

        output.push_str(&self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );

        output.push_str(&self.generate_expression(&assignment.value));

        // Store variable for later use
        self.current_variables
            .insert(var_name.clone(), var_name.clone());

        output.push_str(&self.indent());

        match assignment.value {

            _ => output.push_str(&format!("let {} = tr.finish()?;\n\n", var_name)),
        }

        output
    }

    fn generate_expression(&mut self, expr: &Expression) -> String {
        let mut output = String::new();

        match expr {
            Expression::Traversal(traversal) => {
                output.push_str(&self.generate_traversal(traversal));
            }
            Expression::Identifier(id) => {
                if let Some(var_name) = self.current_variables.get(id) {
                    output.push_str(&self.indent());
                    output.push_str(&format!(
                        "tr.current_step = TraversalValue::from({});\n",
                        var_name
                    ));
                }
            }
            Expression::StringLiteral(s) => {
                output.push_str(&self.indent());
                output.push_str(&format!("\"{}\"", s));
            }
            Expression::IntegerLiteral(i) => {
                output.push_str(&self.indent());
                output.push_str(&i.to_string());
            }
            Expression::FloatLiteral(f) => {
                output.push_str(&self.indent());
                output.push_str(&f.to_string());
            }
            Expression::BooleanLiteral(b) => {
                output.push_str(&self.indent());
                output.push_str(&b.to_string());
            }
            Expression::AddVertex(add_vertex) => {
                output.push_str(&self.generate_add_vertex(add_vertex, None));
            }
            Expression::AddEdge(add_edge) => {
                output.push_str(&self.generate_add_edge(add_edge));
            }
            Expression::Exists(traversal) => {
                output.push_str(&self.generate_exists_check(traversal));
            }
            _ => {}
        }

        output
    }

    fn generate_traversal(&self, traversal: &Traversal) -> String {
        let mut output = String::new();

        // Generate start node
        match &traversal.start {
            Vertex { types, ids } => {
                if let Some(ids) = ids {
                    output.push_str(&self.indent());
                    if let Some(var_name) = self.current_variables.get(&ids[0]) {
                        output.push_str(&format!("tr.v_from_id(&txn, {});\n", var_name));
                    } else {
                        output.push_str(&format!(
                            "tr.v_from_id(&txn, &data.{});\n",
                            to_snake_case(&ids[0])
                        ));
                    }
                } else if let Some(types) = types {
                    output.push_str(&self.indent());
                    output.push_str(&format!(
                        "tr.v_from_types(&txn, &[{}]);\n",
                        types
                            .iter()
                            .map(|t| format!("\"{}\"", t))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                } else {
                    output.push_str(&self.indent());
                    output.push_str("tr.v(&txn);\n");
                }
            }
            Edge { types, ids } => {
                if let Some(ids) = ids {
                    output.push_str(&self.indent());
                    if let Some(var_name) = self.current_variables.get(&ids[0]) {
                        output.push_str(&format!("tr.e_from_id(&txn, {});\n", var_name));
                    } else {
                        output.push_str(&format!(
                            "tr.e_from_id(&txn, &data.{});\n",
                            to_snake_case(&ids[0])
                        ));
                    }
                } else if let Some(types) = types {
                    output.push_str(&self.indent());
                    output.push_str("tr.e(&txn);\n");
                } else {
                    output.push_str(&self.indent());
                    output.push_str("tr.e(&txn);\n");
                }
            }
            Variable(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    output.push_str(&self.indent());
                    output.push_str(&format!(
                        "tr.current_step = TraversalValue::from({}.clone());\n",
                        var_name
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
                Step::Props(_) => {
                    // If this is part of a count comparison, skip property checks
                    if i < traversal.steps.len() - 2
                        && matches!(traversal.steps[i + 1], Step::BooleanOperation(_))
                        && matches!(traversal.steps[i + 2], Step::Count)
                    {
                        skip_next = true;
                        continue;
                    }
                    output.push_str(&self.generate_step(step));
                }
                Step::BooleanOperation(_) => {
                    // Skip boolean operations if they're part of a count comparison
                    if i < traversal.steps.len() - 1
                        && matches!(traversal.steps[i + 1], Step::Count)
                    {
                        continue;
                    }
                    output.push_str(&self.generate_step(step));
                }
                Step::Vertex(graph_step) => match graph_step {
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
                    _ => output.push_str(&self.generate_step(step)),
                },
                Step::Edge(graph_step) => match graph_step {
                    GraphStep::InV => output.push_str("tr.in_v(&txn);\n"),
                    GraphStep::OutV => output.push_str("tr.out_v(&txn);\n"),
                    GraphStep::BothV => output.push_str("tr.both_v(&txn);\n"),
                    _ => output.push_str(&self.generate_step(step)),
                },
                _ => output.push_str(&self.generate_step(step)),
            }
        }

        output
    }
    fn generate_boolean_operation(&self, bool_op: &BooleanOp) -> String {
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
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::String(val) if *val == \"{}\")));\n", id));
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
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val > {})));\n", id));
                }
                _ => output.push_str("// Unhandled value type in GT\n"),
            },
            BooleanOp::LessThan(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val < {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Float(val) if val < {})));\n", f));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("tr.filter_nodes(&txn, |node| Ok(matches!(node.check_property(current_prop).unwrap(), Value::Integer(val) if val < {})));\n", id));
                }
                _ => output.push_str("// Unhandled value type in LT\n"),
            },
            _ => output.push_str("// Unhandled boolean operation\n"),
        }
        output
    }

    fn generate_step(&self, step: &Step) -> String {
        let mut output = String::new();
        output.push_str(&self.indent());

        match step {
            Step::Props(props) => {
                output.push_str(&format!(
                    "tr.get_properties(&txn, &vec![{}]);\n",
                    props
                        .iter()
                        .map(|p| format!("\"{}\".to_string()", p))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                // Return the property name for the next step
                output.push_str(&format!("let current_prop = \"{}\";\n", props[0]));
            }
            Step::BooleanOperation(bool_op) => {
                output.push_str(&self.generate_boolean_operation(bool_op));
            }
            Step::Vertex(graph_step) => match graph_step {
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
                GraphStep::OutV => output.push_str("tr.out_v(&txn);\n"),
                GraphStep::InV => output.push_str("tr.in_v(&txn);\n"),
                GraphStep::BothV => output.push_str("tr.both_v(&txn);\n"),
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
            Step::Where(expr) => {
                match &**expr {
                    Expression::BooleanLiteral(b) => {
                        output.push_str(&format!("tr.filter_nodes(&txn, |_| Ok({}));\n", b));
                    }
                    Expression::Exists(traversal) => {
                        output.push_str(&self.generate_exists_check(traversal));
                    }
                    Expression::And(exprs) => {
                        output.push_str("tr.filter_nodes(&txn, |node| {\n");
                        output.push_str(&self.indent());
                        output.push_str("    Ok(");
                        for (i, expr) in exprs.iter().enumerate() {
                            if i > 0 {
                                output.push_str(" && ");
                            }
                            output.push_str(&self.generate_filter_condition(expr));
                        }
                        output.push_str(")\n");
                        output.push_str(&self.indent());
                        output.push_str("});\n");
                    }
                    Expression::Or(exprs) => {
                        output.push_str("tr.filter_nodes(&txn, |node| {\n");
                        output.push_str(&self.indent());
                        output.push_str("    Ok(");
                        for (i, expr) in exprs.iter().enumerate() {
                            if i > 0 {
                                output.push_str(" || ");
                            }
                            output.push_str(&self.generate_filter_condition(expr));
                        }
                        output.push_str(")\n");
                        output.push_str(&self.indent());
                        output.push_str("});\n");
                    }
                    Expression::Traversal(_) => {
                        // For traversal-based conditions
                        output.push_str("tr.filter_nodes(&txn, |node| {\n");
                        output.push_str(&self.indent());
                        output.push_str("    Ok(");
                        output.push_str(&self.generate_filter_condition(expr));
                        output.push_str("    )\n");
                        output.push_str(&self.indent());
                        output.push_str("});\n");
                        // output.push_str("    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));\n");
                        // output.push_str(&self.generate_traversal(traversal));
                        // output.push_str(&self.indent());
                        // output.push_str("    tr.count();\n");
                        // output.push_str(&self.indent());
                        // output.push_str("    let count = tr.finish()?.as_count().unwrap();\n");
                        // output.push_str(&self.indent());
                        // output.push_str("    Ok(count > 0)\n");
                        // output.push_str(&self.indent());
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
            Step::ID => {
                output.push_str("tr.get_id();\n");
            }
            Step::Update(update) => {
                let props = update
                    .fields
                    .iter()
                    .map(|(k, v)| {
                        format!("\"{}\".to_string() => {}", k, self.expression_to_value(v))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                output.push_str(&format!(
                    "tr.update_props(&mut txn, props!{{ {} }});\n",
                    props
                ));
            }
            _ => {}
        }

        output
    }

    fn generate_filter_condition(&self, expr: &Expression) -> String {
        match expr {
            Expression::BooleanLiteral(b) => b.to_string(),
            Expression::Exists(traversal) => {
                format!("{{ let mut inner_tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone())); {} inner_tr.count(); let count = inner_tr.finish()?.as_count().unwrap(); count > 0 }}", 
                    self.generate_traversal(traversal))
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
                        Step::Props(props) => {
                            let prop_name = &props[0];
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
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::String(val) if *val == \"{}\"))", prop_name, id));
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
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val > {}))", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GT */"),
                                    },
                                    BooleanOp::LessThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val < {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Float(val) if *val < {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("node.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Integer(val) if *val < {}))", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LT */"),
                                    },
                                    _ => output.push_str("/* Unhandled boolean operation */"),
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
                                    _ => output.push_str("/* Unhandled boolean operation */"),
                                }
                            } else {
                                output.push_str("count > 0");
                            }
                            if inner_traversal {
                                output.push_str("}");
                            }
                            return output;
                        }
                        _ => {
                            inner_traversal = true;
                            if i == 0 {
                                output.push_str("{");
                                output.push_str("let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));");
                                output.push_str(&self.generate_step(step));
                            } else {
                                output.push_str(&self.generate_step(step));
                            }
                        }
                    }
                }
                output
            }

            Expression::And(exprs) => {
                let conditions = exprs
                    .iter()
                    .map(|e| self.generate_filter_condition(e))
                    .collect::<Vec<_>>();
                format!("({})", conditions.join(" && "))
            }
            Expression::Or(exprs) => {
                let conditions = exprs
                    .iter()
                    .map(|e| self.generate_filter_condition(e))
                    .collect::<Vec<_>>();
                format!("({})", conditions.join(" || "))
            }
            _ => format!("/* Unhandled filter condition: {:?} */", expr),
        }
    }

    fn generate_add_vertex(&mut self, add_vertex: &AddVertex, var_name: Option<&str>) -> String {
        let mut output = String::new();

        output.push_str(&self.indent());
        output.push_str(
            "let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);\n",
        );

        let vertex_type = add_vertex
            .vertex_type
            .as_ref()
            .map_or("".to_string(), |t| t.clone());
        let props = if let Some(fields) = &add_vertex.fields {
            self.generate_props_macro(fields)
        } else {
            "props!{}".to_string()
        };

        output.push_str(&self.indent());
        output.push_str(&format!(
            "tr.add_v(&mut txn, \"{}\", {}, None);\n",
            vertex_type, props
        ));

        if let Some(name) = var_name {
            output.push_str(&self.indent());
            output.push_str(&format!("let {} = tr.result()?;\n", name));
            self.current_variables
                .insert(name.to_string(), name.to_string());
        }

        output
    }

    fn generate_add_edge(&mut self, add_edge: &AddEdge) -> String {
        let mut output = String::new();

        output.push_str(&self.indent());
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

        let from_id = match &add_edge.connection.from_id {
            IdType::Literal(id) => format!("\"{}\"", id),
            IdType::Identifier(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    format!("&{}.get_id()?", var_name)
                } else {
                    format!("\"{}\"", var)
                }
            }
        };

        let to_id = match &add_edge.connection.to_id {
            IdType::Literal(id) => format!("\"{}\"", id),
            IdType::Identifier(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    format!("&{}.get_id()?", var_name)
                } else {
                    format!("\"{}\"", var)
                }
            }
        };

        output.push_str(&self.indent());
        output.push_str(&format!(
            "tr.add_e(&mut txn, \"{}\", {}, {}, {});\n",
            edge_type, from_id, to_id, props
        ));
        output.push_str(&format!("tr.result()?;\n"));

        output
    }

    fn generate_drop(&self, expr: &Expression) -> String {
        let mut output = String::new();
        output.push_str(&self.indent());
        output.push_str("tr.drop(&mut txn);\n");
        output
    }

    fn generate_exists_check(&self, traversal: &Traversal) -> String {
        let mut output = String::new();
        output.push_str("tr.filter_nodes(&txn, |node| {\n");
        output.push_str(&self.indent());
        output.push_str("let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));\n");
        output.push_str(&self.indent());
        output.push_str(&self.generate_traversal(traversal));
        output.push_str(&self.indent());
        output.push_str("tr.count();\n");
        output.push_str(&self.indent());
        output.push_str("let count = tr.finish()?.as_count().unwrap();\n");
        output.push_str(&self.indent());
        output.push_str("Ok(count > 0)\n");
        output.push_str(&self.indent());
        output.push_str("});\n");
        output
    }

    fn generate_return_values(&self, return_values: &[Expression]) -> String {
        let mut output = String::new();

        for (i, expr) in return_values.iter().enumerate() {
            output.push_str(&self.indent());
            if let Expression::Identifier(id) = expr {
                output.push_str(&format!(
                    "return_vals.insert(\"{}\".to_string(), ReturnValue::TraversalValues({}));\n",
                    id, id
                ));
            }
        }

        output.push_str(&self.indent());
        output.push_str("response.body = sonic_rs::to_vec(&return_vals).unwrap();\n\n");

        output
    }

    fn expression_to_return_value(&self, expr: &Expression) -> String {
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

    fn value_type_to_rust(&self, value: &ValueType) -> String {
        match value {
            ValueType::Literal(value) => self.value_to_rust(value),
            ValueType::Identifier(identifier) => format!("\"{}\"", identifier),
        }
    }

    fn value_to_rust(&self, value: &Value) -> String {
        match value {
            Value::String(s) => format!("\"{}\"", s),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Empty => "Value::Empty".to_string(),
            Value::Array(arr) => format!(
                "vec![{}]",
                arr.iter()
                    .map(|v| self.value_to_rust(v))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    fn expression_to_value(&self, expr: &Expression) -> String {
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
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c.is_uppercase() {
            if !result.is_empty() {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
        } else {
            result.push(c);
        }
    }

    result
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
}
