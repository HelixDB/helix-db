use crate::{
    helixc::analyzer::types::{
        AddEdge, AddNode, AddVector, Assignment, BatchAddVector, BooleanOp, EdgeSchema,
        EvaluatesToNumber, Exclude, Expression, FieldValue, ForLoop, GraphStep, IdType, NodeSchema,
        Object, Parameter, Query, SearchVector, Source,
        StartNode::{Anonymous, Edge, Node, Variable},
        Statement, Step, Traversal, ValueType, VectorData,
    },
    helixc::parser::helix_parser::FieldType,
    protocol::value::Value,
};
use std::{collections::HashMap, vec};

pub struct CodeGenerator {
    indent_level: usize,
    current_variables: HashMap<String, String>,
    params: Vec<String>,
}

impl CodeGenerator {
    pub fn new() -> Self {
        Self {
            indent_level: 0,
            current_variables: HashMap::new(),
            params: Vec::new(),
        }
    }
    /**
     * helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::AddEAdapter, add_n::AddNAdapter, e::EAdapter, e_from_id::EFromId,
            e_from_types::EFromTypes, n::NAdapter, n_from_id::NFromId, n_from_types::NFromTypes,
        },
        tr_val::TraversalVal,
        util::{
            dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::Update,
        },
    },
     */

    pub fn generate_headers(&mut self) -> String {
        let mut output = String::new();
        output.push_str("use std::collections::{HashMap, HashSet};\n");
        output.push_str("use std::cell::RefCell;\n");
        output.push_str("use std::sync::Arc;\n");
        output.push_str("use std::time::Instant;\n\n");
        output.push_str("use get_routes::handler;\n");
        output.push_str("use helixdb::helix_engine::vector_core::vector::HVector;\n");
        output.push_str("use helixdb::{\n");
        output.push_str("    node_matches,\n");
        output.push_str("    props,\n");
        output.push_str("    helix_engine::graph_core::ops::{\n");
        output.push_str("        g::G,\n");
        output.push_str("        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},\n");
        output.push_str(
            "        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},\n",
        );
        output.push_str("        vectors::{ insert::InsertVAdapter, search::SearchVAdapter},\n");
        output.push_str("        source::{add_e::{AddEAdapter, EdgeType}, add_n::AddNAdapter, e::EAdapter, e_from_id::EFromId, e_from_types::EFromTypes, n::NAdapter, n_from_id::NFromId, n_from_types::NFromTypesAdapter},\n");
        output.push_str("        tr_val::{TraversalVal, Traversable},\n");
        output.push_str("        util::{dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut, filter_ref::FilterRefAdapter, range::RangeAdapter, update::Update},\n");
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
                self.field_type_to_rust(&field.field_type, &field.name)
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
                self.field_type_to_rust(&field.field_type, &field.name)
            ));
        }

        output.push_str("}\n");
        output
    }

    fn field_type_to_rust(&mut self, field_type: &FieldType, param_name: &str) -> String {
        match field_type {
            FieldType::String => "String".to_string(),
            FieldType::F32 => "f32".to_string(),
            FieldType::F64 => "f64".to_string(),
            FieldType::I8 => "i8".to_string(),
            FieldType::I16 => "i16".to_string(),
            FieldType::I32 => "i32".to_string(),
            FieldType::I64 => "i64".to_string(),
            FieldType::U8 => "u8".to_string(),
            FieldType::U16 => "u16".to_string(),
            FieldType::U32 => "u32".to_string(),
            FieldType::U64 => "u64".to_string(),
            FieldType::U128 => "u128".to_string(),
            FieldType::Boolean => "bool".to_string(),
            FieldType::Uuid => "String".to_string(), // TODO: Look into using UUID (would require custom serdes impl)
            FieldType::Array(field) => {
                format!("{}", self.field_type_to_rust(&field, &param_name))
            }
            FieldType::Identifier(id) => format!("{}", id),
            FieldType::Object(_) => format!("{}Data", to_snake_case(&param_name)),
        }
    }

    fn object_field_to_rust(&mut self, name: &str, is_inner: bool) -> String {
        let mut output = String::new();
        if is_inner {
            output.push_str(&format!("{}Data,\n", to_snake_case(&name)));
        } else {
            output.push_str(&format!(
                "{}: {}Data,\n",
                to_snake_case(&name),
                to_snake_case(&name)
            ));
        }
        output
    }

    fn array_field_to_rust(
        &mut self,
        name: &str,
        inner: &FieldType,
        objects: &mut String,
        is_inner: bool,
    ) -> String {
        let mut output = String::new();
        if is_inner {
            output.push_str(&format!("Vec<{}>\n", self.field_type_to_rust(inner, name)));
        } else {
            output.push_str(&format!(
                "{}: Vec<{}>,\n",
                to_snake_case(&name),
                match &inner {
                    FieldType::Object(obj) => {
                        // self.params.push(name.to_string());
                        // let object_type = self.object_type_to_rust(name, obj, objects);
                        // objects.push_str(&object_type);
                        self.object_field_to_rust(name, true)
                    }
                    _ => self.field_type_to_rust(inner, name),
                }
            ));
        }
        output
    }

    fn object_type_to_rust(
        &mut self,
        param_name: &str,
        fields: &HashMap<String, FieldType>,
        objects: &mut String,
    ) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str("#[derive(Serialize, Deserialize)]\n");
        output.push_str(&mut self.indent());
        output.push_str(&format!("struct {}Data {{\n", to_snake_case(&param_name)));
        let _ = fields
            .iter()
            .map(|(name, type_name)| {
                output.push_str(&mut self.indent());
                output.push_str(&mut self.indent());
                output.push_str(&format!(
                    "{}: {},\n",
                    to_snake_case(&name),
                    match type_name {
                        // TODO: Have separate internal string for type defs
                        FieldType::Object(obj) => {
                            self.params.push(name.clone());
                            let object_type = self.object_type_to_rust(name, obj, objects);
                            objects.push_str(&object_type);
                            self.object_field_to_rust(name, true)
                        }
                        FieldType::Array(obj) => {
                            if let FieldType::Object(obj) = obj.as_ref() {
                                self.params.push(name.clone());
                                let object_type = self.object_type_to_rust(name, obj, objects);
                                objects.push_str(&object_type);
                            }
                            self.array_field_to_rust(name, obj, objects, true)
                        }
                        _ => self.field_type_to_rust(type_name, &param_name),
                    }
                ));
            })
            .collect::<Vec<_>>();
        output.push_str(&mut self.indent());
        output.push_str("}\n\n");
        output
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
            let mut objects = String::new();
            for param in &query.parameters {
                output.push_str(&mut self.indent());
                self.params.push(param.name.clone());
                match &param.param_type {
                    FieldType::Object(_) => {
                        output.push_str(&self.object_field_to_rust(&param.name, false))
                    }
                    FieldType::Array(_) => output.push_str(&self.array_field_to_rust(
                        &param.name,
                        &param.param_type,
                        &mut objects,
                        false,
                    )),
                    _ => output.push_str(&format!(
                        "{}: {},\n",
                        to_snake_case(&param.name),
                        self.field_type_to_rust(&param.param_type, &param.name)
                    )),
                }
            }

            self.indent_level -= 1;
            output.push_str(&mut self.indent());
            output.push_str("}\n\n");

            for param in &query.parameters {
                match &param.param_type {
                    FieldType::Object(fields) => {
                        output.push_str(&mut self.object_type_to_rust(
                            &param.name,
                            fields,
                            &mut objects,
                        ));
                    }
                    FieldType::Array(fields) => match fields.as_ref() {
                        FieldType::Object(fields) => {
                            output.push_str(&mut self.object_type_to_rust(
                                &param.name,
                                fields,
                                &mut objects,
                            ));
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }

            output.push_str(&objects);

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
        output.push_str("let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());\n");

        // Setup database transaction
        output.push_str(&mut self.indent());
        output.push_str("let db = Arc::clone(&input.graph.storage);\n");
        output.push_str(&mut self.indent());

        if query.statements.iter().any(|s| self.should_be_mut(s)) {
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

        if query.statements.iter().any(|s| self.should_be_mut(s)) {
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

    fn should_be_mut(&mut self, statement: &Statement) -> bool {
        matches!(statement, Statement::AddNode(_))
            || matches!(statement, Statement::AddEdge(_))
            || matches!(statement, Statement::Drop(_))
            || matches!(statement, Statement::AddVector(_))
            || matches!(statement, Statement::BatchAddVector(_))
            || {
                match statement {
                    Statement::Assignment(assignment) => {
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
                    }
                    Statement::ForLoop(for_loop) => {
                        for_loop.statements.iter().any(|s| self.should_be_mut(s))
                    }
                    _ => false,
                }
            }
    }

    fn generate_statement(&mut self, statement: &Statement, query: &Query) -> String {
        let mut output = match statement {
            Statement::Assignment(assignment) => self.generate_assignment(assignment, query),
            Statement::AddNode(add_node) => self.generate_add_node(add_node, None),
            Statement::AddEdge(add_edge) => self.generate_add_edge(add_edge, None),
            Statement::Drop(expr) => self.generate_drop(expr, query),
            Statement::AddVector(add_vector) => self.generate_add_vector(add_vector),
            Statement::SearchVector(search_vector) => self.generate_search_vector(search_vector),
            Statement::BatchAddVector(batch_add_vector) => {
                self.generate_batch_add_vector(batch_add_vector)
            }
            Statement::ForLoop(for_loop) => self.generate_for_loop(for_loop, query),
        };
        match statement {
            Statement::Assignment(_) | Statement::ForLoop(_) => {}
            _ => output.push_str(";\nlet _ = tr.collect_to::<Vec<_>>();\n\n"),
        }
        output
    }

    fn generate_for_loop(&mut self, for_loop: &ForLoop, query: &Query) -> String {
        let mut output = String::new();
        // if the in_variable is in params, then use data.in_variable else use in_variable
        if self.params.contains(&for_loop.in_variable) {
            // arguments
            let arguments = for_loop
                .variables
                .iter()
                .map(|v| v.clone())
                .collect::<Vec<_>>()
                .join(", ");

            // output.push_str(&format!(
            //     "for {}Data {{ {} }} in data.{} {{\n",
            //     for_loop.in_variable, arguments, for_loop.in_variable
            // ));
            output.push_str(&format!("for data in data.{} {{\n", for_loop.in_variable));
        } else {
            // TODO handle error if variables is more than 1
            output.push_str(&format!("for data in {} {{\n", for_loop.in_variable));
        }

        for statement in &for_loop.statements {
            output.push_str(&mut self.generate_statement(statement, query));
        }

        output.push_str(&mut self.indent());
        output.push_str("}\n");

        output
    }

    fn generate_add_vector(&mut self, add_vector: &AddVector) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str("let tr = G::new_mut(Arc::clone(&db), &mut txn)\n");

        // possible id for hvector nid
        let (props, _) = if let Some(fields) = &add_vector.fields {
            let possible_id = match fields.get("id") {
                Some(ValueType::Literal(Value::String(s))) => Some(s.clone()),
                Some(ValueType::Identifier(identifier)) => {
                    Some(format!("data.{}.clone()", identifier))
                }
                _ => None,
            };
            (self.generate_props_macro(&fields), possible_id)
        } else {
            ("props!{}".to_string(), None)
        };
        match &add_vector.data {
            Some(VectorData::Vector(vec)) => {
                output.push_str(&mut self.indent());
                match &add_vector.fields {
                    Some(fields) => output.push_str(&format!(
                        ".insert_v::<fn(&HVector) -> bool>(&{:?}, Hashmap::from({}))\n",
                        vec,
                        self.generate_props_macro(&fields)
                    )),
                    None => output.push_str(&format!(
                        ".insert_v::<fn(&HVector) -> bool>(&{:?}, None)\n",
                        vec
                    )),
                };
            }
            Some(VectorData::Identifier(id)) => {
                output.push_str(&mut self.indent());
                match &add_vector.fields {
                    Some(fields) => output.push_str(&format!(
                        ".insert_v::<fn(&HVector) -> bool>(&data.{}, Hashmap::from({}))\n",
                        id,
                        self.generate_props_macro(&fields)
                    )),
                    None => output.push_str(&format!(
                        ".insert_v::<fn(&HVector) -> bool>(&data.{}, None)\n",
                        id
                    )),
                };
            }
            None => (),
        };

        output
    }

    fn generate_batch_add_vector(&mut self, batch_add_vector: &BatchAddVector) -> String {
        let mut output = String::new();

        //iterate over the vectors and insert them
        output.push_str(&mut self.indent());
        let vec_id = match &batch_add_vector.vec_identifier {
            Some(id) => id,
            None => "vecs",
        };
        output.push_str(&mut self.indent());
        output.push_str(&mut self.indent());
        output.push_str("let tr = G::new_mut(Arc::clone(&db), &mut txn)\n");

        output.push_str(&mut self.indent());
        output.push_str(&mut self.indent());
        match &batch_add_vector.fields {
            Some(fields) => output.push_str(&format!(
                ".insert_vs::<fn(&HVector) -> bool>(&data.{vec_id}, Hashmap::from({}));",
                self.generate_props_macro(&fields)
            )),
            None => output.push_str(&format!(
                ".insert_vs::<fn(&HVector) -> bool>(&data.{vec_id}, None);"
            )),
        };
        output.push_str(&mut self.indent());
        output
    }

    fn generate_search_vector(&mut self, vec: &SearchVector) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        // output.push_str("let tr = G::new(Arc::clone(&db), &txn);\n");
        let k = match &vec.k {
            Some(EvaluatesToNumber::I8(k)) => k.to_string(),
            Some(EvaluatesToNumber::I16(k)) => k.to_string(),
            Some(EvaluatesToNumber::I32(k)) => k.to_string(),
            Some(EvaluatesToNumber::I64(k)) => k.to_string(),
            Some(EvaluatesToNumber::U8(k)) => k.to_string(),
            Some(EvaluatesToNumber::U16(k)) => k.to_string(),
            Some(EvaluatesToNumber::U32(k)) => k.to_string(),
            Some(EvaluatesToNumber::U64(k)) => k.to_string(),
            Some(EvaluatesToNumber::U128(k)) => k.to_string(),
            Some(EvaluatesToNumber::F32(k)) => k.to_string(),
            Some(EvaluatesToNumber::F64(k)) => k.to_string(),
            Some(EvaluatesToNumber::Identifier(id)) => format!("data.{} as usize", id),
            None => "10".to_string(),
        };
        match &vec.data {
            Some(VectorData::Vector(v)) => {
                output.push_str(&mut self.indent());
                output.push_str(&format!(
                    ".search_v::<fn(&HVector) -> bool>(&{:?}, {}, None)\n",
                    v, k
                ));
            }
            Some(VectorData::Identifier(id)) => {
                output.push_str(&mut self.indent());
                output.push_str(&format!(
                    ".search_v::<fn(&HVector) -> bool>(&data.{}, {}, None)\n",
                    id, k
                ));
            }
            None => panic!("No vector data provided for search vector, {:?}", vec),
        };
        output
    }

    fn generate_assignment(&mut self, assignment: &Assignment, query: &Query) -> String {
        let mut output = String::new();
        let var_name = &assignment.variable;

        output.push_str(&mut self.indent());
        // output.push_str("let tr = G::new(Arc::clone(&db), &txn)\n"); // TODO might not be needed

        output.push_str(&format!(
            "{};",
            &mut self.generate_expression(&assignment.value, query)
        ));

        // Store variable for later use
        self.current_variables
            .insert(var_name.clone(), var_name.clone());

        output.push_str(&mut self.indent());

        match assignment.value {
            _ => output
                .push_str(format!("let {} = tr.collect_to::<Vec<_>>();\n\n", var_name).as_str()),
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
                        "let tr = G::new_from(Arc::clone(&db), &txn, {}.clone())", // TODO: remove clone
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
            Expression::AddNode(add_node) => {
                output.push_str(&mut self.generate_add_node(add_node, None));
            }
            Expression::AddEdge(add_edge) => {
                output.push_str(&mut self.generate_add_edge(add_edge, None));
            }
            Expression::BatchAddVector(batch_add_vector) => {
                output.push_str(&mut self.generate_batch_add_vector(batch_add_vector));
            }
            Expression::AddVector(add_vector) => {
                output.push_str(&mut self.generate_add_vector(add_vector));
            }
            Expression::SearchVector(search_vector) => {
                output.push_str(&mut self.indent());
                output.push_str("let tr = G::new(Arc::clone(&db), &txn)\n");
                output.push_str(&mut self.generate_search_vector(search_vector));
            }
            Expression::Exists(traversal) => {
                output.push_str(&mut self.indent());
                output.push_str("let tr = G::new(Arc::clone(&db), &txn)\n");
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
                output.push_str(&mut self.indent());
                output.push_str("let tr = G::new(Arc::clone(&db), &txn)\n");
                if let Some(ids) = ids {
                    output.push_str(&mut self.indent());
                    if let Some(var_name) = self.current_variables.get(&ids[0]) {
                        output.push_str(&format!(".n_from_id({})\n", var_name));
                    } else {
                        output.push_str(&format!(".n_from_id(&data.{})\n", to_snake_case(&ids[0])));
                    }
                } else if let Some(types) = types {
                    output.push_str(&mut self.indent());
                    output.push_str(&format!(
                        ".n_from_types(&[{}])\n",
                        types
                            .iter()
                            .map(|t| format!("\"{}\"", t))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                } else {
                    output.push_str(&mut self.indent());
                    output.push_str(".n()\n");
                }
            }
            Edge { types, ids } => {
                output.push_str(&mut self.indent());
                output.push_str("let tr = G::new(Arc::clone(&db), &txn)\n");
                if let Some(ids) = ids {
                    output.push_str(&mut self.indent());
                    if let Some(var_name) = self.current_variables.get(&ids[0]) {
                        output.push_str(&format!(".e_from_id({})\n", var_name));
                    } else {
                        output.push_str(&format!(".e_from_id(&data.{})\n", to_snake_case(&ids[0])));
                    }
                } else if let Some(types) = types {
                    output.push_str(&mut self.indent());
                    output.push_str(&mut self.indent());
                    output.push_str(&format!(
                        ".e_from_types(&[{}])\n",
                        types
                            .iter()
                            .map(|t| format!("\"{}\"", t))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                } else {
                    output.push_str(&mut self.indent());
                    output.push_str(".e()\n");
                }
            }
            Variable(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    output.push_str(&mut self.indent());
                    output.push_str(&format!(
                        "let tr = G::new_from(Arc::clone(&db), &txn, {}.clone())\n",
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
                            output.push_str(&format!(".out(\"{}\")\n", types[0]));
                        } else {
                            output.push_str(".out(\"\");\n");
                        }
                    }
                    GraphStep::In(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!(".in_(\"{}\")\n", types[0]));
                        } else {
                            output.push_str(".in_(\"\");\n");
                        }
                    }
                    GraphStep::OutE(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!(".out_e(\"{}\")\n", types[0]));
                        } else {
                            output.push_str(".out_e(\"\");\n");
                        }
                    }
                    GraphStep::InE(types) => {
                        if let Some(types) = types {
                            output.push_str(&format!(".in_e(\"{}\")\n", types[0]));
                        } else {
                            output.push_str(".in_e(\"\");\n");
                        }
                    }
                    _ => output.push_str(&mut self.generate_step(step, query)),
                },
                Step::Edge(graph_step) => match graph_step {
                    GraphStep::ToN => output.push_str(".in_v()\n"),
                    GraphStep::FromN => output.push_str(".out_v()\n"),
                    _ => output.push_str(&mut self.generate_step(step, query)),
                },
                _ => output.push_str(&mut self.generate_step(step, query)),
            }
        }

        output
    }
    fn generate_boolean_operation(&mut self, bool_op: &BooleanOp) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str(".filter_ref(|val, _| {\n");
        output.push_str(&mut self.indent());
        output.push_str("if let Ok(val) = val {\n");
        output.push_str(&mut self.indent());
        output.push_str("Ok(matches!(val.check_properties(current_prop).unwrap(), ");
        match bool_op {
            BooleanOp::Equal(value) => match &**value {
                Expression::BooleanLiteral(b) => {
                    output.push_str(&format!("Value::Boolean(val) if *val == {})));\n", b));
                }
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("Value::I64(val) if *val == {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("Value::F64(val) if *val == {})));\n", f));
                }
                Expression::StringLiteral(s) => {
                    output.push_str(&format!("Value::String(val) if *val == \"{}\")));\n", s));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("Value::String(val) if *val == \"{}\")));\n", id));
                }
                _ => output.push_str(&format!("// Unhandled value type in EQ\n {:?}", value)),
            },
            BooleanOp::GreaterThan(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("Value::I64(val) if val > {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("Value::F64(val) if val > {})));\n", f));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("Value::I64(val) if val > {})));\n", id));
                }
                _ => output.push_str("// Unhandled value type in GT\n"),
            },
            BooleanOp::GreaterThanOrEqual(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("Value::I64(val) if val >= {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("Value::F64(val) if val >= {})));\n", f));
                }
                Expression::StringLiteral(s) => {
                    output.push_str(&format!("Value::String(val) if val >= \"{}\")));\n", s));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("Value::I64(val) if val >= {})));\n", id));
                }
                _ => output.push_str("// Unhandled value type in GTE\n"),
            },
            BooleanOp::LessThan(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("Value::I64(val) if val < {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("Value::F64(val) if val < {})));\n", f));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("Value::I64(val) if val < {})));\n", id));
                }
                _ => output.push_str("// Unhandled value type in LT\n"),
            },
            BooleanOp::LessThanOrEqual(value) => match &**value {
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("Value::I64(val) if val <= {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("Value::F64(val) if val <= {})));\n", f));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("Value::I64(val) if val <= {})));\n", id));
                }
                _ => output.push_str("// Unhandled value type in LTE\n"),
            },
            BooleanOp::NotEqual(value) => match &**value {
                Expression::Identifier(id) => {
                    output.push_str(&format!("Value::String(val) if *val != \"{}\"))));\n", id));
                }
                Expression::StringLiteral(s) => {
                    output.push_str(&format!("Value::String(val) if *val != \"{}\"))));\n", s));
                }
                Expression::IntegerLiteral(i) => {
                    output.push_str(&format!("Value::I64(val) if *val != {})));\n", i));
                }
                Expression::FloatLiteral(f) => {
                    output.push_str(&format!("Value::F64(val) if *val != {})));\n", f));
                }
                Expression::BooleanLiteral(b) => {
                    output.push_str(&format!("Value::Boolean(val) if *val != {})));\n", b));
                }
                _ => output.push_str(&format!("// Unhandled value type in NEQ\n {:?}", value)),
            },
            _ => output.push_str(&format!("// Unhandled boolean operation {:?}\n", bool_op)),
        }
        output.push_str("} else { false }\n");
        output.push_str("})\n");
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
                        output.push_str(&format!(".out(\"{}\")\n", types[0]));
                    } else {
                        output.push_str(".out(\"\")\n");
                    }
                }
                GraphStep::In(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!(".in_(\"{}\")\n", types[0]));
                    } else {
                        output.push_str(".in_(\"\")\n");
                    }
                }
                GraphStep::OutE(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!(".out_e(\"{}\")\n", types[0]));
                    } else {
                        output.push_str(".out_e(\"\")\n");
                    }
                }
                GraphStep::InE(types) => {
                    if let Some(types) = types {
                        output.push_str(&format!(".in_e(\"{}\")\n", types[0]));
                    } else {
                        output.push_str(".in_e(\"\")\n");
                    }
                }
                GraphStep::ToN => output.push_str(".in_v()\n"),
                GraphStep::FromN => output.push_str(".out_v()\n"),
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

                output.push_str(&format!(".range({}, {})\n", start, end));
            }
            Step::Where(expr) => {
                match &**expr {
                    Expression::BooleanLiteral(b) => {
                        output.push_str(".filter_ref(|_, _| {\n");
                        output.push_str(&mut self.indent());
                        output.push_str(&format!("{}", b));
                        output.push_str("})\n");
                    }
                    Expression::Exists(traversal) => {
                        output.push_str(&mut self.generate_exists_check(traversal, query));
                    }
                    Expression::And(exprs) => {
                        output.push_str(".filter_ref(|val, _| {\n");
                        output.push_str(&mut self.indent());
                        output.push_str("if let Ok(val) = val {\n");
                        output.push_str(&mut self.indent());
                        for (i, expr) in exprs.iter().enumerate() {
                            if i > 0 {
                                output.push_str(" && ");
                            }
                            output.push_str(&mut self.generate_filter_condition(expr, query));
                        }
                        output.push_str(&mut self.indent());
                        output.push_str("} else { false }\n");
                        output.push_str("})\n");
                    }
                    Expression::Or(exprs) => {
                        output.push_str(".filter_ref(|val, _| {\n");
                        output.push_str(&mut self.indent());
                        output.push_str("if let Ok(val) = val {\n");
                        output.push_str(&mut self.indent());
                        for (i, expr) in exprs.iter().enumerate() {
                            if i > 0 {
                                output.push_str(" || ");
                            }
                            output.push_str(&mut self.generate_filter_condition(expr, query));
                        }
                        output.push_str(&mut self.indent());
                        output.push_str("} else { false }\n");
                        output.push_str("})\n");
                    }
                    Expression::Traversal(_) => {
                        // For traversal-based conditions
                        output.push_str(".filter_ref(|val, _| {\n");
                        output.push_str(&mut self.indent());
                        output.push_str("if let Ok(val) = val {\n");
                        output.push_str(&mut self.indent());
                        output.push_str(&mut self.generate_filter_condition(expr, query));
                        output.push_str(&mut self.indent());
                        output.push_str("} else { false }\n");
                        output.push_str("})\n");
                        // output.push_str("    let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from(node.clone()));\n");
                        // output.push_str(&mut self.generate_traversal(traversal));
                        // output.push_str(&mut self.indent());
                        // output.push_str("    tr.count();\n");
                        // output.push_str(&mut self.indent());
                        // output.push_str("    let count = let tr = tr.finish()?.as_count().unwrap();\n");
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
                output.push_str(".count();\n");
            }
            // Step::ID => {
            //     // output.push_str("let tr = tr.id();\n");
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
                output.push_str(&format!(".update(props!{{ {} }})\n", props));
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
                format!(
                    "{{
                let count = G::new(Arc::clone(&db), val.clone()){}.count();
                count > 0 }}",
                    self.generate_traversal(traversal, query)
                )
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
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Boolean(val) if *val == {}))", prop_name, b));
                                        }
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::I64(val) if *val == {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::F64(val) if *val == {}))", prop_name, f));
                                        }
                                        Expression::StringLiteral(s) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::String(val) if *val == \"{}\"))", prop_name, s));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| *v == data.{})", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in EQ */"),
                                    },
                                    BooleanOp::GreaterThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::I64(val) if *val > {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::F64(val) if *val > {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| *v > data.{})", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GT */"),
                                    },
                                    BooleanOp::GreaterThanOrEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::I64(val) if *val >= {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::F64(val) if *val >= {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| *v >= data.{})", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GTE */"),
                                    },
                                    BooleanOp::LessThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::I64(val) if *val < {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::F64(val) if *val < {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| *v < data.{})", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LT */"),
                                    },
                                    BooleanOp::LessThanOrEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::I64(val) if *val <= {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::F64(val) if *val <= {}))", prop_name, f));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| *v <= data.{})", prop_name, id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LTE */"),
                                    },
                                    BooleanOp::NotEqual(value) => match &**value {
                                        Expression::StringLiteral(s) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::String(val) if *val != \"{}\"))", prop_name, s));
                                        }
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::I64(val) if *val != {}))", prop_name, i));
                                        }
                                        Expression::FloatLiteral(f) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::F64(val) if *val != {}))", prop_name, f));
                                        }
                                        Expression::BooleanLiteral(b) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| matches!(v, Value::Boolean(val) if *val != {}))", prop_name, b));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("val.check_property(\"{}\").map_or(false, |v| *v != data.{})", prop_name, id));
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
                                    "val.check_property(\"{}\").is_some()",
                                    prop_name
                                ));
                            }
                            if inner_traversal {
                                output.push_str("}");
                            }

                            return output;
                        }
                        Step::Count => {
                            output.push_str("let count = tr.count();\n");
                            if let Some(Step::BooleanOperation(bool_op)) =
                                traversal.steps.get(i + 1)
                            {
                                match bool_op {
                                    BooleanOp::Equal(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count == {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count == data.{}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in EQ */"),
                                    },
                                    BooleanOp::GreaterThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count > {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count > data.{}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GT */"),
                                    },
                                    BooleanOp::LessThan(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count < {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count < data.{}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LT */"),
                                    },
                                    BooleanOp::GreaterThanOrEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count >= {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count >= data.{}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in GTE */"),
                                    },
                                    BooleanOp::LessThanOrEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count <= {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count <= data.{}", id));
                                        }
                                        _ => output.push_str("/* Unhandled value type in LTE */"),
                                    },
                                    BooleanOp::NotEqual(value) => match &**value {
                                        Expression::IntegerLiteral(i) => {
                                            output.push_str(&format!("count != {}", i));
                                        }
                                        Expression::Identifier(id) => {
                                            output.push_str(&format!("count != data.{}", id));
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
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::Boolean(val) if *val == {}))\n", b));
                                }
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if *val == {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::F64(val) if *val == {}))\n", f));
                                }
                                Expression::StringLiteral(s) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::String(val) if *val == \"{}\"))\n", s));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::String(val) if *val == {}))\n", id));
                                }
                                _ => output.push_str(&format!(
                                    "// Unhandled value type in EQ\n {:?}",
                                    value
                                )),
                            },
                            BooleanOp::GreaterThan(value) => match &**value {
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if val > {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::F64(val) if val > {}))\n", f));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if val > {}))\n", id));
                                }
                                _ => output.push_str("// Unhandled value type in GT\n"),
                            },
                            BooleanOp::GreaterThanOrEqual(value) => match &**value {
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if val >= {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::F64(val) if val >= {}))\n", f));
                                }
                                Expression::StringLiteral(s) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::String(val) if val >= \"{}\"))\n", s));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if val >= {}))\n", id));
                                }
                                _ => output.push_str("// Unhandled value type in GTE\n"),
                            },
                            BooleanOp::LessThan(value) => match &**value {
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if val < {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::F64(val) if val < {}))\n", f));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if val < {}))\n", id));
                                }
                                _ => output.push_str("// Unhandled value type in LT\n"),
                            },
                            BooleanOp::LessThanOrEqual(value) => match &**value {
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if val <= {}))\n", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::F64(val) if val <= {}))\n", f));
                                }
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(node.check_property(current_prop).unwrap(), Value::I64(val) if val <= {}))\n", id));
                                }
                                _ => output.push_str("// Unhandled value type in LTE\n"),
                            },
                            BooleanOp::NotEqual(value) => match &**value {
                                Expression::Identifier(id) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::String(val) if *val != \"{}\")", id));
                                }
                                Expression::StringLiteral(s) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::String(val) if *val != \"{}\")", s));
                                }
                                Expression::IntegerLiteral(i) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::I64(val) if *val != {})", i));
                                }
                                Expression::FloatLiteral(f) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::F64(val) if *val != {})", f));
                                }
                                Expression::BooleanLiteral(b) => {
                                    output.push_str(&format!("matches!(val.check_property(current_prop).unwrap(), Value::Boolean(val) if *val != {})", b));
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
                                output.push_str("let tr = G::new(Arc::clone(&db), val.clone())");
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

    fn generate_add_node(&mut self, add_node: &AddNode, var_name: Option<&str>) -> String {
        let mut output = String::new();

        output.push_str(&mut self.indent());
        output.push_str("let tr = G::new_mut(Arc::clone(&db), &mut txn)\n");
        // todo conditionally do new_mut_from

        let node_type = add_node
            .node_type
            .as_ref()
            .map_or("".to_string(), |t| t.clone());

        let (props, possible_id) = if let Some(fields) = &add_node.fields {
            let possible_id = match fields.get("id") {
                Some(ValueType::Literal(Value::String(s))) => Some(s.clone()),
                Some(ValueType::Identifier(identifier)) => {
                    Some(format!("data.{}.clone()", identifier))
                }
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
                    ".add_n(\"{}\", {}, None, Some({}))",
                    node_type, props, id
                ));
            }
            None => {
                output.push_str(&format!(".add_n(\"{}\", {}, None, None)", node_type, props));
            }
        }

        if let Some(name) = var_name {
            output.push_str(&mut self.indent());
            output.push_str(&format!(
                "let {} = tr.collect_to::<Vec<_>>();\n", // TODO: change to return single value
                name
            ));
            self.current_variables
                .insert(name.to_string(), name.to_string());
        }

        output
    }

    fn generate_add_edge(&mut self, add_edge: &AddEdge, var_name: Option<&str>) -> String {
        let mut output = String::new();

        output.push_str(&mut self.indent());
        output.push_str("let tr = G::new_mut(Arc::clone(&db), &mut txn)\n");
        // todo conditionally do new_mut_from

        let edge_type_label = add_edge
            .edge_type
            .as_ref()
            .map_or("".to_string(), |t| t.clone());

        let (props, possible_id) = if let Some(fields) = &add_edge.fields {
            let possible_id = match fields.get("id") {
                Some(ValueType::Literal(Value::String(s))) => s.clone(),
                Some(ValueType::Identifier(identifier)) => {
                    format!("data.{}.clone()", format!("{}", identifier))
                }
                _ => "None".to_string(),
            };
            (self.generate_props_macro(&fields), possible_id)
        } else {
            ("props!{}".to_string(), "None".to_string())
        };

        // TODO: change
        let from_id = match &add_edge.connection.from_id.as_ref().unwrap() {
            IdType::Literal(id) => format!("\"{}\"", id),
            IdType::Identifier(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    format!("{}.id()", to_snake_case(var_name))
                } else {
                    format!("data.id()") // TODO handle properly
                }
            }
        };

        let to_id = match &add_edge.connection.to_id.as_ref().unwrap() {
            IdType::Literal(id) => format!("\"{}\"", id),
            IdType::Identifier(var) => {
                if let Some(var_name) = self.current_variables.get(var) {
                    format!("{}.id()", to_snake_case(var_name))
                } else {
                    format!("data.id()") // TODO handle properly
                }
            }
        };

        let should_check: bool = true; // TODO: check some how?

        let edge_type = match &add_edge.edge_type {
            Some(_) => "EdgeType::Vec",
            None => "EdgeType::Std",
        };

        output.push_str(&mut self.indent());
        output.push_str(&format!(
            ".add_e(\"{}\", {}, {}, {}, {}, {}, {})",
            edge_type_label, props, possible_id, from_id, to_id, should_check, edge_type,
        ));
        // output.push_str(&format!("tr.result()?;\n"));

        if let Some(name) = var_name {
            output.push_str(&mut self.indent());
            output.push_str(&format!(
                "let {} = tr.collect_to::<Vec<_>>();\n", // TODO: change to return single value
                name
            ));
            self.current_variables
                .insert(name.to_string(), name.to_string());
        }

        // TODO: collect to vec or single value?

        output
    }

    fn generate_drop(&mut self, expr: &Expression, query: &Query) -> String {
        let mut output = String::new();
        output.push_str(&mut self.indent());
        output.push_str("let tr = G::new_mut(Arc::clone(&db), &mut txn)\n");
        // todo conditionally do new_mut_from
        match expr {
            Expression::Traversal(traversal) => {
                output.push_str(&mut self.generate_traversal(traversal, query));
            }
            _ => {}
        }
        output.push_str(&mut self.indent());
        output.push_str(".drop();\n");
        output
    }

    fn generate_exists_check(&mut self, traversal: &Traversal, query: &Query) -> String {
        let mut output = String::new();
        output.push_str("let tr = tr.filter_ref(&txn, |val| {\n");
        output.push_str(&mut self.indent());
        output.push_str("if let Ok(val) = val {\n");
        output.push_str(&mut self.indent());
        output.push_str("let tr = G::new(Arc::clone(&db), &txn, val.clone())\n");
        output.push_str(&mut self.indent());
        output.push_str(&mut self.generate_traversal(traversal, query));
        output.push_str(&mut self.indent());
        output.push_str("let count = tr.count();\n");
        output.push_str(&mut self.indent());
        output.push_str("Ok(count > 0)\n");
        output.push_str(&mut self.indent());
        output.push_str("} else { false }\n");
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
                Expression::Empty => {
                    output.push_str(&format!(
                        "return_vals.insert(\"message\".to_string(), ReturnValue::Empty);\n",
                    ));
                }
                Expression::Traversal(traversal) => {
                    output.push_str(&mut self.generate_traversal(traversal, query));
                    output.push_str(&mut self.indent());
                    output.push_str("let return_val = tr.collect::<Vec<_>>();\n");
                    output.push_str(&mut self.indent());
                    if let Variable(var_name) = &traversal.start {
                        output.push_str(&format!(
                            "return_vals.insert(\"{}\".to_string(), ReturnValue::from_traversal_value_array_with_mixin(return_val, remapping_vals.borrow_mut()));\n",
                            var_name,
                        ));
                    } else {
                        println!("Unhandled traversal return value: {:?}", expr);
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

    fn value_type_to_rust(&mut self, value: &ValueType) -> String {
        match value {
            ValueType::Literal(value) => self.value_to_rust(value),
            ValueType::Identifier(identifier) => format!("data.{}", to_snake_case(identifier)),
            _ => unreachable!(),
        }
    }

    fn value_to_rust(&mut self, value: &Value) -> String {
        match value {
            Value::String(s) => format!("\"{}\"", s),
            Value::I64(i) => i.to_string(),
            Value::F64(f) => f.to_string(),
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
        output.push_str(";");
        output.push_str(&format!("let tr = tr.map(|{}| {{\n", var_name));
        output.push_str(&mut self.indent());
        output.push_str(&format!("match {} {{\n", var_name));
        output.push_str(&mut self.indent());
        output.push_str(&format!("Ok(ref item) => {{\n",));

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
        output.push_str(&format!("{}.id().clone(),\n", var_name));
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
        // handle error
        output.push_str("}");
        output.push_str(&self.indent());
        output.push_str("Err(e) => {\n");
        output.push_str(&self.indent());
        output.push_str("println!(\"Error: {:?}\", e);\n");
        output.push_str(&self.indent());
        output.push_str("return Err(GraphError::ConversionError(\"Error: {:?}\".to_string()))");
        output.push_str(&self.indent());
        output.push_str("}};");
        output.push_str(&self.indent());
        output.push_str("item");

        output.push_str("}).filter_map(|item| item.ok());\n");
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
        let tr = tr.for_each_node(&txn, |node, txn| {
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
        // output
        output.push_str(";");
        output.push_str(&format!("let tr = tr.map(|{}| {{\n", var_name));
        output.push_str(&mut self.indent());
        output.push_str(&format!("match {} {{\n", var_name));
        output.push_str(&mut self.indent());
        output.push_str(&format!("Ok(ref item) => {{\n",));
        for (key, field) in object.fields.iter() {
            output.push_str(&mut self.indent());
            match field {
                // TODO: handle traversal with multiple steps
                FieldValue::Traversal(traversal) => {
                    output.push_str(&format!("let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::from({}.clone()));\n", to_snake_case(var_name)));
                    output.push_str(&mut self.indent());
                    output.push_str(&mut self.generate_traversal(traversal, query));
                    output.push_str(&mut self.indent());
                    match traversal.steps.last() {
                        Some(Step::Object(obj)) => {
                            if let Some((field_name, _)) = obj.fields.first() {
                                if field_name.as_str() == "id" {
                                    output.push_str(&format!(
                                        "let {} = tr.collect::<Vec<_>>()?[0].id();\n",
                                        to_snake_case(key)
                                    ));
                                }
                            }
                        }
                        _ => {
                            output.push_str(&format!(
                                "let {} = tr.collect::<Vec<_>>();\n",
                                to_snake_case(key)
                            ));
                        }
                    }
                }
                // TODO: handle expression with multiple steps
                FieldValue::Expression(expr) => {
                    output.push_str(&mut self.indent());
                    output.push_str(&mut self.generate_expression(expr, query));
                    output.push_str(&mut self.indent());
                    match expr {
                        Expression::Traversal(traversal) => match traversal.steps.last().unwrap() {
                            Step::Object(obj) => {
                                if let Some((field_name, _)) = obj.fields.first() {
                                    if field_name.as_str() == "id" {
                                        output.push_str(&format!(
                                            "let {} = tr.collect::<Vec<_>>()?[0].id();\n",
                                            to_snake_case(key)
                                        ));
                                    }
                                }
                            }
                            _ => {
                                output.push_str(&format!(
                                    "let {} = tr.collect::<Vec<_>>()?[0];\n",
                                    to_snake_case(key)
                                ));
                            }
                        },

                        _ => {
                            output.push_str(&format!(
                                "let {} = tr.collect::<Vec<_>>();\n",
                                to_snake_case(key)
                            ));
                        }
                    }
                }
                FieldValue::Literal(value) => {
                    output.push_str(&format!(
                        "let {} = {}.check_property(\"{}\");\n",
                        to_snake_case(key),
                        var_name,
                        self.value_to_rust(value)
                    ));
                }
                FieldValue::Identifier(id) => {
                    output.push_str(&format!(
                        "let {} = {}.check_property(\"{}\");\n",
                        to_snake_case(key),
                        var_name,
                        to_snake_case(id)
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
        output.push_str(&format!("{}.id().clone(),\n", var_name));
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
        output.push_str(&self.indent());

        // handle error
        output.push_str("}");
        output.push_str(&self.indent());
        output.push_str("Err(e) => {\n");
        output.push_str(&self.indent());
        output.push_str("println!(\"Error: {:?}\", e);\n");
        output.push_str(&self.indent());
        output.push_str("return Err(GraphError::ConversionError(\"Error: {:?}\".to_string()))");
        output.push_str(&self.indent());
        output.push_str("}};");
        output.push_str(&self.indent());
        output.push_str("item");

        output.push_str("}).filter_map(|item| item.ok());\n");
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
            FieldValue::Identifier(id) => {
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
                                    "ReturnValue::from({}.id())\n",
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
                Expression::Empty => {
                    output.push_str(&format!("ReturnValue::Empty\n"));
                }
                Expression::Identifier(id) => {
                    output.push_str(&format!("ReturnValue::from({}.id())\n", to_snake_case(key)));
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

pub fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    let mut prev_is_upper = false;

    while let Some(c) = chars.next() {
        if c.is_uppercase() {
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
