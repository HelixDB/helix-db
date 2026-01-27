use std::fmt::{self, Display};

use crate::helixc::generator::{
    return_values::{ReturnValue, ReturnValueStruct, RustFieldType},
    statements::Statement,
    utils::{EmbedData, GeneratedType},
};

pub struct Query {
    pub embedding_model_to_use: Option<String>,
    pub mcp_handler: Option<String>,
    pub name: String,
    pub statements: Vec<Statement>,
    pub parameters: Vec<Parameter>, // iterate through and print each one
    pub sub_parameters: Vec<(String, Vec<Parameter>)>,
    pub return_values: Vec<(String, ReturnValue)>, // Legacy approach
    pub return_structs: Vec<ReturnValueStruct>,    // New struct-based approach
    pub use_struct_returns: bool,                  // Flag to use new vs old approach
    pub is_mut: bool,
    pub hoisted_embedding_calls: Vec<EmbedData>,
}

impl Query {
    fn print_handler(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_mut {
            writeln!(f, "#[handler(is_write)]")
        } else {
            writeln!(f, "#[handler]")
        }
    }

    fn print_parameters(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, parameters) in &self.sub_parameters {
            writeln!(f, "#[derive(Serialize, Deserialize, Clone)]")?;
            writeln!(f, "pub struct {name} {{")?;
            for parameter in parameters {
                writeln!(f, "    pub {}: {},", parameter.name, parameter.field_type)?;
            }
            writeln!(f, "}}")?;
        }
        Ok(())
    }

    fn print_return_values(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.use_struct_returns {
            // Generate struct definitions for new approach (including nested structs)
            for struct_def in &self.return_structs {
                write!(f, "{}", struct_def.generate_all_struct_defs())?;
                writeln!(f)?;
            }
        }
        // Legacy approach doesn't need struct generation (uses json! macro)
        Ok(())
    }

    fn print_input_struct(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "#[derive(Serialize, Deserialize, Clone)]")?;
        writeln!(f, "pub struct {}Input {{\n", self.name)?;
        write!(
            f,
            "{}",
            self.parameters
                .iter()
                .map(|p| format!("{p}"))
                .collect::<Vec<_>>()
                .join(",\n")
        )?;
        write!(f, "\n}}\n")?;
        Ok(())
    }

    fn print_hoisted_embedding_calls(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.hoisted_embedding_calls.is_empty() {
            writeln!(
                f,
                "Err(IoContFn::create_err(move |__internal_cont_tx, __internal_ret_chan| Box::pin(async move {{"
            )?;
            // ((({ })))

            for (i, embed) in self.hoisted_embedding_calls.iter().enumerate() {
                let name = EmbedData::name_from_index(i);
                writeln!(f, "let {name} = {embed};")?;
            }

            writeln!(
                f,
                "__internal_cont_tx.send_async((__internal_ret_chan, Box::new(move || {{"
            )?;
            // ((({ }))).await.expect("Cont Channel should be alive")

            for (i, _) in self.hoisted_embedding_calls.iter().enumerate() {
                let name = EmbedData::name_from_index(i);
                writeln!(f, "let {name}: Vec<f64> = {name}?;")?;
            }
        }
        Ok(())
    }

    fn print_txn_commit(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "txn.commit().map_err(|e| GraphError::New(format!(\"Failed to commit transaction: {{:?}}\", e)))?;"
        )
    }

    fn print_query(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // prints the function signature
        if !self.parameters.is_empty() {
            self.print_input_struct(f)?;
            self.print_parameters(f)?;
        }
        if !self.return_values.is_empty() {
            self.print_return_values(f)?;
        }

        self.print_handler(f)?;
        writeln!(
            f,
            "pub fn {} (input: HandlerInput) -> Result<Response, GraphError> {{",
            self.name
        )?;

        // print the db boilerplate
        writeln!(f, "let db = Arc::clone(&input.graph.storage);")?;
        if !self.parameters.is_empty() {
            match self.hoisted_embedding_calls.is_empty() {
                true => writeln!(
                    f,
                    "let data = input.request.in_fmt.deserialize::<{}Input>(&input.request.body)?;",
                    self.name
                )?,
                false => writeln!(
                    f,
                    "let data = input.request.in_fmt.deserialize::<{}Input>(&input.request.body)?.into_owned();",
                    self.name
                )?,
            }
        }

        // print embedding calls
        self.print_hoisted_embedding_calls(f)?;
        writeln!(f, "let arena = Bump::new();")?;

        match self.is_mut {
            true => writeln!(
                f,
                "let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!(\"Failed to start write transaction: {{:?}}\", e)))?;"
            )?,
            false => writeln!(
                f,
                "let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!(\"Failed to start read transaction: {{:?}}\", e)))?;"
            )?,
        }

        // prints each statement
        for statement in &self.statements {
            writeln!(f, "    {statement};")?;
        }

        // Generate return value
        if self.use_struct_returns && !self.return_structs.is_empty() {
            // New struct-based approach - map during response construction
            write!(f, "let response = json!({{")?;
            for (i, struct_def) in self.return_structs.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                writeln!(f)?;

                if struct_def.is_aggregate {
                    // Aggregate/GroupBy - return the enum directly (it already implements Serialize)
                    writeln!(
                        f,
                        "    \"{}\": {}",
                        struct_def.source_variable, struct_def.source_variable
                    )?;
                } else if struct_def.source_variable.is_empty() {
                    // Object literal - construct from multiple sources
                    // Generate each field from the object literal
                    for (field_idx, field) in struct_def.fields.iter().enumerate() {
                        if field_idx > 0 {
                            write!(f, ",")?;
                            writeln!(f)?;
                        }

                        let field_info = &struct_def.field_infos[field_idx];

                        // Determine how to construct this field based on its source
                        let field_value = match &field_info.source {
                            crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                closure_source_var: Some(source_var),
                                accessed_field_name: Some(prop_name),
                                nested_struct_name: None,
                                ..
                            } => {
                                // Simple property access like app::{name}
                                if prop_name == "id" {
                                    format!("uuid_str({}.id(), &arena)", source_var)
                                } else if prop_name == "label" {
                                    format!("{}.label()", source_var)
                                } else {
                                    format!("{}.get_property(\"{}\")", source_var, prop_name)
                                }
                            }
                            _ if matches!(
                                field_info.field_type,
                                crate::helixc::generator::return_values::ReturnFieldType::Nested(_)
                            ) => {
                                // Nested struct or object - need to construct recursively
                                let nested_fields = match &field_info.field_type {
                                    crate::helixc::generator::return_values::ReturnFieldType::Nested(fields) => fields,
                                    _ => {
                                        debug_assert!(false, "Type invariant: expected Nested field type after matches! guard");
                                        static EMPTY: Vec<crate::helixc::generator::return_values::ReturnFieldInfo> = Vec::new();
                                        &EMPTY
                                    }
                                };

                                // Extract nested struct name and source var from field info
                                let (nested_struct_name, source_var) = match &field_info.source {
                                    crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        closure_source_var: Some(src),
                                        nested_struct_name: Some(name),
                                        ..
                                    } => (Some(name.clone()), Some(src.clone())),
                                    crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        closure_source_var: Some(src),
                                        ..
                                    } => (None, Some(src.clone())),
                                    _ => (None, None),
                                };

                                // Check if this is a collection (Vec)
                                let is_vec = field.field_type.starts_with("Vec<");

                                if is_vec {
                                    // Generate construction for each element
                                    if let Some(src_var) = source_var {
                                        let struct_name = nested_struct_name.as_deref().unwrap_or("UnknownStruct");

                                        format!("vec![{}].into_iter().map(|item| {} {{\n{}            \n}}).collect::<Vec<_>>()",
                                            src_var,
                                            struct_name,
                                            nested_fields.iter().map(|f| {
                                                let val = if f.name == "id" {
                                                    "uuid_str(item.id(), &arena)".to_string()
                                                } else if f.name == "label" {
                                                    "item.label()".to_string()
                                                } else {
                                                    format!("item.get_property(\"{}\")", f.name)
                                                };
                                                format!("                {}: {},", f.name, val)
                                            }).collect::<Vec<_>>().join("\n")
                                        )
                                    } else {
                                        "Vec::new()".to_string()
                                    }
                                } else {
                                    // Single nested object
                                    if let (Some(struct_name), Some(src_var)) = (nested_struct_name.as_ref(), source_var.as_ref()) {
                                        format!("{} {{\n{}            \n}}",
                                            struct_name,
                                            nested_fields.iter().map(|f| {
                                                let val = if f.name == "id" {
                                                    format!("uuid_str({}.id(), &arena)", src_var)
                                                } else if f.name == "label" {
                                                    format!("{}.label()", src_var)
                                                } else {
                                                    format!("{}.get_property(\"{}\")", src_var, f.name)
                                                };
                                                format!("                {}: {},", f.name, val)
                                            }).collect::<Vec<_>>().join("\n")
                                        )
                                    } else {
                                        "serde_json::Value::Null".to_string()
                                    }
                                }
                            }
                            _ => {
                                // Fallback
                                "serde_json::Value::Null".to_string()
                            }
                        };

                        write!(f, "    \"{}\": {}", field.name, field_value)?;
                    }
                } else if struct_def.is_collection {
                    // Collection - generate mapping code
                    // Use HQL closure param name if available, otherwise fall back to singular form
                    let singular_var = struct_def
                        .closure_param_name
                        .as_deref()
                        .unwrap_or_else(|| struct_def.source_variable.trim_end_matches('s'));
                    // Check if any field is a nested traversal (needs Result handling)
                    let has_nested = struct_def.fields.iter().any(|f| f.is_nested_traversal);

                    if has_nested {
                        writeln!(
                            f,
                            "    \"{}\": {}.iter().map(|{}| Ok::<_, GraphError>({} {{",
                            struct_def.source_variable,
                            struct_def.source_variable,
                            singular_var,
                            struct_def.name
                        )?;
                    } else {
                        writeln!(
                            f,
                            "    \"{}\": {}.iter().map(|{}| {} {{",
                            struct_def.source_variable,
                            struct_def.source_variable,
                            singular_var,
                            struct_def.name
                        )?;
                    }

                    // Generate field assignments
                    for (field_idx, field) in struct_def.fields.iter().enumerate() {
                        let field_value = if field.is_nested_traversal {
                            // Get the nested traversal info from field_infos
                            let field_info = &struct_def.field_infos[field_idx];

                            // Handle scalar nested traversals with graph steps (e.g., _::Out<HasCluster>::COUNT)
                            // These require full G::from_iter wrapper even though they return scalar values
                            if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                traversal_code: Some(trav_code),
                                traversal_type,
                                requires_full_traversal: true,
                                nested_struct_name: None,  // Distinguish from nested struct traversals
                                ..
                            } = &field_info.source {
                                let (source_var, is_single_source) = if let Some(trav_type) = traversal_type {
                                    use crate::helixc::generator::traversal_steps::TraversalType;
                                    match trav_type {
                                        TraversalType::FromSingle(var) => {
                                            let v = var.inner();
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), true)
                                        }
                                        TraversalType::FromIter(var) => {
                                            let v = var.inner();
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), false)
                                        }
                                        _ => (singular_var.to_string(), false)
                                    }
                                } else {
                                    (singular_var.to_string(), false)
                                };

                                let iterator_expr = if is_single_source {
                                    format!("std::iter::once({}.clone())", source_var)
                                } else {
                                    format!("{}.iter().cloned()", source_var)
                                };

                                // Check if this is a singular TraversalValue (not Vec)
                                let is_singular_traversal_value = matches!(
                                    &field_info.field_type,
                                    crate::helixc::generator::return_values::ReturnFieldType::Simple(ty) if ty == &RustFieldType::TraversalValue
                                );

                                let is_vec_traversal_value = matches!(
                                    &field_info.field_type,
                                    crate::helixc::generator::return_values::ReturnFieldType::Simple(RustFieldType::Vec(inner)) if inner.as_ref() == &RustFieldType::TraversalValue
                                );

                                // Check if this is a Vec<Value> (from graph navigation returning mapped values)
                                let is_vec_value = matches!(
                                    &field_info.field_type,
                                    crate::helixc::generator::return_values::ReturnFieldType::Simple(RustFieldType::Vec(inner)) if inner.as_ref() == &RustFieldType::Value
                                );

                                // Check if this is an Option<Value> (from ::FIRST)
                                let is_option_value = matches!(
                                    &field_info.field_type,
                                    crate::helixc::generator::return_values::ReturnFieldType::Simple(RustFieldType::OptionValue)
                                );

                                if is_singular_traversal_value {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.collect_to_obj()?", iterator_expr, trav_code)
                                } else if is_option_value {
                                    // For ::FIRST, take the first element from the iterator
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.next().transpose()?", iterator_expr, trav_code)
                                } else if is_vec_traversal_value || is_vec_value {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.collect::<Result<Vec<_>, _>>()?", iterator_expr, trav_code)
                                } else {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}", iterator_expr, trav_code)
                                }
                            // Handle scalar nested traversals with closure parameters (e.g., username: u::{name})
                            // or anonymous traversals (e.g., creatorID: _::In<Created>::ID)
                            } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                closure_source_var: Some(_),
                                accessed_field_name: accessed_field,
                                nested_struct_name: None,
                                ..
                            } = &field_info.source {
                                // Use singular_var which is the actual closure parameter (e.g., "e" from entries::|e|)
                                // This is a scalar field accessing a closure parameter or anonymous variable
                                let field_to_access = accessed_field.as_ref()
                                    .map(|s| s.as_str())
                                    .unwrap_or(field.name.as_str());

                                if field_to_access == "id" || field_to_access == "ID" {
                                    format!("uuid_str({}.id(), &arena)", singular_var)
                                } else if field_to_access == "label" || field_to_access == "Label" {
                                    format!("{}.label()", singular_var)
                                } else {
                                    format!("{}.get_property(\"{}\")", singular_var, field_to_access)
                                }
                            } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                traversal_code: Some(trav_code),
                                nested_struct_name: Some(nested_name),
                                traversal_type,
                                closure_source_var,
                                closure_param_name: _,
                                own_closure_param,
                                is_first,
                                ..
                            } = &field_info.source {
                                // Generate nested traversal code
                                let nested_fields = match &field_info.field_type {
                                    crate::helixc::generator::return_values::ReturnFieldType::Nested(fields) => fields,
                                    _ => {
                                        debug_assert!(false, "Type invariant: Nested traversal must have Nested field type");
                                        static EMPTY: Vec<crate::helixc::generator::return_values::ReturnFieldInfo> = Vec::new();
                                        &EMPTY
                                    }
                                };

                                // Extract the actual source variable from the traversal type
                                // Resolve "_" and "val" placeholders to actual iteration variable
                                let (source_var, is_single_source) = if let Some(trav_type) = traversal_type {
                                    use crate::helixc::generator::traversal_steps::TraversalType;
                                    match trav_type {
                                        TraversalType::FromSingle(var) => {
                                            let v = var.inner();
                                            // Resolve placeholders: both "_" and "val" should use the iteration variable
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), true)
                                        }
                                        TraversalType::FromIter(var) => {
                                            let v = var.inner();
                                            // Resolve placeholders: both "_" and "val" should use the iteration variable
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), false)
                                        }
                                        _ => {
                                            (singular_var.to_string(), false)
                                        }
                                    }
                                } else {
                                    (singular_var.to_string(), false)
                                };

                                // Determine if we need iter().cloned() or std::iter::once()
                                let iterator_expr = if is_single_source {
                                    format!("std::iter::once({}.clone())", source_var)
                                } else {
                                    format!("{}.iter().cloned()", source_var)
                                };

                                // Determine the closure parameter name to use in .map(|param| ...)
                                // Only use own_closure_param (this traversal's closure), not parent context
                                let closure_param = own_closure_param.as_ref()
                                    .map(|s| s.as_str())
                                    .filter(|s| !s.is_empty() && *s != "_" && *s != "val")
                                    .unwrap_or("item");

                                // Generate field assignments for nested struct
                                // Check if we're in a closure context, resolve "_" placeholder
                                let _closure_context_var = closure_source_var.as_ref()
                                    .map(|s| if s == "_" { singular_var } else { s.as_str() })
                                    .unwrap_or(singular_var);

                                let mut nested_field_assigns = String::new();
                                let mut has_vec_traversal_value = false;
                                for nested_field in nested_fields {
                                    // Check if this nested field is itself a nested traversal with a nested struct
                                    let nested_val = if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        traversal_code: Some(inner_trav_code),
                                        nested_struct_name: Some(inner_nested_name),
                                        traversal_type: inner_traversal_type,
                                        ..
                                    } = &nested_field.source {
                                        // This is a deeply nested traversal - generate nested traversal code

                                        // Extract the source variable for this deeply nested traversal
                                        let _inner_source_var = if let Some(inner_trav_type) = inner_traversal_type {
                                            use crate::helixc::generator::traversal_steps::TraversalType;
                                            match inner_trav_type {
                                                TraversalType::FromSingle(var) | TraversalType::FromIter(var) => {
                                                    let v = var.inner();
                                                    // Resolve placeholders: "_" and "val" should use "item" in nested context
                                                    if v == "_" || v == "val" { "item" } else { v }
                                                }
                                                _ => "item"
                                            }
                                        } else {
                                            "item"
                                        };

                                        // Get the nested fields if available
                                        let inner_fields_str = if let crate::helixc::generator::return_values::ReturnFieldType::Nested(inner_fields) = &nested_field.field_type {
                                            // Generate field assignments for the deeply nested struct
                                            let mut inner_assigns = String::new();
                                            for inner_f in inner_fields {
                                                let inner_val = if inner_f.name == "id" {
                                                    "uuid_str(inner_item.id(), &arena)".to_string()
                                                } else if inner_f.name == "label" {
                                                    "inner_item.label()".to_string()
                                                } else {
                                                    format!("inner_item.get_property(\"{}\")", inner_f.name)
                                                };
                                                inner_assigns.push_str(&format!("\n{}: {},", inner_f.name, inner_val));
                                            }
                                            format!(".map(|inner_item| inner_item.map(|inner_item| {} {{{}\n}})).collect::<Result<Vec<_>, _>>()?", inner_nested_name, inner_assigns)
                                        } else {
                                            ".collect::<Vec<_>>()".to_string()
                                        };
                                        format!("G::from_iter(&db, &txn, std::iter::once({}.clone()), &arena){}{}", closure_param, inner_trav_code, inner_fields_str)
                                    } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        traversal_code: Some(inner_trav_code),
                                        traversal_type: inner_traversal_type,
                                        requires_full_traversal: true,
                                        nested_struct_name: None,  // No nested struct = Vec<TraversalValue>
                                        ..
                                    } = &nested_field.source {
                                        // Handle traversals returning Vec<TraversalValue> (no object step)
                                        // e.g., instances: cluster::Out<CreatedInstance>
                                        has_vec_traversal_value = true;

                                        // Extract source variable from traversal type
                                        let (inner_source_var, is_single_source) = if let Some(inner_trav_type) = inner_traversal_type {
                                            use crate::helixc::generator::traversal_steps::TraversalType;
                                            match inner_trav_type {
                                                TraversalType::FromSingle(var) => {
                                                    let v = var.inner();
                                                    let resolved = if v == "_" || v == "val" { closure_param } else { v.as_str() };
                                                    (resolved.to_string(), true)
                                                }
                                                TraversalType::FromIter(var) => {
                                                    let v = var.inner();
                                                    let resolved = if v == "_" || v == "val" { closure_param } else { v.as_str() };
                                                    (resolved.to_string(), false)
                                                }
                                                _ => (closure_param.to_string(), false)
                                            }
                                        } else {
                                            (closure_param.to_string(), false)
                                        };

                                        let inner_iterator_expr = if is_single_source {
                                            format!("std::iter::once({}.clone())", inner_source_var)
                                        } else {
                                            format!("{}.iter().cloned()", inner_source_var)
                                        };

                                        format!("G::from_iter(&db, &txn, {}, &arena){}.collect::<Result<Vec<_>, _>>()?", inner_iterator_expr, inner_trav_code)
                                    } else {
                                        // Check if this field itself is a nested traversal that accesses the closure parameter
                                        // Extract both the access variable and the actual field being accessed
                                        let (access_var, accessed_field_name) = if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                            closure_source_var: Some(source_var),
                                            accessed_field_name: accessed_field,
                                            ..
                                        } = &nested_field.source {
                                            // Case 1: Accessing a closure variable's field (e.g., usr::ID)
                                            let field_to_access = accessed_field.as_ref()
                                                .map(|s| s.as_str())
                                                .unwrap_or(nested_field.name.as_str());
                                            (source_var.as_str(), field_to_access)
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                            closure_source_var: None,
                                            accessed_field_name: Some(accessed_field),
                                            ..
                                        } = &nested_field.source {
                                            // Case 2: NestedTraversal with remapped field name (e.g., postID: id)
                                            (closure_param, accessed_field.as_str())
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::ImplicitField { property_name: Some(prop) } = &nested_field.source {
                                            // Case 3: Implicit field with remapped name (e.g., postID: id)
                                            (closure_param, prop.as_str())
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::SchemaField { property_name: Some(prop) } = &nested_field.source {
                                            // Case 4: Schema field with remapped name (e.g., post: content)
                                            (closure_param, prop.as_str())
                                        } else {
                                            // Case 5: Default - use the field name as-is
                                            (closure_param, nested_field.name.as_str())
                                        };

                                        if accessed_field_name == "id" || accessed_field_name == "ID" {
                                            format!("uuid_str({}.id(), &arena)", access_var)
                                        } else if accessed_field_name == "label" || accessed_field_name == "Label" {
                                            format!("{}.label()", access_var)
                                        } else if accessed_field_name == "from_node" {
                                            format!("uuid_str({}.from_node(), &arena)", access_var)
                                        } else if accessed_field_name == "to_node" {
                                            format!("uuid_str({}.to_node(), &arena)", access_var)
                                        } else {
                                            format!("{}.get_property(\"{}\")", access_var, accessed_field_name)
                                        }
                                    };
                                    nested_field_assigns.push_str(&format!("\n                        {}: {},", nested_field.name, nested_val));
                                }

                                // Check if any nested field is a deeply nested traversal that needs error handling
                                let has_deeply_nested = nested_fields.iter().any(|f| matches!(
                                    f.source,
                                    crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        nested_struct_name: Some(_),
                                        ..
                                    }
                                ));

                                // For ::FIRST, use .next().unwrap_or(Ok(Default::default()))? instead of .collect()
                                if *is_first {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.map(|{}| {} {{{}\n                    }})).next().unwrap_or(Ok(Default::default()))?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                } else if has_deeply_nested || has_vec_traversal_value {
                                    // Use and_then so the closure can return Result and use ?
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.and_then(|{}| Ok({} {{{}\n                    }}))).collect::<Result<Vec<_>, _>>()?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                } else {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.map(|{}| {} {{{}\n                    }})).collect::<Result<Vec<_>, _>>()?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                }
                            } else {
                                "Vec::new()".to_string()
                            }
                        } else {
                            // Get property name from source if available (for field remapping)
                            let field_info = &struct_def.field_infos[field_idx];
                            let property_name = match &field_info.source {
                                crate::helixc::generator::return_values::ReturnFieldSource::ImplicitField { property_name } => {
                                    property_name.as_ref().map(|s| s.as_str()).unwrap_or(&field.name)
                                },
                                crate::helixc::generator::return_values::ReturnFieldSource::SchemaField { property_name } => {
                                    property_name.as_ref().map(|s| s.as_str()).unwrap_or(&field.name)
                                },
                                _ => &field.name,
                            };

                            // Use property_name to determine access method
                            if property_name == "id" {
                                format!("uuid_str({}.id(), &arena)", singular_var)
                            } else if property_name == "label" {
                                format!("{}.label()", singular_var)
                            } else if property_name == "from_node" {
                                format!("uuid_str({}.from_node(), &arena)", singular_var)
                            } else if property_name == "to_node" {
                                format!("uuid_str({}.to_node(), &arena)", singular_var)
                            } else if property_name == "data" {
                                format!("{}.data()", singular_var)
                            } else if property_name == "score" {
                                format!("{}.score()", singular_var)
                            } else {
                                // Regular schema field - use property_name for get_property
                                format!("{}.get_property(\"{}\")", singular_var, property_name)
                            }
                        };
                        writeln!(f, "        {}: {},", field.name, field_value)?;
                    }

                    // Check if any field is a nested traversal (needs Result handling)
                    let has_nested = struct_def.fields.iter().any(|f| f.is_nested_traversal);
                    if has_nested {
                        write!(f, "    }})).collect::<Result<Vec<_>, GraphError>>()?")
                    } else {
                        write!(f, "    }}).collect::<Vec<_>>()")
                    }?;
                } else {
                    // Single item - direct struct construction
                    // For anonymous traversals, use the source variable directly as the "item"
                    let singular_var = struct_def.source_variable.as_str();

                    writeln!(
                        f,
                        "    \"{}\": {} {{",
                        struct_def.source_variable, struct_def.name
                    )?;

                    for (field_idx, field) in struct_def.fields.iter().enumerate() {
                        let field_value = if field.is_nested_traversal {
                            // Same nested traversal logic as collection case
                            let field_info = &struct_def.field_infos[field_idx];

                            // Handle scalar nested traversals with graph steps (e.g., _::Out<HasCluster>::COUNT)
                            // These require full G::from_iter wrapper even though they return scalar values
                            if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                traversal_code: Some(trav_code),
                                traversal_type,
                                requires_full_traversal: true,
                                nested_struct_name: None,  // Distinguish from nested struct traversals
                                ..
                            } = &field_info.source {
                                let (source_var, is_single_source) = if let Some(trav_type) = traversal_type {
                                    use crate::helixc::generator::traversal_steps::TraversalType;
                                    match trav_type {
                                        TraversalType::FromSingle(var) => {
                                            let v = var.inner();
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), true)
                                        }
                                        TraversalType::FromIter(var) => {
                                            let v = var.inner();
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), false)
                                        }
                                        _ => (singular_var.to_string(), false)
                                    }
                                } else {
                                    (singular_var.to_string(), false)
                                };

                                let iterator_expr = if is_single_source {
                                    format!("std::iter::once({}.clone())", source_var)
                                } else {
                                    format!("{}.iter().cloned()", source_var)
                                };

                                // Check if this is a singular TraversalValue (not Vec)
                                let is_singular_traversal_value = matches!(
                                    &field_info.field_type,
                                    crate::helixc::generator::return_values::ReturnFieldType::Simple(ty) if ty == &RustFieldType::TraversalValue
                                );

                                let is_vec_traversal_value = matches!(
                                    &field_info.field_type,
                                    crate::helixc::generator::return_values::ReturnFieldType::Simple(RustFieldType::Vec(inner)) if inner.as_ref() == &RustFieldType::TraversalValue
                                );

                                // Check if this is a Vec<Value> (from graph navigation returning mapped values)
                                let is_vec_value = matches!(
                                    &field_info.field_type,
                                    crate::helixc::generator::return_values::ReturnFieldType::Simple(RustFieldType::Vec(inner)) if inner.as_ref() == &RustFieldType::Value
                                );

                                // Check if this is an Option<Value> (from ::FIRST)
                                let is_option_value = matches!(
                                    &field_info.field_type,
                                    crate::helixc::generator::return_values::ReturnFieldType::Simple(RustFieldType::OptionValue)
                                );

                                if is_singular_traversal_value {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.collect_to_obj()?", iterator_expr, trav_code)
                                } else if is_option_value {
                                    // For ::FIRST, take the first element from the iterator
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.next().transpose()?", iterator_expr, trav_code)
                                } else if is_vec_traversal_value || is_vec_value {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.collect::<Result<Vec<_>, _>>()?", iterator_expr, trav_code)
                                } else {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}", iterator_expr, trav_code)
                                }
                            // Handle scalar nested traversals with closure parameters (e.g., username: u::{name})
                            // or anonymous traversals (e.g., creatorID: _::In<Created>::ID)
                            } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                closure_source_var: Some(_),
                                accessed_field_name: accessed_field,
                                nested_struct_name: None,
                                ..
                            } = &field_info.source {
                                // Use singular_var which is the actual closure parameter (e.g., "e" from entries::|e|)
                                // This is a scalar field accessing a closure parameter or anonymous variable
                                let field_to_access = accessed_field.as_ref()
                                    .map(|s| s.as_str())
                                    .unwrap_or(field.name.as_str());

                                if field_to_access == "id" || field_to_access == "ID" {
                                    format!("uuid_str({}.id(), &arena)", singular_var)
                                } else if field_to_access == "label" || field_to_access == "Label" {
                                    format!("{}.label()", singular_var)
                                } else {
                                    format!("{}.get_property(\"{}\")", singular_var, field_to_access)
                                }
                            } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                traversal_code: Some(trav_code),
                                nested_struct_name: Some(nested_name),
                                traversal_type,
                                closure_source_var,
                                closure_param_name: _,
                                own_closure_param,
                                is_first,
                                ..
                            } = &field_info.source {
                                let nested_fields = match &field_info.field_type {
                                    crate::helixc::generator::return_values::ReturnFieldType::Nested(fields) => fields,
                                    _ => {
                                        debug_assert!(false, "Type invariant: Nested traversal must have Nested field type");
                                        static EMPTY: Vec<crate::helixc::generator::return_values::ReturnFieldInfo> = Vec::new();
                                        &EMPTY
                                    }
                                };

                                // Extract the actual source variable from the traversal type
                                // Resolve "_" and "val" placeholders to actual source variable
                                let (source_var, is_single_source) = if let Some(trav_type) = traversal_type {
                                    use crate::helixc::generator::traversal_steps::TraversalType;
                                    match trav_type {
                                        TraversalType::FromSingle(var) => {
                                            let v = var.inner();
                                            // Resolve placeholders: both "_" and "val" should use the source variable
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), true)
                                        }
                                        TraversalType::FromIter(var) => {
                                            let v = var.inner();
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), false)
                                        }
                                        _ => {
                                            (struct_def.source_variable.clone(), false)
                                        }
                                    }
                                } else {
                                    (struct_def.source_variable.clone(), false)
                                };

                                // Determine if we need iter().cloned() or std::iter::once()
                                let iterator_expr = if is_single_source {
                                    format!("std::iter::once({}.clone())", source_var)
                                } else {
                                    format!("{}.iter().cloned()", source_var)
                                };

                                // Determine the closure parameter name to use in .map(|param| ...)
                                // Only use own_closure_param (this traversal's closure), not parent context
                                let closure_param = own_closure_param.as_ref()
                                    .map(|s| s.as_str())
                                    .filter(|s| !s.is_empty() && *s != "_" && *s != "val")
                                    .unwrap_or("item");

                                // Check if we're in a closure context
                                let _closure_context_var = closure_source_var.as_ref().map(|s| s.as_str()).unwrap_or(&struct_def.source_variable);

                                let mut nested_field_assigns = String::new();
                                let mut has_vec_traversal_value = false;
                                for nested_field in nested_fields {
                                    // Check if this nested field is itself a nested traversal with a nested struct
                                    let nested_val = if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        traversal_code: Some(inner_trav_code),
                                        nested_struct_name: Some(inner_nested_name),
                                        traversal_type: inner_traversal_type,
                                        ..
                                    } = &nested_field.source {
                                        // This is a deeply nested traversal - generate nested traversal code

                                        // Extract the source variable for this deeply nested traversal
                                        let _inner_source_var = if let Some(inner_trav_type) = inner_traversal_type {
                                            use crate::helixc::generator::traversal_steps::TraversalType;
                                            match inner_trav_type {
                                                TraversalType::FromSingle(var) | TraversalType::FromIter(var) => {
                                                    let v = var.inner();
                                                    // Resolve placeholders: "_" and "val" should use "item" in nested context
                                                    if v == "_" || v == "val" { "item" } else { v }
                                                }
                                                _ => "item"
                                            }
                                        } else {
                                            "item"
                                        };

                                        // Get the nested fields if available
                                        let inner_fields_str = if let crate::helixc::generator::return_values::ReturnFieldType::Nested(inner_fields) = &nested_field.field_type {
                                            // Generate field assignments for the deeply nested struct
                                            let mut inner_assigns = String::new();
                                            for inner_f in inner_fields {
                                                let inner_val = if inner_f.name == "id" {
                                                    "uuid_str(inner_item.id(), &arena)".to_string()
                                                } else if inner_f.name == "label" {
                                                    "inner_item.label()".to_string()
                                                } else {
                                                    format!("inner_item.get_property(\"{}\")", inner_f.name)
                                                };
                                                inner_assigns.push_str(&format!("\n{}: {},", inner_f.name, inner_val));
                                            }
                                            format!(".map(|inner_item| inner_item.map(|inner_item| {} {{{}\n}})).collect::<Result<Vec<_>, _>>()?", inner_nested_name, inner_assigns)
                                        } else {
                                            ".collect::<Vec<_>>()".to_string()
                                        };
                                        format!("G::from_iter(&db, &txn, std::iter::once({}.clone()), &arena){}{}", closure_param, inner_trav_code, inner_fields_str)
                                    } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        traversal_code: Some(inner_trav_code),
                                        traversal_type: inner_traversal_type,
                                        requires_full_traversal: true,
                                        nested_struct_name: None,  // No nested struct = Vec<TraversalValue>
                                        ..
                                    } = &nested_field.source {
                                        // Handle traversals returning Vec<TraversalValue> (no object step)
                                        // e.g., instances: cluster::Out<CreatedInstance>
                                        has_vec_traversal_value = true;

                                        // Extract source variable from traversal type
                                        let (inner_source_var, is_single_source) = if let Some(inner_trav_type) = inner_traversal_type {
                                            use crate::helixc::generator::traversal_steps::TraversalType;
                                            match inner_trav_type {
                                                TraversalType::FromSingle(var) => {
                                                    let v = var.inner();
                                                    let resolved = if v == "_" || v == "val" { closure_param } else { v.as_str() };
                                                    (resolved.to_string(), true)
                                                }
                                                TraversalType::FromIter(var) => {
                                                    let v = var.inner();
                                                    let resolved = if v == "_" || v == "val" { closure_param } else { v.as_str() };
                                                    (resolved.to_string(), false)
                                                }
                                                _ => (closure_param.to_string(), false)
                                            }
                                        } else {
                                            (closure_param.to_string(), false)
                                        };

                                        let inner_iterator_expr = if is_single_source {
                                            format!("std::iter::once({}.clone())", inner_source_var)
                                        } else {
                                            format!("{}.iter().cloned()", inner_source_var)
                                        };

                                        format!("G::from_iter(&db, &txn, {}, &arena){}.collect::<Result<Vec<_>, _>>()?", inner_iterator_expr, inner_trav_code)
                                    } else {
                                        // Check if this field itself is a nested traversal that accesses the closure parameter
                                        // Extract both the access variable and the actual field being accessed
                                        let (access_var, accessed_field_name) = if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                            closure_source_var: Some(source_var),
                                            accessed_field_name: accessed_field,
                                            ..
                                        } = &nested_field.source {
                                            // Case 1: Accessing a closure variable's field (e.g., usr::ID)
                                            let field_to_access = accessed_field.as_ref()
                                                .map(|s| s.as_str())
                                                .unwrap_or(nested_field.name.as_str());
                                            (source_var.as_str(), field_to_access)
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                            closure_source_var: None,
                                            accessed_field_name: Some(accessed_field),
                                            ..
                                        } = &nested_field.source {
                                            // Case 2: NestedTraversal with remapped field name (e.g., postID: id)
                                            (closure_param, accessed_field.as_str())
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::ImplicitField { property_name: Some(prop) } = &nested_field.source {
                                            // Case 3: Implicit field with remapped name (e.g., postID: id)
                                            (closure_param, prop.as_str())
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::SchemaField { property_name: Some(prop) } = &nested_field.source {
                                            // Case 4: Schema field with remapped name (e.g., post: content)
                                            (closure_param, prop.as_str())
                                        } else {
                                            // Case 5: Default - use the field name as-is
                                            (closure_param, nested_field.name.as_str())
                                        };

                                        if accessed_field_name == "id" || accessed_field_name == "ID" {
                                            format!("uuid_str({}.id(), &arena)", access_var)
                                        } else if accessed_field_name == "label" || accessed_field_name == "Label" {
                                            format!("{}.label()", access_var)
                                        } else if accessed_field_name == "from_node" {
                                            format!("uuid_str({}.from_node(), &arena)", access_var)
                                        } else if accessed_field_name == "to_node" {
                                            format!("uuid_str({}.to_node(), &arena)", access_var)
                                        } else {
                                            format!("{}.get_property(\"{}\")", access_var, accessed_field_name)
                                        }
                                    };
                                    nested_field_assigns.push_str(&format!("\n                        {}: {},", nested_field.name, nested_val));
                                }

                                // Check if any nested field is a deeply nested traversal that needs error handling
                                let has_deeply_nested = nested_fields.iter().any(|f| matches!(
                                    f.source,
                                    crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        nested_struct_name: Some(_),
                                        ..
                                    }
                                ));

                                // For ::FIRST, use .next().unwrap_or(Ok(Default::default()))? instead of .collect()
                                if *is_first {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.map(|{}| {} {{{}\n                    }})).next().unwrap_or(Ok(Default::default()))?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                } else if has_deeply_nested || has_vec_traversal_value {
                                    // Use and_then so the closure can return Result and use ?
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.and_then(|{}| Ok({} {{{}\n                    }}))).collect::<Result<Vec<_>, _>>()?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                } else {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.map(|{}| {} {{{}\n                    }})).collect::<Result<Vec<_>, _>>()?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                }
                            } else {
                                "Vec::new()".to_string()
                            }
                        } else {
                            // Get property name from source if available (for field remapping)
                            let field_info = &struct_def.field_infos[field_idx];
                            let property_name = match &field_info.source {
                                crate::helixc::generator::return_values::ReturnFieldSource::ImplicitField { property_name } => {
                                    property_name.as_ref().map(|s| s.as_str()).unwrap_or(&field.name)
                                },
                                crate::helixc::generator::return_values::ReturnFieldSource::SchemaField { property_name } => {
                                    property_name.as_ref().map(|s| s.as_str()).unwrap_or(&field.name)
                                },
                                _ => &field.name,
                            };

                            // Use property_name to determine access method
                            if property_name == "id" {
                                format!("uuid_str({}.id(), &arena)", struct_def.source_variable)
                            } else if property_name == "label" {
                                format!("{}.label()", struct_def.source_variable)
                            } else if property_name == "from_node" {
                                format!(
                                    "uuid_str({}.from_node(), &arena)",
                                    struct_def.source_variable
                                )
                            } else if property_name == "to_node" {
                                format!(
                                    "uuid_str({}.to_node(), &arena)",
                                    struct_def.source_variable
                                )
                            } else if property_name == "data" {
                                format!("{}.data()", struct_def.source_variable)
                            } else if property_name == "score" {
                                format!("{}.score()", struct_def.source_variable)
                            } else {
                                // Regular schema field - use property_name for get_property
                                format!(
                                    "{}.get_property(\"{}\")",
                                    struct_def.source_variable, property_name
                                )
                            }
                        };
                        writeln!(f, "        {}: {},", field.name, field_value)?;
                    }

                    write!(f, "    }}")?;
                }
            }
            writeln!(f)?;
            writeln!(f, "}});")?;
            self.print_txn_commit(f)?;
            writeln!(f, "Ok(input.request.out_fmt.create_response(&response))")?;
        } else if !self.return_values.is_empty() {
            // Legacy json! macro approach
            write!(f, "let response = json!({{")?;
            for (i, (field_name, ret_val)) in self.return_values.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                writeln!(f)?;

                // If this return value has schema fields, extract them into json
                if !ret_val.fields.is_empty() {
                    write!(f, "    \"{}\": json!({{", field_name)?;
                    for (j, field) in ret_val.fields.iter().enumerate() {
                        if j > 0 {
                            write!(f, ",")?;
                        }
                        writeln!(f)?;
                        if field.name == "id" {
                            write!(
                                f,
                                "        \"{}\": uuid_str({}.id(), &arena)",
                                field.name, field_name
                            )?;
                        } else if field.name == "label" {
                            write!(f, "        \"{}\": {}.label()", field.name, field_name)?;
                        } else {
                            write!(
                                f,
                                "        \"{}\": {}.get_property(\"{}\").unwrap()",
                                field.name, field_name, field.name
                            )?;
                        }
                    }
                    writeln!(f)?;
                    write!(f, "    }})")?;
                } else {
                    // For scalar or other types, serialize directly
                    // If there's a literal value, use it directly
                    if let Some(ref lit) = ret_val.literal_value {
                        write!(f, "    \"{}\": {}", field_name, lit)?;
                    } else {
                        write!(f, "    \"{}\": {}", field_name, field_name)?;
                    }
                }
            }
            writeln!(f)?;
            writeln!(f, "}});")?;
            self.print_txn_commit(f)?;
            writeln!(f, "Ok(input.request.out_fmt.create_response(&response))")?;
        } else {
            self.print_txn_commit(f)?;
            writeln!(f, "Ok(input.request.out_fmt.create_response(&()))")?;
        }

        if !self.hoisted_embedding_calls.is_empty() {
            writeln!(f, r#"}}))).await.expect("Cont Channel should be alive")"#)?;
            writeln!(f, "}})))")?;
        }
        writeln!(f, "}}")?;
        Ok(())
    }

    fn print_mcp(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.mcp_handler.is_none() {
            return Ok(());
        }

        let struct_name = format!("{}Input", self.name);
        let mcp_struct_name = format!("{}McpInput", self.name);
        let mcp_function_name = format!("{}Mcp", self.name);

        writeln!(f, "#[derive(Deserialize, Clone)]")?;
        writeln!(f, "pub struct {mcp_struct_name} {{")?;
        writeln!(f, "    connection_id: String,")?;
        if !self.parameters.is_empty() {
            writeln!(f, "    data: {struct_name},")?;
        } else {
            writeln!(f, "    data: serde_json::Value,")?;
        }
        writeln!(f, "}}")?;

        writeln!(f, "#[mcp_handler]")?;
        writeln!(
            f,
            "pub fn {mcp_function_name}(input: &mut MCPToolInput) -> Result<Response, GraphError> {{"
        )?;

        match self.hoisted_embedding_calls.is_empty() {
            true => writeln!(
                f,
                "let data = input.request.in_fmt.deserialize::<{mcp_struct_name}>(&input.request.body)?;"
            )?,
            false => writeln!(
                f,
                "let data = input.request.in_fmt.deserialize::<{mcp_struct_name}>(&input.request.body)?.into_owned();"
            )?,
        }

        writeln!(
            f,
            "let mut connections = input.mcp_connections.lock().map_err(|_| GraphError::Default)?;"
        )?;
        writeln!(
            f,
            "let mut connection = match connections.remove_connection(&data.connection_id) {{"
        )?;
        writeln!(f, "    Some(conn) => conn,")?;
        writeln!(f, "    None => return Err(GraphError::Default),")?;
        writeln!(f, "}};")?;
        writeln!(f, "drop(connections);")?;
        // print the db boilerplate
        writeln!(f, "let db = Arc::clone(&input.mcp_backend.db);")?;
        writeln!(f, "let arena = Bump::new();")?;
        match self.hoisted_embedding_calls.is_empty() {
            true => writeln!(f, "let data = &data.data;")?,
            false => writeln!(f, "let data = data.data;")?,
        }
        writeln!(f, "let connections = Arc::clone(&input.mcp_connections);")?;

        self.print_hoisted_embedding_calls(f)?;
        writeln!(f, "let arena = Bump::new();")?;

        match self.is_mut {
            true => writeln!(
                f,
                "let mut txn = db.graph_env.write_txn().map_err(|e| GraphError::New(format!(\"Failed to start write transaction: {{:?}}\", e)))?;"
            )?,
            false => writeln!(
                f,
                "let txn = db.graph_env.read_txn().map_err(|e| GraphError::New(format!(\"Failed to start read transaction: {{:?}}\", e)))?;"
            )?,
        }

        for statement in &self.statements {
            writeln!(f, "    {statement};")?;
        }

        // Generate return value - same logic as regular handler
        if self.use_struct_returns && !self.return_structs.is_empty() {
            // New struct-based approach - map during response construction
            write!(f, "let response = json!({{")?;
            for (i, struct_def) in self.return_structs.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                writeln!(f)?;

                if struct_def.is_aggregate {
                    // Aggregate/GroupBy - return the enum directly (it already implements Serialize)
                    writeln!(
                        f,
                        "    \"{}\": {}",
                        struct_def.source_variable, struct_def.source_variable
                    )?;
                } else if struct_def.is_collection {
                    // Collection - generate mapping code
                    // Use HQL closure param name if available, otherwise fall back to singular form
                    let singular_var = struct_def
                        .closure_param_name
                        .as_deref()
                        .unwrap_or_else(|| struct_def.source_variable.trim_end_matches('s'));
                    // Check if any field is a nested traversal (needs Result handling)
                    let has_nested = struct_def.fields.iter().any(|f| f.is_nested_traversal);

                    if has_nested {
                        writeln!(
                            f,
                            "    \"{}\": {}.iter().map(|{}| Ok::<_, GraphError>({} {{",
                            struct_def.source_variable,
                            struct_def.source_variable,
                            singular_var,
                            struct_def.name
                        )?;
                    } else {
                        writeln!(
                            f,
                            "    \"{}\": {}.iter().map(|{}| {} {{",
                            struct_def.source_variable,
                            struct_def.source_variable,
                            singular_var,
                            struct_def.name
                        )?;
                    }

                    // Generate field assignments
                    for (field_idx, field) in struct_def.fields.iter().enumerate() {
                        let field_value = if field.is_nested_traversal {
                            // Get the nested traversal info from field_infos
                            let field_info = &struct_def.field_infos[field_idx];

                            // Handle scalar nested traversals with closure parameters (e.g., username: u::{name})
                            // or anonymous traversals (e.g., creatorID: _::In<Created>::ID)
                            if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                closure_source_var: Some(_),
                                accessed_field_name: accessed_field,
                                nested_struct_name: None,
                                ..
                            } = &field_info.source {
                                // Use singular_var which is the actual closure parameter (e.g., "e" from entries::|e|)
                                // This is a scalar field accessing a closure parameter or anonymous variable
                                let field_to_access = accessed_field.as_ref()
                                    .map(|s| s.as_str())
                                    .unwrap_or(field.name.as_str());

                                if field_to_access == "id" || field_to_access == "ID" {
                                    format!("uuid_str({}.id(), &arena)", singular_var)
                                } else if field_to_access == "label" || field_to_access == "Label" {
                                    format!("{}.label()", singular_var)
                                } else {
                                    format!("{}.get_property(\"{}\")", singular_var, field_to_access)
                                }
                            } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                traversal_code: Some(trav_code),
                                nested_struct_name: Some(nested_name),
                                traversal_type,
                                closure_source_var,
                                closure_param_name: _,
                                own_closure_param,
                                is_first,
                                ..
                            } = &field_info.source {
                                // Generate nested traversal code
                                let nested_fields = match &field_info.field_type {
                                    crate::helixc::generator::return_values::ReturnFieldType::Nested(fields) => fields,
                                    _ => {
                                        debug_assert!(false, "Type invariant: Nested traversal must have Nested field type");
                                        static EMPTY: Vec<crate::helixc::generator::return_values::ReturnFieldInfo> = Vec::new();
                                        &EMPTY
                                    }
                                };

                                // Extract the actual source variable from the traversal type
                                // Resolve "_" and "val" placeholders to actual iteration variable
                                let (source_var, is_single_source) = if let Some(trav_type) = traversal_type {
                                    use crate::helixc::generator::traversal_steps::TraversalType;
                                    match trav_type {
                                        TraversalType::FromSingle(var) => {
                                            let v = var.inner();
                                            // Resolve placeholders: both "_" and "val" should use the iteration variable
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), true)
                                        }
                                        TraversalType::FromIter(var) => {
                                            let v = var.inner();
                                            // Resolve placeholders: both "_" and "val" should use the iteration variable
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), false)
                                        }
                                        _ => {
                                            (singular_var.to_string(), false)
                                        }
                                    }
                                } else {
                                    (singular_var.to_string(), false)
                                };

                                // Determine if we need iter().cloned() or std::iter::once()
                                let iterator_expr = if is_single_source {
                                    format!("std::iter::once({}.clone())", source_var)
                                } else {
                                    format!("{}.iter().cloned()", source_var)
                                };

                                // Determine the closure parameter name to use in .map(|param| ...)
                                // Only use own_closure_param (this traversal's closure), not parent context
                                let closure_param = own_closure_param.as_ref()
                                    .map(|s| s.as_str())
                                    .filter(|s| !s.is_empty() && *s != "_" && *s != "val")
                                    .unwrap_or("item");

                                // Generate field assignments for nested struct
                                // Check if we're in a closure context, resolve "_" placeholder
                                let _closure_context_var = closure_source_var.as_ref()
                                    .map(|s| if s == "_" { singular_var } else { s.as_str() })
                                    .unwrap_or(singular_var);

                                let mut nested_field_assigns = String::new();
                                let mut has_vec_traversal_value = false;
                                for nested_field in nested_fields {
                                    // Check if this nested field is itself a nested traversal with a nested struct
                                    let nested_val = if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        traversal_code: Some(inner_trav_code),
                                        nested_struct_name: Some(inner_nested_name),
                                        traversal_type: inner_traversal_type,
                                        ..
                                    } = &nested_field.source {
                                        // This is a deeply nested traversal - generate nested traversal code

                                        // Extract the source variable for this deeply nested traversal
                                        let _inner_source_var = if let Some(inner_trav_type) = inner_traversal_type {
                                            use crate::helixc::generator::traversal_steps::TraversalType;
                                            match inner_trav_type {
                                                TraversalType::FromSingle(var) | TraversalType::FromIter(var) => {
                                                    let v = var.inner();
                                                    // Resolve placeholders: "_" and "val" should use "item" in nested context
                                                    if v == "_" || v == "val" { "item" } else { v }
                                                }
                                                _ => "item"
                                            }
                                        } else {
                                            "item"
                                        };

                                        // Get the nested fields if available
                                        let inner_fields_str = if let crate::helixc::generator::return_values::ReturnFieldType::Nested(inner_fields) = &nested_field.field_type {
                                            // Generate field assignments for the deeply nested struct
                                            let mut inner_assigns = String::new();
                                            for inner_f in inner_fields {
                                                let inner_val = if inner_f.name == "id" {
                                                    "uuid_str(inner_item.id(), &arena)".to_string()
                                                } else if inner_f.name == "label" {
                                                    "inner_item.label()".to_string()
                                                } else {
                                                    format!("inner_item.get_property(\"{}\")", inner_f.name)
                                                };
                                                inner_assigns.push_str(&format!("\n{}: {},", inner_f.name, inner_val));
                                            }
                                            format!(".map(|inner_item| inner_item.map(|inner_item| {} {{{}\n}})).collect::<Result<Vec<_>, _>>()?", inner_nested_name, inner_assigns)
                                        } else {
                                            ".collect::<Vec<_>>()".to_string()
                                        };
                                        format!("G::from_iter(&db, &txn, std::iter::once({}.clone()), &arena){}{}", closure_param, inner_trav_code, inner_fields_str)
                                    } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        traversal_code: Some(inner_trav_code),
                                        traversal_type: inner_traversal_type,
                                        requires_full_traversal: true,
                                        nested_struct_name: None,  // No nested struct = Vec<TraversalValue>
                                        ..
                                    } = &nested_field.source {
                                        // Handle traversals returning Vec<TraversalValue> (no object step)
                                        // e.g., instances: cluster::Out<CreatedInstance>
                                        has_vec_traversal_value = true;

                                        // Extract source variable from traversal type
                                        let (inner_source_var, is_single_source) = if let Some(inner_trav_type) = inner_traversal_type {
                                            use crate::helixc::generator::traversal_steps::TraversalType;
                                            match inner_trav_type {
                                                TraversalType::FromSingle(var) => {
                                                    let v = var.inner();
                                                    let resolved = if v == "_" || v == "val" { closure_param } else { v.as_str() };
                                                    (resolved.to_string(), true)
                                                }
                                                TraversalType::FromIter(var) => {
                                                    let v = var.inner();
                                                    let resolved = if v == "_" || v == "val" { closure_param } else { v.as_str() };
                                                    (resolved.to_string(), false)
                                                }
                                                _ => (closure_param.to_string(), false)
                                            }
                                        } else {
                                            (closure_param.to_string(), false)
                                        };

                                        let inner_iterator_expr = if is_single_source {
                                            format!("std::iter::once({}.clone())", inner_source_var)
                                        } else {
                                            format!("{}.iter().cloned()", inner_source_var)
                                        };

                                        format!("G::from_iter(&db, &txn, {}, &arena){}.collect::<Result<Vec<_>, _>>()?", inner_iterator_expr, inner_trav_code)
                                    } else {
                                        // Check if this field itself is a nested traversal that accesses the closure parameter
                                        // Extract both the access variable and the actual field being accessed
                                        let (access_var, accessed_field_name) = if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                            closure_source_var: Some(source_var),
                                            accessed_field_name: accessed_field,
                                            ..
                                        } = &nested_field.source {
                                            // Case 1: Accessing a closure variable's field (e.g., usr::ID)
                                            let field_to_access = accessed_field.as_ref()
                                                .map(|s| s.as_str())
                                                .unwrap_or(nested_field.name.as_str());
                                            (source_var.as_str(), field_to_access)
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                            closure_source_var: None,
                                            accessed_field_name: Some(accessed_field),
                                            ..
                                        } = &nested_field.source {
                                            // Case 2: NestedTraversal with remapped field name (e.g., postID: id)
                                            (closure_param, accessed_field.as_str())
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::ImplicitField { property_name: Some(prop) } = &nested_field.source {
                                            // Case 3: Implicit field with remapped name (e.g., postID: id)
                                            (closure_param, prop.as_str())
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::SchemaField { property_name: Some(prop) } = &nested_field.source {
                                            // Case 4: Schema field with remapped name (e.g., post: content)
                                            (closure_param, prop.as_str())
                                        } else {
                                            // Case 5: Default - use the field name as-is
                                            (closure_param, nested_field.name.as_str())
                                        };

                                        if accessed_field_name == "id" || accessed_field_name == "ID" {
                                            format!("uuid_str({}.id(), &arena)", access_var)
                                        } else if accessed_field_name == "label" || accessed_field_name == "Label" {
                                            format!("{}.label()", access_var)
                                        } else if accessed_field_name == "from_node" {
                                            format!("uuid_str({}.from_node(), &arena)", access_var)
                                        } else if accessed_field_name == "to_node" {
                                            format!("uuid_str({}.to_node(), &arena)", access_var)
                                        } else {
                                            format!("{}.get_property(\"{}\")", access_var, accessed_field_name)
                                        }
                                    };
                                    nested_field_assigns.push_str(&format!("\n                        {}: {},", nested_field.name, nested_val));
                                }

                                // Check if any nested field is a deeply nested traversal that needs error handling
                                let has_deeply_nested = nested_fields.iter().any(|f| matches!(
                                    f.source,
                                    crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        nested_struct_name: Some(_),
                                        ..
                                    }
                                ));

                                // For ::FIRST, use .next().unwrap_or(Ok(Default::default()))? instead of .collect()
                                if *is_first {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.map(|{}| {} {{{}\n                    }})).next().unwrap_or(Ok(Default::default()))?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                } else if has_deeply_nested || has_vec_traversal_value {
                                    // Use and_then so the closure can return Result and use ?
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.and_then(|{}| Ok({} {{{}\n                    }}))).collect::<Result<Vec<_>, _>>()?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                } else {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.map(|{}| {} {{{}\n                    }})).collect::<Result<Vec<_>, _>>()?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                }
                            } else {
                                "Vec::new()".to_string()
                            }
                        } else {
                            // Get property name from source if available (for field remapping)
                            let field_info = &struct_def.field_infos[field_idx];
                            let property_name = match &field_info.source {
                                crate::helixc::generator::return_values::ReturnFieldSource::ImplicitField { property_name } => {
                                    property_name.as_ref().map(|s| s.as_str()).unwrap_or(&field.name)
                                },
                                crate::helixc::generator::return_values::ReturnFieldSource::SchemaField { property_name } => {
                                    property_name.as_ref().map(|s| s.as_str()).unwrap_or(&field.name)
                                },
                                _ => &field.name,
                            };

                            // Use property_name to determine access method
                            if property_name == "id" {
                                format!("uuid_str({}.id(), &arena)", singular_var)
                            } else if property_name == "label" {
                                format!("{}.label()", singular_var)
                            } else if property_name == "from_node" {
                                format!("uuid_str({}.from_node(), &arena)", singular_var)
                            } else if property_name == "to_node" {
                                format!("uuid_str({}.to_node(), &arena)", singular_var)
                            } else if property_name == "data" {
                                format!("{}.data()", singular_var)
                            } else if property_name == "score" {
                                format!("{}.score()", singular_var)
                            } else {
                                // Regular schema field - use property_name for get_property
                                format!("{}.get_property(\"{}\")", singular_var, property_name)
                            }
                        };
                        writeln!(f, "        {}: {},", field.name, field_value)?;
                    }

                    // Check if any field is a nested traversal (needs Result handling)
                    let has_nested = struct_def.fields.iter().any(|f| f.is_nested_traversal);
                    if has_nested {
                        write!(f, "    }})).collect::<Vec<_>>()")
                    } else {
                        write!(f, "    }}).collect::<Vec<_>>()")
                    }?;
                } else {
                    // Single item - direct struct construction
                    // For anonymous traversals, use the source variable directly as the "item"
                    let singular_var = struct_def.source_variable.as_str();

                    writeln!(
                        f,
                        "    \"{}\": {} {{",
                        struct_def.source_variable, struct_def.name
                    )?;

                    for (field_idx, field) in struct_def.fields.iter().enumerate() {
                        let field_value = if field.is_nested_traversal {
                            // Same nested traversal logic as collection case
                            let field_info = &struct_def.field_infos[field_idx];

                            // Handle scalar nested traversals with closure parameters (e.g., username: u::{name})
                            // or anonymous traversals (e.g., creatorID: _::In<Created>::ID)
                            if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                closure_source_var: Some(_),
                                accessed_field_name: accessed_field,
                                nested_struct_name: None,
                                ..
                            } = &field_info.source {
                                // Use singular_var which is the actual closure parameter (e.g., "e" from entries::|e|)
                                // This is a scalar field accessing a closure parameter or anonymous variable
                                let field_to_access = accessed_field.as_ref()
                                    .map(|s| s.as_str())
                                    .unwrap_or(field.name.as_str());

                                if field_to_access == "id" || field_to_access == "ID" {
                                    format!("uuid_str({}.id(), &arena)", singular_var)
                                } else if field_to_access == "label" || field_to_access == "Label" {
                                    format!("{}.label()", singular_var)
                                } else {
                                    format!("{}.get_property(\"{}\")", singular_var, field_to_access)
                                }
                            } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                traversal_code: Some(trav_code),
                                nested_struct_name: Some(nested_name),
                                traversal_type,
                                closure_source_var,
                                closure_param_name: _,
                                own_closure_param,
                                is_first,
                                ..
                            } = &field_info.source {
                                let nested_fields = match &field_info.field_type {
                                    crate::helixc::generator::return_values::ReturnFieldType::Nested(fields) => fields,
                                    _ => {
                                        debug_assert!(false, "Type invariant: Nested traversal must have Nested field type");
                                        static EMPTY: Vec<crate::helixc::generator::return_values::ReturnFieldInfo> = Vec::new();
                                        &EMPTY
                                    }
                                };

                                // Extract the actual source variable from the traversal type
                                // Resolve "_" and "val" placeholders to actual source variable
                                let (source_var, is_single_source) = if let Some(trav_type) = traversal_type {
                                    use crate::helixc::generator::traversal_steps::TraversalType;
                                    match trav_type {
                                        TraversalType::FromSingle(var) => {
                                            let v = var.inner();
                                            // Resolve placeholders: both "_" and "val" should use the source variable
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), true)
                                        }
                                        TraversalType::FromIter(var) => {
                                            let v = var.inner();
                                            let resolved = if v == "_" || v == "val" { singular_var } else { v.as_str() };
                                            (resolved.to_string(), false)
                                        }
                                        _ => {
                                            (struct_def.source_variable.clone(), false)
                                        }
                                    }
                                } else {
                                    (struct_def.source_variable.clone(), false)
                                };

                                // Determine if we need iter().cloned() or std::iter::once()
                                let iterator_expr = if is_single_source {
                                    format!("std::iter::once({}.clone())", source_var)
                                } else {
                                    format!("{}.iter().cloned()", source_var)
                                };

                                // Determine the closure parameter name to use in .map(|param| ...)
                                // Only use own_closure_param (this traversal's closure), not parent context
                                let closure_param = own_closure_param.as_ref()
                                    .map(|s| s.as_str())
                                    .filter(|s| !s.is_empty() && *s != "_" && *s != "val")
                                    .unwrap_or("item");

                                // Check if we're in a closure context
                                let _closure_context_var = closure_source_var.as_ref().map(|s| s.as_str()).unwrap_or(&struct_def.source_variable);

                                let mut nested_field_assigns = String::new();
                                for nested_field in nested_fields {
                                    // Check if this nested field is itself a nested traversal with a nested struct
                                    let nested_val = if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        traversal_code: Some(inner_trav_code),
                                        nested_struct_name: Some(inner_nested_name),
                                        traversal_type: inner_traversal_type,
                                        ..
                                    } = &nested_field.source {
                                        // This is a deeply nested traversal - generate nested traversal code

                                        // Extract the source variable for this deeply nested traversal
                                        let _inner_source_var = if let Some(inner_trav_type) = inner_traversal_type {
                                            use crate::helixc::generator::traversal_steps::TraversalType;
                                            match inner_trav_type {
                                                TraversalType::FromSingle(var) | TraversalType::FromIter(var) => {
                                                    let v = var.inner();
                                                    // Resolve placeholders: "_" and "val" should use "item" in nested context
                                                    if v == "_" || v == "val" { "item" } else { v }
                                                }
                                                _ => "item"
                                            }
                                        } else {
                                            "item"
                                        };

                                        // Get the nested fields if available
                                        let inner_fields_str = if let crate::helixc::generator::return_values::ReturnFieldType::Nested(inner_fields) = &nested_field.field_type {
                                            // Generate field assignments for the deeply nested struct
                                            let mut inner_assigns = String::new();
                                            for inner_f in inner_fields {
                                                let inner_val = if inner_f.name == "id" {
                                                    "uuid_str(inner_item.id(), &arena)".to_string()
                                                } else if inner_f.name == "label" {
                                                    "inner_item.label()".to_string()
                                                } else {
                                                    format!("inner_item.get_property(\"{}\")", inner_f.name)
                                                };
                                                inner_assigns.push_str(&format!("\n{}: {},", inner_f.name, inner_val));
                                            }
                                            format!(".map(|inner_item| inner_item.map(|inner_item| {} {{{}\n}})).collect::<Result<Vec<_>, _>>()?", inner_nested_name, inner_assigns)
                                        } else {
                                            ".collect::<Result<Vec<_>,_>>()?".to_string()
                                        };
                                        format!("G::from_iter(&db, &txn, std::iter::once({}.clone()), &arena){}{}", closure_param, inner_trav_code, inner_fields_str)
                                    } else {
                                        // Check if this field itself is a nested traversal that accesses the closure parameter
                                        // Extract both the access variable and the actual field being accessed
                                        let (access_var, accessed_field_name) = if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                            closure_source_var: Some(source_var),
                                            accessed_field_name: accessed_field,
                                            ..
                                        } = &nested_field.source {
                                            // Case 1: Accessing a closure variable's field (e.g., usr::ID)
                                            let field_to_access = accessed_field.as_ref()
                                                .map(|s| s.as_str())
                                                .unwrap_or(nested_field.name.as_str());
                                            (source_var.as_str(), field_to_access)
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                            closure_source_var: None,
                                            accessed_field_name: Some(accessed_field),
                                            ..
                                        } = &nested_field.source {
                                            // Case 2: NestedTraversal with remapped field name (e.g., postID: id)
                                            (closure_param, accessed_field.as_str())
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::ImplicitField { property_name: Some(prop) } = &nested_field.source {
                                            // Case 3: Implicit field with remapped name (e.g., postID: id)
                                            (closure_param, prop.as_str())
                                        } else if let crate::helixc::generator::return_values::ReturnFieldSource::SchemaField { property_name: Some(prop) } = &nested_field.source {
                                            // Case 4: Schema field with remapped name (e.g., post: content)
                                            (closure_param, prop.as_str())
                                        } else {
                                            // Case 5: Default - use the field name as-is
                                            (closure_param, nested_field.name.as_str())
                                        };

                                        if accessed_field_name == "id" || accessed_field_name == "ID" {
                                            format!("uuid_str({}.id(), &arena)", access_var)
                                        } else if accessed_field_name == "label" || accessed_field_name == "Label" {
                                            format!("{}.label()", access_var)
                                        } else if accessed_field_name == "from_node" {
                                            format!("uuid_str({}.from_node(), &arena)", access_var)
                                        } else if accessed_field_name == "to_node" {
                                            format!("uuid_str({}.to_node(), &arena)", access_var)
                                        } else {
                                            format!("{}.get_property(\"{}\")", access_var, accessed_field_name)
                                        }
                                    };
                                    nested_field_assigns.push_str(&format!("\n                        {}: {},", nested_field.name, nested_val));
                                }

                                // Check if any nested field is a deeply nested traversal that needs error handling
                                let has_deeply_nested = nested_fields.iter().any(|f| matches!(
                                    f.source,
                                    crate::helixc::generator::return_values::ReturnFieldSource::NestedTraversal {
                                        nested_struct_name: Some(_),
                                        ..
                                    }
                                ));

                                // For ::FIRST, use .next().unwrap_or(Ok(Default::default()))? instead of .collect()
                                if *is_first {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.map(|{}| {} {{{}\n                    }})).next().unwrap_or(Ok(Default::default()))?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                } else if has_deeply_nested {
                                    // Use and_then so the closure can return Result and use ?
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.and_then(|{}| Ok({} {{{}\n                    }}))).collect::<Result<Vec<_>, _>>()?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                } else {
                                    format!("G::from_iter(&db, &txn, {}, &arena){}.map(|{}| {}.map(|{}| {} {{{}\n                    }})).collect::<Result<Vec<_>, _>>()?",
                                        iterator_expr, trav_code, closure_param, closure_param, closure_param, nested_name, nested_field_assigns)
                                }
                            } else {
                                "Vec::new()".to_string()
                            }
                        } else {
                            // Get property name from source if available (for field remapping)
                            let field_info = &struct_def.field_infos[field_idx];
                            let property_name = match &field_info.source {
                                crate::helixc::generator::return_values::ReturnFieldSource::ImplicitField { property_name } => {
                                    property_name.as_ref().map(|s| s.as_str()).unwrap_or(&field.name)
                                },
                                crate::helixc::generator::return_values::ReturnFieldSource::SchemaField { property_name } => {
                                    property_name.as_ref().map(|s| s.as_str()).unwrap_or(&field.name)
                                },
                                _ => &field.name,
                            };

                            // Use property_name to determine access method
                            if property_name == "id" {
                                format!("uuid_str({}.id(), &arena)", struct_def.source_variable)
                            } else if property_name == "label" {
                                format!("{}.label()", struct_def.source_variable)
                            } else if property_name == "from_node" {
                                format!(
                                    "uuid_str({}.from_node(), &arena)",
                                    struct_def.source_variable
                                )
                            } else if property_name == "to_node" {
                                format!(
                                    "uuid_str({}.to_node(), &arena)",
                                    struct_def.source_variable
                                )
                            } else if property_name == "data" {
                                format!("{}.data()", struct_def.source_variable)
                            } else if property_name == "score" {
                                format!("{}.score()", struct_def.source_variable)
                            } else {
                                // Regular schema field - use property_name for get_property
                                format!(
                                    "{}.get_property(\"{}\")",
                                    struct_def.source_variable, property_name
                                )
                            }
                        };
                        writeln!(f, "        {}: {},", field.name, field_value)?;
                    }

                    write!(f, "    }}")?;
                }
            }
            writeln!(f)?;
            writeln!(f, "}});")?;
            self.print_txn_commit(f)?;
            writeln!(f, "let mut connections = connections.lock().unwrap();")?;
            writeln!(f, "connections.add_connection(connection);")?;
            writeln!(f, "drop(connections);")?;
            writeln!(
                f,
                "Ok(helix_db::protocol::format::Format::Json.create_response(&response))"
            )?;
        } else if !self.return_values.is_empty() {
            // Legacy json! macro approach
            write!(f, "let response = json!({{")?;
            for (i, (field_name, ret_val)) in self.return_values.iter().enumerate() {
                if i > 0 {
                    write!(f, ",")?;
                }
                writeln!(f)?;

                // If this return value has schema fields, extract them into json
                if !ret_val.fields.is_empty() {
                    write!(f, "    \"{}\": json!({{", field_name)?;
                    for (j, field) in ret_val.fields.iter().enumerate() {
                        if j > 0 {
                            write!(f, ",")?;
                        }
                        writeln!(f)?;
                        if field.name == "id" {
                            write!(
                                f,
                                "        \"{}\": uuid_str({}.id(), &arena)",
                                field.name, field_name
                            )?;
                        } else if field.name == "label" {
                            write!(f, "        \"{}\": {}.label()", field.name, field_name)?;
                        } else {
                            write!(
                                f,
                                "        \"{}\": {}.get_property(\"{}\").unwrap()",
                                field.name, field_name, field.name
                            )?;
                        }
                    }
                    writeln!(f)?;
                    write!(f, "    }})")?;
                } else {
                    // For scalar or other types, serialize directly
                    // If there's a literal value, use it directly
                    if let Some(ref lit) = ret_val.literal_value {
                        write!(f, "    \"{}\": {}", field_name, lit)?;
                    } else {
                        write!(f, "    \"{}\": {}", field_name, field_name)?;
                    }
                }
            }
            writeln!(f)?;
            writeln!(f, "}});")?;
            self.print_txn_commit(f)?;
            writeln!(f, "let mut connections = connections.lock().unwrap();")?;
            writeln!(f, "connections.add_connection(connection);")?;
            writeln!(f, "drop(connections);")?;
            writeln!(
                f,
                "Ok(helix_db::protocol::format::Format::Json.create_response(&response))"
            )?;
        } else {
            self.print_txn_commit(f)?;
            writeln!(f, "let mut connections = connections.lock().unwrap();")?;
            writeln!(f, "connections.add_connection(connection);")?;
            writeln!(f, "drop(connections);")?;
            writeln!(
                f,
                "Ok(helix_db::protocol::format::Format::Json.create_response(&()))"
            )?;
        }
        if !self.hoisted_embedding_calls.is_empty() {
            writeln!(f, r#"}}))).await.expect("Cont Channel should be alive")"#)?;
            writeln!(f, "}})))")?;
        }
        writeln!(f, "}}")?;
        Ok(())
    }
}

impl Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.print_query(f)?;
        self.print_mcp(f)
    }
}
impl Default for Query {
    fn default() -> Self {
        Self {
            embedding_model_to_use: None,
            mcp_handler: None,
            name: "".to_string(),
            statements: vec![],
            parameters: vec![],
            sub_parameters: vec![],
            return_values: vec![],
            return_structs: vec![],
            use_struct_returns: true, // Enable new struct-based returns
            is_mut: false,
            hoisted_embedding_calls: vec![],
        }
    }
}

impl Query {
    pub fn add_hoisted_embed(&mut self, embed_data: EmbedData) -> String {
        let name = EmbedData::name_from_index(self.hoisted_embedding_calls.len());
        self.hoisted_embedding_calls.push(embed_data);
        name
    }
}

pub struct Parameter {
    pub name: String,
    pub field_type: GeneratedType,
    pub is_optional: bool,
}
impl Display for Parameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.is_optional {
            true => write!(f, "pub {}: Option<{}>", self.name, self.field_type),
            false => write!(f, "pub {}: {}", self.name, self.field_type),
        }
    }
}
