//! Semantic analyzer for Helix‑QL.

use colored::Colorize;

use crate::helixc::{
    generator::new::{
        generator_types::{
            Assignment as GeneratedAssignment, Parameter as GeneratedParameter,
            Query as GeneratedQuery, ReturnValue, ReturnValueExpr, Source as GeneratedSource,
            Statement as GeneratedStatement,
        },
        object_remapping_generation::{
            ExcludeField, FieldRemapping, IdentifierRemapping, ObjectRemapping, Remapping,
            RemappingType, TraversalRemapping, ValueRemapping,
        },
        source_steps::{AddE, AddN, AddV, EFromID, EFromType, NFromID, NFromType, SourceStep},
        traversal_steps::{
            In as GeneratedIn, InE as GeneratedInE, Out as GeneratedOut, OutE as GeneratedOutE,
            Step as GeneratedStep, Traversal as GeneratedTraversal, TraversalType,
        },
        utils::{GenRef, GeneratedType, GeneratedValue, RustType as GeneratedRustType, Separator},
    },
    parser::{helix_parser::*, location::Loc},
};

use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
};

use super::{fix::Fix, pretty};

/// A single diagnostic to be surfaced to the editor.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub location: Loc,
    pub message: String,
    pub hint: Option<String>,
    pub filename: Option<String>,
    pub severity: DiagnosticSeverity,
    pub fix: Option<Fix>,
}

#[derive(Debug, Clone)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
    Hint,
    Empty,
}

impl Diagnostic {
    pub fn new(
        location: Loc,
        message: impl Into<String>,
        severity: DiagnosticSeverity,
        hint: Option<String>,
        fix: Option<Fix>,
    ) -> Self {
        Self {
            location,
            message: message.into(),
            hint,
            fix,
            filename: None,
            severity,
        }
    }

    pub fn render(&self, src: &str, filename: &str) -> String {
        pretty::render(self, src, filename)
    }
}

pub fn analyze(src: &Source) -> (Vec<Diagnostic>, GeneratedSource) {
    let mut ctx = Ctx::new(src);
    ctx.check_schema();
    ctx.check_queries();
    (ctx.diagnostics, ctx.output)
}

/// Internal working context shared by all passes.
struct Ctx<'a> {
    src: &'a Source,

    /// Quick look‑ups
    node_set: HashSet<&'a str>,
    vector_set: HashSet<&'a str>,
    edge_map: HashMap<&'a str, &'a EdgeSchema>,
    node_fields: HashMap<&'a str, HashMap<&'a str, &'a FieldType>>,
    edge_fields: HashMap<&'a str, HashMap<&'a str, &'a FieldType>>,
    vector_fields: HashMap<&'a str, HashMap<&'a str, &'a FieldType>>,
    diagnostics: Vec<Diagnostic>,
    output: GeneratedSource,
}

impl<'a> Ctx<'a> {
    fn new(src: &'a Source) -> Self {
        // Build field look‑ups once
        let node_fields = src
            .node_schemas
            .iter()
            .map(|n| {
                (
                    n.name.1.as_str(),
                    n.fields
                        .iter()
                        .map(|f| (f.name.as_str(), &f.field_type))
                        .collect(),
                )
            })
            .collect();

        let edge_fields = src
            .edge_schemas
            .iter()
            .map(|e| {
                (
                    e.name.1.as_str(),
                    e.properties
                        .as_ref()
                        .map(|v| v.iter().map(|f| (f.name.as_str(), &f.field_type)).collect())
                        .unwrap_or_else(HashMap::new),
                )
            })
            .collect();

        let vector_fields = src
            .vector_schemas
            .iter()
            .map(|v| {
                (
                    v.name.as_str(),
                    v.fields
                        .iter()
                        .map(|f| (f.name.as_str(), &f.field_type))
                        .collect(),
                )
            })
            .collect();

        Self {
            node_set: src.node_schemas.iter().map(|n| n.name.1.as_str()).collect(),
            vector_set: src.vector_schemas.iter().map(|v| v.name.as_str()).collect(),
            edge_map: src
                .edge_schemas
                .iter()
                .map(|e| (e.name.1.as_str(), e))
                .collect(),
            node_fields,
            edge_fields,
            vector_fields,
            src,
            diagnostics: Vec::new(),
            output: GeneratedSource::default(),
        }
    }

    // ---------- Pass #1: schema --------------------------
    /// Validate that every edge references declared node types.
    fn check_schema(&mut self) {
        for edge in &self.src.edge_schemas {
            if !self.node_set.contains(edge.from.1.as_str())
                && !self.vector_set.contains(edge.from.1.as_str())
            {
                self.push_schema_err(
                    &edge.from.1,
                    edge.from.0.clone(),
                    format!("`{}` is not a declared node type", edge.from.1),
                    Some(format!("Declare `N::{}` before this edge", edge.from.1)),
                );
            }
            if !self.node_set.contains(edge.to.1.as_str())
                && !self.vector_set.contains(edge.to.1.as_str())
            {
                self.push_schema_err(
                    &edge.to.1,
                    edge.to.0.clone(),
                    format!("`{}` is not a declared node type", edge.to.1),
                    Some(format!("Declare `N::{}` before this edge", edge.to.1)),
                );
            }
            self.output.edges.push(edge.clone().into());
        }
        for node in &self.src.node_schemas {
            self.output.nodes.push(node.clone().into());
        }
        for vector in &self.src.vector_schemas {
            self.output.vectors.push(vector.clone().into());
        }
    }

    // ---------- Pass #2: queries -------------------------
    fn check_queries(&mut self) {
        for q in &self.src.queries {
            self.check_query(q);
        }
    }

    fn check_query(&mut self, q: &'a Query) {
        let mut query = GeneratedQuery::default();
        query.name = q.name.clone();
        // -------------------------------------------------
        // Parameter validation
        // -------------------------------------------------
        for param in &q.parameters {
            if let FieldType::Identifier(ref id) = param.param_type.1 {
                if !self.node_set.contains(id.as_str()) && !self.vector_set.contains(id.as_str()) {
                    self.push_query_err(
                        q,
                        param.param_type.0.clone(),
                        format!("unknown type `{}` for parameter `{}`", id, param.name.1),
                        "declare or use a matching schema object or use a primitive type",
                    );
                }
            }
            // constructs parameters and sub‑parameters for generator
            GeneratedParameter::unwrap_param(
                param.clone(),
                &mut query.parameters,
                &mut query.sub_parameters,
            );
        }

        // -------------------------------------------------
        // Statement‑by‑statement walk
        // -------------------------------------------------
        let mut scope: HashMap<&str, Type<'a>> = HashMap::new();
        for param in &q.parameters {
            scope.insert(param.name.1.as_str(), Type::from(&param.param_type.1));
        }

        use StatementType::*;
        for stmt in &q.statements {
            match &stmt.statement {
                Assignment(assign) => {
                    if scope.contains_key(assign.variable.as_str()) {
                        self.push_query_err(
                            q,
                            assign.loc.clone(),
                            format!("variable `{}` is already declared", assign.variable),
                            "rename the new variable or remove the previous definition",
                        );
                        continue;
                    }

                    let (rhs_ty, stmt) =
                        self.infer_expr_type(&assign.value, &scope, q, None, Some(&mut query));
                    scope.insert(assign.variable.as_str(), rhs_ty);
                    assert!(stmt.is_some(), "Assignment statement should be generated");
                    let assignment = GeneratedStatement::Assignment(GeneratedAssignment {
                        variable: GenRef::Std(assign.variable.clone()),
                        value: Box::new(stmt.unwrap()),
                    });
                    query.statements.push(assignment);
                }

                AddNode(add) => {
                    query.is_mut = true;
                    if let Some(ref ty) = add.node_type {
                        if !self.node_set.contains(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("`AddN<{}>` refers to unknown node type", ty),
                                "declare the node schema first",
                            );
                        }
                    }
                }

                AddEdge(add) => {
                    query.is_mut = true;
                    if let Some(ref ty) = add.edge_type {
                        if !self.edge_map.contains_key(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("`AddE<{}>` refers to unknown edge type", ty),
                                "declare the edge schema first",
                            );
                        }
                    }
                }

                AddVector(add) => {
                    query.is_mut = true;
                    if let Some(ref ty) = add.vector_type {
                        if !self.vector_set.contains(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("vector type `{}` has not been declared", ty),
                                "add a `V::{}` schema first",
                            );
                        }
                    }
                }

                BatchAddVector(add) => {
                    if let Some(ref ty) = add.vector_type {
                        if !self.vector_set.contains(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("vector type `{}` has not been declared", ty),
                                "add a `V::{}` schema first",
                            );
                        }
                    }
                }

                Drop(expr) => {
                    // Nothing special right now; still type‑check sub‑expressions
                    query.is_mut = true;
                    self.infer_expr_type(expr, &scope, q, None, Some(&mut query));
                }

                SearchVector(expr) => {
                    if let Some(ref ty) = expr.vector_type {
                        if !self.vector_set.contains(ty.as_str()) {
                            self.push_query_err(
                                q,
                                expr.loc.clone(),
                                format!("vector type `{}` has not been declared", ty),
                                "add a `V::{}` schema first",
                            );
                        }
                    }
                }

                ForLoop(fl) => {
                    // Ensure the collection exists
                    if !scope.contains_key(fl.in_variable.as_str()) {
                        self.push_query_err(
                            q,
                            fl.loc.clone(),
                            format!("`{}` is not defined in the current scope", fl.in_variable),
                            "add a statement assigning it before the loop",
                        );
                    }
                    // Add loop vars to new child scope and walk the body
                    let mut body_scope = scope.clone();
                    for v in &fl.variables {
                        body_scope.insert(v.as_str(), Type::Unknown);
                    }
                    for body_stmt in &fl.statements {
                        // Recursive walk (but without infinite nesting for now)
                        if let StatementType::Assignment(a) = &body_stmt.statement {
                            let (t, _) = self.infer_expr_type(
                                &a.value,
                                &body_scope,
                                q,
                                None,
                                Some(&mut query),
                            );
                            body_scope.insert(a.variable.as_str(), t);
                        }
                    }
                }

                _ => { /* SearchVector handled above; others TBD */ }
            }
        }

        // -------------------------------------------------
        // Validate RETURN expressions
        // -------------------------------------------------
        if q.return_values.is_empty() {
            let end = q.loc.end.clone();
            self.push_query_warn(
                q,
                Loc::new(end.clone(), end, q.loc.span.clone()),
                "query has no RETURN clause".to_string(),
                "add `RETURN <expr>` at the end",
                None,
            );
        }
        for ret in &q.return_values {
            let (_, stmt) = self.infer_expr_type(ret, &scope, q, None, Some(&mut query));

            assert!(stmt.is_some(), "RETURN value should be a valid expression");
            match stmt.unwrap() {
                GeneratedStatement::Traversal(traversal) => match &traversal.source_step.inner() {
                    SourceStep::Identifier(v) => {
                        query.return_values.push(ReturnValue::new_named(
                            v.clone(),
                            ReturnValueExpr::Traversal(traversal.clone()),
                        ));
                    }
                    _ => {
                        query.return_values.push(ReturnValue::new_unnamed(
                            ReturnValueExpr::Traversal(traversal.clone()),
                        ));
                    }
                },
                GeneratedStatement::Identifier(id) => {
                    query.return_values.push(ReturnValue::new_named(
                        id.clone(),
                        ReturnValueExpr::Identifier(id.clone()),
                    ));
                }
                GeneratedStatement::Literal(l) => {
                    query
                        .return_values
                        .push(ReturnValue::new_literal(l.clone()));
                }
                _ => {
                    self.push_query_err(
                        q,
                        ret.loc.clone(),
                        "RETURN value is not a valid expression".to_string(),
                        "add a valid expression",
                    );
                }
            }
        }
        self.output.queries.push(query);
    }

    // -----------------------------------------------------
    // Helpers
    // -----------------------------------------------------
    fn push_schema_err(&mut self, _ident: &str, loc: Loc, msg: String, hint: Option<String>) {
        self.diagnostics.push(Diagnostic::new(
            loc,
            msg,
            DiagnosticSeverity::Error,
            hint,
            None,
        ));
    }
    fn push_query_err(&mut self, q: &Query, loc: Loc, msg: String, hint: impl Into<String>) {
        self.diagnostics.push(Diagnostic::new(
            loc,
            format!("{} (in QUERY named `{}`)", msg, q.name),
            DiagnosticSeverity::Error,
            Some(hint.into()),
            None,
        ));
    }

    fn push_query_err_with_fix(
        &mut self,
        q: &Query,
        loc: Loc,
        msg: String,
        hint: impl Into<String>,
        fix: Fix,
    ) {
        self.diagnostics.push(Diagnostic::new(
            loc,
            format!("{} (in QUERY named `{}`)", msg, q.name),
            DiagnosticSeverity::Error,
            Some(hint.into()),
            Some(fix),
        ));
    }

    fn push_query_warn(
        &mut self,
        q: &Query,
        loc: Loc,
        msg: String,
        hint: impl Into<String>,
        fix: Option<Fix>,
    ) {
        self.diagnostics.push(Diagnostic::new(
            loc,
            format!("{} (in QUERY named `{}`)", msg, q.name),
            DiagnosticSeverity::Warning,
            Some(hint.into()),
            fix,
        ));
    }

    /// Infer the semantic `Type` of an expression *and* perform all traversal
    ///‑specific validations (property access, field exclusions, step ordering).
    fn infer_expr_type(
        &mut self,
        expression: &'a Expression,
        scope: &HashMap<&'a str, Type<'a>>,
        q: &'a Query,
        parent_ty: Option<Type<'a>>,
        gen_query: Option<&mut GeneratedQuery>,
    ) -> (Type<'a>, Option<GeneratedStatement>) {
        // TODO: Look at returning statement as well or passing mut query to push to
        use ExpressionType::*;
        let expr = &expression.expr;
        match expr {
            Identifier(name) => match scope.get(name.as_str()) {
                Some(t) => (
                    t.clone(),
                    Some(GeneratedStatement::Identifier(GenRef::Ref(name.clone()))),
                ),
                None => {
                    self.push_query_err(
                        q,
                        expression.loc.clone(),
                        format!("variable named `{}` is not in scope", name),
                        "declare it earlier or fix the typo",
                    );
                    (Type::Unknown, None)
                }
            },

            IntegerLiteral(i) => (
                Type::Scalar(FieldType::I32),
                Some(GeneratedStatement::Literal(GenRef::Literal(i.to_string()))),
            ),
            FloatLiteral(f) => (
                Type::Scalar(FieldType::F64),
                Some(GeneratedStatement::Literal(GenRef::Literal(f.to_string()))),
            ),
            StringLiteral(s) => (
                Type::Scalar(FieldType::String),
                Some(GeneratedStatement::Literal(GenRef::Literal(s.to_string()))),
            ),
            BooleanLiteral(b) => (
                Type::Boolean,
                Some(GeneratedStatement::Literal(GenRef::Literal(b.to_string()))),
            ),
            Empty => (Type::Unknown, None),

            Traversal(tr) | Exists(tr) => {
                let mut gen_traversal = GeneratedTraversal::default();
                let final_ty =
                    self.check_traversal(tr, scope, q, parent_ty, &mut gen_traversal, gen_query);
                // push query
                let stmt = GeneratedStatement::Traversal(gen_traversal);

                if matches!(expr, Exists(_)) {
                    (Type::Boolean, Some(stmt))
                } else {
                    (final_ty, Some(stmt))
                }
            }

            AddNode(add) => {
                if let Some(ref ty) = add.node_type {
                    if !self.node_set.contains(ty.as_str()) {
                        self.push_query_err(
                            q,
                            add.loc.clone(),
                            format!("`AddN<{}>` refers to unknown node type", ty),
                            "declare the node schema first",
                        );
                    }
                    let label = GenRef::Literal(ty.clone());
                    // Validate fields if both type and fields are present
                    if let Some(fields) = &add.fields {
                        // Get the field set before validation
                        // TODO: Check field types
                        let field_set = self.node_fields.get(ty.as_str()).cloned();
                        if let Some(field_set) = field_set {
                            for (field_name, _) in fields {
                                if !field_set.contains_key(field_name.as_str()) {
                                    self.push_query_err(
                                        q,
                                        add.loc.clone(),
                                        format!("`{}` is not a field of node `{}`", field_name, ty),
                                        "check the schema field names",
                                    );
                                }
                            }
                        }
                        let properties = fields
                            .iter()
                            .map(|(field_name, value)| {
                                (
                                    field_name.clone(),
                                    match value {
                                        ValueType::Literal(l) => {
                                            GeneratedValue::Literal(GenRef::from(l.clone()))
                                        }
                                        ValueType::Identifier(i) => {
                                            // when doing object field access would need to include object here
                                            GeneratedValue::Identifier(GenRef::Std(format!(
                                                "data.{}",
                                                i.clone()
                                            )))
                                        }
                                        v => {
                                            self.push_query_err(
                                                q,
                                                add.loc.clone(),
                                                format!("`{:?}` is not a valid field value", v),
                                                "use a literal or identifier",
                                            );
                                            GeneratedValue::Unknown
                                        }
                                    },
                                )
                            })
                            .collect();
                        let add_n = AddN {
                            label,
                            properties,
                            secondary_indices: None, // TODO: Add secondary indices by checking against labeled `INDEX` fields in schema
                        };
                        let stmt = GeneratedStatement::Traversal(GeneratedTraversal {
                            source_step: Separator::Period(SourceStep::AddN(add_n)),
                            steps: vec![],
                            traversal_type: TraversalType::Mut,
                        });
                        if let Some(gen_query) = gen_query {
                            gen_query.is_mut = true;
                        }

                        return (Type::Nodes(Some(ty)), Some(stmt));
                    }
                }
                self.push_query_err(
                    q,
                    add.loc.clone(),
                    "`AddN` must have a node type".to_string(),
                    "add a node type",
                );
                (Type::Nodes(None), None)
            }
            AddEdge(add) => {
                if let Some(ref ty) = add.edge_type {
                    if !self.edge_map.contains_key(ty.as_str()) {
                        self.push_query_err(
                            q,
                            add.loc.clone(),
                            format!("`AddE<{}>` refers to unknown edge type", ty),
                            "declare the edge schema first",
                        );
                    }
                    let label = GenRef::Literal(ty.clone());
                    // Validate fields if both type and fields are present
                    if let Some(fields) = &add.fields {
                        // Get the field set before validation
                        let field_set = self.edge_fields.get(ty.as_str()).cloned();
                        if let Some(field_set) = field_set {
                            for (field_name, _) in fields {
                                if !field_set.contains_key(field_name.as_str()) {
                                    self.push_query_err(
                                        q,
                                        add.loc.clone(),
                                        format!("`{}` is not a field of edge `{}`", field_name, ty),
                                        "check the schema field names",
                                    );
                                }
                            }
                        }
                        let properties = fields
                            .iter()
                            .map(|(field_name, value)| {
                                (
                                    field_name.clone(),
                                    match value {
                                        ValueType::Literal(l) => {
                                            GeneratedValue::Literal(GenRef::from(l.clone()))
                                        }
                                        ValueType::Identifier(i) => GeneratedValue::Identifier(
                                            GenRef::Std(format!("data.{}", i.clone())),
                                        ),
                                        v => {
                                            self.push_query_err(
                                                q,
                                                add.loc.clone(),
                                                format!("`{:?}` is not a valid field value", v),
                                                "use a literal or identifier",
                                            );
                                            GeneratedValue::Unknown
                                        }
                                    },
                                )
                            })
                            .collect();

                        let to = match &add.connection.to_id {
                            Some(id) => match id {
                                IdType::Identifier(id) => {
                                    GenRef::Ref(format!("data.{}", id.clone()))
                                }
                                IdType::Literal(id) => GenRef::Literal(id.clone()),
                            },
                            _ => {
                                self.push_query_err(
                                    q,
                                    add.loc.clone(),
                                    "`AddE` must have a to id".to_string(),
                                    "add a to id",
                                );
                                GenRef::Unknown
                            }
                        };
                        let from = match &add.connection.from_id {
                            Some(id) => match id {
                                IdType::Identifier(id) => GenRef::Ref(id.clone()),
                                IdType::Literal(id) => GenRef::Literal(id.clone()),
                            },
                            _ => {
                                self.push_query_err(
                                    q,
                                    add.loc.clone(),
                                    "`AddE` must have a from id".to_string(),
                                    "add a from id",
                                );
                                GenRef::Unknown
                            }
                        };
                        let add_e = AddE {
                            to,
                            from,
                            label,
                            properties,
                            secondary_indices: None, // TODO: Add secondary indices by checking against labeled `INDEX` fields in schema
                        };
                        let stmt = GeneratedStatement::Traversal(GeneratedTraversal {
                            source_step: Separator::Period(SourceStep::AddE(add_e)),
                            steps: vec![],
                            traversal_type: TraversalType::Mut,
                        });
                        if let Some(gen_query) = gen_query {
                            gen_query.is_mut = true;
                        }
                        return (Type::Edges(Some(ty)), Some(stmt));
                    }
                }
                self.push_query_err(
                    q,
                    add.loc.clone(),
                    "`AddE` must have an edge type".to_string(),
                    "add an edge type",
                );
                (Type::Edges(None), None)
            }
            AddVector(add) => {
                if let Some(ref ty) = add.vector_type {
                    if !self.vector_set.contains(ty.as_str()) {
                        self.push_query_err(
                            q,
                            add.loc.clone(),
                            format!("vector type `{}` has not been declared", ty),
                            format!("add a `V::{}` schema first", ty),
                        );
                    }
                    // Validate vector fields
                    if let Some(fields) = &add.fields {
                        let field_set = self.vector_fields.get(ty.as_str()).cloned();
                        if let Some(field_set) = field_set {
                            for (field_name, _) in fields {
                                if !field_set.contains_key(field_name.as_str()) {
                                    self.push_query_err(
                                        q,
                                        add.loc.clone(),
                                        format!(
                                            "`{}` is not a field of vector `{}`",
                                            field_name, ty
                                        ),
                                        "check the schema field names",
                                    );
                                }
                            }
                        }
                        if let Some(vec_data) = &add.data {
                            let label = GenRef::Literal(ty.clone());
                            let properties = fields
                                .iter()
                                .map(|(field_name, value)| {
                                    (
                                        field_name.clone(),
                                        match value {
                                            ValueType::Literal(l) => {
                                                GeneratedValue::Literal(GenRef::from(l.clone()))
                                            }
                                            ValueType::Identifier(i) => GeneratedValue::Identifier(
                                                GenRef::Std(format!("data.{}", i.clone())),
                                            ),
                                            v => {
                                                self.push_query_err(
                                                    q,
                                                    add.loc.clone(),
                                                    format!("`{:?}` is not a valid field value", v),
                                                    "use a literal or identifier",
                                                );
                                                GeneratedValue::Unknown
                                            }
                                        },
                                    )
                                })
                                .collect();
                            let vec = match vec_data {
                                VectorData::Vector(v) => {
                                    GeneratedValue::Literal(GenRef::Ref(format!(
                                        "[{}]",
                                        v.iter()
                                            .map(|f| f.to_string())
                                            .collect::<Vec<String>>()
                                            .join(",")
                                    )))
                                }
                                VectorData::Identifier(i) => {
                                    GeneratedValue::Identifier(GenRef::Ref(i.clone()))
                                }
                            };
                            let add_v = AddV {
                                vec,
                                label,
                                properties,
                            };
                            let stmt = GeneratedStatement::Traversal(GeneratedTraversal {
                                source_step: Separator::Period(SourceStep::AddV(add_v)),
                                steps: vec![],
                                traversal_type: TraversalType::Mut,
                            });
                            if let Some(gen_query) = gen_query {
                                gen_query.is_mut = true;
                            }
                            return (Type::Vector(Some(ty)), Some(stmt));
                        }
                    }
                }
                self.push_query_err(
                    q,
                    add.loc.clone(),
                    "`AddV` must have a vector type".to_string(),
                    "add a vector type",
                );
                (Type::Vector(None), None)
            }
            // BatchAddVector(add) => {
            //     if let Some(ref ty) = add.vector_type {
            //         if !self.vector_set.contains(ty.as_str()) {
            //             self.push_query_err(
            //                 q,
            //                 add.loc.clone(),
            //                 format!("vector type `{}` has not been declared", ty),
            //                 format!("add a `V::{}` schema first", ty),
            //             );
            //         }
            //     }
            //     Type::Vector(add.vector_type.as_deref())
            // }
            // SearchVector(sv) => {
            //     if let Some(ref ty) = sv.vector_type {
            //         if !self.vector_set.contains(ty.as_str()) {
            //             self.push_query_err(
            //                 q,
            //                 sv.loc.clone(),
            //                 format!("vector type `{}` has not been declared", ty),
            //                 format!("add a `V::{}` schema first", ty),
            //             );
            //         }
            //     }
            //     // Search returns nodes that contain the vectors
            //     Type::Nodes(None)
            // }
            // And(v) | Or(v) => {
            //     for e in v {
            //         self.infer_expr_type(e, scope, q, parent_ty.clone());
            //     }
            //     Type::Boolean
            // }
            _ => {
                println!("Unknown expression: {:?}", expr);
                todo!()
            }
        }
    }

    // -----------------------------------------------------
    // Traversal‑specific checks
    // -----------------------------------------------------
    fn check_traversal(
        &mut self,
        tr: &'a Traversal,
        scope: &HashMap<&'a str, Type<'a>>,
        q: &'a Query,
        parent_ty: Option<Type<'a>>,
        gen_traversal: &mut GeneratedTraversal,
        gen_query: Option<&mut GeneratedQuery>,
    ) -> Type<'a> {
        let mut previous_step = None;
        let mut cur_ty = match &tr.start {
            StartNode::Node { node_type, ids } => {
                if !self.node_set.contains(node_type.as_str()) {
                    self.push_query_err(
                        q,
                        tr.loc.clone(),
                        format!("unknown node type `{}`", node_type),
                        format!("declare N::{} in the schema first", node_type),
                    );
                }
                if let Some(ids) = ids {
                    assert!(ids.len() == 1, "multiple ids not supported yet");
                    gen_traversal.source_step = Separator::Period(SourceStep::NFromID(NFromID {
                        id: GenRef::Literal(ids[0].clone()),
                        label: GenRef::Literal(node_type.clone()),
                    }));
                } else {
                    gen_traversal.source_step =
                        Separator::Period(SourceStep::NFromType(NFromType {
                            label: GenRef::Literal(node_type.clone()),
                        }));
                }

                gen_traversal.traversal_type = TraversalType::Ref;
                Type::Nodes(Some(node_type))
            }
            StartNode::Edge { edge_type, ids } => {
                if !self.edge_map.contains_key(edge_type.as_str()) {
                    self.push_query_err(
                        q,
                        tr.loc.clone(),
                        format!("unknown edge type `{}`", edge_type),
                        format!("declare E::{} in the schema first", edge_type),
                    );
                }
                if let Some(ids) = ids {
                    assert!(ids.len() == 1, "multiple ids not supported yet");
                    gen_traversal.source_step = Separator::Period(SourceStep::EFromID(EFromID {
                        id: GenRef::Literal(ids[0].clone()),
                        label: GenRef::Literal(edge_type.clone()),
                    }));
                } else {
                    gen_traversal.source_step =
                        Separator::Period(SourceStep::EFromType(EFromType {
                            label: GenRef::Literal(edge_type.clone()),
                        }));
                }
                gen_traversal.traversal_type = TraversalType::Ref;
                Type::Edges(Some(edge_type))
            }

            StartNode::Identifier(identifier) => {
                scope.get(identifier.as_str()).cloned().map_or_else(
                    || {
                        self.push_query_err(
                            q,
                            tr.loc.clone(),
                            format!("variable named `{}` is not in scope", identifier),
                            format!(
                                "declare {} in the current scope or fix the typo",
                                identifier
                            ),
                        );
                        Type::Unknown
                    },
                    |var_type| {
                        gen_traversal.traversal_type = TraversalType::FromVar;
                        gen_traversal.source_step = Separator::Empty(SourceStep::Identifier(
                            GenRef::Std(identifier.clone()),
                        ));
                        var_type.clone()
                    },
                )
            }
            // anonymous will be the traversal type rather than the start type
            StartNode::Anonymous => parent_ty.unwrap_or(Type::Unknown),
        };

        // Track excluded fields for property validation
        let mut excluded: HashMap<&str, Loc> = HashMap::new();

        // Stream through the steps
        let number_of_steps = match tr.steps.len() {
            0 => 0,
            n => n - 1,
        };

        for (i, graph_step) in tr.steps.iter().enumerate() {
            let step = &graph_step.step;
            match step {
                StepType::Node(gs) | StepType::Edge(gs) => {
                    match self.apply_graph_step(&gs, &cur_ty, q, gen_traversal) {
                        Some(new_ty) => cur_ty = new_ty,
                        None => { /* error already recorded */ }
                    }
                    excluded.clear(); // Traversal to a new element resets exclusions
                }

                StepType::Count => {
                    cur_ty = Type::Scalar(FieldType::I64);
                    excluded.clear();
                    gen_traversal
                        .steps
                        .push(Separator::Period(GeneratedStep::Count));
                }

                StepType::Exclude(ex) => {
                    // checks if exclude is either the last step or the step before an object remapping or closure
                    // i.e. you cant have `N<Type>::!{field1}::Out<Label>`
                    if !(i == number_of_steps
                        || (i != number_of_steps - 1
                            && (!matches!(tr.steps[i + 1].step, StepType::Closure(_))
                                || !matches!(tr.steps[i + 1].step, StepType::Object(_)))))
                    {
                        self.push_query_err(
                            q,
                            ex.loc.clone(),
                            "exclude is only valid as the last step in a traversal, 
                            or as the step before an object remapping or closure"
                                .to_string(),
                            "move exclude steps to the end of the traversal, 
                            or remove the traversal steps following the exclude"
                                .to_string(),
                        );
                    }
                    self.validate_exclude(&cur_ty, tr, ex, &excluded, q);
                    for (_, key) in &ex.fields {
                        excluded.insert(key.as_str(), ex.loc.clone());
                    }
                    gen_traversal
                        .steps
                        .push(Separator::Period(GeneratedStep::Remapping(Remapping {
                            variable_name: "".to_string(),
                            is_inner: false,
                            remappings: vec![RemappingType::ExcludeField(ExcludeField {
                                fields_to_exclude: ex
                                    .fields
                                    .iter()
                                    .map(|(_, field)| field.clone())
                                    .collect(),
                            })],
                        })));
                }

                StepType::Object(obj) => {
                    // TODO: Fix issue with step count being incorrect (i think its counting each field as a step)
                    // if i != number_of_steps {
                    //     println!("{} {}", i, number_of_steps);
                    //     self.push_query_err(
                    //         q,
                    //         obj.loc.clone(),
                    //         "object is only valid as the last step in a traversal".to_string(),
                    //         "move the object to the end of the traversal",
                    //     );
                    // }
                    self.validate_object(&cur_ty, tr, obj, &excluded, q, gen_traversal, None);
                }

                StepType::Where(expr) => {
                    self.infer_expr_type(expr, scope, q, Some(cur_ty.clone()), None);
                    // Where/boolean ops don't change the element type,
                    // so `cur_ty` stays the same.
                }

                StepType::BooleanOperation(b_op) => {
                    let step = previous_step.unwrap();
                    let property_type = match &b_op.op {
                        BooleanOpType::LessThanOrEqual(expr)
                        | BooleanOpType::LessThan(expr)
                        | BooleanOpType::GreaterThanOrEqual(expr)
                        | BooleanOpType::GreaterThan(expr)
                        | BooleanOpType::Equal(expr)
                        | BooleanOpType::NotEqual(expr) => {
                            match self.infer_expr_type(expr, scope, q, Some(cur_ty.clone()), None) {
                                (Type::Scalar(ft), _) => ft.clone(),
                                (field_type, _) => {
                                    self.push_query_err(
                                        q,
                                        b_op.loc.clone(),
                                        format!("boolean operation `{}` cannot be applied to `{}`", b_op.loc.span, field_type.kind_str()),
                                        "make sure the expression evaluates to a number or a string".to_string(),
                                    );
                                    return field_type;
                                }
                            }
                        }
                        _ => return cur_ty.clone(),
                    };

                    // get type of field name
                    let field_name = match step {
                        StepType::Object(obj) => {
                            let fields = obj.fields;
                            assert!(fields.len() == 1);
                            Some(fields[0].value.value.clone())
                        }
                        _ => None,
                    };
                    if let Some(FieldValueType::Identifier(field_name)) = &field_name {
                        match &cur_ty {
                            Type::Nodes(Some(node_ty)) => {
                                let field_set = self.node_fields.get(node_ty).cloned();
                                if let Some(field_set) = field_set {
                                    match field_set.get(field_name.as_str()) {
                                        Some(field) => {
                                            if field != &&property_type {
                                                self.push_query_err(
                                                    q,
                                                    b_op.loc.clone(),
                                                    format!("property `{field_name}` is of type `{field}` (from node type `{node_ty}::{{{field_name}}}`), which does not match type of compared value `{property_type}`"),
                                                    "make sure comparison value is of the same type as the property".to_string(),
                                                );
                                            }
                                        }
                                        None => {
                                            self.push_query_err(
                                                q,
                                                b_op.loc.clone(),
                                                format!(
                                                    "`{}` is not a field of {} `{}`",
                                                    field_name, "node", node_ty
                                                ),
                                                "check the schema field names",
                                            );
                                        }
                                    }
                                }
                            }
                            Type::Edges(Some(edge_ty)) => {
                                let field_set = self.edge_fields.get(edge_ty).cloned();
                                if let Some(field_set) = field_set {
                                    match field_set.get(field_name.as_str()) {
                                        Some(field) => {
                                            if field != &&property_type {
                                                self.push_query_err(
                                                    q,
                                                    b_op.loc.clone(),
                                                    format!("property `{field_name}` is of type `{field}` (from edge type `{edge_ty}::{{{field_name}}}`), which does not match type of compared value `{property_type}`"),
                                                    "make sure comparison value is of the same type as the property".to_string(),
                                                );
                                            }
                                        }
                                        None => {
                                            self.push_query_err(
                                                q,
                                                b_op.loc.clone(),
                                                format!(
                                                    "`{}` is not a field of {} `{}`",
                                                    field_name, "edge", edge_ty
                                                ),
                                                "check the schema field names",
                                            );
                                        }
                                    }
                                }
                            }
                            _ => {
                                self.push_query_err(
                                    q,
                                    b_op.loc.clone(),
                                    "boolean operation can only be applied to scalar values"
                                        .to_string(),
                                    "make sure the expression evaluates to a number or a string"
                                        .to_string(),
                                );
                            }
                        }
                    }

                    // self.infer_expr_type(expr, scope, q);
                    // Where/boolean ops don't change the element type,
                    // so `cur_ty` stays the same.
                }

                StepType::Update(update) => {
                    // Update returns the same type (nodes/edges) it started with.
                    
                    match tr.steps.last() {
                        Some(step) => match &step.step {
                            StepType::Node(gs) => {
                                let node_ty = gs.get_item_type().unwrap();
                                let field_set = self.node_fields.get(node_ty.as_str()).cloned();
                                if let Some(field_set) = field_set {
                                    for FieldAddition { key, value, loc } in &update.fields {
                                        if !field_set.contains_key(key.as_str()) {
                                            self.push_query_err(
                                                q,
                                                loc.clone(),
                                                format!(
                                                    "`{}` is not a field of node `{}`",
                                                    key, node_ty
                                                ),
                                                "check the schema field names",
                                            );
                                        }
                                    }
                                }
                            }

                            StepType::Edge(gs) => {
                                let edge_ty = gs.get_item_type().unwrap();
                                let field_set = self.edge_fields.get(edge_ty.as_str()).cloned();
                                if let Some(field_set) = field_set {
                                    for FieldAddition { key, value, loc } in &update.fields {
                                        if !field_set.contains_key(key.as_str()) {
                                            self.push_query_err(
                                                q,
                                                loc.clone(),
                                                format!(
                                                    "`{}` is not a field of edge `{}`",
                                                    key, edge_ty
                                                ),
                                                "check the schema field names",
                                            );
                                        }
                                    }
                                }
                            }
                            _ => {
                                self.push_query_err(
                                    q,
                                    update.loc.clone(),
                                    "update is only valid on nodes or edges".to_string(),
                                    "update is only valid on nodes or edges".to_string(),
                                );
                                return cur_ty.clone();
                            }
                        },
                        None => match &tr.start {
                            StartNode::Node { node_type, .. } => {
                                let node_ty = node_type.as_str();
                                let field_set = self.node_fields.get(node_ty).cloned();
                                if let Some(field_set) = field_set {
                                    for FieldAddition { key, value, loc } in &update.fields {
                                        if !field_set.contains_key(key.as_str()) {
                                            self.push_query_err(
                                                q,
                                                loc.clone(),
                                                format!(
                                                    "`{}` is not a field of node `{}`",
                                                    key, node_ty
                                                ),
                                                "check the schema field names",
                                            );
                                        }
                                    }
                                }
                            }
                            StartNode::Edge { edge_type, .. } => {
                                let edge_ty = edge_type.as_str();
                                let field_set = self.edge_fields.get(edge_ty).cloned();
                                if let Some(field_set) = field_set {
                                    for FieldAddition { key, value, loc } in &update.fields {
                                        if !field_set.contains_key(key.as_str()) {
                                            self.push_query_err(
                                                q,
                                                loc.clone(),
                                                format!(
                                                    "`{}` is not a field of edge `{}`",
                                                    key, edge_ty
                                                ),
                                                "check the schema field names",
                                            );
                                        }
                                    }
                                }
                            }
                            _ => {
                                self.push_query_err(
                                    q,
                                    update.loc.clone(),
                                    "update is only valid on nodes or edges".to_string(),
                                    "update is only valid on nodes or edges".to_string(),
                                );
                                return cur_ty.clone();
                            }
                        },
                    };

                    excluded.clear();
                }

                StepType::AddEdge(add) => {
                    if let Some(ref ty) = add.edge_type {
                        if !self.edge_map.contains_key(ty.as_str()) {
                            self.push_query_err(
                                q,
                                add.loc.clone(),
                                format!("`AddE<{}>` refers to unknown edge type", ty),
                                "declare the edge schema first",
                            );
                        }
                    }
                    cur_ty = Type::Edges(add.edge_type.as_deref());
                    excluded.clear();
                }

                StepType::Range(_) => { /* doesn't affect type */ }
                StepType::Closure(cl) => {
                    if i != number_of_steps {
                        self.push_query_err(
                            q,
                            cl.loc.clone(),
                            "closure is only valid as the last step in a traversal".to_string(),
                            "move the closure to the end of the traversal",
                        );
                    }
                    // Add identifier to a temporary scope so inner uses pass
                    // let mut tmp_scope = scope.clone();
                    // tmp_scope.insert(cl.identifier.as_str(), cur_ty.clone());
                    // // Walk the object literal
                    // let obj_expr = Expression::Object(Object {
                    //     fields: cl.object.fields.clone(),
                    //     should_spread: cl.object.should_spread,
                    // });
                    // self.infer_expr_type(&obj_expr, &tmp_scope, q);
                    // cur_ty = Type::Unknown;
                }

                StepType::SearchVector(_) => {
                    // SearchV on a traversal returns nodes again
                    cur_ty = Type::Nodes(None);
                    excluded.clear();
                }
            }
            previous_step = Some(step.clone());
        }

        cur_ty
    }

    fn validate_exclude_fields(
        &mut self,
        ex: &Exclude,
        field_set: &HashMap<&str, &FieldType>,
        excluded: &HashMap<&str, Loc>,
        q: &'a Query,
        type_name: &str,
        type_kind: &str,
        span: Option<Loc>,
    ) {
        for (loc, key) in &ex.fields {
            if let Some(loc) = excluded.get(key.as_str()) {
                self.push_query_err_with_fix(
                    q,
                    loc.clone(),
                    format!("field `{}` was previously excluded in this traversal", key),
                    format!("remove the exclusion of `{}`", key),
                    Fix::new(span.clone(), Some(loc.clone()), None),
                );
            } else if !field_set.contains_key(key.as_str()) {
                self.push_query_err(
                    q,
                    loc.clone(),
                    format!("`{}` is not a field of {} `{}`", key, type_kind, type_name),
                    "check the schema field names",
                );
            }
        }
    }

    fn validate_exclude(
        &mut self,
        cur_ty: &Type<'a>,
        tr: &Traversal,
        ex: &Exclude,
        excluded: &HashMap<&str, Loc>,
        q: &'a Query,
    ) {
        match &cur_ty {
            Type::Nodes(Some(node_ty)) => {
                if let Some(field_set) = self.node_fields.get(node_ty).cloned() {
                    self.validate_exclude_fields(
                        ex,
                        &field_set,
                        &excluded,
                        q,
                        node_ty,
                        "node",
                        Some(tr.loc.clone()),
                    );
                }
            }
            Type::Edges(Some(edge_ty)) => {
                // for (key, val) in &obj.fields {
                if let Some(field_set) = self.edge_fields.get(edge_ty).cloned() {
                    self.validate_exclude_fields(
                        ex,
                        &field_set,
                        &excluded,
                        q,
                        edge_ty,
                        "edge",
                        Some(tr.loc.clone()),
                    );
                }
            }
            Type::Vector(Some(vector_ty)) => {
                // Vectors only have 'id' and 'embedding' fields
                if let Some(fields) = self.vector_fields.get(vector_ty).cloned() {
                    self.validate_exclude_fields(
                        ex,
                        &fields,
                        &excluded,
                        q,
                        vector_ty,
                        "vector",
                        Some(tr.loc.clone()),
                    );
                }
            }
            Type::Anonymous(ty) => {
                self.validate_exclude(ty, tr, ex, excluded, q);
            }
            _ => {
                self.push_query_err(
                    q,
                    ex.fields[0].0.clone(),
                    "cannot access properties on this type".to_string(),
                    "exclude is only valid on nodes, edges and vectors",
                );
            }
        }
    }

    fn validate_object(
        &mut self,
        cur_ty: &Type<'a>,
        tr: &Traversal,
        obj: &'a Object,
        excluded: &HashMap<&str, Loc>,
        q: &'a Query,
        gen_traversal: &mut GeneratedTraversal,
        gen_query: Option<&mut GeneratedQuery>,
    ) {
        match &cur_ty {
            Type::Nodes(Some(node_ty)) => {
                if let Some(field_set) = self.node_fields.get(node_ty).cloned() {
                    // if there is only one field then it is a property access
                    if obj.fields.len() == 1 {
                        let field = match &obj.fields[0].value.value {
                            FieldValueType::Identifier(lit) => lit.clone(),
                            field_type => {
                                self.push_query_err(
                                    q,
                                    obj.fields[0].loc.clone(),
                                    format!(
                                        "field access `{:?}` must be a valid field name",
                                        field_type
                                    ),
                                    "check the schema for valid field names",
                                );
                                return;
                            }
                        };
                        gen_traversal
                            .steps
                            .push(Separator::Period(GeneratedStep::PropertyFetch(
                                GenRef::Literal(field),
                            )));
                    } else if obj.fields.len() > 1 {
                        // if there are multiple fields then it is a field remapping
                        // push object remapping where
                        let remapping =
                            self.parse_object_remapping(&obj.fields, q, false, &HashMap::new());
                        gen_traversal
                            .steps
                            .push(Separator::Period(GeneratedStep::Remapping(remapping)));
                    } else {
                        // error
                        self.push_query_err(
                            q,
                            obj.fields[0].value.loc.clone(),
                            "object must have at least one field".to_string(),
                            "object must have at least one field".to_string(),
                        );
                    }

                    self.validate_object_fields(
                        obj,
                        &field_set,
                        &excluded,
                        q,
                        node_ty,
                        "node",
                        Some(tr.loc.clone()),
                    );
                }
            }
            Type::Edges(Some(edge_ty)) => {
                // for (key, val) in &obj.fields {
                if let Some(field_set) = self.edge_fields.get(edge_ty).cloned() {
                    self.validate_object_fields(
                        obj,
                        &field_set,
                        &excluded,
                        q,
                        edge_ty,
                        "edge",
                        Some(tr.loc.clone()),
                    );
                }
            }
            Type::Vector(Some(vector_ty)) => {
                // Vectors only have 'id' and 'embedding' fields
                if let Some(fields) = self.vector_fields.get(vector_ty).cloned() {
                    self.validate_object_fields(
                        obj,
                        &fields,
                        &excluded,
                        q,
                        vector_ty,
                        "vector",
                        Some(tr.loc.clone()),
                    );
                }
            }
            Type::Anonymous(ty) => {
                self.validate_object(ty, tr, obj, excluded, q, gen_traversal, gen_query);
            }
            _ => {
                self.push_query_err(
                    q,
                    obj.fields[0].value.loc.clone(),
                    "cannot access properties on this type".to_string(),
                    "property access is only valid on nodes, edges and vectors",
                );
            }
        }
    }

    /// Check that a graph‑navigation step is allowed for the current element
    /// kind and return the post‑step kind.
    fn apply_graph_step(
        &mut self,
        gs: &'a GraphStep,
        cur_ty: &Type<'a>,
        q: &'a Query,
        traversal: &mut GeneratedTraversal,
    ) -> Option<Type<'a>> {
        use GraphStepType::*;
        match (&gs.step, cur_ty.base()) {
            // Node‑to‑Edge
            (OutE(Some(label)), Type::Nodes(_)) => {
                traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::OutE(GeneratedOutE {
                        label: GenRef::Literal(label.clone()),
                    })));
                Some(Type::Edges(Some(
                    gs.loc
                        .span
                        .trim_matches(|c: char| c == '"' || c.is_whitespace() || c == '\n')
                        .trim_start_matches("OutE<")
                        .trim_end_matches(">"),
                )))
            }
            (InE(Some(label)), Type::Nodes(_)) => {
                traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::InE(GeneratedInE {
                        label: GenRef::Literal(label.clone()),
                    })));
                Some(Type::Edges(Some(
                    gs.loc
                        .span
                        .trim_matches(|c: char| c == '"' || c.is_whitespace() || c == '\n')
                        .trim_start_matches("InE<")
                        .trim_end_matches(">"),
                )))
            }

            // Node‑to‑Node
            (Out(Some(label)), Type::Nodes(_)) => {
                traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::Out(GeneratedOut {
                        label: GenRef::Literal(label.clone()),
                    })));
                Some(Type::Nodes(Some(
                    gs.loc
                        .span
                        .trim_matches(|c: char| c == '"' || c.is_whitespace() || c == '\n')
                        .trim_start_matches("Out<")
                        .trim_start_matches("In<")
                        .trim_end_matches(">"),
                )))
            }
            (In(Some(label)), Type::Nodes(_)) => {
                traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::In(GeneratedIn {
                        label: GenRef::Literal(label.clone()),
                    })));
                Some(Type::Nodes(Some(
                    gs.loc
                        .span
                        .trim_matches(|c: char| c == '"' || c.is_whitespace() || c == '\n')
                        .trim_start_matches("Out<")
                        .trim_start_matches("In<")
                        .trim_end_matches(">"),
                )))
            }

            // Edge‑to‑Node
            (FromN, Type::Edges(_)) => {
                traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::FromN));
                Some(Type::Nodes(Some(gs.loc.span.trim_matches(|c: char| {
                    c == '"' || c.is_whitespace() || c == '\n'
                }))))
            }
            (ToN, Type::Edges(_)) => {
                traversal.steps.push(Separator::Period(GeneratedStep::ToN));
                Some(Type::Nodes(Some(gs.loc.span.trim_matches(|c: char| {
                    c == '"' || c.is_whitespace() || c == '\n'
                }))))
            }

            // Anything else is illegal
            _ => {
                self.push_query_err(
                    q,
                    gs.loc.clone(),
                    format!(
                        "traversal step `{}` cannot follow a step that returns {}",
                        gs.loc
                            .span
                            .trim_matches(|c: char| c == '"' || c.is_whitespace() || c == '\n')
                            .bold(),
                        cur_ty
                            .kind_str()
                            .trim_matches(|c: char| c == '"' || c.is_whitespace() || c == '\n')
                            .bold()
                    ),
                    self.get_traversal_step_hint(cur_ty, &gs.step).as_str(),
                );
                None
            }
        }
    }

    fn get_traversal_step_hint(
        &self,
        current_step: &Type<'a>,
        next_step: &GraphStepType,
    ) -> String {
        match (current_step, next_step) {
            (Type::Nodes(Some(span)), GraphStepType::ToN | GraphStepType::FromN) => {
                format!(
                    "\n{}\n{}",
                    format!(
                        "      • Use `OutE` or `InE` to traverse edges from `{}`",
                        span
                    ),
                    format!(
                        "      • Use `Out` or `In` to traverse nodes from `{}`",
                        span
                    ),
                )
            }
            (Type::Edges(Some(span)), GraphStepType::OutE(_) | GraphStepType::InE(_)) => {
                format!("use `FromN` or `ToN` to traverse nodes from `{}`", span)
            }
            (Type::Edges(Some(span)), GraphStepType::Out(_) | GraphStepType::In(_)) => {
                format!("use `FromN` or `ToN` to traverse nodes from `{}`", span)
            }
            (_, _) => "re-order the traversal or remove the invalid step".to_string(),
        }
    }

    fn validate_object_fields(
        &mut self,
        obj: &Object,
        field_set: &HashMap<&str, &FieldType>,
        excluded: &HashMap<&str, Loc>,
        q: &'a Query,
        type_name: &str,
        type_kind: &str,
        span: Option<Loc>,
    ) {
        for FieldAddition { key, value, .. } in &obj.fields {
            if let Some(loc) = excluded.get(key.as_str()) {
                // for the "::"
                let mut loc = loc.clone();
                loc.end.column += 2;
                loc.span.push_str("::");
                self.push_query_err_with_fix(
                    q,
                    value.loc.clone(),
                    format!("field `{}` was previously excluded in this traversal", key),
                    format!("remove the exclusion of `{}`", key),
                    Fix::new(span.clone(), Some(loc.clone()), Some(String::new())),
                );
            } else if !field_set.contains_key(key.as_str()) {
                self.push_query_err(
                    q,
                    value.loc.clone(),
                    format!("`{}` is not a field of {} `{}`", key, type_kind, type_name),
                    "check the schema field names",
                );
            }
        }
    }

    fn parse_object_remapping(
        &mut self,
        obj: &'a Vec<FieldAddition>,
        q: &'a Query,
        is_inner: bool,
        scope: &HashMap<&'a str, Type<'a>>,
    ) -> Remapping {
        // for each field

        let remappings = obj
            .into_iter()
            .map(|FieldAddition { key, value, .. }| {
                match &value.value {
                    // if the field value is a traversal then it is a TraversalRemapping
                    FieldValueType::Traversal(traversal) => {
                        let mut inner_traversal = GeneratedTraversal::default();
                        self.check_traversal(
                            &traversal,
                            scope,
                            q,
                            None,
                            &mut inner_traversal,
                            None,
                        );
                        RemappingType::TraversalRemapping(TraversalRemapping {
                            variable_name: key.clone(),
                            new_field: key.clone(),
                            new_value: inner_traversal,
                        })
                    }
                    FieldValueType::Expression(expr) => {
                        match &expr.expr {
                            ExpressionType::Traversal(traversal) => {
                                let mut inner_traversal = GeneratedTraversal::default();
                                self.check_traversal(
                                    &traversal,
                                    &HashMap::new(),
                                    q,
                                    None,
                                    &mut inner_traversal,
                                    None,
                                );

                                RemappingType::TraversalRemapping(TraversalRemapping {
                                    variable_name: key.clone(),
                                    new_field: key.clone(),
                                    new_value: inner_traversal,
                                })
                            }
                            ExpressionType::Exists(exists) => {
                                todo!()
                            }
                            ExpressionType::BooleanLiteral(bo_lit) => {
                                RemappingType::ValueRemapping(ValueRemapping {
                                    variable_name: key.clone(),
                                    field_name: key.clone(),
                                    value: GenRef::Literal(bo_lit.to_string()), // TODO: Implement
                                })
                            }
                            ExpressionType::FloatLiteral(float) => {
                                RemappingType::ValueRemapping(ValueRemapping {
                                    variable_name: key.clone(),
                                    field_name: key.clone(),
                                    value: GenRef::Literal(float.to_string()), // TODO: Implement
                                })
                            }
                            ExpressionType::StringLiteral(string) => {
                                RemappingType::ValueRemapping(ValueRemapping {
                                    variable_name: key.clone(),
                                    field_name: key.clone(),
                                    value: GenRef::Literal(string.clone()), // TODO: Implement
                                })
                            }
                            ExpressionType::IntegerLiteral(integer) => {
                                RemappingType::ValueRemapping(ValueRemapping {
                                    variable_name: key.clone(),
                                    field_name: key.clone(),
                                    value: GenRef::Literal(integer.to_string()), // TODO: Implement
                                })
                            }
                            ExpressionType::Identifier(identifier) => {
                                RemappingType::IdentifierRemapping(IdentifierRemapping {
                                    variable_name: key.clone(),
                                    field_name: key.clone(),
                                    identifier_value: identifier.into(), // TODO: Implement
                                })
                            }
                            _ => {
                                self.push_query_err(
                                    q,
                                    expr.loc.clone(),
                                    "invalid expression".to_string(),
                                    "invalid expression".to_string(),
                                );
                                RemappingType::Empty
                            }
                        }
                    }
                    // if field value is identifier then push field remapping
                    FieldValueType::Literal(lit) => {
                        RemappingType::ValueRemapping(ValueRemapping {
                            variable_name: key.clone(),
                            field_name: key.clone(),
                            value: GenRef::from(lit.clone()), // TODO: Implement
                        })
                    }
                    FieldValueType::Identifier(identifier) => {
                        RemappingType::IdentifierRemapping(IdentifierRemapping {
                            variable_name: key.clone(),
                            field_name: key.clone(),
                            identifier_value: identifier.into(), // TODO: Implement
                        })
                    }
                    // if the field value is another object or closure then recurse (sub mapping would go where traversal would go)
                    FieldValueType::Fields(fields) => {
                        let remapping = self.parse_object_remapping(&fields, q, true, scope);
                        RemappingType::ObjectRemapping(ObjectRemapping {
                            variable_name: key.clone(),
                            field_name: key.clone(),
                            remapping,
                        })
                    } // object or closure
                    FieldValueType::Empty => {
                        self.push_query_err(
                            q,
                            obj[0].loc.clone(),
                            "object must have at least one field".to_string(),
                            "object must have at least one field".to_string(),
                        );
                        RemappingType::Empty
                    } // err
                }
                // cast to a remapping type
            })
            .collect();
        Remapping {
            variable_name: "".to_string(),
            is_inner,
            remappings,
        }
    }
}

#[derive(Debug, Clone)]
enum Type<'a> {
    Nodes(Option<&'a str>),
    Edges(Option<&'a str>),
    Vector(Option<&'a str>),
    Scalar(FieldType),
    Anonymous(Box<Type<'a>>),
    Boolean,
    Unknown,
}

impl<'a> Type<'a> {
    fn kind_str(&self) -> &'static str {
        match self {
            Type::Nodes(_) => "nodes",
            Type::Edges(_) => "edges",
            Type::Vector(_) => "vectors",
            Type::Scalar(_) => "scalar",
            Type::Boolean => "boolean",
            Type::Unknown => "unknown",
            Type::Anonymous(ty) => ty.kind_str(),
        }
    }

    /// Recursively strip <code>Anonymous</code> layers and return the base type.
    fn base(&self) -> &Type<'a> {
        match self {
            Type::Anonymous(inner) => inner.base(),
            _ => self,
        }
    }

    /// Same, but returns an owned clone for convenience.
    fn cloned_base(&self) -> Type<'a> {
        match self {
            Type::Anonymous(inner) => inner.cloned_base(),
            _ => self.clone(),
        }
    }
}

impl<'a> From<&'a FieldType> for Type<'a> {
    fn from(ft: &'a FieldType) -> Self {
        use FieldType::*;
        match ft {
            String | Boolean | F32 | F64 | I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128
            | Uuid | Date => Type::Scalar(ft.clone()),
            Array(_) | Object(_) | Identifier(_) => Type::Unknown,
        }
    }
}

// ---------------------------------
// Tests
// ---------------------------------
#[cfg(test)]
mod analyzer_tests {
    use super::*;
    use crate::helixc::parser::helix_parser::HelixParser;

    /// Convenience helper – parse text and return diagnostics.
    fn run(src: &str) -> Vec<Diagnostic> {
        let input = write_to_temp_file(vec![src]);
        let parsed = HelixParser::parse_source(&input)
            .expect("parser should succeed – these tests are for the analyzer");
        analyze(&parsed).0
    }

    #[test]
    fn reports_unknown_node_in_edge() {
        let hx = r#"
            E::Likes {
                From: User,
                To: Post,
                Properties: {}
            }
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("not a declared node type")),
            "expected a diagnostic about undeclared node types, got: {:?}",
            diags
        );
    }

    #[test]
    fn detects_redeclared_variable() {
        let hx = r#"
            N::User { Name: String }

            QUERY dupVar() =>
                u <- N<User>
                u <- N<User>
                RETURN u
        "#;
        let diags = run(hx);
        assert!(
            diags.iter().any(|d| d.message.contains("already declared")),
            "expected a diagnostic about variable redeclaration, got: {:?}",
            diags
        );
    }

    #[test]
    fn flags_invalid_property_access() {
        let hx = r#"
            N::User { name: String }

            QUERY badField() =>
                u <- N<User>
                n <- u::{age}
                RETURN n
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("is not a field of node")),
            "expected a diagnostic about invalid property access, got: {:?}",
            diags
        );
    }

    #[test]
    fn traversal_step_order_enforced() {
        let hx = r#"
            N::User { name: String }

            QUERY wrongStep() =>
                e <- N<User>::FromN   // OutN on nodes is illegal (needs Edge)
                RETURN e
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("cannot follow a step that returns")),
            "expected a diagnostic about illegal step ordering, got: {:?}",
            diags
        );
    }

    #[test]
    fn clean_query_produces_no_diagnostics() {
        let hx = r#"
            N::User { name: String }
            N::Post { title: String }
            E::Wrote {
                From: User,
                To: Post,
                Properties: {}
            }

            QUERY ok(hey: User) =>
                u <- N<User>
                p <- u::Out<Wrote>
                RETURN p::!{title}::{title}
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(
            diags.is_empty(),
            "expected no diagnostics, got: {:?}",
            diags
        );
    }

    #[test]
    fn validates_edge_properties() {
        let hx = r#"
            N::User { name: String }
            N::Post { title: String }
            E::Wrote {
                From: User,
                To: Post,
                Properties: {
                    date: String,
                    likes: I32
                }
            }

            QUERY badEdgeField() =>
                e <- N<User>::OutE<Wrote>
                n <- e::{invalid_field}
                RETURN n
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(false);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("is not a field of edge")),
            "expected a diagnostic about invalid edge property access, got: {:?}",
            diags
        );
    }

    #[test]
    fn validates_node_properties() {
        let hx = r#"
            N::User { name: String }
            N::Post { title: String }

            QUERY badNodeField() =>
                n <- N<User>::{invalid_field}
                RETURN n
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("is not a field of edge")),
            "expected a diagnostic about invalid edge property access, got: {:?}",
            diags
        );
    }

    #[test]
    fn validates_vector_properties() {
        let hx = r#"
            V::UserEmbedding {
                content: String
            }
            
            QUERY badVectorField(vec: [F64], content: String) =>
                v <- AddV<UserEmbedding>(vec,{content: content})
                RETURN v
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(false);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("is not a field of vector")),
            "expected a diagnostic about invalid vector property access, got: {:?}",
            diags
        );
    }

    #[test]
    fn handles_untyped_nodes() {
        let hx = r#"
            N::User { name: String }

            QUERY untypedNode() =>
                u <- N<User>::{some_field}
                RETURN n
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(
            diags.is_empty(),
            "expected no diagnostics for untyped node access, got: {:?}",
            diags
        );
    }

    #[test]
    fn respects_excluded_fields() {
        let hx = r#"
            N::User { name: String, age: I32 }

            QUERY excludedField() =>
                u <- N<User>
                n <- u::!{name}::{name}
                RETURN n
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(false);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("was previously excluded")),
            "expected a diagnostic about accessing excluded field, got: {:?}",
            diags
        );
    }

    #[test]
    fn validates_add_node_fields() {
        let hx = r#"
            N::User { name: String }

            QUERY badAddNodeField() =>
                n <- AddN<User>({invalid_field: "test"})
                RETURN n
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("is not a field of node")),
            "expected a diagnostic about invalid node field, got: {:?}",
            diags
        );
    }

    #[test]
    fn validates_add_edge_fields() {
        let hx = r#"
            N::User { name: String }
            N::Post { title: String }
            E::Wrote {
                From: User,
                To: Post,
                Properties: {
                    date: String
                }
            }

            QUERY badAddEdgeField() =>
                n1 <- AddN<User>({name: "test"})
                n2 <- AddN<Post>({title: "test"})
                e <- AddE<Wrote>({invalid_field: "test"})::To(n1)::From(n2)
                RETURN e
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("is not a field of edge")),
            "expected a diagnostic about invalid edge field, got: {:?}",
            diags
        );
    }

    #[test]
    fn validates_add_vector_fields() {
        let hx = r#"
            V::UserEmbedding {
                content: String
            }
            
            QUERY badAddVectorField() =>
                v <- AddV<UserEmbedding>([1.0, 2.0], {invalid_field: "test"})
                RETURN v
        "#;
        let diags = run(hx);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("is not a valid vector field")),
            "expected a diagnostic about invalid vector field, got: {:?}",
            diags
        );
    }

    #[test]
    fn validate_boolean_comparison() {
        let hx = r#"
            N::User { name: String }

            QUERY booleanComparison() =>
                a <- N<User>::WHERE(_::{name}::EQ(10))
                RETURN a
        "#;
        let diags = run(hx);
        for d in diags.iter() {
            println!("{}", d.render(hx, "query.hx"));
        }
        assert!(false);
        assert!(
            diags.is_empty(),
            "expected no diagnostics, got: {:?}",
            diags
        );
    }
}
