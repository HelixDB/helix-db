use std::fmt::{self, Display};

use crate::helixc::generator::{
    return_values::ReturnValue, statements::Statement, utils::GeneratedType,
};

pub struct Query {
    pub embedding_model_to_use: Option<String>,
    pub mcp_handler: Option<String>,
    pub name: String,
    pub statements: Vec<Statement>,
    pub parameters: Vec<Parameter>, // iterate through and print each one
    pub sub_parameters: Vec<(String, Vec<Parameter>)>,
    pub return_values: Vec<ReturnValue>,
    pub is_mut: bool,
}
impl Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // prints sub parameter structs (e.g. (docs: {doc: String, id: String}))
        for (name, parameters) in &self.sub_parameters {
            writeln!(f, "#[derive(Serialize, Deserialize)]")?;
            writeln!(f, "pub struct {name} {{")?;
            for parameter in parameters {
                writeln!(f, "    pub {}: {},", parameter.name, parameter.field_type)?;
            }
            writeln!(f, "}}")?;
        }
        // prints top level parameters (e.g. (docs: {doc: String, id: String}))
        // if !self.parameters.is_empty() {
        writeln!(f, "#[derive(Serialize, Deserialize)]")?;
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
        // }

        if let Some(mcp_handler) = &self.mcp_handler {
            writeln!(
                f,
                "#[tool_call({}, {})]",
                mcp_handler,
                match self.is_mut {
                    true => "with_write",
                    false => "with_read",
                }
            )?;
        }
        writeln!(
            f,
            "#[handler({})]",
            match self.is_mut {
                true => "with_write",
                false => "with_read",
            }
        )?; // Handler macro

        // prints the function signature
        writeln!(
            f,
            "pub fn {} (input: &HandlerInput) -> Result<Response, GraphError> {{",
            self.name
        )?;
        writeln!(f, "{{")?;

        // prints each statement
        for statement in &self.statements {
            writeln!(f, "    {statement};")?;
        }

        // commit the transaction
        // writeln!(f, "    txn.commit().unwrap();")?;

        // create the return values
        writeln!(
            f,
            "let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();"
        )?;
        if !self.return_values.is_empty() {
            for return_value in &self.return_values {
                writeln!(f, "    {return_value}")?;
            }
        }

        writeln!(f, "}}")?;
        writeln!(f, "}}")
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
            is_mut: false,
        }
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
