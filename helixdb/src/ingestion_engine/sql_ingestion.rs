use crate::helix_engine::types::GraphError;
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use rusqlite::{
    params, types::Value as RusqliteValue, Connection as SqliteConn, Result as SqliteResult,
};
use std::{
    error::Error,
    fmt,
    fs::File,
    io::Write,
    path::Path,
    collections::{
        HashMap,
        HashSet
    },
};

#[derive(Debug)]
pub enum IngestionError {
    SqliteError(rusqlite::Error),
    GraphError(GraphError),
    MappingError(String),
    HttpError(String),
}

impl fmt::Display for IngestionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IngestionError::SqliteError(e) => write!(f, "{}", e),
            IngestionError::GraphError(e) => write!(f, "{}", e),
            IngestionError::MappingError(e) => write!(f, "{}", e),
            IngestionError::HttpError(e) => write!(f, "{}", e),
        }
    }
}

impl Error for IngestionError {}

impl From<rusqlite::Error> for IngestionError {
    fn from(error: rusqlite::Error) -> Self {
        IngestionError::SqliteError(error)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<RusqliteValue> for Value {
    fn from(value: RusqliteValue) -> Self {
        match value {
            RusqliteValue::Null => Value::Null,
            RusqliteValue::Integer(i) => Value::Integer(i),
            RusqliteValue::Real(f) => Value::Real(f),
            RusqliteValue::Text(s) => Value::Text(s),
            RusqliteValue::Blob(b) => Value::Blob(b),
        }
    }
}

#[derive(Serialize)]
pub struct GraphSchema {
    pub nodes: HashMap<String, Vec<(String, String)>>,
    pub edges: HashMap<String, EdgeSchema>,
}

impl GraphSchema {
    pub fn new() -> Self {
        GraphSchema {
            nodes: HashMap::new(),
            edges: HashMap::new(),
        }
    }
}

#[derive(Serialize)]
pub struct EdgeSchema {
    pub from: String,
    pub to: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Serialize)]
struct NodePayload {
    payload_type: String,
    label: String,
    properties: HashMap<String, Value>,
}

#[derive(Deserialize)]
struct NodeResponse {
    id: u64,
}

#[derive(Serialize)]
struct EdgePayload {
    payload_type: String,
    label: String,
    from: u64,
    to: u64,
    properties: HashMap<String, Value>,
}

#[derive(Debug)]
pub struct TableSchema {
    name: String,
    columns: Vec<ColumnInfo>,
    primary_keys: HashSet<String>,
    foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug)]
pub struct ForeignKey {
    from_table: String,
    from_column: String,
    to_table: String,
    to_column: String,
}

#[derive(Debug)]
pub struct ColumnInfo {
    name: String,
    data_type: String,
    is_primary_key: bool,
}

impl fmt::Display for ForeignKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{} → {}.{}",
            self.from_table, self.from_column, self.to_table, self.to_column
        )
    }
}

impl fmt::Display for ColumnInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pk_indicator = if self.is_primary_key {
            " (Primary Key)"
        } else {
            ""
        };
        write!(f, "{} ({}{})", self.name, self.data_type, pk_indicator)
    }
}

impl fmt::Display for TableSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // table header
        writeln!(f, "Table: {}", self.name)?;
        writeln!(f, "{}", "-".repeat(40))?;

        // columns section
        writeln!(f, "Columns:")?;
        if self.columns.is_empty() {
            writeln!(f, "  None")?;
        } else {
            for (i, column) in self.columns.iter().enumerate() {
                writeln!(f, "  {}. {}", i + 1, column)?;
            }
        }
        writeln!(f)?;

        // primary keys section
        writeln!(f, "Primary Keys:")?;
        if self.primary_keys.is_empty() {
            writeln!(f, "  None")?;
        } else {
            let mut pks: Vec<&String> = self.primary_keys.iter().collect();
            pks.sort(); // Sort for consistent output
            for pk in pks {
                writeln!(f, "  - {}", pk)?;
            }
        }
        writeln!(f)?;

        // foreign keys section
        writeln!(f, "Foreign Keys:")?;
        if self.foreign_keys.is_empty() {
            writeln!(f, "  None")?;
        } else {
            for (i, fk) in self.foreign_keys.iter().enumerate() {
                writeln!(f, "  {}. {}", i + 1, fk)?;
            }
        }
        writeln!(f, "{}", "-".repeat(40))?;

        Ok(())
    }
}

#[derive(Serialize)]
struct GraphData {
    nodes: Vec<NodePayload>,
    edges: Vec<EdgePayload>,
}

pub struct SqliteIngestor {
    pub sqlite_conn: SqliteConn,
    pub instance: String,
    pub batch_size: usize,
    pub id_mappings: HashMap<String, HashMap<String, u64>>,
    pub graph_schema: GraphSchema,
}

impl SqliteIngestor {
    pub fn new(
        sqlite_path: &str,
        instance: Option<String>,
        batch_size: usize,
    ) -> Result<Self, IngestionError> {
        let sqlite_conn = SqliteConn::open(sqlite_path)?;

        Ok(SqliteIngestor {
            sqlite_conn,
            instance: instance.unwrap_or("http://localhost:6969".to_string()),
            batch_size,
            id_mappings: HashMap::new(),
            graph_schema: GraphSchema {
                nodes: HashMap::new(),
                edges: HashMap::new(),
            },
        })
    }

    pub fn extract_schema(&mut self) -> Result<Vec<TableSchema>, IngestionError> {
        let table_names: Vec<String> = self
            .sqlite_conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )?
            .query_map(params![], |row| row.get(0))?
            .collect::<SqliteResult<Vec<String>>>()?;

        table_names
            .into_iter()
            .map(|table_name| {
                let mut col_stmt = self
                    .sqlite_conn
                    .prepare(&format!("PRAGMA table_info({})", table_name))?;

                let (columns, primary_keys): (Vec<ColumnInfo>, HashSet<String>) = col_stmt // statement
                    .query_map(params![], |row| {
                        let name: String = row.get(1)?;
                        let is_pk: i32 = row.get(5)?;
                        Ok((
                            ColumnInfo {
                                name: name.clone(),
                                data_type: row.get(2)?,
                                is_primary_key: is_pk > 0,
                            },
                            if is_pk > 0 { Some(name) } else { None },
                        ))
                    })?
                    .collect::<SqliteResult<Vec<_>>>()?
                    .into_iter()
                    .fold(
                        (Vec::new(), HashSet::new()),
                        |(mut cols, mut pks), (col, pk)| {
                            cols.push(col);
                            if let Some(pk) = pk {
                                pks.insert(pk);
                            }
                            (cols, pks)
                        },
                    );

                let foreign_keys: Vec<ForeignKey> = self
                    .sqlite_conn
                    .prepare(&format!("PRAGMA foreign_key_list({})", table_name))?
                    .query_map(params![], |row| {
                        Ok(ForeignKey {
                            from_table: table_name.clone(),
                            from_column: row.get(3)?,
                            to_table: row.get(2)?,
                            to_column: row.get(4)?,
                        })
                    })?
                    .collect::<SqliteResult<Vec<_>>>()?;

                Ok(TableSchema {
                    name: table_name,
                    columns,
                    primary_keys,
                    foreign_keys,
                })
            })
            .collect::<Result<Vec<TableSchema>, IngestionError>>()
    }

    pub fn ingest_table(&mut self, table_schema: &TableSchema) -> Result<(), IngestionError> {
        let count_query = format!("SELECT COUNT(*) FROM {}", table_schema.name);
        let max_rows: usize = self
            .sqlite_conn
            .query_row(&count_query, params![], |row| row.get(0))
            .map_err(|e| IngestionError::SqliteError(e))?;

        let query = format!("SELECT * FROM {}", table_schema.name);
        let mut stmt = self.sqlite_conn.prepare(&query)?;

        let column_names: Vec<String> = stmt.column_names().into_iter().map(String::from).collect();

        let mut table_id_mapping = HashMap::new();

        let mut row_count = 0;
        let mut rows = stmt.query(params![])?;

        let mut batch_nodes: Vec<(NodePayload, String)> = Vec::new();

        while let Some(row) = rows.next()? {
            let mut properties = HashMap::new();
            let mut primary_key_value = String::new();

            for (i, col_name) in column_names.iter().enumerate() {
                let value: RusqliteValue = row.get(i).map_err(|e| {
                    IngestionError::MappingError(format!(
                        "Failed to get value for column {}: {}",
                        col_name, e
                    ))
                })?;
                properties.insert(col_name.clone(), Value::from(value.clone()));

                // track primary key for creating edges
                if table_schema.primary_keys.contains(col_name) {
                    match value {
                        RusqliteValue::Text(s) => {
                            primary_key_value = s;
                        }
                        RusqliteValue::Integer(i) => {
                            primary_key_value = i.to_string();
                        }
                        _ => {
                            return Err(IngestionError::MappingError(format!(
                                "Unsupported primary key type for column {}",
                                col_name
                            )));
                        }
                    }
                }
            }

            let node = NodePayload {
                payload_type: "node".to_string(),
                label: table_schema.name.clone(),
                properties,
            };

            batch_nodes.push((node, primary_key_value.clone()));

            row_count += 1;

            if row_count % self.batch_size == 0 || row_count == max_rows {
                let node_ids = self.send_node_batch(&batch_nodes, &table_schema.name)?;

                for ((_, pk), node_id) in batch_nodes.iter().zip(node_ids.iter()) {
                    if !pk.is_empty() {
                        table_id_mapping.insert(pk.clone(), *node_id);
                    }
                }

                println!(
                    "Sent batch of {} nodes for table {} (total: {}/{})",
                    batch_nodes.len(),
                    table_schema.name,
                    row_count,
                    max_rows
                );

                batch_nodes.clear();
            }
        }

        self.id_mappings
            .insert(table_schema.name.clone(), table_id_mapping);
        println!(
            "Completed migrating {} rows from table {}",
            row_count, table_schema.name
        );

        Ok(())
    }

    fn send_node_batch(
        &self,
        batch_nodes: &[(NodePayload, String)],
        table_name: &str,
    ) -> Result<Vec<u64>, IngestionError> {
        if batch_nodes.is_empty() {
            return Ok(Vec::new());
        }

        let nodes: Vec<&NodePayload> = batch_nodes.iter().map(|(node, _)| node).collect();
        let url = format!("{}/ingestnodes", self.instance);

        let client = Client::new();
        let response = client.post(&url).json(&nodes).send().map_err(|e| {
            IngestionError::HttpError(format!("Failed to send nodes to {}: {}", url, e))
        })?;

        if !response.status().is_success() {
            return Err(IngestionError::HttpError(format!(
                "Request to {} failed with status: {}",
                url,
                response.status()
            )));
        }

        let node_ids: Vec<NodeResponse> = response.json().map_err(|e| {
            IngestionError::HttpError(format!("Failed to parse node response: {}", e))
        })?;

        if node_ids.len() != batch_nodes.len() {
            return Err(IngestionError::HttpError(format!(
                "Expected {} node IDs for table {}, got {}",
                batch_nodes.len(),
                table_name,
                node_ids.len()
            )));
        }

        Ok(node_ids.into_iter().map(|node| node.id).collect())
    }

    pub fn create_edges(&mut self, schemas: &[TableSchema]) -> Result<(), IngestionError> {
        for schema in schemas {
            for fk in &schema.foreign_keys {
                println!(
                    "Processing FK from {}.{} to {}.{}",
                    fk.from_table, fk.from_column, fk.to_table, fk.to_column
                );

                let query = format!(
                    "SELECT a.{}, a.{} FROM {} a JOIN {} b ON a.{} = b.{}",
                    schema.primary_keys.iter().next().ok_or_else(|| {
                        IngestionError::MappingError(format!(
                            "No primary key found for table {}",
                            schema.name
                        ))
                    })?,
                    fk.from_column, // get foreign key column
                    fk.from_table,
                    fk.to_table,
                    fk.from_column, // join conditions
                    fk.to_column,
                );

                let mut stmt = self.sqlite_conn.prepare(&query)?;
                let mut rows = stmt.query(params![])?;

                let from_mappings = self.id_mappings.get(&fk.from_table).ok_or_else(|| {
                    IngestionError::MappingError(format!(
                        "No ID mappings found for table {}",
                        fk.from_table
                    ))
                })?;

                let to_mappings = self.id_mappings.get(&fk.to_table).ok_or_else(|| {
                    IngestionError::MappingError(format!(
                        "No ID mappings found for table {}",
                        fk.to_table
                    ))
                })?;

                let mut edge_count = 0;
                let mut batch_edges: Vec<EdgePayload> = Vec::new();

                while let Some(row) = rows.next()? {
                    let from_pk: String = row.get(0)?;
                    let to_fk: String = row.get(1)?;

                    if let (Some(&from_node_id), Some(&to_node_id)) =
                        (from_mappings.get(&from_pk), to_mappings.get(&to_fk))
                    {
                        let edge_type = format!(
                            "{}_TO_{}",
                            fk.from_table.to_uppercase(),
                            fk.to_table.to_uppercase()
                        );

                        let edge = EdgePayload {
                            payload_type: "edge".to_string(),
                            label: edge_type,
                            from: from_node_id,
                            to: to_node_id,
                            properties: HashMap::new(), // TODO: might want to support properties
                                                        // on edges other than them just being
                                                        // connections
                        };

                        batch_edges.push(edge);
                        edge_count += 1;

                        if batch_edges.len() >= self.batch_size
                            || (edge_count >= 1 && rows.next()?.is_none())
                        {
                            self.send_edge_batch(&batch_edges, fk)?;

                            println!(
                                "Sent batch of {} edges for FK {}.{} -> {}.{} (total: {})",
                                batch_edges.len(),
                                fk.from_table,
                                fk.from_column,
                                fk.to_table,
                                fk.to_column,
                                edge_count
                            );

                            batch_edges.clear();
                        }
                    }
                }

                // Send any remaining edges
                if !batch_edges.is_empty() {
                    self.send_edge_batch(&batch_edges, fk)?;
                    println!(
                        "Sent final batch of {} edges for FK {}.{} -> {}.{} (total: {})",
                        batch_edges.len(),
                        fk.from_table,
                        fk.from_column,
                        fk.to_table,
                        fk.to_column,
                        edge_count
                    );
                }

                println!(
                    "Created {} edges for relationship {}.{} -> {}.{}",
                    edge_count, fk.from_table, fk.from_column, fk.to_table, fk.to_column
                );
            }
        }

        Ok(())
    }

    fn send_edge_batch(
        &self,
        batch_edges: &[EdgePayload],
        fk: &ForeignKey,
    ) -> Result<(), IngestionError> {
        if batch_edges.is_empty() {
            return Ok(());
        }

        let url = format!("{}/ingestedges", self.instance);

        let client = Client::new();
        let response = client.post(&url).json(&batch_edges).send().map_err(|e| {
            IngestionError::HttpError(format!("Failed to send edges to {}: {}", url, e))
        })?;

        if !response.status().is_success() {
            return Err(IngestionError::HttpError(format!(
                "Request to {} failed with status: {} for FK {}.{} -> {}.{}",
                url,
                response.status(),
                fk.from_table,
                fk.from_column,
                fk.to_table,
                fk.to_column
            )));
        }

        Ok(())
    }

    pub fn dump_to_json(&mut self, output_path: &str) -> Result<(), IngestionError> {
        let schemas = self.extract_schema()?;
        schemas.iter().for_each(|schema| {
            let columns = schema
                .columns
                .iter()
                .filter_map(|column| {
                    let name = to_camel_case(&column.name);
                    // if name contains id in any form, skip it
                    if name.clone().to_lowercase().contains("id") {
                        return None;
                    }
                    Some((name, map_sql_type_to_helix_type(&column.data_type)))
                })
                .collect::<Vec<(String, String)>>();
            self.graph_schema.nodes.insert(schema.name.clone(), columns);

            // add edges to the graph schema
            schema.foreign_keys.iter().for_each(|fk| {
                self.graph_schema.edges.insert(
                    // use ai here to generate edge name based on to and from tables
                    format!(
                        "{}To{}",
                        to_camel_case(&fk.from_table),
                        to_camel_case(&fk.to_table)
                    ),
                    EdgeSchema {
                        from: to_camel_case(&fk.from_table),
                        to: to_camel_case(&fk.to_table),
                        properties: vec![],
                    },
                );
            });
        });

        let mut graph_data = GraphData {
            nodes: Vec::new(),
            edges: Vec::new(),
        };

        // collect all nodes from all tables
        for schema in &schemas {
            let table_nodes = self.collect_table_nodes(schema)?;
            graph_data.nodes.extend(table_nodes);
        }

        // create a mapping from table name and primary key to node index
        let mut node_indices = HashMap::new();
        for (idx, node) in graph_data.nodes.iter().enumerate() {
            let label = &node.label;
            let properties = &node.properties;

            // find the primary key column for this table
            let schema = schemas.iter().find(|s| s.name == *label).ok_or_else(|| {
                IngestionError::MappingError(format!("Schema not found for table {}", label))
            })?;

            let pk = schema.primary_keys.iter().next().ok_or_else(|| {
                IngestionError::MappingError(format!("No primary key found for table {}", label))
            })?;

            // get the primary key value
            if let Some(Value::Text(pk_value)) = properties.get(pk) {
                node_indices.insert((label.clone(), pk_value.clone()), idx);
            } else if let Some(Value::Integer(pk_value)) = properties.get(pk) {
                node_indices.insert((label.clone(), pk_value.to_string()), idx);
            }
        }

        // collect all edges based on foreign keys
        for schema in &schemas {
            for fk in &schema.foreign_keys {
                println!(
                    "Processing FK from {}.{} to {}.{}",
                    fk.from_table, fk.from_column, fk.to_table, fk.to_column
                );

                let query = format!(
                    "SELECT a.{}, a.{} FROM {} a JOIN {} b ON a.{} = b.{}",
                    schema.primary_keys.iter().next().ok_or_else(|| {
                        IngestionError::MappingError(format!(
                            "No primary key found for table {}",
                            schema.name
                        ))
                    })?,
                    fk.from_column, // get foreign key column
                    fk.from_table,
                    fk.to_table,
                    fk.from_column, // join conditions
                    fk.to_column,
                );

                let mut stmt = self.sqlite_conn.prepare(&query)?;
                let mut rows = stmt.query(params![])?;

                while let Some(row) = rows.next()? {
                    // get the primary key value as a string
                    let from_pk: String = match row.get(0)? {
                        RusqliteValue::Integer(i) => i.to_string(),
                        RusqliteValue::Text(s) => s,
                        _ => {
                            return Err(IngestionError::MappingError(format!(
                                "Unsupported primary key type for column {}",
                                fk.from_column
                            )))
                        }
                    };

                    // get the foreign key value as a string
                    let to_fk: String = match row.get(1)? {
                        RusqliteValue::Integer(i) => i.to_string(),
                        RusqliteValue::Text(s) => s,
                        _ => {
                            return Err(IngestionError::MappingError(format!(
                                "Unsupported foreign key type for column {}",
                                fk.from_column
                            )))
                        }
                    };

                    // look up the node indices
                    let from_key = (fk.from_table.clone(), from_pk);
                    let to_key = (fk.to_table.clone(), to_fk);

                    if let (Some(&from_idx), Some(&to_idx)) =
                        (node_indices.get(&from_key), node_indices.get(&to_key))
                    {
                        let edge_type = format!(
                            "{}_TO_{}",
                            fk.from_table.to_uppercase(),
                            fk.to_table.to_uppercase()
                        );

                        let edge = EdgePayload {
                            payload_type: "edge".to_string(),
                            label: edge_type.clone(),
                            from: from_idx as u64,
                            to: to_idx as u64,
                            properties: HashMap::new(),
                        };

                        graph_data.edges.push(edge);
                    }
                }
            }
        }

        // write the data to a JSONL file as lines of json objects
        // let json_data = serde_json::to_string_pretty(&graph_data).map_err(|e| {
        //     IngestionError::MappingError(format!("Failed to serialize graph data: {}", e))
        // })?;

        let path = Path::new(output_path).join("ingestion.jsonl");
        let mut file = File::create(&path).map_err(|e| {
            IngestionError::MappingError(format!("Failed to create output file: {}", e))
        })?;
        println!("Created ingestion file at {}", path.to_str().unwrap());
        for node in &graph_data.nodes {
            let mut json_data = serde_json::to_string(node).map_err(|e| {
                IngestionError::MappingError(format!("Failed to serialize graph data: {}", e))
            })?;
            //append a newline to the json data
            json_data.push_str("\n");
            file.write_all(json_data.as_bytes()).map_err(|e| {
                IngestionError::MappingError(format!("Failed to write to output file: {}", e))
            })?;
        }
        for edge in &graph_data.edges {
            let mut json_data = serde_json::to_string(edge).map_err(|e| {
                IngestionError::MappingError(format!("Failed to serialize graph data: {}", e))
            })?;
            //append a newline to the json data
            json_data.push_str("\n");
            file.write_all(json_data.as_bytes()).map_err(|e| {
                IngestionError::MappingError(format!("Failed to write to output file: {}", e))
            })?;
        }

        println!("Successfully dumped graph data to {}", output_path);
        println!(
            "Total nodes: {}, Total edges: {}",
            graph_data.nodes.len(),
            graph_data.edges.len()
        );

        Ok(())
    }

    fn collect_table_nodes(
        &mut self,
        table_schema: &TableSchema,
    ) -> Result<Vec<NodePayload>, IngestionError> {
        let mut nodes = Vec::new();

        let query = format!("SELECT * FROM {}", table_schema.name);
        let mut stmt = self.sqlite_conn.prepare(&query)?;

        let column_names: Vec<String> = stmt
            .column_names()
            .into_iter()
            .filter_map(|name| {
                let name = String::from(name);
                // if name.clone().to_lowercase().contains("id") {
                //     return None;
                // }
                // Some(to_camel_case(&name))
                Some(name)
            })
            .collect();
        let mut rows = stmt.query(params![])?;

        while let Some(row) = rows.next()? {
            let mut properties = HashMap::new();

            for (i, col_name) in column_names.iter().enumerate() {
                let value: RusqliteValue = row.get(i).map_err(|e| {
                    IngestionError::MappingError(format!(
                        "Failed to get value for column {}: {}",
                        col_name, e
                    ))
                })?;

                // // name contains id in any form, skip it
                // if col_name.clone().to_lowercase().contains("id") {
                //     continue;
                // }
                properties.insert(col_name.clone(), Value::from(value.clone()));
            }

            let node = NodePayload {
                payload_type: "node".to_string(),
                label: table_schema.name.clone(),
                properties,
            };
            nodes.push(node);
        }

        println!(
            "Collected {} nodes from table {}",
            nodes.len(),
            table_schema.name
        );
        Ok(nodes)
    }

    fn create_schemas(&mut self, output_path: &str) -> Result<(), IngestionError> {
        // create file if it doesn't exist
        let mut file = File::create(output_path).map_err(|e| {
            IngestionError::MappingError(format!("Failed to create output file: {}", e))
        })?;

        // go through the graph data and construct the helix schemas
        self.graph_schema
            .nodes
            .iter()
            .for_each(|(label, properties)| {
                let mut str_to_write = format!("N::{} {{\n", to_camel_case(label));
                properties
                    .iter()
                    .enumerate()
                    .for_each(|(i, (property_name, property_type))| {
                        str_to_write.push_str(&format!(
                            "\t{}: {}",
                            to_camel_case(property_name),
                            property_type
                        ));
                        if i != properties.len() - 1 {
                            str_to_write.push_str(",\n");
                        }
                    });
                str_to_write.push_str("\n}\n\n");
                file.write_all(str_to_write.as_bytes()).unwrap();
            });

        // edges section
        self.graph_schema
            .edges
            .iter()
            .for_each(|(edge_type, edge)| {
                let mut str_to_write = format!("E::{} {{\n", edge_type);
                str_to_write.push_str(&format!("\tFrom: {},\n", edge.from));
                str_to_write.push_str(&format!("\tTo: {},\n", edge.to));
                str_to_write.push_str("\tProperties: {\n");
                edge.properties.iter().enumerate().for_each(
                    |(i, (property_name, property_type))| {
                        str_to_write.push_str(&format!(
                            "\t\t{}: {}",
                            to_camel_case(property_name),
                            property_type
                        ));
                        if i != edge.properties.len() - 1 {
                            str_to_write.push_str(",\n");
                        }
                    },
                );
                str_to_write.push_str("\t}\n");
                str_to_write.push_str("\n}\n\n");
                file.write_all(str_to_write.as_bytes()).unwrap();
            });
        Ok(())
    }

    pub fn ingest(&mut self) -> Result<(), IngestionError> {
        self.extract_schema()?;

        // for schema in &schemas {
        //     self.ingest_table(schema)?;
        // }

        // create edges
        // create indexes

        // if --dump flag is set, dump the ingestion stats to a file
        // path = ./helix_ingestion.json
        let path = Path::new("./");
        // create the file if it doesn't exist
        if !path.exists() {
            let mut file = File::create(path).unwrap();
            file.write_all(b"{}").unwrap();
        }
        self.dump_to_json(path.to_str().unwrap())?;

        // create the schema file
        let schema_path = Path::new("./schema.hx");
        println!("Creating schema file at {}", schema_path.to_str().unwrap());
        self.create_schemas(schema_path.to_str().unwrap())?;
        println!(
            "Successfully created schema file at {}",
            schema_path.to_str().unwrap()
        );
        Ok(())
    }
}

pub fn to_camel_case(s: &str) -> String {
    // Handle empty strings
    if s.is_empty() {
        return String::new();
    }

    // Split by any non-alphanumeric character (spaces, underscores, hyphens)
    let parts: Vec<&str> = s.split(|c: char| !c.is_alphanumeric()).collect();

    // Process each part and join them
    parts
        .iter()
        .filter(|part| !part.is_empty()) // Filter out empty parts
        .map(|part| {
            // Handle all uppercase words (like INTEGER)
            if part.chars().all(|c| c.is_uppercase()) {
                // Convert to title case (first letter uppercase, rest lowercase)
                let mut result = String::with_capacity(part.len());
                let mut chars = part.chars();
                if let Some(first) = chars.next() {
                    result.push(first);
                }
                for c in chars {
                    result.push(c.to_lowercase().next().unwrap());
                }
                result
            } else {
                // For mixed case or lowercase words, capitalize the first letter
                let mut result = String::with_capacity(part.len());
                let mut chars = part.chars();
                if let Some(first) = chars.next() {
                    result.push(first.to_uppercase().next().unwrap());
                }
                result.extend(chars);
                result
            }
        })
        .collect::<Vec<String>>()
        .join("")
}

pub fn map_sql_type_to_helix_type(sql_type: &str) -> String {
    let helix_type = match sql_type {
        "INTEGER" => "Integer",
        "INT" => "Integer",
        "FLOAT" => "Float",
        "TEXT" => "String",
        "BOOLEAN" => "Boolean",
        "REAL" => "Float",
        "DATE" => "String",     // TODO: Implement date type
        "TIME" => "String",     // TODO: Implement time type
        "DATETIME" => "String", // TODO: Implement datetime type
        "BLOB" => "String",
        "JSON" => "String",
        "UUID" => "String",
        "URL" => "String",
        _ => {
            panic!("Unsupported type: {}", sql_type);
        }
    };
    helix_type.to_string()
}

#[derive(Serialize, Deserialize)]
pub struct IngestSqlRequest {
    pub job_id: String,
    pub job_name: String,
    pub batch_size: usize,
    pub file_path: String,
}
// / A handler that will automatically get built into
// / the helix container as an endpoint
// /
// / This handler will ingest a SQL(Lite) database into the helix instance
// /
// / The handler will take in a JSON payload with the following fields:
// / - db_url: The URL of the SQL(Lite) database
// / - instance: The instance name of the helix instance
// / - batch_size: The batch size for the ingestion
// /
// / NOTE
// / - This function is simply meant to read from the jsonl data and upload it to the db
// / - It does not handle converting the sql data to helix format
// / - It does not handle uploading or downloading from s3
// /
// / ALSO
// / - The ingest function above does the conversion to helix format
// / - The CLI will do the uploading to s3
// / - The admin server or cli will handle the downloading from s3
// ///
// #[local_handler]
// pub fn ingest_sql(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
//     let data: IngestSqlRequest = match sonic_rs::from_slice(&input.request.body) {
//         Ok(data) => data,
//         Err(err) => return Err(GraphError::from(err)),
//     };

//     let db = Arc::clone(&input.graph.storage);
//     let mut txn = db.graph_env.write_txn().unwrap();

//     // read data from path $DATA_DIR/imports/<job_id>.json
//     // but dont load the json into memory, just read the file
//     // line by line and parse the json objects
//     let path = Path::new(&data.file_path);
//     let file = File::open(path.join("ingestion.jsonl")).unwrap();
//     let reader = BufReader::new(file);
//     let mut tr = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

//     // TODO: need to look at overwriting the id's with the new UUIDs from helix
//     // but keeping all of the connections.
//     // this is because we can't use integers as ids for a graph database.
//     // would have to essentially map all of the generated ids to the old ids for the edges

//     // TODO: could look at using a byte stream to allow for zero copy ingestion
//     reader.lines().for_each(|line| {
//         let line = line.unwrap();
//         let mut data: HashMap<String, ProtocolValue> = sonic_rs::from_str(&line).unwrap();
//         match data.get("payload_type") {
//             Some(ProtocolValue::String(payload_type)) => {
//                 match payload_type.as_str() {
//                     "node" => {
//                         let label = match data.get("label") {
//                             Some(ProtocolValue::String(label)) => label.to_string(),
//                             _ => panic!("error getting value {}", line!()),
//                         };
//                         let properties = match data.remove("properties") {
//                             Some(ProtocolValue::Object(properties)) => properties,
//                             _ => panic!("error getting value {}", line!()),
//                         };
//                         // insert into db without returning the node
//                         let _ = tr.add_v_temp(
//                             &mut txn,
//                             &label,
//                             properties
//                                 .into_iter()
//                                 .filter_map(|(k, v)| {
//                                     if k.to_lowercase().contains("id") {
//                                         None
//                                     } else {
//                                         Some((k, v))
//                                     }
//                                 })
//                                 .collect(),
//                             None,
//                         );
//                     }
//                     "edge" => {
//                         let label = match data.get("label") {
//                             Some(ProtocolValue::String(label)) => label.to_string(),
//                             _ => panic!("error getting value {}", line!()),
//                         };
//                         let properties = match data.remove("properties") {
//                             Some(ProtocolValue::Object(properties)) => properties,
//                             _ => panic!("error getting value {}", line!()),
//                         };
//                         let from = match data.remove("from") {
//                             Some(ProtocolValue::U128(from)) => from,
//                             _ => panic!("error getting value {}", line!()),
//                         };
//                         let to = match data.remove("to") {
//                             Some(ProtocolValue::U128(to)) => to,
//                             _ => panic!("error getting value {}", line!()),
//                         };
//                         // insert into db without returning the edge
//                         let _ = tr.add_e_temp(
//                             &mut txn,
//                             &label,
//                             from,
//                             to,
//                             properties
//                                 .into_iter()
//                                 .filter_map(|(k, v)| {
//                                     if k.to_lowercase().contains("id") {
//                                         None
//                                     } else {
//                                         Some((k, v))
//                                     }
//                                 })
//                                 .collect(),
//                         );
//                     }
//                     _ => panic!("error getting value {}", line!()),
//                 }
//             }
//             _ => panic!("error getting value {}", line!()),
//         }
//     });

//     txn.commit()?;

//     // the function will then need to log the fact the ingestion has been completed

//     Ok(())
// }
