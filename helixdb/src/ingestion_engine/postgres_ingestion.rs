use crate::helix_engine::{
    types::GraphError,
    vector_core::vector::HVector,
};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use rand::Rng;
use reqwest::blocking::Client;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    collections::HashMap,
    fmt,
    fs::File,
    io::Write,
    path::Path,
    str::FromStr,
};
use tokio_postgres::{
    config::SslMode,
    types::Type,
    Client as PgClient,
    Config,
    NoTls,
    Row,
};

#[derive(Debug)]
pub enum IngestionError {
    PostgresError(tokio_postgres::Error),
    GraphError(GraphError),
    MappingError(String),
    HttpError(String),
}

impl fmt::Display for IngestionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IngestionError::PostgresError(e) => write!(f, "{}", e),
            IngestionError::GraphError(e) => write!(f, "{}", e),
            IngestionError::MappingError(e) => write!(f, "{}", e),
            IngestionError::HttpError(e) => write!(f, "{}", e),
        }
    }
}

impl Error for IngestionError {}

impl From<tokio_postgres::Error> for IngestionError {
    fn from(error: tokio_postgres::Error) -> Self {
        IngestionError::PostgresError(error)
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
    Boolean(bool),
    Vector(HVector),
    // Timestamp(chrono::DateTime<chrono::Utc>),
    // Uuid(uuid::Uuid),
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
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_keys: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub from_table: String,
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
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
        let pk_indicator = if self.is_nullable {
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

pub struct PostgresIngestor {
    pub pg_client: PgClient,
    pub instance: String,
    pub batch_size: usize,
    pub id_mappings: HashMap<String, HashMap<String, u64>>,
    pub graph_schema: GraphSchema,
}

impl PostgresIngestor {
    pub async fn new(
        db_url: &str,
        instance: Option<String>,
        batch_size: usize,
        use_ssl: bool,
    ) -> Result<Self, IngestionError> {
        // Parse the connection string and update SSL mode if needed
        let mut config = Config::from_str(db_url).unwrap();
        let client = match use_ssl {
            true => {
                config.ssl_mode(SslMode::Require);
                let (client, connection) = {
                    // Create a TLS connector with native-tls
                    let tls_connector = TlsConnector::builder()
                        .danger_accept_invalid_certs(true) // Only use this for development/testing
                        .build()
                        .map_err(|e| IngestionError::MappingError(format!("TLS error: {}", e)))?;

                    let connector = MakeTlsConnector::new(tls_connector);
                    let tls_config = config.clone();
                    tls_config.connect(connector).await?
                };
                // Connect to the database
                // let (client, connection) = config.connect(tls).await?;

                // Spawn a task to handle the connection
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("PostgreSQL connection error: {}", e);
                    }
                });
                client
            }
            false => {
                config.ssl_mode(SslMode::Disable);
                let (client, connection) = config.connect(NoTls).await?;
                // Spawn a task to handle the connection
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("PostgreSQL connection error: {}", e);
                    }
                });
                client
            }
        };

        // Parse the connection string

        // Set up the connection with or without TLS


        Ok(PostgresIngestor {
            pg_client: client,
            instance: instance.unwrap_or("http://localhost:6969".to_string()),
            batch_size,
            id_mappings: HashMap::new(),
            graph_schema: GraphSchema {
                nodes: HashMap::new(),
                edges: HashMap::new(),
            },
        })
    }

    pub async fn extract_schema(&mut self) -> Result<Vec<TableSchema>, IngestionError> {
        let mut schemas = Vec::new();

        // Get all tables from the public schema
        let query = "
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            AND table_name NOT LIKE 'pg_%'
            AND table_name NOT LIKE 'sql_%'
            ORDER BY table_name";

        let rows = self.pg_client.query(query, &[]).await?;

        for row in rows {
            let table_name: String = row.get(0);
            let mut columns = Vec::new();
            let mut primary_keys = Vec::new();
            let mut foreign_keys = Vec::new();

            // Get column information
            let col_query = "
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = $1
                ORDER BY ordinal_position";

            let col_rows = self.pg_client.query(col_query, &[&table_name]).await?;

            for col_row in col_rows {
                let col_name: String = col_row.get(0);
                let data_type: String = col_row.get(1);
                let is_nullable: String = col_row.get(2);

                columns.push(ColumnInfo {
                    name: col_name.clone(),
                    data_type: map_sql_type_to_helix_type(&data_type),
                    is_nullable: is_nullable == "YES",
                });
            }

            // Get primary key information
            let pk_query = "
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::text::regclass
                AND i.indisprimary";

            let pk_rows = self.pg_client.query(pk_query, &[&table_name]).await?;

            for pk_row in pk_rows {
                let pk_name: String = pk_row.get(0);
                primary_keys.push(pk_name);
            }

            // Get foreign key information
            let fk_query = "
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
                AND tc.table_name = $1";

            let fk_rows = self.pg_client.query(fk_query, &[&table_name]).await?;

            for fk_row in fk_rows {
                let from_column: String = fk_row.get(0);
                let to_table: String = fk_row.get(1);
                let to_column: String = fk_row.get(2);

                let foreign_key = ForeignKey {
                    from_table: table_name.clone(),
                    from_column: from_column,
                    to_table: to_table,
                    to_column: to_column,
                };

                foreign_keys.push(foreign_key);
            }

            schemas.push(TableSchema {
                name: table_name,
                columns,
                primary_keys,
                foreign_keys,
            });
        }

        Ok(schemas)
    }

    pub async fn ingest_table(&mut self, table_schema: &TableSchema) -> Result<(), IngestionError> {
        // Count rows in the table
        let count_query = format!("SELECT COUNT(*) FROM {}", table_schema.name);
        let count_row = self.pg_client.query_one(&count_query, &[]).await?;
        let max_rows: i64 = count_row.get(0);

        // Prepare the query to fetch all rows
        let query = format!("SELECT * FROM {}", table_schema.name);
        let stmt = self.pg_client.prepare(&query).await?;

        // Get column names
        let column_names: Vec<String> = stmt
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let mut table_id_mapping = HashMap::new();
        let mut row_count = 0;
        let mut batch_nodes: Vec<(NodePayload, String)> = Vec::new();

        // Execute the query and process rows in batches
        let rows = self.pg_client.query(&query, &[]).await?;

        for row in rows {
            let mut properties = HashMap::new();
            let mut primary_key_value = String::new();

            for (i, col_name) in column_names.iter().enumerate() {
                let value = self.extract_value_from_row(&row, i)?;
                properties.insert(col_name.clone(), value.clone());

                // Track primary key for creating edges
                if table_schema.primary_keys.iter().any(|pk| pk == col_name) {
                    match &value {
                        Value::Text(s) => {
                            primary_key_value = s.clone();
                        }
                        Value::Integer(i) => {
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

            if row_count % self.batch_size == 0 || row_count == max_rows as usize {
                let node_ids = self
                    .send_node_batch(&batch_nodes, &table_schema.name)
                    .await?;

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

    fn extract_value_from_row(&self, row: &Row, index: usize) -> Result<Value, IngestionError> {
        let col_type = row.columns()[index].type_();

        // Handle date type specifically
        if col_type == &Type::DATE {
            match row.try_get::<_, Option<chrono::NaiveDate>>(index) {
                Ok(Some(date)) => return Ok(Value::Text(date.to_string())),
                Ok(None) => return Ok(Value::Null),
                Err(_) => {
                    // If that fails, try as string
                    let val: Option<String> = row.try_get(index)?;
                    match val {
                        Some(v) => return Ok(Value::Text(v)),
                        None => return Ok(Value::Null),
                    }
                }
            }
        }

        // For timestamp types, we'll use a different approach
        if col_type == &Type::TIMESTAMP || col_type == &Type::TIMESTAMPTZ {
            // Try to get as string first
            match row.try_get::<_, Option<String>>(index) {
                Ok(Some(s)) => return Ok(Value::Text(s)),
                Ok(None) => return Ok(Value::Null),
                Err(_) => {
                    // If that fails, try as NaiveDateTime
                    match row.try_get::<_, Option<chrono::NaiveDateTime>>(index) {
                        Ok(Some(dt)) => return Ok(Value::Text(dt.to_string())),
                        Ok(None) => return Ok(Value::Null),
                        Err(_) => {
                            // If that fails, try as DateTime<Utc>
                            match row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(index) {
                                Ok(Some(dt)) => return Ok(Value::Text(dt.to_rfc3339())),
                                Ok(None) => return Ok(Value::Null),
                                Err(_) => {
                                    // If all else fails, convert to string
                                    let val: Option<String> = row.try_get(index)?;
                                    match val {
                                        Some(v) => return Ok(Value::Text(v)),
                                        None => return Ok(Value::Null),
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if col_type.name() == "vector" {
            let val: Option<String> = row.try_get(index)?;
            match val {
                Some(vector_str) => {
                    // Parse the string representation, e.g., "[1,2,3]"
                    let vec_f64 = Self::parse_vector_string(&vector_str)?;
                    Ok(Value::Vector(HVector::new(vec_f64)))
                }
                None => Ok(Value::Null),
            }
        } else {
            match col_type {
                &Type::INT2 => {
                    let val: Option<i16> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Integer(v as i64)),
                        None => Ok(Value::Null),
                    }
                }
                &Type::INT4 => {
                    let val: Option<i32> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Integer(v as i64)),
                        None => Ok(Value::Null),
                    }
                }
                &Type::INT8 => {
                    let val: Option<i64> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Integer(v)),
                        None => Ok(Value::Null),
                    }
                }
                &Type::FLOAT4 => {
                    let val: Option<f32> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Real(v as f64)),
                        None => Ok(Value::Null),
                    }
                }
                &Type::FLOAT8 => {
                    let val: Option<f64> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Real(v)),
                        None => Ok(Value::Null),
                    }
                }
                &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => {
                    let val: Option<String> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Text(v)),
                        None => Ok(Value::Null),
                    }
                }
                &Type::BOOL => {
                    let val: Option<bool> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Boolean(v)),
                        None => Ok(Value::Null),
                    }
                }
                &Type::UUID => {
                    let val: Option<uuid::Uuid> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Text(v.to_string())),
                        None => Ok(Value::Null),
                    }
                }
                &Type::BYTEA => {
                    let val: Option<Vec<u8>> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Blob(v)),
                        None => Ok(Value::Null),
                    }
                }
                &Type::NUMERIC => {
                    let val: Option<rust_decimal::Decimal> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Real(v.to_f64().unwrap_or(0.0))),
                        None => Ok(Value::Null),
                    }
                }
                _ => {
                    // For unsupported types, convert to string
                    let val: Option<String> = row.try_get(index)?;
                    match val {
                        Some(v) => Ok(Value::Text(v)),
                        None => Ok(Value::Null),
                    }
                }
            }
        }
    }

    async fn send_node_batch(
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

    pub async fn create_edges(&mut self, schemas: &[TableSchema]) -> Result<(), IngestionError> {
        for schema in schemas {
            for fk in &schema.foreign_keys {
                println!(
                    "Processing FK from {}.{} to {}.{}",
                    fk.from_table, fk.from_column, fk.to_table, fk.to_column
                );

                // Get the primary key column for the from_table
                let from_pk = schema.primary_keys.iter().next().ok_or_else(|| {
                    IngestionError::MappingError(format!(
                        "No primary key found for table {}",
                        schema.name
                    ))
                })?;

                let query = format!(
                    "SELECT a.{}, a.{} FROM {} a JOIN {} b ON a.{} = b.{}",
                    from_pk,
                    fk.from_column, // get foreign key column
                    fk.from_table,
                    fk.to_table,
                    fk.from_column, // join conditions
                    fk.to_column,
                );

                let rows = self.pg_client.query(&query, &[]).await?;

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

                for row in rows {
                    // Get the primary key value as a string
                    let from_pk_value: String = match row.try_get::<_, i32>(0) {
                        Ok(val) => val.to_string(),
                        Err(_) => match row.try_get::<_, i64>(0) {
                            Ok(val) => val.to_string(),
                            Err(_) => match row.try_get::<_, String>(0) {
                                Ok(val) => val,
                                Err(_) => continue, // Skip this row if we can't get the primary key
                            },
                        },
                    };

                    // Get the foreign key value as a string
                    let to_fk_value: String = match row.try_get::<_, i32>(1) {
                        Ok(val) => val.to_string(),
                        Err(_) => match row.try_get::<_, i64>(1) {
                            Ok(val) => val.to_string(),
                            Err(_) => match row.try_get::<_, String>(1) {
                                Ok(val) => val,
                                Err(_) => continue, // Skip this row if we can't get the foreign key
                            },
                        },
                    };

                    if let (Some(&from_node_id), Some(&to_node_id)) = (
                        from_mappings.get(&from_pk_value),
                        to_mappings.get(&to_fk_value),
                    ) {
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

                        if batch_edges.len() >= self.batch_size {
                            self.send_edge_batch(&batch_edges, fk).await?;

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
                    self.send_edge_batch(&batch_edges, fk).await?;
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

    async fn send_edge_batch(
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

    pub async fn dump_to_json(&mut self, output_path: &str) -> Result<(), IngestionError> {
        let schemas = self.extract_schema().await?;

        // Helper function to normalize table names by removing timestamps and random numbers
        fn normalize_table_name(name: &str) -> String {
            name.split('_').next().unwrap_or(name).to_string()
        }

        // Process all tables from the schema
        for schema in &schemas {
            let normalized_name = normalize_table_name(&schema.name);
            let columns = schema
                .columns
                .iter()
                .filter_map(|column| {
                    let name = to_camel_case(&column.name);
                    Some((name, map_sql_type_to_helix_type(&column.data_type)))
                })
                .collect::<Vec<(String, String)>>();
            self.graph_schema
                .nodes
                .insert(normalized_name.clone(), columns);

            // Add edges to the graph schema based on foreign key relationships
            schema.foreign_keys.iter().for_each(|fk| {
                let from_table = normalize_table_name(&fk.from_table);
                let to_table = normalize_table_name(&fk.to_table);
                let edge_name = format!(
                    "{}To{}",
                    to_camel_case(&from_table),
                    to_camel_case(&to_table)
                );
                self.graph_schema.edges.insert(
                    edge_name,
                    EdgeSchema {
                        from: to_camel_case(&from_table),
                        to: to_camel_case(&to_table),
                        properties: vec![],
                    },
                );
            });
        }

        let mut graph_data = GraphData {
            nodes: Vec::new(),
            edges: Vec::new(),
        };

        // Collect all nodes from all tables
        for schema in &schemas {
            let mut table_nodes = self.collect_table_nodes(schema).await?;
            // Normalize the label names
            for node in &mut table_nodes {
                node.label = normalize_table_name(&node.label);
            }
            graph_data.nodes.extend(table_nodes);
        }

        // Create a mapping from table name and primary key to node index
        let mut node_indices = HashMap::new();
        for (idx, node) in graph_data.nodes.iter().enumerate() {
            let label = &node.label;
            let properties = &node.properties;

            // Find the primary key column for this table
            let schema = schemas
                .iter()
                .find(|s| normalize_table_name(&s.name) == *label)
                .ok_or_else(|| {
                    IngestionError::MappingError(format!("Schema not found for table {}", label))
                })?;

            let pk = schema.primary_keys.iter().next().ok_or_else(|| {
                IngestionError::MappingError(format!("No primary key found for table {}", label))
            })?;

            // Get the primary key value
            if let Some(Value::Text(pk_value)) = properties.get(pk) {
                node_indices.insert((label.clone(), pk_value.clone()), idx);
            } else if let Some(Value::Integer(pk_value)) = properties.get(pk) {
                node_indices.insert((label.clone(), pk_value.to_string()), idx);
            }
        }

        // Collect all edges based on foreign keys
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
                    fk.from_column,
                    fk.from_table,
                    fk.to_table,
                    fk.from_column,
                    fk.to_column,
                );

                let rows = self.pg_client.query(&query, &[]).await?;

                for row in rows {
                    let from_pk = self.extract_value_as_string(&row, 0)?;
                    let to_fk = self.extract_value_as_string(&row, 1)?;

                    // Look up the node indices using normalized table names
                    let from_key = (normalize_table_name(&fk.from_table), from_pk);
                    let to_key = (normalize_table_name(&fk.to_table), to_fk);

                    if let (Some(&from_idx), Some(&to_idx)) =
                        (node_indices.get(&from_key), node_indices.get(&to_key))
                    {
                        let edge_type = format!(
                            "{}_TO_{}",
                            normalize_table_name(&fk.from_table).to_uppercase(),
                            normalize_table_name(&fk.to_table).to_uppercase()
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

        // Write nodes to JSONL file
        let mut file = File::create(format!("{}/ingestion.jsonl", output_path)).map_err(|e| {
            IngestionError::MappingError(format!("Failed to create nodes file: {}", e))
        })?;
        println!("Created nodes file at {}", output_path);
        // Write nodes and edges
        for node in &graph_data.nodes {
            let mut json_data = serde_json::to_string(node).map_err(|e| {
                IngestionError::MappingError(format!("Failed to serialize node: {}", e))
            })?;
            json_data.push('\n');
            file.write_all(json_data.as_bytes()).map_err(|e| {
                IngestionError::MappingError(format!("Failed to write node: {}", e))
            })?;
        }

        for edge in &graph_data.edges {
            let mut json_data = serde_json::to_string(edge).map_err(|e| {
                IngestionError::MappingError(format!("Failed to serialize edge: {}", e))
            })?;
            json_data.push('\n');
            file.write_all(json_data.as_bytes()).map_err(|e| {
                IngestionError::MappingError(format!("Failed to write edge: {}", e))
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

    // Helper function to extract value as string from a row
    fn extract_value_as_string(&self, row: &Row, index: usize) -> Result<String, IngestionError> {
        match row.try_get::<_, i32>(index) {
            Ok(val) => Ok(val.to_string()),
            Err(_) => match row.try_get::<_, i64>(index) {
                Ok(val) => Ok(val.to_string()),
                Err(_) => match row.try_get::<_, String>(index) {
                    Ok(val) => Ok(val),
                    Err(_) => Err(IngestionError::MappingError(
                        "Failed to extract value as string".to_string(),
                    )),
                },
            },
        }
    }

    async fn collect_table_nodes(
        &mut self,
        table_schema: &TableSchema,
    ) -> Result<Vec<NodePayload>, IngestionError> {
        let mut nodes = Vec::new();

        let query = format!("SELECT * FROM {}", table_schema.name);
        let stmt = self.pg_client.prepare(&query).await?;

        let column_names: Vec<String> = stmt
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect();

        let rows = self.pg_client.query(&query, &[]).await?;

        for row in rows {
            let mut properties = HashMap::new();

            for (i, col_name) in column_names.iter().enumerate() {
                let value = self.extract_value_from_row(&row, i)?;
                properties.insert(col_name.clone(), value);

                // Check if this column is a primary key
                if table_schema.primary_keys.iter().any(|pk| pk == col_name) {
                    properties.insert("is_primary_key".to_string(), Value::Boolean(true));
                }
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

    pub async fn ingest(&mut self, output_dir: &str) -> Result<(), IngestionError> {
        let schemas = self.extract_schema().await?;

        // for schema in &schemas {
        //     self.ingest_table(schema).await?;
        // }

        // create edges
        // create indexes

        // if --dump flag is set, dump the ingestion stats to a file
        // path = ./helix_ingestion.json
        let path = Path::new(output_dir);
        // create the file if it doesn't exist
        self.dump_to_json(path.to_str().unwrap()).await?;

        // create the schema file
        let schema_path = Path::new(output_dir).join("schema.hx");
        println!("Creating schema file at {}", schema_path.to_str().unwrap());
        self.create_schemas(schema_path.to_str().unwrap())?;
        println!(
            "Successfully created schema file at {}",
            schema_path.to_str().unwrap()
        );
        Ok(())
    }

    fn parse_vector_string(vector_str: &str) -> Result<Vec<f64>, IngestionError> {
        let cleaned = vector_str.trim_matches(|c| c == '[' || c == ']');
        if cleaned.is_empty() {
            return Ok(Vec::new());
        }
        let values: Result<Vec<f64>, _> = cleaned
            .split(',')
            .map(|s| {
                s.trim()
                    .parse::<f64>()
                    .map_err(|e| IngestionError::MappingError(format!("Failed to parse vector value: {}", e)))
            })
        .collect();
        values
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
    let helix_type = match sql_type.to_uppercase().as_str() {
        "INTEGER" => "Integer",
        "INT" => "Integer",
        "BIGINT" => "Integer",
        "SMALLINT" => "Integer",
        "FLOAT" => "Float",
        "DOUBLE PRECISION" => "Float",
        "REAL" => "Float",
        "TEXT" => "String",
        "VARCHAR" => "String",
        "CHAR" => "String",
        "BOOLEAN" => "Boolean",
        "DATE" => "String",                        // TODO: Implement date type
        "TIME" => "String",                        // TODO: Implement time type
        "TIMESTAMP" => "String",                   // TODO: Implement datetime type
        "TIMESTAMP WITHOUT TIME ZONE" => "String", // Handle timestamp without time zone
        "TIMESTAMP WITH TIME ZONE" => "String",    // Handle timestamp with time zone
        "BLOB" => "String",
        "JSON" => "String",
        "JSONB" => "String",
        "UUID" => "String",
        "URL" => "String",
        "NUMERIC" => "Float",
        "DECIMAL" => "Float",
        "BIT" => "Boolean",
        "CHARACTER" => "String",
        "CHARACTER VARYING" => "String",
        "TEXT VARYING" => "String",
        "CHARACTER LARGE OBJECT" => "String",
        "TEXT LARGE OBJECT" => "String",
        "BYTEA" => "String",
        "STRING" => "String", // Add explicit handling for "String" type
        "VECTOR" => "HVector", // Add explicit handling for "String" type
        _ => {
            // Instead of panicking, return "String" as a fallback
            "String"
        }
    };
    helix_type.to_string()
}
