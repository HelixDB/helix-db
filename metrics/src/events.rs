use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum EventType {
    #[serde(rename = "cli_install")]
    CliInstall,
    #[serde(rename = "compile")]
    Compile,
    #[serde(rename = "deploy_local")]
    DeployLocal,
    #[serde(rename = "redeploy_cloud")]
    DeployCloud,
    #[serde(rename = "redeploy_local")]
    RedeployLocal,
    #[serde(rename = "query_success")]
    QuerySuccess,
    #[serde(rename = "query_error")]
    QueryError,
    #[serde(rename = "write_error")]
    WriteError,
    #[serde(rename = "read_error")]
    ReadError,
    #[serde(rename = "test")]
    Test,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::CliInstall => "cli_install",
            EventType::Compile => "compile",
            EventType::DeployLocal => "deploy_local",
            EventType::DeployCloud => "deploy_cloud",
            EventType::RedeployLocal => "redeploy_local",
            EventType::QuerySuccess => "query_success",
            EventType::QueryError => "query_error",
            EventType::WriteError => "write_error",
            EventType::ReadError => "read_error",
            EventType::Test => "test",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RawEvent<D: Serialize + std::fmt::Debug> {
    pub os: String,
    pub event_type: EventType,
    pub event_data: D,
    pub user_id: Option<String>,
    pub email: Option<String>,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventData {
    CliInstall,
    Compile(CompileEvent),
    DeployLocal(DeployLocalEvent),
    DeployCloud(DeployCloudEvent),
    RedeployLocal(RedeployLocalEvent),
    QuerySuccess(QuerySuccessEvent),
    QueryError(QueryErrorEvent),
    WriteError(WriteErrorEvent),
    ReadError(ReadErrorEvent),
    Test(TestEvent),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TestEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_sec: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>, 
}

impl Default for TestEvent {
    fn default() -> Self {
        TestEvent {
            cluster_id: "test_cluster".to_string(),
            queries_string: "test_queries".to_string(),
            num_of_queries: 0,
            time_taken_sec: 0,
            success: true,
            error_messages: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompileEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_seconds: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeployLocalEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_sec: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RedeployLocalEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_sec: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeployCloudEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_sec: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct WriteErrorEvent {
    pub cluster_id: String,
    pub key: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_string: Option<String>,
    pub value: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_string: Option<String>,
    pub time_taken_usec: u32,
    pub error_messages: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadErrorEvent {
    pub cluster_id: String,
    pub key: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_string: Option<String>,
    pub value: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_string: Option<String>,
    pub time_taken_usec: u32,
    pub error_messages: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryErrorEvent {
    pub query_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_json: Option<String>,
    pub time_taken_usec: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuerySuccessEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    pub query_name: String,
    pub time_taken_usec: u32,
}
