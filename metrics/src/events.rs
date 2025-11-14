use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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
    #[serde(rename = "invalid_api_key")]
    InvalidApiKey,
    #[serde(rename = "test")]
    Test,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CliInstall => "cli_install",
            Self::Compile => "compile",
            Self::DeployLocal => "deploy_local",
            Self::DeployCloud => "deploy_cloud",
            Self::RedeployLocal => "redeploy_local",
            Self::QuerySuccess => "query_success",
            Self::QueryError => "query_error",
            Self::WriteError => "write_error",
            Self::ReadError => "read_error",
            Self::InvalidApiKey => "invalid_api_key",
            Self::Test => "test",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RawEvent<D: Serialize + std::fmt::Debug + Clone> {
    pub os: &'static str,
    pub event_type: EventType,
    pub event_data: D,
    pub user_id: Option<&'static str>,
    pub email: Option<&'static str>,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    InvalidApiKey(InvalidApiKeyEvent),
    Test(TestEvent),
}

impl From<CompileEvent> for EventData {
    fn from(e: CompileEvent) -> Self {
        Self::Compile(e)
    }
}

impl From<DeployLocalEvent> for EventData {
    fn from(e: DeployLocalEvent) -> Self {
        Self::DeployLocal(e)
    }
}

impl From<DeployCloudEvent> for EventData {
    fn from(e: DeployCloudEvent) -> Self {
        Self::DeployCloud(e)
    }
}

impl From<RedeployLocalEvent> for EventData {
    fn from(e: RedeployLocalEvent) -> Self {
        Self::RedeployLocal(e)
    }
}

impl From<QuerySuccessEvent> for EventData {
    fn from(e: QuerySuccessEvent) -> Self {
        Self::QuerySuccess(e)
    }
}

impl From<QueryErrorEvent> for EventData {
    fn from(e: QueryErrorEvent) -> Self {
        Self::QueryError(e)
    }
}

impl From<WriteErrorEvent> for EventData {
    fn from(e: WriteErrorEvent) -> Self {
        Self::WriteError(e)
    }
}

impl From<ReadErrorEvent> for EventData {
    fn from(e: ReadErrorEvent) -> Self {
        Self::ReadError(e)
    }
}

impl From<TestEvent> for EventData {
    fn from(e: TestEvent) -> Self {
        Self::Test(e)
    }
}

impl From<InvalidApiKeyEvent> for EventData {
    fn from(e: InvalidApiKeyEvent) -> Self {
        Self::InvalidApiKey(e)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
        Self {
            cluster_id: "test_cluster".to_string(),
            queries_string: "test_queries".to_string(),
            num_of_queries: 0,
            time_taken_sec: 0,
            success: true,
            error_messages: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompileEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_seconds: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeployLocalEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_sec: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RedeployLocalEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_sec: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeployCloudEvent {
    pub cluster_id: String,
    pub queries_string: String,
    pub num_of_queries: u32,
    pub time_taken_sec: u32,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_messages: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QuerySuccessEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    pub query_name: String,
    pub time_taken_usec: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InvalidApiKeyEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster_id: Option<String>,
    pub time_taken_usec: u32,
}
