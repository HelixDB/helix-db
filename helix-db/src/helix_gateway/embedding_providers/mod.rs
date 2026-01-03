use crate::helix_engine::types::GraphError;
use reqwest::Client;
use sonic_rs::JsonValueTrait;
use sonic_rs::{JsonContainerTrait, json};
use std::env;
use url::Url;

/// Trait for embedding models to fetch text embeddings.
#[allow(async_fn_in_trait)]
pub trait EmbeddingModel {
    fn fetch_embedding(&self, text: &str) -> Result<Vec<f32>, GraphError>;
    async fn fetch_embedding_async(&self, text: &str) -> Result<Vec<f32>, GraphError>;
}

#[derive(Debug, Clone)]
pub enum EmbeddingProvider {
    OpenAI,
    Gemini {
        task_type: String,
    },
    AzureOpenAI {
        resource_name: String,
        deployment_id: String,
    },
    Local,
}

pub struct EmbeddingModelImpl {
    pub(crate) provider: EmbeddingProvider,
    api_key: Option<String>,
    client: Client,
    pub(crate) model: String,
    pub(crate) url: Option<String>,
}

impl EmbeddingModelImpl {
    pub fn new(
        api_key: Option<&str>,
        model: Option<&str>,
        _url: Option<&str>,
    ) -> Result<Self, GraphError> {
        let (provider, model_name) = Self::parse_provider_and_model(model)?;
        let api_key = match &provider {
            EmbeddingProvider::OpenAI => {
                let key = api_key
                    .map(String::from)
                    .or_else(|| env::var("OPENAI_API_KEY").ok())
                    .ok_or_else(|| GraphError::from("OPENAI_API_KEY not set"))?;
                Some(key)
            }
            EmbeddingProvider::Gemini { .. } => {
                let key = api_key
                    .map(String::from)
                    .or_else(|| env::var("GEMINI_API_KEY").ok())
                    .ok_or_else(|| GraphError::from("GEMINI_API_KEY not set"))?;
                Some(key)
            }
            EmbeddingProvider::AzureOpenAI { .. } => {
                let key = api_key
                    .map(String::from)
                    .or_else(|| env::var("AZURE_OPENAI_API_KEY").ok())
                    .ok_or_else(|| GraphError::from("AZURE_OPENAI_API_KEY not set"))?;
                Some(key)
            }
            EmbeddingProvider::Local => None,
        };

        let url = match &provider {
            EmbeddingProvider::Local => {
                let url_str = _url.unwrap_or("http://localhost:8699/embed");
                Url::parse(url_str).map_err(|e| GraphError::from(format!("Invalid URL: {e}")))?;
                Some(url_str.to_string())
            }
            _ => None,
        };

        Ok(EmbeddingModelImpl {
            provider,
            api_key,
            client: Client::new(),
            model: model_name,
            url,
        })
    }

    pub(crate) fn parse_provider_and_model(
        model: Option<&str>,
    ) -> Result<(EmbeddingProvider, String), GraphError> {
        match model {
            Some(m) if m.starts_with("gemini:") => {
                let parts: Vec<&str> = m.splitn(2, ':').collect();
                let model_and_task = parts.get(1).unwrap_or(&"gemini-embedding-001");
                let (model_name, task_type) = if model_and_task.contains(':') {
                    let task_parts: Vec<&str> = model_and_task.splitn(2, ':').collect();
                    (
                        task_parts[0].to_string(),
                        task_parts
                            .get(1)
                            .unwrap_or(&"RETRIEVAL_DOCUMENT")
                            .to_string(),
                    )
                } else {
                    (model_and_task.to_string(), "RETRIEVAL_DOCUMENT".to_string())
                };

                Ok((EmbeddingProvider::Gemini { task_type }, model_name))
            }
            Some(m) if m.starts_with("openai:") => {
                let model_name = m
                    .strip_prefix("openai:")
                    .unwrap_or("text-embedding-ada-002");
                Ok((EmbeddingProvider::OpenAI, model_name.to_string()))
            }
            Some(m) if m.starts_with("azure_openai:") => {
                let model_name = m
                    .strip_prefix("azure_openai:")
                    .unwrap_or("text-embedding-3-small");

                // Get Azure-specific configuration from environment
                let resource_name = env::var("AZURE_OPENAI_RESOURCE_NAME")
                    .map_err(|_| GraphError::from("AZURE_OPENAI_RESOURCE_NAME not set"))?;

                // deployment_id comes from the model_name
                let deployment_id = if model_name.is_empty() {
                    return Err(GraphError::from("Azure OpenAI deployment ID not specified"));
                } else {
                    model_name.to_string()
                };

                Ok((
                    EmbeddingProvider::AzureOpenAI {
                        resource_name,
                        deployment_id,
                    },
                    model_name.to_string(),
                ))
            }
            Some("local") => Ok((EmbeddingProvider::Local, "local".to_string())),

            Some(_) => Ok((
                EmbeddingProvider::OpenAI,
                "text-embedding-ada-002".to_string(),
            )),
            None => Err(GraphError::from("No embedding provider available")),
        }
    }
}

impl EmbeddingModel for EmbeddingModelImpl {
    /// Must be called with an active tokio context
    fn fetch_embedding(&self, text: &str) -> Result<Vec<f32>, GraphError> {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(self.fetch_embedding_async(text))
    }

    async fn fetch_embedding_async(&self, text: &str) -> Result<Vec<f32>, GraphError> {
        match &self.provider {
            EmbeddingProvider::OpenAI => {
                let api_key = self
                    .api_key
                    .as_ref()
                    .ok_or_else(|| GraphError::from("OpenAI API key not set"))?;

                let response = self
                    .client
                    .post("https://api.openai.com/v1/embeddings")
                    .header("Authorization", format!("Bearer {api_key}"))
                    .json(&json!({
                        "input": text,
                        "model": &self.model,
                    }))
                    .send()
                    .await
                    .map_err(|e| GraphError::from(format!("Failed to send request: {e}")))?;

                let text_response = response
                    .text()
                    .await
                    .map_err(|e| GraphError::from(format!("Failed to parse response: {e}")))?;

                let response = sonic_rs::from_str::<sonic_rs::Value>(&text_response)
                    .map_err(|e| GraphError::from(format!("Failed to parse response: {e}")))?;

                let embedding = response["data"][0]["embedding"]
                    .as_array()
                    .ok_or_else(|| GraphError::from("Invalid embedding format"))?
                    .iter()
                    .map(|v| {
                        v.as_f64()
                            .ok_or_else(|| GraphError::from("Invalid float value"))
                            .map(|f| f as f32)
                    })
                    .collect::<Result<Vec<f32>, GraphError>>()?;

                Ok(embedding)
            }
            EmbeddingProvider::AzureOpenAI {
                resource_name,
                deployment_id,
            } => {
                let api_key = self
                    .api_key
                    .as_ref()
                    .ok_or_else(|| GraphError::from("AzureOpenAI API key not set"))?;

                let url = format!(
                    "https://{}.openai.azure.com/openai/deployments/{}/embeddings?api-version=2024-10-21",
                    resource_name, deployment_id
                );
                let response = self
                    .client
                    .post(&url)
                    .header("api-key", api_key)
                    .header("Content-Type", "application/json")
                    .json(&json!({
                        "input": text
                    }))
                    .send()
                    .await
                    .map_err(|e| GraphError::from(format!("Failed to send request: {e}")))?;

                let text_response = response
                    .text()
                    .await
                    .map_err(|e| GraphError::from(format!("Failed to parse response: {e}")))?;

                let response = sonic_rs::from_str::<sonic_rs::Value>(&text_response)
                    .map_err(|e| GraphError::from(format!("Failed to parse response: {e}")))?;

                // Azure OpenAI uses the same response format as OpenAI
                let embedding = response["data"][0]["embedding"]
                    .as_array()
                    .ok_or_else(|| {
                        GraphError::from("Invalid embedding format from Azure OpenAI API")
                    })?
                    .iter()
                    .map(|v| {
                        v.as_f64()
                            .ok_or_else(|| GraphError::from("Invalid float value"))
                            .map(|v| v as f32)
                    })
                    .collect::<Result<Vec<f32>, GraphError>>()?;
                Ok(embedding)
            }

            EmbeddingProvider::Gemini { task_type } => {
                let api_key = self
                    .api_key
                    .as_ref()
                    .ok_or_else(|| GraphError::from("Gemini API key not set"))?;

                let url = format!(
                    "https://generativelanguage.googleapis.com/v1beta/models/{}:embedContent",
                    self.model
                );

                let response = self
                    .client
                    .post(&url)
                    .header("x-goog-api-key", api_key)
                    .header("Content-Type", "application/json")
                    .json(&json!({
                        "content": {
                            "parts": [{"text": text}]
                        },
                        "taskType": task_type
                    }))
                    .send()
                    .await
                    .map_err(|e| GraphError::from(format!("Failed to send request: {e}")))?;

                let text_response = response
                    .text()
                    .await
                    .map_err(|e| GraphError::from(format!("Failed to parse response: {e}")))?;

                let response = sonic_rs::from_str::<sonic_rs::Value>(&text_response)
                    .map_err(|e| GraphError::from(format!("Failed to parse response: {e}")))?;

                let embedding = response["embedding"]["values"]
                    .as_array()
                    .ok_or_else(|| GraphError::from("Invalid embedding format from Gemini API"))?
                    .iter()
                    .map(|v| {
                        v.as_f64()
                            .ok_or_else(|| GraphError::from("Invalid float value"))
                            .map(|f| f as f32)
                    })
                    .collect::<Result<Vec<f32>, GraphError>>()?;

                Ok(embedding)
            }

            EmbeddingProvider::Local => {
                let url = self
                    .url
                    .as_ref()
                    .ok_or_else(|| GraphError::from("Local URL not set"))?;

                let response = self
                    .client
                    .post(url)
                    .json(&json!({
                        "text": text,
                        "chunk_style": "recursive",
                        "chunk_size": 100
                    }))
                    .send()
                    .await
                    .map_err(|e| GraphError::from(format!("Request failed: {e}")))?;

                let text_response = response
                    .text()
                    .await
                    .map_err(|e| GraphError::from(format!("Failed to parse response: {e}")))?;

                let response = sonic_rs::from_str::<sonic_rs::Value>(&text_response)
                    .map_err(|e| GraphError::from(format!("Failed to parse JSON response: {e}")))?;

                let embedding = response["embedding"]
                    .as_array()
                    .ok_or_else(|| GraphError::from("Invalid embedding format"))?
                    .iter()
                    .map(|v| {
                        v.as_f64()
                            .ok_or_else(|| GraphError::from("Invalid float value"))
                            .map(|f| f as f32)
                    })
                    .collect::<Result<Vec<f32>, GraphError>>()?;

                Ok(embedding)
            }
        }
    }
}

/// Creates embedding based on provider.
pub fn get_embedding_model(
    api_key: Option<&str>,
    model: Option<&str>,
    url: Option<&str>,
) -> Result<EmbeddingModelImpl, GraphError> {
    EmbeddingModelImpl::new(api_key, model, url)
}

#[macro_export]
/// Fetches an embedding from the embedding model.
///
/// If no model or url is provided, it will use the default model and url.
///
/// This must be called on a sync worker, but with a tokio context, and in a place that returns a Result
///
/// ## Example Use
/// ```rust
/// use helix_db::embed;
/// let query = embed!("Hello, world!");
/// let embedding = embed!("Hello, world!", "text-embedding-ada-002");
/// let embedding = embed!("Hello, world!", "gemini:gemini-embedding-001:SEMANTIC_SIMILARITY");
/// let embedding = embed!("Hello, world!", "text-embedding-ada-002", "http://localhost:8699/embed");
/// ```
macro_rules! embed {
    ($db:expr, $query:expr) => {{
        let embedding_model =
            get_embedding_model(None, $db.storage_config.embedding_model.as_deref(), None);
        embedding_model.fetch_embedding($query)?
    }};
    ($db:expr, $query:expr, $provider:expr) => {{
        let embedding_model = get_embedding_model(None, Some($provider), None);
        embedding_model.fetch_embedding($query)?
    }};
    ($db:expr, $query:expr, $provider:expr, $url:expr) => {{
        let embedding_model = get_embedding_model(None, Some($provider), Some($url));
        embedding_model.fetch_embedding($query)?
    }};
}

#[macro_export]
/// Fetches an embedding from the embedding model.
///
/// If no model or url is provided, it will use the default model and url.
///
macro_rules! embed_async {
    (INNER_MODEL: $model:expr, $query:expr) => {
        match $model {
            Ok(m) => m.fetch_embedding_async($query).await,
            Err(e) => Err(e),
        }
    };
    ($db:expr, $query:expr) => {{
        let embedding_model =
            get_embedding_model(None, $db.storage_config.embedding_model.as_deref(), None);
        embed_async!(INNER_MODEL: embedding_model, $query)
    }};
    ($db:expr, $query:expr, $provider:expr) => {{
        let embedding_model = get_embedding_model(None, Some($provider), None);
        embed_async!(INNER_MODEL: embedding_model, $query)
    }};
    ($db:expr, $query:expr, $provider:expr, $url:expr) => {{
        let embedding_model = get_embedding_model(None, Some($provider), Some($url));
        embed_async!(INNER_MODEL: embedding_model, $query)
    }};
}
