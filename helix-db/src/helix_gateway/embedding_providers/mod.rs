use crate::helix_engine::types::GraphError;
use reqwest::Client;
use sonic_rs::JsonValueTrait;
use sonic_rs::{JsonContainerTrait, json};
use std::env;
use url::Url;

#[cfg(feature = "model2vec")]
use model2vec_rs::model::StaticModel;

/// Embedding providers for generating text embeddings.
///
/// HelixDB supports four embedding providers:
///
/// ## OpenAI (requires `reqwest` feature)
/// - Format: `"openai:{model}"` or just `"{model}"` for default
/// - Requires: `OPENAI_API_KEY` environment variable
/// - Example: `"text-embedding-ada-002"`, `"openai:text-embedding-3-small"`
/// - Network: External API call to api.openai.com
/// - Cost: Paid per token
///
/// ## Gemini (requires `reqwest` feature)
/// - Format: `"gemini:{model}"` or `"gemini:{model}:{task_type}"`
/// - Requires: `GEMINI_API_KEY` environment variable
/// - Example: `"gemini:gemini-embedding-001"`, `"gemini:gemini-embedding-001:SEMANTIC_SIMILARITY"`
/// - Network: External API call to Google's API
/// - Cost: Paid per character
/// - Task types: `RETRIEVAL_QUERY`, `RETRIEVAL_DOCUMENT`, `SEMANTIC_SIMILARITY`, `CLASSIFICATION`, `CLUSTERING`
///
/// ## Local (requires `reqwest` feature)
/// - Format: `"local"`
/// - Requires: Local HTTP server running on `http://localhost:8699/embed`
/// - Network: HTTP call to localhost
/// - Cost: Free (self-hosted)
/// - Note: You must run your own embedding server
///
/// ## Model2Vec (requires `model2vec` feature)
/// - Format: `"model2vec:{model}"` or `"model2vec:"` for default
/// - Requires: No API key, no server
/// - Example: `"model2vec:minishlab/potion-base-8M"`, `"model2vec:minishlab/potion-base-32M"`
/// - Default: `"minishlab/potion-base-32M"` (768 dimensions)
/// - Network: Downloads model from HuggingFace on first use, then fully offline
/// - Cost: Free (in-process)
/// - Speed: <1ms inference after model load
/// - Models cached in: `~/.cache/huggingface/`
/// - Available models:
///   - `minishlab/potion-base-2M` (2MB, 256 dims, fastest)
///   - `minishlab/potion-base-8M` (8MB, 256 dims, balanced)
///   - `minishlab/potion-base-32M` (32MB, 768 dims, recommended)
///   - `minishlab/potion-retrieval-32M` (32MB, 768 dims, optimized for retrieval)
///
/// # Usage
///
/// Configure in `config.hx.json`:
/// ```json
/// {
///   "embedding_model": "model2vec:minishlab/potion-base-32M"
/// }
/// ```
///
/// Or use in HelixQL queries:
/// ```hql
/// #[model("model2vec:minishlab/potion-base-8M")]
/// QUERY search(query: String) =>
///     results <- SearchV<Document>(Embed(query), 10)
///     RETURN results
/// ```
///
/// # Feature Flags
///
/// - `server`: Enables OpenAI, Gemini, and Local providers (requires `reqwest`)
/// - `model2vec`: Enables Model2Vec provider (requires `model2vec-rs`)
///
/// Build with both:
/// ```bash
/// cargo build --features server,model2vec
/// ```
/// Trait for embedding models to fetch text embeddings.
#[allow(async_fn_in_trait)]
pub trait EmbeddingModel {
    fn fetch_embedding(&self, text: &str) -> Result<Vec<f64>, GraphError>;
    async fn fetch_embedding_async(&self, text: &str) -> Result<Vec<f64>, GraphError>;
}

#[derive(Debug, Clone)]
pub enum EmbeddingProvider {
    OpenAI,
    Gemini { task_type: String },
    Local,
    Model2Vec { model_name: String },
}

pub struct EmbeddingModelImpl {
    pub(crate) provider: EmbeddingProvider,
    api_key: Option<String>,
    client: Client,
    pub(crate) model: String,
    pub(crate) url: Option<String>,
    #[cfg(feature = "model2vec")]
    pub(crate) model2vec: Option<StaticModel>,
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
            EmbeddingProvider::Local => None,
            EmbeddingProvider::Model2Vec { .. } => None,
        };

        let url = match &provider {
            EmbeddingProvider::Local => {
                let url_str = _url.unwrap_or("http://localhost:8699/embed");
                Url::parse(url_str).map_err(|e| GraphError::from(format!("Invalid URL: {e}")))?;
                Some(url_str.to_string())
            }
            _ => None,
        };

        // Load model2vec model if using Model2Vec provider
        #[cfg(feature = "model2vec")]
        let model2vec = match &provider {
            EmbeddingProvider::Model2Vec { model_name } => {
                Some(
                    StaticModel::from_pretrained(
                        model_name, None, // No HF token needed for public models
                        None, // Use model's default normalization
                        None, // No subfolder
                    )
                    .map_err(|e| {
                        GraphError::from(format!(
                            "Failed to load model2vec model '{}': {}",
                            model_name, e
                        ))
                    })?,
                )
            }
            _ => None,
        };

        #[cfg(not(feature = "model2vec"))]
        let _model2vec: Option<()> = None;

        Ok(EmbeddingModelImpl {
            provider,
            api_key,
            client: Client::new(),
            model: model_name,
            url,
            #[cfg(feature = "model2vec")]
            model2vec,
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
            Some("local") => Ok((EmbeddingProvider::Local, "local".to_string())),

            // Model2Vec provider (in-process, local embedding generation)
            // Format: "model2vec:{model_name}"
            // Example: "model2vec:minishlab/potion-base-8M"
            // Default model: "minishlab/potion-base-32M"
            //
            // Features:
            // - No API key required
            // - No network calls (after initial model download)
            // - Works fully offline
            // - Fast inference (<1ms after model load)
            // - Models cached in ~/.cache/huggingface/
            //
            // Available models:
            // - minishlab/potion-base-2M (2MB, 256 dims)
            // - minishlab/potion-base-8M (8MB, 256 dims)
            // - minishlab/potion-base-32M (32MB, 768 dims) [recommended]
            // - minishlab/potion-retrieval-32M (32MB, 768 dims)
            Some(m) if m.starts_with("model2vec:") => {
                let model_name = m
                    .strip_prefix("model2vec:")
                    .filter(|s| !s.is_empty())
                    .unwrap_or("minishlab/potion-base-32M");
                Ok((
                    EmbeddingProvider::Model2Vec {
                        model_name: model_name.to_string(),
                    },
                    model_name.to_string(),
                ))
            }

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
    fn fetch_embedding(&self, text: &str) -> Result<Vec<f64>, GraphError> {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(self.fetch_embedding_async(text))
    }

    async fn fetch_embedding_async(&self, text: &str) -> Result<Vec<f64>, GraphError> {
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
                    })
                    .collect::<Result<Vec<f64>, GraphError>>()?;

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
                    })
                    .collect::<Result<Vec<f64>, GraphError>>()?;

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
                    })
                    .collect::<Result<Vec<f64>, GraphError>>()?;

                Ok(embedding)
            }

            #[cfg(feature = "model2vec")]
            EmbeddingProvider::Model2Vec { .. } => {
                let model = self
                    .model2vec
                    .as_ref()
                    .ok_or_else(|| GraphError::from("Model2Vec model not loaded"))?;

                // Clone for blocking task (cheap Arc-based clone)
                let text_owned = text.to_string();
                let model_clone = model.clone();

                // Run on blocking threadpool to avoid blocking async runtime
                let embedding = tokio::task::spawn_blocking(move || -> Vec<f64> {
                    let embedding_f32 = model_clone.encode_single(&text_owned);
                    embedding_f32.into_iter().map(|v| v as f64).collect()
                })
                .await
                .map_err(|e| GraphError::from(format!("Model2Vec task failed: {}", e)))?;

                Ok(embedding)
            }

            #[cfg(not(feature = "model2vec"))]
            EmbeddingProvider::Model2Vec { .. } => Err(GraphError::from(
                "Model2Vec provider requires 'model2vec' feature. \
                     Compile with --features model2vec",
            )),
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
/// let embedding = embed!("Hello, world!", "model2vec:minishlab/potion-base-32M");
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
