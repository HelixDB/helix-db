// TODO: do this all in a seperate crate?

use crate::helix_engine::types::GraphError;
use reqwest::blocking::Client;
use serde_json::json;
use std::env;

pub trait EmbeddingModel {
    fn fetch_embedding(&self, text: &str) -> Result<Vec<f64>, GraphError>;
}

#[cfg(feature = "embed_openai")]
struct EmbeddingModelImpl {
    api_key: String,
    client: Client,
    model: String,
}

#[cfg(feature = "embed_openai")]
impl EmbeddingModelImpl {
    fn new(api_key: Option<&str>, model: Option<&str>) -> Self {
        let key = api_key
            .map(String::from)
            .unwrap_or_else(|| env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY not set"));
        EmbeddingModelImpl {
            api_key: key,
            client: Client::new(),
            model: model.map(String::from).unwrap_or("text-embedding-ada-002".into()),
        }
    }
}

#[cfg(feature = "embed_openai")]
impl EmbeddingModel for EmbeddingModelImpl {
    fn fetch_embedding(&self, text: &str) -> Result<Vec<f64>, GraphError> {
        let response = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&json!({
                "input": text,
                "model": &self.model,
            }))
        .send()
            .map_err(|e| GraphError::from(format!("Failed to send request: {}", e)))?
            .json::<serde_json::Value>()
            .map_err(|e| GraphError::from(format!("Failed to parse response: {}", e)))?;

        let embedding = response["data"][0]["embedding"]
            .as_array()
            .ok_or_else(|| GraphError::from("Invalid embedding format"))?
            .iter()
            .map(|v| v.as_f64().ok_or_else(|| GraphError::from("Invalid float value")))
            .collect::<Result<Vec<f64>, GraphError>>()?;

        Ok(embedding)
    }
}

#[cfg(feature = "embed_local")]
struct EmbeddingModelImpl {
    url: String,
    client: Client,
}

#[cfg(feature = "embed_local")]
impl EmbeddingModelImpl {
    fn new(url: Option<&str>) -> Self {
        EmbeddingModelImpl {
            url: url.map(String::from).unwrap_or("http://localhost:8699/embed".into()),
            client: Client::new(),
        }
    }
}

#[cfg(feature = "embed_local")]
impl EmbeddingModel for EmbeddingModelImpl {
    fn fetch_embedding(&self, text: &str) -> Result<Vec<f64>, GraphError> {
        let response = self
            .client
            .post(&self.url)
            .json(&json!({
                "text": text,
                "chunk_style": "recursive",
                "chunk_size": 100
            }))
        .send()
            .map_err(|e| GraphError::from(format!("Request failed: {}", e)))?
            .json::<serde_json::Value>()
            .map_err(|e| GraphError::from(format!("Failed to parse response: {}", e)))?;

        let embedding = response["embedding"]
            .as_array()
            .ok_or_else(|| GraphError::from("Invalid embedding format"))?
            .iter()
            .map(|v| v.as_f64().ok_or_else(|| GraphError::from("Invalid float value")))
            .collect::<Result<Vec<f64>, GraphError>>()?;

        Ok(embedding)
    }
}

pub fn get_embedding_model(api_key: Option<&str>, model: Option<&str>, url: Option<&str>) -> Box<dyn EmbeddingModel> {
    #[cfg(feature = "embed_openai")]
    return Box::new(EmbeddingModelImpl::new(api_key, model));

    #[cfg(feature = "embed_local")]
    return Box::new(EmbeddingModelImpl::new(url));

    #[cfg(not(any(feature = "openai", feature = "local")))]
    panic!("No embedding model feature enabled. Enable either 'openai' or 'local'.");
}


/* basically do this:
    let query = {
        let embedding_model = get_embedding_model(None, None, None);
        embedding_model
            .fetch_embedding(query)
            .expect("Failed to fetch embedding")
    };
*/

