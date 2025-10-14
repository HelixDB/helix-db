use crate::helix_gateway::embedding_providers::{
    EmbeddingModel, EmbeddingModelImpl, EmbeddingProvider, get_embedding_model,
};

// Integration tests (require API keys and network)
#[test]
#[ignore] // Requires API key and network
fn test_openai_embedding_success() {
    let model = get_embedding_model(None, Some("text-embedding-ada-002"), None).unwrap();
    let result = model.fetch_embedding("test text");
    assert!(result.is_ok());
    let embedding = result.unwrap();
    println!("embedding: {embedding:?}");
}

#[test]
#[ignore] // Requires API key and network
fn test_gemini_embedding_success() {
    let model = get_embedding_model(None, Some("gemini-embedding-001"), None).unwrap();
    let result = model.fetch_embedding("test text");
    assert!(result.is_ok());
    let embedding = result.unwrap();
    println!("embedding: {embedding:?}");
}

#[test]
#[ignore] // Requires API key and network
fn test_gemini_embedding_with_task_type() {
    let model = get_embedding_model(
        None,
        Some("gemini:gemini-embedding-001:SEMANTIC_SIMILARITY"),
        None,
    )
    .unwrap();
    let result = model.fetch_embedding("test text");
    assert!(result.is_ok());
    let embedding = result.unwrap();
    println!("embedding: {embedding:?}");
}

#[test]
#[ignore] // Requires local embedding server
fn test_local_embedding_success() {
    let model =
        get_embedding_model(None, Some("local"), Some("http://localhost:8699/embed")).unwrap();
    let result = model.fetch_embedding("test text");
    assert!(result.is_ok());
    let embedding = result.unwrap();
    println!("embedding: {:?}", embedding);
}

#[test]
fn test_local_embedding_invalid_url() {
    let model = get_embedding_model(None, Some("local"), Some("invalid_url"));
    assert!(model.is_err());
}

// Unit tests for parse_provider_and_model
#[test]
fn test_parse_openai_provider_with_model() {
    let result =
        EmbeddingModelImpl::parse_provider_and_model(Some("openai:text-embedding-3-small"));
    assert!(result.is_ok());
    let (provider, model) = result.unwrap();
    assert!(matches!(provider, EmbeddingProvider::OpenAI));
    assert_eq!(model, "text-embedding-3-small");
}

#[test]
fn test_parse_openai_provider_empty_model() {
    let result = EmbeddingModelImpl::parse_provider_and_model(Some("openai:"));
    assert!(result.is_ok());
    let (provider, model) = result.unwrap();
    assert!(matches!(provider, EmbeddingProvider::OpenAI));
    assert_eq!(model, ""); // Returns empty string when no model specified after colon
}

#[test]
fn test_parse_gemini_provider_with_model() {
    let result = EmbeddingModelImpl::parse_provider_and_model(Some("gemini:gemini-embedding-002"));
    assert!(result.is_ok());
    let (provider, model) = result.unwrap();
    match provider {
        EmbeddingProvider::Gemini { task_type } => {
            assert_eq!(task_type, "RETRIEVAL_DOCUMENT");
        }
        _ => panic!("Expected Gemini provider"),
    }
    assert_eq!(model, "gemini-embedding-002");
}

#[test]
fn test_parse_gemini_provider_with_task_type() {
    let result = EmbeddingModelImpl::parse_provider_and_model(Some(
        "gemini:gemini-embedding-001:SEMANTIC_SIMILARITY",
    ));
    assert!(result.is_ok());
    let (provider, model) = result.unwrap();
    match provider {
        EmbeddingProvider::Gemini { task_type } => {
            assert_eq!(task_type, "SEMANTIC_SIMILARITY");
        }
        _ => panic!("Expected Gemini provider"),
    }
    assert_eq!(model, "gemini-embedding-001");
}

#[test]
fn test_parse_gemini_provider_empty_model() {
    let result = EmbeddingModelImpl::parse_provider_and_model(Some("gemini:"));
    assert!(result.is_ok());
    let (provider, model) = result.unwrap();
    match provider {
        EmbeddingProvider::Gemini { task_type } => {
            assert_eq!(task_type, "RETRIEVAL_DOCUMENT");
        }
        _ => panic!("Expected Gemini provider"),
    }
    assert_eq!(model, ""); // Returns empty string when no model specified after colon
}

#[test]
fn test_parse_local_provider() {
    let result = EmbeddingModelImpl::parse_provider_and_model(Some("local"));
    assert!(result.is_ok());
    let (provider, model) = result.unwrap();
    assert!(matches!(provider, EmbeddingProvider::Local));
    assert_eq!(model, "local");
}

#[test]
fn test_parse_unknown_provider_defaults_to_openai() {
    let result = EmbeddingModelImpl::parse_provider_and_model(Some("unknown-provider"));
    assert!(result.is_ok());
    let (provider, model) = result.unwrap();
    assert!(matches!(provider, EmbeddingProvider::OpenAI));
    assert_eq!(model, "text-embedding-ada-002");
}

#[test]
fn test_parse_no_provider_returns_error() {
    let result = EmbeddingModelImpl::parse_provider_and_model(None);
    assert!(result.is_err());
}

#[test]
fn test_new_openai_without_api_key_fails() {
    unsafe {
        std::env::remove_var("OPENAI_API_KEY");
    }
    let result = EmbeddingModelImpl::new(None, Some("openai:text-embedding-ada-002"), None);
    assert!(result.is_err());
}

#[test]
fn test_new_gemini_without_api_key_fails() {
    unsafe {
        std::env::remove_var("GEMINI_API_KEY");
    }
    let result = EmbeddingModelImpl::new(None, Some("gemini:gemini-embedding-001"), None);
    assert!(result.is_err());
}

#[test]
fn test_new_local_with_valid_url() {
    let result = EmbeddingModelImpl::new(None, Some("local"), Some("http://localhost:8699/embed"));
    assert!(result.is_ok());
    let model = result.unwrap();
    assert_eq!(model.url, Some("http://localhost:8699/embed".to_string()));
}

#[test]
fn test_new_local_with_invalid_url() {
    let result = EmbeddingModelImpl::new(None, Some("local"), Some("not-a-valid-url"));
    assert!(result.is_err());
}

#[test]
fn test_new_local_default_url() {
    let result = EmbeddingModelImpl::new(None, Some("local"), None);
    assert!(result.is_ok());
    let model = result.unwrap();
    assert_eq!(model.url, Some("http://localhost:8699/embed".to_string()));
}

#[test]
fn test_get_embedding_model_wrapper() {
    let result = get_embedding_model(None, Some("local"), Some("http://localhost:8080"));
    assert!(result.is_ok());
}
