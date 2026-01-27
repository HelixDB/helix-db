use eyre::{Result, eyre};
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest_eventsource::{Event, RequestBuilderExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// SSE event types from Helix Cloud backend
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SseEvent {
    /// GitHub login: Contains user code and verification URI
    UserVerification {
        user_code: String,
        verification_uri: String,
    },

    /// Successful authentication/operation
    Success {
        #[serde(flatten)]
        data: serde_json::Value,
    },

    /// Device code timeout (5-minute window expired)
    DeviceCodeTimeout { message: String },

    /// Error event
    Error { error: String },

    /// Progress update with percentage
    Progress {
        percentage: f64,
        message: Option<String>,
    },

    /// Log message from operation (supports both level and severity field names)
    Log {
        message: String,
        #[serde(alias = "level")]
        severity: Option<String>,
        timestamp: Option<String>,
    },

    /// Backfill complete marker (logs endpoint)
    BackfillComplete,

    /// Status transition (e.g., PENDING → PROVISIONING → READY)
    StatusTransition {
        from: Option<String>,
        to: String,
        message: Option<String>,
    },

    /// Cluster creation: Checkout required (Stripe)
    CheckoutRequired { url: String },

    /// Cluster creation: Payment confirmed
    PaymentConfirmed,

    /// Cluster creation: Creating project
    CreatingProject,

    /// Cluster creation: Project created successfully
    ProjectCreated { cluster_id: String },

    // Deploy events
    /// Deploy: Validating queries
    ValidatingQueries,

    /// Deploy: Building with progress
    Building { estimated_percentage: u16 },

    /// Deploy: Deploying to infrastructure
    Deploying,

    /// Deploy: Successfully deployed (new instance)
    Deployed { url: String, auth_key: String },

    /// Deploy: Successfully redeployed (existing instance)
    Redeployed { url: String },

    /// Deploy: Bad request error
    BadRequest { error: String },

    /// Deploy: Query validation error
    QueryValidationError { error: String },
}

/// SSE client for streaming events from Helix Cloud
pub struct SseClient {
    url: String,
    headers: Vec<(String, String)>,
    timeout: Duration,
    use_post: bool,
}

impl SseClient {
    /// Create a new SSE client
    pub fn new(url: String) -> Self {
        Self {
            url,
            headers: Vec::new(),
            timeout: Duration::from_secs(300), // 5 minutes default
            use_post: false,
        }
    }

    /// Add a header to the request
    #[allow(dead_code)]
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Set the timeout duration
    #[allow(dead_code)]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Use POST method instead of GET
    pub fn post(mut self) -> Self {
        self.use_post = true;
        self
    }

    /// Connect to SSE stream and process events
    pub async fn connect<F>(&self, mut handler: F) -> Result<()>
    where
        F: FnMut(SseEvent) -> Result<bool>,
    {
        let client = reqwest::Client::builder().timeout(self.timeout).build()?;

        let mut request = if self.use_post {
            client.post(&self.url)
        } else {
            client.get(&self.url)
        };
        for (key, value) in &self.headers {
            request = request.header(key, value);
        }

        let mut event_source = request.eventsource()?;

        while let Some(event) = event_source.next().await {
            match event {
                Ok(Event::Open) => {
                    // Connection opened
                }
                Ok(Event::Message(message)) => {
                    // Parse the SSE event
                    let sse_event: SseEvent = serde_json::from_str(&message.data)
                        .map_err(|e| eyre!("Failed to parse SSE event: {}", e))?;

                    // Call handler - if it returns false, stop processing
                    if !handler(sse_event)? {
                        event_source.close();
                        break;
                    }
                }
                Err(err) => {
                    event_source.close();
                    return Err(eyre!("SSE stream error: {}", err));
                }
            }
        }

        Ok(())
    }
}

/// Progress bar handler for SSE events with real-time progress
pub struct SseProgressHandler {
    progress_bar: ProgressBar,
}

impl SseProgressHandler {
    /// Create a new progress handler with a message
    pub fn new(message: &str) -> Self {
        let progress_bar = ProgressBar::new(100);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{msg}\n{bar:40.cyan/blue} {pos}%")
                .expect("Invalid progress bar template")
                .progress_chars("=>-"),
        );
        progress_bar.set_message(message.to_string());

        Self { progress_bar }
    }

    /// Update progress percentage
    pub fn set_progress(&self, percentage: f64) {
        self.progress_bar.set_position(percentage as u64);
    }

    /// Update progress message
    pub fn set_message(&self, message: &str) {
        self.progress_bar.set_message(message.to_string());
    }

    /// Print a log message below the progress bar
    pub fn println(&self, message: &str) {
        self.progress_bar.println(message);
    }

    /// Finish the progress bar with a message
    pub fn finish(&self, message: &str) {
        self.progress_bar.finish_with_message(message.to_string());
    }

    /// Finish with error
    pub fn finish_error(&self, message: &str) {
        self.progress_bar.abandon_with_message(message.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sse_event_deserialization() {
        // Test UserVerification (externally-tagged format with snake_case)
        let json = r#"{
            "user_verification": {
                "user_code": "ABC-123",
                "verification_uri": "https://github.com/login/device"
            }
        }"#;
        let event: SseEvent = serde_json::from_str(json).unwrap();
        match event {
            SseEvent::UserVerification { user_code, .. } => {
                assert_eq!(user_code, "ABC-123");
            }
            _ => panic!("Wrong event type"),
        }

        // Test Progress (externally-tagged format with snake_case)
        let json = r#"{
            "progress": {
                "percentage": 45.5,
                "message": "Building..."
            }
        }"#;
        let event: SseEvent = serde_json::from_str(json).unwrap();
        match event {
            SseEvent::Progress { percentage, .. } => {
                assert_eq!(percentage, 45.5);
            }
            _ => panic!("Wrong event type"),
        }
    }
}
