use crate::output::{standard_spinner_style, STANDARD_SPINNER_TICK_MILLIS};
use eyre::{eyre, Result};
use futures_util::StreamExt;
use indicatif::ProgressBar;
use reqwest_eventsource::{Event, RequestBuilderExt};
use serde::{Deserialize, Serialize};
use std::io::IsTerminal;
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

    /// Cluster creation: Creating project
    CreatingProject,

    /// Cluster creation: Project created successfully
    ProjectCreated { cluster_id: String },

    // Deploy events
    /// Deploy: Validating queries
    ValidatingQueries,

    /// Deploy: Building with progress
    Building {
        #[serde(default)]
        estimated_percentage: u16,
    },

    /// Deploy: Deploying to infrastructure
    Deploying,

    /// Deploy: Successfully deployed (new instance)
    Deployed { url: String, auth_key: String },

    /// Deploy: Successfully redeployed (existing instance)
    Redeployed { url: String },

    /// Deploy: Unified done event (dashboard/CLI parity path)
    Done {
        url: String,
        auth_key: Option<String>,
    },

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
                    let sse_event = parse_sse_event(&message.data)?;

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

pub(crate) fn parse_sse_event(payload: &str) -> Result<SseEvent> {
    if let Ok(event) = serde_json::from_str::<SseEvent>(payload) {
        return Ok(event);
    }

    let value: serde_json::Value = serde_json::from_str(payload)
        .map_err(|e| eyre!("Failed to parse SSE event JSON: {}", e))?;

    let event_type = value
        .get("type")
        .and_then(|t| t.as_str())
        .ok_or_else(|| eyre!("Failed to parse SSE event: unsupported format"))?;

    match event_type {
        "progress" => Ok(SseEvent::Progress {
            percentage: value
                .get("percentage")
                .and_then(|v| v.as_f64())
                .unwrap_or_default(),
            message: value
                .get("message")
                .and_then(|m| m.as_str())
                .map(str::to_string),
        }),
        "log" => Ok(SseEvent::Log {
            message: value
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or_default()
                .to_string(),
            severity: value
                .get("severity")
                .and_then(|s| s.as_str())
                .map(str::to_string),
            timestamp: value
                .get("timestamp")
                .and_then(|t| t.as_str())
                .map(str::to_string),
        }),
        "backfill_complete" => Ok(SseEvent::BackfillComplete),
        "status_transition" => Ok(SseEvent::StatusTransition {
            from: value
                .get("from")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            to: value
                .get("to")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            message: value
                .get("message")
                .and_then(|m| m.as_str())
                .map(str::to_string),
        }),
        "success" => Ok(SseEvent::Success {
            data: value
                .get("data")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        }),
        "validating_queries" => Ok(SseEvent::ValidatingQueries),
        "building" => Ok(SseEvent::Building {
            estimated_percentage: value
                .get("estimated_percentage")
                .and_then(|v| v.as_u64())
                .unwrap_or_default() as u16,
        }),
        "deploying" => Ok(SseEvent::Deploying),
        "deployed" => Ok(SseEvent::Deployed {
            url: value
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            auth_key: value
                .get("auth_key")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        "redeployed" => Ok(SseEvent::Redeployed {
            url: value
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        }),
        "done" => Ok(SseEvent::Done {
            url: value
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            auth_key: value
                .get("auth_key")
                .and_then(|v| v.as_str())
                .map(str::to_string),
        }),
        "bad_request" => Ok(SseEvent::BadRequest {
            error: value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Bad request")
                .to_string(),
        }),
        "query_validation_error" => Ok(SseEvent::QueryValidationError {
            error: value
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Query validation failed")
                .to_string(),
        }),
        "error" => Ok(SseEvent::Error {
            error: value
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("Unknown SSE error")
                .to_string(),
        }),
        other => Err(eyre!("Failed to parse SSE event type: {}", other)),
    }
}

/// Progress bar handler for SSE events with real-time progress
pub struct SseProgressHandler {
    progress_bar: ProgressBar,
}

impl SseProgressHandler {
    /// Create a new progress handler with a message
    pub fn new(message: &str) -> Self {
        let progress_bar = if std::io::stdout().is_terminal() {
            let pb = ProgressBar::new_spinner();
            pb.set_style(standard_spinner_style());
            pb.enable_steady_tick(Duration::from_millis(STANDARD_SPINNER_TICK_MILLIS));
            pb.set_message(message.to_string());
            pb
        } else {
            ProgressBar::hidden()
        };

        Self { progress_bar }
    }

    /// Update progress percentage
    pub fn set_progress(&self, _percentage: f64) {
        // Spinner mode does not render a numeric progress bar.
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

        // Internal-tagged logs event format used by /logs/live endpoints
        let json = r#"{
            "type": "log",
            "message": "hello",
            "severity": "info",
            "timestamp": "2026-02-13T00:00:00Z"
        }"#;
        let event = parse_sse_event(json).unwrap();
        match event {
            SseEvent::Log {
                message,
                severity,
                timestamp,
            } => {
                assert_eq!(message, "hello");
                assert_eq!(severity.as_deref(), Some("info"));
                assert_eq!(timestamp.as_deref(), Some("2026-02-13T00:00:00Z"));
            }
            _ => panic!("Wrong internal-tagged event type"),
        }

        // Internal-tagged deploy event format compatibility
        let json = r#"{
            "type": "building",
            "estimated_percentage": 42
        }"#;
        let event = parse_sse_event(json).unwrap();
        match event {
            SseEvent::Building {
                estimated_percentage,
            } => {
                assert_eq!(estimated_percentage, 42);
            }
            _ => panic!("Wrong internal-tagged deploy event type"),
        }

        // External-tagged building event with missing percentage should default to 0
        let json = r#"{
            "building": {}
        }"#;
        let event = parse_sse_event(json).unwrap();
        match event {
            SseEvent::Building {
                estimated_percentage,
            } => {
                assert_eq!(estimated_percentage, 0);
            }
            _ => panic!("Wrong external-tagged deploy event type"),
        }

        // Internal-tagged building event with missing percentage should default to 0
        let json = r#"{
            "type": "building"
        }"#;
        let event = parse_sse_event(json).unwrap();
        match event {
            SseEvent::Building {
                estimated_percentage,
            } => {
                assert_eq!(estimated_percentage, 0);
            }
            _ => panic!("Wrong internal-tagged deploy event type"),
        }
    }

    #[test]
    fn internal_sse_variants_preserve_payloads_and_defaults() {
        let cases = [
            (
                r#"{"type":"status_transition","from":"PENDING","to":"READY","message":"done"}"#,
                "StatusTransition",
            ),
            (r#"{"type":"success","data":{"key":"value"}}"#, "Success"),
            (
                r#"{"type":"deployed","url":"https://gateway","auth_key":"key"}"#,
                "Deployed",
            ),
            (r#"{"type":"done","url":"https://gateway"}"#, "Done"),
            (r#"{"type":"bad_request"}"#, "BadRequest"),
            (
                r#"{"type":"query_validation_error"}"#,
                "QueryValidationError",
            ),
            (r#"{"type":"error"}"#, "Error"),
        ];

        for (payload, expected) in cases {
            let event = parse_sse_event(payload).unwrap();
            assert!(format!("{event:?}").starts_with(expected));
        }
    }

    #[test]
    fn malformed_and_unknown_sse_events_are_rejected() {
        assert!(parse_sse_event("not-json")
            .unwrap_err()
            .to_string()
            .contains("JSON"));
        assert!(parse_sse_event(r#"{"type":"future_event"}"#)
            .unwrap_err()
            .to_string()
            .contains("future_event"));
        assert!(parse_sse_event(r#"{"message":"missing type"}"#)
            .unwrap_err()
            .to_string()
            .contains("unsupported format"));
    }
}
