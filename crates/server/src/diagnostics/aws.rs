//! Where this server and its S3 bucket run, and how far apart they are.
//!
//! Every probe only reads: the EC2 (IMDSv2) or ECS task metadata endpoint
//! for this server's region, an unsigned `HEAD` of the bucket for its
//! region, and `HEAD`s of a key that does not exist for the round-trip
//! time. Each is bounded by a short timeout so an unreachable endpoint
//! costs seconds, not the object store's minutes of retries.

use std::sync::OnceLock;
use std::time::Duration;

use object_store::{ObjectStore, ObjectStoreExt as _};

/// Address of the EC2 instance metadata service.
const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254";
/// Longest one metadata request may take: where the endpoint exists it
/// answers in milliseconds, and elsewhere nothing answers at all.
const METADATA_TIMEOUT: Duration = Duration::from_secs(1);
/// Longest the bucket-region lookup may take.
const BUCKET_REGION_TIMEOUT: Duration = Duration::from_secs(3);
/// Round trips measured by [`probe_latency`].
pub(crate) const LATENCY_SAMPLES: usize = 5;
/// Longest one round trip may take before the probe stops.
pub(crate) const LATENCY_TIMEOUT: Duration = Duration::from_secs(2);

/// The AWS region this server runs in, reading variables through `env`:
/// from the ECS task metadata endpoint when `ECS_CONTAINER_METADATA_URI_V4`
/// is set, else from EC2 instance metadata (IMDSv2), else `None`.
///
/// As in the AWS SDKs, `AWS_EC2_METADATA_DISABLED=true` skips instance
/// metadata and `AWS_EC2_METADATA_SERVICE_ENDPOINT` replaces its address.
pub(crate) async fn server_region(env: impl Fn(&str) -> Option<String>) -> Option<String> {
    let imds = (!env("AWS_EC2_METADATA_DISABLED")
        .is_some_and(|disabled| disabled.eq_ignore_ascii_case("true")))
    .then(|| {
        env("AWS_EC2_METADATA_SERVICE_ENDPOINT").unwrap_or_else(|| DEFAULT_IMDS_ENDPOINT.to_owned())
    });
    region_from(
        env("ECS_CONTAINER_METADATA_URI_V4").as_deref(),
        imds.as_deref(),
    )
    .await
}

/// [`server_region`] against explicit endpoints.
async fn region_from(ecs: Option<&str>, imds: Option<&str>) -> Option<String> {
    /// One client for every lookup, built on first use. It keeps no idle
    /// connections: lookups are rare, and a pooled connection would outlive
    /// the runtime that opened it.
    static CLIENT: OnceLock<Option<reqwest::Client>> = const { OnceLock::new() };
    // Metadata endpoints are link-local: a proxy would only break them.
    let client = CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(METADATA_TIMEOUT)
                .no_proxy()
                .pool_max_idle_per_host(0)
                .build()
                .ok()
        })
        .as_ref()?;
    let from_ecs = match ecs {
        Some(ecs) => ecs_region(client, ecs).await,
        None => None,
    };
    let Some(region) = from_ecs else {
        return match imds {
            Some(imds) => imds_region(client, imds).await,
            None => None,
        };
    };
    Some(region)
}

/// The region in the ECS task's ARN, `arn:aws:ecs:<region>:<account>:task/...`.
async fn ecs_region(client: &reqwest::Client, endpoint: &str) -> Option<String> {
    let task = client
        .get(format!("{endpoint}/task"))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()?;
    serde_json::from_str::<serde_json::Value>(&task)
        .ok()?
        .get("TaskARN")?
        .as_str()?
        .split(':')
        .nth(3)
        .filter(|region| !region.is_empty())
        .map(str::to_owned)
}

/// The instance's region from IMDSv2: a session token, then the region.
async fn imds_region(client: &reqwest::Client, endpoint: &str) -> Option<String> {
    let token = client
        .put(format!("{endpoint}/latest/api/token"))
        .header("X-aws-ec2-metadata-token-ttl-seconds", "60")
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()?;
    client
        .get(format!("{endpoint}/latest/meta-data/placement/region"))
        .header("X-aws-ec2-metadata-token", token)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()
        .map(|region| region.trim().to_owned())
        .filter(|region| !region.is_empty())
}

/// The region AWS S3 reports for `bucket`, from an unsigned `HEAD`.
pub(crate) async fn bucket_region(bucket: &str) -> Result<String, String> {
    let options = object_store::ClientOptions::new()
        .with_timeout(BUCKET_REGION_TIMEOUT)
        .with_connect_timeout(METADATA_TIMEOUT);
    object_store::aws::resolve_bucket_region(bucket, &options)
        .await
        .map_err(|error| error.to_string())
}

/// What [`probe_latency`] measured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Latency {
    /// The median of [`LATENCY_SAMPLES`] completed round trips.
    Measured(Duration),
    /// One round trip took longer than [`LATENCY_TIMEOUT`].
    TimedOut,
    /// A request failed before reaching the store.
    Failed(String),
}

/// Times [`LATENCY_SAMPLES`] sequential `HEAD`s of `key`, which should not
/// exist: "not found" and "permission denied" are completed round trips.
pub(crate) async fn probe_latency(
    store: &dyn ObjectStore,
    key: &object_store::path::Path,
) -> Latency {
    let mut samples = Vec::with_capacity(LATENCY_SAMPLES);
    for _ in 0..LATENCY_SAMPLES {
        let started = tokio::time::Instant::now();
        let Ok(outcome) = tokio::time::timeout(LATENCY_TIMEOUT, store.head(key)).await else {
            return Latency::TimedOut;
        };
        match outcome {
            Ok(_)
            | Err(
                object_store::Error::NotFound { .. }
                | object_store::Error::PermissionDenied { .. }
                | object_store::Error::Unauthenticated { .. },
            ) => samples.push(started.elapsed()),
            Err(error) => return Latency::Failed(error.to_string()),
        }
    }
    samples.sort_unstable();
    Latency::Measured(samples[LATENCY_SAMPLES / 2])
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;
    use axum::routing::{get, put};
    use axum::Router;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt as _;

    use super::*;

    /// Serves `router` on a loopback port and returns its base URL.
    async fn serve(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await });
        format!("http://{address}")
    }

    /// An IMDSv2 endpoint that answers only token-bearing region requests.
    fn imds(region: &'static str) -> Router {
        Router::new()
            .route(
                "/latest/api/token",
                // A request without the IMDSv2 headers panics the handler,
                // which drops the connection and so fails the lookup.
                put(|headers: HeaderMap| async move {
                    assert_eq!(headers["x-aws-ec2-metadata-token-ttl-seconds"], "60");
                    "token-1"
                }),
            )
            .route(
                "/latest/meta-data/placement/region",
                get(move |headers: HeaderMap| async move {
                    assert_eq!(headers["x-aws-ec2-metadata-token"], "token-1");
                    region
                }),
            )
    }

    fn ecs(task: &'static str) -> Router {
        Router::new().route("/task", get(move || async move { task }))
    }

    #[tokio::test]
    async fn the_region_comes_from_ecs_task_metadata_then_instance_metadata() {
        let imds_url = serve(imds("eu-west-2\n")).await;
        let ecs_url = serve(ecs(
            r#"{"TaskARN":"arn:aws:ecs:ap-southeast-2:123456789012:task/cluster/abc"}"#,
        ))
        .await;
        let broken_ecs = serve(ecs(r#"{"TaskARN":"not-an-arn"}"#)).await;

        assert_eq!(
            region_from(Some(&ecs_url), Some(&imds_url))
                .await
                .as_deref(),
            Some("ap-southeast-2")
        );
        assert_eq!(
            region_from(Some(&broken_ecs), Some(&imds_url))
                .await
                .as_deref(),
            Some("eu-west-2"),
            "an ECS answer without a region falls back to instance metadata"
        );
        assert_eq!(
            region_from(None, Some(&imds_url)).await.as_deref(),
            Some("eu-west-2")
        );
        assert_eq!(region_from(Some(&broken_ecs), None).await, None);
        assert_eq!(region_from(None, None).await, None);
    }

    #[tokio::test]
    async fn metadata_failures_mean_no_region() {
        let empty_region = serve(imds("  \n")).await;
        let no_token = serve(Router::new()).await;
        let not_json = serve(ecs("not json")).await;
        // Nothing listens on a port that was just released.
        let closed = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", listener.local_addr().unwrap())
        };
        for imds in [&empty_region, &no_token, &closed] {
            assert_eq!(region_from(None, Some(imds)).await, None, "{imds}");
        }
        assert_eq!(region_from(Some(&not_json), None).await, None);
        assert_eq!(region_from(Some(&closed), None).await, None);
    }

    #[tokio::test]
    async fn the_server_region_honours_the_sdk_environment() {
        let imds_url = serve(imds("eu-west-2")).await;
        let ecs_url = serve(ecs(r#"{"TaskARN":"arn:aws:ecs:sa-east-1:1:task/c/t"}"#)).await;
        let env = |pairs: Vec<(&'static str, String)>| {
            move |name: &str| {
                pairs
                    .iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| value.clone())
            }
        };
        assert_eq!(
            server_region(env(vec![(
                "AWS_EC2_METADATA_SERVICE_ENDPOINT",
                imds_url.clone()
            )]))
            .await
            .as_deref(),
            Some("eu-west-2")
        );
        assert_eq!(
            server_region(env(vec![
                ("AWS_EC2_METADATA_DISABLED", "True".into()),
                ("AWS_EC2_METADATA_SERVICE_ENDPOINT", imds_url.clone()),
            ]))
            .await,
            None,
            "disabled instance metadata is never asked"
        );
        assert_eq!(
            server_region(env(vec![
                ("AWS_EC2_METADATA_DISABLED", "true".into()),
                ("ECS_CONTAINER_METADATA_URI_V4", ecs_url),
            ]))
            .await
            .as_deref(),
            Some("sa-east-1")
        );
    }

    #[tokio::test]
    async fn latency_is_the_median_round_trip_of_a_missing_key() {
        let store = InMemory::new();
        let key = object_store::path::Path::from("db/.helix-diagnostics-probe");
        assert!(matches!(
            probe_latency(&store, &key).await,
            Latency::Measured(median) if median < LATENCY_TIMEOUT
        ));

        store
            .put(&key, object_store::PutPayload::from_static(b"present"))
            .await
            .unwrap();
        assert!(matches!(
            probe_latency(&store, &key).await,
            Latency::Measured(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_round_trip_ends_the_probe() {
        let stalled = object_store::throttle::ThrottledStore::new(
            InMemory::new(),
            object_store::throttle::ThrottleConfig {
                wait_get_per_call: LATENCY_TIMEOUT * 2,
                ..object_store::throttle::ThrottleConfig::default()
            },
        );
        let key = object_store::path::Path::from("db/.helix-diagnostics-probe");
        assert_eq!(probe_latency(&stalled, &key).await, Latency::TimedOut);
    }

    #[tokio::test]
    async fn a_request_that_never_reaches_the_store_fails_the_probe() {
        // Nothing listens on a port that was just released.
        let closed = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", listener.local_addr().unwrap())
        };
        let unreachable = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_endpoint(closed)
            .with_allow_http(true)
            .with_access_key_id("key")
            .with_secret_access_key("secret")
            .with_retry(object_store::RetryConfig {
                backoff: object_store::BackoffConfig::default(),
                max_retries: 0,
                retry_timeout: LATENCY_TIMEOUT,
            })
            .build()
            .unwrap();
        let key = object_store::path::Path::from("db/.helix-diagnostics-probe");
        assert!(matches!(
            probe_latency(&unreachable, &key).await,
            Latency::Failed(_)
        ));
    }
}
