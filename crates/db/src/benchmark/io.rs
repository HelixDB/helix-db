//! HTTP-attempt diagnostics below object_store's retry loop.
//!
//! Delegates to its normal reqwest connector without changing retries, timeouts,
//! signing, pooling or response bodies. Offered upload bytes and delivered download
//! bytes are application measurements, NOT wire bytes. EC2 packet counters must
//! independently account for buffering, TLS/HTTP overhead and TCP retransmission.
//! Counts include object_store retries, but redirects and protocol-level retries
//! inside reqwest are below this hook. Do not label them wire request counts.

use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};

use async_trait::async_trait;
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use parking_lot::Mutex;
use serde::Serialize;
use slatedb::object_store::{self, client, ClientOptions};

static METRICS: OnceLock<Arc<Metrics>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
enum Service {
    S3,
    NonS3OrUnsigned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "UPPERCASE")]
enum Method {
    Get,
    Head,
    Put,
    Post,
    Delete,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct Key {
    service: Service,
    method: Method,
}

#[derive(Debug, Default, Clone, Serialize)]
struct Counters {
    connector_attempts: u64,
    request_body_bytes_offered: u64,
    response_body_bytes_delivered: u64,
    in_flight: u64,
    peak_in_flight: u64,
    headers: BTreeMap<u16, u64>,
    transport_errors: BTreeMap<&'static str, u64>,
    cancelled_before_headers: u64,
    bodies_complete: u64,
    bodies_failed: u64,
    bodies_dropped: u64,
}

#[derive(Debug, Default)]
struct Metrics(Mutex<BTreeMap<Key, Counters>>);

#[derive(Serialize)]
struct Sample {
    #[serde(flatten)]
    key: Key,
    #[serde(flatten)]
    counters: Counters,
}

impl Metrics {
    fn snapshot(&self) -> Vec<Sample> {
        self.0
            .lock()
            .iter()
            .map(|(key, counters)| Sample {
                key: *key,
                counters: counters.clone(),
            })
            .collect()
    }
}

/// Wrapper for the dependency's own connector, including credential-provider I/O.
#[derive(Debug, Clone)]
pub(crate) struct Connector {
    metrics: Arc<Metrics>,
}

impl Default for Connector {
    fn default() -> Self {
        Self {
            metrics: METRICS.get_or_init(|| Arc::new(Metrics::default())).clone(),
        }
    }
}

impl client::HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<client::HttpClient> {
        let inner = client::ReqwestConnector::default().connect(options)?;
        Ok(client::HttpClient::new(ObservedClient {
            inner,
            metrics: self.metrics.clone(),
        }))
    }
}

#[derive(Debug)]
struct ObservedClient {
    inner: client::HttpClient,
    metrics: Arc<Metrics>,
}

struct Attempt {
    metrics: Arc<Metrics>,
    key: Key,
}

/// Ownership of an unresolved attempt makes cancellation observable on drop.
struct AwaitingHeaders(Option<Attempt>);

impl Drop for AwaitingHeaders {
    fn drop(&mut self) {
        let Some(attempt) = self.0.take() else {
            return;
        };
        let mut metrics = attempt.metrics.0.lock();
        let counters = metrics.get_mut(&attempt.key).expect("attempt was started");
        counters.cancelled_before_headers += 1;
        counters.in_flight -= 1;
    }
}

fn error_kind(error: &client::HttpError) -> &'static str {
    match error.kind() {
        client::HttpErrorKind::Connect => "connect",
        client::HttpErrorKind::Request => "request",
        client::HttpErrorKind::Timeout => "timeout",
        client::HttpErrorKind::Interrupted => "interrupted",
        client::HttpErrorKind::Decode => "decode",
        client::HttpErrorKind::Unknown => "unknown",
        _ => "unrecognized",
    }
}

#[async_trait]
impl client::HttpService for ObservedClient {
    async fn call(
        &self,
        request: client::HttpRequest,
    ) -> Result<client::HttpResponse, client::HttpError> {
        // Do not log URLs, keys, headers, credentials, or payload contents. The
        // signing service distinguishes S3 traffic from IMDS/STS credential calls.
        let signed_s3 = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value.starts_with("AWS4-HMAC-SHA256 ") && value.contains("/s3/aws4_request,")
            });
        let key = Key {
            service: if signed_s3 {
                Service::S3
            } else {
                Service::NonS3OrUnsigned
            },
            method: match request.method().as_str() {
                "GET" => Method::Get,
                "HEAD" => Method::Head,
                "PUT" => Method::Put,
                "POST" => Method::Post,
                "DELETE" => Method::Delete,
                _ => Method::Other,
            },
        };
        {
            let mut metrics = self.metrics.0.lock();
            let counters = metrics.entry(key).or_default();
            counters.connector_attempts += 1;
            counters.request_body_bytes_offered +=
                u64::try_from(request.body().content_length()).expect("HTTP body fits u64");
            counters.in_flight += 1;
            counters.peak_in_flight = counters.peak_in_flight.max(counters.in_flight);
        }
        let mut awaiting = AwaitingHeaders(Some(Attempt {
            metrics: self.metrics.clone(),
            key,
        }));
        let response = self.inner.execute(request).await;
        let attempt = awaiting.0.take().expect("one result for one attempt");
        match response {
            Err(error) => {
                let mut metrics = attempt.metrics.0.lock();
                let counters = metrics.get_mut(&key).expect("attempt was started");
                *counters
                    .transport_errors
                    .entry(error_kind(&error))
                    .or_default() += 1;
                counters.in_flight -= 1;
                Err(error)
            }
            Ok(response) => {
                let (parts, body) = response.into_parts();
                *attempt
                    .metrics
                    .0
                    .lock()
                    .get_mut(&key)
                    .expect("attempt was started")
                    .headers
                    .entry(parts.status.as_u16())
                    .or_default() += 1;
                let mut body = ObservedBody {
                    inner: body,
                    record: Some(BodyRecord {
                        attempt,
                        delivered: 0,
                    }),
                };
                if body.inner.is_end_stream() {
                    body.finish(BodyEnd::Complete);
                }
                Ok(client::HttpResponse::from_parts(
                    parts,
                    client::HttpResponseBody::new(body),
                ))
            }
        }
    }
}

struct BodyRecord {
    attempt: Attempt,
    delivered: u64,
}
struct ObservedBody {
    inner: client::HttpResponseBody,
    record: Option<BodyRecord>,
}
enum BodyEnd {
    Complete,
    Failed,
    Dropped,
}

impl ObservedBody {
    fn finish(&mut self, end: BodyEnd) {
        let Some(record) = self.record.take() else {
            return;
        };
        let mut metrics = record.attempt.metrics.0.lock();
        let counters = metrics
            .get_mut(&record.attempt.key)
            .expect("attempt was started");
        counters.response_body_bytes_delivered += record.delivered;
        counters.in_flight -= 1;
        match end {
            BodyEnd::Complete => counters.bodies_complete += 1,
            BodyEnd::Failed => counters.bodies_failed += 1,
            BodyEnd::Dropped => counters.bodies_dropped += 1,
        }
    }
}

impl Drop for ObservedBody {
    fn drop(&mut self) {
        self.finish(BodyEnd::Dropped);
    }
}

impl Body for ObservedBody {
    type Data = Bytes;
    type Error = client::HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        match &result {
            Poll::Ready(Some(Ok(frame))) => {
                let Some(record) = self.record.as_mut() else {
                    return result;
                };
                record.delivered += u64::try_from(frame.data_ref().map_or(0, Bytes::len))
                    .expect("HTTP frame fits u64");
                if self.inner.is_end_stream() {
                    self.finish(BodyEnd::Complete);
                }
            }
            Poll::Ready(Some(Err(_))) => self.finish(BodyEnd::Failed),
            Poll::Ready(None) => self.finish(BodyEnd::Complete),
            Poll::Pending => {}
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// Returns the process-wide counters, or an empty list before any client
/// connected through [`Connector`].
pub(super) fn snapshot() -> serde_json::Value {
    let Some(metrics) = METRICS.get() else {
        return serde_json::Value::Array(Vec::new());
    };
    serde_json::to_value(metrics.snapshot()).expect("connector counters are JSON representable")
}

#[cfg(test)]
mod tests {
    use super::*;
    use client::{HttpConnector, HttpService};
    use std::collections::VecDeque;

    #[derive(Debug)]
    enum Reply {
        Response(client::HttpResponse),
        Error(client::HttpErrorKind),
        Pending,
    }

    #[derive(Debug, Clone)]
    struct Script(Arc<Mutex<VecDeque<Reply>>>);

    #[async_trait]
    impl HttpService for Script {
        async fn call(
            &self,
            _: client::HttpRequest,
        ) -> Result<client::HttpResponse, client::HttpError> {
            let next = self.0.lock().pop_front().expect("unexpected HTTP attempt");
            match next {
                Reply::Response(response) => Ok(response),
                Reply::Error(kind) => Err(client::HttpError::new(
                    kind,
                    std::io::Error::other("fixture"),
                )),
                Reply::Pending => std::future::pending().await,
            }
        }
    }

    #[derive(Debug, Clone)]
    struct ScriptConnector {
        script: Script,
        metrics: Arc<Metrics>,
    }

    impl HttpConnector for ScriptConnector {
        fn connect(&self, _: &ClientOptions) -> object_store::Result<client::HttpClient> {
            Ok(client::HttpClient::new(ObservedClient {
                inner: client::HttpClient::new(self.script.clone()),
                metrics: self.metrics.clone(),
            }))
        }
    }

    fn response(status: u16, body: impl Into<Bytes>) -> client::HttpResponse {
        let mut response = client::HttpResponse::new(client::HttpResponseBody::from(body.into()));
        *response.status_mut() = status.try_into().unwrap();
        response
            .headers_mut()
            .insert("etag", "\"fixture\"".parse().unwrap());
        response
    }

    fn request(method: &str, signed: bool) -> client::HttpRequest {
        let mut request = client::HttpRequest::new(Bytes::from_static(b"upload").into());
        *request.method_mut() = method.parse().unwrap();
        *request.uri_mut() = "https://private.example/private-object".parse().unwrap();
        if signed {
            request.headers_mut().insert("authorization", "AWS4-HMAC-SHA256 Credential=secret/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=secret".parse().unwrap());
        }
        request
    }

    fn client(replies: Vec<Reply>) -> (client::HttpClient, Arc<Metrics>) {
        let metrics = Arc::new(Metrics::default());
        let connector = ScriptConnector {
            script: Script(Arc::new(Mutex::new(replies.into()))),
            metrics: metrics.clone(),
        };
        (
            connector.connect(&ClientOptions::default()).unwrap(),
            metrics,
        )
    }

    #[tokio::test]
    async fn complete_empty_and_abandoned_bodies_preserve_http_semantics() {
        let (client, metrics) = client(vec![
            Reply::Response(response(200, "download")),
            Reply::Response(response(204, "")),
            Reply::Response(response(404, "unread")),
        ]);
        let response = client.execute(request("GET", true)).await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(response.headers()["etag"], "\"fixture\"");
        assert_eq!(response.body().size_hint().exact(), Some(8));
        assert_eq!(metrics.snapshot()[0].counters.in_flight, 1);
        assert_eq!(response.into_body().bytes().await.unwrap(), "download");
        drop(client.execute(request("GET", true)).await.unwrap());
        drop(client.execute(request("GET", true)).await.unwrap());
        let snapshot = metrics.snapshot();
        let counters = &snapshot[0].counters;
        assert_eq!(snapshot[0].key.service, Service::S3);
        assert_eq!(counters.connector_attempts, 3);
        assert_eq!(counters.request_body_bytes_offered, 18);
        assert_eq!(counters.response_body_bytes_delivered, 8);
        assert_eq!(
            (
                counters.bodies_complete,
                counters.bodies_dropped,
                counters.in_flight
            ),
            (2, 1, 0)
        );
        assert_eq!(
            counters.headers,
            BTreeMap::from([(200, 1), (204, 1), (404, 1)])
        );
        let json = serde_json::to_string(&snapshot).unwrap();
        for sensitive in [
            "secret",
            "private.example",
            "private-object",
            "upload",
            "download",
        ] {
            assert!(!json.contains(sensitive));
        }
    }

    #[tokio::test]
    async fn transport_errors_keep_retry_classification_and_cancellation_is_counted() {
        let kinds = [
            client::HttpErrorKind::Connect,
            client::HttpErrorKind::Request,
            client::HttpErrorKind::Timeout,
            client::HttpErrorKind::Interrupted,
            client::HttpErrorKind::Decode,
            client::HttpErrorKind::Unknown,
        ];
        let (client, metrics) = client(
            kinds
                .iter()
                .copied()
                .map(Reply::Error)
                .chain([Reply::Pending])
                .collect(),
        );
        for kind in kinds {
            assert_eq!(
                client
                    .execute(request("PUT", true))
                    .await
                    .unwrap_err()
                    .kind(),
                kind
            );
        }
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(1),
            client.execute(request("PUT", true))
        )
        .await
        .is_err());
        let snapshot = metrics.snapshot();
        let counters = &snapshot[0].counters;
        assert_eq!(counters.connector_attempts, 7);
        assert_eq!(counters.transport_errors.values().sum::<u64>(), 6);
        assert_eq!(counters.cancelled_before_headers, 1);
        assert_eq!(counters.in_flight, 0);
    }

    struct Chunks {
        frames: VecDeque<Result<Frame<Bytes>, client::HttpError>>,
        pending: bool,
    }

    impl Body for Chunks {
        type Data = Bytes;
        type Error = client::HttpError;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            if std::mem::take(&mut self.pending) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(self.frames.pop_front())
        }
    }

    #[tokio::test]
    async fn chunk_errors_trailers_and_partial_drops_account_only_delivered_bytes() {
        use futures::StreamExt;
        let bodies = [
            Chunks {
                frames: VecDeque::from([
                    Ok(Frame::data(Bytes::from_static(b"abc"))),
                    Err(client::HttpError::new(
                        client::HttpErrorKind::Interrupted,
                        std::io::Error::other("stream"),
                    )),
                ]),
                pending: true,
            },
            Chunks {
                frames: VecDeque::from([
                    Ok(Frame::data(Bytes::from_static(b"def"))),
                    Ok(Frame::trailers(Default::default())),
                ]),
                pending: false,
            },
            Chunks {
                frames: VecDeque::from([
                    Ok(Frame::data(Bytes::from_static(b"gh"))),
                    Ok(Frame::data(Bytes::from_static(b"unread"))),
                ]),
                pending: false,
            },
        ];
        let (client, metrics) = client(
            bodies
                .into_iter()
                .map(|body| {
                    Reply::Response(client::HttpResponse::new(client::HttpResponseBody::new(
                        body,
                    )))
                })
                .collect(),
        );
        let mut first = client
            .execute(request("GET", true))
            .await
            .unwrap()
            .into_body()
            .bytes_stream();
        assert_eq!(first.next().await.unwrap().unwrap(), "abc");
        assert_eq!(
            first.next().await.unwrap().unwrap_err().kind(),
            client::HttpErrorKind::Interrupted
        );
        drop(first);
        assert_eq!(
            client
                .execute(request("GET", true))
                .await
                .unwrap()
                .into_body()
                .bytes()
                .await
                .unwrap(),
            "def"
        );
        let mut third = client
            .execute(request("GET", true))
            .await
            .unwrap()
            .into_body()
            .bytes_stream();
        assert_eq!(third.next().await.unwrap().unwrap(), "gh");
        drop(third);
        let snapshot = metrics.snapshot();
        let counters = &snapshot[0].counters;
        assert_eq!(counters.response_body_bytes_delivered, 8);
        assert_eq!(
            (
                counters.bodies_complete,
                counters.bodies_failed,
                counters.bodies_dropped,
                counters.in_flight
            ),
            (1, 1, 1, 0)
        );
    }

    #[tokio::test]
    async fn methods_and_non_s3_traffic_are_separate_and_in_flight_is_exact() {
        let methods = ["GET", "HEAD", "PUT", "POST", "DELETE", "PATCH"];
        let (client, metrics) = client(
            (0..methods.len() * 2)
                .map(|_| Reply::Response(response(200, "body")))
                .collect(),
        );
        let mut responses = Vec::new();
        for signed in [false, true] {
            for method in methods {
                responses.push(client.execute(request(method, signed)).await.unwrap());
            }
        }
        assert_eq!(metrics.snapshot().len(), 12);
        assert!(metrics
            .snapshot()
            .iter()
            .all(|row| row.counters.in_flight == 1));
        drop(responses);
        assert!(metrics
            .snapshot()
            .iter()
            .all(|row| row.counters.in_flight == 0 && row.counters.bodies_dropped == 1));
    }

    #[tokio::test]
    async fn real_sdk_retry_counts_each_attempt_instead_of_each_object_put() {
        use slatedb::object_store::ObjectStoreExt;
        let metrics = Arc::new(Metrics::default());
        let script = Script(Arc::new(Mutex::new(VecDeque::from([
            Reply::Response(response(503, "busy")),
            Reply::Response(response(200, "")),
        ]))));
        let store = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name("fixture")
            .with_region("us-east-1")
            .with_access_key_id("fixture")
            .with_secret_access_key("fixture")
            .with_http_connector(ScriptConnector {
                script: script.clone(),
                metrics: metrics.clone(),
            })
            .build()
            .unwrap();
        store
            .put(
                &object_store::path::Path::from("object"),
                Bytes::from_static(b"payload").into(),
            )
            .await
            .unwrap();
        assert!(script.0.lock().is_empty());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(
            snapshot[0].key,
            Key {
                service: Service::S3,
                method: Method::Put
            }
        );
        let counters = &snapshot[0].counters;
        assert_eq!(
            (
                counters.connector_attempts,
                counters.request_body_bytes_offered,
                counters.in_flight
            ),
            (2, 14, 0)
        );
        assert_eq!(counters.headers, BTreeMap::from([(200, 1), (503, 1)]));
    }
}
