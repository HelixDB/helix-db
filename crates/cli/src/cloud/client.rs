//! WorkOS-session authenticated Helix Cloud client.
//!
//! This is the only Cloud transport used by the CLI. It never accepts an API
//! key or service credential and retries only the typed WFE rejection that is
//! guaranteed to occur before handler dispatch.

use crate::{paths, service_endpoints};
use eyre::{eyre, Result, WrapErr as _};
use fs2::FileExt as _;
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const REFRESH_WINDOW_SECONDS: i64 = 60;
/// The largest page WFE serves; larger requests silently fall back to 50.
const PAGE_SIZE: &str = "200";
/// Upper bound on pages followed by [`CloudClient::list`] (10,000 items), so a
/// server that never ends pagination cannot hang the CLI.
const MAX_PAGES: usize = 50;
const PRE_DISPATCH_AUTH_REASON: &str = "SESSION_REJECTED_PRE_DISPATCH";
/// Generous because the server waits on identity providers and may be setting up
/// a first-time account.
const PUBLIC_POST_TIMEOUT: Duration = Duration::from_secs(60);
/// The longest error body without a JSON message that is still worth printing.
const MAX_PLAIN_ERROR_BODY: usize = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SessionCredentials {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
    pub email: String,
}

impl SessionCredentials {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.access_token.trim().is_empty()
            || self.refresh_token.trim().is_empty()
            || self.expires_at <= 0
            || self.email.trim().is_empty()
        {
            return Err(eyre!("stored WorkOS session is incomplete"));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RefreshResponse {
    access_token: String,
    refresh_token: String,
    #[serde(deserialize_with = "deserialize_i64")]
    expires_at: i64,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JsonI64 {
    Number(i64),
    String(String),
}

pub(crate) fn deserialize_i64<'de, D>(deserializer: D) -> std::result::Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    match JsonI64::deserialize(deserializer)? {
        JsonI64::Number(value) => Ok(value),
        JsonI64::String(value) => value.parse().map_err(serde::de::Error::custom),
    }
}

#[derive(Debug)]
struct RawResponse {
    status: StatusCode,
    body: Vec<u8>,
}

impl RawResponse {
    async fn read(request: reqwest::RequestBuilder) -> Result<Self> {
        let response = request.send().await?;
        let status = response.status();
        let body = response.bytes().await?.to_vec();
        Ok(Self { status, body })
    }

    fn is_pre_dispatch_auth_rejection(&self) -> bool {
        if self.status != StatusCode::UNAUTHORIZED {
            return false;
        }
        serde_json::from_slice::<Value>(&self.body)
            .ok()
            .and_then(|body| body.get("details").and_then(Value::as_array).cloned())
            .is_some_and(|details| {
                details.iter().any(|detail| {
                    detail.get("reason").and_then(Value::as_str) == Some(PRE_DISPATCH_AUTH_REASON)
                })
            })
    }

    fn into_value(self, action: &str) -> Result<Value> {
        if !self.status.is_success() {
            let message = serde_json::from_slice::<Value>(&self.body)
                .ok()
                .and_then(|body| {
                    body.get("message")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| {
                    // Without a JSON message this may be a proxy's error page,
                    // which says nothing the status does not and floods the
                    // terminal.
                    let body = String::from_utf8_lossy(&self.body);
                    let body = body.trim();
                    if body.starts_with('<') || body.len() > MAX_PLAIN_ERROR_BODY {
                        String::new()
                    } else {
                        body.to_owned()
                    }
                });
            return Err(HttpError {
                action: action.to_owned(),
                status: self.status,
                message,
            }
            .into());
        }
        if self.body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&self.body).wrap_err_with(|| format!("decode {action} response"))
    }
}

/// A non-success WFE response, typed so callers can react to the status
/// (e.g. fall back to a search only on "not found").
#[derive(Debug, thiserror::Error)]
#[error("Failed to {action}: HTTP {status} {message}")]
pub struct HttpError {
    pub action: String,
    pub status: StatusCode,
    pub message: String,
}

struct CredentialLock {
    file: File,
}

impl CredentialLock {
    fn acquire(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .wrap_err_with(|| format!("open credential lock {}", path.display()))?;
        file.lock_exclusive()
            .wrap_err_with(|| format!("lock credentials {}", path.display()))?;
        Ok(Self { file })
    }
}

impl Drop for CredentialLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

#[derive(Clone)]
pub struct CloudClient {
    http: reqwest::Client,
    base_url: String,
    credentials_path: PathBuf,
    lock_path: PathBuf,
    public_post_timeout: Duration,
}

impl CloudClient {
    pub fn new() -> Result<Self> {
        let home = paths::helix_home_dir()?;
        Self::with_paths(
            service_endpoints::url(service_endpoints::ServiceEndpoint::Cloud),
            home.join("credentials"),
        )
    }

    pub fn with_paths(base_url: String, credentials_path: PathBuf) -> Result<Self> {
        let lock_path = credentials_path.with_extension("lock");
        Ok(Self {
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            base_url: base_url.trim_end_matches('/').to_owned(),
            credentials_path,
            lock_path,
            public_post_timeout: PUBLIC_POST_TIMEOUT,
        })
    }

    pub fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    pub fn load_session(&self) -> Result<SessionCredentials> {
        load_session(&self.credentials_path)
    }

    pub fn store_session(&self, session: &SessionCredentials) -> Result<()> {
        let _lock = CredentialLock::acquire(&self.lock_path)?;
        persist_session(&self.credentials_path, session)
    }

    pub fn remove_session(&self) -> Result<()> {
        let _lock = CredentialLock::acquire(&self.lock_path)?;
        match fs::remove_file(&self.credentials_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).wrap_err_with(|| {
                format!("remove credentials {}", self.credentials_path.display())
            }),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_public_post_timeout(mut self, timeout: Duration) -> Self {
        self.public_post_timeout = timeout;
        self
    }

    /// Whether requests reach the server protected: over HTTPS, or without
    /// leaving this machine.
    pub(crate) fn has_secure_transport(&self) -> bool {
        reqwest::Url::parse(&self.base_url).is_ok_and(|url| {
            url.scheme() == "https"
                || url.host_str().is_some_and(|host| {
                    host == "localhost"
                        || host
                            .trim_matches(['[', ']'])
                            .parse::<std::net::IpAddr>()
                            .is_ok_and(|address| address.is_loopback())
                })
        })
    }

    /// POSTs without a session. These are the login requests, which run with
    /// someone waiting at a terminal, so a stalled server fails them after
    /// [`PUBLIC_POST_TIMEOUT`] rather than hanging the login. A request that gets
    /// no answer is an error that downcasts to [`reqwest::Error`].
    pub async fn public_post(&self, path: &str, body: Value, action: &str) -> Result<Value> {
        let request = self
            .request(None, Method::POST, path, &[], Some(body))
            .timeout(self.public_post_timeout);
        RawResponse::read(request)
            .await
            .wrap_err_with(|| format!("Failed to {action}: could not reach Helix Cloud"))?
            .into_value(action)
    }

    pub async fn get(&self, path: &str, action: &str) -> Result<Value> {
        self.authenticated_request(Method::GET, path, &[], None, action)
            .await
    }

    /// GET a single resource and deserialize it.
    pub async fn fetch<T: DeserializeOwned>(&self, path: &str, action: &str) -> Result<T> {
        let value = self.get(path, action).await?;
        serde_json::from_value(value).wrap_err_with(|| format!("decode {action} response"))
    }

    /// GET every page of a collection, following `nextPageToken`, and
    /// deserialize the items under `field`.
    pub async fn list<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, &str)],
        field: &str,
        action: &str,
    ) -> Result<Vec<T>> {
        let mut items = Vec::new();
        let mut token = String::new();
        for _ in 0..MAX_PAGES {
            let mut page_query = query.to_vec();
            page_query.push(("page.size", PAGE_SIZE));
            if !token.is_empty() {
                page_query.push(("page.token", &token));
            }
            let mut page = self
                .authenticated_request(Method::GET, path, &page_query, None, action)
                .await?;
            // WFE emits empty collections as `[]`, so a missing field means
            // a malformed response, not an empty one.
            let Some(page_items) = page.get_mut(field).map(Value::take) else {
                return Err(eyre!(
                    "Failed to {action}: the response has no `{field}` list"
                ));
            };
            items.extend(
                serde_json::from_value::<Vec<T>>(page_items)
                    .wrap_err_with(|| format!("decode {action} response"))?,
            );
            let next = page
                .get("nextPageToken")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if next.is_empty() {
                return Ok(items);
            }
            if next == token {
                return Err(eyre!(
                    "Failed to {action}: the server repeated a page token"
                ));
            }
            token = next.to_owned();
        }
        Err(eyre!(
            "Failed to {action}: more than {MAX_PAGES} pages; narrow the request"
        ))
    }

    pub async fn post(&self, path: &str, body: Value, action: &str) -> Result<Value> {
        self.authenticated_request(Method::POST, path, &[], Some(body), action)
            .await
    }

    pub async fn patch(&self, path: &str, body: Value, action: &str) -> Result<Value> {
        self.authenticated_request(Method::PATCH, path, &[], Some(body), action)
            .await
    }

    pub async fn delete(&self, path: &str, action: &str) -> Result<Value> {
        self.authenticated_request(Method::DELETE, path, &[], None, action)
            .await
    }

    async fn authenticated_request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Value>,
        action: &str,
    ) -> Result<Value> {
        let session = self.current_session().await?;
        let response = self
            .send(
                Some(&session.access_token),
                method.clone(),
                path,
                query,
                body.clone(),
            )
            .await?;
        if !response.is_pre_dispatch_auth_rejection() {
            return response.into_value(action);
        }

        // This is the only retry path. The WFE interceptor guarantees this
        // typed error happened before handler dispatch.
        let refreshed = self.refresh_after_rejection(&session.access_token).await?;
        self.send(Some(&refreshed.access_token), method, path, query, body)
            .await?
            .into_value(action)
    }

    /// Take the credential lock without blocking a runtime worker: `flock`
    /// blocks, and the holder may be awaiting a refresh on another task, so
    /// blocking every worker on the lock would deadlock concurrent requests.
    async fn lock(&self) -> Result<CredentialLock> {
        let path = self.lock_path.clone();
        tokio::task::spawn_blocking(move || CredentialLock::acquire(&path)).await?
    }

    async fn current_session(&self) -> Result<SessionCredentials> {
        let _lock = self.lock().await?;
        let session = load_session(&self.credentials_path).map_err(|error| {
            eyre!("{error}. Run 'helix auth login' to create a WorkOS session.")
        })?;
        if session.expires_at > unix_now() + REFRESH_WINDOW_SECONDS {
            return Ok(session);
        }
        self.refresh_locked(session).await
    }

    async fn refresh_after_rejection(
        &self,
        rejected_access_token: &str,
    ) -> Result<SessionCredentials> {
        let _lock = self.lock().await?;
        let session = load_session(&self.credentials_path)?;
        if session.access_token != rejected_access_token
            && session.expires_at > unix_now() + REFRESH_WINDOW_SECONDS
        {
            return Ok(session);
        }
        self.refresh_locked(session).await
    }

    async fn refresh_locked(&self, session: SessionCredentials) -> Result<SessionCredentials> {
        let response = self
            .send(
                None,
                Method::POST,
                "/v1/auth/refresh",
                &[],
                Some(serde_json::json!({"refreshToken": session.refresh_token})),
            )
            .await?;
        let value = response.into_value("refresh WorkOS session")?;
        let rotated: RefreshResponse = serde_json::from_value(value)?;
        let refreshed = SessionCredentials {
            access_token: rotated.access_token,
            refresh_token: rotated.refresh_token,
            expires_at: rotated.expires_at,
            email: session.email,
        };
        persist_session(&self.credentials_path, &refreshed)?;
        Ok(refreshed)
    }

    async fn send(
        &self,
        access_token: Option<&str>,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Value>,
    ) -> Result<RawResponse> {
        RawResponse::read(self.request(access_token, method, path, query, body)).await
    }

    fn request(
        &self,
        access_token: Option<&str>,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Value>,
    ) -> reqwest::RequestBuilder {
        let mut request = self
            .http
            .request(method, format!("{}{}", self.base_url, path))
            .query(query);
        if let Some(access_token) = access_token {
            request = request.bearer_auth(access_token);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        request
    }
}

fn load_session(path: &Path) -> Result<SessionCredentials> {
    let raw = fs::read(path).wrap_err_with(|| format!("read credentials {}", path.display()))?;
    let session: SessionCredentials = serde_json::from_slice(&raw).map_err(|_| {
        eyre!(
            "obsolete or invalid Cloud credentials at {}; API-key credentials are not supported",
            path.display()
        )
    })?;
    session.validate()?;
    Ok(session)
}

fn persist_session(path: &Path, session: &SessionCredentials) -> Result<()> {
    session.validate()?;
    let Some(parent) = path.parent() else {
        return Err(eyre!("credential path has no parent"));
    };
    fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        let temporary = parent.join(format!(".credentials.tmp.{}", std::process::id()));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(session)?)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let temporary = parent.join(format!(".credentials.tmp.{}", std::process::id()));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(session)?)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
    }
    Ok(())
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionCredentials {
        SessionCredentials {
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            expires_at: unix_now() + 3600,
            email: "user@example.com".into(),
        }
    }

    fn client(base_url: &str) -> CloudClient {
        CloudClient::with_paths(base_url.to_owned(), PathBuf::from("credentials")).unwrap()
    }

    #[test]
    fn only_https_and_this_machine_count_as_secure_transport() {
        for secure in [
            "https://api.prod.helix-db.com",
            "https://localhost.example.com",
            "http://localhost:8080",
            "http://127.0.0.1:8080",
            "http://127.8.0.1",
            "http://[::1]:8080",
        ] {
            assert!(client(secure).has_secure_transport(), "{secure}");
        }
        for insecure in [
            "http://api.prod.helix-db.com",
            "http://staging",
            "http://localhost.example.com",
            "http://127.0.0.1.example.com",
            "http://10.0.0.1",
            "http://[2001:db8::1]",
            "not a url",
        ] {
            assert!(!client(insecure).has_secure_transport(), "{insecure}");
        }
    }

    #[tokio::test]
    async fn public_post_gives_up_on_a_stalled_server() {
        // Connections sit in the backlog: accepted by the kernel, never answered.
        let stalled = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = client(&format!("http://{}", stalled.local_addr().unwrap()))
            .with_public_post_timeout(Duration::from_millis(200));

        let error = client
            .public_post("/v1/auth/password:sign-in", Value::Null, "sign in")
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Failed to sign in: could not reach Helix Cloud"
        );
        assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_timeout());
    }

    #[test]
    fn error_messages_skip_pages_a_proxy_sent() {
        let message = |body: &str| {
            let response = RawResponse {
                status: StatusCode::BAD_GATEWAY,
                body: body.as_bytes().to_vec(),
            };
            let error = response.into_value("act").unwrap_err();
            error.downcast_ref::<HttpError>().unwrap().message.clone()
        };
        let long = "x".repeat(MAX_PLAIN_ERROR_BODY + 1);

        assert_eq!(
            message(r#"{"message":"from the server"}"#),
            "from the server"
        );
        assert_eq!(message(&format!(r#"{{"message":"{long}"}}"#)), long);
        assert_eq!(message("  upstream unavailable\n"), "upstream unavailable");
        assert_eq!(message("<html><body>Bad gateway</body></html>"), "");
        assert_eq!(message(&long), "");
        assert_eq!(message(""), "");
    }

    #[test]
    fn session_file_is_strict_and_atomic() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credentials");
        let expected = session();
        persist_session(&path, &expected).unwrap();
        assert_eq!(load_session(&path).unwrap(), expected);

        fs::write(&path, ["helix", "_user", "_key=obsolete"].concat()).unwrap();
        assert!(load_session(&path)
            .unwrap_err()
            .to_string()
            .contains("API-key credentials"));
        fs::write(&path, r#"{"access_token":"a","refresh_token":"r","expires_at":1,"email":"e","api_key":"forbidden"}"#).unwrap();
        assert!(load_session(&path).is_err());
    }

    #[test]
    fn only_typed_pre_dispatch_rejection_is_retryable() {
        let typed = RawResponse {
            status: StatusCode::UNAUTHORIZED,
            body: serde_json::to_vec(
                &serde_json::json!({"details":[{"reason":PRE_DISPATCH_AUTH_REASON}]}),
            )
            .unwrap(),
        };
        assert!(typed.is_pre_dispatch_auth_rejection());
        for response in [
            RawResponse {
                status: StatusCode::UNAUTHORIZED,
                body: br#"{"message":"expired"}"#.to_vec(),
            },
            RawResponse {
                status: StatusCode::GATEWAY_TIMEOUT,
                body: Vec::new(),
            },
            RawResponse {
                status: StatusCode::BAD_GATEWAY,
                body: serde_json::to_vec(
                    &serde_json::json!({"details":[{"reason":PRE_DISPATCH_AUTH_REASON}]}),
                )
                .unwrap(),
            },
        ] {
            assert!(!response.is_pre_dispatch_auth_rejection());
        }
    }

    #[test]
    fn refresh_response_accepts_protobuf_json_int64() {
        let response: RefreshResponse = serde_json::from_str(
            r#"{"accessToken":"access","refreshToken":"refresh","expiresAt":"1777000000"}"#,
        )
        .unwrap();
        assert_eq!(response.expires_at, 1_777_000_000);
    }
}
