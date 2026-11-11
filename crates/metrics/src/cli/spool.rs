use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use uuid::Uuid;

use super::{CliMetricsError, MetricsLevel};
use crate::telemetry::{self, ClientInfo, Envelope, Event, Source};

const MAX_SPOOL_BYTES: u64 = 16 * 1024 * 1024;
const MAX_SPOOL_AGE: Duration = Duration::from_secs(30 * 24 * 60 * 60);
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

pub(super) fn persist_events(
    root: &Path,
    client: &ClientInfo,
    events: Vec<Event>,
) -> Result<(), CliMetricsError> {
    for envelope in telemetry::encode_envelopes(Source::Cli, client, events)? {
        persist_envelope(root, &envelope)?;
    }
    Ok(())
}

fn persist_envelope(root: &Path, envelope: &[u8]) -> Result<PathBuf, CliMetricsError> {
    Envelope::from_slice(envelope)?;
    let spool = spool_dir(root)?;
    let path = spool.join(format!("{}.json", Uuid::now_v7()));
    let mut temporary = tempfile::NamedTempFile::new_in(&spool)?;
    temporary.write_all(envelope)?;
    temporary.as_file().sync_all()?;
    temporary.persist(&path).map_err(|error| error.error)?;
    Ok(path)
}

pub(super) async fn deliver_pending(
    root: &Path,
    endpoint: &str,
    max_envelopes: usize,
    request_timeout: Duration,
) -> Result<(), CliMetricsError> {
    telemetry::validate_endpoint(endpoint)?;
    let client = reqwest::Client::builder()
        .timeout(request_timeout)
        .build()
        .map_err(telemetry::TelemetryError::from)?;
    deliver_pending_with_client(root, endpoint, max_envelopes, &client).await
}

async fn deliver_pending_with_client(
    root: &Path,
    endpoint: &str,
    max_envelopes: usize,
    client: &reqwest::Client,
) -> Result<(), CliMetricsError> {
    for path in pending_files(root)?.into_iter().take(max_envelopes) {
        let body = fs::read(&path)?;
        if Envelope::from_slice(&body).is_err() {
            fs::remove_file(path)?;
            continue;
        }
        match telemetry::post_envelope(client, endpoint, body).await {
            telemetry::Delivery::Accepted | telemetry::Delivery::Rejected => {
                fs::remove_file(path)?;
            }
            telemetry::Delivery::NoResponse => return Ok(()),
        }
    }
    Ok(())
}

pub(super) fn apply_privacy(root: &Path, level: MetricsLevel) -> Result<(), CliMetricsError> {
    for path in pending_files(root)? {
        if level == MetricsLevel::Off {
            fs::remove_file(path)?;
            continue;
        }
        if level == MetricsLevel::Full {
            continue;
        }
        let mut envelope = match Envelope::from_slice(&fs::read(&path)?) {
            Ok(envelope) => envelope,
            Err(_) => {
                fs::remove_file(path)?;
                continue;
            }
        };
        envelope.client.user_id = None;
        replace_atomically(&path, &envelope.to_vec()?)?;
    }
    Ok(())
}

pub(super) fn cleanup_obsolete(root: &Path) -> Result<(), CliMetricsError> {
    let metrics = root.join("metrics");
    if !metrics.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(&metrics)? {
        let path = entry?.path();
        if path.is_dir() {
            if path == metrics.join("spool") {
                for entry in fs::read_dir(path)? {
                    let path = entry?.path();
                    let keep = path.extension().and_then(|value| value.to_str()) == Some("json")
                        && path
                            .file_stem()
                            .and_then(|value| value.to_str())
                            .is_some_and(|value| Uuid::parse_str(value).is_ok());
                    if !keep && path.is_file() {
                        fs::remove_file(path)?;
                    }
                }
            }
            continue;
        }
        let obsolete = path
            .file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|name| {
                name.ends_with(".pb")
                    || name.ends_with(".tmp")
                    || name.ends_with(".json")
                    || name.ends_with(".json.rejected")
            });
        if obsolete {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

pub(super) fn prune(root: &Path) -> Result<(), CliMetricsError> {
    let now = SystemTime::now();
    let mut files = pending_files(root)?
        .into_iter()
        .filter_map(|path| {
            let metadata = fs::metadata(&path).ok()?;
            let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            Some((path, modified, metadata.len()))
        })
        .collect::<Vec<_>>();

    for (path, modified, _) in &files {
        if now.duration_since(*modified).unwrap_or_default() > MAX_SPOOL_AGE {
            let _ = fs::remove_file(path);
        }
    }

    files.retain(|(path, _, _)| path.exists());
    files.sort_by_key(|(_, modified, _)| *modified);
    let mut total = files.iter().map(|(_, _, len)| *len).sum::<u64>();
    for (path, _, len) in files {
        if total <= MAX_SPOOL_BYTES {
            break;
        }
        if fs::remove_file(path).is_ok() {
            total = total.saturating_sub(len);
        }
    }
    Ok(())
}

fn replace_atomically(path: &Path, contents: &[u8]) -> Result<(), CliMetricsError> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "spool path has no parent")
    })?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(contents)?;
    temporary.as_file().sync_all()?;
    #[cfg(windows)]
    fs::remove_file(path)?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn pending_files(root: &Path) -> Result<Vec<PathBuf>, CliMetricsError> {
    let spool = spool_dir(root)?;
    let mut files = fs::read_dir(spool)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && path.extension().and_then(|value| value.to_str()) == Some("json")
                && path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| Uuid::parse_str(value).is_ok())
        })
        .collect::<Vec<_>>();
    files.sort();
    Ok(files)
}

fn spool_dir(root: &Path) -> Result<PathBuf, CliMetricsError> {
    let spool = root.join("metrics").join("spool");
    fs::create_dir_all(&spool)?;
    Ok(spool)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;

    fn client(user_id: Option<&str>) -> ClientInfo {
        ClientInfo {
            version: "1.2.3".to_owned(),
            os: "darwin".to_owned(),
            arch: "arm64".to_owned(),
            installation_id: "installation-1".to_owned(),
            user_id: user_id.map(str::to_owned),
            session_id: None,
            environment: None,
            project_id: None,
            deployment_id: None,
        }
    }

    fn event(value_bytes: usize) -> Event {
        Event::new(
            "cli.test",
            serde_json::json!({"value": "x".repeat(value_bytes)}),
        )
        .expect("event")
    }

    async fn server(status: Option<u16>) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&requests);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = vec![0_u8; 64 * 1024];
            let _ = stream.read(&mut request).await;
            observed.fetch_add(1, Ordering::Relaxed);
            if let Some(status) = status {
                let reason = if status == 202 {
                    "Accepted"
                } else {
                    "Bad Request"
                };
                let response = format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\n\r\n");
                stream
                    .write_all(response.as_bytes())
                    .await
                    .expect("respond");
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        (format!("http://{address}/v1/events"), requests)
    }

    #[test]
    fn atomically_persists_complete_json_envelopes() {
        let root = tempfile::tempdir().expect("root");
        persist_events(root.path(), &client(None), vec![event(1)]).expect("persist");
        let files = pending_files(root.path()).expect("pending");
        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].extension().and_then(|value| value.to_str()),
            Some("json")
        );
        Envelope::from_slice(&fs::read(&files[0]).expect("read")).expect("complete envelope");
        assert!(fs::read_dir(files[0].parent().expect("parent"))
            .expect("read spool")
            .all(|entry| {
                !entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp")
            }));
    }

    #[test]
    fn privacy_downgrade_strips_user_and_off_discards_spool() {
        let root = tempfile::tempdir().expect("root");
        persist_events(root.path(), &client(Some("user-1")), vec![event(1)]).expect("persist");
        apply_privacy(root.path(), MetricsLevel::Basic).expect("strip identity");
        let path = pending_files(root.path()).expect("pending").remove(0);
        let envelope = Envelope::from_slice(&fs::read(path).expect("read")).expect("envelope");
        assert_eq!(envelope.client.user_id, None);

        persist_events(root.path(), &client(Some("user-1")), vec![event(1)]).expect("persist");
        apply_privacy(root.path(), MetricsLevel::Off).expect("discard");
        assert!(pending_files(root.path()).expect("pending").is_empty());
    }

    #[test]
    fn obsolete_protobuf_and_daily_files_are_deleted() {
        let root = tempfile::tempdir().expect("root");
        let spool = spool_dir(root.path()).expect("spool");
        fs::write(spool.join("obsolete.pb"), b"protobuf").expect("write protobuf");
        fs::write(spool.join("partial.tmp"), b"partial").expect("write temporary");
        fs::write(root.path().join("metrics").join("2026-07-27.json"), b"{}\n")
            .expect("write daily");
        cleanup_obsolete(root.path()).expect("cleanup");
        assert_eq!(
            fs::read_dir(spool)
                .expect("read spool")
                .filter_map(Result::ok)
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn response_deletes_spool_for_accepted_and_rejected_requests() {
        for status in [202, 400] {
            let root = tempfile::tempdir().expect("root");
            persist_events(root.path(), &client(None), vec![event(1)]).expect("persist");
            let (endpoint, requests) = server(Some(status)).await;
            deliver_pending(root.path(), &endpoint, 1, REQUEST_TIMEOUT)
                .await
                .expect("delivery");
            assert_eq!(requests.load(Ordering::Relaxed), 1);
            assert!(pending_files(root.path()).expect("pending").is_empty());
        }
    }

    #[tokio::test]
    async fn no_response_retains_spool() {
        let root = tempfile::tempdir().expect("root");
        persist_events(root.path(), &client(None), vec![event(1)]).expect("persist");
        let (endpoint, requests) = server(None).await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .expect("client");
        deliver_pending_with_client(root.path(), &endpoint, 1, &client)
            .await
            .expect("delivery");
        assert_eq!(requests.load(Ordering::Relaxed), 1);
        assert_eq!(pending_files(root.path()).expect("pending").len(), 1);
    }

    #[test]
    fn pruning_keeps_spool_below_cap() {
        let root = tempfile::tempdir().expect("root");
        let envelope = Envelope::new(Source::Cli, client(None), vec![event(15_000)])
            .expect("envelope")
            .to_vec()
            .expect("encode");
        let spool = spool_dir(root.path()).expect("spool");
        let files = MAX_SPOOL_BYTES / u64::try_from(envelope.len()).expect("length") + 2;
        for _ in 0..files {
            let path = spool.join(format!("{}.json", Uuid::now_v7()));
            fs::write(path, &envelope).expect("write envelope");
        }
        prune(root.path()).expect("prune");
        let bytes = pending_files(root.path())
            .expect("pending")
            .into_iter()
            .map(|path| fs::metadata(path).expect("metadata").len())
            .sum::<u64>();
        assert!(bytes <= MAX_SPOOL_BYTES);
    }
}
