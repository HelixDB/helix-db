use std::env;
use std::future::Future;
use std::path::Path;
use std::sync::OnceLock;
use tokio::sync::Mutex;

static CURRENT_DIR_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn current_dir_lock() -> &'static Mutex<()> {
    CURRENT_DIR_LOCK.get_or_init(|| Mutex::new(()))
}

pub async fn with_current_dir<T, Fut>(path: &Path, f: impl FnOnce() -> Fut) -> T
where
    Fut: Future<Output = T>,
{
    let _guard = current_dir_lock().lock().await;
    let previous = env::current_dir().expect("Failed to read current dir");
    env::set_current_dir(path).expect("Failed to set current dir");
    let result = f().await;
    let _ = env::set_current_dir(previous);
    result
}
