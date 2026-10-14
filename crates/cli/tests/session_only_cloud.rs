use std::{fs, path::Path};

#[test]
fn cloud_cli_has_no_legacy_auth_or_direct_gateway_path() {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let forbidden = [
        "x-api-key",
        "helix_admin_key",
        "helix_user_key",
        "HELIX_API_KEY",
        "query_auth_header",
        "query_auth_env",
        "gateway_url",
    ];
    for entry in walk(&source) {
        let contents = fs::read_to_string(&entry).unwrap();
        for needle in forbidden {
            assert!(
                !contents.contains(needle),
                "{} contains forbidden legacy Cloud path {needle}",
                entry.display()
            );
        }
    }
}

fn walk(directory: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    files
}
