use crate::errors::CliError;
use color_eyre::owo_colors::OwoColorize;
use eyre::Result;
use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::Path;

pub fn command_exists(command: &str) -> bool {
    command_exists_in_path(
        command,
        std::env::var_os("PATH"),
        std::env::var_os("PATHEXT"),
    )
}

fn command_exists_in_path(
    command: &str,
    path: Option<OsString>,
    path_ext: Option<OsString>,
) -> bool {
    let command_path = Path::new(command);
    if command_path.components().count() > 1 {
        return is_executable(command_path);
    }

    let Some(path) = path else {
        return false;
    };

    let extensions = command_extensions(command, path_ext);
    std::env::split_paths(&path).any(|dir| {
        extensions
            .iter()
            .any(|extension| is_executable(&dir.join(format!("{command}{extension}"))))
    })
}

fn command_extensions(command: &str, path_ext: Option<OsString>) -> Vec<String> {
    if cfg!(windows) && Path::new(command).extension().is_none() {
        let path_ext = path_ext
            .and_then(|value| value.into_string().ok())
            .unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".to_string());
        let mut extensions = vec![String::new()];
        extensions.extend(
            path_ext
                .split(';')
                .filter(|extension| !extension.is_empty())
                .map(|extension| {
                    if extension.starts_with('.') {
                        extension.to_string()
                    } else {
                        format!(".{extension}")
                    }
                }),
        );
        extensions
    } else {
        vec![String::new()]
    }
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    path.is_file()
        && path
            .metadata()
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

pub fn print_newline() {
    println!();
}

pub fn print_lines(lines: &[&str]) {
    for line in lines {
        println!("  {line}");
    }
}

pub fn print_instructions(title: &str, steps: &[&str]) {
    if !crate::output::Verbosity::current().show_normal() {
        return;
    }
    print_newline();
    println!("{}", title.bold());
    for (i, step) in steps.iter().enumerate() {
        println!("  {}. {step}", (i + 1).to_string().bright_white().bold());
    }
}

pub fn print_header(title: &str) {
    println!("{}", title.bold().underline());
}

pub fn print_field(key: &str, value: &str) {
    println!("  {}: {value}", key.bright_white().bold());
}

pub fn print_error(message: &str) {
    let error = CliError::new(message);
    eprint!("{}", error.render());
}

pub fn print_error_with_hint(message: &str, hint: &str) {
    let error = CliError::new(message).with_hint(hint);
    eprint!("{}", error.render());
}

pub fn print_warning(message: &str) {
    let warning = CliError::warning(message);
    eprint!("{}", warning.render());
}

pub fn print_confirm(message: &str) -> Result<bool> {
    if !std::io::stdin().is_terminal() {
        return Ok(false);
    }

    crate::prompts::confirm(message)
}

pub fn print_prompt(message: &str) -> std::io::Result<String> {
    use std::io::{self, Write};
    print!("{} ", message.yellow().bold());
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input)
}

pub fn add_env_var_to_file(path: &std::path::Path, key: &str, value: &str) -> Result<()> {
    let mut content = std::fs::read_to_string(path).unwrap_or_default();
    let replacement = format!("{key}={value}");
    let mut replaced = false;

    let lines: Vec<String> = content
        .lines()
        .map(|line| {
            if line.trim_start().starts_with(&format!("{key}=")) {
                replaced = true;
                replacement.clone()
            } else {
                line.to_string()
            }
        })
        .collect();

    content = lines.join("\n");
    if !replaced {
        if !content.is_empty() && !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str(&replacement);
    }
    if !content.ends_with('\n') {
        content.push('\n');
    }

    std::fs::write(path, content)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_exists_in_path_finds_platform_executable() {
        let dir = tempfile::tempdir().unwrap();
        let command = dir
            .path()
            .join(if cfg!(windows) { "node.CMD" } else { "node" });
        std::fs::write(&command, "").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&command).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&command, permissions).unwrap();
        }

        assert!(command_exists_in_path(
            "node",
            Some(dir.path().as_os_str().to_os_string()),
            Some(OsString::from(".CMD")),
        ));
    }

    #[test]
    fn command_extensions_include_windows_path_ext() {
        let extensions = command_extensions("node", Some(OsString::from(".EXE;.CMD")));

        if cfg!(windows) {
            assert_eq!(extensions, vec!["", ".EXE", ".CMD"]);
        } else {
            assert_eq!(extensions, vec![""]);
        }
    }

    #[test]
    fn command_lookup_handles_direct_missing_and_non_executable_paths() {
        let dir = tempfile::tempdir().unwrap();
        let command = dir.path().join("tool");
        std::fs::write(&command, "").unwrap();

        #[cfg(unix)]
        assert!(!command_exists_in_path(
            command.to_str().unwrap(),
            None,
            None
        ));
        #[cfg(not(unix))]
        assert!(command_exists_in_path(
            command.to_str().unwrap(),
            None,
            None
        ));

        assert!(!command_exists_in_path("missing", None, None));
        assert!(!command_exists_in_path(
            "missing",
            Some(dir.path().as_os_str().to_os_string()),
            None,
        ));
    }

    #[test]
    fn env_file_values_are_added_replaced_and_newline_terminated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".env");

        add_env_var_to_file(&path, "HELIX_API_KEY", "first").unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "HELIX_API_KEY=first\n"
        );

        std::fs::write(&path, "OTHER=value\n  HELIX_API_KEY=old\nTAIL=value").unwrap();
        add_env_var_to_file(&path, "HELIX_API_KEY", "second").unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "OTHER=value\nHELIX_API_KEY=second\nTAIL=value\n"
        );
    }

    #[test]
    fn noninteractive_and_printing_helpers_are_safe() {
        crate::output::Verbosity::set(crate::output::Verbosity::Normal);
        print_newline();
        print_lines(&["one", "two"]);
        print_instructions("Next", &["first", "second"]);
        print_header("Header");
        print_field("Key", "Value");
        print_error("failure");
        print_error_with_hint("failure", "retry");
        print_warning("careful");
        assert!(!print_confirm("continue?").unwrap());

        crate::output::Verbosity::set(crate::output::Verbosity::Silent);
        print_instructions("Hidden", &["step"]);
        crate::output::Verbosity::set(crate::output::Verbosity::Normal);
    }
}
