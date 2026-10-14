//! Local capture/replay utility; it never updates the checked-in regression gate.
#[path = "../tests/plans/support/mod.rs"]
mod support;

use std::{error::Error, path::Path};

fn read(path: &Path) -> Result<support::Capture, Box<dyn Error>> {
    let capture: support::Capture = serde_json::from_slice(&std::fs::read(path)?)?;
    if capture.schema != 1 || capture.cases.is_empty() {
        return Err("unsupported or empty planner capture".into());
    }
    Ok(capture)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let output = match args.as_slice() {
        [command, output] if command == "capture" => {
            let capture = support::Capture::plan(support::inputs());
            (output, serde_json::to_value(capture)?)
        }
        [command, output, sdk_root, count] if command == "capture" => {
            let mut inputs = support::inputs();
            let sdk = support::sdk_inputs(Path::new(sdk_root), count.to_str().ok_or("fixture count")?.parse()?);
            for (name, input) in sdk {
                if inputs.insert(name, input).is_some() {
                    return Err("duplicate planner case".into());
                }
            }
            (output, serde_json::to_value(support::Capture::plan(inputs))?)
        }
        [command, input, output] if command == "replay" => {
            (output, serde_json::to_value(read(Path::new(input))?.replay())?)
        }
        [command, input, output] if command == "manifest" => {
            (output, read(Path::new(input))?.manifest())
        }
        _ => return Err("usage: planner_snapshots capture OUTPUT [SDK_ROOT EXPECTED_COUNT] | replay INPUT OUTPUT | manifest INPUT OUTPUT".into()),
    };
    let count = output.1["cases"].as_object().ok_or("missing cases")?.len();
    // Refuse to clobber previous evidence or a reviewed baseline accidentally.
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.0)?;
    file.write_all(&serde_json::to_vec_pretty(&output.1)?)?;
    file.write_all(b"\n")?;
    println!("wrote {count} cases");
    Ok(())
}
