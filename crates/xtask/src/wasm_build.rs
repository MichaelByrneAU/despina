use std::process::Command;

use color_eyre::eyre::{Result, bail};
use serde_json::{Value, json};

use crate::{target_directory, workspace_root};

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    dev: bool,
}

pub fn run(args: Args) -> Result<()> {
    let crate_directory = workspace_root().join("crates/despina-wasm");
    let output_directory = target_directory().join("wasm-pkg");
    let profile = if args.dev { "--dev" } else { "--release" };

    let status = Command::new("wasm-pack")
        .args([
            "build",
            profile,
            "--target",
            "web",
            "--out-name",
            "despina",
            "--out-dir",
        ])
        .arg(&output_directory)
        .arg(".")
        .current_dir(&crate_directory)
        .status()?;

    if !status.success() {
        bail!("wasm-pack build failed");
    }

    let js_source_directory = crate_directory.join("js");
    let mut js_file_names: Vec<String> = Vec::new();

    for entry in std::fs::read_dir(&js_source_directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        js_file_names.push(file_name.to_string_lossy().into_owned());
        std::fs::copy(entry.path(), output_directory.join(&file_name))?;
    }

    let package_json_path = output_directory.join("package.json");
    let contents = std::fs::read_to_string(&package_json_path)?;
    let mut package: Value = serde_json::from_str(&contents)?;

    package["name"] = json!("despina");
    package["main"] = json!("index.js");
    package["types"] = json!("index.d.ts");

    if let Some(array) = package.get_mut("files").and_then(Value::as_array_mut) {
        for name in &js_file_names {
            let value = Value::String(name.clone());
            if !array.contains(&value) {
                array.push(value);
            }
        }
    }

    package["exports"] = json!({ ".": { "import": "./index.js", "types": "./index.d.ts" } });
    package["sideEffects"] = json!(false);

    std::fs::write(&package_json_path, serde_json::to_string_pretty(&package)?)?;

    Ok(())
}
