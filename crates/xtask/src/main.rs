mod clean_docs;
mod duckdb_install;
mod wasm_build;

use std::path::{Path, PathBuf};

use clap::Parser;
use color_eyre::eyre::Result;

#[derive(Parser)]
#[command(name = "xtask", about = "Development tasks for the despina workspace.")]
enum Cli {
    WasmBuild(wasm_build::Args),
    #[command(about = "Install the DuckDB extension into the local extension directory.")]
    DuckdbInstall,
    #[command(about = "Remove the generated Python documentation site.")]
    CleanDocs,
}

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("xtask crate should be two levels below workspace root")
}

fn target_directory() -> PathBuf {
    workspace_root().join("target")
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    match cli {
        Cli::WasmBuild(args) => wasm_build::run(args),
        Cli::DuckdbInstall => duckdb_install::run(),
        Cli::CleanDocs => clean_docs::run(),
    }
}
