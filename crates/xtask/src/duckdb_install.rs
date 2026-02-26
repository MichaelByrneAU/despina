use std::process::Command;

use color_eyre::eyre::{Result, WrapErr, bail};

use crate::target_directory;

fn duckdb_query(query: &str) -> Result<String> {
    let output = Command::new("duckdb")
        .args(["-noheader", "-csv", "-c", query])
        .output()?;

    if !output.status.success() {
        bail!(
            "duckdb query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

fn home_directory() -> Result<std::path::PathBuf> {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .map_err(|_| color_eyre::eyre::eyre!("could not determine home directory"))
}

pub fn run() -> Result<()> {
    let source = target_directory()
        .join("release")
        .join("despina_duckdb.duckdb_extension");

    let version = duckdb_query("SELECT version()")?;
    let platform = duckdb_query("SELECT * FROM pragma_platform()")?;

    let destination_directory = home_directory()?
        .join(".duckdb/extensions")
        .join(&version)
        .join(&platform);

    std::fs::create_dir_all(&destination_directory)?;

    let destination = destination_directory.join("despina.duckdb_extension");
    std::fs::copy(&source, &destination)
        .wrap_err_with(|| format!("could not copy extension from {}", source.display()))?;

    println!("Installed to {}", destination.display());

    Ok(())
}
