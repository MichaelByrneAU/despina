use color_eyre::eyre::Result;

use crate::workspace_root;

pub fn run() -> Result<()> {
    let docs_site = workspace_root().join("crates/despina-py/docs/site");

    if docs_site.exists() {
        std::fs::remove_dir_all(&docs_site)?;
    }

    Ok(())
}
