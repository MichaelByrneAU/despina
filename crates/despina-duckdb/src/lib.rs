mod vtab;

pub use vtab::ReadMatVTab;

#[cfg(feature = "loadable")]
mod entry {
    use duckdb::{duckdb_entrypoint_c_api, Connection};
    use std::error::Error;

    use super::ReadMatVTab;

    #[duckdb_entrypoint_c_api(ext_name = "despina", min_duckdb_version = "v1.2.0")]
    pub unsafe fn extension_init(con: Connection) -> Result<(), Box<dyn Error>> {
        con.register_table_function::<ReadMatVTab>("read_mat")?;
        Ok(())
    }
}
