use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};

use despina::Matrix;
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};

/// Matches DuckDB's `STANDARD_VECTOR_SIZE`. The FFI function
/// `duckdb_vector_size()` exists, but this value has been stable across all
/// released versions.
const CHUNK_CAPACITY: usize = 2048;

/// DuckDB table function that reads `.mat` binary matrix files.
///
/// Presents the matrix in wide format with `Origin`, `Destination`, and one
/// column per matrix table. Register with
/// `conn.register_table_function::<ReadMatVTab>("read_mat")`.
pub struct ReadMatVTab;

/// Bind-phase data: the loaded matrix and query options.
pub struct MatBindData {
    matrix: Matrix,
    include_zeros: bool,
    table_count: u8,
}

/// Init-phase data: tracks the current scan position and projected columns.
pub struct MatInitData {
    cursor_position: AtomicU64,
    /// Maps each output column position to its original column index from
    /// `bind()`. Populated via projection pushdown so that `func()` only
    /// writes the columns DuckDB actually needs.
    projected_columns: Vec<usize>,
}

impl VTab for ReadMatVTab {
    type InitData = MatInitData;
    type BindData = MatBindData;

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // path
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            (
                "tables".to_string(),
                LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ),
            (
                "include_zeros".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
        ])
    }

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let path = bind.get_parameter(0).to_string();

        let include_zeros = bind
            .get_named_parameter("include_zeros")
            .map(|v| v.to_string() == "true")
            .unwrap_or(false);

        let table_filter: Option<Vec<String>> = bind
            .get_named_parameter("tables")
            .map(|v| parse_table_list(&v.to_string()));

        let matrix = match &table_filter {
            Some(names) => {
                let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                Matrix::open_tables(&path, &name_refs)?
            }
            None => Matrix::open(&path)?,
        };

        // Register output columns.
        bind.add_result_column("Origin", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column(
            "Destination",
            LogicalTypeHandle::from(LogicalTypeId::Bigint),
        );

        let table_count = matrix.table_count();
        for table_info in matrix.header().tables() {
            bind.add_result_column(
                table_info.name(),
                LogicalTypeHandle::from(LogicalTypeId::Double),
            );
        }

        let zone_count = u64::from(matrix.zone_count());
        bind.set_cardinality(zone_count * zone_count, false);

        Ok(MatBindData {
            matrix,
            include_zeros,
            table_count,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let max_threads = std::thread::available_parallelism()
            .map(|n| n.get() as u64)
            .unwrap_or(1);
        init.set_max_threads(max_threads);
        let projected_columns = init
            .get_column_indices()
            .into_iter()
            .map(|idx| idx as usize)
            .collect();
        Ok(MatInitData {
            cursor_position: AtomicU64::new(0),
            projected_columns,
        })
    }

    fn supports_pushdown() -> bool {
        true
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let matrix = &bind_data.matrix;
        let zone_count = u64::from(matrix.zone_count());
        let total_cells = zone_count * zone_count;
        let table_count = bind_data.table_count;

        // Collect eligible (origin, destination) pairs for this chunk.
        // Each thread claims positions in batches via atomic fetch_add to avoid
        // data races. Relaxed ordering suffices because cursor_position is the
        // only shared mutable state.
        let mut rows: Vec<(u16, u16)> = Vec::with_capacity(CHUNK_CAPACITY);

        while rows.len() < CHUNK_CAPACITY {
            let batch = (CHUNK_CAPACITY - rows.len()) as u64;
            let start = init_data
                .cursor_position
                .fetch_add(batch, Ordering::Relaxed);
            if start >= total_cells {
                break;
            }
            let end = (start + batch).min(total_cells);

            for pos in start..end {
                let origin = (pos / zone_count) as u16 + 1;
                let destination = (pos % zone_count) as u16 + 1;

                if !bind_data.include_zeros {
                    let dest_index = usize::from(destination - 1);
                    let all_zero =
                        (1..=table_count).all(|t| matrix.row(t, origin)[dest_index] == 0.0);
                    if all_zero {
                        continue;
                    }
                }

                rows.push((origin, destination));
            }
        }

        if rows.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        // Write only the projected columns.
        for (output_index, &original_index) in init_data.projected_columns.iter().enumerate() {
            match original_index {
                0 => {
                    // Origin column.
                    let mut vector = output.flat_vector(output_index);
                    let data = vector.as_mut_slice::<i64>();
                    for (i, &(origin, _)) in rows.iter().enumerate() {
                        data[i] = i64::from(origin);
                    }
                }
                1 => {
                    // Destination column.
                    let mut vector = output.flat_vector(output_index);
                    let data = vector.as_mut_slice::<i64>();
                    for (i, &(_, destination)) in rows.iter().enumerate() {
                        data[i] = i64::from(destination);
                    }
                }
                col => {
                    // Table value column: original column 2 → table 1, etc.
                    let table_index = (col - 2) as u8 + 1;
                    let mut vector = output.flat_vector(output_index);
                    let data = vector.as_mut_slice::<f64>();
                    for (i, &(origin, destination)) in rows.iter().enumerate() {
                        let row = matrix.row(table_index, origin);
                        data[i] = row[usize::from(destination - 1)];
                    }
                }
            }
        }

        output.set_len(rows.len());
        Ok(())
    }
}

/// Parses a DuckDB `LIST(VARCHAR)` string representation into table names.
///
/// DuckDB converts `['DIST', 'TIME']` to `[DIST, TIME]` via `duckdb_get_varchar`.
/// The `", "` separator is DuckDB's current serialisation format for list values;
/// future DuckDB version bumps should verify this hasn't changed.
fn parse_table_list(s: &str) -> Vec<String> {
    let s = s.trim();
    let inner = s
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(s);
    if inner.is_empty() {
        return Vec::new();
    }
    inner.split(", ").map(|s| s.trim().to_owned()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_table_list_multiple() {
        assert_eq!(parse_table_list("[DIST, TIME]"), vec!["DIST", "TIME"],);
    }

    #[test]
    fn parse_table_list_single() {
        assert_eq!(parse_table_list("[DIST]"), vec!["DIST"]);
    }

    #[test]
    fn parse_table_list_empty() {
        let result: Vec<String> = parse_table_list("[]");
        assert!(result.is_empty());
    }
}
