use despina::Matrix;

pub struct WideRow {
    pub origin: u16,
    pub destination: u16,
    pub values: Vec<f64>,
}

pub struct WideSchema {
    pub names: Vec<String>,
    pub indices: Vec<u8>,
}

pub fn resolve_tables(matrix: &Matrix, requested: &[String]) -> color_eyre::Result<WideSchema> {
    if requested.is_empty() {
        let names: Vec<String> = matrix.tables().map(|t| t.name().to_owned()).collect();
        let indices: Vec<u8> = (1..=matrix.table_count()).collect();
        Ok(WideSchema { names, indices })
    } else {
        let mut names = Vec::with_capacity(requested.len());
        let mut indices = Vec::with_capacity(requested.len());
        for name in requested {
            let table_index = matrix
                .header()
                .table_index_by_name(name)
                .ok_or_else(|| color_eyre::eyre::eyre!("table `{name}` not found in matrix"))?;
            names.push(name.clone());
            indices.push(table_index);
        }
        Ok(WideSchema { names, indices })
    }
}

pub fn wide_rows(matrix: &Matrix, schema: &WideSchema, include_zero_rows: bool) -> Vec<WideRow> {
    let zone_count = matrix.zone_count();
    let mut rows = Vec::new();

    for origin in 1..=zone_count {
        for destination in 1..=zone_count {
            let values: Vec<f64> = schema
                .indices
                .iter()
                .map(|&ti| matrix.get(ti, origin, destination))
                .collect();

            if !include_zero_rows && values.iter().all(|&v| v == 0.0) {
                continue;
            }

            rows.push(WideRow {
                origin,
                destination,
                values,
            });
        }
    }
    rows
}
