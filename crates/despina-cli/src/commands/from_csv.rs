use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;

use color_eyre::eyre::{Result, bail, eyre};

use crate::cli::FromCsvArgs;
use crate::parse::{parse_rename_specs, parse_separator, parse_type_code_specs};

pub fn run(args: FromCsvArgs) -> Result<()> {
    let import = &args.import;
    let type_specs = parse_type_code_specs(&import.type_code)?;
    let renames = parse_rename_specs(&import.rename)?;
    let separator = parse_separator(&args.separator)?;

    let file = File::open(&import.file)?;
    let buf = BufReader::new(file);
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(separator)
        .from_reader(buf);

    let headers: Vec<String> = reader.headers()?.iter().map(|h| h.to_owned()).collect();

    let origin_col_index = headers
        .iter()
        .position(|h| h == &import.origin_col)
        .ok_or_else(|| {
            eyre!(
                "origin column `{}` not found in CSV header",
                import.origin_col
            )
        })?;
    let destination_col_index = headers
        .iter()
        .position(|h| h == &import.destination_col)
        .ok_or_else(|| {
            eyre!(
                "destination column `{}` not found in CSV header",
                import.destination_col
            )
        })?;

    let table_columns: Vec<(usize, String)> = if import.tables.is_empty() {
        headers
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != origin_col_index && *i != destination_col_index)
            .map(|(i, name)| (i, name.clone()))
            .collect()
    } else {
        import
            .tables
            .iter()
            .map(|name| {
                let index = headers
                    .iter()
                    .position(|h| h == name)
                    .ok_or_else(|| eyre!("table column `{name}` not found in CSV header"))?;
                Ok((index, name.clone()))
            })
            .collect::<Result<Vec<_>>>()?
    };

    if table_columns.is_empty() {
        bail!("no table columns found in CSV");
    }

    let table_names: Vec<String> = table_columns
        .iter()
        .map(|(_, name)| renames.get(name).unwrap_or(name).clone())
        .collect();

    let zone_base = import.zone_base;
    let mut max_zone: u16 = 0;
    let mut cells: HashMap<(u16, u16), Vec<f64>> = HashMap::new();

    for result in reader.records() {
        let record = result?;
        let origin_raw: u64 = record
            .get(origin_col_index)
            .ok_or_else(|| eyre!("row missing origin column"))?
            .parse()?;
        let destination_raw: u64 = record
            .get(destination_col_index)
            .ok_or_else(|| eyre!("row missing destination column"))?
            .parse()?;

        let origin = origin_raw
            .checked_sub(u64::from(zone_base))
            .and_then(|v| v.checked_add(1))
            .and_then(|v| u16::try_from(v).ok())
            .ok_or_else(|| eyre!("origin zone ID {origin_raw} out of range"))?;
        let destination = destination_raw
            .checked_sub(u64::from(zone_base))
            .and_then(|v| v.checked_add(1))
            .and_then(|v| u16::try_from(v).ok())
            .ok_or_else(|| eyre!("destination zone ID {destination_raw} out of range"))?;

        if origin == 0 || destination == 0 {
            bail!(
                "zone ID maps to internal 0 (zone_base={zone_base}, value={})",
                if origin == 0 {
                    origin_raw
                } else {
                    destination_raw
                }
            );
        }

        max_zone = max_zone.max(origin).max(destination);

        let values: Vec<f64> = table_columns
            .iter()
            .map(|(col_index, col_name)| {
                record
                    .get(*col_index)
                    .ok_or_else(|| eyre!("row missing column `{col_name}`"))
                    .and_then(|s| s.parse::<f64>().map_err(Into::into))
            })
            .collect::<Result<Vec<_>>>()?;

        if !import.allow_duplicate_od && cells.contains_key(&(origin, destination)) {
            bail!(
                "duplicate OD pair (origin={origin_raw}, destination={destination_raw}); \
                 use --allow-duplicate-od to keep last value"
            );
        }

        cells.insert((origin, destination), values);
    }

    crate::import::build_matrix(&cells, max_zone, &table_names, &type_specs, import)
}
