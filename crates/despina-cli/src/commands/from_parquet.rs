use std::collections::HashMap;
use std::fs::File;

use arrow::array::AsArray;
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::ArrowPrimitiveType;
use color_eyre::eyre::{Result, bail, eyre};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::cli::FromParquetArgs;
use crate::parse::{parse_rename_specs, parse_type_code_specs};

pub fn run(args: FromParquetArgs) -> Result<()> {
    let import = &args.import;
    let type_specs = parse_type_code_specs(&import.type_code)?;
    let renames = parse_rename_specs(&import.rename)?;

    let file = File::open(&import.file)?;
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let arrow_schema = reader_builder.schema().clone();
    let reader = reader_builder.build()?;

    let origin_col_index = arrow_schema.index_of(&import.origin_col).map_err(|_| {
        eyre!(
            "origin column `{}` not found in Parquet schema",
            import.origin_col
        )
    })?;
    let destination_col_index = arrow_schema
        .index_of(&import.destination_col)
        .map_err(|_| {
            eyre!(
                "destination column `{}` not found in Parquet schema",
                import.destination_col
            )
        })?;

    let table_columns: Vec<(usize, String)> = if import.tables.is_empty() {
        arrow_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != origin_col_index && *i != destination_col_index)
            .map(|(i, f)| (i, f.name().clone()))
            .collect()
    } else {
        import
            .tables
            .iter()
            .map(|name| {
                let index = arrow_schema
                    .index_of(name)
                    .map_err(|_| eyre!("table column `{name}` not found in Parquet schema"))?;
                Ok((index, name.clone()))
            })
            .collect::<Result<Vec<_>>>()?
    };

    if table_columns.is_empty() {
        bail!("no table columns found in Parquet file");
    }

    let table_names: Vec<String> = table_columns
        .iter()
        .map(|(_, name)| renames.get(name).unwrap_or(name).clone())
        .collect();

    let zone_base = import.zone_base;
    let mut max_zone: u16 = 0;
    let mut cells: HashMap<(u16, u16), Vec<f64>> = HashMap::new();

    for batch_result in reader {
        let batch = batch_result?;
        let row_count = batch.num_rows();

        let origins = cast_column::<arrow::datatypes::UInt32Type>(
            &batch,
            origin_col_index,
            &import.origin_col,
        )?;
        let destinations = cast_column::<arrow::datatypes::UInt32Type>(
            &batch,
            destination_col_index,
            &import.destination_col,
        )?;

        let table_arrays: Vec<Vec<f64>> = table_columns
            .iter()
            .map(|(col_index, col_name)| {
                cast_column::<arrow::datatypes::Float64Type>(&batch, *col_index, col_name)
            })
            .collect::<Result<Vec<_>>>()?;

        for row_index in 0..row_count {
            let origin_raw = origins[row_index];
            let destination_raw = destinations[row_index];

            let origin = u64::from(origin_raw)
                .checked_sub(u64::from(zone_base))
                .and_then(|v| v.checked_add(1))
                .and_then(|v| u16::try_from(v).ok())
                .ok_or_else(|| eyre!("origin zone ID {origin_raw} out of range"))?;
            let destination = u64::from(destination_raw)
                .checked_sub(u64::from(zone_base))
                .and_then(|v| v.checked_add(1))
                .and_then(|v| u16::try_from(v).ok())
                .ok_or_else(|| eyre!("destination zone ID {destination_raw} out of range"))?;

            max_zone = max_zone.max(origin).max(destination);

            let values: Vec<f64> = table_arrays.iter().map(|col| col[row_index]).collect();

            if !import.allow_duplicate_od && cells.contains_key(&(origin, destination)) {
                bail!(
                    "duplicate OD pair (origin={origin_raw}, destination={destination_raw}); \
                     use --allow-duplicate-od to keep last value"
                );
            }

            cells.insert((origin, destination), values);
        }
    }

    crate::import::build_matrix(&cells, max_zone, &table_names, &type_specs, import)
}

fn cast_column<T: ArrowPrimitiveType>(
    batch: &arrow::record_batch::RecordBatch,
    col_index: usize,
    col_name: &str,
) -> Result<Vec<T::Native>> {
    let column = batch.column(col_index);
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let casted = cast_with_options(column, &T::DATA_TYPE, &options).map_err(|e| {
        eyre!(
            "cannot convert column `{col_name}` to {}: {e}",
            T::DATA_TYPE
        )
    })?;
    Ok(casted.as_primitive::<T>().values().to_vec())
}
