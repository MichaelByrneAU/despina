use std::fs::File;

use color_eyre::eyre::Result;

use crate::cli::ToCsvArgs;
use crate::parse::parse_separator;
use crate::wide;

pub fn run(args: ToCsvArgs) -> Result<()> {
    let export = &args.export;
    let separator = parse_separator(&args.separator)?;
    let matrix = despina::open(&export.file)?;
    let schema = wide::resolve_tables(&matrix, &export.tables)?;
    let rows = wide::wide_rows(&matrix, &schema, export.include_zero_rows);

    let file = File::create(&export.output)?;
    let mut writer = csv::WriterBuilder::new()
        .delimiter(separator)
        .from_writer(file);

    let mut header: Vec<String> = vec![export.origin_col.clone(), export.destination_col.clone()];
    header.extend(schema.names.iter().cloned());
    writer.write_record(&header)?;

    let zone_base = export.zone_base;

    for row in &rows {
        let origin_out = u64::from(row.origin) - 1 + u64::from(zone_base);
        let destination_out = u64::from(row.destination) - 1 + u64::from(zone_base);

        let mut record: Vec<String> = vec![origin_out.to_string(), destination_out.to_string()];
        for &value in &row.values {
            record.push(format_value(value));
        }
        writer.write_record(&record)?;
    }

    writer.flush()?;
    Ok(())
}

fn format_value(value: f64) -> String {
    if value == value.trunc() && value.is_finite() {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}
