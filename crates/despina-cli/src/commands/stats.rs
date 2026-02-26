use color_eyre::eyre::Result;
use despina::{ReaderBuilder, RowBuf};

use crate::cli::{OutputFormat, StatsArgs};
use crate::output::{StatsJson, TableStatsJson, stats_table, summary_table};

pub fn run(args: StatsArgs) -> Result<()> {
    let mut reader = ReaderBuilder::new().from_path(&args.file)?;
    let table_count = usize::from(reader.header().table_count());
    let mut totals = vec![0.0_f64; table_count];
    let mut diagonals = vec![0.0_f64; table_count];
    let mut row = RowBuf::with_zone_count(reader.header().zone_count());

    while reader.read_row(&mut row)? {
        let table_slot = usize::from(row.table_index() - 1);
        let values = row.values();
        totals[table_slot] += values.iter().copied().sum::<f64>();
        diagonals[table_slot] += values[usize::from(row.row_index() - 1)];
    }

    let header = reader.header();
    match args.format {
        OutputFormat::Text => {
            let file_display = args.file.display();
            let summary = summary_table(&[
                ("File", &file_display),
                ("Zones", &header.zone_count()),
                ("Tables", &header.table_count()),
            ]);
            println!("{summary}");
            println!();
            println!("{}", stats_table(header, &totals, &diagonals));
        }
        OutputFormat::Json => {
            let json = StatsJson {
                file: args.file.display().to_string(),
                zone_count: header.zone_count(),
                table_count: header.table_count(),
                tables: header
                    .tables()
                    .iter()
                    .enumerate()
                    .map(|(i, t)| TableStatsJson {
                        index: t.index(),
                        name: t.name().to_owned(),
                        type_code: t.type_code().to_string(),
                        total: totals[i],
                        diagonal: diagonals[i],
                    })
                    .collect(),
            };
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }
    Ok(())
}
