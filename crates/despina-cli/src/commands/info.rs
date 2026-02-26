use color_eyre::eyre::Result;
use despina::ReaderBuilder;

use crate::cli::{InfoArgs, OutputFormat};
use crate::output::{InfoJson, TableInfoJson, info_table, summary_table};

pub fn run(args: InfoArgs) -> Result<()> {
    let reader = ReaderBuilder::new().from_path(&args.file)?;
    let header = reader.header();

    match args.format {
        OutputFormat::Text => {
            let file_display = args.file.display();
            let summary = summary_table(&[
                ("File", &file_display),
                ("Zones", &header.zone_count()),
                ("Tables", &header.table_count()),
                ("Banner", &header.banner()),
                ("Run ID", &header.run_id()),
            ]);
            println!("{summary}");
            println!();
            println!("{}", info_table(header));
        }
        OutputFormat::Json => {
            let json = InfoJson {
                file: args.file.display().to_string(),
                zone_count: header.zone_count(),
                table_count: header.table_count(),
                banner: header.banner().to_owned(),
                run_id: header.run_id().to_owned(),
                tables: header
                    .tables()
                    .iter()
                    .map(|t| TableInfoJson {
                        index: t.index(),
                        name: t.name().to_owned(),
                        type_code: t.type_code().to_string(),
                    })
                    .collect(),
            };
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }
    Ok(())
}
