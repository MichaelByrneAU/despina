use color_eyre::eyre::Result;
use despina::{ReaderBuilder, RowBuf};

use crate::cli::ValidateArgs;
use crate::output::{eprint_fail, print_ok};

pub fn run(args: ValidateArgs) -> Result<()> {
    match validate_file(&args) {
        Ok(summary) => {
            if !args.quiet {
                print_ok(&format!(
                    "{} (zones={}, tables={}, rows={})",
                    args.file.display(),
                    summary.zone_count,
                    summary.table_count,
                    summary.row_count,
                ));
            }
            Ok(())
        }
        Err(err) => {
            if !args.quiet {
                eprint_fail(&format!("{} ({err})", args.file.display()));
            }
            std::process::exit(1);
        }
    }
}

fn validate_file(args: &ValidateArgs) -> despina::Result<ValidationSummary> {
    let mut reader = ReaderBuilder::new().from_path(&args.file)?;
    let summary = ValidationSummary {
        zone_count: reader.header().zone_count(),
        table_count: reader.header().table_count(),
        row_count: reader.header().row_count(),
    };
    let mut row = RowBuf::new();
    while reader.read_row(&mut row)? {}
    Ok(summary)
}

#[derive(Debug, Clone, Copy)]
struct ValidationSummary {
    zone_count: u16,
    table_count: u8,
    row_count: u32,
}
