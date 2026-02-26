use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};

/// Inspect, validate, and convert .mat matrix files.
#[derive(Debug, Parser)]
#[command(name = "despina", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Print header metadata and table catalogue.
    Info(InfoArgs),
    /// Parse and validate the full file.
    Validate(ValidateArgs),
    /// Print per-table totals and diagonal totals.
    Stats(StatsArgs),
    /// Convert a .mat file to wide-format CSV.
    #[command(name = "to-csv")]
    ToCsv(ToCsvArgs),
    /// Convert a wide-format CSV to a .mat file.
    #[command(name = "from-csv")]
    FromCsv(FromCsvArgs),
    /// Convert a .mat file to wide-format Parquet.
    #[command(name = "to-parquet")]
    ToParquet(ToParquetArgs),
    /// Convert a wide-format Parquet to a .mat file.
    #[command(name = "from-parquet")]
    FromParquet(FromParquetArgs),
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum ParquetCompression {
    #[default]
    Snappy,
    Zstd,
    None,
}

#[derive(Debug, Parser)]
pub struct InfoArgs {
    /// Path to the .mat file.
    pub file: PathBuf,

    /// Output format.
    #[arg(long, default_value = "text")]
    pub format: OutputFormat,
}

#[derive(Debug, Parser)]
pub struct ValidateArgs {
    /// Path to the .mat file.
    pub file: PathBuf,

    /// Suppress status output; use exit code only.
    #[arg(long)]
    pub quiet: bool,
}

#[derive(Debug, Parser)]
pub struct StatsArgs {
    /// Path to the .mat file.
    pub file: PathBuf,

    /// Output format.
    #[arg(long, default_value = "text")]
    pub format: OutputFormat,
}

/// Common options for wide-format export commands.
#[derive(Debug, Parser)]
pub struct ExportArgs {
    /// Path to the input .mat file.
    pub file: PathBuf,

    /// Output file path.
    #[arg(short, long)]
    pub output: PathBuf,

    /// Name of the origin column.
    #[arg(long, default_value = "Origin")]
    pub origin_col: String,

    /// Name of the destination column.
    #[arg(long, default_value = "Destination")]
    pub destination_col: String,

    /// Zone numbering base (0 or 1).
    #[arg(long, default_value = "1")]
    pub zone_base: u16,

    /// Include rows where all table values are zero.
    #[arg(long)]
    pub include_zero_rows: bool,

    /// Export only the named tables (repeatable). Exports all if omitted.
    #[arg(long = "table")]
    pub tables: Vec<String>,
}

/// Common options for wide-format import commands.
#[derive(Debug, Parser)]
pub struct ImportArgs {
    /// Path to the input file (CSV or Parquet).
    pub file: PathBuf,

    /// Output .mat file path.
    #[arg(short, long)]
    pub output: PathBuf,

    /// Name of the origin column.
    #[arg(long, default_value = "Origin")]
    pub origin_col: String,

    /// Name of the destination column.
    #[arg(long, default_value = "Destination")]
    pub destination_col: String,

    /// Zone numbering base (0 or 1).
    #[arg(long, default_value = "1")]
    pub zone_base: u16,

    /// Total number of zones. If omitted, inferred from the maximum zone ID.
    #[arg(long)]
    pub zone_count: Option<u16>,

    /// Import only the named columns as tables (repeatable). Imports all
    /// non-OD columns if omitted.
    #[arg(long = "table")]
    pub tables: Vec<String>,

    /// Type code for tables. A bare code (D, S, 0-9) sets the default for all
    /// tables; TABLE:CODE sets a per-table override. Repeatable. Default: D.
    #[arg(long)]
    pub type_code: Vec<String>,

    /// Rename a table column: ORIGINAL:NEW. Repeatable.
    #[arg(long)]
    pub rename: Vec<String>,

    /// Allow duplicate (origin, destination) pairs (last value wins).
    /// By default, duplicates cause an error.
    #[arg(long)]
    pub allow_duplicate_od: bool,

    /// Treat missing OD pairs as an error instead of a warning.
    #[arg(long)]
    pub strict: bool,

    /// Banner text for the output .mat header.
    #[arg(long)]
    pub banner: Option<String>,

    /// Run identifier for the output .mat header.
    #[arg(long)]
    pub run_id: Option<String>,
}

#[derive(Debug, Parser)]
pub struct ToCsvArgs {
    #[command(flatten)]
    pub export: ExportArgs,

    /// Field separator character (default: comma). Use "tab" for tab-separated.
    #[arg(short, long, default_value = ",")]
    pub separator: String,
}

#[derive(Debug, Parser)]
pub struct FromCsvArgs {
    #[command(flatten)]
    pub import: ImportArgs,

    /// Field separator character (default: comma). Use "tab" for tab-separated.
    #[arg(short, long, default_value = ",")]
    pub separator: String,
}

#[derive(Debug, Parser)]
pub struct ToParquetArgs {
    #[command(flatten)]
    pub export: ExportArgs,

    /// Compression codec for the Parquet file.
    #[arg(long, default_value = "snappy")]
    pub compression: ParquetCompression,
}

#[derive(Debug, Parser)]
pub struct FromParquetArgs {
    #[command(flatten)]
    pub import: ImportArgs,
}
