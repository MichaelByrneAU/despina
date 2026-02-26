use clap::Parser;
use color_eyre::eyre::Result;

mod cli;
mod commands;
mod import;
mod output;
mod parse;
mod wide;

use crate::cli::{Cli, Command};

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    match cli.command {
        Command::Info(args) => commands::info::run(args),
        Command::Validate(args) => commands::validate::run(args),
        Command::Stats(args) => commands::stats::run(args),
        Command::ToCsv(args) => commands::to_csv::run(args),
        Command::FromCsv(args) => commands::from_csv::run(args),
        Command::ToParquet(args) => commands::to_parquet::run(args),
        Command::FromParquet(args) => commands::from_parquet::run(args),
    }
}
