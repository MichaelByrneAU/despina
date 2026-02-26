use comfy_table::{Attribute, Cell, CellAlignment, Color, ContentArrangement, Table};
use owo_colors::OwoColorize;
use owo_colors::Stream::Stderr;
use serde::Serialize;

pub fn print_ok(message: &str) {
    println!(
        "{} {message}",
        "OK".if_supports_color(owo_colors::Stream::Stdout, |text| text.green())
    );
}

pub fn eprint_fail(message: &str) {
    eprintln!(
        "{} {message}",
        "FAIL".if_supports_color(Stderr, |text| text.red())
    );
}

pub fn info_table(header: &despina::Header) -> Table {
    let mut table = Table::new();
    table
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("#").set_alignment(CellAlignment::Right),
            Cell::new("Name"),
            Cell::new("Type"),
        ]);

    for info in header.tables() {
        table.add_row(vec![
            Cell::new(info.index()).set_alignment(CellAlignment::Right),
            Cell::new(info.name()),
            Cell::new(info.type_code().to_string()),
        ]);
    }
    table
}

pub fn stats_table(header: &despina::Header, totals: &[f64], diagonals: &[f64]) -> Table {
    let mut table = Table::new();
    table
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_header(vec![
            Cell::new("#").set_alignment(CellAlignment::Right),
            Cell::new("Name"),
            Cell::new("Type"),
            Cell::new("Total").set_alignment(CellAlignment::Right),
            Cell::new("Diagonal").set_alignment(CellAlignment::Right),
        ]);

    for (index, info) in header.tables().iter().enumerate() {
        table.add_row(vec![
            Cell::new(info.index()).set_alignment(CellAlignment::Right),
            Cell::new(info.name()),
            Cell::new(info.type_code().to_string()),
            Cell::new(format!("{:.6}", totals[index])).set_alignment(CellAlignment::Right),
            Cell::new(format!("{:.6}", diagonals[index])).set_alignment(CellAlignment::Right),
        ]);
    }
    table
}

pub fn summary_table(rows: &[(&str, &dyn std::fmt::Display)]) -> Table {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    for (key, value) in rows {
        table.add_row(vec![
            Cell::new(key)
                .add_attribute(Attribute::Bold)
                .fg(Color::Cyan),
            Cell::new(value),
        ]);
    }
    table
}

#[derive(Debug, Serialize)]
pub struct InfoJson {
    pub file: String,
    pub zone_count: u16,
    pub table_count: u8,
    pub banner: String,
    pub run_id: String,
    pub tables: Vec<TableInfoJson>,
}

#[derive(Debug, Serialize)]
pub struct TableInfoJson {
    pub index: u8,
    pub name: String,
    #[serde(rename = "type")]
    pub type_code: String,
}

#[derive(Debug, Serialize)]
pub struct StatsJson {
    pub file: String,
    pub zone_count: u16,
    pub table_count: u8,
    pub tables: Vec<TableStatsJson>,
}

#[derive(Debug, Serialize)]
pub struct TableStatsJson {
    pub index: u8,
    pub name: String,
    #[serde(rename = "type")]
    pub type_code: String,
    pub total: f64,
    pub diagonal: f64,
}
