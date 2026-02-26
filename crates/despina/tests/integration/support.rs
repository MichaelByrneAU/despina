use despina::{ReaderBuilder, RowBuf, TableDef, TypeCode};
use std::path::{Path, PathBuf};

const DESCRIPTOR_ZERO: u8 = 0x00;

// rows[(row_index - 1) * table_count + (table_index - 1)]
pub struct DecodedRows {
    pub zone_count: u16,
    pub table_count: u8,
    pub rows: Vec<Vec<f64>>,
}

fn append_record(out: &mut Vec<u8>, payload: &[u8]) {
    let total_size = payload.len() as u32 + 4;
    out.extend_from_slice(&total_size.to_le_bytes());
    out.extend_from_slice(payload);
}

fn type_code_token(type_code: TypeCode) -> u8 {
    match type_code {
        TypeCode::Fixed(p) if p <= 9 => b'0' + p,
        TypeCode::Float32 => b'S',
        TypeCode::Float64 => b'D',
        TypeCode::Fixed(p) => panic!("invalid fixed-point type code for fixture generation: {p}"),
    }
}

pub fn build_header_bytes(
    banner: &str,
    run_id: &str,
    zone_count: u16,
    tables: &[TableDef],
) -> Vec<u8> {
    assert!(
        !tables.is_empty(),
        "tables must not be empty for header fixtures"
    );

    let mut out = Vec::new();
    append_record(&mut out, banner.as_bytes());

    let run_record = format!("ID={run_id}");
    append_record(&mut out, run_record.as_bytes());

    let par = format!("PAR Zones={zone_count} M={}", tables.len());
    append_record(&mut out, par.as_bytes());

    let mut mvr = format!("MVR {}\0", tables.len()).into_bytes();
    for table in tables {
        mvr.extend_from_slice(table.name().as_bytes());
        mvr.push(b'=');
        mvr.push(type_code_token(table.type_code()));
        mvr.push(0);
    }
    append_record(&mut out, &mvr);

    append_record(&mut out, b"ROW\0");
    out
}

pub fn build_header_only(
    banner: &[u8],
    run_id: &[u8],
    par: &[u8],
    mvr: &[u8],
    row_marker: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        banner.len() + run_id.len() + par.len() + mvr.len() + row_marker.len() + 20,
    );
    append_record(&mut out, banner);
    append_record(&mut out, run_id);
    append_record(&mut out, par);
    append_record(&mut out, mvr);
    append_record(&mut out, row_marker);
    out
}

pub fn append_row_record(out: &mut Vec<u8>, row_index: u16, table_index: u8, payload: &[u8]) {
    let chunk_size = u16::try_from(payload.len() + 2).expect("payload too large for row record");
    out.extend_from_slice(&row_index.to_le_bytes());
    out.push(table_index);
    out.extend_from_slice(&chunk_size.to_le_bytes());
    out.extend_from_slice(payload);
}

pub fn payload_zero_row() -> Vec<u8> {
    vec![0x80, 0x80, DESCRIPTOR_ZERO]
}

pub fn build_zero_matrix_bytes(
    banner: &str,
    run_id: &str,
    zone_count: u16,
    tables: &[TableDef],
) -> Vec<u8> {
    let mut out = build_header_bytes(banner, run_id, zone_count, tables);
    let row_payload = payload_zero_row();

    for row_index in 1..=zone_count {
        for table_index in 1..=tables.len() as u8 {
            append_row_record(&mut out, row_index, table_index, &row_payload);
        }
    }

    out
}

pub fn parse_header(buf: &[u8]) -> despina::Header {
    ReaderBuilder::new()
        .from_bytes(buf)
        .expect("buffer should contain a valid matrix header")
        .header()
        .clone()
}

pub fn read_table_sums(buf: &[u8]) -> Vec<f64> {
    let mut reader = ReaderBuilder::new()
        .from_bytes(buf)
        .expect("buffer should decode as matrix");
    let table_count = usize::from(reader.header().table_count());
    let mut sums = vec![0.0f64; table_count];
    let mut row = RowBuf::new();

    while reader.read_row(&mut row).expect("row read should succeed") {
        sums[usize::from(row.table_index() - 1)] += row.values().iter().sum::<f64>();
    }

    sums
}

pub fn decode_rows(buf: &[u8]) -> DecodedRows {
    let mut reader = ReaderBuilder::new()
        .from_bytes(buf)
        .expect("buffer should decode as matrix");
    let zone_count = reader.header().zone_count();
    let table_count = reader.header().table_count();
    let total_rows = usize::from(zone_count) * usize::from(table_count);
    let mut rows = vec![Vec::new(); total_rows];
    let mut row = RowBuf::new();

    while reader.read_row(&mut row).expect("row read should succeed") {
        let flat_index = usize::from(row.row_index() - 1) * usize::from(table_count)
            + usize::from(row.table_index() - 1);
        rows[flat_index] = row.values().to_vec();
    }

    DecodedRows {
        zone_count,
        table_count,
        rows,
    }
}

pub fn golden_fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("golden")
        .join(relative)
}

pub fn read_golden_fixture(relative: &str) -> Vec<u8> {
    std::fs::read(golden_fixture_path(relative))
        .unwrap_or_else(|err| panic!("failed to read golden fixture {relative}: {err}"))
}
