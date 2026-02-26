use std::sync::Once;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use despina::{
    Matrix, PreparedSelection, Reader, ReaderBuilder, RowBuf, TableDef, TypeCode, WriterBuilder,
};

// 32 zones, tables T1..T32, all Fixed(2), compact and regular payloads.
const FIXTURE_COMPACT_UNIFORM: &[u8] =
    include_bytes!("../../../fixtures/bench/compact_uniform_32_tables_32_zones.mat");

// 2048 zones, 32 tables:
// ZR1..ZR8 all zero, C1..C8 constants (1..128), RW1..RW8 row index (I*1),
// PR1..PR8 scaled row index (I*1..I*8). All Fixed(2).
const FIXTURE_MEDIUM_MIXED: &[u8] =
    include_bytes!("../../../fixtures/bench/medium_mixed_modes_32_tables_2048_zones.mat");

const SELECTED_TABLE_NAMES_COMPACT: &[&str] = &["T1", "T16", "T32"];
const SELECTED_TABLE_INDICES_COMPACT: &[u8] = &[1, 16, 32];
const SELECTED_TABLE_NAMES_MEDIUM: &[&str] = &["ZR1", "C4", "PR8"];
const SELECTED_TABLE_INDICES_MEDIUM: &[u8] = &[1, 12, 32];

fn criterion_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(30)
}

fn fixture_dims(bytes: &[u8]) -> (u16, u8, u32) {
    let reader = ReaderBuilder::new()
        .from_bytes(bytes)
        .expect("fixture should parse");
    let header = reader.header();
    (
        header.zone_count(),
        header.table_count(),
        header.row_count(),
    )
}

fn assert_fixture_contracts_once() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let compact = ReaderBuilder::new()
            .from_bytes(FIXTURE_COMPACT_UNIFORM)
            .expect("compact fixture should parse");
        let compact_header = compact.header();
        assert_eq!(compact_header.zone_count(), 32);
        assert_eq!(compact_header.table_count(), 32);
        for name in SELECTED_TABLE_NAMES_COMPACT {
            assert!(
                compact_header.table_index_by_name(name).is_some(),
                "compact fixture must include table {name}",
            );
        }

        let medium = ReaderBuilder::new()
            .from_bytes(FIXTURE_MEDIUM_MIXED)
            .expect("medium fixture should parse");
        let medium_header = medium.header();
        assert_eq!(medium_header.zone_count(), 2048);
        assert_eq!(medium_header.table_count(), 32);
        for name in SELECTED_TABLE_NAMES_MEDIUM {
            assert!(
                medium_header.table_index_by_name(name).is_some(),
                "medium fixture must include table {name}",
            );
        }
    });
}

fn decode_all_rows<R: std::io::Read>(reader: &mut Reader<R>, row: &mut RowBuf) -> f64 {
    let mut checksum = 0.0f64;

    while reader.read_row(row).expect("row read should succeed") {
        checksum += f64::from(row.row_index()) * 1e-6;
        checksum += f64::from(row.table_index()) * 1e-3;
        checksum += row.values().iter().sum::<f64>();
    }

    checksum
}

fn decode_selected_rows<R: std::io::Read>(
    reader: &mut Reader<R>,
    row: &mut RowBuf,
    selection: PreparedSelection,
) -> f64 {
    let mut checksum = 0.0f64;

    while reader
        .read_selected_row(selection, row)
        .expect("row read should succeed")
    {
        checksum += f64::from(row.row_index()) * 1e-6;
        checksum += f64::from(row.table_index()) * 1e-3;
        checksum += row.values().iter().sum::<f64>();
    }

    checksum
}

fn load_matrix(bytes: &[u8]) -> Matrix {
    Matrix::from_bytes(black_box(bytes)).expect("fixture should parse")
}

fn synthetic_value(row_index: usize, table_index: usize, destination_index: usize) -> f64 {
    let raw = (row_index as u64 + 1).wrapping_mul(37_129)
        ^ (table_index as u64 + 1).wrapping_mul(4_099)
        ^ (destination_index as u64 + 1).wrapping_mul(1_048_573);
    let magnitude = (raw % 20_000) as f64 / 100.0;

    if (raw & 0x1f) == 0 {
        0.0
    } else if (raw & 1) == 0 {
        magnitude
    } else {
        -magnitude
    }
}

fn build_writer_rows(zone_count: u16, table_count: u8) -> Vec<Vec<f64>> {
    let zone_count_usize = usize::from(zone_count);
    let table_count_usize = usize::from(table_count);
    let mut rows = Vec::with_capacity(zone_count_usize * table_count_usize);

    for row_index in 0..zone_count_usize {
        for table_index in 0..table_count_usize {
            let mut row = Vec::with_capacity(zone_count_usize);
            for destination_index in 0..zone_count_usize {
                row.push(synthetic_value(row_index, table_index, destination_index));
            }
            rows.push(row);
        }
    }

    rows
}

fn build_table_defs(table_count: u8) -> Vec<TableDef> {
    (1..=table_count)
        .map(|index| TableDef::new(format!("T{index}"), TypeCode::Fixed(2)))
        .collect()
}

fn encode_rows_to_vec(zone_count: u16, tables: &[TableDef], rows: &[Vec<f64>]) -> usize {
    let expected_rows = usize::from(zone_count) * tables.len();
    assert_eq!(rows.len(), expected_rows);

    let mut writer = WriterBuilder::new()
        .open_writer(Vec::new(), zone_count, tables)
        .expect("writer setup should be valid");
    for row in rows {
        writer
            .write_next_row(row)
            .expect("fixture row width should match zone count");
    }
    let bytes = writer.finish().expect("all rows should be written");
    bytes.len()
}

fn benchmark_reader(c: &mut Criterion) {
    assert_fixture_contracts_once();
    let mut group = c.benchmark_group("reader");

    let all_row_fixtures = [
        (
            "compact_uniform_32_tables_32_zones",
            FIXTURE_COMPACT_UNIFORM,
        ),
        (
            "medium_mixed_modes_32_tables_2048_zones",
            FIXTURE_MEDIUM_MIXED,
        ),
    ];

    for (name, bytes) in all_row_fixtures {
        let (_, _, row_count) = fixture_dims(bytes);
        let mut reader = ReaderBuilder::new()
            .from_bytes(bytes)
            .expect("fixture should parse");
        let mut row = RowBuf::new();

        group.throughput(Throughput::Elements(u64::from(row_count)));
        group.bench_with_input(
            BenchmarkId::new("read_row_all_rows", name),
            &bytes,
            |b, _| {
                b.iter(|| {
                    reader.reset().expect("reader reset should succeed");
                    black_box(decode_all_rows(&mut reader, &mut row));
                });
            },
        );
    }

    let selected_fixtures = [
        (
            "compact_uniform_32_tables_32_zones",
            FIXTURE_COMPACT_UNIFORM,
            SELECTED_TABLE_NAMES_COMPACT,
            SELECTED_TABLE_INDICES_COMPACT,
        ),
        (
            "medium_mixed_modes_32_tables_2048_zones",
            FIXTURE_MEDIUM_MIXED,
            SELECTED_TABLE_NAMES_MEDIUM,
            SELECTED_TABLE_INDICES_MEDIUM,
        ),
    ];

    for (name, bytes, names, indices) in selected_fixtures {
        let (zone_count, _, _) = fixture_dims(bytes);
        let selected_row_count = u64::from(zone_count) * indices.len() as u64;
        let mut reader_for_names = ReaderBuilder::new()
            .from_bytes(bytes)
            .expect("fixture should parse");
        let mut row_for_names = RowBuf::new();
        let selection_for_names = reader_for_names
            .prepare_selection_by_name(names)
            .expect("table names should resolve");
        let mut reader_for_indices = ReaderBuilder::new()
            .from_bytes(bytes)
            .expect("fixture should parse");
        let mut row_for_indices = RowBuf::new();
        let selection_for_indices = reader_for_indices
            .prepare_selection(indices)
            .expect("table indices should be valid");

        group.throughput(Throughput::Elements(selected_row_count));
        group.bench_with_input(
            BenchmarkId::new("read_selected_rows_from_name_selection", name),
            &bytes,
            |b, _| {
                b.iter(|| {
                    reader_for_names
                        .reset()
                        .expect("reader reset should succeed");
                    black_box(decode_selected_rows(
                        &mut reader_for_names,
                        &mut row_for_names,
                        selection_for_names,
                    ));
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("read_selected_rows_from_index_selection", name),
            &bytes,
            |b, _| {
                b.iter(|| {
                    reader_for_indices
                        .reset()
                        .expect("reader reset should succeed");
                    black_box(decode_selected_rows(
                        &mut reader_for_indices,
                        &mut row_for_indices,
                        selection_for_indices,
                    ));
                });
            },
        );
    }

    group.finish();
}

fn benchmark_matrix(c: &mut Criterion) {
    assert_fixture_contracts_once();
    let mut group = c.benchmark_group("matrix");

    let fixtures = [
        (
            "compact_uniform_32_tables_32_zones",
            FIXTURE_COMPACT_UNIFORM,
        ),
        (
            "medium_mixed_modes_32_tables_2048_zones",
            FIXTURE_MEDIUM_MIXED,
        ),
    ];

    for (name, bytes) in fixtures {
        let (zone_count, table_count, _) = fixture_dims(bytes);
        let cell_count = u64::from(zone_count) * u64::from(zone_count) * u64::from(table_count);
        group.throughput(Throughput::Elements(cell_count));
        group.bench_with_input(BenchmarkId::new("from_bytes", name), &bytes, |b, bytes| {
            b.iter(|| black_box(load_matrix(bytes)));
        });
    }

    group.finish();
}

fn benchmark_writer(c: &mut Criterion) {
    assert_fixture_contracts_once();
    const ZONE_COUNT: u16 = 256;
    const TABLE_COUNT: u8 = 8;

    let mut group = c.benchmark_group("writer");
    let cell_count = u64::from(ZONE_COUNT) * u64::from(ZONE_COUNT) * u64::from(TABLE_COUNT);
    group.throughput(Throughput::Elements(cell_count));

    let tables = build_table_defs(TABLE_COUNT);
    let rows = build_writer_rows(ZONE_COUNT, TABLE_COUNT);
    group.bench_function("write_rows_synthetic_256x8", |b| {
        b.iter(|| black_box(encode_rows_to_vec(ZONE_COUNT, &tables, &rows)));
    });

    group.finish();
}

fn benchmark_aggregates(c: &mut Criterion) {
    assert_fixture_contracts_once();
    let mut group = c.benchmark_group("aggregates");

    let fixtures = [
        (
            "compact_uniform_32_tables_32_zones",
            FIXTURE_COMPACT_UNIFORM,
        ),
        (
            "medium_mixed_modes_32_tables_2048_zones",
            FIXTURE_MEDIUM_MIXED,
        ),
    ];

    for (name, bytes) in fixtures {
        let matrix = Matrix::from_bytes(bytes).expect("fixture should parse");
        let (zone_count, table_count, _) = fixture_dims(bytes);
        let cell_count = u64::from(zone_count) * u64::from(zone_count);

        group.throughput(Throughput::Elements(cell_count));
        group.bench_with_input(BenchmarkId::new("total", name), &matrix, |b, matrix| {
            b.iter(|| {
                let mut checksum = 0.0f64;
                for table_index in 1..=table_count {
                    checksum += black_box(matrix.table_by_index(table_index).total());
                }
                black_box(checksum)
            });
        });
        group.bench_with_input(
            BenchmarkId::new("diagonal_total", name),
            &matrix,
            |b, matrix| {
                b.iter(|| {
                    let mut checksum = 0.0f64;
                    for table_index in 1..=table_count {
                        checksum += black_box(matrix.table_by_index(table_index).diagonal_total());
                    }
                    black_box(checksum)
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = performance;
    config = criterion_config();
    targets = benchmark_reader, benchmark_matrix, benchmark_writer, benchmark_aggregates
}
criterion_main!(performance);
