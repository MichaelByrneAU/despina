use despina::{Matrix, TableDef, TypeCode, WriterBuilder};

use crate::support::{decode_rows, read_table_sums};

fn fixed_tables(count: u8) -> Vec<TableDef> {
    (1..=count)
        .map(|index| TableDef::new(format!("T{index}"), TypeCode::Fixed(0)))
        .collect()
}

fn write_matrix(
    zone_count: u16,
    tables: &[TableDef],
    mut value_at: impl FnMut(usize, usize, usize) -> f64,
) -> Vec<u8> {
    let zone_count_usize = usize::from(zone_count);
    let mut out = Vec::new();
    let mut writer = WriterBuilder::new()
        .open_writer(&mut out, zone_count, tables)
        .expect("matrix setup should be valid");
    let mut row_values = vec![0.0f64; zone_count_usize];

    for row_index in 0..zone_count_usize {
        for table_index in 0..tables.len() {
            for (destination, value) in row_values.iter_mut().enumerate() {
                *value = value_at(row_index, table_index, destination);
            }
            writer
                .write_next_row(&row_values)
                .expect("generated row should match zone count");
        }
    }

    writer.finish().expect("all rows should be written");
    out
}

fn write_constant_matrix(zone_count: u16, tables: &[TableDef], value: f64) -> Vec<u8> {
    write_matrix(zone_count, tables, |_, _, _| value)
}

#[test]
fn zone_count_boundaries_round_trip() {
    let zone_counts = [2u16, 3, 4, 10, 100, 127, 128, 129, 255, 256, 257, 512, 1024];
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];

    for zone_count in zone_counts {
        let bytes = write_constant_matrix(zone_count, &tables, 1.0);
        let sums = read_table_sums(&bytes);
        let expected = f64::from(zone_count) * f64::from(zone_count);
        assert!((sums[0] - expected).abs() < 1e-9, "zone_count={zone_count}");
    }
}

#[test]
fn table_count_boundaries_round_trip() {
    let table_counts = [1u8, 2, 3, 5, 10, 16, 17, 31, 32];
    let zone_count = 12u16;

    for table_count in table_counts {
        let tables = fixed_tables(table_count);
        let bytes = write_matrix(zone_count, &tables, |_, table_index, _| {
            (table_index as f64 + 1.0) * 1000.0
        });

        let sums = read_table_sums(&bytes);
        let cell_count = f64::from(zone_count) * f64::from(zone_count);
        for (table_index, sum) in sums.iter().enumerate() {
            let expected = cell_count * (table_index as f64 + 1.0) * 1000.0;
            assert!((sum - expected).abs() < 1e-6, "table={}", table_index + 1);
        }
    }
}

#[test]
fn all_type_codes_uniform_value_round_trip() {
    let entries = [
        ("D0", TypeCode::Fixed(0)),
        ("D1", TypeCode::Fixed(1)),
        ("D2", TypeCode::Fixed(2)),
        ("D3", TypeCode::Fixed(3)),
        ("D4", TypeCode::Fixed(4)),
        ("D5", TypeCode::Fixed(5)),
        ("D6", TypeCode::Fixed(6)),
        ("D7", TypeCode::Fixed(7)),
        ("D8", TypeCode::Fixed(8)),
        ("D9", TypeCode::Fixed(9)),
        ("F32", TypeCode::Float32),
        ("F64", TypeCode::Float64),
    ];
    let tables: Vec<TableDef> = entries
        .iter()
        .map(|(name, type_code)| TableDef::new(*name, *type_code))
        .collect();

    let zone_count = 10u16;
    let bytes = write_constant_matrix(zone_count, &tables, 4.0);
    let sums = read_table_sums(&bytes);
    let expected = 4.0 * f64::from(zone_count) * f64::from(zone_count);

    for sum in sums {
        assert!((sum - expected).abs() < 1e-6);
    }
}

#[test]
fn all_zero_rows_round_trip() {
    for type_code in [
        TypeCode::Fixed(0),
        TypeCode::Fixed(2),
        TypeCode::Fixed(9),
        TypeCode::Float32,
        TypeCode::Float64,
    ] {
        let tables = [TableDef::new("T", type_code)];
        let bytes = write_constant_matrix(32, &tables, 0.0);
        let sums = read_table_sums(&bytes);
        assert_eq!(sums[0], 0.0, "type_code={type_code:?}");
    }
}

#[test]
fn fixed0_magnitude_boundaries_round_trip() {
    let boundaries = [
        255.0,
        256.0,
        257.0,
        65_535.0,
        65_536.0,
        65_537.0,
        16_777_215.0,
        16_777_216.0,
        50_000_000.0,
    ];
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];

    for value in boundaries {
        let bytes = write_constant_matrix(5, &tables, value);
        let decoded = decode_rows(&bytes);
        assert_eq!(decoded.zone_count, 5);
        assert_eq!(decoded.table_count, 1);
        for row in decoded.rows {
            for actual in row {
                assert_eq!(actual, value, "value={value}");
            }
        }
    }
}

#[test]
fn fixed2_negative_values_round_trip() {
    let values = [-1.0, -1.25, -100.0, -255.99, -0.01];
    let tables = [TableDef::new("T", TypeCode::Fixed(2))];

    for value in values {
        let bytes = write_constant_matrix(5, &tables, value);
        let decoded = decode_rows(&bytes);
        for row in decoded.rows {
            for actual in row {
                assert!(
                    (actual - value).abs() < 1e-10,
                    "value={value}, actual={actual}"
                );
            }
        }
    }
}

#[test]
fn fixed2_half_even_rounding() {
    let cases = [
        (0.005, 0.00),
        (0.015, 0.02),
        (0.025, 0.02),
        (0.035, 0.04),
        (1.005, 1.00),
        (-0.015, -0.02),
        (0.004, 0.00),
        (0.006, 0.01),
    ];
    let tables = [TableDef::new("T", TypeCode::Fixed(2))];

    for (input, expected) in cases {
        let bytes = write_constant_matrix(1, &tables, input);
        let decoded = decode_rows(&bytes);
        let actual = decoded.rows[0][0];
        assert!((actual - expected).abs() < 1e-10);
    }
}

#[test]
fn fixed3_half_even_rounding() {
    let cases = [
        (0.0005, 0.000),
        (0.0015, 0.002),
        (0.0025, 0.002),
        (1.2345, 1.234),
        (1.2355, 1.236),
    ];
    let tables = [TableDef::new("T", TypeCode::Fixed(3))];

    for (input, expected) in cases {
        let bytes = write_constant_matrix(1, &tables, input);
        let decoded = decode_rows(&bytes);
        let actual = decoded.rows[0][0];
        assert!((actual - expected).abs() < 1e-10);
    }
}

#[test]
fn float32_exact_values_round_trip() {
    let values = [1.0, -2.5, 0.0, 3.125, -0.5, 100.0, 0.25, 1024.0];
    let zone_count = values.len() as u16;
    let tables = [TableDef::new("T", TypeCode::Float32)];

    let bytes = write_matrix(zone_count, &tables, |_, _, destination| values[destination]);
    let decoded = decode_rows(&bytes);

    for row in decoded.rows {
        for (destination, actual) in row.into_iter().enumerate() {
            let expected = values[destination] as f32 as f64;
            assert!((actual - expected).abs() < 1e-12);
        }
    }
}

#[test]
fn float32_precision_matches_casting() {
    let zone_count = 10u16;
    let tables = [TableDef::new("T", TypeCode::Float32)];

    let bytes = write_matrix(zone_count, &tables, |row_index, _, destination| {
        ((row_index + 1) as f64) * 1.234_567_89 + destination as f64 * 0.031_25
    });
    let decoded = decode_rows(&bytes);

    for (row_index, row) in decoded.rows.iter().enumerate() {
        for (destination, actual) in row.iter().enumerate() {
            let input = ((row_index + 1) as f64) * 1.234_567_89 + destination as f64 * 0.031_25;
            let expected = input as f32 as f64;
            assert!((actual - expected).abs() < 1e-6);
        }
    }
}

#[test]
fn float64_exact_values_round_trip() {
    let values = [
        std::f64::consts::PI,
        std::f64::consts::E,
        1.0 / 3.0,
        -1e15,
        0.0,
        1.0,
        -0.123_456_789_012_345,
        42.0,
    ];
    let zone_count = values.len() as u16;
    let tables = [TableDef::new("T", TypeCode::Float64)];

    let bytes = write_matrix(zone_count, &tables, |_, _, destination| values[destination]);
    let decoded = decode_rows(&bytes);

    for row in decoded.rows {
        for (destination, actual) in row.into_iter().enumerate() {
            assert_eq!(actual, values[destination]);
        }
    }
}

#[test]
fn sparse_matrix_round_trip() {
    let non_zero = [
        (0usize, 0usize, 1.25),
        (0, 10, -3.50),
        (5, 3, 100.0),
        (10, 15, -0.01),
        (19, 19, 42.0),
    ];
    let tables = [TableDef::new("T", TypeCode::Fixed(2))];

    let bytes = write_matrix(20, &tables, |row_index, _, destination| {
        non_zero
            .iter()
            .find(|(row, dest, _)| *row == row_index && *dest == destination)
            .map(|(_, _, value)| *value)
            .unwrap_or(0.0)
    });
    let decoded = decode_rows(&bytes);

    for (row_index, row) in decoded.rows.iter().enumerate() {
        for (destination, actual) in row.iter().enumerate() {
            let expected = non_zero
                .iter()
                .find(|(row, dest, _)| *row == row_index && *dest == destination)
                .map(|(_, _, value)| *value)
                .unwrap_or(0.0);
            assert!((actual - expected).abs() < 1e-10);
        }
    }
}

#[test]
fn diagonal_matrix_round_trip() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];

    let bytes = write_matrix(15, &tables, |row_index, _, destination| {
        if destination == row_index {
            (row_index as f64 + 1.0) * 10.0
        } else {
            0.0
        }
    });
    let decoded = decode_rows(&bytes);

    for (row_index, row) in decoded.rows.iter().enumerate() {
        for (destination, actual) in row.iter().enumerate() {
            let expected = if row_index == destination {
                (row_index as f64 + 1.0) * 10.0
            } else {
                0.0
            };
            assert_eq!(*actual, expected);
        }
    }
}

#[test]
fn rle_boundary_127_128_round_trip() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];

    for zone_count in [127u16, 128u16] {
        let bytes = write_constant_matrix(zone_count, &tables, 42.0);
        let sums = read_table_sums(&bytes);
        let expected = 42.0 * f64::from(zone_count) * f64::from(zone_count);
        assert!((sums[0] - expected).abs() < 1e-6, "zone_count={zone_count}");
    }
}

#[test]
fn long_uniform_runs_round_trip() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];

    let bytes = write_constant_matrix(300, &tables, 7.0);
    let sums = read_table_sums(&bytes);
    let expected = 7.0 * 300.0 * 300.0;
    assert!((sums[0] - expected).abs() < 1e-6);
}

#[test]
fn matrix_api_round_trip() {
    let zone_count = 12u16;
    let tables = [
        TableDef::new("TRIPS", TypeCode::Fixed(2)),
        TableDef::new("DIST", TypeCode::Float32),
        TableDef::new("TIME", TypeCode::Float64),
    ];

    let bytes = write_matrix(zone_count, &tables, |_, table_index, _| match table_index {
        0 => std::f64::consts::PI,
        1 => 2.5_f32 as f64,
        _ => std::f64::consts::E,
    });

    let matrix = Matrix::from_bytes(&bytes).unwrap();
    let mut round_trip_bytes = Vec::new();
    matrix.write_to_writer(&mut round_trip_bytes).unwrap();
    let matrix_round_trip = Matrix::from_bytes(&round_trip_bytes).unwrap();

    assert_eq!(matrix_round_trip.zone_count(), zone_count);
    assert_eq!(matrix_round_trip.table_count(), 3);

    for table_index in 1..=3u8 {
        let left = matrix.table_data(table_index);
        let right = matrix_round_trip.table_data(table_index);
        assert_eq!(left.len(), right.len());
        for (lhs, rhs) in left.iter().zip(right.iter()) {
            let tolerance = if table_index == 2 { 1e-5 } else { 1e-10 };
            assert!((lhs - rhs).abs() < tolerance);
        }
    }
}

#[test]
fn matrix_api_large_round_trip_spot_checks() {
    let zone_count = 256u16;
    let tables = [
        TableDef::new("T1", TypeCode::Fixed(0)),
        TableDef::new("T2", TypeCode::Float64),
    ];

    let bytes = write_matrix(
        zone_count,
        &tables,
        |row_index, table_index, destination| {
            if table_index == 0 {
                (row_index as f64 + 1.0) * 100.0 + destination as f64
            } else {
                (row_index as f64 + 1.0) * 0.25 + destination as f64 * 0.5
            }
        },
    );

    let matrix = Matrix::from_bytes(&bytes).unwrap();
    let mut second_bytes = Vec::new();
    matrix.write_to_writer(&mut second_bytes).unwrap();
    let matrix_round_trip = Matrix::from_bytes(&second_bytes).unwrap();

    assert_eq!(matrix_round_trip.zone_count(), zone_count);
    assert_eq!(matrix_round_trip.table_count(), 2);

    let sample_origins = [1u16, zone_count / 2, zone_count];
    let sample_destinations = [1u16, zone_count / 2, zone_count];

    for origin in sample_origins {
        for destination in sample_destinations {
            let expected_fixed = f64::from(origin) * 100.0 + f64::from(destination - 1);
            let expected_float = f64::from(origin) * 0.25 + f64::from(destination - 1) * 0.5;
            assert_eq!(
                matrix_round_trip.get(1, origin, destination),
                expected_fixed
            );
            assert!((matrix_round_trip.get(2, origin, destination) - expected_float).abs() < 1e-12);
        }
    }
}
