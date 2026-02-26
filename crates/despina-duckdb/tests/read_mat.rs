use despina_duckdb::ReadMatVTab;
use duckdb::Connection;

use std::path::{Path, PathBuf};

fn golden_fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("golden")
        .join(relative)
}

fn setup() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.register_table_function::<ReadMatVTab>("read_mat")
        .unwrap();
    conn
}

fn assert_close(label: &str, actual: f64, expected: f64, tolerance: f64) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{label}: expected {expected}, got {actual}, tolerance {tolerance}"
    );
}

// TABLE1 (Fixed(2)), 2 zones, all 1.0.
const FIXTURE_BASIC_ZONES_2: &str = "conforming/basic_zones_2.mat";

// TABLE1, 200 zones, row 1 J=1..127→1.0 J=128..200→2.0.
const FIXTURE_RLE_SPLIT_AT_127: &str = "conforming/rle_split_at_127.mat";

// TABLE1, 200 zones, row 1 J=1..128→1.0 J=129..200→2.0.
const FIXTURE_RLE_SPLIT_AT_128: &str = "conforming/rle_split_at_128.mat";

// T1..T32, 32 zones, uniform 1.0..32.0.
const FIXTURE_TABLES_32_UNIFORM_ZONES_32: &str = "conforming/tables_32_uniform_zones_32.mat";

// ZERO/ONE/BYTEVAL/WORDVAL/HALF/NEGONE/PI/BIGVAL, 40 zones.
const FIXTURE_TABLES_8_VALUE_FAMILIES_ZONES_40: &str =
    "conforming/tables_8_value_families_zones_40.mat";

// T1..T6, Float32, 20 zones.
const FIXTURE_DEC_S_VALUE_SUITE: &str = "conforming/dec_s_value_suite.mat";

// T1..T6, Float64, 20 zones.
const FIXTURE_DEC_D_VALUE_SUITE: &str = "conforming/dec_d_value_suite.mat";

// T2/T4/T6 (Fixed(2)), 20 zones.
const FIXTURE_FILE_EVEN_TABLES_SUBSET: &str = "conforming/file_even_tables_subset.mat";

// D0..D9/DS/DD, 20 zones, all 12 type codes.
const FIXTURE_DEC_VECTOR_FULL_0_TO_9_S_D: &str = "conforming/dec_vector_full_0_to_9_s_d.mat";

#[test]
fn golden_row_counts() {
    let cases: &[(&str, i64)] = &[
        (FIXTURE_BASIC_ZONES_2, 4),                        // 2²
        (FIXTURE_RLE_SPLIT_AT_127, 40_000),                // 200²
        (FIXTURE_RLE_SPLIT_AT_128, 40_000),                // 200²
        (FIXTURE_TABLES_32_UNIFORM_ZONES_32, 1_024),       // 32²
        (FIXTURE_TABLES_8_VALUE_FAMILIES_ZONES_40, 1_600), // 40²
        (FIXTURE_DEC_S_VALUE_SUITE, 400),                  // 20²
        (FIXTURE_DEC_D_VALUE_SUITE, 400),                  // 20²
        (FIXTURE_FILE_EVEN_TABLES_SUBSET, 400),            // 20²
        (FIXTURE_DEC_VECTOR_FULL_0_TO_9_S_D, 400),         // 20²
    ];

    let conn = setup();
    for &(fixture, expected_count) in cases {
        let path = golden_fixture_path(fixture);
        let count: i64 = conn
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM read_mat('{}', include_zeros := true)",
                    path.display()
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, expected_count, "fixture={fixture}");
    }
}

#[test]
fn golden_column_names() {
    let cases: &[(&str, &[&str])] = &[
        (FIXTURE_BASIC_ZONES_2, &["Origin", "Destination", "TABLE1"]),
        (
            FIXTURE_FILE_EVEN_TABLES_SUBSET,
            &["Origin", "Destination", "T2", "T4", "T6"],
        ),
        (
            FIXTURE_DEC_VECTOR_FULL_0_TO_9_S_D,
            &[
                "Origin",
                "Destination",
                "D0",
                "D1",
                "D2",
                "D3",
                "D4",
                "D5",
                "D6",
                "D7",
                "D8",
                "D9",
                "DS",
                "DD",
            ],
        ),
        (
            FIXTURE_TABLES_32_UNIFORM_ZONES_32,
            &[
                "Origin",
                "Destination",
                "T1",
                "T2",
                "T3",
                "T4",
                "T5",
                "T6",
                "T7",
                "T8",
                "T9",
                "T10",
                "T11",
                "T12",
                "T13",
                "T14",
                "T15",
                "T16",
                "T17",
                "T18",
                "T19",
                "T20",
                "T21",
                "T22",
                "T23",
                "T24",
                "T25",
                "T26",
                "T27",
                "T28",
                "T29",
                "T30",
                "T31",
                "T32",
            ],
        ),
    ];

    let conn = setup();
    for &(fixture, expected_names) in cases {
        let path = golden_fixture_path(fixture);
        let mut stmt = conn
            .prepare(&format!(
                "DESCRIBE SELECT * FROM read_mat('{}', include_zeros := true)",
                path.display()
            ))
            .unwrap();

        let names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(
            names.iter().map(String::as_str).collect::<Vec<_>>(),
            expected_names,
            "fixture={fixture}"
        );
    }
}

#[test]
fn golden_table_sums() {
    let tolerance = |expected: f64| 1e-4 + expected.abs() * 1e-10;

    let cases: &[(&str, &[(&str, f64)])] = &[
        (FIXTURE_BASIC_ZONES_2, &[("TABLE1", 4.0)]),
        (
            FIXTURE_TABLES_8_VALUE_FAMILIES_ZONES_40,
            &[
                ("ZERO", 0.0),
                ("ONE", 1600.0),
                ("BYTEVAL", 408000.0),
                ("WORDVAL", 409600.0),
                ("HALF", 800.0),
                ("NEGONE", -1600.0),
                ("PI", 5024.000000000001),
                ("BIGVAL", 112000000.0),
            ],
        ),
        (
            FIXTURE_DEC_S_VALUE_SUITE,
            &[
                ("T1", 133.33319425582886),
                ("T2", 1255.9999999999995),
                ("T3", 0.0),
                ("T4", 400.0),
                ("T5", -400.0),
                ("T6", 493827156.0000001),
            ],
        ),
        (
            FIXTURE_DEC_D_VALUE_SUITE,
            &[
                ("T1", 133.33320000000006),
                ("T2", 1255.9999999999995),
                ("T3", 0.0),
                ("T4", 400.0),
                ("T5", -400.0),
                ("T6", 493827156.0000001),
            ],
        ),
        (
            FIXTURE_DEC_VECTOR_FULL_0_TO_9_S_D,
            &[
                ("D0", 0.0),
                ("D1", 40.0),
                ("D2", 47.999999999999986),
                ("D3", 49.20000000000001),
                ("D4", 49.399999999999984),
                ("D5", 49.38400000000001),
                ("D6", 49.38280000000002),
                ("D7", 49.38271999999999),
                ("D8", 49.38271600000003),
                ("D9", 49.382715600000004),
                ("DS", 49.38271641731262),
                ("DD", 49.382715600000004),
            ],
        ),
    ];

    let conn = setup();
    for &(fixture, table_sums) in cases {
        let path = golden_fixture_path(fixture);
        for &(table_name, expected_sum) in table_sums {
            let actual_sum: f64 = conn
                .query_row(
                    &format!(
                        "SELECT SUM(\"{table_name}\") \
                         FROM read_mat('{}', include_zeros := true)",
                        path.display()
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_close(
                &format!("{fixture} {table_name}"),
                actual_sum,
                expected_sum,
                tolerance(expected_sum),
            );
        }
    }
}

#[test]
fn golden_table_selection() {
    let conn = setup();

    // file_even_tables_subset with single table selection.
    let path = golden_fixture_path(FIXTURE_FILE_EVEN_TABLES_SUBSET);

    let mut stmt = conn
        .prepare(&format!(
            "DESCRIBE SELECT * FROM read_mat('{}', tables := ['T4'])",
            path.display()
        ))
        .unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, ["Origin", "Destination", "T4"]);

    let sum: f64 = conn
        .query_row(
            &format!(
                "SELECT SUM(T4) FROM read_mat('{}', tables := ['T4'], include_zeros := true)",
                path.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_close("file_even_tables_subset T4", sum, 1600.0, 1e-4);

    // tables_32_uniform_zones_32 selecting two tables.
    let path = golden_fixture_path(FIXTURE_TABLES_32_UNIFORM_ZONES_32);

    let mut stmt = conn
        .prepare(&format!(
            "DESCRIBE SELECT * FROM read_mat('{}', tables := ['T1', 'T32'])",
            path.display()
        ))
        .unwrap();
    let names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(names, ["Origin", "Destination", "T1", "T32"]);

    let sum_t1: f64 = conn
        .query_row(
            &format!(
                "SELECT SUM(T1) \
                 FROM read_mat('{}', tables := ['T1', 'T32'], include_zeros := true)",
                path.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_close("tables_32 T1", sum_t1, 1024.0, 1e-4);

    let sum_t32: f64 = conn
        .query_row(
            &format!(
                "SELECT SUM(T32) \
                 FROM read_mat('{}', tables := ['T1', 'T32'], include_zeros := true)",
                path.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_close("tables_32 T32", sum_t32, 32768.0, 1e-4);
}

#[test]
fn golden_zero_row_filtering() {
    let conn = setup();
    let path = golden_fixture_path(FIXTURE_TABLES_8_VALUE_FAMILIES_ZONES_40);

    // ZERO table only, default (exclude zeros) → 0 rows.
    let count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_mat('{}', tables := ['ZERO'])",
                path.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "ZERO default should yield 0 rows");

    // ZERO table only, include_zeros → 1600 rows (40²).
    let count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) \
                 FROM read_mat('{}', tables := ['ZERO'], include_zeros := true)",
                path.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1600, "ZERO include_zeros should yield 1600 rows");

    // ZERO + ONE, default → 1600 rows (ONE is non-zero everywhere).
    let count: i64 = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM read_mat('{}', tables := ['ZERO', 'ONE'])",
                path.display()
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1600, "ZERO+ONE default should yield 1600 rows");
}

#[test]
fn golden_rle_boundary_values() {
    let conn = setup();

    // Both RLE fixtures are byte-identical (the reference tool encodes the
    // same plane regardless of the script-level split point).  All 200
    // destinations for origin 1 carry value 1.0; every other origin is zero.
    for (fixture, label) in [
        (FIXTURE_RLE_SPLIT_AT_127, "rle_split_at_127"),
        (FIXTURE_RLE_SPLIT_AT_128, "rle_split_at_128"),
    ] {
        let path = golden_fixture_path(fixture);
        let sum: f64 = conn
            .query_row(
                &format!(
                    "SELECT SUM(TABLE1) \
                     FROM read_mat('{}', include_zeros := true) WHERE Origin = 1",
                    path.display()
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_close(&format!("{label} origin=1"), sum, 200.0, 1e-4);

        let count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM read_mat('{}')", path.display()),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 200, "{label} non-zero count");
    }
}

#[test]
fn invalid_table_name_returns_error() {
    let conn = setup();
    let path = golden_fixture_path(FIXTURE_BASIC_ZONES_2);
    let result: Result<i64, _> = conn.query_row(
        &format!(
            "SELECT COUNT(*) FROM read_mat('{}', tables := ['NONEXISTENT'])",
            path.display()
        ),
        [],
        |row| row.get(0),
    );
    assert!(result.is_err());
}

#[test]
fn nonexistent_file_returns_error() {
    let conn = setup();
    let result: Result<i64, _> = conn.query_row(
        "SELECT COUNT(*) FROM read_mat('/tmp/does_not_exist_at_all.mat')",
        [],
        |row| row.get(0),
    );
    assert!(result.is_err());
}
