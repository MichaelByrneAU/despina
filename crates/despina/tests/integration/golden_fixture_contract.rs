use despina::{ErrorKind, Matrix, ReaderBuilder, RowBuf, TypeCode};

use crate::support::read_golden_fixture;

struct HeaderCase {
    fixture: &'static str,
    zone_count: u16,
    tables: &'static [(&'static str, TypeCode)],
}

struct SummaryCase {
    fixture: &'static str,
    expected: &'static [(&'static str, f64, f64)],
}

// TABLE1 is all 1.0 for 2 zones.
const FIXTURE_BASIC_ZONES_2: &str = "conforming/basic_zones_2.mat";

// 200 zones, row 1 splits at J=127 (1.0 then 2.0), rest 0.
const FIXTURE_RLE_SPLIT_AT_127: &str = "conforming/rle_split_at_127.mat";

// 200 zones, row 1 splits at J=128 (1.0 then 2.0), rest 0.
const FIXTURE_RLE_SPLIT_AT_128: &str = "conforming/rle_split_at_128.mat";

// 32 zones, T1..T32 are uniform constants 1.0..32.0.
const FIXTURE_TABLES_32_UNIFORM_ZONES_32: &str = "conforming/tables_32_uniform_zones_32.mat";

// 40 zones, ZERO/ONE/BYTEVAL/WORDVAL/HALF/NEGONE/PI/BIGVAL constants.
const FIXTURE_TABLES_8_VALUE_FAMILIES_ZONES_40: &str =
    "conforming/tables_8_value_families_zones_40.mat";

// 20 zones, T1..T6 constants under DEC=S.
const FIXTURE_DEC_S_VALUE_SUITE: &str = "conforming/dec_s_value_suite.mat";

// 20 zones, T1..T6 constants under DEC=D.
const FIXTURE_DEC_D_VALUE_SUITE: &str = "conforming/dec_d_value_suite.mat";

// 20 zones, even-table subset T2/T4/T6 as 2.0/4.0/6.0.
const FIXTURE_FILE_EVEN_TABLES_SUBSET: &str = "conforming/file_even_tables_subset.mat";

// 20 zones, D0..D9/DS/DD all from 0.123456789, one DEC mode each.
const FIXTURE_DEC_VECTOR_FULL_0_TO_9_S_D: &str = "conforming/dec_vector_full_0_to_9_s_d.mat";

// Single-table 1/0 case, header exists but row stream is incomplete.
const FIXTURE_INVALID_DIVIDE_BY_ZERO_DIRECT: &str =
    "nonconforming/invalid_divide_by_zero_direct.mat";

// Single-table 1/(I-I) case, header exists but row stream is incomplete.
const FIXTURE_INVALID_DIVIDE_BY_ZERO_COMPUTED: &str =
    "nonconforming/invalid_divide_by_zero_computed.mat";

// NUMER/DENOM/RESULT with zero denominator, row stream is incomplete.
const FIXTURE_INVALID_ELEMENTWISE_DIVIDE_BY_ZERO_DENOMINATOR: &str =
    "nonconforming/invalid_elementwise_divide_by_zero_denominator.mat";

fn assert_close(label: &str, actual: f64, expected: f64, tolerance: f64) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{label}: expected {expected}, got {actual}, tolerance {tolerance}"
    );
}

fn tolerance_for(expected: f64, type_code: TypeCode) -> f64 {
    let base = match type_code {
        TypeCode::Float32 => 1e-5,
        _ => 1e-6,
    };
    base + expected.abs() * 1e-12
}

fn header_prefix_end(bytes: &[u8]) -> usize {
    let mut offset = 0usize;
    for _ in 0..5 {
        assert!(
            offset + 4 <= bytes.len(),
            "header record size prefix truncated"
        );
        let total_size = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        assert!(
            total_size >= 4,
            "header record total_size must be at least 4"
        );
        offset += total_size;
        assert!(offset <= bytes.len(), "header record overruns file size");
    }
    offset
}

#[test]
fn golden_headers_match_expected_catalogue() {
    const TABLES_32: [(&str, TypeCode); 32] = [
        ("T1", TypeCode::Fixed(2)),
        ("T2", TypeCode::Fixed(2)),
        ("T3", TypeCode::Fixed(2)),
        ("T4", TypeCode::Fixed(2)),
        ("T5", TypeCode::Fixed(2)),
        ("T6", TypeCode::Fixed(2)),
        ("T7", TypeCode::Fixed(2)),
        ("T8", TypeCode::Fixed(2)),
        ("T9", TypeCode::Fixed(2)),
        ("T10", TypeCode::Fixed(2)),
        ("T11", TypeCode::Fixed(2)),
        ("T12", TypeCode::Fixed(2)),
        ("T13", TypeCode::Fixed(2)),
        ("T14", TypeCode::Fixed(2)),
        ("T15", TypeCode::Fixed(2)),
        ("T16", TypeCode::Fixed(2)),
        ("T17", TypeCode::Fixed(2)),
        ("T18", TypeCode::Fixed(2)),
        ("T19", TypeCode::Fixed(2)),
        ("T20", TypeCode::Fixed(2)),
        ("T21", TypeCode::Fixed(2)),
        ("T22", TypeCode::Fixed(2)),
        ("T23", TypeCode::Fixed(2)),
        ("T24", TypeCode::Fixed(2)),
        ("T25", TypeCode::Fixed(2)),
        ("T26", TypeCode::Fixed(2)),
        ("T27", TypeCode::Fixed(2)),
        ("T28", TypeCode::Fixed(2)),
        ("T29", TypeCode::Fixed(2)),
        ("T30", TypeCode::Fixed(2)),
        ("T31", TypeCode::Fixed(2)),
        ("T32", TypeCode::Fixed(2)),
    ];

    const DEC_VECTOR_FULL: [(&str, TypeCode); 12] = [
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
        ("DS", TypeCode::Float32),
        ("DD", TypeCode::Float64),
    ];

    let cases = [
        HeaderCase {
            fixture: FIXTURE_BASIC_ZONES_2,
            zone_count: 2,
            tables: &[("TABLE1", TypeCode::Fixed(2))],
        },
        HeaderCase {
            fixture: FIXTURE_RLE_SPLIT_AT_127,
            zone_count: 200,
            tables: &[("TABLE1", TypeCode::Fixed(2))],
        },
        HeaderCase {
            fixture: FIXTURE_RLE_SPLIT_AT_128,
            zone_count: 200,
            tables: &[("TABLE1", TypeCode::Fixed(2))],
        },
        HeaderCase {
            fixture: FIXTURE_TABLES_32_UNIFORM_ZONES_32,
            zone_count: 32,
            tables: &TABLES_32,
        },
        HeaderCase {
            fixture: FIXTURE_FILE_EVEN_TABLES_SUBSET,
            zone_count: 20,
            tables: &[
                ("T2", TypeCode::Fixed(2)),
                ("T4", TypeCode::Fixed(2)),
                ("T6", TypeCode::Fixed(2)),
            ],
        },
        HeaderCase {
            fixture: FIXTURE_DEC_VECTOR_FULL_0_TO_9_S_D,
            zone_count: 20,
            tables: &DEC_VECTOR_FULL,
        },
    ];

    for case in cases {
        let bytes = read_golden_fixture(case.fixture);
        let reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let header = reader.header();

        assert_eq!(
            header.zone_count(),
            case.zone_count,
            "fixture={}",
            case.fixture
        );
        assert_eq!(
            header.table_count() as usize,
            case.tables.len(),
            "fixture={}",
            case.fixture
        );

        for (table_info, expected) in header.tables().iter().zip(case.tables.iter()) {
            assert_eq!(table_info.name(), expected.0, "fixture={}", case.fixture);
            assert_eq!(
                table_info.type_code(),
                expected.1,
                "fixture={}",
                case.fixture
            );
        }
    }
}

#[test]
fn golden_table_summaries_match_expected_subset() {
    let cases = [
        SummaryCase {
            fixture: FIXTURE_BASIC_ZONES_2,
            expected: &[("TABLE1", 4.0, 2.0)],
        },
        SummaryCase {
            fixture: FIXTURE_TABLES_8_VALUE_FAMILIES_ZONES_40,
            expected: &[
                ("ZERO", 0.0, 0.0),
                ("ONE", 1600.0, 40.0),
                ("BYTEVAL", 408000.0, 10200.0),
                ("WORDVAL", 409600.0, 10240.0),
                ("HALF", 800.0, 20.0),
                ("NEGONE", -1600.0, -40.0),
                ("PI", 5024.000000000001, 125.60000000000001),
                ("BIGVAL", 112000000.0, 2800000.0),
            ],
        },
        SummaryCase {
            fixture: FIXTURE_DEC_S_VALUE_SUITE,
            expected: &[
                ("T1", 133.33319425582886, 6.666659712791443),
                ("T2", 1255.9999999999995, 62.800000000000004),
                ("T3", 0.0, 0.0),
                ("T4", 400.0, 20.0),
                ("T5", -400.0, -20.0),
                ("T6", 493827156.0000001, 24691357.800000004),
            ],
        },
        SummaryCase {
            fixture: FIXTURE_DEC_D_VALUE_SUITE,
            expected: &[
                ("T1", 133.33320000000006, 6.6666599999999985),
                ("T2", 1255.9999999999995, 62.800000000000004),
                ("T3", 0.0, 0.0),
                ("T4", 400.0, 20.0),
                ("T5", -400.0, -20.0),
                ("T6", 493827156.0000001, 24691357.800000004),
            ],
        },
        SummaryCase {
            fixture: FIXTURE_DEC_VECTOR_FULL_0_TO_9_S_D,
            expected: &[
                ("D0", 0.0, 0.0),
                ("D1", 40.0, 2.0000000000000004),
                ("D2", 47.999999999999986, 2.4000000000000012),
                ("D3", 49.20000000000001, 2.460000000000001),
                ("D4", 49.399999999999984, 2.4699999999999993),
                ("D5", 49.38400000000001, 2.4692),
                ("D6", 49.38280000000002, 2.46914),
                ("D7", 49.38271999999999, 2.469136),
                ("D8", 49.38271600000003, 2.469135800000001),
                ("D9", 49.382715600000004, 2.46913578),
                ("DS", 49.38271641731262, 2.469135820865631),
                ("DD", 49.382715600000004, 2.46913578),
            ],
        },
    ];

    for case in cases {
        let bytes = read_golden_fixture(case.fixture);
        let matrix = Matrix::from_bytes(&bytes).unwrap();

        for &(table_name, expected_total, expected_diagonal) in case.expected {
            let table = matrix.table(table_name);
            let total_tolerance = tolerance_for(expected_total, table.type_code());
            let diagonal_tolerance = tolerance_for(expected_diagonal, table.type_code());
            assert_close(
                &format!("{} total {}", case.fixture, table_name),
                table.total(),
                expected_total,
                total_tolerance,
            );
            assert_close(
                &format!("{} diagonal {}", case.fixture, table_name),
                table.diagonal_total(),
                expected_diagonal,
                diagonal_tolerance,
            );
        }
    }
}

#[test]
fn golden_headers_round_trip_byte_for_byte() {
    let fixtures = [
        FIXTURE_BASIC_ZONES_2,
        FIXTURE_RLE_SPLIT_AT_127,
        FIXTURE_RLE_SPLIT_AT_128,
        FIXTURE_TABLES_32_UNIFORM_ZONES_32,
        FIXTURE_TABLES_8_VALUE_FAMILIES_ZONES_40,
        FIXTURE_DEC_S_VALUE_SUITE,
        FIXTURE_DEC_D_VALUE_SUITE,
        FIXTURE_FILE_EVEN_TABLES_SUBSET,
        FIXTURE_DEC_VECTOR_FULL_0_TO_9_S_D,
    ];

    for fixture in fixtures {
        let bytes = read_golden_fixture(fixture);
        let matrix = Matrix::from_bytes(&bytes).unwrap();
        let mut round_trip = Vec::new();
        matrix.write_to_writer(&mut round_trip).unwrap();

        let source_header_end = header_prefix_end(&bytes);
        let round_trip_header_end = header_prefix_end(&round_trip);
        assert_eq!(
            &bytes[..source_header_end],
            &round_trip[..round_trip_header_end],
            "fixture={fixture}"
        );
    }
}

#[test]
fn golden_nonconforming_fixtures_are_rejected() {
    let fixtures = [
        FIXTURE_INVALID_DIVIDE_BY_ZERO_DIRECT,
        FIXTURE_INVALID_DIVIDE_BY_ZERO_COMPUTED,
        FIXTURE_INVALID_ELEMENTWISE_DIVIDE_BY_ZERO_DENOMINATOR,
    ];

    for fixture in fixtures {
        let bytes = read_golden_fixture(fixture);

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        let err = loop {
            match reader.read_row(&mut row) {
                Ok(true) => {}
                Ok(false) => panic!("fixture should be nonconforming: {fixture}"),
                Err(err) => break err,
            }
        };
        assert!(
            matches!(err.kind(), ErrorKind::UnexpectedEof),
            "fixture={fixture}"
        );

        let err = Matrix::from_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err.kind(), ErrorKind::UnexpectedEof),
            "fixture={fixture}"
        );
    }
}
