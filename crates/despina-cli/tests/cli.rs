use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use despina::{ReaderBuilder, TableDef, TypeCode, WriterBuilder};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn cli_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_despina"))
}

fn run_cli(args: &[&str]) -> Output {
    Command::new(cli_bin())
        .args(args)
        .output()
        .expect("failed to execute despina binary")
}

fn stdout_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("fixtures")
        .join("golden")
        .join(relative)
}

fn temp_file_path(stem: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let serial = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "despina-cli-{stem}-{}-{serial}-{nanos}.{extension}",
        std::process::id()
    ))
}

fn write_stats_fixture(path: &Path) {
    let tables = [
        TableDef::new("T1", TypeCode::Fixed(2)),
        TableDef::new("T2", TypeCode::Float64),
    ];
    let mut writer = WriterBuilder::new()
        .banner("STATS TEST")
        .run_id("stats-run")
        .open_path(path, 3, &tables)
        .expect("writer should open");

    let rows: [[f64; 3]; 6] = [
        [1.0, 2.0, 3.0],
        [10.0, 11.0, 12.0],
        [4.0, 5.0, 6.0],
        [13.0, 14.0, 15.0],
        [7.0, 8.0, 9.0],
        [16.0, 17.0, 18.0],
    ];
    for row in rows {
        writer
            .write_next_row(&row)
            .expect("row write should succeed");
    }
    writer.finish().expect("writer should finish");
}

fn write_conversion_fixture(path: &Path) {
    let tables = [
        TableDef::new("DIST_AM", TypeCode::Float32),
        TableDef::new("TIME_AM", TypeCode::Fixed(2)),
    ];
    let mut writer = WriterBuilder::new()
        .banner("CONVERSION TEST")
        .run_id("conversion-run")
        .open_path(path, 3, &tables)
        .expect("writer should open");

    let rows: [[f64; 3]; 6] = [
        [0.0, 7.5, 12.0],  // DIST_AM origin 1
        [0.0, 15.3, 25.0], // TIME_AM origin 1
        [8.1, 0.0, 5.5],   // DIST_AM origin 2
        [20.1, 0.0, 10.0], // TIME_AM origin 2
        [11.0, 6.0, 0.0],  // DIST_AM origin 3
        [22.0, 12.0, 0.0], // TIME_AM origin 3
    ];
    for row in rows {
        writer
            .write_next_row(&row)
            .expect("row write should succeed");
    }
    writer.finish().expect("writer should finish");
}

#[test]
fn no_command_shows_help_and_exits_with_error() {
    let output = run_cli(&[]);
    assert_ne!(output.status.code(), Some(0));
}

#[test]
fn help_flag_shows_help() {
    let output = run_cli(&["--help"]);
    let stdout = stdout_text(&output);
    assert!(stdout.contains("info"));
    assert!(stdout.contains("validate"));
    assert!(stdout.contains("stats"));
    assert!(stdout.contains("to-csv"));
    assert!(stdout.contains("from-csv"));
    assert!(stdout.contains("to-parquet"));
    assert!(stdout.contains("from-parquet"));
}

#[test]
fn info_prints_header_for_fixture() {
    let fixture = fixture_path("conforming/basic_zones_2.mat");
    let fixture_str = fixture.to_str().unwrap();
    let output = run_cli(&["info", fixture_str]);

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    assert!(stdout.contains("2"), "should show zone count");
    assert!(stdout.contains("TABLE1"));
}

#[test]
fn info_json_format() {
    let path = temp_file_path("info-json", "mat");
    write_stats_fixture(&path);
    let path_str = path.to_str().unwrap();

    let output = run_cli(&["info", "--format", "json", path_str]);
    let _ = std::fs::remove_file(&path);

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("should be valid JSON");
    assert_eq!(json["zone_count"], 3);
    assert_eq!(json["table_count"], 2);
    assert_eq!(json["tables"][0]["name"], "T1");
    assert_eq!(json["tables"][1]["name"], "T2");
}

#[test]
fn validate_reports_ok_for_conforming_fixture() {
    let fixture = fixture_path("conforming/basic_zones_2.mat");
    let fixture_str = fixture.to_str().unwrap();
    let output = run_cli(&["validate", fixture_str]);

    assert_eq!(output.status.code(), Some(0));
    assert!(stdout_text(&output).contains("OK"));
}

#[test]
fn validate_reports_failure_for_nonconforming_fixture() {
    let fixture = fixture_path("nonconforming/invalid_divide_by_zero_direct.mat");
    let fixture_str = fixture.to_str().unwrap();
    let output = run_cli(&["validate", fixture_str]);

    assert_eq!(output.status.code(), Some(1));
    let stderr = stderr_text(&output);
    assert!(stderr.contains("FAIL"));
}

#[test]
fn stats_reports_totals_and_diagonals() {
    let path = temp_file_path("stats", "mat");
    write_stats_fixture(&path);
    let path_str = path.to_str().unwrap();

    let output = run_cli(&["stats", path_str]);
    let _ = std::fs::remove_file(&path);

    assert_eq!(output.status.code(), Some(0));
    let stdout = stdout_text(&output);
    assert!(stdout.contains("T1"));
    assert!(stdout.contains("T2"));
    assert!(stdout.contains("45.000000"));
    assert!(stdout.contains("15.000000"));
    assert!(stdout.contains("126.000000"));
    assert!(stdout.contains("42.000000"));
}

#[test]
fn stats_json_format() {
    let path = temp_file_path("stats-json", "mat");
    write_stats_fixture(&path);
    let path_str = path.to_str().unwrap();

    let output = run_cli(&["stats", "--format", "json", path_str]);
    let _ = std::fs::remove_file(&path);

    assert_eq!(output.status.code(), Some(0));
    let json: serde_json::Value =
        serde_json::from_str(&stdout_text(&output)).expect("should be valid JSON");
    assert_eq!(json["zone_count"], 3);
    assert_eq!(json["tables"][0]["total"], 45.0);
    assert_eq!(json["tables"][0]["diagonal"], 15.0);
    assert_eq!(json["tables"][1]["total"], 126.0);
    assert_eq!(json["tables"][1]["diagonal"], 42.0);
}

#[test]
fn to_csv_exports_wide_format() {
    let mat_path = temp_file_path("to-csv", "mat");
    let csv_path = temp_file_path("to-csv", "csv");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(0));

    let csv_content = std::fs::read_to_string(&csv_path).unwrap();
    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);

    let first_line = csv_content.lines().next().unwrap();
    assert_eq!(first_line, "Origin,Destination,DIST_AM,TIME_AM");

    assert!(
        !csv_content.contains("\n1,1,"),
        "all-zero diagonal should be excluded by default"
    );

    assert!(csv_content.contains("1,2,7.5,15.3"));
}

#[test]
fn to_csv_include_zero_rows() {
    let mat_path = temp_file_path("to-csv-zeros", "mat");
    let csv_path = temp_file_path("to-csv-zeros", "csv");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let csv_content = std::fs::read_to_string(&csv_path).unwrap();
    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);

    assert!(csv_content.contains("1,1,"));
    assert_eq!(csv_content.lines().count(), 10);
}

#[test]
fn to_csv_table_filter() {
    let mat_path = temp_file_path("to-csv-filter", "mat");
    let csv_path = temp_file_path("to-csv-filter", "csv");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--table",
        "DIST_AM",
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let csv_content = std::fs::read_to_string(&csv_path).unwrap();
    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);

    let first_line = csv_content.lines().next().unwrap();
    assert_eq!(first_line, "Origin,Destination,DIST_AM");
}

#[test]
fn to_csv_zone_base_0() {
    let mat_path = temp_file_path("to-csv-base0", "mat");
    let csv_path = temp_file_path("to-csv-base0", "csv");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--zone-base",
        "0",
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let csv_content = std::fs::read_to_string(&csv_path).unwrap();
    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);

    let second_line = csv_content.lines().nth(1).unwrap();
    assert!(second_line.starts_with("0,0,"));
}

#[test]
fn from_csv_round_trip() {
    let mat_path = temp_file_path("from-csv-src", "mat");
    let csv_path = temp_file_path("from-csv-rt", "csv");
    let mat_out_path = temp_file_path("from-csv-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--type-code",
        "D",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let original = despina::open(&mat_path).unwrap();
    let round_tripped = despina::open(&mat_out_path).unwrap();

    assert_eq!(original.zone_count(), round_tripped.zone_count());
    assert_eq!(original.table_count(), round_tripped.table_count());

    for table in original.tables() {
        let rt_table = round_tripped.table(table.name());
        for origin in 1..=original.zone_count() {
            for destination in 1..=original.zone_count() {
                assert_eq!(
                    table.get(origin, destination),
                    rt_table.get(origin, destination),
                    "mismatch at table={}, origin={}, destination={}",
                    table.name(),
                    origin,
                    destination,
                );
            }
        }
    }

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn to_parquet_creates_file() {
    let mat_path = temp_file_path("to-pq", "mat");
    let pq_path = temp_file_path("to-pq", "parquet");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-parquet",
        mat_path.to_str().unwrap(),
        "-o",
        pq_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(0));

    let metadata = std::fs::metadata(&pq_path).unwrap();
    assert!(metadata.len() > 0);

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&pq_path);
}

#[test]
fn from_parquet_round_trip() {
    let mat_path = temp_file_path("from-pq-src", "mat");
    let pq_path = temp_file_path("from-pq-rt", "parquet");
    let mat_out_path = temp_file_path("from-pq-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-parquet",
        mat_path.to_str().unwrap(),
        "-o",
        pq_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-parquet",
        pq_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--type-code",
        "D",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let original = despina::open(&mat_path).unwrap();
    let round_tripped = despina::open(&mat_out_path).unwrap();

    assert_eq!(original.zone_count(), round_tripped.zone_count());
    assert_eq!(original.table_count(), round_tripped.table_count());

    for table in original.tables() {
        let rt_table = round_tripped.table(table.name());
        for origin in 1..=original.zone_count() {
            for destination in 1..=original.zone_count() {
                assert_eq!(
                    table.get(origin, destination),
                    rt_table.get(origin, destination),
                    "mismatch at table={}, origin={}, destination={}",
                    table.name(),
                    origin,
                    destination,
                );
            }
        }
    }

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&pq_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn parquet_zone_base_0_round_trip() {
    let mat_path = temp_file_path("pq-base0-src", "mat");
    let pq_path = temp_file_path("pq-base0", "parquet");
    let mat_out_path = temp_file_path("pq-base0-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-parquet",
        mat_path.to_str().unwrap(),
        "-o",
        pq_path.to_str().unwrap(),
        "--zone-base",
        "0",
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-parquet",
        pq_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--zone-base",
        "0",
        "--type-code",
        "D",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let original = despina::open(&mat_path).unwrap();
    let round_tripped = despina::open(&mat_out_path).unwrap();

    assert_eq!(original.zone_count(), round_tripped.zone_count());
    for table in original.tables() {
        let rt_table = round_tripped.table(table.name());
        for origin in 1..=original.zone_count() {
            for destination in 1..=original.zone_count() {
                assert_eq!(
                    table.get(origin, destination),
                    rt_table.get(origin, destination),
                );
            }
        }
    }

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&pq_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn csv_with_custom_columns_round_trip() {
    let mat_path = temp_file_path("csv-custom-col-src", "mat");
    let csv_path = temp_file_path("csv-custom-col", "csv");
    let mat_out_path = temp_file_path("csv-custom-col-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--origin-col",
        "From",
        "--destination-col",
        "To",
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let csv_content = std::fs::read_to_string(&csv_path).unwrap();
    assert!(csv_content.starts_with("From,To,"));

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--origin-col",
        "From",
        "--destination-col",
        "To",
        "--type-code",
        "D",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let original = despina::open(&mat_path).unwrap();
    let round_tripped = despina::open(&mat_out_path).unwrap();
    assert_eq!(original.zone_count(), round_tripped.zone_count());

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_with_banner_and_run_id() {
    let mat_path = temp_file_path("csv-meta-src", "mat");
    let csv_path = temp_file_path("csv-meta", "csv");
    let mat_out_path = temp_file_path("csv-meta-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--type-code",
        "S",
        "--banner",
        "MY CUSTOM BANNER",
        "--run-id",
        "MY RUN",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let reader = ReaderBuilder::new()
        .from_path(&mat_out_path)
        .expect("should parse");
    assert_eq!(reader.header().banner(), "MY CUSTOM BANNER");
    assert_eq!(reader.header().run_id(), "MY RUN");
    assert_eq!(reader.header().tables()[0].type_code(), TypeCode::Float32);

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_per_table_type_codes() {
    let mat_path = temp_file_path("per-table-src", "mat");
    let csv_path = temp_file_path("per-table", "csv");
    let mat_out_path = temp_file_path("per-table-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--type-code",
        "DIST_AM:S",
        "--type-code",
        "TIME_AM:2",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let reader = ReaderBuilder::new()
        .from_path(&mat_out_path)
        .expect("should parse");
    assert_eq!(reader.header().tables()[0].type_code(), TypeCode::Float32);
    assert_eq!(reader.header().tables()[1].type_code(), TypeCode::Fixed(2));

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_default_plus_override_type_codes() {
    let mat_path = temp_file_path("default-override-src", "mat");
    let csv_path = temp_file_path("default-override", "csv");
    let mat_out_path = temp_file_path("default-override-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    // Default S, override DIST_AM to D.
    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--type-code",
        "S",
        "--type-code",
        "DIST_AM:D",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let reader = ReaderBuilder::new()
        .from_path(&mat_out_path)
        .expect("should parse");
    // DIST_AM gets the override (D), TIME_AM gets the default (S).
    assert_eq!(reader.header().tables()[0].type_code(), TypeCode::Float64);
    assert_eq!(reader.header().tables()[1].type_code(), TypeCode::Float32);

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_rename_tables() {
    let mat_path = temp_file_path("rename-src", "mat");
    let csv_path = temp_file_path("rename", "csv");
    let mat_out_path = temp_file_path("rename-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    // Rename DIST_AM -> DISTANCE, TIME_AM -> TRAVEL_TIME.
    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--rename",
        "DIST_AM:DISTANCE",
        "--rename",
        "TIME_AM:TRAVEL_TIME",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let reader = ReaderBuilder::new()
        .from_path(&mat_out_path)
        .expect("should parse");
    assert_eq!(reader.header().tables()[0].name(), "DISTANCE");
    assert_eq!(reader.header().tables()[1].name(), "TRAVEL_TIME");

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_rename_with_per_table_type_code() {
    let mat_path = temp_file_path("rename-tc-src", "mat");
    let csv_path = temp_file_path("rename-tc", "csv");
    let mat_out_path = temp_file_path("rename-tc-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    // Rename DIST_AM -> DISTANCE, then set type code for the renamed name.
    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--rename",
        "DIST_AM:DISTANCE",
        "--type-code",
        "DISTANCE:S",
        "--type-code",
        "TIME_AM:0",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let reader = ReaderBuilder::new()
        .from_path(&mat_out_path)
        .expect("should parse");
    assert_eq!(reader.header().tables()[0].name(), "DISTANCE");
    assert_eq!(reader.header().tables()[0].type_code(), TypeCode::Float32);
    assert_eq!(reader.header().tables()[1].name(), "TIME_AM");
    assert_eq!(reader.header().tables()[1].type_code(), TypeCode::Fixed(0));

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_rejects_duplicate_od_by_default() {
    let csv_path = temp_file_path("dup-od", "csv");
    let mat_out_path = temp_file_path("dup-od-out", "mat");

    // Write CSV with a duplicate OD pair.
    std::fs::write(&csv_path, "Origin,Destination,VALUE\n1,2,10.0\n1,2,20.0\n").unwrap();

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
    ]);
    assert_ne!(output.status.code(), Some(0));
    let stderr = stderr_text(&output);
    assert!(
        stderr.contains("duplicate OD pair"),
        "should mention duplicate OD: {stderr}"
    );

    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_allows_duplicate_od_with_flag() {
    let csv_path = temp_file_path("dup-od-allow", "csv");
    let mat_out_path = temp_file_path("dup-od-allow-out", "mat");

    // Write CSV with a duplicate OD pair — last value wins.
    std::fs::write(
        &csv_path,
        "Origin,Destination,VALUE\n1,2,10.0\n1,2,20.0\n2,1,5.0\n",
    )
    .unwrap();

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--allow-duplicate-od",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let matrix = despina::open(&mat_out_path).unwrap();
    assert_eq!(matrix.table("VALUE").get(1, 2), 20.0);

    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_warns_about_missing_od_pairs() {
    let csv_path = temp_file_path("missing-od", "csv");
    let mat_out_path = temp_file_path("missing-od-out", "mat");

    // Write a sparse CSV: only 2 of 4 possible pairs for 2 zones.
    std::fs::write(&csv_path, "Origin,Destination,VALUE\n1,2,10.0\n2,1,5.0\n").unwrap();

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(0));
    let stderr = stderr_text(&output);
    assert!(
        stderr.contains("missing from input"),
        "should warn about missing OD pairs: {stderr}"
    );
    assert!(
        stderr.contains("2 of 4"),
        "should say 2 of 4 pairs missing: {stderr}"
    );

    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_strict_errors_on_missing_od() {
    let csv_path = temp_file_path("strict-od", "csv");
    let mat_out_path = temp_file_path("strict-od-out", "mat");

    std::fs::write(&csv_path, "Origin,Destination,VALUE\n1,2,10.0\n2,1,5.0\n").unwrap();

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--strict",
    ]);
    assert_ne!(output.status.code(), Some(0));
    let stderr = stderr_text(&output);
    assert!(
        stderr.contains("missing from input"),
        "should error about missing OD pairs: {stderr}"
    );

    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_no_warning_when_all_pairs_present() {
    let mat_path = temp_file_path("full-od-src", "mat");
    let csv_path = temp_file_path("full-od", "csv");
    let mat_out_path = temp_file_path("full-od-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        csv_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(0));
    let stderr = stderr_text(&output);
    assert!(
        !stderr.contains("missing"),
        "should not warn when all pairs present: {stderr}"
    );

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn csv_tab_separator_round_trip() {
    let mat_path = temp_file_path("tab-sep-src", "mat");
    let tsv_path = temp_file_path("tab-sep", "tsv");
    let mat_out_path = temp_file_path("tab-sep-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-csv",
        mat_path.to_str().unwrap(),
        "-o",
        tsv_path.to_str().unwrap(),
        "--include-zero-rows",
        "--separator",
        "tab",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let tsv_content = std::fs::read_to_string(&tsv_path).unwrap();
    let first_line = tsv_content.lines().next().unwrap();
    assert!(
        first_line.contains('\t'),
        "header should be tab-separated: {first_line}"
    );
    assert!(
        !first_line.contains(','),
        "header should not contain commas: {first_line}"
    );

    let output = run_cli(&[
        "from-csv",
        tsv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--separator",
        "tab",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let original = despina::open(&mat_path).unwrap();
    let round_tripped = despina::open(&mat_out_path).unwrap();
    assert_eq!(original.zone_count(), round_tripped.zone_count());
    assert_eq!(original.table_count(), round_tripped.table_count());

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&tsv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_parquet_per_table_type_codes() {
    let mat_path = temp_file_path("pq-tc-src", "mat");
    let pq_path = temp_file_path("pq-tc", "parquet");
    let mat_out_path = temp_file_path("pq-tc-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-parquet",
        mat_path.to_str().unwrap(),
        "-o",
        pq_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-parquet",
        pq_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--type-code",
        "DIST_AM:S",
        "--type-code",
        "TIME_AM:0",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let reader = ReaderBuilder::new()
        .from_path(&mat_out_path)
        .expect("should parse");
    assert_eq!(reader.header().tables()[0].type_code(), TypeCode::Float32);
    assert_eq!(reader.header().tables()[1].type_code(), TypeCode::Fixed(0));

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&pq_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_parquet_rename_tables() {
    let mat_path = temp_file_path("pq-rename-src", "mat");
    let pq_path = temp_file_path("pq-rename", "parquet");
    let mat_out_path = temp_file_path("pq-rename-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-parquet",
        mat_path.to_str().unwrap(),
        "-o",
        pq_path.to_str().unwrap(),
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-parquet",
        pq_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--rename",
        "DIST_AM:DISTANCE",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let reader = ReaderBuilder::new()
        .from_path(&mat_out_path)
        .expect("should parse");
    assert_eq!(reader.header().tables()[0].name(), "DISTANCE");
    assert_eq!(reader.header().tables()[1].name(), "TIME_AM");

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&pq_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn to_parquet_compression_zstd() {
    let mat_path = temp_file_path("pq-zstd-src", "mat");
    let pq_path = temp_file_path("pq-zstd", "parquet");
    let mat_out_path = temp_file_path("pq-zstd-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-parquet",
        mat_path.to_str().unwrap(),
        "-o",
        pq_path.to_str().unwrap(),
        "--compression",
        "zstd",
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-parquet",
        pq_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--type-code",
        "D",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let original = despina::open(&mat_path).unwrap();
    let round_tripped = despina::open(&mat_out_path).unwrap();

    assert_eq!(original.zone_count(), round_tripped.zone_count());
    assert_eq!(original.table_count(), round_tripped.table_count());

    for table in original.tables() {
        let rt_table = round_tripped.table(table.name());
        for origin in 1..=original.zone_count() {
            for destination in 1..=original.zone_count() {
                assert_eq!(
                    table.get(origin, destination),
                    rt_table.get(origin, destination),
                    "mismatch at table={}, origin={}, destination={}",
                    table.name(),
                    origin,
                    destination,
                );
            }
        }
    }

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&pq_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn to_parquet_compression_none() {
    let mat_path = temp_file_path("pq-none-src", "mat");
    let pq_path = temp_file_path("pq-none", "parquet");
    let mat_out_path = temp_file_path("pq-none-out", "mat");
    write_conversion_fixture(&mat_path);

    let output = run_cli(&[
        "to-parquet",
        mat_path.to_str().unwrap(),
        "-o",
        pq_path.to_str().unwrap(),
        "--compression",
        "none",
        "--include-zero-rows",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let output = run_cli(&[
        "from-parquet",
        pq_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
        "--type-code",
        "D",
    ]);
    assert_eq!(output.status.code(), Some(0));

    let original = despina::open(&mat_path).unwrap();
    let round_tripped = despina::open(&mat_out_path).unwrap();

    assert_eq!(original.zone_count(), round_tripped.zone_count());
    assert_eq!(original.table_count(), round_tripped.table_count());

    for table in original.tables() {
        let rt_table = round_tripped.table(table.name());
        for origin in 1..=original.zone_count() {
            for destination in 1..=original.zone_count() {
                assert_eq!(
                    table.get(origin, destination),
                    rt_table.get(origin, destination),
                    "mismatch at table={}, origin={}, destination={}",
                    table.name(),
                    origin,
                    destination,
                );
            }
        }
    }

    let _ = std::fs::remove_file(&mat_path);
    let _ = std::fs::remove_file(&pq_path);
    let _ = std::fs::remove_file(&mat_out_path);
}

#[test]
fn from_csv_defaults_to_float64_without_type_code_flag() {
    let csv_path = temp_file_path("no-tc", "csv");
    let mat_out_path = temp_file_path("no-tc-out", "mat");

    std::fs::write(
        &csv_path,
        "Origin,Destination,VALUE\n1,1,1.5\n1,2,2.5\n2,1,3.5\n2,2,4.5\n",
    )
    .unwrap();

    let output = run_cli(&[
        "from-csv",
        csv_path.to_str().unwrap(),
        "-o",
        mat_out_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(0));

    let reader = ReaderBuilder::new()
        .from_path(&mat_out_path)
        .expect("should parse");
    assert_eq!(reader.header().tables()[0].type_code(), TypeCode::Float64);

    let _ = std::fs::remove_file(&csv_path);
    let _ = std::fs::remove_file(&mat_out_path);
}
