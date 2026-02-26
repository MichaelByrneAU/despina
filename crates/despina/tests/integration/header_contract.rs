use despina::{TableDef, TypeCode, WriterBuilder};

use crate::support::parse_header;

fn fixed_tables(count: u8) -> Vec<TableDef> {
    (1..=count)
        .map(|index| TableDef::new(format!("T{index}"), TypeCode::Fixed(2)))
        .collect()
}

fn write_zero_matrix(banner: &str, run_id: &str, zone_count: u16, tables: &[TableDef]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut writer = WriterBuilder::new()
        .banner(banner)
        .run_id(run_id)
        .open_writer(&mut out, zone_count, tables)
        .unwrap();
    let row = vec![0.0f64; usize::from(zone_count)];
    let row_count = usize::from(zone_count) * tables.len();
    for _ in 0..row_count {
        writer.write_next_row(&row).unwrap();
    }
    writer.finish().unwrap();
    out
}

#[test]
fn zone_count_boundaries_round_trip() {
    let zone_counts = [
        2u16, 3, 4, 10, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1024,
    ];
    let tables = [TableDef::new("T", TypeCode::Fixed(2))];

    for zone_count in zone_counts {
        let run_id = format!("zones={zone_count}");
        let bytes = write_zero_matrix("MAT PGM=MATRIX VER=1", &run_id, zone_count, &tables);
        let header = parse_header(&bytes);

        assert_eq!(header.zone_count(), zone_count);
        assert_eq!(header.table_count(), 1);
        assert_eq!(header.banner(), "MAT PGM=MATRIX VER=1");
        assert_eq!(header.run_id(), run_id);
        assert_eq!(header.tables()[0].index(), 1);
        assert_eq!(header.tables()[0].name(), "T");
        assert_eq!(header.tables()[0].type_code(), TypeCode::Fixed(2));
    }
}

#[test]
fn table_count_boundaries_round_trip() {
    let table_counts = [1u8, 2, 3, 5, 10, 15, 16, 17, 31, 32];

    for table_count in table_counts {
        let tables = fixed_tables(table_count);
        let bytes = write_zero_matrix("MAT PGM=MATRIX VER=1", "table-count-boundary", 5, &tables);
        let header = parse_header(&bytes);

        assert_eq!(header.table_count(), table_count);
        assert_eq!(header.tables().len(), usize::from(table_count));
        for (index, table) in header.tables().iter().enumerate() {
            assert_eq!(table.index(), index as u8 + 1);
            assert_eq!(table.name(), format!("T{}", index + 1));
        }
    }
}

#[test]
fn type_code_catalogue_round_trip() {
    let entries = [
        ("DEC0", TypeCode::Fixed(0)),
        ("DEC1", TypeCode::Fixed(1)),
        ("DEC2", TypeCode::Fixed(2)),
        ("DEC3", TypeCode::Fixed(3)),
        ("DEC4", TypeCode::Fixed(4)),
        ("DEC5", TypeCode::Fixed(5)),
        ("DEC6", TypeCode::Fixed(6)),
        ("DEC7", TypeCode::Fixed(7)),
        ("DEC8", TypeCode::Fixed(8)),
        ("DEC9", TypeCode::Fixed(9)),
        ("F32", TypeCode::Float32),
        ("F64", TypeCode::Float64),
    ];
    let tables: Vec<TableDef> = entries
        .iter()
        .map(|(name, type_code)| TableDef::new(*name, *type_code))
        .collect();

    let bytes = write_zero_matrix("MAT PGM=MATRIX VER=1", "type-code-catalogue", 4, &tables);
    let header = parse_header(&bytes);

    assert_eq!(header.table_count(), entries.len() as u8);
    for ((expected_name, expected_type), table) in entries.iter().zip(header.tables()) {
        assert_eq!(table.name(), *expected_name);
        assert_eq!(table.type_code(), *expected_type);
    }
}

#[test]
fn banner_and_run_id_round_trip() {
    let cases = [
        ("MAT PGM=MATRIX VER=1", "Model Run"),
        ("DESPINA", "Morning Peak"),
        ("Custom Banner", "run-identifier-123"),
        ("", "non-empty"),
        ("non-empty", ""),
        ("", ""),
    ];
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];

    for (banner, run_id) in cases {
        let bytes = write_zero_matrix(banner, run_id, 2, &tables);
        let header = parse_header(&bytes);
        assert_eq!(header.banner(), banner);
        assert_eq!(header.run_id(), run_id);
    }
}

#[test]
fn table_names_round_trip() {
    let names = [
        "T",
        "T1",
        "AM",
        "TRIPS",
        "DIST",
        "TIME",
        "TOLL",
        "SKIMTIME",
        "LONGNAME1",
    ];
    let tables: Vec<TableDef> = names
        .iter()
        .map(|name| TableDef::new(*name, TypeCode::Fixed(2)))
        .collect();

    let bytes = write_zero_matrix("MAT PGM=MATRIX VER=1", "table-name-suite", 2, &tables);
    let header = parse_header(&bytes);

    assert_eq!(header.tables().len(), names.len());
    for (expected, table) in names.iter().zip(header.tables()) {
        assert_eq!(table.name(), *expected);
    }
}

#[test]
fn table_index_lookup_is_case_sensitive() {
    let tables = [
        TableDef::new("TRIPS", TypeCode::Fixed(2)),
        TableDef::new("DIST", TypeCode::Float32),
        TableDef::new("TIME", TypeCode::Float64),
    ];

    let bytes = write_zero_matrix("MAT PGM=MATRIX VER=1", "table-lookup-suite", 4, &tables);
    let header = parse_header(&bytes);

    assert_eq!(header.table_index_by_name("TRIPS"), Some(1));
    assert_eq!(header.table_index_by_name("DIST"), Some(2));
    assert_eq!(header.table_index_by_name("TIME"), Some(3));
    assert_eq!(header.table_index_by_name("trips"), None);
    assert_eq!(header.table_index_by_name("MISSING"), None);
}

#[test]
fn row_count_formula_holds() {
    let cases = [(2u16, 1u8), (5, 3), (10, 10), (100, 5), (255, 2), (1000, 1)];

    for (zone_count, table_count) in cases {
        let tables = fixed_tables(table_count);
        let bytes = write_zero_matrix(
            "MAT PGM=MATRIX VER=1",
            "row-count-formula",
            zone_count,
            &tables,
        );
        let header = parse_header(&bytes);

        assert_eq!(
            header.row_count(),
            u32::from(zone_count) * u32::from(table_count)
        );
    }
}

#[test]
fn mixed_32_table_catalogue_round_trip() {
    let tables: Vec<TableDef> = (0..32u8)
        .map(|index| {
            let type_code = match index % 4 {
                0 => TypeCode::Fixed(0),
                1 => TypeCode::Fixed(2),
                2 => TypeCode::Float32,
                _ => TypeCode::Float64,
            };
            TableDef::new(format!("T{:02}", index + 1), type_code)
        })
        .collect();

    let bytes = write_zero_matrix("MAT PGM=MATRIX VER=1", "mixed-32-catalogue", 10, &tables);
    let header = parse_header(&bytes);

    assert_eq!(header.table_count(), 32);
    for (index, table) in header.tables().iter().enumerate() {
        let expected_type = match index % 4 {
            0 => TypeCode::Fixed(0),
            1 => TypeCode::Fixed(2),
            2 => TypeCode::Float32,
            _ => TypeCode::Float64,
        };
        assert_eq!(table.index(), index as u8 + 1);
        assert_eq!(table.name(), format!("T{:02}", index + 1));
        assert_eq!(table.type_code(), expected_type);
    }
}
