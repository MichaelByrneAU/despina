use despina::{ErrorKind, MatrixBuilder, ReaderBuilder, RowBuf, TableDef, TypeCode, WriterBuilder};

use crate::support::{
    append_row_record, build_header_bytes, build_header_only, build_zero_matrix_bytes,
    payload_zero_row,
};

#[test]
fn empty_file_rejected() {
    let err = ReaderBuilder::new().from_bytes(&[]).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
}

#[test]
fn header_length_too_small_rejected() {
    let data = [3u8, 0, 0, 0];
    let err = ReaderBuilder::new().from_bytes(&data).unwrap_err();
    assert!(matches!(
        err.kind(),
        ErrorKind::InvalidHeaderLength {
            record_index: 1,
            total_size: 3
        }
    ));
}

#[test]
fn header_record_overrun_rejected() {
    let mut data = Vec::new();
    data.extend_from_slice(&100u32.to_le_bytes());
    data.extend_from_slice(&[0u8; 2]);

    let err = ReaderBuilder::new().from_bytes(&data).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
}

#[test]
fn par_missing_zones_rejected() {
    let bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR M=3",
        b"MVR 3\0A=0\0B=0\0C=0\0",
        b"ROW\0",
    );

    let err = ReaderBuilder::new().from_bytes(&bytes).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidPar(_)));
}

#[test]
fn par_missing_table_count_rejected() {
    let bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR Zones=10",
        b"MVR 1\0A=0\0",
        b"ROW\0",
    );

    let err = ReaderBuilder::new().from_bytes(&bytes).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidPar(_)));
}

#[test]
fn par_zero_zones_rejected() {
    let bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR Zones=0 M=1",
        b"MVR 1\0A=0\0",
        b"ROW\0",
    );

    let err = ReaderBuilder::new().from_bytes(&bytes).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidPar(_)));
}

#[test]
fn par_zero_table_count_rejected() {
    let bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR Zones=10 M=0",
        b"MVR 0\0",
        b"ROW\0",
    );

    let err = ReaderBuilder::new().from_bytes(&bytes).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidPar(_)));
}

#[test]
fn par_mvr_count_mismatch_rejected() {
    let bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR Zones=5 M=2",
        b"MVR 3\0A=0\0B=0\0C=0\0",
        b"ROW\0",
    );

    let err = ReaderBuilder::new().from_bytes(&bytes).unwrap_err();
    assert!(matches!(
        err.kind(),
        ErrorKind::TableCountMismatch { par: 2, mvr: 3 }
    ));
}

#[test]
fn mvr_invalid_type_code_rejected() {
    let bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR Zones=5 M=1",
        b"MVR 1\0A=Z\0",
        b"ROW\0",
    );

    let err = ReaderBuilder::new().from_bytes(&bytes).unwrap_err();
    assert!(matches!(
        err.kind(),
        ErrorKind::InvalidTypeCode { token } if token == "Z"
    ));
}

#[test]
fn mvr_missing_equals_rejected() {
    let bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR Zones=5 M=1",
        b"MVR 1\0NOEQUALSSIGN\0",
        b"ROW\0",
    );

    let err = ReaderBuilder::new().from_bytes(&bytes).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidMvr(_)));
}

#[test]
fn missing_row_marker_rejected() {
    let bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR Zones=5 M=1",
        b"MVR 1\0A=0\0",
        b"DATA",
    );

    let err = ReaderBuilder::new().from_bytes(&bytes).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::MissingRowMarker));
}

#[test]
fn row_chunk_size_too_small_rejected() {
    let mut bytes = build_header_only(
        b"banner",
        b"ID=test",
        b"PAR Zones=3 M=1",
        b"MVR 1\0T=0\0",
        b"ROW\0",
    );
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&1u16.to_le_bytes());

    let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
    let mut row = RowBuf::new();
    let err = reader.read_row(&mut row).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidChunkSize(1)));
}

#[test]
fn row_invalid_preamble_rejected() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];
    let mut bytes = build_header_bytes("banner", "test", 3, &tables);

    append_row_record(&mut bytes, 1, 1, &[0x00, 0x80, 0x00]);
    let zero_row = payload_zero_row();
    append_row_record(&mut bytes, 2, 1, &zero_row);
    append_row_record(&mut bytes, 3, 1, &zero_row);

    let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
    let mut row = RowBuf::new();
    let err = reader.read_row(&mut row).unwrap_err();
    assert!(matches!(
        err.kind(),
        ErrorKind::InvalidPreamble { got: [0x00, 0x80] }
    ));
}

#[test]
fn trailing_bytes_after_last_row_rejected() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];
    let mut bytes = build_zero_matrix_bytes("banner", "test", 3, &tables);
    bytes.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);

    let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
    let mut row = RowBuf::new();
    for _ in 0..3 {
        assert!(reader.read_row(&mut row).unwrap());
    }

    match reader.read_row(&mut row) {
        Ok(false) => {}
        Ok(true) => panic!("reader should not decode rows beyond expected matrix size"),
        Err(err) => assert!(matches!(err.kind(), ErrorKind::TrailingBytes)),
    }
}

#[test]
fn truncated_row_data_rejected() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];
    let bytes = build_zero_matrix_bytes("banner", "test", 10, &tables);
    let truncated = &bytes[..bytes.len() - 1];

    let mut reader = ReaderBuilder::new().from_bytes(truncated).unwrap();
    let mut row = RowBuf::new();
    let mut saw_error = false;

    loop {
        match reader.read_row(&mut row) {
            Ok(true) => {}
            Ok(false) => break,
            Err(err) => {
                assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
                saw_error = true;
                break;
            }
        }
    }

    assert!(
        saw_error,
        "truncated row data must fail during streaming read"
    );
}

#[test]
fn writer_rejects_zero_zone_count() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];
    let err = WriterBuilder::new()
        .open_writer(Vec::new(), 0, &tables)
        .unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidZoneCount));
}

#[test]
fn writer_rejects_empty_table_list() {
    let err = WriterBuilder::new()
        .open_writer(Vec::new(), 10, &[])
        .unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidTableCount(_)));
}

#[test]
fn writer_rejects_empty_table_name() {
    let tables = [TableDef::new("", TypeCode::Fixed(0))];
    let err = WriterBuilder::new()
        .open_writer(Vec::new(), 10, &tables)
        .unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidTableName(_)));
}

#[test]
fn writer_rejects_non_ascii_table_name() {
    let tables = [TableDef::new("TRÏPS", TypeCode::Fixed(0))];
    let err = WriterBuilder::new()
        .open_writer(Vec::new(), 10, &tables)
        .unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::InvalidTableName(_)));
}

#[test]
fn writer_rejects_invalid_type_code() {
    let tables = [TableDef::new("T", TypeCode::Fixed(10))];
    let err = WriterBuilder::new()
        .open_writer(Vec::new(), 5, &tables)
        .unwrap_err();
    assert!(matches!(
        err.kind(),
        ErrorKind::InvalidTypeCode { token } if token == "10"
    ));
}

#[test]
fn writer_rejects_excess_rows() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];
    let mut sink = Vec::new();
    let mut writer = WriterBuilder::new()
        .open_writer(&mut sink, 2, &tables)
        .unwrap();

    writer.write_next_row(&[0.0; 2]).unwrap();
    writer.write_next_row(&[0.0; 2]).unwrap();
    let err = writer.write_next_row(&[0.0; 2]).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::WriterFinished));
}

#[test]
fn writer_finish_rejects_incomplete_matrix() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];
    let mut sink = Vec::new();
    let mut writer = WriterBuilder::new()
        .open_writer(&mut sink, 3, &tables)
        .unwrap();

    writer.write_next_row(&[0.0; 3]).unwrap();
    let err = writer.finish().unwrap_err();
    assert!(matches!(
        err.kind(),
        ErrorKind::IncompleteMatrix {
            expected: 3,
            written: 1
        }
    ));
}

#[test]
fn writer_rejects_wrong_values_length() {
    let tables = [TableDef::new("T", TypeCode::Fixed(0))];
    let mut sink = Vec::new();
    let mut writer = WriterBuilder::new()
        .open_writer(&mut sink, 5, &tables)
        .unwrap();

    let err = writer.write_next_row(&[0.0; 3]).unwrap_err();
    assert!(matches!(
        err.kind(),
        ErrorKind::ZoneCountMismatch {
            expected: 5,
            got: 3
        }
    ));
}

#[test]
fn matrix_builder_rejects_invalid_type_code() {
    let err = MatrixBuilder::new(5)
        .table("T", TypeCode::Fixed(10))
        .build()
        .unwrap_err();
    assert!(matches!(
        err.kind(),
        ErrorKind::InvalidTypeCode { token } if token == "10"
    ));
}
