from __future__ import annotations

import despina
import numpy as np
import pytest


class TestCSV:
    def test_to_csv_from_csv_round_trip(self, single_table_matrix, tmp_path):
        csv_path = tmp_path / "test.csv"
        single_table_matrix.to_csv(csv_path, include_zero_rows=True)

        restored = despina.from_csv(csv_path, on_missing_od="ignore")
        assert restored.zone_count == single_table_matrix.zone_count
        assert restored.table_count == single_table_matrix.table_count

        for name in single_table_matrix:
            np.testing.assert_array_almost_equal(
                restored[name],
                single_table_matrix[name],
            )

    def test_csv_custom_column_names(self, single_table_matrix, tmp_path):
        csv_path = tmp_path / "custom.csv"
        single_table_matrix.to_csv(
            csv_path,
            origin_col="O",
            destination_col="D",
            include_zero_rows=True,
        )

        restored = despina.from_csv(
            csv_path,
            origin_col="O",
            destination_col="D",
            on_missing_od="ignore",
        )
        assert restored.zone_count == 2

    def test_csv_zero_base(self, tmp_path):
        matrix = despina.create(2, [("T", "D")])
        matrix["T"][0, 1] = 5.0
        csv_path = tmp_path / "zero_base.csv"
        matrix.to_csv(csv_path, zone_base=0, include_zero_rows=True)

        restored = despina.from_csv(
            csv_path,
            zone_base=0,
            on_missing_od="ignore",
        )
        assert restored["T"][0, 1] == pytest.approx(5.0)


class TestPandas:
    def test_to_pandas_columns_and_dtypes(self, single_table_matrix):
        pytest.importorskip("pandas")
        frame = single_table_matrix.to_pandas(include_zero_rows=True)
        assert "Origin" in frame.columns
        assert "Destination" in frame.columns
        assert "DATA" in frame.columns
        assert frame["DATA"].dtype == np.float64

    def test_from_pandas_round_trip(self, single_table_matrix):
        pytest.importorskip("pandas")
        frame = single_table_matrix.to_pandas(include_zero_rows=True)
        restored = despina.from_pandas(frame, on_missing_od="ignore")
        assert restored.zone_count == single_table_matrix.zone_count
        np.testing.assert_array_almost_equal(
            restored["DATA"],
            single_table_matrix["DATA"],
        )

    def test_from_pandas_with_multiple_tables(self):
        pytest.importorskip("pandas")
        matrix = despina.create(2, [("A", "D"), ("B", "D")])
        matrix["A"][0, 1] = 1.0
        matrix["B"][1, 0] = 2.0
        frame = matrix.to_pandas(include_zero_rows=True)
        restored = despina.from_pandas(frame, on_missing_od="ignore")
        assert restored.table_names == ("A", "B")
        assert restored["A"][0, 1] == pytest.approx(1.0)
        assert restored["B"][1, 0] == pytest.approx(2.0)


class TestPolars:
    def test_to_polars_columns(self, single_table_matrix):
        pytest.importorskip("polars")
        frame = single_table_matrix.to_polars(include_zero_rows=True)
        assert "Origin" in frame.columns
        assert "Destination" in frame.columns
        assert "DATA" in frame.columns

    def test_from_polars_round_trip(self, single_table_matrix):
        pytest.importorskip("polars")
        frame = single_table_matrix.to_polars(include_zero_rows=True)
        restored = despina.from_polars(frame, on_missing_od="ignore")
        assert restored.zone_count == single_table_matrix.zone_count
        np.testing.assert_array_almost_equal(
            restored["DATA"],
            single_table_matrix["DATA"],
        )
