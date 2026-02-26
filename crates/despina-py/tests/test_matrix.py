from __future__ import annotations

import pytest

import despina
import numpy as np
from despina import TableSpec


class TestConstructionAndMetadata:
    def test_zone_count(self, small_matrix):
        assert small_matrix.zone_count == 3

    def test_table_count(self, small_matrix):
        assert small_matrix.table_count == 2

    def test_table_names(self, small_matrix):
        assert small_matrix.table_names == ("TRIPS", "DIST")

    def test_table_defs(self, small_matrix):
        assert small_matrix.table_defs == (("TRIPS", "D"), ("DIST", "2"))

    def test_banner_default(self, small_matrix):
        assert isinstance(small_matrix.banner, str)

    def test_run_id_default(self, small_matrix):
        assert isinstance(small_matrix.run_id, str)

    def test_custom_banner_and_run_id(self):
        matrix = despina.create(
            2,
            [("T", "D")],
            banner="Test Banner",
            run_id="Run001",
        )
        assert matrix.banner == "Test Banner"
        assert matrix.run_id == "Run001"

    def test_create_with_table_spec(self):
        matrix = despina.create(2, [TableSpec.float64("TRIPS")])
        assert matrix.table_names == ("TRIPS",)

    def test_create_with_mixed_input(self):
        matrix = despina.create(
            2,
            [
                TableSpec.float64("TRIPS"),
                ("DIST", "2"),
                despina.table("TIME", "S"),
            ],
        )
        assert matrix.table_names == ("TRIPS", "DIST", "TIME")


class TestLike:
    def test_like_copies_schema(self):
        source = despina.create(
            5,
            [("TRIPS", "D"), ("DIST", "2")],
            banner="AM Peak",
            run_id="R001",
        )
        result = despina.Matrix.like(source)
        assert result.zone_count == 5
        assert result.table_names == ("TRIPS", "DIST")
        assert result.table_defs == (("TRIPS", "D"), ("DIST", "2"))
        assert result.banner == "AM Peak"
        assert result.run_id == "R001"
        # Should be zero-initialised
        assert result["TRIPS"].sum() == 0.0

    def test_like_override_banner(self):
        source = despina.create(3, [("T", "D")], banner="Old", run_id="R1")
        result = despina.Matrix.like(source, banner="New", run_id="R2")
        assert result.banner == "New"
        assert result.run_id == "R2"

    def test_like_module_function(self):
        source = despina.create(3, [("T", "D")])
        result = despina.like(source)
        assert result.zone_count == source.zone_count
        assert result.table_names == source.table_names


class TestNumpyNativeCellAccess:
    def test_uninitialised_is_zero(self):
        matrix = despina.create(3, [("T", "D")])
        assert matrix["T"][0, 0] == 0.0

    def test_set_get_round_trip(self):
        matrix = despina.create(3, [("T", "D")])
        matrix["T"][1, 2] = 7.5
        assert matrix["T"][1, 2] == 7.5

    def test_row_access(self, single_table_matrix):
        row = single_table_matrix["DATA"][0, :]
        assert row.shape == (2,)
        np.testing.assert_array_equal(row, [1.0, 2.0])

    def test_row_access_second_row(self, single_table_matrix):
        row = single_table_matrix["DATA"][1, :]
        assert row.shape == (2,)
        np.testing.assert_array_equal(row, [3.0, 4.0])

    def test_view_semantics(self):
        matrix = despina.create(2, [("T", "D")])
        matrix["T"] = np.ones((2, 2))
        array = matrix["T"]
        array[0, 0] = 999.0
        # View: mutation flows through
        assert matrix["T"][0, 0] == 999.0


class TestContainerProtocol:
    def test_contains_by_name(self, small_matrix):
        assert "TRIPS" in small_matrix
        assert "MISSING" not in small_matrix

    def test_contains_rejects_int(self, small_matrix):
        assert 0 not in small_matrix

    def test_contains_bool_returns_false(self, small_matrix):
        assert True not in small_matrix
        assert False not in small_matrix

    def test_getitem_by_name(self, small_matrix):
        array = small_matrix["TRIPS"]
        assert isinstance(array, np.ndarray)
        assert array.shape == (3, 3)
        assert array.dtype == np.float64
        assert array[0, 1] == 10.0

    def test_getitem_by_index(self, small_matrix):
        array = small_matrix[0]
        assert isinstance(array, np.ndarray)
        assert array.shape == (3, 3)
        assert array[0, 1] == 10.0

    def test_setitem_by_name(self):
        matrix = despina.create(2, [("T", "D")])
        matrix["T"] = np.ones((2, 2))
        assert matrix["T"][0, 0] == 1.0

    def test_setitem_from_list_of_lists(self):
        matrix = despina.create(2, [("T", "D")])
        matrix["T"] = [[1.0, 2.0], [3.0, 4.0]]
        assert matrix["T"][0, 1] == 2.0

    def test_len(self, small_matrix):
        assert len(small_matrix) == 2

    def test_iter_yields_names(self, small_matrix):
        names = list(small_matrix)
        assert names == ["TRIPS", "DIST"]
        assert all(isinstance(name, str) for name in names)

    def test_keys(self, small_matrix):
        assert small_matrix.keys() == ("TRIPS", "DIST")

    def test_values_returns_ndarrays(self, small_matrix):
        arrays = small_matrix.values()
        assert len(arrays) == 2
        assert all(isinstance(a, np.ndarray) for a in arrays)
        assert arrays[0][0, 1] == 10.0

    def test_items_returns_name_array_pairs(self, small_matrix):
        pairs = small_matrix.items()
        assert len(pairs) == 2
        assert pairs[0][0] == "TRIPS"
        assert isinstance(pairs[0][1], np.ndarray)
        assert pairs[1][0] == "DIST"

    def test_dict_conversion(self):
        matrix = despina.create(2, [("A", "D"), ("B", "D")])
        matrix["A"] = np.ones((2, 2))
        matrix["B"] = np.full((2, 2), 2.0)
        result = dict(matrix)
        assert set(result.keys()) == {"A", "B"}
        assert result["A"][0, 0] == 1.0
        assert result["B"][0, 0] == 2.0

    def test_arithmetic_workflow(self):
        am = despina.create(3, [("TRIPS", "D")])
        pm = despina.create(3, [("TRIPS", "D")])
        am["TRIPS"] = np.ones((3, 3))
        pm["TRIPS"] = np.full((3, 3), 2.0)
        result = despina.Matrix.like(am)
        for name in am:
            result[name] = am[name] + pm[name]
        assert result["TRIPS"].sum() == pytest.approx(27.0)


class TestTableFiltering:
    def test_read_subset(self, small_matrix, tmp_path):
        path = tmp_path / "filter.mat"
        small_matrix.write(path)
        filtered = despina.read(str(path), tables=["DIST"])
        assert filtered.table_count == 1
        assert filtered.table_names == ("DIST",)
        assert filtered["DIST"][0, 2] == 5.5

    def test_from_bytes_subset(self, small_matrix):
        data = small_matrix.to_bytes()
        filtered = despina.from_bytes(data, tables=["TRIPS"])
        assert filtered.table_count == 1
        assert filtered.table_names == ("TRIPS",)
        assert filtered["TRIPS"][0, 1] == 10.0

    def test_none_loads_all(self, small_matrix):
        data = small_matrix.to_bytes()
        restored = despina.from_bytes(data, tables=None)
        assert restored.table_count == 2
        assert restored.table_names == ("TRIPS", "DIST")

    def test_preserves_file_order(self, small_matrix):
        data = small_matrix.to_bytes()
        filtered = despina.from_bytes(data, tables=["DIST", "TRIPS"])
        assert filtered.table_names == ("TRIPS", "DIST")

    def test_unknown_name_raises(self, small_matrix):
        data = small_matrix.to_bytes()
        with pytest.raises(despina.DespinaValidationError):
            despina.from_bytes(data, tables=["NONEXISTENT"])

    def test_empty_list_raises(self, small_matrix):
        data = small_matrix.to_bytes()
        with pytest.raises(despina.DespinaValidationError):
            despina.from_bytes(data, tables=[])

    def test_filtered_round_trip(self, small_matrix):
        data = small_matrix.to_bytes()
        filtered = despina.from_bytes(data, tables=["DIST"])
        round_tripped = despina.from_bytes(filtered.to_bytes())
        assert round_tripped.table_count == 1
        assert round_tripped.table_names == ("DIST",)
        assert round_tripped["DIST"][0, 2] == 5.5
        assert round_tripped["DIST"][2, 0] == 3.25


class TestSerialisation:
    def test_to_bytes_returns_bytes(self, small_matrix):
        data = small_matrix.to_bytes()
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_from_bytes_round_trip(self, small_matrix):
        data = small_matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored.zone_count == small_matrix.zone_count
        assert restored.table_names == small_matrix.table_names

    def test_copy_creates_independent_matrix(self, small_matrix):
        copied = small_matrix.copy()
        copied["TRIPS"][0, 1] = 999.0
        assert small_matrix["TRIPS"][0, 1] == 10.0
        assert copied["TRIPS"][0, 1] == 999.0

    def test_write_read_round_trip(self, small_matrix, tmp_path):
        path = tmp_path / "test.mat"
        small_matrix.write(path)
        restored = despina.read(str(path))
        assert restored.zone_count == small_matrix.zone_count
        assert restored.table_names == small_matrix.table_names
        assert restored["TRIPS"][0, 1] == small_matrix["TRIPS"][0, 1]

    def test_bytes_dunder(self, small_matrix):
        assert bytes(small_matrix) == small_matrix.to_bytes()
