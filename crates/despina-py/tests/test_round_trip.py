from __future__ import annotations

import despina
import numpy as np
import pytest

class TestRoundTripSmall:
    def test_sparse_non_zero_values(self, small_matrix):
        data = small_matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["TRIPS"][0, 1] == 10.0
        assert restored["TRIPS"][1, 2] == 20.0
        assert restored["DIST"][0, 2] == 5.5
        assert restored["DIST"][2, 0] == 3.25

    def test_all_zeros(self, zeros_matrix):
        data = zeros_matrix.to_bytes()
        restored = despina.from_bytes(data)
        stack = np.stack(list(restored.values()))
        assert np.all(stack == 0.0)
        assert restored.zone_count == 4

    def test_single_table(self, single_table_matrix):
        data = single_table_matrix.to_bytes()
        restored = despina.from_bytes(data)
        np.testing.assert_array_equal(
            restored["DATA"],
            single_table_matrix["DATA"],
        )


class TestRoundTripTypeCodes:
    def test_all_12_type_codes(self, multi_type_matrix):
        data = multi_type_matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored.table_count == 12
        for table_index in range(12):
            original = multi_type_matrix[table_index]
            round_tripped = restored[table_index]
            np.testing.assert_array_almost_equal(
                round_tripped,
                original,
                decimal=4,
                err_msg=f"Table index {table_index} mismatch",
            )


class TestRoundTripValues:
    def test_negative_values(self):
        matrix = despina.create(3, [("NEG", "2")])
        matrix["NEG"][0, 1] = -10.5
        matrix["NEG"][1, 2] = -0.25
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["NEG"][0, 1] == pytest.approx(-10.5, abs=0.01)
        assert restored["NEG"][1, 2] == pytest.approx(-0.25, abs=0.01)

    def test_fractional_values(self):
        matrix = despina.create(2, [("FRAC", "4")])
        matrix["FRAC"][0, 1] = 3.1415
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["FRAC"][0, 1] == pytest.approx(3.1415, abs=0.0001)

    def test_magnitude_boundary_255(self):
        matrix = despina.create(2, [("T", "0")])
        matrix["T"][0, 0] = 255.0
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["T"][0, 0] == 255.0

    def test_magnitude_boundary_256(self):
        matrix = despina.create(2, [("T", "0")])
        matrix["T"][0, 0] = 256.0
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["T"][0, 0] == 256.0

    def test_magnitude_boundary_65535(self):
        matrix = despina.create(2, [("T", "0")])
        matrix["T"][0, 0] = 65535.0
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["T"][0, 0] == 65535.0

    def test_magnitude_boundary_65536(self):
        matrix = despina.create(2, [("T", "0")])
        matrix["T"][0, 0] = 65536.0
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["T"][0, 0] == 65536.0


class TestRoundTripLarge:
    def test_large_matrix_random_values(self, large_matrix):
        data = large_matrix.to_bytes()
        restored = despina.from_bytes(data)
        tolerances = [1e-10, 0.005, 1e-2, 0.5]
        for table_index in range(4):
            original = large_matrix[table_index]
            round_tripped = restored[table_index]
            np.testing.assert_allclose(
                round_tripped,
                original,
                atol=tolerances[table_index],
                err_msg=f"Table index {table_index} mismatch",
            )


class TestRoundTripMetadata:
    def test_banner_and_run_id_preserved(self):
        matrix = despina.create(
            2,
            [("T", "D")],
            banner="MY BANNER",
            run_id="RUN123",
        )
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored.banner == "MY BANNER"
        assert restored.run_id == "RUN123"


class TestRoundTripSpecialMatrices:
    def test_sparse_matrix(self):
        matrix = despina.create(20, [("T", "D")])
        matrix["T"][0, 19] = 42.0
        matrix["T"][9, 9] = 7.0
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["T"][0, 19] == 42.0
        assert restored["T"][9, 9] == 7.0
        assert restored["T"].sum() == pytest.approx(49.0)

    def test_diagonal_matrix(self):
        matrix = despina.create(5, [("T", "D")])
        for zone in range(5):
            matrix["T"][zone, zone] = float(zone + 1)
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        for zone in range(5):
            assert restored["T"][zone, zone] == float(zone + 1)
        assert restored["T"].sum() == pytest.approx(15.0)
        assert np.diag(restored["T"]).sum() == pytest.approx(15.0)


class TestViewSemantics:
    """Verify that __getitem__ returns views, not copies."""

    def test_getitem_returns_view(self):
        matrix = despina.create(3, [("T", "D")])
        arr = matrix["T"]
        arr[0, 1] = 42.0
        assert matrix["T"][0, 1] == 42.0

    def test_in_place_mutation_flows_through(self):
        matrix = despina.create(2, [("T", "D")])
        matrix["T"][0, 0] = 10.0
        matrix["T"][1, 1] = 20.0
        arr = matrix["T"]
        arr *= 2.0
        assert matrix["T"][0, 0] == 20.0
        assert matrix["T"][1, 1] == 40.0

    def test_setitem_replaces_array(self):
        matrix = despina.create(2, [("T", "D")])
        old_view = matrix["T"]
        new_data = np.ones((2, 2), dtype=np.float64) * 5.0
        matrix["T"] = new_data
        # Old view should NOT reflect new data (different array)
        assert old_view[0, 0] == 0.0
        assert matrix["T"][0, 0] == 5.0

    def test_copy_is_detached(self):
        matrix = despina.create(2, [("T", "D")])
        matrix["T"][0, 0] = 1.0
        copied = matrix.copy()
        copied["T"][0, 0] = 999.0
        assert matrix["T"][0, 0] == 1.0

    def test_round_trip_preserves_in_place_changes(self):
        matrix = despina.create(2, [("T", "D")])
        matrix["T"][0, 1] = 7.5
        data = matrix.to_bytes()
        restored = despina.from_bytes(data)
        assert restored["T"][0, 1] == 7.5
