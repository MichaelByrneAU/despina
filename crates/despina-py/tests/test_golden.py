from __future__ import annotations

import despina
import pytest
from despina import DespinaParseError

def _tolerance_for(expected: float, type_code: str) -> float:
    base = 1e-5 if type_code == "S" else 1e-6
    return base + abs(expected) * 1e-12


class TestGoldenHeaders:
    def test_basic_zones_2(self, golden_dir):
        matrix = despina.read(golden_dir / "conforming" / "basic_zones_2.mat")
        assert matrix.zone_count == 2
        assert matrix.table_count == 1
        assert matrix.table_names == ("TABLE1",)
        assert matrix.tables[0].type_code == "2"

    def test_tables_32_uniform(self, golden_dir):
        matrix = despina.read(
            golden_dir / "conforming" / "tables_32_uniform_zones_32.mat",
        )
        assert matrix.zone_count == 32
        assert matrix.table_count == 32
        expected_names = tuple(f"T{i}" for i in range(1, 33))
        assert matrix.table_names == expected_names
        for meta in matrix.tables:
            assert meta.type_code == "2"

    def test_dec_vector_full(self, golden_dir):
        matrix = despina.read(
            golden_dir / "conforming" / "dec_vector_full_0_to_9_s_d.mat",
        )
        assert matrix.zone_count == 20
        assert matrix.table_count == 12
        expected_names = tuple(f"D{i}" for i in range(10)) + ("DS", "DD")
        assert matrix.table_names == expected_names

        expected_codes = [str(i) for i in range(10)] + ["S", "D"]
        for meta, code in zip(matrix.tables, expected_codes):
            assert meta.type_code == code

    def test_value_families(self, golden_dir):
        matrix = despina.read(
            golden_dir / "conforming" / "tables_8_value_families_zones_40.mat",
        )
        assert matrix.zone_count == 40
        assert matrix.table_count == 8
        expected = (
            "ZERO",
            "ONE",
            "BYTEVAL",
            "WORDVAL",
            "HALF",
            "NEGONE",
            "PI",
            "BIGVAL",
        )
        assert matrix.table_names == expected


class TestGoldenTotals:
    def test_basic_zones_2(self, golden_dir):
        matrix = despina.read(golden_dir / "conforming" / "basic_zones_2.mat")
        assert matrix["TABLE1"].sum() == pytest.approx(
            4.0, abs=_tolerance_for(4.0, "2")
        )

    def test_tables_32_uniform(self, golden_dir):
        matrix = despina.read(
            golden_dir / "conforming" / "tables_32_uniform_zones_32.mat",
        )
        # T1 total = 1.0 * 32^2 = 1024.0
        assert matrix["T1"].sum() == pytest.approx(
            1024.0, abs=_tolerance_for(1024.0, "2")
        )
        # T32 total = 32.0 * 32^2 = 32768.0
        assert matrix["T32"].sum() == pytest.approx(
            32768.0, abs=_tolerance_for(32768.0, "2")
        )

    def test_value_families(self, golden_dir):
        matrix = despina.read(
            golden_dir / "conforming" / "tables_8_value_families_zones_40.mat",
        )
        assert matrix["ZERO"].sum() == pytest.approx(0.0, abs=1e-6)
        assert matrix["ONE"].sum() == pytest.approx(
            1600.0, abs=_tolerance_for(1600.0, "2")
        )

    def test_dec_vector_precision(self, golden_dir):
        matrix = despina.read(
            golden_dir / "conforming" / "dec_vector_full_0_to_9_s_d.mat",
        )
        # Spot-check D0 (Fixed(0)) - all 0.123456789 rounds to 0
        assert matrix["D0"].sum() == pytest.approx(0.0, abs=1e-6)
        # D1 (Fixed(1)) - 0.123456789 rounds to 0.1, total = 0.1 * 400 = 40.0
        assert matrix["D1"].sum() == pytest.approx(40.0, abs=_tolerance_for(40.0, "1"))
        # DD (Float64)
        assert matrix["DD"].sum() == pytest.approx(
            49.382715600000004,
            abs=_tolerance_for(49.382715600000004, "D"),
        )


class TestGoldenRoundTrip:
    def test_read_write_round_trip(self, golden_dir):
        path = golden_dir / "conforming" / "tables_8_value_families_zones_40.mat"
        original = despina.read(path)
        data = original.to_bytes()
        restored = despina.from_bytes(data)

        assert restored.zone_count == original.zone_count
        assert restored.table_names == original.table_names
        for meta in original.tables:
            orig_total = original[meta.name].sum()
            rest_total = restored[meta.name].sum()
            assert rest_total == pytest.approx(
                orig_total,
                abs=_tolerance_for(orig_total, meta.type_code),
            )


class TestGoldenNonconforming:
    @pytest.mark.parametrize(
        "fixture_name",
        [
            "invalid_divide_by_zero_direct.mat",
            "invalid_divide_by_zero_computed.mat",
            "invalid_elementwise_divide_by_zero_denominator.mat",
        ],
    )
    def test_rejected(self, golden_dir, fixture_name):
        path = golden_dir / "nonconforming" / fixture_name
        with pytest.raises(DespinaParseError):
            despina.read(path)


