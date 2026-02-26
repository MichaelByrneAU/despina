from __future__ import annotations

import pytest
from despina import DespinaValidationError, TableSpec, TypeCode, table


class TestTypeCode:
    def test_float64_token(self):
        assert TypeCode.float64().token == "D"

    def test_float32_token(self):
        assert TypeCode.float32().token == "S"

    def test_fixed_range(self):
        for decimal_places in range(10):
            tc = TypeCode.fixed(decimal_places)
            assert tc.token == str(decimal_places)

    def test_invalid_token_raises(self):
        with pytest.raises(DespinaValidationError):
            TypeCode("X")

    def test_parse_from_string(self):
        assert TypeCode.parse("D").token == "D"
        assert TypeCode.parse("S").token == "S"
        assert TypeCode.parse("5").token == "5"

    def test_parse_from_int(self):
        assert TypeCode.parse(0).token == "0"
        assert TypeCode.parse(9).token == "9"

    def test_parse_passthrough(self):
        original = TypeCode.float64()
        assert TypeCode.parse(original) is original

    def test_parse_bool_rejected(self):
        with pytest.raises(DespinaValidationError):
            TypeCode.parse(True)

    def test_parse_invalid_type_rejected(self):
        with pytest.raises(DespinaValidationError):
            TypeCode.parse(3.14)

    def test_fixed_out_of_range_rejected(self):
        with pytest.raises(DespinaValidationError):
            TypeCode.fixed(10)
        with pytest.raises(DespinaValidationError):
            TypeCode.fixed(-1)

    def test_fixed_bool_rejected(self):
        with pytest.raises(DespinaValidationError):
            TypeCode.fixed(True)

    def test_frozen(self):
        tc = TypeCode.float64()
        with pytest.raises(AttributeError):
            tc.token = "S"


class TestTableSpec:
    def test_from_pair(self):
        spec = TableSpec.from_pair("TRIPS", "D")
        assert spec.name == "TRIPS"
        assert spec.type_code.token == "D"

    def test_float32_convenience(self):
        spec = TableSpec.float32("TIME")
        assert spec.as_pair() == ("TIME", "S")

    def test_float64_convenience(self):
        spec = TableSpec.float64("DIST")
        assert spec.as_pair() == ("DIST", "D")

    def test_fixed_convenience(self):
        spec = TableSpec.fixed("COST", 2)
        assert spec.as_pair() == ("COST", "2")

    def test_empty_name_rejected(self):
        with pytest.raises(DespinaValidationError):
            TableSpec.from_pair("", "D")

    def test_whitespace_name_rejected(self):
        with pytest.raises(DespinaValidationError):
            TableSpec.from_pair("   ", "D")


class TestTableFunction:
    def test_equivalence_to_from_pair(self):
        assert (
            table("DIST", "D").as_pair() == TableSpec.from_pair("DIST", "D").as_pair()
        )

    def test_with_int_type_code(self):
        spec = table("COST", 2)
        assert spec.as_pair() == ("COST", "2")
