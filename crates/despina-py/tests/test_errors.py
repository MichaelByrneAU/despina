from __future__ import annotations

import despina
import numpy as np
import pytest
from despina import (
    DespinaError,
    DespinaIoError,
    DespinaParseError,
    DespinaValidationError,
    DespinaWriterError,
)

class TestExceptionHierarchy:
    def test_io_error_is_despina_error(self):
        assert issubclass(DespinaIoError, DespinaError)

    def test_parse_error_is_despina_error(self):
        assert issubclass(DespinaParseError, DespinaError)

    def test_validation_error_is_despina_error(self):
        assert issubclass(DespinaValidationError, DespinaError)

    def test_writer_error_is_despina_error(self):
        assert issubclass(DespinaWriterError, DespinaError)

    def test_all_catchable_via_base(self):
        with pytest.raises(DespinaError):
            despina.create(0, [("T", "D")])


class TestMatrixCreationValidation:
    def test_zero_zone_count(self):
        with pytest.raises(DespinaValidationError):
            despina.create(0, [("T", "D")])

    def test_empty_tables(self):
        with pytest.raises(DespinaValidationError):
            despina.create(2, [])

    def test_duplicate_table_names(self):
        with pytest.raises(DespinaValidationError):
            despina.create(2, [("T", "D"), ("T", "D")])


class TestTableAccessValidation:
    def test_getitem_missing_table_name(self):
        matrix = despina.create(2, [("T", "D")])
        with pytest.raises(DespinaValidationError):
            matrix["MISSING"]

    def test_getitem_out_of_range_index(self):
        matrix = despina.create(2, [("T", "D")])
        with pytest.raises(DespinaValidationError):
            matrix[5]

    def test_getitem_bool_table_key_rejected(self):
        matrix = despina.create(2, [("T", "D")])
        with pytest.raises(DespinaValidationError):
            matrix[True]

    def test_getitem_float_table_key_rejected(self):
        matrix = despina.create(2, [("T", "D")])
        with pytest.raises(DespinaValidationError):
            matrix[1.5]


class TestArrayDimensionValidation:
    def test_setitem_wrong_shape(self):
        matrix = despina.create(2, [("T", "D")])
        with pytest.raises(DespinaValidationError):
            matrix["T"] = np.ones((3, 3))


class TestParseErrors:
    def test_empty_bytes(self):
        with pytest.raises(DespinaParseError):
            despina.from_bytes(b"")

    def test_truncated_bytes(self):
        with pytest.raises(DespinaParseError):
            despina.from_bytes(b"\x00\x01\x02\x03")


class TestTableLookupErrors:
    def test_missing_table_name(self):
        matrix = despina.create(2, [("T", "D")])
        with pytest.raises(DespinaValidationError):
            matrix["MISSING"]

    def test_out_of_range_table_index(self):
        matrix = despina.create(2, [("T", "D")])
        with pytest.raises(DespinaValidationError):
            matrix[5]
