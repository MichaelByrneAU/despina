from __future__ import annotations

from os import PathLike
from typing import Iterable

import numpy as np
import numpy.typing as npt

class DespinaError(Exception):
    """Base exception for despina parse, validation, and write failures."""

    kind: str
    """Machine-readable error kind (e.g. ``"unexpected_eof"``)."""

    offset: int | None
    """Byte offset in the input where the error occurred, if applicable."""

class DespinaIoError(DespinaError): ...
class DespinaParseError(DespinaError): ...
class DespinaValidationError(DespinaError): ...
class DespinaWriterError(DespinaError): ...

class TableDef:
    """Validated table definition pairing a name with a type-code token."""

    def __init__(self, name: str, type_code: str | int, /) -> None: ...
    @property
    def name(self) -> str: ...
    @property
    def type_code(self) -> str: ...
    def __repr__(self) -> str: ...

class TableInfo:
    """Frozen table metadata returned by readers and writers."""

    index: int
    name: str
    type_code: str

    def __repr__(self) -> str: ...

class SchemaResult:
    """Validated schema metadata (no data allocation)."""

    zone_count: int
    banner: str
    run_id: str

    def tables(self) -> list[TableInfo]: ...
    def __repr__(self) -> str: ...

class _MatrixCore:
    @staticmethod
    def open(path: str | PathLike[str], /) -> _MatrixCore: ...
    @staticmethod
    def from_bytes(bytes: bytes | bytearray | memoryview, /) -> _MatrixCore: ...
    @staticmethod
    def open_tables(
        path: str | PathLike[str],
        table_names: list[str],
        /,
    ) -> _MatrixCore: ...
    @staticmethod
    def from_bytes_tables(
        bytes: bytes | bytearray | memoryview,
        table_names: list[str],
        /,
    ) -> _MatrixCore: ...
    @staticmethod
    def create(
        zone_count: int,
        tables: Iterable[TableDef | tuple[str, str | int]],
        banner: str | None = None,
        run_id: str | None = None,
    ) -> _MatrixCore: ...
    @staticmethod
    def validate_schema(
        zone_count: int,
        tables: Iterable[TableDef | tuple[str, str | int]],
        banner: str | None = None,
        run_id: str | None = None,
    ) -> SchemaResult: ...
    @property
    def zone_count(self) -> int: ...
    @property
    def table_count(self) -> int: ...
    @property
    def banner(self) -> str: ...
    @property
    def run_id(self) -> str: ...
    def tables(self) -> list[TableInfo]: ...
    def table_index_by_name(self, name: str, /) -> int | None: ...
    def checked_get(
        self, table: str | int, origin: int, destination: int, /
    ) -> float | None: ...
    def get(self, table: str | int, origin: int, destination: int, /) -> float: ...
    def set(
        self, table: str | int, origin: int, destination: int, value: float, /
    ) -> None: ...
    def table_total(self, table: str | int, /) -> float: ...
    def table_diagonal_total(self, table: str | int, /) -> float: ...
    def row(self, table: str | int, origin: int, /) -> list[float]: ...
    def table_array(self, table: str | int, /) -> npt.NDArray[np.float64]: ...
    def set_table_array(
        self, table: str | int, values: npt.NDArray[np.float64], /
    ) -> None: ...
    def stack_array(self) -> npt.NDArray[np.float64]: ...
    def set_stack_array(self, values: npt.NDArray[np.float64], /) -> None: ...
    def write(self, path: str | PathLike[str], /) -> None: ...
    def to_bytes(self) -> bytes: ...
    def __bytes__(self) -> bytes: ...
    @staticmethod
    def write_from_stack(
        path: str | PathLike[str],
        zone_count: int,
        tables: Iterable[TableDef | tuple[str, str | int]],
        stack: npt.NDArray[np.float64],
        banner: str | None = None,
        run_id: str | None = None,
    ) -> None: ...
    @staticmethod
    def to_bytes_from_stack(
        zone_count: int,
        tables: Iterable[TableDef | tuple[str, str | int]],
        stack: npt.NDArray[np.float64],
        banner: str | None = None,
        run_id: str | None = None,
    ) -> bytes: ...
    def __repr__(self) -> str: ...

