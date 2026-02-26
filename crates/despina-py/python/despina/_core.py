from __future__ import annotations

from typing import NoReturn

from ._despina import (
    DespinaError,
    DespinaIoError,
    DespinaParseError,
    DespinaValidationError,
    DespinaWriterError,
    TableDef,
    TableInfo,
    _MatrixCore,
)

__all__ = [
    "DespinaError",
    "DespinaIoError",
    "DespinaParseError",
    "DespinaValidationError",
    "DespinaWriterError",
    "TableDef",
    "TableInfo",
    "_MatrixCore",
    "_raise_validation",
]


def _raise_validation(message: str) -> NoReturn:
    raise DespinaValidationError(message)
