from __future__ import annotations

import numbers
from dataclasses import dataclass
from typing import Iterable

from ._core import DespinaValidationError, TableDef


@dataclass(frozen=True)
class TypeCode:
    """Validated table type-code token.

    Supported tokens are fixed-decimal precisions ``"0"`` .. ``"9"``,
    ``"S"`` for float32 storage, and ``"D"`` for float64 storage.

    Instances are immutable and validated on construction.

    Examples
    --------
    >>> TypeCode.float64().token
    'D'
    >>> TypeCode.fixed(2).token
    '2'
    """

    token: str

    def __post_init__(self) -> None:
        token = self.token
        if not isinstance(token, str):
            raise DespinaValidationError("type code token must be a string")
        if token not in {"S", "D", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}:
            raise DespinaValidationError(
                f"invalid type code {token!r}; expected '0'..'9', 'S', or 'D'"
            )

    @classmethod
    def fixed(cls, decimal_places: int) -> TypeCode:
        """Construct a fixed-decimal type code.

        Parameters
        ----------
        decimal_places:
            Number of decimal places in ``0..9``.
        """

        if isinstance(decimal_places, bool) or not isinstance(
            decimal_places, numbers.Integral
        ):
            raise DespinaValidationError(
                "fixed decimal places must be an integer within 0..9"
            )
        decimal_places_value = int(decimal_places)
        if decimal_places_value < 0 or decimal_places_value > 9:
            raise DespinaValidationError(
                f"invalid fixed-point precision {decimal_places_value}; expected 0..9"
            )
        return cls(str(decimal_places_value))

    @classmethod
    def float32(cls) -> TypeCode:
        """Construct float32 type code ``'S'``."""

        return cls("S")

    @classmethod
    def float64(cls) -> TypeCode:
        """Construct float64 type code ``'D'``."""

        return cls("D")

    @classmethod
    def parse(cls, value: TypeCode | str | int) -> TypeCode:
        """Parse user input into a validated :class:`TypeCode`.

        This accepts an existing :class:`TypeCode`, a string token, or an
        integer fixed-decimal precision in ``0..9``.

        Parameters
        ----------
        value:
            ``TypeCode`` instance, string token, or integer precision.

        Returns
        -------
        TypeCode
            Validated type-code object.
        """

        if isinstance(value, TypeCode):
            return value

        if isinstance(value, bool):
            raise DespinaValidationError(
                "type code must be int 0..9 or string '0'..'9', 'S', or 'D'"
            )

        if isinstance(value, numbers.Integral):
            return cls.fixed(value)

        if isinstance(value, str):
            return cls(value)

        raise DespinaValidationError(
            "type code must be int 0..9 or string '0'..'9', 'S', or 'D'"
        )


@dataclass(frozen=True)
class TableSpec:
    """Validated table schema item.

    ``TableSpec`` combines a non-empty table name with a validated
    :class:`TypeCode`. Preferred over loose tuples when constructing schema
    programmatically.

    Examples
    --------
    >>> spec = TableSpec.float32("TIME_AM")
    >>> spec.as_pair()
    ('TIME_AM', 'S')
    """

    name: str
    type_code: TypeCode

    def __post_init__(self) -> None:
        if not isinstance(self.name, str):
            raise DespinaValidationError("table name must be a string")
        if not self.name.strip():
            raise DespinaValidationError("table name must not be empty")

    @classmethod
    def from_pair(cls, name: str, type_code: TypeCode | str | int) -> TableSpec:
        """Create :class:`TableSpec` from loose ``(name, type_code)`` input."""

        return cls(name=name, type_code=TypeCode.parse(type_code))

    @classmethod
    def fixed(cls, name: str, decimal_places: int) -> TableSpec:
        """Create fixed-decimal table spec."""

        return cls(name=name, type_code=TypeCode.fixed(decimal_places))

    @classmethod
    def float32(cls, name: str) -> TableSpec:
        """Create float32 table spec."""

        return cls(name=name, type_code=TypeCode.float32())

    @classmethod
    def float64(cls, name: str) -> TableSpec:
        """Create float64 table spec."""

        return cls(name=name, type_code=TypeCode.float64())

    def as_pair(self) -> tuple[str, str]:
        """Return validated ``(name, type_code_token)`` pair."""

        return (self.name, self.type_code.token)


def table(name: str, type_code: TypeCode | str | int) -> TableSpec:
    """Convenience constructor for :class:`TableSpec`.

    Convenience constructor equivalent to :meth:`TableSpec.from_pair`, for
    concise schema literals.

    Examples
    --------
    >>> table("DIST_AM", "D").as_pair()
    ('DIST_AM', 'D')
    """

    return TableSpec.from_pair(name, type_code)


def normalise_table_defs(
    tables: Iterable[TableDef | TableSpec | tuple[str, TypeCode | str | int]],
) -> list[tuple[str, str]]:
    """Normalise table definitions into validated ``(name, token)`` pairs.

    Used by matrix-creation entrypoints for consistent validation behaviour.

    Parameters
    ----------
    tables:
        Iterable of ``TableDef``, ``TableSpec``, or ``(name, type_code)`` pairs.

    Returns
    -------
    list[tuple[str, str]]
        Normalised table definitions in input order.
    """

    normalised: list[tuple[str, str]] = []
    seen_names: set[str] = set()

    for offset, item in enumerate(tables):
        if isinstance(item, TableSpec):
            spec = item
        elif isinstance(item, TableDef):
            spec = TableSpec.from_pair(item.name, item.type_code)
        else:
            try:
                name, type_code = item
            except Exception as error:  # pragma: no cover - defensive
                raise DespinaValidationError(
                    f"tables[{offset}] must be TableSpec, TableDef, or (name, type_code) tuple"
                ) from error
            spec = TableSpec.from_pair(name, type_code)

        if spec.name in seen_names:
            raise DespinaValidationError(f"duplicate table name {spec.name!r}")
        seen_names.add(spec.name)
        normalised.append(spec.as_pair())

    if not normalised:
        raise DespinaValidationError("at least one table definition is required")

    return normalised
