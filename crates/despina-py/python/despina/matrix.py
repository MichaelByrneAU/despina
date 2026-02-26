from __future__ import annotations

import csv
import difflib
import numbers
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Literal, Mapping, Sequence

import numpy as np

from ._core import DespinaValidationError, TableDef, _MatrixCore, _raise_validation
from .capabilities import (
    has_dataframe_support,
    has_pandas_support,
    has_parquet_support,
    has_polars_support,
)
from .schema import TableSpec, TypeCode, normalise_table_defs

TableKey = str | int
TableLike = TableDef | TableSpec | tuple[str, TypeCode | str | int]
TableArrayInput = np.ndarray | Iterable[Iterable[float]]
LongZeroPolicy = Literal["auto", "include", "exclude"]

_LONG_EXPORT_AUTO_INCLUDE_CELL_LIMIT = 200_000


def _missing_table_message(name: str, table_names: Sequence[str]) -> str:
    message = f"table {name!r} not found"
    if not table_names:
        return message

    suggestion = difflib.get_close_matches(name, table_names, n=1, cutoff=0.6)
    if suggestion:
        message += f"; did you mean {suggestion[0]!r}?"

    preview = ", ".join(repr(item) for item in table_names[:8])
    if len(table_names) > 8:
        preview += ", ..."
    message += f"; available tables: {preview}"
    return message


def _is_integral(value: object) -> bool:
    return isinstance(value, numbers.Integral) and not isinstance(value, bool)


def _resolve_parquet_engine(requested: str) -> str:
    """Resolve the effective parquet engine name.

    Returns ``'pyarrow'``, ``'fastparquet'``, or ``'polars'``.
    Raises ``ImportError`` when no suitable backend is available.
    """
    if requested == "polars":
        if not has_polars_support():
            raise ImportError(
                "polars is required for parquet_engine='polars'; "
                "install with `uv add polars`"
            )
        return "polars"

    if requested in {"pyarrow", "fastparquet"}:
        if not has_parquet_support(engine=requested):
            raise ImportError(
                "parquet export requires pandas and engine "
                f"{requested!r}; install with `uv add pandas {requested}`"
            )
        return requested

    # auto
    if has_parquet_support(engine="pyarrow"):
        return "pyarrow"
    if has_parquet_support(engine="fastparquet"):
        return "fastparquet"
    if has_polars_support():
        return "polars"
    raise ImportError(
        "parquet export requires pandas+pyarrow/fastparquet or polars; "
        "install with `uv add pandas pyarrow` or `uv add polars`"
    )


def _resolve_include_zeros(
    *,
    include_zeros: bool | None,
    zero_policy: LongZeroPolicy,
    total_cells: int,
) -> bool:
    if include_zeros is not None:
        return bool(include_zeros)

    if zero_policy == "include":
        return True
    if zero_policy == "exclude":
        return False
    if zero_policy == "auto":
        return total_cells <= _LONG_EXPORT_AUTO_INCLUDE_CELL_LIMIT
    _raise_validation("zero_policy must be 'auto', 'include', or 'exclude'")


@dataclass(frozen=True)
class TableMeta:
    """Lightweight table metadata.

    ``index`` is the 0-based table position and ``type_code`` is the token
    controlling on-disk representation during writes.
    """

    index: int
    name: str
    type_code: str


@dataclass(frozen=True)
class _CoreSnapshot:
    """All data extracted from a transient ``_MatrixCore``."""

    zone_count: int
    table_count: int
    banner: str
    run_id: str
    table_metas: tuple[TableMeta, ...]
    table_names: tuple[str, ...]
    table_index_by_name: dict[str, int]
    tables: dict[str, np.ndarray]


def _extract_from_core(core: _MatrixCore) -> _CoreSnapshot:
    """Extract all data from a transient ``_MatrixCore`` into Python-owned state."""
    metas = tuple(
        TableMeta(index=item.index - 1, name=item.name, type_code=item.type_code)
        for item in core.tables()
    )
    table_names = tuple(meta.name for meta in metas)
    table_index_by_name = {meta.name: meta.index for meta in metas}

    tables: dict[str, np.ndarray] = {}
    for meta in metas:
        tables[meta.name] = core.table_array(meta.index + 1)

    return _CoreSnapshot(
        zone_count=core.zone_count,
        table_count=core.table_count,
        banner=core.banner,
        run_id=core.run_id,
        table_metas=metas,
        table_names=table_names,
        table_index_by_name=table_index_by_name,
        tables=tables,
    )


class Matrix:
    """Loaded ``.mat`` matrix with random table and cell access.

    Each table is stored as a ``(zone_count, zone_count)`` ``float64``
    NumPy array. Indexing (``matrix["table_name"]``) returns the stored
    array. In-place edits apply to the matrix immediately::

        time = matrix["HwySkim_Time"]
        time[0, 0] = 5.0          # modifies matrix directly
        time *= 1.1               # in-place scale

    Assignment (``matrix["table_name"] = array``) replaces the stored
    array after shape validation. Use assignment when creating a new
    array (e.g. ``matrix["T"] = matrix["T"] * 1.02``).

    Examples
    --------
    >>> import despina
    >>> matrix = despina.create(2, [("DIST_AM", "D")])
    >>> matrix["DIST_AM"][0, 1] = 7.5
    >>> matrix["DIST_AM"][0, 1]
    7.5
    """

    def __init__(
        self,
        *,
        zone_count: int,
        table_count: int,
        banner: str,
        run_id: str,
        table_metas: tuple[TableMeta, ...],
        table_names: tuple[str, ...],
        table_index_by_name: dict[str, int],
        tables: dict[str, np.ndarray],
    ) -> None:
        self._zone_count = zone_count
        self._table_count = table_count
        self._banner = banner
        self._run_id = run_id
        self._table_metas = table_metas
        self._table_names = table_names
        self._table_index_by_name = table_index_by_name
        self._tables = tables

    @classmethod
    def _from_core(cls, core: _MatrixCore) -> Matrix:
        """Construct a Matrix by extracting all data from a transient core."""
        snapshot = _extract_from_core(core)
        return cls(
            zone_count=snapshot.zone_count,
            table_count=snapshot.table_count,
            banner=snapshot.banner,
            run_id=snapshot.run_id,
            table_metas=snapshot.table_metas,
            table_names=snapshot.table_names,
            table_index_by_name=snapshot.table_index_by_name,
            tables=snapshot.tables,
        )

    @classmethod
    def read(
        cls,
        path: str | Path,
        *,
        tables: Sequence[str] | None = None,
    ) -> Matrix:
        """Open a matrix file from disk.

        Parameters
        ----------
        path:
            Path to a ``.mat`` file.
        tables:
            Optional subset of table names to load. When provided, only the
            named tables are decoded and allocated, while other tables are
            skipped. Table names must match the header exactly. Pass ``None``
            (the default) to load all tables.

        Returns
        -------
        Matrix
            Parsed matrix object.

        Examples
        --------
        >>> matrix = Matrix.read("skims.mat")
        >>> matrix.zone_count >= 1
        True
        """

        resolved = Path(path)
        if tables is not None:
            core = _MatrixCore.open_tables(resolved, list(tables))
        else:
            core = _MatrixCore.open(resolved)
        return cls._from_core(core)

    @classmethod
    def from_bytes(
        cls,
        data: bytes | bytearray | memoryview,
        *,
        tables: Sequence[str] | None = None,
    ) -> Matrix:
        """Parse a matrix from in-memory bytes.

        Accepts cloud, database, or API payloads where matrix content is not
        stored on local disk.

        Parameters
        ----------
        data:
            Byte payload containing ``.mat`` content.
        tables:
            Optional subset of table names to load. When provided, only the
            named tables are decoded and allocated, while other tables are
            skipped. Pass ``None`` (the default) to load all tables.
        """

        if tables is not None:
            core = _MatrixCore.from_bytes_tables(data, list(tables))
        else:
            core = _MatrixCore.from_bytes(data)
        return cls._from_core(core)

    @classmethod
    def create(
        cls,
        zone_count: int,
        tables: Iterable[TableLike],
        *,
        banner: str | None = None,
        run_id: str | None = None,
    ) -> Matrix:
        """Create a new empty matrix with the provided schema.

        ``tables`` accepts ``TableSpec`` values, ``TableDef`` values, or
        ``(name, type_code)`` pairs.

        Returns
        -------
        Matrix
            Matrix with zero-initialised values and validated table schema.
        """

        table_defs = normalise_table_defs(tables)

        # Validate schema only (no data allocation).
        result = _MatrixCore.validate_schema(
            zone_count, table_defs, banner=banner, run_id=run_id,
        )
        validated_zone_count = result.zone_count
        validated_banner = result.banner
        validated_run_id = result.run_id

        metas = tuple(
            TableMeta(index=item.index - 1, name=item.name, type_code=item.type_code)
            for item in result.tables()
        )
        table_names = tuple(meta.name for meta in metas)
        table_index_by_name = {meta.name: meta.index for meta in metas}
        numpy_tables = {
            meta.name: np.zeros(
                (validated_zone_count, validated_zone_count), dtype=np.float64,
            )
            for meta in metas
        }

        return cls(
            zone_count=validated_zone_count,
            table_count=len(metas),
            banner=validated_banner,
            run_id=validated_run_id,
            table_metas=metas,
            table_names=table_names,
            table_index_by_name=table_index_by_name,
            tables=numpy_tables,
        )

    @classmethod
    def like(
        cls,
        other: Matrix,
        *,
        banner: str | None = None,
        run_id: str | None = None,
    ) -> Matrix:
        """Create a new zero-initialised matrix with the same schema as *other*.

        Table definitions, zone count, banner, and run id are copied from
        *other* unless explicitly overridden.

        Parameters
        ----------
        other:
            Source matrix whose schema is copied.
        banner:
            Override banner text.  Defaults to ``other.banner``.
        run_id:
            Override run identifier.  Defaults to ``other.run_id``.

        Returns
        -------
        Matrix
            New zero-initialised matrix with matching schema.
        """

        return cls.create(
            zone_count=other.zone_count,
            tables=other.table_defs,
            banner=banner if banner is not None else other.banner,
            run_id=run_id if run_id is not None else other.run_id,
        )

    @property
    def zone_count(self) -> int:
        """Return number of zones."""

        return self._zone_count

    @property
    def table_count(self) -> int:
        """Return number of tables."""

        return self._table_count

    @property
    def banner(self) -> str:
        """Return header banner text."""

        return self._banner

    @property
    def run_id(self) -> str:
        """Return header run identifier."""

        return self._run_id

    @property
    def tables(self) -> tuple[TableMeta, ...]:
        """Return table metadata in header order."""

        return self._table_metas

    @property
    def table_defs(self) -> tuple[tuple[str, str], ...]:
        """Return table definitions as ``(name, type_code)`` tuples."""

        return tuple((meta.name, meta.type_code) for meta in self._table_metas)

    @property
    def table_names(self) -> tuple[str, ...]:
        """Return table names in header order."""

        return self._table_names

    def table_index_by_name(self, name: str) -> int | None:
        """Return 0-based table index for ``name`` or ``None`` if missing."""

        return self._table_index_by_name.get(name)

    def _resolve_table_index(self, table: TableKey) -> int:
        if isinstance(table, str):
            table_index = self.table_index_by_name(table)
            if table_index is None:
                _raise_validation(_missing_table_message(table, self.table_names))
            return table_index

        if not _is_integral(table):
            _raise_validation("table must be a table name (str) or 0-based index (int)")
        table_index = int(table)
        if table_index < 0 or table_index >= self.table_count:
            _raise_validation(
                f"table index must be within 0..{self.table_count - 1} (got {table_index})"
            )
        return table_index

    def _resolve_table_name(self, table: TableKey) -> str:
        """Resolve a table key (name or 0-based index) to a table name."""
        return self._table_names[self._resolve_table_index(table)]

    def reindex_zones(
        self,
        keep: Sequence[int] | np.ndarray,
        *,
        basis: Literal["zone", "index"] = "zone",
    ) -> Matrix:
        """Return a new matrix keeping selected zones.

        Parameters
        ----------
        keep:
            1D sequence of zone ids or positions.
        basis:
            ``"zone"`` for 1-based zone ids (default),
            ``"index"`` for 0-based positions.

        Returns
        -------
        Matrix
            New matrix containing only selected zones, preserving table order
            and header metadata.

        Examples
        --------
        >>> reduced = matrix.reindex_zones([1, 3, 5], basis="zone")
        >>> reduced.zone_count
        3
        """

        keep_values = np.asarray(keep)
        if keep_values.ndim != 1:
            _raise_validation("keep must be a 1D sequence of zone references")
        if keep_values.size == 0:
            _raise_validation("keep must contain at least one zone reference")
        if np.issubdtype(keep_values.dtype, np.bool_):
            _raise_validation("keep must contain integer zone references, not booleans")

        if np.issubdtype(keep_values.dtype, np.integer):
            keep_array = keep_values.astype(np.int64, copy=False)
        elif np.issubdtype(keep_values.dtype, np.floating):
            if not np.all(np.isfinite(keep_values)):
                _raise_validation("keep must contain finite integer zone references")
            if not np.all(np.equal(keep_values, np.floor(keep_values))):
                _raise_validation("keep must contain integer zone references")
            keep_array = keep_values.astype(np.int64)
        else:
            _raise_validation("keep must contain integer zone references")

        if basis == "zone":
            keep_positions = keep_array - 1
        elif basis == "index":
            keep_positions = keep_array
        else:
            _raise_validation("basis must be 'zone' or 'index'")

        if np.any(keep_positions < 0) or np.any(keep_positions >= self.zone_count):
            _raise_validation(
                f"keep references must resolve within 0..{self.zone_count - 1} "
                f"(got {keep_array.tolist()} with basis={basis!r})"
            )
        if np.unique(keep_positions).size != keep_positions.size:
            _raise_validation("keep references must be unique")

        reduced = Matrix.create(
            int(keep_positions.size),
            self.table_defs,
            banner=self.banner,
            run_id=self.run_id,
        )
        for meta in self._table_metas:
            values = self._tables[meta.name]
            reduced_values = values[np.ix_(keep_positions, keep_positions)]
            reduced[meta.index] = reduced_values
        return reduced

    def _select_tables(
        self,
        tables: TableKey | Sequence[TableKey] | None,
    ) -> tuple[TableMeta, ...]:
        if tables is None:
            return self._table_metas

        if isinstance(tables, bool):
            _raise_validation(
                "tables must be a table selector (name/index) or a sequence of selectors"
            )

        if isinstance(tables, str) or _is_integral(tables):
            table_keys: Sequence[TableKey] = (tables,)
        else:
            try:
                table_keys = tuple(tables)
            except TypeError as error:
                raise DespinaValidationError(
                    "tables must be a table selector (name/index) or a sequence of selectors"
                ) from error

        selected: list[TableMeta] = []
        seen_indices: set[int] = set()
        for item in table_keys:
            meta = self._table_metas[self._resolve_table_index(item)]
            if meta.index in seen_indices:
                _raise_validation(
                    f"duplicate table selection {meta.name!r}; each table must be selected once"
                )
            selected.append(meta)
            seen_indices.add(meta.index)
        return tuple(selected)

    def _long_columns(
        self,
        *,
        tables: TableKey | Sequence[TableKey] | None = None,
        include_zeros: bool | None = None,
        zero_policy: LongZeroPolicy = "auto",
        zone_base: int = 1,
    ) -> dict[str, np.ndarray]:
        if zone_base not in {0, 1}:
            _raise_validation("zone_base must be 0 or 1")

        selected = self._select_tables(tables)
        empty_int = np.array([], dtype=np.int32)
        empty_float = np.array([], dtype=np.float64)
        empty_table = np.array([], dtype=object)
        if not selected:
            return {
                "table_index": empty_int,
                "table": empty_table,
                "origin": empty_int,
                "destination": empty_int,
                "value": empty_float,
            }

        zone_count = self.zone_count
        include_zeros_resolved = _resolve_include_zeros(
            include_zeros=include_zeros,
            zero_policy=zero_policy,
            total_cells=len(selected) * zone_count * zone_count,
        )

        if include_zeros_resolved:
            table_count = len(selected)
            cells_per_table = zone_count * zone_count
            total_cells = table_count * cells_per_table

            base_origins = np.repeat(np.arange(zone_count, dtype=np.int32), zone_count)
            base_destinations = np.tile(
                np.arange(zone_count, dtype=np.int32), zone_count
            )
            origins = np.tile(base_origins, table_count)
            destinations = np.tile(base_destinations, table_count)
            if zone_base == 1:
                origins += 1
                destinations += 1

            table_indices = np.empty(total_cells, dtype=np.int32)
            table_names = np.empty(total_cells, dtype=object)
            values = np.empty(total_cells, dtype=np.float64)

            selected_indices = tuple(meta.index for meta in selected)
            natural_order = tuple(range(self.table_count))
            if selected_indices == natural_order:
                stacked = np.stack(
                    [self._tables[name] for name in self._table_names]
                ).reshape(-1)
                values[:] = stacked
                for table_offset, meta in enumerate(selected):
                    start = table_offset * cells_per_table
                    end = start + cells_per_table
                    table_indices[start:end] = meta.index
                    table_names[start:end] = meta.name
            else:
                for table_offset, meta in enumerate(selected):
                    start = table_offset * cells_per_table
                    end = start + cells_per_table
                    table_indices[start:end] = meta.index
                    table_names[start:end] = meta.name
                    values[start:end] = self._tables[meta.name].reshape(-1)

            return {
                "table_index": table_indices,
                "table": table_names,
                "origin": origins,
                "destination": destinations,
                "value": values,
            }

        chunks: list[tuple[TableMeta, np.ndarray, np.ndarray, np.ndarray]] = []
        total_nonzero = 0
        for meta in selected:
            values_2d = self._tables[meta.name]
            origins, destinations = np.nonzero(values_2d)
            values = values_2d[origins, destinations]
            total_nonzero += int(values.size)
            chunks.append((meta, origins, destinations, values))

        table_indices = np.empty(total_nonzero, dtype=np.int32)
        table_names = np.empty(total_nonzero, dtype=object)
        origins = np.empty(total_nonzero, dtype=np.int32)
        destinations = np.empty(total_nonzero, dtype=np.int32)
        values = np.empty(total_nonzero, dtype=np.float64)

        cursor = 0
        for meta, origin_idx, destination_idx, row_values in chunks:
            count = int(row_values.size)
            if count == 0:
                continue
            next_cursor = cursor + count
            table_indices[cursor:next_cursor] = meta.index
            table_names[cursor:next_cursor] = meta.name
            origins[cursor:next_cursor] = origin_idx + zone_base
            destinations[cursor:next_cursor] = destination_idx + zone_base
            values[cursor:next_cursor] = row_values
            cursor = next_cursor

        return {
            "table_index": table_indices,
            "table": table_names,
            "origin": origins,
            "destination": destinations,
            "value": values,
        }

    def _wide_columns(
        self,
        *,
        tables: TableKey | Sequence[TableKey] | None = None,
        origin_col: str = "Origin",
        destination_col: str = "Destination",
        rename_columns: Mapping[str, str] | None = None,
        zone_base: int = 1,
        include_zero_rows: bool = False,
        sort_od: bool = True,
    ) -> dict[str, np.ndarray]:
        if zone_base not in {0, 1}:
            _raise_validation("zone_base must be 0 or 1")
        if not isinstance(origin_col, str) or not origin_col:
            _raise_validation("origin_col must be a non-empty string")
        if not isinstance(destination_col, str) or not destination_col:
            _raise_validation("destination_col must be a non-empty string")
        if origin_col == destination_col:
            _raise_validation("origin_col and destination_col must differ")

        selected = self._select_tables(tables)
        zone_count = self.zone_count

        if selected:
            stack = np.stack(
                [self._tables[meta.name] for meta in selected], axis=0
            )
            if include_zero_rows:
                mask = np.ones((zone_count, zone_count), dtype=bool)
            else:
                mask = np.any(stack != 0.0, axis=0)
        else:
            stack = np.zeros((0, zone_count, zone_count), dtype=np.float64)
            if include_zero_rows:
                mask = np.ones((zone_count, zone_count), dtype=bool)
            else:
                mask = np.zeros((zone_count, zone_count), dtype=bool)

        origin_idx, destination_idx = np.nonzero(mask)
        if sort_od and origin_idx.size > 0:
            order = np.lexsort((destination_idx, origin_idx))
            origin_idx = origin_idx[order]
            destination_idx = destination_idx[order]

        columns: dict[str, np.ndarray] = {
            origin_col: (origin_idx + zone_base).astype(np.int64, copy=False),
            destination_col: (destination_idx + zone_base).astype(np.int64, copy=False),
        }

        rename = dict(rename_columns or {})
        selected_names = {meta.name for meta in selected}
        unknown_renames = [name for name in rename.keys() if name not in selected_names]
        if unknown_renames:
            preview = ", ".join(repr(item) for item in unknown_renames[:8])
            if len(unknown_renames) > 8:
                preview += ", ..."
            _raise_validation(f"rename_columns contains unknown table names: {preview}")

        seen_names = {origin_col, destination_col}
        for table_offset, meta in enumerate(selected):
            output_name = rename.get(meta.name, meta.name)
            if not isinstance(output_name, str) or not output_name.strip():
                _raise_validation(
                    f"output column name for {meta.name!r} must be a non-empty string"
                )
            if output_name in seen_names:
                _raise_validation(
                    f"duplicate output column name {output_name!r}; "
                    "origin/destination and table columns must be unique"
                )
            seen_names.add(output_name)
            columns[output_name] = stack[table_offset, origin_idx, destination_idx]

        return columns

    def to_pandas(
        self,
        *,
        origin_col: str = "Origin",
        destination_col: str = "Destination",
        tables: TableKey | Sequence[TableKey] | None = None,
        rename_columns: Mapping[str, str] | None = None,
        zone_base: int = 1,
        include_zero_rows: bool = False,
        sort_od: bool = True,
    ):
        """Return canonical wide OD data as a pandas DataFrame.

        The resulting columns are ``Origin``, ``Destination``, and one column
        per selected table unless renamed.

        Rows are filtered according to ``include_zero_rows`` and may be sorted
        by ``(origin, destination)`` when ``sort_od=True``.

        Examples
        --------
        >>> frame = matrix.to_pandas(tables=["DIST_AM", "TIME_AM"])
        >>> list(frame.columns[:2])
        ['Origin', 'Destination']
        """

        if not has_pandas_support():
            raise ImportError(
                "pandas is required for to_pandas(); install with `uv add pandas` "
                "or `uv add despina[dataframe]`"
            )

        import pandas as pd

        columns = self._wide_columns(
            tables=tables,
            origin_col=origin_col,
            destination_col=destination_col,
            rename_columns=rename_columns,
            zone_base=zone_base,
            include_zero_rows=include_zero_rows,
            sort_od=sort_od,
        )
        return pd.DataFrame(columns)

    def to_polars(
        self,
        *,
        origin_col: str = "Origin",
        destination_col: str = "Destination",
        tables: TableKey | Sequence[TableKey] | None = None,
        rename_columns: Mapping[str, str] | None = None,
        zone_base: int = 1,
        include_zero_rows: bool = False,
        sort_od: bool = True,
    ):
        """Return canonical wide OD data as a polars DataFrame.

        Behaviour matches :meth:`to_pandas` for column naming, row filtering,
        and sorting options.

        Examples
        --------
        >>> frame = matrix.to_polars(tables="DIST_AM")
        >>> frame.columns[0]
        'Origin'
        """

        if not has_polars_support():
            raise ImportError(
                "polars is required for to_polars(); install with `uv add polars`"
            )

        import polars as pl

        columns = self._wide_columns(
            tables=tables,
            origin_col=origin_col,
            destination_col=destination_col,
            rename_columns=rename_columns,
            zone_base=zone_base,
            include_zero_rows=include_zero_rows,
            sort_od=sort_od,
        )
        return pl.DataFrame(columns)

    def to_csv(
        self,
        path: str | Path,
        *,
        origin_col: str = "Origin",
        destination_col: str = "Destination",
        tables: TableKey | Sequence[TableKey] | None = None,
        rename_columns: Mapping[str, str] | None = None,
        zone_base: int = 1,
        include_zero_rows: bool = False,
        sort_od: bool = True,
        encoding: str = "utf-8",
        sep: str = ",",
        quotechar: str = '"',
        csv_kwargs: Mapping[str, object] | None = None,
    ) -> Path:
        """Write canonical wide OD rows to CSV and return output path.

        By default, rows where all selected tables are zero are excluded.

        Parameters controlling delimiter and quoting are passed through to the
        standard ``csv`` writer after removing conflicting keys.

        Examples
        --------
        >>> out = matrix.to_csv("skims_wide.csv")
        >>> out.name
        'skims_wide.csv'
        """

        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        columns = self._wide_columns(
            tables=tables,
            origin_col=origin_col,
            destination_col=destination_col,
            rename_columns=rename_columns,
            zone_base=zone_base,
            include_zero_rows=include_zero_rows,
            sort_od=sort_od,
        )

        options = dict(csv_kwargs or {})
        options.pop("delimiter", None)
        options.pop("quotechar", None)
        headers = list(columns.keys())
        with out_path.open("w", newline="", encoding=encoding) as handle:
            writer = csv.writer(handle, delimiter=sep, quotechar=quotechar, **options)
            writer.writerow(headers)
            if headers:
                writer.writerows(zip(*(columns[name] for name in headers)))
        return out_path

    def to_parquet(
        self,
        path: str | Path,
        *,
        origin_col: str = "Origin",
        destination_col: str = "Destination",
        tables: TableKey | Sequence[TableKey] | None = None,
        rename_columns: Mapping[str, str] | None = None,
        zone_base: int = 1,
        include_zero_rows: bool = False,
        sort_od: bool = True,
        parquet_engine: str = "auto",
        parquet_kwargs: Mapping[str, object] | None = None,
    ) -> Path:
        """Write canonical wide OD rows to Parquet and return output path.

        ``parquet_engine='auto'`` selects an available backend in this order:
        pandas+pyarrow, pandas+fastparquet, then polars.

        This method raises ``ImportError`` when no suitable backend is
        available for the requested engine.

        Examples
        --------
        >>> out = matrix.to_parquet("skims_wide.parquet", parquet_engine="auto")
        >>> out.suffix
        '.parquet'
        """

        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        options = dict(parquet_kwargs or {})

        if parquet_engine not in {"auto", "pyarrow", "fastparquet", "polars"}:
            _raise_validation(
                "parquet_engine must be 'auto', 'pyarrow', 'fastparquet', or 'polars'"
            )

        od_kwargs = dict(
            origin_col=origin_col,
            destination_col=destination_col,
            tables=tables,
            rename_columns=rename_columns,
            zone_base=zone_base,
            include_zero_rows=include_zero_rows,
            sort_od=sort_od,
        )

        engine = _resolve_parquet_engine(parquet_engine)
        if engine == "polars":
            self.to_polars(**od_kwargs).write_parquet(out_path, **options)
        else:
            self.to_pandas(**od_kwargs).to_parquet(
                out_path, engine=engine, index=False, **options
            )
        return out_path

    def to_csv_long(
        self,
        path: str | Path,
        *,
        tables: TableKey | Sequence[TableKey] | None = None,
        include_zeros: bool | None = None,
        zero_policy: LongZeroPolicy = "auto",
        zone_base: int = 1,
    ) -> Path:
        """Write long-form records as CSV and return output path.

        Uses a pandas fast path when available. Otherwise writes records with
        the built-in ``csv`` module.

        Examples
        --------
        >>> matrix.to_csv_long("cells.csv", tables="DIST_AM", zero_policy="exclude")
        """

        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if has_dataframe_support():
            frame = self.to_long_dataframe(
                tables=tables,
                include_zeros=include_zeros,
                zero_policy=zero_policy,
                zone_base=zone_base,
            )
            frame.to_csv(out_path, index=False)
            return out_path

        with out_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["table_index", "table", "origin", "destination", "value"])
            columns = self._long_columns(
                tables=tables,
                include_zeros=include_zeros,
                zero_policy=zero_policy,
                zone_base=zone_base,
            )
            writer.writerows(
                zip(
                    columns["table_index"],
                    columns["table"],
                    columns["origin"],
                    columns["destination"],
                    columns["value"],
                )
            )

        return out_path

    def to_long_dataframe(
        self,
        *,
        tables: TableKey | Sequence[TableKey] | None = None,
        include_zeros: bool | None = None,
        zero_policy: LongZeroPolicy = "auto",
        zone_base: int = 1,
    ):
        """Return long-form data as a pandas DataFrame.

        Returned columns are always:
        ``table_index``, ``table``, ``origin``, ``destination``, ``value``.

        Examples
        --------
        >>> frame = matrix.to_long_dataframe(tables=["DIST_AM"], zero_policy="exclude")
        >>> set(frame.columns)
        {'table_index', 'table', 'origin', 'destination', 'value'}
        """

        if zone_base not in {0, 1}:
            _raise_validation("zone_base must be 0 or 1")

        if not has_dataframe_support():
            raise ImportError(
                "pandas is required for to_long_dataframe(); install with "
                "`uv add despina[dataframe]` or `uv add pandas`; "
                "check availability via `despina.has_dataframe_support()`"
            )

        import pandas as pd

        columns = self._long_columns(
            tables=tables,
            include_zeros=include_zeros,
            zero_policy=zero_policy,
            zone_base=zone_base,
        )
        if columns["value"].size == 0:
            return pd.DataFrame(
                columns=["table_index", "table", "origin", "destination", "value"]
            )

        return pd.DataFrame(
            {
                "table_index": columns["table_index"],
                "table": columns["table"],
                "origin": columns["origin"],
                "destination": columns["destination"],
                "value": columns["value"],
            }
        )

    def to_parquet_long(
        self,
        path: str | Path,
        *,
        tables: TableKey | Sequence[TableKey] | None = None,
        include_zeros: bool | None = None,
        zero_policy: LongZeroPolicy = "auto",
        zone_base: int = 1,
        parquet_engine: str = "pyarrow",
    ) -> Path:
        """Write long-form records as Parquet and return output path.

        Requires pandas and the selected parquet engine.

        Examples
        --------
        >>> out = matrix.to_parquet_long("cells.parquet", tables="DIST_AM")
        >>> out.suffix
        '.parquet'
        """

        if not has_parquet_support(engine=parquet_engine):
            raise ImportError(
                "parquet export requires pandas and the selected engine; "
                f"engine={parquet_engine!r}. Check availability via "
                "`despina.has_parquet_support(engine=...)`."
            )

        frame = self.to_long_dataframe(
            tables=tables,
            include_zeros=include_zeros,
            zero_policy=zero_policy,
            zone_base=zone_base,
        )
        out_path = Path(path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(out_path, engine=parquet_engine, index=False)
        return out_path

    def _write_table_defs(self) -> list[TableDef]:
        """Return ``TableDef`` list for the current schema."""
        return [TableDef(meta.name, meta.type_code) for meta in self._table_metas]

    def _write_stack(self) -> np.ndarray:
        """Return a contiguous ``(table_count, zone_count, zone_count)`` stack."""
        return np.stack(
            [self._tables[meta.name] for meta in self._table_metas], axis=0
        )

    def write(self, path: str | Path) -> Path:
        """Write matrix contents to ``path`` and return output path.

        Existing files are overwritten.

        Examples
        --------
        >>> matrix.write("skims_updated.mat")
        """

        out_path = Path(path)
        _MatrixCore.write_from_stack(
            out_path,
            self._zone_count,
            self._write_table_defs(),
            self._write_stack(),
            banner=self._banner,
            run_id=self._run_id,
        )
        return out_path

    def to_bytes(self) -> bytes:
        """Serialise matrix contents to bytes.

        For caching, transport, and deep-copy operations.
        """

        return _MatrixCore.to_bytes_from_stack(
            self._zone_count,
            self._write_table_defs(),
            self._write_stack(),
            banner=self._banner,
            run_id=self._run_id,
        )

    def copy(self) -> Matrix:
        """Return a deep copy."""

        return Matrix(
            zone_count=self._zone_count,
            table_count=self._table_count,
            banner=self._banner,
            run_id=self._run_id,
            table_metas=self._table_metas,
            table_names=self._table_names,
            table_index_by_name=dict(self._table_index_by_name),
            tables={name: arr.copy() for name, arr in self._tables.items()},
        )

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def __getitem__(self, table: TableKey) -> np.ndarray:
        return self._tables[self._resolve_table_name(table)]

    def __setitem__(self, table: TableKey, values: TableArrayInput) -> None:
        name = self._resolve_table_name(table)
        array = np.ascontiguousarray(values, dtype=np.float64)
        expected = (self._zone_count, self._zone_count)
        if array.shape != expected:
            _raise_validation(
                f"array shape {array.shape} does not match expected {expected}"
            )
        self._tables[name] = array

    def __contains__(self, table: object) -> bool:
        if isinstance(table, str):
            return table in self._table_index_by_name
        return False

    def __iter__(self) -> Iterator[str]:
        return iter(self._table_names)

    def __len__(self) -> int:
        return self._table_count

    def keys(self) -> tuple[str, ...]:
        """Return table names in header order."""

        return self.table_names

    def values(self) -> tuple[np.ndarray, ...]:
        """Return table arrays in header order as 2D ``float64`` arrays."""

        return tuple(self._tables[name] for name in self._table_names)

    def items(self) -> tuple[tuple[str, np.ndarray], ...]:
        """Return ``(table_name, ndarray)`` pairs in header order."""

        return tuple(
            (name, self._tables[name]) for name in self._table_names
        )

    def __repr__(self) -> str:
        return (
            f"Matrix(zone_count={self.zone_count}, table_count={self.table_count}, "
            f"table_names={list(self.table_names)!r})"
        )
