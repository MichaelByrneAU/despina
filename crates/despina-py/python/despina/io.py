from __future__ import annotations

from pathlib import Path
from typing import Iterable, Mapping, Sequence

from .matrix import Matrix, TableLike
from .schema import TypeCode
from .tabular import (
    matrix_from_csv,
    matrix_from_pandas,
    matrix_from_parquet,
    matrix_from_polars,
)


def read(
    path: str | Path,
    *,
    tables: Sequence[str] | None = None,
) -> Matrix:
    """Read a ``.mat`` file from disk into an eager :class:`Matrix`.

    The standard entry point for random access to tables and cells, NumPy
    transforms, and tabular export.

    Parameters
    ----------
    path:
        Path to the source ``.mat`` file.
    tables:
        Optional subset of table names to load. When provided, only the
        named tables are decoded and allocated, while other tables are
        skipped. Pass ``None`` (the default) to load all tables.

    Returns
    -------
    Matrix
        Loaded matrix with parsed header metadata and table values.

    Examples
    --------
    >>> import despina
    >>> matrix = despina.read("skims.mat")
    >>> matrix.zone_count >= 1
    True
    """

    return Matrix.read(path, tables=tables)


def from_bytes(
    data: bytes | bytearray | memoryview,
    *,
    tables: Sequence[str] | None = None,
) -> Matrix:
    """Parse an eager :class:`Matrix` from in-memory bytes.

    Accepts API payloads, object-store reads, or cache layers where matrix
    bytes are already materialised in memory.

    Parameters
    ----------
    data:
        Byte payload containing valid ``.mat`` content.
    tables:
        Optional subset of table names to load. When provided, only the
        named tables are decoded and allocated, while other tables are
        skipped. Pass ``None`` (the default) to load all tables.

    Returns
    -------
    Matrix
        Parsed in-memory matrix.

    Examples
    --------
    >>> import despina
    >>> source = despina.create(2, [("DIST_AM", "D")])
    >>> payload = source.to_bytes()
    >>> restored = despina.from_bytes(payload)
    >>> restored.table_names
    ('DIST_AM',)
    """

    return Matrix.from_bytes(data, tables=tables)


def create(
    zone_count: int,
    tables: Iterable[TableLike],
    *,
    banner: str | None = None,
    run_id: str | None = None,
) -> Matrix:
    """Create a new empty eager matrix with explicit schema.

    Parameters
    ----------
    zone_count:
        Number of origins and destinations. Must be greater than zero.
    tables:
        Table definitions in header order. Each item may be a
        :class:`despina.schema.TableSpec`, a core ``TableDef``, or a
        ``(name, type_code)`` pair.
    banner, run_id:
        Optional header metadata written to output files.

    Returns
    -------
    Matrix
        New matrix with all table values initialised to ``0.0``.

    Examples
    --------
    >>> import despina
    >>> matrix = despina.create(
    ...     2,
    ...     [
    ...         despina.TableSpec.float64("DIST_AM"),
    ...         despina.TableSpec.float32("TIME_AM"),
    ...     ],
    ... )
    >>> matrix.table_names
    ('DIST_AM', 'TIME_AM')
    """

    return Matrix.create(zone_count, tables, banner=banner, run_id=run_id)


def like(
    other: Matrix,
    *,
    banner: str | None = None,
    run_id: str | None = None,
) -> Matrix:
    """Create a new zero-initialised matrix with the same schema as *other*.

    Convenience wrapper around :meth:`Matrix.like`.

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

    Examples
    --------
    >>> import despina
    >>> source = despina.create(2, [("DIST_AM", "D")])
    >>> copy = despina.like(source)
    >>> copy.zone_count == source.zone_count
    True
    """

    return Matrix.like(other, banner=banner, run_id=run_id)


def from_csv(
    path: str | Path,
    *,
    origin_col: str = "Origin",
    destination_col: str = "Destination",
    table_columns: Sequence[str] | None = None,
    rename_tables: Mapping[str, str] | None = None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None = None,
    zone_base: int = 1,
    zone_count: int | None = None,
    on_duplicate_od: str = "error",
    on_missing_od: str = "warn",
    missing_warning_limit: int = 10,
    include_zero_rows: bool = True,
    sort_od: bool = False,
    strict_zone_range: bool = True,
    drop_extra_columns: bool = True,
    encoding: str = "utf-8",
    sep: str = ",",
    quotechar: str = '"',
    csv_kwargs: Mapping[str, object] | None = None,
    banner: str | None = None,
    run_id: str | None = None,
) -> Matrix:
    """Build a matrix from wide OD CSV input.

    The input is expected to follow canonical wide OD form:
    ``Origin``, ``Destination``, then one or more table value columns. OD
    column names and table mapping rules are configurable for non-standard
    exports.

    Validation behaviour is explicit and configurable. Duplicate OD pairs are
    rejected. Missing OD pairs can warn, error, or be ignored.

    Parameters
    ----------
    path:
        CSV file path.
    origin_col, destination_col:
        Column names for origin and destination identifiers.
    table_columns:
        Optional explicit list of value columns to treat as matrix tables.
        Defaults to all non-OD columns.
    rename_tables:
        Optional mapping from source value column names to output table names.
    table_type_codes:
        Optional mapping assigning type codes per table.
    zone_base:
        Base for OD identifiers in source data. Use ``1`` for 1-based zones
        (default) or ``0`` for 0-based positions.
    zone_count:
        Optional explicit zone count. When omitted, inferred from observed
        OD identifiers.
    on_duplicate_od:
        Duplicate OD handling policy. Currently only ``"error"`` is supported.
    on_missing_od:
        Missing OD handling policy: ``"warn"``, ``"ignore"``, or ``"error"``.
    missing_warning_limit:
        Maximum number of missing OD pairs to include in warning previews.
    include_zero_rows:
        When ``False``, input rows where all selected table values are zero are
        discarded before matrix construction.
    sort_od:
        When ``True``, OD rows are sorted by ``(origin, destination)`` before
        matrix construction.
    strict_zone_range:
        When ``True``, observed OD indexes beyond ``zone_count`` raise a
        validation error.
    drop_extra_columns:
        When ``False``, extra non-selected value columns raise an error.
    encoding, sep, quotechar, csv_kwargs:
        CSV parser options.
    banner, run_id:
        Optional header metadata for the resulting matrix.

    Returns
    -------
    Matrix
        Matrix built from validated CSV rows.

    Examples
    --------
    >>> import despina
    >>> matrix = despina.from_csv("skims_wide.csv")
    >>> matrix.table_count >= 1
    True

    Non-canonical input names can be mapped:

    >>> matrix = despina.from_csv(
    ...     "skims_export.csv",
    ...     origin_col="O",
    ...     destination_col="D",
    ...     rename_tables={"DistanceKm": "DIST_AM"},
    ... )
    """

    return matrix_from_csv(
        path,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        rename_tables=rename_tables,
        table_type_codes=table_type_codes,
        zone_base=zone_base,
        zone_count=zone_count,
        on_duplicate_od=on_duplicate_od,
        on_missing_od=on_missing_od,
        missing_warning_limit=missing_warning_limit,
        include_zero_rows=include_zero_rows,
        sort_od=sort_od,
        strict_zone_range=strict_zone_range,
        drop_extra_columns=drop_extra_columns,
        encoding=encoding,
        sep=sep,
        quotechar=quotechar,
        csv_kwargs=csv_kwargs,
        banner=banner,
        run_id=run_id,
    )


def from_parquet(
    path: str | Path,
    *,
    origin_col: str = "Origin",
    destination_col: str = "Destination",
    table_columns: Sequence[str] | None = None,
    rename_tables: Mapping[str, str] | None = None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None = None,
    zone_base: int = 1,
    zone_count: int | None = None,
    on_duplicate_od: str = "error",
    on_missing_od: str = "warn",
    missing_warning_limit: int = 10,
    include_zero_rows: bool = True,
    sort_od: bool = False,
    strict_zone_range: bool = True,
    drop_extra_columns: bool = True,
    columns: Sequence[str] | None = None,
    parquet_engine: str = "auto",
    parquet_kwargs: Mapping[str, object] | None = None,
    banner: str | None = None,
    run_id: str | None = None,
) -> Matrix:
    """Build a matrix from wide OD Parquet input.

    Behaviour mirrors :func:`from_csv`, including OD validation defaults,
    table renaming, type-code assignment, and zone-base handling.

    Parameters
    ----------
    path:
        Parquet file path.
    origin_col, destination_col, table_columns, rename_tables, table_type_codes:
        Same semantics as :func:`from_csv`.
    zone_base, zone_count, on_duplicate_od, on_missing_od:
        Same validation controls as :func:`from_csv`.
    missing_warning_limit, include_zero_rows, sort_od, strict_zone_range:
        Same ingestion controls as :func:`from_csv`.
    drop_extra_columns:
        Controls handling of non-selected value columns.
    columns:
        Optional subset of parquet columns to read from source.
    parquet_engine:
        ``"auto"``, ``"pyarrow"``, ``"fastparquet"``, or ``"polars"``.
    parquet_kwargs:
        Engine-specific options passed to backend readers.
    banner, run_id:
        Optional header metadata for the resulting matrix.

    Returns
    -------
    Matrix
        Matrix built from validated parquet rows.

    Examples
    --------
    >>> import despina
    >>> matrix = despina.from_parquet("skims_wide.parquet")
    >>> "DIST_AM" in matrix
    True
    """

    return matrix_from_parquet(
        path,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        rename_tables=rename_tables,
        table_type_codes=table_type_codes,
        zone_base=zone_base,
        zone_count=zone_count,
        on_duplicate_od=on_duplicate_od,
        on_missing_od=on_missing_od,
        missing_warning_limit=missing_warning_limit,
        include_zero_rows=include_zero_rows,
        sort_od=sort_od,
        strict_zone_range=strict_zone_range,
        drop_extra_columns=drop_extra_columns,
        columns=columns,
        parquet_engine=parquet_engine,
        parquet_kwargs=parquet_kwargs,
        banner=banner,
        run_id=run_id,
    )


def from_pandas(
    frame,
    *,
    origin_col: str = "Origin",
    destination_col: str = "Destination",
    table_columns: Sequence[str] | None = None,
    rename_tables: Mapping[str, str] | None = None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None = None,
    zone_base: int = 1,
    zone_count: int | None = None,
    on_duplicate_od: str = "error",
    on_missing_od: str = "warn",
    missing_warning_limit: int = 10,
    include_zero_rows: bool = True,
    sort_od: bool = False,
    strict_zone_range: bool = True,
    drop_extra_columns: bool = True,
    banner: str | None = None,
    run_id: str | None = None,
) -> Matrix:
    """Build a matrix from a pandas DataFrame in wide OD form.

    Parameters mirror :func:`from_csv`, except that tabular input is supplied as
    an in-memory pandas DataFrame.

    Parameters
    ----------
    frame:
        pandas DataFrame containing OD and table columns.
    origin_col, destination_col, table_columns, rename_tables, table_type_codes:
        Column and schema mapping controls.
    zone_base, zone_count, on_duplicate_od, on_missing_od:
        OD interpretation and validation controls.
    missing_warning_limit, include_zero_rows, sort_od, strict_zone_range:
        Ingestion behaviour controls.
    drop_extra_columns:
        Controls handling of non-selected value columns.
    banner, run_id:
        Optional header metadata for the resulting matrix.

    Returns
    -------
    Matrix
        Matrix built from validated dataframe rows.

    Examples
    --------
    >>> import pandas as pd
    >>> import despina
    >>> frame = pd.DataFrame(
    ...     {
    ...         "Origin": [1, 1, 2, 2],
    ...         "Destination": [1, 2, 1, 2],
    ...         "DIST_AM": [0.0, 7.5, 8.1, 0.0],
    ...     }
    ... )
    >>> matrix = despina.from_pandas(frame)
    >>> matrix.zone_count
    2
    """

    return matrix_from_pandas(
        frame,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        rename_tables=rename_tables,
        table_type_codes=table_type_codes,
        zone_base=zone_base,
        zone_count=zone_count,
        on_duplicate_od=on_duplicate_od,
        on_missing_od=on_missing_od,
        missing_warning_limit=missing_warning_limit,
        include_zero_rows=include_zero_rows,
        sort_od=sort_od,
        strict_zone_range=strict_zone_range,
        drop_extra_columns=drop_extra_columns,
        banner=banner,
        run_id=run_id,
    )


def from_polars(
    frame,
    *,
    origin_col: str = "Origin",
    destination_col: str = "Destination",
    table_columns: Sequence[str] | None = None,
    rename_tables: Mapping[str, str] | None = None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None = None,
    zone_base: int = 1,
    zone_count: int | None = None,
    on_duplicate_od: str = "error",
    on_missing_od: str = "warn",
    missing_warning_limit: int = 10,
    include_zero_rows: bool = True,
    sort_od: bool = False,
    strict_zone_range: bool = True,
    drop_extra_columns: bool = True,
    banner: str | None = None,
    run_id: str | None = None,
) -> Matrix:
    """Build a matrix from a polars DataFrame in wide OD form.

    Parameters mirror :func:`from_csv`, except that tabular input is supplied as
    an in-memory polars DataFrame.

    Parameters
    ----------
    frame:
        polars DataFrame containing OD and table columns.
    origin_col, destination_col, table_columns, rename_tables, table_type_codes:
        Column and schema mapping controls.
    zone_base, zone_count, on_duplicate_od, on_missing_od:
        OD interpretation and validation controls.
    missing_warning_limit, include_zero_rows, sort_od, strict_zone_range:
        Ingestion behaviour controls.
    drop_extra_columns:
        Controls handling of non-selected value columns.
    banner, run_id:
        Optional header metadata for the resulting matrix.

    Returns
    -------
    Matrix
        Matrix built from validated dataframe rows.

    Examples
    --------
    >>> import polars as pl
    >>> import despina
    >>> frame = pl.DataFrame(
    ...     {
    ...         "Origin": [1, 1, 2, 2],
    ...         "Destination": [1, 2, 1, 2],
    ...         "DIST_AM": [0.0, 7.5, 8.1, 0.0],
    ...     }
    ... )
    >>> matrix = despina.from_polars(frame)
    >>> matrix.table_names
    ('DIST_AM',)
    """

    return matrix_from_polars(
        frame,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        rename_tables=rename_tables,
        table_type_codes=table_type_codes,
        zone_base=zone_base,
        zone_count=zone_count,
        on_duplicate_od=on_duplicate_od,
        on_missing_od=on_missing_od,
        missing_warning_limit=missing_warning_limit,
        include_zero_rows=include_zero_rows,
        sort_od=sort_od,
        strict_zone_range=strict_zone_range,
        drop_extra_columns=drop_extra_columns,
        banner=banner,
        run_id=run_id,
    )
