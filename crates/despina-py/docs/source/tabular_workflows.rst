Tabular Workflows
=================

``despina`` uses a canonical wide OD shape for CSV, Parquet, pandas, and
polars interchange. Each row represents one origin/destination pair, table
values occupy separate columns, and the same conventions apply whether data
starts in files or dataframes. Validation controls for OD completeness and
schema correctness are available at both import and export.

Wide OD Shape
-------------

In wide OD form each row represents one origin/destination pair and table
values live in separate columns. The required OD columns are ``Origin`` and
``Destination`` by default. Additional columns map to matrix tables (for example
``DIST_AM``, ``TIME_AM``).

.. code-block:: text

   Origin,Destination,DIST_AM,TIME_AM
   1,1,0.0,0.0
   1,2,7.5,12.0
   2,1,8.1,13.4

Import from CSV
---------------

Use :func:`despina.from_csv` to ingest data from CSV sources.

.. code-block:: python

   import despina

   matrix = despina.from_csv(
       "skims_wide.csv",
       origin_col="Origin",
       destination_col="Destination",
   )

Import behaviour is strict by default. Duplicate OD pairs raise an error,
because duplicates almost always indicate an upstream aggregation defect.
Missing OD pairs trigger a warning by default and are filled with ``0.0``
in the resulting matrix.

.. code-block:: python

   matrix = despina.from_csv(
       "skims_sparse.csv",
       zone_count=120,
       on_missing_od="warn",
   )

Pass ``on_missing_od="ignore"`` only when sparse input is expected and
validated elsewhere.

Import with Different Column Names
----------------------------------

When source files use non-canonical naming, map OD and table columns explicitly
at import. This keeps your internal schema stable while accepting external naming
layouts.

.. code-block:: python

   matrix = despina.from_csv(
       "skims_export.csv",
       origin_col="O",
       destination_col="D",
       rename_tables={
           "DistanceKM": "DIST_AM",
           "TravelTimeMin": "TIME_AM",
       },
       table_type_codes={
           "DIST_AM": "D",
           "TIME_AM": "S",
       },
       zone_count=120,
   )

Import from Parquet
-------------------

:func:`despina.from_parquet` mirrors CSV import semantics. Backend selection
and engine-specific options are passed through ``parquet_engine`` and
``parquet_kwargs``.

.. code-block:: python

   matrix = despina.from_parquet(
       "skims_wide.parquet",
       parquet_engine="auto",
   )

Import from pandas or polars
----------------------------

When tabular processing has already happened in memory, use
:func:`despina.from_pandas` or :func:`despina.from_polars` to import directly
from a dataframe and avoid writing and re-reading an intermediate file.

.. code-block:: python

   import pandas as pd
   import despina

   frame = pd.read_csv("skims_wide.csv")
   matrix = despina.from_pandas(frame)

.. code-block:: python

   import polars as pl
   import despina

   frame = pl.read_csv("skims_wide.csv")
   matrix = despina.from_polars(frame)

Export Defaults
---------------

:meth:`~despina.matrix.Matrix.to_csv`,
:meth:`~despina.matrix.Matrix.to_parquet`,
:meth:`~despina.matrix.Matrix.to_pandas`, and
:meth:`~despina.matrix.Matrix.to_polars` all emit wide OD data.

By default, rows where all selected table values are zero are excluded. This
reduces output size significantly for sparse skim sets while preserving all
non-zero values.

.. code-block:: python

   matrix.to_csv("skims_sparse_view.csv")                         # Zero rows excluded by default.
   matrix.to_csv("skims_dense_view.csv", include_zero_rows=True)  # Include all OD pairs.

Rename Output Columns
---------------------

Output table columns can be renamed for publishing and mapped back during
re-import. This is useful when external naming conventions differ from your
internal schema.

.. code-block:: python

   matrix.to_csv(
       "skims_publish.csv",
       rename_columns={
           "DIST_AM": "DistanceKm_AM",
           "TIME_AM": "TimeMin_AM",
       },
   )

   rebuilt = despina.from_csv(
       "skims_publish.csv",
       rename_tables={
           "DistanceKm_AM": "DIST_AM",
           "TimeMin_AM": "TIME_AM",
       },
   )

More Import Options
-------------------

When source data is imperfect, four parameters control the most common
validation and normalisation adjustments.

- ``zone_base`` controls whether input OD identifiers are interpreted as
  0-based or 1-based integers.
- ``strict_zone_range`` controls whether observed zone identifiers outside
  ``zone_count`` raise immediately or expand the inferred range.
- ``drop_extra_columns`` controls whether non-selected value columns are
  ignored or rejected.
- ``sort_od`` controls whether rows are sorted before matrix population.
