Quickstart
==========

These examples follow one :class:`despina.Matrix <despina.matrix.Matrix>` from
creation through editing, serialisation, and tabular interchange. The sequence
covers five stages:

1. A matrix is created with a fixed zone count and an ordered table schema.
2. Scalar and vectorised edits are applied, then table-level diagnostics are checked.
3. ``.mat`` write and read are shown side by side as a usage demonstration.
4. Wide OD tabular output is exported to CSV and Parquet.
5. The tabular output is imported back into a matrix after OD and schema validation.

Installation
------------

`uv <https://docs.astral.sh/uv/>`_ is used to manage the Python environment in
these examples. Equivalent ``pip`` workflows work when the same dependencies are
installed.

Install the base package:

.. code-block:: bash

   uv add despina

Optional dependencies are only needed for dataframe and Parquet APIs:

- Install ``pandas`` for :func:`despina.from_pandas <despina.from_pandas>` and
  :meth:`despina.matrix.Matrix.to_pandas`.
- Install ``polars`` for :func:`despina.from_polars <despina.from_polars>` and
  :meth:`despina.matrix.Matrix.to_polars`.
- Use a Parquet backend for :func:`despina.from_parquet <despina.from_parquet>`
  and :meth:`despina.matrix.Matrix.to_parquet`. Supported backend paths are
  ``pandas`` with ``pyarrow`` or ``fastparquet``, and ``polars``.

For one-off execution without creating a project, ``uv`` can run a script in a
temporary environment that includes ``despina``:

.. code-block:: bash

   uv run --with despina python your_script.py

Step 1: Create a Matrix
-----------------------

A :class:`despina.Matrix <despina.matrix.Matrix>` is defined by a zone count
and an ordered list of :class:`despina.TableSpec <despina.schema.TableSpec>`
entries. Each entry carries a table name and storage type code. Table order is
preserved in file headers and tabular exports.

.. code-block:: python

   import despina

   # Zone count and table order are fixed at construction and cannot be
   # changed afterwards.
   matrix = despina.create(
       3,
       [
           despina.TableSpec.float64("DIST_AM"),
           despina.TableSpec.float32("TIME_AM"),
       ],
       banner="MAT PGM=SKIMS VER=1",
       run_id="BASE",
   )

   # Every cell starts at 0.0. Type codes are preserved in file metadata.
   print(matrix.zone_count)
   print(matrix.table_names)

``banner`` and ``run_id`` set the string fields embedded in the ``.mat`` file
header. Both are optional and default to empty strings.

Step 2: Inspect and Edit Values
-------------------------------

Subscript access (``matrix["table_name"]``) returns the stored NumPy array.
In-place edits apply to the matrix immediately. Note that NumPy arrays use
0-based indexing, while ``.mat`` files use 1-based zone IDs: zone 1
corresponds to row/column index 0.

.. code-block:: python

   # Read and write individual cells (0-based indexing).
   matrix["DIST_AM"][0, 1] = 7.5
   assert matrix["DIST_AM"][0, 1] == 7.5

   # In-place operations modify the matrix directly.
   dist = matrix["DIST_AM"]
   dist *= 1.05
   dist[dist < 0.0] = 0.0

   # If you create a new array, assign it back to update the matrix.
   matrix["DIST_AM"] = dist * 1.01

Table totals provide a compact check of edited values before writing output.

.. code-block:: python

   import numpy as np

   print(matrix["DIST_AM"].sum())
   print(np.diag(matrix["DIST_AM"]).sum())

Step 3: Demonstrate ``.mat`` Write and Read
-------------------------------------------

:meth:`despina.matrix.Matrix.write` serialises the complete matrix to a
``.mat`` file. :func:`despina.read <despina.read>` parses it back into memory.
The two are shown together here to demonstrate the round-trip API.

.. code-block:: python

   matrix.write("skims_updated.mat")

   reloaded = despina.read("skims_updated.mat")
   print(reloaded.table_count)
   print(reloaded["DIST_AM"].sum())

Step 4: Export Wide OD Data
---------------------------

The canonical tabular form is wide OD: one row per origin-destination pair and
one value column per table. :meth:`~despina.matrix.Matrix.to_csv` has no
dataframe dependency. :meth:`~despina.matrix.Matrix.to_parquet` requires one of
the optional Parquet backends described in the installation section.

.. code-block:: python

   reloaded.to_csv("skims_wide.csv")
   reloaded.to_parquet("skims_wide.parquet")

By default, rows where every selected table value is zero are omitted. Dense
output is available by setting ``include_zero_rows=True``.

.. code-block:: python

   reloaded.to_csv("skims_dense.csv", include_zero_rows=True)

Step 5: Rebuild from CSV or Parquet
-----------------------------------

:func:`despina.from_csv <despina.from_csv>` and
:func:`despina.from_parquet <despina.from_parquet>` read the same wide OD
schema produced at export. Both paths validate OD keys and table columns before
constructing the matrix.

.. code-block:: python

   from_csv = despina.from_csv("skims_wide.csv")
   from_parquet = despina.from_parquet("skims_wide.parquet")

   from_csv.write("skims_from_csv.mat")
   from_parquet.write("skims_from_parquet.mat")

Optional DataFrame Flow
-----------------------

The dataframe adapters convert between wide OD frames and
:class:`despina.Matrix <despina.matrix.Matrix>` without writing intermediate
files.

.. code-block:: python

   import pandas as pd

   frame = pd.read_csv("skims_wide.csv")
   matrix_pd = despina.from_pandas(frame)
   frame_roundtrip = matrix_pd.to_pandas()

.. code-block:: python

   import polars as pl

   frame = pl.read_csv("skims_wide.csv")
   matrix_pl = despina.from_polars(frame)
   frame_roundtrip = matrix_pl.to_polars()
