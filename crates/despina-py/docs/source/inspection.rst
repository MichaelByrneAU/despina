Inspecting Matrices and Tables
==============================

The :class:`~despina.matrix.Matrix` object exposes metadata at two levels:
global header attributes that describe the file as a whole, and per-table
metadata available through the :attr:`~despina.matrix.Matrix.tables` property
and the table selector methods.

Matrix Metadata
---------------

Global header attributes are available directly on the matrix object.

.. code-block:: python

   import despina

   matrix = despina.read("skims.mat")

   print(matrix.zone_count)   # Origin-destination dimension.
   print(matrix.table_count)  # Number of tables in the file.
   print(matrix.banner)       # Text annotation embedded in the header.
   print(matrix.run_id)       # Run identifier embedded in the header.

All four attributes reflect values written into the binary file header.
``zone_count`` determines the shape of every table: each table is a
``zone_count × zone_count`` array. ``banner`` and ``run_id`` are strings
set at write time and carry no structural meaning within the file.

Table Metadata
--------------

The :attr:`~despina.matrix.Matrix.tables` property returns a tuple of
:class:`~despina.matrix.TableMeta` objects in file order. Each holds the
table's 0-based index, string name, and type code.

.. code-block:: python

   for meta in matrix.tables:
       print(meta.index, meta.name, meta.type_code)

Example output:

.. code-block:: text

   0 DIST_AM D
   1 TIME_AM S
   2 COST_AM D

``index`` is the 0-based table position used by positional selectors. ``name``
is the string identifier used throughout the API. ``type_code`` is the on-disk
encoding token. See :doc:`type_codes` for the mapping between tokens, numeric
precision, and representable range.

Two further properties summarise the schema without returning full metadata
objects:

.. code-block:: python

   print(matrix.table_names)  # ('DIST_AM', 'TIME_AM', 'COST_AM')
   print(matrix.table_defs)   # [('DIST_AM', 'D'), ('TIME_AM', 'S'), ('COST_AM', 'D')]

Iterating the matrix directly yields table name strings in file order.

.. code-block:: python

   for name in matrix:
       print(name, matrix[name].sum())

Selecting Tables
----------------

Subscript access returns the table as a 2D ``float64`` NumPy array.
The ``in`` operator tests whether a table name exists.

.. code-block:: python

   dist = matrix["DIST_AM"]  # Returns a 2D float64 NumPy array.

   if "TOLL_AM" in matrix:
       toll = matrix["TOLL_AM"]

:meth:`~despina.matrix.Matrix.table_index_by_name` resolves a name to its
0-based integer index, or returns ``None`` when the name is not present.

.. code-block:: python

   idx = matrix.table_index_by_name("TIME_AM")  # E.g. 1, or None if absent.

Methods that accept a ``tables=`` keyword take a single selector or a sequence
of selectors. Duplicate selectors that resolve to the same table are rejected.

.. code-block:: python

   matrix.to_csv_long("dist_only.csv", tables="DIST_AM", zero_policy="exclude")
   matrix.to_csv_long(
       "dist_time.csv",
       tables=["DIST_AM", "TIME_AM"],
       zero_policy="exclude",
   )

Table Data and Summaries
------------------------

Subscript access returns the table as a 2D ``float64`` NumPy array. Standard
NumPy operations provide summaries directly on the returned array.

.. code-block:: python

   import numpy as np

   dist = matrix["DIST_AM"]

   print(dist.shape)              # (zone_count, zone_count)
   print(dist.sum())              # Sum of all cell values.
   print(np.diag(dist).sum())     # Sum of intrazonal cells (origin == destination).

Aggregate array operations are also available directly:

.. code-block:: python

   print(dist.sum(axis=1))        # 1D float64 array of per-origin totals.
   print(dist.sum(axis=0))        # 1D float64 array of per-destination totals.
   print(np.diag(dist))           # 1D float64 array of diagonal values.

``dict(matrix)`` returns all tables as a name-keyed dictionary of NumPy arrays.
A subset can be constructed with a comprehension.

.. code-block:: python

   arrays = dict(matrix)                                          # All tables.
   subset = {n: matrix[n] for n in ["DIST_AM", "TIME_AM"]}       # Named subset.

Reading from Bytes
------------------

:func:`despina.from_bytes` parses a matrix from an in-memory byte sequence
rather than a file path. It accepts ``bytes``, ``bytearray``, or ``memoryview``
and returns a fully initialised :class:`~despina.matrix.Matrix`.

.. code-block:: python

   payload = load_bytes_somehow()
   matrix = despina.from_bytes(payload)  # No intermediate file required.
   print(matrix.zone_count, matrix.table_count)
