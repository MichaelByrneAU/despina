NumPy Workflows
================

The :class:`despina.Matrix <despina.matrix.Matrix>` API stores each table as a
``(zone_count, zone_count)`` float64 NumPy array. Subscript access
(``matrix["DIST_AM"]``) returns the stored array directly.

Core Edit Pattern
-----------------

Subscript access returns the stored array. In-place edits apply to the matrix
immediately.

.. code-block:: python

   import despina
   import numpy as np

   matrix = despina.read("skims.mat")

   dist = matrix["DIST_AM"]
   dist *= 1.03
   dist[dist < 0.0] = 0.0

   # No assignment needed: dist is the matrix's own array.

When you create a **new** array (e.g. with an expression like
``matrix["T"] * 1.02``), the result is a separate array. Assign it back to
update the matrix.

.. code-block:: python

   matrix["DIST_AM"] = matrix["DIST_AM"] * 1.02

When To Assign Back
-------------------

Subscript access (``matrix["DIST_AM"]``) and ``dict(matrix)`` return the stored
arrays. In-place operations (``*=``, indexed assignment, ``np.add.at``) modify
the matrix directly. Operations that produce a new array (``*``, ``+``,
``np.maximum``) do not, so the result must be assigned back.

.. code-block:: python

   # In-place: modifies the matrix directly.
   matrix["DIST_AM"] *= 1.05

   # New array: must assign back.
   matrix["DIST_AM"] = np.maximum(matrix["DIST_AM"], 0.0)

Single-Table Patterns
---------------------

Scaling a table applies a multiplier to every cell in one step. Since ``*=`` is
an in-place operation, the matrix is updated directly.

.. code-block:: python

   matrix["DIST_AM"] *= 1.05

One table can serve as a mask over another. The boolean comparison produces an
array of the same shape. Only the cells where the condition is true receive the
adjusted value. In the example below, distance cells are discounted by 10 percent
wherever the corresponding travel time exceeds 45 minutes. All other cells are
unchanged.

.. code-block:: python

   dist = matrix["DIST_AM"]
   time = matrix["TIME_AM"]

   dist[time > 45.0] *= 0.9

``np.ix_`` converts two 1D index arrays into a pair of broadcastable arrays
that together select a rectangular block from the 2D table. The result is an
open mesh index suitable for NumPy fancy indexing. In the example below, a
4 x 3 submatrix covering four specific origin zones and three destination zones
is scaled by 15 percent. No other cells are touched. Remember that these are
0-based indices, so index 0 corresponds to zone 1.

.. code-block:: python

   dist = matrix["DIST_AM"]

   selected_origins = np.array([0, 1, 2, 3])
   selected_destinations = np.array([10, 11, 12])
   dist[np.ix_(selected_origins, selected_destinations)] *= 1.15

Sparse Updates by Coordinate
----------------------------

When updates arrive as sparse event lists of (origin index, destination index,
delta) triples, ``np.add.at`` gives correct accumulation semantics for repeated
coordinates. Plain indexed addition (``dist[indices] += deltas``) uses a
buffered write path that may apply only one contribution per unique coordinate
when indices repeat. ``np.add.at`` is unbuffered and applies each delta
individually. In the example below, coordinate (4, 7) appears twice with deltas
of 2.0 and 1.0, so its cell receives a total addition of 3.0.

.. code-block:: python

   dist = matrix["DIST_AM"]

   origin_index = np.array([4, 4, 9, 9])
   destination_index = np.array([7, 7, 3, 8])
   deltas = np.array([2.0, 1.0, -0.5, 3.0])

   np.add.at(dist, (origin_index, destination_index), deltas)

Edit Multiple Tables Together
-----------------------------

The dict-like interface is a natural fit when related edits need to be
coordinated. Since subscript access returns the stored arrays, in-place edits
apply to the matrix directly.

.. code-block:: python

   matrix["DIST_AM"] *= 1.03
   matrix["TIME_AM"] *= 0.98

Batch Updates with a Transform Loop
------------------------------------

A dictionary of callables expresses many table operations in one loop. Each
value in the mapping is a callable that receives the current table as a NumPy
array and returns the modified version. The two transforms in the example apply
different operations to each table: a proportional scale on distance and a
zero-floor clamp on time.

.. code-block:: python

   transforms = {
       "DIST_AM": lambda values: values * 1.02,
       "TIME_AM": lambda values: np.maximum(values, 0.0),
   }

   for name, fn in transforms.items():
       matrix[name] = fn(matrix[name])

Whole-Table Replacement
-----------------------

Subscript assignment replaces an entire table in one expression.

.. code-block:: python

   matrix["DIST_AM"] = matrix["DIST_AM"] * 1.01

The subscript form is natural when the new value is a straightforward array
expression.

Long-Form Export
----------------

In long form each row holds a single cell value alongside its origin,
destination, and table name, rather than one row per OD pair with one value
column per table. This suits downstream systems that expect a normalised record
layout and is also the natural format for compact sparse output containing only
non-zero values.

.. code-block:: python

   matrix.to_csv_long("cells.csv", zero_policy="exclude")
   matrix.to_csv_long("dist_only.csv", tables="DIST_AM", zero_policy="exclude")

   frame = matrix.to_long_dataframe(zero_policy="exclude")
   matrix.to_parquet_long("cells.parquet", zero_policy="exclude")

:meth:`~despina.matrix.Matrix.to_csv_long` writes all selected tables to a
single CSV file. Passing ``tables=`` restricts the output to a named subset.
:meth:`~despina.matrix.Matrix.to_long_dataframe` returns the equivalent data as
an in-memory dataframe without writing a file.
:meth:`~despina.matrix.Matrix.to_parquet_long` writes to Parquet. For very
large matrices, ``zero_policy="exclude"`` is usually the right default to avoid
writing large numbers of zero-valued rows.

Shape and Indexing Reminders
----------------------------

The following conventions govern array shapes and index interpretation
throughout the API:

1. Table arrays have shape ``(zone_count, zone_count)``.
2. All indexing is 0-based NumPy indexing. Zone 1 in the ``.mat`` file
   corresponds to row/column index 0.
3. ``matrix["table"]`` returns the stored array. In-place edits apply
   immediately.
4. ``matrix["table"] = arr`` replaces the stored array after shape validation.
   Use this when assigning a new array produced by an expression.

Performance Tips
----------------

Prefer vectorised NumPy operations over Python loops. In-place operations
(``*=``, indexed assignment) avoid creating intermediate arrays.

For multi-table jobs, ``np.stack(list(matrix.values()))`` produces a single
3D array for cross-table operations.
