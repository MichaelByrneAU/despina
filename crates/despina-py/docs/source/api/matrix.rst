Matrix API
==========

The in-memory API is centred on :class:`despina.Matrix`. A matrix stores one
or more named tables, each a ``(zone_count, zone_count)`` float64 NumPy array.

Indexing
--------

All cell indexing is 0-based NumPy indexing. Zone 1 in the ``.mat`` file
corresponds to row/column index 0.

Quick Example
-------------

.. code-block:: python

   import despina

   matrix = despina.read("skims.mat")

   dist = matrix["DIST_AM"]       # Returns the stored NumPy array.
   dist *= 1.03                   # In-place, modifies matrix directly.
   dist[0, 0] = 0.0               # Single cell edit.

   matrix.to_parquet("skims_wide.parquet")

Important behavioural notes:

1. Subscript access (``matrix["DIST_AM"]``) returns the stored array.
   In-place edits modify the matrix directly.
2. Subscript assignment (``matrix["DIST_AM"] = arr``) replaces the stored
   array after shape validation. Use this when assigning a new array
   produced by an expression.
3. Export helpers exclude all-zero rows by default unless
   ``include_zero_rows=True`` is passed.

Module Reference
----------------

.. automodule:: despina.matrix
   :members:
